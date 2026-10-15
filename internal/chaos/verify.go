package chaos

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/ZofSpades/tradewatch/internal/detect"
	"github.com/ZofSpades/tradewatch/internal/events"
	"github.com/ZofSpades/tradewatch/internal/events/twpb"
	"github.com/ZofSpades/tradewatch/internal/kafka"
	"github.com/ZofSpades/tradewatch/internal/replay"
	"github.com/ZofSpades/tradewatch/internal/rules"
)

// Fault is one fault the chaos script injected, as it logged it.
type Fault struct {
	At   time.Time `json:"at"`
	What string    `json:"what"`
}

// Config configures a verification.
type Config struct {
	Session       string
	Recording     string
	RulesDir      string
	PostgresURL   string
	Kafka         kafka.Config
	PrometheusURL string
	Faults        []Fault
	// Groups are the consumer groups that must drain before alerts are
	// compared.
	Groups []string
	Drain  time.Duration
	Commit string
	Log    *slog.Logger
}

// Recovery is a counter's increase over the run, from Prometheus.
type Recovery struct {
	What     string  `json:"what"`
	Increase float64 `json:"increase"`
}

// Result is the outcome of a chaos run.
type Result struct {
	Session  string    `json:"session"`
	Commit   string    `json:"commit,omitempty"`
	Started  time.Time `json:"started"`
	Finished time.Time `json:"finished"`
	Faults   []Fault   `json:"faults"`
	// Events and Labels are the recorded session's.
	Events     int        `json:"events"`
	Labels     int        `json:"labels"`
	Comparison Comparison `json:"comparison"`
	// AlertRecords counts every delivery of the session's alerts on the
	// alerts topic, AlertKeys their distinct keys, and Deliveries the
	// deliveries the alert sink counted in Postgres.
	AlertRecords int `json:"alert_records"`
	AlertKeys    int `json:"alert_keys"`
	Deliveries   int `json:"deliveries"`
	// RaisedTwice lists alerts whose audit trail records more than one
	// "raised" entry.
	RaisedTwice []string   `json:"raised_twice"`
	Recovery    []Recovery `json:"recovery"`
	Passed      bool       `json:"passed"`
}

// Verify waits for the pipeline to drain, then compares what it stored for
// the session with an offline replay of the session's recording.
func Verify(ctx context.Context, cfg Config) (*Result, error) {
	res := &Result{Session: cfg.Session, Commit: cfg.Commit, Faults: cfg.Faults, Finished: time.Now().UTC()}
	if len(cfg.Faults) > 0 {
		res.Started = cfg.Faults[0].At
	}
	cfg.Log.Info("waiting for the consumer groups to drain", "groups", cfg.Groups)
	if err := WaitDrained(ctx, cfg.Kafka, cfg.Log, cfg.Groups, cfg.Drain); err != nil {
		return nil, err
	}

	set, err := rules.Load(cfg.RulesDir)
	if err != nil {
		return nil, err
	}
	if err := detect.Validate(set); err != nil {
		return nil, err
	}
	out, err := replay.Run(ctx, cfg.Recording, []*rules.Set{set})
	if err != nil {
		return nil, fmt.Errorf("replay the recording: %w", err)
	}
	if out.Header.GetSessionId() != cfg.Session {
		return nil, fmt.Errorf("the recording is of session %s, not %s", out.Header.GetSessionId(), cfg.Session)
	}
	res.Events, res.Labels = out.Events, len(out.Labels)

	pg, err := pgx.Connect(ctx, cfg.PostgresURL)
	if err != nil {
		return nil, err
	}
	defer pg.Close(context.Background())
	stored, deliveries, err := storedAlerts(ctx, pg, cfg.Session)
	if err != nil {
		return nil, err
	}
	res.Deliveries = deliveries
	res.Comparison = Compare(out.Alerts[0], stored)
	if res.RaisedTwice, err = raisedTwice(ctx, pg, cfg.Session); err != nil {
		return nil, err
	}
	if res.AlertRecords, res.AlertKeys, err = SessionRecords(ctx, cfg.Kafka, cfg.Log, cfg.Kafka.Topics().Alerts, cfg.Session); err != nil {
		return nil, err
	}
	if cfg.PrometheusURL != "" && !res.Started.IsZero() {
		res.Recovery = recovery(ctx, cfg.PrometheusURL, time.Since(res.Started)+time.Minute, cfg.Log)
	}
	res.Passed = res.Comparison.Expected > 0 && res.Comparison.OK() && len(res.RaisedTwice) == 0 && len(res.Faults) > 0
	return res, nil
}

func storedAlerts(ctx context.Context, pg *pgx.Conn, session string) (map[string]*twpb.Alert, int, error) {
	rows, err := pg.Query(ctx, `SELECT alert_key, payload, deliveries FROM alerts WHERE session_id = $1`, session)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	out := map[string]*twpb.Alert{}
	total := 0
	for rows.Next() {
		var key string
		var payload []byte
		var n int
		if err := rows.Scan(&key, &payload, &n); err != nil {
			return nil, 0, err
		}
		var a twpb.Alert
		if err := events.Unmarshal(payload, &a); err != nil {
			return nil, 0, fmt.Errorf("alert %s: %w", key, err)
		}
		out[key] = &a
		total += n
	}
	return out, total, rows.Err()
}

func raisedTwice(ctx context.Context, pg *pgx.Conn, session string) ([]string, error) {
	rows, err := pg.Query(ctx, `SELECT a.alert_key FROM alert_audit a JOIN alerts USING (alert_key)
		WHERE alerts.session_id = $1 AND a.action = 'raised' GROUP BY a.alert_key HAVING count(*) > 1 ORDER BY 1`, session)
	if err != nil {
		return nil, err
	}
	keys, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if keys == nil {
		keys = []string{}
	}
	return keys, err
}

// recoveryQueries are the counters that show the pipeline absorbing the
// faults. A process killed between two scrapes loses its last increments,
// so the figures are lower bounds.
var recoveryQueries = []struct{ what, metric string }{
	{"raw records ingest dropped as duplicates", "tradewatch_ingest_duplicates_total"},
	{"market events the detectors dropped as duplicates", "tradewatch_detector_duplicate_events_total"},
	{"alerts regenerated while rebuilding state after a handover, and not produced again", "tradewatch_detector_replayed_alerts_total"},
	{"alert deliveries the sink absorbed by key", "tradewatch_alertsink_alert_duplicates_total"},
}

func recovery(ctx context.Context, promURL string, window time.Duration, log *slog.Logger) []Recovery {
	var out []Recovery
	secs := strconv.Itoa(int(math.Ceil(window.Seconds())))
	for _, q := range recoveryQueries {
		v, err := promScalar(ctx, promURL, fmt.Sprintf("sum(increase(%s[%ss]))", q.metric, secs))
		if err != nil {
			log.Warn("prometheus query failed", "metric", q.metric, "err", err)
			continue
		}
		out = append(out, Recovery{What: q.what, Increase: math.Round(v)})
	}
	return out
}

func promScalar(ctx context.Context, base, expr string) (float64, error) {
	u := base + "/api/v1/query?" + url.Values{"query": {expr}}.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return 0, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return 0, err
	}
	var pr struct {
		Status string `json:"status"`
		Data   struct {
			Result []struct {
				Value [2]any `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &pr); err != nil || pr.Status != "success" {
		return 0, fmt.Errorf("query %q: %s", expr, body)
	}
	if len(pr.Data.Result) == 0 {
		return 0, nil
	}
	s, _ := pr.Data.Result[0].Value[1].(string)
	return strconv.ParseFloat(s, 64)
}
