package marketsim

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/ZofSpades/tradewatch/internal/events"
	"github.com/ZofSpades/tradewatch/internal/events/twpb"
	"github.com/ZofSpades/tradewatch/internal/kafka"
	"github.com/ZofSpades/tradewatch/internal/metrics"
	"github.com/ZofSpades/tradewatch/internal/recording"
	"github.com/ZofSpades/tradewatch/internal/sim"
)

// Runner runs trading sessions one after another and exposes a small
// control API to start, stop and re-pace them.
type Runner struct {
	cfg     Config
	log     *slog.Logger
	cl      *kgo.Client
	adm     *kadm.Client
	uni     *sim.Universe
	pace    *pacer
	pm      *publishMetrics
	corrupt *corrupter
	prefix  string
	running prometheus.Gauge
	done    prometheus.Counter

	events    atomic.Uint64
	simTimeNs atomic.Int64

	mu        sync.Mutex
	state     string
	wantRun   bool
	cancel    context.CancelFunc
	session   sim.Session
	sessionNo int
	completed int
	wake      chan struct{}
}

// NewRunner returns a runner publishing through cl.
func NewRunner(cfg Config, cl *kgo.Client, reg prometheus.Registerer, log *slog.Logger) *Runner {
	r := &Runner{
		cfg: cfg, log: log, cl: cl, adm: kadm.NewClient(cl),
		uni:     sim.DefaultUniverse(),
		pace:    newPacer(cfg.Pace, cfg.Speed, cfg.Rate),
		pm:      newPublishMetrics(reg),
		corrupt: newCorrupter(cfg.MalformedRate, cfg.Seed, reg),
		prefix:  cfg.prefix(time.Now()),
		state:   "stopped",
		wake:    make(chan struct{}, 1),
		running: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: metrics.Namespace, Subsystem: "marketsim", Name: "running",
			Help: "1 while a session is being simulated.",
		}),
		done: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metrics.Namespace, Subsystem: "marketsim", Name: "sessions_completed_total",
			Help: "Sessions simulated to their close.",
		}),
	}
	reg.MustRegister(r.running, r.done)
	return r
}

// Universe returns the universe sessions run over.
func (r *Runner) Universe() *sim.Universe { return r.uni }

// SessionConfig returns the simulator configuration the runner uses for
// session n (0-based) when it starts at wallNow.
func (r *Runner) SessionConfig(n int, wallNow time.Time) sim.Config {
	return r.cfg.sessionConfig(n, r.prefix, wallNow)
}

// Start requests that sessions run.
func (r *Runner) Start() {
	r.mu.Lock()
	r.wantRun = true
	r.mu.Unlock()
	r.signal()
}

// Stop ends the current session early and pauses until Start.
func (r *Runner) Stop() {
	r.mu.Lock()
	r.wantRun = false
	if r.cancel != nil {
		r.cancel()
	}
	r.mu.Unlock()
	r.signal()
}

func (r *Runner) signal() {
	select {
	case r.wake <- struct{}{}:
	default:
	}
}

// Run creates the topics, publishes reference data and simulates sessions
// until the configured number is done or ctx is cancelled.
func (r *Runner) Run(ctx context.Context) error {
	topics := r.cfg.Kafka.Topics()
	if err := kafka.EnsureTopics(ctx, r.adm, topics.Specs(r.cfg.Kafka.Partitions), r.cfg.Kafka.Replication); err != nil {
		return err
	}
	if err := r.publishRefData(ctx, r.uni.RefData()...); err != nil {
		return fmt.Errorf("publish reference data: %w", err)
	}
	r.log.Info("reference data published", "accounts", len(r.uni.Accounts), "instruments", len(r.uni.Instruments))
	if r.cfg.AutoStart {
		r.Start()
	}
	for n := 0; r.cfg.Sessions == 0 || n < r.cfg.Sessions; n++ {
		sctx, ok := r.awaitStart(ctx)
		if !ok {
			return nil
		}
		err := r.runSession(ctx, sctx, n)
		r.mu.Lock()
		r.cancel()
		r.cancel = nil
		r.mu.Unlock()
		if ctx.Err() != nil {
			return nil
		}
		if err != nil {
			return err
		}
	}
	r.mu.Lock()
	r.state = "finished"
	r.mu.Unlock()
	return nil
}

func (r *Runner) awaitStart(ctx context.Context) (context.Context, bool) {
	for {
		r.mu.Lock()
		if r.wantRun {
			sctx, cancel := context.WithCancel(ctx)
			r.cancel = cancel
			r.state = "running"
			r.mu.Unlock()
			return sctx, true
		}
		r.state = "stopped"
		r.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, false
		case <-r.wake:
		}
	}
}

func (r *Runner) runSession(ctx, sctx context.Context, n int) error {
	scfg := r.SessionConfig(n, time.Now())
	var rec *recording.Writer
	if r.cfg.RecordDir != "" {
		cfgJSON, _ := json.Marshal(scfg)
		var err error
		rec, err = recording.Create(filepath.Join(r.cfg.RecordDir, scfg.SessionID+".twrec.gz"), &twpb.RecordingHeader{
			SessionId: scfg.SessionID, CreatedAtNs: time.Now().UnixNano(), ConfigJson: string(cfgJSON),
		})
		if err != nil {
			return fmt.Errorf("create recording: %w", err)
		}
	}
	pub := newPublisher(sctx, r.cl, r.cfg.Kafka.Topics(), scfg.SessionID, r.pace, rec, r.pm, r.corrupt, func(t int64) {
		r.events.Add(1)
		r.simTimeNs.Store(t)
		r.pm.simTime.Set(float64(t) / 1e9)
	})
	s, err := sim.New(scfg, r.uni, pub)
	if err != nil {
		return err
	}
	session := &twpb.RefDataRecord{Record: &twpb.RefDataRecord_Session{Session: s.Session().Proto()}}
	if err := r.publishRefData(ctx, session); err != nil {
		return fmt.Errorf("publish session: %w", err)
	}
	if rec != nil {
		for _, rd := range append(r.uni.RefData(), session) {
			if err := rec.WriteRefData(rd); err != nil {
				return err
			}
		}
	}

	r.mu.Lock()
	r.session, r.sessionNo = s.Session(), n+1
	r.mu.Unlock()
	r.pace.reanchor()
	r.running.Set(1)
	r.log.Info("session started", "session", scfg.SessionID, "seed", scfg.Seed, "open", scfg.Open, "duration", scfg.Duration, "pace", r.cfg.Pace)

	runErr := s.Run(sctx)
	r.running.Set(0)
	flushCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	flushErr := r.cl.Flush(flushCtx)
	var recErr error
	if rec != nil {
		recErr = rec.Close()
	}

	stats := s.Stats()
	stopped := errors.Is(runErr, context.Canceled)
	if stopped {
		runErr = nil
	} else if runErr == nil {
		r.mu.Lock()
		r.completed++
		r.mu.Unlock()
		r.done.Inc()
	}
	r.log.Info("session ended", "session", scfg.SessionID, "stopped_early", stopped,
		"events", stats.Events, "orders", stats.Orders, "trades", stats.Trades, "volume", stats.Volume)
	return errors.Join(runErr, flushErr, pub.failure(), recErr)
}

// publishRefData produces reference-data records and waits for them to be
// acknowledged, so consumers can rely on them before the first market event.
func (r *Runner) publishRefData(ctx context.Context, recs ...*twpb.RefDataRecord) error {
	topic := r.cfg.Kafka.Topics().RefData
	out := make([]*kgo.Record, 0, len(recs))
	for _, rd := range recs {
		value, err := events.Marshal(rd)
		if err != nil {
			return err
		}
		out = append(out, &kgo.Record{
			Topic: topic, Key: []byte(RefDataKey(rd)), Value: value,
			Headers: kafka.EventMeta{Schema: events.SchemaRefData}.Headers(),
		})
	}
	return r.cl.ProduceSync(ctx, out...).FirstErr()
}

// RefDataKey is the compaction key of a reference-data record.
func RefDataKey(rd *twpb.RefDataRecord) string {
	switch v := rd.GetRecord().(type) {
	case *twpb.RefDataRecord_Account:
		return "account/" + v.Account.GetAccountId()
	case *twpb.RefDataRecord_Instrument:
		return "instrument/" + v.Instrument.GetSymbol()
	case *twpb.RefDataRecord_Session:
		return "session/" + v.Session.GetSessionId()
	default:
		return "unknown"
	}
}

// Status is the JSON body of GET /api/v1/sim/status.
type Status struct {
	State             string   `json:"state"`
	SessionID         string   `json:"session_id,omitempty"`
	SessionNumber     int      `json:"session_number"`
	SessionsCompleted int      `json:"sessions_completed"`
	SessionOpen       string   `json:"session_open,omitempty"`
	SessionClose      string   `json:"session_close,omitempty"`
	SimTime           string   `json:"sim_time,omitempty"`
	EventsPublished   uint64   `json:"events_published"`
	Pace              PaceMode `json:"pace"`
	Speed             float64  `json:"speed"`
	Rate              float64  `json:"rate"`
	Symbols           []string `json:"symbols"`
}

// Status returns the runner's current state.
func (r *Runner) Status() Status {
	r.mu.Lock()
	defer r.mu.Unlock()
	mode, speed, rate := r.pace.settings()
	st := Status{
		State: r.state, SessionNumber: r.sessionNo, SessionsCompleted: r.completed,
		EventsPublished: r.events.Load(), Pace: mode, Speed: speed, Rate: rate,
	}
	if r.session.ID != "" {
		st.SessionID = r.session.ID
		st.SessionOpen = time.Unix(0, r.session.Open).UTC().Format(time.RFC3339Nano)
		st.SessionClose = time.Unix(0, r.session.Close).UTC().Format(time.RFC3339Nano)
	}
	if t := r.simTimeNs.Load(); t > 0 {
		st.SimTime = time.Unix(0, t).UTC().Format(time.RFC3339Nano)
	}
	for _, in := range r.uni.Instruments {
		if len(r.cfg.Symbols) == 0 || contains(r.cfg.Symbols, in.Symbol) {
			st.Symbols = append(st.Symbols, in.Symbol)
		}
	}
	return st
}

func contains(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}

// PaceRequest is the JSON body of PUT /api/v1/sim/pace.
type PaceRequest struct {
	Mode  PaceMode `json:"mode"`
	Speed float64  `json:"speed"`
	Rate  float64  `json:"rate"`
}

// Register adds the control API to the admin server.
func (r *Runner) Register(srv *metrics.Server) {
	srv.HandleFunc("GET /api/v1/sim/status", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, r.Status())
	})
	srv.HandleFunc("POST /api/v1/sim/start", func(w http.ResponseWriter, _ *http.Request) {
		r.Start()
		writeJSON(w, http.StatusAccepted, r.Status())
	})
	srv.HandleFunc("POST /api/v1/sim/stop", func(w http.ResponseWriter, _ *http.Request) {
		r.Stop()
		writeJSON(w, http.StatusAccepted, r.Status())
	})
	srv.HandleFunc("PUT /api/v1/sim/pace", func(w http.ResponseWriter, req *http.Request) {
		var p PaceRequest
		if err := json.NewDecoder(http.MaxBytesReader(w, req.Body, 1<<16)).Decode(&p); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON body"})
			return
		}
		probe := r.cfg
		probe.Pace, probe.Speed, probe.Rate = p.Mode, p.Speed, p.Rate
		if err := probe.Validate(); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		r.pace.set(p.Mode, p.Speed, p.Rate)
		writeJSON(w, http.StatusOK, r.Status())
	})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
