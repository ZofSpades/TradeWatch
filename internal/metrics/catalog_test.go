package metrics_test

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"go.yaml.in/yaml/v3"

	"github.com/ZofSpades/tradewatch/internal/alertsink"
	"github.com/ZofSpades/tradewatch/internal/api"
	"github.com/ZofSpades/tradewatch/internal/detector"
	"github.com/ZofSpades/tradewatch/internal/ingest"
	"github.com/ZofSpades/tradewatch/internal/marketsim"
	"github.com/ZofSpades/tradewatch/internal/metrics"
)

// catalog records the descriptors of every collector registered with it,
// whether or not the collector has exported a sample yet: a vector with no
// label values observed is absent from a scrape but still described.
type catalog struct {
	t     *testing.T
	descs map[string]string // metric name -> descriptor
}

var fqName = regexp.MustCompile(`fqName: "([^"]+)"`)

func (c *catalog) Register(col prometheus.Collector) error {
	ch := make(chan *prometheus.Desc)
	go func() {
		col.Describe(ch)
		close(ch)
	}()
	for d := range ch {
		m := fqName.FindStringSubmatch(d.String())
		if m == nil {
			c.t.Fatalf("cannot read the name of %s", d)
		}
		c.descs[m[1]] = d.String()
	}
	return nil
}

func (c *catalog) MustRegister(cols ...prometheus.Collector) {
	for _, col := range cols {
		_ = c.Register(col)
	}
}

func (c *catalog) Unregister(prometheus.Collector) bool { return false }

// serviceMetrics returns every metric the TradeWatch services export, by
// name: the services' own series, plus the Go runtime and process series
// of the shared registry.
func serviceMetrics(t *testing.T) map[string]string {
	t.Helper()
	c := &catalog{t: t, descs: map[string]string{}}
	quiet := slog.New(slog.DiscardHandler)
	marketsim.NewRunner(marketsim.Config{Pace: marketsim.PaceFast}, nil, c, quiet)
	ingest.NewService(ingest.Config{}, c, quiet)
	if _, err := detector.NewService(detector.Config{RulesDir: "../../rules"}, c, quiet); err != nil {
		t.Fatal(err)
	}
	alertsink.NewService(alertsink.Config{}, nil, nil, c, quiet)
	if _, err := api.NewServer(nil, nil, nil, "http://marketsim:8090", c, quiet); err != nil {
		t.Fatal(err)
	}
	reg := metrics.NewRegistry()
	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range families {
		c.descs[f.GetName()] = f.GetHelp()
	}
	return c.descs
}

// metricName matches the metric names a PromQL expression refers to. Label
// values and durations never match: they do not start with these prefixes.
var metricName = regexp.MustCompile(`\b(?:tradewatch|go|process)_[a-zA-Z0-9_]+\b`)

// unknownMetrics returns the names expr refers to that no service exports.
// A histogram or summary is also queried through its _bucket, _sum and
// _count series. Prometheus adds `up` to every target.
func unknownMetrics(expr string, exported map[string]string) []string {
	var unknown []string
	for _, name := range metricName.FindAllString(expr, -1) {
		base := name
		for _, suffix := range []string{"_bucket", "_sum", "_count"} {
			if b, ok := strings.CutSuffix(name, suffix); ok && exported[b] != "" {
				base = b
			}
		}
		if exported[base] == "" {
			unknown = append(unknown, name)
		}
	}
	return unknown
}

const monitoringDir = "../../deploy/compose"

type dashboard struct {
	UID        string  `json:"uid"`
	Title      string  `json:"title"`
	Panels     []panel `json:"panels"`
	Templating struct {
		List []struct {
			Name  string `json:"name"`
			Query any    `json:"query"`
		} `json:"list"`
	} `json:"templating"`
}

type panel struct {
	ID         int    `json:"id"`
	Type       string `json:"type"`
	Title      string `json:"title"`
	Datasource *struct {
		UID string `json:"uid"`
	} `json:"datasource"`
	GridPos struct{ X, Y, W, H int } `json:"gridPos"`
	Targets []struct {
		RefID string `json:"refId"`
		Expr  string `json:"expr"`
	} `json:"targets"`
	Panels []panel `json:"panels"`
}

func loadDashboards(t *testing.T) map[string]dashboard {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(monitoringDir, "grafana", "dashboards", "*.json"))
	if err != nil || len(files) == 0 {
		t.Fatalf("no dashboards found: %v", err)
	}
	out := map[string]dashboard{}
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		var d dashboard
		dec := json.NewDecoder(bytes.NewReader(b))
		if err := dec.Decode(&d); err != nil {
			t.Fatalf("%s: %v", f, err)
		}
		out[filepath.Base(f)] = d
	}
	return out
}

// Every query on every provisioned dashboard must refer to metrics the
// services export, so renaming a metric cannot silently empty a panel.
func TestDashboardsQueryExportedMetrics(t *testing.T) {
	exported := serviceMetrics(t)
	exported["up"] = "synthetic"
	if runtime.GOOS != "linux" {
		// The process collector exports fewer series outside Linux; the
		// services only ever run on Linux.
		exported["process_open_fds"] = "linux only"
	}
	uids := map[string]string{}
	for file, d := range loadDashboards(t) {
		if d.UID == "" || d.Title == "" {
			t.Errorf("%s: missing uid or title", file)
		}
		if other, dup := uids[d.UID]; dup {
			t.Errorf("%s and %s share the uid %q", file, other, d.UID)
		}
		uids[d.UID] = file
		for _, v := range d.Templating.List {
			if q, ok := v.Query.(string); ok {
				if unknown := unknownMetrics(q, exported); len(unknown) > 0 {
					t.Errorf("%s: variable %s queries unknown metrics %v", file, v.Name, unknown)
				}
			}
		}
		ids := map[int]bool{}
		var check func(panels []panel)
		check = func(panels []panel) {
			for _, p := range panels {
				if ids[p.ID] {
					t.Errorf("%s: panel id %d is used twice", file, p.ID)
				}
				ids[p.ID] = true
				if p.GridPos.W <= 0 || p.GridPos.X < 0 || p.GridPos.X+p.GridPos.W > 24 {
					t.Errorf("%s: panel %q does not fit the 24-column grid: %+v", file, p.Title, p.GridPos)
				}
				check(p.Panels)
				if p.Type == "row" {
					continue
				}
				if p.Datasource == nil || p.Datasource.UID != "prometheus" {
					t.Errorf("%s: panel %q does not use the provisioned Prometheus data source", file, p.Title)
				}
				if len(p.Targets) == 0 {
					t.Errorf("%s: panel %q has no queries", file, p.Title)
				}
				for _, tg := range p.Targets {
					if strings.TrimSpace(tg.Expr) == "" {
						t.Errorf("%s: panel %q query %s is empty", file, p.Title, tg.RefID)
					}
					if unknown := unknownMetrics(tg.Expr, exported); len(unknown) > 0 {
						t.Errorf("%s: panel %q queries unknown metrics %v", file, p.Title, unknown)
					}
				}
			}
		}
		check(d.Panels)
	}
}

// The alerting rules must refer to exported metrics too.
func TestAlertRulesQueryExportedMetrics(t *testing.T) {
	exported := serviceMetrics(t)
	exported["up"] = "synthetic"
	files, err := filepath.Glob(filepath.Join(monitoringDir, "prometheus", "rules", "*.yml"))
	if err != nil || len(files) == 0 {
		t.Fatalf("no rule files found: %v", err)
	}
	rules := 0
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		var doc struct {
			Groups []struct {
				Rules []struct {
					Alert string `yaml:"alert"`
					Expr  string `yaml:"expr"`
				} `yaml:"rules"`
			} `yaml:"groups"`
		}
		if err := yaml.Unmarshal(b, &doc); err != nil {
			t.Fatalf("%s: %v", f, err)
		}
		for _, g := range doc.Groups {
			for _, r := range g.Rules {
				rules++
				if unknown := unknownMetrics(r.Expr, exported); len(unknown) > 0 {
					t.Errorf("%s: rule %s queries unknown metrics %v", filepath.Base(f), r.Alert, unknown)
				}
			}
		}
	}
	if rules == 0 {
		t.Fatal("no rules parsed")
	}
}

func TestUnknownMetrics(t *testing.T) {
	exported := map[string]string{"tradewatch_x_seconds": "h", "tradewatch_y_total": "c"}
	expr := `histogram_quantile(0.9, sum by (le) (rate(tradewatch_x_seconds_bucket{job=~"a|b"}[5m]))) / tradewatch_y_total + tradewatch_z_total`
	got := unknownMetrics(expr, exported)
	if len(got) != 1 || got[0] != "tradewatch_z_total" {
		t.Errorf("unknownMetrics = %v, want [tradewatch_z_total]", got)
	}
}
