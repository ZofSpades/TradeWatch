package replay

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"regexp"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/ZofSpades/tradewatch/internal/detect"
	"github.com/ZofSpades/tradewatch/internal/events"
	"github.com/ZofSpades/tradewatch/internal/events/twpb"
	"github.com/ZofSpades/tradewatch/internal/ingest"
	ob "github.com/ZofSpades/tradewatch/internal/orderbook"
	"github.com/ZofSpades/tradewatch/internal/recording"
	"github.com/ZofSpades/tradewatch/internal/rules"
	"github.com/ZofSpades/tradewatch/internal/sim"
)

const repoRules = "../../rules"

// recorder writes a session the way marketsim does, and also runs it
// through an engine directly, for comparison.
type recorder struct {
	w       *recording.Writer
	session string
	ref     *ingest.ReferenceData
	engine  *detect.Engine
	alerts  []*twpb.Alert
	labels  int
}

func (r *recorder) Emit(sym *sim.Symbol, ev *ob.Event) error {
	msg := sim.ToProto(r.session, sym, ev, 0)
	wrapped, err := events.Wrap(proto.Clone(msg))
	if err != nil {
		return err
	}
	if err := r.w.WriteEvent(wrapped); err != nil {
		return err
	}
	mev, rej := ingest.Normalize(msg, r.ref, ev.Time)
	if rej != nil {
		return rej
	}
	r.alerts = append(r.alerts, r.engine.Process(mev, -1)...)
	return nil
}

func (r *recorder) Label(l *twpb.GroundTruthLabel) error {
	r.labels++
	return r.w.WriteLabel(l)
}

func record(t *testing.T, set *rules.Set) (*bytes.Buffer, *recorder) {
	t.Helper()
	cfg := sim.DefaultConfig()
	cfg.Seed, cfg.SessionID, cfg.Duration, cfg.ClosingWindow = 5, "rec-5", 25*time.Minute, 5*time.Minute
	cfg.Injection = sim.Injection{WashTrading: 2, Spoofing: 2, MomentumIgnition: 3, QuoteStuffing: 2, MarkingTheClose: 1}
	var buf bytes.Buffer
	w, err := recording.NewWriter(&buf, &twpb.RecordingHeader{SessionId: cfg.SessionID})
	if err != nil {
		t.Fatal(err)
	}
	engine, err := detect.NewEngine(set, detect.Config{Instance: "replay"})
	if err != nil {
		t.Fatal(err)
	}
	rec := &recorder{w: w, session: cfg.SessionID, engine: engine}
	uni := sim.DefaultUniverse()
	s, err := sim.New(cfg, uni, rec)
	if err != nil {
		t.Fatal(err)
	}
	refData := append(uni.RefData(), &twpb.RefDataRecord{Record: &twpb.RefDataRecord_Session{Session: s.Session().Proto()}})
	for _, rd := range refData {
		if err := w.WriteRefData(rd); err != nil {
			t.Fatal(err)
		}
	}
	rec.ref = ingest.NewReferenceData(refData...)
	if err := s.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return &buf, rec
}

func TestReplayReproducesTheLiveAlerts(t *testing.T) {
	prod, err := rules.Load(repoRules)
	if err != nil {
		t.Fatal(err)
	}
	buf, rec := record(t, prod)
	if len(rec.alerts) == 0 || rec.labels == 0 {
		t.Fatalf("%d alerts, %d labels; the test does not exercise detection", len(rec.alerts), rec.labels)
	}
	path := filepath.Join(t.TempDir(), "rec-5.twrec.gz")
	if err := os.WriteFile(path, buf.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}

	// A copy of the rules with a far higher momentum threshold, as a candidate
	// rule change to backtest.
	changed := t.TempDir()
	for _, name := range []string{rules.TaxonomyFile, "momentum_ignition.yaml"} {
		src, err := os.ReadFile(filepath.Join(repoRules, name))
		if err != nil {
			t.Fatal(err)
		}
		s := string(src)
		if name != rules.TaxonomyFile {
			s = regexp.MustCompile(`min_move_bps: \d+`).ReplaceAllString(s, "min_move_bps: 200")
		}
		if err := os.WriteFile(filepath.Join(changed, name), []byte(s), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	candidate, err := rules.Load(changed)
	if err != nil {
		t.Fatal(err)
	}
	onlyMomentum := []*twpb.Alert{}
	for _, a := range rec.alerts {
		if a.GetRuleId() == "TW-MOMO-001" {
			onlyMomentum = append(onlyMomentum, a)
		}
	}

	out, err := Run(context.Background(), path, []*rules.Set{prod, candidate})
	if err != nil {
		t.Fatal(err)
	}
	if out.Header.GetSessionId() != "rec-5" || len(out.Labels) != rec.labels || out.Symbols == 0 || out.Events == 0 {
		t.Fatalf("outcome: header %v, %d labels, %d symbols, %d events", out.Header, len(out.Labels), out.Symbols, out.Events)
	}
	if len(out.Alerts[0]) != len(rec.alerts) {
		t.Fatalf("replay raised %d alerts, the live run %d", len(out.Alerts[0]), len(rec.alerts))
	}
	for i, a := range out.Alerts[0] {
		if !proto.Equal(a, rec.alerts[i]) {
			t.Fatalf("alert %d differs between replay and the live run", i)
		}
	}
	if len(onlyMomentum) == 0 {
		t.Fatal("no momentum alerts in the recording; the backtest is not exercised")
	}
	// The candidate holds only the momentum rule: every alert it keeps has
	// the production alert's key, and the higher threshold can only drop.
	diff := Compare(onlyMomentum, out.Alerts[1])
	if len(diff.OnlyA) == 0 || len(diff.OnlyB) != 0 || diff.Both+len(diff.OnlyA) != len(onlyMomentum) {
		t.Errorf("backtest: %d kept, %d dropped, %d new, of %d momentum alerts", diff.Both, len(diff.OnlyA), len(diff.OnlyB), len(onlyMomentum))
	}
	t.Logf("replayed %d events: %d alerts; min_move_bps 200 keeps %d of %d momentum alerts", out.Events, len(out.Alerts[0]), diff.Both, len(onlyMomentum))
}
