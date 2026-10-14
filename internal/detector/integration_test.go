//go:build integration

package detector

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"

	"github.com/ZofSpades/tradewatch/internal/detect"
	"github.com/ZofSpades/tradewatch/internal/events"
	"github.com/ZofSpades/tradewatch/internal/events/twpb"
	"github.com/ZofSpades/tradewatch/internal/ingest"
	"github.com/ZofSpades/tradewatch/internal/kafka"
	"github.com/ZofSpades/tradewatch/internal/marketsim"
	ob "github.com/ZofSpades/tradewatch/internal/orderbook"
	"github.com/ZofSpades/tradewatch/internal/rules"
	"github.com/ZofSpades/tradewatch/internal/sim"
	"github.com/ZofSpades/tradewatch/internal/testutil"
)

var quiet = slog.New(slog.DiscardHandler)

const rulesDir = "../../rules"

func itKafka(t *testing.T) kafka.Config {
	return kafka.Config{
		Brokers: testutil.KafkaBrokers(t), ClientID: "it-detector", Prefix: testutil.UniquePrefix(t),
		Partitions: 6, Replication: 1, LogLevel: kgo.LogLevelWarn,
	}
}

func cleanup(t *testing.T, cfg kafka.Config) {
	t.Cleanup(func() {
		cl, err := kafka.NewClient(cfg, quiet)
		if err != nil {
			return
		}
		defer cl.Close()
		var names []string
		for _, s := range cfg.Topics().Specs(cfg.Partitions) {
			names = append(names, s.Name)
		}
		_, _ = kadm.NewClient(cl).DeleteTopics(context.Background(), names...)
	})
}

func sessionConfig(kcfg kafka.Config, pace marketsim.PaceMode, rate float64) marketsim.Config {
	return marketsim.Config{
		Kafka: kcfg, Seed: 77, Sessions: 1, Start: time.Date(2026, 3, 9, 14, 30, 0, 0, time.UTC),
		Duration: 40 * time.Minute, ClosingWindow: 4 * time.Minute, SnapshotInterval: time.Second, Activity: 1,
		Pace: pace, Rate: rate, AutoStart: true,
		Injection: sim.Injection{WashTrading: 4, Spoofing: 4, MomentumIgnition: 4, QuoteStuffing: 4, MarkingTheClose: 1},
	}
}

// inProcess computes the alerts the pipeline must produce: the same session
// run through ingest.Normalize and the detection engine without Kafka.
type inProcess struct {
	session string
	ref     *ingest.ReferenceData
	eng     *detect.Engine
	alerts  map[string]*twpb.Alert
}

func (p *inProcess) Emit(sym *sim.Symbol, ev *ob.Event) error {
	mev, rej := ingest.Normalize(sim.ToProto(p.session, sym, ev, 0), p.ref, 1)
	if rej != nil {
		return rej
	}
	for _, a := range p.eng.Process(mev, -1) {
		p.alerts[a.GetAlertKey()] = normalized(a)
	}
	return nil
}

// normalized clears the fields that legitimately differ between a live and
// an in-process run: wall-clock times, and the trace.
func normalized(a *twpb.Alert) *twpb.Alert {
	c := proto.Clone(a).(*twpb.Alert)
	c.EmittedAtNs, c.TriggerPublishedAtNs, c.TraceId = 0, 0, ""
	return c
}

func expectedAlerts(t *testing.T, mcfg marketsim.Config) map[string]*twpb.Alert {
	t.Helper()
	runner := marketsim.NewRunner(mcfg, nil, prometheus.NewRegistry(), quiet)
	scfg := runner.SessionConfig(0, time.Now())
	set, err := rules.Load(rulesDir)
	if err != nil {
		t.Fatal(err)
	}
	eng, err := detect.NewEngine(set, detect.Config{Instance: "it"})
	if err != nil {
		t.Fatal(err)
	}
	uni := sim.DefaultUniverse()
	p := &inProcess{session: scfg.SessionID, eng: eng, alerts: map[string]*twpb.Alert{}}
	s, err := sim.New(scfg, uni, p)
	if err != nil {
		t.Fatal(err)
	}
	p.ref = ingest.NewReferenceData(append(uni.RefData(), &twpb.RefDataRecord{Record: &twpb.RefDataRecord_Session{Session: s.Session().Proto()}})...)
	if err := s.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(p.alerts) < 10 {
		t.Fatalf("only %d expected alerts; the test does not exercise detection", len(p.alerts))
	}
	return p.alerts
}

// background runs a service until stopped.
type background struct {
	cancel context.CancelFunc
	done   chan error
}

func (b *background) stop(t *testing.T) {
	t.Helper()
	b.cancel()
	select {
	case err := <-b.done:
		if err != nil {
			t.Fatalf("service stopped with error: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("service did not stop")
	}
}

func goRun(run func(context.Context) error, closers ...func()) *background {
	ctx, cancel := context.WithCancel(context.Background())
	b := &background{cancel: cancel, done: make(chan error, 1)}
	go func() {
		err := run(ctx)
		for _, c := range closers {
			c()
		}
		b.done <- err
	}()
	return b
}

func startMarketsim(t *testing.T, mcfg marketsim.Config) *background {
	cl, err := kafka.NewClient(mcfg.Kafka, quiet)
	if err != nil {
		t.Fatal(err)
	}
	r := marketsim.NewRunner(mcfg, cl, prometheus.NewRegistry(), quiet)
	return goRun(r.Run, cl.Close)
}

func startIngest(t *testing.T, kcfg kafka.Config) *background {
	svc := ingest.NewService(ingest.Config{
		Kafka: kcfg, Group: kcfg.Prefix + "ingest", CommitInterval: 200 * time.Millisecond,
		GapTimeout: 5 * time.Second, MaxHeld: 20_000, IdleAfter: 10 * time.Minute, RefDataTimeout: 2 * time.Second,
	}, prometheus.NewRegistry(), quiet)
	cl, err := kafka.NewClient(kcfg, quiet, svc.ClientOptions()...)
	if err != nil {
		t.Fatal(err)
	}
	refCl, err := kafka.NewClient(kcfg, quiet, kgo.ConsumeTopics(kcfg.Topics().RefData), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	svc.Attach(cl, ingest.NewRefDataTail(refCl, kcfg.Topics().RefData, quiet))
	return goRun(svc.Run, cl.Close, refCl.Close)
}

func startDetector(t *testing.T, kcfg kafka.Config) *background {
	b, _ := startDetectorService(t, kcfg)
	return b
}

func startDetectorService(t *testing.T, kcfg kafka.Config) (*background, *Service) {
	svc, err := NewService(Config{
		Kafka: kcfg, Group: kcfg.Prefix + "detector", RulesDir: rulesDir, ReloadInterval: time.Hour,
		CommitInterval: 200 * time.Millisecond, Instance: "it", Engine: detect.Config{Instance: "it"},
	}, prometheus.NewRegistry(), quiet)
	if err != nil {
		t.Fatal(err)
	}
	cl, err := kafka.NewClient(kcfg, quiet, svc.ClientOptions()...)
	if err != nil {
		t.Fatal(err)
	}
	svc.Attach(cl)
	return goRun(svc.Run, cl.Close), svc
}

// committedOffsets returns the detector group's committed market_events
// offsets by partition.
func committedOffsets(t *testing.T, kcfg kafka.Config) map[int32]int64 {
	t.Helper()
	cl, err := kafka.NewClient(kcfg, quiet)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	resp, err := kadm.NewClient(cl).FetchOffsets(context.Background(), kcfg.Prefix+"detector")
	if err != nil {
		t.Fatal(err)
	}
	out := map[int32]int64{}
	resp.Each(func(o kadm.OffsetResponse) {
		if o.Err == nil && o.Topic == kcfg.Topics().MarketEvents {
			out[o.Partition] = o.At
		}
	})
	return out
}

// waitCaughtUp waits until svc owns every partition and has processed it to
// its current end.
func waitCaughtUp(t *testing.T, kcfg kafka.Config, svc *Service, timeout time.Duration) {
	t.Helper()
	cl, err := kafka.NewClient(kcfg, quiet)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ends, err := kadm.NewClient(cl).ListEndOffsets(context.Background(), kcfg.Topics().MarketEvents)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(timeout)
	for {
		svc.mu.Lock()
		done := len(svc.parts) == int(kcfg.Partitions)
		for p, part := range svc.parts {
			if end, ok := ends.Lookup(kcfg.Topics().MarketEvents, p); ok && part.next < end.Offset {
				done = false
			}
		}
		svc.mu.Unlock()
		if done {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("detector did not catch up")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// alertTail collects the alerts topic, with each record's traceparent
// header.
type alertTail struct {
	mu      sync.Mutex
	alerts  []*twpb.Alert
	parents []string
	stop    context.CancelFunc
}

func tailAlerts(t *testing.T, kcfg kafka.Config) *alertTail {
	cl, err := kafka.NewClient(kcfg, quiet, kgo.ConsumeTopics(kcfg.Topics().Alerts), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	at := &alertTail{stop: cancel}
	go func() {
		defer cl.Close()
		for ctx.Err() == nil {
			cl.PollFetches(ctx).EachRecord(func(r *kgo.Record) {
				var a twpb.Alert
				if err := events.Unmarshal(r.Value, &a); err != nil {
					t.Errorf("undecodable alert: %v", err)
					return
				}
				tp, _ := kafka.HeaderValue(r.Headers, "traceparent")
				at.mu.Lock()
				at.alerts = append(at.alerts, &a)
				at.parents = append(at.parents, tp)
				at.mu.Unlock()
			})
		}
	}()
	t.Cleanup(cancel)
	return at
}

func (at *alertTail) unique() map[string][]*twpb.Alert {
	at.mu.Lock()
	defer at.mu.Unlock()
	out := map[string][]*twpb.Alert{}
	for _, a := range at.alerts {
		out[a.GetAlertKey()] = append(out[a.GetAlertKey()], a)
	}
	return out
}

func (at *alertTail) waitFor(t *testing.T, n int, timeout time.Duration) map[string][]*twpb.Alert {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		got := at.unique()
		if len(got) >= n {
			return got
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out with %d of %d alerts", len(got), n)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// compare requires the live alerts to be exactly the expected ones: no key
// missing, no key extra, identical content apart from wall-clock fields,
// and every copy of a duplicated alert identical too.
func compare(t *testing.T, got map[string][]*twpb.Alert, want map[string]*twpb.Alert) (duplicates int) {
	t.Helper()
	for key, w := range want {
		copies, ok := got[key]
		if !ok {
			t.Errorf("alert %s (%s %s) missing", key, w.GetRuleId(), w.GetSymbol())
			continue
		}
		for _, c := range copies {
			if c.GetEmittedAtNs() == 0 {
				t.Errorf("alert %s has no emitted_at", key)
			}
			if !proto.Equal(normalized(c), w) {
				t.Errorf("alert %s differs from the in-process alert:\n got %v\nwant %v", key, normalized(c), w)
			}
		}
		duplicates += len(copies) - 1
	}
	for key, copies := range got {
		if _, ok := want[key]; !ok {
			t.Errorf("unexpected alert %s (%s %s)", key, copies[0].GetRuleId(), copies[0].GetSymbol())
		}
	}
	return duplicates
}

func TestLivePipelineProducesTheInProcessAlerts(t *testing.T) {
	kcfg := itKafka(t)
	cleanup(t, kcfg)
	mcfg := sessionConfig(kcfg, marketsim.PaceFast, 0)
	want := expectedAlerts(t, mcfg)

	ms := startMarketsim(t, mcfg)
	in := startIngest(t, kcfg)
	det := startDetector(t, kcfg)
	alerts := tailAlerts(t, kcfg)
	got := alerts.waitFor(t, len(want), 3*time.Minute)
	time.Sleep(2 * time.Second) // let any extra alert arrive
	det.stop(t)
	in.stop(t)
	ms.stop(t)

	if dups := compare(t, alerts.unique(), want); dups != 0 {
		t.Errorf("%d duplicate alerts without any handover", dups)
	}
	t.Logf("live pipeline produced the %d in-process alerts exactly (%d keys seen)", len(want), len(got))
}

func TestDetectorHandoversLoseAndDuplicateNothing(t *testing.T) {
	kcfg := itKafka(t)
	cleanup(t, kcfg)
	// marketsim publishes at a fixed rate so detectors are stopped, started
	// and rebalanced while events are still flowing.
	mcfg := sessionConfig(kcfg, marketsim.PaceRate, 4_000)
	want := expectedAlerts(t, mcfg)

	ms := startMarketsim(t, mcfg)
	in := startIngest(t, kcfg)
	alerts := tailAlerts(t, kcfg)

	first := startDetector(t, kcfg)
	alerts.waitFor(t, len(want)/4, 2*time.Minute)
	first.stop(t)
	second := startDetector(t, kcfg)
	alerts.waitFor(t, len(want)/2, 2*time.Minute)
	third := startDetector(t, kcfg) // forces a rebalance mid-stream
	alerts.waitFor(t, len(want), 3*time.Minute)
	time.Sleep(2 * time.Second)
	second.stop(t)
	third.stop(t)
	in.stop(t)
	ms.stop(t)

	dups := compare(t, alerts.unique(), want)
	if dups != 0 {
		t.Errorf("%d duplicate alerts after graceful handovers; replayed alerts must not be produced again", dups)
	}
	t.Logf("%d alerts across a stop/start handover and a rebalance, %d duplicates", len(want), dups)
}

// A detector stopped while it is still rebuilding state must leave the
// previous owner's checkpoint alone. Committing its replay position would
// move the frontier back, and the next owner would produce the alerts in
// between a second time.
func TestStopDuringReplayKeepsThePreviousCheckpoint(t *testing.T) {
	kcfg := itKafka(t)
	cleanup(t, kcfg)
	// Rate pacing keeps the session open when the first detector stops, so
	// its successor has state to rebuild.
	mcfg := sessionConfig(kcfg, marketsim.PaceRate, 4_000)
	want := expectedAlerts(t, mcfg)

	ms := startMarketsim(t, mcfg)
	in := startIngest(t, kcfg)
	alerts := tailAlerts(t, kcfg)
	first := startDetector(t, kcfg)
	alerts.waitFor(t, len(want)/2, 2*time.Minute)
	first.stop(t)
	before := committedOffsets(t, kcfg)

	second, svc := startDetectorService(t, kcfg)
	deadline := time.Now().Add(time.Minute)
	for stopped := false; !stopped; {
		svc.mu.Lock()
		for _, part := range svc.parts {
			if part.next < part.replayUntil {
				// The service loop is blocked on mu; it finishes at most
				// one more batch before it sees the cancellation.
				second.cancel()
				stopped = true
				break
			}
		}
		svc.mu.Unlock()
		if !stopped && time.Now().After(deadline) {
			t.Fatal("the second detector never replayed")
		}
		time.Sleep(time.Millisecond)
	}
	second.stop(t)
	for p, off := range before {
		if after := committedOffsets(t, kcfg)[p]; after < off {
			t.Errorf("partition %d: the stopped replay moved the committed offset back from %d to %d", p, off, after)
		}
	}

	third, thirdSvc := startDetectorService(t, kcfg)
	alerts.waitFor(t, len(want), 3*time.Minute)
	waitCaughtUp(t, kcfg, thirdSvc, time.Minute)
	time.Sleep(time.Second)
	third.stop(t)
	in.stop(t)
	ms.stop(t)
	if dups := compare(t, alerts.unique(), want); dups != 0 {
		t.Errorf("%d duplicate alerts after a replay was interrupted", dups)
	}
}

// With tracing on, a sampled event is followed from marketsim through ingest
// to a detector, every alert record carries the trace of the span that
// raised it, and an alert raised by a sampled event continues the event's
// trace. Tracing changes no alert.
func TestTracesFollowSampledEventsAndEveryAlert(t *testing.T) {
	rec := testutil.RecordSpans(t)
	kcfg := itKafka(t)
	cleanup(t, kcfg)
	mcfg := sessionConfig(kcfg, marketsim.PaceFast, 0)
	mcfg.TraceSampleRatio = 0.02
	want := expectedAlerts(t, mcfg)

	ms := startMarketsim(t, mcfg)
	in := startIngest(t, kcfg)
	det, svc := startDetectorService(t, kcfg)
	alerts := tailAlerts(t, kcfg)
	alerts.waitFor(t, len(want), 3*time.Minute)
	waitCaughtUp(t, kcfg, svc, time.Minute)
	time.Sleep(time.Second)
	det.stop(t)
	in.stop(t)
	ms.stop(t)
	if dups := compare(t, alerts.unique(), want); dups != 0 {
		t.Errorf("%d duplicate alerts", dups)
	}

	const (
		marketsimScope = "github.com/ZofSpades/tradewatch/internal/marketsim"
		ingestScope    = "github.com/ZofSpades/tradewatch/internal/ingest"
		detectorScope  = "github.com/ZofSpades/tradewatch/internal/detector"
	)
	spans := rec.Ended()
	byID := map[trace.SpanID]sdktrace.ReadOnlySpan{}
	children := map[trace.SpanID][]sdktrace.ReadOnlySpan{}
	raised := map[string]sdktrace.ReadOnlySpan{} // by trace ID
	for _, sp := range spans {
		byID[sp.SpanContext().SpanID()] = sp
		children[sp.Parent().SpanID()] = append(children[sp.Parent().SpanID()], sp)
		if strings.HasPrefix(sp.Name(), "raise ") {
			raised[sp.SpanContext().TraceID().String()] = sp
		}
	}
	child := func(parent sdktrace.ReadOnlySpan, scope, name string) sdktrace.ReadOnlySpan {
		if parent == nil {
			return nil
		}
		for _, c := range children[parent.SpanContext().SpanID()] {
			if c.InstrumentationScope().Name == scope && c.Name() == name {
				return c
			}
		}
		return nil
	}

	// Every alert record names the trace of the span that raised it.
	alerts.mu.Lock()
	for i, a := range alerts.alerts {
		parts := strings.Split(alerts.parents[i], "-")
		sp := raised[a.GetTraceId()]
		switch {
		case len(a.GetTraceId()) != 32:
			t.Errorf("alert %s has trace ID %q", a.GetAlertKey(), a.GetTraceId())
		case len(parts) != 4 || parts[1] != a.GetTraceId():
			t.Errorf("alert %s: header traceparent %q does not carry trace %s", a.GetAlertKey(), alerts.parents[i], a.GetTraceId())
		case sp == nil || sp.Name() != "raise "+a.GetRuleId() || testutil.SpanAttrs(sp)["tradewatch.alert.key"] != a.GetAlertKey():
			t.Errorf("alert %s: no matching raise span", a.GetAlertKey())
		}
	}
	alerts.mu.Unlock()

	// Every sampled event reached a detector through ingest, hop by hop.
	sampled, complete := 0, 0
	for _, sp := range spans {
		if sp.InstrumentationScope().Name != marketsimScope {
			continue
		}
		sampled++
		topic := strings.TrimPrefix(sp.Name(), "send ")
		processed := child(sp, ingestScope, "process "+topic)
		released := child(processed, ingestScope, "send "+kcfg.Topics().MarketEvents)
		if child(released, detectorScope, "process "+kcfg.Topics().MarketEvents) != nil {
			complete++
		}
	}
	if sampled < 100 {
		t.Fatalf("only %d events were sampled; the test needs more", sampled)
	}
	if complete != sampled {
		t.Errorf("%d of %d sampled events were traced through to a detector", complete, sampled)
	}

	// An alert raised by a sampled event continues that event's trace.
	continued := 0
	for _, sp := range raised {
		if parent := byID[sp.Parent().SpanID()]; parent != nil {
			if parent.InstrumentationScope().Name != detectorScope || !strings.HasPrefix(parent.Name(), "process ") {
				t.Errorf("%s's parent is %s", sp.Name(), parent.Name())
			}
			continued++
		}
	}
	t.Logf("%d alerts traced; %d sampled events followed end to end; %d alerts continued a sampled event's trace",
		len(raised), complete, continued)
}
