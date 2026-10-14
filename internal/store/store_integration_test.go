//go:build integration

package store

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/ZofSpades/tradewatch/internal/events/twpb"
	"github.com/ZofSpades/tradewatch/internal/testutil"
)

var quiet = slog.New(slog.DiscardHandler)

func open(t *testing.T) *Store {
	t.Helper()
	url := testutil.PostgresSchema(t)
	ctx := context.Background()
	if _, err := Migrate(ctx, url, quiet); err != nil {
		t.Fatal(err)
	}
	s, err := Open(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	return s
}

func testAlert(key string, mods ...func(*twpb.Alert)) *twpb.Alert {
	a := &twpb.Alert{
		AlertKey: key, SessionId: "S1", AbuseType: twpb.AbuseType_ABUSE_TYPE_SPOOFING, Detector: "spoofing",
		RuleId: "TW-SPOOF-001", RuleVersion: 1, RuleDigest: "d1", TaxonomyCode: "MAR-SPOOF",
		Severity: twpb.Severity_SEVERITY_HIGH, Score: 70, Symbol: "QZAP", AccountIds: []string{"ACC-1"},
		BeneficialOwnerIds: []string{"BO-1"}, WindowStartNs: 1_000, WindowEndNs: 2_000, DetectedAtNs: 3_000,
		Explanation: "explained", EmittedAtNs: 4_000, DetectorInstance: "det-a",
		Evidence: &twpb.Evidence{OrderIds: []uint64{11, 12}, Features: []*twpb.Feature{{Name: "layers", Value: 3, Threshold: 2, Op: ">=", Breached: true}}},
	}
	for _, m := range mods {
		m(a)
	}
	return a
}

func TestMigrateAppliesOnceUnderConcurrency(t *testing.T) {
	url := testutil.PostgresSchema(t)
	ctx := context.Background()
	var mu sync.Mutex
	var applied []int64
	var wg sync.WaitGroup
	for range 3 {
		wg.Go(func() {
			v, err := Migrate(ctx, url, quiet)
			if err != nil {
				t.Error(err)
			}
			mu.Lock()
			applied = append(applied, v...)
			mu.Unlock()
		})
	}
	wg.Wait()
	if !slices.Equal(applied, []int64{1}) {
		t.Fatalf("applied %v across three concurrent runs, want [1] once", applied)
	}
	if v, err := Migrate(ctx, url, quiet); err != nil || len(v) != 0 {
		t.Fatalf("second migration applied %v, err %v", v, err)
	}
}

func TestUpsertAlertsIsIdempotent(t *testing.T) {
	s := open(t)
	ctx := context.Background()
	a, b := testAlert("k-a"), testAlert("k-b", func(x *twpb.Alert) { x.Symbol = "MOSS" })
	raised, dups, err := s.UpsertAlerts(ctx, []*twpb.Alert{a, b, a}) // a duplicate within the batch
	if err != nil {
		t.Fatal(err)
	}
	if len(raised) != 2 || raised[0].Key != "k-a" || raised[1].Key != "k-b" || dups != 1 {
		t.Fatalf("raised %d (%v), duplicates %d", len(raised), raised, dups)
	}
	// A redelivery with different content changes only bookkeeping.
	changed := testAlert("k-a", func(x *twpb.Alert) { x.Score = 99 })
	raised, dups, err = s.UpsertAlerts(ctx, []*twpb.Alert{changed})
	if err != nil || len(raised) != 0 || dups != 1 {
		t.Fatalf("redelivery: raised %v, duplicates %d, err %v", raised, dups, err)
	}
	got, full, err := s.Get(ctx, "k-a")
	if err != nil {
		t.Fatal(err)
	}
	if got.Deliveries != 3 || got.Score != 70 || got.Status != StatusOpen || !proto.Equal(full, a) {
		t.Errorf("stored %+v, payload equal to first delivery: %v", got, proto.Equal(full, a))
	}
	if got.AbuseType != "spoofing" || got.Severity != "high" || got.DetectedAtNs != 3_000 {
		t.Errorf("names and times: %+v", got)
	}
	audit, err := s.Audit(ctx, "k-a")
	if err != nil || len(audit) != 1 || audit[0].Action != "raised" || audit[0].Actor != "detector/det-a" {
		t.Fatalf("audit %+v, err %v", audit, err)
	}
}

func TestConcurrentDeliveriesOfOneAlertStoreOneRow(t *testing.T) {
	s := open(t)
	ctx := context.Background()
	const n = 8
	var mu sync.Mutex
	raisedTotal := 0
	var wg sync.WaitGroup
	for range n {
		wg.Go(func() {
			raised, _, err := s.UpsertAlerts(ctx, []*twpb.Alert{testAlert("k-same")})
			if err != nil {
				t.Error(err)
			}
			mu.Lock()
			raisedTotal += len(raised)
			mu.Unlock()
		})
	}
	wg.Wait()
	got, _, err := s.Get(ctx, "k-same")
	if err != nil {
		t.Fatal(err)
	}
	if raisedTotal != 1 || got.Deliveries != n {
		t.Fatalf("raised %d times, %d deliveries recorded; want 1 and %d", raisedTotal, got.Deliveries, n)
	}
}

func TestLifecycleAndAuditTrail(t *testing.T) {
	s := open(t)
	ctx := context.Background()
	if _, _, err := s.UpsertAlerts(ctx, []*twpb.Alert{testAlert("k1")}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Transition(ctx, "k1", StatusEscalated, "ana", "skip triage"); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("open -> escalated: %v", err)
	}
	a, err := s.Transition(ctx, "k1", StatusTriaged, "ana", "")
	if err != nil || a.Status != StatusTriaged {
		t.Fatalf("open -> triaged: %+v %v", a, err)
	}
	if _, err := s.Transition(ctx, "k1", StatusDismissed, "ana", "  "); !errors.Is(err, ErrNoteRequired) {
		t.Fatalf("dismissal without a reason: %v", err)
	}
	if _, err := s.Comment(ctx, "k1", "bob", "counterparty is a known market maker"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Transition(ctx, "k1", StatusDismissed, "bob", "legitimate hedging"); err != nil {
		t.Fatal(err)
	}
	var te *TransitionError
	if _, err := s.Transition(ctx, "k1", StatusTriaged, "ana", ""); !errors.As(err, &te) || te.From != StatusDismissed {
		t.Fatalf("leaving a final status: %v", err)
	}
	audit, err := s.Audit(ctx, "k1")
	if err != nil {
		t.Fatal(err)
	}
	var lines []string
	for _, e := range audit {
		lines = append(lines, fmt.Sprintf("%s %s %s>%s %q", e.Actor, e.Action, e.From, e.To, e.Note))
	}
	want := []string{
		`detector/det-a raised >open ""`,
		`ana transition open>triaged ""`,
		`bob comment > "counterparty is a known market maker"`,
		`bob transition triaged>dismissed "legitimate hedging"`,
	}
	if !slices.Equal(lines, want) {
		t.Fatalf("audit trail:\n%v\nwant\n%v", lines, want)
	}
	for _, err := range []error{
		func() error { _, err := s.Transition(ctx, "nope", StatusTriaged, "ana", ""); return err }(),
		func() error { _, err := s.Comment(ctx, "nope", "ana", "x"); return err }(),
		func() error { _, err := s.Audit(ctx, "nope"); return err }(),
		func() error { _, _, err := s.Get(ctx, "nope"); return err }(),
	} {
		if !errors.Is(err, ErrNotFound) {
			t.Errorf("unknown alert: %v", err)
		}
	}
}

func TestConcurrentTransitionsAreSerialized(t *testing.T) {
	s := open(t)
	ctx := context.Background()
	if _, _, err := s.UpsertAlerts(ctx, []*twpb.Alert{testAlert("k1")}); err != nil {
		t.Fatal(err)
	}
	errs := make(chan error, 2)
	for _, who := range []string{"ana", "bob"} {
		go func() {
			_, err := s.Transition(ctx, "k1", StatusTriaged, who, "")
			errs <- err
		}()
	}
	var ok, refused int
	for range 2 {
		switch err := <-errs; {
		case err == nil:
			ok++
		case errors.Is(err, ErrInvalidTransition):
			refused++
		default:
			t.Fatal(err)
		}
	}
	if ok != 1 || refused != 1 {
		t.Fatalf("%d succeeded, %d refused; want exactly one of each", ok, refused)
	}
}

func TestListFiltersAndPaginates(t *testing.T) {
	s := open(t)
	ctx := context.Background()
	var batch []*twpb.Alert
	for i := range 25 {
		batch = append(batch, testAlert(fmt.Sprintf("k%02d", i), func(a *twpb.Alert) {
			a.DetectedAtNs = int64(1_000 + i/2) // pairs share a detection time; the key breaks ties
			a.Score = float64(50 + i)
			if i%3 == 0 {
				a.AbuseType, a.Symbol, a.AccountIds = twpb.AbuseType_ABUSE_TYPE_WASH_TRADING, "MOSS", []string{"ACC-1", "ACC-9"}
			}
		}))
	}
	if _, _, err := s.UpsertAlerts(ctx, batch); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Transition(ctx, "k03", StatusTriaged, "ana", ""); err != nil {
		t.Fatal(err)
	}

	var seen []string
	cursor := ""
	for page := 0; ; page++ {
		got, next, err := s.List(ctx, Filter{Limit: 10, Cursor: cursor})
		if err != nil {
			t.Fatal(err)
		}
		for _, a := range got {
			seen = append(seen, a.Key)
		}
		if next == "" {
			if page != 2 {
				t.Errorf("%d pages, want 3", page+1)
			}
			break
		}
		cursor = next
	}
	var want []string
	for i := 24; i >= 0; i-- {
		want = append(want, fmt.Sprintf("k%02d", i))
	}
	if !slices.Equal(seen, want) {
		t.Fatalf("pages gave %v\nwant %v", seen, want)
	}

	for _, c := range []struct {
		f    Filter
		want int
	}{
		{Filter{AbuseType: "wash_trading"}, 9},
		{Filter{Symbol: "MOSS", Account: "ACC-9"}, 9},
		{Filter{Status: StatusTriaged}, 1},
		{Filter{MinScore: 70}, 5},
		{Filter{Session: "S2"}, 0},
	} {
		got, _, err := s.List(ctx, c.f)
		if err != nil || len(got) != c.want {
			t.Errorf("List(%+v) = %d alerts, err %v; want %d", c.f, len(got), err, c.want)
		}
	}
	if _, _, err := s.List(ctx, Filter{Cursor: "not-a-cursor"}); !errors.Is(err, ErrBadCursor) {
		t.Errorf("bad cursor: %v", err)
	}

	counts, err := s.Count(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if counts.Total != 25 || counts.ByType["wash_trading"] != 9 || counts.ByStatus[StatusTriaged] != 1 ||
		counts.BySymbol["QZAP"] != 16 {
		t.Errorf("counts = %+v", counts)
	}
	recent, err := s.Recent(ctx, 5)
	if err != nil || len(recent) != 5 {
		t.Fatalf("recent: %d %v", len(recent), err)
	}
}

func snapshot(digest, by string, at time.Time, rules ...*twpb.RuleDocument) *twpb.RuleSetSnapshot {
	return &twpb.RuleSetSnapshot{RulesetDigest: digest, LoadedAtNs: at.UnixNano(), LoadedBy: by, Rules: rules}
}

func TestRuleSetsAreRecordedIdempotently(t *testing.T) {
	s := open(t)
	ctx := context.Background()
	if _, _, err := s.CurrentRules(ctx); !errors.Is(err, ErrNotFound) {
		t.Fatalf("before any load: %v", err)
	}
	wash1 := &twpb.RuleDocument{RuleId: "TW-WASH-001", Version: 1, Digest: "w1", Detector: "wash_trading",
		Name: "Wash", TaxonomyCode: "MAR-WASH", Enabled: true, Source: []byte("window: 10m")}
	spoof1 := &twpb.RuleDocument{RuleId: "TW-SPOOF-001", Version: 1, Digest: "s1", Detector: "spoofing",
		Name: "Spoof", TaxonomyCode: "MAR-SPOOF", Enabled: true, Source: []byte("window: 5m")}
	t0 := time.Date(2026, 10, 14, 9, 0, 0, 0, time.UTC)
	first := snapshot("set-1", "det-a", t0, wash1, spoof1)
	for range 2 { // a redelivered snapshot is stored once
		if err := s.RecordRuleSet(ctx, first); err != nil {
			t.Fatal(err)
		}
	}
	// The wash rule is edited without a version bump, and loaded later.
	wash1b := proto.Clone(wash1).(*twpb.RuleDocument)
	wash1b.Digest, wash1b.Source = "w1b", []byte("window: 20m")
	if err := s.RecordRuleSet(ctx, snapshot("set-2", "det-b", t0.Add(time.Minute), wash1b, spoof1)); err != nil {
		t.Fatal(err)
	}
	load, rules, err := s.CurrentRules(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if load.Digest != "set-2" || load.LoadedBy != "det-b" || !load.LoadedAt.Equal(t0.Add(time.Minute)) || len(rules) != 2 {
		t.Fatalf("current = %+v with %d rules", load, len(rules))
	}
	if rules[0].RuleID != "TW-SPOOF-001" || rules[0].VersionReused || rules[1].Digest != "w1b" || !rules[1].VersionReused {
		t.Errorf("rules = %+v", rules)
	}
	history, err := s.RuleHistory(ctx, "TW-WASH-001")
	if err != nil || len(history) != 2 {
		t.Fatalf("history %+v, err %v", history, err)
	}
	if _, err := s.RuleHistory(ctx, "TW-NOPE"); !errors.Is(err, ErrNotFound) {
		t.Errorf("unknown rule: %v", err)
	}
}
