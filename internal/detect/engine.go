// Package detect is TradeWatch's detection engine: a pure library that turns
// the per-symbol sequenced market_events stream into explainable alerts.
//
// The engine owns one state per (session, symbol): a Market view shared by
// all detectors and one Detector instance per enabled rule. It drops
// duplicate events by sequence number, keeps every window in event time,
// and completes the drafts detectors emit into alerts with a deterministic
// key, a score, a rendered explanation and an evidence trail.
//
// The same engine runs inside the detector service, the replay tool and the
// evaluator, so live processing, replays and evaluation produce the same
// alerts for the same input.
package detect

import (
	"math"
	"slices"
	"strings"
	"time"

	"github.com/ZofSpades/tradewatch/internal/events"
	"github.com/ZofSpades/tradewatch/internal/events/twpb"
	"github.com/ZofSpades/tradewatch/internal/rules"
)

// Config bounds the engine's resources.
type Config struct {
	// Instance names this engine in the alerts it produces.
	Instance string
	// MaxOrders bounds the orders remembered per symbol (default 200,000).
	MaxOrders int
	// OrderHorizon is how long order details are remembered (default 15m).
	OrderHorizon time.Duration
	// ContextBefore is how much market history before an alert's window its
	// price chart shows (default 60s).
	ContextBefore time.Duration
	// MaxContextPoints bounds the points of an alert's chart (default 300).
	MaxContextPoints int
	// IdleAfter is how far a sequence's newest event may fall behind the
	// stream's newest event, in ingest time, before the sequence is taken
	// for abandoned and its state dropped (default 10m).
	IdleAfter time.Duration
}

func (c Config) withDefaults() Config {
	if c.MaxOrders <= 0 {
		c.MaxOrders = 200_000
	}
	if c.OrderHorizon <= 0 {
		c.OrderHorizon = 15 * time.Minute
	}
	if c.ContextBefore <= 0 {
		c.ContextBefore = time.Minute
	}
	if c.MaxContextPoints <= 0 {
		c.MaxContextPoints = 300
	}
	if c.IdleAfter <= 0 {
		c.IdleAfter = 10 * time.Minute
	}
	return c
}

// Stats counts what an engine did.
type Stats struct {
	Events        uint64
	Duplicates    uint64
	Stale         uint64 // events of a session that already closed
	Gaps          uint64 // gap markers and unmarked jumps in the sequence
	LostEvents    uint64 // sequence numbers covered by gaps
	Late          uint64 // events whose time was before the symbol's watermark
	Alerts        uint64
	Suppressed    uint64
	OrdersEvicted uint64
	Abandoned     uint64 // sequences dropped after their session went idle without closing
	OpenSymbols   int
}

type symKey struct{ session, symbol string }

func (k symKey) String() string { return k.session + "|" + k.symbol }

func parseKey(s string) (symKey, bool) {
	i := strings.LastIndexByte(s, '|')
	if i <= 0 || i == len(s)-1 {
		return symKey{}, false
	}
	return symKey{s[:i], s[i+1:]}, true
}

// mark records, at most once per second of event time, an input position
// and the sequence number of the event there, for restart checkpoints.
type mark struct {
	pos  int64
	t    int64
	next uint64
}

type detInstance struct {
	b *boundRule
	d Detector
}

type symbolState struct {
	key        symKey
	next       uint64
	now        int64
	market     *Market
	owners     map[string]string // account -> beneficial owner
	dets       []*detInstance    // in rule ID order
	suppress   map[string]suppression
	marks      []mark
	lastExpire int64
	ingested   int64 // ingest time of the newest event
}

// suppression silences repeats of an alert until a point in event time.
// Reprocessing must regenerate the alert to rebuild the suppression, so a
// checkpoint taken while it is active reaches back to needFrom: the
// earliest event the alert could have depended on.
type suppression struct {
	until    int64
	needFrom int64
}

// Engine runs detectors over one stream of market events. It is not safe
// for concurrent use; the detector service runs one engine per partition.
type Engine struct {
	cfg   Config
	set   *rules.Set
	bound []*boundRule

	symbols     map[symKey]*symbolState
	closed      map[symKey]bool
	closedOrder []symKey
	restored    map[symKey]uint64

	// ingested is the newest ingest time seen; idleSweep is the second of
	// ingest time in which idle sequences were last looked for.
	ingested  int64
	idleSweep int64

	out   []*twpb.Alert
	stats Stats
}

// maxClosed bounds how many closed sessions are remembered to drop their
// late duplicates.
const maxClosed = 32

// NewEngine binds a rule set.
func NewEngine(set *rules.Set, cfg Config) (*Engine, error) {
	bound, err := bind(set)
	if err != nil {
		return nil, err
	}
	return &Engine{
		cfg: cfg.withDefaults(), set: set, bound: bound,
		symbols: map[symKey]*symbolState{}, closed: map[symKey]bool{}, restored: map[symKey]uint64{},
	}, nil
}

// Rules returns the rule set in effect.
func (e *Engine) Rules() *rules.Set { return e.set }

// SetRules swaps the rule set between two events. Detector state of rules
// that remain is kept, so a parameter change applies from the next event;
// rules that were removed lose their state, new rules start empty.
func (e *Engine) SetRules(set *rules.Set) error {
	bound, err := bind(set)
	if err != nil {
		return err
	}
	e.set, e.bound = set, bound
	for _, st := range e.symbols {
		old := map[string]*detInstance{}
		for _, di := range st.dets {
			old[di.b.rule.ID] = di
		}
		st.dets = st.dets[:0]
		for _, b := range bound {
			if di, ok := old[b.rule.ID]; ok && di.b.rule.Detector == b.rule.Detector {
				di.b = b
				st.dets = append(st.dets, di)
			} else {
				st.dets = append(st.dets, &detInstance{b: b, d: b.kind.New()})
			}
		}
	}
	return nil
}

// ruleHorizon is the history an alert of the rule can depend on: the
// detector's windows plus the chart context before them.
func (e *Engine) ruleHorizon(b *boundRule) int64 {
	return int64(b.params.Horizon() + e.cfg.ContextBefore)
}

// Horizon is how much event-time history the detectors and the order memory
// look back over. Reprocessing this much history, and further back while a
// suppression is active (see Checkpoint), rebuilds the state exactly.
func (e *Engine) Horizon() time.Duration {
	h := e.cfg.OrderHorizon
	for _, b := range e.bound {
		h = max(h, time.Duration(e.ruleHorizon(b)))
	}
	return h
}

// Stats returns cumulative counters.
func (e *Engine) Stats() Stats {
	s := e.stats
	s.OpenSymbols = len(e.symbols)
	return s
}

func (e *Engine) newSymbol(key symKey) *symbolState {
	st := &symbolState{
		key: key, next: 1, market: newMarket(),
		owners: map[string]string{}, suppress: map[string]suppression{},
	}
	if n, ok := e.restored[key]; ok {
		st.next = n
		delete(e.restored, key)
	}
	for _, b := range e.bound {
		st.dets = append(st.dets, &detInstance{b: b, d: b.kind.New()})
	}
	e.symbols[key] = st
	return st
}

// Process runs one event through every detector of its symbol and returns
// the alerts it produced. pos is the event's position in its input (a
// Kafka offset), used for restart checkpoints; pass -1 when there is none.
// The returned slice is reused by the next call.
func (e *Engine) Process(ev *twpb.MarketEvent, pos int64) []*twpb.Alert {
	e.out = e.out[:0]
	h := events.HeaderOf(ev)
	if h == nil {
		return nil
	}
	key := symKey{h.GetSessionId(), h.GetSymbol()}
	if e.closed[key] {
		e.stats.Stale++
		return nil
	}
	st := e.symbols[key]
	if st == nil {
		st = e.newSymbol(key)
	}
	seq := h.GetSeq()
	if seq < st.next {
		e.stats.Duplicates++
		return nil
	}
	if seq > st.next {
		e.stats.Gaps++
		e.stats.LostEvents += seq - st.next
	}
	t := h.GetEventTimeNs()
	if t < st.now {
		e.stats.Late++
		t = st.now
	}
	st.now = t
	if w := ev.GetIngestedAtNs(); w > 0 {
		st.ingested = w
		e.ingested = max(e.ingested, w)
	}
	e.stats.Events++
	if pos >= 0 && (len(st.marks) == 0 || t-st.marks[len(st.marks)-1].t >= int64(time.Second)) {
		st.marks = append(st.marks, mark{pos: pos, t: t, next: seq})
	}
	kind := events.KindOf(ev)
	st.next = seq + 1
	if kind == events.KindGap {
		g := ev.GetGap()
		e.stats.Gaps++
		e.stats.LostEvents += g.GetLastSeq() - seq + 1
		st.next = g.GetLastSeq() + 1
	}

	e.before(st, ev, kind, t)
	c := Context{e: e, st: st, ev: ev, h: h, kind: kind}
	for _, di := range st.dets {
		c.b = di.b
		di.d.OnEvent(&c)
	}
	e.after(st, ev, kind)
	e.maintain(st)
	if kind == events.KindBook && ev.GetBook().GetPhase() == twpb.TradingPhase_TRADING_PHASE_CLOSED {
		e.close(key)
	}
	e.forgetIdle()
	return e.out
}

// forgetIdle drops the state of sequences whose newest event was ingested
// more than IdleAfter before the stream's newest event: the sessions of a
// producer that stopped without closing them, as a marketsim restart does.
// Without further events their state cannot produce an alert, but it would
// hold every checkpoint back at the old session's positions. Event time
// cannot tell an abandoned session from a live one, because two sessions'
// clocks are unrelated; ingest time can, and since it travels with the
// events the decision is the same in every run over the same input. The
// check runs whenever the newest ingest time enters a new second. A session
// that resumes after being dropped starts from a fresh state, with a gap.
func (e *Engine) forgetIdle() {
	sec := e.ingested / int64(time.Second)
	if sec == e.idleSweep {
		return
	}
	e.idleSweep = sec
	limit := e.ingested - int64(e.cfg.IdleAfter)
	for key, st := range e.symbols {
		if st.ingested > 0 && st.ingested < limit {
			delete(e.symbols, key)
			e.stats.Abandoned++
		}
	}
}

// before updates the shared market view with the event.
func (e *Engine) before(st *symbolState, ev *twpb.MarketEvent, kind events.Kind, t int64) {
	enr := ev.GetEnrichment()
	for _, a := range []*twpb.AccountRef{enr.GetAccount(), enr.GetBuyAccount(), enr.GetSellAccount()} {
		if a != nil && a.GetBeneficialOwnerId() != "" {
			st.owners[a.GetAccountId()] = a.GetBeneficialOwnerId()
		}
	}
	m := st.market
	switch kind {
	case events.KindOrder:
		o := ev.GetOrder()
		if prev, ok := m.orders[o.GetOrderId()]; ok && o.GetAction() == twpb.OrderAction_ORDER_ACTION_REPLACE {
			prev.Price, prev.Qty, prev.Open = o.GetPrice(), o.GetQuantity(), o.GetQuantity()
			m.remember(prev, e.cfg.MaxOrders)
			return
		}
		m.remember(&OrderInfo{
			ID: o.GetOrderId(), Account: o.GetAccountId(), Owner: st.owners[o.GetAccountId()],
			Side: o.GetSide(), Type: o.GetType(), TIF: o.GetTimeInForce(), Price: o.GetPrice(),
			Qty: o.GetQuantity(), Open: o.GetQuantity(), Entry: t,
		}, e.cfg.MaxOrders)
	case events.KindTrade:
		tr := ev.GetTrade()
		m.PrevLastPrice = m.LastPrice
		if m.PrevLastPrice == 0 {
			m.PrevLastPrice = tr.GetPrice()
		}
		m.LastPrice, m.LastTradeTime = tr.GetPrice(), t
	case events.KindBook:
		m.applySnapshot(ev.GetBook(), t)
	}
}

// after drops orders that the event closed.
func (e *Engine) after(st *symbolState, ev *twpb.MarketEvent, kind events.Kind) {
	m := st.market
	switch kind {
	case events.KindTrade:
		tr := ev.GetTrade()
		for _, leg := range []struct {
			id     uint64
			leaves int64
		}{{tr.GetBuyOrderId(), tr.GetBuyLeavesQuantity()}, {tr.GetSellOrderId(), tr.GetSellLeavesQuantity()}} {
			if o, ok := m.orders[leg.id]; ok {
				o.Open = leg.leaves
				if leg.leaves == 0 {
					delete(m.orders, leg.id)
				}
			}
		}
		m.PrevLastPrice = m.LastPrice
	case events.KindCancel:
		delete(m.orders, ev.GetCancel().GetOrderId())
	}
}

// maintain expires state older than the horizons, at most once per second
// of event time.
func (e *Engine) maintain(st *symbolState) {
	if st.now-st.lastExpire < int64(time.Second) {
		return
	}
	st.lastExpire = st.now
	horizon := int64(e.Horizon())
	e.stats.OrdersEvicted += uint64(st.market.expire(st.now-int64(e.cfg.OrderHorizon), st.now-horizon))
	for k, sup := range st.suppress {
		if sup.until <= st.now {
			delete(st.suppress, k)
		}
	}
	// Keep the latest mark at or before the replay cut and everything after.
	cut := e.replayCut(st)
	i := 0
	for i+1 < len(st.marks) && st.marks[i+1].t <= cut {
		i++
	}
	if i > 0 {
		st.marks = append(st.marks[:0], st.marks[i:]...)
	}
}

// replayCut is the event time reprocessing of a symbol must start from:
// the horizon, or earlier while a suppression is active so that the alert
// behind it is regenerated.
func (e *Engine) replayCut(st *symbolState) int64 {
	cut := st.now - int64(e.Horizon())
	for _, sup := range st.suppress {
		if sup.until > st.now {
			cut = min(cut, sup.needFrom)
		}
	}
	return cut
}

func (e *Engine) close(key symKey) {
	delete(e.symbols, key)
	e.closed[key] = true
	e.closedOrder = append(e.closedOrder, key)
	if len(e.closedOrder) > maxClosed {
		delete(e.closed, e.closedOrder[0])
		e.closedOrder = e.closedOrder[1:]
	}
}

// Checkpoint returns the earliest input position from which reprocessing
// rebuilds the engine's current state, the next sequence number at that
// position for every open sequence, and the recently closed sessions. ok is
// false when no positioned event has been processed.
func (e *Engine) Checkpoint() (pos int64, next map[string]uint64, closed []string, ok bool) {
	pos = math.MaxInt64
	next = map[string]uint64{}
	for key, st := range e.symbols {
		if len(st.marks) == 0 {
			continue
		}
		cut := e.replayCut(st)
		m := st.marks[0]
		for _, mk := range st.marks[1:] {
			if mk.t > cut {
				break
			}
			m = mk
		}
		pos = min(pos, m.pos)
		next[key.String()] = m.next
	}
	for _, k := range e.closedOrder {
		closed = append(closed, k.String())
	}
	return pos, next, closed, pos != math.MaxInt64
}

// Restore primes a new engine with a checkpoint before it reprocesses
// events from the checkpoint's position.
func (e *Engine) Restore(next map[string]uint64, closed []string) {
	for s, n := range next {
		if k, ok := parseKey(s); ok {
			e.restored[k] = n
		}
	}
	for _, s := range closed {
		if k, ok := parseKey(s); ok && !e.closed[k] {
			e.closed[k] = true
			e.closedOrder = append(e.closedOrder, k)
		}
	}
}

// Context is what a detector sees while handling one event.
type Context struct {
	e    *Engine
	st   *symbolState
	ev   *twpb.MarketEvent
	h    *twpb.EventHeader
	kind events.Kind
	b    *boundRule
}

// Event returns the event being processed.
func (c *Context) Event() *twpb.MarketEvent { return c.ev }

// Kind returns the event's kind.
func (c *Context) Kind() events.Kind { return c.kind }

// Now returns the symbol's event-time watermark: the time of the current
// event, never earlier than any event before it.
func (c *Context) Now() int64 { return c.st.now }

// Seq returns the current event's sequence number.
func (c *Context) Seq() uint64 { return c.h.GetSeq() }

// Symbol returns the symbol being processed.
func (c *Context) Symbol() string { return c.st.key.symbol }

// Market returns the shared market view.
func (c *Context) Market() *Market { return c.st.market }

// Params returns the rule parameters of the detector being run.
func (c *Context) Params() Params { return c.b.params }

// Owner returns an account's beneficial owner, if known.
func (c *Context) Owner(account string) string { return c.st.owners[account] }

// Emit hands a pattern the detector found to the engine, which suppresses
// repeats within the rule's suppression period and completes the rest into
// alerts.
func (c *Context) Emit(d Draft) { c.e.emit(c, d) }

func (e *Engine) emit(c *Context, d Draft) {
	st, b := c.st, c.b
	accounts := sortedUnique(d.Accounts)
	sk := d.SuppressKey
	if sk == "" {
		sk = strings.Join(accounts, ",")
	}
	sk = b.rule.ID + "|" + sk
	if sup, ok := st.suppress[sk]; ok && st.now < sup.until {
		e.stats.Suppressed++
		return
	}
	st.suppress[sk] = suppression{until: st.now + int64(b.rule.SuppressFor), needFrom: st.now - e.ruleHorizon(b)}

	var owners []string
	for _, a := range accounts {
		if o := st.owners[a]; o != "" {
			owners = append(owners, o)
		}
	}
	owners = sortedUnique(owners)
	sc := score(d.Features)
	severity := b.rule.Severity.Proto()
	if b.rule.EscalateAtScore > 0 && sc >= b.rule.EscalateAtScore {
		severity = twpb.Severity_SEVERITY_CRITICAL
	}
	facts := d.Facts
	if facts == nil {
		facts = map[string]string{}
	}
	var sb strings.Builder
	if err := b.rule.Explanation.Execute(&sb, ExplanationData{
		Symbol: st.key.symbol, Accounts: accounts, Owners: owners, F: featureMap(d.Features), Facts: facts,
	}); err != nil {
		sb.Reset()
		sb.WriteString("explanation could not be rendered: " + err.Error())
	}
	features := make([]*twpb.Feature, len(d.Features))
	for i, f := range d.Features {
		features[i] = &twpb.Feature{Name: f.Name, Value: f.Value, Threshold: f.Threshold, Op: f.Op, Breached: f.breached(), Unit: f.Unit}
	}
	sortTimeline(d.Timeline)
	orders := slices.Clone(d.Orders)
	slices.Sort(orders)
	trades := slices.Clone(d.Trades)
	slices.Sort(trades)
	e.out = append(e.out, &twpb.Alert{
		AlertKey:           AlertKey(b.rule.ID, st.key.session, st.key.symbol, accounts, d.Anchor),
		SessionId:          st.key.session,
		AbuseType:          b.rule.Taxonomy.Proto(),
		Detector:           b.rule.Detector,
		RuleId:             b.rule.ID,
		RuleVersion:        b.rule.Version,
		RuleDigest:         b.rule.Digest,
		TaxonomyCode:       b.rule.Taxonomy.Code,
		Severity:           severity,
		Score:              sc,
		Symbol:             st.key.symbol,
		AccountIds:         accounts,
		BeneficialOwnerIds: owners,
		WindowStartNs:      d.WindowStart,
		WindowEndNs:        d.WindowEnd,
		DetectedAtNs:       st.now,
		Explanation:        strings.Join(strings.Fields(sb.String()), " "),
		Evidence: &twpb.Evidence{
			OrderIds: slices.Compact(orders), TradeIds: slices.Compact(trades), Features: features,
			Timeline:     d.Timeline,
			PriceContext: st.market.context(d.WindowStart-int64(e.cfg.ContextBefore), st.now, e.cfg.MaxContextPoints),
		},
		TriggerPublishedAtNs: c.h.GetPublishedAtNs(),
		TriggerSeq:           c.h.GetSeq(),
		DetectorInstance:     e.cfg.Instance,
	})
	e.stats.Alerts++
}
