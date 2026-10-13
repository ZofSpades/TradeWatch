package detect

import (
	"crypto/sha256"
	"encoding/hex"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/ZofSpades/tradewatch/internal/events/twpb"
)

// Feature is one measured quantity and the threshold its rule compares it
// against. Op is ">=" or "<=" for a threshold the feature must meet, or ""
// for an informational value that does not affect the decision or the
// score.
type Feature struct {
	Name      string
	Value     float64
	Threshold float64
	Op        string
	Unit      string
}

// AtLeast is a feature that must reach its threshold.
func AtLeast(name string, value, threshold float64, unit string) Feature {
	return Feature{Name: name, Value: value, Threshold: threshold, Op: ">=", Unit: unit}
}

// AtMost is a feature that must not exceed its threshold.
func AtMost(name string, value, threshold float64, unit string) Feature {
	return Feature{Name: name, Value: value, Threshold: threshold, Op: "<=", Unit: unit}
}

// Info is a feature reported for context only.
func Info(name string, value float64, unit string) Feature {
	return Feature{Name: name, Value: value, Unit: unit}
}

func (f Feature) breached() bool {
	switch f.Op {
	case ">=":
		return f.Value >= f.Threshold
	case "<=":
		return f.Value <= f.Threshold
	default:
		return false
	}
}

// margin is how far the feature is past its threshold, as a ratio (1 = at
// the threshold).
func (f Feature) margin() float64 {
	switch f.Op {
	case ">=":
		if f.Threshold <= 0 {
			return 2
		}
		return f.Value / f.Threshold
	case "<=":
		if f.Value <= 0 {
			return 2
		}
		return f.Threshold / f.Value
	default:
		return 0
	}
}

// score maps features to 0..100. An alert whose every thresholded feature
// is exactly at its threshold scores 50; one where every feature is at
// least twice past its threshold scores 100. The score orders alerts for
// triage; it is not a probability.
func score(fs []Feature) float64 {
	var sum float64
	var n int
	for _, f := range fs {
		if f.Op == "" {
			continue
		}
		sum += math.Max(0, math.Min(1, f.margin()-1))
		n++
	}
	if n == 0 {
		return 50
	}
	return math.Round((50+50*sum/float64(n))*10) / 10
}

// Draft is what a detector hands to Context.Emit; the engine completes it
// into an Alert.
type Draft struct {
	// Accounts involved; the engine sorts and de-duplicates them.
	Accounts []string
	// Anchor identifies this instance of the pattern within the rule,
	// session, symbol and accounts, for example "trade:1234". It must be
	// derived from the events alone so that reprocessing yields the same
	// alert key.
	Anchor string
	// SuppressKey groups alerts for suppression; empty means the accounts.
	SuppressKey string

	WindowStart, WindowEnd int64
	Features               []Feature
	// Facts are strings for the explanation template, such as a side.
	Facts map[string]string

	Orders   []uint64
	Trades   []uint64
	Timeline []*twpb.EvidenceEvent
}

// AlertKey derives the deterministic idempotency key of an alert. The rule
// version is deliberately not part of it: the same pattern instance seen
// again under a newer version of the rule is the same alert.
func AlertKey(ruleID, session, symbol string, accounts []string, anchor string) string {
	h := sha256.New()
	for _, part := range []string{ruleID, session, symbol, strings.Join(accounts, ","), anchor} {
		h.Write([]byte(part))
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))[:32]
}

func sortedUnique(xs []string) []string {
	out := slices.Clone(xs)
	slices.Sort(out)
	return slices.Compact(out)
}

// ExplanationData is the data an explanation template sees.
type ExplanationData struct {
	Symbol   string
	Accounts []string
	Owners   []string
	F        map[string]float64
	Facts    map[string]string
}

// humanDuration renders a rule parameter for an explanation, such as
// "500 ms", "1 s", "90 s", "10 min" or "1 h".
func humanDuration(d time.Duration) string {
	num := func(v float64) string { return strconv.FormatFloat(v, 'f', -1, 64) }
	switch {
	case d < time.Second:
		return num(float64(d.Milliseconds())) + " ms"
	case d < time.Minute || d%time.Minute != 0:
		return num(d.Seconds()) + " s"
	case d < time.Hour || d%time.Hour != 0:
		return num(d.Minutes()) + " min"
	default:
		return num(d.Hours()) + " h"
	}
}

func featureMap(fs []Feature) map[string]float64 {
	m := make(map[string]float64, len(fs))
	for _, f := range fs {
		m[f.Name] = f.Value
	}
	return m
}

// evidenceOrder renders a remembered order as a timeline entry.
func evidenceOrder(o *OrderInfo, role string) *twpb.EvidenceEvent {
	return &twpb.EvidenceEvent{
		EventTimeNs: o.Entry, Kind: twpb.EvidenceKind_EVIDENCE_KIND_ORDER, OrderId: o.ID,
		AccountId: o.Account, Side: o.Side, Price: o.Price, Quantity: o.Qty, Role: role,
	}
}

// evidenceTrade renders a trade as a timeline entry from one account's
// point of view.
func evidenceTrade(t int64, seq uint64, tr *twpb.TradeEvent, account, role string) *twpb.EvidenceEvent {
	side, other := twpb.Side_SIDE_BUY, tr.GetSellAccountId()
	if tr.GetSellAccountId() == account {
		side, other = twpb.Side_SIDE_SELL, tr.GetBuyAccountId()
	}
	return &twpb.EvidenceEvent{
		EventTimeNs: t, Seq: seq, Kind: twpb.EvidenceKind_EVIDENCE_KIND_TRADE, TradeId: tr.GetTradeId(),
		AccountId: account, Side: side, Price: tr.GetPrice(), Quantity: tr.GetQuantity(),
		CounterpartyAccountId: other, Role: role,
	}
}

func sortTimeline(tl []*twpb.EvidenceEvent) {
	slices.SortStableFunc(tl, func(a, b *twpb.EvidenceEvent) int {
		if a.GetEventTimeNs() != b.GetEventTimeNs() {
			if a.GetEventTimeNs() < b.GetEventTimeNs() {
				return -1
			}
			return 1
		}
		switch {
		case a.GetSeq() < b.GetSeq():
			return -1
		case a.GetSeq() > b.GetSeq():
			return 1
		}
		return 0
	})
}
