package kafka

import (
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

// The parser accepts exactly what strconv.ParseUint accepts for a
// sequence number, and every value survives a round trip, however long.
func TestEventMetaEdgeCases(t *testing.T) {
	for _, c := range []struct {
		seq    string
		want   uint64
		hasSeq bool
	}{
		{"0", 0, true},
		{"18446744073709551615", 1<<64 - 1, true},
		{"18446744073709551616", 0, false},
		{"+5", 0, false},
		{"-5", 0, false},
		{"", 0, false},
		{"12a", 0, false},
		{"007", 7, true},
	} {
		m := ParseEventMeta([]kgo.RecordHeader{{Key: HeaderSeq, Value: []byte(c.seq)}})
		if m.HasSeq != c.hasSeq || m.Seq != c.want {
			t.Errorf("seq %q: got %d, %v; want %d, %v", c.seq, m.Seq, m.HasSeq, c.want, c.hasSeq)
		}
	}
	long := EventMeta{Schema: "tradewatch.v1." + strings.Repeat("Event", 30), Session: strings.Repeat("s", 200), Seq: 1<<64 - 1, HasSeq: true}
	if got := ParseEventMeta(long.Headers()); got != long {
		t.Errorf("long values: got %+v", got)
	}
	short := EventMeta{Schema: "tradewatch.v1.DeadLetter"}
	hs := short.Headers()
	if len(hs) != 1 || ParseEventMeta(hs) != short {
		t.Errorf("schema only: %v", hs)
	}
	// Header values must not overlap: growing one cannot change another.
	full := EventMeta{Schema: "a", Session: "b", Seq: 7, HasSeq: true}.Headers()
	_ = append(full[0].Value, 'x', 'y', 'z')
	if string(full[1].Value) != "b" || string(full[2].Value) != "7" {
		t.Errorf("headers overlap: %q %q", full[1].Value, full[2].Value)
	}
}

// Every record marketsim and ingest produce gets routing headers, and every
// record ingest consumes has them parsed.
var benchMeta = EventMeta{Schema: "tradewatch.v1.MarketEvent", Session: "live-20261014T154446Z-001", Seq: 1_234_567, HasSeq: true}

func BenchmarkEventMetaHeaders(b *testing.B) {
	b.ReportAllocs()
	var hs []kgo.RecordHeader
	for b.Loop() {
		hs = benchMeta.Headers()
	}
	_ = hs
}

func BenchmarkParseEventMeta(b *testing.B) {
	hs := benchMeta.Headers()
	b.ReportAllocs()
	var m EventMeta
	for b.Loop() {
		m = ParseEventMeta(hs)
	}
	_ = m
}
