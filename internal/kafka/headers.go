package kafka

import (
	"strconv"
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Record header keys. Routing metadata is duplicated in headers so that a
// consumer can identify a record (and fill a sequence gap) even when its
// value cannot be decoded.
const (
	// HeaderSchema is the fully-qualified protobuf message name of the value.
	HeaderSchema = "tw-schema"
	// HeaderSession is the session the event belongs to.
	HeaderSession = "tw-session"
	// HeaderSeq is the event's per-symbol sequence number, in decimal.
	HeaderSeq = "tw-seq"
)

// EventMeta is the routing metadata carried in record headers.
type EventMeta struct {
	Schema  string
	Session string
	Seq     uint64
	// HasSeq reports whether a valid sequence header was present.
	HasSeq bool
}

// headerBlock holds a record's routing headers and their values in one
// allocation. Building headers runs for every record marketsim and ingest
// produce; with a separate allocation per header and per value it was a
// hotspot (docs/performance.md).
type headerBlock struct {
	hs  [3]kgo.RecordHeader
	buf [96]byte
}

// Headers encodes m as record headers. The headers belong to one record:
// their values never overlap, so appending to one cannot change another.
func (m EventMeta) Headers() []kgo.RecordHeader {
	b := new(headerBlock)
	buf := b.buf[:0]
	n := 0
	put := func(key string, start int) {
		b.hs[n] = kgo.RecordHeader{Key: key, Value: buf[start:len(buf):len(buf)]}
		n++
	}
	// A value that does not fit in the block's buffer moves the rest to a
	// new array; values already taken keep pointing at the old one.
	start := len(buf)
	buf = append(buf, m.Schema...)
	put(HeaderSchema, start)
	if m.Session != "" {
		start = len(buf)
		buf = append(buf, m.Session...)
		put(HeaderSession, start)
	}
	if m.HasSeq {
		start = len(buf)
		buf = strconv.AppendUint(buf, m.Seq, 10)
		put(HeaderSeq, start)
	}
	return b.hs[:n]
}

// interned shares the strings of header values that repeat on every
// record, schema names and session IDs, so that parsing them does not
// allocate a string per record. It is bounded: when full it starts over.
var interned struct {
	sync.RWMutex
	m map[string]string
}

const maxInterned = 1024

func intern(b []byte) string {
	interned.RLock()
	s, ok := interned.m[string(b)] // a lookup by string(b) does not allocate
	interned.RUnlock()
	if ok {
		return s
	}
	s = string(b)
	interned.Lock()
	if interned.m == nil || len(interned.m) >= maxInterned {
		interned.m = make(map[string]string, 64)
	}
	interned.m[s] = s
	interned.Unlock()
	return s
}

// parseSeq parses an unsigned decimal sequence number, accepting exactly
// what strconv.ParseUint(s, 10, 64) does, without converting to a string.
func parseSeq(b []byte) (uint64, bool) {
	if len(b) == 0 {
		return 0, false
	}
	var n uint64
	for _, c := range b {
		if c < '0' || c > '9' {
			return 0, false
		}
		d := uint64(c - '0')
		if n > (1<<64-1-d)/10 {
			return 0, false // overflow
		}
		n = n*10 + d
	}
	return n, true
}

// ParseEventMeta decodes routing metadata from record headers. Missing or
// malformed headers leave the corresponding fields zero.
func ParseEventMeta(hs []kgo.RecordHeader) EventMeta {
	var m EventMeta
	for _, h := range hs {
		switch h.Key {
		case HeaderSchema:
			m.Schema = intern(h.Value)
		case HeaderSession:
			m.Session = intern(h.Value)
		case HeaderSeq:
			if n, ok := parseSeq(h.Value); ok {
				m.Seq, m.HasSeq = n, true
			}
		}
	}
	return m
}

// HeaderValue returns the value of the first header with the given key.
func HeaderValue(hs []kgo.RecordHeader, key string) (string, bool) {
	for _, h := range hs {
		if h.Key == key {
			return string(h.Value), true
		}
	}
	return "", false
}
