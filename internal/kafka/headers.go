package kafka

import (
	"strconv"

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

// Headers encodes m as record headers.
func (m EventMeta) Headers() []kgo.RecordHeader {
	hs := []kgo.RecordHeader{{Key: HeaderSchema, Value: []byte(m.Schema)}}
	if m.Session != "" {
		hs = append(hs, kgo.RecordHeader{Key: HeaderSession, Value: []byte(m.Session)})
	}
	if m.HasSeq {
		hs = append(hs, kgo.RecordHeader{Key: HeaderSeq, Value: strconv.AppendUint(nil, m.Seq, 10)})
	}
	return hs
}

// ParseEventMeta decodes routing metadata from record headers. Missing or
// malformed headers leave the corresponding fields zero.
func ParseEventMeta(hs []kgo.RecordHeader) EventMeta {
	var m EventMeta
	for _, h := range hs {
		switch h.Key {
		case HeaderSchema:
			m.Schema = string(h.Value)
		case HeaderSession:
			m.Session = string(h.Value)
		case HeaderSeq:
			if n, err := strconv.ParseUint(string(h.Value), 10, 64); err == nil {
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
