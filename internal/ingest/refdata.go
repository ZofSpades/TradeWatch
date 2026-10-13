package ingest

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/ZofSpades/tradewatch/internal/events"
	"github.com/ZofSpades/tradewatch/internal/events/twpb"
)

// ReferenceData is a concurrency-safe in-memory reference-data store. It
// implements Reference.
type ReferenceData struct {
	mu          sync.RWMutex
	accounts    map[string]*twpb.Account
	instruments map[string]*twpb.Instrument
	sessions    map[string]*twpb.TradingSession
}

// NewReferenceData returns a store holding recs.
func NewReferenceData(recs ...*twpb.RefDataRecord) *ReferenceData {
	r := &ReferenceData{
		accounts:    map[string]*twpb.Account{},
		instruments: map[string]*twpb.Instrument{},
		sessions:    map[string]*twpb.TradingSession{},
	}
	for _, rec := range recs {
		r.Apply(rec)
	}
	return r
}

// Apply inserts or replaces one record.
func (r *ReferenceData) Apply(rec *twpb.RefDataRecord) {
	r.mu.Lock()
	defer r.mu.Unlock()
	switch v := rec.GetRecord().(type) {
	case *twpb.RefDataRecord_Account:
		r.accounts[v.Account.GetAccountId()] = v.Account
	case *twpb.RefDataRecord_Instrument:
		r.instruments[v.Instrument.GetSymbol()] = v.Instrument
	case *twpb.RefDataRecord_Session:
		r.sessions[v.Session.GetSessionId()] = v.Session
	}
}

func (r *ReferenceData) Account(id string) (*twpb.Account, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	a, ok := r.accounts[id]
	return a, ok
}

func (r *ReferenceData) Instrument(symbol string) (*twpb.Instrument, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	i, ok := r.instruments[symbol]
	return i, ok
}

func (r *ReferenceData) Session(id string) (*twpb.TradingSession, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	s, ok := r.sessions[id]
	return s, ok
}

// Counts returns the number of accounts, instruments and sessions held.
func (r *ReferenceData) Counts() (accounts, instruments, sessions int) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.accounts), len(r.instruments), len(r.sessions)
}

// RefDataTail keeps a ReferenceData up to date from the compacted refdata
// topic.
type RefDataTail struct {
	Data *ReferenceData

	cl       *kgo.Client
	adm      *kadm.Client
	topic    string
	log      *slog.Logger
	consumed atomic.Int64 // next offset to consume on partition 0
	lastSync atomic.Int64 // unix nanos of the last successful CatchUp
}

// NewRefDataTail consumes topic from the beginning with cl, which must be a
// direct (non-group) consumer of that topic.
func NewRefDataTail(cl *kgo.Client, topic string, log *slog.Logger) *RefDataTail {
	return &RefDataTail{Data: NewReferenceData(), cl: cl, adm: kadm.NewClient(cl), topic: topic, log: log}
}

// Run consumes until ctx is cancelled.
func (t *RefDataTail) Run(ctx context.Context) {
	for {
		fetches := t.cl.PollFetches(ctx)
		if ctx.Err() != nil {
			return
		}
		fetches.EachError(func(topic string, p int32, err error) {
			t.log.Warn("refdata fetch error", "topic", topic, "partition", p, "err", err)
		})
		fetches.EachRecord(func(r *kgo.Record) {
			var rec twpb.RefDataRecord
			if err := events.Unmarshal(r.Value, &rec); err != nil {
				t.log.Warn("skipping undecodable refdata record", "offset", r.Offset, "err", err)
			} else {
				t.Data.Apply(&rec)
			}
			if r.Partition == 0 {
				t.consumed.Store(r.Offset + 1)
			}
		})
	}
}

// CatchUp waits until every refdata record that exists right now has been
// applied, or the timeout elapses.
func (t *RefDataTail) CatchUp(ctx context.Context, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	ends, err := t.adm.ListEndOffsets(ctx, t.topic)
	if err != nil {
		return fmt.Errorf("refdata end offsets: %w", err)
	}
	end, ok := ends.Lookup(t.topic, 0)
	if !ok || end.Err != nil {
		return errors.New("refdata end offset unavailable")
	}
	for t.consumed.Load() < end.Offset {
		select {
		case <-ctx.Done():
			return fmt.Errorf("refdata at offset %d, want %d: %w", t.consumed.Load(), end.Offset, ctx.Err())
		case <-time.After(5 * time.Millisecond):
		}
	}
	t.lastSync.Store(time.Now().UnixNano())
	return nil
}

// SyncedWithin reports whether a CatchUp succeeded within the last d.
func (t *RefDataTail) SyncedWithin(d time.Duration) bool {
	return time.Since(time.Unix(0, t.lastSync.Load())) < d
}
