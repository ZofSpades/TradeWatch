package testutil

import (
	"context"
	"testing"

	"github.com/ZofSpades/tradewatch/internal/events"
	"github.com/ZofSpades/tradewatch/internal/events/twpb"
	"github.com/ZofSpades/tradewatch/internal/orderbook"
	"github.com/ZofSpades/tradewatch/internal/recording"
	"github.com/ZofSpades/tradewatch/internal/sim"
)

// Record simulates the session that cfg describes and writes its recording
// to path the way marketsim does: the reference data, every event and every
// ground-truth label.
func Record(t testing.TB, path string, cfg sim.Config) {
	t.Helper()
	w, err := recording.Create(path, &twpb.RecordingHeader{SessionId: cfg.SessionID})
	if err != nil {
		t.Fatal(err)
	}
	uni := sim.DefaultUniverse()
	s, err := sim.New(cfg, uni, &recordingEmitter{w: w, session: cfg.SessionID})
	if err != nil {
		t.Fatal(err)
	}
	session := &twpb.RefDataRecord{Record: &twpb.RefDataRecord_Session{Session: s.Session().Proto()}}
	for _, rd := range append(uni.RefData(), session) {
		if err := w.WriteRefData(rd); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

type recordingEmitter struct {
	w       *recording.Writer
	session string
}

func (r *recordingEmitter) Emit(sym *sim.Symbol, ev *orderbook.Event) error {
	wrapped, err := events.Wrap(sim.ToProto(r.session, sym, ev, 0))
	if err != nil {
		return err
	}
	return r.w.WriteEvent(wrapped)
}

func (r *recordingEmitter) Label(l *twpb.GroundTruthLabel) error { return r.w.WriteLabel(l) }
