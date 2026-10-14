// Package replay reruns detection over a session recording, offline.
//
// marketsim records every session it publishes: the reference data, each
// event exactly as the matching engine produced it, and the ground-truth
// labels. Replaying a recording runs the events through the ingest stage
// and a detection engine per rule set, exactly as the services would, so a
// rule change can be backtested on recorded history: the alerts of two
// rule sets are compared by alert key, which does not depend on the rule
// version.
package replay

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/ZofSpades/tradewatch/internal/detect"
	"github.com/ZofSpades/tradewatch/internal/events"
	"github.com/ZofSpades/tradewatch/internal/events/twpb"
	"github.com/ZofSpades/tradewatch/internal/ingest"
	"github.com/ZofSpades/tradewatch/internal/recording"
	"github.com/ZofSpades/tradewatch/internal/rules"
)

// Outcome is the result of replaying a recording.
type Outcome struct {
	Header  *twpb.RecordingHeader
	Events  int
	Symbols int
	Labels  []*twpb.GroundTruthLabel
	// Alerts holds the alerts of each rule set, in the order given.
	Alerts [][]*twpb.Alert
}

// Run replays the recording at path through one engine per rule set.
func Run(ctx context.Context, path string, sets []*rules.Set) (*Outcome, error) {
	r, err := recording.Open(path)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return replay(ctx, r, sets)
}

func replay(ctx context.Context, r *recording.Reader, sets []*rules.Set) (*Outcome, error) {
	out := &Outcome{Header: r.Header(), Alerts: make([][]*twpb.Alert, len(sets))}
	engines := make([]*detect.Engine, len(sets))
	for i, set := range sets {
		e, err := detect.NewEngine(set, detect.Config{Instance: "replay"})
		if err != nil {
			return nil, err
		}
		engines[i] = e
	}
	var refRecords []*twpb.RefDataRecord
	var ref *ingest.ReferenceData
	symbols := map[string]bool{}
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		entry, err := r.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		switch e := entry.GetEntry().(type) {
		case *twpb.RecordingEntry_Refdata:
			if ref != nil {
				return nil, errors.New("replay: reference data after the first event")
			}
			refRecords = append(refRecords, e.Refdata)
		case *twpb.RecordingEntry_Label:
			out.Labels = append(out.Labels, e.Label)
		case *twpb.RecordingEntry_Event:
			if ref == nil {
				ref = ingest.NewReferenceData(refRecords...)
			}
			h := events.HeaderOf(e.Event)
			mev, rej := ingest.Normalize(events.Unwrap(e.Event), ref, h.GetEventTimeNs())
			if rej != nil {
				return nil, fmt.Errorf("replay: event %s/%s#%d rejected by ingest: %v", h.GetSessionId(), h.GetSymbol(), h.GetSeq(), rej)
			}
			symbols[h.GetSymbol()] = true
			for i, eng := range engines {
				out.Alerts[i] = append(out.Alerts[i], eng.Process(mev, int64(out.Events))...)
			}
			out.Events++
		}
	}
	out.Symbols = len(symbols)
	return out, nil
}

// Diff is the comparison of two alert lists by alert key.
type Diff struct {
	OnlyA, OnlyB []*twpb.Alert
	Both         int
}

// Compare matches the alerts of two rule sets by alert key. The key does
// not include the rule version, so the same pattern caught under two
// versions of a rule counts as caught by both.
func Compare(a, b []*twpb.Alert) Diff {
	inA, inB := map[string]bool{}, map[string]bool{}
	for _, x := range a {
		inA[x.GetAlertKey()] = true
	}
	for _, x := range b {
		inB[x.GetAlertKey()] = true
	}
	var d Diff
	for _, x := range a {
		if !inB[x.GetAlertKey()] {
			d.OnlyA = append(d.OnlyA, x)
		} else {
			d.Both++
		}
	}
	for _, x := range b {
		if !inA[x.GetAlertKey()] {
			d.OnlyB = append(d.OnlyB, x)
		}
	}
	return d
}
