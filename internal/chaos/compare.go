// Package chaos checks that the pipeline loses and duplicates no alert while
// its services are killed, restarted and rebalanced and old records are
// replayed into it. `make chaos` injects the faults into the compose stack
// during one recorded session. This package then compares the alerts
// Postgres stored for the session with those an offline replay of the
// session's recording produces, and writes the report to
// docs/results/chaos.
package chaos

import (
	"slices"

	"google.golang.org/protobuf/proto"

	"github.com/ZofSpades/tradewatch/internal/events/twpb"
)

// normalized clears what legitimately differs between a live alert and the
// same alert replayed offline: wall-clock times, the instance that raised
// it, and its trace.
func normalized(a *twpb.Alert) *twpb.Alert {
	c := proto.Clone(a).(*twpb.Alert)
	c.EmittedAtNs, c.TriggerPublishedAtNs, c.TraceId, c.DetectorInstance = 0, 0, "", ""
	return c
}

// Comparison matches the alerts stored for a session with the expected
// ones, by alert key.
type Comparison struct {
	Expected int `json:"expected"`
	Stored   int `json:"stored"`
	// Missing are expected alerts that were not stored; Unexpected, stored
	// alerts no replay produces; Differing, stored alerts whose content is
	// not the expected one.
	Missing    []string `json:"missing"`
	Unexpected []string `json:"unexpected"`
	Differing  []string `json:"differing"`
}

// OK reports whether every expected alert was stored, with its expected
// content, and nothing else was.
func (c Comparison) OK() bool {
	return len(c.Missing) == 0 && len(c.Unexpected) == 0 && len(c.Differing) == 0
}

// Compare matches stored alerts, by key, with the expected ones.
func Compare(expected []*twpb.Alert, stored map[string]*twpb.Alert) Comparison {
	want := map[string]*twpb.Alert{}
	for _, a := range expected {
		want[a.GetAlertKey()] = a
	}
	c := Comparison{Expected: len(want), Stored: len(stored), Missing: []string{}, Unexpected: []string{}, Differing: []string{}}
	for key, w := range want {
		got, ok := stored[key]
		switch {
		case !ok:
			c.Missing = append(c.Missing, key)
		case !proto.Equal(normalized(got), normalized(w)):
			c.Differing = append(c.Differing, key)
		}
	}
	for key := range stored {
		if _, ok := want[key]; !ok {
			c.Unexpected = append(c.Unexpected, key)
		}
	}
	slices.Sort(c.Missing)
	slices.Sort(c.Unexpected)
	slices.Sort(c.Differing)
	return c
}
