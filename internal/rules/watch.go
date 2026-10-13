package rules

import (
	"context"
	"time"
)

// Watcher reloads a rules directory when its content changes. It polls
// rather than relying on file-system notifications, which do not work on
// every bind mount (for example Docker Desktop volumes on macOS and
// Windows).
type Watcher struct {
	Dir      string
	Interval time.Duration
	// OnLoad receives every successfully loaded set, OnError every failed
	// reload. The previous set stays in effect after a failure.
	OnLoad  func(*Set)
	OnError func(error)
	// Validate, when set, can reject a set that parsed but cannot be used
	// (for example because a detector rejects its parameters).
	Validate func(*Set) error
	last     string
}

// Run polls until ctx is cancelled. The directory's current state is
// treated as already loaded.
func (w *Watcher) Run(ctx context.Context) {
	w.last, _ = DirDigest(w.Dir)
	t := time.NewTicker(w.Interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		d, err := DirDigest(w.Dir)
		if err != nil || d == w.last {
			if err != nil && w.OnError != nil {
				w.OnError(err)
			}
			continue
		}
		w.last = d
		set, err := Load(w.Dir)
		if err == nil && w.Validate != nil {
			err = w.Validate(set)
		}
		if err != nil {
			if w.OnError != nil {
				w.OnError(err)
			}
			continue
		}
		if w.OnLoad != nil {
			w.OnLoad(set)
		}
	}
}
