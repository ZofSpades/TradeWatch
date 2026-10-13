package rules

import (
	"time"

	"go.yaml.in/yaml/v3"
)

// Duration is a time.Duration written as a Go duration string ("750ms",
// "10m") in rule parameters.
type Duration time.Duration

// UnmarshalYAML parses a duration string.
func (d *Duration) UnmarshalYAML(n *yaml.Node) error {
	var s string
	if err := n.Decode(&s); err != nil {
		return err
	}
	v, err := time.ParseDuration(s)
	if err != nil {
		return err
	}
	*d = Duration(v)
	return nil
}

// D returns the duration.
func (d Duration) D() time.Duration { return time.Duration(d) }

// Ns returns the duration in nanoseconds.
func (d Duration) Ns() int64 { return int64(d) }
