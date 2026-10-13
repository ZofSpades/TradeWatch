package detect

import (
	"fmt"
	"io"
	"slices"
	"time"

	"github.com/ZofSpades/tradewatch/internal/rules"
)

// Detector looks for one kind of pattern in one symbol's event stream. An
// instance is created per (session, symbol) and receives every event in
// sequence order, after the shared Market view has been updated with it.
//
// To keep alerts identical with and without a restart, a detector's
// decisions may only depend on events inside its declared Horizon of event
// time: windows are measured in event time, and nothing older than the
// horizon may influence a result.
type Detector interface {
	OnEvent(c *Context)
}

// Params are a detector's rule parameters.
type Params interface {
	// Validate checks relationships between parameters that the rule
	// schema cannot express.
	Validate() error
	// Horizon is how much event-time history the detector needs.
	Horizon() time.Duration
}

// Kind describes a detector implementation.
type Kind struct {
	Name string
	// NewParams returns a parameter struct to decode a rule into.
	NewParams func() Params
	// New returns detector state for one symbol.
	New func() Detector
	// Samples are representative explanation inputs. Every rule's
	// explanation template is rendered with each of them when the rule is
	// loaded, so a template that refers to a feature the detector does not
	// produce is rejected up front rather than at alert time.
	Samples []ExplanationData
}

var registry = map[string]Kind{}

// Register adds a detector implementation. It panics on duplicates.
func Register(k Kind) {
	if _, dup := registry[k.Name]; dup {
		panic("detect: duplicate detector " + k.Name)
	}
	registry[k.Name] = k
}

// Kinds lists the registered detector names.
func Kinds() []string {
	names := make([]string, 0, len(registry))
	for n := range registry {
		names = append(names, n)
	}
	slices.Sort(names)
	return names
}

// boundRule is an enabled rule with its decoded parameters.
type boundRule struct {
	rule   *rules.Rule
	kind   Kind
	params Params
}

// bind decodes and checks every enabled rule of a set.
func bind(set *rules.Set) ([]*boundRule, error) {
	var out []*boundRule
	for _, r := range set.Enabled() {
		k, ok := registry[r.Detector]
		if !ok {
			return nil, fmt.Errorf("rule %s: no detector %q", r.ID, r.Detector)
		}
		p := k.NewParams()
		if err := r.DecodeParams(p); err != nil {
			return nil, err
		}
		if err := p.Validate(); err != nil {
			return nil, fmt.Errorf("rule %s: %w", r.ID, err)
		}
		for i, s := range k.Samples {
			if err := r.Explanation.Execute(io.Discard, s); err != nil {
				return nil, fmt.Errorf("rule %s: explanation does not render for sample %d: %w", r.ID, i, err)
			}
		}
		out = append(out, &boundRule{rule: r, kind: k, params: p})
	}
	return out, nil
}

// Validate reports whether every enabled rule of the set can be used: its
// detector exists, its parameters are consistent and its explanation
// renders.
func Validate(set *rules.Set) error {
	_, err := bind(set)
	return err
}
