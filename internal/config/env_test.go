package config

import (
	"strings"
	"testing"
	"time"
)

func TestEnvTypedValues(t *testing.T) {
	env := FromMap(map[string]string{
		"S":     "  hello ",
		"I":     "42",
		"U":     "18446744073709551615",
		"F":     "0.25",
		"B":     "true",
		"D":     "1500ms",
		"L":     " a, b ,,c ",
		"EMPTY": "   ",
	})

	if got := env.String("S", "x"); got != "hello" {
		t.Errorf("String = %q", got)
	}
	if got := env.String("EMPTY", "def"); got != "def" {
		t.Errorf("String(empty) = %q, want default", got)
	}
	if got := env.Int("I", 0); got != 42 {
		t.Errorf("Int = %d", got)
	}
	if got := env.Uint64("U", 0); got != 18446744073709551615 {
		t.Errorf("Uint64 = %d", got)
	}
	if got := env.Float("F", 0); got != 0.25 {
		t.Errorf("Float = %v", got)
	}
	if got := env.Bool("B", false); !got {
		t.Errorf("Bool = %v", got)
	}
	if got := env.Duration("D", 0); got != 1500*time.Millisecond {
		t.Errorf("Duration = %v", got)
	}
	if got := strings.Join(env.List("L", nil), "|"); got != "a|b|c" {
		t.Errorf("List = %q", got)
	}
	if got := env.Int("MISSING", 7); got != 7 {
		t.Errorf("Int(missing) = %d, want default", got)
	}
	if err := env.Err(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestEnvAccumulatesErrors(t *testing.T) {
	env := FromMap(map[string]string{"I": "x", "D": "soon", "B": "maybe"})
	env.Int("I", 1)
	env.Duration("D", time.Second)
	env.Bool("B", false)
	env.Required("NEEDED")

	err := env.Err()
	if err == nil {
		t.Fatal("expected errors")
	}
	for _, key := range []string{"I:", "D:", "B:", "NEEDED:"} {
		if !strings.Contains(err.Error(), key) {
			t.Errorf("error %q does not mention %s", err, key)
		}
	}
}
