package detector

import (
	"fmt"
	"testing"
	"time"

	"github.com/ZofSpades/tradewatch/internal/config"
)

func TestCheckpointRoundTripAndSizeLimit(t *testing.T) {
	cp := Checkpoint{Version: 1, At: 7, LowWater: 100, Frontier: 250, Next: map[string]uint64{"live-001|QZAP": 42}}
	for i := 0; i < 200; i++ {
		cp.Closed = append(cp.Closed, fmt.Sprintf("live-20261013T090000Z-%03d|QZAP", i))
	}
	enc, err := cp.encode()
	if err != nil {
		t.Fatal(err)
	}
	if len(enc) > maxCheckpointBytes {
		t.Fatalf("encoded checkpoint is %d bytes", len(enc))
	}
	back, ok := decodeCheckpoint(enc)
	if !ok || back.LowWater != 100 || back.Frontier != 250 || back.Next["live-001|QZAP"] != 42 {
		t.Fatalf("decoded = %+v %v", back, ok)
	}
	if n := len(back.Closed); n == 0 || back.Closed[n-1] != cp.Closed[len(cp.Closed)-1] {
		t.Errorf("the most recently closed sessions must be kept, got %d ending %v", n, back.Closed[n-1:])
	}
	for _, bad := range []string{"", "member-1", `{"v":2}`, `{"v":1,`} {
		if _, ok := decodeCheckpoint(bad); ok {
			t.Errorf("decodeCheckpoint(%q) accepted foreign metadata", bad)
		}
	}
}

func TestCheckpointTooLargeWithoutClosedSessionsIsAnError(t *testing.T) {
	cp := Checkpoint{Version: 1, Next: map[string]uint64{}}
	for i := 0; i < 200; i++ {
		cp.Next[fmt.Sprintf("live-20261013T090000Z-%03d|QZAP", i)] = uint64(i)
	}
	if _, err := cp.encode(); err == nil {
		t.Fatal("an oversized checkpoint must not be truncated silently")
	}
}

func TestLoadConfig(t *testing.T) {
	cfg, err := LoadConfig(config.FromMap(map[string]string{
		"TW_DETECTOR_INSTANCE":        "det-a",
		"TW_DETECTOR_IDLE_AFTER":      "3m",
		"TW_DETECTOR_MAX_ORDERS":      "5000",
		"TW_DETECTOR_RULES_DIR":       "/etc/rules",
		"TW_KAFKA_TOPIC_PREFIX":       "it-",
		"TW_DETECTOR_COMMIT_INTERVAL": "250ms",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Instance != "det-a" || cfg.Engine.Instance != "det-a" || cfg.Engine.IdleAfter != 3*time.Minute ||
		cfg.Engine.MaxOrders != 5000 || cfg.RulesDir != "/etc/rules" || cfg.CommitInterval != 250*time.Millisecond ||
		cfg.Kafka.Prefix != "it-" || cfg.Group != "detector" {
		t.Errorf("config = %+v", cfg)
	}
	if _, err := LoadConfig(config.FromMap(map[string]string{"TW_DETECTOR_COMMIT_INTERVAL": "0s"})); err == nil {
		t.Error("a zero commit interval must be rejected")
	}
	if _, err := LoadConfig(config.FromMap(map[string]string{"TW_DETECTOR_IDLE_AFTER": "soon"})); err == nil {
		t.Error("an unparsable duration must be rejected")
	}
}
