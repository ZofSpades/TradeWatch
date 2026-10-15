//go:build integration

package api

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ZofSpades/tradewatch/internal/recording"
	"github.com/ZofSpades/tradewatch/internal/sim"
	"github.com/ZofSpades/tradewatch/internal/store"
	"github.com/ZofSpades/tradewatch/internal/testutil"
)

type runJSON struct {
	ID          int64  `json:"id"`
	Kind        string `json:"kind"`
	Status      string `json:"status"`
	Summary     string `json:"summary"`
	RequestedBy string `json:"requested_by"`
	RulesDigest string `json:"rules_digest"`
	Error       string `json:"error"`
	Types       []struct {
		AbuseType string `json:"abuse_type"`
		Episodes  int    `json:"episodes"`
		Alerts    int    `json:"alerts"`
	} `json:"types"`
}

func decodeRun(t *testing.T, body []byte) runJSON {
	t.Helper()
	var r runJSON
	if err := json.Unmarshal(body, &r); err != nil {
		t.Fatalf("run: %v: %s", err, body)
	}
	return r
}

func TestReplayIsScoredAndStoredAsAnEvaluationRun(t *testing.T) {
	dir := t.TempDir()
	cfg := sim.DefaultConfig()
	cfg.Seed, cfg.SessionID, cfg.Duration, cfg.ClosingWindow = 7, "rec-7", 15*time.Minute, 3*time.Minute
	cfg.Injection = sim.Injection{WashTrading: 6, Spoofing: 6, MomentumIgnition: 4, QuoteStuffing: 6, MarkingTheClose: 1}
	testutil.Record(t, filepath.Join(dir, "rec-7.twrec.gz"), cfg)
	// A session still being recorded, and a file that is not a recording.
	for name, data := range map[string]string{"rec-8.twrec.gz" + recording.Partial: "partial", "notes.txt": "x"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(data), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	var abandoned *store.EvaluationRun
	e := setup(t, func(srv *Server) {
		var err error
		abandoned, err = srv.store.StartEvaluation(t.Context(), store.EvalKindReplay, "old.twrec.gz", "admin", "d")
		if err != nil {
			t.Fatal(err)
		}
		if err := srv.SetReplays(t.Context(), dir, "../../rules"); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(srv.replays.wait)
	})
	admin, analyst := e.login("admin", adminPassword), e.login("analyst", analystPassword)

	code, body, _ := e.do("GET", fmt.Sprintf("/api/v1/evaluations/%d", abandoned.ID), analyst, nil)
	expect(t, "abandoned run", code, 200, body)
	if r := decodeRun(t, body); r.Status != store.EvalFailed || !strings.Contains(r.Error, "API stopped") {
		t.Errorf("the run a previous API left: %+v", r)
	}

	code, body, _ = e.do("GET", "/api/v1/recordings", analyst, nil)
	expect(t, "recordings as an analyst", code, 403, body)
	code, body, _ = e.do("GET", "/api/v1/recordings", admin, nil)
	expect(t, "recordings", code, 200, body)
	var list struct {
		Recordings []struct{ Name string } `json:"recordings"`
	}
	if err := json.Unmarshal(body, &list); err != nil || len(list.Recordings) != 1 || list.Recordings[0].Name != "rec-7.twrec.gz" {
		t.Errorf("recordings: %s", body)
	}

	for _, name := range []string{"../rec-7.twrec.gz", "rec-8.twrec.gz", "missing.twrec.gz", "notes.txt"} {
		code, body, _ = e.do("POST", "/api/v1/replays", admin, map[string]string{"recording": name})
		expect(t, "replay "+name, code, 404, body)
	}
	code, body, _ = e.do("POST", "/api/v1/replays", analyst, map[string]string{"recording": "rec-7.twrec.gz"})
	expect(t, "replay as an analyst", code, 403, body)

	e.srv.replays.mu.Lock()
	e.srv.replays.running = true
	e.srv.replays.mu.Unlock()
	code, body, _ = e.do("POST", "/api/v1/replays", admin, map[string]string{"recording": "rec-7.twrec.gz"})
	expect(t, "replay while one runs", code, 409, body)
	e.srv.replays.mu.Lock()
	e.srv.replays.running = false
	e.srv.replays.mu.Unlock()

	code, body, _ = e.do("POST", "/api/v1/replays", admin, map[string]string{"recording": "rec-7.twrec.gz"})
	expect(t, "replay", code, 202, body)
	run := decodeRun(t, body)
	if run.Kind != store.EvalKindReplay || run.Status != store.EvalRunning || run.RequestedBy != "admin" || run.RulesDigest == "" {
		t.Errorf("started run %+v", run)
	}
	deadline := time.Now().Add(2 * time.Minute)
	for run.Status == store.EvalRunning {
		if time.Now().After(deadline) {
			t.Fatal("the replay did not finish")
		}
		time.Sleep(200 * time.Millisecond)
		code, body, _ = e.do("GET", fmt.Sprintf("/api/v1/evaluations/%d", run.ID), analyst, nil)
		expect(t, "run", code, 200, body)
		run = decodeRun(t, body)
	}
	episodes := 0
	for _, ty := range run.Types {
		episodes += ty.Episodes
	}
	if run.Status != store.EvalDone || !strings.Contains(run.Summary, "events on") || len(run.Types) == 0 || episodes == 0 {
		t.Errorf("finished run %+v", run)
	}

	code, body, _ = e.do("GET", "/api/v1/evaluations?limit=5", analyst, nil)
	expect(t, "runs", code, 200, body)
	var runs struct {
		Runs []runJSON `json:"runs"`
	}
	if err := json.Unmarshal(body, &runs); err != nil || len(runs.Runs) != 2 || runs.Runs[0].ID != run.ID || runs.Runs[1].ID != abandoned.ID {
		t.Errorf("runs, newest first: %s", body)
	}
	for _, id := range []string{"abc", "0", "999999"} {
		code, body, _ = e.do("GET", "/api/v1/evaluations/"+id, analyst, nil)
		expect(t, "run "+id, code, 404, body)
	}
}

func TestReplaysAreUnavailableWithoutARecordingsDirectory(t *testing.T) {
	e := setup(t)
	admin := e.login("admin", adminPassword)
	code, body, _ := e.do("GET", "/api/v1/recordings", admin, nil)
	expect(t, "recordings", code, 503, body)
	code, body, _ = e.do("POST", "/api/v1/replays", admin, map[string]string{"recording": "rec-7.twrec.gz"})
	expect(t, "replay", code, 503, body)
	code, body, _ = e.do("GET", "/api/v1/evaluations", admin, nil)
	expect(t, "runs", code, 200, body)
}
