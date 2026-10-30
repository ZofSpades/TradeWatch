package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ZofSpades/tradewatch/internal/detect"
	"github.com/ZofSpades/tradewatch/internal/eval"
	"github.com/ZofSpades/tradewatch/internal/replay"
	"github.com/ZofSpades/tradewatch/internal/rules"
	"github.com/ZofSpades/tradewatch/internal/store"
)

// An admin can replay one of marketsim's recordings through the detection
// engine with the current rules. The replay runs in the background, one at
// a time, and is stored as an evaluation run, scored against the
// recording's ground-truth labels like an evaluator run.

const recordingSuffix = ".twrec.gz"

var recordingName = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*\.twrec\.gz$`)

var (
	errReplayRunning = errors.New("a replay is already running")
	errNoRecording   = errors.New("no such recording")
)

// replayer runs replays in the background until its context ends.
type replayer struct {
	ctx      context.Context
	store    *store.Store
	dir      string
	rulesDir string
	log      *slog.Logger

	mu      sync.Mutex
	running bool
	wg      sync.WaitGroup
}

// SetReplays lets admins replay the recordings in dir with the rules in
// rulesDir, until ctx ends. Replays a previous API process left running are
// marked as failed: they ran in that process.
func (s *Server) SetReplays(ctx context.Context, dir, rulesDir string) error {
	if fi, err := os.Stat(dir); err != nil || !fi.IsDir() {
		return fmt.Errorf("recordings directory %q is not a directory", dir)
	}
	if _, err := s.store.AbandonEvaluations(ctx, store.EvalKindReplay, "the API stopped before the replay finished"); err != nil {
		return err
	}
	s.replays = &replayer{ctx: ctx, store: s.store, dir: dir, rulesDir: rulesDir, log: s.log}
	return nil
}

type recordingInfo struct {
	Name       string    `json:"name"`
	SizeBytes  int64     `json:"size_bytes"`
	ModifiedAt time.Time `json:"modified_at"`
}

// recordings lists the complete recordings, newest first. A session still
// being recorded ends in recording.Partial, so it is left out.
func (rp *replayer) recordings() ([]recordingInfo, error) {
	entries, err := os.ReadDir(rp.dir)
	if err != nil {
		return nil, err
	}
	out := []recordingInfo{}
	for _, e := range entries {
		if !e.Type().IsRegular() || !strings.HasSuffix(e.Name(), recordingSuffix) {
			continue
		}
		fi, err := e.Info()
		if err != nil {
			continue // removed meanwhile
		}
		out = append(out, recordingInfo{Name: e.Name(), SizeBytes: fi.Size(), ModifiedAt: fi.ModTime().UTC()})
	}
	slices.SortFunc(out, func(a, b recordingInfo) int { return b.ModifiedAt.Compare(a.ModifiedAt) })
	return out, nil
}

// start begins replaying the named recording for user.
func (rp *replayer) start(name, user string) (*store.EvaluationRun, error) {
	if !recordingName.MatchString(name) {
		return nil, errNoRecording
	}
	path := filepath.Join(rp.dir, name)
	if fi, err := os.Stat(path); err != nil || !fi.Mode().IsRegular() {
		return nil, errNoRecording
	}
	set, err := rules.Load(rp.rulesDir)
	if err == nil {
		err = detect.Validate(set)
	}
	if err != nil {
		return nil, fmt.Errorf("load the rules: %w", err)
	}
	rp.mu.Lock()
	defer rp.mu.Unlock()
	if rp.running {
		return nil, errReplayRunning
	}
	run, err := rp.store.StartEvaluation(rp.ctx, store.EvalKindReplay, name, user, set.Digest)
	if err != nil {
		return nil, err
	}
	rp.running = true
	rp.wg.Add(1)
	go rp.run(run.ID, path, set)
	return run, nil
}

func (rp *replayer) run(id int64, path string, set *rules.Set) {
	defer rp.wg.Done()
	out, err := replay.Run(rp.ctx, path, []*rules.Set{set})
	summary, types := "", []eval.TypeSummary{}
	switch {
	case err == nil:
		types = eval.ScoreAlerts("replay", out.Alerts[0], out.Labels).Summary()
		summary = fmt.Sprintf("%s on %s; %s; %s", count(out.Events, "event"), count(out.Symbols, "symbol"),
			count(len(out.Labels), "labelled episode"), count(len(out.Alerts[0]), "alert"))
	case rp.ctx.Err() != nil:
		err = errors.New("the API stopped before the replay finished")
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(rp.ctx), 10*time.Second)
	defer cancel()
	if ferr := rp.store.FinishEvaluation(ctx, id, summary, types, err); ferr != nil {
		rp.log.Warn("replay outcome not stored", "run", id, "err", ferr)
	}
	rp.mu.Lock()
	rp.running = false
	rp.mu.Unlock()
}

// wait blocks until no replay is running.
func (rp *replayer) wait() {
	if rp != nil {
		rp.wg.Wait()
	}
}

func (s *Server) listRecordings(w http.ResponseWriter, r *http.Request) {
	if s.replays == nil {
		problem(w, http.StatusServiceUnavailable, "replays are not configured (TW_API_RECORDINGS_DIR)")
		return
	}
	list, err := s.replays.recordings()
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"recordings": list})
}

func (s *Server) startReplay(w http.ResponseWriter, r *http.Request) {
	if s.replays == nil {
		problem(w, http.StatusServiceUnavailable, "replays are not configured (TW_API_RECORDINGS_DIR)")
		return
	}
	var req struct {
		Recording string `json:"recording"`
	}
	if !decode(w, r, &req) {
		return
	}
	run, err := s.replays.start(req.Recording, claimsOf(r).Subject)
	switch {
	case errors.Is(err, errNoRecording):
		problem(w, http.StatusNotFound, "no such recording")
	case errors.Is(err, errReplayRunning):
		problem(w, http.StatusConflict, "a replay is already running")
	case err != nil:
		s.internalError(w, r, err)
	default:
		writeJSON(w, http.StatusAccepted, evaluationJSON(run))
	}
}

// evaluationRun is an evaluation run as the API serves it.
type evaluationRun struct {
	ID          int64           `json:"id"`
	Kind        string          `json:"kind"`
	Status      string          `json:"status"`
	Source      string          `json:"source"`
	Summary     string          `json:"summary,omitempty"`
	RequestedBy string          `json:"requested_by"`
	RulesDigest string          `json:"rules_digest"`
	StartedAt   time.Time       `json:"started_at"`
	FinishedAt  *time.Time      `json:"finished_at,omitempty"`
	Error       string          `json:"error,omitempty"`
	Types       json.RawMessage `json:"types"`
}

func evaluationJSON(r *store.EvaluationRun) evaluationRun {
	return evaluationRun{
		ID: r.ID, Kind: r.Kind, Status: r.Status, Source: r.Source, Summary: r.Summary, RequestedBy: r.RequestedBy,
		RulesDigest: r.RulesDigest, StartedAt: r.StartedAt.UTC(), FinishedAt: r.FinishedAt, Error: r.Error, Types: r.Types,
	}
}

func (s *Server) listEvaluations(w http.ResponseWriter, r *http.Request) {
	limit, ok := intParam(r, "limit", 20, 1, 100)
	if !ok {
		problem(w, http.StatusBadRequest, "limit must be an integer from 1 to 100")
		return
	}
	runs, err := s.store.Evaluations(r.Context(), limit)
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	out := make([]evaluationRun, len(runs))
	for i, run := range runs {
		out[i] = evaluationJSON(run)
	}
	writeJSON(w, http.StatusOK, map[string]any{"runs": out})
}

func (s *Server) getEvaluation(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || id < 1 {
		problem(w, http.StatusNotFound, "no such evaluation run")
		return
	}
	run, err := s.store.Evaluation(r.Context(), id)
	switch {
	case errors.Is(err, store.ErrNotFound):
		problem(w, http.StatusNotFound, "no such evaluation run")
	case err != nil:
		s.internalError(w, r, err)
	default:
		writeJSON(w, http.StatusOK, evaluationJSON(run))
	}
}

// count is "1 alert" or "<n> alerts".
func count(n int, noun string) string {
	if n == 1 {
		return "1 " + noun
	}
	return strconv.Itoa(n) + " " + noun + "s"
}
