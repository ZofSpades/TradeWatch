package store

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
)

// Kinds and states of evaluation runs.
const (
	EvalKindEval   = "eval"
	EvalKindReplay = "replay"

	EvalRunning = "running"
	EvalDone    = "done"
	EvalFailed  = "failed"
)

// EvaluationRun is one stored evaluation: a `make eval` run, or a replay of
// a recording, scored against the ground truth.
type EvaluationRun struct {
	ID          int64
	Kind        string
	Status      string
	Source      string
	Summary     string
	RequestedBy string
	RulesDigest string
	StartedAt   time.Time
	FinishedAt  *time.Time
	Error       string
	// Types holds the figures of each abuse type, as JSON.
	Types json.RawMessage
}

const evaluationColumns = `id, kind, status, source, summary, requested_by, rules_digest, started_at, finished_at, error, types`

func scanEvaluation(row pgx.Row) (*EvaluationRun, error) {
	var r EvaluationRun
	err := row.Scan(&r.ID, &r.Kind, &r.Status, &r.Source, &r.Summary, &r.RequestedBy, &r.RulesDigest,
		&r.StartedAt, &r.FinishedAt, &r.Error, &r.Types)
	return &r, err
}

// StartEvaluation records a run that has begun and returns it.
func (s *Store) StartEvaluation(ctx context.Context, kind, source, requestedBy, rulesDigest string) (*EvaluationRun, error) {
	return scanEvaluation(s.pool.QueryRow(ctx, `INSERT INTO evaluation_runs (kind, source, requested_by, rules_digest)
		VALUES ($1, $2, $3, $4) RETURNING `+evaluationColumns, kind, source, requestedBy, rulesDigest))
}

// FinishEvaluation records a run's outcome: its summary and figures, or the
// error that ended it.
func (s *Store) FinishEvaluation(ctx context.Context, id int64, summary string, types any, runErr error) error {
	status, msg := EvalDone, ""
	if runErr != nil {
		status, msg = EvalFailed, runErr.Error()
	}
	js, err := json.Marshal(types)
	if err != nil {
		return err
	}
	tag, err := s.pool.Exec(ctx, `UPDATE evaluation_runs SET status = $2, summary = $3, error = $4, types = $5, finished_at = now()
		WHERE id = $1 AND status = 'running'`, id, status, summary, msg, js)
	if err == nil && tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return err
}

// AbandonEvaluations marks the runs of a kind that are still running as
// failed with reason, and returns how many there were. A process that runs
// evaluations calls it when it starts, for runs its predecessor left.
func (s *Store) AbandonEvaluations(ctx context.Context, kind, reason string) (int64, error) {
	tag, err := s.pool.Exec(ctx, `UPDATE evaluation_runs SET status = 'failed', error = $2, finished_at = now()
		WHERE kind = $1 AND status = 'running'`, kind, reason)
	return tag.RowsAffected(), err
}

// Evaluations returns the most recent runs, newest first.
func (s *Store) Evaluations(ctx context.Context, limit int) ([]*EvaluationRun, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+evaluationColumns+` FROM evaluation_runs
		ORDER BY started_at DESC, id DESC LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []*EvaluationRun{}
	for rows.Next() {
		r, err := scanEvaluation(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// Evaluation returns one run, or ErrNotFound.
func (s *Store) Evaluation(ctx context.Context, id int64) (*EvaluationRun, error) {
	r, err := scanEvaluation(s.pool.QueryRow(ctx, `SELECT `+evaluationColumns+` FROM evaluation_runs WHERE id = $1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	return r, err
}
