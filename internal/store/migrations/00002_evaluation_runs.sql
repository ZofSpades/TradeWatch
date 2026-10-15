-- +goose Up

-- One row per evaluation: a `make eval` run over simulated sessions, or a
-- replay of a recorded session through the detectors. Both are scored
-- against the simulator's ground-truth labels.
CREATE TABLE evaluation_runs (
    id           bigserial PRIMARY KEY,
    kind         text NOT NULL CHECK (kind IN ('eval', 'replay')),
    status       text NOT NULL DEFAULT 'running' CHECK (status IN ('running', 'done', 'failed')),
    -- What was evaluated: the simulation plan, or the recording's file name.
    source       text NOT NULL,
    -- A line on what the run covered, written when it finishes.
    summary      text NOT NULL DEFAULT '',
    requested_by text NOT NULL,
    rules_digest text NOT NULL,
    started_at   timestamptz NOT NULL DEFAULT now(),
    finished_at  timestamptz,
    error        text NOT NULL DEFAULT '',
    -- The figures of each abuse type (eval.TypeSummary).
    types        jsonb NOT NULL DEFAULT '[]'
);

CREATE INDEX evaluation_runs_by_time ON evaluation_runs (started_at DESC, id DESC);

-- +goose Down
DROP TABLE evaluation_runs;
