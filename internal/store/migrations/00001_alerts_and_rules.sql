-- +goose Up

-- One row per alert, keyed by the detector's deterministic alert key, so a
-- redelivered alert updates delivery bookkeeping instead of adding a row.
-- Event-time fields keep the protobuf's nanosecond integers; wall-clock
-- bookkeeping uses timestamptz.
CREATE TABLE alerts (
    alert_key            text PRIMARY KEY,
    session_id           text NOT NULL,
    symbol               text NOT NULL,
    abuse_type           text NOT NULL,
    detector             text NOT NULL,
    rule_id              text NOT NULL,
    rule_version         integer NOT NULL,
    rule_digest          text NOT NULL,
    taxonomy_code        text NOT NULL,
    severity             text NOT NULL,
    score                double precision NOT NULL,
    account_ids          text[] NOT NULL,
    beneficial_owner_ids text[] NOT NULL,
    window_start_ns      bigint NOT NULL,
    window_end_ns        bigint NOT NULL,
    detected_at_ns       bigint NOT NULL,
    emitted_at_ns        bigint NOT NULL,
    explanation          text NOT NULL,
    -- The alert as first received (deterministic protobuf encoding).
    payload              bytea NOT NULL,
    status               text NOT NULL DEFAULT 'open'
                         CHECK (status IN ('open', 'triaged', 'escalated', 'dismissed')),
    -- How many times the alert was delivered; above 1 means at-least-once
    -- delivery produced duplicates that the key absorbed.
    deliveries           integer NOT NULL DEFAULT 1,
    first_seen_at        timestamptz NOT NULL DEFAULT now(),
    last_seen_at         timestamptz NOT NULL DEFAULT now(),
    status_changed_at    timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX alerts_by_time ON alerts (detected_at_ns DESC, alert_key DESC);
CREATE INDEX alerts_by_status ON alerts (status, detected_at_ns DESC, alert_key DESC);
CREATE INDEX alerts_by_symbol ON alerts (symbol, detected_at_ns DESC, alert_key DESC);
CREATE INDEX alerts_by_session ON alerts (session_id);
CREATE INDEX alerts_by_arrival ON alerts (first_seen_at DESC, alert_key DESC);
CREATE INDEX alerts_by_account ON alerts USING gin (account_ids);

-- Append-only history of every alert: when it was raised, every status
-- change and every analyst comment, with who did it.
CREATE TABLE alert_audit (
    id          bigserial PRIMARY KEY,
    alert_key   text NOT NULL REFERENCES alerts (alert_key),
    at          timestamptz NOT NULL DEFAULT now(),
    actor       text NOT NULL,
    action      text NOT NULL CHECK (action IN ('raised', 'transition', 'comment')),
    from_status text,
    to_status   text,
    note        text NOT NULL DEFAULT ''
);

CREATE INDEX alert_audit_by_alert ON alert_audit (alert_key, id);

-- Rule governance, fed from the rules_audit topic. A rule version is
-- identified by its source digest; two digests with the same rule ID and
-- version number mean the rule was edited without a version bump.
CREATE TABLE rule_versions (
    rule_id       text NOT NULL,
    digest        text NOT NULL,
    version       integer NOT NULL,
    detector      text NOT NULL,
    name          text NOT NULL,
    taxonomy_code text NOT NULL,
    enabled       boolean NOT NULL,
    source        text NOT NULL,
    first_seen_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (rule_id, digest)
);

CREATE TABLE rule_sets (
    digest        text PRIMARY KEY,
    first_seen_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE rule_set_members (
    ruleset_digest text NOT NULL REFERENCES rule_sets (digest),
    rule_id        text NOT NULL,
    rule_digest    text NOT NULL,
    PRIMARY KEY (ruleset_digest, rule_id),
    FOREIGN KEY (rule_id, rule_digest) REFERENCES rule_versions (rule_id, digest)
);

-- Every load of a rule set by a detector instance.
CREATE TABLE rule_set_loads (
    ruleset_digest text NOT NULL REFERENCES rule_sets (digest),
    loaded_by      text NOT NULL,
    loaded_at      timestamptz NOT NULL,
    PRIMARY KEY (ruleset_digest, loaded_by, loaded_at)
);

CREATE INDEX rule_set_loads_by_time ON rule_set_loads (loaded_at DESC);

-- +goose Down
DROP TABLE rule_set_loads;
DROP TABLE rule_set_members;
DROP TABLE rule_sets;
DROP TABLE rule_versions;
DROP TABLE alert_audit;
DROP TABLE alerts;
