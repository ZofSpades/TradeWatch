package store

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/ZofSpades/tradewatch/internal/events/twpb"
)

// RuleVersion is one version of a rule as some detector loaded it.
type RuleVersion struct {
	RuleID       string
	Version      int
	Digest       string
	Detector     string
	Name         string
	TaxonomyCode string
	Enabled      bool
	Source       string
	FirstSeenAt  time.Time
	// VersionReused is set when another digest carries the same rule ID and
	// version number: the rule was edited without bumping its version.
	VersionReused bool
}

// RuleSetLoad is one load of a rule set by a detector instance.
type RuleSetLoad struct {
	Digest   string
	LoadedBy string
	LoadedAt time.Time
}

// RecordRuleSet stores a rule set a detector loaded, with its rules and the
// load itself. Recording the same snapshot again changes nothing.
func (s *Store) RecordRuleSet(ctx context.Context, snap *twpb.RuleSetSnapshot) error {
	b := &pgx.Batch{}
	b.Queue(`INSERT INTO rule_sets (digest) VALUES ($1) ON CONFLICT DO NOTHING`, snap.GetRulesetDigest())
	for _, r := range snap.GetRules() {
		b.Queue(`INSERT INTO rule_versions (rule_id, digest, version, detector, name, taxonomy_code, enabled, source)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8) ON CONFLICT DO NOTHING`,
			r.GetRuleId(), r.GetDigest(), int64(r.GetVersion()), r.GetDetector(), r.GetName(), r.GetTaxonomyCode(),
			r.GetEnabled(), strings.ToValidUTF8(string(r.GetSource()), "�"))
		b.Queue(`INSERT INTO rule_set_members (ruleset_digest, rule_id, rule_digest) VALUES ($1, $2, $3)
			ON CONFLICT DO NOTHING`, snap.GetRulesetDigest(), r.GetRuleId(), r.GetDigest())
	}
	b.Queue(`INSERT INTO rule_set_loads (ruleset_digest, loaded_by, loaded_at) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING`,
		snap.GetRulesetDigest(), snap.GetLoadedBy(), time.Unix(0, snap.GetLoadedAtNs()).UTC())
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) // a no-op once committed
	if err := tx.SendBatch(ctx, b).Close(); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

const ruleColumns = `v.rule_id, v.version, v.digest, v.detector, v.name, v.taxonomy_code, v.enabled, v.source,
	v.first_seen_at, EXISTS (SELECT 1 FROM rule_versions o
		WHERE o.rule_id = v.rule_id AND o.version = v.version AND o.digest <> v.digest)`

func scanRules(rows pgx.Rows) ([]RuleVersion, error) {
	defer rows.Close()
	var out []RuleVersion
	for rows.Next() {
		var r RuleVersion
		if err := rows.Scan(&r.RuleID, &r.Version, &r.Digest, &r.Detector, &r.Name, &r.TaxonomyCode, &r.Enabled,
			&r.Source, &r.FirstSeenAt, &r.VersionReused); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// CurrentRules returns the most recent rule-set load and that set's rules
// in rule ID order, or ErrNotFound before any detector has loaded rules.
func (s *Store) CurrentRules(ctx context.Context) (RuleSetLoad, []RuleVersion, error) {
	var load RuleSetLoad
	err := s.pool.QueryRow(ctx, `SELECT ruleset_digest, loaded_by, loaded_at FROM rule_set_loads
		ORDER BY loaded_at DESC, loaded_by DESC LIMIT 1`).Scan(&load.Digest, &load.LoadedBy, &load.LoadedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return load, nil, ErrNotFound
	}
	if err != nil {
		return load, nil, err
	}
	rows, err := s.pool.Query(ctx, `SELECT `+ruleColumns+` FROM rule_set_members m
		JOIN rule_versions v ON v.rule_id = m.rule_id AND v.digest = m.rule_digest
		WHERE m.ruleset_digest = $1 ORDER BY v.rule_id`, load.Digest)
	if err != nil {
		return load, nil, err
	}
	rules, err := scanRules(rows)
	return load, rules, err
}

// RuleHistory returns every version of a rule that a detector has loaded,
// newest first, or ErrNotFound for an unknown rule.
func (s *Store) RuleHistory(ctx context.Context, ruleID string) ([]RuleVersion, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+ruleColumns+` FROM rule_versions v WHERE v.rule_id = $1
		ORDER BY v.version DESC, v.first_seen_at DESC`, ruleID)
	if err != nil {
		return nil, err
	}
	rules, err := scanRules(rows)
	if err == nil && len(rules) == 0 {
		return nil, ErrNotFound
	}
	return rules, err
}
