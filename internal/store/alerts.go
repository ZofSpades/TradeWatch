package store

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/ZofSpades/tradewatch/internal/events"
	"github.com/ZofSpades/tradewatch/internal/events/twpb"
)

// Status is an alert's place in the analyst workflow.
type Status string

// The analyst workflow: an open alert is triaged, and a triaged alert is
// either escalated or dismissed. Escalated and dismissed are final.
const (
	StatusOpen      Status = "open"
	StatusTriaged   Status = "triaged"
	StatusEscalated Status = "escalated"
	StatusDismissed Status = "dismissed"
)

// Statuses lists every status in workflow order.
var Statuses = []Status{StatusOpen, StatusTriaged, StatusEscalated, StatusDismissed}

var lifecycle = map[Status][]Status{
	StatusOpen:    {StatusTriaged},
	StatusTriaged: {StatusEscalated, StatusDismissed},
}

// Valid reports whether s is a known status.
func (s Status) Valid() bool { return slices.Contains(Statuses, s) }

// CanTransition reports whether the workflow allows moving from one status
// to another.
func CanTransition(from, to Status) bool { return slices.Contains(lifecycle[from], to) }

// NeedsNote reports whether moving an alert to s must record a reason:
// closing an alert either way has to say why.
func NeedsNote(s Status) bool { return s == StatusEscalated || s == StatusDismissed }

// MaxNoteLength bounds notes and comments, in bytes.
const MaxNoteLength = 4000

var (
	// ErrInvalidTransition is returned (wrapped in a TransitionError) when
	// the workflow does not allow a status change.
	ErrInvalidTransition = errors.New("store: status change not allowed")
	// ErrNoteRequired is returned when a note is missing where one is
	// required, or longer than MaxNoteLength.
	ErrNoteRequired = errors.New("store: a note of at most 4000 bytes is required")
	// ErrBadCursor is returned for a page cursor that List did not produce.
	ErrBadCursor = errors.New("store: invalid cursor")
)

// TransitionError describes a status change the workflow refused.
type TransitionError struct{ From, To Status }

func (e *TransitionError) Error() string {
	return fmt.Sprintf("store: an alert cannot move from %s to %s", e.From, e.To)
}

// Is makes errors.Is(err, ErrInvalidTransition) match.
func (e *TransitionError) Is(target error) bool { return target == ErrInvalidTransition }

// Alert is an alert as stored, without its evidence.
type Alert struct {
	Key             string
	SessionID       string
	Symbol          string
	AbuseType       string // for example "wash_trading"
	Detector        string
	RuleID          string
	RuleVersion     int
	RuleDigest      string
	TaxonomyCode    string
	Severity        string // for example "high"
	Score           float64
	AccountIDs      []string
	OwnerIDs        []string
	WindowStartNs   int64
	WindowEndNs     int64
	DetectedAtNs    int64
	EmittedAtNs     int64
	Explanation     string
	Status          Status
	Deliveries      int
	FirstSeenAt     time.Time
	LastSeenAt      time.Time
	StatusChangedAt time.Time
}

// AbuseTypeName is the stored name of an abuse type, for example
// "wash_trading" for ABUSE_TYPE_WASH_TRADING.
func AbuseTypeName(t twpb.AbuseType) string {
	return strings.ToLower(strings.TrimPrefix(t.String(), "ABUSE_TYPE_"))
}

// SeverityName is the stored name of a severity, for example "high".
func SeverityName(s twpb.Severity) string {
	return strings.ToLower(strings.TrimPrefix(s.String(), "SEVERITY_"))
}

const alertColumns = `alert_key, session_id, symbol, abuse_type, detector, rule_id, rule_version, rule_digest,
	taxonomy_code, severity, score, account_ids, beneficial_owner_ids, window_start_ns, window_end_ns,
	detected_at_ns, emitted_at_ns, explanation, status, deliveries, first_seen_at, last_seen_at, status_changed_at`

func scanAlert(row pgx.Row) (*Alert, error) {
	var a Alert
	err := row.Scan(&a.Key, &a.SessionID, &a.Symbol, &a.AbuseType, &a.Detector, &a.RuleID, &a.RuleVersion,
		&a.RuleDigest, &a.TaxonomyCode, &a.Severity, &a.Score, &a.AccountIDs, &a.OwnerIDs, &a.WindowStartNs,
		&a.WindowEndNs, &a.DetectedAtNs, &a.EmittedAtNs, &a.Explanation, &a.Status, &a.Deliveries,
		&a.FirstSeenAt, &a.LastSeenAt, &a.StatusChangedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return &a, nil
}

// detectorActor names the detector instance that raised an alert in the
// audit trail.
func detectorActor(a *twpb.Alert) string {
	if a.GetDetectorInstance() == "" {
		return "detector"
	}
	return "detector/" + a.GetDetectorInstance()
}

// UpsertAlerts stores a batch of alerts in one transaction. An alert whose
// key is already stored only has its delivery count and last-seen time
// updated: the first delivery's content stands, and its analyst status is
// never touched. It returns the alerts stored for the first time, in input
// order, and the number of deliveries that were duplicates.
func (s *Store) UpsertAlerts(ctx context.Context, alerts []*twpb.Alert) (raised []*Alert, duplicates int, err error) {
	if len(alerts) == 0 {
		return nil, 0, nil
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, 0, err
	}
	defer tx.Rollback(ctx) // a no-op once committed

	inserts := &pgx.Batch{}
	for _, a := range alerts {
		payload, err := events.Marshal(a)
		if err != nil {
			return nil, 0, err
		}
		inserts.Queue(`INSERT INTO alerts (alert_key, session_id, symbol, abuse_type, detector, rule_id, rule_version,
				rule_digest, taxonomy_code, severity, score, account_ids, beneficial_owner_ids, window_start_ns,
				window_end_ns, detected_at_ns, emitted_at_ns, explanation, payload)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)
			ON CONFLICT (alert_key) DO NOTHING
			RETURNING `+alertColumns,
			a.GetAlertKey(), a.GetSessionId(), a.GetSymbol(), AbuseTypeName(a.GetAbuseType()), a.GetDetector(),
			a.GetRuleId(), int64(a.GetRuleVersion()), a.GetRuleDigest(), a.GetTaxonomyCode(),
			SeverityName(a.GetSeverity()), a.GetScore(), nonNil(a.GetAccountIds()), nonNil(a.GetBeneficialOwnerIds()),
			a.GetWindowStartNs(), a.GetWindowEndNs(), a.GetDetectedAtNs(), a.GetEmittedAtNs(), a.GetExplanation(), payload)
	}
	results := tx.SendBatch(ctx, inserts)
	follow := &pgx.Batch{}
	for _, a := range alerts {
		rec, err := scanAlert(results.QueryRow())
		switch {
		case errors.Is(err, ErrNotFound): // the key was already stored
			duplicates++
			follow.Queue(`UPDATE alerts SET deliveries = deliveries + 1, last_seen_at = now() WHERE alert_key = $1`,
				a.GetAlertKey())
		case err != nil:
			results.Close()
			return nil, 0, err
		default:
			raised = append(raised, rec)
			follow.Queue(`INSERT INTO alert_audit (alert_key, actor, action, to_status) VALUES ($1, $2, 'raised', 'open')`,
				a.GetAlertKey(), detectorActor(a))
		}
	}
	if err := results.Close(); err != nil {
		return nil, 0, err
	}
	if err := tx.SendBatch(ctx, follow).Close(); err != nil {
		return nil, 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, 0, err
	}
	return raised, duplicates, nil
}

func nonNil(xs []string) []string {
	if xs == nil {
		return []string{}
	}
	return xs
}

// Get returns an alert and the full alert as raised, evidence included.
func (s *Store) Get(ctx context.Context, key string) (*Alert, *twpb.Alert, error) {
	var payload []byte
	row := s.pool.QueryRow(ctx, `SELECT `+alertColumns+`, payload FROM alerts WHERE alert_key = $1`, key)
	var a Alert
	err := row.Scan(&a.Key, &a.SessionID, &a.Symbol, &a.AbuseType, &a.Detector, &a.RuleID, &a.RuleVersion,
		&a.RuleDigest, &a.TaxonomyCode, &a.Severity, &a.Score, &a.AccountIDs, &a.OwnerIDs, &a.WindowStartNs,
		&a.WindowEndNs, &a.DetectedAtNs, &a.EmittedAtNs, &a.Explanation, &a.Status, &a.Deliveries,
		&a.FirstSeenAt, &a.LastSeenAt, &a.StatusChangedAt, &payload)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil, ErrNotFound
	}
	if err != nil {
		return nil, nil, err
	}
	var full twpb.Alert
	if err := events.Unmarshal(payload, &full); err != nil {
		return nil, nil, fmt.Errorf("store: decode alert %s: %w", key, err)
	}
	return &a, &full, nil
}

// Filter selects alerts for List. Zero fields do not filter.
type Filter struct {
	Status    Status
	AbuseType string
	Symbol    string
	Account   string
	Session   string
	MinScore  float64
	// Limit is the page size: 50 by default, at most 500.
	Limit int
	// Cursor continues from a previous page.
	Cursor string
}

// List returns alerts newest first by detection time, and the cursor of the
// next page, which is empty on the last page.
func (s *Store) List(ctx context.Context, f Filter) ([]*Alert, string, error) {
	limit := f.Limit
	if limit <= 0 {
		limit = 50
	}
	limit = min(limit, 500)
	var where []string
	var args []any
	arg := func(v any) string {
		args = append(args, v)
		return "$" + strconv.Itoa(len(args))
	}
	if f.Status != "" {
		where = append(where, "status = "+arg(f.Status))
	}
	if f.AbuseType != "" {
		where = append(where, "abuse_type = "+arg(f.AbuseType))
	}
	if f.Symbol != "" {
		where = append(where, "symbol = "+arg(f.Symbol))
	}
	if f.Account != "" {
		where = append(where, arg(f.Account)+" = ANY (account_ids)")
	}
	if f.Session != "" {
		where = append(where, "session_id = "+arg(f.Session))
	}
	if f.MinScore > 0 {
		where = append(where, "score >= "+arg(f.MinScore))
	}
	if f.Cursor != "" {
		t, key, err := decodeCursor(f.Cursor)
		if err != nil {
			return nil, "", err
		}
		where = append(where, fmt.Sprintf("(detected_at_ns, alert_key) < (%s, %s)", arg(t), arg(key)))
	}
	q := `SELECT ` + alertColumns + ` FROM alerts`
	if len(where) > 0 {
		q += " WHERE " + strings.Join(where, " AND ")
	}
	q += " ORDER BY detected_at_ns DESC, alert_key DESC LIMIT " + arg(limit+1)
	rows, err := s.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()
	var out []*Alert
	for rows.Next() {
		a, err := scanAlert(rows)
		if err != nil {
			return nil, "", err
		}
		out = append(out, a)
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}
	next := ""
	if len(out) > limit {
		out = out[:limit]
		last := out[limit-1]
		next = encodeCursor(last.DetectedAtNs, last.Key)
	}
	return out, next, nil
}

func encodeCursor(t int64, key string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(strconv.FormatInt(t, 10) + ":" + key))
}

func decodeCursor(c string) (int64, string, error) {
	b, err := base64.RawURLEncoding.DecodeString(c)
	if err != nil {
		return 0, "", ErrBadCursor
	}
	ts, key, ok := strings.Cut(string(b), ":")
	t, err := strconv.ParseInt(ts, 10, 64)
	if !ok || err != nil || key == "" {
		return 0, "", ErrBadCursor
	}
	return t, key, nil
}

// Recent returns the alerts stored most recently, newest first.
func (s *Store) Recent(ctx context.Context, n int) ([]*Alert, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+alertColumns+` FROM alerts ORDER BY first_seen_at DESC, alert_key DESC LIMIT $1`, n)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Alert
	for rows.Next() {
		a, err := scanAlert(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// AuditEntry is one line of an alert's history.
type AuditEntry struct {
	ID     int64
	At     time.Time
	Actor  string
	Action string // raised, transition or comment
	From   Status // empty unless Action is transition
	To     Status // empty for comments
	Note   string
}

// Audit returns an alert's history, oldest first.
func (s *Store) Audit(ctx context.Context, key string) ([]AuditEntry, error) {
	var exists bool
	if err := s.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM alerts WHERE alert_key = $1)`, key).Scan(&exists); err != nil {
		return nil, err
	}
	if !exists {
		return nil, ErrNotFound
	}
	rows, err := s.pool.Query(ctx, `SELECT id, at, actor, action, coalesce(from_status, ''), coalesce(to_status, ''), note
		FROM alert_audit WHERE alert_key = $1 ORDER BY id`, key)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []AuditEntry
	for rows.Next() {
		var e AuditEntry
		if err := rows.Scan(&e.ID, &e.At, &e.Actor, &e.Action, &e.From, &e.To, &e.Note); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

func checkNote(note string, required bool) error {
	if len(note) > MaxNoteLength || required && strings.TrimSpace(note) == "" {
		return ErrNoteRequired
	}
	return nil
}

// Transition moves an alert through the workflow and records the change in
// its history, in one transaction. Concurrent changes to one alert are
// serialized: the later one sees the earlier one's status and fails with a
// TransitionError if the workflow no longer allows it.
func (s *Store) Transition(ctx context.Context, key string, to Status, actor, note string) (*Alert, error) {
	if err := checkNote(note, NeedsNote(to)); err != nil {
		return nil, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) // a no-op once committed
	var from Status
	err = tx.QueryRow(ctx, `SELECT status FROM alerts WHERE alert_key = $1 FOR UPDATE`, key).Scan(&from)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if !CanTransition(from, to) {
		return nil, &TransitionError{From: from, To: to}
	}
	a, err := scanAlert(tx.QueryRow(ctx, `UPDATE alerts SET status = $2, status_changed_at = now()
		WHERE alert_key = $1 RETURNING `+alertColumns, key, to))
	if err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO alert_audit (alert_key, actor, action, from_status, to_status, note)
		VALUES ($1, $2, 'transition', $3, $4, $5)`, key, actor, from, to, note); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return a, nil
}

// Comment adds an analyst's note to an alert's history.
func (s *Store) Comment(ctx context.Context, key, actor, note string) (AuditEntry, error) {
	if err := checkNote(note, true); err != nil {
		return AuditEntry{}, err
	}
	e := AuditEntry{Actor: actor, Action: "comment", Note: note}
	err := s.pool.QueryRow(ctx, `INSERT INTO alert_audit (alert_key, actor, action, note)
		SELECT alert_key, $2, 'comment', $3 FROM alerts WHERE alert_key = $1
		RETURNING id, at`, key, actor, note).Scan(&e.ID, &e.At)
	if errors.Is(err, pgx.ErrNoRows) {
		return AuditEntry{}, ErrNotFound
	}
	return e, err
}

// Counts are alert totals.
type Counts struct {
	Total    int64
	ByStatus map[Status]int64
	ByType   map[string]int64
	BySymbol map[string]int64
}

// Count returns alert totals by status, abuse type and symbol.
func (s *Store) Count(ctx context.Context) (Counts, error) {
	c := Counts{ByStatus: map[Status]int64{}, ByType: map[string]int64{}, BySymbol: map[string]int64{}}
	rows, err := s.pool.Query(ctx, `SELECT status, abuse_type, symbol, count(*) FROM alerts GROUP BY 1, 2, 3`)
	if err != nil {
		return c, err
	}
	defer rows.Close()
	for rows.Next() {
		var st Status
		var typ, sym string
		var n int64
		if err := rows.Scan(&st, &typ, &sym, &n); err != nil {
			return c, err
		}
		c.Total += n
		c.ByStatus[st] += n
		c.ByType[typ] += n
		c.BySymbol[sym] += n
	}
	return c, rows.Err()
}
