package api

import (
	"errors"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/ZofSpades/tradewatch/internal/auth"
	"github.com/ZofSpades/tradewatch/internal/events/twpb"
	"github.com/ZofSpades/tradewatch/internal/live"
	"github.com/ZofSpades/tradewatch/internal/store"
)

func (s *Server) login(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	if !decode(w, r, &req) {
		return
	}
	if req.Username == "" || req.Password == "" {
		problem(w, http.StatusBadRequest, "username and password are required")
		return
	}
	token, claims, err := s.auth.Login(req.Username, req.Password)
	switch {
	case errors.Is(err, auth.ErrTooManyAttempts):
		s.m.logins.WithLabelValues("locked").Inc()
		s.log.Warn("login refused: too many failures", "username", req.Username, "remote", r.RemoteAddr)
		w.Header().Set("Retry-After", "900")
		problem(w, http.StatusTooManyRequests, "too many failed logins for this username; try again later")
	case errors.Is(err, auth.ErrInvalidCredentials):
		s.m.logins.WithLabelValues("failed").Inc()
		s.log.Warn("login failed", "username", req.Username, "remote", r.RemoteAddr)
		problem(w, http.StatusUnauthorized, "invalid username or password")
	case err != nil:
		s.internalError(w, r, err)
	default:
		s.m.logins.WithLabelValues("ok").Inc()
		s.log.Info("login", "username", claims.Subject, "role", claims.Role)
		writeJSON(w, http.StatusOK, map[string]any{
			"access_token": token, "token_type": "Bearer", "expires_in": int(s.auth.TTL().Seconds()),
			"username": claims.Subject, "role": claims.Role,
		})
	}
}

func (s *Server) me(w http.ResponseWriter, r *http.Request) {
	c := claimsOf(r)
	writeJSON(w, http.StatusOK, map[string]any{"username": c.Subject, "role": c.Role, "expires_at": c.ExpiresAt.UTC()})
}

var abuseTypes = []string{"wash_trading", "spoofing", "momentum_ignition", "quote_stuffing", "marking_the_close"}

// intParam parses an optional integer query parameter within [lo, hi].
func intParam(r *http.Request, name string, def, lo, hi int) (int, bool) {
	v := r.URL.Query().Get(name)
	if v == "" {
		return def, true
	}
	n, err := strconv.Atoi(v)
	return n, err == nil && n >= lo && n <= hi
}

func (s *Server) listAlerts(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	f := store.Filter{
		Status: store.Status(q.Get("status")), AbuseType: q.Get("abuse_type"), Symbol: q.Get("symbol"),
		Account: q.Get("account"), Session: q.Get("session"), Cursor: q.Get("cursor"),
	}
	if f.Status != "" && !f.Status.Valid() {
		problem(w, http.StatusBadRequest, "unknown status "+strconv.Quote(string(f.Status)))
		return
	}
	if f.AbuseType != "" && !slices.Contains(abuseTypes, f.AbuseType) {
		problem(w, http.StatusBadRequest, "unknown abuse_type "+strconv.Quote(f.AbuseType))
		return
	}
	if v := q.Get("min_score"); v != "" {
		score, err := strconv.ParseFloat(v, 64)
		if err != nil || score < 0 || score > 100 {
			problem(w, http.StatusBadRequest, "min_score must be a number from 0 to 100")
			return
		}
		f.MinScore = score
	}
	limit, ok := intParam(r, "limit", 50, 1, 500)
	if !ok {
		problem(w, http.StatusBadRequest, "limit must be an integer from 1 to 500")
		return
	}
	f.Limit = limit
	alerts, next, err := s.store.List(r.Context(), f)
	if errors.Is(err, store.ErrBadCursor) {
		problem(w, http.StatusBadRequest, "invalid cursor")
		return
	}
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	out := make([]live.Summary, len(alerts))
	for i, a := range alerts {
		out[i] = live.Summarize(a)
	}
	page := map[string]any{"alerts": out}
	if next != "" {
		page["next_cursor"] = next
	}
	writeJSON(w, http.StatusOK, page)
}

func (s *Server) getAlert(w http.ResponseWriter, r *http.Request) {
	a, full, err := s.store.Get(r.Context(), r.PathValue("key"))
	if errors.Is(err, store.ErrNotFound) {
		problem(w, http.StatusNotFound, "no such alert")
		return
	}
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, detail(a, full))
}

func (s *Server) getAudit(w http.ResponseWriter, r *http.Request) {
	entries, err := s.store.Audit(r.Context(), r.PathValue("key"))
	if errors.Is(err, store.ErrNotFound) {
		problem(w, http.StatusNotFound, "no such alert")
		return
	}
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	out := make([]auditEntry, len(entries))
	for i, e := range entries {
		out[i] = toAuditEntry(e)
	}
	writeJSON(w, http.StatusOK, map[string]any{"entries": out})
}

func (s *Server) transition(w http.ResponseWriter, r *http.Request) {
	var req struct {
		To   store.Status `json:"to"`
		Note string       `json:"note"`
	}
	if !decode(w, r, &req) {
		return
	}
	if !req.To.Valid() {
		problem(w, http.StatusBadRequest, "unknown status "+strconv.Quote(string(req.To)))
		return
	}
	actor := claimsOf(r).Subject
	a, err := s.store.Transition(r.Context(), r.PathValue("key"), req.To, actor, req.Note)
	var te *store.TransitionError
	switch {
	case errors.Is(err, store.ErrNotFound):
		problem(w, http.StatusNotFound, "no such alert")
		return
	case errors.As(err, &te):
		problem(w, http.StatusConflict, "an alert cannot move from "+string(te.From)+" to "+string(te.To))
		return
	case errors.Is(err, store.ErrNoteRequired):
		problem(w, http.StatusUnprocessableEntity, "moving an alert to "+string(req.To)+" requires a note of at most 4000 bytes")
		return
	case err != nil:
		s.internalError(w, r, err)
		return
	}
	s.log.Info("alert status changed", "alert", a.Key, "to", a.Status, "by", actor)
	if err := s.views.Updated(r.Context(), a); err != nil {
		// Postgres holds the change; the live feed just misses this event.
		s.log.Warn("publishing an alert update failed", "alert", a.Key, "err", err)
	}
	writeJSON(w, http.StatusOK, live.Summarize(a))
}

func (s *Server) comment(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Note string `json:"note"`
	}
	if !decode(w, r, &req) {
		return
	}
	e, err := s.store.Comment(r.Context(), r.PathValue("key"), claimsOf(r).Subject, req.Note)
	switch {
	case errors.Is(err, store.ErrNotFound):
		problem(w, http.StatusNotFound, "no such alert")
	case errors.Is(err, store.ErrNoteRequired):
		problem(w, http.StatusUnprocessableEntity, "a comment needs a note of 1 to 4000 bytes")
	case err != nil:
		s.internalError(w, r, err)
	default:
		writeJSON(w, http.StatusCreated, toAuditEntry(e))
	}
}

func (s *Server) stats(w http.ResponseWriter, r *http.Request) {
	c, err := s.store.Count(r.Context())
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	byStatus := map[string]int64{}
	for _, st := range store.Statuses {
		byStatus[string(st)] = c.ByStatus[st]
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"total": c.Total, "by_status": byStatus, "by_abuse_type": c.ByType, "by_symbol": c.BySymbol,
	})
}

func (s *Server) liveView(w http.ResponseWriter, r *http.Request) {
	limit, ok := intParam(r, "limit", 50, 1, 500)
	if !ok {
		problem(w, http.StatusBadRequest, "limit must be an integer from 1 to 500")
		return
	}
	counts, err := s.views.Counts(r.Context())
	if err != nil {
		s.log.Warn("reading live counts failed", "err", err)
		problem(w, http.StatusServiceUnavailable, "the live view is unavailable")
		return
	}
	recent, err := s.views.Recent(r.Context(), limit)
	if err != nil {
		s.log.Warn("reading recent alerts failed", "err", err)
		problem(w, http.StatusServiceUnavailable, "the live view is unavailable")
		return
	}
	if recent == nil {
		recent = []live.Summary{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"counts": counts, "recent": recent})
}

func (s *Server) rules(w http.ResponseWriter, r *http.Request) {
	load, rules, err := s.store.CurrentRules(r.Context())
	if errors.Is(err, store.ErrNotFound) {
		problem(w, http.StatusNotFound, "no detector has loaded rules yet")
		return
	}
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	out := make([]ruleVersion, len(rules))
	for i, rv := range rules {
		out[i] = toRuleVersion(rv, false)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"ruleset_digest": load.Digest, "loaded_by": load.LoadedBy, "loaded_at": load.LoadedAt.UTC(), "rules": out,
	})
}

func (s *Server) ruleHistory(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	versions, err := s.store.RuleHistory(r.Context(), id)
	if errors.Is(err, store.ErrNotFound) {
		problem(w, http.StatusNotFound, "no such rule")
		return
	}
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	out := make([]ruleVersion, len(versions))
	for i, rv := range versions {
		out[i] = toRuleVersion(rv, true)
	}
	writeJSON(w, http.StatusOK, map[string]any{"rule_id": id, "versions": out})
}

// Wire types beyond live.Summary.

type alertDetail struct {
	live.Summary
	Detector         string     `json:"detector"`
	RuleDigest       string     `json:"rule_digest"`
	DetectorInstance string     `json:"detector_instance,omitempty"`
	EmittedAt        *time.Time `json:"emitted_at,omitempty"`
	LastSeenAt       time.Time  `json:"last_seen_at"`
	TriggerSeq       uint64     `json:"trigger_seq,omitempty"`
	Evidence         evidence   `json:"evidence"`
}

type evidence struct {
	OrderIDs     []uint64        `json:"order_ids"`
	TradeIDs     []uint64        `json:"trade_ids"`
	Features     []feature       `json:"features"`
	Timeline     []evidenceEvent `json:"timeline"`
	PriceContext []pricePoint    `json:"price_context"`
}

type feature struct {
	Name      string   `json:"name"`
	Value     float64  `json:"value"`
	Threshold *float64 `json:"threshold,omitempty"`
	Op        string   `json:"op,omitempty"`
	Breached  bool     `json:"breached"`
	Unit      string   `json:"unit,omitempty"`
}

type evidenceEvent struct {
	Time         time.Time `json:"time"`
	Seq          uint64    `json:"seq,omitempty"`
	Kind         string    `json:"kind"`
	OrderID      uint64    `json:"order_id,omitempty"`
	TradeID      uint64    `json:"trade_id,omitempty"`
	AccountID    string    `json:"account_id,omitempty"`
	Counterparty string    `json:"counterparty_account_id,omitempty"`
	Side         string    `json:"side,omitempty"`
	Price        int64     `json:"price"`
	Quantity     int64     `json:"quantity"`
	Role         string    `json:"role"`
}

type pricePoint struct {
	Time      time.Time `json:"time"`
	LastPrice int64     `json:"last_price"`
	BestBid   int64     `json:"best_bid"`
	BestAsk   int64     `json:"best_ask"`
	BidSize   int64     `json:"bid_size"`
	AskSize   int64     `json:"ask_size"`
}

type auditEntry struct {
	ID     int64     `json:"id"`
	At     time.Time `json:"at"`
	Actor  string    `json:"actor"`
	Action string    `json:"action"`
	From   string    `json:"from_status,omitempty"`
	To     string    `json:"to_status,omitempty"`
	Note   string    `json:"note"`
}

type ruleVersion struct {
	RuleID        string    `json:"rule_id"`
	Version       int       `json:"version"`
	Digest        string    `json:"digest"`
	Detector      string    `json:"detector"`
	Name          string    `json:"name"`
	TaxonomyCode  string    `json:"taxonomy_code"`
	Enabled       bool      `json:"enabled"`
	FirstSeenAt   time.Time `json:"first_seen_at"`
	VersionReused bool      `json:"version_reused"`
	Source        string    `json:"source,omitempty"`
}

func nsTime(v int64) time.Time { return time.Unix(0, v).UTC() }

// enumName lower-cases a protobuf enum value name without its prefix.
func enumName(name, prefix string) string { return strings.ToLower(strings.TrimPrefix(name, prefix)) }

func detail(a *store.Alert, full *twpb.Alert) alertDetail {
	d := alertDetail{
		Summary: live.Summarize(a), Detector: a.Detector, RuleDigest: a.RuleDigest,
		DetectorInstance: full.GetDetectorInstance(), LastSeenAt: a.LastSeenAt.UTC(), TriggerSeq: full.GetTriggerSeq(),
	}
	if a.EmittedAtNs > 0 {
		t := nsTime(a.EmittedAtNs)
		d.EmittedAt = &t
	}
	ev := full.GetEvidence()
	d.Evidence = evidence{
		OrderIDs: append([]uint64{}, ev.GetOrderIds()...), TradeIDs: append([]uint64{}, ev.GetTradeIds()...),
		Features: []feature{}, Timeline: []evidenceEvent{}, PriceContext: []pricePoint{},
	}
	for _, f := range ev.GetFeatures() {
		out := feature{Name: f.GetName(), Value: f.GetValue(), Op: f.GetOp(), Breached: f.GetBreached(), Unit: f.GetUnit()}
		if f.GetOp() != "" {
			th := f.GetThreshold()
			out.Threshold = &th
		}
		d.Evidence.Features = append(d.Evidence.Features, out)
	}
	for _, e := range ev.GetTimeline() {
		side := ""
		if e.GetSide() != twpb.Side_SIDE_UNSPECIFIED {
			side = enumName(e.GetSide().String(), "SIDE_")
		}
		d.Evidence.Timeline = append(d.Evidence.Timeline, evidenceEvent{
			Time: nsTime(e.GetEventTimeNs()), Seq: e.GetSeq(), Kind: enumName(e.GetKind().String(), "EVIDENCE_KIND_"),
			OrderID: e.GetOrderId(), TradeID: e.GetTradeId(), AccountID: e.GetAccountId(),
			Counterparty: e.GetCounterpartyAccountId(), Side: side, Price: e.GetPrice(), Quantity: e.GetQuantity(),
			Role: e.GetRole(),
		})
	}
	for _, p := range ev.GetPriceContext() {
		d.Evidence.PriceContext = append(d.Evidence.PriceContext, pricePoint{
			Time: nsTime(p.GetEventTimeNs()), LastPrice: p.GetLastPrice(), BestBid: p.GetBestBid(), BestAsk: p.GetBestAsk(),
			BidSize: p.GetBidSize(), AskSize: p.GetAskSize(),
		})
	}
	return d
}

func toAuditEntry(e store.AuditEntry) auditEntry {
	return auditEntry{ID: e.ID, At: e.At.UTC(), Actor: e.Actor, Action: e.Action, From: string(e.From), To: string(e.To), Note: e.Note}
}

func toRuleVersion(r store.RuleVersion, withSource bool) ruleVersion {
	out := ruleVersion{
		RuleID: r.RuleID, Version: r.Version, Digest: r.Digest, Detector: r.Detector, Name: r.Name,
		TaxonomyCode: r.TaxonomyCode, Enabled: r.Enabled, FirstSeenAt: r.FirstSeenAt.UTC(), VersionReused: r.VersionReused,
	}
	if withSource {
		out.Source = r.Source
	}
	return out
}
