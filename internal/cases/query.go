package cases

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

var (
	ErrNotFound        = errors.New("case not found")
	ErrAlreadyResolved = errors.New("case already resolved")
)

// Logs states. A case's logs are the bundles its alerts named, including the
// alerts of its child cases.
const (
	LogsNone     = "none"     // no alert named a bundle (cooldown, or a fault that doesn't collect logs)
	LogsWaiting  = "waiting"  // named, but none has arrived yet
	LogsAttached = "attached" // at least one has arrived
)

type Case struct {
	ID           int64      `json:"case_id"`
	Scope        string     `json:"scope"`
	SiteID       string     `json:"site_id"`
	DeviceID     string     `json:"device_id,omitempty"`
	RuleID       string     `json:"rule_id"`
	Title        string     `json:"title"`
	Severity     string     `json:"severity"`
	Status       string     `json:"status"`
	AlertCount   int        `json:"alert_count"`
	Devices      int        `json:"devices"`
	ParentCaseID *int64     `json:"parent_case_id,omitempty"`
	OpenedAt     time.Time  `json:"opened_at"`
	LastAlertAt  time.Time  `json:"last_alert_at"`
	ResolvedAt   *time.Time `json:"resolved_at,omitempty"`
	Resolution   string     `json:"resolution,omitempty"`
	Logs         string     `json:"logs"`
}

type CaseAlert struct {
	AlertID    string    `json:"alert_id"`
	CaseID     int64     `json:"case_id"`
	DeviceID   string    `json:"device_id"`
	Code       string    `json:"code"`
	Severity   string    `json:"severity"`
	Value      float64   `json:"value"`
	Message    string    `json:"message,omitempty"`
	ObservedAt time.Time `json:"observed_at"`
	ReceivedAt time.Time `json:"received_at"`
	BundleID   string    `json:"bundle_id,omitempty"`
}

type CaseBundle struct {
	BundleID   string     `json:"bundle_id"`
	DeviceID   string     `json:"device_id"`
	State      string     `json:"state"` // waiting or attached
	ObjectKey  string     `json:"object_key,omitempty"`
	SizeBytes  int64      `json:"size_bytes,omitempty"`
	UploadedAt *time.Time `json:"uploaded_at,omitempty"`
}

type CaseDetail struct {
	Case
	Alerts   []CaseAlert  `json:"alerts"`
	Bundles  []CaseBundle `json:"bundles"`
	Children []Case       `json:"children,omitempty"`
}

type Filter struct {
	Status   string // open, resolved or all
	SiteID   string
	DeviceID string
	RuleID   string
	Limit    int
}

// caseAlerts matches the alerts that belong to case c: its own, and those of
// its child cases.
const caseAlerts = `(a.case_id = c.case_id OR a.case_id IN (SELECT ch.case_id FROM cases.cases ch WHERE ch.parent_case_id = c.case_id))`

const caseColumns = `c.case_id, c.scope, c.site_id, coalesce(c.device_id, ''), c.rule_id, c.title, c.severity, c.status,
	c.alert_count, c.parent_case_id, c.opened_at, c.last_alert_at, c.resolved_at, c.resolution,
	(SELECT count(DISTINCT a.device_id) FROM cases.alerts a WHERE ` + caseAlerts + `),
	(SELECT count(*) FROM cases.alerts a WHERE ` + caseAlerts + ` AND a.bundle_id IS NOT NULL),
	(SELECT count(*) FROM cases.alerts a JOIN cases.bundles b USING (bundle_id) WHERE ` + caseAlerts + `)`

func scanCase(row interface{ Scan(...any) error }) (Case, error) {
	var c Case
	var parent sql.NullInt64
	var resolved sql.NullTime
	var named, arrived int
	if err := row.Scan(&c.ID, &c.Scope, &c.SiteID, &c.DeviceID, &c.RuleID, &c.Title, &c.Severity, &c.Status,
		&c.AlertCount, &parent, &c.OpenedAt, &c.LastAlertAt, &resolved, &c.Resolution, &c.Devices, &named, &arrived); err != nil {
		return c, err
	}
	if parent.Valid {
		c.ParentCaseID = &parent.Int64
	}
	if resolved.Valid {
		t := resolved.Time
		c.ResolvedAt = &t
	}
	switch {
	case arrived > 0:
		c.Logs = LogsAttached
	case named > 0:
		c.Logs = LogsWaiting
	default:
		c.Logs = LogsNone
	}
	return c, nil
}

func (s *Store) ListCases(ctx context.Context, f Filter) ([]Case, error) {
	var where []string
	var args []any
	add := func(cond string, v any) {
		args = append(args, v)
		where = append(where, fmt.Sprintf(cond, len(args)))
	}
	switch f.Status {
	case "", "open":
		add("c.status = $%d", "open")
	case "resolved":
		add("c.status = $%d", "resolved")
	case "all":
	default:
		return nil, fmt.Errorf("status must be open, resolved or all")
	}
	if f.SiteID != "" {
		add("c.site_id = $%d", f.SiteID)
	}
	if f.DeviceID != "" {
		add("c.device_id = $%d", f.DeviceID)
	}
	if f.RuleID != "" {
		add("c.rule_id = $%d", f.RuleID)
	}
	if f.Limit <= 0 {
		f.Limit = 50
	}
	q := "SELECT " + caseColumns + " FROM cases.cases c"
	if len(where) > 0 {
		q += " WHERE " + strings.Join(where, " AND ")
	}
	args = append(args, f.Limit)
	q += fmt.Sprintf(" ORDER BY c.opened_at DESC, c.case_id DESC LIMIT $%d", len(args))
	return s.queryCases(ctx, q, args...)
}

func (s *Store) queryCases(ctx context.Context, q string, args ...any) ([]Case, error) {
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Case{}
	for rows.Next() {
		c, err := scanCase(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

func (s *Store) GetCase(ctx context.Context, id int64) (CaseDetail, error) {
	c, err := scanCase(s.db.QueryRowContext(ctx, "SELECT "+caseColumns+" FROM cases.cases c WHERE c.case_id = $1", id))
	if errors.Is(err, sql.ErrNoRows) {
		return CaseDetail{}, ErrNotFound
	}
	if err != nil {
		return CaseDetail{}, err
	}
	d := CaseDetail{Case: c, Alerts: []CaseAlert{}, Bundles: []CaseBundle{}}

	rows, err := s.db.QueryContext(ctx, `
		SELECT a.alert_id, a.case_id, a.device_id, a.code, a.severity, a.value, a.message, a.observed_at, a.received_at, coalesce(a.bundle_id, '')
		FROM cases.alerts a, (SELECT $1::bigint AS case_id) c
		WHERE `+caseAlerts+` ORDER BY a.observed_at, a.alert_id`, id)
	if err != nil {
		return d, err
	}
	defer rows.Close()
	for rows.Next() {
		var a CaseAlert
		if err := rows.Scan(&a.AlertID, &a.CaseID, &a.DeviceID, &a.Code, &a.Severity, &a.Value, &a.Message, &a.ObservedAt, &a.ReceivedAt, &a.BundleID); err != nil {
			return d, err
		}
		d.Alerts = append(d.Alerts, a)
	}
	if err := rows.Err(); err != nil {
		return d, err
	}

	brows, err := s.db.QueryContext(ctx, `
		SELECT DISTINCT a.bundle_id, a.device_id, b.object_key, b.size_bytes, b.uploaded_at
		FROM cases.alerts a
		CROSS JOIN (SELECT $1::bigint AS case_id) c
		LEFT JOIN cases.bundles b ON b.bundle_id = a.bundle_id
		WHERE `+caseAlerts+` AND a.bundle_id IS NOT NULL
		ORDER BY a.bundle_id`, id)
	if err != nil {
		return d, err
	}
	defer brows.Close()
	for brows.Next() {
		var b CaseBundle
		var key sql.NullString
		var size sql.NullInt64
		var at sql.NullTime
		if err := brows.Scan(&b.BundleID, &b.DeviceID, &key, &size, &at); err != nil {
			return d, err
		}
		b.State = LogsWaiting
		if key.Valid {
			t := at.Time
			b.State, b.ObjectKey, b.SizeBytes, b.UploadedAt = LogsAttached, key.String, size.Int64, &t
		}
		d.Bundles = append(d.Bundles, b)
	}
	if err := brows.Err(); err != nil {
		return d, err
	}

	d.Children, err = s.queryCases(ctx, "SELECT "+caseColumns+" FROM cases.cases c WHERE c.parent_case_id = $1 ORDER BY c.case_id", id)
	return d, err
}

// ResolveCase closes a case and any open child cases with it.
func (s *Store) ResolveCase(ctx context.Context, id int64, resolution string) (resolvedChildren int, err error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback() //nolint:errcheck

	var status string
	err = tx.QueryRowContext(ctx, `SELECT status FROM cases.cases WHERE case_id = $1 FOR UPDATE`, id).Scan(&status)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, ErrNotFound
	}
	if err != nil {
		return 0, err
	}
	if status != "open" {
		return 0, ErrAlreadyResolved
	}
	if _, err := tx.ExecContext(ctx, `UPDATE cases.cases SET status = 'resolved', resolved_at = now(), resolution = $2 WHERE case_id = $1`, id, resolution); err != nil {
		return 0, err
	}
	res, err := tx.ExecContext(ctx, `
		UPDATE cases.cases SET status = 'resolved', resolved_at = now(), resolution = $2
		WHERE parent_case_id = $1 AND status = 'open'`, id, fmt.Sprintf("resolved with site case %d", id))
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return int(n), tx.Commit()
}

// Stats is what the metrics gauges show about cases right now.
type Stats struct {
	Open                 map[[2]string]int // {rule, scope} -> open cases
	WaitingForLogs       int
	OldestWaitingSeconds float64
}

func (s *Store) Stats(ctx context.Context) (Stats, error) {
	st := Stats{Open: map[[2]string]int{}}
	rows, err := s.db.QueryContext(ctx, `SELECT rule_id, scope, count(*) FROM cases.cases WHERE status = 'open' GROUP BY 1, 2`)
	if err != nil {
		return st, err
	}
	defer rows.Close()
	for rows.Next() {
		var rule, scope string
		var n int
		if err := rows.Scan(&rule, &scope, &n); err != nil {
			return st, err
		}
		st.Open[[2]string{rule, scope}] = n
	}
	if err := rows.Err(); err != nil {
		return st, err
	}
	err = s.db.QueryRowContext(ctx, `
		SELECT count(*), coalesce(extract(epoch FROM now() - min(c.opened_at)), 0)
		FROM cases.cases c
		WHERE c.status = 'open'
		  AND EXISTS (SELECT 1 FROM cases.alerts a WHERE `+caseAlerts+` AND a.bundle_id IS NOT NULL)
		  AND NOT EXISTS (SELECT 1 FROM cases.alerts a JOIN cases.bundles b USING (bundle_id) WHERE `+caseAlerts+`)`).
		Scan(&st.WaitingForLogs, &st.OldestWaitingSeconds)
	return st, err
}
