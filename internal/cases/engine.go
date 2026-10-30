package cases

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/shubhampatel19/connectivity-contracts/wire"
	"github.com/shubhampatel19/connectivity-go-kit/platform"
)

type Outcome string

const (
	OutcomeDuplicate      Outcome = "duplicate"       // seen before: retry, redelivery or replay
	OutcomeNoRule         Outcome = "no_rule"         // no rule for this code; recorded only
	OutcomeBelowThreshold Outcome = "below_threshold" // a rule matched, but not enough yet for a case
	OutcomeOpened         Outcome = "opened"          // opened a new case
	OutcomeAttached       Outcome = "attached"        // added to an open case
)

const (
	ScopeDevice = "device"
	ScopeSite   = "site"
)

// Result is what happened to one alert.
type Result struct {
	Outcome Outcome
	RuleID  string
	CaseID  int64
	Scope   string
	Adopted int // open device cases that became children of a new site case
}

// ProcessAlert records an alert and applies its rule, all in one
// transaction, so a crash leaves either the whole decision or none of it,
// and a replay of the same alert finds it already recorded.
//
// The order of checks is the policy:
//  1. An alert already recorded is a duplicate; nothing else happens.
//  2. An open site case for this site and rule takes the alert.
//  3. An open device case for this device and rule takes it.
//  4. Otherwise the rule's conditions (min_value, threshold) must hold.
//  5. If enough devices at the site have raised the code, open a site case
//     and make the site's open device cases for the rule its children.
//  6. Otherwise open a device case.
func (s *Store) ProcessAlert(ctx context.Context, env wire.AlertEnvelope, rules *Rules) (_ Result, err error) {
	ctx, span := platform.Tracer().Start(ctx, "postgres decide alert",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(attribute.String("db.system", "postgresql")))
	defer func() {
		platform.RecordError(ctx, err)
		span.End()
	}()
	a := env.Alert
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Result{}, err
	}
	defer tx.Rollback() //nolint:errcheck

	ins, err := tx.ExecContext(ctx, `
		INSERT INTO cases.alerts (alert_id, device_id, site_id, code, severity, value, message,
		                          observed_at, received_at, bundle_id, outcome)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NULLIF($10, ''), 'pending')
		ON CONFLICT (alert_id) DO NOTHING`,
		a.AlertID, a.DeviceID, a.SiteID, string(a.Code), string(a.Severity), a.Value, a.Message,
		a.ObservedAt, env.ReceivedAt, a.BundleID)
	if err != nil {
		return Result{}, fmt.Errorf("record alert: %w", err)
	}
	if n, err := ins.RowsAffected(); err != nil {
		return Result{}, err
	} else if n == 0 {
		return Result{Outcome: OutcomeDuplicate}, nil
	}

	res := Result{Outcome: OutcomeNoRule}
	if rule := rules.Match(a.Code); rule != nil {
		if res, err = decide(ctx, tx, a, rule); err != nil {
			return Result{}, err
		}
	}

	var caseID sql.NullInt64
	if res.CaseID != 0 {
		caseID = sql.NullInt64{Int64: res.CaseID, Valid: true}
	}
	if _, err := tx.ExecContext(ctx, `UPDATE cases.alerts SET outcome = $2, rule_id = NULLIF($3, ''), case_id = $4 WHERE alert_id = $1`,
		a.AlertID, string(res.Outcome), res.RuleID, caseID); err != nil {
		return Result{}, fmt.Errorf("record outcome: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return Result{}, err
	}
	return res, nil
}

func decide(ctx context.Context, tx *sql.Tx, a wire.DeviceAlert, rule *Rule) (Result, error) {
	res := Result{RuleID: rule.ID}
	storm := rule.Storm != nil && a.SiteID != ""

	if storm {
		// Serialize every decision for this site and rule. Without it, two
		// consumers handling different devices could each count four devices,
		// each open a device case, and both miss the fifth that makes a storm.
		if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtext($1))`, "cases/"+a.SiteID+"/"+rule.ID); err != nil {
			return res, fmt.Errorf("lock site: %w", err)
		}
		if id, err := openCase(ctx, tx, ScopeSite, a.SiteID, rule.ID); err != nil || id != 0 {
			return attached(ctx, tx, res, id, ScopeSite, a, err)
		}
	}
	if id, err := openCase(ctx, tx, ScopeDevice, a.DeviceID, rule.ID); err != nil || id != 0 {
		return attached(ctx, tx, res, id, ScopeDevice, a, err)
	}

	ok, err := qualifies(ctx, tx, a, rule)
	if err != nil || !ok {
		res.Outcome = OutcomeBelowThreshold
		return res, err
	}

	if storm {
		var devices int
		if err := tx.QueryRowContext(ctx, `
			SELECT count(DISTINCT device_id) FROM cases.alerts
			WHERE site_id = $1 AND code = $2
			  AND observed_at BETWEEN $3::timestamptz - make_interval(secs => $4) AND $3::timestamptz + make_interval(secs => $4)`,
			a.SiteID, string(a.Code), a.ObservedAt, rule.Storm.Window.Seconds()).Scan(&devices); err != nil {
			return res, fmt.Errorf("count site devices: %w", err)
		}
		if devices >= rule.Storm.Devices {
			id, isNew, err := openNewCase(ctx, tx, ScopeSite, a, rule)
			if err != nil || !isNew {
				return attached(ctx, tx, res, id, ScopeSite, a, err)
			}
			adopted, err := tx.ExecContext(ctx, `
				UPDATE cases.cases SET parent_case_id = $1
				WHERE scope = 'device' AND site_id = $2 AND rule_id = $3 AND status = 'open' AND parent_case_id IS NULL`,
				id, a.SiteID, rule.ID)
			if err != nil {
				return res, fmt.Errorf("adopt device cases: %w", err)
			}
			n, _ := adopted.RowsAffected()
			res.Adopted = int(n)
			return opened(ctx, tx, res, id, ScopeSite, a)
		}
	}

	id, isNew, err := openNewCase(ctx, tx, ScopeDevice, a, rule)
	if err != nil || !isNew {
		return attached(ctx, tx, res, id, ScopeDevice, a, err)
	}
	return opened(ctx, tx, res, id, ScopeDevice, a)
}

// qualifies checks a rule's conditions. Windows are centred on the alert's
// observed time rather than ending at it, so alerts that arrive late or out
// of order (a gateway draining its queue) still count together.
func qualifies(ctx context.Context, tx *sql.Tx, a wire.DeviceAlert, rule *Rule) (bool, error) {
	if rule.MinValue != nil && a.Value < *rule.MinValue {
		return false, nil
	}
	if t := rule.Threshold; t != nil {
		var n int
		if err := tx.QueryRowContext(ctx, `
			SELECT count(*) FROM cases.alerts
			WHERE device_id = $1 AND code = $2
			  AND observed_at BETWEEN $3::timestamptz - make_interval(secs => $4) AND $3::timestamptz + make_interval(secs => $4)`,
			a.DeviceID, string(a.Code), a.ObservedAt, t.Window.Seconds()).Scan(&n); err != nil {
			return false, fmt.Errorf("count alerts in window: %w", err)
		}
		if n < t.Count {
			return false, nil
		}
	}
	return true, nil
}

// openCase returns the open case for a subject and rule, locked for this
// transaction, or 0 if there is none.
func openCase(ctx context.Context, tx *sql.Tx, scope, key, ruleID string) (int64, error) {
	var id int64
	err := tx.QueryRowContext(ctx, `
		SELECT case_id FROM cases.cases
		WHERE scope = $1 AND scope_key = $2 AND rule_id = $3 AND status = 'open'
		FOR UPDATE`, scope, key, ruleID).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	return id, err
}

// openNewCase inserts a case, or, if another consumer opened one for the same
// subject and rule a moment ago, returns that one with isNew false. The
// partial unique index is what makes this safe without any locking in Go.
func openNewCase(ctx context.Context, tx *sql.Tx, scope string, a wire.DeviceAlert, rule *Rule) (id int64, isNew bool, err error) {
	key, device, title := a.DeviceID, sql.NullString{String: a.DeviceID, Valid: true}, rule.Title
	if scope == ScopeSite {
		key, device, title = a.SiteID, sql.NullString{}, "Site-wide: "+rule.Title
	}
	err = tx.QueryRowContext(ctx, `
		INSERT INTO cases.cases (scope, scope_key, site_id, device_id, rule_id, title, severity, opened_at, last_alert_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, now(), $8)
		ON CONFLICT (scope, scope_key, rule_id) WHERE status = 'open' DO NOTHING
		RETURNING case_id`,
		scope, key, a.SiteID, device, rule.ID, title, rule.Severity, a.ObservedAt).Scan(&id)
	if err == nil {
		return id, true, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return 0, false, fmt.Errorf("open case: %w", err)
	}
	id, err = openCase(ctx, tx, scope, key, rule.ID)
	if err == nil && id == 0 {
		err = errors.New("open case: conflicting case vanished before it could be read")
	}
	return id, false, err
}

func opened(ctx context.Context, tx *sql.Tx, res Result, id int64, scope string, a wire.DeviceAlert) (Result, error) {
	res, err := attached(ctx, tx, res, id, scope, a, nil)
	res.Outcome = OutcomeOpened
	return res, err
}

func attached(ctx context.Context, tx *sql.Tx, res Result, id int64, scope string, a wire.DeviceAlert, err error) (Result, error) {
	if err != nil {
		return res, err
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE cases.cases SET alert_count = alert_count + 1, last_alert_at = GREATEST(last_alert_at, $2)
		WHERE case_id = $1`, id, a.ObservedAt); err != nil {
		return res, fmt.Errorf("attach alert: %w", err)
	}
	res.Outcome, res.CaseID, res.Scope = OutcomeAttached, id, scope
	return res, nil
}
