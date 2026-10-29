package cases

// Integration tests against a real Postgres: deduplication lives in SQL
// (primary keys, a partial unique index, an advisory lock), so a fake store
// would test nothing that matters. Set CASES_TEST_DATABASE_URL to run them;
// CI runs them against a Postgres service container. They drop and recreate
// the "cases" schema, so never point them at a database you care about.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/shubhampatel19/connectivity-contracts/wire"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

func testStore(t *testing.T) *Store {
	t.Helper()
	dsn := os.Getenv("CASES_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("CASES_TEST_DATABASE_URL not set")
	}
	ctx := context.Background()
	st, err := OpenStore(ctx, dsn, quiet)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	if _, err := st.db.ExecContext(ctx, `DROP SCHEMA IF EXISTS cases CASCADE`); err != nil {
		t.Fatal(err)
	}
	if err := st.Migrate(ctx, quiet); err != nil {
		t.Fatal(err)
	}
	return st
}

func testRules(t *testing.T) *Rules {
	t.Helper()
	r, err := LoadRules("")
	if err != nil {
		t.Fatal(err)
	}
	return r
}

var t0 = time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)

var alertSeq int

func alert(device, site string, code wire.AlertCode, value float64, at time.Time) wire.AlertEnvelope {
	alertSeq++
	return wire.AlertEnvelope{ReceivedAt: at.Add(time.Second), Alert: wire.DeviceAlert{
		AlertID: fmt.Sprintf("alert-%06d", alertSeq), DeviceID: device, SiteID: site,
		Code: code, Severity: wire.SeverityWarning, Value: value, ObservedAt: at,
	}}
}

func process(t *testing.T, st *Store, r *Rules, env wire.AlertEnvelope) Result {
	t.Helper()
	res, err := st.ProcessAlert(context.Background(), env, r)
	if err != nil {
		t.Fatalf("ProcessAlert(%s): %v", env.Alert.AlertID, err)
	}
	return res
}

func expect(t *testing.T, res Result, outcome Outcome, scope string) {
	t.Helper()
	if res.Outcome != outcome || res.Scope != scope {
		t.Fatalf("got outcome %s scope %q, want %s %q (%+v)", res.Outcome, res.Scope, outcome, scope, res)
	}
}

func count(t *testing.T, st *Store, q string, args ...any) int {
	t.Helper()
	var n int
	if err := st.db.QueryRow(q, args...).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestSameAlertTwiceIsADuplicate(t *testing.T) {
	st, r := testStore(t), testRules(t)
	env := alert("d1", "s1", wire.AlertThermalCritical, 93, t0)
	expect(t, process(t, st, r, env), OutcomeOpened, ScopeDevice)
	expect(t, process(t, st, r, env), OutcomeDuplicate, "")
	if n := count(t, st, `SELECT alert_count FROM cases.cases`); n != 1 {
		t.Fatalf("a redelivered alert must not be counted twice, alert_count = %d", n)
	}
}

func TestUnknownCodeIsRecordedWithoutACase(t *testing.T) {
	st, r := testStore(t), testRules(t)
	expect(t, process(t, st, r, alert("d1", "s1", "fan.stalled", 0, t0)), OutcomeNoRule, "")
	if count(t, st, `SELECT count(*) FROM cases.cases`) != 0 || count(t, st, `SELECT count(*) FROM cases.alerts WHERE outcome = 'no_rule'`) != 1 {
		t.Fatal("an unmatched alert is stored with outcome no_rule and opens nothing")
	}
}

func TestOpenCaseAbsorbsRepeatsFromTheSameDevice(t *testing.T) {
	st, r := testStore(t), testRules(t)
	first := process(t, st, r, alert("d1", "s1", wire.AlertThermalCritical, 93, t0))
	expect(t, first, OutcomeOpened, ScopeDevice)
	second := process(t, st, r, alert("d1", "s1", wire.AlertThermalCritical, 95, t0.Add(time.Hour)))
	expect(t, second, OutcomeAttached, ScopeDevice)
	if second.CaseID != first.CaseID {
		t.Fatal("the second alert should join the open case")
	}
	other := process(t, st, r, alert("d2", "s1", wire.AlertThermalCritical, 93, t0))
	if other.CaseID == first.CaseID {
		t.Fatal("another device gets its own case")
	}
}

func TestThresholdNeedsRepeatsWithinTheWindow(t *testing.T) {
	st, r := testStore(t), testRules(t) // PSU: 3 alerts within 15 minutes
	psu := func(device string, at time.Time) Result {
		return process(t, st, r, alert(device, "s1", wire.AlertPSURail, 11.78, at))
	}
	expect(t, psu("d1", t0), OutcomeBelowThreshold, "")
	expect(t, psu("d1", t0.Add(40*time.Minute)), OutcomeBelowThreshold, "") // too far from the first
	expect(t, psu("d1", t0.Add(50*time.Minute)), OutcomeBelowThreshold, "") // two in the window
	expect(t, psu("d1", t0.Add(52*time.Minute)), OutcomeOpened, ScopeDevice)
	expect(t, psu("d1", t0.Add(3*time.Hour)), OutcomeAttached, ScopeDevice) // the open case takes any later one
	expect(t, psu("d2", t0.Add(50*time.Minute)), OutcomeBelowThreshold, "") // counts are per device
}

// A gateway draining its queue delivers alerts late and possibly out of
// order. The window is around each alert's observed time, so they still count.
func TestThresholdCountsAlertsDeliveredOutOfOrder(t *testing.T) {
	st, r := testStore(t), testRules(t)
	expect(t, process(t, st, r, alert("d1", "s1", wire.AlertPSURail, 11.78, t0.Add(10*time.Minute))), OutcomeBelowThreshold, "")
	expect(t, process(t, st, r, alert("d1", "s1", wire.AlertPSURail, 11.78, t0.Add(5*time.Minute))), OutcomeBelowThreshold, "")
	expect(t, process(t, st, r, alert("d1", "s1", wire.AlertPSURail, 11.78, t0)), OutcomeOpened, ScopeDevice)
}

func TestMinValue(t *testing.T) {
	st, r := testStore(t), testRules(t) // disk: open at 5 sectors
	expect(t, process(t, st, r, alert("d1", "s1", wire.AlertDiskReallocated, 1, t0)), OutcomeBelowThreshold, "")
	expect(t, process(t, st, r, alert("d1", "s1", wire.AlertDiskReallocated, 4, t0)), OutcomeBelowThreshold, "")
	expect(t, process(t, st, r, alert("d1", "s1", wire.AlertDiskReallocated, 5, t0)), OutcomeOpened, ScopeDevice)
	expect(t, process(t, st, r, alert("d1", "s1", wire.AlertDiskReallocated, 6, t0)), OutcomeAttached, ScopeDevice)
}

func TestResolvedCaseLetsTheNextAlertOpenANewOne(t *testing.T) {
	st, r := testStore(t), testRules(t)
	first := process(t, st, r, alert("d1", "s1", wire.AlertThermalCritical, 93, t0))
	if _, err := st.ResolveCase(context.Background(), first.CaseID, "replaced fan"); err != nil {
		t.Fatal(err)
	}
	if _, err := st.ResolveCase(context.Background(), first.CaseID, "again"); !errors.Is(err, ErrAlreadyResolved) {
		t.Fatalf("resolving twice: got %v, want ErrAlreadyResolved", err)
	}
	if _, err := st.ResolveCase(context.Background(), 999999, "x"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("resolving a missing case: got %v, want ErrNotFound", err)
	}
	again := process(t, st, r, alert("d1", "s1", wire.AlertThermalCritical, 94, t0.Add(time.Hour)))
	expect(t, again, OutcomeOpened, ScopeDevice)
	if again.CaseID == first.CaseID {
		t.Fatal("a fault after resolution is a new case")
	}
}

// Five devices at one site overheating within ten minutes is one site case.
// The cases already opened become its children; later devices join it.
func TestStormOpensOneSiteCase(t *testing.T) {
	st, r := testStore(t), testRules(t)
	for i := 1; i <= 4; i++ {
		expect(t, process(t, st, r, alert(fmt.Sprintf("d%d", i), "s1", wire.AlertThermalCritical, 93, t0.Add(time.Duration(i)*time.Minute))), OutcomeOpened, ScopeDevice)
	}
	storm := process(t, st, r, alert("d5", "s1", wire.AlertThermalCritical, 93, t0.Add(5*time.Minute)))
	expect(t, storm, OutcomeOpened, ScopeSite)
	if storm.Adopted != 4 {
		t.Fatalf("the four open device cases should become children, adopted %d", storm.Adopted)
	}
	later := process(t, st, r, alert("d6", "s1", wire.AlertThermalCritical, 93, t0.Add(6*time.Minute)))
	expect(t, later, OutcomeAttached, ScopeSite)
	repeat := process(t, st, r, alert("d1", "s1", wire.AlertThermalCritical, 93, t0.Add(7*time.Minute)))
	expect(t, repeat, OutcomeAttached, ScopeSite)
	expect(t, process(t, st, r, alert("x1", "s2", wire.AlertThermalCritical, 93, t0)), OutcomeOpened, ScopeDevice)

	d, err := st.GetCase(context.Background(), storm.CaseID)
	if err != nil {
		t.Fatal(err)
	}
	if d.Scope != ScopeSite || d.DeviceID != "" || len(d.Children) != 4 || d.Devices != 6 || len(d.Alerts) != 7 {
		t.Fatalf("site case: scope %s, %d children, %d devices, %d alerts; want site, 4, 6, 7", d.Scope, len(d.Children), d.Devices, len(d.Alerts))
	}
	children, err := st.ResolveCase(context.Background(), storm.CaseID, "chiller repaired")
	if err != nil || children != 4 {
		t.Fatalf("resolving the site case should resolve its 4 children: %d, %v", children, err)
	}
	if n := count(t, st, `SELECT count(*) FROM cases.cases WHERE status = 'open' AND site_id = 's1'`); n != 0 {
		t.Fatalf("%d cases still open at the site", n)
	}
}

// Every device of a site overheating at once, processed by many concurrent
// consumers, still yields exactly one site case and loses no alert.
func TestConcurrentStormYieldsOneSiteCase(t *testing.T) {
	st, r := testStore(t), testRules(t)
	const devices = 30
	var wg sync.WaitGroup
	errs := make(chan error, devices)
	for i := 0; i < devices; i++ {
		env := alert(fmt.Sprintf("d%02d", i), "s1", wire.AlertThermalCritical, 93, t0.Add(time.Duration(i)*time.Second))
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := st.ProcessAlert(context.Background(), env, r); err != nil {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	site := count(t, st, `SELECT count(*) FROM cases.cases WHERE scope = 'site'`)
	deviceCases := count(t, st, `SELECT count(*) FROM cases.cases WHERE scope = 'device'`)
	orphans := count(t, st, `SELECT count(*) FROM cases.cases WHERE scope = 'device' AND parent_case_id IS NULL`)
	inCases := count(t, st, `SELECT count(*) FROM cases.alerts WHERE case_id IS NOT NULL`)
	if site != 1 || deviceCases != 4 || orphans != 0 || inCases != devices {
		t.Fatalf("site cases %d (want 1), device cases %d (want 4), orphaned device cases %d (want 0), alerts in cases %d (want %d)",
			site, deviceCases, orphans, inCases, devices)
	}
}

// The same device's alerts racing (a rebalance, a replay overlapping live
// traffic) open one case; the unique index sends the losers to the winner.
func TestConcurrentAlertsFromOneDeviceOpenOneCase(t *testing.T) {
	st, r := testStore(t), testRules(t)
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		env := alert("d1", "s1", wire.AlertDiskReallocated, 9, t0)
		wg.Add(2)
		for k := 0; k < 2; k++ { // each alert delivered twice, concurrently
			go func() {
				defer wg.Done()
				if _, err := st.ProcessAlert(context.Background(), env, r); err != nil {
					t.Error(err)
				}
			}()
		}
	}
	wg.Wait()
	if n := count(t, st, `SELECT count(*) FROM cases.cases`); n != 1 {
		t.Fatalf("%d cases, want 1", n)
	}
	if n := count(t, st, `SELECT alert_count FROM cases.cases`); n != 10 {
		t.Fatalf("alert_count %d, want 10: duplicates counted or alerts lost", n)
	}
}

func TestLogsAttachWhicheverArrivesFirst(t *testing.T) {
	st, r := testStore(t), testRules(t)
	ctx := context.Background()
	bundle := func(id, device string) wire.BundleUploadedEvent {
		return wire.BundleUploadedEvent{BundleID: id, DeviceID: device, Reason: wire.ReasonAlert,
			ObjectKey: "bundles/" + device + "/" + id + ".tar.gz", SizeBytes: 1234, CollectedAt: t0, UploadedAt: t0.Add(time.Minute)}
	}
	logs := func(id int64) string {
		d, err := st.GetCase(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		return d.Logs
	}

	// Alert first, bundle later.
	a := alert("d1", "s1", wire.AlertThermalCritical, 93, t0)
	a.Alert.BundleID = "b1"
	c1 := process(t, st, r, a)
	if got := logs(c1.CaseID); got != LogsWaiting {
		t.Fatalf("before the bundle arrives: %s, want waiting", got)
	}
	if s, _ := st.Stats(ctx); s.WaitingForLogs != 1 {
		t.Fatalf("stats should count 1 case waiting for logs, got %+v", s)
	}
	if dup, err := st.SaveBundle(ctx, bundle("b1", "d1")); err != nil || dup {
		t.Fatalf("SaveBundle: dup %v err %v", dup, err)
	}
	if dup, _ := st.SaveBundle(ctx, bundle("b1", "d1")); !dup {
		t.Fatal("the same bundle event twice should report a duplicate")
	}
	if got := logs(c1.CaseID); got != LogsAttached {
		t.Fatalf("after the bundle arrives: %s, want attached", got)
	}

	// Bundle first, alert later.
	if _, err := st.SaveBundle(ctx, bundle("b2", "d2")); err != nil {
		t.Fatal(err)
	}
	b := alert("d2", "s1", wire.AlertThermalCritical, 93, t0)
	b.Alert.BundleID = "b2"
	c2 := process(t, st, r, b)
	d, err := st.GetCase(ctx, c2.CaseID)
	if err != nil {
		t.Fatal(err)
	}
	if d.Logs != LogsAttached || len(d.Bundles) != 1 || d.Bundles[0].ObjectKey == "" {
		t.Fatalf("a bundle that arrived first should attach at once: %+v", d.Bundles)
	}

	// No bundle named at all.
	c3 := process(t, st, r, alert("d3", "s9", wire.AlertThermalCritical, 93, t0))
	if got := logs(c3.CaseID); got != LogsNone {
		t.Fatalf("a case whose alerts named no bundle: %s, want none", got)
	}
	if s, _ := st.Stats(ctx); s.WaitingForLogs != 0 || s.Open[[2]string{"thermal-critical", ScopeDevice}] != 3 {
		t.Fatalf("unexpected stats %+v", s)
	}
}

// The telemetry processor migrates the same database with its own
// public.schema_migrations. The two must not see each other's versions.
func TestMigrationsAreIndependentOfTheProcessors(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	if _, err := st.db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS public.schema_migrations (version TEXT PRIMARY KEY);
		INSERT INTO public.schema_migrations VALUES ('001_cases') ON CONFLICT DO NOTHING`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.db.Exec(`DELETE FROM public.schema_migrations WHERE version = '001_cases'`) }) //nolint:errcheck
	if _, err := st.db.ExecContext(ctx, `DROP SCHEMA cases CASCADE`); err != nil {
		t.Fatal(err)
	}
	if err := st.Migrate(ctx, quiet); err != nil {
		t.Fatal(err)
	}
	if err := st.Migrate(ctx, quiet); err != nil {
		t.Fatalf("migrating twice must be a no-op: %v", err)
	}
	var exists bool
	if err := st.db.QueryRow(`SELECT to_regclass('cases.cases') IS NOT NULL`).Scan(&exists); err != nil || !exists {
		t.Fatal("cases.cases should exist even though public.schema_migrations has a 001_cases row")
	}
}
