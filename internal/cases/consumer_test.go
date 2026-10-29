package cases

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/shubhampatel19/connectivity-contracts/wire"
)

type fakeStore struct {
	alerts  []wire.AlertEnvelope
	bundles []wire.BundleUploadedEvent
	result  Result
	err     error
}

func (f *fakeStore) ProcessAlert(_ context.Context, env wire.AlertEnvelope, _ *Rules) (Result, error) {
	if f.err != nil {
		return Result{}, f.err
	}
	f.alerts = append(f.alerts, env)
	return f.result, nil
}

func (f *fakeStore) SaveBundle(_ context.Context, ev wire.BundleUploadedEvent) (bool, error) {
	f.bundles = append(f.bundles, ev)
	return false, f.err
}

func testConsumer(st store) *Consumer {
	c := newConsumer(ConsumerConfig{}, nil, st, nil, quiet, NewMetrics(prometheus.NewRegistry()))
	c.now = func() time.Time { return t0.Add(3 * time.Second) }
	return c
}

func record(t *testing.T, topic string, v any) *kgo.Record {
	t.Helper()
	b, ok := v.([]byte)
	if !ok {
		var err error
		if b, err = json.Marshal(v); err != nil {
			t.Fatal(err)
		}
	}
	return &kgo.Record{Topic: topic, Value: b}
}

func TestConsumerHandlesAlertsAndCountsOutcomes(t *testing.T) {
	st := &fakeStore{result: Result{Outcome: OutcomeOpened, RuleID: "thermal-critical", Scope: ScopeSite, CaseID: 7, Adopted: 3}}
	c := testConsumer(st)
	env := alert("d1", "s1", wire.AlertThermalCritical, 93, t0)
	if err := c.handle(context.Background(), record(t, wire.TopicAlerts, env)); err != nil {
		t.Fatal(err)
	}
	if len(st.alerts) != 1 || st.alerts[0].Alert.AlertID != env.Alert.AlertID {
		t.Fatalf("store should get the alert, got %+v", st.alerts)
	}
	if got := testutil.ToFloat64(c.m.casesOpened.WithLabelValues("thermal-critical", ScopeSite)); got != 1 {
		t.Fatalf("cases opened = %v", got)
	}
	if got := testutil.ToFloat64(c.m.adopted); got != 3 {
		t.Fatalf("adopted = %v", got)
	}
}

// An alert from long ago, replayed today, is still valid: validation uses the
// time ingestion received it, not the time the case service reads it.
func TestReplayedOldAlertIsStillValid(t *testing.T) {
	st := &fakeStore{result: Result{Outcome: OutcomeDuplicate}}
	c := testConsumer(st)
	c.now = func() time.Time { return t0.Add(90 * 24 * time.Hour) }
	if err := c.handle(context.Background(), record(t, wire.TopicAlerts, alert("d1", "s1", wire.AlertThermalCritical, 93, t0))); err != nil {
		t.Fatalf("a replayed alert must not be rejected for its age: %v", err)
	}
}

func TestUnprocessableRecordsArePoisonNotRetried(t *testing.T) {
	bad := alert("d1", "s1", wire.AlertThermalCritical, 93, t0)
	bad.Alert.Severity = "urgent"
	noReceipt := alert("d1", "s1", wire.AlertThermalCritical, 93, t0)
	noReceipt.ReceivedAt = time.Time{}
	for name, rec := range map[string]*kgo.Record{
		"bad json":          record(t, wire.TopicAlerts, []byte("{not json")),
		"invalid alert":     record(t, wire.TopicAlerts, bad),
		"no received_at":    record(t, wire.TopicAlerts, noReceipt),
		"bundle without id": record(t, wire.TopicBundles, wire.BundleUploadedEvent{DeviceID: "d1"}),
		"unknown topic":     record(t, "telemetry.raw", map[string]string{}),
	} {
		st := &fakeStore{}
		err := testConsumer(st).handle(context.Background(), rec)
		if !errors.Is(err, errPoison) {
			t.Errorf("%s: got %v, want a poison error", name, err)
		}
		if len(st.alerts) != 0 {
			t.Errorf("%s: nothing should reach the store", name)
		}
	}
}

func TestDatabaseErrorsAreRetriedNotSkipped(t *testing.T) {
	st := &fakeStore{err: errors.New("connection refused")}
	err := testConsumer(st).handle(context.Background(), record(t, wire.TopicAlerts, alert("d1", "s1", wire.AlertThermalCritical, 93, t0)))
	if err == nil || errors.Is(err, errPoison) {
		t.Fatalf("a database error must be retried, got %v", err)
	}
}
