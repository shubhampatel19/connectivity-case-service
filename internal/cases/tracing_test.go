package cases

import (
	"context"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/shubhampatel19/connectivity-contracts/wire"
)

func TestAlertDecisionJoinsTheDevicesTrace(t *testing.T) {
	spans := tracetest.NewSpanRecorder()
	prevTP, prevProp := otel.GetTracerProvider(), otel.GetTextMapPropagator()
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(spans)))
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() { otel.SetTracerProvider(prevTP); otel.SetTextMapPropagator(prevProp) })

	const deviceTrace = "4bf92f3577b34da6a3ce929d0e0e4736"
	c := testConsumer(&fakeStore{result: Result{Outcome: OutcomeOpened, RuleID: "thermal-critical", Scope: ScopeDevice, CaseID: 12}})
	rec := record(t, wire.TopicAlerts, alert("d1", "s1", wire.AlertThermalCritical, 93, t0))
	rec.Headers = []kgo.RecordHeader{{Key: "traceparent", Value: []byte("00-" + deviceTrace + "-00f067aa0ba902b7-01")}}

	if err := c.handleWithRetry(context.Background(), rec); err != nil {
		t.Fatal(err)
	}
	ended := spans.Ended()
	if len(ended) != 1 {
		t.Fatalf("want 1 span, got %d", len(ended))
	}
	s := ended[0]
	if s.SpanKind() != trace.SpanKindConsumer || s.SpanContext().TraceID().String() != deviceTrace {
		t.Errorf("span kind %v in trace %s; want a consumer span in the device's trace", s.SpanKind(), s.SpanContext().TraceID())
	}
	got := map[string]string{}
	for _, kv := range s.Attributes() {
		got[string(kv.Key)] = kv.Value.Emit()
	}
	for k, want := range map[string]string{"outcome": "opened", "case.id": "12", "case.rule": "thermal-critical", "device.id": "d1"} {
		if got[k] != want {
			t.Errorf("%s = %q, want %q", k, got[k], want)
		}
	}
}
