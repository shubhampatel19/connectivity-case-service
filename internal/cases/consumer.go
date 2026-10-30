package cases

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/shubhampatel19/connectivity-contracts/wire"
	"github.com/shubhampatel19/connectivity-go-kit/platform"
)

// errPoison marks a record that can never succeed (bad JSON, an invalid
// alert). Those are logged, counted and skipped; retrying them would block
// the partition forever.
var errPoison = errors.New("poison record")

type store interface {
	ProcessAlert(ctx context.Context, env wire.AlertEnvelope, rules *Rules) (Result, error)
	SaveBundle(ctx context.Context, ev wire.BundleUploadedEvent) (duplicate bool, err error)
}

type ConsumerConfig struct {
	Brokers  []string
	Group    string
	MaxPoll  int
	MaxRetry time.Duration
}

// Consumer reads device.alerts and bundles.uploaded. Delivery is
// at-least-once: offsets are committed only after the database transaction,
// so a crash replays records, and the alerts table's primary key turns each
// replayed alert into a no-op.
type Consumer struct {
	cfg   ConsumerConfig
	cl    *kgo.Client
	store store
	rules *Rules
	log   *slog.Logger
	m     *Metrics
	now   func() time.Time
}

func NewConsumer(cfg ConsumerConfig, st store, rules *Rules, log *slog.Logger, m *Metrics) (*Consumer, error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumerGroup(cfg.Group),
		kgo.ConsumeTopics(wire.TopicAlerts, wire.TopicBundles),
		// A new group starts from the beginning: a case service deployed
		// after devices began alerting still sees every alert.
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		kgo.FetchMaxWait(500*time.Millisecond),
	)
	if err != nil {
		return nil, fmt.Errorf("create kafka consumer: %w", err)
	}
	return newConsumer(cfg, cl, st, rules, log, m), nil
}

func newConsumer(cfg ConsumerConfig, cl *kgo.Client, st store, rules *Rules, log *slog.Logger, m *Metrics) *Consumer {
	if cfg.MaxPoll <= 0 {
		cfg.MaxPoll = 200
	}
	if cfg.MaxRetry <= 0 {
		cfg.MaxRetry = 10 * time.Second
	}
	return &Consumer{cfg: cfg, cl: cl, store: st, rules: rules, log: log, m: m, now: time.Now}
}

func (c *Consumer) Close() { c.cl.Close() }

// Run polls until ctx is cancelled.
func (c *Consumer) Run(ctx context.Context) error {
	c.log.Info("consumer started", "group", c.cfg.Group, "topics", []string{wire.TopicAlerts, wire.TopicBundles})
	for {
		fetches := c.cl.PollRecords(ctx, c.cfg.MaxPoll)
		if ctx.Err() != nil || fetches.IsClientClosed() {
			c.cl.AllowRebalance()
			return nil
		}
		fetches.EachError(func(topic string, partition int32, err error) {
			c.log.Error("fetch error", "topic", topic, "partition", partition, "err", err)
		})
		var stopped bool
		fetches.EachRecord(func(rec *kgo.Record) {
			if !stopped && c.handleWithRetry(ctx, rec) != nil {
				stopped = true // only on shutdown
			}
		})
		if stopped {
			c.cl.AllowRebalance() // leave this poll uncommitted; the next owner replays it
			return nil
		}
		if err := c.cl.CommitUncommittedOffsets(ctx); err != nil {
			c.m.commitErrors.Inc()
			c.log.Error("offset commit failed", "err", err)
		}
		c.cl.AllowRebalance()
	}
}

// handleWithRetry retries transient failures in place with capped backoff.
// Blocking the partition is deliberate: skipping ahead would reorder a
// device's alerts, and thresholds count them. Consumer lag shows it happening.
//
// Each record gets one consumer span that continues the device's trace from
// the record headers: the alert's trace runs from the device, through the
// gateway and the ingest API, to the case decision.
func (c *Consumer) handleWithRetry(ctx context.Context, rec *kgo.Record) (err error) {
	spanCtx, span := platform.Tracer().Start(platform.ContextFromHeaders(ctx, recordHeaders(rec)), "process "+rec.Topic,
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			attribute.String("messaging.system", "kafka"),
			attribute.String("messaging.destination.name", rec.Topic),
			attribute.String("messaging.consumer.group.name", c.cfg.Group),
			attribute.Int("messaging.destination.partition.id", int(rec.Partition)),
			attribute.Int64("messaging.kafka.offset", rec.Offset),
		))
	defer func() {
		platform.RecordError(spanCtx, err)
		span.End()
	}()
	ctx = spanCtx

	backoff := 200 * time.Millisecond
	for attempt := 1; ; attempt++ {
		start := time.Now()
		err := c.handle(ctx, rec)
		if err == nil {
			c.m.duration.WithLabelValues(rec.Topic).Observe(time.Since(start).Seconds())
			return nil
		}
		if errors.Is(err, errPoison) {
			span.SetAttributes(attribute.String("outcome", "poison"))
			platform.RecordError(ctx, err)
			c.log.ErrorContext(ctx, "skipping unprocessable record", "topic", rec.Topic, "partition", rec.Partition, "offset", rec.Offset, "err", err)
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		c.m.retries.WithLabelValues(rec.Topic).Inc()
		span.AddEvent("retry", trace.WithAttributes(attribute.Int("attempt", attempt), attribute.String("error", err.Error())))
		c.log.WarnContext(ctx, "record failed, retrying", "topic", rec.Topic, "partition", rec.Partition, "offset", rec.Offset,
			"attempt", attempt, "retry_in", backoff, "err", err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, c.cfg.MaxRetry)
	}
}

func (c *Consumer) handle(ctx context.Context, rec *kgo.Record) error {
	switch rec.Topic {
	case wire.TopicAlerts:
		var env wire.AlertEnvelope
		if err := json.Unmarshal(rec.Value, &env); err != nil {
			c.m.alertsRejected.WithLabelValues("bad_json").Inc()
			return fmt.Errorf("%w: %v", errPoison, err)
		}
		// Validate against when ingestion received it, not now: a replayed
		// week-old alert is still a valid alert.
		if err := env.Alert.Validate(env.ReceivedAt); err != nil || env.ReceivedAt.IsZero() {
			c.m.alertsRejected.WithLabelValues("invalid").Inc()
			return fmt.Errorf("%w: invalid alert %q: %v", errPoison, env.Alert.AlertID, err)
		}
		span := trace.SpanFromContext(ctx)
		span.SetAttributes(
			attribute.String("alert.id", env.Alert.AlertID),
			attribute.String("alert.code", string(env.Alert.Code)),
			attribute.String("device.id", env.Alert.DeviceID),
			attribute.String("site.id", env.Alert.SiteID),
		)
		res, err := c.store.ProcessAlert(ctx, env, c.rules)
		if err != nil {
			return err
		}
		span.SetAttributes(attribute.String("outcome", string(res.Outcome)))
		if res.CaseID != 0 {
			span.SetAttributes(attribute.Int64("case.id", res.CaseID), attribute.String("case.rule", res.RuleID), attribute.String("case.scope", res.Scope))
		}
		c.m.alerts.WithLabelValues(string(res.Outcome)).Inc()
		if res.Outcome == OutcomeDuplicate {
			return nil
		}
		c.m.decisionDelay.Observe(c.now().Sub(env.ReceivedAt).Seconds())
		if res.Outcome == OutcomeOpened {
			c.m.casesOpened.WithLabelValues(res.RuleID, res.Scope).Inc()
			c.m.adopted.Add(float64(res.Adopted))
			c.log.InfoContext(ctx, "case opened", "case_id", res.CaseID, "rule", res.RuleID, "scope", res.Scope,
				"device_id", env.Alert.DeviceID, "site_id", env.Alert.SiteID, "alert_id", env.Alert.AlertID, "adopted", res.Adopted)
		}
		return nil

	case wire.TopicBundles:
		var ev wire.BundleUploadedEvent
		if err := json.Unmarshal(rec.Value, &ev); err != nil {
			return fmt.Errorf("%w: %v", errPoison, err)
		}
		if ev.BundleID == "" || ev.DeviceID == "" || ev.ObjectKey == "" {
			return fmt.Errorf("%w: bundle event missing bundle id, device id or object key", errPoison)
		}
		trace.SpanFromContext(ctx).SetAttributes(attribute.String("device.id", ev.DeviceID), attribute.String("bundle.id", ev.BundleID))
		dup, err := c.store.SaveBundle(ctx, ev)
		if err != nil {
			return err
		}
		result := "stored"
		if dup {
			result = "duplicate"
		}
		trace.SpanFromContext(ctx).SetAttributes(attribute.String("outcome", result))
		c.m.bundles.WithLabelValues(result).Inc()
		return nil

	default:
		return fmt.Errorf("%w: unexpected topic %q", errPoison, rec.Topic)
	}
}

// RefreshStats keeps the case gauges current until ctx is cancelled.
func RefreshStats(ctx context.Context, st *Store, every time.Duration, log *slog.Logger, m *Metrics) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		qctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		s, err := st.Stats(qctx)
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			m.statsErrors.Inc()
			log.Warn("refreshing case gauges failed", "err", err)
		} else {
			m.openCases.Reset() // drop rule/scope pairs that no longer have open cases
			for k, n := range s.Open {
				m.openCases.WithLabelValues(k[0], k[1]).Set(float64(n))
			}
			m.waitingForLogs.Set(float64(s.WaitingForLogs))
			m.oldestWaiting.Set(s.OldestWaitingSeconds)
			m.statsLastSuccessTS.SetToCurrentTime()
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// recordHeaders turns Kafka record headers into the map the trace
// propagator reads.
func recordHeaders(rec *kgo.Record) map[string]string {
	m := make(map[string]string, len(rec.Headers))
	for _, h := range rec.Headers {
		m[h.Key] = string(h.Value)
	}
	return m
}
