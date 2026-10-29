// Command case-service turns device alerts into support cases.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/shubhampatel19/connectivity-case-service/internal/cases"
	"github.com/shubhampatel19/connectivity-go-kit/platform"
)

func main() {
	log := platform.NewLogger("case-service")
	if err := run(log); err != nil {
		log.Error("exiting", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	var env platform.Env
	brokers := env.List("KAFKA_BROKERS", "localhost:29092")
	group := env.String("CONSUMER_GROUP", "case-service")
	dsn := env.Required("DATABASE_URL")
	httpAddr := env.String("HTTP_ADDR", ":8085")
	rulesFile := env.String("RULES_FILE", "")
	maxPoll := env.Int("MAX_POLL_RECORDS", 200)
	statsEvery := env.Duration("STATS_INTERVAL", 15*time.Second)
	env.Check(maxPoll > 0, "MAX_POLL_RECORDS must be positive")
	env.Check(statsEvery > 0, "STATS_INTERVAL must be positive")
	if err := env.Err(); err != nil {
		return err
	}

	// Bad rules stop the service before it consumes anything: running with a
	// rule set nobody intended is worse than not running.
	rules, err := cases.LoadRules(rulesFile)
	if err != nil {
		return fmt.Errorf("load rules: %w", err)
	}
	for _, r := range rules.List() {
		log.Info("rule loaded", "id", r.ID, "code", r.Code, "severity", r.Severity,
			"min_value", r.MinValue, "threshold", r.Threshold, "storm", r.Storm)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	store, err := cases.OpenStore(ctx, dsn, log)
	if err != nil {
		return err
	}
	defer store.Close()
	if err := store.Migrate(ctx, log); err != nil {
		return fmt.Errorf("migrate database: %w", err)
	}

	reg := platform.NewRegistry()
	m := cases.NewMetrics(reg)
	consumer, err := cases.NewConsumer(cases.ConsumerConfig{Brokers: brokers, Group: group, MaxPoll: maxPoll}, store, rules, log, m)
	if err != nil {
		return err
	}
	defer consumer.Close()

	// If the API server dies, take the process down with it: a case service
	// support can't query is broken even if it is still consuming.
	ctx, stop := context.WithCancel(ctx)
	defer stop()
	go func() {
		api := cases.NewAPI(store, rules, log, m)
		if err := platform.Serve(ctx, log, httpAddr, api.Handler(platform.MetricsHandler(reg))); err != nil {
			log.Error("http server failed", "err", err)
			stop()
		}
	}()
	go cases.RefreshStats(ctx, store, statsEvery, log, m)

	return consumer.Run(ctx)
}
