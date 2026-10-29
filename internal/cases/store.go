// Package cases turns device alerts into support cases. A rule set decides
// which alerts deserve a case; three layers of deduplication keep one fault
// from becoming many cases; log bundles are attached as they arrive.
package cases

import (
	"context"
	"database/sql"
	"fmt"
	"io/fs"
	"log/slog"
	"sort"
	"strings"
	"time"

	_ "github.com/lib/pq"

	"github.com/shubhampatel19/connectivity-case-service/migrations"
	"github.com/shubhampatel19/connectivity-contracts/wire"
)

// migrationLockID names the advisory lock that serializes migrations across
// replicas. It differs from the telemetry processor's, which migrates the
// same database at the same time.
const migrationLockID = 727_004

type Store struct{ db *sql.DB }

// OpenStore connects to Postgres, waiting for it to come up: containers start
// in any order, and a service that crash-loops while its database boots is
// noise in your incident timeline.
func OpenStore(ctx context.Context, dsn string, log *slog.Logger) (*Store, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(10)
	db.SetConnMaxLifetime(30 * time.Minute)

	for attempt := 1; ; attempt++ {
		pingCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		err = db.PingContext(pingCtx)
		cancel()
		if err == nil {
			return &Store{db: db}, nil
		}
		if attempt >= 30 || ctx.Err() != nil {
			db.Close()
			return nil, fmt.Errorf("postgres not reachable: %w", err)
		}
		log.Warn("waiting for postgres", "attempt", attempt, "err", err)
		select {
		case <-ctx.Done():
		case <-time.After(2 * time.Second):
		}
	}
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) Ping(ctx context.Context) error { return s.db.PingContext(ctx) }

// Migrate applies every embedded migration not yet recorded in
// cases.schema_migrations, each in its own transaction, under an advisory
// lock so replicas starting together take turns.
func (s *Store) Migrate(ctx context.Context, log *slog.Logger) error {
	conn, err := s.db.Conn(ctx) // advisory locks belong to one session
	if err != nil {
		return err
	}
	defer conn.Close()

	if _, err := conn.ExecContext(ctx, `SELECT pg_advisory_lock($1)`, migrationLockID); err != nil {
		return fmt.Errorf("take migration lock: %w", err)
	}
	defer conn.ExecContext(context.Background(), `SELECT pg_advisory_unlock($1)`, migrationLockID) //nolint:errcheck

	if _, err := conn.ExecContext(ctx, `CREATE SCHEMA IF NOT EXISTS cases;
		CREATE TABLE IF NOT EXISTS cases.schema_migrations (
			version    TEXT PRIMARY KEY,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`); err != nil {
		return err
	}
	applied := map[string]bool{}
	rows, err := conn.QueryContext(ctx, `SELECT version FROM cases.schema_migrations`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			rows.Close()
			return err
		}
		applied[v] = true
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}

	names, err := fs.Glob(migrations.FS, "*.sql")
	if err != nil {
		return err
	}
	sort.Strings(names)
	for _, name := range names {
		version := strings.TrimSuffix(name, ".sql")
		if applied[version] {
			continue
		}
		body, err := fs.ReadFile(migrations.FS, name)
		if err != nil {
			return err
		}
		tx, err := conn.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, string(body)); err != nil {
			tx.Rollback() //nolint:errcheck
			return fmt.Errorf("migration %s: %w", version, err)
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO cases.schema_migrations (version) VALUES ($1)`, version); err != nil {
			tx.Rollback() //nolint:errcheck
			return err
		}
		if err := tx.Commit(); err != nil {
			return err
		}
		log.Info("applied migration", "version", version)
	}
	return nil
}

// SaveBundle indexes an uploaded log bundle. duplicate is true when it was
// already indexed, which is normal: bundle events can be published twice.
func (s *Store) SaveBundle(ctx context.Context, ev wire.BundleUploadedEvent) (duplicate bool, err error) {
	res, err := s.db.ExecContext(ctx, `
		INSERT INTO cases.bundles (bundle_id, device_id, reason, object_key, size_bytes, collected_at, uploaded_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (bundle_id) DO NOTHING`,
		ev.BundleID, ev.DeviceID, string(ev.Reason), ev.ObjectKey, ev.SizeBytes, ev.CollectedAt, ev.UploadedAt)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n == 0, err
}
