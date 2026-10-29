// Package migrations holds the case service's schema as numbered SQL files,
// applied in filename order on startup and recorded in
// cases.schema_migrations.
//
// Rules: never edit a migration that has shipped; add a new one. Keep each
// one safe to re-run (IF NOT EXISTS), so a crash halfway through can't wedge
// the next start.
package migrations

import "embed"

//go:embed *.sql
var FS embed.FS
