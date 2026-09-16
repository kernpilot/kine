package pgsql

// KUBEHZ-PATCH P3 — storage parameters on the kine table. This whole file is
// ours; see kubehz/MERGE-GUIDE.md#p3.
//
// INTENT: the kine table carries autovacuum_vacuum_scale_factor = 0.05 and
//   autovacuum_analyze_scale_factor = 0.02 (PostgreSQL's defaults are 0.2
//   and 0.1), on a new table through CREATE TABLE ... WITH (...) and on an
//   existing table through one ALTER TABLE ... SET (...) at startup when
//   pg_class.reloptions differ. Two reasons:
//   1. A lower vacuum threshold shrinks the plateau slack the file carries
//      above live data, the slack kine_db_size_bytes shows (P2).
//   2. P2's compared figure, n_live_tup × avg_width, takes the width from
//      the last ANALYZE. A low analyze threshold keeps the limit honest
//      after the row shape changes (larger objects, a new CRD).
//
// Not a correctness patch: if the ALTER fails (a role without ALTER on the
// table) kine logs a warning and starts. Only the pgsql driver has this;
// sqlite and the others are untouched by construction.

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"strings"

	"github.com/sirupsen/logrus"
)

// kineRelOptions are the storage parameters the kine table must carry.
var kineRelOptions = map[string]string{
	"autovacuum_vacuum_scale_factor":  "0.05",
	"autovacuum_analyze_scale_factor": "0.02",
}

// relOptionsClause renders the WITH (...) clause for CREATE TABLE, keys
// sorted so the statement is stable.
func relOptionsClause() string {
	return "WITH (" + renderRelOptions(sortedRelOptionKeys()) + ")"
}

// relOptionsToSet returns the wanted options that are absent from or differ
// in current, which is pg_class.reloptions joined with "," (each entry is
// "key=value"). Unrelated options in current are left alone.
func relOptionsToSet(current string) []string {
	have := map[string]string{}
	for _, entry := range strings.Split(current, ",") {
		if key, value, ok := strings.Cut(strings.TrimSpace(entry), "="); ok {
			have[key] = value
		}
	}
	var missing []string
	for _, key := range sortedRelOptionKeys() {
		if have[key] != kineRelOptions[key] {
			missing = append(missing, key)
		}
	}
	return missing
}

// alterRelOptionsSQL is the statement that sets keys on the kine table.
func alterRelOptionsSQL(keys []string) string {
	return "ALTER TABLE kine SET (" + renderRelOptions(keys) + ")"
}

// ensureRelOptions reads the table's reloptions and sets the ones that
// differ. It is idempotent: a table that already carries them causes no
// statement and no log line.
func ensureRelOptions(ctx context.Context, db *sql.DB) error {
	var current string
	err := db.QueryRowContext(ctx,
		`SELECT COALESCE(array_to_string(reloptions, ','), '') FROM pg_class WHERE oid = 'kine'::regclass`,
	).Scan(&current)
	if err != nil {
		return fmt.Errorf("reading the kine table's storage parameters: %w", err)
	}
	keys := relOptionsToSet(current)
	if len(keys) == 0 {
		return nil
	}
	if _, err := db.ExecContext(ctx, alterRelOptionsSQL(keys)); err != nil {
		return fmt.Errorf("setting the kine table's storage parameters: %w", err)
	}
	logrus.Infof("Set storage parameters on the kine table: %s", renderRelOptions(keys))
	return nil
}

func sortedRelOptionKeys() []string {
	keys := make([]string, 0, len(kineRelOptions))
	for key := range kineRelOptions {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return keys
}

func renderRelOptions(keys []string) string {
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, key+" = "+kineRelOptions[key])
	}
	return strings.Join(parts, ", ")
}
