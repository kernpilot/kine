package pgsql

// KUBEHZ-PATCH P3 — storage parameters on the kine table. This whole file is
// ours; see kubehz/MERGE-GUIDE.md#p3.
//
// INTENT: the kine table carries autovacuum_vacuum_scale_factor = 0.05 and
//   autovacuum_analyze_scale_factor = 0.02 (PostgreSQL's defaults are 0.2
//   and 0.1), set through one idempotent ALTER TABLE ... SET (...) at
//   startup when pg_class.reloptions differ. A new table and an upgraded one
//   take the same path; the CREATE TABLE statement stays upstream's. Two
//   reasons:
//   1. A lower vacuum threshold shrinks the plateau slack the file carries
//      above live data, the slack kine_db_size_bytes shows (P2).
//   2. P2's compared figure takes avg_width from the last ANALYZE. A low
//      analyze threshold keeps the limit honest after the row shape changes
//      (larger objects, a new CRD).
//
// Not a correctness patch: if the ALTER fails (a role without ALTER on the
// table, or a lock held past the bound) kine logs a warning and starts. On
// CockroachDB, which has no table storage parameters, no statement runs.
// Only the pgsql driver has this; sqlite and the others are untouched.

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"strings"

	"github.com/sirupsen/logrus"
)

// cockroachDB is set by setup()'s version probe. New() reads it right after.
var cockroachDB bool

// kineRelOptions are the storage parameters the kine table must carry.
var kineRelOptions = map[string]string{
	"autovacuum_vacuum_scale_factor":  "0.05",
	"autovacuum_analyze_scale_factor": "0.02",
}

// relOptionsPlan decides the statement to run: "" for CockroachDB and for a
// table that already carries the parameters, else the ALTER for the ones
// that are absent from or differ in current, which is pg_class.reloptions
// joined with "," (each entry is "key=value"). Unrelated options in current
// are left alone.
func relOptionsPlan(cockroach bool, current string) string {
	if cockroach {
		return ""
	}
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
	if len(missing) == 0 {
		return ""
	}
	return "ALTER TABLE kine SET (" + renderRelOptions(missing) + ")"
}

// setRelOptions reads the table's reloptions and runs the plan. It is
// idempotent: a table that already carries them causes no statement and no
// log line. Both statements are bounded by kubehzStatementTimeout.
func setRelOptions(ctx context.Context, db *sql.DB, cockroach bool) error {
	if cockroach {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, kubehzStatementTimeout)
	defer cancel()
	var current string
	err := db.QueryRowContext(ctx,
		`SELECT COALESCE(array_to_string(reloptions, ','), '') FROM pg_class WHERE oid = 'kine'::regclass`,
	).Scan(&current)
	if err != nil {
		return fmt.Errorf("reading the kine table's storage parameters: %w", err)
	}
	stmt := relOptionsPlan(cockroach, current)
	if stmt == "" {
		return nil
	}
	if _, err := db.ExecContext(ctx, stmt); err != nil {
		return fmt.Errorf("setting the kine table's storage parameters: %w", err)
	}
	logrus.Infof("Set storage parameters on the kine table: %s", strings.TrimSuffix(strings.TrimPrefix(stmt, "ALTER TABLE kine SET ("), ")"))
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
