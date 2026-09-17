package pgsql

// KUBEHZ-PATCH P3 tests. Hermetic: no PostgreSQL. The ALTER is chosen exactly
// when pg_class.reloptions differ, and never on CockroachDB. The CREATE TABLE
// statement stays upstream's. The database round trip (setRelOptions) runs
// in TestKubehzP2P3Postgres against a real PostgreSQL.

import (
	"strings"
	"testing"
)

const alterBoth = "ALTER TABLE kine SET (autovacuum_analyze_scale_factor = 0.02, autovacuum_vacuum_scale_factor = 0.05)"

func TestKubehzP3CreateTableIsUpstreams(t *testing.T) {
	create := schema[0]
	if !strings.HasPrefix(strings.TrimSpace(create), "CREATE TABLE IF NOT EXISTS kine") {
		t.Fatalf("schema[0] is not the CREATE TABLE statement: %q", create)
	}
	if strings.Contains(create, "WITH (") || strings.Contains(create, "autovacuum") {
		t.Fatalf("CREATE TABLE carries storage parameters; they belong to the ALTER path (CockroachDB rejects them):\n%s", create)
	}
}

func TestKubehzP3AlterExactlyWhenDiffer(t *testing.T) {
	for _, tc := range []struct {
		name      string
		cockroach bool
		current   string // pg_class.reloptions joined with ","
		want      string // the ALTER statement, "" when none must run
	}{
		{"fresh table, no reloptions: ALTER both", false, "", alterBoth},
		{"both already set", false, "autovacuum_vacuum_scale_factor=0.05,autovacuum_analyze_scale_factor=0.02", ""},
		{"both set in the other order with an unrelated option", false, "fillfactor=70,autovacuum_analyze_scale_factor=0.02,autovacuum_vacuum_scale_factor=0.05", ""},
		{"only vacuum set", false, "autovacuum_vacuum_scale_factor=0.05", "ALTER TABLE kine SET (autovacuum_analyze_scale_factor = 0.02)"},
		{"analyze set, vacuum at the default", false, "autovacuum_analyze_scale_factor=0.02,autovacuum_vacuum_scale_factor=0.2", "ALTER TABLE kine SET (autovacuum_vacuum_scale_factor = 0.05)"},
		{"unrelated option only", false, "fillfactor=70", alterBoth},
		{"CockroachDB: no statement", true, "", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := relOptionsPlan(tc.cockroach, tc.current); got != tc.want {
				t.Fatalf("cockroach=%v reloptions %q:\n got %q\nwant %q", tc.cockroach, tc.current, got, tc.want)
			}
		})
	}
}
