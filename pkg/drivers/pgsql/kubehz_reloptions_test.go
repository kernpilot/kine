package pgsql

// KUBEHZ-PATCH P3 tests. Hermetic: no PostgreSQL. The CREATE TABLE statement
// carries the storage parameters, and the ALTER path is chosen exactly when
// pg_class.reloptions differ. The database round trip itself
// (ensureRelOptions) is not run here.

import (
	"strings"
	"testing"
)

const wantClause = "WITH (autovacuum_analyze_scale_factor = 0.02, autovacuum_vacuum_scale_factor = 0.05)"

func TestKubehzP3CreateTableCarriesRelOptions(t *testing.T) {
	if relOptionsClause() != wantClause {
		t.Fatalf("clause: %q, want %q", relOptionsClause(), wantClause)
	}
	create := schema[0]
	if !strings.HasPrefix(strings.TrimSpace(create), "CREATE TABLE IF NOT EXISTS kine") {
		t.Fatalf("schema[0] is not the CREATE TABLE statement: %q", create)
	}
	if !strings.Contains(create, wantClause) {
		t.Fatalf("CREATE TABLE does not carry the storage parameters:\n%s", create)
	}
	if strings.Index(create, wantClause) < strings.Index(create, ")") {
		t.Fatal("the WITH clause must follow the column list")
	}
}

func TestKubehzP3AlterExactlyWhenDiffer(t *testing.T) {
	for _, tc := range []struct {
		name    string
		current string // pg_class.reloptions joined with ","
		want    string // the ALTER statement, "" when none must run
	}{
		{"fresh table without options", "", "ALTER TABLE kine SET (autovacuum_analyze_scale_factor = 0.02, autovacuum_vacuum_scale_factor = 0.05)"},
		{"both already set", "autovacuum_vacuum_scale_factor=0.05,autovacuum_analyze_scale_factor=0.02", ""},
		{"both set in the other order with an unrelated option", "fillfactor=70,autovacuum_analyze_scale_factor=0.02,autovacuum_vacuum_scale_factor=0.05", ""},
		{"only vacuum set", "autovacuum_vacuum_scale_factor=0.05", "ALTER TABLE kine SET (autovacuum_analyze_scale_factor = 0.02)"},
		{"analyze set, vacuum at the default", "autovacuum_analyze_scale_factor=0.02,autovacuum_vacuum_scale_factor=0.2", "ALTER TABLE kine SET (autovacuum_vacuum_scale_factor = 0.05)"},
		{"unrelated option only", "fillfactor=70", "ALTER TABLE kine SET (autovacuum_analyze_scale_factor = 0.02, autovacuum_vacuum_scale_factor = 0.05)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			keys := relOptionsToSet(tc.current)
			got := ""
			if len(keys) > 0 {
				got = alterRelOptionsSQL(keys)
			}
			if got != tc.want {
				t.Fatalf("reloptions %q:\n got %q\nwant %q", tc.current, got, tc.want)
			}
		})
	}
}
