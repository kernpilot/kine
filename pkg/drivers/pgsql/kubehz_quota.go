package pgsql

// KUBEHZ-PATCH P2 (pgsql) — this whole file is ours; see kubehz/MERGE-GUIDE.md#p2.
// The live-bytes figure --quota-bytes compares on PostgreSQL.

import (
	"context"

	"github.com/k3s-io/kine/pkg/server"
)

// liveSizeSQL estimates the bytes of live heap tuples in the kine table:
// n_live_tup (the cumulative-statistics live row count, kept current by the
// stats collector and autovacuum) times the sum of the per-column average
// widths ANALYZE recorded in pg_stats. It costs two catalog lookups and no
// table scan. Indexes and page overhead are not counted; before the first
// ANALYZE pg_stats has no rows and the estimate is 0. pg_total_relation_size
// was rejected for this role: it is the file, which plateaus under
// autovacuum and stays large after a write-then-delete spike.
const liveSizeSQL = `
	SELECT COALESCE((
		SELECT s.n_live_tup * SUM(p.avg_width)
		FROM pg_stat_user_tables AS s
		JOIN pg_stats AS p ON p.schemaname = s.schemaname AND p.tablename = s.relname
		WHERE s.schemaname = current_schema() AND s.relname = 'kine'
		GROUP BY s.n_live_tup
	), 0)::bigint`

// compile-time check: the dialect handed to sqllog.New offers a live figure.
var _ server.LiveSizer = (*notifyingDialect)(nil)

// LiveSize implements server.LiveSizer.
func (d *notifyingDialect) LiveSize() server.SizeSource {
	return d.liveSize
}

func (d *notifyingDialect) liveSize(ctx context.Context) (int64, error) {
	var size int64
	if err := d.DB.QueryRowContext(ctx, liveSizeSQL).Scan(&size); err != nil {
		return 0, err
	}
	return size, nil
}
