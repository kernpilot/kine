package generic

// KUBEHZ-PATCH P6 — this whole file is ours; see kubehz/CHANGELOG.md, P6, and
// upstream issue k3s-io/kine#824. Tx.Insert calls it for the drivers without
// LastInsertID (PostgreSQL, CockroachDB).

import (
	"context"
	"database/sql"
	"errors"

	"github.com/k3s-io/kine/pkg/query"
	"github.com/sirupsen/logrus"
)

// insertSavepoint is the savepoint each insert attempt starts at.
const insertSavepoint = "kine_insert"

// insertAtSavepoint runs upstream's retryInsert on the transaction's own
// connection. Each attempt starts at insertSavepoint: the first attempt sets
// it, and a retry first rolls back to it, because a failed statement aborts
// a PostgreSQL transaction. The retry gets a new id from the sequence, as on
// the pool. A savepoint statement that fails is returned with the insert's
// error; the insert then fails too (the transaction is aborted or gone).
func (t *Tx) insertAtSavepoint(ctx context.Context, cols ...any) (int64, error) {
	var (
		attempts int
		spErr    error
	)
	queryRow := func(ctx context.Context, q *query.Named, args ...any) *sql.Row {
		stmt := "SAVEPOINT " + insertSavepoint
		if attempts > 0 {
			stmt = "ROLLBACK TO SAVEPOINT " + insertSavepoint
		}
		attempts++
		logrus.Tracef("TX EXEC: %s", stmt)
		if _, err := t.x.ExecContext(ctx, stmt); err != nil && spErr == nil {
			spErr = err
		}
		return t.queryRow(ctx, q, args...)
	}
	id, err := retryInsert(ctx, queryRow, t.d.InsertRetry, t.d.InsertReturningSQL, cols...)
	if err != nil && spErr != nil {
		err = errors.Join(spErr, err)
	}
	return id, err
}
