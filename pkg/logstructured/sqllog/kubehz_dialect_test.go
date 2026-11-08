package sqllog

// KUBEHZ-PATCH P2 test helpers: a fake dialect and a log constructor for the
// hermetic P2 tests in this package (kubehz_quota_test.go,
// kubehz_relief_test.go). Up to v0.17.0-kubehz.4 they were in P1's test file;
// P1 was dropped on 2026-11-08.

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"time"

	"github.com/k3s-io/kine/pkg/drivers"
	"github.com/k3s-io/kine/pkg/query"
	"github.com/k3s-io/kine/pkg/server"
)

var errNotUsed = errors.New("not used by this test")

// newTestLog is New with a 1 s poll interval (startWatch refuses none) and
// compact-batch-size batch.
func newTestLog(d server.Dialect, batch int64) *SQLLog {
	return New(d, &sync.WaitGroup{}, &drivers.Config{CompactBatchSize: batch, PollInterval: time.Second})
}

// fakeDialect is a server.Dialect for tests that embed it and override what
// they use. A poller that the relief starts calls PrepareAfter, which
// succeeds, and then QueryAfter, which fails with context.Canceled on
// purpose: poll logs nothing for it, drops the statement and returns to its
// select. Every other method fails with errNotUsed.
type fakeDialect struct{}

func (d *fakeDialect) PrepareAfter(context.Context, int64) (*query.Stmt, error) {
	return &query.Stmt{}, nil
}

func (d *fakeDialect) QueryAfter(context.Context, *query.Stmt, int64) (*sql.Rows, error) {
	return nil, context.Canceled
}

func (d *fakeDialect) After(context.Context, string, string, int64, int64) (*sql.Rows, error) {
	return nil, errNotUsed
}

func (d *fakeDialect) CurrentRevision(context.Context) (int64, error) { return 1, nil }

func (d *fakeDialect) ListCurrent(context.Context, string, string, int64, bool, bool) (*sql.Rows, error) {
	return nil, errNotUsed
}

func (d *fakeDialect) List(context.Context, string, string, int64, int64, bool, bool) (*sql.Rows, error) {
	return nil, errNotUsed
}
func (d *fakeDialect) CountCurrent(context.Context, string, string) (int64, int64, error) {
	return 0, 0, errNotUsed
}

func (d *fakeDialect) Count(context.Context, string, string, int64) (int64, int64, int64, error) {
	return 0, 0, 0, errNotUsed
}

func (d *fakeDialect) Insert(context.Context, string, bool, bool, int64, int64, []byte) (int64, error) {
	return 0, errNotUsed
}
func (d *fakeDialect) DeleteRevision(context.Context, int64) error          { return errNotUsed }
func (d *fakeDialect) GetCompactRevision(context.Context) (int64, error)    { return 0, errNotUsed }
func (d *fakeDialect) SetCompactRevision(context.Context, int64) error      { return errNotUsed }
func (d *fakeDialect) Compact(context.Context, int64, int64) (int64, error) { return 0, errNotUsed }
func (d *fakeDialect) PostCompact(context.Context) error                    { return errNotUsed }
func (d *fakeDialect) Fill(context.Context, int64) error                    { return errNotUsed }
func (d *fakeDialect) IsFill(string) bool                                   { return false }
func (d *fakeDialect) IsTxRetry(error) bool                                 { return false }
func (d *fakeDialect) BeginTx(context.Context, *sql.TxOptions) (server.Transaction, error) {
	return nil, errNotUsed
}
func (d *fakeDialect) GetSize(context.Context) (int64, error) { return 0, errNotUsed }
func (d *fakeDialect) FillRetryDelay(context.Context)         {}
func (d *fakeDialect) TranslateStartKey(k string) string      { return k }
