package pgsql

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/sirupsen/logrus"
)

// RevisionChannel is the PostgreSQL notification channel carrying newly
// inserted revisions. The payload is the revision id as decimal text and
// nothing else: NOTIFY payloads are capped at 8000 bytes, so a value must
// never travel this way.
const RevisionChannel = "kine_revision"

// notifyInterval bounds how often a revision notification is emitted.
//
// This is the whole design. The obvious implementation — an AFTER INSERT
// trigger calling pg_notify per row — is unusable: measured against this
// benchmark it cut write throughput by 96.8% (5076 -> 163 puts/s) and pushed
// put p99 from 39.5 ms to 876 ms, because every backend serializes on the
// shared async-notification queue.
//
// A notification carries no data. It only tells another kine "poll now", and
// that poll drains every revision waiting. So one notification per interval is
// worth exactly as much as one per row, and costs a bounded amount instead of
// scaling with the write rate. At 10 ms the write path pays for at most 100
// notifications per second no matter how hard it is driven, and a cross-
// instance watcher waits ~10 ms instead of up to a second.
const notifyInterval = 10 * time.Millisecond

// revisionNotifier coalesces revision announcements onto one connection.
type revisionNotifier struct {
	latest atomic.Int64 // highest revision inserted by THIS kine instance
}

// record marks a revision as worth announcing. Called on the write path, so it
// must stay this cheap — no locks, no I/O.
func (r *revisionNotifier) record(rev int64) {
	for {
		cur := r.latest.Load()
		if rev <= cur || r.latest.CompareAndSwap(cur, rev) {
			return
		}
	}
}

// run announces the highest recorded revision at most once per interval, on a
// dedicated connection, entirely off the write path. A NOTIFY issued inside the
// inserting transaction would add its cost to every write; this cannot.
func (r *revisionNotifier) run(ctx context.Context, wg *sync.WaitGroup, config *pgx.ConnConfig) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		var sent int64
		for ctx.Err() == nil {
			conn, err := pgx.ConnectConfig(ctx, config)
			if err != nil {
				if !sleepCtx(ctx, time.Second) {
					return
				}
				continue
			}
			tick := time.NewTicker(notifyInterval)
			for ctx.Err() == nil {
				select {
				case <-ctx.Done():
					tick.Stop()
					conn.Close(context.Background())
					return
				case <-tick.C:
				}
				rev := r.latest.Load()
				if rev <= sent {
					continue
				}
				if _, err := conn.Exec(ctx, "SELECT pg_notify($1, $2)", RevisionChannel, strconv.FormatInt(rev, 10)); err != nil {
					break // reconnect; the peer's ticker still covers us
				}
				sent = rev
			}
			tick.Stop()
			conn.Close(context.Background())
		}
	}()
}

// startRevisionListener opens a DEDICATED connection, LISTENs on
// RevisionChannel, and forwards each revision it hears to the returned channel.
//
// WHY A SEPARATE CONNECTION: a connection blocked in WaitForNotification cannot
// serve queries, so it must not come from the shared pool. This costs two extra
// connections per kine process (one listening, one announcing), which is worth
// stating plainly — an unbounded pool against a fixed max_connections is
// already the largest failure mode kine has.
//
// WHY THIS EXISTS AT ALL: kine signals its own poll loop in-process on every
// insert (sqllog/sql.go:660), so a SINGLE kine wakes its watchers in
// milliseconds. That signal does not cross a process boundary. With two kine
// instances on one database, a watcher on the instance that did not receive the
// write waits for its 1 s fallback ticker — measured at p50 715 ms.
//
// THIS IS A WAKE-UP HINT, NEVER A SOURCE OF TRUTH. Notifications fire on COMMIT
// and are not durable: a disconnected listener loses everything sent while it
// was down, and there is no replay. The caller must keep its periodic poll as
// the safety net. Treating this as the delivery mechanism would trade a latency
// win for silently missed watch events, which is far worse than being late.
func startRevisionListener(ctx context.Context, wg *sync.WaitGroup, config *pgx.ConnConfig) <-chan int64 {
	revs := make(chan int64, 1024)

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(revs)

		backoff := time.Second
		for ctx.Err() == nil {
			if err := listenOnce(ctx, config, revs); err != nil {
				if ctx.Err() != nil {
					return
				}
				// Losing the listener is not fatal: the poll loop's ticker
				// still delivers every event, just later. Log at info so an
				// operator can correlate a latency change with it, and retry.
				logrus.Infof("kine revision listener disconnected, retrying in %s: %v", backoff, err)
				if !sleepCtx(ctx, backoff) {
					return
				}
				if backoff < 30*time.Second {
					backoff *= 2
				}
				continue
			}
			backoff = time.Second
		}
	}()

	return revs
}

// listenOnce holds one connection for as long as it stays healthy. It returns
// on the first error so the caller can reconnect.
func listenOnce(ctx context.Context, config *pgx.ConnConfig, revs chan<- int64) error {
	conn, err := pgx.ConnectConfig(ctx, config)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())

	if _, err := conn.Exec(ctx, "LISTEN "+RevisionChannel); err != nil {
		return err
	}
	logrus.Debugf("kine listening for revision notifications on %s", RevisionChannel)

	for {
		n, err := conn.WaitForNotification(ctx)
		if err != nil {
			return err
		}
		rev, err := strconv.ParseInt(n.Payload, 10, 64)
		if err != nil {
			continue // not ours, or malformed; the poll loop still covers it
		}
		// Non-blocking on purpose. If the consumer is behind, the revisions
		// already queued wake it just as well, and blocking here would stall
		// the listener behind a slow poll loop.
		select {
		case revs <- rev:
		default:
		}
	}
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}
