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

const (
	reconnectFloor = time.Second
	reconnectCap   = 30 * time.Second
)

// reconnectBackoff is the retry delay the two background connections share:
// 1 s, doubling to 30 s. The notifier's 10 ms ticker must never become its
// reconnect interval: a role or permission error on pg_notify would then open
// a new connection every 10 ms against a tenant role with CONNECTION LIMIT 12,
// the wall kubehz/CHANGELOG.md measures.
type reconnectBackoff struct {
	next time.Duration
}

// delay returns the wait before the next connection attempt and doubles it
// for the attempt after, up to reconnectCap.
func (b *reconnectBackoff) delay() time.Duration {
	if b.next < reconnectFloor {
		b.next = reconnectFloor
	}
	d := b.next
	b.next = min(d*2, reconnectCap)
	return d
}

// settle resets the delay after a session that stayed up for at least
// reconnectCap. A shorter session counts as part of the same outage, so a
// connection that connects and fails at once keeps backing off.
func (b *reconnectBackoff) settle(healthyFor time.Duration) {
	if healthyFor >= reconnectCap {
		b.next = 0
	}
}

// revisionNotifier coalesces revision announcements onto one connection.
type revisionNotifier struct {
	latest atomic.Int64 // highest revision inserted by THIS kine instance
}

// record marks a revision as worth announcing. Called on the write path, so it
// must stay this cheap — no locks, no I/O. Never moves latest backwards.
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
		var (
			sent    int64
			backoff reconnectBackoff
		)
		for ctx.Err() == nil {
			started := time.Now()
			err := r.notifyOnce(ctx, config, &sent)
			if ctx.Err() != nil {
				return
			}
			// Losing the notifier is not fatal: peers fall back to their
			// poll ticker. Log at info, back off, retry.
			backoff.settle(time.Since(started))
			d := backoff.delay()
			logrus.Infof("kine revision notifier disconnected, retrying in %s: %v", d, err)
			if !sleepCtx(ctx, d) {
				return
			}
		}
	}()
}

// notifyOnce holds one connection and announces the highest recorded revision
// at most once per notifyInterval, until the connection fails. sent outlives
// the connection so a reconnect announces only what is still unannounced.
func (r *revisionNotifier) notifyOnce(ctx context.Context, config *pgx.ConnConfig, sent *int64) error {
	conn, err := pgx.ConnectConfig(ctx, config)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(context.Background()) }()

	tick := time.NewTicker(notifyInterval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
		}
		rev := r.latest.Load()
		if rev <= *sent {
			continue
		}
		if _, err := conn.Exec(ctx, "SELECT pg_notify($1, $2)", RevisionChannel, strconv.FormatInt(rev, 10)); err != nil {
			return err // reconnect; the peer's ticker still covers us
		}
		*sent = rev
	}
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
// insert (SQLLog.Append sends on s.notify, sqllog/sql.go), so a SINGLE kine
// wakes its watchers in milliseconds. That signal does not cross a process
// boundary. With two kine instances on one database, a watcher on the instance
// that did not receive the write waits for its 1 s fallback ticker — measured
// at p50 715 ms.
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

		var backoff reconnectBackoff
		for ctx.Err() == nil {
			started := time.Now()
			err := listenOnce(ctx, config, revs)
			if ctx.Err() != nil {
				return
			}
			// Losing the listener is not fatal: the poll loop's ticker
			// still delivers every event, just later. Log at info so an
			// operator can correlate a latency change with it, and retry.
			backoff.settle(time.Since(started))
			d := backoff.delay()
			logrus.Infof("kine revision listener disconnected, retrying in %s: %v", d, err)
			if !sleepCtx(ctx, d) {
				return
			}
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
	defer func() { _ = conn.Close(context.Background()) }()

	if _, err := conn.Exec(ctx, "LISTEN "+RevisionChannel); err != nil {
		return err
	}
	logrus.Debugf("kine listening for revision notifications on %s", RevisionChannel)

	for {
		n, err := conn.WaitForNotification(ctx)
		if err != nil {
			return err
		}
		forwardRevision(n.Payload, revs)
	}
}

// parseRevision reads a notification payload: a positive revision id in
// decimal and nothing else. Anything else is not ours or malformed and is
// dropped; the poll ticker still covers whatever it meant to announce.
func parseRevision(payload string) (int64, bool) {
	rev, err := strconv.ParseInt(payload, 10, 64)
	if err != nil || rev <= 0 {
		return 0, false
	}
	return rev, true
}

// forwardRevision hands a parsed payload to the poll loop without blocking and
// reports whether it was queued. If the consumer is behind, the revisions
// already queued wake it just as well, and blocking here would stall the
// listener behind a slow poll loop.
func forwardRevision(payload string, revs chan<- int64) bool {
	rev, ok := parseRevision(payload)
	if !ok {
		return false
	}
	select {
	case revs <- rev:
		return true
	default:
		return false
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
