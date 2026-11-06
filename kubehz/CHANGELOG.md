# Changelog — kubehz fork of k3s-io/kine

Every entry names the measurement that justifies it and the command that
re-checks it. A patch whose benefit stops reproducing should be dropped, and
dropping it is recorded here too.

Format: each patch has a stable id (`P1`, `P2`, …) that also appears as a
`KUBEHZ-PATCH <id>` marker in the source and a section in
[MERGE-GUIDE.md](MERGE-GUIDE.md).

---

## Unreleased

### P4 — a counter of failed TLS handshakes on the client listener

Adds `pkg/endpoint/kubehz_tls.go` and `pkg/metrics/kubehz_tls.go`. Touches
`pkg/endpoint/endpoint.go` (one marker-wrapped line in `grpcServer`).

**Why.** kine shows nothing for a TLS handshake that fails on its client
listener: gRPC closes the connection and logs the error only at its own
info level, which kine does not show. With `--trusted-ca-file`, nobody can
see that kine refused a client with no certificate, a certificate from
another CA, or an expired certificate.

**What.** `kine_tls_handshake_failures_total{reason}`, with the reasons
`no_client_cert`, `unknown_authority`, `expired`, `bad_certificate`,
`client_alert`, `not_tls`, `closed`, `timeout` and `other`. A wrapper
around gRPC's server transport credentials reads the handshake error and
returns the wrapped results unchanged, so what is accepted or refused stays
upstream's. The counter is registered only when the listener serves TLS,
with every reason at 0. With `--debug`, one DEBUG line per failure.
The rules are in [MERGE-GUIDE.md](MERGE-GUIDE.md#p4).

**Tests** (`pkg/endpoint/kubehz_tls_test.go`):
- `TestKubehzP4Reasons`: real handshakes against the server config that
  `Listen` builds, on TLS 1.2 and 1.3. A valid client connects and counts
  nothing. No client certificate, a certificate from another CA, an expired
  certificate, a certificate for server auth only, a client that does not
  trust the server, a client with no ALPN, plain HTTP/2, a connection
  closed at once and a client that sends nothing until the deadline each
  count once under their reason, and the client sees a refusal.
- `TestKubehzP4ReasonTable`: the errors no real handshake makes here
  (reset, broken pipe, the text-only certificate errors).
- `TestKubehzP4Unchanged`: the wrapper returns the same values, counts
  only a failure, not `ErrConnDispatched`, and `Clone` keeps the wrapper.
- `TestKubehzP4Listen`: a real kine on sqlite with `--trusted-ca-file`.
  An etcd client writes and reads. A client with no certificate and one
  with a certificate from another CA are refused and count once each. The
  counter is registered with every reason at 0, and not at all without
  server TLS.

Each of these 14 mutations makes a test fail: no wrap in `grpcServer`, no
registration, no reason at 0, `ErrConnDispatched` counted, the error
hidden, `Clone` without the wrapper, and each reason's check removed
(client alert, unknown authority, expired, no certificate, the text
certificate errors, not TLS, closed, timeout).

**Re-check** `go test -tags=test -race -run TestKubehzP4 ./pkg/endpoint/`.

### P2 follow-up — three failed samples in a row count as full

Touches `pkg/server/kubehz_quota.go`, `pkg/metrics/kubehz_quota.go` and
`pkg/app/app.go` (the flag text).

**Why.** A failed sample keeps the last figure. The bytes the figure does
not count (another process's writes, the undercharge of a shrinking
update, a lost acknowledgement, the old value in a delete row) show only
in a good sample. While the samples fail, nothing shows them, with no time
bound.

**What.** Three failed samples in a row (`quotaSampleFailLimit`) make the
quota full, whatever the figure, until a sample succeeds. It is the full
behaviour of B481: new keys are refused, the two lanes write, and the
relief compacts. Every live read counts, also the read again after a pass;
a read that failed because kine stops does not. At the 30 s interval the
quota is full about 90 s after the last good sample at the earliest. A
WARN line starts the state and an INFO line ends it. The new gauge
`kine_quota_consecutive_sample_errors` shows the failed samples in a row,
and `kine_quota_full` is 1 from the third. No schema and no stored data
change. The rules are in [MERGE-GUIDE.md](MERGE-GUIDE.md#p2), item 1 and
"Bounds".

**Tests** (`pkg/server/kubehz_quota_test.go`):
- `TestKubehzP2FailedSamplesFull`: two failed samples and one stopped
  sample leave the quota open; the third failed sample makes it full (a
  new key refused, a no-growth update and a small Lease create pass);
  one good sample clears it and the row starts at 0 again. The gauges, the
  WARN line and the INFO line follow, and no line says the figure reached
  the limit.
- `TestKubehzP2FailedSamplesTick`: through the sampler's tick, three
  failed samples set `kine_quota_full` and start the relief; a good sample
  below the limit ends both.
- `TestKubehzP2PendingWindow` made ten failed samples in a row, which now
  make the quota full. It now makes two, and then a good sample, which
  must keep the bytes written since the mark of the good sample two good
  samples back. So the mutation "a failed sample moves the base" still
  fails it.

Each of these 10 mutations makes a test fail: the write check ignores the
unknown size, two failed samples are enough, a good sample does not clear
the state, a stopped sample counts, `Full` ignores the unknown size,
`publish` ignores it, the row is not reset, no gauge on a failure, the
line about the figure while the size is unknown, and no `publish` when the
state starts.

**Re-check** as for the P2 revision below.

The relief compacts while the size is unknown, so maybe while the
database is not full. That is no new risk: it is the compaction kine's own
compactor does, of history at least `QuotaReliefLag` old and never past
this process's poller. The comment on `quotaSampleFailLimit` says so.

### CI — the PostgreSQL steps run on 17 and 18

`unit.yml` ran its PostgreSQL service on `postgres:16`. It now runs the
whole job once per image of a matrix, `postgres:17.11` and
`postgres:18.4`, pinned by tag as before, with `fail-fast: false`. The
job name carries the image (`Unit Tests (postgres:18.4)`). No branch rule
requires the old name.

### P2 revision — a full cluster keeps running and frees space by itself (B481)

Adds `pkg/logstructured/sqllog/kubehz_relief.go`. Touches
`pkg/server/kubehz_quota.go`, `pkg/metrics/kubehz_quota.go`,
`pkg/drivers/pgsql/kubehz_quota.go`, `pkg/endpoint/endpoint.go`,
`pkg/logstructured/logstructured.go`, `pkg/app/app.go` (the flag text) and
`go.mod` (`golang.org/x/time` is direct now).

**Why.** B481, found 2026-11-05 on k3s v1.36.6 against PostgreSQL 17 with
a limit of 48 MiB. At
the limit the first P2 refused every put, as etcd's capped applier does.
On k3s that loses the cluster: the leader-election Lease update fails and
k3s exits (twice in 23 minutes). A delete raised the figure from 138 MiB to
246 MiB, because the delete row keeps the old value. Nothing compacted in
12 minutes: kine's own compactor is off by default and the apiserver's
compaction starts with a put the limit refused. Only staff could bring the
cluster back (`--compact-min-retain 10` and `ANALYZE kine` by hand).

The same run also found the overshoot. 140 writes of 900 KB passed in 18 s,
before the 30 s sample saw them: 2.5 times the limit.

**What.** The rules are in [MERGE-GUIDE.md](MERGE-GUIDE.md#p2), items 1-6.
In short:
- Every put reserves its bytes before the check. The figure is the last
  good sample plus the bytes reserved since the committed mark of the good
  sample two samples back. A refused write gives its bytes back, and so
  does a write that provably did not happen; any other error counts as
  written. A failed sample changes nothing (fail closed).
- While full, a new key is refused except a small control key (Leases,
  master leases, the compaction key). An update that grows its value by
  64 bytes or less passes, in two token buckets (control: 100/s, cleanup:
  20/s and 4 MiB/s). Each lane has a cap: control creates stop at
  `limit + 16 MiB`, cleanup updates 32 MiB below the ceiling of
  `2 × limit + 64 MiB`, control updates at the ceiling.
- After each sample that finds the quota full, also after a failed one,
  kine compacts the history up to the newest tick revision that is at
  least 2 min old (bounded by the poller's revision and the newest row),
  in transactions that never end the process. It starts the poller in the
  background when no watch did, and writes its health key when no revision
  came during the lag and the pass reached the row before the newest one
  it read. A tick runs one pass: a compaction (20 s) or, on PostgreSQL, a
  statistics refresh (`VACUUM (ANALYZE, SKIP_LOCKED) kine`, 2 min), never
  both. The refresh runs at least 15 s after the end of the last deleting
  pass; until then the tick runs no pass, so deletes cannot put the
  refresh off. It checks that the VACUUM ran. Three failed refreshes in a
  row pause the refresh for 10 min.
- The PostgreSQL estimate uses `GREATEST(n_live_tup, reltuples)`, so a
  crash or a CNPG failover (both reset `n_live_tup`) does not lift the
  limit. The source watches the insert and vacuum counts for a reset,
  because `reltuples` misses the rows written after the last VACUUM, and
  reports it; the quota then schedules the refresh. While a reset is owed,
  the figure is the table size, an upper bound, until a VACUUM ran.
- New metrics: `kine_quota_full`, `kine_quota_pending_bytes`,
  `kine_quota_refused_total{reason}`, `kine_quota_lane_writes_total{lane}`,
  `kine_quota_relief_passes_total`, `kine_quota_relief_rows_total`,
  `kine_quota_sample_errors_total` and the two last-run timestamps.

The design went through two Astra (gpt-6-astra) rounds before the code.
An Astra code review of the first implementation (`53bc833`) gave 17
findings. This revision answers them: the committed marks, the store and
load order, the lane caps, the relief lag and head marker, the relief
after a failed sample, the owed refresh, the poller start, the settle
rules and the reset watch.

A second Astra review, of `7a3f8e6`, found nine more (N1-N9) and called
five of the 17 only partly fixed. This revision answers the nine:
- N1: the refresh waits 15 s after the last deleting pass, measured, not
  one tick, because a slow tick can come right after a pass.
- N2 and N9: any VACUUM since the reset settles it, also autovacuum's or
  the relief's; the inline refresh is followed by one background refresh
  at a time; a first sample after a VACUUM is no reset.
- N3: the refresh has a 10 s sub-budget, so the compaction keeps its time.
- N4: a refresh counts only when `vacuum_count` of the table and its TOAST
  table went up (`SKIP_LOCKED` skips a held table with no error).
- N5: the poller starts in the background, one start at a time per log.
- N6: a health-key marker only after a pass with no error that reached
  the newest row.
- N7: an owed refresh also runs while the quota is not full.
- N8: a tick takes its timestamp after it read the revision.
Also new: three failed refreshes in a row pause the refresh for 10 min.
Accepted, with their bounds in [MERGE-GUIDE.md](MERGE-GUIDE.md#p2): a
shrinking update below the limit is undercharged (by up to one limit, for
that gap alone), kine's own TTL deletes have no request deadline, two
writers are not fenced, lost acknowledgements on updates give their bytes
back, and a tenant can ratchet its own Leases through the control lane.
The third review corrected these statements (below).

A third Astra review, of `6466f38`, found three blocking points. This
revision answers them:
- Refresh starvation: a deleting pass on every tick moved the last delete
  each time, so the refresh could wait for as long as there was history to
  delete. Now a tick runs a refresh pass or a compaction pass, and while
  an owed refresh waits for its 15 s the tick runs no pass. The wait counts
  from the end of the deleting pass.
- Uncoordinated refreshes: the reset watch ran its own VACUUM, inline and
  in the background, with no regard for the relief's deletes, and the
  background one outlived a shutdown. Now the watch only reports the reset
  (`StatsOwed`), and every VACUUM the quota asks for runs on the sampler
  goroutine.
- The head marker compared with the cached current revision. After a
  marker whose acknowledgement was lost, that cache is stale and a second
  marker followed. Now the marker compares with the newest row the pass
  read in its transaction.
The review also corrected the accepted bounds: the shrinking-update gap is
an envelope for that gap alone, with no time bound; a TTL operation can
wait while the rest of the process goes on; two writers have no time
bound; lost acknowledgements add up; the reset watch misses a reset on a
running source too (the vacuum counts now catch one of those cases).
kine's own compactor and autovacuum run outside the coordination. The
15 s settle is a margin, not a proof. All of it is in
[MERGE-GUIDE.md](MERGE-GUIDE.md#p2), "Bounds".

A fourth Astra review, of `5a7d308`, found the three blocking points
fixed and two more on error paths. This revision answers them:
- A batch whose commit returned an error reported 0 rows and owed no
  refresh, but the commit can have taken effect (a lost acknowledgement).
  Such a pass is now stale too, so the refresh waits for its deletes.
- A refresh settled the reset only at the next good sample. If the
  samples failed after it, the reset stayed owed, and refresh passes could
  keep the compaction out for good. Now a refresh that ran settles the
  reset at once, and its vacuum counts become the watch's baseline, so a
  reset before the next sample still shows.
It also corrected the wording: a lagging poller tries to fill a gap or
skips it after a second, a short pass gets no quiet tick, autovacuum has
no deadline during the pause, a slow refresh delays the next sample, the
reset misses in general terms, and "this bounds retained rows" is gone.

A fifth Astra review, of `45a61bd`, found both points fixed and no new
defect (verdict: ship). It named low test and wording gaps, and this
revision closes them: the refresh-budget test now blocks the refresh until
its context ends, so a refresh that does not stop with the sampler fails
it; the cancelled-before-the-transaction test is back (round 4 replaced
it by mistake); the second-reset check first asserts that the insert
counters read 0, so only the vacuum baseline can see that reset. The
wording: the delay keeps the bytes written meanwhile reserved (it does not
make the figure read high), the quiet tick depends on the time since the
pass ended, and the reset misses are measured against the watch's baseline
and include counts at or above the catalog's estimates at the first sample.

The new PostgreSQL phase found one more: a VACUUM right after a delete
reads low. PostgreSQL sets `n_live_tup` to an absolute value in the VACUUM,
and the deleting backend's counters reach the shared statistics up to
10 s later, so the deleted rows are subtracted twice. Measured: 100 rows
written after such a refresh read 0.865 of the actual size for 15 s. The
relief now refreshes at least 15 s after its delete.

The reworked PostgreSQL phases found one more: a reset that comes while
the insert counters still read 0 from the reset before is no decrease. The
vacuum counts go down then, so the watch reads them too.

**Measured.** `TestKubehzP2P3Postgres` on PostgreSQL 17.11, the estimate
against `SUM(pg_column_size(value) + pg_column_size(old_value))`. PostgreSQL
18.4 gives the same ratios except one, named below the k3s table (it
passes there too):

| Data | Estimate / actual | Ratio |
|---|---|---|
| 200 TOAST rows of 65,536 bytes | 13,187,400 / 13,107,400 | 1.006 (the query alone after `pg_stat_reset()` too) |
| the source after that reset: the table size until the quota's refresh pass, then | 13,187,400 / 13,107,400 | 1.006 |
| half of them deleted, then `RefreshStats` | 6,593,700 / 6,553,700 | 1.006 |
| 100 rows written after the last VACUUM | 13,121,463 / 13,107,400 | 1.001 |
| a reset after those rows (the table size, then the refresh pass) | 13,188,600 / 13,107,400 | 1.006 |
| 100 more rows written after the last VACUUM | 19,848,843 / 19,661,100 | 1.010 |
| then a reset and a new source (the table size, then the refresh pass) | 19,783,800 / 19,661,100 | 1.006 |
| a new source after a reset and a VACUUM (no reset to see) | 19,783,800 / 19,661,100 | 1.006 |
| a reset while another process holds the table (the refresh pass reports the skipped VACUUM, the reset stays owed) | 20,701,184 (`pg_table_size`) / 19,661,100 | 1.053, the upper bound |
| then a VACUUM from elsewhere | 19,783,800 / 19,661,100 | 1.006 |
| 4,000 inline rows of 1 KiB, after a statistics reset | 4,348,000 / 4,116,000 | 1.056 (the old query read 0) |

The B481 scenario on k3s v1.36.6 against PostgreSQL 17 (isolated docker
network, limit 48 MiB), on the first implementation `53bc833`:

| Step | B481 (`v0.17.0-kubehz.3`) | `53bc833` |
|---|---|---|
| Fill with 900 KB ConfigMaps | 140 writes passed (2.5 × the limit) | refused at write 55 (47 MiB), after 14 s |
| 3 min at the limit | k3s exited twice | 0 exits, the Lease renewed every 30 s, node Ready |
| Hostile checks (a new ConfigMap, a Lease +12 KB, a ConfigMap +1 KB refused; a small new Lease accepted) | not run | 4 of 4 as expected |
| Delete the fill | the figure rose to 246 MiB and stayed | rose to 98 MB, then below the limit 61 s after the delete, with no staff step (relief: 482 rows) |

The same scenario on the code of this revision, `3060e7b` (later commits
change only these notes), against PostgreSQL 17, on two k3s versions:

| Step | k3s v1.36.6 | k3s v1.37.2 |
|---|---|---|
| Fill with 900 KB ConfigMaps | refused at write 55 (47 MiB), after 9 s | refused at write 55, after 9 s |
| 3 min at the limit | 0 exits, the Lease renewed every 30 s, node Ready | the same |
| Hostile checks | 4 of 4 as expected | 4 of 4 as expected |
| Delete the fill | puts accepted again 246 s after the delete, with no staff step; live data 0.8 MB | puts accepted again 231 s after the delete, with no staff step; live data 0.9 MB |

The recovery takes longer than on `53bc833` because the relief now
compacts only revisions at least 2 min old (`QuotaReliefLag`), and on
PostgreSQL the statistics refresh waits 15 s after the last deleting pass.
On `5a7d308` (the third revision) the same runs took 231 s and 260 s.
`TestKubehzP2P3Postgres` passes on PostgreSQL 18.4 with `3060e7b` as well.
There, one reading differs from the table above: "100 rows written after
the last VACUUM" read 0.971 (inside the test's band of 0.9 to 1.3).

**Sixth revision (final review, 2026-11-06).** The refresh VACUUM runs
with `TRUNCATE false`. Before, its truncation phase took an ACCESS
EXCLUSIVE lock on the kine table: on PostgreSQL 17 the refresh shrank a
test heap from 4,685,824 to 1,171,456 bytes, so reads and writes could
queue behind it. `TestKubehzP2RefreshKeepsTrailingPages` fails on the old
statement and passes on the new one, on PostgreSQL 17 and 18.4. A plain
VACUUM as its control still truncates. The live-size bands of
`TestKubehzP2P3Postgres` are unchanged on both.

The same review restated two documented limits (MERGE-GUIDE.md P2,
"Bounds"): a TTL delete that stalls for longer than the relief lag, and the
poller of a second kine process that falls behind.

**Seventh revision (final review, 2026-11-06).** The review did not accept
the first limit. With a limit, every TTL delete now has 60 s
(`server.QuotaTTLDeleteTimeout`, below the 2 min lag) for its read and its
insert. `Listen` sets it before `Start`. A delete that the timeout ended
returns the context error, and the TTL loop tries the key again. Before,
`LogStructured.Delete` reported it as "not deleted", and the loop dropped
the key. `TestKubehzP2TTLDeleteTimeout` stalls an insert after its read,
then writes two newer values and compacts. With no timeout, the stalled
insert deletes the newest value. `TestKubehzP2TTLStart` and
`TestKubehzP2ListenTTLDeleteTimeout` pin the wiring, also its order before
`Start`, and `TestKubehzP2SetTTLDeleteTimeout` pins the helper. Ten
mutations: each one makes a test fail. The second limit is
now a condition for `--quota-bytes`: one active kine process per database,
also during the handoff.

**Re-check** `go test -tags=test -race -run TestKubehzP2 ./pkg/server/ ./pkg/app/ ./pkg/endpoint/ ./pkg/drivers/pgsql/ ./pkg/logstructured/ ./pkg/logstructured/sqllog/`,
with `KINE_TEST_PGSQL_DSN` set for the PostgreSQL part. The new tests:
- on a fake backend: `TestKubehzP2Lanes`, `TestKubehzP2Ceiling`,
  `TestKubehzP2LaneRate`, `TestKubehzP2Reservation`,
  `TestKubehzP2ReservationAcrossSamples`, `TestKubehzP2SampleOrder`,
  `TestKubehzP2LaneCaps`, `TestKubehzP2PendingWindow`,
  `TestKubehzP2ReliefTick`, `TestKubehzP2ReliefHead`,
  `TestKubehzP2ReliefAfterFailedSample`, `TestKubehzP2ReliefPasses`,
  `TestKubehzP2ReliefNoStarvation`, `TestKubehzP2ReliefSettleFromPassEnd`,
  `TestKubehzP2ReliefHeadOnlyWhenItHelps`, `TestKubehzP2ReliefRefreshPause`,
  `TestKubehzP2ReliefPausedCompacts`, `TestKubehzP2ReliefOwedWhileNotFull`,
  `TestKubehzP2ReliefStatsOwed`, `TestKubehzP2ReliefNoStats`,
  `TestKubehzP2ReliefBudgets`, `TestKubehzP2ReliefStaleOnError`,
  `TestKubehzP2ReliefPauseExpiry`, `TestKubehzP2ReliefTimestampAfterRead`.
  The fake relief clamps to the row before the head and reports the
  backend's current revision as the head, as the real one does.
- on a fake dialect: `TestKubehzP2ReliefBounds`,
  `TestKubehzP2ReliefStartsPoller`, `TestKubehzP2ReliefRefreshOnly`,
  `TestKubehzP2ReliefNotStaleWithoutRefresher`,
  `TestKubehzP2ReliefCancelled`, `TestKubehzP2ReliefCancelledInBatch`,
  `TestKubehzP2ReliefPollerRetry`, `TestKubehzP2ReliefLostCommitAck`
- `TestKubehzP2ListenRelief`: a real kine on sqlite fills to the limit. A
  Lease update and a no-growth update pass, a new key is refused, and kine
  recovers by itself within 15 s after the deletes (relief lag 1 s here)
- new phases in `TestKubehzP2P3Postgres`: the floor after a reset, the
  refresh after a relief, rows written after the last VACUUM, both reset
  checks (each: `StatsOwed` and the table size, then the quota's refresh
  pass, then the estimate), a new source after a VACUUM (no reset, no
  reset log line), a VACUUM that skipped a held table
  (`errVacuumSkipped`, also through the refresh pass, and the reset stays
  owed), a reset while the insert counters already read 0 (the vacuum
  counts see it), a reset right after a refresh and before the next sample
  (the refresh's baseline sees it; the test first asserts that the insert
  counters read 0), and a VACUUM from elsewhere that
  settles it. After every refresh pass `StatsOwed` is false at once. Autovacuum
  is off for the later ones, so only a VACUUM the test or the quota runs
  brings `reltuples` up to date.

Each of these mutations makes a test fail:
- quota: no update lane, the reservation after the write, the base one
  sample back, the relief target at the current revision, no growth check,
  a strict ceiling, a failed sample that moves the base, control creates
  closed
- wiring: no relief, a forward that drops the relief, a relief that keeps
  `--compact-min-retain`
- PostgreSQL: no heap `reltuples` floor, no TOAST `reltuples` floor,
  `RefreshStats` a no-op
- relief: no poller bound, the newest row compacted, no batching, a
  statistics refresh on 0 rows, a pass that goes on after an error.

The mutations of the first two revisions (25, then 13, all caught) were
rebuilt for the third, since its passes changed. The mutations of the
third to fifth revisions: 64, all caught on `3060e7b` (each applied to a
backup copy, the named test run, the file restored and compared). Two
missed the first run of the third: a first-sample check that ignores a
VACUUM behaves the same except for one log line, so the test now fails on
that line; one mutation did not compile. Both are caught since:
- accounting and lanes: the marks read the written bytes, the base stored
  before the live figure, a create error refunded, an existing key
  counted, an update error refunded, a lost compare-and-swap counted,
  control creates up to the ceiling, cleanup up to the ceiling
- the tick: no lag, no publish after a failed sample, no relief after a
  failed sample, the timestamp before the revision read, `StatsOwed`
  ignored, no quiet tick, a refresh and a compaction in one tick, a
  refresh only while full, no settle wait, no pause, quiet ticks while
  paused
- the refresh pass: the compaction's budget, the debt not cleared on
  success, the failure count not reset on success, no new sample after it
- the compaction pass: a delete that owes no refresh, a refresh owed for
  rows on a dialect without statistics, the settle measured from the
  start of the pass, no head marker, a marker after any pass, a marker
  that ignores the head the pass read, a marker one row short
- the relief: a refresh pass that also compacts, a refresh on every pass,
  a failed refresh reported as done, a refresh pass that starts the
  poller, no poller start, a poller start in the pass, a poller start
  that is not one at a time, no retry after a failed start, a failed
  commit recorded, a batch under another context, a batch that ignores
  the cancellation, a delete that ignores the cancellation, the
  post-compaction step on 0 rows, rows stale on a dialect without
  statistics, no head reported
- PostgreSQL: no reset check on a count that went down, no check on the
  vacuum counts, no first-sample check, a first-sample check that ignores
  a VACUUM (caught by the reset log lines), no settle by the vacuum
  counts, no table-size fallback, the reset not kept between samples,
  `StatsOwed` always false, a skipped VACUUM not checked, `reltuples`
  only, no `reltuples` floor
- the fourth revision: a failed commit that owes no refresh, a stale pass
  ignored after an error, the settle not set while paused, a refresh
  budget of hours, a stopped refresh counted as failed, a refresh that
  does not settle the reset, a refresh that sets no baseline
- the fifth revision: a refresh that does not end when the sampler stops.

Not covered by a test: the reader's load order in `figure` (base, then
live). A wrong order opens a window of two atomic loads; a seam there
would sit on the write path. The writer's order has its test. Also not
covered: a VACUUM that skips only the TOAST table (PostgreSQL cannot lock
a TOAST table alone), and a figure that reads low for a moment: the
PostgreSQL phases take the first reading in the band.

Not caused by this change: `TestBackend_Get` in `pkg/drivers/nats` fails
under `-race` on the base `7c8a4f7` too.

---

## Released `v0.17.0-kubehz.3` — on upstream `v0.17.0` (`6fb95f5`); release commit `a390482`

Image `ghcr.io/kernpilot/kine:v0.17.0-kubehz.3`, index digest

```
sha256:40e3d839fcc1800ec38b8ce67f99d580ce9cf6376f3831d40ef88a62e6edb7a8
```

(linux/amd64 + linux/arm64; read back from GHCR on 2026-09-17). The moving
`kubehz` tag now points at the same digest. `publish-kubehz.yml` built and
signed it: keyless cosign signature, buildkit provenance and SBOM in the
index, and an attested SPDX SBOM. The run's own verify gate passed (run
`35193544031`). Consumers pin the digest above (`KUBEHZ_EXTCP_KINE_IMAGE`).
Carries P1 and its hardening from the two releases before. This release adds
P2, the per-database size limit (`--quota-bytes`), and P3, the storage
parameters that keep the kine table's statistics current. Both sections are
below.

The release's own CI ran the P2 estimate against a real PostgreSQL
(`TestKubehzP2P3Postgres`). On 200 rows of 65,536 incompressible bytes the
estimate read 13,187,400 bytes against an actual 13,107,400, a ratio of
1.006. The test bound is 0.9 to 1.3.

### P3 — storage parameters on the kine table

Adds `pkg/drivers/pgsql/kubehz_reloptions.go`. Touches `setup()` (one flag
from its existing CockroachDB probe) and `New()` in
`pkg/drivers/pgsql/pgsql.go`. The `CREATE TABLE` statement stays upstream's.

**What.** The kine table carries `autovacuum_vacuum_scale_factor = 0.05`
and `autovacuum_analyze_scale_factor = 0.02` (PostgreSQL's defaults are 0.2
and 0.1). New or upgraded, the table gets them from one idempotent
`ALTER TABLE kine SET (...)` at startup for the parameters that differ in
`pg_class.reloptions`, with one INFO line. A table that already carries
them causes no statement and no line. CockroachDB: the parameters are not
applied (no table storage parameters there), and the fork is not tested
there beyond upstream's suite, which the fork's CI keeps. Only the pgsql driver has
this. sqlite and the others are untouched.

**Why.** Two reasons, both about P2. A lower vacuum threshold shrinks the
plateau slack the file carries above live data, the slack
`kine_db_size_bytes` shows. And P2's compared figure, `n_live_tup ×
avg_width`, takes the width from the last `ANALYZE`, so a low analyze
threshold keeps the limit honest after the row shape changes (larger
objects, a new CRD).

**Not a correctness patch.** If the `ALTER` fails (a role without `ALTER`
on the table, or a lock held by an anti-wraparound autovacuum past the 10 s
bound), kine logs a warning and starts.

**Re-check** `go test -tags=test -race -run TestKubehzP3 ./pkg/drivers/pgsql/`:
the `CREATE TABLE` statement carries no storage parameters, and the
`ALTER` is chosen exactly when the reloptions differ and never on
CockroachDB. Against a real PostgreSQL
(`unit.yml` runs one), `TestKubehzP2P3Postgres` opens the driver with
`New()` and checks that `pg_class.reloptions` carries both parameters.

### P2 — per-database size limit (`--quota-bytes`)

Adds `pkg/server/kubehz_quota.go`, `pkg/metrics/kubehz_quota.go` and
`pkg/drivers/pgsql/kubehz_quota.go`. Touches `pkg/app/app.go`,
`pkg/endpoint/endpoint.go`, `pkg/server/kv.go`,
`pkg/logstructured/logstructured.go` and `pkg/logstructured/sqllog/sql.go`.

**Why.** kine has no size limit: `Alarm` is unsupported, there is no quota
flag, and PostgreSQL has no per-database quota. A kine per tenant on a
shared PostgreSQL therefore lets one tenant fill the shard. etcd has
`--quota-backend-bytes`. Above it the capped applier refuses puts with
`ErrGRPCNoSpace` and keeps reads, deletes and compaction working, so the
client can free space. The apiserver already handles that error.

**What.** `--quota-bytes <n>` (`KINE_QUOTA_BYTES`), default 0 = no limit,
so an existing deployment changes nothing. With a limit, live data and the
physical size are sampled once at start and then every 30 s. A failed
first sample fails the start: setup just succeeded on the same pool, so a
failing size query is a real fault, and the process manager restarts kine
rather than serving with no limit until the next tick. At or above
the limit on live data, `Put` and every `Txn` that puts return etcd's
error, same gRPC code (`ResourceExhausted`) and message (`etcdserver:
mvcc: database space exceeded`). Range, Watch, delete transactions and
`Compact` keep working. One INFO line when the limit is first reached, one
when live data is below it again. Three gauges, registered only with a
limit: `kine_quota_bytes`, `kine_live_bytes` (the compared figure) and
`kine_db_size_bytes` (the physical size). A refused write is not logged
per request (upstream logs every write error with the full request). The
transition lines are the signal.

**Which bytes.** The limit compares live data, not the file. On PostgreSQL
the relation plateaus under autovacuum: kine's compaction deletes old
revisions and autovacuum makes the space reusable. After a write-then-
delete spike the file stays large while live data is small, and a limit on
`pg_total_relation_size` would then refuse a customer who holds almost
nothing. So the pgsql driver estimates the on-disk size of live rows in
one query with two terms. Heap: `n_live_tup × Σ avg_width` from
`pg_stat_user_tables` and `pg_stats`. `ANALYZE` measures a datum as stored
inline, so a `value` or `old_value` still above about 2 KB after
compression (most Pods, Secrets and CRs) counts as its 18-byte pointer
there. TOAST: the toast relation's `n_live_tup × 1996`
(`TOAST_MAX_CHUNK_SIZE` on 8 KB pages, one row per chunk), an upper bound
within one chunk per value. Both terms are compressed sizes, so the figure
is the compressed size of live rows. etcd's in-use figure is uncompressed,
so a kine cluster reads smaller for the same objects. Two catalog lookups
and no scan. Indexes and page overhead are not counted.
`pgstattuple_approx` would be closer but needs an extension the tenant
role cannot create. The heap term needs statistics: at start, when
`pg_stats` has no row for the table, kine runs `ANALYZE kine` once
(bounded, a warning on failure). After that autovacuum must be enabled for
the figure to stay current (the CNPG default). The physical size stays
what `Status` reports and what `kine_db_size_bytes` shows: a capacity
figure for whoever runs the PostgreSQL, not the customer's. The source
travels as an optional interface (`server.LiveSizer`) from the pgsql
dialect through `SQLLog` and `LogStructured`. A driver without one
(sqlite) falls back to its `DbSize`.

**Exactly etcd's rule.** A first draft kept the apiserver's compaction
bookkeeping key writable above the limit. On the live figure that changes
nothing worth a rule of its own, so the exemption was dropped: every put
is refused, as in etcd.

**Not a benchmark patch.** This is a safety bound, not an optimisation, so
there is no number to re-measure. The write path pays one atomic load, and
the sampler one catalog query per 30 s.

**Re-check** `go test -tags=test -race -run TestKubehzP2 ./pkg/server/ ./pkg/app/ ./pkg/endpoint/ ./pkg/drivers/pgsql/ ./pkg/logstructured/sqllog/`
(`unit.yml` fails when the tests are missing, not only when they fail).
`TestKubehzP2Listen` runs a real kine on sqlite through `endpoint.Listen`
and a client `Put`, so the wiring block is covered too.
`TestKubehzP2LiveSizeChain` pins the forwarding of the live figure.
`TestKubehzP2ListenFailsOnFirstSample` pins the failed start.
`TestKubehzP2P3Postgres` runs against the PostgreSQL service in `unit.yml`:
200 rows with 64 KB incompressible values, `VACUUM ANALYZE`, and the
estimate must be within 0.9 and 1.3 of
`SUM(pg_column_size(value) + pg_column_size(old_value))`. Without the
TOAST term it reads a few kilobytes. Mutation-checked:
with the `Full()` check removed from the wrapper's `Create`,
`TestKubehzP2Limit` fails on `Put above the limit`. With the P2 block
removed from `endpoint.Listen`, `TestKubehzP2Listen` fails. With
`SQLLog.LiveSize` returning nil, `TestKubehzP2LiveSizeChain` fails.

---

## Released `v0.17.0-kubehz.2` — on upstream `v0.17.0` (`6fb95f5`); release commit `14e6b9d`

Image `ghcr.io/kernpilot/kine:v0.17.0-kubehz.2`, index digest

```
sha256:cec411fcd1dd8b778b4aae637dc479b31661bbe8d04dd7beed326a061308d0f8
```

(linux/amd64 + linux/arm64; read back from GHCR on 2026-09-07). The first
release built by the signed `publish-kubehz.yml`: keyless cosign signature,
buildkit provenance and SBOM in the index, and an attested SPDX SBOM.
Verified with the MERGE-GUIDE command (`cosign verify`, identity
`publish-kubehz.yml@refs/tags/v0.17.0-kubehz.2`) and
`cosign verify-attestation --type spdxjson`; both pass, and the run's own
verify gate passed first. Consumers pin the digest above
(`KUBEHZ_EXTCP_KINE_IMAGE`). Carries P1 with the hardening below.

### P1 hardening (no behaviour change on the happy path)

- The notifier backs off on a failed `pg_notify` the same way the listener
  backs off on a lost `LISTEN`: 1 s, doubling to 30 s (`reconnectBackoff`).
  Before, an `Exec` error closed the connection and reconnected at once, so
  a role or permission error reconnected every 10 ms: about 100 backend
  forks and authentications per second per kine, forever.
- The reset rule changed for both connections: the delay drops back to 1 s
  only after a session that stayed **connected** for 30 s, measured from a
  successful connect. Before, the listener reset after any session that
  failed post-connect, so a connection that connected and failed at once
  retried every second; and time spent in a hanging dial cannot count as
  a healthy session.
- `SQLLog.poll` drops the external channel once the driver closes it on
  shutdown instead of spinning on the always-ready closed channel.
- A notification payload must be a positive revision; `0` and negatives are
  dropped like malformed text (they were forwarded and ignored by the poll).
- Hermetic unit tests for `record`, payload parsing, the non-blocking
  forward, the backoff, and the `case <-external:` arm waking `poll` on a
  fake dialect (`pkg/drivers/pgsql/kubehz_notify_test.go`,
  `pkg/logstructured/sqllog/kubehz_notify_test.go`). `unit.yml` fails when
  they are missing, not only when they fail.
- `publish-kubehz.yml` signs keyless with cosign, attaches buildkit
  provenance (`mode=max`) and SBOM, attests a standalone SPDX SBOM and
  verifies all of it before the run goes green. This tag is the first
  signed release and the first real run of that lane; the verify gate
  passed on the first run.

---

## Released `v0.17.0-kubehz.1` — on upstream `v0.17.0` (`6fb95f5`); release commit `e1778aa`

Image `ghcr.io/kernpilot/kine:v0.17.0-kubehz.1`, index digest

```
sha256:1f5ac8562e0b2a7b100f7be3c0d9cb572da3989859da581f255587a5515ea69a
```

(linux/amd64 + linux/arm64; read back from GHCR on 2026-09-05). Built by
the pre-signing `publish-kubehz.yml`: **no cosign signature, SBOM or
provenance on this image.** Consumers pin the tag today
(`KUBEHZ_EXTCP_KINE_IMAGE`); pin the digest above instead, and move to the
first signed release when it exists. Carries P1 only.

### P1 — cross-instance watch wake-up via coalesced LISTEN/NOTIFY

Adds `pkg/drivers/pgsql/notify.go`; touches `pgsql.go` and `sqllog/sql.go`.

**Why.** kine signals its poll loop in-process on every insert, so one instance
wakes its watchers in milliseconds. That signal does not cross a process
boundary, so in a multi-replica deployment — which is what Kamaji and k0smotron
run — a watcher on the instance that did not receive the write waits for its 1
second fallback ticker.

**Measured.**

| | p50 | p99 |
|---|---|---|
| stock kine, cross-instance | 715.7 ms | 716.6 ms |
| with P1 | **20.0 ms** | **21.3 ms** |
| stock, through real apiservers (HA) | 499.6 ms | 1000.9 ms |
| with P1, through real apiservers | **15.2 ms** | **25.3 ms** |

Saturated write throughput is unchanged: 5076 → 5124 writes/s, inside a 0.7 %
noise floor.

**Durability.** The poll ticker is deliberately kept, because `NOTIFY` is not
durable. Verified by killing the LISTEN backend mid-run: **0 events lost**, p99
degraded to 712.9 ms during reconnect, then recovered.

**Re-check** `benchmarks/verify-patches.sh` (or `run-cross.sh` directly).

**Independently reproduced 2026-08-29** on freshly built binaries, upstream
anchor versus patched: stock p99 **718.3 ms** → patched **25.3 ms**, 28.4x. The
verifier was itself mutation-checked by pointing it at an unpatched binary,
where it correctly reported 1.0x and failed.

**Rejected implementation.** A per-row `AFTER INSERT` trigger calling
`pg_notify` cut write throughput 96.8 % (5076 → 163 writes/s) and pushed put p99
from 39.5 ms to 876 ms. Do not reintroduce it; see MERGE-GUIDE.md#p1.

---

## Deliberately not patched

These were measured and left alone. Recorded so the work is not repeated, and
so a future PostgreSQL release can be re-tested against a known baseline rather
than a guess.

| candidate | measured | verdict |
|---|---|---|
| TOAST `EXTERNAL` on `value` | null at 512 B / 2 KB / 8 KB / 64 KB | no effect across the TOAST boundary in either direction |
| covering index, drop redundant indexes | +11.7 % tmpfs, ±0.0 % disk | storage-dependent; worthless when I/O-bound |
| `--poll-batch-size` 100 / 2000 | inside noise | no effect |
| `fillfactor = 70` | −2.7 %, inside noise | kine never UPDATEs a row, so HOT cannot apply |
| drop `kine_prev_revision_index` | −1.4 %, inside noise | costs nothing measurable to keep |
| UNLOGGED table | +46 % | disqualified: not replicated, so a failover promotes an empty table |
| table partitioning | impossible | a unique constraint must include every partition column, and kine needs both `PRIMARY KEY (id)` and `UNIQUE (name, prev_revision)` |
| write batching | 26.8x slow storage, 1.5x fast WAL | substitutes for a fast WAL rather than adding to it |

## Configuration findings that need no patch

The largest wins are not code. They are recorded here because they are easy to
lose and expensive to rediscover.

1. **Set both connection-pool flags.** `--datastore-max-idle-connections`
   defaults to 20 while `--datastore-max-open-connections` defaults to
   unlimited. That mismatch churns connections: **7.80 % write errors** on
   kine's defaults, and still **6 %** against a real apiserver after capping
   max-open alone. Set max-open below the server's `max_connections` and
   max-idle **equal to it**. Measured 0.00 % errors and +91 % throughput.
2. **Give PostgreSQL a fast, durable WAL device.** Moving only `pg_wal` to fast
   storage captured **97 %** of a full RAM-disk speedup (3189 → 11 094
   writes/s). Table and index I/O are the remaining 3 %. CNPG supports a
   dedicated WAL volume on its own storage class.
3. **`synchronous_commit = off` is then unnecessary.** Worth +124 % on a slow
   WAL and **+3.5 %** on a fast one — it attacks the same bottleneck. Prefer the
   WAL device and keep full commit durability.
4. **Budget for replication before anything else.** Measured against a real
   streaming standby (`state = streaming` and `sync_state = sync` both verified
   before the runs):

   | arm | writes/s | p99 | vs standalone |
   |---|---|---|---|
   | standalone | 5 341 | 34.6 ms | — |
   | async replica | 3 686 | 57.2 ms | −31.0 % |
   | **synchronous replica** | **1 517** | **128.1 ms** | **−71.6 %** |

   An async replica alone costs 31 % of write throughput; synchronous costs
   72 %. Every other number in this suite was measured standalone, so a
   replicated deployment should be planned from these figures rather than from
   the headline ones — they differ by up to 7x.

   **The WAL device still pays under replication.** Full matrix:

   | WAL | replication | writes/s | tax |
   |---|---|---|---|
   | slow | none | 5 341 | — |
   | slow | async | 3 686 | −31 % |
   | slow | synchronous | 1 517 | −72 % |
   | fast | none | 10 946 | — |
   | fast | async | 9 952 | **−9 %** |
   | fast | synchronous | **3 020** | −72 % |

   A fast WAL is worth **+99 % under synchronous replication** (+105 %
   standalone), so the two costs **compose multiplicatively** — synchronous
   replication is a constant ~72 % tax at either WAL speed, because the standby
   round trip is a different wait from the local flush. A prediction that they
   would substitute, by analogy with `synchronous_commit = off`, was measured
   and refuted: that setting substitutes because it removes *the same* local
   flush wait. **Provision the fast WAL volume even with replication**, and note
   it also cuts the asynchronous-replication penalty from −31 % to −9 %.

## Density model — for anyone deploying kine per tenant

Measured on this fork's binaries. **Connections are the only binding resource.**

| resource | per tenant control plane | at 500 tenants |
|---|---|---|
| **connections** | **7.0 direct, 2.25 behind PgBouncer** | ~3 500 / ~1 125 |
| kine memory | 56 MB | ~28 GB |
| kine CPU | ~1 % of a core | ~5 cores |

**Tenants per PostgreSQL ≈ `max_connections / 7`.** Measured dead linear to the
wall: 4/8/16/24 tenants used 29/57/113/169 connections at 0.00 % errors; 32
tenants exhausted `max_connections=200` and produced **15.65 % write errors**
with `FATAL: sorry, too many clients already`.

**P1 costs exactly 2 of those 7 connections** — its LISTEN and its notifier,
both standing. Dropping P1 raises tenants-per-PostgreSQL by roughly 30 %.

**PgBouncer roughly triples density and silently defeats P1.** kine works
through transaction-mode pooling with 0.00 % errors, and per-tenant connections
fall from 7.06 to 2.25 (pooling is per user+database, and each tenant has its
own database, so `default_pool_size` becomes the per-tenant cost). But
cross-instance watch p99 goes from 21 ms to **1001 ms** — the fallback ticker —
because NOTIFY reaches a different backend than LISTEN. **kine logs no error.**
This only matters where a tenant runs multiple kine replicas.

Re-measure with `benchmarks/sweep-density.sh`, `sweep-density2.sh` and
`sweep-density3.sh`.

**D5 (2026-09-01): the wall holds off past 500 tenants on direct
connections.** `sweep-density-wall.sh` laddered the two levers with full
production shape (postgres 18.4, per-tenant roles with `CONNECTION LIMIT
12` owning their own databases, kine pool 8/8, quiet-CP load of 2 writers
@ 5/s + 4 watchers each):

| max_connections | pool | tenants | conns used | err % | worst-tenant put p99 | pg RSS |
|---|---|---|---|---|---|---|
| 300 | 8 | 40 | 204 | 0.00 | 47 ms | 1.0 GB |
| 800 | 8 | 100 | 514 | 0.00 | 56 ms | 2.3 GB |
| 2000 | 8 | 250 | 1 289 | 0.00 | 72 ms | 5.6 GB |
| 4000 | 8 | **500** | 3 006 | **0.00** | 224 ms | 12.3 GB |
| 2600 | **4** | 500 | 2 592 | **4.17** | 1 701 ms | 11.1 GB |

- Quiet tenants hold **~5–6 connections**, under the loaded-tenant 7 —
  the `max_connections / 7` rule is the safe sizing bound, not the floor.
- **Halving the pool to 4/4 is refuted**: 4.17 % write errors and 1.7 s
  p99 at 500 tenants — pool starvation, the equal-flags scar again. The
  per-role `CONNECTION LIMIT` contained the damage to the starved tenants
  (the shared pool never drained), which is why the roles must carry it.
- The 224 ms p99 at 500 is instrument-contaminated (one 24-core host ran
  postgres AND all 500 kine AND all 500 load generators); in a real
  deployment the kine fleet runs elsewhere and postgres carries only
  ~12 GB RSS + ~3 000 backends. Confirm latency on target hardware.
- No pooler needed for 500. PgBouncer (~2.25 conns/tenant) remains the
  next 3× if a deployment ever needs >max_connections/7 — with the
  cross-instance watch caveat above.

Summary table: `benchmarks/results/d5-wall.txt` (per-tenant JSONs are
local artifacts, not committed — 1 400 files say nothing the table does
not).

## Open hazards in upstream kine, not yet patched

Carried here because they bound how kine can be deployed, not because they are
fixed.

- **A watch starting at revision 0 reads the entire table into memory, per
  watcher, unbounded.** `LogStructured.Watch` (`pkg/logstructured/logstructured.go`) calls
  `l.log.After(ctx, key, end, revision, 0)` — **limit zero** — and
  `watcher.Create` (`pkg/server/watch.go`) passes the client's `StartRevision` through unsubstituted, so a
  `clientv3` watch with no `WithRev()` sends 0 and gets the whole history
  materialised through `RowsToEvents`/`bytes.Clone`.

  Diagnosed by heap profile (99.3 % of live heap in `RowsToEvents`, `-peek`
  attributing **100 % to `SQLLog.After`**) and confirmed by changing one
  variable. Same 657 043-row table, 100 writers, 256 watchers:

  | watch start revision | writes/s | peak RSS | |
  |---|---|---|---|
  | 0 (etcd default) | — | **12.89 GB** | aborted at the 12 GB cap |
  | current revision | 10 082 | **1.22 GB** | completed |

  `--poll-batch-size` does not bound it (500/50/10 → 12.51/12.42/12.98 GB;
  this call bypasses that limit) and `GOMEMLIMIT=4GiB` does not contain it
  (12.59 GB; the memory is live).

  **Relevance is narrower than the raw number suggests.** A Kubernetes reflector
  does LIST-then-WATCH from a specific `resourceVersion`, so an apiserver in
  steady state does not trigger this. Clients that do: anything watching without
  a revision — `etcdctl watch`, monitoring agents, hand-written controllers —
  and this suite's own load generator, which used the etcd default.

  A server-side bound on the catch-up read would be a genuine patch. It is not
  written yet, because the first question is whether kubehz will ever have a
  client that watches from 0, and the client-side discipline is free.

  **This is the only unbounded watch cost.** Two others exist and both are
  bounded, measured separately at 50 writers on a fresh table:

  | cost | driven by | magnitude | bounded? |
  |---|---|---|---|
  | fan-out | watcher count | 8.8x throughput, 52x p99 across 64 → 2048 watchers | yes, degrades smoothly |
  | steady-state memory | watcher count | ~1.8 MB per watcher (0.16 GB at 64, 3.70 GB at 2048) | yes, roughly linear |
  | catch-up read | revision-0 watch **on an aged table** | 1.22 → 12.89 GB on 657 k rows | **no** |

  The fan-out figures were re-measured with reflector-like watches to rule out
  catch-up contamination; every point moved less than the 5-6.5 % noise floor,
  so fan-out cost is genuine and separable.

- **A dropped watcher would not be told it lost its place — but the drop path
  could not be reached.** `broadcaster.go` unsubscribes a subscriber whose
  buffer is full, and the tail of `watcher.watch` (`pkg/server/watch.go`) then sends `Canceled: true` with
  **`CompactRevision: 0` and an empty reason**, where kine's own signal for an
  invalid position (the compacted branch of the same function) is `Cancel(id, currentRev, compactRev,
  ErrCompacted)`. On that reading a reflector reconnects from its last
  resourceVersion and silently skips the lost events.

  **Three attempts failed to trigger the drop**, using `benchmarks/dropprobe/`:

  | attempt | result |
  |---|---|
  | watcher stops reading its channel, 40 000 matching events written | all 40 000 delivered, no gap, no cancel |
  | watcher `SIGSTOP`ped, flood on a *different* prefix | no drop — and the test was wrong: kine filters per watcher *after* the broadcaster, so non-matching events are drained by the filtering goroutine and never pressure the buffer |
  | watcher `SIGSTOP`ped, flood on the matching prefix | inconclusive; the watcher still observed only its warmup event |

  The first failure is the instructive one: **`clientv3` drains the gRPC stream
  into its own unbounded buffer whether or not the application reads the
  channel**, so an application-level slow consumer creates no server-side
  backpressure at all. Reaching the drop needs real transport stalling, and
  even freezing the client process did not produce it here.

  **So this is a code-visible hazard with no demonstrated trigger, and no patch
  is justified on present evidence.** The probe is committed so the attempt is
  cheap to repeat; the fix, if anyone reaches the path, is to pass
  `wr.CompactRevision` and a reason into the final `Cancel` of `watcher.watch`
  instead of the literal zeros.

Neither `P2` nor `P3` is being carried. P2 was written, measured, found to
change nothing, and reverted; P3's trigger could not be demonstrated. Both
hazards are real and both remain unexplained at the mechanism level. Neither has
an upstream issue.

**The fork currently carries exactly one patch, P1.** That is the honest state:
the watch-path hazards that motivated forking turned out to need diagnosis
before they need code.
