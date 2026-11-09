# Merge guide — how to sync this fork with new upstream kine

**Read this before resolving any conflict in this repository.** It is written for
whoever (or whatever) does the merge, and it assumes no memory of why these
patches exist.

## The model

| branch | what it is |
|---|---|
| `upstream-master` | a pristine mirror of upstream `k3s-io/kine`, kept on `origin`: the commit of the last upstream release that was merged into `main`. Never edit it. |
| `main` | upstream plus the patches below, plus `kubehz/` and `benchmarks/`. It only moves forward. |

**An upstream sync is a merge.** The ruleset on `main` refuses a force
push, so `main` is never rebased. To sync:

1. Make a branch from `main`.
2. Merge the newest upstream release tag into it. Use a release tag, not
   upstream master.
3. Resolve each conflict with the rules below.
4. Run the gates and `benchmarks/verify-patches.sh`.
5. Open a PR. It lands as a merge commit
   (`gh pr merge <n> --merge --match-head-commit <sha>`), never as a squash:
   a squash loses the upstream ancestry, and the next sync then meets every
   upstream change again as a conflict. The ruleset on `main` allows both
   methods (since 2026-11-08). Every other PR lands as a squash.
6. After the PR lands, move `upstream-master` to the commit of the tag.
   Not before: `benchmarks/build.sh` builds the stock binary from it.
7. Bring each open fork PR up to date: merge the new `main` into its
   branch, then re-run its gates.
8. A sync is not a release. To ship it, tag `v<upstream>-kubehz.1`
   (see "Image releases" below).

The patches are tracked by their `KUBEHZ-PATCH` markers and by
`CHANGELOG.md`, not by commit order. The history of `main` mixes upstream
and fork commits. Grep `KUBEHZ-PATCH` to find every patch.

**Current base.** Upstream `v0.17.5` (`f82bd25`), the newest release tag
on 2026-11-08. PR 14 (branch `sync/v0.17.5`) merged it into `main`; P1
was dropped in that sync (see `CHANGELOG.md`).

A clone has only `origin` (this fork). The recipe below works from a fresh
clone; skip the lines you already have.

```bash
git clone https://github.com/kernpilot/kine.git && cd kine
git remote add upstream https://github.com/k3s-io/kine.git
git fetch upstream --tags                       # k3s-io/kine
git checkout -b sync/<tag> origin/main
git merge --no-ff <tag>                         # the newest upstream release tag
# resolve using the rules below, then:
go test -tags=test ./...                        # with CGO_ENABLED=1 and with 0
cd benchmarks && ./setup.sh && ./verify-patches.sh   # re-measures every claim in CHANGELOG.md
# open the PR; it lands as a merge commit. After it landed:
git push origin '<tag>^{commit}:refs/heads/upstream-master'
```

Dependabot branches on `origin` (`dependabot/...`) come from upstream's
`.github/dependabot.yml` and track upstream's dependencies. Close them
unmerged: upstream owns those bumps and they arrive with the next sync.

To report a bug upstream or send a patch there, read `UPSTREAM.md` first.

## Rules for resolving a conflict here

1. **Find the marker.** Every in-place edit is wrapped in
   `// KUBEHZ-PATCH <id> BEGIN … // KUBEHZ-PATCH <id> END`. Grep for
   `KUBEHZ-PATCH` to see all of them. Anything outside a marker is upstream's
   and should take upstream's side.
2. **Preserve the INTENT line, not the diff.** Each marker states what must
   remain true. If upstream restructured the surrounding code, re-express the
   intent in the new structure rather than forcing the old lines back in.
3. **Never drop an INVARIANT.** Where a marker names an invariant, that line is
   load-bearing for correctness and a patch that loses it is worse than a patch
   that is absent.
4. **When in doubt, drop the patch.** Every patch here is an optimisation, a
   safety bound or an observation (a metric), and each marker says which.
   None is required for kine to be correct. A dropped patch that is recorded
   in `CHANGELOG.md` is recoverable; a half-merged one is a silent bug.
5. **Re-measure before believing.** `benchmarks/verify-patches.sh` re-runs the
   experiment behind every claim and prints measured-versus-recorded. A patch
   whose benefit no longer reproduces on a newer PostgreSQL or a newer kine
   should be dropped, not carried.

## Why the benchmarks live in this repo

The numbers that justify each patch depend on **kine's version, PostgreSQL's
version, and the storage underneath**. All three move. Keeping the suite next to
the code means a future merge can answer "is this patch still worth carrying?"
by running it, instead of trusting a number measured against PostgreSQL 18.3 in
August 2026.

Several of the original findings were version- or environment-specific in ways
that were not obvious until measured — for instance, the covering index measured
**+11.7 % on tmpfs and ±0.0 % on an encrypted-btrfs volume**, because the disk
was the real constraint. A patch justified on one storage profile can be worth
nothing on another. Re-run, do not assume.

## The patches

### P1 — cross-instance watch wake-up (dropped 2026-11-08)

P1 woke the poll loop of a second kine instance on the same database
through PostgreSQL `LISTEN`/`NOTIFY`. It was dropped on 2026-11-08 under
rules 4 and 5:
- No product runs more than one kine on one database. The KKP-lite kine
  has one replica, the extCP HA tier is shelved, and managed state is one
  kine per tenant.
- On v0.17.5 its p99 claim did not reproduce: p50 about 20 ms against
  709 ms stock, but p99 720 ms against 709 ms.
- Upstream `ca3c2d5` moved every write to a path that P1 did not see. P1
  needed a port, and without it P1 stopped with no error.

The last version, ported to v0.17.5 with its tests, is on branch
`archive/p1-port-v0.17.5` (`0f83a48`). `CHANGELOG.md` has the
measurements, and the rejected per-row trigger (write throughput −96.8 %;
do not use it again). If a product starts to run more than one kine on one
database, start from that branch, and first fix its known limit: the
notifier sends only the highest revision, so a lower revision that
commits last in a burst waits for the ticker. Since v0.17.5,
`--datastore-poll-interval` is a configuration alternative, at the cost of
one poll query per interval while idle.

### P2 — per-database size limit (`--quota-bytes`)

**Files** `pkg/server/kubehz_quota.go`, `pkg/metrics/kubehz_quota.go`,
`pkg/drivers/pgsql/kubehz_quota.go`, `pkg/logstructured/sqllog/kubehz_relief.go`
and `pkg/logstructured/kubehz_ttl.go` (new, conflict-free). Edits:
`pkg/drivers/pgsql/pgsql.go` (the one-time `ANALYZE` in `New`, and the
`quotaDialect` wrapper that `New` hands to `sqllog.New`; the type is in
`kubehz_quota.go`), `pkg/app/app.go` (the flag), `pkg/endpoint/endpoint.go` (the config field,
the wiring, the TTL delete timeout before `backend.Start`, and the test
seams), `pkg/server/kv.go` (two log conditions),
`pkg/logstructured/logstructured.go` (three forwarding methods, the TTL
delete timeout field, and the TTL loop start: upstream's
`go ttl.Run(ctx, l)` is `go ttlRun(ctx, l.ttlBackend())`, so the `ttl`
import moved to `kubehz_ttl.go`) and `pkg/logstructured/sqllog/sql.go` (one
forwarding method, and the `polledRev` field with its one store at the top
of the poll loop: upstream removed the field in v0.17.1 and the relief
needs it), and `go.mod` (`golang.org/x/time` is a direct
dependency now). Tests `pkg/server/kubehz_quota_test.go`,
`pkg/app/kubehz_quota_test.go`, `pkg/endpoint/kubehz_quota_test.go` (a
real kine on sqlite through `Listen`, the B481 relief end to end),
`pkg/drivers/pgsql/kubehz_quota_test.go`,
`pkg/logstructured/kubehz_ttl_test.go` (the TTL delete timeout),
`pkg/logstructured/sqllog/kubehz_quota_test.go` (the forwarding chain),
`pkg/logstructured/sqllog/kubehz_relief_test.go` (the relief bounds) with
its fake dialect in `pkg/logstructured/sqllog/kubehz_dialect_test.go`,
and `pkg/drivers/pgsql/kubehz_pgsql_test.go` against the PostgreSQL
service `unit.yml` runs on 17 and 18 (skipped without
`KINE_TEST_PGSQL_DSN`; it needs a role that may call `pg_stat_reset()`). `unit.yml` fails when they are missing.

**Problem.** Upstream kine has no size limit: `Alarm` is unsupported, there
is no quota flag, and PostgreSQL has no per-database quota. One kine per
tenant on a shared PostgreSQL lets one tenant fill the shard. etcd caps the
store with `--quota-backend-bytes`: above it the capped applier refuses
every put with `ErrGRPCNoSpace` and keeps reads, deletes and compaction.
The first version of P2 copied that, and B481 showed what it does to a
cluster that reaches the limit: k3s loses its leader-election Lease and
exits, a delete makes the figure larger (the delete row keeps the old
value), and the apiserver's compaction starts with a put the limit refuses,
so nothing ever frees the space. Only staff could bring it back.

**Approach.** `--quota-bytes <n>`, default 0 = no limit. With a limit,
`endpoint.Listen` wraps the backend in `server.WithQuota`, samples two
figures once at start (a failed first sample fails `Listen`), and then
every `server.QuotaSampleInterval` (30 s): live data, which the limit
compares, and the physical size the `Status` RPC reports (`Backend.DbSize`),
which only feeds a gauge. The rules:

1. The figure is the last good live sample plus the bytes this process
   reserved, less the bytes that were committed at the start of the good
   sample two samples back. Every put reserves its bytes before the check,
   so concurrent writes see each other and the limit is passed by one
   write at most. A refused write gives its bytes back, and so does a write
   that provably did not happen (a create whose key exists, an update that
   lost its compare-and-swap). Any other error counts as written, because
   the row can exist. The marks read committed bytes, so a write that is
   still running at a mark stays in the figure. A sample stores the live
   figure before it moves the base, and a reader loads the base first, so
   a mixed read reads high. A create counts key + value, an update key +
   2 × value (the row also carries the old value). A failed sample changes
   nothing, so the bytes keep adding up and a sampler that keeps failing
   refuses writes (fail closed). Only a good sample shows the bytes the
   figure does not count (the gaps under "Bounds"). So three failed
   live-size reads in a row (`quotaSampleFailLimit`) make the quota full,
   whatever the figure, until a read succeeds: new keys are refused, and
   the lanes and the relief of items 2 to 4 work as at the limit. Every
   live read counts: the sample of a tick, and the read again after a
   refresh or a compaction pass. A read that failed because kine stops does
   not count. With only the samples of the ticks, that is about 90 s after
   the last good read at the 30 s interval. That is no minimum and no
   deadline: the reads after a pass can make it shorter, down to about one
   interval, and a slow pass that delays a tick makes it longer. A write
   reads this state before the figure, so a read that makes the size known
   again with a figure at the limit cannot let through a write that
   compared the figure from before it. A WARN line starts the state, an
   INFO line ends it, and `kine_quota_consecutive_sample_errors` shows the
   failed reads in a row (`kine_quota_full` is 1 from the third).
2. At or above the limit ("full") a new key is refused, except a small
   control key. Deletes, reads, watches and `Compact` pass. An update
   passes only if it does not grow the value by more than 64 bytes; it is
   then charged key + new + old exactly (the check reads the key). Two
   token buckets: the control lane (`/registry/leases/`,
   `/registry/masterleases/` and kine's stand-in for the apiserver's
   compaction key, values up to 4 KiB, 100/s) and the cleanup lane (every
   other key, 20/s and 4 MiB/s). An update passes only when its compare
   revision is the revision that the check read (0 when the key has no live
   row). The insert copies the old value from the row at the compare
   revision, and rows do not change after the insert. So that row holds the
   value that the check read, and the charge is exact. With no live row,
   such an update is a create: key + value, under the create rules. The
   check answers an update at any other revision itself, as a lost
   compare-and-swap with the revision and the key as read (no key when it
   has no live row). That is the answer `LogStructured.Update` gives, with
   no error. The backend does not see the update. The apiserver then
   retries as after any conflict, or reports a key with no live row as not
   found. At an older revision, a compaction can make an older row of a key
   the newest again, so the backend could write such an update with no
   charge. At a newer revision, another write can land there before the
   insert, and after a delete the update writes the key again (upstream
   issue k3s-io/kine#825). That includes an update at the revision of a
   deleted key's delete row.
3. The ceiling is `2 × limit + 64 MiB`: a lane write passes only while
   figure + its cost stays at or below its lane's cap. Control creates stop
   at `limit + 16 MiB` (new Leases while full are few, so many are a fault
   or an attack). Cleanup updates stop 32 MiB below the ceiling, so cleanup
   churn cannot close the renewals. Only control updates go up to the
   ceiling. Deletes are never refused, so the database can hold up to the
   ceiling plus the data deleted since the last relief pass.
4. Relief. After each sample that finds the quota full (also after a
   failed sample: the written bytes alone can fill it), one pass compacts
   the history up to the newest tick revision that is at least
   `server.QuotaReliefLag` (2 min) old. A tick takes its timestamp after
   it read the current revision, so a revision never gets a time from
   before it was seen. The lag is longer than the apiserver's 60 s request
   timeout: a compaction that is too recent lets a write that paused
   between its read and its insert pass its compare-and-swap. The pass
   never passes the revision the poller has sent to watchers, and never the
   newest row. With no watch yet the poller has not run, so the pass starts
   it and compacts nothing. The start runs in the background, one at a time
   per log, so the sampler never waits for its revision read; a start that
   failed lets a later pass try again. If no revision came during the lag
   (the target is the newest row), and the pass compacted up to the row
   before it with no error, it writes kine's health key, so the deleted rows
   fall behind the newest row. The newest row is the one the pass read in
   its transaction (`MAX(id)`), not the cached current revision: a marker
   whose write lost its acknowledgement moved the head, and the cache need
   not show that. A pass that failed or stopped early writes no marker, so
   markers cannot pile up while the compaction does not keep up.
   It does not go through `compact()`, whose
   `--compact-min-retain` floor would keep the deleted rows while no new
   revision comes. Each batch is its own transaction that commits or rolls
   back (never `MustCommit`), under the pass's 20 s budget.
   kine's own TTL deletes have no request deadline upstream. With a limit,
   `Listen` sets `server.QuotaTTLDeleteTimeout` (60 s, below the lag) on
   the backend before `Start`, and each TTL delete (its read and its
   insert) must finish in that time. `LogStructured.Delete` reports a
   failed insert as "not deleted" with no error, and the TTL loop then
   drops the key. So a delete that the timeout ended returns the context
   error instead, and the loop tries the key again. Without a limit the
   TTL loop is upstream's.
5. The statistics refresh (PostgreSQL: `VACUUM (ANALYZE, SKIP_LOCKED,
   TRUNCATE false) kine`) is a pass of its own. `TRUNCATE false` keeps the
   VACUUM away from the ACCESS EXCLUSIVE lock of its truncation phase,
   which `SKIP_LOCKED` does not avoid: reads and writes would queue behind
   it. Autovacuum still truncates. A tick runs one refresh pass or one
   compaction pass, never both, and a refresh pass compacts nothing. A
   compaction pass that deleted rows on a dialect that keeps statistics
   (PostgreSQL, `ReliefResult.Stale`) makes a refresh owed; so does a pass
   whose commit returned an error, because the commit can have taken
   effect (a lost acknowledgement). The refresh
   runs at least 15 s (`quotaRefreshSettle`) after the END of the last
   deleting pass. A VACUUM sets `n_live_tup` to an absolute value, and the
   deleting backend's counter changes reach the shared statistics later
   (an idle backend tries every 10 s). So a VACUUM that overlaps the delete
   or follows it too closely subtracts the deleted rows twice, and the
   figure then reads low. While an owed refresh waits for the 15 s, the
   tick runs no pass (a quiet tick): a compaction on every tick would move
   the last delete each time and put the refresh off for as long as there
   is history to delete. Whether the next tick is quiet depends on the
   time since the end of that pass, not on the pass's length alone: a slow
   sample or a tick that waited also counts. For example, with ticks on
   time every 30 s, a deleting pass of more than 15 s is followed by a
   quiet tick, and a shorter one by the refresh at once. The 15 s is a
   margin, not a proof: PostgreSQL gives no flush deadline under
   contention. An owed refresh also runs while the
   quota is not full. A refresh pass has 2 min (`quotaRefreshBudget`), a
   compaction pass 20 s. The sampler waits for a pass, so a slow refresh
   delays the next sample by up to 2 min (the bytes written meanwhile are
   reserved and stay in the figure; the gaps under "Bounds" stay as they
   are), and three slow failures
   put the compaction off by about 6 min before the pause. On PostgreSQL a
   refresh counts only when the manual `vacuum_count` of the table and of
   its TOAST table went up: `SKIP_LOCKED` skips a table that another
   process holds and reports no error. A failed refresh stays owed. After
   three failed refreshes in a row it pauses for 10 min, with a warning.
   The compaction then runs on every tick that finds the quota full with
   a target, and autovacuum, which the deletes start, can update the
   statistics meanwhile (with no deadline). A delete during the pause
   still sets the settle, so the refresh after the pause waits for it.
   SQLite keeps no such statistics: there a delete owes no refresh and no
   tick is quiet.
6. The PostgreSQL estimate uses `GREATEST(n_live_tup, reltuples)` for the
   heap and TOAST row counts, because a crash and a promoted standby reset
   `n_live_tup`, and analyzes the table first when `pg_stats` has no row
   for it. `reltuples` misses the rows written after the last VACUUM or
   ANALYZE, so the source also watches for a reset: an insert or vacuum
   count that went down since the last sample, or, at the first sample, an
   insert counter below the catalog row count while no VACUUM ran since the
   reset (an ANALYZE can see committed rows before their insert counters
   flushed, so a low counter alone is no proof). The vacuum counts catch a
   reset that comes while the insert counters still read 0 from the reset
   before. While a reset is owed, the figure is `pg_table_size('kine')`, an
   upper bound. Any VACUUM of both the table and its TOAST table settles
   the reset, also one by autovacuum. The source runs no VACUUM itself: it
   reports the reset (`StatsOwed`), and the quota runs the refresh pass on
   the sampler goroutine, like every pass, so no VACUUM the quota asks for
   overlaps the relief's deletes. A refresh that ran settles the reset at
   once, so samples that fail after it cannot keep the refresh owed, and
   its vacuum counts become the watch's baseline, so a reset before the
   next sample still shows as a decrease. Each size query has its own
   10 s deadline.
   The estimate can read high after a VACUUM. A VACUUM, also one by
   autovacuum, sets `n_live_tup` to an absolute value, and a backend sends
   its insert counters to the shared statistics at most once a second (an
   idle backend 10 s later, and at most 60 s later when the shared entries
   are busy). Inserts whose counters arrive after the VACUUM count twice,
   until the next VACUUM for the TOAST row count and the next ANALYZE or
   VACUUM for the heap row count. That is about the last second of inserts
   of each busy backend, and it only refuses early. Measured on PostgreSQL
   17.11 and 18.4: of 200 inserts of 64 KiB, up to 95 had not sent their
   counters when a VACUUM ran, and the TOAST row count then read 6,600 plus
   33 for each of them (CHANGELOG.md, "P2 test"). The estimate cannot tell
   these rows from new ones without a table scan, which it does not do
   ("Which bytes, and why" below), so it stays as it is. For the same
   reason `TestKubehzP2P3Postgres` flushes the counters of its inserting
   session (`pg_stat_force_next_flush()`) before a VACUUM that it reads
   after, and checks that they arrived. So it needs PostgreSQL 15 or later.

INFO lines on each transition and on each relief pass, and a WARN line when
three failed live-size reads make the quota full. The metrics
(`metrics.QuotaMetrics()`) are registered only with a limit. `kv.go`
skips the per-request error log for the no-space error: the apiserver
answers 500, its clients and controllers retry, and that line prints the
full request each time.

**Which bytes, and why.** On PostgreSQL the relation plateaus under
autovacuum (kine's compaction deletes old revisions, autovacuum makes the
space reusable), and after a write-then-delete spike the file stays large
while live data is small. A limit on `pg_total_relation_size` would then
refuse a customer who holds almost nothing, so the file is a capacity
figure for whoever runs the PostgreSQL, and the limit is on live data. The
pgsql driver estimates it in one query with two terms. Heap: rows ×
`Σ avg_width` from `pg_stats`. `ANALYZE` measures a datum as stored inline,
so an out-of-line TOAST value (a `value` or `old_value` above about 2 KB
after compression) counts as its 18-byte pointer here. TOAST: the toast
relation's rows × 1996 (`TOAST_MAX_CHUNK_SIZE` on 8 KB pages), an upper
bound within one chunk per value. The whole figure is the compressed
on-disk size of live rows, so it reads smaller than etcd's uncompressed
in-use figure for the same objects. Catalog lookups only, no scan, no
indexes or page overhead. `pgstattuple_approx` would be closer but needs an
extension the tenant role cannot create. The source travels as the
optional interface `server.LiveSizer`, asserted at the call site: the
pgsql `quotaDialect` offers it, `SQLLog`
and `LogStructured` forward it, `endpoint.Listen` asks for it and falls
back to `DbSize` when it gets nil (sqlite, the others). The relief travels
the same way as `server.QuotaRelief` (`SQLLog` implements it,
`LogStructured` forwards it, with `StatsOwed`), and the statistics refresh
as `server.StatsRefresher` on the dialect.

**Bounds, for a tenant that is hostile.** Above the limit only the two
lanes put, at their rates, and a lane value grows by 64 bytes per update
at most. The lane caps and the ceiling bound where that ends. The known
gaps, accepted with their bounds:

- An update below the limit that shrinks a value is charged
  `key + 2 × new`, so less than it writes, by at most the old value. This
  gap alone lets the stored data pass the figure by up to one limit while
  no good sample shows it. That is an envelope for this gap alone, with
  every other write charged correctly. A good sample need not show the
  missing bytes at once (the statistics lag), so there is no time bound.
  The other gaps here (stale widths, a missed reset, lost
  acknowledgements, uncharged deletes) are separate allowances. Together
  they give no hard bound on the database.
- A tenant that spends the control lane on its own keys can starve its own
  controllers' renewals, or grow its Leases by 64 bytes per update until
  the ceiling stops the renewals. That harms only its own cluster.
- A TTL delete that paused between its read and its insert for longer
  than the lag could pass its compare-and-swap after a relief pass removed
  the row it conflicts with, and delete a newer value of that key. The 60 s
  timeout (rule 4) ends the delete before that. The timeout is on the
  client: the insert stops when PostgreSQL acts on the cancel request. An
  insert that the server still runs more than 60 s later (a lost cancel
  request) is not bounded. The apiserver's own requests rest on the same
  cancel. Only a key queued from a leased revision takes this path (in
  Kubernetes: Events and master leases). The effect stays in the tenant's
  own data.
- Each kine process counts only its own writes, and nothing fences a
  second writer. Two processes on one database (a rolling update) see
  each other's writes when a good sample shows them, with no time bound.
  The relief stays behind its own poller only. A second process's poller
  can fall behind by more than the lag, through a stall or through too
  little throughput. A row the relief then removes is a gap to that
  poller: it tries to fill the revision (`Fill`), or after a second skips
  it, as for any gap. Either way its watchers miss that event (a delete
  too) until they list again.
  Upstream's compactor has the same exposure for a poller that falls
  `--compact-min-retain` revisions behind.
  **Condition for `--quota-bytes`: one active kine process per database,
  also during the handoff.** Use a Recreate rollout, or stop or drain the
  old process before the new one starts. A rolling update with an overlap
  does not meet this, because a stalled poller of the old process during
  the overlap is exactly this case. k3s with several servers on one
  external datastore runs several kine processes on one database all the
  time, so it does not meet it either.
- When an update's insert fails after its commit (a lost acknowledgement)
  and the reread finds the row, `LogStructured` reports a lost
  compare-and-swap, and the quota gives the bytes back. The figure is then
  short by the sum of all such writes until a good sample shows them.
  Below the limit no lane rate limits them, and there is no time bound.
- The reset watch misses a reset that none of the counters it reads shows.
  On a running source that is a reset after which none of the four counts
  (inserts and vacuums of the table and of its TOAST table) is below the
  watch's baseline: the last sample's counts, with the vacuum counts of a
  refresh since then. For example: no VACUUM since the reset before, and
  as many rows inserted after the reset as before it. At the first sample
  the watch compares the insert counts with the catalog's row estimates,
  and misses a reset when both counts are at or above their estimates (a
  stale `reltuples` can be lower than the rows the counters saw). The estimate then misses the rows written after
  the last VACUUM until autovacuum or autoanalyze brings `reltuples` back.
  That is no stated time or byte bound.
- While samples fail, no sample shows the bytes these gaps leave out.
  Three failed live-size reads in a row make the quota full (item 1). So
  the writes below the limit that no good read followed are those until
  the third failed read: with only the samples of the ticks, about 90 s
  of writes at the 30 s interval; less when the reads after a pass fail
  too, more when a slow pass delays a tick. After that only the lanes
  write, as at the limit.
- kine's own compactor (`--compact-interval`) and autovacuum run outside
  this coordination. A refresh pass can follow a delete of kine's
  compactor too closely, and an autovacuum can follow a relief delete too
  closely. Both read low the way item 5 describes, until the next VACUUM.
  The relief's own passes never overlap each other.

None of this bounds the physical file or the WAL; those are the PostgreSQL
operator's (CNPG volume and WAL limits).

**Invariant.** The sizes are sampled, never queried on the write path
while below the limit: a write costs atomic operations. Only a write while
full reads its key. A failed sample keeps the last value and never clears
the limit. Three failed live-size reads in a row make the quota full until
a good read (`TestKubehzP2FailedSamplesFull`,
`TestKubehzP2FailedSamplesTick`); a write reads that state before the
figure (`TestKubehzP2RecoveryRace`).
The first sample must succeed or kine does not start. On
PostgreSQL the compared figure is live data: if a sync loses a
forwarding method the limit silently becomes a file-size limit, which is
why `TestKubehzP2LiveSizeChain` exists; if it loses the relief forward or
the wiring, B481 comes back, which is why `TestKubehzP2ListenRelief`
exists. The TOAST term, the `reltuples` floor and the reset watch are
load-bearing, which is why `TestKubehzP2P3Postgres` measures the estimate
against `pg_column_size` on a real PostgreSQL: after `pg_stat_reset()`,
after a `RefreshStats`, for rows written after the last VACUUM, for both
reset checks, for a VACUUM that skipped the table, and for the upper bound
while a reset is owed (with `StatsOwed`, the quota's refresh pass and a
refresh pass that the VACUUM skipped). The order of the passes is
load-bearing too: a refresh right after the delete reads low, and a
compaction on every tick starves the refresh. `TestKubehzP2ReliefPasses`,
`TestKubehzP2ReliefNoStarvation`, `TestKubehzP2ReliefSettleFromPassEnd`,
`TestKubehzP2ReliefRefreshPause`, `TestKubehzP2ReliefPausedCompacts`,
`TestKubehzP2ReliefOwedWhileNotFull`, `TestKubehzP2ReliefStatsOwed`,
`TestKubehzP2ReliefNoStats`, `TestKubehzP2ReliefBudgets`,
`TestKubehzP2ReliefBounds` and `TestKubehzP2ReliefRefreshOnly` pin it.
Some rules have no test. A reader of the figure loads the base before the
live sample (`figure()`), and no test hook can pause a reader between the
two loads: keep that order by reading the code. A VACUUM that skips only
the TOAST table has no test (PostgreSQL cannot lock a TOAST table alone).
The PostgreSQL phases take the first reading in the band, so they cannot
show that the figure never read low for a moment. They call the refresh
pass directly: the scheduling (settle, quiet ticks, pause) is tested on the
fake backend only.

**Not an optimisation.** This is a safety bound. There is no benchmark to
re-measure. The test suite is the check.

**If it conflicts.** The new files never conflict. If upstream changes
the flag table, re-add the one `Int64Flag`. If `endpoint.Listen` is
restructured, keep upstream's order (backend `Start`, then `server.New`) and
re-express the block: sample and relieve from the unwrapped backend, hand
the wrapped one to `server.New`. If upstream changes what `pgsql.New`
returns, keep upstream's return value and wrap the dialect in
`quotaDialect` there: `TestKubehzP2P3Postgres` fails when the backend
`New` returns offers no live figure. The forwarding methods are additions next
to `DbSize`. Re-add them wherever `DbSize` lands. If upstream changes
`Backend` so that puts no longer go through `Create`/`Update`, move the
checks to whatever the new write methods are. The tests on a fake backend
show which calls must be refused and which must pass. If upstream changes
the poll loop, keep the `polledRev` store at the top of each iteration: it
must only ever hold a revision whose events were sent. If upstream changes
`SQLLog.Watch` (the relief starts the poller with it) or the transaction
methods the relief uses (`CurrentRevision`, `GetCompactRevision`,
`Compact(from, to)`, `SetCompactRevision`), re-check `kubehz_relief.go`
against the new contract. If upstream changes
`ErrKeyExists` or how an update reports a lost compare-and-swap, re-check
which errors `quotaBackend` settles as written. If the PostgreSQL
statistics flush intervals change (`PGSTAT_IDLE_INTERVAL`, 10 s), re-check
that `quotaRefreshSettle` (15 s) stays above them. If the statistics
reset stops setting the vacuum counts to 0, re-check the reset watch. If `pg_stat_user_tables`
renames `vacuum_count` or `autovacuum_count`, re-check `vacuumCountsSQL`
and `statsCountersSQL`. If `startWatch` changes, keep the relief's poller
start in the background and one at a time. If upstream adds its
own size limit, compare it with B481 before mapping `--quota-bytes` onto
it. `New()` in `pgsql.go` also carries the one-time `ANALYZE` block. Keep
it after `setup()`.

**Re-verify** `go test -tags=test -race -run TestKubehzP2 ./pkg/server/ ./pkg/app/ ./pkg/endpoint/ ./pkg/drivers/pgsql/ ./pkg/logstructured/sqllog/`,
with `KINE_TEST_PGSQL_DSN` set for the PostgreSQL part.

### P3 — storage parameters on the kine table

**Files** `pkg/drivers/pgsql/kubehz_reloptions.go` (new, conflict-free).
`pkg/drivers/pgsql/pgsql.go`: one assignment inside `setup()` after its
CockroachDB probe, and one call in `New()` after `setup()`. The `CREATE
TABLE` statement is upstream's. Test
`pkg/drivers/pgsql/kubehz_reloptions_test.go`. `unit.yml` fails when it is
missing.

**Problem.** With PostgreSQL's defaults, autovacuum runs after 20 % of the
table changed and autoanalyze after 10 %. On a per-tenant kine table that
means the file carries up to a fifth of dead rows above live data, and
P2's `avg_width` can be a tenth of the table stale.

**Approach.** The table carries `autovacuum_vacuum_scale_factor = 0.05`
and `autovacuum_analyze_scale_factor = 0.02`. New or upgraded, it gets
them from one idempotent `ALTER TABLE kine SET (...)` at startup for the
parameters that differ in `pg_class.reloptions` (read as one string
through `array_to_string`), with one INFO line. One code path, one test
table. The `CREATE TABLE` statement stays upstream's, because CockroachDB
rejects a `WITH (...)` clause there and the fork's CI keeps upstream's
CockroachDB matrix. On CockroachDB no statement runs: `setup()` already
probes `select version()` for it, and the flag it sets is the only probe.
Both statements are bounded by 10 s: an anti-wraparound autovacuum can
hold the lock the `ALTER` waits on. Only the pgsql driver has this.

**Invariant.** The parameters live in one map, `kineRelOptions`, and the
`ALTER` renders from it. `relOptionsPlan` is the whole decision (CockroachDB,
nothing to set, or the statement) and the decision table test pins it. A
failed or timed-out `ALTER` is a warning, never a failed start.

**If it conflicts.** The `CREATE TABLE` statement carries nothing of ours.
`TestKubehzP3CreateTableIsUpstreams` fails if a merge lets a `WITH` clause
in. If upstream changes how `setup()` detects CockroachDB, keep the one
assignment after its probe, whatever the probe becomes. If upstream adds
its own storage parameters, keep the lower value for these two. If
upstream restructures `New()`, the only requirement is that
`setRelOptions` runs after the table exists and before serving.

**Re-verify after an upstream merge.** `go test -tags=test -race -run
TestKubehzP3 ./pkg/drivers/pgsql/` passes. Against a real PostgreSQL
(`TestKubehzP2P3Postgres` with `KINE_TEST_PGSQL_DSN`, as `unit.yml` runs
it): after one start, `SELECT reloptions FROM pg_class WHERE oid =
'kine'::regclass` lists both parameters, and a second start runs no
`ALTER` (no "Set storage parameters" line).

### P4 — failed TLS handshakes on the client listener

**Files** `pkg/endpoint/kubehz_tls.go` and `pkg/metrics/kubehz_tls.go`
(new, conflict-free). `pkg/endpoint/endpoint.go`: one marker-wrapped line
in `grpcServer`, between `credentials.NewTLS` and `grpc.Creds`. Test
`pkg/endpoint/kubehz_tls_test.go`. `unit.yml` fails when it is missing.

**Problem.** kine shows nothing for a TLS handshake that fails on its
client listener. gRPC closes the connection and logs the error only at its
own info level, which kine does not show. With `--trusted-ca-file` the
listener refuses a client with no certificate, or with a certificate from
another CA, and nobody can see that it did.

**Approach.** A wrapper around gRPC's server transport credentials calls
the wrapped `ServerHandshake`, reads its error, and returns its results
unchanged. Each failure adds one to `kine_tls_handshake_failures_total`
under one reason: `no_client_cert`, `unknown_authority`, `expired`,
`bad_certificate` (another certificate error: key usage, signature,
parse), `client_alert` (the client stopped the handshake, for example
because it does not trust the server certificate), `not_tls`, `closed`
(the client closed or reset the connection, for example a TCP probe),
`timeout` (gRPC's connection timeout) and `other` (for example gRPC's ALPN
check after a complete TLS handshake). `ErrConnDispatched` is no failure.
With `--debug`, one DEBUG line per failure gives the address, the reason
and the error. The counter is registered only when the listener serves
TLS, with every reason at 0, so a kine without TLS has upstream's metric
set. The metrics listener (`--metrics-bind-address`) is not counted.

**Invariant.** The wrapper changes nothing that is accepted or refused.
The reasons are a fixed set. A reason comes from the error's type where
`crypto/tls` or `crypto/x509` gives one; `crypto/tls` gives the missing
client certificate and some certificate errors as text only. If a Go
release changes that text, the failure counts as `other`.
`TestKubehzP4Reasons` does real handshakes on TLS 1.2 and 1.3 against
the server config that `Listen` builds, so it fails then. In TLS 1.3 the
server refuses a client certificate after the client's side of the
handshake is done, so a test client reads once to see the refusal. A test
client sends its certificate with `GetClientCertificate`, as an etcd
client does: with `Certificates`, `crypto/tls` sends no certificate that
the server's CA list does not name.

**Not an optimisation.** This is an observation. There is no benchmark to
re-measure. The test suite is the check.

**If it conflicts.** The new files never conflict. If upstream
restructures `grpcServer`, keep the wrap on whatever server credentials it
builds. If upstream builds the gRPC server TLS some other way (for example
`grpc.Creds` with other credentials, or TLS on the listener), wrap the
handshake there, or drop P4: it is an observation, not a correctness or
safety fix. If upstream adds its own handshake metric, drop P4. P5 counts
its refusals in this counter (`limit`, `pending_limit`); keep those
reasons if P4 stays.

**Re-verify** `go test -tags=test -race -run TestKubehzP4 ./pkg/endpoint/`.

### P5 — a handshake timeout and connection limits on the client listener

**Files** `pkg/endpoint/kubehz_listen.go`,
`pkg/endpoint/kubehz_listen_linux.go` and
`pkg/endpoint/kubehz_listen_other.go` (new, conflict-free).
`pkg/endpoint/endpoint.go`: three `Config` fields, a check at the start of
`Listen`, one line after `createListener`, one option in `grpcServer` and
one credentials wrap after P4's, each marker-wrapped. `pkg/app/app.go`:
three flags. `pkg/endpoint/kubehz_tls.go` (P4): the reasons `limit` and
`pending_limit`. `go.mod`: `golang.org/x/sys` is a direct dependency.
Tests `pkg/endpoint/kubehz_listen_test.go`,
`pkg/endpoint/kubehz_listen_linux_test.go` and
`pkg/app/kubehz_listen_test.go`. `unit.yml` fails when they are missing.

**Problem.** kine sets no bound of its own on its client listener. A new
connection has gRPC's 120 s for the TLS handshake and the HTTP/2
preface, and kine accepts any number of connections. In front of kine, a
TLS passthrough gateway cannot bound a handshake it does not terminate,
and some gateways cannot limit connections. So slow or idle connections
can hold file descriptors and goroutines for 120 s each, with no limit.
A single limit alone is not enough: clients with no valid certificate can
fill it with handshakes they never end, and then a valid client cannot
connect until one of them times out.

**Approach.** Three flags, all off by default.
- `--listen-handshake-timeout` (default 0: gRPC's 120 s) sets gRPC's
  `ConnectionTimeout`: an absolute deadline that gRPC puts on a new
  connection before the TLS handshake and clears when the HTTP/2
  transport is set up (the client preface and the first SETTINGS). Bytes
  that trickle in do not extend it, and an established connection does
  not keep it. A TLS handshake that runs out of it counts as `timeout` in
  P4's counter; an HTTP/2 setup that runs out of it is not counted.
- `--listen-max-connections` (default 0: no limit) wraps the listener.
  Without a pending budget, a connection counts from `Accept` until its
  first `Close`, also during its handshake. Over the limit, `Accept`
  closes the new connection before kine reads from it, counts it as
  `limit` in `kine_tls_handshake_failures_total` (the counter is
  registered only with TLS), and waits for the next one.
- `--listen-max-pending-handshakes` (default 0: off; needs TLS) splits
  that budget. A connection whose TLS handshake has not ended counts
  against it. Over it, `Accept` closes the new connection before kine
  reads from it and counts it as `pending_limit`. A credentials wrapper
  outside P4's moves a connection to `--listen-max-connections` when its
  handshake succeeds; when that limit is full it closes the connection
  and counts it as `limit`. So handshakes that stall cannot take the
  slots of connections that ended one (with `--trusted-ca-file`: of
  authenticated clients). With `--debug`, one DEBUG line per refusal.
- gRPC sets `TCP_USER_TIMEOUT` (its keepalive timeout, 20 s in kine) only
  on a `*net.TCPConn`, and the wrapper hides that type. So on Linux the
  wrapper sets the same value itself; elsewhere gRPC sets nothing either.
  It bounds unacknowledged writes; it is no idle limit.
- A negative value, a pending budget without `--cert-file` and
  `--key-file`, or any of the three with `Config.GRPCServer` set (a server
  that the caller built), makes `Listen` fail.
- The keepalive settings stay etcd's, with no flag: client pings no more
  often than every 5 s and none without an active stream; server pings
  after 2 h idle, with a 20 s timeout.

**Invariant.** With all three at 0, kine builds the same gRPC server and
serves the same listener as upstream. Each accepted connection counts
against a budget and frees it once. During the move after the handshake
it briefly counts against both: the move reserves its slot of the limit
before it frees its pending slot, so it can refuse early but never pass a
limit. The move and `Close` swap one atomic state, so a `Close` during the
move, or two `Close` calls at once, free one slot. gRPC closes the accepted
connection on every path (a failed handshake, a failed transport setup,
the end of the transport), so no slot leaks. The limits count all client
connections, not the connections of one source: one source can still
take every pending slot, and while the pending budget is full no new
client gets in, valid or not. The limits protect the resources and the
connections that are up; they cannot admit an unknown client during an
attack. A limit per source belongs in front of kine (a firewall, or a
gateway that keeps the source address, for example with PROXY protocol).

**Not an optimisation.** This is a safety bound, off by default. The test
suite is the check.

**If it conflicts.** The new files never conflict. If upstream adds its
own handshake timeout or connection limit, take upstream's and drop that
part of P5; say in the changelog what happens to the flag names. If
upstream wraps the listener itself (for example with `cmux`), keep the
limit as the outer wrapper, so that it counts every connection. Keep the
credentials wrap of the pending budget outside P4's, so P4 counts the TLS
result and P5 its own refusals; it needs the connection gRPC hands to
`ServerHandshake` to be the one `Accept` returned. If gRPC stops setting
`TCP_USER_TIMEOUT`, or finds the socket through an interface the wrapper
can pass on, drop `setUserTimeout`. If gRPC removes
`grpc.ConnectionTimeout` (it is experimental), set the deadline in the
wrapper instead, and clear it once the transport is up
(`TestKubehzP5WatchOutlivesTimeout`).

**Re-verify** `go test -tags=test -race -run TestKubehzP5 ./pkg/endpoint/ ./pkg/app/`.

## Patches considered and deliberately NOT taken

Recorded so nobody spends the effort twice. Each was measured.

| candidate | measured | why not |
|---|---|---|
| TOAST `SET STORAGE EXTERNAL` on `value` | null at 512 B, 2 KB, 8 KB and 64 KB | no effect anywhere across the TOAST boundary |
| Covering index / dropping redundant indexes | +11.7 % on tmpfs, **±0.0 % on disk** | only helps when not I/O-bound; upstream issue #596 proposes it |
| `--poll-batch-size` 100 or 2000 | inside noise | no effect either direction |
| Table `fillfactor` | −2.7 %, inside noise | HOT updates need UPDATEs; kine only ever INSERTs and DELETEs |
| Dropping `kine_prev_revision_index` | −1.4 %, inside noise | not costing anything measurable |
| UNLOGGED table | +46 % | **not replicated**; a routine failover promotes an empty table |
| Table partitioning | not possible | PostgreSQL requires a unique constraint to include every partition column; kine needs both `PRIMARY KEY (id)` and `UNIQUE (name, prev_revision)`, and no single partition key satisfies both |
| Write batching | 26.8x on slow storage, **1.5x on a fast WAL** | it substitutes for a fast WAL device rather than adding to it; a storage decision beats a code change here |

## Known upstream issues this fork relates to

- **#63** "postgres db backend connection pooling" — open since 2020-11-18.
- **#596** "PostgreSQL: Drop redundant indexes, add covering index, tune TOAST
  and connection pool" — open since 2026-02-15, proposes changes this fork
  measured and mostly did not adopt.

Both are configuration or schema matters rather than code we must carry:
**kine's connection-pool defaults are the single largest correctness-adjacent
problem and they need no patch at all.** Set
`--datastore-max-open-connections` below the server's `max_connections` **and**
`--datastore-max-idle-connections` equal to it. The default max-idle of 20
against an unbounded max-open produces connection churn that presented as
**7.80 % write errors** on kine's defaults and **6 % against a real apiserver
even after capping max-open alone**.

## Image releases (GHCR)

The patched image ships as a PUBLIC package at `ghcr.io/kernpilot/kine` —
tenant extCP control planes pull it from THEIR machines with no credentials.

Version scheme: `v<upstream>-kubehz.<n>`

- `<upstream>` — the k3s-io/kine release tag last merged into `main`
  (`git describe --tags upstream-master`, after the sync landed).
- `<n>` — the patchset release counter on that base. Bump it for any release
  from the same base; a sync with a newer upstream resets it to 1.

Release = push the tag; `.github/workflows/publish-kubehz.yml` builds
linux/amd64 + linux/arm64 (CGO_ENABLED=0 — drops only sqlite/dqlite; the
extCP rung is postgres-only), pushes the version tag plus the moving
`kubehz` tag, signs the digest keyless with cosign, attaches buildkit
provenance and SBOM, attests a standalone SPDX SBOM, and verifies all of it
before the run goes green. The run's summary prints the digest; copy it into
the "Released" section of `CHANGELOG.md`.

GHCR does not enforce tag immutability, so consumers pin the DIGEST
(`KUBEHZ_EXTCP_KINE_IMAGE`), never the tag; `kubehz` exists for humans. To
verify a release (no key, no account):

```bash
cosign verify \
  --certificate-identity-regexp '^https://github\.com/kernpilot/kine/\.github/workflows/publish-kubehz\.yml@refs/tags/v[0-9]+\.[0-9]+\.[0-9]+-kubehz\.[0-9]+$' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  ghcr.io/kernpilot/kine@sha256:<digest>
```

The ref anchor matters: the workflow also runs on `workflow_dispatch` from
any branch (edge builds, tagged by sha), and those images are signed too.
Only a `v*-kubehz.*` tag ref passes the command above. A `workflow_dispatch`
on an EXISTING tag ref rebuilds and re-pushes that version tag and moves
`kubehz` to the new digest; the old digest stays valid and signed, which is
why consumers pin the digest, not the tag.

`v0.17.0-kubehz.2` is the first signed release.
`v0.17.0-kubehz.1` predates the signing lane and is unsigned; see the
CHANGELOG release section.
