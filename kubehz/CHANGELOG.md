# Changelog — kubehz fork of k3s-io/kine

Every entry names the measurement that justifies it and the command that
re-checks it. A patch whose benefit stops reproducing should be dropped, and
dropping it is recorded here too.

Format: each patch has a stable id (`P1`, `P2`, …) that also appears as a
`KUBEHZ-PATCH <id>` marker in the source and a section in
[MERGE-GUIDE.md](MERGE-GUIDE.md).

---

## Unreleased — forked from `6fb95f5` (go1.26 / etcd 3.7 / kubernetes 1.37)

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

## Open hazards in upstream kine, not yet patched

Carried here because they bound how kine can be deployed, not because they are
fixed.

- **A watch starting at revision 0 reads the entire table into memory, per
  watcher, unbounded.** `logstructured.go:239` calls
  `l.log.After(ctx, key, end, revision, 0)` — **limit zero** — and
  `watch.go:145` passes the client's `StartRevision` through unsubstituted, so a
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
  buffer is full, and `server/watch.go:243-247` then sends `Canceled: true` with
  **`CompactRevision: 0` and an empty reason**, where kine's own signal for an
  invalid position (`watch.go:177`) is `Cancel(id, currentRev, compactRev,
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
  `wr.CompactRevision` and a reason into the `Cancel` at `watch.go:245` instead
  of the literal zeros.

Neither `P2` nor `P3` is being carried. P2 was written, measured, found to
change nothing, and reverted; P3's trigger could not be demonstrated. Both
hazards are real and both remain unexplained at the mechanism level. Neither has
an upstream issue.

**The fork currently carries exactly one patch, P1.** That is the honest state:
the watch-path hazards that motivated forking turned out to need diagnosis
before they need code.
