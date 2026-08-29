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

**Re-check** `benchmarks/run-cross.sh`

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

## Open hazards in upstream kine, not yet patched

Carried here because they bound how kine can be deployed, not because they are
fixed.

- **Watch buffering is unbounded in bytes.** `broadcaster.go` gives each
  subscriber 100 batches and `sqllog/sql.go` gives each watcher another 100;
  each batch holds up to `--poll-batch-size` events, each carrying the full
  object value. Nothing bounds that in bytes. Measured: kine at **1.02 GB** on a
  fresh table and **>12 GB within 60 s** once the table held prior data, at 100
  writers and 256 watchers — reproducible in two arms, and `--compact-interval
  30s` did not prevent it. An unguarded run took a 124 GB host to 120 GB used
  with all swap consumed. The failure mode is the host OOM killer rather than
  kine shedding load.
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

`P2` (bound the watch buffers) is justified and outstanding — it reproduces at
1.02 GB on a fresh table and >12 GB once the table holds history. `P3` is not
justified until the drop path can be demonstrated. Neither has an upstream issue.
