# PostgreSQL server tuning — sweep results, 2026-09-01

`sweep-pgtune.sh` plus four follow-on arms. The question this had to answer:
**does `shared_buffers` sizing reproduce a real win over stock, and what is the
honest put latency once a tenant database is at its fill ceiling?**

Short answer: yes, but only when `shared_buffers` and `max_wal_size` are raised
**together** — each alone measures null — and the win is **+7.0 %**, not the
order of magnitude the sweep's header implies. 4 GB of `shared_buffers` is no
better than 2 GB. At the 5 GiB per-tenant ceiling a saturating tenant sees put
**p50 4.6 ms / p95 14.2 ms / p99 23.1 ms** at ~17,000 writes/s.

---

## The rig

| | |
|---|---|
| host | 24 core, 124 GB RAM, docker root on **btrfs over dm-crypt** (LUKS, NVMe, `/mnt/scratch`) |
| shared with | the `kubehz-dev` kind cluster, two other kind clusters, four registries — loadavg 50-70 throughout |
| PostgreSQL | `postgres:18.3`, recreated from `setup.sh` (prod extcp-ds runs **18.4**; see caveats) |
| kine | `kine-patched` as built, pool 80/80 (`KINE_MAX_OPEN`/`KINE_MAX_IDLE`) |
| shape | 60 s, 100 writers, 256 watchers, 2 KB incompressible values, unless noted |

Two backends, both rebuilt for this sweep:

- **`kine-bench-pg`** — `-p 55432:5432`, anonymous volume. Exactly `setup.sh`.
- **`kine-bench-walmem`** — data on the btrfs volume, `pg_wal` on tmpfs.
  **One deviation from `setup.sh`:** the container-level `--tmpfs /pgwal` was
  replaced with a bind mount of a host tmpfs directory. `docker restart`
  recreates a `--tmpfs` mount **empty**, and PostgreSQL then refuses to start:
  `invalid checkpoint record` / `PANIC: could not locate a valid checkpoint
  record`. Every restart-based sweep is incompatible with the backend as
  `setup.sh` builds it. Verified by doing it. See *Defects*.

### The warm-up, and what the "72.9 % noise floor" actually was

Four discarded runs (`pgtune-warmup-r1..4`) preceded the disk arm:

| | r1 | r2 | r3 | r4 |
|---|---|---|---|---|
| writes/s | 4,779 | 5,064 | 4,952 | **3,514** |

Every disk-arm baseline in this suite has the same shape — `base-a`
4946/3146/3062, `v2dsk-base-a` ~5100-5400 then `v2dsk-base-b` ~3100-3190,
`cow-yes` 5422/4633/3164/3157. It is not noise. It is a **storage transient**:
after a few minutes of sustained ~1.5 GB/min rewriting, the encrypted NVMe
settles into a slower steady state and stays there, and it recovers after an
idle period. A block-ordered sweep run on a cold volume charges that decay to
whichever config ran later.

With the volume warmed first, the disk arm's within-config spread collapsed
from the recorded **72.9 %** to **0.5-3.1 %**. The pgtune numbers AUDIT recorded
as *unmeasured* were not unmeasurable — the instrument was cold.

Rig consistency against history: warmed-up cold-state runs 4,779-5,064 against
the historical 4,946-5,422; steady-state `pg-stock` 2,938-3,295 against the
historical 3,062-3,190. Both regimes reproduce; the box is ~3 % slower than in
August.

---

## Arm A — `sweep-pgtune.sh` as written, disk backend

`REPS=3`, run after the warm-up. All settings verified in force
(`results/pg-*.pgsettings.txt`); `shared_buffers` was **not** clamped.

| config | n | writes/s (median) | runs | spread | p50 | p95 | p99 | vs stock |
|---|---|---|---|---|---|---|---|---|
| `pg-stock` | 3 | 3,026 | 3295 3026 2938 | 12.2 % | 30.1 | 54.7 | 69.5 | — |
| `pg-buffers` | 3 | 2,892 | 2895 2892 2880 | 0.5 % | 33.4 | 55.0 | 65.8 | −4.4 % |
| `pg-wal` | 3 | 2,886 | 2922 2861 2886 | 2.1 % | 32.0 | 54.3 | **101.3** | −4.6 % |
| `pg-both` | 3 | 2,928 | 2868 2928 2956 | 3.1 % | 32.3 | 54.8 | 65.1 | −3.2 % |

`pg-stock`'s 12.2 % spread is its r1 alone (3,295 — the tail of the warm-up
decay); r2/r3 are 3,026/2,938.

**Reading: null.** On a datastore whose WAL fsync is the constraint, nothing
Postgres is configured to do about buffers or checkpoints matters. The one
signed observation is `pg-wal`'s p99 — 101 ms against 65-70 ms elsewhere. A
`max_wal_size` of 8 GB with a 30-minute checkpoint timeout does not remove the
checkpoint, it defers it into one larger burst, and on slow storage that burst
is visible in the tail.

### The same script had run before

`results/pg-*.json` already existed, dated 2026-08-29 07:46-07:55 UTC, and their
means are exactly the numbers AUDIT filed as *unmeasured*: stock 3,756
(3098/4413), buffers 3,746 (3171/4320), wal 3,061 (3010/3113), both 4,133
(3236/5030). Those files are overwritten by this run; the originals are in git
at the commit that preceded it.

---

## Arm B + D1 — the same four configs on the low-variance backend, interleaved

`kine-bench-walmem`. Passes alternate direction (`stock buffers wal both` /
`both wal buffers stock` / `stock buffers wal both`) so drift is shared, and
Arm D1 adds a 4 GB point with its own interleaved stock controls. Every
config's settings verified in force each time.

| config | n | writes/s (median) | min-max | vs stock | p50 | p95 | p99 |
|---|---|---|---|---|---|---|---|
| stock (128 MB / 1 GB WAL) | 5 | 10,063 | 9,613-10,294 | — | 6.4 | 31.5 | 49.7 |
| `shared_buffers` 2 GB only | 3 | 10,297 | 10,258-10,482 | +2.3 % | 6.3 | 30.7 | 49.4 |
| WAL block only (8 GB) | 3 | 10,266 | 9,998-10,406 | +2.0 % | 5.8 | 33.2 | 54.2 |
| **both, 2 GB** | 3 | **10,792** | 10,681-10,917 | **+7.2 %** | 5.8 | 30.4 | 49.1 |
| **both, 4 GB** | 3 | 10,582 | 10,524-10,796 | +5.2 % | 6.0 | 30.6 | 49.1 |
| both, pooled 2+4 GB | 6 | 10,737 | 10,524-10,917 | +6.7 % | 5.9 | 30.5 | 49.1 |

The five stock runs span 9,613-10,294; the six tuned runs span 10,524-10,917.
**The two populations do not overlap**, across eleven runs interleaved over
25 minutes. Each leg alone lands inside the stock spread. The pair does not.

### Why, from the server's own counters

| config | buffer hit % | `blks_read` / 60 s | checkpoints / 60 s | buffers written per checkpoint round |
|---|---|---|---|---|
| stock 128 MB | 98.85 | ~600,000 | 7-8 | ~2,500 |
| 2 GB buffers, 1 GB WAL | 99.995 | ~3,000 | 8 | ~455,000 |
| 128 MB, 8 GB WAL | 98.83 | ~610,000 | 0 | 0 |
| 2 GB buffers, 8 GB WAL | 99.99 | ~3,000 | 0 | 0 |
| 4 GB buffers, 8 GB WAL | **100.000** | **~205** | 0 | 0 |

`shared_buffers` alone does exactly what it is supposed to — physical reads fall
**237x** — and buys nothing, because the pages it now keeps are dirty and a
1 GB `max_wal_size` still forces a checkpoint every ~8 s, which then has to
write 455,000 of them in a burst instead of letting backends dribble 2,500 out.
Raising the WAL alone removes the checkpoints and leaves the 600,000 reads. Only
both together remove both costs. **This is why the original block-ordered sweep
found nothing coherent: three of its four cells are cancellations.**

4 GB drives physical reads to essentially zero (205 blocks) and is still not
faster than 2 GB. The working set is ~3.3 GB per run; 2 GB already holds
everything the write path re-touches.

---

## Arm F — the config that can actually ship

Arm B's winner is not deployable. `extcp-ds` has a **dedicated 5 GiB
`walStorage` volume** on `local-path`, which cannot be expanded, and
`max_wal_size=8GB` would fill it. This arm measures the deployable pair
directly, interleaved 3 v 3.

`shared_buffers=2GB effective_cache_size=6GB max_wal_size=2GB
min_wal_size=512MB checkpoint_timeout=15min wal_buffers=64MB`

| config | n | writes/s (median) | runs | p50 | p95 | p99 | max |
|---|---|---|---|---|---|---|---|
| stock | 3 | 9,898 | 9796 9946 9898 | 6.5 | 32.5 | 50.4 | 140 |
| **candidate** | 3 | **10,590** | 10590 10486 10624 | 6.0 | 30.8 | 49.4 | 135 |

**+7.0 %**, populations disjoint (stock max 9,946 < candidate min 10,486). The
tail does not regress: p99 49.4 vs 50.4 ms, max 135 vs 140 ms.

Checkpoint work, same runs: 3 checkpoints per 60 s instead of 7, checkpoint
`write_time` 27.6 s instead of 41.3 s, and `sync_time` **0.8 s instead of
5.2 s** — a 6x drop in the fsync the checkpoint has to force. On ceph-backed
data volumes that sync is more expensive than it is here, so this is a lower
bound.

**Peak `pg_wal` measured, not estimated:** a 120 s saturating run at 10,652
writes/s (~55 MB/s of WAL) peaked at **2,064 MB** — 40 % of the 5 GiB volume.
`max_wal_size` caps retention, not rate, so this number does not grow with
prod's much lower write rate.

---

## The full-fill latency number

`extcp-ds` gives each tenant a **5 GiB `tenantStorageCeiling`**, and kine's
compaction is off by default, so a tenant database spends most of its life near
that ceiling rather than empty — which is the state every other number in this
suite is measured in.

### First attempt failed, and the failure is the more useful result

Arm C kept the table and ran again at 1,074,590 rows / 6,082 MB. **Every run
died within 9-10 seconds** with kine RSS over the 16 GB cap — 17.0 GB, 19.4 GB,
21.4 GB observed. The load generator's watchers connect from revision 0, so each
one replays the whole table: the **catch-up read** the fork's `CHANGELOG.md`
records as the single *unbounded* watch cost (1.22 → 12.89 GB at 657 k rows).
Reproduced independently in Arm D2 at 3.3 GB / ~620 k rows.

**At the 5 GiB tenant ceiling, a revision-0 watcher is not a slow client, it is
an out-of-memory condition.** A Kubernetes reflector LISTs and then WATCHes from
the revision it got, so an apiserver does not do this — but `etcdctl watch`,
monitoring agents and hand-written controllers do.

### Second attempt, reflector-shaped watchers

Re-run with `-watch-from-current`, 100 writers / 32 watchers — **its own series,
not comparable to the 256-watcher arms** — on walmem with 2 GB buffers.

| generation | rows at end | total relation | writes/s | p50 | p95 | **p99** | max |
|---|---|---|---|---|---|---|---|
| g0 (empty → 5.8 GB) | 1,025,863 | 5,797 MB | 17,092 | 4.6 | 14.2 | **23.1** | 119 |
| g1 (→ 11 GB) | 2,025,044 | 11 GB | 16,651 | 4.6 | 14.2 | 23.2 | 84 |
| g2 (→ 16 GB) | 2,921,709 | 16 GB | 14,942 | 4.9 | 15.5 | 26.0 | 1,016 |
| g3 (→ 21 GB) | 3,696,175 | 21 GB | 12,904 | 5.5 | 18.2 | 31.9 | 618 |
| stock @ 24 GB | 4,283,392 | 24 GB | 9,784 | 7.5 | 21.8 | 42.6 | 386 |
| tuned @ 27 GB | 4,879,828 | 27 GB | 9,938 | 6.9 | 23.2 | 42.0 | 436 |
| stock @ 30 GB | 5,390,925 | 30 GB | 8,515 | 8.5 | 24.5 | 49.2 | 434 |
| tuned @ 34 GB | 6,062,525 | 34 GB | 11,191 | 6.5 | 21.1 | 34.3 | 426 |

**The number: at the 5 GiB ceiling, put p50 4.6 ms / p95 14.2 ms / p99 23.1 ms,
at ~17,000 writes/s, for one tenant saturating the datastore alone.**

Degradation with fill is graceful and roughly linear: at **4x** the ceiling
(21 GB) p99 is 31.9 ms and throughput has fallen 25 %. Fill is not a cliff. The
`max` column tells the other half — a 1,016 ms outlier at g2 — checkpoint bursts
on a large dirty set produce second-scale stalls that the percentiles hide.

The stock-vs-tuned pair inside this arm is **not** a measurement: the table grows
~3 GB per run, so each config faces a different table. Reported for completeness
only.

**What this does not say.** One tenant alone. The 500-tenant shape is the D5
wall sweep's number (worst-tenant p99 224 ms, itself instrument-contaminated).
Concurrency dominates fill by an order of magnitude; do not add these.

---

## Conclusions

1. **`shared_buffers` alone: null.** +2.3 %, inside the stock spread, on a rig
   that resolves 3 %. It removes 99.5 % of physical reads and converts them into
   a checkpoint write burst of equal cost.
2. **WAL sizing alone: null.** +2.0 %. And on slow storage it costs p99
   (101 ms vs 65-70 ms, Arm A).
3. **Together: +7.0 %, reproducible, populations disjoint.** Measured twice —
   at the bench's 8 GB WAL (+7.2 %) and at the deployable 2 GB WAL (+7.0 %).
   The mechanism is understood and reads on the server's own counters.
4. **4 GB of `shared_buffers` buys nothing over 2 GB.** The earlier bench's
   pinned 4 GB was oversized for this working set.
5. **The disk arm's "72.9 % noise floor" was an un-warmed instrument.** Four
   discarded runs take it to ~3 %. Any future sweep on `kine-bench-pg` should
   warm the volume first, or interleave, or both.
6. **Full-fill latency is not the risk. Revision-0 watchers are.** p99 rises
   23 → 32 ms across four times the tenant ceiling; a single revision-0 watcher
   on a table at the ceiling takes kine past 16 GB RSS in under ten seconds.

## Defects found

- **`sweep-pgtune.sh` cannot use any backend but `kine-bench-pg`.** It reads
  `$PG_CONTAINER` but hardcodes `PG_PORT=55432 PG_EXEC_PORT=5432` in its
  `run.sh` call, and its `psql` helper omits `-p`. Pointing it at
  `kine-bench-mem` or `kine-bench-walmem` dials the wrong port. Not fixed —
  arms B/D/E/F ran from standalone drivers instead.
- **`sweep-pgtune.sh` records nothing for the stock arm.**
  `results/pg-stock.pgsettings.txt` is written empty, because the verification
  loop iterates the settings *passed in* and stock passes none. A reader cannot
  tell from the artifacts what stock was.
- **`docker restart` destroys `kine-bench-walmem`.** `setup.sh` gives it a
  container-level `--tmpfs /pgwal`, which Docker recreates empty on restart;
  PostgreSQL then `PANIC`s with `could not locate a valid checkpoint record` and
  the container will not come back. Any restart-based sweep against that backend
  bricks it. Worked around here with a host tmpfs bind mount.
- **`checkpoint_completion_target=0.9` is a no-op** in `pg-wal` and `pg-both` —
  0.9 has been the PostgreSQL default since 14, and `postgres:18.3` reports it
  as such.
- The sweep header's "the kine table reached 1.5 GB in a single 60-second run"
  holds only in the *cold* storage regime. Warmed, the same run produces
  ~820 MB on the disk arm.

## Caveats

- `postgres:18.3` here; live extcp-ds runs **18.4**. The D5 wall sweep used 18.4.
- Storage is local btrfs-over-LUKS. Prod extcp-ds data is on **ceph-ha**
  (replica-3, network) with WAL on node-local NVMe. The walmem arm has the right
  *shape* (fast WAL, slower data) but a faster data path. Physical reads and
  checkpoint syncs both cost more on ceph, so +7.0 % is a lower bound there.
- The host ran three kind clusters throughout (loadavg 50-70). Interleaving,
  not isolation, is what makes these comparisons hold.
