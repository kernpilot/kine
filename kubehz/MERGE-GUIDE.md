# Merge guide — how to rebase this fork onto new upstream kine

**Read this before resolving any conflict in this repository.** It is written for
whoever (or whatever) does the merge, and it assumes no memory of why these
patches exist.

## The model

| branch | what it is |
|---|---|
| `upstream-master` | a pristine mirror of `k3s-io/kine` master. Never edit it. |
| `main` | `upstream-master` plus the patches below, plus `kubehz/` and `benchmarks/`. |

Updating to a newer upstream:

```bash
git fetch upstream                       # k3s-io/kine
git checkout upstream-master && git merge --ff-only upstream/master
git push origin upstream-master
git checkout main && git rebase upstream-master
# resolve using the rules below, then:
cd benchmarks && ./verify-patches.sh     # re-measures every claim in CHANGELOG.md
```

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
4. **When in doubt, drop the patch.** Every patch here is an optimisation or a
   safety bound, and each marker says which. None is required for kine to be
   correct. A dropped patch that is recorded in `CHANGELOG.md` is recoverable; a
   half-merged one is a silent bug.
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

### P1 — cross-instance watch wake-up

**Files** `pkg/drivers/pgsql/notify.go` (new, conflict-free),
`pkg/drivers/pgsql/pgsql.go`, `pkg/logstructured/sqllog/sql.go`.

**Problem.** kine signals its own poll loop in-process on every insert, so a
single instance wakes its watchers in milliseconds. That signal does not cross a
process boundary. With two kine instances on one database, a watcher on the
instance that did not receive the write waits for its 1 second fallback ticker.
Measured: p50 **715 ms**, p99 **716 ms**, against 18/21 ms on the writing
instance. Multi-replica kine is what Kamaji and k0smotron deploy.

**Approach.** A notification carries no data — it only means "poll now", and
that poll drains everything waiting. So one notification per interval is worth
as much as one per row. This emits at most one `pg_notify` per 10 ms from a
dedicated connection on a background goroutine; the write path pays a single
atomic compare-and-swap.

**Rejected alternative, do not reintroduce.** An `AFTER INSERT` trigger calling
`pg_notify` per row is the obvious implementation and it is unusable: measured,
it cut write throughput **96.8 %** (5076 → 163 writes/s) and pushed put p99 from
39.5 ms to 876 ms, because every backend serialises on PostgreSQL's shared
async-notification queue.

**Measured** cross-instance watch p50 715.7 → 20.0 ms, p99 716.6 → 21.3 ms, with
saturated write throughput unchanged (5076 → 5124/s, inside a 0.7 % noise
floor). Confirmed through real apiservers in an HA topology: p50 499.6 → 15.2 ms,
p99 1000.9 → 25.3 ms.

**Invariant.** The 1 second poll ticker stays. `NOTIFY` fires on COMMIT and is
not durable — a listener that reconnects cannot replay what it missed, so the
channel may only make a poll happen sooner. Verified by terminating the LISTEN
backend mid-run: **0 events lost**, p99 degraded to 712.9 ms for the reconnect
window, then recovered. Without the ticker that same failure drops watch events
silently, which is far worse than being late.

**If it conflicts.** Keep upstream's `select{}` shape and re-add the
`case <-external:` arm. `RevisionNotify()` is deliberately an *optional
interface* asserted at the call site, not a method on `server.Dialect`, so the
sqlite, mysql and nats drivers never see it — preserve that. If the dialect
plumbing changes beyond easy repair, drop P1: it is a latency optimisation, not
a correctness fix.

**Re-verify** `benchmarks/run-cross.sh` (two kine instances, one database).

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

- `<upstream>` — the k3s-io/kine tag `main` is currently rebased onto
  (`git describe --tags upstream-master`).
- `<n>` — the patchset release counter on that base. Bump it for any release
  from the same base; a rebase onto a newer upstream resets it to 1.

Release = push the tag; `.github/workflows/publish-kubehz.yml` builds
linux/amd64 + linux/arm64 (CGO_ENABLED=0 — drops only sqlite/dqlite; the
extCP rung is postgres-only) and pushes the immutable version tag plus the
moving `kubehz` tag. Consumers PIN the immutable tag
(`KUBEHZ_EXTCP_KINE_IMAGE`); `kubehz` exists for humans.
