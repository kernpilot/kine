# kine benchmark suite

Lives in this fork on purpose. Every patch in `kubehz/CHANGELOG.md` is justified
by a number, and every one of those numbers depends on kine's version,
PostgreSQL's version and the storage underneath — all of which move. Keeping the
suite next to the code means a future maintainer can answer "is this patch still
worth carrying?" by running it.

```bash
./setup.sh          # three PostgreSQL backends (disk, tmpfs, split-WAL)
./build.sh          # kine-upstream + kine-patched, both from this repo
./verify-patches.sh # re-measure every claim in the changelog
```

## The three backends, and why

Storage dominates every kine-level knob measured here by more than an order of
magnitude, so a result is meaningless without knowing which backend produced it.
Every result file records its backend.

| backend | port | use |
|---|---|---|
| `kine-bench-pg` | 55432 | real disk — the only place durability settings mean anything |
| `kine-bench-mem` | 55442 | tmpfs — for experiments about kine's logic, where disk is a confounder |
| `kine-bench-walmem` | 55452 | table data on disk, `pg_wal` on tmpfs — isolates commit fsync |

**tmpfs is an instrument, not a configuration.** A production WAL on tmpfs is
unrecoverable after an unclean stop. It is used here to locate bottlenecks.

## Things this suite learned the hard way

Each of these produced a confident, wrong number first. They are listed because
the same traps catch anyone repeating the work.

1. **Write the way Kubernetes writes.** The apiserver never issues a bare `Put`
   for an existing key — every update is a transaction comparing `ModRevision`,
   and kine accepts only that shape. With bare `Put`s, kine silently discards
   the write on a `prev_revision` unique-index collision while still returning
   success: 200 puts over 10 keys persisted 29 rows, over-reporting throughput
   ~7x.
2. **Check delivery against rows persisted, not writes acknowledged.** An
   apparent 99 % watch-event loss was entirely the harness.
3. **Payload compressibility is a load parameter.** A repeated-byte filler
   compresses ~20:1, so the benchmark wrote a twentieth of the bytes it claimed.
   Kubernetes payloads are protobuf and compress poorly; the filler is now
   incompressible and seeded.
4. **Closed-loop drivers hide saturation.** Measuring from each operation's
   scheduled time rather than its send time changed a reported p99 of 40 ms into
   13 688 ms at the same offered load. `loadgen2` is the open-loop driver; use it
   for anything latency-related.
5. **Sample counts matter.** Percentiles from ~9 samples come out identical to
   each other and look clean. `-latency-sample-every 1` for low-rate runs, and
   every result records its sample count.
6. **Never compare across shapes.** `analyze.py` refuses to compute a delta
   between runs with different writer or watcher counts, because doing so once
   rendered a 50-writer run as a "+47 % win" against a 100-writer baseline.
7. **kine's watch buffers are unbounded in bytes.** An unguarded aged-table run
   took a 124 GB host to 120 GB used with all swap consumed. `run.sh` now
   refuses to start below `MIN_FREE_GB` and kills a run exceeding
   `KINE_RSS_CAP_GB`, recording peak RSS with every result.

## Layout

| path | what |
|---|---|
| `run.sh` | one run: kine + load, against a chosen backend |
| `run-cross.sh` | two kine instances on one database — the only topology where the poll ticker is visible |
| `run-apiserver.sh`, `run-apiserver-ha.sh` | a real kube-apiserver on kine |
| `loadgen/`, `loadgen2/` | etcd-API drivers; `loadgen2` is open-loop with Zipfian keys |
| `apiload/` | drives a real apiserver over the Kubernetes REST API |
| `sweep-*.sh` | the experiment sets |
| `variants/` | SQL applied after kine creates its schema |
| `analyze.py` | scores results against a measured noise floor |
| `results/` | one JSON per run, plus the applied config and peak RSS |
