# kubehz fork of k3s-io/kine

A small set of benchmark-justified patches on top of upstream kine, plus the
benchmark suite that justifies them.

**Start here:**

| file | what |
|---|---|
| [MERGE-GUIDE.md](MERGE-GUIDE.md) | **read before resolving any conflict.** How to sync with a new upstream release, what each patch must preserve, and when to drop one. |
| [CHANGELOG.md](CHANGELOG.md) | every patch, the measurement behind it, and the command that re-checks it. Also what was measured and deliberately *not* patched. |
| [../benchmarks/](../benchmarks/) | the suite. `./setup.sh && ./verify-patches.sh` re-checks the patches; `./build.sh` builds the binaries the `run-*.sh` benchmarks compare. |

## Why this fork exists

Upstream kine is actively maintained (12-38 commits/month), but its PostgreSQL
path moves slowly: issue #63 on connection pooling has been open since
2020-11-18, and #596, proposing schema and pool changes, since 2026-02-15.
Waiting for a merge is not a plan for PostgreSQL-specific work.

The fork is deliberately narrow. It carries two patches, P2 and P3. P1
(the cross-instance watch wake-up) was dropped on 2026-11-08: see
CHANGELOG.md. Its last version is on branch `archive/p1-port-v0.17.5`.

P2 (the `--quota-bytes` limit on live data, and the B481 lanes and relief
that keep a full cluster running and cleaning up) adds
`pkg/server/kubehz_quota.go`, `pkg/metrics/kubehz_quota.go`,
`pkg/drivers/pgsql/kubehz_quota.go` and `pkg/logstructured/sqllog/kubehz_relief.go`,
and `pkg/logstructured/kubehz_ttl.go`. It carries marker-wrapped edits in
`pkg/app/app.go` (the flag), `pkg/endpoint/endpoint.go` (the config field,
the wiring, the TTL delete timeout and the test seams), `pkg/server/kv.go`
(two log conditions), `pkg/logstructured/logstructured.go` (three
forwarding methods, the TTL delete timeout field and the TTL loop start),
`pkg/logstructured/sqllog/sql.go` (one forwarding method, and the
`polledRev` field with its store) and `pkg/drivers/pgsql/pgsql.go` (the
one-time `ANALYZE`, and the `quotaDialect` wrapper that `New` hands to
`sqllog.New`). `go.mod` differs from upstream too: `golang.org/x/time` is a
direct dependency (P2's rate limits). On `v0.17.5` upstream is above every
dependency floor the fork set, so `go.sum` is upstream's.

P3 (storage parameters on the kine table) adds
`pkg/drivers/pgsql/kubehz_reloptions.go` and touches `setup()` (one flag from
its CockroachDB probe) and `New()` in `pkg/drivers/pgsql/pgsql.go`. The
`CREATE TABLE` statement stays upstream's.

Beside those source files, the fork carries its own tests (every
`kubehz_*_test.go`. One runs against the PostgreSQL service `unit.yml`
provides), two workflow files (`unit.yml` gains a step per patch and that
service, `publish-kubehz.yml` is ours), a few `.gitignore` lines, and the
`kubehz/` and `benchmarks/` directories. Nothing else differs from
upstream. The check, from the repository root, is

```bash
git diff --stat upstream-master..main -- . ':!benchmarks' ':!kubehz' ':!.github' ':!.gitignore' ':!**/kubehz_*_test.go'
```

which must list exactly these files:

```
go.mod
pkg/app/app.go
pkg/drivers/pgsql/kubehz_quota.go
pkg/drivers/pgsql/kubehz_reloptions.go
pkg/drivers/pgsql/pgsql.go
pkg/endpoint/endpoint.go
pkg/logstructured/kubehz_ttl.go
pkg/logstructured/logstructured.go
pkg/logstructured/sqllog/kubehz_relief.go
pkg/logstructured/sqllog/sql.go
pkg/metrics/kubehz_quota.go
pkg/server/kubehz_quota.go
pkg/server/kv.go
```

## Principles

1. **Every patch carries a number.** If a change cannot be justified by a
   measurement in `benchmarks/results/`, it does not go in.
2. **Every patch is marked.** In-place edits are wrapped in `KUBEHZ-PATCH <id>`
   markers stating intent, invariants and what to do on conflict. Grep
   `KUBEHZ-PATCH` to find all of them.
3. **Prefer new files to edits.** A new file cannot conflict.
4. **Dropping a patch is a normal outcome.** None of these is required for
   correctness; each says so. A dropped patch recorded in the changelog is
   recoverable, a half-merged one is a silent bug.
5. **Configuration beats code.** The largest wins found were deployment
   settings, not patches — see the configuration section of the changelog. They
   are recorded here because they are easy to lose and expensive to rediscover.
