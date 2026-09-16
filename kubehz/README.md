# kubehz fork of k3s-io/kine

A small set of benchmark-justified patches on top of upstream kine, plus the
benchmark suite that justifies them.

**Start here:**

| file | what |
|---|---|
| [MERGE-GUIDE.md](MERGE-GUIDE.md) | **read before resolving any conflict.** How to rebase onto new upstream, what each patch must preserve, and when to drop one. |
| [CHANGELOG.md](CHANGELOG.md) | every patch, the measurement behind it, and the command that re-checks it. Also what was measured and deliberately *not* patched. |
| [../benchmarks/](../benchmarks/) | the suite. `./setup.sh && ./build.sh && ./verify-patches.sh` |

## Why this fork exists

Upstream kine is actively maintained (12-38 commits/month), but its PostgreSQL
path moves slowly: issue #63 on connection pooling has been open since
2020-11-18, and #596, proposing schema and pool changes, since 2026-02-15.
Waiting for a merge is not a plan for PostgreSQL-specific work.

The fork is deliberately narrow. It carries two patches.

P1 touches three files. `pkg/drivers/pgsql/notify.go` is new and cannot
conflict. `pkg/drivers/pgsql/pgsql.go` and `pkg/logstructured/sqllog/sql.go`
carry small marker-wrapped edits. `sqllog/sql.go` is a file upstream churns
(~21 commits a year), so that edit is written to be dropped cheaply, and is
a candidate for upstreaming rather than indefinite carrying.

P2 (the `--quota-bytes` size limit) adds `pkg/server/kubehz_quota.go` and
`pkg/metrics/kubehz_quota.go`, and carries marker-wrapped edits in
`pkg/app/app.go` (the flag), `pkg/endpoint/endpoint.go` (the config field
and the wiring) and `pkg/server/kv.go` (two log conditions).

Beside those source files, the fork carries its own tests (every
`kubehz_*_test.go`), two workflow files (`unit.yml` gains a step per patch,
`publish-kubehz.yml` is ours), a few `.gitignore` lines, and the `kubehz/`
and `benchmarks/` directories. Nothing else differs from upstream; the
check is

```bash
git diff --stat upstream-master..main -- . ':!benchmarks' ':!kubehz' ':!.github' ':!.gitignore' ':!**/kubehz_*_test.go'
```

which must list exactly `notify.go`, `pgsql.go`, `sqllog/sql.go`,
`server/kubehz_quota.go`, `metrics/kubehz_quota.go`, `app/app.go`,
`endpoint/endpoint.go` and `server/kv.go`.

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
