# Contributing to upstream kine (k3s-io/kine)

Read this before you open an issue or a PR on `k3s-io/kine`. It was checked
on 2026-11-08. The rules can change, so read the sources again before each
contribution.

## Where the rules are

kine has no CONTRIBUTING, code of conduct or issue template of its own. The
k3s-io org keeps them in k3s, and they apply to kine too:

- `k3s-io/k3s` `CONTRIBUTING.md`, with the section "AI Guidance".
- The CNCF Community Code of Conduct (k3s `CODE_OF_CONDUCT.md`).
- `k3s-io/k3s` `.github/ISSUE_TEMPLATE/bug_report.md` and
  `.github/PULL_REQUEST_TEMPLATE.md`.
- Security reports: email `security@k3s.io` (k3s `.github/SECURITY.md`).
  Private vulnerability reporting is off on kine and k3s. Never put a
  vulnerability in a public issue.

## Issues

- One bug per issue.
- Use the fields of the k3s bug report template only: Environmental Info,
  Describe the bug, Steps To Reproduce, Expected behavior, Actual behavior,
  Additional context / logs.
- Keep it short. Maintainers ignore or reject issues with long AI prose,
  "Why it matters" sections, or long, made-up steps to reproduce.
- Link code at a commit SHA, not at a branch.
- Say what is verified: read in the source, or reproduced at runtime.

## AI use

- Disclose AI help in one line at the top, with the tools and models. k3s
  requires this in a PR description. Do it in issues too.
- Upstream commits carry no AI co-author: no `Co-Authored-By`,
  `assisted-by` or `co-developed` trailer for an AI tool. This is the
  opposite of the rule for our own repositories.
- A human answers review comments and follow-up questions, without AI tools.
  If nobody can explain a change, the maintainers close the PR.

## Pull requests

- One issue per PR. Open the issue first for a large or complex change.
- Normally one commit, with a short title and an empty body.
- Every commit needs a DCO sign-off (`git commit -s`) from the human author
  (k3s `.github/dco.yml` requires it from everyone).
- No closing keywords (`Fixes #n`): their testers close the issues.
- Fill in the PR template: proposed changes, type, verification, testing,
  linked issues, the release-note block.
- Review and test the change yourself before you open the PR.
- No tests that are trivial, over-scaffolded or very large.
- Build the branch from upstream `master`, not from our `main`: our commits
  carry the `kubehz/` files and our trailers.

## What we reported

| Date | Upstream | What | Our side |
|---|---|---|---|
| 2026-11-08 | #824 | `Tx.Insert` inserts through the pool, not the transaction (two connections per write) | found in the v0.17.5 sync review |
| 2026-11-08 | #825 | an update txn at a deleted key's delete revision writes the key again | the same review (it also bypassed P2) |
