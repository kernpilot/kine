#!/usr/bin/env bash
# Builds the two binaries every comparison needs:
#
#   kine-patched   this working tree, with the fork's patches
#   kine-upstream  the pristine upstream anchor
#
# Both are built from THIS repository, so a comparison can never accidentally
# measure two different kine versions against each other — which is the whole
# reason the anchor is a branch here rather than a separately cloned upstream.
#
# The anchor ref is RESOLVED rather than assumed. The first version of this
# script hardcoded `upstream-master`, which does not exist locally in a fresh
# clone — it lives on the remote — so the script failed on its first real use
# with "fatal: invalid reference". Anything that only ever ran in the author's
# working copy has that failure mode waiting.
set -euo pipefail
cd "$(dirname "$0")/.."

say() { printf '» %s\n' "$*"; }
die() { printf '\033[31m✗\033[0m %s\n' "$*" >&2; exit 1; }

go build -buildvcs=false -o kine-patched . \
  && say "built kine-patched ($(git rev-parse --short HEAD))"

# Resolve the upstream anchor: a local branch, or any remote that carries it.
ANCHOR=""
for ref in upstream-master origin/upstream-master kernpilot/upstream-master \
           origin/master upstream/master; do
  if git rev-parse --verify --quiet "$ref^{commit}" >/dev/null; then
    ANCHOR="$ref"
    break
  fi
done
[[ -n "$ANCHOR" ]] || die "no upstream anchor found. Fetch one:
    git remote add upstream https://github.com/k3s-io/kine
    git fetch upstream master
  then re-run."

say "upstream anchor: $ANCHOR ($(git rev-parse --short "$ANCHOR"))"

WT=$(mktemp -d)
cleanup() { git worktree remove --force "$WT" >/dev/null 2>&1 || true; rm -rf "$WT"; }
trap cleanup EXIT

git worktree add -q --detach "$WT" "$ANCHOR" || die "could not create a worktree at $ANCHOR"
( cd "$WT" && go build -buildvcs=false -o "$OLDPWD/kine-upstream" . ) \
  || die "upstream build failed — the anchor may not compile with this toolchain"
say "built kine-upstream ($ANCHOR)"

# Prove they actually differ. Identical binaries mean the anchor resolved to the
# patched tree, and every subsequent comparison would silently measure kine
# against itself and report every patch as worthless.
if cmp -s kine-patched kine-upstream; then
  die "kine-patched and kine-upstream are byte-identical — the anchor is wrong.
  A comparison against this pair would report every patch as having no effect."
fi
say "binaries differ, as they must — ready for ./verify-patches.sh"
