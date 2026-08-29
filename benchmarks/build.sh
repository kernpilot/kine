#!/usr/bin/env bash
# Builds the two binaries every comparison needs:
#   kine-upstream  the pristine upstream anchor (branch upstream-master)
#   kine-patched   this working tree, with the fork's patches
# Both are built from THIS repository so a comparison can never accidentally
# measure two different kine versions against each other.
set -euo pipefail
cd "$(dirname "$0")/.."
go build -buildvcs=false -o kine-patched . && echo "built kine-patched ($(git rev-parse --short HEAD))"
WT=$(mktemp -d)
git worktree add -q --detach "$WT" upstream-master
( cd "$WT" && go build -buildvcs=false -o - . > /dev/null 2>&1 || true
  go build -buildvcs=false -o "$OLDPWD/kine-upstream" . ) && echo "built kine-upstream (upstream-master)"
git worktree remove --force "$WT" 2>/dev/null || true
