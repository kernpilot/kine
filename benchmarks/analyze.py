#!/usr/bin/env python3
"""Summarise sweep results against a measured noise floor.

WHY THIS EXISTS AS A SCRIPT. Reading a table of throughput numbers by eye
reliably produces findings that are not there. Every config in this sweep is
compared against the baseline's own run-to-run spread, and anything inside that
spread is printed as a null result rather than as a small win. The sweep
re-measures the baseline several times THROUGHOUT its run, so the noise floor
reported here includes machine drift over the sweep's duration and not just the
variation between two adjacent runs.

Usage: ./analyze.py [group ...]      (default: every group found)
"""
import json
import pathlib
import statistics
import sys

RESULTS = pathlib.Path(__file__).parent / "results"


def load(pattern):
    """Load every result JSON whose label starts with `pattern`."""
    out = []
    for f in sorted(RESULTS.glob(f"{pattern}*.json")):
        try:
            out.append(json.loads(f.read_text()))
        except (json.JSONDecodeError, OSError):
            continue
    return out


def stat(runs, key):
    vals = [r[key] for r in runs if key in r and r[key] is not None]
    if not vals:
        return None, None
    return statistics.mean(vals), (max(vals) - min(vals))


def spread_pct(runs, key):
    vals = [r[key] for r in runs if key in r]
    if len(vals) < 2 or not min(vals):
        return 0.0
    return 100.0 * (max(vals) - min(vals)) / min(vals)


def main():
    # Results are reported per ARM. A tmpfs run and a disk run are not
    # comparable — kine measured 11,043 writes/s on tmpfs against 3,100-5,100 on
    # the encrypted-btrfs volume — so pooling them would invent a noise floor
    # that describes neither.
    arm = "mem"
    if len(sys.argv) > 1 and sys.argv[1] in ("mem", "disk"):
        arm = sys.argv.pop(1)
    prefix = "v2mem-" if arm == "mem" else "v2dsk-"
    label = "tmpfs (logic arm)" if arm == "mem" else "btrfs+dm-crypt (durability arm)"

    # The baseline is measured in several passes across the sweep; pooling them
    # gives a noise floor that includes drift, which a single pair cannot.
    base = []
    for p in (prefix + "base-a", prefix + "base-b", prefix + "base-c"):
        base.extend(load(p))
    if not base:
        print(f"no {arm} baseline runs found yet")
        return 1
    print(f"ARM: {label}")

    base_shape = (base[0].get("writers"), base[0].get("watchers"))
    base_rate, _ = stat(base, "put_rate_per_sec")
    noise = spread_pct(base, "put_rate_per_sec")
    print(f"baseline: {len(base)} runs, mean {base_rate:,.0f} writes/s, "
          f"run-to-run spread {noise:.1f}%  <- noise floor")
    print(f"{'config':<26}{'runs':>5}{'writes/s':>11}{'vs base':>10}"
          f"{'p99 ms':>9}{'err%':>8}   verdict")
    print("-" * 88)

    groups = sys.argv[1:] or sorted({
        f.stem.rsplit("-r", 1)[0]
        for f in RESULTS.glob(f"{prefix}*.json")
    })

    for g in groups:
        runs = load(g)
        if not runs:
            continue
        rate, _ = stat(runs, "put_rate_per_sec")
        p99, _ = stat(runs, "put_p99_ms")
        err, _ = stat(runs, "put_error_rate_pct")
        # A config run at a different writer or watcher count is NOT comparable
        # to the baseline on absolute throughput, and printing a percentage
        # against it invites exactly the wrong reading — the watcher-scaling
        # series runs 50 writers against a 100-writer baseline, which would
        # otherwise show up as a "+47% win". Such rows are marked and their
        # delta suppressed; they are only meaningful within their own series.
        shape = (runs[0].get("writers"), runs[0].get("watchers"))
        if shape != base_shape:
            print(f"{g:<26}{len(runs):>5}{rate:>11,.0f}{'  n/a':>10}"
                  f"{p99:>9.1f}{err:>8.2f}   different shape "
                  f"({shape[0]}w/{shape[1]}watch) — compare within its own series")
            continue
        delta = 100.0 * (rate - base_rate) / base_rate if base_rate else 0.0
        # A change smaller than the baseline's own spread is not a measurement
        # of anything. Say so plainly rather than reporting a signed number that
        # invites over-reading.
        if abs(delta) <= noise:
            verdict = f"no effect (within {noise:.1f}% noise)"
        elif delta > 0:
            verdict = f"FASTER {delta:+.1f}%"
        else:
            verdict = f"SLOWER {delta:+.1f}%"
        print(f"{g:<26}{len(runs):>5}{rate:>11,.0f}{delta:>9.1f}%"
              f"{p99:>9.1f}{err:>8.2f}   {verdict}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
