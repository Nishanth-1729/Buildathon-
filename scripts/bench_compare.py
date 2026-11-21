#!/usr/bin/env python3
"""Alternate two Shopping Sessions executables on a local Linux dataset."""
import argparse
import os
from pathlib import Path
import statistics
import subprocess
import tempfile
import time


def evict(paths):
    for path in paths:
        with path.open("rb") as input_file:
            os.posix_fadvise(input_file.fileno(), 0, 0, os.POSIX_FADV_DONTNEED)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("data", type=Path)
    parser.add_argument("baseline", type=Path)
    parser.add_argument("candidate", type=Path)
    parser.add_argument("--runs", type=int, default=9)
    parser.add_argument("--cpus", type=int, default=8)
    parser.add_argument("--cold", action="store_true", help="request input page-cache eviction before each run")
    args = parser.parse_args()
    available = sorted(os.sched_getaffinity(0))
    if not 1 <= args.cpus <= len(available) or args.runs < 1:
        parser.error("request between one and the available CPU count and at least one measured run")
    cpus = set(available[:args.cpus])
    os.sched_setaffinity(0, cpus)
    data = args.data.resolve(strict=True)
    paths = sorted(data.rglob("*.csv"))
    if not (data / "manifest.json").is_file() or not paths:
        parser.error("data must contain manifest.json and the input CSVs")
    bins = [args.baseline.resolve(strict=True), args.candidate.resolve(strict=True)]
    env = os.environ | {"NCPU": str(args.cpus)}
    samples = [[], []]
    print(f"CPUs {sorted(cpus)} | dataset {data} | eviction requested {args.cold}", flush=True)
    for pair in range(args.runs + 1):
        order = (0, 1) if pair % 2 == 0 else (1, 0)
        for version in order:
            if args.cold:
                evict(paths)
            with tempfile.TemporaryDirectory(prefix="shopping-bench-", dir=Path.cwd()) as out:
                start = time.perf_counter()
                result = subprocess.run(
                    [str(bins[version]), str(data), out], env=env, capture_output=True, text=True
                )
                elapsed = time.perf_counter() - start
                if result.returncode:
                    raise SystemExit(f"{bins[version]} exited {result.returncode}\n{result.stderr}")
                phases = ", ".join(line.strip() for line in result.stderr.splitlines())
                label = "baseline" if version == 0 else "candidate"
                print(f"{pair:2d} {label:9s} {elapsed:.4f} s | {phases}", flush=True)
                if pair != 0:
                    samples[version].append(elapsed)
    baseline, candidate = map(statistics.median, samples)
    paired = statistics.median(c / b - 1 for b, c in zip(*samples)) * 100
    print(f"baseline {baseline:.4f} s | candidate {candidate:.4f} s | median paired change {paired:+.1f}%")


if __name__ == "__main__":
    main()
