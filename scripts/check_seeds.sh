#!/bin/sh
# Cross-checks the Rust pipeline against the Python oracle on generated seeds.
# Usage: scripts/check_seeds.sh EVENTS SEED...   (needs `. ./env.sh` first)
set -eu
events="$1"; shift
clicks=$((events / 4))
cargo build --release --quiet
for seed in "$@"; do
  dir="data/seed-$seed-$events"
  [ -d "$dir" ] || generate --seed "$seed" --events "$events" --clicks "$clicks" --output "$dir" >/dev/null
  rm -rf "out/seed-$seed" "out/oracle-$seed"
  target/release/shopping-sessions "$dir" "out/seed-$seed" 2>/dev/null
  python -I oracle/oracle.py "$dir" "out/oracle-$seed"
  echo "seed $seed ($events events):"
  python scripts/compare.py "out/seed-$seed" "out/oracle-$seed"
done
