#!/bin/sh
set -eu

profile_dir=$(mktemp -d)
trap 'rm -rf "$profile_dir"' EXIT
host=$(rustc -vV | sed -n 's/^host: //p')
profdata="$(rustc --print sysroot)/lib/rustlib/$host/bin/llvm-profdata"

profile_runs=1
profile_flags=
if [ "$#" -eq 0 ]; then
    rustc --edition=2021 -O scripts/pgo_train.rs -o "$profile_dir/generate"
    train_data="$profile_dir/input"
    "$profile_dir/generate" "$train_data"
else
    train_data="$1"
    # Keep profile counts from concurrent workers and reduce dependence on
    # the scheduling of a single training run.
    profile_flags="-C llvm-args=-instrprof-atomic-counter-update-all"
    profile_runs=3
fi

RUSTFLAGS="-C target-cpu=x86-64-v3 -C profile-generate=$profile_dir/raw $profile_flags" \
    CARGO_TARGET_DIR="$profile_dir/instrumented" cargo build --release --locked
mkdir -p "$profile_dir/raw"
run=1
while [ "$run" -le "$profile_runs" ]; do
    LLVM_PROFILE_FILE="$profile_dir/raw/run-$run.profraw" NCPU=8 \
        "$profile_dir/instrumented/release/shopping-sessions" "$train_data" "$profile_dir/output-$run"
    run=$((run + 1))
done
"$profdata" merge "$profile_dir/raw" -o "$profile_dir/shopping.profdata"

RUSTFLAGS="-C target-cpu=x86-64-v3 -C profile-use=$profile_dir/shopping.profdata" \
    cargo build --release --locked
