#!/bin/sh
set -eu

profile_dir=$(mktemp -d)
trap 'rm -rf "$profile_dir"' EXIT
host=$(rustc -vV | sed -n 's/^host: //p')
profdata="$(rustc --print sysroot)/lib/rustlib/$host/bin/llvm-profdata"

rustc --edition=2021 -O scripts/pgo_train.rs -o "$profile_dir/generate"
"$profile_dir/generate" "$profile_dir/input"

RUSTFLAGS="-C target-cpu=x86-64-v3 -C profile-generate=$profile_dir/raw" \
    CARGO_TARGET_DIR="$profile_dir/instrumented" cargo build --release --locked
NCPU=8 "$profile_dir/instrumented/release/shopping-sessions" "$profile_dir/input" "$profile_dir/output"
"$profdata" merge "$profile_dir/raw" -o "$profile_dir/shopping.profdata"

RUSTFLAGS="-C target-cpu=x86-64-v3 -C profile-use=$profile_dir/shopping.profdata" \
    cargo build --release --locked
