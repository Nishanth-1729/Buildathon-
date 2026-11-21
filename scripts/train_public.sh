#!/bin/sh
# Generate only public, reproducible inputs for LLVM profiling.
set -eu

training_dir=$(mktemp -d)
trap 'rm -rf "$training_dir"' EXIT
archive="$training_dir/generator.zip"
url=https://www.joinbuilders.org/buildathon/tracks/shopping-sessions/files/generator.zip
sha256=13724cadbac5a99d847e27cea911cd2e338ff972b46c2039da87452011bb101e

curl --fail --silent --show-error --location --connect-timeout 10 --max-time 60 --retry 2 "$url" -o "$archive"
printf '%s  %s\n' "$sha256" "$archive" | sha256sum -c -
unzip -q "$archive" -d "$training_dir"
(
    cd "$training_dir/generator"
    cargo build --release --locked
    ./target/release/generate --seed 1 --events 1000000 --clicks 250000 --threads 4 --output "$training_dir/input"
)
sh scripts/build_pgo.sh "$training_dir/input"
