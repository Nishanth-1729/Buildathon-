# Shopping Sessions — Team Epsilon

A single Rust binary (no external crates) that implements [rules.md](rules.md): it cleans the event, click and link logs in `$DATA_DIR`, joins each person's IDs, builds sessions, credits and assists campaigns, and writes `$OUTPUT_DIR/user_daily/` and `$OUTPUT_DIR/campaign_daily/` as Parquet.

## How it works

Three parallel passes over cache-sized buckets, then the write. Timestamps are kept as `u32` milliseconds after `window_start - 7 days`.

| Pass | What happens | Code |
| --- | --- | --- |
| Parse | Every CSV is memory-mapped and cut into 4 MB chunks; a row belongs to the chunk holding its first byte. Each row is classified from the 128 bytes at its own start with AVX2 (commas, newline, quotes); long rows and file tails use the scalar splitter that defines the dialect. Event copies go to 512 buckets by `event_id` hash. The thread that parses the last link chunk resolves linked IDs while the others keep parsing. | [src/csv.rs](src/csv.rs), [src/main.rs](src/main.rs) |
| Values | F1/F2 timestamps are validated and split with one SSSE3 pass and dated from a month table (1900–2199; other years take the scalar path). F3 has a 13-digit SIMD path. | [src/ts.rs](src/ts.rs) |
| IDs | Short ASCII IDs pack into a `u64` whose integer order is the byte order; other IDs are interned. Nothing compares by hash alone. | [src/key.rs](src/key.rs) |
| Links | Union-find over IDs numbered in byte order, with the smaller number always the root, so a component's root is its name. Linked IDs go into an open-addressing table behind a bit filter. | [src/main.rs](src/main.rs) |
| Buckets | Records are appended to per-bucket lists of 32 KB chunks carved from per-thread 32 MB huge-page slabs ([src/chunks.rs](src/chunks.rs)): no regrowth, no copies, and about a third of the page faults of growing vectors. | [src/chunks.rs](src/chunks.rs) |
| Dedup + route | Per event bucket, a bitmap proves most `event_id`s unique; repeated bits (collisions included) go through an exact map that keeps the smallest `ingest_ts`. Valid winners and clicks are resolved to their person (prefetched lookups) and sent to 512 person buckets. | [src/main.rs](src/main.rs) |
| Sessions | Per person bucket: a dense local index and counting sort group events and clicks by person; each person's events are sorted by time, split on gaps over 30 minutes, credited (latest click within 7 days, smallest `click_id` on ties) and assisted (merged 7-day click intervals per campaign). | [src/main.rs](src/main.rs) |
| Write | A minimal Parquet writer: PLAIN, uncompressed, `day` as `DATE`. | [src/parquet.rs](src/parquet.rs) |

If a dataset is outside this layout's limits (a window too long for `u32` offsets, or more than 254 categories), the previous general pipeline in [src/legacy.rs](src/legacy.rs) runs instead.

## Results on the dev machine

8 threads pinned with `taskset -c 0-7` (4 cores with hyperthreading, i9-12900K, inputs in the page cache). Scorer times will differ.

| Dataset | Events | Wall time | Previous version |
| --- | --- | --- | --- |
| practice size, seed 21 | 20M | 0.52 s | 1.34 s |
| final size, seed 61 | 60M | 1.59 s | 4.1 s |

## Verification

- `cargo test --release`: dialect, chunk ownership, randomized scanner-vs-reference tests (block scanner and row scanner, 3 and 7 fields), timestamp forms and a randomized timestamp-vs-reference test, ID packing order, dedup collisions.
- All 36 fixtures pass, natively and in the container.
- The dev set (seed 1) matches `dev-answer` exactly.
- [oracle/oracle.py](oracle/oracle.py), an independent plain Python implementation, agrees exactly at 20M (seed 21). At 60M (seed 61) the output matches the previous pipeline exactly.

## Run it

```sh
. ./env.sh                  # toolchains live in .tools/ and .venv/ inside this folder
make test native-fixtures native-dev
make seeds EVENTS=1000000 SEEDS="2 3 4"
make bench DATA=data/p20m

# Container, scorer flags (rootless podman on an SELinux host shown):
make build fixtures DOCKER=podman DOCKER_OPTS="--security-opt label=disable"
```

Datasets come from the published generator, for example `generate --seed 21 --events 20000000 --clicks 5000000 --output data/p20m`.
