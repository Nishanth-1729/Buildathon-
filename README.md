# Shopping Sessions — Team Epsilon

A single Rust binary (no external crates) that implements [rules.md](rules.md): it cleans the event, click and link logs in `$DATA_DIR`, joins each person's IDs, builds sessions, credits and assists campaigns, and writes `$OUTPUT_DIR/user_daily/` and `$OUTPUT_DIR/campaign_daily/` as Parquet.

## How it works

Three parallel passes over cache-sized buckets, then the write. Timestamps are kept as `u32` milliseconds after `window_start - 7 days`.

| Pass | What happens | Code |
| --- | --- | --- |
| Parse | AVX2 classifies the first 96 bytes of an event row or 64 bytes of a click or link row. Long rows and file tails use the scalar splitter. Event copies go to 512 buckets by `event_id` hash. Input mappings are released before routing. | [src/csv.rs](src/csv.rs), [src/main.rs](src/main.rs) |
| Values | F1/F2 timestamps are validated and split with one SSSE3 pass and dated from a month table (1900–2199; other years take the scalar path). F3 has a 13-digit SIMD path. | [src/ts.rs](src/ts.rs) |
| IDs | Short ASCII IDs pack into a `u64` whose integer order is the byte order; other IDs are interned. Nothing compares by hash alone. | [src/key.rs](src/key.rs) |
| Links | Union-find over IDs numbered in byte order, with the smaller number always the root, so a component's root is its name. Linked IDs go into an open-addressing table behind a bit filter. | [src/main.rs](src/main.rs) |
| Buckets | Records are appended to per-bucket lists of 32 KB chunks carved from per-thread 32 MB huge-page slabs ([src/chunks.rs](src/chunks.rs)): no regrowth, no copies, and about a third of the page faults of growing vectors. | [src/chunks.rs](src/chunks.rs) |
| Dedup + route | A two-bit counter per hash position tracks whether it was hit once or more than once. Repeated positions use an exact map that keeps the smallest `ingest_ts` for each ID. Valid winners and clicks are routed to their person's bucket. | [src/main.rs](src/main.rs) |
| Sessions | A dense index and counting sort group events and clicks by person. Large histories use 30-minute bins and their exact first and last event times, which gives the same sessions as sorting all timestamps. Shorter histories use a timestamp sort. | [src/main.rs](src/main.rs) |
| Write | The Parquet writer streams PLAIN pages directly to disk, using 32-bit user-day count columns and a Parquet `DATE` for `day`. | [src/parquet.rs](src/parquet.rs) |

If a dataset is outside this layout's limits (a window too long for `u32` offsets, or more than 254 categories), the previous general pipeline in [src/legacy.rs](src/legacy.rs) runs instead.

## Local comparison

Five measured runs after warmup used the public 20M dataset with seed 21, Rust 1.85.0, and eight pinned CPUs on an AMD EPYC 7763.

| Build | Median wall time | Median peak RSS |
| --- | ---: | ---: |
| `e3ef48c` | 1.121 s | 3,433 MiB |
| Optimized with PGO | 0.892 s | 2,708 MiB |

The Docker build trains profile-guided optimization (PGO) on generated synthetic events. It copies only the final binary to the runtime image and falls back to the standard release build if profiling is unavailable. The same build can be run locally with `rustup component add llvm-tools-preview` followed by `sh scripts/build_pgo.sh`.

## Earlier results on the dev machine

8 threads pinned with `taskset -c 0-7` (4 cores with hyperthreading, i9-12900K, inputs in the page cache). Scorer times will differ.

| Dataset | Events | Wall time | Previous version |
| --- | --- | --- | --- |
| practice size, seed 21 | 20M | 0.52 s | 1.34 s |
| final size, seed 61 | 60M | 1.59 s | 4.1 s |

## Verification

- `cargo test --release`: dialect, chunk ownership, randomized scanner-vs-reference tests (block scanner and row scanner, 3 and 7 fields), timestamp forms and a randomized timestamp-vs-reference test, ID packing order, dedup collisions.
- All 36 fixtures pass with the profiled native build. Both tables match `e3ef48c` at 20M and 60M events.
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

## Parser optimization benchmark

The parser checks CSV quote positions in parallel with BMI2 bit extraction.
It caches exact calendar-day prefixes near the manifest window and decodes
common UTC clock fields with validated lookup tables. Other date formats and
uncached days use the general timestamp parser. The pipeline uses 1,024
buckets to reduce the working set of each deduplication and session task.

The table compares two profile-guided builds using the same build script.
Both ran on eight pinned vCPUs on an AMD EPYC 7763 VM. Each row reports the
median of six timed runs at 20M or four at 60M after one warmup per build,
with alternating run order. These are local process times.

| Generated input | Base 6474495 | Parser optimization |
| --- | ---: | ---: |
| 20M, seed 21 | 0.697 s | 0.667 s |
| 20M, seed 43 | 0.794 s | 0.757 s |
| 20M, seed 97 | 0.777 s | 0.677 s |
| 60M, seed 61 | 3.103 s | 2.800 s |

The profiled binary passes all 36 contest fixtures. Both tables match
the official dev answer, the Python oracle on a cross-chunk duplicate
case, and the base commit on the four generated datasets above.
