# Shopping Sessions — Team Epsilon

A single Rust binary (no external crates) that implements [rules.md](rules.md): it cleans the event, click and link logs in `$DATA_DIR`, joins each person's IDs, builds sessions, credits and assists campaigns, and writes `$OUTPUT_DIR/user_daily/` and `$OUTPUT_DIR/campaign_daily/` as Parquet.

## How it works

| Step | What happens | Code |
| --- | --- | --- |
| Read | Every CSV is memory-mapped and cut into 4 MB chunks; a line belongs to the chunk holding its first byte, and each file's first line is its header. | [src/csv.rs](src/csv.rs) |
| Split | An AVX2 scanner finds `,` `\n` `"` 64 bytes at a time and applies the dialect exactly: whole-field quotes only, `""` is null, `\r` is data, wrong field counts are dropped. A randomized test checks it against a plain scalar splitter. | [src/csv.rs](src/csv.rs) |
| Values | F1/F2/F3 timestamps with calendar and offset checks, ASCII digits only, i64 overflow treated as out of window. | [src/ts.rs](src/ts.rs) |
| IDs | IDs up to 15 bytes are packed into a u128 whose integer order is the byte order; longer IDs are interned. Nothing compares by hash alone. | [src/key.rs](src/key.rs) |
| Links | Parsed and resolved with union-find on their own thread while events are parsed; each person is named by their byte-wise smallest ID. | [src/main.rs](src/main.rs) |
| Dedup | Event copies are bucketed by `event_id` hash; per bucket, the copy with the smallest `ingest_ts` is kept and only then validated. | [src/main.rs](src/main.rs) |
| Sessions | Survivors and valid clicks are bucketed by person, sorted by time, split on gaps over 30 minutes, credited by binary search, and assisted by merging each campaign's 7-day click intervals. | [src/main.rs](src/main.rs) |
| Write | A minimal Parquet writer: PLAIN, uncompressed, `day` as `DATE`. | [src/parquet.rs](src/parquet.rs) |

## Results on the dev machine

8 threads pinned with `taskset` (i9-12900K, inputs in the page cache). Scorer times will differ.

| Dataset | Events | Wall time | Peak RSS (includes mapped inputs) |
| --- | --- | --- | --- |
| dev, seed 1 | 1M | 0.07 s | — |
| practice size, seed 21 | 20M | 1.33 s | 4.9 GB |
| final size, seed 61 | 60M | 4.1 s | 13.2 GB |

## Verification

- `cargo test --release`: timestamp, dialect and chunk-ownership unit tests, plus the randomized scanner test.
- All 36 fixtures pass, natively and in the container.
- The dev set (seed 1) matches `dev-answer` exactly.
- [oracle/oracle.py](oracle/oracle.py) is an independent, deliberately plain Python implementation of the rules. It also passes all 36 fixtures and the dev answer, and it agrees with the Rust output exactly on seeds 2, 3, 4 and 7 (1M events), seed 11 (3M) and seed 21 (20M).

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
