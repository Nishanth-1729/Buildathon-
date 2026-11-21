IMAGE ?= shopping-sessions:latest
CPUS  ?= 8
MEM   ?= 24g
# The scorer's platform. PLATFORM=linux/arm64 builds and runs natively on an Apple-silicon Mac, for fast local loops.
PLATFORM ?= linux/amd64
DEV   ?= data/dev
# docker (default) or podman. Rootless podman on SELinux hosts also wants DOCKER_OPTS="--security-opt label=disable".
DOCKER ?= docker
DOCKER_OPTS ?=
FIXTURES ?= fixtures
BIN := target/release/shopping-sessions
export IMAGE CPUS MEM PLATFORM DOCKER DOCKER_OPTS

.PHONY: build fixtures dev run-dev check-dev native native-fixtures native-dev seeds bench test

## Build the image for the scorer's platform, linux/amd64
build:
ifeq ($(DOCKER),podman)
	podman build $(DOCKER_OPTS) --platform $(PLATFORM) -t $(IMAGE) .
else
	docker buildx build --platform $(PLATFORM) --load -t $(IMAGE) .
endif

## Run the image on all 36 edge-case fixtures and diff against the expected tables
fixtures: build
	python3 scripts/run_fixtures.py $(FIXTURES) -- scripts/docker_run.sh

## Make the 1M-event dev set with the published generator (needs `generate` on PATH)
dev:
	generate --seed 1 --events 1000000 --clicks 250000 --output $(DEV)

## Run on the dev set with scorer flags and time it
run-dev: build
	rm -rf out && time scripts/docker_run.sh $(DEV) out

## Compare your dev output with the published dev answer: make check-dev ANSWER=dev-answer
ANSWER ?= dev-answer
check-dev:
	python3 scripts/compare.py out $(ANSWER)

## Without containers: build the binary, unit tests, fixtures, dev set
native:
	cargo build --release

test:
	cargo test --release

native-fixtures: native
	python3 scripts/run_fixtures.py $(FIXTURES) -- $(BIN)

native-dev: native
	rm -rf out/dev && $(BIN) $(DEV) out/dev && python3 scripts/compare.py out/dev $(ANSWER)

## Cross-check against the Python oracle: make seeds EVENTS=1000000 SEEDS="2 3 4"
EVENTS ?= 1000000
SEEDS ?= 2 3 4
seeds:
	scripts/check_seeds.sh $(EVENTS) $(SEEDS)

## Time the binary pinned to 8 CPUs on a dataset: make bench DATA=data/p20m
DATA ?= data/p20m
bench: native
	rm -rf out/bench && /usr/bin/time -f "wall %e s, max RSS %M KB" taskset -c 0-7 env NCPU=8 $(BIN) $(DATA) out/bench
