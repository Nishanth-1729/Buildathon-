# Shopping Sessions: Rust pipeline (Team Epsilon).
# Profile on generated training data during the build. The runtime image
# only receives the executable.
FROM rust:1.85-slim-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY .cargo .cargo
COPY src src
COPY scripts/build_pgo.sh scripts/pgo_train.rs scripts/train_public.sh scripts/
ARG PUBLIC_PGO=1
RUN if rustup component add llvm-tools-preview; then \
        if [ "$PUBLIC_PGO" = 1 ] && apt-get update && \
                apt-get install -y --no-install-recommends ca-certificates curl unzip && \
                sh scripts/train_public.sh; then \
            :; \
        else \
            echo "Using the bundled compiler training workload" >&2; \
            sh scripts/build_pgo.sh || cargo build --release --locked; \
        fi; \
    else \
        echo "Profiling tools unavailable, building the standard release" >&2; \
        cargo build --release --locked; \
    fi

# Runtime: the scorer runs uid 1000 with a read-only root; the binary writes
# only to $OUTPUT_DIR and needs no temp files.
FROM debian:bookworm-slim
RUN useradd --uid 1000 --no-create-home --home-dir /scratch app
COPY --from=build /src/target/release/shopping-sessions /app/shopping-sessions
USER 1000:1000
CMD ["/app/shopping-sessions"]
