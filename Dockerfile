# Shopping Sessions: Rust pipeline (Team Epsilon).
# Profile on generated training data during the build. The runtime image
# only receives the executable.
FROM rust:1.85-slim-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY .cargo .cargo
COPY src src
COPY scripts/build_pgo.sh scripts/pgo_train.rs scripts/
RUN if rustup component add llvm-tools-preview && sh scripts/build_pgo.sh; then \
        :; \
    else \
        echo "Profiling unavailable, building the standard release" >&2; \
        cargo build --release --locked; \
    fi

# Runtime: the scorer runs uid 1000 with a read-only root; the binary writes
# only to $OUTPUT_DIR and needs no temp files.
FROM debian:bookworm-slim
RUN useradd --uid 1000 --no-create-home --home-dir /scratch app
COPY --from=build /src/target/release/shopping-sessions /app/shopping-sessions
USER 1000:1000
CMD ["/app/shopping-sessions"]
