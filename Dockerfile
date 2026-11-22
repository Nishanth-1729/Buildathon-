# Shopping Sessions: Rust pipeline (Team Epsilon).
# Build stage: no crates to download, so the build is offline and quick.
FROM rust:1.85-slim-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY .cargo .cargo
COPY src src
RUN cargo build --release --locked

# Runtime: the scorer runs uid 1000 with a read-only root; the binary writes
# only to $OUTPUT_DIR and needs no temp files.
FROM debian:bookworm-slim
RUN useradd --uid 1000 --no-create-home --home-dir /scratch app
COPY --from=build /src/target/release/shopping-sessions /app/shopping-sessions
USER 1000:1000
CMD ["/app/shopping-sessions"]
