# EXPERIMENT (not for the final): same code as e5aa405, plain release build, no PGO.
FROM rust:1.85-slim-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY .cargo .cargo
COPY src src
RUN cargo build --release --locked

FROM debian:bookworm-slim
RUN useradd --uid 1000 --no-create-home --home-dir /scratch app
COPY --from=build /src/target/release/shopping-sessions /app/shopping-sessions
USER 1000:1000
CMD ["/app/shopping-sessions"]
