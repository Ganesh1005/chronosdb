# Build stage.
FROM rust:1.99-slim AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock* ./
COPY crates ./crates
RUN cargo build --release -p chronos-net

# Runtime stage.
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists
COPY --from=build /src/target/release/chronos /usr/local/bin/chronos
VOLUME /data
ENTRYPOINT ["chronos"]
