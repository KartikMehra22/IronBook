# syntax=docker/dockerfile:1.6
FROM rust:1.95-slim AS build
WORKDIR /src

RUN apt-get update && apt-get install -y --no-install-recommends \
    protobuf-compiler ca-certificates && rm -rf /var/lib/apt/lists/*

# Copy whole workspace; cargo resolves the path-dependencies in one shot.
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY proto ./proto

RUN cargo build --release -p reference-oracle

FROM gcr.io/distroless/cc-debian12:nonroot
COPY --from=build /src/target/release/reference-oracle /usr/local/bin/reference-oracle
USER nonroot
EXPOSE 7080
ENTRYPOINT ["/usr/local/bin/reference-oracle"]
