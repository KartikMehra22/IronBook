# syntax=docker/dockerfile:1.6

# ────── Stage 1: build the build-runner Go binary ──────────────────────
FROM golang:1.23-alpine AS gobuild
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=arm64 \
    go build -trimpath -ldflags="-s -w" -o /out/build-runner ./apps/build-runner

# ────── Stage 2: build environment (Rust + Go + CMake toolchains) ──────
# Day-5 scope: Rust toolchain. Phase 4 extends this image with go-toolchain
# and cmake + g++ behind a feature flag.
FROM rust:1.75-bookworm

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      golang-go \
      cmake \
      g++ \
      build-essential \
      ca-certificates \
 && rm -rf /var/lib/apt/lists/*

COPY --from=gobuild /out/build-runner /usr/local/bin/build-runner

WORKDIR /work
ENTRYPOINT ["/usr/local/bin/build-runner"]
