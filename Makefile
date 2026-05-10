SHELL := /usr/bin/env bash
.SHELLFLAGS := -eu -o pipefail -c
export PATH := $(HOME)/.cargo/bin:$(PATH)
.DEFAULT_GOAL := help
.PHONY: help proto build lint fmt test-unit test-integ ci-local

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS=":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

proto: ## Generate Go + Rust protobuf bindings
	buf generate

build: ## Build all Go binaries and Rust crates (debug)
	go build ./...
	cargo build --workspace

lint: ## Lint Go and Rust
	cargo clippy --workspace --all-targets -- -D warnings
	@command -v golangci-lint > /dev/null || { echo "install golangci-lint"; exit 1; }
	golangci-lint run

fmt: ## Format code
	cargo fmt --all
	gofmt -w .

test-unit: ## Run unit tests
	cargo nextest run --workspace || cargo test --workspace
	go test ./... -race -count=1 -timeout 60s

test-integ: ## Run integration tests (Testcontainers; requires Docker)
	go test -tags=integration ./... -count=1 -timeout 5m

ci-local: lint test-unit ## Mirror PR CI locally
