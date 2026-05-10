SHELL := /usr/bin/env bash
.SHELLFLAGS := -eu -o pipefail -c
export PATH := $(HOME)/.cargo/bin:$(PATH)
.DEFAULT_GOAL := help
.PHONY: help proto build lint fmt test-unit test-integ ci-local dev dev-up dev-down demo images dev-secrets

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

dev-up: ## Bring up local kind cluster
	./tools/kindcluster/up.sh

dev-down: ## Tear down local kind cluster
	./tools/kindcluster/down.sh

dev-secrets: ## Apply dev-only Postgres + MinIO secrets (devpassword123)
	kubectl --kubeconfig $$PWD/kubeconfig.local create ns ironbook --dry-run=client -o yaml | kubectl --kubeconfig $$PWD/kubeconfig.local apply -f -
	kubectl --kubeconfig $$PWD/kubeconfig.local -n ironbook create secret generic postgres --from-literal=user=ironbook --from-literal=password=devpassword123 --dry-run=client -o yaml | kubectl --kubeconfig $$PWD/kubeconfig.local apply -f -
	kubectl --kubeconfig $$PWD/kubeconfig.local -n ironbook create secret generic minio    --from-literal=user=ironbook --from-literal=password=devpassword123 --dry-run=client -o yaml | kubectl --kubeconfig $$PWD/kubeconfig.local apply -f -
	kubectl --kubeconfig $$PWD/kubeconfig.local create ns builds --dry-run=client -o yaml | kubectl --kubeconfig $$PWD/kubeconfig.local apply -f -
	kubectl --kubeconfig $$PWD/kubeconfig.local -n builds   create secret generic postgres --from-literal=user=ironbook --from-literal=password=devpassword123 --dry-run=client -o yaml | kubectl --kubeconfig $$PWD/kubeconfig.local apply -f -
	kubectl --kubeconfig $$PWD/kubeconfig.local -n builds   create secret generic minio    --from-literal=user=ironbook --from-literal=password=devpassword123 --dry-run=client -o yaml | kubectl --kubeconfig $$PWD/kubeconfig.local apply -f -

images: ## Build + load all IronBook container images into the kind cluster
	docker build --platform linux/arm64 -t ironbook/submission-api:dev    -f apps/submission-api/Dockerfile    .
	docker build --platform linux/arm64 -t ironbook/build-runner:dev      -f apps/build-runner/Dockerfile      .
	docker build --platform linux/arm64 -t ironbook/admission-webhook:dev -f apps/admission-webhook/Dockerfile .
	docker build --platform linux/arm64 -t ironbook/fairness-gateway:dev  -f apps/fairness-gateway/Dockerfile  .
	kind load docker-image ironbook/submission-api:dev ironbook/build-runner:dev \
	                       ironbook/admission-webhook:dev ironbook/fairness-gateway:dev \
	                       --name ironbook-control

dev: dev-up ## Bring up local cluster + apply dev overlay
	kubectl --kubeconfig $$PWD/kubeconfig.local create ns cert-manager      --dry-run=client -o yaml | kubectl --kubeconfig $$PWD/kubeconfig.local apply -f -
	kubectl --kubeconfig $$PWD/kubeconfig.local create ns argocd            --dry-run=client -o yaml | kubectl --kubeconfig $$PWD/kubeconfig.local apply -f -
	kubectl --kubeconfig $$PWD/kubeconfig.local create ns gatekeeper-system --dry-run=client -o yaml | kubectl --kubeconfig $$PWD/kubeconfig.local apply -f -
	# `--load-restrictor LoadRestrictionsNone` lets sandbox-host's ConfigMapGenerator
	# read profiles from deploy/policies/ (above the base kustomization dir).
	kustomize build --load-restrictor LoadRestrictionsNone deploy/manifests/overlays/dev \
	  | kubectl --kubeconfig $$PWD/kubeconfig.local apply --server-side -f -
	$(MAKE) dev-secrets
	kubectl --kubeconfig $$PWD/kubeconfig.local rollout status -n cert-manager deploy/cert-manager-webhook --timeout=300s

demo: dev images ## End-to-end demo: spin up cluster, build images, seed a Rust hello-world, see status=READY
	kubectl --kubeconfig $$PWD/kubeconfig.local rollout status -n ironbook    deploy/submission-api    --timeout=180s
	kubectl --kubeconfig $$PWD/kubeconfig.local rollout status -n ironbook    deploy/admission-webhook --timeout=180s
	kubectl --kubeconfig $$PWD/kubeconfig.local rollout status -n submissions deploy/fairness-gateway  --timeout=180s
	@echo "[demo] tarballing the Rust hello-world fixture..."
	cd tests/e2e/fixtures/submissions/correct-rust-hello && tar -czf /tmp/correct-rust-hello.tar.gz .
	@echo "[demo] uploading via submission-api..."
	@kubectl --kubeconfig $$PWD/kubeconfig.local -n ironbook port-forward svc/submission-api 8080:8080 > /tmp/pf.log 2>&1 & \
	PF=$$!; sleep 3; \
	curl -sS -X POST --data-binary @/tmp/correct-rust-hello.tar.gz 'http://localhost:8080/v1/upload?language=rust'; \
	echo; kill $$PF 2>/dev/null; wait 2>/dev/null
	@echo "[demo] watching the build Job..."
	@sleep 5
	kubectl --kubeconfig $$PWD/kubeconfig.local -n builds get jobs,pods
	@echo "[demo] cluster up. Try the gateway at:"
	@echo "  kubectl --kubeconfig=kubeconfig.local -n submissions port-forward svc/fairness-gateway 8081:8080"

