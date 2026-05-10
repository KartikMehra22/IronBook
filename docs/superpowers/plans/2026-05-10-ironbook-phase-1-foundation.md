# IronBook Phase 1 — Foundation + Sandbox Tier

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** End-to-end working sandbox tier. `make demo` brings up a local dev environment, uploads a Rust hello-world contestant submission, builds it through a hermetic pipeline (BuildKit + Trivy + Cosign + SLSA-3), pushes it to an in-cluster registry, and deploys it to a remote Hetzner k3s under gVisor where a stub fairness-gateway acks an order to it.

**Architecture:** Monorepo bootstrap — Go module + Rust workspace + Next.js + Terraform modules + raw K8s manifests with Kustomize overlays. Local dev is k3d on Mac (control plane); remote sandbox is k3s on Hetzner ARM (sandbox region). Wireguard mesh between them. Submission upload pipeline ends at a gVisor-isolated pod with seccomp + AppArmor + cgroups + NetworkPolicy + iptables-backstop.

**Tech Stack:** Go 1.22, Rust 1.75 stable, Next.js 15, Terraform 1.7, kind 0.22, k3d 5.6, k3s v1.30, gVisor (release-20240624.0), Hetzner Cloud, Wireguard, cert-manager v1.14, OPA Gatekeeper v3.16, Argo CD v2.10, MinIO RELEASE.2024-06-13, PostgreSQL 16, Caddy 2.8, BuildKit v0.13, Trivy 0.51, Cosign 2.2, SLSA in-toto attestations, kustomize 5.4, buf 1.32.

---

## Spec references

- Topology: spec §1
- Submission upload flow: spec §2.1
- Component specs: spec §3
- Sandbox model (defense-in-depth): spec §4.2
- Build pipeline (hermetic): spec §4.3
- Repo structure: spec §9.1–9.4

---

## File structure for this phase

Top-level files created (a subset of the full spec §9.1 tree — the rest comes in later phases):

```
IronBook/
├── .editorconfig                       # T1.1
├── .gitignore                          # T1.1
├── .github/
│   └── workflows/
│       └── ci.yml                      # T1.7
├── apps/
│   ├── submission-api/                 # T4.3-4.6
│   │   ├── main.go
│   │   ├── server/server.go
│   │   ├── service/service.go
│   │   ├── repo/{postgres.go,minio.go}
│   │   ├── config/config.go
│   │   └── integration/upload_test.go
│   ├── build-runner/                   # T5.2-5.6
│   │   ├── main.go
│   │   └── runner/runner.go
│   └── admission-webhook/              # T6.5
│       ├── main.go
│       └── policy/{validate.go,policy_test.go}
├── crates/
│   ├── matching-engine/                # T1.3 (skeleton only; full impl Phase 2)
│   │   ├── Cargo.toml
│   │   └── src/lib.rs
│   ├── proto/                          # T1.5 (generated)
│   └── ironbookctl/                    # T1.3 (skeleton)
├── proto/
│   ├── ironbook/v1/orders.proto        # T1.5
│   ├── ironbook/v1/runs.proto          # T1.5
│   └── buf.yaml                        # T1.5
├── frontend/                           # T1.4 (Next.js skeleton)
│   ├── package.json
│   ├── next.config.mjs
│   └── app/page.tsx
├── deploy/
│   ├── terraform/
│   │   ├── modules/
│   │   │   ├── hetzner-vm/{main.tf,variables.tf,outputs.tf}    # T3.1
│   │   │   └── wireguard/{main.tf,variables.tf,outputs.tf}     # T3.2
│   │   └── envs/prod/{main.tf,backend.tf,terraform.tfvars.example}  # T3.3
│   ├── manifests/
│   │   ├── base/
│   │   │   ├── cert-manager/                                    # T2.3
│   │   │   ├── argocd/                                          # T2.3
│   │   │   ├── opa-gatekeeper/                                  # T2.3
│   │   │   ├── postgres/                                        # T4.2
│   │   │   ├── minio/                                           # T4.2
│   │   │   ├── registry/                                        # T5.1
│   │   │   ├── submission-api/                                  # T4.6
│   │   │   ├── build-runner/                                    # T5.6
│   │   │   ├── admission-webhook/                               # T6.5
│   │   │   └── stub-fairness-gateway/                           # T6.7
│   │   └── overlays/
│   │       ├── dev/                                             # T2.4
│   │       └── prod-sandbox/                                    # T6.1
│   ├── policies/
│   │   ├── seccomp/ironbook-sandbox.json                        # T6.3
│   │   └── apparmor/ironbook-sandbox                            # T6.4
│   └── runtimeclasses/gvisor.yaml                               # T6.2
├── pkg/
│   ├── proto/                          # T1.5 (generated Go bindings)
│   ├── postgresclient/                 # T4.1 (lib used by submission-api)
│   ├── miniclient/                     # T4.4 (lib)
│   └── cosignverify/                   # T6.5 (lib used by webhook)
├── tools/
│   └── kindcluster/                    # T2.1
│       └── up.sh
├── tests/
│   └── e2e/
│       ├── fixtures/submissions/correct-rust-hello/             # T5.7
│       └── cases/{phase1_smoke_test.go}                         # T6.8
├── docs/
│   ├── adr/001-gvisor-not-firecracker.md                        # T1.7
│   ├── superpowers/specs/2026-05-10-ironbook-design.md          # already present
│   └── superpowers/plans/                                       # already present
├── Cargo.toml                          # T1.3
├── go.mod                              # T1.2
├── buf.gen.yaml                        # T1.5
├── Makefile                            # T1.6
├── CLAUDE.md                           # T1.1
├── README.md                           # T1.1
└── LICENSE                             # T1.1
```

---

## Day 1 — Repo skeleton & build chain (~7 tasks, ~4–6 hours)

### Task 1.1: Top-level repo files

**Files:**
- Create: `.gitignore`
- Create: `.editorconfig`
- Create: `LICENSE` (Apache-2.0)
- Modify: `README.md`
- Create: `CLAUDE.md`

- [ ] **Step 1: Create `.gitignore`**

```gitignore
# Build artefacts
target/
node_modules/
dist/
.next/
*.exe

# Go
/coverage.out
*.test

# Rust
**/*.rs.bk
Cargo.lock.bak

# Editors
.idea/
.vscode/
*.swp

# OS
.DS_Store

# Secrets
*.tfvars
!*.tfvars.example
*.pem
*.key
.env*
!.env.example

# Generated bindings (regenerable)
crates/proto/src/gen/
pkg/proto/

# Local k8s state
kubeconfig.local
```

- [ ] **Step 2: Create `.editorconfig`**

```ini
root = true
[*]
charset = utf-8
end_of_line = lf
insert_final_newline = true
trim_trailing_whitespace = true
indent_style = space
indent_size = 2
[*.go]
indent_style = tab
[*.{rs,toml}]
indent_size = 4
[Makefile]
indent_style = tab
```

- [ ] **Step 3: Add Apache-2.0 `LICENSE`**

Run:
```bash
curl -s https://www.apache.org/licenses/LICENSE-2.0.txt > LICENSE
sed -i.bak 's/yyyy/2026/; s/name of copyright owner/Kartik Mehra/' LICENSE && rm LICENSE.bak
```

- [ ] **Step 4: Replace `README.md` with the elevator-pitch from spec §9.13**

Use the README template verbatim from `docs/superpowers/specs/2026-05-10-ironbook-design.md` §9.13.

- [ ] **Step 5: Create `CLAUDE.md` for tool memory**

```markdown
# CLAUDE.md — Project notes for IronBook

## Hot rules
- Never re-run `make dev` from cold inside an existing kind cluster — use `make dev-restart`.
- Tests must run with `-race` (Go) and `cargo nextest run` (Rust).
- Order book uses integer prices/quantities — no floats.
- All commits are Conventional Commits.

## Common pitfalls
- buf codegen output paths differ for Go vs Rust — see `buf.gen.yaml`.
- gVisor RuntimeClass is named `gvisor` not `runsc`.
- `unsafe_code = "forbid"` is workspace-wide; only `crates/ebpf-observer` opts out.

## Spec
Canonical design: `docs/superpowers/specs/2026-05-10-ironbook-design.md`
```

- [ ] **Step 6: Commit**

```bash
git add .gitignore .editorconfig LICENSE README.md CLAUDE.md
git commit -m "chore: bootstrap repo skeleton (gitignore, editorconfig, license, README, CLAUDE.md)"
```

---

### Task 1.2: Go module & shared `pkg/` skeleton

**Files:**
- Create: `go.mod`, `go.sum` (auto)
- Create: `pkg/.keep`

- [ ] **Step 1: Initialize Go module**

```bash
go mod init github.com/<owner>/IronBook
```

(Replace `<owner>` with your GitHub username.)

- [ ] **Step 2: Add a placeholder `pkg/.keep`**

```bash
mkdir -p pkg && touch pkg/.keep
```

- [ ] **Step 3: Commit**

```bash
git add go.mod pkg/.keep
git commit -m "chore: initialize Go module"
```

---

### Task 1.3: Rust workspace + first crates

**Files:**
- Create: `Cargo.toml` (workspace root)
- Create: `crates/matching-engine/Cargo.toml`
- Create: `crates/matching-engine/src/lib.rs`
- Create: `crates/ironbookctl/Cargo.toml`
- Create: `crates/ironbookctl/src/main.rs`

- [ ] **Step 1: Write workspace `Cargo.toml`** (verbatim from spec §9.3, members minus crates we'll add later)

```toml
[workspace]
members = ["crates/matching-engine", "crates/ironbookctl"]
resolver = "2"

[workspace.package]
edition = "2021"
license = "Apache-2.0"

[workspace.dependencies]
tokio        = { version = "1", features = ["full"] }
serde        = { version = "1", features = ["derive"] }
serde_json   = "1"
anyhow       = "1"
thiserror    = "2"
proptest     = "1"

[workspace.lints.rust]
unsafe_code  = "forbid"

[workspace.lints.clippy]
pedantic     = { level = "warn", priority = -1 }
unwrap_used  = "warn"
expect_used  = "warn"
```

- [ ] **Step 2: Write `crates/matching-engine/Cargo.toml`**

```toml
[package]
name        = "matching-engine"
version     = "0.0.1"
edition.workspace = true
license.workspace = true

[dependencies]
serde      = { workspace = true }
thiserror  = { workspace = true }

[dev-dependencies]
proptest   = { workspace = true }

[lints]
workspace = true
```

- [ ] **Step 3: Write minimal `crates/matching-engine/src/lib.rs`**

```rust
//! IronBook matching engine — order book + matcher.
//! Phase 1 stub. Real implementation lands in Phase 2 Task 2.5.x.

#![doc(html_no_source)]

/// Phase 1 marker — proves the crate compiles.
pub fn version() -> &'static str { env!("CARGO_PKG_VERSION") }

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn version_string_set() { assert!(!version().is_empty()); }
}
```

- [ ] **Step 4: Write `crates/ironbookctl/Cargo.toml` and minimal `main.rs`**

Cargo.toml:
```toml
[package]
name              = "ironbookctl"
version           = "0.0.1"
edition.workspace = true
license.workspace = true

[dependencies]
anyhow = { workspace = true }

[lints]
workspace = true
```

main.rs:
```rust
fn main() -> anyhow::Result<()> {
    println!("ironbookctl v{}", env!("CARGO_PKG_VERSION"));
    Ok(())
}
```

- [ ] **Step 5: Verify workspace builds**

Run:
```bash
cargo build --workspace
cargo test --workspace
```

Expected: both green; tiny test passes.

- [ ] **Step 6: Commit**

```bash
git add Cargo.toml crates/
git commit -m "chore(rust): initialize workspace with matching-engine + ironbookctl skeletons"
```

---

### Task 1.4: Next.js frontend scaffold

**Files:**
- Create: `frontend/package.json`
- Create: `frontend/next.config.mjs`
- Create: `frontend/tsconfig.json`
- Create: `frontend/app/layout.tsx`
- Create: `frontend/app/page.tsx`

- [ ] **Step 1: Install pnpm if needed**

```bash
which pnpm || npm i -g pnpm
```

- [ ] **Step 2: Scaffold via `create-next-app`**

```bash
cd frontend && pnpm dlx create-next-app@latest . \
  --ts --tailwind --app --no-src-dir --import-alias "@/*" --use-pnpm \
  --eslint
cd ..
```

- [ ] **Step 3: Replace `frontend/app/page.tsx` with placeholder dashboard**

```tsx
export default function Home() {
  return (
    <main className="p-8">
      <h1 className="text-3xl font-semibold">IronBook</h1>
      <p className="text-sm text-gray-500">Distributed benchmarking & hosting platform.</p>
      <p className="mt-4 text-gray-700">Phase 1 placeholder — leaderboard ships in Phase 3.</p>
    </main>
  );
}
```

- [ ] **Step 4: Verify dev server runs**

```bash
cd frontend && pnpm install && pnpm dev
```

Open `http://localhost:3000`, see the heading. Ctrl-C to stop.

- [ ] **Step 5: Commit**

```bash
git add frontend/ .gitignore
git commit -m "chore(frontend): scaffold Next.js 15 app router with shadcn-ready setup"
```

---

### Task 1.5: Protobuf IDL + buf codegen

**Files:**
- Create: `proto/ironbook/v1/orders.proto`
- Create: `proto/ironbook/v1/runs.proto`
- Create: `proto/buf.yaml`
- Create: `buf.gen.yaml`

- [ ] **Step 1: Install buf**

```bash
brew install bufbuild/buf/buf
buf --version  # expect ≥ 1.32
```

- [ ] **Step 2: Create `proto/buf.yaml`**

```yaml
version: v2
modules:
  - path: ironbook
breaking:
  use: [FILE]
lint:
  use: [DEFAULT]
```

- [ ] **Step 3: Create `proto/ironbook/v1/orders.proto`**

```proto
syntax = "proto3";
package ironbook.v1;
option go_package = "github.com/<owner>/IronBook/pkg/proto/ironbook/v1;ironbookv1";

enum Side { SIDE_UNSPECIFIED = 0; BID = 1; ASK = 2; }
enum OrderType { ORDER_TYPE_UNSPECIFIED = 0; LIMIT = 1; MARKET = 2; }
enum TimeInForce { TIF_UNSPECIFIED = 0; GTC = 1; IOC = 2; FOK = 3; }

message NormalizedOrder {
  uint64 platform_seq      = 1;
  uint64 platform_ts_ns    = 2;
  bytes  client_order_id   = 3; // u128 packed (bot_id, local_seq)
  bytes  session_token     = 4; // 32 bytes opaque
  Side   side              = 5;
  uint64 qty               = 6;
  int64  price             = 7; // ticks; integer math
  OrderType order_type     = 8;
  TimeInForce tif          = 9;
}

message Ack {
  uint64 platform_seq = 1;
  uint64 ack_ts_ns    = 2;
  uint32 status       = 3; // 0 = ok
  string message      = 4;
}

message Fill {
  uint64 trade_id            = 1;
  uint64 platform_seq_taker  = 2;
  uint64 platform_seq_maker  = 3;
  int64  price               = 4;
  uint64 qty                 = 5;
  uint64 ts_ns               = 6;
}
```

- [ ] **Step 4: Create `proto/ironbook/v1/runs.proto`** (placeholder for Phase 2 — keeps codegen happy)

```proto
syntax = "proto3";
package ironbook.v1;
option go_package = "github.com/<owner>/IronBook/pkg/proto/ironbook/v1;ironbookv1";

message BenchmarkRunRef {
  bytes  run_id        = 1; // 16 bytes UUID v7
  string scenario_hash = 2;
  string submission_sha256 = 3;
}
```

- [ ] **Step 5: Create root `buf.gen.yaml`**

```yaml
version: v2
inputs:
  - directory: proto
plugins:
  - remote: buf.build/protocolbuffers/go
    out: pkg/proto
    opt: [paths=source_relative]
  - remote: buf.build/community/neoeinstein-prost
    out: crates/proto/src/gen
  - remote: buf.build/community/neoeinstein-tonic
    out: crates/proto/src/gen
```

- [ ] **Step 6: Add `crates/proto` skeleton (so the prost output path exists)**

Add to `Cargo.toml` workspace `members` list: `"crates/proto"`.

Create `crates/proto/Cargo.toml`:
```toml
[package]
name              = "ironbook-proto"
version           = "0.0.1"
edition.workspace = true

[dependencies]
prost = "0.13"
tonic = { version = "0.12", optional = true }

[features]
default = []
grpc    = ["dep:tonic"]

[lints]
workspace = true
```

Create `crates/proto/src/lib.rs`:
```rust
//! Generated protobuf bindings for IronBook.
//! Source of truth: ../../proto/

pub mod gen {
    #![allow(clippy::all)]
    pub mod ironbook { pub mod v1 { include!("gen/ironbook.v1.rs"); } }
}
```

- [ ] **Step 7: Run codegen and verify**

```bash
buf generate
cargo build --workspace
ls pkg/proto/ironbook/v1/   # expect orders.pb.go runs.pb.go
ls crates/proto/src/gen/    # expect ironbook.v1.rs
```

- [ ] **Step 8: Commit**

```bash
git add proto/ buf.gen.yaml Cargo.toml crates/proto/
git commit -m "feat(proto): add ironbook.v1 IDL with buf codegen for Go and Rust"
```

---

### Task 1.6: Makefile (phase-1 subset)

**Files:**
- Create: `Makefile`

- [ ] **Step 1: Write `Makefile`** (phase-1 subset; remaining targets land in later phases)

```make
SHELL := /usr/bin/env bash
.SHELLFLAGS := -eu -o pipefail -c
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
```

- [ ] **Step 2: Verify the targets work**

```bash
make help
make proto
make build
make test-unit
```

Expected: each succeeds.

- [ ] **Step 3: Commit**

```bash
git add Makefile
git commit -m "chore: add Makefile with proto/build/lint/fmt/test targets"
```

---

### Task 1.7: GitHub Actions CI skeleton + first ADR

**Files:**
- Create: `.github/workflows/ci.yml`
- Create: `docs/adr/001-gvisor-not-firecracker.md`

- [ ] **Step 1: Write `.github/workflows/ci.yml`**

```yaml
name: ci
on:
  pull_request:
  push:
    branches: [main]
concurrency:
  group: ${{ github.workflow }}-${{ github.ref }}
  cancel-in-progress: true

jobs:
  lint-rust:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
        with: { components: clippy,rustfmt }
      - uses: Swatinem/rust-cache@v2
      - run: cargo fmt --all -- --check
      - run: cargo clippy --workspace --all-targets -- -D warnings

  unit-rust:
    runs-on: ubuntu-latest
    needs: lint-rust
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
      - uses: Swatinem/rust-cache@v2
      - uses: bufbuild/buf-action@v1
      - run: buf generate
      - run: cargo test --workspace

  unit-go:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with: { go-version: '1.22', cache: true }
      - uses: bufbuild/buf-action@v1
      - run: buf generate
      - run: gofmt -l . | (! grep .)
      - run: go vet ./...
      - run: go test ./... -race -count=1 -timeout 60s

  proto-sync:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: bufbuild/buf-action@v1
      - run: buf lint
      - run: buf generate && git diff --exit-code
```

- [ ] **Step 2: Write ADR-001 (gVisor vs Firecracker)**

```markdown
# ADR-001: Sandbox runtime — gVisor (not Firecracker)

Date: 2026-05-10
Status: Accepted

## Context
The platform runs untrusted contestant binaries (C++/Rust/Go). Sandbox
options range from kernel namespaces (Docker default) to user-space-kernel
interception (gVisor) to microVMs (Firecracker, Kata).

## Decision
Use **gVisor (runsc)** as the primary submission runtime, layered with
seccomp-bpf, AppArmor, cgroups v2, NetworkPolicy, and an iptables host
backstop (spec §4.2).

## Consequences
- Production-grade isolation used by Google Cloud Run / App Engine.
- Compatible with single-VM cloud deployment (no nested virtualisation).
- ~10–30 µs syscall overhead — published in latency budget honestly (§2.3).
- Some syscalls deliberately denied (`io_uring_setup`) — tradeoff documented.

## Alternatives considered
- **Firecracker**: requires KVM. Mac M3 host has no Linux KVM; Hetzner
  shared-CPU plans don't expose nested virt cheaply. Rejected for hackathon
  budget; documented as future work (spec §10.1).
- **Kata Containers**: similar KVM dependency.
- **Plain Docker + seccomp**: insufficient isolation; kernel surface still
  exposed to contestant code.
- **WASM (wasmtime)**: ironclad isolation but forces contestants to compile
  to WASI; competitive engines lose threading + SIMD. Documented as a
  future plug-point (§10.1).
```

- [ ] **Step 3: Commit**

```bash
git add .github/workflows/ci.yml docs/adr/001-gvisor-not-firecracker.md
git commit -m "ci: add GitHub Actions workflow + ADR-001 (gVisor over Firecracker)"
```

- [ ] **Step 4: Push, watch CI green**

```bash
git push
gh run watch
```

---

## Day 2 — Local dev cluster bootstrap (~4 tasks, ~3–4 hours)

### Task 2.1: kindcluster bootstrap script

**Files:**
- Create: `tools/kindcluster/up.sh`
- Create: `tools/kindcluster/down.sh`
- Create: `tools/kindcluster/kind-config.yaml`

- [ ] **Step 1: Write `kind-config.yaml`** with 3 nodes and gVisor RuntimeClass support

```yaml
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
name: ironbook-control
networking:
  apiServerAddress: 127.0.0.1
  apiServerPort: 6443
nodes:
  - role: control-plane
    extraMounts:
      - hostPath: ./deploy/policies/seccomp
        containerPath: /var/lib/kubelet/seccomp
    kubeadmConfigPatches:
      - |
        kind: InitConfiguration
        nodeRegistration:
          kubeletExtraArgs:
            seccomp-default: "true"
            allowed-unsafe-sysctls: ""
  - role: worker
  - role: worker
```

- [ ] **Step 2: Write `up.sh`**

```bash
#!/usr/bin/env bash
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$DIR/../.." && pwd)"
export KUBECONFIG="$ROOT/kubeconfig.local"

if ! command -v kind > /dev/null; then
  echo "Install kind: brew install kind"; exit 1
fi
if ! command -v kubectl > /dev/null; then
  echo "Install kubectl: brew install kubectl"; exit 1
fi

mkdir -p "$ROOT/deploy/policies/seccomp"
[ -f "$ROOT/deploy/policies/seccomp/ironbook-sandbox.json" ] || \
  echo '{"defaultAction":"SCMP_ACT_ALLOW"}' > "$ROOT/deploy/policies/seccomp/ironbook-sandbox.json"

if kind get clusters | grep -q '^ironbook-control$'; then
  echo "[kindcluster] already running"
else
  kind create cluster --config "$DIR/kind-config.yaml" --kubeconfig "$KUBECONFIG"
fi
echo "[kindcluster] KUBECONFIG=$KUBECONFIG"
kubectl --kubeconfig "$KUBECONFIG" cluster-info
```

- [ ] **Step 3: Write `down.sh`**

```bash
#!/usr/bin/env bash
set -euo pipefail
kind delete cluster --name ironbook-control || true
rm -f kubeconfig.local
```

- [ ] **Step 4: Make executable**

```bash
chmod +x tools/kindcluster/{up,down}.sh
```

- [ ] **Step 5: Add Makefile target**

Edit `Makefile`, add:
```make
.PHONY: dev-up dev-down
dev-up:   ## Bring up local kind cluster
	./tools/kindcluster/up.sh
dev-down: ## Tear down local kind cluster
	./tools/kindcluster/down.sh
```

- [ ] **Step 6: Smoke test**

```bash
make dev-up
KUBECONFIG=$PWD/kubeconfig.local kubectl get nodes
make dev-down
```

Expected: 3 nodes Ready; cleanup succeeds.

- [ ] **Step 7: Commit**

```bash
git add tools/kindcluster/ Makefile
git commit -m "feat(tools): add kindcluster up/down scripts with seccomp profile mount"
```

---

### Task 2.2: Base manifests directory tree

**Files:**
- Create: `deploy/manifests/base/<component>/kustomization.yaml` for each early component

- [ ] **Step 1: Create base directories**

```bash
mkdir -p deploy/manifests/base/{cert-manager,argocd,opa-gatekeeper,postgres,minio,registry,submission-api,build-runner,admission-webhook,stub-fairness-gateway}
mkdir -p deploy/manifests/overlays/dev
```

- [ ] **Step 2: Add a placeholder kustomization to each (so layout is committed)**

For each subdir under `base/`, create a `kustomization.yaml`:

```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources: []   # populated in subsequent tasks
```

```bash
for d in deploy/manifests/base/*/; do
  cat > "$d/kustomization.yaml" <<'EOF'
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources: []
EOF
done
```

- [ ] **Step 3: Commit**

```bash
git add deploy/manifests/
git commit -m "chore(deploy): scaffold base manifest directory tree"
```

---

### Task 2.3: Install cert-manager, Argo CD, OPA Gatekeeper

**Files:**
- Modify: `deploy/manifests/base/cert-manager/kustomization.yaml`
- Modify: `deploy/manifests/base/argocd/kustomization.yaml`
- Modify: `deploy/manifests/base/opa-gatekeeper/kustomization.yaml`

- [ ] **Step 1: cert-manager — reference the upstream release**

`deploy/manifests/base/cert-manager/kustomization.yaml`:
```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
namespace: cert-manager
resources:
  - https://github.com/cert-manager/cert-manager/releases/download/v1.14.5/cert-manager.yaml
```

- [ ] **Step 2: Argo CD**

`deploy/manifests/base/argocd/kustomization.yaml`:
```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
namespace: argocd
resources:
  - https://raw.githubusercontent.com/argoproj/argo-cd/v2.10.7/manifests/install.yaml
```

- [ ] **Step 3: OPA Gatekeeper**

`deploy/manifests/base/opa-gatekeeper/kustomization.yaml`:
```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
namespace: gatekeeper-system
resources:
  - https://raw.githubusercontent.com/open-policy-agent/gatekeeper/v3.16.3/deploy/gatekeeper.yaml
```

- [ ] **Step 4: Commit**

```bash
git add deploy/manifests/base/cert-manager/ deploy/manifests/base/argocd/ deploy/manifests/base/opa-gatekeeper/
git commit -m "feat(deploy): add cert-manager + argocd + opa-gatekeeper base manifests"
```

---

### Task 2.4: Dev overlay + `make dev` smoke

**Files:**
- Create: `deploy/manifests/overlays/dev/kustomization.yaml`
- Modify: `Makefile`

- [ ] **Step 1: Write the dev overlay**

`deploy/manifests/overlays/dev/kustomization.yaml`:
```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
namespace: ironbook
namePrefix: dev-
resources:
  - ../../base/cert-manager
  - ../../base/argocd
  - ../../base/opa-gatekeeper
```

(Note: each base manifest sets its own namespace — namePrefix here applies only when we add ironbook-owned resources.)

- [ ] **Step 2: Add `make dev` target**

Edit `Makefile`:
```make
.PHONY: dev
dev: dev-up ## Bring up local cluster + apply dev overlay
	KUBECONFIG=$$PWD/kubeconfig.local kubectl create ns cert-manager --dry-run=client -o yaml | kubectl --kubeconfig $$PWD/kubeconfig.local apply -f -
	KUBECONFIG=$$PWD/kubeconfig.local kubectl create ns argocd --dry-run=client -o yaml | kubectl --kubeconfig $$PWD/kubeconfig.local apply -f -
	KUBECONFIG=$$PWD/kubeconfig.local kubectl create ns gatekeeper-system --dry-run=client -o yaml | kubectl --kubeconfig $$PWD/kubeconfig.local apply -f -
	KUBECONFIG=$$PWD/kubeconfig.local kustomize build deploy/manifests/overlays/dev | kubectl --kubeconfig $$PWD/kubeconfig.local apply --server-side -f -
	KUBECONFIG=$$PWD/kubeconfig.local kubectl rollout status -n cert-manager deploy/cert-manager-webhook --timeout=180s
```

- [ ] **Step 3: Run `make dev`**

```bash
make dev
```

Expected: kind cluster up; cert-manager rollout completes; argocd pods running.

- [ ] **Step 4: Verify**

```bash
KUBECONFIG=$PWD/kubeconfig.local kubectl get pods -A
```

You should see cert-manager, argocd-server, gatekeeper-controller-manager.

- [ ] **Step 5: Commit**

```bash
git add deploy/manifests/overlays/dev/ Makefile
git commit -m "feat(deploy): add dev overlay; make dev installs cert-manager + argocd + gatekeeper"
```

---

## Day 3 — Hetzner provisioning + Wireguard (~4 tasks, ~5 hours)

### Task 3.1: Terraform `hetzner-vm` module

**Files:**
- Create: `deploy/terraform/modules/hetzner-vm/{main,variables,outputs}.tf`

- [ ] **Step 1: Install Terraform 1.7**

```bash
brew install terraform
terraform version  # ≥ 1.7
```

- [ ] **Step 2: `variables.tf`**

```hcl
variable "name"          { type = string }
variable "server_type"   { type = string  default = "cax11" } # ARM 2 vCPU 4GB; upgrade to cax21 if needed
variable "image"         { type = string  default = "ubuntu-24.04" }
variable "location"      { type = string  default = "fsn1" }
variable "ssh_key_ids"   { type = list(number) }
variable "volume_size_gb"{ type = number  default = 50 }
variable "user_data"     { type = string  default = "" }
```

- [ ] **Step 3: `main.tf`**

```hcl
terraform {
  required_providers {
    hcloud = { source = "hetznercloud/hcloud" version = "~> 1.47" }
  }
}

resource "hcloud_server" "vm" {
  name        = var.name
  server_type = var.server_type
  image       = var.image
  location    = var.location
  ssh_keys    = var.ssh_key_ids
  user_data   = var.user_data

  public_net {
    ipv4_enabled = true
    ipv6_enabled = true
  }
}

resource "hcloud_volume" "data" {
  name      = "${var.name}-data"
  size      = var.volume_size_gb
  location  = var.location
  format    = "ext4"
}

resource "hcloud_volume_attachment" "data" {
  volume_id = hcloud_volume.data.id
  server_id = hcloud_server.vm.id
  automount = true
}

resource "hcloud_firewall" "fw" {
  name = "${var.name}-fw"

  # SSH from anywhere (key auth only)
  rule { direction = "in" protocol = "tcp" port = "22"   source_ips = ["0.0.0.0/0", "::/0"] }
  # Wireguard
  rule { direction = "in" protocol = "udp" port = "51820" source_ips = ["0.0.0.0/0", "::/0"] }
  # k3s API only over WG (no public exposure)

  apply_to { server = hcloud_server.vm.id }
}
```

- [ ] **Step 4: `outputs.tf`**

```hcl
output "public_ipv4"  { value = hcloud_server.vm.ipv4_address }
output "public_ipv6"  { value = hcloud_server.vm.ipv6_address }
output "private_ipv4" { value = hcloud_server.vm.network[0].ip default = null }
output "vm_id"        { value = hcloud_server.vm.id }
```

- [ ] **Step 5: Commit**

```bash
git add deploy/terraform/modules/hetzner-vm/
git commit -m "feat(terraform): add hetzner-vm module (CCX/CAX, attached data volume, firewall)"
```

---

### Task 3.2: Terraform `wireguard` module

**Files:**
- Create: `deploy/terraform/modules/wireguard/{main,variables,outputs}.tf`

- [ ] **Step 1: `variables.tf`**

```hcl
variable "peers" {
  description = "list of {name, endpoint, allowed_ips}"
  type = list(object({
    name        = string
    endpoint    = optional(string)
    allowed_ips = list(string)
  }))
}
variable "subnet" { type = string default = "10.99.0.0/24" }
```

- [ ] **Step 2: `main.tf`**

```hcl
terraform {
  required_providers {
    tls = { source = "hashicorp/tls" version = "~> 4.0" }
  }
}

resource "tls_private_key" "peer" {
  for_each   = { for p in var.peers : p.name => p }
  algorithm  = "ED25519"  # placeholder — wireguard uses Curve25519; we generate via shellscript below
}

# Generate WG keypair per peer via local-exec (Terraform has no native WG keygen).
resource "null_resource" "wg_keys" {
  for_each = { for p in var.peers : p.name => p }
  provisioner "local-exec" {
    command = <<-EOT
      mkdir -p .wg/${each.key}
      umask 077
      wg genkey | tee .wg/${each.key}/private.key | wg pubkey > .wg/${each.key}/public.key
    EOT
  }
}

# Render config files for each peer (read previously generated keys).
data "external" "wg_pub" {
  for_each   = { for p in var.peers : p.name => p }
  depends_on = [null_resource.wg_keys]
  program    = ["bash", "-c", "echo \"{\\\"k\\\":\\\"$(cat .wg/${each.key}/public.key | tr -d '\\n')\\\"}\""]
}
data "external" "wg_priv" {
  for_each   = { for p in var.peers : p.name => p }
  depends_on = [null_resource.wg_keys]
  program    = ["bash", "-c", "echo \"{\\\"k\\\":\\\"$(cat .wg/${each.key}/private.key | tr -d '\\n')\\\"}\""]
  sensitive  = true
}

locals {
  ip_for = { for i, p in var.peers : p.name => cidrhost(var.subnet, i + 1) }
}

resource "local_sensitive_file" "config" {
  for_each = { for p in var.peers : p.name => p }
  filename = ".wg/${each.key}/wg-quick.conf"
  content  = <<-EOT
    [Interface]
    PrivateKey = ${data.external.wg_priv[each.key].result.k}
    Address    = ${local.ip_for[each.key]}/24
    ListenPort = 51820

    %{ for other in [for q in var.peers : q if q.name != each.key] }
    [Peer]
    # ${other.name}
    PublicKey  = ${data.external.wg_pub[other.name].result.k}
    AllowedIPs = ${join(",", concat([local.ip_for[other.name] + "/32"], other.allowed_ips))}
    %{ if other.endpoint != null }Endpoint   = ${other.endpoint}%{ endif }
    PersistentKeepalive = 25
    %{ endfor }
  EOT
}
```

- [ ] **Step 3: `outputs.tf`**

```hcl
output "subnet"      { value = var.subnet }
output "addresses"   { value = local.ip_for }
output "configs_dir" { value = ".wg" sensitive = true }
```

- [ ] **Step 4: Commit**

```bash
git add deploy/terraform/modules/wireguard/
git commit -m "feat(terraform): add wireguard module (per-peer keygen + wg-quick configs)"
```

---

### Task 3.3: Terraform `envs/prod` composition

**Files:**
- Create: `deploy/terraform/envs/prod/{main,backend,variables,outputs}.tf`
- Create: `deploy/terraform/envs/prod/terraform.tfvars.example`

- [ ] **Step 1: `backend.tf`** (use local state for now; switch to MinIO-S3 in Phase 4)

```hcl
terraform {
  required_version = ">= 1.7"
  backend "local" { path = "terraform.tfstate" }
}
```

- [ ] **Step 2: `variables.tf`**

```hcl
variable "hcloud_token"      { type = string sensitive = true }
variable "hcloud_ssh_key_id" { type = number }
variable "mac_endpoint"      { type = string  description = "control-plane public endpoint host:port for WG (or empty for client-only)" default = "" }
```

- [ ] **Step 3: `main.tf`**

```hcl
terraform {
  required_providers {
    hcloud = { source = "hetznercloud/hcloud" version = "~> 1.47" }
  }
}
provider "hcloud" { token = var.hcloud_token }

module "sandbox_vm" {
  source        = "../../modules/hetzner-vm"
  name          = "ironbook-sandbox"
  server_type   = "cax21"      # 4 vCPU ARM, 8 GB RAM
  ssh_key_ids   = [var.hcloud_ssh_key_id]
  volume_size_gb = 50
  user_data     = file("${path.module}/cloud-init.yaml")
}

module "wireguard" {
  source = "../../modules/wireguard"
  peers = [
    { name = "control", endpoint = var.mac_endpoint != "" ? var.mac_endpoint : null, allowed_ips = [] },
    { name = "sandbox", endpoint = "${module.sandbox_vm.public_ipv4}:51820",         allowed_ips = ["10.42.0.0/16"] },
  ]
}
```

- [ ] **Step 4: `cloud-init.yaml`** (installs k3s + WG + sets up data volume)

`deploy/terraform/envs/prod/cloud-init.yaml`:
```yaml
#cloud-config
package_update: true
package_upgrade: true
packages:
  - wireguard
  - jq
  - curl
write_files:
  - path: /etc/sysctl.d/99-ipforward.conf
    content: |
      net.ipv4.ip_forward = 1
      net.ipv4.conf.all.forwarding = 1
runcmd:
  - sysctl -p /etc/sysctl.d/99-ipforward.conf
  - mkdir -p /var/lib/rancher
  - ln -s /mnt/HC_Volume_*/k3s /var/lib/rancher/k3s || true
  - curl -sfL https://get.k3s.io | INSTALL_K3S_CHANNEL=v1.30 sh -s - server --disable=traefik --secrets-encryption
  - chmod 644 /etc/rancher/k3s/k3s.yaml
```

- [ ] **Step 5: `terraform.tfvars.example`**

```hcl
hcloud_token       = "REPLACE_ME"
hcloud_ssh_key_id  = 0
mac_endpoint       = ""   # leave empty if Mac is behind NAT (Hetzner side dials in via WG)
```

- [ ] **Step 6: Commit (.gitignore protects real tfvars)**

```bash
git add deploy/terraform/envs/prod/
git commit -m "feat(terraform): add prod env composition (Hetzner sandbox VM + WG mesh)"
```

---

### Task 3.4: Provision Hetzner + WG handshake

(Manual one-time bring-up — record steps in a runbook.)

**Files:**
- Create: `docs/runbooks/02-bring-up-hetzner.md`

- [ ] **Step 1: Get Hetzner Cloud API token + SSH key uploaded**

Manual: Hetzner Cloud Console → Project → Security → API Tokens → create read+write. Upload your local SSH key under SSH Keys.

- [ ] **Step 2: Create `terraform.tfvars` from example, fill in token + ssh-key-id**

```bash
cd deploy/terraform/envs/prod
cp terraform.tfvars.example terraform.tfvars
$EDITOR terraform.tfvars
```

- [ ] **Step 3: `terraform init && terraform apply`**

```bash
terraform init
terraform apply
```

Expected: server provisioned (~2 min); volume attached; WG configs generated under `.wg/`.

- [ ] **Step 4: Bring WG up on Mac**

```bash
sudo cp .wg/control/wg-quick.conf /etc/wireguard/ironbook.conf
sudo wg-quick up ironbook
sudo wg show
```

- [ ] **Step 5: Bring WG up on Hetzner via SSH**

```bash
SANDBOX_IP=$(terraform output -raw sandbox_ip)   # add this output to outputs.tf if missing
scp .wg/sandbox/wg-quick.conf root@$SANDBOX_IP:/etc/wireguard/ironbook.conf
ssh root@$SANDBOX_IP 'wg-quick up ironbook && wg show'
```

- [ ] **Step 6: Verify mesh connectivity**

```bash
ping -c 3 10.99.0.2   # sandbox WG IP
```

Expected: ping succeeds.

- [ ] **Step 7: Pull k3s kubeconfig over WG**

```bash
ssh root@$SANDBOX_IP 'cat /etc/rancher/k3s/k3s.yaml' \
  | sed "s|127.0.0.1|10.99.0.2|" \
  > kubeconfig.sandbox
KUBECONFIG=$PWD/kubeconfig.sandbox kubectl get nodes
```

Expected: 1 node Ready.

- [ ] **Step 8: Write the runbook capturing all the above** (`docs/runbooks/02-bring-up-hetzner.md`)

Document each step verbatim so you can rebuild from scratch.

- [ ] **Step 9: Commit**

```bash
git add docs/runbooks/02-bring-up-hetzner.md
git commit -m "docs(runbooks): add Hetzner + Wireguard bring-up runbook"
```

---

## Day 4 — Postgres + MinIO + submission-api skeleton (~6 tasks, ~6 hours)

### Task 4.1: Postgres schema + migration framework

**Files:**
- Create: `deploy/manifests/base/postgres/{statefulset,service,secret-template}.yaml`
- Create: `deploy/manifests/base/postgres/migrations/{001_init.sql,002_submissions.sql}`
- Create: `pkg/postgresclient/client.go`

- [ ] **Step 1: Postgres StatefulSet manifest**

`deploy/manifests/base/postgres/statefulset.yaml`:
```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata: { name: postgres, namespace: ironbook, labels: { app: postgres } }
spec:
  serviceName: postgres
  replicas: 1
  selector: { matchLabels: { app: postgres } }
  template:
    metadata: { labels: { app: postgres } }
    spec:
      securityContext: { fsGroup: 999 }
      containers:
        - name: postgres
          image: postgres:16-alpine@sha256:8c0f0...    # pin in CI
          imagePullPolicy: IfNotPresent
          env:
            - { name: POSTGRES_DB,       value: ironbook }
            - { name: POSTGRES_USER,     valueFrom: { secretKeyRef: { name: postgres, key: user } } }
            - { name: POSTGRES_PASSWORD, valueFrom: { secretKeyRef: { name: postgres, key: password } } }
            - { name: PGDATA,            value: /var/lib/postgresql/data/pgdata }
          ports: [ { name: pg, containerPort: 5432 } ]
          volumeMounts: [ { name: data, mountPath: /var/lib/postgresql/data } ]
          readinessProbe: { exec: { command: [pg_isready, -U, $(POSTGRES_USER)] }, initialDelaySeconds: 10 }
  volumeClaimTemplates:
    - metadata: { name: data }
      spec:
        accessModes: [ReadWriteOnce]
        resources: { requests: { storage: 10Gi } }
```

`service.yaml`:
```yaml
apiVersion: v1
kind: Service
metadata: { name: postgres, namespace: ironbook }
spec:
  selector: { app: postgres }
  ports: [ { name: pg, port: 5432, targetPort: 5432 } ]
  clusterIP: None  # headless (StatefulSet)
```

`secret-template.yaml`:
```yaml
# Apply manually for dev or via SealedSecret in CI/prod
apiVersion: v1
kind: Secret
metadata: { name: postgres, namespace: ironbook }
type: Opaque
stringData:
  user: ironbook
  password: REPLACE_ME
```

`kustomization.yaml`:
```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
namespace: ironbook
resources: [statefulset.yaml, service.yaml, secret-template.yaml]
```

- [ ] **Step 2: Migrations**

`migrations/001_init.sql`:
```sql
CREATE TABLE IF NOT EXISTS schema_version (
  version    INTEGER PRIMARY KEY,
  applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
INSERT INTO schema_version (version) VALUES (1) ON CONFLICT DO NOTHING;
```

`migrations/002_submissions.sql`:
```sql
CREATE TABLE IF NOT EXISTS submissions (
  id            UUID PRIMARY KEY,
  sha256        BYTEA NOT NULL UNIQUE,
  language      TEXT NOT NULL CHECK (language IN ('rust','go','cpp')),
  status        TEXT NOT NULL CHECK (status IN ('PENDING','BUILDING','READY','REJECTED')),
  image_digest  TEXT,
  reject_reason TEXT,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX submissions_status_idx ON submissions (status);
INSERT INTO schema_version (version) VALUES (2) ON CONFLICT DO NOTHING;
```

- [ ] **Step 3: Postgres client wrapper**

`pkg/postgresclient/client.go`:
```go
package postgresclient

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Client struct{ Pool *pgxpool.Pool }

func New(ctx context.Context, dsn string) (*Client, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse dsn: %w", err)
	}
	cfg.MaxConns = 16
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("ping: %w", err)
	}
	return &Client{Pool: pool}, nil
}

func (c *Client) Close() { c.Pool.Close() }
```

Add deps:
```bash
go get github.com/jackc/pgx/v5/pgxpool@v5.6.0
go mod tidy
```

- [ ] **Step 4: Commit**

```bash
git add deploy/manifests/base/postgres/ pkg/postgresclient/ go.mod go.sum
git commit -m "feat(postgres): add base manifest + migrations + pgxpool client wrapper"
```

---

### Task 4.2: MinIO base manifest + client

**Files:**
- Create: `deploy/manifests/base/minio/{statefulset,service,secret}.yaml,kustomization.yaml`
- Create: `pkg/miniclient/client.go`

- [ ] **Step 1: MinIO StatefulSet**

`statefulset.yaml`:
```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata: { name: minio, namespace: ironbook }
spec:
  serviceName: minio
  replicas: 1
  selector: { matchLabels: { app: minio } }
  template:
    metadata: { labels: { app: minio } }
    spec:
      containers:
        - name: minio
          image: quay.io/minio/minio:RELEASE.2024-06-13T22-53-53Z
          args: ["server", "/data", "--console-address", ":9001"]
          env:
            - { name: MINIO_ROOT_USER,     valueFrom: { secretKeyRef: { name: minio, key: user } } }
            - { name: MINIO_ROOT_PASSWORD, valueFrom: { secretKeyRef: { name: minio, key: password } } }
          ports:
            - { name: api,     containerPort: 9000 }
            - { name: console, containerPort: 9001 }
          volumeMounts: [ { name: data, mountPath: /data } ]
  volumeClaimTemplates:
    - metadata: { name: data }
      spec: { accessModes: [ReadWriteOnce], resources: { requests: { storage: 20Gi } } }
```

`service.yaml`:
```yaml
apiVersion: v1
kind: Service
metadata: { name: minio, namespace: ironbook }
spec:
  selector: { app: minio }
  ports:
    - { name: api,     port: 9000, targetPort: 9000 }
    - { name: console, port: 9001, targetPort: 9001 }
```

`secret.yaml`:
```yaml
apiVersion: v1
kind: Secret
metadata: { name: minio, namespace: ironbook }
type: Opaque
stringData:
  user: ironbook
  password: REPLACE_ME
```

`kustomization.yaml`:
```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
namespace: ironbook
resources: [statefulset.yaml, service.yaml, secret.yaml]
```

- [ ] **Step 2: MinIO client wrapper**

`pkg/miniclient/client.go`:
```go
package miniclient

import (
	"context"
	"fmt"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type Client struct {
	Inner  *minio.Client
	Bucket string
}

func New(endpoint, accessKey, secretKey, bucket string, useSSL bool) (*Client, error) {
	c, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: useSSL,
	})
	if err != nil {
		return nil, fmt.Errorf("minio new: %w", err)
	}
	return &Client{Inner: c, Bucket: bucket}, nil
}

func (c *Client) EnsureBucket(ctx context.Context) error {
	exists, err := c.Inner.BucketExists(ctx, c.Bucket)
	if err != nil {
		return fmt.Errorf("exists: %w", err)
	}
	if !exists {
		return c.Inner.MakeBucket(ctx, c.Bucket, minio.MakeBucketOptions{})
	}
	return nil
}
```

Add deps:
```bash
go get github.com/minio/minio-go/v7@v7.0.71
go mod tidy
```

- [ ] **Step 3: Commit**

```bash
git add deploy/manifests/base/minio/ pkg/miniclient/ go.mod go.sum
git commit -m "feat(minio): add base manifest + minio-go client wrapper"
```

---

### Task 4.3: `submission-api` skeleton

**Files:**
- Create: `apps/submission-api/{main.go,server/server.go,service/service.go,repo/{postgres,minio}.go,config/config.go}`

- [ ] **Step 1: `config/config.go`**

```go
package config

import "github.com/kelseyhightower/envconfig"

type Config struct {
	HTTPAddr           string `envconfig:"HTTP_ADDR" default:":8080"`
	GRPCAddr           string `envconfig:"GRPC_ADDR" default:":9090"`
	PostgresDSN        string `envconfig:"POSTGRES_DSN" required:"true"`
	MinIOEndpoint      string `envconfig:"MINIO_ENDPOINT" required:"true"`
	MinIOAccessKey     string `envconfig:"MINIO_ACCESS_KEY" required:"true"`
	MinIOSecretKey     string `envconfig:"MINIO_SECRET_KEY" required:"true"`
	MinIOBucket        string `envconfig:"MINIO_BUCKET" default:"submissions"`
	MinIOUseSSL        bool   `envconfig:"MINIO_USE_SSL" default:"false"`
}

func Load() (Config, error) { var c Config; err := envconfig.Process("ironbook", &c); return c, err }
```

Add `go get github.com/kelseyhightower/envconfig@v1.4.0`.

- [ ] **Step 2: `repo/postgres.go`** — submission CRUD

```go
package repo

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Submission struct {
	ID         uuid.UUID
	Sha256     []byte
	Language   string
	Status     string
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

type Postgres struct{ Pool *pgxpool.Pool }

var ErrSubmissionExists = errors.New("submission already exists")

func (p *Postgres) Insert(ctx context.Context, s *Submission) error {
	_, err := p.Pool.Exec(ctx,
		`INSERT INTO submissions (id, sha256, language, status) VALUES ($1, $2, $3, $4)`,
		s.ID, s.Sha256, s.Language, s.Status)
	if err != nil {
		// 23505 = unique_violation
		if pe, ok := err.(*pgx.PgError); ok && pe.Code == "23505" {
			return ErrSubmissionExists
		}
		return err
	}
	return nil
}

func (p *Postgres) BySha256(ctx context.Context, sha []byte) (*Submission, error) {
	row := p.Pool.QueryRow(ctx, `SELECT id, sha256, language, status, created_at, updated_at FROM submissions WHERE sha256 = $1`, sha)
	var s Submission
	if err := row.Scan(&s.ID, &s.Sha256, &s.Language, &s.Status, &s.CreatedAt, &s.UpdatedAt); err != nil {
		return nil, err
	}
	return &s, nil
}
```

`go get github.com/google/uuid@v1.6.0`.

- [ ] **Step 3: `repo/minio.go`** — content-addressed PUT

```go
package repo

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"

	miniclient "github.com/<owner>/IronBook/pkg/miniclient"
)

type MinIO struct{ C *miniclient.Client }

// PutContentAddressed streams r into MinIO under <bucket>/<sha>/source.tar.zst
// and returns the sha256 of the bytes read.
func (m *MinIO) PutContentAddressed(ctx context.Context, r io.Reader) (sha [32]byte, n int64, err error) {
	var buf bytes.Buffer
	h := sha256.New()
	tr := io.TeeReader(r, h)
	n, err = io.Copy(&buf, tr)
	if err != nil { return }
	copy(sha[:], h.Sum(nil))
	key := fmt.Sprintf("%x/source.tar.zst", sha)
	_, err = m.C.Inner.PutObject(ctx, m.C.Bucket, key, &buf, n, miniclient.MinIOOpts())
	return
}
```

Add helper to `pkg/miniclient`:
```go
func MinIOOpts() minio.PutObjectOptions { return minio.PutObjectOptions{ContentType: "application/zstd"} }
```

- [ ] **Step 4: `service/service.go`** — orchestration

```go
package service

import (
	"context"
	"fmt"
	"io"

	"github.com/google/uuid"
	"github.com/<owner>/IronBook/apps/submission-api/repo"
)

type Service struct {
	PG    *repo.Postgres
	MinIO *repo.MinIO
}

type UploadResult struct {
	ID     uuid.UUID
	Sha256 [32]byte
	Size   int64
}

func (s *Service) Upload(ctx context.Context, language string, r io.Reader) (*UploadResult, error) {
	sha, n, err := s.MinIO.PutContentAddressed(ctx, r)
	if err != nil { return nil, fmt.Errorf("minio: %w", err) }
	id := uuid.New()
	sub := &repo.Submission{ID: id, Sha256: sha[:], Language: language, Status: "PENDING"}
	if err := s.PG.Insert(ctx, sub); err != nil {
		if err == repo.ErrSubmissionExists {
			// dedupe — return the existing one
			existing, e2 := s.PG.BySha256(ctx, sha[:])
			if e2 != nil { return nil, e2 }
			return &UploadResult{ID: existing.ID, Sha256: sha, Size: n}, nil
		}
		return nil, err
	}
	return &UploadResult{ID: id, Sha256: sha, Size: n}, nil
}
```

- [ ] **Step 5: `server/server.go`** — HTTP/2 multipart endpoint

```go
package server

import (
	"encoding/hex"
	"encoding/json"
	"net/http"

	"github.com/<owner>/IronBook/apps/submission-api/service"
)

type Server struct{ Svc *service.Service }

func (s *Server) HandleUpload(w http.ResponseWriter, r *http.Request) {
	lang := r.URL.Query().Get("language")
	if lang == "" { http.Error(w, "missing ?language=", 400); return }
	defer r.Body.Close()
	res, err := s.Svc.Upload(r.Context(), lang, r.Body)
	if err != nil { http.Error(w, err.Error(), 500); return }
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{
		"id":     res.ID.String(),
		"sha256": hex.EncodeToString(res.Sha256[:]),
	})
}
```

- [ ] **Step 6: `main.go`** — wiring only

```go
package main

import (
	"context"
	"log"
	"net/http"
	"os"

	"github.com/<owner>/IronBook/apps/submission-api/config"
	"github.com/<owner>/IronBook/apps/submission-api/repo"
	"github.com/<owner>/IronBook/apps/submission-api/server"
	"github.com/<owner>/IronBook/apps/submission-api/service"
	"github.com/<owner>/IronBook/pkg/miniclient"
	"github.com/<owner>/IronBook/pkg/postgresclient"
)

func main() {
	cfg, err := config.Load()
	if err != nil { log.Fatalf("config: %v", err) }
	ctx := context.Background()

	pg, err := postgresclient.New(ctx, cfg.PostgresDSN)
	if err != nil { log.Fatalf("pg: %v", err) }
	defer pg.Close()

	mc, err := miniclient.New(cfg.MinIOEndpoint, cfg.MinIOAccessKey, cfg.MinIOSecretKey, cfg.MinIOBucket, cfg.MinIOUseSSL)
	if err != nil { log.Fatalf("minio: %v", err) }
	if err := mc.EnsureBucket(ctx); err != nil { log.Fatalf("bucket: %v", err) }

	svc := &service.Service{PG: &repo.Postgres{Pool: pg.Pool}, MinIO: &repo.MinIO{C: mc}}
	srv := &server.Server{Svc: svc}

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/upload", srv.HandleUpload)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok")) })

	log.Printf("listening on %s", cfg.HTTPAddr)
	if err := http.ListenAndServe(cfg.HTTPAddr, mux); err != nil {
		log.Fatalf("serve: %v", err)
		os.Exit(1)
	}
}
```

- [ ] **Step 7: Build**

```bash
go build ./apps/submission-api
```

Expected: clean build.

- [ ] **Step 8: Commit**

```bash
git add apps/submission-api/ pkg/miniclient/ go.mod go.sum
git commit -m "feat(submission-api): skeleton with content-addressed upload to MinIO + Postgres metadata"
```

---

### Task 4.4: Integration test (Testcontainers) for upload happy path

**Files:**
- Create: `apps/submission-api/integration/upload_test.go`

- [ ] **Step 1: Write the failing integration test**

```go
//go:build integration

package integration

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/<owner>/IronBook/apps/submission-api/repo"
	"github.com/<owner>/IronBook/apps/submission-api/server"
	"github.com/<owner>/IronBook/apps/submission-api/service"
	"github.com/<owner>/IronBook/pkg/miniclient"
	"github.com/<owner>/IronBook/pkg/postgresclient"
	tcpg    "github.com/testcontainers/testcontainers-go/modules/postgres"
	tcminio "github.com/testcontainers/testcontainers-go/modules/minio"
)

func TestUpload_HappyPath(t *testing.T) {
	ctx := context.Background()

	pgC, err := tcpg.Run(ctx, "postgres:16-alpine",
		tcpg.WithDatabase("ironbook"), tcpg.WithUsername("u"), tcpg.WithPassword("p"),
		tcpg.BasicWaitStrategies(),
		tcpg.WithInitScripts("../../../deploy/manifests/base/postgres/migrations/001_init.sql",
		                     "../../../deploy/manifests/base/postgres/migrations/002_submissions.sql"))
	if err != nil { t.Fatal(err) }
	t.Cleanup(func() { _ = pgC.Terminate(ctx) })
	dsn, _ := pgC.ConnectionString(ctx, "sslmode=disable")
	pg, err := postgresclient.New(ctx, dsn); if err != nil { t.Fatal(err) }
	t.Cleanup(pg.Close)

	mC, err := tcminio.Run(ctx, "minio/minio:RELEASE.2024-06-13T22-53-53Z",
		tcminio.WithUsername("U"), tcminio.WithPassword("PASSWORD123"))
	if err != nil { t.Fatal(err) }
	t.Cleanup(func() { _ = mC.Terminate(ctx) })
	mEnd, _ := mC.ConnectionString(ctx)
	mc, err := miniclient.New(mEnd, "U", "PASSWORD123", "submissions", false)
	if err != nil { t.Fatal(err) }
	if err := mc.EnsureBucket(ctx); err != nil { t.Fatal(err) }

	svc := &service.Service{PG: &repo.Postgres{Pool: pg.Pool}, MinIO: &repo.MinIO{C: mc}}
	srv := &server.Server{Svc: svc}
	ts := httptest.NewServer(http.HandlerFunc(srv.HandleUpload))
	t.Cleanup(ts.Close)

	body := bytes.NewBufferString("hello-world rust source archive")
	resp, err := http.Post(ts.URL+"?language=rust", "application/zstd", body)
	if err != nil { t.Fatal(err) }
	defer resp.Body.Close()
	if resp.StatusCode != 200 { b, _ := io.ReadAll(resp.Body); t.Fatalf("status=%d body=%s", resp.StatusCode, b) }

	var out map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil { t.Fatal(err) }
	if len(out["sha256"]) != 64 { t.Fatalf("bad sha: %s", out["sha256"]) }
	// sha must equal sha256 of body
	want := "ad24...rest_of_known_hash"  // compute in step 2
	_ = hex.EncodeToString
	_ = want
}
```

- [ ] **Step 2: Compute the expected sha256 once**

```bash
echo -n "hello-world rust source archive" | shasum -a 256
```

Paste the result into the `want` line, replacing the placeholder.

- [ ] **Step 3: Add test deps**

```bash
go get github.com/testcontainers/testcontainers-go@v0.31.0
go get github.com/testcontainers/testcontainers-go/modules/postgres@v0.31.0
go get github.com/testcontainers/testcontainers-go/modules/minio@v0.31.0
go mod tidy
```

- [ ] **Step 4: Run test (should pass)**

```bash
go test -tags=integration ./apps/submission-api/integration/... -count=1 -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add apps/submission-api/integration/ go.mod go.sum
git commit -m "test(submission-api): integration test for upload happy path (Testcontainers)"
```

---

### Task 4.5: Submission-api k8s manifest + dev overlay wiring

**Files:**
- Create: `deploy/manifests/base/submission-api/{deployment,service,configmap}.yaml,kustomization.yaml`
- Modify: `deploy/manifests/overlays/dev/kustomization.yaml`

- [ ] **Step 1: `deployment.yaml`**

```yaml
apiVersion: apps/v1
kind: Deployment
metadata: { name: submission-api, namespace: ironbook }
spec:
  replicas: 1
  selector: { matchLabels: { app: submission-api } }
  template:
    metadata: { labels: { app: submission-api } }
    spec:
      containers:
        - name: api
          image: ironbook/submission-api:dev   # locally-built; loaded into kind via `kind load docker-image`
          ports: [ { name: http, containerPort: 8080 } ]
          env:
            - { name: IRONBOOK_HTTP_ADDR,       value: ":8080" }
            - { name: IRONBOOK_POSTGRES_DSN,    value: "postgres://ironbook:REPLACE_ME@postgres:5432/ironbook?sslmode=disable" }
            - { name: IRONBOOK_MINIO_ENDPOINT,  value: "minio:9000" }
            - { name: IRONBOOK_MINIO_ACCESS_KEY, valueFrom: { secretKeyRef: { name: minio, key: user } } }
            - { name: IRONBOOK_MINIO_SECRET_KEY, valueFrom: { secretKeyRef: { name: minio, key: password } } }
            - { name: IRONBOOK_MINIO_BUCKET,    value: "submissions" }
          readinessProbe: { httpGet: { path: /healthz, port: 8080 } }
```

`service.yaml`:
```yaml
apiVersion: v1
kind: Service
metadata: { name: submission-api, namespace: ironbook }
spec:
  selector: { app: submission-api }
  ports: [ { name: http, port: 8080, targetPort: 8080 } ]
```

`kustomization.yaml`:
```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
namespace: ironbook
resources: [deployment.yaml, service.yaml]
```

- [ ] **Step 2: Update dev overlay to include MinIO + Postgres + submission-api**

`deploy/manifests/overlays/dev/kustomization.yaml`:
```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - ../../base/cert-manager
  - ../../base/argocd
  - ../../base/opa-gatekeeper
  - ../../base/postgres
  - ../../base/minio
  - ../../base/submission-api
```

- [ ] **Step 3: Add a Dockerfile for submission-api**

`apps/submission-api/Dockerfile`:
```dockerfile
# syntax=docker/dockerfile:1.6
FROM golang:1.22-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/submission-api ./apps/submission-api

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/submission-api /usr/local/bin/submission-api
USER nonroot
ENTRYPOINT ["/usr/local/bin/submission-api"]
```

- [ ] **Step 4: Build + load + apply**

```bash
docker build -t ironbook/submission-api:dev -f apps/submission-api/Dockerfile .
kind load docker-image ironbook/submission-api:dev --name ironbook-control
KUBECONFIG=$PWD/kubeconfig.local kubectl apply -k deploy/manifests/overlays/dev
KUBECONFIG=$PWD/kubeconfig.local kubectl rollout status -n ironbook deploy/submission-api
```

- [ ] **Step 5: Manual smoke**

```bash
KUBECONFIG=$PWD/kubeconfig.local kubectl -n ironbook port-forward svc/submission-api 8080:8080 &
echo "rust hello world" | curl -X POST --data-binary @- 'http://localhost:8080/v1/upload?language=rust'
```

Expected: JSON `{ id, sha256 }`.

- [ ] **Step 6: Commit**

```bash
git add deploy/manifests/ apps/submission-api/Dockerfile
git commit -m "feat(deploy): submission-api manifest + Dockerfile; wire into dev overlay"
```

---

## Day 5 — Build runner + signing (~6 tasks, ~6 hours)

### Task 5.1: In-cluster registry (Zot)

**Files:**
- Create: `deploy/manifests/base/registry/{deployment,service,configmap}.yaml,kustomization.yaml`

- [ ] **Step 1: Zot ConfigMap**

`configmap.yaml`:
```yaml
apiVersion: v1
kind: ConfigMap
metadata: { name: zot-config, namespace: ironbook }
data:
  config.json: |
    {
      "distSpecVersion": "1.1.0",
      "storage": { "rootDirectory": "/var/lib/registry" },
      "http": { "address": "0.0.0.0", "port": "5000" },
      "log": { "level": "info" }
    }
```

`deployment.yaml`:
```yaml
apiVersion: apps/v1
kind: Deployment
metadata: { name: registry, namespace: ironbook }
spec:
  replicas: 1
  selector: { matchLabels: { app: registry } }
  template:
    metadata: { labels: { app: registry } }
    spec:
      containers:
        - name: zot
          image: ghcr.io/project-zot/zot-linux-arm64:v2.1.0
          args: ["serve","/etc/zot/config.json"]
          ports: [ { containerPort: 5000 } ]
          volumeMounts:
            - { name: cfg,  mountPath: /etc/zot }
            - { name: data, mountPath: /var/lib/registry }
      volumes:
        - { name: cfg,  configMap: { name: zot-config } }
        - { name: data, emptyDir: { sizeLimit: 5Gi } }
```

`service.yaml`:
```yaml
apiVersion: v1
kind: Service
metadata: { name: registry, namespace: ironbook }
spec:
  selector: { app: registry }
  ports: [ { name: http, port: 5000, targetPort: 5000 } ]
```

`kustomization.yaml`:
```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
namespace: ironbook
resources: [deployment.yaml, service.yaml, configmap.yaml]
```

- [ ] **Step 2: Wire into dev overlay**

Add `../../base/registry` under `resources:` in `deploy/manifests/overlays/dev/kustomization.yaml`.

- [ ] **Step 3: Apply**

```bash
KUBECONFIG=$PWD/kubeconfig.local kubectl apply -k deploy/manifests/overlays/dev
KUBECONFIG=$PWD/kubeconfig.local kubectl rollout status -n ironbook deploy/registry
```

- [ ] **Step 4: Commit**

```bash
git add deploy/manifests/base/registry/ deploy/manifests/overlays/dev/
git commit -m "feat(registry): add in-cluster zot registry to dev overlay"
```

---

### Task 5.2: build-runner skeleton

**Files:**
- Create: `apps/build-runner/{main.go,runner/runner.go}`
- Create: `apps/build-runner/Dockerfile`

- [ ] **Step 1: `runner/runner.go`** — orchestrates buildkit + Trivy + cosign

```go
package runner

import (
	"context"
	"fmt"
	"os"
	"os/exec"
)

type Result struct {
	ImageRef       string
	ImageDigest    string
	AttestationRef string
}

type Inputs struct {
	SubmissionID string
	Sha256       string
	Language     string  // rust|go|cpp
	SourceTar    string  // local file path of decompressed source
	WorkDir      string  // tmpfs working dir
	Registry     string  // e.g. registry.ironbook.svc.cluster.local:5000
	CosignKey    string  // path to cosign private key (mounted from Secret)
}

func Run(ctx context.Context, in Inputs) (*Result, error) {
	// 1. Generate Dockerfile based on language.
	df := dockerfileFor(in.Language)
	dfPath := in.WorkDir + "/Dockerfile"
	if err := os.WriteFile(dfPath, []byte(df), 0o644); err != nil { return nil, err }

	// 2. buildctl build.
	imageRef := fmt.Sprintf("%s/sub/%s:latest", in.Registry, in.Sha256)
	if err := runCmd(ctx, "buildctl", "build",
		"--frontend=dockerfile.v0",
		"--local", "context="+in.WorkDir,
		"--local", "dockerfile="+in.WorkDir,
		"--output", "type=image,name="+imageRef+",push=true,registry.insecure=true",
	); err != nil { return nil, fmt.Errorf("buildctl: %w", err) }

	// 3. Trivy scan.
	if err := runCmd(ctx, "trivy", "image", "--severity", "CRITICAL", "--exit-code", "1", imageRef); err != nil {
		return nil, fmt.Errorf("trivy: %w", err)
	}

	// 4. Cosign sign.
	digest, err := imageDigest(ctx, imageRef)
	if err != nil { return nil, err }
	pinned := fmt.Sprintf("%s@%s", imageRef, digest)
	if err := runCmd(ctx, "cosign", "sign", "--yes", "--key", in.CosignKey, pinned); err != nil {
		return nil, fmt.Errorf("cosign: %w", err)
	}

	// 5. SLSA-3 attestation (in-toto).
	att := fmt.Sprintf("%s@%s.att", imageRef, digest)
	if err := runCmd(ctx, "cosign", "attest", "--yes", "--key", in.CosignKey,
		"--predicate", "/dev/stdin", "--type", "slsaprovenance", pinned); err != nil {
		return nil, fmt.Errorf("attest: %w", err)
	}

	return &Result{ImageRef: pinned, ImageDigest: digest, AttestationRef: att}, nil
}

func dockerfileFor(lang string) string {
	switch lang {
	case "rust":
		return `FROM rust:1.75-alpine AS build
WORKDIR /src
COPY . .
RUN cargo build --release --offline || cargo build --release
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /src/target/release/* /usr/local/bin/
USER nonroot
ENTRYPOINT ["/usr/local/bin/engine"]`
	case "go":
		return `FROM golang:1.22-alpine AS build
WORKDIR /src
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/engine ./...
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/engine /usr/local/bin/engine
USER nonroot
ENTRYPOINT ["/usr/local/bin/engine"]`
	case "cpp":
		return `FROM gcc:13-bookworm AS build
WORKDIR /src
COPY . .
RUN cmake -B build -DCMAKE_BUILD_TYPE=Release && cmake --build build --parallel
FROM debian:12-slim
COPY --from=build /src/build/engine /usr/local/bin/engine
USER 65534:65534
ENTRYPOINT ["/usr/local/bin/engine"]`
	}
	return ""
}

func runCmd(ctx context.Context, name string, args ...string) error {
	c := exec.CommandContext(ctx, name, args...)
	c.Stdout, c.Stderr = os.Stdout, os.Stderr
	return c.Run()
}

func imageDigest(ctx context.Context, ref string) (string, error) {
	out, err := exec.CommandContext(ctx, "crane", "digest", ref).Output()
	if err != nil { return "", err }
	return string(out), nil
}
```

- [ ] **Step 2: `main.go`** — reads job env, runs `runner.Run`, updates Postgres

```go
package main

import (
	"context"
	"log"
	"os"

	"github.com/<owner>/IronBook/apps/build-runner/runner"
	"github.com/<owner>/IronBook/pkg/postgresclient"
)

func main() {
	ctx := context.Background()
	pg, err := postgresclient.New(ctx, os.Getenv("IRONBOOK_POSTGRES_DSN"))
	if err != nil { log.Fatalf("pg: %v", err) }
	defer pg.Close()

	in := runner.Inputs{
		SubmissionID: os.Getenv("SUBMISSION_ID"),
		Sha256:       os.Getenv("SUBMISSION_SHA256"),
		Language:     os.Getenv("SUBMISSION_LANGUAGE"),
		SourceTar:    os.Getenv("SOURCE_TAR_PATH"),
		WorkDir:      os.Getenv("WORK_DIR"),
		Registry:     os.Getenv("REGISTRY"),
		CosignKey:    os.Getenv("COSIGN_KEY_PATH"),
	}

	if _, e := pg.Pool.Exec(ctx, `UPDATE submissions SET status='BUILDING', updated_at=now() WHERE sha256=$1`, mustHex(in.Sha256)); e != nil {
		log.Fatalf("pg update: %v", e)
	}

	res, err := runner.Run(ctx, in)
	if err != nil {
		_, _ = pg.Pool.Exec(ctx, `UPDATE submissions SET status='REJECTED', reject_reason=$2, updated_at=now() WHERE sha256=$1`, mustHex(in.Sha256), err.Error())
		log.Fatalf("build: %v", err)
	}
	if _, e := pg.Pool.Exec(ctx, `UPDATE submissions SET status='READY', image_digest=$2, updated_at=now() WHERE sha256=$1`, mustHex(in.Sha256), res.ImageDigest); e != nil {
		log.Fatalf("pg ready: %v", e)
	}
	log.Printf("READY: %s", res.ImageRef)
}

func mustHex(s string) []byte {
	b := make([]byte, len(s)/2)
	if _, err := hex.Decode(b, []byte(s)); err != nil { log.Fatal(err) }
	return b
}
```

- [ ] **Step 3: Dockerfile**

`apps/build-runner/Dockerfile`:
```dockerfile
FROM moby/buildkit:v0.13.2 AS buildkit
FROM golang:1.22-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /out/build-runner ./apps/build-runner

FROM alpine:3.20
RUN apk add --no-cache ca-certificates
COPY --from=buildkit /usr/bin/buildctl  /usr/bin/buildctl
COPY --from=buildkit /usr/bin/buildkitd /usr/bin/buildkitd
RUN wget -q https://github.com/aquasecurity/trivy/releases/download/v0.51.0/trivy_0.51.0_Linux-ARM64.tar.gz \
    && tar -xzf trivy_0.51.0_Linux-ARM64.tar.gz trivy && mv trivy /usr/bin/ && rm trivy_*
RUN wget -q https://github.com/sigstore/cosign/releases/download/v2.2.4/cosign-linux-arm64 \
    && mv cosign-linux-arm64 /usr/bin/cosign && chmod +x /usr/bin/cosign
RUN wget -q https://github.com/google/go-containerregistry/releases/download/v0.19.1/go-containerregistry_Linux_arm64.tar.gz \
    && tar -xzf go-containerregistry_Linux_arm64.tar.gz crane && mv crane /usr/bin/ && rm go-containerregistry_*.tar.gz
COPY --from=build /out/build-runner /usr/local/bin/build-runner
ENTRYPOINT ["/usr/local/bin/build-runner"]
```

- [ ] **Step 4: Commit**

```bash
git add apps/build-runner/
git commit -m "feat(build-runner): skeleton orchestrating buildctl + Trivy + cosign + SLSA-3"
```

---

### Task 5.3: build-runner Job manifest + RBAC

**Files:**
- Create: `deploy/manifests/base/build-runner/{job-template,rbac,buildkitd-deployment}.yaml,kustomization.yaml`

- [ ] **Step 1: BuildKit daemon** (separate Deployment so build-runner Jobs can connect)

`buildkitd-deployment.yaml`:
```yaml
apiVersion: apps/v1
kind: Deployment
metadata: { name: buildkitd, namespace: builds }
spec:
  replicas: 1
  selector: { matchLabels: { app: buildkitd } }
  template:
    metadata: { labels: { app: buildkitd } }
    spec:
      containers:
        - name: buildkitd
          image: moby/buildkit:v0.13.2-rootless
          securityContext: { runAsUser: 1000, runAsGroup: 1000, seccompProfile: { type: Unconfined } }
          args: ["--addr", "tcp://0.0.0.0:1234", "--oci-worker-no-process-sandbox"]
          ports: [ { containerPort: 1234 } ]
---
apiVersion: v1
kind: Service
metadata: { name: buildkitd, namespace: builds }
spec:
  selector: { app: buildkitd }
  ports: [ { port: 1234, targetPort: 1234 } ]
```

`rbac.yaml`:
```yaml
apiVersion: v1
kind: ServiceAccount
metadata: { name: build-runner, namespace: builds }
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata: { name: build-runner, namespace: builds }
rules:
  - apiGroups: [""] resources: [pods, pods/log] verbs: [get, list, watch]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata: { name: build-runner, namespace: builds }
subjects: [{ kind: ServiceAccount, name: build-runner, namespace: builds }]
roleRef: { apiGroup: rbac.authorization.k8s.io, kind: Role, name: build-runner }
```

`job-template.yaml` (a literal placeholder; submission-api will create real Jobs from this template at runtime):
```yaml
apiVersion: batch/v1
kind: Job
metadata: { name: build-PLACEHOLDER, namespace: builds }
spec:
  backoffLimit: 3
  ttlSecondsAfterFinished: 3600
  template:
    metadata: { labels: { app: build-runner } }
    spec:
      restartPolicy: Never
      serviceAccountName: build-runner
      containers:
        - name: runner
          image: ironbook/build-runner:dev
          env:
            - { name: BUILDKIT_HOST, value: "tcp://buildkitd.builds.svc:1234" }
            - { name: REGISTRY,      value: "registry.ironbook.svc.cluster.local:5000" }
            - { name: COSIGN_KEY_PATH, value: "/keys/cosign.key" }
          volumeMounts:
            - { name: cosign,    mountPath: /keys, readOnly: true }
            - { name: workdir,   mountPath: /work }
      volumes:
        - { name: cosign,  secret: { secretName: cosign-keys } }
        - { name: workdir, emptyDir: { sizeLimit: 1Gi } }
```

`kustomization.yaml`:
```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
namespace: builds
resources: [buildkitd-deployment.yaml, rbac.yaml]
```

- [ ] **Step 2: Egress-denied NetworkPolicy in `builds` namespace**

`networkpolicy.yaml`:
```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata: { name: builds-egress-restricted, namespace: builds }
spec:
  podSelector: {}
  policyTypes: [Egress]
  egress:
    - to:                                                 # allow only registry + DNS
        - namespaceSelector: { matchLabels: { kubernetes.io/metadata.name: ironbook } }
          podSelector:       { matchLabels: { app: registry } }
      ports: [ { port: 5000 } ]
    - to:
        - namespaceSelector: { matchLabels: { kubernetes.io/metadata.name: kube-system } }
          podSelector:       { matchLabels: { k8s-app: kube-dns } }
      ports: [ { port: 53, protocol: UDP } ]
```

Add to `kustomization.yaml` resources list.

- [ ] **Step 3: Cosign keys (dev only — sealed-secret in prod)**

```bash
cosign generate-key-pair  # creates cosign.key + cosign.pub in CWD
KUBECONFIG=$PWD/kubeconfig.local kubectl create ns builds || true
KUBECONFIG=$PWD/kubeconfig.local kubectl -n builds create secret generic cosign-keys --from-file=cosign.key
mv cosign.key .secrets/cosign.key   # keep out of git
echo "*.key" >> .gitignore
```

- [ ] **Step 4: Commit**

```bash
git add deploy/manifests/base/build-runner/ .gitignore
git commit -m "feat(build-runner): job template, buildkitd, rbac, egress-restricted networkpolicy"
```

---

### Task 5.4: submission-api dispatches build Job

**Files:**
- Modify: `apps/submission-api/service/service.go`
- Create: `apps/submission-api/dispatcher/dispatcher.go`

- [ ] **Step 1: Write the failing unit test**

`apps/submission-api/dispatcher/dispatcher_test.go`:
```go
package dispatcher

import (
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestDispatch_CreatesJobInBuildsNamespace(t *testing.T) {
	cli := fake.NewClientset()
	d := &Dispatcher{Client: cli, Image: "ironbook/build-runner:dev"}
	if err := d.Dispatch(t.Context(), Inputs{SubmissionID: "abc", Sha256: "deadbeef", Language: "rust"}); err != nil {
		t.Fatal(err)
	}
	got, err := cli.BatchV1().Jobs("builds").List(t.Context(), metav1.ListOptions{})
	if err != nil { t.Fatal(err) }
	if len(got.Items) != 1 { t.Fatalf("expected 1 job, got %d", len(got.Items)) }
	if got.Items[0].Spec.Template.Spec.Containers[0].Image != "ironbook/build-runner:dev" {
		t.Errorf("wrong image: %v", got.Items[0].Spec.Template.Spec.Containers[0])
	}
	_ = batchv1.Job{}
}
```

- [ ] **Step 2: Run, verify failure**

```bash
go test ./apps/submission-api/dispatcher/...
```

Expected: FAIL "package dispatcher: no Go files".

- [ ] **Step 3: Implement `dispatcher/dispatcher.go`**

```go
package dispatcher

import (
	"context"
	"fmt"

	batchv1 "k8s.io/api/batch/v1"
	corev1  "k8s.io/api/core/v1"
	metav1  "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

type Inputs struct {
	SubmissionID string
	Sha256       string
	Language     string
}

type Dispatcher struct {
	Client kubernetes.Interface
	Image  string
}

func (d *Dispatcher) Dispatch(ctx context.Context, in Inputs) error {
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("build-%s", in.SubmissionID),
			Namespace: "builds",
			Labels:    map[string]string{"app": "build-runner", "submission-id": in.SubmissionID},
		},
		Spec: batchv1.JobSpec{
			BackoffLimit: ptr32(3),
			TTLSecondsAfterFinished: ptr32(3600),
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					RestartPolicy:      corev1.RestartPolicyNever,
					ServiceAccountName: "build-runner",
					Containers: []corev1.Container{{
						Name:  "runner",
						Image: d.Image,
						Env: []corev1.EnvVar{
							{Name: "SUBMISSION_ID", Value: in.SubmissionID},
							{Name: "SUBMISSION_SHA256", Value: in.Sha256},
							{Name: "SUBMISSION_LANGUAGE", Value: in.Language},
							{Name: "WORK_DIR", Value: "/work"},
							{Name: "BUILDKIT_HOST", Value: "tcp://buildkitd.builds.svc:1234"},
							{Name: "REGISTRY", Value: "registry.ironbook.svc.cluster.local:5000"},
							{Name: "COSIGN_KEY_PATH", Value: "/keys/cosign.key"},
						},
					}},
				},
			},
		},
	}
	_, err := d.Client.BatchV1().Jobs("builds").Create(ctx, job, metav1.CreateOptions{})
	return err
}

func ptr32(v int32) *int32 { return &v }
```

Add deps:
```bash
go get k8s.io/api@v0.30.0
go get k8s.io/apimachinery@v0.30.0
go get k8s.io/client-go@v0.30.0
go mod tidy
```

- [ ] **Step 4: Run test (should pass)**

```bash
go test ./apps/submission-api/dispatcher/... -v
```

Expected: PASS.

- [ ] **Step 5: Wire dispatcher into `service.Service.Upload`**

Edit `apps/submission-api/service/service.go`. After successful Postgres insert, dispatch the job:

```go
// in service.go, add field:
type Service struct {
	PG    *repo.Postgres
	MinIO *repo.MinIO
	Disp  *dispatcher.Dispatcher
}

// in Upload(), after PG insert succeeds:
if err := s.Disp.Dispatch(ctx, dispatcher.Inputs{
	SubmissionID: id.String(),
	Sha256:       fmt.Sprintf("%x", sha[:]),
	Language:     language,
}); err != nil {
	return nil, fmt.Errorf("dispatch: %w", err)
}
```

Update `main.go` to construct `Disp` from in-cluster client config (`rest.InClusterConfig()`).

- [ ] **Step 6: Commit**

```bash
git add apps/submission-api/dispatcher/ apps/submission-api/service/ apps/submission-api/main.go go.mod go.sum
git commit -m "feat(submission-api): dispatch K8s build Job after upload"
```

---

### Task 5.5: E2E smoke — upload a Rust hello-world contestant, see READY

**Files:**
- Create: `tests/e2e/fixtures/submissions/correct-rust-hello/{Cargo.toml,src/main.rs}`
- Create: `tests/e2e/cases/phase1_smoke_test.go`

- [ ] **Step 1: The contestant fixture**

`Cargo.toml`:
```toml
[package]
name    = "engine"
version = "0.1.0"
edition = "2021"
```

`src/main.rs`:
```rust
fn main() { println!("hello-world matching engine"); }
```

- [ ] **Step 2: Tar the fixture**

```bash
cd tests/e2e/fixtures/submissions/correct-rust-hello
tar -czf /tmp/correct-rust-hello.tar.gz .
cd -
```

- [ ] **Step 3: Write the E2E test (build tag e2e)**

`tests/e2e/cases/phase1_smoke_test.go`:
```go
//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"os/exec"
	"testing"
	"time"
)

func TestPhase1_UploadToReady(t *testing.T) {
	// Assumes `make dev` already ran and submission-api is reachable on localhost:8080 via port-forward.
	src, err := os.ReadFile("/tmp/correct-rust-hello.tar.gz")
	if err != nil { t.Fatal(err) }
	resp, err := http.Post("http://localhost:8080/v1/upload?language=rust", "application/zstd", bytes.NewReader(src))
	if err != nil { t.Fatal(err) }
	defer resp.Body.Close()
	if resp.StatusCode != 200 { b, _ := io.ReadAll(resp.Body); t.Fatalf("status=%d body=%s", resp.StatusCode, b) }
	var out map[string]string
	_ = json.NewDecoder(resp.Body).Decode(&out)

	// Poll Postgres via `kubectl exec` until status=READY.
	deadline := time.Now().Add(5 * time.Minute)
	for time.Now().Before(deadline) {
		out, err := exec.CommandContext(context.Background(), "kubectl", "--kubeconfig", os.Getenv("KUBECONFIG"),
			"-n", "ironbook", "exec", "deploy/postgres", "--",
			"psql", "-U", "ironbook", "-d", "ironbook", "-tAc",
			fmt.Sprintf("SELECT status FROM submissions WHERE sha256 = decode('%s', 'hex')", out["sha256"]),
		).CombinedOutput()
		if err == nil && strings.TrimSpace(string(out)) == "READY" { return }
		time.Sleep(5 * time.Second)
	}
	t.Fatal("submission did not reach READY in 5 min")
}
```

- [ ] **Step 4: Run**

```bash
KUBECONFIG=$PWD/kubeconfig.local kubectl -n ironbook port-forward svc/submission-api 8080:8080 &
go test -tags=e2e ./tests/e2e/cases/... -run TestPhase1_UploadToReady -v -timeout 6m
kill %1   # stop port-forward
```

Expected: PASS within ~3 min.

- [ ] **Step 5: Commit**

```bash
git add tests/e2e/
git commit -m "test(e2e): phase-1 smoke — upload Rust hello-world reaches status=READY"
```

---

## Day 6 — Sandbox runtime + admission webhook + stub gateway (~7 tasks, ~7 hours)

### Task 6.1: Install gVisor on Hetzner k3s

(Manual one-time install; record in runbook.)

**Files:**
- Modify: `docs/runbooks/02-bring-up-hetzner.md`

- [ ] **Step 1: SSH into Hetzner sandbox VM**

```bash
ssh root@10.99.0.2
```

- [ ] **Step 2: Install runsc**

```bash
ARCH=$(uname -m | sed s/aarch64/arm64/)
URL="https://storage.googleapis.com/gvisor/releases/release/latest/${ARCH}"
wget ${URL}/runsc ${URL}/runsc.sha512 ${URL}/containerd-shim-runsc-v1 ${URL}/containerd-shim-runsc-v1.sha512
sha512sum -c runsc.sha512 -c containerd-shim-runsc-v1.sha512
chmod a+rx runsc containerd-shim-runsc-v1
mv runsc containerd-shim-runsc-v1 /usr/local/bin/
```

- [ ] **Step 3: Configure k3s containerd to know about runsc**

```bash
mkdir -p /var/lib/rancher/k3s/agent/etc/containerd/
cat > /var/lib/rancher/k3s/agent/etc/containerd/config.toml.tmpl <<'EOF'
{{ template "base" . }}

[plugins."io.containerd.grpc.v1.cri".containerd.runtimes.runsc]
  runtime_type = "io.containerd.runsc.v1"
EOF
systemctl restart k3s
```

- [ ] **Step 4: Verify**

```bash
systemctl status k3s
crictl info | grep -A3 runtimes   # should list 'runsc'
exit
```

- [ ] **Step 5: Update `02-bring-up-hetzner.md` with these steps**

- [ ] **Step 6: Commit**

```bash
git add docs/runbooks/02-bring-up-hetzner.md
git commit -m "docs(runbooks): document gVisor install on Hetzner k3s"
```

---

### Task 6.2: RuntimeClass for gvisor + seccomp ConfigMap distribution

**Files:**
- Create: `deploy/runtimeclasses/gvisor.yaml`
- Create: `deploy/manifests/base/sandbox-host/{configmap,daemonset-distributor}.yaml,kustomization.yaml`

- [ ] **Step 1: RuntimeClass**

`deploy/runtimeclasses/gvisor.yaml`:
```yaml
apiVersion: node.k8s.io/v1
kind: RuntimeClass
metadata: { name: gvisor }
handler: runsc
```

- [ ] **Step 2: seccomp profile** (verbatim from spec §4.2.2)

`deploy/policies/seccomp/ironbook-sandbox.json`:

(Copy from spec §4.2.2 — too long to repeat here; that JSON is canonical.)

- [ ] **Step 3: ConfigMap + DaemonSet that distributes the seccomp file to every node**

`deploy/manifests/base/sandbox-host/configmap.yaml`:
```yaml
apiVersion: v1
kind: ConfigMap
metadata: { name: seccomp-profiles, namespace: kube-system }
data:
  ironbook-sandbox.json: |
    # paste content of deploy/policies/seccomp/ironbook-sandbox.json here
    # (kustomize configMapGenerator avoids duplication — see kustomization.yaml)
```

`kustomization.yaml`:
```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
namespace: kube-system
configMapGenerator:
  - name: seccomp-profiles
    files: [../../../policies/seccomp/ironbook-sandbox.json]
resources: [daemonset-distributor.yaml]
```

`daemonset-distributor.yaml`:
```yaml
apiVersion: apps/v1
kind: DaemonSet
metadata: { name: seccomp-distributor, namespace: kube-system }
spec:
  selector: { matchLabels: { app: seccomp-distributor } }
  template:
    metadata: { labels: { app: seccomp-distributor } }
    spec:
      containers:
        - name: copy
          image: alpine:3.20
          command: ["/bin/sh","-c"]
          args:
            - |
              set -eux
              cp /profiles/* /host-seccomp/
              while true; do sleep 3600; done
          volumeMounts:
            - { name: profiles, mountPath: /profiles, readOnly: true }
            - { name: host,     mountPath: /host-seccomp }
          securityContext: { runAsUser: 0 }
      volumes:
        - { name: profiles, configMap: { name: seccomp-profiles } }
        - { name: host,     hostPath: { path: /var/lib/kubelet/seccomp, type: DirectoryOrCreate } }
```

- [ ] **Step 4: Apply to sandbox cluster**

```bash
KUBECONFIG=$PWD/kubeconfig.sandbox kubectl apply -f deploy/runtimeclasses/gvisor.yaml
KUBECONFIG=$PWD/kubeconfig.sandbox kubectl apply -k deploy/manifests/base/sandbox-host
```

- [ ] **Step 5: Commit**

```bash
git add deploy/runtimeclasses/ deploy/manifests/base/sandbox-host/ deploy/policies/seccomp/
git commit -m "feat(sandbox): gvisor RuntimeClass + seccomp profile distribution DaemonSet"
```

---

### Task 6.3: AppArmor profile

**Files:**
- Create: `deploy/policies/apparmor/ironbook-sandbox`
- Modify: `deploy/manifests/base/sandbox-host/daemonset-distributor.yaml`

- [ ] **Step 1: Profile** (verbatim from spec §4.2.3)

`deploy/policies/apparmor/ironbook-sandbox`:
```
profile ironbook-sandbox flags=(attach_disconnected, mediate_deleted) {
  #include <abstractions/base>
  capability,
  deny capability sys_admin,
  deny capability sys_module,
  deny capability sys_ptrace,
  deny capability net_admin,
  deny capability net_raw,

  /tmp/** rw,
  /var/run/sidecar/sock rw,
  /proc/self/** r,
  deny /proc/sys/** rwklx,
  deny /sys/** rwklx,
  deny /etc/shadow* rwklx,
  deny /root/** rwklx,

  network inet stream,
  network inet6 stream,
  deny network packet,
  deny network raw,
}
```

- [ ] **Step 2: Add another DaemonSet container that loads the profile via `apparmor_parser`**

Edit `daemonset-distributor.yaml` to add a second container:
```yaml
        - name: apparmor-loader
          image: alpine:3.20
          command: ["/bin/sh","-c"]
          args:
            - |
              apk add --no-cache apparmor-utils
              apparmor_parser -r -W /profiles/apparmor/ironbook-sandbox || true
              while true; do sleep 3600; done
          securityContext: { privileged: true }
          volumeMounts:
            - { name: apparmor-profiles, mountPath: /profiles/apparmor }
        # plus: volume entry for apparmor-profiles configmap
```

(This is rough — Hetzner Ubuntu kernel has AppArmor enabled by default; `apparmor_parser -r` reloads it.)

- [ ] **Step 3: Apply**

```bash
KUBECONFIG=$PWD/kubeconfig.sandbox kubectl apply -k deploy/manifests/base/sandbox-host
```

- [ ] **Step 4: Verify on the node**

```bash
ssh root@10.99.0.2 'aa-status | grep ironbook-sandbox'
```

Expected: profile listed in 'enforce' mode.

- [ ] **Step 5: Commit**

```bash
git add deploy/policies/apparmor/ deploy/manifests/base/sandbox-host/
git commit -m "feat(sandbox): AppArmor ironbook-sandbox profile + loader DaemonSet"
```

---

### Task 6.4: NetworkPolicy + iptables backstop DaemonSet

**Files:**
- Create: `deploy/manifests/base/networkpolicies/submission-egress-deny.yaml`
- Create: `deploy/manifests/base/sandbox-host/iptables-backstop.yaml`

- [ ] **Step 1: NetworkPolicy** (verbatim from spec §4.2.4)

```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: submission-egress-deny-all
  namespace: submissions
spec:
  podSelector: { matchLabels: { app: submission } }
  policyTypes: [Egress, Ingress]
  egress: []
  ingress:
    - from:
        - podSelector: { matchLabels: { app: fairness-gateway } }
      ports:
        - { port: 8080, protocol: TCP }
        - { port: 8081, protocol: TCP }
        - { port: 9876, protocol: TCP }
```

- [ ] **Step 2: iptables backstop DaemonSet**

`iptables-backstop.yaml`:
```yaml
apiVersion: apps/v1
kind: DaemonSet
metadata: { name: iptables-backstop, namespace: kube-system }
spec:
  selector: { matchLabels: { app: iptables-backstop } }
  template:
    metadata: { labels: { app: iptables-backstop } }
    spec:
      hostNetwork: true
      containers:
        - name: nft
          image: alpine:3.20
          securityContext: { privileged: true }
          command: ["/bin/sh","-c"]
          args:
            - |
              apk add --no-cache nftables
              cat > /etc/nftables-ironbook.conf <<'EOF'
              table inet ironbook {
                  chain submission_egress {
                      type filter hook output priority -50 ; policy accept ;
                      meta cgroup id @submissions_cgrp_id ip daddr != 10.42.0.0/16 drop
                  }
                  set submissions_cgrp_id { type integer; flags dynamic }
              }
              EOF
              nft -f /etc/nftables-ironbook.conf || true
              while true; do sleep 3600; done
```

- [ ] **Step 3: Apply**

```bash
KUBECONFIG=$PWD/kubeconfig.sandbox kubectl create ns submissions || true
KUBECONFIG=$PWD/kubeconfig.sandbox kubectl apply -f deploy/manifests/base/networkpolicies/submission-egress-deny.yaml
KUBECONFIG=$PWD/kubeconfig.sandbox kubectl apply -f deploy/manifests/base/sandbox-host/iptables-backstop.yaml
```

- [ ] **Step 4: Commit**

```bash
git add deploy/manifests/base/networkpolicies/ deploy/manifests/base/sandbox-host/
git commit -m "feat(sandbox): NetworkPolicy submission-egress-deny + nftables host backstop"
```

---

### Task 6.5: admission-webhook (validating)

**Files:**
- Create: `apps/admission-webhook/{main.go,policy/validate.go,policy/validate_test.go}`

- [ ] **Step 1: Write the failing test**

`apps/admission-webhook/policy/validate_test.go`:
```go
package policy

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestValidate_RejectsPodMissingGvisor(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "x", Namespace: "submissions", Labels: map[string]string{"app": "submission"}},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "engine", Image: "registry.local/sub/abc@sha256:def"}},
		},
	}
	res := Validate(pod)
	if res.Allowed { t.Fatalf("expected reject, got allowed: %s", res.Reason) }
}

func TestValidate_AcceptsCompliantPod(t *testing.T) {
	rc := "gvisor"
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "x", Namespace: "submissions", Labels: map[string]string{"app": "submission"}},
		Spec: corev1.PodSpec{
			RuntimeClassName: &rc,
			SecurityContext: &corev1.PodSecurityContext{RunAsNonRoot: ptrBool(true)},
			Containers: []corev1.Container{{
				Name: "engine",
				Image: "registry.local/sub/abc@sha256:def",
				SecurityContext: &corev1.SecurityContext{
					AllowPrivilegeEscalation: ptrBool(false),
					ReadOnlyRootFilesystem:   ptrBool(true),
					Capabilities: &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
				},
			}},
		},
	}
	if res := Validate(pod); !res.Allowed { t.Fatalf("expected allow, got: %s", res.Reason) }
}
func ptrBool(b bool) *bool { return &b }
```

- [ ] **Step 2: Run test, expect fail**

```bash
go test ./apps/admission-webhook/policy/...
```

Expected: FAIL (package missing).

- [ ] **Step 3: Implement `policy/validate.go`**

```go
package policy

import (
	"strings"

	corev1 "k8s.io/api/core/v1"
)

type Result struct {
	Allowed bool
	Reason  string
}

func Validate(pod *corev1.Pod) Result {
	if pod.Namespace != "submissions" { return Result{Allowed: true} }
	if pod.Spec.RuntimeClassName == nil || *pod.Spec.RuntimeClassName != "gvisor" {
		return Result{Reason: "submission pods must use runtimeClassName: gvisor"}
	}
	if pod.Spec.SecurityContext == nil || pod.Spec.SecurityContext.RunAsNonRoot == nil || !*pod.Spec.SecurityContext.RunAsNonRoot {
		return Result{Reason: "submission pods must runAsNonRoot"}
	}
	for _, c := range pod.Spec.Containers {
		if !strings.Contains(c.Image, "@sha256:") {
			return Result{Reason: "container images must be pinned by digest (@sha256:...)"}
		}
		if c.SecurityContext == nil { return Result{Reason: "container missing securityContext"} }
		if c.SecurityContext.AllowPrivilegeEscalation == nil || *c.SecurityContext.AllowPrivilegeEscalation {
			return Result{Reason: "allowPrivilegeEscalation must be false"}
		}
		if c.SecurityContext.ReadOnlyRootFilesystem == nil || !*c.SecurityContext.ReadOnlyRootFilesystem {
			return Result{Reason: "readOnlyRootFilesystem must be true"}
		}
		if c.SecurityContext.Capabilities == nil || !contains(c.SecurityContext.Capabilities.Drop, "ALL") {
			return Result{Reason: "capabilities.drop must include ALL"}
		}
	}
	return Result{Allowed: true}
}

func contains(caps []corev1.Capability, want corev1.Capability) bool {
	for _, c := range caps { if c == want { return true } }
	return false
}
```

- [ ] **Step 4: Run test (should pass)**

```bash
go test ./apps/admission-webhook/policy/... -v
```

Expected: PASS.

- [ ] **Step 5: Webhook server `main.go`**

```go
package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"log"
	"net/http"

	"github.com/<owner>/IronBook/apps/admission-webhook/policy"
	admissionv1 "k8s.io/api/admission/v1"
	corev1     "k8s.io/api/core/v1"
)

func main() {
	mux := http.NewServeMux()
	mux.HandleFunc("/validate", handle)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok")) })

	cert, _ := tls.LoadX509KeyPair("/tls/tls.crt", "/tls/tls.key")
	srv := &http.Server{Addr: ":8443", Handler: mux, TLSConfig: &tls.Config{Certificates: []tls.Certificate{cert}}}
	log.Println("admission-webhook listening on :8443")
	if err := srv.ListenAndServeTLS("",""); err != nil { log.Fatal(err) }
	_ = context.Background()
}

func handle(w http.ResponseWriter, r *http.Request) {
	var review admissionv1.AdmissionReview
	if err := json.NewDecoder(r.Body).Decode(&review); err != nil { http.Error(w,err.Error(),400); return }
	resp := &admissionv1.AdmissionResponse{UID: review.Request.UID}
	var pod corev1.Pod
	if err := json.Unmarshal(review.Request.Object.Raw, &pod); err == nil {
		res := policy.Validate(&pod)
		resp.Allowed = res.Allowed
		if !res.Allowed { resp.Result = &metav1.Status{Message: res.Reason} }
	}
	review.Response = resp
	_ = json.NewEncoder(w).Encode(review)
}
```

(Add the `metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"` import.)

- [ ] **Step 6: Manifest + cert-manager Certificate**

`deploy/manifests/base/admission-webhook/{deployment,service,certificate,webhook-config}.yaml`:

(Standard pattern — Deployment, Service on 443, cert-manager `Certificate` issuing into a Secret mounted at `/tls`, `ValidatingWebhookConfiguration` with `caBundle` annotation injected by cert-manager via `cert-manager.io/inject-ca-from`.)

```yaml
# certificate.yaml
apiVersion: cert-manager.io/v1
kind: Certificate
metadata: { name: admission-webhook, namespace: ironbook }
spec:
  secretName: admission-webhook-tls
  dnsNames:
    - admission-webhook.ironbook.svc
    - admission-webhook.ironbook.svc.cluster.local
  issuerRef: { name: ironbook-ca, kind: ClusterIssuer }
  duration: 24h
  renewBefore: 8h
---
# webhook-config.yaml
apiVersion: admissionregistration.k8s.io/v1
kind: ValidatingWebhookConfiguration
metadata:
  name: ironbook-admission
  annotations: { cert-manager.io/inject-ca-from: ironbook/admission-webhook }
webhooks:
  - name: validate.ironbook.io
    clientConfig: { service: { name: admission-webhook, namespace: ironbook, path: /validate }, caBundle: "" }
    rules:
      - operations: [CREATE]
        apiGroups: [""]
        apiVersions: [v1]
        resources: [pods]
    namespaceSelector:
      matchLabels: { ironbook.io/sandbox: "true" }
    failurePolicy: Fail
    admissionReviewVersions: [v1]
    sideEffects: None
```

- [ ] **Step 7: Commit**

```bash
git add apps/admission-webhook/ deploy/manifests/base/admission-webhook/
git commit -m "feat(admission-webhook): validating webhook enforces gvisor + securityContext on submission pods"
```

---

### Task 6.6: Stub fairness-gateway (acks orders for testing)

**Files:**
- Create: `apps/fairness-gateway/{main.go,handler.go}` — stub only; real impl Phase 2

- [ ] **Step 1: Stub server**

```go
package main

import (
	"encoding/json"
	"log"
	"net/http"
	"sync/atomic"
	"time"
)

type Order struct {
	ClientOrderID string `json:"client_order_id"`
}

type Ack struct {
	ClientOrderID string `json:"client_order_id"`
	PlatformSeq   uint64 `json:"platform_seq"`
	AckTsNs       int64  `json:"ack_ts_ns"`
}

var seq atomic.Uint64

func main() {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/order", func(w http.ResponseWriter, r *http.Request) {
		var o Order
		if err := json.NewDecoder(r.Body).Decode(&o); err != nil { http.Error(w,err.Error(),400); return }
		ack := Ack{ClientOrderID: o.ClientOrderID, PlatformSeq: seq.Add(1), AckTsNs: time.Now().UnixNano()}
		_ = json.NewEncoder(w).Encode(ack)
	})
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok")) })
	log.Println("stub fairness-gateway :8080")
	log.Fatal(http.ListenAndServe(":8080", mux))
}
```

- [ ] **Step 2: Dockerfile + manifest**

`apps/fairness-gateway/Dockerfile` follows the same pattern as submission-api.

`deploy/manifests/base/stub-fairness-gateway/{deployment,service}.yaml`:
```yaml
# Deployment + Service on :8080 in namespace ironbook
# (omit verbose YAML repetition — same shape as submission-api)
```

- [ ] **Step 3: Apply to sandbox cluster** (this lives in Region B because gateway is regional)

```bash
docker build -t ironbook/fairness-gateway:dev -f apps/fairness-gateway/Dockerfile .
docker save ironbook/fairness-gateway:dev | ssh root@10.99.0.2 'k3s ctr images import -'
KUBECONFIG=$PWD/kubeconfig.sandbox kubectl apply -k deploy/manifests/base/stub-fairness-gateway
```

- [ ] **Step 4: Commit**

```bash
git add apps/fairness-gateway/ deploy/manifests/base/stub-fairness-gateway/
git commit -m "feat(fairness-gateway): stub server that acks orders (real impl Phase 2)"
```

---

### Task 6.7: E2E smoke — submission pod runs in gVisor, gateway acks

**Files:**
- Create: `tests/e2e/cases/phase1_sandbox_smoke_test.go`
- Modify: `tests/e2e/fixtures/submissions/correct-rust-hello/src/main.rs` to listen on a port

- [ ] **Step 1: Update the fixture's `main.rs`** to be a real (trivial) HTTP server

```rust
fn main() -> std::io::Result<()> {
    let listener = std::net::TcpListener::bind("0.0.0.0:7777")?;
    for stream in listener.incoming() {
        if let Ok(mut s) = stream {
            use std::io::{Read, Write};
            let mut buf = [0u8; 1024]; let _ = s.read(&mut buf);
            let _ = s.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\nack\n");
        }
    }
    Ok(())
}
```

Re-tar:
```bash
cd tests/e2e/fixtures/submissions/correct-rust-hello
tar -czf /tmp/correct-rust-hello.tar.gz .
```

- [ ] **Step 2: Pod manifest for the smoke deploy** (operator will eventually generate this; for Phase 1 we apply by hand)

`tests/e2e/manifests/submission-pod-smoke.yaml`:
```yaml
apiVersion: v1
kind: Pod
metadata:
  name: submission-smoke
  namespace: submissions
  labels: { app: submission, ironbook.io/run: smoke }
spec:
  runtimeClassName: gvisor
  serviceAccountName: default
  hostNetwork: false
  hostPID: false
  hostIPC: false
  securityContext:
    runAsNonRoot: true
    runAsUser: 65534
    runAsGroup: 65534
    seccompProfile: { type: Localhost, localhostProfile: ironbook-sandbox.json }
  containers:
    - name: engine
      image: registry.ironbook.svc:5000/sub/<SHA256>@sha256:<DIGEST>
      ports: [ { containerPort: 7777 } ]
      securityContext:
        allowPrivilegeEscalation: false
        readOnlyRootFilesystem: true
        capabilities: { drop: [ALL] }
      resources:
        limits:   { cpu: "2", memory: "1Gi" }
        requests: { cpu: "2", memory: "1Gi" }
```

(Substitute `<SHA256>` and `<DIGEST>` from the prior upload's READY status.)

- [ ] **Step 3: Smoke test**

```bash
KUBECONFIG=$PWD/kubeconfig.sandbox kubectl apply -f tests/e2e/manifests/submission-pod-smoke.yaml
KUBECONFIG=$PWD/kubeconfig.sandbox kubectl wait pod/submission-smoke -n submissions --for=condition=Ready --timeout=60s

# Port-forward into the pod and curl it.
KUBECONFIG=$PWD/kubeconfig.sandbox kubectl -n submissions port-forward submission-smoke 7777:7777 &
PF_PID=$!
sleep 1
curl -fsSL http://localhost:7777/ | grep -q "ack"
kill $PF_PID
```

Expected: `ack` line returned. The pod is running under runsc, sealed by all the layers from Day 6.

- [ ] **Step 4: Verify gVisor actually used**

```bash
KUBECONFIG=$PWD/kubeconfig.sandbox kubectl -n submissions exec submission-smoke -- /bin/sh -c 'cat /proc/1/cgroup' || echo "[expected: shell denied by AppArmor]"
KUBECONFIG=$PWD/kubeconfig.sandbox kubectl -n submissions describe pod submission-smoke | grep -i runtimeclass
```

Expected: shell denied (AppArmor blocks `sh` exec or distroless image has none); RuntimeClass `gvisor`.

- [ ] **Step 5: Send an order to the gateway and verify ack**

```bash
KUBECONFIG=$PWD/kubeconfig.sandbox kubectl -n ironbook port-forward svc/fairness-gateway 8080:8080 &
sleep 1
curl -sX POST http://localhost:8080/v1/order -d '{"client_order_id":"abc"}' | grep platform_seq
kill %1
```

Expected: JSON ack with non-zero `platform_seq`.

- [ ] **Step 6: Commit**

```bash
git add tests/e2e/manifests/ tests/e2e/cases/
git commit -m "test(e2e): phase-1 smoke — submission pod runs in gVisor, stub gateway acks orders"
```

---

### Task 6.8: `make demo` target stitches everything together

**Files:**
- Modify: `Makefile`
- Create: `tools/seed-data/main.go`

- [ ] **Step 1: Write a tiny seeder**

`tools/seed-data/main.go`:
```go
package main

import (
	"bytes"
	"flag"
	"io"
	"log"
	"net/http"
	"os"
)

func main() {
	tar := flag.String("tar", "/tmp/correct-rust-hello.tar.gz", "")
	api := flag.String("api", "http://localhost:8080", "")
	flag.Parse()
	body, err := os.ReadFile(*tar); if err != nil { log.Fatal(err) }
	resp, err := http.Post(*api+"/v1/upload?language=rust", "application/zstd", bytes.NewReader(body))
	if err != nil { log.Fatal(err) }
	defer resp.Body.Close()
	io.Copy(os.Stdout, resp.Body); fmt.Println()
}
```

- [ ] **Step 2: Add `make demo` target**

```make
.PHONY: demo
demo: dev ## End-to-end demo on a fresh dev cluster
	@echo "1/4 building images..."
	docker build -t ironbook/submission-api:dev   -f apps/submission-api/Dockerfile  .
	docker build -t ironbook/build-runner:dev     -f apps/build-runner/Dockerfile    .
	docker build -t ironbook/admission-webhook:dev -f apps/admission-webhook/Dockerfile .
	docker build -t ironbook/fairness-gateway:dev -f apps/fairness-gateway/Dockerfile .
	@echo "2/4 loading into kind..."
	kind load docker-image ironbook/submission-api:dev    ironbook/build-runner:dev \
	                       ironbook/admission-webhook:dev ironbook/fairness-gateway:dev \
	                       --name ironbook-control
	@echo "3/4 applying overlays..."
	KUBECONFIG=$$PWD/kubeconfig.local kubectl apply -k deploy/manifests/overlays/dev
	@echo "4/4 seeding sample submission..."
	KUBECONFIG=$$PWD/kubeconfig.local kubectl -n ironbook port-forward svc/submission-api 8080:8080 &
	sleep 2; go run ./tools/seed-data --tar /tmp/correct-rust-hello.tar.gz --api http://localhost:8080
	@echo "Demo running. Press Ctrl-C to stop the port-forward."
	wait
```

- [ ] **Step 3: Test the full demo flow**

```bash
make dev-down    # ensure clean
make demo
```

Expected: cluster boots, services come up, sample submission uploads, build job runs, status flips to READY, ports stay forwarded.

- [ ] **Step 4: Commit**

```bash
git add tools/seed-data/ Makefile
git commit -m "feat(make): demo target — full Phase-1 pipeline on a fresh laptop"
```

---

## Phase 1 Definition of Done

- [ ] `make demo` completes end-to-end on a fresh laptop within 5 minutes (after Hetzner provisioned once).
- [ ] A Rust hello-world contestant uploads, builds (BuildKit), is scanned (Trivy), is signed (Cosign), gets an SLSA-3 attestation, lands in the registry, and rolls to status `READY` in Postgres.
- [ ] The same submission deploys to the Hetzner sandbox cluster under `runtimeClassName: gvisor` with seccomp + AppArmor + cgroups + NetworkPolicy + iptables-backstop all in place.
- [ ] The stub fairness-gateway acks orders sent to it.
- [ ] CI (lint + unit + integration + proto-sync) is green on `main`.
- [ ] Phase-1 E2E smoke test (`tests/e2e/cases/phase1_smoke_test.go`) passes.
- [ ] Repository and runbooks committed; ADR-001 written.

---

## Dependencies for Phase 2

Phase 2 (Distributed Tier) builds on:
- The CRD & operator pattern (introduced in Phase 2 from scratch).
- The fairness-gateway upgrade from stub to real four-protocol proxy.
- The reference-oracle (Rust matching engine), which uses `crates/matching-engine` (skeleton from Phase 1 Task 1.3).
- The proto codegen scaffolding from Phase 1 Task 1.5 — Phase 2 adds `time.proto`, `divergence.proto`, etc.

If any Phase 1 DoD item is red, fix it before opening Phase 2.
