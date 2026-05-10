# IronBook Phase 4 — Hardening + Blueprint

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task.

**Goal:** A platform that survives chaos, resists cheating, ships with full supply-chain provenance, and reads to a judge as a *production* system. The Architecture Blueprint document (the spec) is polished as a submission deliverable; ten ADRs are written; Argo CD GitOps is fully wired; chaos and anti-cheat suites are green.

**Architecture (Phase 4 deltas):** Add `chaos-agent` (on-demand failure injector), `ebpf-observer` (Rust+aya BPF program for syscall accounting), wire anti-cheat penalty into `scoring-engine`, OPA Gatekeeper policies enforced cluster-wide, Argo CD ApplicationSet active, demo theater (chaos button on the dashboard), and a polished blueprint document.

**Tech Stack:** Rust (`aya` 0.13), Linux 6.x kernel headers (sandbox host), nftables, tc/netem, OPA/Rego, Argo CD ApplicationSet.

**No more AI:** the plan stays self-sufficient with verbatim code for the BPF program, the chaos catalogue, and OPA policies.

---

## Spec references

- Anti-cheat: spec §4.7, §6.8
- Chaos suite: spec §7.6
- Supply-chain: spec §4.3, §4.4
- ADR layout: spec §9.10
- Argo CD ApplicationSet: spec §9.7

---

## File structure for this phase

```
apps/
└── chaos-agent/                      # T19.x
    ├── main.go
    └── injector/{netem,kill,cgroup}.go
crates/
└── ebpf-observer/                    # T20.x
    ├── Cargo.toml
    ├── src/main.rs                   # userspace
    └── ebpf/src/syscalls.bpf.rs      # BPF program
deploy/
├── manifests/
│   └── base/
│       ├── chaos-agent/              # T19.x
│       ├── ebpf-observer/            # T20.x  (DaemonSet, privileged)
│       └── opa-policies/             # T22.x  (ConstraintTemplate + Constraint)
├── argocd/
│   └── applicationset.yaml           # T22.x
docs/
├── adr/                              # T23.1 — 10 ADRs
│   ├── 001-gvisor-not-firecracker.md  # already in Phase 1
│   ├── 002-redpanda-not-kafka.md
│   ├── 003-clickhouse-not-timescaledb.md
│   ├── 004-rust-for-hot-path-go-for-rest.md
│   ├── 005-two-region-not-single-region.md
│   ├── 006-glicko-2-not-elo-not-pure-tps.md
│   ├── 007-content-addressing-replay-logs.md
│   ├── 008-gateway-stamps-dont-trust-submission.md
│   ├── 009-correctness-as-gate-not-weight.md
│   └── 010-sealed-secrets-not-vault.md
└── runbooks/                         # T19.4
    ├── 03-chaos-playbook.md
    ├── 04-anti-cheat-investigation.md
    └── ... (plus 02 from Phase 1)
```

---

## Day 19 — chaos-agent (~4 tasks, ~6 hours)

### Task 19.1: chaos-agent skeleton + injectors

**Files:**
- Create: `apps/chaos-agent/{main.go,injector/{netem,kill,cgroup}.go}`

- [ ] **Step 1: `injector/kill.go`** — pod kill via K8s API

```go
package injector

import (
	"context"
	"k8s.io/client-go/kubernetes"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type Killer struct{ Cli kubernetes.Interface }

func (k *Killer) KillPod(ctx context.Context, namespace, name string) error {
	zero := int64(0)
	return k.Cli.CoreV1().Pods(namespace).Delete(ctx, name, metav1.DeleteOptions{GracePeriodSeconds: &zero})
}
```

- [ ] **Step 2: `injector/netem.go`** — packet loss / latency via `tc qdisc` (executed in the target node via privileged DaemonSet sidecar, or via `nsenter`)

```go
package injector

import (
	"context"
	"fmt"
	"os/exec"
)

type Netem struct{}

// Inject 10% loss on egress of submission cgroup. Runs on the node host.
func (n *Netem) AddLoss(ctx context.Context, ifname string, lossPct int) error {
	cmd := exec.CommandContext(ctx, "tc", "qdisc", "add", "dev", ifname, "root", "netem", "loss", fmt.Sprintf("%d%%", lossPct))
	return cmd.Run()
}
func (n *Netem) Clear(ctx context.Context, ifname string) error {
	return exec.CommandContext(ctx, "tc", "qdisc", "del", "dev", ifname, "root").Run()
}
```

- [ ] **Step 3: `injector/cgroup.go`** — CPU throttle via cgroups v2

```go
package injector

import (
	"fmt"
	"os"
)

// HalveCPU sets cpu.max in the submission's cgroup to "100000 100000" → 1 cpu.
func HalveCPU(cgroupPath string) error {
	return os.WriteFile(fmt.Sprintf("%s/cpu.max", cgroupPath), []byte("100000 100000"), 0o644)
}
func RestoreCPU(cgroupPath string) error {
	return os.WriteFile(fmt.Sprintf("%s/cpu.max", cgroupPath), []byte("200000 100000"), 0o644)
}
```

- [ ] **Step 4: HTTP API**

```go
package main

import (
	"encoding/json"
	"net/http"
	"os"

	"github.com/<owner>/IronBook/apps/chaos-agent/injector"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

func main() {
	cfg, _ := rest.InClusterConfig()
	cli, _ := kubernetes.NewForConfig(cfg)
	mux := http.NewServeMux()
	killer := &injector.Killer{Cli: cli}
	mux.HandleFunc("/v1/inject/kill-pod", func(w http.ResponseWriter, r *http.Request) {
		var req struct { Namespace, Name string }
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil { http.Error(w,err.Error(),400); return }
		if err := killer.KillPod(r.Context(), req.Namespace, req.Name); err != nil { http.Error(w,err.Error(),500); return }
		w.WriteHeader(204)
	})
	mux.HandleFunc("/v1/inject/netem", func(w http.ResponseWriter, r *http.Request) {
		var req struct { Iface string; LossPct int }
		_ = json.NewDecoder(r.Body).Decode(&req)
		_ = (&injector.Netem{}).AddLoss(r.Context(), req.Iface, req.LossPct)
		w.WriteHeader(204)
	})
	// ... clear, cgroup endpoints
	_ = os.Setenv
	_ = http.ListenAndServe(":8090", mux)
}
```

- [ ] **Step 5: Manifest** — Deployment + ServiceAccount with RBAC for `pods/delete`; for netem and cgroup writes, deploy a privileged DaemonSet sidecar that the chaos-agent calls into. (For Phase 4 simplicity: chaos-agent itself runs `hostNetwork: true` + `privileged: true` on a single node — all chaos targets the sandbox region.)

`deploy/manifests/base/chaos-agent/{deployment,rbac,service}.yaml`:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata: { name: chaos-agent, namespace: ironbook }
spec:
  replicas: 1
  selector: { matchLabels: { app: chaos-agent } }
  template:
    metadata: { labels: { app: chaos-agent } }
    spec:
      serviceAccountName: chaos-agent
      hostNetwork: true
      hostPID: true
      containers:
        - name: agent
          image: ironbook/chaos-agent:dev
          securityContext: { privileged: true }
          ports: [ { containerPort: 8090 } ]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata: { name: chaos-agent }
rules:
  - apiGroups: [""]   resources: [pods]              verbs: [get, list, watch, delete]
  - apiGroups: [""]   resources: [pods/eviction]     verbs: [create]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata: { name: chaos-agent }
subjects: [{ kind: ServiceAccount, name: chaos-agent, namespace: ironbook }]
roleRef:  { apiGroup: rbac.authorization.k8s.io, kind: ClusterRole, name: chaos-agent }
```

- [ ] **Step 6: Commit**

```bash
git add apps/chaos-agent/ deploy/manifests/base/chaos-agent/
git commit -m "feat(chaos-agent): pod-kill / tc-netem / cgroup-throttle injectors with HTTP API"
```

---

### Task 19.2: Chaos suite scenarios (the table from spec §7.6)

**Files:**
- Create: `tests/chaos/{scenarios.yaml,run.go}`

- [ ] **Step 1: Each scenario as YAML**

```yaml
# tests/chaos/scenarios.yaml
- name: oracle-pod-kill-mid-run
  inject_at_seconds: 30
  action: { kind: kill_pod, selector: "app=reference-oracle,run=$RUN_ID" }
  expected:
    final_status: COMPLETE
    score_within_pct_of_baseline: 5

- name: gateway-pod-kill-mid-run
  inject_at_seconds: 30
  action: { kind: kill_pod, selector: "app=fairness-gateway,run=$RUN_ID" }
  expected: { final_status: COMPLETE }

- name: network-loss-10pct
  inject_at_seconds: 10
  action: { kind: netem, iface: eth0, loss_pct: 10 }
  duration_seconds: 60
  expected: { final_status: COMPLETE, p99_increase_pct_at_least: 50 }

- name: cpu-throttle-50pct
  inject_at_seconds: 10
  action: { kind: cgroup_halve_cpu, target: submission }
  expected: { tps_decrease_pct_at_least: 30 }

- name: redpanda-broker-restart
  action: { kind: kill_pod, selector: "app=redpanda" }
  expected:
    final_status: COMPLETE
    no_data_loss: true
- name: clickhouse-down-30s
  action: { kind: scale_to_zero, target: deployment/clickhouse, restore_after: 30 }
  expected: { final_status: COMPLETE, leaderboard_resumes_within_seconds: 60 }
- name: wg-link-flap
  action: { kind: wg_down_seconds, target: 5 }
  expected: { final_status: COMPLETE }
- name: clock-skew-50ms
  action: { kind: chrony_settime, delta_ms: 50 }
  expected: { time_service_skew_alarm: true }
```

- [ ] **Step 2: Driver** — `run.go` reads YAML, kicks a benchmark, calls chaos-agent at the right offset, asserts expected outcomes.

(Boilerplate: the table-driven test harness loops over each entry, files a `BenchmarkRun`, sleeps to `inject_at_seconds`, calls chaos-agent, waits for `final_status`, asserts conditions.)

- [ ] **Step 3: Add nightly job to `.github/workflows/nightly.yml`**

- [ ] **Commit.**

```bash
git add tests/chaos/ .github/workflows/nightly.yml
git commit -m "test(chaos): YAML-driven chaos scenarios from spec §7.6 + nightly CI job"
```

---

### Task 19.3: Demo theater — chaos button on dashboard

**Files:**
- Create: `frontend/app/(admin)/chaos/page.tsx`
- Create: `frontend/components/ChaosButton.tsx`

- [ ] **Step 1: Component**

```tsx
"use client";
import { useState } from "react";

export default function ChaosButton({ runId }: { runId: string }) {
  const [busy, setBusy] = useState(false);
  const inject = async (kind: string, body: any) => {
    setBusy(true);
    await fetch(`/api/chaos/${kind}`, { method: "POST", body: JSON.stringify({ ...body, runId }) });
    setTimeout(() => setBusy(false), 800);
  };
  return (
    <div className="flex gap-2">
      <button disabled={busy} onClick={() => inject("netem", { lossPct: 10, durationSec: 30 })}
              className="bg-amber-500 text-white px-4 py-2 rounded">
        Inject 10% loss
      </button>
      <button disabled={busy} onClick={() => inject("kill-pod", { selector: `run=${runId}` })}
              className="bg-red-600 text-white px-4 py-2 rounded">
        Kill submission pod
      </button>
      <button disabled={busy} onClick={() => inject("cpu-halve", { target: "submission" })}
              className="bg-orange-500 text-white px-4 py-2 rounded">
        Halve CPU
      </button>
    </div>
  );
}
```

- [ ] **Step 2: Page** wraps the component + a link back to leaderboard.

- [ ] **Step 3: Next.js API route** at `/api/chaos/[kind]/route.ts` proxies to chaos-agent service.

- [ ] **Commit.**

```bash
git add frontend/
git commit -m "feat(frontend): chaos button on /chaos page (demo theater)"
```

---

### Task 19.4: Chaos runbook

**Files:**
- Create: `docs/runbooks/03-chaos-playbook.md`

```markdown
# Chaos Playbook

## When to run
- During every demo (live).
- Nightly via CI.
- Before any major release.

## Manual triggers (UI)
- /chaos page exposes three buttons. Each calls a Next.js API route → chaos-agent.

## CLI triggers
ironbookctl chaos kill --selector "run=$RID,app=reference-oracle"
ironbookctl chaos netem --iface eth0 --loss 10 --duration 30s
ironbookctl chaos cpu-halve --target submission --run $RID

## Expected outcomes per scenario
[ table mirrored from spec §7.6 ]

## Rollback
- Networks: `tc qdisc del dev eth0 root` (chaos-agent /v1/inject/clear).
- CPU: chaos-agent /v1/inject/cgroup-restore.
- Pods: deletion is non-restorable; the operator schedules a fresh pod automatically.
```

- [ ] **Commit.**

```bash
git add docs/runbooks/03-chaos-playbook.md
git commit -m "docs(runbooks): chaos playbook"
```

---

## Day 20 — eBPF observer + anti-cheat (~5 tasks, ~7 hours)

### Task 20.1: aya-rs scaffold

**Files:**
- Create: `crates/ebpf-observer/{Cargo.toml,src/main.rs,ebpf/Cargo.toml,ebpf/src/syscalls.bpf.rs}`

- [ ] **Step 1: Install bpf-linker**

```bash
cargo install bpf-linker
```

- [ ] **Step 2: Workspace exception** (allow `unsafe` only in this crate)

In root `Cargo.toml`, set `unsafe_code = "allow"` only when this crate is included via a feature flag, OR opt out of workspace lints for this crate:

In `crates/ebpf-observer/Cargo.toml`:
```toml
[lints]
# Override workspace: BPF code is unavoidably unsafe.
rust.unsafe_code = "allow"
```

- [ ] **Step 3: BPF program** — `ebpf/src/syscalls.bpf.rs`

```rust
#![no_std]
#![no_main]

use aya_bpf::{macros::{map, tracepoint}, programs::TracePointContext, maps::HashMap, helpers::bpf_get_current_cgroup_id};

#[map(name = "SYSCALL_COUNTS")]
static mut SYSCALL_COUNTS: HashMap<u64, u64> = HashMap::with_max_entries(8192, 0);

#[tracepoint]
pub fn on_sys_enter(ctx: TracePointContext) -> i32 {
    let cgroup_id = unsafe { bpf_get_current_cgroup_id() };
    unsafe {
        let cur = SYSCALL_COUNTS.get(&cgroup_id).copied().unwrap_or(0);
        let _ = SYSCALL_COUNTS.insert(&cgroup_id, &(cur + 1), 0);
    }
    0
}

#[panic_handler]
fn panic(_: &core::panic::PanicInfo) -> ! { loop {} }
```

- [ ] **Step 4: Userspace** — `src/main.rs`

```rust
use aya::{Ebpf, programs::TracePoint, maps::HashMap as AyaHashMap, util::online_cpus};
use opentelemetry_otlp::WithExportConfig;
use std::time::Duration;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();

    let mut bpf = Ebpf::load(include_bytes_aligned!("../ebpf/target/bpfel-unknown-none/release/syscalls"))?;
    let prog: &mut TracePoint = bpf.program_mut("on_sys_enter").unwrap().try_into()?;
    prog.load()?;
    prog.attach("syscalls", "sys_enter")?;

    let mut counts: AyaHashMap<_, u64, u64> = AyaHashMap::try_from(bpf.map_mut("SYSCALL_COUNTS").unwrap())?;
    loop {
        for entry in counts.iter() {
            let (cgroup_id, count) = entry?;
            // Emit OTLP metric — for Phase 4 we just print + push to a /metrics scrape endpoint.
            println!("cgroup={} syscalls={}", cgroup_id, count);
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}
```

- [ ] **Step 5: Build BPF blob, then userspace**

```bash
cd crates/ebpf-observer/ebpf
cargo +nightly build --release --target=bpfel-unknown-none -Z build-std=core
cd ../../..
cargo build -p ebpf-observer
```

- [ ] **Step 6: DaemonSet manifest** with `privileged: true` + `hostPID: true`

(Standard pattern; mount `/sys/fs/bpf` and `/sys/kernel/debug`.)

- [ ] **Commit.**

```bash
git add crates/ebpf-observer/ deploy/manifests/base/ebpf-observer/
git commit -m "feat(ebpf-observer): aya tracepoint program counting syscalls per cgroup; userspace OTLP exporter"
```

---

### Task 20.2: anti-cheat scoring path

**Files:**
- Modify: `apps/scoring-engine/scorer/score.go`
- Create: `apps/scoring-engine/anti_cheat/anti_cheat.go`

- [ ] **Step 1: Read syscall counts from a Prometheus query against `ebpf-observer`**

`anti_cheat.go`:
```go
package anti_cheat

import (
	"context"
	"github.com/prometheus/client_golang/api"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
)

type Signals struct {
	ClockGettimeFraction float64
	SyscallToOrderRatio  float64
	BlockedEgressBytes   float64
	MemoryPctSustained   float64
	DivergenceCount      int
}

func Compute(s Signals) float64 {
	score := 0.0
	if s.ClockGettimeFraction > 0.30 { score += 0.1 }
	if s.SyscallToOrderRatio > 5    { score += 0.2 }
	if s.BlockedEgressBytes > 0     { score += 0.5 }
	if s.MemoryPctSustained > 0.90  { score += 0.1 }
	if s.DivergenceCount > 3        { score += 0.3 }
	if score > 1.0 { score = 1.0 }
	return score
}

func Query(ctx context.Context, addr string, runID string) (Signals, error) {
	c, _ := api.NewClient(api.Config{Address: addr})
	api := v1.NewAPI(c)
	// pseudo-queries
	_ = api
	return Signals{ /* populated from Prometheus */ }, nil
}
```

- [ ] **Step 2: Wire `anti_cheat.Compute` into scoring-engine — pass `Inputs.AntiCheatFlags`** computed from signals.

- [ ] **Step 3: Test** (use a stub Prometheus client; assert weights sum correctly).

- [ ] **Commit.**

```bash
git add apps/scoring-engine/anti_cheat/ apps/scoring-engine/scorer/
git commit -m "feat(scoring-engine): wire anti-cheat penalty from ebpf-observer Prometheus signals"
```

---

### Task 20.3: Smoke — known-cheating fixture submissions get penalized

**Files:**
- Create: `tests/e2e/fixtures/submissions/cheat-clock-spoof/{Cargo.toml,src/main.rs}` (calls `clock_gettime` 1M times before each ack)
- Create: `tests/e2e/fixtures/submissions/malicious-egress/...` (calls `getaddrinfo` + `connect` to 1.1.1.1)

For each, assert via E2E test that `score == 0` *or* anti-cheat flag is registered.

- [ ] **Commit.**

---

## Day 21 — Supply chain hardening (~3 tasks, ~5 hours)

### Task 21.1: Cosign verification webhook (admission level)

**Files:**
- Modify: `apps/admission-webhook/policy/validate.go`
- Create: `pkg/cosignverify/verify.go`

- [ ] **Step 1: Implement signature check using `sigstore-go`**

`pkg/cosignverify/verify.go`:
```go
package cosignverify

import (
	"context"
	"github.com/sigstore/cosign/v2/pkg/cosign"
	"github.com/sigstore/cosign/v2/pkg/oci/remote"
	"github.com/google/go-containerregistry/pkg/name"
)

type Verifier struct{ PubKeyPath string }

func (v *Verifier) Verify(ctx context.Context, imageRef string) error {
	ref, err := name.ParseReference(imageRef); if err != nil { return err }
	co := &cosign.CheckOpts{} // populated with public key
	_, _, err = cosign.VerifyImageSignatures(ctx, ref, co)
	return err
}
```

- [ ] **Step 2: Validate calls `Verify` for every container image referenced in a submission pod.**

- [ ] **Step 3: Test with our own signed image (passes) and an unsigned image (fails).**

- [ ] **Commit.**

```bash
git add apps/admission-webhook/ pkg/cosignverify/
git commit -m "feat(admission-webhook): verify Cosign signatures on every submission pod image"
```

---

### Task 21.2: SLSA-3 attestation verification

**Files:**
- Modify: `apps/admission-webhook/policy/validate.go`

- [ ] **Step 1: Verify in-toto attestation matches the image digest** (using `cosign verify-attestation`)

- [ ] **Step 2: Test, commit.**

---

### Task 21.3: SBOM generation in build-runner

**Files:**
- Modify: `apps/build-runner/runner/runner.go`

- [ ] **Step 1: Run `syft` on the built image, attach as attestation**

```go
if err := runCmd(ctx, "syft", imageRef, "-o", "spdx-json", "--file", in.WorkDir+"/sbom.json"); err != nil { return nil, err }
if err := runCmd(ctx, "cosign", "attest", "--yes", "--key", in.CosignKey,
    "--predicate", in.WorkDir+"/sbom.json", "--type", "spdxjson", pinned); err != nil { return nil, err }
```

- [ ] **Step 2: Commit.**

```bash
git add apps/build-runner/
git commit -m "feat(build-runner): generate SBOM via syft and attach as cosign attestation"
```

---

## Day 22 — OPA Gatekeeper policies + Argo CD (~3 tasks, ~5 hours)

### Task 22.1: Gatekeeper ConstraintTemplates + Constraints

**Files:**
- Create: `deploy/manifests/base/opa-policies/{ct-no-latest,ct-no-privileged,ct-must-cosign,ct-must-runtimeclass-gvisor}.yaml`
- Create: `deploy/manifests/base/opa-policies/constraints.yaml`

- [ ] **Step 1: ConstraintTemplate `K8sNoLatest`**

```yaml
apiVersion: templates.gatekeeper.sh/v1
kind: ConstraintTemplate
metadata: { name: k8snolatest }
spec:
  crd:
    spec:
      names: { kind: K8sNoLatest }
  targets:
    - target: admission.k8s.gatekeeper.sh
      rego: |
        package k8snolatest
        violation[{"msg": msg}] {
          input.review.kind.kind == "Pod"
          c := input.review.object.spec.containers[_]
          endswith(c.image, ":latest")
          msg := sprintf("image %v uses :latest", [c.image])
        }
        violation[{"msg": msg}] {
          input.review.kind.kind == "Pod"
          c := input.review.object.spec.containers[_]
          not contains(c.image, "@sha256:")
          not contains(c.image, ":")
          msg := sprintf("image %v missing tag/digest", [c.image])
        }
```

- [ ] **Step 2: Constraint** binding the template to `submissions/` namespace + admission-webhook gate.

- [ ] **Step 3: Repeat for: no-privileged, must-cosign-sig (Rego from spec §4.4), must-runtimeclass-gvisor.** All Rego policies are verbatim from spec §4.

- [ ] **Step 4: Apply, verify** with `kubectl apply -f a-violating-pod.yaml` getting rejected.

- [ ] **Commit.**

```bash
git add deploy/manifests/base/opa-policies/
git commit -m "feat(opa): four ConstraintTemplates + Constraints (no-latest, no-privileged, must-cosign, must-gvisor)"
```

---

### Task 22.2: Argo CD ApplicationSet

**Files:**
- Create: `deploy/argocd/applicationset.yaml`

(Verbatim from spec §9.7.)

- [ ] **Step 1: Apply**

```bash
KUBECONFIG=$PWD/kubeconfig.local kubectl apply -f deploy/argocd/applicationset.yaml
KUBECONFIG=$PWD/kubeconfig.local kubectl -n argocd get applications
```

Expected: two `Application`s (`ironbook-control`, `ironbook-sandbox`) appear and sync.

- [ ] **Step 2: Smoke** — `git push` triggers Argo CD reconcile within 1 min; observe rollout via `argocd app sync` or autosync.

- [ ] **Commit.**

```bash
git add deploy/argocd/
git commit -m "feat(argocd): ApplicationSet wiring two clusters; autosync + selfheal"
```

---

### Task 22.3: GitOps drill — change a deployment, watch Argo CD reconcile

- [ ] **Step 1:** Modify a non-critical Deployment replica count via git commit; push.
- [ ] **Step 2:** Observe Argo CD apply within seconds; verify cluster state matches.
- [ ] **Step 3:** Document this in `docs/runbooks/05-gitops-deploy.md`.

- [ ] **Commit.**

---

## Day 23 — Architecture Decision Records (ADRs) + Blueprint polish (~5 tasks, ~7 hours)

This is where the prize gets locked in. Treat the spec doc as a submission deliverable: tighten prose, fix any drift, generate diagrams as static SVGs (mermaid-cli) for offline viewing.

### Task 23.1: ADRs 002–010

**Files:**
- Create: `docs/adr/{002…010}-*.md`

Each ADR follows the same shape as ADR-001 (Phase 1 Task 1.7): Context, Decision, Consequences, Alternatives. Word-budget: ~250–350 words each.

- [ ] **Step 1: ADR-002 — Redpanda over Kafka**

Key points: single binary, no JVM, no Zookeeper, Kafka API-compatible, lower ops burden at hackathon scale, tiered storage to MinIO. Trade: smaller community, single vendor; mitigated by API compatibility (Kafka clients work).

- [ ] **Step 2: ADR-003 — ClickHouse over TimescaleDB**

Key points: column store ingestion ~3-10× higher than Timescale at our row pattern; mergeable `quantileTDigestState` is *the* reason scoring works at 1Hz; native protocol. Trade: ClickHouse SQL dialect quirks; mitigated by limited scope of queries.

- [ ] **Step 3: ADR-004 — Rust on hot path, Go elsewhere**

Key points: Rust for matching engine, telemetry-ingester, telemetry-sidecar, divergence-detector, time-service — places where allocation control + lock-free queueing matter. Go for control-plane services, operator, gateway — where K8s ergonomics dominate. Trade: bilingual codebase; mitigated by clean gRPC boundaries.

- [ ] **Step 4: ADR-005 — Two-region**

Key points: Mac control plane + Hetzner sandbox region. Reasoning: untrusted code never touches dev workstation; real cross-Internet latency in metrics shows operational maturity. Trade: ops complexity; mitigated by Wireguard mesh.

- [ ] **Step 5: ADR-006 — Glicko-2 over ELO over pure-TPS**

Key points: pure TPS rewards single-run wins; ELO has no uncertainty bands; Glicko-2 produces `μ ± φ` which is the *honest* leaderboard. Trade: math complexity; mitigated by tested reference impl.

- [ ] **Step 6: ADR-007 — Content-addressed replay logs**

Key points: sha256 = file_id; immutable; A/B replay needs byte-identical input by definition. Trade: storage growth (mitigated by MinIO + object-lock + 7d retention).

- [ ] **Step 7: ADR-008 — Gateway stamps; never trust submission clocks**

Key points: latency measured at the gateway; `platform_seq + platform_ts` are the only authoritative ordering. Trade: extra hop at the gateway; mitigated by batched stamp requests (~5µs amortized).

- [ ] **Step 8: ADR-009 — Correctness as gate, not weight**

Key points: a wrong matching engine is worse than a slow one; the formula reflects that. Below 0.999 match rate ⇒ score = 0. Trade: harsh; mitigated by detailed correctness reporting.

- [ ] **Step 9: ADR-010 — Sealed Secrets over HashiCorp Vault**

Key points: hackathon scope ≠ Vault operational cost; Sealed Secrets is honest at this scale; HSM is documented as future work. Trade: secrets in git (encrypted) require a stable cluster keypair.

- [ ] **Step 10: Commit**

```bash
git add docs/adr/
git commit -m "docs(adr): write ADRs 002–010 (one per major architectural decision)"
```

---

### Task 23.2: Mermaid → static SVGs

**Files:**
- Create: `docs/diagrams/*.svg`
- Modify: spec doc — embed SVG references *alongside* mermaid blocks (judges can view either)

- [ ] **Step 1: Install mermaid-cli**

```bash
pnpm i -g @mermaid-js/mermaid-cli
```

- [ ] **Step 2: For each mermaid diagram in the spec, render to SVG**

```bash
mkdir -p docs/diagrams
mmdc -i input-topology.mmd -o docs/diagrams/topology.svg
# repeat for: state-machine, hot-path, telemetry-pipeline, replay, crd-relationships
```

- [ ] **Step 3: Add SVG references to the spec near each mermaid block**

```markdown
![Topology](../../diagrams/topology.svg)
```

- [ ] **Commit.**

---

### Task 23.3: Spec final pass

**Files:**
- Modify: `docs/superpowers/specs/2026-05-10-ironbook-design.md`

- [ ] **Step 1: Update front-matter `Status` → `Production-ready (hackathon submission)`**
- [ ] **Step 2: Add an "Implementation status" section** mapping each spec section to commit ranges.
- [ ] **Step 3: Tighten the prose** in Sections 1, 5, 9 — they're the most-read by judges.
- [ ] **Step 4: Add a "How to read this document" preamble**:

```markdown
## How to read this document

If you have 5 minutes: read §0 (Context) + §1.1 (Architectural principles) + §10 (Future work).
If you have 30 minutes: add §2 (Data Flow), §5 (Correctness), §9.12 (Demo).
If you're evaluating engineering rigor: read §3 (Components), §4 (Security), §7 (Testing), §8 (Failure Modes), and the ADRs in docs/adr/.
```

- [ ] **Commit.**

```bash
git add docs/superpowers/specs/
git commit -m "docs(spec): final polish — prose tightening, implementation status, how-to-read preamble"
```

---

### Task 23.4: Whitepaper-style PDF export (optional but high-leverage)

- [ ] **Step 1: Use Pandoc**

```bash
brew install pandoc
pandoc docs/superpowers/specs/2026-05-10-ironbook-design.md \
       --toc --pdf-engine=xelatex \
       --metadata title="IronBook Architecture Blueprint" \
       --metadata author="Kartik Mehra" \
       -V geometry:margin=1in -V monofont="Menlo" \
       -o docs/IronBook-Architecture-Blueprint.pdf
```

- [ ] **Step 2: Commit the PDF (it's a deliverable for the hackathon submission)**

```bash
git add docs/IronBook-Architecture-Blueprint.pdf
git commit -m "docs: produce architecture blueprint PDF (hackathon submission deliverable)"
```

---

### Task 23.5: Phase 4 close-out

- [ ] **Step 1: `make ci-local` green; `make ci-self-replay` green; `make chaos local-1h` green for at least one scenario.**
- [ ] **Step 2: Tag `phase-4-complete`; push.**

---

## Phase 4 Definition of Done

- [ ] `chaos-agent` exposes 4 endpoints (kill-pod, netem, cgroup-halve, restore); RBAC scoped narrowly.
- [ ] All 8 chaos scenarios from spec §7.6 codified in YAML + driven by a CI suite.
- [ ] Demo "chaos button" works in the dashboard.
- [ ] `ebpf-observer` DaemonSet runs on the sandbox node; emits per-cgroup syscall counts.
- [ ] Anti-cheat penalty wired into `scoring-engine`; cheat-fixture submissions score 0 or are flagged.
- [ ] Cosign signatures verified at admission for *every* submission pod; SLSA-3 attestations checked.
- [ ] SBOM attached as cosign attestation on every built image.
- [ ] OPA Gatekeeper enforcing 4 ConstraintTemplates cluster-wide.
- [ ] Argo CD ApplicationSet active; `git push` to main reconciles both clusters.
- [ ] 10 ADRs written.
- [ ] Spec polished; PDF generated.
- [ ] `phase-4-complete` git tag pushed.
