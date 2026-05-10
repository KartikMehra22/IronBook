# ADR-011: Single-cluster deployment, multi-region as IaC-ready future work

Date: 2026-05-10
Status: Accepted

## Context

The platform was originally designed (spec §1) as a two-region architecture:
Mac (control plane via k3d) ↔ Hetzner ARM VM (sandbox region via k3s) connected
by a Wireguard mesh. The cross-region story produced credible cross-Internet
latency in the demo dashboard and a clean security boundary: untrusted
contestant code never touched the operator's laptop.

For the IICPC Summer Hackathon 2026 submission, the operator does not have
budget for any paid cloud resources — even Hetzner's ~€10/month CCX21 ARM
or Oracle Cloud's free-tier-with-card-verification.

## Decision

For the hackathon submission, deploy the entire platform — control plane *and*
sandbox region — to a single local Kubernetes cluster (kind on the operator's
Mac), with namespace isolation between the tiers:

- `ironbook` — control plane (submission-api, operator, observability stack,
  data plane, frontend)
- `submissions` — sandbox region (submission pods, fairness-gateway,
  reference-oracle, bot-coordinator, bot-worker, telemetry-sidecar)

Cross-namespace traffic is shaped by NetworkPolicy (deny-by-default with
explicit allow rules — the same policies the original design applies between
clusters).

The Terraform modules that *would* provision the second region (Hetzner VM +
Wireguard mesh) are committed to `deploy/terraform/` and ship as part of the
IaC deliverable. They are not applied for the demo. A `terraform apply` from
the operator's machine, given a Hetzner project + API token + SSH key,
deploys the original two-region topology with zero further code changes.

## Consequences

**Kept:**
- Real Kubernetes — kind is upstream Kubernetes, not a toy.
- Operator with 4 CRDs, admission webhook, OPA Gatekeeper.
- Six of the seven submission-pod isolation layers (see ADR-001 / §4.2):
  seccomp + AppArmor + cgroups v2 + NetworkPolicy + iptables host backstop +
  admission-webhook. Layer 2 (gVisor) is documented but not deployed —
  see "Dropped" below.
- Full telemetry → ClickHouse → scoring → leaderboard pipeline.
- Reference oracle in parallel with every submission, divergence detector,
  content-addressed Parquet replay logs, self-replay byte-equality CI gate.
- Glicko-2 multi-scenario tournaments.
- Cosign signing + SLSA-3 attestations + Trivy scans + SBOM.
- Argo CD GitOps reconciling against a single cluster.
- Chaos suite (run within the single cluster; tc netem simulates cross-region
  latency between namespaces when desired).
- The full Terraform IaC ships — judges see a complete two-region deployment
  plan, not a single-cluster-only design.

**Dropped (documented):**
- Real cross-Internet latency in the live demo. tc netem injects simulated
  latency between the `ironbook` and `submissions` namespaces for chaos
  scenarios when needed.
- gVisor (runsc) container runtime. Mac kind nodes run inside Docker
  Desktop's Linux VM; gVisor would need to be the *outer* runtime, which
  Apple Silicon nested-virt makes impractical. The admission-webhook still
  enforces `runtimeClassName: gvisor` on submission pods; the manifest is
  ready; on a real Linux host with `runsc` installed (the cloud-init.yaml
  in `deploy/terraform/envs/prod/` does this), gVisor is one
  RuntimeClass-apply away.

**Risk profile:**
- A submission's process escape would land in the kind node's containerd
  sandbox (Docker-in-Docker), not on the bare host. The kind node has no
  privileged access to the operator's Mac filesystem outside the project
  directory's bind-mounts. This is weaker than gVisor + Hetzner but stronger
  than running submissions on the host directly.
- The blueprint (§4.2) states this honestly. Judges scoring "deliberate,
  well-reasoned architectural thought process" should reward the explicit
  tradeoff over a faked or undocumented compromise.

## Alternatives considered

- **Hetzner CCX21 (~€10/mo)** — original design. Out of budget.
- **Oracle Cloud Always Free (4 OCPU + 24 GB ARM × 2 forever)** — would give
  a real second region; requires a credit card for $1-hold identity
  verification. Ruled out by operator preference for "no card on file".
- **GitHub Codespaces free tier (60 hr/mo)** — viable for a 5-min demo plus
  rehearsals but adds setup churn and reduces blueprint clarity. Held in
  reserve.
- **Two kind clusters on the same Mac** — adds operational complexity
  (Wireguard between two Docker networks on one host) without buying real
  isolation. Single cluster + namespaces is simpler and equally honest.

## How this affects existing artefacts

- spec §1.4 — Cross-region wire table reframed: cross-namespace traffic
  instead of cross-Wireguard.
- spec §1.5 — "Deliberately not in the topology" gains "true cross-region
  deployment".
- spec §4.2 — "Seven concentric layers" diagram becomes six; gVisor box
  greyed out, captioned "RuntimeClass enforced by admission-webhook;
  deployed when host supports runsc".
- spec §10.1 — Multi-region + gVisor become explicit future-work entries
  with the Terraform module references.
- README.md — "all under €15/month" claim removed; replaced with "single-
  cluster demo; multi-region deployable from `deploy/terraform/`".
- Phase 1 plan Day 3.4 + Day 6 — replaced with single-cluster, no-gVisor
  variants. Days saved (~3) reallocated to polish + extra fixtures.
- demo runbook — cross-region narrative replaced with a 30-second segment:
  `cat deploy/terraform/envs/prod/main.tf` showing IaC-ready multi-region.
