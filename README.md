# IronBook
A distributed benchmarking and hosting platform for trading infrastructure.
Submissions in C++, Rust, or Go are securely hosted, stress-tested by a
distributed bot fleet, and ranked on a live leaderboard scored by latency,
throughput, and a divergence-tested correctness oracle.

## Why this is unusual
- A reference matching engine runs in parallel with every submission as the
  correctness oracle; live divergence detection on the leaderboard.
- All inputs are content-addressed and replayable — A/B comparisons of two
  submissions on byte-identical input are one CLI command.
- Designed for two regions; deployed as a single Kubernetes cluster with
  namespace isolation between control plane and sandbox tier. The Terraform
  for the original Mac↔Hetzner two-region topology ships under
  `deploy/terraform/` — one `terraform apply` away from cross-region. (ADR-011)
- seccomp + AppArmor + cgroups v2 + NetworkPolicy + iptables host backstop +
  admission-webhook — six active layers of submission isolation, plus a
  seventh (gVisor `runtimeClassName`) enforced by the admission-webhook
  contract and ready to activate on any Linux host. (ADR-011, spec §4.2)
- Glicko-2 ratings with uncertainty bands; multi-scenario tournaments
  instead of single-run wins.
- Self-replay byte-equality CI gate proves the input pipeline is
  deterministic.

## Quickstart
make dev
make demo

5 minutes from `git clone` to a live leaderboard. See docs/demo.md.

## Architecture
docs/superpowers/specs/2026-05-10-ironbook-design.md.
ADRs in docs/adr/. Runbooks in docs/runbooks/.
