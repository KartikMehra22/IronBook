# IronBook — Implementation Plan Index

> **For agentic workers:** This file is the entry point. Each phase plan is self-contained and uses superpowers:subagent-driven-development (recommended) or superpowers:executing-plans for execution.

**Goal of the program:** Ship a distributed benchmarking & hosting platform for trading infrastructure that wins the IICPC Summer Hackathon 2026 (deadline 2026-06-10) and reads as production-grade engineering.

**Calendar:** 25 build days (May 10 → June 3), 6-day submission buffer (June 4 → June 10).
**Constraint:** AI assistance available only Days 1–12 (Claude cutoff). Days 13–25 are solo.

---

## Phases

| # | Plan | Days | Status when done |
|---|---|---|---|
| 1 | [Foundation + Sandbox Tier](./2026-05-10-ironbook-phase-1-foundation.md) | 1–6 (May 10–15) | `make demo` builds + signs + deploys a Rust hello-world contestant under gVisor on Hetzner; stub gateway acks. |
| 2 | [Distributed Tier](./2026-05-10-ironbook-phase-2-distributed.md) | 7–12 (May 16–21) | Operator with 4 CRDs; full matching engine with property tests; real fairness-gateway; bot-coordinator; first end-to-end `BenchmarkRun → COMPLETE`. **Day 12 = Claude cutoff.** |
| 3 | [Telemetry & Replay](./2026-05-10-ironbook-phase-3-telemetry-replay.md) | 13–18 (May 22–27) | Redpanda + ClickHouse + Redis; live leaderboard with Glicko-2; deterministic replay; self-replay byte-equality CI gate green. |
| 4 | [Hardening + Blueprint](./2026-05-10-ironbook-phase-4-hardening.md) | 19–23 (May 28–Jun 1) | Chaos-agent + 8 scenarios; eBPF-driven anti-cheat; OPA Gatekeeper; Argo CD GitOps; 10 ADRs; spec polished. |
| 5 | [Polish + Demo](./2026-05-10-ironbook-phase-5-polish-demo.md) | 24–25 (Jun 2–3) | Benchmark charts, 5-min demo video, README, reproducibility check, submission package. |
| — | Submission buffer | 26–31 (Jun 4–10) | Re-record / re-stage / fix only. No new features. |

---

## How to read these plans

- **Each plan has a header** declaring goal, architecture deltas from the prior phase, tech stack, and spec cross-references.
- **Tasks are numbered `<phase>.<index>`** and contain steps with checkbox syntax, exact file paths, full code blocks, and explicit shell commands.
- **Definition-of-done lists** at the end of every phase let you gate progress.
- **Dependencies for the next phase** are listed at the bottom of each plan; if a current-phase DoD is red, the next phase's assumptions break.

---

## Cross-cutting things

### Things that span phases
- **Spec doc** (`docs/superpowers/specs/2026-05-10-ironbook-design.md`) — referenced from every plan. Phase 4 polishes it; Phase 5 produces the PDF.
- **CI** — Phase 1 stands it up; every later phase adds gates (`ci-self-replay` in Phase 3; chaos nightly in Phase 4).
- **The 10 ADRs** — Phase 1 writes ADR-001; Phase 4 writes ADRs 002–010. They're the most concentrated form of "deliberate, well-reasoned architectural thought process."

### Things you should *not* do (anti-goals)
- Adding features beyond the spec scope. The spec is locked. Any new requirement gets a §10 entry, not a code change.
- Re-architecting between phases. The architecture is fixed; phases add layers, not rebuild them.
- Skipping the property tests on the matching engine (Phase 2). They are the credibility multiplier; they take a few hours and pay back the entire correctness section of the score.
- Skipping the self-replay CI gate (Phase 3). Without it, the determinism story is unfounded.
- Adding new dependencies past Day 12. Anything Day 13+ should use the libraries already in `Cargo.toml` / `go.mod`.

### Day 12 cutoff — what *must* land before
- Full matching-engine crate (Phase 2 Tasks 7.1–7.6).
- All four CRDs + the BenchmarkRun reconciler (Phase 2 Tasks 10.x–11.2).
- Real fairness-gateway (Phase 2 Task 9.x).
- time-service + reference-oracle services (Phase 2 Tasks 8.x).
- The first end-to-end `BenchmarkRun → COMPLETE` (Phase 2 Task 12.4).

If any of these is red on Day 12 EOD, **freeze scope** and skip ahead to Phase 3 with whatever shipped — partial-but-deep beats wide-but-shallow.

---

## Execution mode

The brainstorming skill ended by asking how you want to execute these plans. Two recommended modes (the writing-plans skill spells these out at the bottom of each plan):

1. **Subagent-driven** (recommended) — for each task, dispatch a fresh subagent with the task's section as its prompt. Two-stage review between tasks.
2. **Inline execution** — work through the plan in this session, checkpointing every few tasks for review.

Pick one before starting Phase 1.

---

## When something breaks

The runbooks in `docs/runbooks/` are your first stop:
- `02-bring-up-hetzner.md` — provisioning + WG handshake
- `03-chaos-playbook.md` — running chaos manually
- `RUNBOOK-01..05` (in spec §8.11) — the most common operational failures

If a runbook doesn't help, the spec § 8 (Failure Modes) names every realistic failure with a recovery path. Quote it.
