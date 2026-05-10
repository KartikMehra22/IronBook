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
