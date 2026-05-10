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
