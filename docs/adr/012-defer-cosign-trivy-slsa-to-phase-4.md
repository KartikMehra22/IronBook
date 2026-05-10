# ADR-012: Defer Cosign + Trivy + SLSA-3 to Phase 4

Date: 2026-05-10
Status: Accepted

## Context

The original Phase 1 plan (Day 5) had build-runner perform the full
production build pipeline:
- `buildctl build` (BuildKit)
- `trivy image` (CVE + license + secret scan)
- `cosign sign` (Sigstore signing)
- SLSA-3 in-toto attestation
- `cosign attest`

For the hackathon submission, this combination has two practical issues:
1. BuildKit rootless on kind-on-Apple-Silicon is fragile (Docker-in-Docker
   privileges + ARM64 buildx + cgroupv2 interactions).
2. The signing/attestation chain is *consumed* by the admission-webhook
   in Phase 4 Task 21 (Cosign verify on every pod create + SLSA verify +
   SBOM). Producing signatures in Phase 1 with no consumer until Phase 4
   front-loads complexity for no near-term value.

## Decision

For Phase 1 the build-runner produces signed-image-shaped artefacts
without signing them: it compiles contestant source and uses
`go-containerregistry` (in-process Go library) to package the resulting
binary atop a distroless base image, then pushes to the in-cluster Zot
registry. Submission status flows PENDING → BUILDING → READY purely on
build success.

In Phase 4 Task 21 the build-runner is extended with `cosign sign`,
`cosign attest --type slsaprovenance`, and `cosign attest --type spdxjson`
calls; the admission-webhook (which already has hooks for verification
in Phase 4) starts enforcing signatures.

## Consequences

**Phase 1 keeps:**
- The full pipeline shape (upload → build → push → READY).
- Hermetic builds (build-runner runs in `builds` namespace with egress
  denied to anywhere except the in-cluster registry + module mirrors).
- Real container images pushed to a real registry.
- Distroless final images (no shell, no package manager).

**Phase 1 defers (covered in Phase 4 plan Task 21):**
- Cosign image signature production.
- SLSA-3 in-toto provenance attestation.
- SPDX SBOM via `syft`.
- Trivy CVE/secret scan as a build-blocking gate (CVE database is
  ARM64-supported but takes ~2 min per build; pulled into Phase 4 where
  CI runs nightly).

**Risk:** between Phase 1 ship and Phase 4 wrap, the admission-webhook
cannot cryptographically verify image provenance. Mitigation: the
webhook still enforces the *image reference shape* (must be
`registry.ironbook.svc:5000/sub/<sha>@sha256:<digest>`), so contestants
cannot point at arbitrary external images. The shape check + the
network-isolated build pipeline means images that land on the registry
were built from contestant source by the platform, just not
cryptographically attested-to until Phase 4.

## Alternatives considered

- **Front-load signing in Phase 1.** Doubles the Day 5 implementation
  effort without any consumer until Phase 4.
- **Skip signing entirely for the hackathon.** Loses the supply-chain
  story which is one of the platform's wow-factor differentiators
  (judges read ADRs and §4.4). Rejected.
- **Use cosign with ephemeral keys (Sigstore Fulcio).** Public Rekor +
  Fulcio require internet egress from build pods, breaking the hermetic
  build constraint. Documented as future work in §10.4.
