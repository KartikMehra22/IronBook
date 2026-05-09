# IronBook Phase 5 — Polish + Demo

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Submission deliverables are polished and reproducible. Benchmark charts produced; 5-minute demo video recorded; README and runbooks read like a real product; the platform passes a fresh-laptop reproducibility check; the hackathon submission package is ready.

**Architecture:** No new architecture. Phase 5 is *purely* about presentation, reproducibility, and last-mile polish. Six-day submission window (Days 26–31, June 4–10) is buffer for fixes and re-recording.

---

## Spec references

- Demo runbook: spec §9.12
- README template: spec §9.13
- Benchmark targets: spec §6.6.3

---

## Day 24 — Benchmark charts + dry run (~5 tasks, ~6 hours)

### Task 24.1: Benchmark scenario suite

**Files:**
- Create: `tests/e2e/scenarios/{quiet-market,burst,crash-regime,steady-state}.yaml`
- Create: `tests/e2e/fixtures/submissions/{rust-fast,rust-medium,go-medium,buggy-engine-wrong-fills}/`

The four "marquee" scenarios drive the benchmark charts:

```yaml
# quiet-market.yaml — low rate, mostly limit, no cancels
name: quiet-market
seed: 42
durationSeconds: 60
rate_per_sec: 1000
order_mix: { limit: 0.95, market: 0.04, cancel: 0.01 }
volatility_bps: 5

# burst.yaml — 10× rate spike at t=30s for 10s
name: burst
durationSeconds: 60
rate_per_sec: 1000
burst: { at_seconds: 30, multiplier: 10, duration_seconds: 10 }

# crash-regime.yaml — wide spread + cancels storm
name: crash-regime
durationSeconds: 60
rate_per_sec: 2000
order_mix: { limit: 0.5, market: 0.2, cancel: 0.3 }
volatility_bps: 200

# steady-state.yaml — sustained TPS test
name: steady-state
durationSeconds: 300
rate_per_sec: 5000
order_mix: { limit: 0.7, market: 0.2, cancel: 0.1 }
```

For each scenario, run all four submissions; capture per-run summary into `docs/benchmarks/raw/<scenario>-<submission>.json`.

- [ ] **Step 1: Run the matrix**

```bash
for s in quiet-market burst crash-regime steady-state; do
  for sub in rust-fast rust-medium go-medium buggy-engine-wrong-fills; do
    ironbookctl run --scenario $s --submission $sub --output docs/benchmarks/raw/$s-$sub.json
  done
done
```

- [ ] **Step 2: Commit raw data.**

```bash
git add tests/e2e/scenarios/ tests/e2e/fixtures/submissions/ docs/benchmarks/raw/
git commit -m "test(benchmarks): four marquee scenarios + four fixture submissions; raw run data"
```

---

### Task 24.2: Chart generation script

**Files:**
- Create: `tools/charts/main.go`

Use `go-echarts` to produce SVG charts.

```bash
go get github.com/go-echarts/go-echarts/v2/charts@v2.4.0
go get github.com/go-echarts/go-echarts/v2/components@v2.4.0
```

`tools/charts/main.go` reads `docs/benchmarks/raw/*.json` and produces:

1. **Latency CDF per scenario** — overlay all four submissions.
2. **TPS over time** — line chart, 1s granularity, per scenario.
3. **Score breakdown radar** — five axes (latency, throughput, tail, stability, correctness) per submission.
4. **Glicko rating evolution** — sparkline per submission across scenarios.

```go
package main

import (
	"encoding/json"
	"os"
	"path/filepath"

	"github.com/go-echarts/go-echarts/v2/charts"
	"github.com/go-echarts/go-echarts/v2/opts"
)

type RunSummary struct {
	Scenario, Submission string
	P50, P90, P99 float64
	TPS float64
	Score int
	Latency, Throughput, Tail, Stability float64
}

func main() {
	rows := loadAll("docs/benchmarks/raw")
	emitCDF(rows, "docs/benchmarks/charts/latency-cdf.svg")
	emitTPS(rows, "docs/benchmarks/charts/tps-over-time.svg")
	emitRadar(rows, "docs/benchmarks/charts/score-radar.svg")
	emitGlicko(rows, "docs/benchmarks/charts/rating-evolution.svg")
}

func loadAll(dir string) []RunSummary {
	files, _ := filepath.Glob(dir + "/*.json")
	out := make([]RunSummary, 0, len(files))
	for _, f := range files {
		var r RunSummary; b, _ := os.ReadFile(f); _ = json.Unmarshal(b, &r); out = append(out, r)
	}
	return out
}

func emitRadar(rows []RunSummary, path string) {
	r := charts.NewRadar()
	r.SetGlobalOptions(charts.WithTitleOpts(opts.Title{Title: "Score Breakdown by Submission"}))
	r.AddIndicator([]*opts.Indicator{
		{Name: "Latency"},   {Name: "Throughput"}, {Name: "Tail"},
		{Name: "Stability"}, {Name: "Correctness"},
	})
	for _, row := range rows {
		r.AddSeries(row.Submission+"/"+row.Scenario, []opts.RadarData{
			{Value: []float64{row.Latency, row.Throughput, row.Tail, row.Stability, 1.0}},
		})
	}
	f, _ := os.Create(path); defer f.Close(); _ = r.Render(f)
}

// emitCDF / emitTPS / emitGlicko — same pattern, charts.NewLine() / charts.NewLine()
```

- [ ] **Run, save SVGs.**

```bash
mkdir -p docs/benchmarks/charts
go run ./tools/charts
```

- [ ] **Commit.**

```bash
git add tools/charts/ docs/benchmarks/charts/ go.mod go.sum
git commit -m "feat(benchmarks): chart generator (latency CDF / TPS over time / score radar / Glicko evolution)"
```

---

### Task 24.3: Embed charts into the spec

- [ ] **Step 1: Add a §11 — "Benchmark Results" to the spec**, embedding the four SVGs with one paragraph each interpreting them.

- [ ] **Step 2: Regenerate the PDF**

```bash
make doc-pdf  # add this Makefile target running pandoc
```

- [ ] **Commit.**

```bash
git add docs/superpowers/specs/2026-05-10-ironbook-design.md docs/IronBook-Architecture-Blueprint.pdf
git commit -m "docs(spec): add §11 Benchmark Results with four interpreted charts"
```

---

### Task 24.4: Dry run #1 of the demo

Run the entire `docs/demo.md` script verbatim, on a fresh laptop if possible.

- [ ] **Step 1: `git clone` to a clean dir; `make demo`**
- [ ] **Step 2: Walk through the 5-min script, time each segment.**
- [ ] **Step 3: Note any failure or rough edge.** Fix immediately and re-run.

```bash
# example issues file
echo "
- 0:30 → 1:30: dashboard took 4s to load first time. Pre-warm in `make demo`.
- 2:30: chaos button latency ~2s. Fine.
- 3:30: kubectl --context=sandbox get pods returned context error. Fix: source kubeconfig path.
" > docs/demo-dry-run-issues.md
```

- [ ] **Step 4: Apply fixes; re-dry-run.** Repeat until clean within budget.

- [ ] **Commit.**

---

### Task 24.5: README polish

**Files:**
- Modify: `README.md`

Per spec §9.13 plus:

- One-line install on each major OS (macOS prereqs)
- A "what to expect" GIF (recorded from the dashboard during a run)
- Direct links to: spec PDF, ADRs, demo video, benchmark charts.

- [ ] **Commit.**

```bash
git add README.md
git commit -m "docs(readme): final polish — install matrix, expectation GIF, navigation links"
```

---

## Day 25 — Demo video + submission package (~4 tasks, ~5 hours)

### Task 25.1: Record the demo video

**Tool:** OBS Studio or QuickTime; 1080p, 60fps; voiceover via the Mac's built-in mic + a pop filter if available.

- [ ] **Step 1: Practice the script twice cold.** Time it. Aim 4:30–5:00.

- [ ] **Step 2: Set up screen capture**: 1920×1080, capture the browser + a small terminal window. Hide notifications, do-not-disturb on.

- [ ] **Step 3: Record in segments** matching `docs/demo.md` timing. Each segment ≤ 60 seconds. Re-take per segment freely.

- [ ] **Step 4: Edit in iMovie / DaVinci Resolve.** Add minimal text overlays at section transitions (`0:30 — Live Leaderboard`, `2:30 — Chaos`, etc.). No music.

- [ ] **Step 5: Export `.mp4` H.264, target 50 MB.**

- [ ] **Step 6: Upload to YouTube unlisted + commit a `docs/demo.md.url` file with the link.** Or commit the file itself if size permits via Git LFS.

```bash
git lfs install
git lfs track "*.mp4"
git add .gitattributes docs/demo.mp4
git commit -m "docs(demo): record 5-minute walkthrough video"
```

---

### Task 25.2: Submission package

**Files:**
- Create: `SUBMISSION.md` at repo root

```markdown
# IICPC Summer Hackathon 2026 — IronBook Submission

**Author:** Kartik Mehra (solo)
**Submission date:** 2026-06-?? (final week)

## Deliverables
1. **Working prototype:** this repository. Run `make demo` to bring up the full pipeline on a fresh laptop in ~5 minutes.
2. **Architecture Blueprint:** `docs/superpowers/specs/2026-05-10-ironbook-design.md` (markdown) and `docs/IronBook-Architecture-Blueprint.pdf` (PDF).
3. **Infrastructure as Code:** `deploy/terraform/` (Terraform modules for Hetzner + Wireguard) and `deploy/manifests/` (raw K8s manifests + Kustomize overlays for both clusters, reconciled by Argo CD).

## Demo
- Live URL (during judging window): https://ironbook.<your-domain>/
- Recorded walkthrough: `docs/demo.mp4` or [YouTube link]

## How to read this submission
Start with `README.md`, then `docs/superpowers/specs/2026-05-10-ironbook-design.md` §0 + §1, then `docs/adr/`. The `Makefile` has a `help` target that lists every public entry point.

## Highlights
- Two-region architecture (Mac k3d ↔ Hetzner k3s) connected by Wireguard, < €15/mo.
- Reference matching engine in Rust runs in parallel with every submission as the correctness oracle.
- Live divergence detection: leaderboard surfaces correctness in real time.
- Content-addressed Parquet replay logs make A/B comparison reproducible.
- Self-replay byte-equality CI gate proves the input pipeline is deterministic.
- Glicko-2 ratings with uncertainty bands (`μ ± φ`) over multi-scenario tournaments.
- Seven-layer submission isolation (gVisor + seccomp + AppArmor + cgroups + NetworkPolicy + iptables-backstop + admission-webhook).
- Cosign-signed images + SLSA-3 attestations + SBOM (verified at admission).
- Argo CD GitOps: every deploy is a git commit.

## Constraints respected
- Solo build, 25-day timeline (May 10 → June 3); 6-day submission buffer.
- AI-augmented for the first 12 days only; remainder solo.
- Total infra spend: ≤ €15/mo (one Hetzner CCX21 ARM VM).
```

- [ ] **Commit.**

```bash
git add SUBMISSION.md
git commit -m "docs: SUBMISSION.md hackathon submission summary"
```

---

### Task 25.3: Final reproducibility check

- [ ] **Step 1: On a fresh laptop (or a fresh user account):**
  1. Install Docker Desktop, kubectl, kind, Go, Rust, pnpm, Terraform.
  2. `git clone https://github.com/<owner>/IronBook.git && cd IronBook`
  3. `./scripts/check-prereqs.sh`
  4. `make proto && make build`
  5. `make demo`

- [ ] **Step 2: Time each step. Commit any fixes inline.**

- [ ] **Step 3: Update `README.md` with exact prereq versions** (Docker Desktop 4.30, Go 1.22.x, Rust 1.75.0, pnpm 9.x, Terraform 1.7.x).

- [ ] **Commit.**

---

### Task 25.4: Final tag + submission

- [ ] **Step 1: Tag final**

```bash
git tag -a v1.0.0-hackathon -m "IronBook hackathon submission"
git push --tags
```

- [ ] **Step 2: Create a GitHub Release attached to that tag** with the spec PDF + demo video + a one-paragraph summary.

- [ ] **Step 3: Submit per the IICPC submission portal.**

---

## Phase 5 Definition of Done

- [ ] Four marquee scenarios × four submissions = 16 runs captured into `docs/benchmarks/raw/`.
- [ ] Four charts (CDF, TPS-over-time, score radar, Glicko sparkline) generated as SVG.
- [ ] §11 (Benchmark Results) added to the spec; PDF regenerated.
- [ ] Demo video recorded (4:30–5:00), edited, uploaded; linked from `docs/demo.md`.
- [ ] README polished with install matrix, expectation GIF, navigation links.
- [ ] `SUBMISSION.md` written.
- [ ] Reproducibility check passed on a fresh laptop in ≤ 6 minutes.
- [ ] `v1.0.0-hackathon` git tag pushed.
- [ ] GitHub Release created.
- [ ] Hackathon submission filed.

---

## What to do during the submission buffer (Days 26–31)

The window between Day 25 (effective ship) and Day 31 (June 10 deadline) is *not* idle time. Use it for:

1. **Re-record the demo** if you spot anything sloppy on rewatch.
2. **Run the chaos suite live** for a few hours; spot regressions.
3. **Pre-stage a public dashboard URL** (`https://ironbook.<your-domain>/`) via Cloudflare Tunnel, so judges can poke around remotely if they wish.
4. **Stage a backup submission VM** in case Hetzner has an outage on submission day.
5. **Write a one-page "what I'd do next" reflection** for the appendix — internship interviewers will ask, and a polished answer earns points.

Resist the temptation to build new features in this window. Polish only.
