//go:build e2e

// Package phase2 e2e: drives a full BenchmarkRun through PENDING → COMPLETE
// against a live kind cluster, asserting on `kubectl get` output. Requires:
//   - kind cluster up (tools/kindcluster/up.sh)
//   - operator running against $KUBECONFIG (in-cluster deploy or `go run`)
//   - dev images loaded: ironbook/{reference-oracle,fairness-gateway,bot-coordinator}:dev
//   - the Submission's status.imageDigest patched to a kind-loaded image
package phase2

import (
	"context"
	"os/exec"
	"strings"
	"testing"
	"time"
)

const (
	manifestPath = "../manifests/sample-resources.yaml"
	runName      = "run-001"
	runNamespace = "default"
	terminalWait = 3 * time.Minute
)

func kubectl(t *testing.T, args ...string) string {
	t.Helper()
	cmd := exec.CommandContext(context.Background(), "kubectl", args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("kubectl %v: %v\n%s", args, err, out)
	}
	return string(out)
}

func TestPhase2_RunReachesComplete(t *testing.T) {
	kubectl(t, "apply", "-f", manifestPath)
	t.Cleanup(func() {
		_ = exec.Command("kubectl", "delete", "-f", manifestPath, "--ignore-not-found", "--wait=false").Run()
	})

	// Submission needs Status.Phase=READY + ImageDigest before the reconciler
	// will advance past ALLOCATING. The Phase-1 build pipeline normally does
	// this; we stub it out for the E2E.
	kubectl(t, "patch", "submission", "hello-rust", "-n", runNamespace, "--subresource=status", "--type=merge",
		"-p", `{"status":{"phase":"READY","imageDigest":"ironbook/reference-oracle:dev"}}`)

	deadline := time.Now().Add(terminalWait)
	var lastPhase string
	for time.Now().Before(deadline) {
		out := kubectl(t, "get", "benchmarkrun", runName, "-n", runNamespace, "-o=jsonpath={.status.phase}")
		lastPhase = strings.TrimSpace(out)
		if lastPhase == "COMPLETE" {
			return
		}
		if lastPhase == "INVALID" || lastPhase == "ABORTED" || lastPhase == "GATEWAY_REJECT" {
			t.Fatalf("run terminated abnormally: phase=%s", lastPhase)
		}
		time.Sleep(2 * time.Second)
	}
	t.Fatalf("run did not reach COMPLETE within %s; last phase=%s", terminalWait, lastPhase)
}
