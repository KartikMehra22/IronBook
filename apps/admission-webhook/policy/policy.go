// Package policy enforces IronBook submission-pod constraints.
//
// Per ADR-011, on the kind/Mac demo host gVisor (Layer 2) is inactive
// but the contract is still enforced for production submission pods.
// Test smoke pods labelled `ironbook.io/test=true` are exempted from the
// runtimeClass + image-digest checks.
package policy

import (
	"strings"

	corev1 "k8s.io/api/core/v1"
)

// Result is the webhook's per-pod decision.
type Result struct {
	Allowed bool
	Reason  string
}

// Validate enforces the IronBook submission-pod constraints. It returns
// `Allowed: true` for pods that aren't in the submissions namespace, so the
// webhook can stay attached to a wider scope without false positives.
func Validate(pod *corev1.Pod) Result {
	// Only enforce on pods labeled `app=submission`.
	if pod.Labels["app"] != "submission" {
		return Result{Allowed: true}
	}
	// Test/smoke pods are exempted from the runtimeClass + image digest checks.
	isTest := pod.Labels["ironbook.io/test"] == "true"

	if pod.Namespace != "submissions" {
		return reject("submission pods must live in the `submissions` namespace")
	}

	if pod.Spec.HostNetwork {
		return reject("hostNetwork must be false")
	}
	if pod.Spec.HostPID {
		return reject("hostPID must be false")
	}
	if pod.Spec.HostIPC {
		return reject("hostIPC must be false")
	}

	if !isTest {
		if pod.Spec.RuntimeClassName == nil || *pod.Spec.RuntimeClassName != "gvisor" {
			return reject("submission pods must use runtimeClassName: gvisor")
		}
	}

	if pod.Spec.SecurityContext == nil ||
		pod.Spec.SecurityContext.RunAsNonRoot == nil ||
		!*pod.Spec.SecurityContext.RunAsNonRoot {
		return reject("pod-level securityContext.runAsNonRoot must be true")
	}

	if len(pod.Spec.Containers) == 0 {
		return reject("submission pod must declare at least one container")
	}

	for _, c := range pod.Spec.Containers {
		if !isTest {
			if !strings.Contains(c.Image, "@sha256:") {
				return reject("container images must be pinned by digest (@sha256:...)")
			}
		}
		if c.SecurityContext == nil {
			return reject("container missing securityContext")
		}
		if c.SecurityContext.AllowPrivilegeEscalation == nil ||
			*c.SecurityContext.AllowPrivilegeEscalation {
			return reject("allowPrivilegeEscalation must be false")
		}
		if c.SecurityContext.ReadOnlyRootFilesystem == nil ||
			!*c.SecurityContext.ReadOnlyRootFilesystem {
			return reject("readOnlyRootFilesystem must be true")
		}
		if c.SecurityContext.Capabilities == nil ||
			!containsCap(c.SecurityContext.Capabilities.Drop, "ALL") {
			return reject("capabilities.drop must include ALL")
		}
	}

	for _, v := range pod.Spec.Volumes {
		if v.HostPath != nil {
			return reject("hostPath volumes are not allowed in submission pods")
		}
	}

	return Result{Allowed: true}
}

func reject(reason string) Result {
	return Result{Reason: reason}
}

func containsCap(have []corev1.Capability, want corev1.Capability) bool {
	for _, c := range have {
		if c == want {
			return true
		}
	}
	return false
}
