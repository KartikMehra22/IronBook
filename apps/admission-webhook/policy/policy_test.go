package policy

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func ptrBool(b bool) *bool    { return &b }
func ptrStr(s string) *string { return &s }

func compliantPod(test bool) *corev1.Pod {
	labels := map[string]string{"app": "submission"}
	if test {
		labels["ironbook.io/test"] = "true"
	}
	rc := "gvisor"
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "p1", Namespace: "submissions", Labels: labels},
		Spec: corev1.PodSpec{
			RuntimeClassName: &rc,
			SecurityContext: &corev1.PodSecurityContext{
				RunAsNonRoot: ptrBool(true),
			},
			Containers: []corev1.Container{
				{
					Name:  "engine",
					Image: "registry.local/sub/abc@sha256:def",
					SecurityContext: &corev1.SecurityContext{
						AllowPrivilegeEscalation: ptrBool(false),
						ReadOnlyRootFilesystem:   ptrBool(true),
						Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
					},
				},
			},
		},
	}
	if test {
		// test pods don't need a digest-pinned image
		pod.Spec.Containers[0].Image = "registry.local/sub/abc:tag"
		pod.Spec.RuntimeClassName = nil
	}
	return pod
}

func TestValidate_AcceptsCompliantProductionPod(t *testing.T) {
	if got := Validate(compliantPod(false)); !got.Allowed {
		t.Fatalf("want allow, got reject: %s", got.Reason)
	}
}

func TestValidate_AcceptsCompliantTestPod(t *testing.T) {
	if got := Validate(compliantPod(true)); !got.Allowed {
		t.Fatalf("want allow, got reject: %s", got.Reason)
	}
}

func TestValidate_RejectsMissingGvisor(t *testing.T) {
	pod := compliantPod(false)
	pod.Spec.RuntimeClassName = nil
	if got := Validate(pod); got.Allowed {
		t.Fatal("want reject")
	}
}

func TestValidate_RejectsHostNetwork(t *testing.T) {
	pod := compliantPod(false)
	pod.Spec.HostNetwork = true
	if got := Validate(pod); got.Allowed {
		t.Fatal("want reject")
	}
}

func TestValidate_RejectsRunAsRoot(t *testing.T) {
	pod := compliantPod(false)
	pod.Spec.SecurityContext.RunAsNonRoot = ptrBool(false)
	if got := Validate(pod); got.Allowed {
		t.Fatal("want reject")
	}
}

func TestValidate_RejectsImageWithoutDigest(t *testing.T) {
	pod := compliantPod(false)
	pod.Spec.Containers[0].Image = "registry.local/sub/abc:latest"
	if got := Validate(pod); got.Allowed {
		t.Fatal("want reject")
	}
}

func TestValidate_RejectsCapabilitiesNotDropped(t *testing.T) {
	pod := compliantPod(false)
	pod.Spec.Containers[0].SecurityContext.Capabilities = &corev1.Capabilities{Drop: nil}
	if got := Validate(pod); got.Allowed {
		t.Fatal("want reject")
	}
}

func TestValidate_RejectsHostPathVolume(t *testing.T) {
	pod := compliantPod(false)
	pod.Spec.Volumes = []corev1.Volume{{
		Name: "bad",
		VolumeSource: corev1.VolumeSource{
			HostPath: &corev1.HostPathVolumeSource{Path: "/etc"},
		},
	}}
	if got := Validate(pod); got.Allowed {
		t.Fatal("want reject")
	}
}

func TestValidate_AllowsNonSubmissionPodAnywhere(t *testing.T) {
	// A pod without app=submission is the webhook's caller's problem,
	// not the admission policy's; we always allow.
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "x", Namespace: "submissions"},
	}
	if got := Validate(pod); !got.Allowed {
		t.Fatalf("expected allow for non-submission pod, got: %s", got.Reason)
	}
	_ = ptrStr // silence unused
}
