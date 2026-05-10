// Package dispatcher creates K8s Jobs in the `builds` namespace to compile
// and package submitted contestant source.
package dispatcher

import (
	"context"
	"fmt"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// Inputs are the per-submission Job parameters.
type Inputs struct {
	SubmissionID string
	Sha256Hex    string // hex-encoded sha256 of the source archive
	Language     string // rust|go|cpp
}

// Dispatcher creates batch/v1 Jobs in the `builds` namespace.
type Dispatcher struct {
	Client    kubernetes.Interface
	Image     string // build-runner image (e.g. "ironbook/build-runner:dev")
	Namespace string // typically "builds"
}

// Dispatch creates the Job. Idempotent: if a Job with the same name already
// exists (e.g. submission re-uploaded after a previous build), Dispatch
// returns nil without creating a duplicate.
func (d *Dispatcher) Dispatch(ctx context.Context, in Inputs) error {
	ns := d.Namespace
	if ns == "" {
		ns = "builds"
	}
	jobName := fmt.Sprintf("build-%s", in.SubmissionID)

	// Check for an existing Job to keep the dispatch idempotent.
	if existing, err := d.Client.BatchV1().Jobs(ns).Get(ctx, jobName, metav1.GetOptions{}); err == nil {
		_ = existing
		return nil
	} else if !apierrors.IsNotFound(err) {
		return fmt.Errorf("get job: %w", err)
	}

	backoff := int32(2)
	ttl := int32(3600)
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName,
			Namespace: ns,
			Labels: map[string]string{
				"app":                  "build-runner",
				"ironbook.io/sub-id":   in.SubmissionID,
				"ironbook.io/language": in.Language,
			},
		},
		Spec: batchv1.JobSpec{
			BackoffLimit:            &backoff,
			TTLSecondsAfterFinished: &ttl,
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					RestartPolicy:      corev1.RestartPolicyNever,
					ServiceAccountName: "build-runner",
					Containers: []corev1.Container{
						{
							Name:  "runner",
							Image: d.Image,
							Env: []corev1.EnvVar{
								{Name: "IRONBOOK_SUBMISSION_ID", Value: in.SubmissionID},
								{Name: "IRONBOOK_SUBMISSION_SHA256", Value: in.Sha256Hex},
								{Name: "IRONBOOK_SUBMISSION_LANGUAGE", Value: in.Language},
								{Name: "IRONBOOK_WORK_DIR", Value: "/work"},
								{Name: "IRONBOOK_REGISTRY", Value: "registry.ironbook.svc.cluster.local:5000"},
								{Name: "IRONBOOK_MINIO_ENDPOINT", Value: "minio.ironbook.svc.cluster.local:9000"},
								{Name: "IRONBOOK_MINIO_BUCKET", Value: "submissions"},
								{Name: "IRONBOOK_MINIO_USE_SSL", Value: "false"},
								envFromSecret("IRONBOOK_MINIO_ACCESS_KEY", "minio", "user"),
								envFromSecret("IRONBOOK_MINIO_SECRET_KEY", "minio", "password"),
								envFromSecret("IRONBOOK_PG_USER", "postgres", "user"),
								envFromSecret("IRONBOOK_PG_PASSWORD", "postgres", "password"),
								{
									Name:  "IRONBOOK_POSTGRES_DSN",
									Value: "postgres://$(IRONBOOK_PG_USER):$(IRONBOOK_PG_PASSWORD)@postgres.ironbook.svc.cluster.local:5432/ironbook?sslmode=disable",
								},
							},
							VolumeMounts: []corev1.VolumeMount{
								{Name: "workdir", MountPath: "/work"},
							},
						},
					},
					Volumes: []corev1.Volume{
						{Name: "workdir", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
					},
				},
			},
		},
	}

	_, err := d.Client.BatchV1().Jobs(ns).Create(ctx, job, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("create job: %w", err)
	}
	return nil
}

// envFromSecret returns an EnvVar that maps a Secret key from the build pod's
// own namespace into the named env var. The build-runner Job runs in `builds`,
// and Day-5 ships a copy of the relevant Secrets there at apply time.
func envFromSecret(envName, secret, key string) corev1.EnvVar {
	return corev1.EnvVar{
		Name: envName,
		ValueFrom: &corev1.EnvVarSource{
			SecretKeyRef: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: secret},
				Key:                  key,
			},
		},
	}
}
