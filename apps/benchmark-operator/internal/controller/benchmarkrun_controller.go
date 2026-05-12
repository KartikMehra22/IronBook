/*
Copyright 2026 Kartik Mehra.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

// Package controller hosts the BenchmarkRun reconciler — the spec §2.2 state
// machine. PENDING → ALLOCATING → PRIMING → RUNNING → DRAINING → COMPLETE,
// with terminal branches INVALID / INSUFFICIENT_CAPACITY / GATEWAY_REJECT /
// ABORTED. The reconciler is purely a function of CR state; resuming
// mid-reconcile after an operator restart is safe.
package controller

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	ironbookv1 "github.com/KartikMehra22/IronBook/apps/benchmark-operator/api/v1"
)

const (
	ironbookFinalizer = "ironbook.io/run-cleanup"

	labelRun  = "ironbook.io/run"
	labelRole = "ironbook.io/role"

	roleOracle      = "reference-oracle"
	roleSubmission  = "submission"
	roleGateway     = "fairness-gateway"
	roleCoordinator = "bot-coordinator"

	// Phase 2 images — built locally + kind-loaded. Phase 4 swaps for
	// sigstore-signed @sha256: refs from the in-cluster registry.
	imgOracle      = "ironbook/reference-oracle:dev"
	imgGateway     = "ironbook/fairness-gateway:dev"
	imgCoordinator = "ironbook/bot-coordinator:dev"
)

// BenchmarkRunReconciler reconciles a BenchmarkRun object.
type BenchmarkRunReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=ironbook.ironbook.io,resources=benchmarkruns,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=ironbook.ironbook.io,resources=benchmarkruns/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=ironbook.ironbook.io,resources=benchmarkruns/finalizers,verbs=update
// +kubebuilder:rbac:groups=ironbook.ironbook.io,resources=submissions,verbs=get;list;watch
// +kubebuilder:rbac:groups=ironbook.ironbook.io,resources=scenarios,verbs=get;list;watch
// +kubebuilder:rbac:groups=ironbook.ironbook.io,resources=botswarms,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;create;update;patch;delete

// Reconcile drives the state machine. Each transition method is idempotent.
func (r *BenchmarkRunReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx).WithValues("run", req.NamespacedName)
	var br ironbookv1.BenchmarkRun
	if err := r.Get(ctx, req.NamespacedName, &br); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if !br.DeletionTimestamp.IsZero() {
		return r.handleDelete(ctx, &br)
	}
	if !controllerutil.ContainsFinalizer(&br, ironbookFinalizer) {
		controllerutil.AddFinalizer(&br, ironbookFinalizer)
		return ctrl.Result{}, r.Update(ctx, &br)
	}

	switch br.Status.Phase {
	case "":
		return r.toPending(ctx, &br)
	case "PENDING":
		return r.toAllocating(ctx, &br)
	case "ALLOCATING":
		return r.toPriming(ctx, &br)
	case "PRIMING":
		return r.observePriming(ctx, &br)
	case "RUNNING":
		return r.observeRunning(ctx, &br)
	case "DRAINING":
		return r.toComplete(ctx, &br)
	case "COMPLETE", "ABORTED", "INVALID", "INSUFFICIENT_CAPACITY", "GATEWAY_REJECT":
		return ctrl.Result{}, nil
	default:
		log.Error(nil, "unknown phase", "phase", br.Status.Phase)
		return ctrl.Result{}, fmt.Errorf("unknown phase %q", br.Status.Phase)
	}
}

// --- state transitions ---------------------------------------------------

func (r *BenchmarkRunReconciler) toPending(ctx context.Context, br *ironbookv1.BenchmarkRun) (ctrl.Result, error) {
	br.Status.Phase = "PENDING"
	meta.SetStatusCondition(&br.Status.Conditions, metav1.Condition{
		Type: "Initialized", Status: metav1.ConditionTrue, Reason: "Created",
		Message: "BenchmarkRun accepted",
	})
	return ctrl.Result{Requeue: true}, r.Status().Update(ctx, br)
}

func (r *BenchmarkRunReconciler) toAllocating(ctx context.Context, br *ironbookv1.BenchmarkRun) (ctrl.Result, error) {
	var sub ironbookv1.Submission
	if err := r.Get(ctx, types.NamespacedName{Namespace: br.Namespace, Name: br.Spec.SubmissionRef.Name}, &sub); err != nil {
		return r.toInvalid(ctx, br, fmt.Sprintf("submission %q not found: %v", br.Spec.SubmissionRef.Name, err))
	}
	if sub.Status.Phase != "READY" {
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	var scn ironbookv1.Scenario
	if err := r.Get(ctx, types.NamespacedName{Namespace: br.Namespace, Name: br.Spec.ScenarioRef.Name}, &scn); err != nil {
		return r.toInvalid(ctx, br, fmt.Sprintf("scenario %q not found: %v", br.Spec.ScenarioRef.Name, err))
	}
	if scn.Status.ContentHash == "" {
		return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
	}

	var swarm ironbookv1.BotSwarm
	if err := r.Get(ctx, types.NamespacedName{Namespace: br.Namespace, Name: br.Spec.BotSwarmRef.Name}, &swarm); err != nil {
		return r.toInvalid(ctx, br, fmt.Sprintf("botswarm %q not found: %v", br.Spec.BotSwarmRef.Name, err))
	}

	br.Status.Phase = "ALLOCATING"
	meta.SetStatusCondition(&br.Status.Conditions, metav1.Condition{
		Type: "RefsValidated", Status: metav1.ConditionTrue, Reason: "AllRefsReady",
	})
	return ctrl.Result{Requeue: true}, r.Status().Update(ctx, br)
}

func (r *BenchmarkRunReconciler) toPriming(ctx context.Context, br *ironbookv1.BenchmarkRun) (ctrl.Result, error) {
	var sub ironbookv1.Submission
	if err := r.Get(ctx, types.NamespacedName{Namespace: br.Namespace, Name: br.Spec.SubmissionRef.Name}, &sub); err != nil {
		return ctrl.Result{}, err
	}
	var scn ironbookv1.Scenario
	if err := r.Get(ctx, types.NamespacedName{Namespace: br.Namespace, Name: br.Spec.ScenarioRef.Name}, &scn); err != nil {
		return ctrl.Result{}, err
	}

	// Endpoints stamped before gateway pod is templated — gateway env reads them.
	br.Status.OracleEndpoint = fmt.Sprintf("%s-%s.%s.svc:7080", roleOracle, br.Name, br.Namespace)
	br.Status.SubmissionEndpoint = fmt.Sprintf("%s-%s.%s.svc:7777", roleSubmission, br.Name, br.Namespace)
	br.Status.GatewayEndpoint = fmt.Sprintf("%s-%s.%s.svc:8080", roleGateway, br.Name, br.Namespace)

	for _, fn := range []func(context.Context, *ironbookv1.BenchmarkRun, *ironbookv1.Submission, *ironbookv1.Scenario) error{
		r.ensureOraclePod,
		r.ensureSubmissionPod,
		r.ensureGatewayPod,
		r.ensureCoordinatorPod,
	} {
		if err := fn(ctx, br, &sub, &scn); err != nil {
			return ctrl.Result{}, err
		}
	}

	br.Status.Phase = "PRIMING"
	meta.SetStatusCondition(&br.Status.Conditions, metav1.Condition{
		Type: "PodsAllocated", Status: metav1.ConditionTrue, Reason: "PodsCreated",
	})
	return ctrl.Result{Requeue: true}, r.Status().Update(ctx, br)
}

func (r *BenchmarkRunReconciler) observePriming(ctx context.Context, br *ironbookv1.BenchmarkRun) (ctrl.Result, error) {
	allReady, err := r.allOwnedPodsReady(ctx, br)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !allReady {
		return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
	}
	now := metav1.Now()
	br.Status.Phase = "RUNNING"
	br.Status.StartedAt = &now
	meta.SetStatusCondition(&br.Status.Conditions, metav1.Condition{
		Type: "PodsReady", Status: metav1.ConditionTrue, Reason: "AllReady",
	})
	return ctrl.Result{Requeue: true}, r.Status().Update(ctx, br)
}

func (r *BenchmarkRunReconciler) observeRunning(ctx context.Context, br *ironbookv1.BenchmarkRun) (ctrl.Result, error) {
	// Phase 2 signal: coordinator Pod reaches Succeeded → run drains.
	// Phase 3 swaps to a Redpanda "all events flushed" signal.
	pods, err := r.listOwnedPods(ctx, br)
	if err != nil {
		return ctrl.Result{}, err
	}
	for i := range pods {
		p := &pods[i]
		if p.Labels[labelRole] == roleCoordinator && p.Status.Phase == corev1.PodSucceeded {
			br.Status.Phase = "DRAINING"
			meta.SetStatusCondition(&br.Status.Conditions, metav1.Condition{
				Type: "ScheduleDispatched", Status: metav1.ConditionTrue, Reason: "CoordinatorDone",
			})
			return ctrl.Result{Requeue: true}, r.Status().Update(ctx, br)
		}
	}
	return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
}

func (r *BenchmarkRunReconciler) toComplete(ctx context.Context, br *ironbookv1.BenchmarkRun) (ctrl.Result, error) {
	now := metav1.Now()
	br.Status.Phase = "COMPLETE"
	br.Status.EndedAt = &now
	meta.SetStatusCondition(&br.Status.Conditions, metav1.Condition{
		Type: "Completed", Status: metav1.ConditionTrue, Reason: "DrainSuccessful",
	})
	return ctrl.Result{}, r.Status().Update(ctx, br)
}

func (r *BenchmarkRunReconciler) toInvalid(ctx context.Context, br *ironbookv1.BenchmarkRun, reason string) (ctrl.Result, error) {
	br.Status.Phase = "INVALID"
	meta.SetStatusCondition(&br.Status.Conditions, metav1.Condition{
		Type: "RefsValidated", Status: metav1.ConditionFalse, Reason: "MissingRef", Message: reason,
	})
	return ctrl.Result{}, r.Status().Update(ctx, br)
}

func (r *BenchmarkRunReconciler) handleDelete(ctx context.Context, br *ironbookv1.BenchmarkRun) (ctrl.Result, error) {
	// Foreground cascade waits on finalizer removal, which causes a deadlock
	// if we wait on the cascade. Actively delete owned pods, then drop the
	// finalizer once the API server confirms they're gone.
	pods, err := r.listOwnedPods(ctx, br)
	if err != nil {
		return ctrl.Result{}, err
	}
	if len(pods) > 0 {
		for i := range pods {
			if pods[i].DeletionTimestamp.IsZero() {
				if err := r.Delete(ctx, &pods[i]); err != nil && !apierrors.IsNotFound(err) {
					return ctrl.Result{}, err
				}
			}
		}
		return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
	}
	controllerutil.RemoveFinalizer(br, ironbookFinalizer)
	return ctrl.Result{}, r.Update(ctx, br)
}

// --- pod templates -------------------------------------------------------

func (r *BenchmarkRunReconciler) ensureOraclePod(ctx context.Context, br *ironbookv1.BenchmarkRun, _ *ironbookv1.Submission, _ *ironbookv1.Scenario) error {
	return r.ensurePod(ctx, br, &corev1.Pod{
		ObjectMeta: r.podMeta(br, roleOracle),
		Spec: corev1.PodSpec{
			RestartPolicy:   corev1.RestartPolicyNever,
			SecurityContext: nonRootPodSecurityContext(),
			Containers: []corev1.Container{{
				Name:            "oracle",
				Image:           imgOracle,
				ImagePullPolicy: corev1.PullIfNotPresent,
				Ports:           []corev1.ContainerPort{{Name: "grpc", ContainerPort: 7080}},
				SecurityContext: hardenedContainerSecurityContext(),
				Resources:       smallResources(),
			}},
		},
	})
}

func (r *BenchmarkRunReconciler) ensureSubmissionPod(ctx context.Context, br *ironbookv1.BenchmarkRun, sub *ironbookv1.Submission, _ *ironbookv1.Scenario) error {
	return r.ensurePod(ctx, br, &corev1.Pod{
		ObjectMeta: r.podMeta(br, roleSubmission),
		Spec: corev1.PodSpec{
			RestartPolicy:   corev1.RestartPolicyNever,
			SecurityContext: nonRootPodSecurityContext(),
			Containers: []corev1.Container{{
				Name:            "engine",
				Image:           sub.Status.ImageDigest,
				ImagePullPolicy: corev1.PullIfNotPresent,
				Ports:           []corev1.ContainerPort{{Name: "engine", ContainerPort: 7777}},
				SecurityContext: hardenedContainerSecurityContext(),
				Resources:       smallResources(),
			}},
		},
	})
}

func (r *BenchmarkRunReconciler) ensureGatewayPod(ctx context.Context, br *ironbookv1.BenchmarkRun, _ *ironbookv1.Submission, _ *ironbookv1.Scenario) error {
	return r.ensurePod(ctx, br, &corev1.Pod{
		ObjectMeta: r.podMeta(br, roleGateway),
		Spec: corev1.PodSpec{
			RestartPolicy:   corev1.RestartPolicyNever,
			SecurityContext: nonRootPodSecurityContext(),
			Containers: []corev1.Container{{
				Name:            "gateway",
				Image:           imgGateway,
				ImagePullPolicy: corev1.PullIfNotPresent,
				Ports:           []corev1.ContainerPort{{Name: "http", ContainerPort: 8080}},
				SecurityContext: hardenedContainerSecurityContext(),
				Env: []corev1.EnvVar{
					{Name: "IRONBOOK_HTTP_ADDR", Value: ":8080"},
					{Name: "IRONBOOK_TIME_SERVICE", Value: "time-service.ironbook.svc.cluster.local:7070"},
					{Name: "IRONBOOK_SUBMISSION_ENDPOINT", Value: br.Status.SubmissionEndpoint},
					{Name: "IRONBOOK_ORACLE_ENDPOINT", Value: br.Status.OracleEndpoint},
					{Name: "IRONBOOK_EVENT_LOG_PATH", Value: "/tmp/events.jsonl"},
				},
				Resources: smallResources(),
			}},
		},
	})
}

func (r *BenchmarkRunReconciler) ensureCoordinatorPod(ctx context.Context, br *ironbookv1.BenchmarkRun, _ *ironbookv1.Submission, scn *ironbookv1.Scenario) error {
	return r.ensurePod(ctx, br, &corev1.Pod{
		ObjectMeta: r.podMeta(br, roleCoordinator),
		Spec: corev1.PodSpec{
			RestartPolicy:   corev1.RestartPolicyNever,
			SecurityContext: nonRootPodSecurityContext(),
			Containers: []corev1.Container{{
				Name:            "coordinator",
				Image:           imgCoordinator,
				ImagePullPolicy: corev1.PullIfNotPresent,
				SecurityContext: hardenedContainerSecurityContext(),
				Env: []corev1.EnvVar{
					{Name: "IRONBOOK_SCENARIO_YAML", Value: scn.Spec.YAMLSpec},
					{Name: "IRONBOOK_SCENARIO_SEED", Value: fmt.Sprintf("%d", br.Spec.Seed)},
					{Name: "IRONBOOK_DURATION_SECONDS", Value: fmt.Sprintf("%d", scn.Spec.DurationSeconds)},
					{Name: "IRONBOOK_GATEWAY_URL", Value: "http://" + br.Status.GatewayEndpoint},
				},
				Resources: smallResources(),
			}},
		},
	})
}

// ensurePod creates the pod if missing and is otherwise a no-op.
func (r *BenchmarkRunReconciler) ensurePod(ctx context.Context, br *ironbookv1.BenchmarkRun, pod *corev1.Pod) error {
	if err := controllerutil.SetControllerReference(br, pod, r.Scheme); err != nil {
		return err
	}
	if err := r.Create(ctx, pod); err != nil && !apierrors.IsAlreadyExists(err) {
		return err
	}
	return nil
}

// --- helpers -------------------------------------------------------------

func (r *BenchmarkRunReconciler) podMeta(br *ironbookv1.BenchmarkRun, role string) metav1.ObjectMeta {
	return metav1.ObjectMeta{
		Name:      fmt.Sprintf("%s-%s", role, br.Name),
		Namespace: br.Namespace,
		Labels: map[string]string{
			labelRun:  br.Name,
			labelRole: role,
		},
	}
}

func (r *BenchmarkRunReconciler) listOwnedPods(ctx context.Context, br *ironbookv1.BenchmarkRun) ([]corev1.Pod, error) {
	var list corev1.PodList
	if err := r.List(ctx, &list,
		client.InNamespace(br.Namespace),
		client.MatchingLabels{labelRun: br.Name},
	); err != nil {
		return nil, err
	}
	return list.Items, nil
}

func (r *BenchmarkRunReconciler) allOwnedPodsReady(ctx context.Context, br *ironbookv1.BenchmarkRun) (bool, error) {
	pods, err := r.listOwnedPods(ctx, br)
	if err != nil {
		return false, err
	}
	// Expect oracle + submission + gateway + coordinator. The coordinator is
	// RestartPolicyNever and may already be Succeeded; treat anything other
	// than Pending as "ready" for the gate.
	if len(pods) < 4 {
		return false, nil
	}
	for i := range pods {
		p := &pods[i]
		if p.Labels[labelRole] == roleCoordinator {
			if p.Status.Phase == corev1.PodPending {
				return false, nil
			}
			continue
		}
		if !podReady(p) {
			return false, nil
		}
	}
	return true, nil
}

func podReady(p *corev1.Pod) bool {
	for _, c := range p.Status.Conditions {
		if c.Type == corev1.PodReady && c.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func nonRootPodSecurityContext() *corev1.PodSecurityContext {
	uid := int64(65532)
	nonRoot := true
	return &corev1.PodSecurityContext{
		RunAsNonRoot: &nonRoot,
		RunAsUser:    &uid,
		RunAsGroup:   &uid,
		SeccompProfile: &corev1.SeccompProfile{
			Type: corev1.SeccompProfileTypeRuntimeDefault,
		},
	}
}

func hardenedContainerSecurityContext() *corev1.SecurityContext {
	f := false
	t := true
	return &corev1.SecurityContext{
		AllowPrivilegeEscalation: &f,
		ReadOnlyRootFilesystem:   &t,
		Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
	}
}

func smallResources() corev1.ResourceRequirements {
	return corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("100m"),
			corev1.ResourceMemory: resource.MustParse("128Mi"),
		},
		Limits: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("500m"),
			corev1.ResourceMemory: resource.MustParse("512Mi"),
		},
	}
}

// SetupWithManager wires the controller into the manager.
func (r *BenchmarkRunReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&ironbookv1.BenchmarkRun{}).
		Owns(&corev1.Pod{}).
		Named("benchmarkrun").
		Complete(r)
}
