/*
Copyright 2026 Kartik Mehra.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	ironbookv1 "github.com/KartikMehra22/IronBook/apps/benchmark-operator/api/v1"
)

// SubmissionReconciler reconciles a Submission object
type SubmissionReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=ironbook.ironbook.io,resources=submissions,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=ironbook.ironbook.io,resources=submissions/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=ironbook.ironbook.io,resources=submissions/finalizers,verbs=update

// Reconcile mirrors what submission-api writes to Postgres.
//
// The CR's lifecycle is owned externally: submission-api creates a Submission
// when source is uploaded; build-runner flips Phase to BUILDING → READY (or
// REJECTED). This reconciler's sole job is to ensure that newly-created CRs
// with an empty Phase get bumped to PENDING and an Initialized Condition
// gets recorded, so `kubectl describe submission` shows lifecycle history.
func (r *SubmissionReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	var sub ironbookv1.Submission
	if err := r.Get(ctx, req.NamespacedName, &sub); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if sub.Status.Phase != "" {
		return ctrl.Result{}, nil
	}
	sub.Status.Phase = "PENDING"
	meta.SetStatusCondition(&sub.Status.Conditions, metav1.Condition{
		Type:    "Initialized",
		Status:  metav1.ConditionTrue,
		Reason:  "Created",
		Message: "Submission accepted; awaiting build",
	})
	if err := r.Status().Update(ctx, &sub); err != nil {
		log.Error(err, "update status to PENDING")
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *SubmissionReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&ironbookv1.Submission{}).
		Named("submission").
		Complete(r)
}
