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

	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	ironbookv1 "github.com/KartikMehra22/IronBook/apps/benchmark-operator/api/v1"
)

// BotSwarmReconciler reconciles a BotSwarm object
type BotSwarmReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=ironbook.ironbook.io,resources=botswarms,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=ironbook.ironbook.io,resources=botswarms/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=ironbook.ironbook.io,resources=botswarms/finalizers,verbs=update

// Reconcile is intentionally a no-op (Phase 2). BotSwarm is a configuration
// resource referenced by BenchmarkRun.spec.botSwarmRef; the BenchmarkRun
// reconciler reads the spec directly and creates pods accordingly.
// Phase 4 may add cross-CR validation (referenced protocols supported by
// build-runner image, OrderMix fractions summing to 1.0) here.
func (r *BotSwarmReconciler) Reconcile(_ context.Context, _ ctrl.Request) (ctrl.Result, error) {
	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *BotSwarmReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&ironbookv1.BotSwarm{}).
		Named("botswarm").
		Complete(r)
}
