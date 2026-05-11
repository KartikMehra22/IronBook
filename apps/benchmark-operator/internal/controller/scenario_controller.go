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
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"

	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	ironbookv1 "github.com/KartikMehra22/IronBook/apps/benchmark-operator/api/v1"
)

// ScenarioReconciler reconciles a Scenario object
type ScenarioReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=ironbook.ironbook.io,resources=scenarios,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=ironbook.ironbook.io,resources=scenarios/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=ironbook.ironbook.io,resources=scenarios/finalizers,verbs=update

// Reconcile computes Status.ContentHash = sha256(yamlSpec || seed_le) once
// per CR. The hash is idempotent — every consumer (replay-engine,
// divergence-detector, scoring-engine) joins on it.
func (r *ScenarioReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	var s ironbookv1.Scenario
	if err := r.Get(ctx, req.NamespacedName, &s); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if s.Status.ContentHash != "" {
		return ctrl.Result{}, nil
	}

	h := sha256.New()
	_, _ = h.Write([]byte(s.Spec.YAMLSpec))
	var seedBytes [8]byte
	binary.LittleEndian.PutUint64(seedBytes[:], uint64(s.Spec.Seed))
	_, _ = h.Write(seedBytes[:])
	s.Status.ContentHash = hex.EncodeToString(h.Sum(nil))

	if err := r.Status().Update(ctx, &s); err != nil {
		log.Error(err, "stamp ContentHash")
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *ScenarioReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&ironbookv1.Scenario{}).
		Named("scenario").
		Complete(r)
}
