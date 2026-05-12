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

package v1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// BenchmarkRunSpec is one benchmark execution: a Submission + Scenario +
// BotSwarm triple plus a seed. The reconciler allocates the four hot-path
// pods (oracle, submission, gateway, coordinator), drives the state
// machine in spec §2.2, and cleans up via finalizer.
type BenchmarkRunSpec struct {
	// SubmissionRef points at a Submission in the SAME namespace that has
	// reached Status.Phase = READY. The reconciler stalls (re-queues) until
	// the build pipeline produces an ImageDigest.
	SubmissionRef corev1.LocalObjectReference `json:"submissionRef"`

	// ScenarioRef points at a Scenario whose Status.ContentHash has been
	// computed by the Scenario controller.
	ScenarioRef corev1.LocalObjectReference `json:"scenarioRef"`

	// BotSwarmRef selects the bot fleet shape.
	BotSwarmRef corev1.LocalObjectReference `json:"botSwarmRef"`

	// Seed is mixed with Scenario.ContentHash to derive the per-run
	// deterministic event schedule the bot-coordinator emits.
	Seed int64 `json:"seed"`
}

// BenchmarkRunStatus is the spec §2.2 state machine.
type BenchmarkRunStatus struct {
	// Phase is the current lifecycle state. Transitions are one-way; once a
	// terminal state (COMPLETE / ABORTED / INVALID / INSUFFICIENT_CAPACITY /
	// GATEWAY_REJECT) is reached, the reconciler is a no-op.
	// +kubebuilder:validation:Enum=PENDING;ALLOCATING;PRIMING;RUNNING;DRAINING;COMPLETE;ABORTED;INVALID;INSUFFICIENT_CAPACITY;GATEWAY_REJECT
	// +optional
	Phase string `json:"phase,omitempty"`

	// StartedAt is the wall-clock time the reconciler entered RUNNING.
	// +optional
	StartedAt *metav1.Time `json:"startedAt,omitempty"`

	// EndedAt is the wall-clock time the reconciler entered COMPLETE.
	// +optional
	EndedAt *metav1.Time `json:"endedAt,omitempty"`

	// SubmissionEndpoint is the host:port the gateway forwards orders to.
	// +optional
	SubmissionEndpoint string `json:"submissionEndpoint,omitempty"`

	// OracleEndpoint is the host:port the gateway tees orders to.
	// +optional
	OracleEndpoint string `json:"oracleEndpoint,omitempty"`

	// GatewayEndpoint is the host:port bots connect to.
	// +optional
	GatewayEndpoint string `json:"gatewayEndpoint,omitempty"`

	// Conditions track per-step state (RefsValidated, PodsAllocated,
	// PodsReady, BotsDispatched, TelemetryFlushed, …).
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Submission",type=string,JSONPath=`.spec.submissionRef.name`
// +kubebuilder:printcolumn:name="Scenario",type=string,JSONPath=`.spec.scenarioRef.name`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

// BenchmarkRun is the Schema for the benchmarkruns API
type BenchmarkRun struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of BenchmarkRun
	// +required
	Spec BenchmarkRunSpec `json:"spec"`

	// status defines the observed state of BenchmarkRun
	// +optional
	Status BenchmarkRunStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// BenchmarkRunList contains a list of BenchmarkRun
type BenchmarkRunList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []BenchmarkRun `json:"items"`
}

func init() {
	SchemeBuilder.Register(&BenchmarkRun{}, &BenchmarkRunList{})
}
