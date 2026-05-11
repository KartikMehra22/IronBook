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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// ScenarioSpec defines a deterministic benchmark scenario.
//
// The (yamlSpec, seed) pair uniquely determines the bot-coordinator's event
// schedule. The reconciler computes ContentHash = sha256(yamlSpec || seed_le)
// and stamps it onto Status; downstream consumers (replay-engine, divergence-
// detector) treat this hash as the scenario's identity.
type ScenarioSpec struct {
	// YAMLSpec is the raw scenario document. Parsed by scenario-compiler.
	// +kubebuilder:validation:MinLength=1
	YAMLSpec string `json:"yamlSpec"`

	// Seed for the deterministic PRNG that compiles yamlSpec → event schedule.
	Seed int64 `json:"seed"`

	// DurationSeconds caps the run wall-clock.
	// +kubebuilder:validation:Minimum=1
	DurationSeconds int32 `json:"durationSeconds"`

	// Targets are the per-scenario scoring targets (spec §6.6.3).
	Targets ScenarioTargets `json:"targets"`
}

// ScenarioTargets are the latency / throughput goals the scoring engine
// compares submissions against. Per scenario, not global.
type ScenarioTargets struct {
	// +kubebuilder:validation:Minimum=1
	P50Us int32 `json:"p50Us"`
	// +kubebuilder:validation:Minimum=1
	P99Us int32 `json:"p99Us"`
	// +kubebuilder:validation:Minimum=1
	TPS int32 `json:"tps"`
}

// ScenarioStatus carries the content-addressed identity computed by the
// reconciler.
type ScenarioStatus struct {
	// ContentHash = sha256(yamlSpec || seed_little_endian_8_bytes), hex.
	// Stable across reconciles. Empty until the reconciler has visited.
	// +optional
	ContentHash string `json:"contentHash,omitempty"`

	// Conditions track reconciler progress.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

// Scenario is the Schema for the scenarios API
type Scenario struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of Scenario
	// +required
	Spec ScenarioSpec `json:"spec"`

	// status defines the observed state of Scenario
	// +optional
	Status ScenarioStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// ScenarioList contains a list of Scenario
type ScenarioList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []Scenario `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Scenario{}, &ScenarioList{})
}
