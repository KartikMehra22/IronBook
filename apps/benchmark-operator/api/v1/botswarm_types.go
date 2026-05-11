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

// BotSwarmSpec defines a reusable named bot-fleet configuration. Referenced
// by BenchmarkRun.spec.botSwarmRef. The bot-coordinator pod consumes this
// at run start to size + shape its dispatch.
type BotSwarmSpec struct {
	// MaxWorkers caps the bot-worker Deployment replicas.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=64
	MaxWorkers int32 `json:"maxWorkers"`

	// Protocols enabled for this swarm. Empty = REST only.
	// +kubebuilder:validation:MinItems=1
	Protocols []string `json:"protocols"`

	// OrderMix is the distribution the schedule compiler samples from.
	OrderMix OrderMixProfile `json:"orderMix"`
}

// OrderMixProfile expresses the per-order distribution as decimal-string
// fractions (limit + market + cancel = 1.0).
type OrderMixProfile struct {
	LimitFraction  string `json:"limitFraction"`
	IocFraction    string `json:"iocFraction"`
	CancelFraction string `json:"cancelFraction"`
}

// BotSwarmStatus is intentionally minimal — BotSwarm is a configuration
// resource, not a stateful one. Conditions track reconciler visibility only.
type BotSwarmStatus struct {
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

// BotSwarm is the Schema for the botswarms API
type BotSwarm struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of BotSwarm
	// +required
	Spec BotSwarmSpec `json:"spec"`

	// status defines the observed state of BotSwarm
	// +optional
	Status BotSwarmStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// BotSwarmList contains a list of BotSwarm
type BotSwarmList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []BotSwarm `json:"items"`
}

func init() {
	SchemeBuilder.Register(&BotSwarm{}, &BotSwarmList{})
}
