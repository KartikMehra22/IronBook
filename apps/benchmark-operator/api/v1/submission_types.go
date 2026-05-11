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

// SubmissionSpec defines the desired state of Submission.
//
// The CR mirrors the row that submission-api writes to Postgres on upload:
// the source archive's sha256 is the immutable identity; the language drives
// which build template is used; the image_digest fills in after the build
// runner completes.
type SubmissionSpec struct {
	// Sha256 of the uploaded source archive, hex-encoded (64 chars).
	// +kubebuilder:validation:Pattern="^[0-9a-f]{64}$"
	Sha256 string `json:"sha256"`

	// Language used to build the submission. Determines the build template.
	// +kubebuilder:validation:Enum=rust;go;cpp
	Language string `json:"language"`
}

// SubmissionStatus mirrors the build pipeline state. Phase ∈ {PENDING,
// BUILDING, READY, REJECTED} matches the Postgres submissions.status column;
// ImageDigest is the @sha256:... ref that the build-runner pushes to the
// in-cluster registry on success.
type SubmissionStatus struct {
	// Phase is the current lifecycle state.
	// +kubebuilder:validation:Enum=PENDING;BUILDING;READY;REJECTED
	// +optional
	Phase string `json:"phase,omitempty"`

	// ImageDigest is the @sha256:... ref pushed to the in-cluster registry
	// once the build-runner has signed + attested the image.
	// +optional
	ImageDigest string `json:"imageDigest,omitempty"`

	// RejectReason is populated when Phase = REJECTED. Surface to the user.
	// +optional
	RejectReason string `json:"rejectReason,omitempty"`

	// Conditions track lifecycle transitions (Initialized, Building, Ready).
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

// Submission is the Schema for the submissions API
type Submission struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of Submission
	// +required
	Spec SubmissionSpec `json:"spec"`

	// status defines the observed state of Submission
	// +optional
	Status SubmissionStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// SubmissionList contains a list of Submission
type SubmissionList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []Submission `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Submission{}, &SubmissionList{})
}
