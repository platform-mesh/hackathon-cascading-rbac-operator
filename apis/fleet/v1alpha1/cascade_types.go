/*
Copyright The Platform Mesh Authors.

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

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// CascadeSpec defines the desired state of Cascade.
type CascadeSpec struct {
	// GVK defines the GroupVersionKind of the resource to cascade.
	GVK metav1.GroupVersionKind `json:"gvk"`

	// Name defines the name of the resource to cascade.
	Name string `json:"name"`

	// Namespace defines the namespace of the resource to cascade.
	// +optional
	Namespace string `json:"namespace,omitempty"`

	// MaxDepth defines the maximum depth of cascading for this resource.
	// +optional
	// +kubebuilder:validation:Minimum=1
	MaxDepth int32 `json:"maxDepth,omitempty"`
}

// CascadeStatus defines the observed state of Cascade.
type CascadeStatus struct {
	// Conditions represent the latest available observations of the object's state
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// ObservedGeneration is the most recent generation observed for this object
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster

// Cascade allows for defining cascading behavior for Kubernetes resources.
type Cascade struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   CascadeSpec   `json:"spec,omitempty"`
	Status CascadeStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// CascadeList contains a list of Cascade.
type CascadeList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Cascade `json:"items"`
}
