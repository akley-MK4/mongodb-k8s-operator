/*
Copyright 2025.

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

// MgoDataReplicaSetSpec defines the desired state of MgoDataReplicaSet
type MgoDataReplicaSetSpec struct {
	// INSERT ADDITIONAL SPEC FIELDS - desired state of cluster
	// Important: Run "make" to regenerate code after modifying this file
	// The following markers will use OpenAPI v3 schema to validate the value
	// More info: https://book.kubebuilder.io/reference/markers/crd-validation.html

	NumSecondaryNodes uint16 `json:"numSecondaryNodes,omitempty"`
	NumArbiterNodes   uint16 `json:"numArbiterNodes,omitempty"`
	// +kubebuilder:default:=/data/db
	DataPath string `json:"dataPath,omitempty"`
	// +kubebuilder:default:=27018
	Port uint16 `json:"port,omitempty"`
	// +kubebuilder:default:=false
	EnableShard bool `json:"enableShard,omitempty"`
}

// MgoDataReplicaSetStatus defines the observed state of MgoDataReplicaSet.
type MgoDataReplicaSetStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "make" to regenerate code after modifying this file

	Conditions []metav1.Condition `json:"conditions,omitempty"`

	Initialized bool `json:"initialized,omitempty"`
	AddedShard  bool `json:"addedShard,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Initialized",type=boolean,JSONPath=`.status.initialized`
// +kubebuilder:printcolumn:name="AddedShard",type=boolean,JSONPath=`.status.addedShard`

// MgoDataReplicaSet is the Schema for the mgodatareplicasets API
type MgoDataReplicaSet struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty,omitzero"`

	// spec defines the desired state of MgoDataReplicaSet
	// +required
	Spec MgoDataReplicaSetSpec `json:"spec"`

	// status defines the observed state of MgoDataReplicaSet
	// +optional
	Status MgoDataReplicaSetStatus `json:"status,omitempty,omitzero"`
}

// +kubebuilder:object:root=true

// MgoDataReplicaSetList contains a list of MgoDataReplicaSet
type MgoDataReplicaSetList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []MgoDataReplicaSet `json:"items"`
}

func init() {
	SchemeBuilder.Register(&MgoDataReplicaSet{}, &MgoDataReplicaSetList{})
}
