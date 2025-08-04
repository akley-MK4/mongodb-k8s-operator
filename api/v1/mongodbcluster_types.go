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
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// MongoDBClusterSpec defines the desired state of MongoDBCluster.
type MongoDBClusterSpec struct {
	Images          map[string]string `json:"images,omitempty"`
	ImagePullPolicy corev1.PullPolicy `json:"imagePullPolicy,omitempty"`

	ResourceRequirements map[string]*corev1.ResourceRequirements `json:"resourceRequirements,omitempty"`

	Shards        map[string]*MgoShardSpec        `json:"shards,omitempty"`
	ConfigServers map[string]*MgoConfigServerSpec `json:"configServers,omitempty"`
	Routers       []*MgoRouterSpec                `json:"routers,omitempty"`
}

// +kubebuilder:validation:Enum:=shard;router;configServer

type ComponentType string

const (
	ComponentTypeShard        ComponentType = "shard"
	ComponentTypeRouter       ComponentType = "router"
	ComponentTypeConfigServer ComponentType = "configServer"
)

// +kubebuilder:object:generate=true

type MgoShardSpec struct {
	// +kubebuilder:default:=255
	Priority uint8 `json:"priority,omitempty"`
	// +kubebuilder:default:=/data/db
	DataPath string `json:"dataPath,omitempty"`
	// +kubebuilder:default:=27017
	Port uint16 `json:"port,omitempty"`
}

// +kubebuilder:object:generate=true

type MgoConfigServerSpec struct {
	// +kubebuilder:default:=255
	Priority uint8 `json:"priority,omitempty"`
	// +kubebuilder:default:=/data/configdb
	DataPath string `json:"dataPath,omitempty"`
	// +kubebuilder:default:=27019
	Port uint16 `json:"port,omitempty"`
}

// +kubebuilder:object:generate=true

type MgoRouterSpec struct {
	// +kubebuilder:default:=255
	Priority uint8 `json:"priority,omitempty"`
	// +kubebuilder:default:=27017
	Port                uint16   `json:"port,omitempty"`
	ConfigServerRSNames []string `json:"configServerRSNames,omitempty"`
}

// MongoDBClusterStatus defines the observed state of MongoDBCluster.
type MongoDBClusterStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "make" to regenerate code after modifying this file
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

// MongoDBCluster is the Schema for the mongodbclusters API.
type MongoDBCluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   MongoDBClusterSpec   `json:"spec,omitempty"`
	Status MongoDBClusterStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// MongoDBClusterList contains a list of MongoDBCluster.
type MongoDBClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []MongoDBCluster `json:"items"`
}

func init() {
	SchemeBuilder.Register(&MongoDBCluster{}, &MongoDBClusterList{})
}
