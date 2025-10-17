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
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	mongodbv1 "github.com/akley-MK4/mongodb-k8s-operator/api/v1"
)

// nolint:unused
// log is for logging in this package.
var mgodatareplicasetlog = logf.Log.WithName("mgodatareplicaset-resource")

// SetupMgoDataReplicaSetWebhookWithManager registers the webhook for MgoDataReplicaSet in the manager.
func SetupMgoDataReplicaSetWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr).For(&mongodbv1.MgoDataReplicaSet{}).
		WithValidator(&MgoDataReplicaSetCustomValidator{}).
		WithDefaulter(&MgoDataReplicaSetCustomDefaulter{}).
		Complete()
}

// TODO(user): EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!

// +kubebuilder:webhook:path=/mutate-mongodb-akleymk4-com-v1-mgodatareplicaset,mutating=true,failurePolicy=fail,sideEffects=None,groups=mongodb.akleymk4.com,resources=mgodatareplicasets,verbs=create;update,versions=v1,name=mmgodatareplicaset-v1.kb.io,admissionReviewVersions=v1

// MgoDataReplicaSetCustomDefaulter struct is responsible for setting default values on the custom resource of the
// Kind MgoDataReplicaSet when those are created or updated.
//
// NOTE: The +kubebuilder:object:generate=false marker prevents controller-gen from generating DeepCopy methods,
// as it is used only for temporary operations and does not need to be deeply copied.
type MgoDataReplicaSetCustomDefaulter struct {
	// TODO(user): Add more fields as needed for defaulting
}

var _ webhook.CustomDefaulter = &MgoDataReplicaSetCustomDefaulter{}

// Default implements webhook.CustomDefaulter so a webhook will be registered for the Kind MgoDataReplicaSet.
func (d *MgoDataReplicaSetCustomDefaulter) Default(_ context.Context, obj runtime.Object) error {
	mgodatareplicaset, ok := obj.(*mongodbv1.MgoDataReplicaSet)

	if !ok {
		return fmt.Errorf("expected an MgoDataReplicaSet object but got %T", obj)
	}
	mgodatareplicasetlog.Info("Defaulting for MgoDataReplicaSet", "name", mgodatareplicaset.GetName())

	// TODO(user): fill in your defaulting logic.

	return nil
}

// TODO(user): change verbs to "verbs=create;update;delete" if you want to enable deletion validation.
// NOTE: The 'path' attribute must follow a specific pattern and should not be modified directly here.
// Modifying the path for an invalid path can cause API server errors; failing to locate the webhook.
// +kubebuilder:webhook:path=/validate-mongodb-akleymk4-com-v1-mgodatareplicaset,mutating=false,failurePolicy=fail,sideEffects=None,groups=mongodb.akleymk4.com,resources=mgodatareplicasets,verbs=create;update,versions=v1,name=vmgodatareplicaset-v1.kb.io,admissionReviewVersions=v1

// MgoDataReplicaSetCustomValidator struct is responsible for validating the MgoDataReplicaSet resource
// when it is created, updated, or deleted.
//
// NOTE: The +kubebuilder:object:generate=false marker prevents controller-gen from generating DeepCopy methods,
// as this struct is used only for temporary operations and does not need to be deeply copied.
type MgoDataReplicaSetCustomValidator struct {
	// TODO(user): Add more fields as needed for validation
}

var _ webhook.CustomValidator = &MgoDataReplicaSetCustomValidator{}

// ValidateCreate implements webhook.CustomValidator so a webhook will be registered for the type MgoDataReplicaSet.
func (v *MgoDataReplicaSetCustomValidator) ValidateCreate(_ context.Context, obj runtime.Object) (admission.Warnings, error) {
	mgodatareplicaset, ok := obj.(*mongodbv1.MgoDataReplicaSet)
	if !ok {
		return nil, fmt.Errorf("expected a MgoDataReplicaSet object but got %T", obj)
	}
	mgodatareplicasetlog.Info("Validation for MgoDataReplicaSet upon creation", "name", mgodatareplicaset.GetName())

	// TODO(user): fill in your validation logic upon object creation.

	return nil, nil
}

// ValidateUpdate implements webhook.CustomValidator so a webhook will be registered for the type MgoDataReplicaSet.
func (v *MgoDataReplicaSetCustomValidator) ValidateUpdate(_ context.Context, oldObj, newObj runtime.Object) (admission.Warnings, error) {
	mgodatareplicaset, ok := newObj.(*mongodbv1.MgoDataReplicaSet)
	if !ok {
		return nil, fmt.Errorf("expected a MgoDataReplicaSet object for the newObj but got %T", newObj)
	}
	mgodatareplicasetlog.Info("Validation for MgoDataReplicaSet upon update", "name", mgodatareplicaset.GetName())

	// TODO(user): fill in your validation logic upon object update.

	return nil, nil
}

// ValidateDelete implements webhook.CustomValidator so a webhook will be registered for the type MgoDataReplicaSet.
func (v *MgoDataReplicaSetCustomValidator) ValidateDelete(ctx context.Context, obj runtime.Object) (admission.Warnings, error) {
	mgodatareplicaset, ok := obj.(*mongodbv1.MgoDataReplicaSet)
	if !ok {
		return nil, fmt.Errorf("expected a MgoDataReplicaSet object but got %T", obj)
	}
	mgodatareplicasetlog.Info("Validation for MgoDataReplicaSet upon deletion", "name", mgodatareplicaset.GetName())

	// TODO(user): fill in your validation logic upon object deletion.

	return nil, nil
}
