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

package controller

import (
	"context"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	mongodbv1 "github.com/akley-MK4/mongodb-k8s-operator/api/v1"
)

// MongoDBClusterReconciler reconciles a MongoDBCluster object
type MongoDBClusterReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=mongodb.akleymk4.com,resources=mongodbclusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=mongodb.akleymk4.com,resources=mongodbclusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=mongodb.akleymk4.com,resources=mongodbclusters/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the MongoDBCluster object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.21.0/pkg/reconcile
func (r *MongoDBClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (retCtrl ctrl.Result, retErr error) {
	log := logf.FromContext(ctx)

	mgoCluster := &mongodbv1.MongoDBCluster{}
	if err := r.Get(ctx, req.NamespacedName, mgoCluster); err != nil {
		if apierrors.IsNotFound(err) {
			log.Info("The resource MongoDBCluster not found. Ignoring since object must be deleted")
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	if len(mgoCluster.Status.Conditions) == 0 {
		meta.SetStatusCondition(&mgoCluster.Status.Conditions, metav1.Condition{Type: "Available", Status: metav1.ConditionUnknown, Reason: "Reconciling", Message: "Starting reconciliation"})
		if err := r.Status().Update(ctx, mgoCluster); err != nil {
			return ctrl.Result{}, err
		}

		if err := r.Get(ctx, req.NamespacedName, mgoCluster); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	defer func() {
		if retErr != nil {
			meta.SetStatusCondition(&mgoCluster.Status.Conditions, metav1.Condition{Type: "Available",
				Status: metav1.ConditionFalse, Reason: "Reconciling",
				Message: retErr.Error(),
			})

			if e := r.Status().Update(ctx, mgoCluster); e != nil {
				return
			}
		}
	}()

	if retCtrl, retErr = r.reconcileShards(ctx, log, mgoCluster); retErr != nil {
		retErr = fmt.Errorf("reconcileShards failed, %v", retErr)
		return
	} else if retCtrl.RequeueAfter > 0 {
		return
	}

	if retCtrl, retErr = r.reconcileConfigServer(ctx, log, mgoCluster); retErr != nil {
		retErr = fmt.Errorf("reconcileConfigServer failed, %v", retErr)
		return
	} else if retCtrl.RequeueAfter > 0 {
		return
	}

	if retCtrl, retErr = r.reconcileRouters(ctx, log, mgoCluster); retErr != nil {
		retErr = fmt.Errorf("reconcileRouters failed, %v", retErr)
		return
	} else if retCtrl.RequeueAfter > 0 {
		return
	}

	// The following implementation will update the status
	meta.SetStatusCondition(&mgoCluster.Status.Conditions, metav1.Condition{Type: "Available",
		Status: metav1.ConditionTrue, Reason: "Reconciling",
		Message: "All components have been successfully deployed"})

	if err := r.Status().Update(ctx, mgoCluster); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *MongoDBClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&mongodbv1.MongoDBCluster{}).
		Named("mongodbcluster").
		Complete(r)
}
