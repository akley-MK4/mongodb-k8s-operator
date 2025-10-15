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

	appsv1 "k8s.io/api/apps/v1"
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

	defer func() {
		if err := r.Get(ctx, req.NamespacedName, mgoCluster); err != nil {
			log.Error(err, "Unable to get the mgoCluster resource")
			if retErr == nil {
				retErr = err
			}
			return
		}

		if retErr != nil {
			meta.SetStatusCondition(&mgoCluster.Status.Conditions, metav1.Condition{Type: "Available",
				Status: metav1.ConditionFalse, Reason: "Reconciling",
				Message: retErr.Error(),
			})
		} else if retCtrl.RequeueAfter <= 0 {
			meta.SetStatusCondition(&mgoCluster.Status.Conditions, metav1.Condition{Type: "Available",
				Status: metav1.ConditionTrue, Reason: "Reconciling",
				Message: "All components have been successfully deployed"})
		}

		if e := r.Status().Update(ctx, mgoCluster); e != nil {
			log.Error(e, "Unable to update the status of the mgoCluster resource")
			if retErr == nil {
				retErr = e
			}
			return
		}
	}()

	if retCtrl, retErr = r.reconcileConfigServer(ctx, log, mgoCluster); retErr != nil || retCtrl.RequeueAfter > 0 {
		if retErr != nil {
			retErr = fmt.Errorf("failed to reconcile the config servers, %v", retErr)
		}
		return
	}

	if retCtrl, retErr = r.reconcileRouters(ctx, log, mgoCluster); retErr != nil || retCtrl.RequeueAfter > 0 {
		if retErr != nil {
			retErr = fmt.Errorf("failed to reconcile the routers, %v", retErr)
		}
		return
	}

	return
}

// SetupWithManager sets up the controller with the Manager.
func (r *MongoDBClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&mongodbv1.MongoDBCluster{}).
		Named("mongodbcluster").
		Owns(&appsv1.StatefulSet{}).
		Complete(r)
}

func GetMongodbClusterRes(ctx context.Context, reader client.Reader, ns string) (*mongodbv1.MongoDBCluster, error) {
	var mgoClusterList mongodbv1.MongoDBClusterList
	if err := reader.List(ctx, &mgoClusterList, client.InNamespace(ns)); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}

	if len(mgoClusterList.Items) <= 0 {
		return nil, nil
	}

	//return mgoClusterList.Items[0].DeepCopy(), nil
	return &mgoClusterList.Items[0], nil
}

func AddMongodbClusterShardToStatus(mgoCluster *mongodbv1.MongoDBCluster, replicaSetId string, statusClient client.StatusClient) (added bool, retErr error) {
	for _, rsId := range mgoCluster.Status.Shards {
		if replicaSetId == rsId {
			return
		}
	}

	patch := client.MergeFrom(mgoCluster.DeepCopy())
	mgoCluster.Status.Shards = append(mgoCluster.Status.Shards, replicaSetId)
	if retErr = statusClient.Status().Patch(context.TODO(), mgoCluster, patch); retErr != nil {
		return
	}

	added = true
	return
}

func RemoveMongodbClusterShardInStatus(mgoCluster *mongodbv1.MongoDBCluster, replicaSetId string, statusClient client.StatusClient) (removed bool, retErr error) {
	rsIdExists := false
	for _, rsId := range mgoCluster.Status.Shards {
		if replicaSetId == rsId {
			rsIdExists = true
			break
		}
	}
	if !rsIdExists {
		return
	}

	patch := client.MergeFrom(mgoCluster.DeepCopy())
	shards := mgoCluster.Status.Shards
	mgoCluster.Status.Shards = []string{}
	for _, rsId := range shards {
		if rsId != replicaSetId {
			mgoCluster.Status.Shards = append(mgoCluster.Status.Shards, rsId)
		}
	}
	if retErr = statusClient.Status().Patch(context.TODO(), mgoCluster, patch); retErr != nil {
		return
	}

	removed = true
	return
}
