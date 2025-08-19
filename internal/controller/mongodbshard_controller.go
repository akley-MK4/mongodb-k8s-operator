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
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	mongodbv1 "github.com/akley-MK4/mongodb-k8s-operator/api/v1"
	mongoclient "github.com/akley-MK4/mongodb-k8s-operator/pkg/mongo-client"
	"github.com/go-logr/logr"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	FinalizerMgoShard = "finalizer-shard"
)

// MongoDBShardReconciler reconciles a MongoDBShard object
type MongoDBShardReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=mongodb.akleymk4.com,resources=mongodbshards,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=mongodb.akleymk4.com,resources=mongodbshards/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=mongodb.akleymk4.com,resources=mongodbshards/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the MongoDBShard object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.21.0/pkg/reconcile
func (r *MongoDBShardReconciler) Reconcile(ctx context.Context, req ctrl.Request) (retCtrl ctrl.Result, retErr error) {
	log := logf.FromContext(ctx)

	var mgoClusterList mongodbv1.MongoDBClusterList
	if err := r.List(ctx, &mgoClusterList, client.InNamespace(req.Namespace)); err != nil {
		if apierrors.IsNotFound(err) && len(mgoClusterList.Items) <= 0 {
			log.Info("The resource MongoDBCluster not found. Ignoring since object must be deleted")
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	mgoCluster := &mgoClusterList.Items[0]

	mgoShard := &mongodbv1.MongoDBShard{}
	if err := r.Get(ctx, req.NamespacedName, mgoShard); err != nil {
		if apierrors.IsNotFound(err) {
			log.Info("The resource MongoDBCluster not found. Ignoring since object must be deleted")
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	if !controllerutil.ContainsFinalizer(mgoShard, FinalizerMgoShard) {
		controllerutil.AddFinalizer(mgoShard, FinalizerMgoShard)
		if err := r.Update(ctx, mgoShard); err != nil {
			return ctrl.Result{}, err
		}
	}

	replicaSetId := mgoShard.GetName()

	if mgoShard.GetDeletionTimestamp() != nil && !mgoShard.GetDeletionTimestamp().IsZero() {
		controllerutil.RemoveFinalizer(mgoShard, FinalizerMgoShard)
		if err := r.Update(ctx, mgoShard); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	defer func() {
		if err := r.Get(ctx, req.NamespacedName, mgoShard); err != nil {
			log.Error(err, "Unable to get the mgoShard resource")
			if retErr == nil {
				retErr = err
			}
			return
		}

		if retErr != nil {
			meta.SetStatusCondition(&mgoShard.Status.Conditions, metav1.Condition{Type: "Available",
				Status: metav1.ConditionFalse, Reason: "Reconciling",
				Message: retErr.Error(),
			})
		} else if retCtrl.RequeueAfter <= 0 {
			meta.SetStatusCondition(&mgoShard.Status.Conditions, metav1.Condition{Type: "Available",
				Status: metav1.ConditionTrue, Reason: "Reconciling",
				Message: "All components have been successfully deployed"})
		}

		if e := r.Status().Update(ctx, mgoShard); e != nil {
			log.Error(e, "Unable to update the status of the mgoCluster resource")
			if retErr == nil {
				retErr = e
			}
			return
		}
	}()

	if retCtrl, retErr = r.reconcileShardHeadlessService(ctx, log, mgoCluster, mgoShard); retErr != nil {
		retErr = fmt.Errorf("resource: Service, replicaSetId: %v, error: %v", replicaSetId, retErr)
		return
	}

	if retCtrl, retErr = r.reconcileShardStatefulSet(ctx, log, mgoCluster, mgoShard); retErr != nil || retCtrl.RequeueAfter > 0 {
		retErr = fmt.Errorf("resource: StatefulSet, replicaSetId: %v, error: %v", replicaSetId, retErr)
		return
	}

	// The pods of the replica set are reconciled, check or initialize the replica set
	primaryMgoAddr, secondaryMgoAddrs, arbiterMgoAddrs := FmtShardMgoAddrs(mgoCluster.GetName(), mgoCluster.GetNamespace(),
		replicaSetId, mgoShard.Spec.Port, mgoShard.Spec.NumSecondaryNodes, mgoShard.Spec.NumArbiterNodes)

	if initialized, err := mongoclient.CheckReplicaSet(mgoCluster.Spec.DBConnTimeout, replicaSetId, primaryMgoAddr, secondaryMgoAddrs, arbiterMgoAddrs); err != nil {
		retErr = fmt.Errorf("an error occurred while checking the replica set of shard in the mongodb cluster, %v", err)
		retCtrl.RequeueAfter = time.Second
		return
	} else if !initialized {
		if err := mongoclient.InitiateReplicaSet(mgoCluster.Spec.DBConnTimeout, replicaSetId, primaryMgoAddr, secondaryMgoAddrs, arbiterMgoAddrs); err != nil {
			retErr = fmt.Errorf("an error occurred while initializing the replica set of shard in the mongodb cluster, %v", err)
			retCtrl.RequeueAfter = time.Second
			return
		}
		log.Info("Successfully initialized the replica set of shard", "replicaSetId", replicaSetId)
	} else {
		log.Info("The replica set of shard has already been initialized in the mongodb cluster", "replicaSetId", replicaSetId)
	}

	// Add the shard to the mongodb cluster
	routerMgoAddr := FmtRouterMgoAddr(mgoCluster.GetName(), mgoCluster.GetNamespace(), mgoCluster.Spec.Routers.ServicePort)
	shardDBAddrs := append([]string{primaryMgoAddr}, secondaryMgoAddrs...)
	shardDBAddrs = append(shardDBAddrs, arbiterMgoAddrs...)
	if exist, err := mongoclient.CheckShard(mgoCluster.Spec.DBConnTimeout, routerMgoAddr, replicaSetId, shardDBAddrs); err != nil {
		retErr = fmt.Errorf("an error occurred while checking the shard %v in the mongodb cluster, %v", replicaSetId, err)
		retCtrl.RequeueAfter = time.Second
		return
	} else if exist {
		log.Info("The shard was added to the mongodb cluster", "replicaSetId", replicaSetId)
		return
	}

	if err := mongoclient.AddShard(mgoCluster.Spec.DBConnTimeout, routerMgoAddr, replicaSetId, shardDBAddrs); err != nil {
		retErr = fmt.Errorf("an error occurred while adding the shard %v to the mongodb cluster, %v", replicaSetId, err)
		retCtrl.RequeueAfter = time.Second
		return
	}
	log.Info("Successfully added the shard to the mongodb cluster", "replicaSetId", replicaSetId)

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *MongoDBShardReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&mongodbv1.MongoDBShard{}).
		Named("mongodbshard").
		Complete(r)
}

func (r *MongoDBShardReconciler) reconcileShardHeadlessService(ctx context.Context, log logr.Logger, mgoCluster *mongodbv1.MongoDBCluster, mgoShard *mongodbv1.MongoDBShard) (ctrl.Result, error) {

	var svc corev1.Service
	replicaSetId := mgoShard.GetName()
	name := FmtShardServiceName(mgoCluster.GetName(), replicaSetId)
	key := client.ObjectKey{
		Namespace: mgoShard.GetNamespace(),
		Name:      name,
	}

	if err := r.Get(ctx, key, &svc); err != nil {
		if !apierrors.IsNotFound(err) {
			return ctrl.Result{}, fmt.Errorf("unable to get the service, %v", err)
		}

		svc.ObjectMeta.Name = name
		svc.ObjectMeta.Namespace = mgoShard.GetNamespace()
		svc.Spec.Type = corev1.ServiceTypeClusterIP
		svc.Spec.ClusterIP = "None"
		svc.Spec.Selector = map[string]string{
			"component-type": string(mongodbv1.ComponentTypeShard),
			"replicaset-id":  replicaSetId,
		}
		svc.Spec.Ports = append(svc.Spec.Ports, corev1.ServicePort{
			Name:       "data",
			Port:       int32(mgoShard.Spec.Port),
			TargetPort: intstr.FromInt(int(mgoShard.Spec.Port)),
		})

		if e := ctrl.SetControllerReference(mgoShard, &svc, r.Scheme); e != nil {
			return ctrl.Result{}, fmt.Errorf("unable to set the controller reference, %v", e)
		}

		if e := r.Create(ctx, &svc); e != nil {
			return ctrl.Result{}, fmt.Errorf("unable to create a service, %v", e)
		}

		log.Info("Successfully created a service for shard", "replicaSetId", replicaSetId)
		return ctrl.Result{}, nil
	} else {
		log.Info("The service of shard exists", "replicaSetId", replicaSetId)
	}

	return ctrl.Result{}, nil
}

func (r *MongoDBShardReconciler) reconcileShardStatefulSet(ctx context.Context, log logr.Logger,
	mgoCluster *mongodbv1.MongoDBCluster, mgoShard *mongodbv1.MongoDBShard) (ctrl.Result, error) {

	replicaSetId := mgoShard.GetName()
	var foundStatefulSet appsv1.StatefulSet
	key := client.ObjectKey{
		Namespace: mgoShard.GetNamespace(),
		Name:      FmtShardStatefulSetName(mgoCluster.GetName(), replicaSetId),
	}

	if err := r.Get(ctx, key, &foundStatefulSet); err != nil {
		if !apierrors.IsNotFound(err) {
			return ctrl.Result{}, fmt.Errorf("unnable to get the stateful set, %v", err)
		}

		if e := r.createShardStatefulSet(ctx, mgoCluster, mgoShard); e != nil {
			return ctrl.Result{}, e
		}
		log.Info("Successfully created a stateful set for shard", "replicaSetId", replicaSetId)
		return ctrl.Result{}, nil
	} else {
		log.Info("The stateful set of shard exists", "replicaSetId", replicaSetId)
	}

	numReplicaSetNodes := CountNumShardReplicaSetNodes(mgoShard.Spec.NumSecondaryNodes, mgoShard.Spec.NumArbiterNodes)
	if foundStatefulSet.Spec.Replicas == nil || (*foundStatefulSet.Spec.Replicas) != numReplicaSetNodes {
		foundStatefulSet.Spec.Replicas = ptr.To(numReplicaSetNodes)
		if err := r.Update(ctx, &foundStatefulSet); err != nil {
			return ctrl.Result{}, fmt.Errorf("unable to update the stateful set found, %v", err)
		}
		return ctrl.Result{RequeueAfter: time.Second}, nil
	}

	return ctrl.Result{}, nil
}

func (r *MongoDBShardReconciler) createShardStatefulSet(ctx context.Context, mgoCluster *mongodbv1.MongoDBCluster, mgoShard *mongodbv1.MongoDBShard) (retErr error) {
	statefulSet, errStatefulSet := r.newShardStatefulSet(mgoCluster, mgoShard)
	if errStatefulSet != nil {
		retErr = fmt.Errorf("unable to create a data object with type StatefulSet, %v", errStatefulSet)
		return
	}

	if e := ctrl.SetControllerReference(mgoCluster, statefulSet, r.Scheme); e != nil {
		retErr = fmt.Errorf("unable to set the controller reference, %v", e)
		return
	}

	if e := r.Create(ctx, statefulSet); e != nil {
		retErr = fmt.Errorf("unable to create a stateful set, %v", e)
		return
	}

	return nil
}

func fmtComponentTypeObjectName(mgoClusterName string, componentType mongodbv1.ComponentType, replicaSetId string) string {
	if replicaSetId != "" {
		return strings.Join([]string{mgoClusterName, string(componentType), replicaSetId}, "-")
	}
	return strings.Join([]string{mgoClusterName, string(componentType)}, "-")
}

func (r *MongoDBShardReconciler) newShardStatefulSet(mgoCluster *mongodbv1.MongoDBCluster, mgoShard *mongodbv1.MongoDBShard) (*appsv1.StatefulSet, error) {
	replicaSetId := mgoShard.GetName()
	objectName := FmtShardStatefulSetName(mgoCluster.GetName(), replicaSetId)

	statefulSet := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      objectName,
			Namespace: mgoCluster.GetNamespace(),
		},

		Spec: appsv1.StatefulSetSpec{
			Replicas: ptr.To(CountNumShardReplicaSetNodes(mgoShard.Spec.NumSecondaryNodes, mgoShard.Spec.NumArbiterNodes)),
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"component-type": string(mongodbv1.ComponentTypeShard)},
			},
			ServiceName: FmtShardServiceName(mgoCluster.GetName(), replicaSetId),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"component-type": string(mongodbv1.ComponentTypeShard),
						"replicaset-id":  replicaSetId,
					},
				},
			},
		},
	}

	var containers []corev1.Container
	// mongodb
	imageMongodb := mgoCluster.Spec.Images["mongodb"]
	if imageMongodb == "" {
		return nil, errors.New("the image of mongod is not configured")
	}

	mongodContainer := corev1.Container{
		Name:            "mongod",
		Image:           imageMongodb,
		ImagePullPolicy: mgoCluster.Spec.ImagePullPolicy,
		Ports: []corev1.ContainerPort{{
			ContainerPort: int32(mgoShard.Spec.Port),
			Name:          "data",
		}},
		Command: []string{"mongod"},
		Args: []string{
			"--shardsvr",
			"--replSet",
			replicaSetId,
			"--dbpath",
			mgoShard.Spec.DataPath,
			"--port",
			strconv.Itoa(int(mgoShard.Spec.Port)),
			"--bind_ip_all",
		},
	}
	if res, ok := mgoCluster.Spec.ResourceRequirements["shard"]; ok {
		mongodContainer.Resources = *res.DeepCopy()
	} else {
		mongodContainer.Resources.Requests = make(corev1.ResourceList)
		mongodContainer.Resources.Limits = make(corev1.ResourceList)
		mongodContainer.Resources.Requests[corev1.ResourceCPU] = *resource.NewMilliQuantity(100, resource.DecimalSI)
		mongodContainer.Resources.Limits[corev1.ResourceCPU] = *resource.NewMilliQuantity(500, resource.DecimalSI)
		mongodContainer.Resources.Requests[corev1.ResourceMemory] = *resource.NewQuantity(1024*1024*125, resource.BinarySI)
		mongodContainer.Resources.Limits[corev1.ResourceMemory] = *resource.NewQuantity(1024*1024*1024, resource.BinarySI)

	}
	containers = append(containers, mongodContainer)

	statefulSet.Spec.Template.Spec.Containers = containers

	return statefulSet, nil
}

func CountNumShardReplicaSetNodes(numSecondaryNodes, numArbiterNodes uint16) int32 {
	return int32(1 + numSecondaryNodes + numArbiterNodes)
}

func FmtShardStatefulSetName(clusterName, replicaSetId string) string {
	return fmtComponentTypeObjectName(clusterName, mongodbv1.ComponentTypeShard, replicaSetId)
}

func FmtShardServiceName(clusterName, replicaSetId string) string {
	return fmtComponentTypeObjectName(clusterName, mongodbv1.ComponentTypeShard, replicaSetId)
}

func FmtShardMgoAddrs(clusterName, ns, replicaSetId string, port uint16, numSecondaryNodes, numArbiterNodes uint16) (
	retPrimaryAddr string, retSecondaryAddrs, retArbiterAddrs []string) {

	svcName := FmtShardServiceName(clusterName, replicaSetId)
	statefulSetName := FmtShardStatefulSetName(clusterName, replicaSetId)

	// Just for testing
	k8sClusterDomain := "quick3"
	numReplicaSetNodes := CountNumShardReplicaSetNodes(numSecondaryNodes, numArbiterNodes)

	var addrs []string
	for i := int32(0); i < numReplicaSetNodes; i++ {
		podName := fmt.Sprintf("%s-%d", statefulSetName, i)
		addrs = append(addrs, fmt.Sprintf("%s.%s.%s.svc.%s:%d", podName, svcName, ns, k8sClusterDomain, port))
	}

	if len(addrs) <= 0 {
		return
	}

	retPrimaryAddr = addrs[0]
	retSecondaryAddrs = addrs[1 : 1+numSecondaryNodes]
	retArbiterAddrs = addrs[1+numSecondaryNodes:]
	return
}
