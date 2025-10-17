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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	mongodbv1 "github.com/akley-MK4/mongodb-k8s-operator/api/v1"
	mongoclient "github.com/akley-MK4/mongodb-k8s-operator/pkg/mongo-client"
	"github.com/go-logr/logr"
)

const (
	FinalizerMgoReplicaSet = "finalizer-mgo-replicaset"
)

// MgoDataReplicaSetReconciler reconciles a MgoDataReplicaSet object
type MgoDataReplicaSetReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=mongodb.akleymk4.com,resources=mgodatareplicasets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=mongodb.akleymk4.com,resources=mgodatareplicasets/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=mongodb.akleymk4.com,resources=mgodatareplicasets/finalizers,verbs=update
// +kubebuilder:rbac:groups=mongodb.akleymk4.com,resources=mongodbclusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the MgoDataReplicaSet object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.21.0/pkg/reconcile
func (r *MgoDataReplicaSetReconciler) Reconcile(ctx context.Context, req ctrl.Request) (retCtrl ctrl.Result, retErr error) {
	log := logf.FromContext(ctx)

	mgoCluster, errMgoCluster := GetMongodbClusterRes(ctx, r, req.Namespace)
	if errMgoCluster != nil {
		return ctrl.Result{}, errMgoCluster
	} else if mgoCluster == nil {
		log.Info("The MongodbCluster resource dose not exists")
		return ctrl.Result{}, nil
	}

	mgoDataReplicaSet := &mongodbv1.MgoDataReplicaSet{}
	if err := r.Get(ctx, req.NamespacedName, mgoDataReplicaSet); err != nil {
		if apierrors.IsNotFound(err) {
			log.Info("The resource MgoDataReplicaSet not found. Ignoring since object must be deleted")
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	replicaSetId := mgoDataReplicaSet.GetName()

	if !controllerutil.ContainsFinalizer(mgoDataReplicaSet, FinalizerMgoReplicaSet) {
		resObj := mgoDataReplicaSet.DeepCopy()
		if controllerutil.AddFinalizer(resObj, FinalizerMgoReplicaSet) {
			if err := r.Update(ctx, resObj); err != nil {
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{}, nil
	}

	if mgoDataReplicaSet.GetDeletionTimestamp() != nil && !mgoDataReplicaSet.GetDeletionTimestamp().IsZero() {
		if err := r.cleanupBeforeDelete(mgoCluster, mgoDataReplicaSet); err != nil {
			log.Error(err, "Failed to clean up related resources for the replica et", "replicaSetId", replicaSetId)
			return ctrl.Result{RequeueAfter: time.Second}, err
		}

		resObj := mgoDataReplicaSet.DeepCopy()
		controllerutil.RemoveFinalizer(resObj, FinalizerMgoReplicaSet)
		if err := r.Update(ctx, resObj); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	addedShard := false
	initialized := false

	defer func() {
		if err := r.updateStatus(ctx, req.NamespacedName, retErr, initialized, addedShard); err != nil {
			if retErr == nil {
				retErr = err
			}
		}
	}()

	if retCtrl, retErr = r.reconcileService(ctx, log, mgoCluster, mgoDataReplicaSet); retErr != nil {
		retErr = fmt.Errorf("resource: Service, replicaSetId: %v, error: %v", replicaSetId, retErr)
		return
	}

	if retCtrl, retErr = r.reconcileStatefulSet(ctx, log, mgoCluster, mgoDataReplicaSet); retErr != nil {
		retErr = fmt.Errorf("resource: StatefulSet, replicaSetId: %v, error: %v", replicaSetId, retErr)
		return
	} else if retCtrl.RequeueAfter > 0 {
		return
	}

	if initialized, retErr = r.checkAndInitializeReplicaSet(mgoCluster, mgoDataReplicaSet, log); retErr != nil {
		retCtrl.RequeueAfter = time.Second
		return
	}

	if mgoDataReplicaSet.Spec.EnableShard {
		// Check and add the shard to the mongodb cluster
		if addedShard, retErr = r.checkAndAddShard(mgoCluster, mgoDataReplicaSet, log); retErr != nil {
			retCtrl.RequeueAfter = time.Second
			return
		}
		if _, err := AddMongodbClusterShardToStatus(mgoCluster, replicaSetId, r); err != nil {
			retErr = err
			retCtrl.RequeueAfter = time.Second
			return
		}
	} else {
		// After downgrading the replica set, it is necessary to remove metadata from the cluster
		if retErr = r.checkAndRemoveShard(mgoCluster, mgoDataReplicaSet, log); retErr != nil {
			retCtrl.RequeueAfter = time.Second
			return
		}
	}

	return
}

// SetupWithManager sets up the controller with the Manager.
func (r *MgoDataReplicaSetReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&mongodbv1.MgoDataReplicaSet{}).
		Named("mgodatareplicaset").
		Owns(&appsv1.StatefulSet{}).
		Complete(r)
}

func (r *MgoDataReplicaSetReconciler) updateStatus(ctx context.Context, ns types.NamespacedName, errResult error, initialized, addedShard bool) error {
	mgoDataReplicaSet := &mongodbv1.MgoDataReplicaSet{}
	if err := r.Get(ctx, ns, mgoDataReplicaSet); err != nil {
		return err
	}

	if errResult != nil {
		if meta.SetStatusCondition(&mgoDataReplicaSet.Status.Conditions, metav1.Condition{Type: "Available",
			Status: metav1.ConditionFalse, Reason: "Reconciling",
			Message: errResult.Error(),
		}) || mgoDataReplicaSet.Status.Initialized != initialized {
			mgoDataReplicaSet.Status.Initialized = initialized
			return r.Status().Update(ctx, mgoDataReplicaSet)
		}
		return nil
	}

	if meta.SetStatusCondition(&mgoDataReplicaSet.Status.Conditions, metav1.Condition{Type: "Available",
		Status: metav1.ConditionTrue, Reason: "Reconciling",
		Message: "The replica set have been successfully deployed"}) ||
		mgoDataReplicaSet.Status.AddedShard != addedShard ||
		mgoDataReplicaSet.Status.Initialized != initialized {

		mgoDataReplicaSet.Status.Initialized = initialized
		mgoDataReplicaSet.Status.AddedShard = addedShard

		return r.Status().Update(ctx, mgoDataReplicaSet)
	}
	return nil
}

func (r *MgoDataReplicaSetReconciler) reconcileService(ctx context.Context, log logr.Logger, mgoCluster *mongodbv1.MongoDBCluster, mgoDataReplicaSet *mongodbv1.MgoDataReplicaSet) (ctrl.Result, error) {

	var svc corev1.Service
	replicaSetId := mgoDataReplicaSet.GetName()
	name := FmtDataReplicaSetServiceName(mgoCluster.GetName(), replicaSetId)
	key := client.ObjectKey{
		Namespace: mgoDataReplicaSet.GetNamespace(),
		Name:      name,
	}

	if err := r.Get(ctx, key, &svc); err != nil {
		if !apierrors.IsNotFound(err) {
			return ctrl.Result{}, fmt.Errorf("unable to get the service, %v", err)
		}

		svc.ObjectMeta.Name = name
		svc.ObjectMeta.Namespace = mgoDataReplicaSet.GetNamespace()
		svc.Spec.Type = corev1.ServiceTypeClusterIP
		svc.Spec.ClusterIP = "None"
		svc.Spec.Selector = map[string]string{
			"component-type": string(mongodbv1.ComponentTypeDataReplicaSet),
			"replicaset-id":  replicaSetId,
		}
		svc.Spec.Ports = append(svc.Spec.Ports, corev1.ServicePort{
			Name:       "data",
			Port:       int32(mgoDataReplicaSet.Spec.Port),
			TargetPort: intstr.FromInt(int(mgoDataReplicaSet.Spec.Port)),
		})

		if e := ctrl.SetControllerReference(mgoDataReplicaSet, &svc, r.Scheme); e != nil {
			return ctrl.Result{}, fmt.Errorf("unable to set the controller reference, %v", e)
		}

		if e := r.Create(ctx, &svc); e != nil {
			return ctrl.Result{}, fmt.Errorf("unable to create a service, %v", e)
		}

		log.Info("Successfully created a service for the data replica set", "replicaSetId", replicaSetId)
		return ctrl.Result{}, nil
	} else {
		log.Info("The service of the data replica set exists", "replicaSetId", replicaSetId)
	}

	return ctrl.Result{}, nil
}

func (r *MgoDataReplicaSetReconciler) reconcileStatefulSet(ctx context.Context, log logr.Logger,
	mgoCluster *mongodbv1.MongoDBCluster, mgoDataReplicaSet *mongodbv1.MgoDataReplicaSet) (ctrl.Result, error) {

	replicaSetId := mgoDataReplicaSet.GetName()
	var foundStatefulSet appsv1.StatefulSet
	key := client.ObjectKey{
		Namespace: mgoDataReplicaSet.GetNamespace(),
		Name:      FmtDataReplicaSetStatefulSetName(mgoCluster.GetName(), replicaSetId),
	}

	if err := r.Get(ctx, key, &foundStatefulSet); err != nil {
		if !apierrors.IsNotFound(err) {
			return ctrl.Result{}, fmt.Errorf("unnable to get the stateful set, %v", err)
		}

		if e := r.createStatefulSet(ctx, mgoCluster, mgoDataReplicaSet); e != nil {
			return ctrl.Result{}, e
		}
		log.Info("Successfully created a stateful set for the data replica set", "replicaSetId", replicaSetId)
		return ctrl.Result{}, nil
	} else {
		log.Info("The stateful set of the data replica set exists", "replicaSetId", replicaSetId)
	}

	numReplicaSetNodes := CountNumDataReplicaSetNodes(mgoDataReplicaSet.Spec.NumSecondaryNodes, mgoDataReplicaSet.Spec.NumArbiterNodes)
	if foundStatefulSet.Spec.Replicas == nil || (*foundStatefulSet.Spec.Replicas) != numReplicaSetNodes {
		cpFoundStatefulSet := foundStatefulSet.DeepCopy()
		cpFoundStatefulSet.Spec.Replicas = ptr.To(numReplicaSetNodes)
		if err := r.Update(ctx, cpFoundStatefulSet); err != nil {
			return ctrl.Result{}, fmt.Errorf("unable to update the stateful set found, %v", err)
		}
		return ctrl.Result{RequeueAfter: time.Second}, nil
	}

	for idx, co := range foundStatefulSet.Spec.Template.Spec.Containers {
		if co.Name != "mongod" {
			continue
		}

		shardArgExists := false
		for _, arg := range co.Args {
			if arg == "--shardsvr" {
				shardArgExists = true
				break
			}
		}
		if shardArgExists != mgoDataReplicaSet.Spec.EnableShard {
			cpFoundStatefulSet := foundStatefulSet.DeepCopy()
			cpFoundStatefulSet.Spec.Template.Spec.Containers[idx].Args = buildDataReplicaSetContainerArgs(mgoDataReplicaSet)
			if err := r.Update(ctx, cpFoundStatefulSet); err != nil {
				return ctrl.Result{}, fmt.Errorf("unable to update the stateful set found, %v", err)
			}
			return ctrl.Result{RequeueAfter: time.Second}, nil
		}
		break
	}

	if foundStatefulSet.Status.ReadyReplicas != numReplicaSetNodes || foundStatefulSet.Status.UpdatedReplicas != numReplicaSetNodes {
		log.Info("Waiting for all pods of the data replica set to be ready",
			"replicaSetId", replicaSetId,
			"replicas", numReplicaSetNodes,
			"readyReplicas", foundStatefulSet.Status.ReadyReplicas,
			"updatedReplicas", foundStatefulSet.Status.UpdatedReplicas,
		)
		return ctrl.Result{RequeueAfter: time.Second}, nil
	}

	return ctrl.Result{}, nil
}

func (r *MgoDataReplicaSetReconciler) createStatefulSet(ctx context.Context, mgoCluster *mongodbv1.MongoDBCluster, mgoDataReplicaSet *mongodbv1.MgoDataReplicaSet) (retErr error) {
	statefulSet, errStatefulSet := r.newStatefulSet(mgoCluster, mgoDataReplicaSet)
	if errStatefulSet != nil {
		retErr = fmt.Errorf("unable to create a data object with type StatefulSet, %v", errStatefulSet)
		return
	}

	if e := ctrl.SetControllerReference(mgoDataReplicaSet, statefulSet, r.Scheme); e != nil {
		retErr = fmt.Errorf("unable to set the controller reference, %v", e)
		return
	}

	if e := r.Create(ctx, statefulSet); e != nil {
		retErr = fmt.Errorf("unable to create a stateful set, %v", e)
		return
	}

	return nil
}

func (r *MgoDataReplicaSetReconciler) cleanupBeforeDelete(mgoCluster *mongodbv1.MongoDBCluster, mgoDataReplicaSet *mongodbv1.MgoDataReplicaSet) error {
	replicaSetId := mgoDataReplicaSet.GetName()
	if _, err := RemoveMongodbClusterShardInStatus(mgoCluster, replicaSetId, r); err != nil {
		return nil
	}

	if !mgoDataReplicaSet.Status.AddedShard {
		return nil
	}

	routerMgoAddr := FmtRouterMgoAddr(mgoCluster.GetName(), mgoCluster.GetNamespace(), mgoCluster.Spec.Routers.ServicePort)
	if err := mongoclient.SafeRemoveShard(mgoCluster.Spec.DBConnTimeout, routerMgoAddr, replicaSetId); err != nil {
		return err
	}
	return nil
}

func (r *MgoDataReplicaSetReconciler) checkAndInitializeReplicaSet(mgoCluster *mongodbv1.MongoDBCluster, mgoDataReplicaSet *mongodbv1.MgoDataReplicaSet,
	log logr.Logger) (retInitialized bool, retErr error) {

	replicaSetId := mgoDataReplicaSet.GetName()
	// The pods of the replica set are reconciled, check or initialize the replica set
	primaryMgoAddr, secondaryMgoAddrs, arbiterMgoAddrs := FmtDataReplicaSetMgoAddrs(mgoCluster.GetName(), mgoCluster.GetNamespace(),
		replicaSetId, mgoDataReplicaSet.Spec.Port, mgoDataReplicaSet.Spec.NumSecondaryNodes, mgoDataReplicaSet.Spec.NumArbiterNodes)

	retInitialized, retErr = mongoclient.CheckReplicaSet(mgoCluster.Spec.DBConnTimeout, replicaSetId, primaryMgoAddr, secondaryMgoAddrs, arbiterMgoAddrs)
	if retErr != nil {
		retErr = fmt.Errorf("an error occurred while checking the data replica set in the mongodb cluster, %v", retErr)
		return
	}

	if !retInitialized {
		if err := mongoclient.InitiateReplicaSet(mgoCluster.Spec.DBConnTimeout, replicaSetId, primaryMgoAddr, secondaryMgoAddrs, arbiterMgoAddrs); err != nil {
			retErr = fmt.Errorf("an error occurred while initializing the data replica set in the mongodb cluster, %v", err)
			return
		}
		retInitialized = true
		log.Info("Successfully initialized the data replica set", "replicaSetId", replicaSetId)
	} else {
		log.Info("The data replica set has already been initialized in the mongodb cluster", "replicaSetId", replicaSetId)
	}

	// Check and add the shard to the mongodb cluster
	if !mgoDataReplicaSet.Spec.EnableShard {
		// After downgrading the replica set, it is necessary to remove metadata from the cluster
		if mgoDataReplicaSet.Status.AddedShard {
			return
		}

		return
	}

	return
}

func (r *MgoDataReplicaSetReconciler) checkAndAddShard(mgoCluster *mongodbv1.MongoDBCluster, mgoDataReplicaSet *mongodbv1.MgoDataReplicaSet,
	log logr.Logger) (retAddedShard bool, retErr error) {

	replicaSetId := mgoDataReplicaSet.GetName()
	// The pods of the replica set are reconciled, check or initialize the replica set
	primaryMgoAddr, secondaryMgoAddrs, arbiterMgoAddrs := FmtDataReplicaSetMgoAddrs(mgoCluster.GetName(), mgoCluster.GetNamespace(),
		replicaSetId, mgoDataReplicaSet.Spec.Port, mgoDataReplicaSet.Spec.NumSecondaryNodes, mgoDataReplicaSet.Spec.NumArbiterNodes)

	routerMgoAddr := FmtRouterMgoAddr(mgoCluster.GetName(), mgoCluster.GetNamespace(), mgoCluster.Spec.Routers.ServicePort)
	shardDBAddrs := append([]string{primaryMgoAddr}, secondaryMgoAddrs...)
	shardDBAddrs = append(shardDBAddrs, arbiterMgoAddrs...)
	retAddedShard, retErr = mongoclient.CheckShardAdded(mgoCluster.Spec.DBConnTimeout, routerMgoAddr, replicaSetId, shardDBAddrs)
	if retErr != nil {
		retErr = fmt.Errorf("an error occurred while checking the shard %v in the mongodb cluster, %v", replicaSetId, retErr)
		return
	}
	if retAddedShard {
		log.Info("The shard was added to the mongodb cluster", "replicaSetId", replicaSetId)
		return
	}

	if err := mongoclient.AddShard(mgoCluster.Spec.DBConnTimeout, routerMgoAddr, replicaSetId, shardDBAddrs); err != nil {
		retErr = fmt.Errorf("an error occurred while adding the shard %v to the mongodb cluster, %v", replicaSetId, err)
		return
	}
	retAddedShard = true
	log.Info("Successfully added the shard to the mongodb cluster", "replicaSetId", replicaSetId)

	return
}

func (r *MgoDataReplicaSetReconciler) checkAndRemoveShard(mgoCluster *mongodbv1.MongoDBCluster, mgoDataReplicaSet *mongodbv1.MgoDataReplicaSet,
	log logr.Logger) (retErr error) {

	return
}

func fmtComponentTypeObjectName(mgoClusterName string, componentType mongodbv1.ComponentType, replicaSetId string) string {
	if replicaSetId != "" {
		return strings.Join([]string{mgoClusterName, string(componentType), replicaSetId}, "-")
	}
	return strings.Join([]string{mgoClusterName, string(componentType)}, "-")
}

func (r *MgoDataReplicaSetReconciler) newStatefulSet(mgoCluster *mongodbv1.MongoDBCluster, mgoDataReplicaSet *mongodbv1.MgoDataReplicaSet) (*appsv1.StatefulSet, error) {
	replicaSetId := mgoDataReplicaSet.GetName()
	objectName := FmtDataReplicaSetStatefulSetName(mgoCluster.GetName(), replicaSetId)

	statefulSet := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      objectName,
			Namespace: mgoCluster.GetNamespace(),
		},

		Spec: appsv1.StatefulSetSpec{
			Replicas: ptr.To(CountNumDataReplicaSetNodes(mgoDataReplicaSet.Spec.NumSecondaryNodes, mgoDataReplicaSet.Spec.NumArbiterNodes)),
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"component-type": string(mongodbv1.ComponentTypeDataReplicaSet)},
			},
			ServiceName: FmtDataReplicaSetServiceName(mgoCluster.GetName(), replicaSetId),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"component-type": string(mongodbv1.ComponentTypeDataReplicaSet),
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
			ContainerPort: int32(mgoDataReplicaSet.Spec.Port),
			Name:          "data",
		}},
		Command: []string{"mongod"},
		Args:    buildDataReplicaSetContainerArgs(mgoDataReplicaSet),
	}

	if res, ok := mgoCluster.Spec.ResourceRequirements["dataReplicaSet"]; ok {
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

func buildDataReplicaSetContainerArgs(mgoDataReplicaSet *mongodbv1.MgoDataReplicaSet) []string {
	args := []string{
		"--replSet",
		mgoDataReplicaSet.GetName(),
		"--dbpath",
		mgoDataReplicaSet.Spec.DataPath,
		"--port",
		strconv.Itoa(int(mgoDataReplicaSet.Spec.Port)),
		"--bind_ip_all",
	}

	if mgoDataReplicaSet.Spec.EnableShard {
		args = append(args, "--shardsvr")
	}

	return args
}

func CountNumDataReplicaSetNodes(numSecondaryNodes, numArbiterNodes uint16) int32 {
	return int32(1 + numSecondaryNodes + numArbiterNodes)
}

func FmtDataReplicaSetStatefulSetName(clusterName, replicaSetId string) string {
	return fmtComponentTypeObjectName(clusterName, mongodbv1.ComponentTypeDataReplicaSet, replicaSetId)
}

func FmtDataReplicaSetServiceName(clusterName, replicaSetId string) string {
	return fmtComponentTypeObjectName(clusterName, mongodbv1.ComponentTypeDataReplicaSet, replicaSetId)
}

func FmtDataReplicaSetMgoAddrs(clusterName, ns, replicaSetId string, port uint16, numSecondaryNodes, numArbiterNodes uint16) (
	retPrimaryAddr string, retSecondaryAddrs, retArbiterAddrs []string) {

	svcName := FmtDataReplicaSetServiceName(clusterName, replicaSetId)
	statefulSetName := FmtDataReplicaSetStatefulSetName(clusterName, replicaSetId)

	// Just for testing
	k8sClusterDomain := "quick3"
	numReplicaSetNodes := CountNumDataReplicaSetNodes(numSecondaryNodes, numArbiterNodes)

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
