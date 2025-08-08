package controller

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	mongodbv1 "github.com/akley-MK4/mongodb-k8s-operator/api/v1"
	mongoclient "github.com/akley-MK4/mongodb-k8s-operator/pkg/mongo-client"
	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func (r *MongoDBClusterReconciler) reconcileShards(ctx context.Context, log logr.Logger, mgoCluster *mongodbv1.MongoDBCluster) (ctrlRet ctrl.Result, retErr error) {
	for replicaSetId, shardSpec := range mgoCluster.Spec.Shards {
		if ctrlRet, retErr = r.reconcileShardHeadlessService(ctx, log, mgoCluster, *shardSpec, replicaSetId); retErr != nil {
			return
		}

		if ctrlRet, retErr = r.reconcileShardStatefulSet(ctx, log, mgoCluster, *shardSpec, replicaSetId); retErr != nil || ctrlRet.RequeueAfter > 0 {
			return
		}
	}

	return
}

func (r *MongoDBClusterReconciler) reconcileShardHeadlessService(ctx context.Context, log logr.Logger, mgoCluster *mongodbv1.MongoDBCluster,
	shardSpec mongodbv1.MgoShardSpec, replicaSetId string) (ctrl.Result, error) {

	var svc corev1.Service
	name := FmtShardServiceName(mgoCluster.GetName(), replicaSetId)
	key := client.ObjectKey{
		Namespace: mgoCluster.GetNamespace(),
		Name:      name,
	}

	if err := r.Get(ctx, key, &svc); err != nil {
		if !apierrors.IsNotFound(err) {
			return ctrl.Result{}, err
		}

		svc.ObjectMeta.Name = name
		svc.ObjectMeta.Namespace = mgoCluster.GetNamespace()
		svc.Spec.Type = corev1.ServiceTypeClusterIP
		svc.Spec.ClusterIP = "None"
		svc.Spec.Selector = map[string]string{
			"component-type": string(mongodbv1.ComponentTypeShard),
			"replicaset-id":  replicaSetId,
		}
		svc.Spec.Ports = append(svc.Spec.Ports, corev1.ServicePort{
			Name:       "data",
			Port:       int32(shardSpec.Port),
			TargetPort: intstr.FromInt(int(shardSpec.Port)),
		})

		if e := ctrl.SetControllerReference(mgoCluster, &svc, r.Scheme); e != nil {
			return ctrl.Result{}, fmt.Errorf("SetControllerReference failed, %v", e)
		}

		if e := r.Create(ctx, &svc); e != nil {
			return ctrl.Result{}, fmt.Errorf("create failed, %v", e)
		}

		log.Info(fmt.Sprintf("Successfully created a headless service for %v %v", mongodbv1.ComponentTypeShard, replicaSetId))
		return ctrl.Result{}, nil
	}

	return ctrl.Result{}, nil
}

func (r *MongoDBClusterReconciler) reconcileShardStatefulSet(ctx context.Context, log logr.Logger, mgoCluster *mongodbv1.MongoDBCluster,
	shardSpec mongodbv1.MgoShardSpec, replicaSetId string) (ctrl.Result, error) {

	var foundStatefulSet appsv1.StatefulSet
	key := client.ObjectKey{
		Namespace: mgoCluster.GetNamespace(),
		Name:      FmtShardStatefulSetName(mgoCluster.GetName(), replicaSetId),
	}

	if err := r.Get(ctx, key, &foundStatefulSet); err != nil {
		if !apierrors.IsNotFound(err) {
			return ctrl.Result{}, err
		}

		if e := r.createShardStatefulSet(ctx, mgoCluster, shardSpec, replicaSetId); e != nil {
			return ctrl.Result{}, e
		}
		log.Info(fmt.Sprintf("Successfully created a StatefulSet for shard %v", replicaSetId))
		return ctrl.Result{}, nil
	}

	numReplicaSetNodes := CountNumShardReplicaSetNodes(shardSpec.NumSecondaryNodes, shardSpec.NumArbiterNodes)
	if foundStatefulSet.Spec.Replicas == nil || (*foundStatefulSet.Spec.Replicas) != numReplicaSetNodes {
		foundStatefulSet.Spec.Replicas = ptr.To(numReplicaSetNodes)
		if err := r.Update(ctx, &foundStatefulSet); err != nil {
			if e := r.Get(ctx, key, &foundStatefulSet); e != nil {
				return ctrl.Result{}, e
			}
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: time.Millisecond * 500}, nil
	}

	// The pods of the replica set are reconciled, check or initialize the replica set
	primaryMgoAddr, secondaryMgoAddrs, arbiterMgoAddrs := FmtShardMgoAddrs(mgoCluster.GetName(), mgoCluster.GetNamespace(),
		replicaSetId, shardSpec.Port, shardSpec.NumSecondaryNodes, shardSpec.NumArbiterNodes)

	if initialized, err := mongoclient.CheckMgoReplicaSet(mgoCluster.Spec.DBConnTimeout, replicaSetId, primaryMgoAddr, secondaryMgoAddrs, arbiterMgoAddrs, log); err != nil {
		log.Error(err, "Failed to check the shard", "replicaSetId", replicaSetId)
		return ctrl.Result{}, nil
	} else if initialized {
		log.Info("The shard has already been initialized", "replicaSetId", replicaSetId)
		return ctrl.Result{}, nil
	}

	if err := mongoclient.InitiateMgoReplicaSet(mgoCluster.Spec.DBConnTimeout, replicaSetId, primaryMgoAddr, secondaryMgoAddrs, arbiterMgoAddrs, log); err != nil {
		log.Error(err, "Failed to initiate the shard", "replicaSetId", replicaSetId)
		return ctrl.Result{RequeueAfter: time.Second}, nil
	}
	log.Info("Successfully initialized the shard", "replicaSetId", replicaSetId)

	return ctrl.Result{}, nil
}

func (r *MongoDBClusterReconciler) createShardStatefulSet(ctx context.Context, mgoCluster *mongodbv1.MongoDBCluster,
	shardSpec mongodbv1.MgoShardSpec, replicaSetId string) (retErr error) {

	statefulSet, errStatefulSet := r.newShardStatefulSet(mgoCluster, shardSpec, replicaSetId)
	if errStatefulSet != nil {
		retErr = fmt.Errorf("newShardStatefulSet failed, %v", errStatefulSet)
		return
	}

	if e := ctrl.SetControllerReference(mgoCluster, statefulSet, r.Scheme); e != nil {
		retErr = fmt.Errorf("setControllerReference failed, %v", e)
		return
	}

	if e := r.Create(ctx, statefulSet); e != nil {
		retErr = fmt.Errorf("create failed, %v", e)
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

func (r *MongoDBClusterReconciler) newShardStatefulSet(mgoCluster *mongodbv1.MongoDBCluster,
	shardSpec mongodbv1.MgoShardSpec, replicaSetId string) (*appsv1.StatefulSet, error) {

	objectName := FmtShardStatefulSetName(mgoCluster.GetName(), replicaSetId)
	statefulSet := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      objectName,
			Namespace: mgoCluster.GetNamespace(),
		},

		Spec: appsv1.StatefulSetSpec{
			Replicas: ptr.To(CountNumShardReplicaSetNodes(shardSpec.NumSecondaryNodes, shardSpec.NumArbiterNodes)),
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
			ContainerPort: int32(shardSpec.Port),
			Name:          "data",
		}},
		Command: []string{"mongod"},
		Args: []string{
			"--shardsvr",
			"--replSet",
			replicaSetId,
			"--dbpath",
			shardSpec.DataPath,
			"--port",
			strconv.Itoa(int(shardSpec.Port)),
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

func FmtShardStatefulSetName(clusterName, confReplicaSetId string) string {
	return fmtComponentTypeObjectName(clusterName, mongodbv1.ComponentTypeShard, confReplicaSetId)
}

func FmtShardServiceName(clusterName, confReplicaSetId string) string {
	return fmtComponentTypeObjectName(clusterName, mongodbv1.ComponentTypeShard, confReplicaSetId)
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
