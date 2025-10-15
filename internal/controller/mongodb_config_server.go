package controller

import (
	"context"
	"errors"
	"fmt"
	"strconv"
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

func (r *MongoDBClusterReconciler) reconcileConfigServer(ctx context.Context, log logr.Logger, mgoCluster *mongodbv1.MongoDBCluster) (ctrlRet ctrl.Result, retErr error) {
	if ctrlRet, retErr = r.reconcileConfigServerService(ctx, log, mgoCluster); retErr != nil {
		retErr = fmt.Errorf("resource: Service, replicaSetId: %v, error: %v", mgoCluster.Spec.ConfigServer.ReplicaSetId, retErr)
		return
	}

	if ctrlRet, retErr = r.reconcileConfigServerStatefulSet(ctx, log, mgoCluster); retErr != nil {
		retErr = fmt.Errorf("resource: StatefulSet, replicaSetId: %v, error: %v", mgoCluster.Spec.ConfigServer.ReplicaSetId, retErr)
		return
	}

	return
}

func (r *MongoDBClusterReconciler) reconcileConfigServerService(ctx context.Context, log logr.Logger,
	mgoCluster *mongodbv1.MongoDBCluster) (ctrl.Result, error) {

	confSrvSpec := mgoCluster.Spec.ConfigServer

	var svc corev1.Service
	svcName := FmtConfigServerServiceName(mgoCluster.GetName(), confSrvSpec.ReplicaSetId)
	key := client.ObjectKey{
		Namespace: mgoCluster.GetNamespace(),
		Name:      svcName,
	}

	if err := r.Get(ctx, key, &svc); err != nil {
		if !apierrors.IsNotFound(err) {
			return ctrl.Result{}, fmt.Errorf("unable to get the service, %v", err)
		}

		svc.ObjectMeta.Name = svcName
		svc.ObjectMeta.Namespace = mgoCluster.GetNamespace()
		svc.Spec.Type = corev1.ServiceTypeClusterIP
		svc.Spec.ClusterIP = "None"
		svc.Spec.Selector = map[string]string{
			"component-type": string(mongodbv1.ComponentTypeConfigServer),
			"replicaset-id":  confSrvSpec.ReplicaSetId,
		}
		svc.Spec.Ports = append(svc.Spec.Ports, corev1.ServicePort{
			Name:       "conf",
			Port:       int32(confSrvSpec.Port),
			TargetPort: intstr.FromInt(int(confSrvSpec.Port)),
		})

		if e := ctrl.SetControllerReference(mgoCluster, &svc, r.Scheme); e != nil {
			return ctrl.Result{}, fmt.Errorf("unable to set the controller reference, %v", e)
		}

		if e := r.Create(ctx, &svc); e != nil {
			return ctrl.Result{}, fmt.Errorf("unable to create a service, %v", e)
		}

		log.Info("Successfully created a service for config server", "replicaSetId", confSrvSpec.ReplicaSetId)
		return ctrl.Result{}, nil
	} else {
		log.Info("The service of config server exists", "replicaSetId", confSrvSpec.ReplicaSetId)
	}

	return ctrl.Result{}, nil
}

func (r *MongoDBClusterReconciler) reconcileConfigServerStatefulSet(ctx context.Context, log logr.Logger,
	mgoCluster *mongodbv1.MongoDBCluster) (ctrl.Result, error) {

	confSrvSpec := mgoCluster.Spec.ConfigServer

	var foundStatefulSet appsv1.StatefulSet
	key := client.ObjectKey{
		Namespace: mgoCluster.GetNamespace(),
		Name:      FmtConfigServerStatefulSetName(mgoCluster.GetName(), confSrvSpec.ReplicaSetId),
	}

	if err := r.Get(ctx, key, &foundStatefulSet); err != nil {
		if !apierrors.IsNotFound(err) {
			return ctrl.Result{}, fmt.Errorf("unable to get the stateful set, %v", err)
		}

		if e := r.createConfigServerStatefulSet(ctx, mgoCluster); e != nil {
			return ctrl.Result{}, e
		}
		log.Info("Successfully created a stateful set for config server", "replicaSetId", confSrvSpec.ReplicaSetId)
		return ctrl.Result{}, nil
	} else {
		log.Info("The stateful set of config server exists", "replicaSetId", confSrvSpec.ReplicaSetId)
	}

	if foundStatefulSet.Spec.Replicas == nil || (*foundStatefulSet.Spec.Replicas) != confSrvSpec.NumReplicas {
		cpFoundStatefulSet := foundStatefulSet.DeepCopy()
		cpFoundStatefulSet.Spec.Replicas = ptr.To(confSrvSpec.NumReplicas)
		if err := r.Update(ctx, cpFoundStatefulSet); err != nil {
			return ctrl.Result{}, fmt.Errorf("unable to update the stateful set, %v", err)
		}
		return ctrl.Result{RequeueAfter: time.Second}, nil
	}

	if foundStatefulSet.Status.ReadyReplicas != confSrvSpec.NumReplicas || foundStatefulSet.Status.UpdatedReplicas != confSrvSpec.NumReplicas {
		log.Info("Waiting for all pods of the config replica set to be ready",
			"replicaSetId", confSrvSpec.ReplicaSetId,
			"replicas", confSrvSpec.NumReplicas,
			"readyReplicas", foundStatefulSet.Status.ReadyReplicas,
			"updatedReplicas", foundStatefulSet.Status.UpdatedReplicas,
		)
		return ctrl.Result{RequeueAfter: time.Second}, nil
	}

	// The pods of the replica set are reconciled, check or initialize the replica set
	if err := r.checkAndSetMgoReplicaSetSetup(mgoCluster, log); err != nil {
		return ctrl.Result{RequeueAfter: time.Second}, err
	}

	return ctrl.Result{}, nil
}

func (r *MongoDBClusterReconciler) createConfigServerStatefulSet(ctx context.Context, mgoCluster *mongodbv1.MongoDBCluster) (retErr error) {
	statefulSet, errStatefulSet := r.newConfigServerStatefulSet(mgoCluster)
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

func (r *MongoDBClusterReconciler) newConfigServerStatefulSet(mgoCluster *mongodbv1.MongoDBCluster) (*appsv1.StatefulSet, error) {
	confSrvSpec := mgoCluster.Spec.ConfigServer

	statefulSet := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      FmtConfigServerStatefulSetName(mgoCluster.GetName(), confSrvSpec.ReplicaSetId),
			Namespace: mgoCluster.GetNamespace(),
		},

		Spec: appsv1.StatefulSetSpec{
			Replicas: ptr.To(confSrvSpec.NumReplicas),
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"component-type": string(mongodbv1.ComponentTypeConfigServer)},
			},
			ServiceName: FmtConfigServerServiceName(mgoCluster.GetName(), confSrvSpec.ReplicaSetId),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"component-type": string(mongodbv1.ComponentTypeConfigServer),
						"replicaset-id":  confSrvSpec.ReplicaSetId,
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
			ContainerPort: int32(confSrvSpec.Port),
			Name:          "data",
		}},
		Command: []string{"mongod"},
		Args: []string{
			"--configsvr",
			"--replSet",
			confSrvSpec.ReplicaSetId,
			"--dbpath",
			confSrvSpec.DataPath,
			"--port",
			strconv.Itoa(int(confSrvSpec.Port)),
			"--bind_ip_all",
		},
	}
	if res, ok := mgoCluster.Spec.ResourceRequirements["configServer"]; ok {
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

func (r *MongoDBClusterReconciler) checkAndSetMgoReplicaSetSetup(mgoCluster *mongodbv1.MongoDBCluster, log logr.Logger) error {
	confSrvSpec := mgoCluster.Spec.ConfigServer
	mgoAddrs := FmtConfigServerMgoAddrs(mgoCluster.GetName(), mgoCluster.GetNamespace(), confSrvSpec.ReplicaSetId, confSrvSpec.NumReplicas, confSrvSpec.Port)

	if initialized, err := mongoclient.CheckReplicaSet(mgoCluster.Spec.DBConnTimeout, confSrvSpec.ReplicaSetId, mgoAddrs[0], mgoAddrs[1:], nil); err != nil {
		return fmt.Errorf("an error occurred while checking the replica set of config server in the mongodb cluster, %v", err)
	} else if initialized {
		log.Info("The replica set of config server has already been initialized in the mongodb cluster", "replicaSetId", confSrvSpec.ReplicaSetId)
		return nil
	}

	if err := mongoclient.InitiateReplicaSet(mgoCluster.Spec.DBConnTimeout, confSrvSpec.ReplicaSetId, mgoAddrs[0], mgoAddrs[1:], nil); err != nil {
		return fmt.Errorf("an error occurred while initializing the replica set of config server in the mongodb cluster, %v", err)
	}
	log.Info("Successfully initialized the replica set of config server", "replicaSetId", confSrvSpec.ReplicaSetId)
	return nil
}

func FmtConfigServerStatefulSetName(clusterName, confReplicaSetId string) string {
	return fmtComponentTypeObjectName(clusterName, mongodbv1.ComponentTypeConfigServer, confReplicaSetId)
}

func FmtConfigServerServiceName(clusterName, confReplicaSetId string) string {
	return fmtComponentTypeObjectName(clusterName, mongodbv1.ComponentTypeConfigServer, confReplicaSetId)
}

func FmtConfigServerMgoAddrs(clusterName, ns, replicaSetId string, numReplicas int32, port uint16) (retAddrs []string) {
	svcName := FmtConfigServerServiceName(clusterName, replicaSetId)
	statefulSetName := FmtConfigServerStatefulSetName(clusterName, replicaSetId)

	// Just for testing
	k8sClusterDomain := "quick3"

	for i := 0; i < int(numReplicas); i++ {
		podName := fmt.Sprintf("%s-%d", statefulSetName, i)
		retAddrs = append(retAddrs, fmt.Sprintf("%s.%s.%s.svc.%s:%d", podName, svcName, ns, k8sClusterDomain, port))
	}

	return
}
