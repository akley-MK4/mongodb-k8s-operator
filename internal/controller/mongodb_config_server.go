package controller

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	mongodbv1 "github.com/akley-MK4/mongodb-k8s-operator/api/v1"
	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func (r *MongoDBClusterReconciler) reconcileConfigServer(ctx context.Context, log logr.Logger, mgoCluster *mongodbv1.MongoDBCluster) (ctrlRet ctrl.Result, retErr error) {
	defer func() {
		if retErr != nil {
			meta.SetStatusCondition(&mgoCluster.Status.Conditions, metav1.Condition{Type: "Available",
				Status: metav1.ConditionFalse, Reason: "ReconcilingConfigServer",
				Message: fmt.Sprintf("Failed to reconcile the config server, %v", retErr)})
		}
	}()

	if ctrlRet, retErr = r.reconcileConfigServerService(ctx, log, mgoCluster); retErr != nil {
		return
	}

	if ctrlRet, retErr = r.reconcileConfigServerStatefulSet(ctx, log, mgoCluster); retErr != nil {
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
			return ctrl.Result{}, err
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
			return ctrl.Result{}, fmt.Errorf("SetControllerReference failed, %v", e)
		}

		if e := r.Create(ctx, &svc); e != nil {
			return ctrl.Result{}, fmt.Errorf("create failed, %v", e)
		}

		log.Info("Successfully created a service for %v %v", mongodbv1.ComponentTypeConfigServer, confSrvSpec.ReplicaSetId)
		return ctrl.Result{}, nil
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
			return ctrl.Result{}, err
		}

		if e := r.createConfigServerStatefulSet(ctx, mgoCluster); e != nil {
			return ctrl.Result{}, e
		}
		log.Info("Successfully created a StatefulSet for the config server %v", confSrvSpec.ReplicaSetId)
		return ctrl.Result{}, nil

	}

	if foundStatefulSet.Spec.Replicas == nil || (*foundStatefulSet.Spec.Replicas) != confSrvSpec.NumReplicas {
		foundStatefulSet.Spec.Replicas = ptr.To(confSrvSpec.NumReplicas)
		if err := r.Update(ctx, &foundStatefulSet); err != nil {
			if e := r.Get(ctx, key, &foundStatefulSet); e != nil {
				return ctrl.Result{}, e
			}

			// The following implementation will update the status
			meta.SetStatusCondition(&mgoCluster.Status.Conditions, metav1.Condition{Type: "Available",
				Status: metav1.ConditionFalse, Reason: "Resizing",
				Message: fmt.Sprintf("Failed to update the size for the custom resource (%s): (%s)", mgoCluster.Name, err)})

			if e := r.Status().Update(ctx, mgoCluster); e != nil {
				return ctrl.Result{}, err
			}

			return ctrl.Result{}, err
		}

		return ctrl.Result{Requeue: true}, nil
	}

	// check the status if the rs is ready

	return ctrl.Result{}, nil
}

func (r *MongoDBClusterReconciler) createConfigServerStatefulSet(ctx context.Context, mgoCluster *mongodbv1.MongoDBCluster) (retErr error) {
	statefulSet, errStatefulSet := r.newConfigServerStatefulSet(mgoCluster)
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

func FmtConfigServerStatefulSetName(clusterName, confReplicaSetId string) string {
	return fmtComponentTypeObjectName(clusterName, mongodbv1.ComponentTypeConfigServer, confReplicaSetId)
}

func FmtConfigServerServiceName(clusterName, confReplicaSetId string) string {
	return fmtComponentTypeObjectName(clusterName, mongodbv1.ComponentTypeConfigServer, confReplicaSetId)
}

func FmtConfigServerURIList(clusterName, ns, replicaSetId string, numReplicas int32, port uint16) (retURIs []string) {
	svcName := FmtConfigServerServiceName(clusterName, replicaSetId)
	statefulSetName := FmtConfigServerStatefulSetName(clusterName, replicaSetId)

	// Just for testing
	k8sClusterDomain := "quick3"

	for i := 0; i < int(numReplicas); i++ {
		podName := fmt.Sprintf("%s-%d", statefulSetName, i)
		retURIs = append(retURIs, fmt.Sprintf("%s.%s.%s.svc.%s:%d", podName, svcName, ns, k8sClusterDomain, port))
	}

	return
}
