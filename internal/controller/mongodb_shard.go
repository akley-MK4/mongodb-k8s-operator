package controller

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

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

const (
	FixReplicas = int32(3)
)

func (r *MongoDBClusterReconciler) reconcileShards(ctx context.Context, log logr.Logger, mgoCluster *mongodbv1.MongoDBCluster) (ctrlRet ctrl.Result, retErr error) {
	defer func() {
		if retErr != nil {
			meta.SetStatusCondition(&mgoCluster.Status.Conditions, metav1.Condition{Type: "Available",
				Status: metav1.ConditionFalse, Reason: "Reconciling",
				Message: fmt.Sprintf("Failed to reconcile the shards, %v", retErr)})
		}
	}()

	for shardName, shardSpec := range mgoCluster.Spec.Shards {
		if ctrlRet, retErr = r.reconcileShardHeadlessService(ctx, log, mgoCluster, *shardSpec, shardName); retErr != nil {
			return
		}

		if ctrlRet, retErr = r.reconcileShardStatefulSet(ctx, log, mgoCluster, *shardSpec, shardName); retErr != nil {
			return
		}
	}

	return
}

func (r *MongoDBClusterReconciler) reconcileShardHeadlessService(ctx context.Context, log logr.Logger, mgoCluster *mongodbv1.MongoDBCluster,
	shardSpec mongodbv1.MgoShardSpec, componentName string) (ctrl.Result, error) {

	var svc corev1.Service
	name := fmtComponentTypeObjectName(mgoCluster.GetName(), mongodbv1.ComponentTypeShard, componentName)
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
			"component-name": componentName,
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

		log.Info("Successfully created a headless service for %v %v", mongodbv1.ComponentTypeShard, componentName)
		return ctrl.Result{}, nil
	}

	return ctrl.Result{}, nil
}

func (r *MongoDBClusterReconciler) reconcileShardStatefulSet(ctx context.Context, log logr.Logger, mgoCluster *mongodbv1.MongoDBCluster,
	shardSpec mongodbv1.MgoShardSpec, componentName string) (ctrl.Result, error) {

	var foundStatefulSet appsv1.StatefulSet
	key := client.ObjectKey{
		Namespace: mgoCluster.GetNamespace(),
		Name:      fmtComponentTypeObjectName(mgoCluster.GetName(), mongodbv1.ComponentTypeShard, componentName),
	}

	if err := r.Get(ctx, key, &foundStatefulSet); err != nil {
		if !apierrors.IsNotFound(err) {
			return ctrl.Result{}, err
		}

		if e := r.createShardStatefulSet(ctx, mgoCluster, shardSpec, componentName); e != nil {
			return ctrl.Result{}, e
		}
		log.Info("Successfully created a StatefulSet for shard %v", componentName)
		return ctrl.Result{}, nil

	}

	if foundStatefulSet.Spec.Replicas == nil || (*foundStatefulSet.Spec.Replicas) != FixReplicas {
		foundStatefulSet.Spec.Replicas = ptr.To(FixReplicas)
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

func (r *MongoDBClusterReconciler) createShardStatefulSet(ctx context.Context, mgoCluster *mongodbv1.MongoDBCluster,
	shardSpec mongodbv1.MgoShardSpec, componentName string) (retErr error) {

	statefulSet, errStatefulSet := r.newShardStatefulSet(mgoCluster, shardSpec, componentName)
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

func fmtComponentTypeObjectName(mgoClusterName string, componentType mongodbv1.ComponentType, componentName string) string {
	return strings.Join([]string{mgoClusterName, string(componentType), componentName}, "-")
}

func (r *MongoDBClusterReconciler) newShardStatefulSet(mgoCluster *mongodbv1.MongoDBCluster,
	shardSpec mongodbv1.MgoShardSpec, componentName string) (*appsv1.StatefulSet, error) {

	objectName := fmtComponentTypeObjectName(mgoCluster.GetName(), mongodbv1.ComponentTypeShard, componentName)
	statefulSet := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      objectName,
			Namespace: mgoCluster.GetNamespace(),
		},

		Spec: appsv1.StatefulSetSpec{
			Replicas: ptr.To(FixReplicas),
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"component-type": string(mongodbv1.ComponentTypeShard)},
			},
			ServiceName: fmtComponentTypeObjectName(mgoCluster.GetName(), mongodbv1.ComponentTypeShard, componentName),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"component-type": string(mongodbv1.ComponentTypeShard),
						"component-name": componentName,
					},
				},
			},
		},
	}

	var containers []corev1.Container

	// mongod
	imageMongod := mgoCluster.Spec.Images["mongod"]
	if imageMongod == "" {
		return nil, errors.New("the image of mongod is not configured")
	}

	mongodContainer := corev1.Container{
		Name:            "mongod",
		Image:           imageMongod,
		ImagePullPolicy: mgoCluster.Spec.ImagePullPolicy,
		Ports: []corev1.ContainerPort{{
			ContainerPort: int32(shardSpec.Port),
			Name:          "data",
		}},
		Command: []string{"mongod"},
		Args: []string{
			"--shardsvr",
			"--replSet",
			componentName,
			"--dbpath",
			shardSpec.DataPath,
			"--port",
			strconv.Itoa(int(shardSpec.Port)),
			"--bind_ip_all",
		},
	}
	if res, ok := mgoCluster.Spec.ResourceRequirements["mongod"]; ok {
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
