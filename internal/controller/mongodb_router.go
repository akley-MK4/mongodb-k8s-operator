package controller

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	mongodbv1 "github.com/akley-MK4/mongodb-k8s-operator/api/v1"
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

func (r *MongoDBClusterReconciler) reconcileRouters(ctx context.Context, log logr.Logger, mgoCluster *mongodbv1.MongoDBCluster) (retCtrl ctrl.Result, retErr error) {
	if retCtrl, retErr = r.reconcileRouterService(ctx, log, mgoCluster); retErr != nil {
		retErr = fmt.Errorf("resource: Service, error: %v", retErr)
		return
	}

	if retCtrl, retErr = r.reconcileRouterDeployment(ctx, log, mgoCluster); retErr != nil {
		retErr = fmt.Errorf("resource: Deployment, error: %v", retErr)
		return
	}

	return
}

func (r *MongoDBClusterReconciler) reconcileRouterService(ctx context.Context, log logr.Logger,
	mgoCluster *mongodbv1.MongoDBCluster) (ctrl.Result, error) {

	routersSpec := mgoCluster.Spec.Routers

	var svc corev1.Service
	name := fmtComponentTypeObjectName(mgoCluster.GetName(), mongodbv1.ComponentTypeRouter, "")
	key := client.ObjectKey{
		Namespace: mgoCluster.GetNamespace(),
		Name:      name,
	}

	if err := r.Get(ctx, key, &svc); err != nil {
		if !apierrors.IsNotFound(err) {
			return ctrl.Result{}, fmt.Errorf("unable to get the service, %v", err)
		}

		svc.ObjectMeta.Name = name
		svc.ObjectMeta.Namespace = mgoCluster.GetNamespace()
		svc.Spec.Type = routersSpec.ServiceType
		svc.Spec.Selector = map[string]string{
			"component-type": string(mongodbv1.ComponentTypeRouter),
		}

		// Route
		routeSvcPort := corev1.ServicePort{
			Name:       "route",
			Port:       int32(routersSpec.ServicePort),
			TargetPort: intstr.FromInt(int(routersSpec.Port)),
		}
		if svc.Spec.Type == corev1.ServiceTypeNodePort {
			routeSvcPort.NodePort = int32(routersSpec.ServicePort)
		}
		svc.Spec.Ports = append(svc.Spec.Ports, routeSvcPort)

		if e := ctrl.SetControllerReference(mgoCluster, &svc, r.Scheme); e != nil {
			return ctrl.Result{}, fmt.Errorf("unable to set the controller reference, %v", e)
		}

		if e := r.Create(ctx, &svc); e != nil {
			return ctrl.Result{}, fmt.Errorf("unable to create the service, %v", e)
		}

		log.Info("Successfully created a service for the routers")
		return ctrl.Result{}, nil
	} else {
		log.Info("The service exists for the routers")
	}

	return ctrl.Result{}, nil
}

func (r *MongoDBClusterReconciler) reconcileRouterDeployment(ctx context.Context, log logr.Logger,
	mgoCluster *mongodbv1.MongoDBCluster) (ctrl.Result, error) {

	routersSpec := mgoCluster.Spec.Routers

	var foundDeployment appsv1.Deployment
	key := client.ObjectKey{
		Namespace: mgoCluster.GetNamespace(),
		Name:      fmtComponentTypeObjectName(mgoCluster.GetName(), mongodbv1.ComponentTypeRouter, ""),
	}

	if err := r.Get(ctx, key, &foundDeployment); err != nil {
		if !apierrors.IsNotFound(err) {
			return ctrl.Result{}, fmt.Errorf("unable to get the deployment, %v", err)
		}

		if e := r.createRouterDeployment(ctx, mgoCluster); e != nil {
			return ctrl.Result{}, e
		}
		log.Info("Successfully created a deployment for the routers")
		return ctrl.Result{}, nil
	} else {
		log.Info("The deployment exists for the routers")
	}

	if foundDeployment.Spec.Replicas == nil || (*foundDeployment.Spec.Replicas) != routersSpec.NumReplicas {
		cpFoundDeployment := foundDeployment.DeepCopy()
		cpFoundDeployment.Spec.Replicas = ptr.To(routersSpec.NumReplicas)
		if err := r.Update(ctx, cpFoundDeployment); err != nil {
			return ctrl.Result{}, fmt.Errorf("unable to update the deployment found, %v", err)
		}
		return ctrl.Result{RequeueAfter: time.Second}, nil
	}

	if foundDeployment.Status.ReadyReplicas != routersSpec.NumReplicas || foundDeployment.Status.UpdatedReplicas != routersSpec.NumReplicas {
		log.Info("Waiting for all pods of the router to be ready",
			"replicas", routersSpec.NumReplicas,
			"readyReplicas", foundDeployment.Status.ReadyReplicas,
			"updatedReplicas", foundDeployment.Status.UpdatedReplicas)
		return ctrl.Result{RequeueAfter: time.Second}, nil
	}

	return ctrl.Result{}, nil
}

func (r *MongoDBClusterReconciler) createRouterDeployment(ctx context.Context, mgoCluster *mongodbv1.MongoDBCluster) (retErr error) {
	deployment, errDeployment := r.newRouterDeployment(mgoCluster)
	if errDeployment != nil {
		retErr = fmt.Errorf("unable to create a data object with type Deployment, %v", errDeployment)
		return
	}

	if e := ctrl.SetControllerReference(mgoCluster, deployment, r.Scheme); e != nil {
		retErr = fmt.Errorf("unable to set the controller reference, %v", e)
		return
	}

	if e := r.Create(ctx, deployment); e != nil {
		retErr = fmt.Errorf("unable to create a deployment, %v", e)
		return
	}

	return nil
}

func (r *MongoDBClusterReconciler) newRouterDeployment(mgoCluster *mongodbv1.MongoDBCluster) (*appsv1.Deployment, error) {
	confSrvSpec := mgoCluster.Spec.ConfigServer
	routersSpec := mgoCluster.Spec.Routers
	objectName := fmtComponentTypeObjectName(mgoCluster.GetName(), mongodbv1.ComponentTypeRouter, "")

	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      objectName,
			Namespace: mgoCluster.GetNamespace(),
		},

		Spec: appsv1.DeploymentSpec{
			Replicas: ptr.To(routersSpec.NumReplicas),
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"component-type": string(mongodbv1.ComponentTypeRouter)},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"component-type": string(mongodbv1.ComponentTypeRouter),
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

	confSrvAddrs := FmtConfigServerMgoAddrs(mgoCluster.GetName(), mgoCluster.GetNamespace(), confSrvSpec.ReplicaSetId, confSrvSpec.NumReplicas, confSrvSpec.Port)
	argConfDBURIList := confSrvSpec.ReplicaSetId + "/" + strings.Join(confSrvAddrs, ",")

	mongodContainer := corev1.Container{
		Name:            "mongos",
		Image:           imageMongodb,
		ImagePullPolicy: mgoCluster.Spec.ImagePullPolicy,
		Ports: []corev1.ContainerPort{{
			ContainerPort: int32(routersSpec.Port),
			Name:          "route",
		}},
		Command: []string{"mongos"},
		Args: []string{
			"--configdb",
			argConfDBURIList,
			"--port",
			strconv.Itoa(int(routersSpec.Port)),
			"--bind_ip_all",
		},
	}
	if res, ok := mgoCluster.Spec.ResourceRequirements["router"]; ok {
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

	deployment.Spec.Template.Spec.Containers = containers

	return deployment, nil
}

func FmtRouterDeploymentName(clusterName string) string {
	return fmtComponentTypeObjectName(clusterName, mongodbv1.ComponentTypeRouter, "")
}

func FmtRouterServiceName(clusterName string) string {
	return fmtComponentTypeObjectName(clusterName, mongodbv1.ComponentTypeRouter, "")
}

func FmtRouterMgoAddr(clusterName, ns string, routerServicePort uint16) string {
	svcName := FmtRouterServiceName(clusterName)
	// Just for testing
	k8sClusterDomain := "quick3"

	return fmt.Sprintf("%s.%s.svc.%s:%d", svcName, ns, k8sClusterDomain, routerServicePort)
}
