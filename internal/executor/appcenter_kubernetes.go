package executor

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const appCenterOperationKey = "station.verdantflare.com/appcenter-creation-operation"

func appCenterWorkloadMatches(t Target, live *appsv1.Deployment, action, operation, station, org string) bool {
	if live.DeletionTimestamp != nil || !owned(live.Annotations, t, station, org) || !preinstalledDeploymentCompatible(t.Deployment, live) {
		return false
	}
	if action == "install" {
		return live.Annotations[appCenterOperationKey] == operation
	}
	return action == "start" && string(live.UID) == t.ExpectedUID
}

func appCenterServiceMatches(t Target, wanted, live *corev1.Service, action, operation, station, org string) bool {
	return live.DeletionTimestamp == nil && owned(live.Annotations, t, station, org) &&
		preinstalledServiceCompatible(wanted, live) && (action != "install" || live.Annotations[appCenterOperationKey] == operation)
}

func (k *Kubernetes) CheckAppCenter(ctx context.Context, t Target, action, operation, station, org string, first bool) error {
	conflict := status.Error(codes.FailedPrecondition, "Protected workload identity or ownership changed")
	if !t.AppCenter || t.InstallationID == "" || t.Deployment == nil ||
		(action != "install" && action != "start") || (action == "start" && t.ExpectedUID == "") {
		return conflict
	}
	live, err := k.Client.AppsV1().Deployments(t.Deployment.Namespace).Get(ctx, t.Deployment.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		if action != "install" {
			return conflict
		}
	} else if err != nil {
		return status.Error(codes.Unavailable, "Protected workload observation unavailable")
	} else if (first && action == "install") || !appCenterWorkloadMatches(t, live, action, operation, station, org) {
		return conflict
	}
	for _, wanted := range t.Services {
		live, err := k.Client.CoreV1().Services(wanted.Namespace).Get(ctx, wanted.Name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			continue
		}
		if err != nil {
			return status.Error(codes.Unavailable, "Protected Service observation unavailable")
		}
		if (first && action == "install") || !appCenterServiceMatches(t, wanted, live, action, operation, station, org) {
			return conflict
		}
	}
	return nil
}
