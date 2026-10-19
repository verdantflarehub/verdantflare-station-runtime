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

func ownedWorkload(a map[string]string, t Target, station, org string) bool {
	if t.OCIChart == nil {
		return owned(a, t, station, org)
	}
	if a[ownerKey] != station || a[orgKey] != org || a[appKey] != t.AppID || len(a[hashKey]) != 64 {
		return false
	}
	if t.InstallationID != "" && a[installationKey] != t.InstallationID {
		return false
	}
	return true
}

func workloadCompatible(t Target, live *appsv1.Deployment) bool {
	if t.OCIChart == nil {
		return preinstalledDeploymentCompatible(t.Deployment, live)
	}
	return live != nil && t.Deployment != nil && live.Namespace == t.Deployment.Namespace && live.Name == t.Deployment.Name && sameImages(live, t.Deployment)
}

func serviceCompatible(t Target, wanted, live *corev1.Service) bool {
	if t.OCIChart == nil {
		return preinstalledServiceCompatible(wanted, live)
	}
	return live != nil && wanted != nil && live.Namespace == wanted.Namespace && live.Name == wanted.Name
}

func appCenterWorkloadMatches(t Target, live *appsv1.Deployment, action, operation, station, org string) bool {
	compatible := workloadCompatible(t, live)
	if !compatible && t.OCIChart != nil && action == "upgrade" {
		compatible = live != nil && t.Deployment != nil && live.Namespace == t.Deployment.Namespace && live.Name == t.Deployment.Name &&
			len(live.Spec.Template.Spec.Containers) == len(t.Deployment.Spec.Template.Spec.Containers)
	}
	if (live.DeletionTimestamp != nil && action != "delete") || !ownedWorkload(live.Annotations, t, station, org) || !compatible {
		return false
	}
	if action == "install" {
		return live.Annotations[appCenterOperationKey] == operation
	}
	return (action == "start" || action == "stop" || action == "restart" || action == "delete" || action == "upgrade") && string(live.UID) == t.ExpectedUID
}

func appCenterServiceMatches(t Target, wanted, live *corev1.Service, action, operation, station, org string) bool {
	return (live.DeletionTimestamp == nil || action == "delete") && ownedWorkload(live.Annotations, t, station, org) &&
		serviceCompatible(t, wanted, live) && (action != "install" || live.Annotations[appCenterOperationKey] == operation)
}

func (k *Kubernetes) CheckAppCenter(ctx context.Context, t Target, action, operation, station, org string, first bool) error {
	conflict := status.Error(codes.FailedPrecondition, "Protected workload identity or ownership changed")
	if !t.AppCenter || (t.InstallationID == "" && !(first && action == "install")) || t.Deployment == nil ||
		(action != "install" && action != "upgrade" && action != "start" && action != "stop" && action != "restart" && action != "delete") ||
		((action == "upgrade" || action == "start" || action == "stop" || action == "restart" || action == "delete") && t.ExpectedUID == "") {
		return conflict
	}
	live, err := k.Client.AppsV1().Deployments(t.Deployment.Namespace).Get(ctx, t.Deployment.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		if action != "install" && !(action == "delete" && !first) {
			return conflict
		}
	} else if err != nil {
		return status.Error(codes.Unavailable, "Protected workload observation unavailable")
	} else if (first && action == "install") || (first && action == "delete" && live.DeletionTimestamp != nil) || !appCenterWorkloadMatches(t, live, action, operation, station, org) {
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
		if (first && action == "install") || (first && action == "delete" && live.DeletionTimestamp != nil) || !appCenterServiceMatches(t, wanted, live, action, operation, station, org) {
			return conflict
		}
	}
	return nil
}
