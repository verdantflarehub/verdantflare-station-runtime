package executor

import (
	"context"
	"encoding/hex"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func (k *Kubernetes) stepOCIChart(ctx context.Context, t Target, action, operation, station, org string) (Result, error) {
	if k.Charts == nil {
		return failed("SERVICE_UNAVAILABLE", "Helm execution is unavailable"), nil
	}
	deployments := k.Client.AppsV1().Deployments(t.Deployment.Namespace)
	live, err := deployments.Get(ctx, t.Deployment.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		if action != "install" {
			return failed("NOT_FOUND", "Application workload is absent"), nil
		}
		live = nil
	} else if err != nil {
		return Result{}, err
	}
	if live != nil {
		if !appCenterWorkloadMatches(t, live, action, operation, station, org) {
			return failed("CONFLICT", "Protected workload identity or ownership changed"), nil
		}
		if action == "install" && !sameImages(live, t.Deployment) {
			return failed("CONFLICT", "Existing workload differs from the fixed Chart target"), nil
		}
	}
	if err := k.dependencies(ctx, t); err != nil {
		return failed("SERVICE_UNAVAILABLE", err.Error()), nil
	}
	creationOperation := operation
	if live != nil && live.Annotations[appCenterOperationKey] != "" {
		creationOperation = live.Annotations[appCenterOperationKey]
	}
	changed, err := k.Charts.Ensure(ctx, t, action, creationOperation, station, org)
	if err != nil {
		if ctx.Err() != nil {
			return Result{}, err
		}
		return failed("SERVICE_UNAVAILABLE", "Helm execution failed: "+err.Error()), nil
	}
	if changed {
		return Result{"running", "installing", "", ""}, nil
	}
	live, err = deployments.Get(ctx, t.Deployment.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return failed("CONFLICT", "Helm release exists but the bound workload is missing"), nil
	}
	if err != nil {
		return Result{}, err
	}
	if !ownedWorkload(live.Annotations, t, station, org) || live.Annotations[hashKey] != hex.EncodeToString(t.Hash[:]) || !sameImages(live, t.Deployment) {
		return failed("CONFLICT", "Helm workload differs from the fixed application version"), nil
	}
	for _, wanted := range t.Services {
		svc, err := k.Client.CoreV1().Services(wanted.Namespace).Get(ctx, wanted.Name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return Result{"running", "installing", "", ""}, nil
		}
		if err != nil {
			return Result{}, err
		}
		if !ownedWorkload(svc.Annotations, t, station, org) {
			return failed("CONFLICT", "Helm Service ownership differs from the installation"), nil
		}
	}
	for _, c := range live.Status.Conditions {
		if (c.Type == appsv1.DeploymentProgressing && c.Status == corev1.ConditionFalse && c.Reason == "ProgressDeadlineExceeded") ||
			(c.Type == appsv1.DeploymentReplicaFailure && c.Status == corev1.ConditionTrue) {
			return failed("SERVICE_UNAVAILABLE", "Chart workload rollout failed"), nil
		}
	}
	desired := int32(1)
	if live.Spec.Replicas != nil {
		desired = *live.Spec.Replicas
	}
	if desired > 0 && live.Status.ObservedGeneration >= live.Generation && live.Status.Replicas == desired && live.Status.UpdatedReplicas == desired && live.Status.ReadyReplicas == desired && live.Status.AvailableReplicas == desired {
		return Result{"succeeded", "completed", "", ""}, nil
	}
	return Result{"running", "installing", "", ""}, nil
}

func (k *Kubernetes) stepOCIChartDelete(ctx context.Context, t Target, station, org string) (Result, error) {
	if k.Charts == nil {
		return failed("SERVICE_UNAVAILABLE", "Helm execution is unavailable"), nil
	}
	changed, err := k.Charts.Uninstall(ctx, t, station, org)
	if err != nil {
		if ctx.Err() != nil {
			return Result{}, err
		}
		return failed("SERVICE_UNAVAILABLE", "Helm uninstall failed: "+err.Error()), nil
	}
	if changed {
		return Result{"running", "stopping", "", ""}, nil
	}
	if _, err := k.Client.AppsV1().Deployments(t.Deployment.Namespace).Get(ctx, t.Deployment.Name, metav1.GetOptions{}); err == nil {
		return failed("CONFLICT", "Helm release is absent but the workload still exists"), nil
	} else if !apierrors.IsNotFound(err) {
		return Result{}, err
	}
	for _, wanted := range t.Services {
		if _, err := k.Client.CoreV1().Services(wanted.Namespace).Get(ctx, wanted.Name, metav1.GetOptions{}); err == nil {
			return Result{"running", "stopping", "", ""}, nil
		} else if !apierrors.IsNotFound(err) {
			return Result{}, err
		}
	}
	return Result{"succeeded", "completed", "", ""}, nil
}
