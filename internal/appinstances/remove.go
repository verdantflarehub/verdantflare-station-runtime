package appinstances

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
)

// StopProgress must come from a durable, authorized coordinator, never a client.
// Every intent is committed before deletion. The instance credential and volume
// remain available for the next explicitly authorized run.
type StopProgress struct {
	Proof WorkerStopProof `json:"proof"`
	Phase string          `json:"phase"`
}

var stopPhases = map[string]int{"sealed": 0, "pod_delete_requested": 1, "pod_deleted": 2, "service_delete_requested": 3, "stopped": 4}

func RemoveWorker(ctx context.Context, client kubernetes.Interface, in Run, p Profile, volume Volume, observed Observation, progress StopProgress,
	checkpoint func(context.Context, StopProgress) error) (StopProgress, error) {
	stage, ok := stopPhases[progress.Phase]
	if !ok || checkpoint == nil || !observed.valid() || !validBoot(observed.BootID) || observed.PodUID == "" ||
		!validID(progress.Proof.StopOperationID) || progress.Proof.StopOperationID == in.OperationID || progress.Proof != stopProof(in, p, observed, progress.Proof.StopOperationID) {
		return progress, ErrInvalid
	}
	desiredPod, desiredService, e := WorkerObjects(in, p, volume)
	if e != nil {
		return progress, e
	}
	node, e := client.CoreV1().Nodes().Get(ctx, p.NodeName, metav1.GetOptions{})
	if e != nil || node.DeletionTimestamp != nil {
		return progress, ErrUnavailable
	}
	ready := false
	for _, condition := range node.Status.Conditions {
		if condition.Type == corev1.NodeReady {
			ready = condition.Status == corev1.ConditionTrue
		}
	}
	if !ready {
		return progress, ErrNotReady
	}
	pod, e := client.CoreV1().Pods(p.Namespace).Get(ctx, desiredPod.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(e) {
		if stage < 1 {
			return progress, ErrBinding
		}
		pod = nil
	} else if e != nil {
		return progress, e
	} else {
		checked := pod.DeepCopy()
		if pod.DeletionTimestamp != nil {
			if stage != 1 || pod.DeletionGracePeriodSeconds == nil || *pod.DeletionGracePeriodSeconds <= 0 {
				return progress, ErrBinding
			}
			checked.DeletionTimestamp = nil
		}
		if stage >= 2 || !podMatches(checked, desiredPod, observed.PodUID, p.NodeName) || pod.ResourceVersion == "" || pod.Spec.NodeName != p.NodeName {
			return progress, ErrBinding
		}
	}
	svc, e := client.CoreV1().Services(p.Namespace).Get(ctx, desiredService.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(e) {
		if stage < 3 {
			return progress, ErrBinding
		}
		svc = nil
	} else if e != nil {
		return progress, e
	} else {
		checked := svc.DeepCopy()
		if svc.DeletionTimestamp != nil {
			if stage != 3 {
				return progress, ErrBinding
			}
			checked.DeletionTimestamp = nil
		}
		if stage >= 4 || !serviceMatches(checked, desiredService, observed.ServiceUID) || svc.ResourceVersion == "" {
			return progress, ErrBinding
		}
	}
	if _, e = InspectVolume(ctx, client, in.Identity, p, volume.PVCName, &volume, map[types.UID]bool{types.UID(observed.PodUID): true}); e != nil {
		return progress, e
	}
	if e = volumeClaimed(ctx, client, in.Identity, p, volume); e != nil {
		return progress, e
	}
	secret, e := client.CoreV1().Secrets(p.Namespace).Get(ctx, instanceName(in.Identity), metav1.GetOptions{})
	if e != nil || !secretMatches(secret, in.Identity, p, observed.SecretUID) {
		return progress, ErrBinding
	}
	advance := func(phase string) error {
		next := StopProgress{Proof: progress.Proof, Phase: phase}
		if stopPhases[phase] != stopPhases[progress.Phase]+1 {
			return ErrConflict
		}
		if e := checkpoint(ctx, next); e != nil {
			return e
		}
		progress = next
		return nil
	}
	if progress.Phase == "sealed" {
		if e = advance("pod_delete_requested"); e != nil {
			return progress, e
		}
	}
	if progress.Phase == "pod_delete_requested" {
		if pod != nil {
			if pod.DeletionTimestamp == nil {
				// Never force termination. UID and RV fence replacement and spec drift.
				grace := int64(30)
				policy := metav1.DeletePropagationBackground
				e = client.CoreV1().Pods(p.Namespace).Delete(ctx, pod.Name, metav1.DeleteOptions{
					Preconditions: &metav1.Preconditions{UID: &pod.UID, ResourceVersion: &pod.ResourceVersion}, GracePeriodSeconds: &grace, PropagationPolicy: &policy})
				if e != nil {
					return progress, e
				}
			}
			// The next call observes termination. A successful DELETE is not proof.
			return progress, nil
		}
		if e = advance("pod_deleted"); e != nil {
			return progress, e
		}
	}
	if progress.Phase == "pod_deleted" {
		if e = advance("service_delete_requested"); e != nil {
			return progress, e
		}
	}
	if progress.Phase == "service_delete_requested" {
		if svc != nil {
			if svc.DeletionTimestamp == nil {
				e = client.CoreV1().Services(p.Namespace).Delete(ctx, svc.Name, metav1.DeleteOptions{
					Preconditions: &metav1.Preconditions{UID: &svc.UID, ResourceVersion: &svc.ResourceVersion}})
				if e != nil {
					return progress, e
				}
			}
			return progress, nil
		}
		if e = advance("stopped"); e != nil {
			return progress, e
		}
	}
	return progress, nil
}
