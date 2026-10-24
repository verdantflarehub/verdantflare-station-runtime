package instances

import (
	"context"
	"encoding/json"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

type workerRecovery struct {
	Start  StartRequest      `json:"start"`
	Worker WorkerObservation `json:"worker"`
}

func prepareWorkerRecovery(ctx context.Context, client kubernetes.Interface, next StartRequest, p HelperProfile, b VolumeBinding, input, state []byte) (*workerRecovery, error) {
	var previous StartRequest
	var execution startState
	if json.Unmarshal(input, &previous) != nil || json.Unmarshal(state, &execution) != nil ||
		previous.identity() != next.identity() || previous.ProfileID != next.ProfileID ||
		previous.OperationID == next.OperationID ||
		execution.Proof == nil || execution.Worker.PodUID == "" || execution.Recovery != nil {
		return nil, ErrBinding
	}
	previous, err := effectiveStart(previous, execution, p)
	if err != nil {
		return nil, err
	}
	if previous.source() != next.source() {
		return nil, ErrBinding
	}
	desired, _, err := workerObjects(previous.identity(), p, b, previous.OperationID, previous.source())
	if err != nil {
		return nil, err
	}
	pod, err := client.CoreV1().Pods(p.Namespace).Get(ctx, desired.Name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	if !workerPodMatches(pod, desired, execution.Worker.PodUID) || pod.Name != execution.Worker.PodName {
		return nil, ErrBinding
	}
	recovery := &workerRecovery{Start: previous, Worker: execution.Worker}
	if err = verifyFailedWorker(ctx, client, p, *recovery, false); err != nil {
		return nil, err
	}
	return recovery, nil
}

func verifyFailedWorker(ctx context.Context, client kubernetes.Interface, p HelperProfile, recovery workerRecovery, allowMissing bool) error {
	node, err := client.CoreV1().Nodes().Get(ctx, p.NodeName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	ready := false
	for _, condition := range node.Status.Conditions {
		if condition.Type == corev1.NodeReady && condition.Status == corev1.ConditionTrue {
			ready = true
		}
	}
	if !ready || !supportedGPU(node) {
		return ErrComputeUnknown
	}
	pod, err := client.CoreV1().Pods(p.Namespace).Get(ctx, recovery.Worker.PodName, metav1.GetOptions{})
	if allowMissing && apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if string(pod.UID) != recovery.Worker.PodUID || pod.Spec.NodeName != p.NodeName ||
		(pod.Status.Phase != corev1.PodFailed && pod.Status.Phase != corev1.PodSucceeded) {
		return ErrBinding
	}
	return nil
}
