package instances

import (
	"context"
	"encoding/json"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
)

type workerRecovery struct {
	Start          StartRequest      `json:"start"`
	Worker         WorkerObservation `json:"worker"`
	PriorOperation string            `json:"prior_operation,omitempty"`
	TerminalCode   string            `json:"terminal_code,omitempty"`
	TerminalReason string            `json:"terminal_reason,omitempty"`
}

func terminalPodFailureCode(pod *corev1.Pod) (code, reason string) {
	if pod == nil {
		return "worker_exited", ""
	}
	if pod.Status.Phase == corev1.PodFailed {
		reason = strings.TrimSpace(pod.Status.Reason)
		if reason == "UnexpectedAdmissionError" {
			return "pod_admission_failed", reason
		}
		if reason != "" {
			return "worker_pod_failed", reason
		}
		return "worker_exited", "PodFailed"
	}
	return "worker_exited", string(pod.Status.Phase)
}

// terminalWorkerPodOwned verifies all immutable ownership, volume/secret binding,
// and non-privileged security invariants of a terminated Worker Pod so an upgraded
// HelperProfile (new image, profile-hash, worker-hash, or scheduled template) can
// safely recover and clean up an earlier version's terminal Pod without accepting
// any foreign, replaced, or privileged object.
func terminalWorkerPodOwned(pod *corev1.Pod, in Identity, p HelperProfile, b VolumeBinding, startID, expectedPodName, expectedPodUID string) bool {
	if pod == nil || !validIdentity(in) || startID == "" || startID == in.CreateOperationID || expectedPodUID == "" {
		return false
	}
	expectedName := "blender-run-" + strings.ReplaceAll(startID, "-", "")
	if pod.Namespace != p.Namespace || pod.Name != expectedName || expectedPodName != expectedName || string(pod.UID) != expectedPodUID {
		return false
	}
	if pod.Labels[instanceKey] != in.InstanceID ||
		pod.Labels[componentKey] != "blender-worker" ||
		pod.Labels[startKey] != startID ||
		pod.Labels["app.kubernetes.io/name"] != "blender-worker" ||
		pod.Labels["app.kubernetes.io/part-of"] != "verdantflare-blender" {
		return false
	}
	if pod.Annotations[stationKey] != in.StationID ||
		pod.Annotations[organizationKey] != in.OrganizationID ||
		pod.Annotations[instanceKey] != in.InstanceID ||
		pod.Annotations[projectKey] != in.ProjectID ||
		pod.Annotations[operationKey] != in.CreateOperationID ||
		pod.Annotations[startKey] != startID {
		return false
	}
	if pod.Spec.HostNetwork || pod.Spec.HostPID || pod.Spec.HostIPC ||
		len(pod.Spec.InitContainers) != 0 || len(pod.Spec.EphemeralContainers) != 0 || len(pod.Spec.Containers) != 1 ||
		pod.Spec.RuntimeClassName != nil ||
		(pod.Spec.ServiceAccountName != "default" && pod.Spec.ServiceAccountName != "") ||
		pod.Spec.NodeSelector[corev1.LabelHostname] != p.NodeName {
		return false
	}
	c := pod.Spec.Containers[0]
	if c.SecurityContext == nil ||
		(c.SecurityContext.Privileged != nil && *c.SecurityContext.Privileged) ||
		(c.SecurityContext.Capabilities != nil && len(c.SecurityContext.Capabilities.Add) != 0) {
		return false
	}
	if len(pod.Spec.Volumes) != 4 {
		return false
	}
	hasDataPVC := false
	hasAuthSecret := false
	emptyDirs := 0
	wantSecret := instanceName(in)
	for _, vol := range pod.Spec.Volumes {
		switch vol.Name {
		case "data":
			if vol.PersistentVolumeClaim == nil || vol.PersistentVolumeClaim.ClaimName != b.PVCName || vol.HostPath != nil {
				return false
			}
			hasDataPVC = true
		case "auth":
			if vol.Secret == nil || vol.Secret.SecretName != wantSecret || vol.HostPath != nil {
				return false
			}
			hasAuthSecret = true
		case "runtime", "shm":
			if vol.EmptyDir == nil || vol.HostPath != nil {
				return false
			}
			emptyDirs++
		default:
			return false
		}
	}
	return hasDataPVC && hasAuthSecret && emptyDirs == 2
}

func terminalWorkerServiceOwned(service *corev1.Service, in Identity, p HelperProfile, expectedName, expectedServiceUID string) bool {
	if service == nil || !validIdentity(in) || expectedName == "" || expectedServiceUID == "" {
		return false
	}
	if service.Namespace != p.Namespace || service.Name != expectedName || string(service.UID) != expectedServiceUID {
		return false
	}
	if service.Spec.Type != corev1.ServiceTypeClusterIP || len(service.Spec.Ports) != 3 {
		return false
	}
	if service.Labels[instanceKey] != in.InstanceID ||
		service.Labels["app.kubernetes.io/name"] != "blender-worker" ||
		service.Labels["app.kubernetes.io/part-of"] != "verdantflare-blender" {
		return false
	}
	if service.Annotations[stationKey] != in.StationID ||
		service.Annotations[organizationKey] != in.OrganizationID ||
		service.Annotations[instanceKey] != in.InstanceID ||
		service.Annotations[projectKey] != in.ProjectID ||
		service.Annotations[operationKey] != in.CreateOperationID {
		return false
	}
	return service.Spec.Selector[instanceKey] == in.InstanceID
}

func removeTerminalWorker(ctx context.Context, client kubernetes.Interface, start StartRequest, p HelperProfile, b VolumeBinding, expected WorkerObservation) (bool, error) {
	expectedName := "blender-run-" + strings.ReplaceAll(start.OperationID, "-", "")
	if !start.valid(p) || expected.PodName != expectedName {
		return false, ErrBinding
	}
	pod, err := client.CoreV1().Pods(p.Namespace).Get(ctx, expectedName, metav1.GetOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return false, err
	}
	if apierrors.IsNotFound(err) {
		pod = nil
	} else if !terminalWorkerPodOwned(pod, start.identity(), p, b, start.OperationID, expected.PodName, expected.PodUID) {
		return false, ErrBinding
	}
	service, err := client.CoreV1().Services(p.Namespace).Get(ctx, expectedName, metav1.GetOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return false, err
	}
	if apierrors.IsNotFound(err) {
		service = nil
	} else if !terminalWorkerServiceOwned(service, start.identity(), p, expectedName, expected.ServiceUID) {
		return false, ErrBinding
	}
	allowed := map[types.UID]bool{}
	if pod != nil {
		allowed[pod.UID] = true
	}
	if _, err = Inspect(ctx, client, start.identity(), p.StorageProfile, b.PVCName, &b, allowed); err != nil {
		return false, err
	}
	if err = verifyClaimOwner(ctx, client, start.identity(), b); err != nil {
		return false, err
	}
	if service != nil {
		uid, rv := service.UID, service.ResourceVersion
		if err = client.CoreV1().Services(p.Namespace).Delete(ctx, expectedName, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid, ResourceVersion: &rv}}); err != nil && !apierrors.IsNotFound(err) {
			return false, err
		}
	}
	if pod != nil {
		uid, rv := pod.UID, pod.ResourceVersion
		if err = client.CoreV1().Pods(p.Namespace).Delete(ctx, expectedName, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid, ResourceVersion: &rv}}); err != nil && !apierrors.IsNotFound(err) {
			return false, err
		}
	}
	if pod != nil || service != nil {
		return false, nil
	}
	node, err := client.CoreV1().Nodes().Get(ctx, p.NodeName, metav1.GetOptions{})
	if err != nil {
		return false, err
	}
	for _, condition := range node.Status.Conditions {
		if condition.Type == corev1.NodeReady && condition.Status == corev1.ConditionTrue {
			return true, nil
		}
	}
	return false, ErrComputeUnknown
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
	expectedName := "blender-run-" + strings.ReplaceAll(previous.OperationID, "-", "")
	pod, err := client.CoreV1().Pods(p.Namespace).Get(ctx, expectedName, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	if !terminalWorkerPodOwned(pod, previous.identity(), p, b, previous.OperationID, execution.Worker.PodName, execution.Worker.PodUID) {
		return nil, ErrBinding
	}
	termCode, termReason := terminalPodFailureCode(pod)
	recovery := &workerRecovery{
		Start:          previous,
		Worker:         execution.Worker,
		PriorOperation: previous.OperationID,
		TerminalCode:   termCode,
		TerminalReason: termReason,
	}
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
