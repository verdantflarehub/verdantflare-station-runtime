package instances

import (
	"context"
	"errors"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
)

const workerTemplateVersion = "blender-desktop-v1"
const gpuResource corev1.ResourceName = "nvidia.com/gpu"

var ErrComputeCapacity = errors.New("COMPUTE_CAPACITY_UNAVAILABLE")
var ErrComputeUnknown = errors.New("COMPUTE_CAPACITY_UNKNOWN")

// These are the approved initial desktop profile, not client supplied requests.
// A template change must change workerTemplateVersion and the registry hash.
func workerResources() corev1.ResourceRequirements {
	return corev1.ResourceRequirements{
		Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2"), corev1.ResourceMemory: resource.MustParse("4Gi"), gpuResource: resource.MustParse("1")},
		Limits:   corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("8"), corev1.ResourceMemory: resource.MustParse("16Gi"), gpuResource: resource.MustParse("1")},
	}
}

func sumResources(dst, src corev1.ResourceList) {
	for key, value := range src {
		q := dst[key]
		q.Add(value)
		dst[key] = q
	}
}

func maxResources(dst, src corev1.ResourceList) {
	for key, value := range src {
		q := dst[key]
		if value.Cmp(q) > 0 {
			dst[key] = value.DeepCopy()
		}
	}
}

// Account for sequential init containers, restartable init sidecars, overhead,
// pod-level requests, and resources retained during an in-place resize.
func podRequests(p *corev1.Pod) corev1.ResourceList {
	total, sidecars, peak := corev1.ResourceList{}, corev1.ResourceList{}, corev1.ResourceList{}
	requests := func(c corev1.Container, statuses []corev1.ContainerStatus) corev1.ResourceList {
		r := c.Resources.Requests.DeepCopy()
		if r == nil {
			r = corev1.ResourceList{}
		}
		for _, status := range statuses {
			if status.Name == c.Name {
				maxResources(r, status.AllocatedResources)
				if status.Resources != nil {
					maxResources(r, status.Resources.Requests)
				}
			}
		}
		return r
	}
	for _, c := range p.Spec.Containers {
		sumResources(total, requests(c, p.Status.ContainerStatuses))
	}
	for _, c := range p.Spec.InitContainers {
		r := requests(c, p.Status.InitContainerStatuses)
		if c.RestartPolicy != nil && *c.RestartPolicy == corev1.ContainerRestartPolicyAlways {
			sumResources(sidecars, r)
			sumResources(total, r)
			maxResources(peak, sidecars)
		} else {
			sumResources(r, sidecars)
			maxResources(peak, r)
		}
	}
	maxResources(total, peak)
	if p.Spec.Resources != nil {
		maxResources(total, p.Spec.Resources.Requests)
	}
	sumResources(total, p.Spec.Overhead)
	return total
}

// AdmitWorker is a conservative read-only preflight. The scheduler is the final
// allocator; the worker always uses PreemptNever and never sets spec.nodeName.
// Call under the node's execution advisory lock immediately before Pod creation.
func AdmitWorker(ctx context.Context, client kubernetes.Interface, nodeName string) error {
	node, err := client.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if node.UID == "" || node.DeletionTimestamp != nil || node.Spec.Unschedulable || node.Labels[corev1.LabelHostname] != nodeName {
		return ErrComputeCapacity
	}
	ready := false
	for _, c := range node.Status.Conditions {
		if c.Type == corev1.NodeReady {
			ready = c.Status == corev1.ConditionTrue
		}
		if (c.Type == corev1.NodeMemoryPressure || c.Type == corev1.NodeDiskPressure || c.Type == corev1.NodePIDPressure) && c.Status != corev1.ConditionFalse {
			return ErrComputeCapacity
		}
	}
	if !ready {
		return ErrComputeCapacity
	}
	for _, taint := range node.Spec.Taints {
		if taint.Effect == corev1.TaintEffectNoSchedule || taint.Effect == corev1.TaintEffectNoExecute {
			return ErrComputeCapacity
		}
	}
	// Fail closed on absent device-plugin exclusivity evidence; a GPU request
	// count alone does not prove a physical device is exclusively allocated.
	if !exclusiveGPU(node) {
		return ErrComputeUnknown
	}
	pods, err := client.CoreV1().Pods("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}
	used := corev1.ResourceList{}
	count := int64(1)
	for i := range pods.Items {
		p := &pods.Items[i]
		if p.Status.Phase == corev1.PodSucceeded || p.Status.Phase == corev1.PodFailed {
			continue
		}
		if p.Spec.NodeName != "" && p.Spec.NodeName != nodeName {
			continue
		}
		// Unscheduled competing pods count too. Affinity is intentionally not
		// used to discount demand: uncertain placement must not inflate capacity.
		if p.Spec.NodeName == "" && !labels.SelectorFromSet(p.Spec.NodeSelector).Matches(labels.Set(node.Labels)) {
			continue
		}
		sumResources(used, podRequests(p))
		count++
	}
	wanted := workerResources().Requests
	wanted[corev1.ResourcePods] = *resource.NewQuantity(count, resource.DecimalSI)
	for name, amount := range wanted {
		available, ok := node.Status.Allocatable[name]
		if !ok || available.Sign() < 0 {
			return ErrComputeUnknown
		}
		if name != corev1.ResourcePods {
			amount.Add(used[name])
		}
		if available.Cmp(amount) < 0 {
			return ErrComputeCapacity
		}
	}
	return nil
}

func exclusiveGPU(node *corev1.Node) bool {
	return node.Labels["nvidia.com/gpu.sharing-strategy"] == "none" &&
		(node.Labels["nvidia.com/gpu.replicas"] == "" || node.Labels["nvidia.com/gpu.replicas"] == "1") &&
		!strings.HasSuffix(node.Labels["nvidia.com/gpu.product"], "-SHARED")
}
