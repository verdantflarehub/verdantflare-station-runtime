package nodegpu

import (
	"context"
	"errors"
	"sort"
	"strings"

	"github.com/verdantflarehub/verdantflare-station-runtime/internal/scheduler"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

type KubeNodeSlotCounter struct {
	Client    kubernetes.Interface
	Namespace string
}

func NewKubeClient(kubeconfig string) (kubernetes.Interface, error) {
	var cfg *rest.Config
	var err error
	if strings.TrimSpace(kubeconfig) != "" {
		cfg, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
	} else {
		cfg, err = rest.InClusterConfig()
	}
	if err != nil {
		return nil, err
	}
	return kubernetes.NewForConfig(cfg)
}

// ResolveNodeUID resolves the authoritative Kubernetes Node.metadata.uid for nodeName.
// Returns an error if nodeName is empty, the Node cannot be fetched, or its UID is empty.
func ResolveNodeUID(ctx context.Context, client kubernetes.Interface, nodeName string) (string, error) {
	cleanName := strings.TrimSpace(nodeName)
	if cleanName == "" {
		return "", errors.New("empty node name")
	}
	if client == nil {
		return "", errors.New("nil kubernetes client")
	}
	nodeObj, err := client.CoreV1().Nodes().Get(ctx, cleanName, metav1.GetOptions{})
	if err != nil {
		return "", err
	}
	uid := strings.TrimSpace(string(nodeObj.UID))
	if uid == "" {
		return "", errors.New("kubernetes node returned empty metadata.uid")
	}
	return uid, nil
}

type NodeGPUSlotObservation struct {
	AllocatedOnNode    int
	ScalingUpOnNode    int
	UnscheduledPending int
	ActivePods         []scheduler.ActiveGPUPodInfo
}

func (k KubeNodeSlotCounter) CountNodeGPUSlots(ctx context.Context, nodeName string) (allocated int, scalingUp int, err error) {
	obs, err := k.ObserveNamespaceGPUPods(ctx, nodeName)
	if err != nil {
		return 0, 0, err
	}
	return obs.AllocatedOnNode, obs.ScalingUpOnNode, nil
}

// ObserveNamespaceGPUPods inspects ALL non-terminal GPU-requesting Pods in Namespace
// (both scheduled onto nodeName AND unscheduled Pending Pods with spec.nodeName == "")
// so unscheduled scale-up bursts cannot bypass the 12-slot ResourceQuota gate and
// admitted leases whose Pods have materialized in Kubernetes are never double-charged.
func (k KubeNodeSlotCounter) ObserveNamespaceGPUPods(ctx context.Context, nodeName string) (NodeGPUSlotObservation, error) {
	if k.Client == nil {
		return NodeGPUSlotObservation{}, nil
	}
	ns := strings.TrimSpace(k.Namespace)
	if ns == "" {
		ns = "verdantflare-app"
	}
	pods, err := k.Client.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{})
	if err != nil {
		return NodeGPUSlotObservation{}, err
	}

	const gpuRes = corev1.ResourceName("nvidia.com/gpu")
	cleanNode := strings.TrimSpace(nodeName)
	var out NodeGPUSlotObservation

	for _, pod := range pods.Items {
		if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
			continue
		}
		var podGPUs int
		for _, c := range pod.Spec.Containers {
			if q, ok := c.Resources.Requests[gpuRes]; ok {
				podGPUs += int(q.Value())
			} else if q, ok := c.Resources.Limits[gpuRes]; ok {
				podGPUs += int(q.Value())
			}
		}
		if podGPUs <= 0 {
			continue
		}

		podNode := strings.TrimSpace(pod.Spec.NodeName)
		if podNode == "" {
			if pod.Status.Phase == corev1.PodPending {
				out.UnscheduledPending += podGPUs
			} else {
				continue
			}
		} else if cleanNode == "" || podNode == cleanNode {
			if pod.Status.Phase == corev1.PodPending {
				out.ScalingUpOnNode += podGPUs
			} else {
				out.AllocatedOnNode += podGPUs
			}
		} else {
			// Scheduled onto a different node.
			continue
		}

		leaseID := strings.TrimSpace(pod.Labels["verdantflare.io/gpu-lease-id"])
		if leaseID == "" && pod.Annotations != nil {
			leaseID = strings.TrimSpace(pod.Annotations["verdantflare.io/gpu-lease-id"])
		}
		instanceID := strings.TrimSpace(pod.Labels["verdantflare.io/instance-id"])
		if instanceID == "" {
			instanceID = strings.TrimSpace(pod.Labels["app.kubernetes.io/instance"])
		}
		appID := strings.TrimSpace(pod.Labels["verdantflare.io/app-id"])
		if appID == "" {
			appID = strings.TrimSpace(pod.Labels["app.kubernetes.io/name"])
		}

		out.ActivePods = append(out.ActivePods, scheduler.ActiveGPUPodInfo{
			PodUID:     normalizePodUID(string(pod.UID)),
			PodName:    pod.Name,
			NodeName:   podNode,
			Phase:      string(pod.Status.Phase),
			GPUSlots:   podGPUs,
			LeaseID:    leaseID,
			InstanceID: instanceID,
			AppID:      appID,
		})
	}

	sort.Slice(out.ActivePods, func(i, j int) bool {
		return out.ActivePods[i].PodUID < out.ActivePods[j].PodUID
	})
	return out, nil
}
