package nodegpu

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/verdantflarehub/verdantflare-station-runtime/internal/scheduler"
	"github.com/verdantflarehub/verdantflare-station-runtime/internal/strictjson"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

type podContainerRecord struct {
	PodUID    string            `json:"pod_uid"`
	SingleApp bool              `json:"single_app"`
	ByName    map[string]string `json:"by_name"`
}

type PodContainerCache struct {
	mu    sync.Mutex
	dir   string
	byPod map[string]podContainerRecord
}

func NewPodContainerCache(dir string) *PodContainerCache {
	return &PodContainerCache{
		dir:   strings.TrimSpace(dir),
		byPod: make(map[string]podContainerRecord),
	}
}

func extractPodContainerRecord(pod *corev1.Pod) (podContainerRecord, bool) {
	if pod == nil {
		return podContainerRecord{}, false
	}
	pUID := normalizePodUID(string(pod.UID))
	if pUID == "" || strings.Contains(pUID, "/") {
		return podContainerRecord{}, false
	}
	rec := podContainerRecord{
		PodUID:    pUID,
		SingleApp: len(pod.Spec.Containers) == 1,
		ByName:    make(map[string]string),
	}
	for _, cs := range pod.Status.ContainerStatuses {
		name := strings.ToLower(strings.TrimSpace(cs.Name))
		if name == "" {
			continue
		}
		candidates := []string{cs.ContainerID}
		if cs.State.Terminated != nil {
			candidates = append(candidates, cs.State.Terminated.ContainerID)
		}
		if cs.LastTerminationState.Terminated != nil {
			candidates = append(candidates, cs.LastTerminationState.Terminated.ContainerID)
		}
		for _, raw := range candidates {
			cid := strings.ToLower(NormalizeContainerID(raw))
			if isHex64ContainerID(cid) {
				rec.ByName[name] = cid
				break
			}
		}
	}
	if len(rec.ByName) == 0 {
		return podContainerRecord{}, false
	}
	return rec, true
}

func (c *PodContainerCache) RecordPod(pod *corev1.Pod) {
	if c == nil {
		return
	}
	rec, ok := extractPodContainerRecord(pod)
	if !ok {
		return
	}
	c.mu.Lock()
	if c.byPod == nil {
		c.byPod = make(map[string]podContainerRecord)
	}
	prev, exists := c.byPod[rec.PodUID]
	changed := !exists || prev.SingleApp != rec.SingleApp || len(prev.ByName) != len(rec.ByName)
	if exists {
		for k, v := range prev.ByName {
			if _, has := rec.ByName[k]; !has {
				rec.ByName[k] = v
				changed = true
			} else if rec.ByName[k] != v {
				changed = true
			}
		}
	}
	c.byPod[rec.PodUID] = rec
	dir := c.dir
	c.mu.Unlock()

	if changed && dir != "" {
		if err := ensurePrivateDir(dir); err == nil {
			if raw, mErr := json.Marshal(rec); mErr == nil {
				_ = writePrivateAtomic(filepath.Join(dir, fmt.Sprintf("pod_cid_%s.json", rec.PodUID)), raw)
			}
		}
	}
}

func matchRecordContainerHint(rec podContainerRecord, containerHint string) string {
	hint := strings.ToLower(strings.TrimSpace(containerHint))
	if hint != "" {
		if cid, ok := rec.ByName[hint]; ok && isHex64ContainerID(cid) {
			return cid
		}
	}
	if rec.SingleApp && len(rec.ByName) == 1 {
		for _, cid := range rec.ByName {
			if isHex64ContainerID(cid) {
				return cid
			}
		}
	}
	return ""
}

func (c *PodContainerCache) Lookup(podUID, containerHint string) string {
	if c == nil {
		return ""
	}
	cleanPod := normalizePodUID(podUID)
	if cleanPod == "" || strings.Contains(cleanPod, "/") {
		return ""
	}
	c.mu.Lock()
	rec, ok := c.byPod[cleanPod]
	dir := c.dir
	c.mu.Unlock()
	if ok {
		if cid := matchRecordContainerHint(rec, containerHint); cid != "" {
			return cid
		}
	}
	if dir != "" {
		raw, err := os.ReadFile(filepath.Join(dir, fmt.Sprintf("pod_cid_%s.json", cleanPod)))
		if err == nil {
			var diskRec podContainerRecord
			if strictjson.Decode(bytes.NewReader(raw), 64<<10, &diskRec) == nil && normalizePodUID(diskRec.PodUID) == cleanPod {
				c.mu.Lock()
				if c.byPod == nil {
					c.byPod = make(map[string]podContainerRecord)
				}
				c.byPod[cleanPod] = diskRec
				c.mu.Unlock()
				if cid := matchRecordContainerHint(diskRec, containerHint); cid != "" {
					return cid
				}
			}
		}
	}
	return ""
}

func (c *PodContainerCache) HasContainer(podUID, containerID string) bool {
	if c == nil {
		return false
	}
	cleanPod := normalizePodUID(podUID)
	cleanCID := strings.ToLower(NormalizeContainerID(containerID))
	if cleanPod == "" || strings.Contains(cleanPod, "/") || !isHex64ContainerID(cleanCID) {
		return false
	}
	_ = c.Lookup(cleanPod, "")
	c.mu.Lock()
	rec, ok := c.byPod[cleanPod]
	c.mu.Unlock()
	if !ok {
		return false
	}
	for _, cid := range rec.ByName {
		if strings.ToLower(NormalizeContainerID(cid)) == cleanCID {
			return true
		}
	}
	return false
}

func (k KubeNodeSlotCounter) CheckPodDeletedAndObserved(ctx context.Context, podUID, containerID string) (bool, error) {
	cleanPod := normalizePodUID(podUID)
	cleanCID := strings.ToLower(NormalizeContainerID(containerID))
	if cleanPod == "" || !isHex64ContainerID(cleanCID) || k.Cache == nil || k.Client == nil {
		return false, nil
	}
	if !k.Cache.HasContainer(cleanPod, cleanCID) {
		return false, nil
	}
	ns := strings.TrimSpace(k.Namespace)
	if ns == "" {
		ns = "verdantflare-app"
	}
	pods, err := k.Client.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{})
	if err != nil {
		return false, err
	}
	for i := range pods.Items {
		pod := &pods.Items[i]
		k.Cache.RecordPod(pod)
		if normalizePodUID(string(pod.UID)) == cleanPod {
			if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
				return true, nil
			}
			return false, nil
		}
	}
	return true, nil
}

type KubeNodeSlotCounter struct {
	Client    kubernetes.Interface
	Namespace string
	Cache     *PodContainerCache
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

	for i := range pods.Items {
		pod := pods.Items[i]
		if k.Cache != nil {
			k.Cache.RecordPod(&pod)
		}
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

func (k KubeNodeSlotCounter) ResolvePodContainerID(ctx context.Context, podUID, containerHint string) (string, error) {
	cleanPod := normalizePodUID(podUID)
	if cleanPod == "" {
		return "", errors.New("empty pod_uid")
	}
	if k.Cache != nil {
		if cid := k.Cache.Lookup(cleanPod, containerHint); cid != "" {
			return cid, nil
		}
	}
	if k.Client == nil {
		return "", nil
	}
	ns := strings.TrimSpace(k.Namespace)
	if ns == "" {
		ns = "verdantflare-app"
	}
	pods, err := k.Client.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{})
	if err != nil {
		return "", err
	}
	var matched string
	for i := range pods.Items {
		pod := &pods.Items[i]
		if k.Cache != nil {
			k.Cache.RecordPod(pod)
		}
		if normalizePodUID(string(pod.UID)) != cleanPod {
			continue
		}
		if rec, ok := extractPodContainerRecord(pod); ok {
			matched = matchRecordContainerHint(rec, containerHint)
		}
	}
	return matched, nil
}
