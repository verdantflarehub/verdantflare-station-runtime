package instances

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/google/uuid"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
)

const startKey = "station.verdantflare.com/start-operation-id"
const workerHashKey = "station.verdantflare.com/worker-hash"

var ErrWorkerExited = errors.New("WORKER_EXITED")

type WorkerSource struct {
	SourceRevisionID string `json:"source_revision_id"`
	SHA256           string `json:"sha256"`
	Size             int64  `json:"size"`
	Empty            bool   `json:"empty"`
}

func (s WorkerSource) valid(max int64) bool {
	id, err := uuid.Parse(s.SourceRevisionID)
	if err != nil || id == uuid.Nil || id.String() != s.SourceRevisionID {
		return false
	}
	if s.Empty {
		return s.SHA256 == "" && s.Size == 0
	}
	return s.Size > 0 && s.Size <= max && regexp.MustCompile(`^[0-9a-f]{64}$`).MatchString(s.SHA256)
}

type WorkerObservation struct {
	PodUID          string `json:"pod_uid"`
	ServiceUID      string `json:"service_uid"`
	PodName         string `json:"pod_name"`
	Namespace       string `json:"namespace"`
	PodReady        bool   `json:"pod_ready"`
	ControlEndpoint string `json:"control_endpoint"`
	FileEndpoint    string `json:"file_endpoint"`
	GUIEndpoint     string `json:"gui_endpoint"`
}

func workerObjects(in Identity, p HelperProfile, b VolumeBinding, startID string, source WorkerSource) (*corev1.Pod, *corev1.Service, error) {
	id, err := uuid.Parse(startID)
	if err != nil || id == uuid.Nil || id.String() != startID || startID == in.CreateOperationID || !source.valid(p.MaxFileBytes) {
		return nil, nil, ErrInvalid
	}
	pod, svc, err := helperObjects(in, p, b)
	if err != nil {
		return nil, nil, err
	}
	name := "blender-run-" + strings.ReplaceAll(startID, "-", "")
	pod.Name, svc.Name = name, name
	pod.Labels[componentKey] = "blender-worker"
	pod.Labels[startKey] = startID
	pod.Annotations[startKey] = startID
	delete(pod.Annotations, helperHashKey)
	pod.Annotations["station.verdantflare.com/worker-template"] = workerTemplateVersion
	no, yes := false, true
	grace := int64(45)
	priority := int32(0)
	preempt := corev1.PreemptNever
	pod.Spec.TerminationGracePeriodSeconds = &grace
	pod.Spec.Priority = &priority
	pod.Spec.PriorityClassName = "blender-nonpreempting"
	pod.Spec.PreemptionPolicy = &preempt
	pod.Spec.SchedulerName = "default-scheduler"
	runtimeSize, shmSize := resource.MustParse("256Mi"), resource.MustParse("2Gi")
	pod.Spec.Volumes = append(pod.Spec.Volumes,
		corev1.Volume{Name: "runtime", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{Medium: corev1.StorageMediumMemory, SizeLimit: &runtimeSize}}},
		corev1.Volume{Name: "shm", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{Medium: corev1.StorageMediumMemory, SizeLimit: &shmSize}}})
	c := &pod.Spec.Containers[0]
	c.Name, c.Command = "blender", nil
	c.Resources = workerResources()
	// The approved KDE image uses bounded sudo during DBus/runtime setup.
	c.SecurityContext = &corev1.SecurityContext{Privileged: &no, AllowPrivilegeEscalation: &yes}
	c.Env = []corev1.EnvVar{
		{Name: "INSTANCE_ID", Value: in.InstanceID}, {Name: "WORKSPACE_ROOT", Value: "/workspace"},
		{Name: "BLENDER_PROJECT_ID", Value: in.ProjectID}, {Name: "BLENDER_CREATE_OPERATION_ID", Value: in.CreateOperationID},
		{Name: "BLENDER_START_OPERATION_ID", Value: startID}, {Name: "BLENDER_CONTAINER_NAME", Value: "blender"},
		{Name: "BLENDER_SOURCE_REVISION_ID", Value: source.SourceRevisionID}, {Name: "BLENDER_START_SHA256", Value: source.SHA256},
		{Name: "BLENDER_START_SIZE", Value: fmt.Sprint(source.Size)}, {Name: "BLENDER_START_EMPTY", Value: fmt.Sprint(source.Empty)},
		{Name: "BLENDER_FILE_AGENT_HOST", Value: "0.0.0.0"}, {Name: "BLENDER_MCP_SECRET_FILE", Value: "/secrets/token"},
		{Name: "BLENDER_FILE_MAX_BODY_BYTES", Value: fmt.Sprint(p.MaxFileBytes)},
		{Name: "BLENDER_MCP_TOKEN_FILE", Value: "/run/user/1000/blender-mcp-token"},
		{Name: "BDWIND_PASSWORD", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: instanceName(in)}, Key: "gui-password"}}},
		{Name: "NVIDIA_DRIVER_CAPABILITIES", Value: "compute,utility,graphics,video,display"},
		{Name: "DISPLAY_SIZEW", Value: "1600"}, {Name: "DISPLAY_SIZEH", Value: "900"}, {Name: "DISPLAY_REFRESH", Value: "30"},
	}
	for _, field := range [][2]string{{"BLENDER_POD_UID", "metadata.uid"}, {"BLENDER_POD_NAME", "metadata.name"}, {"BLENDER_POD_NAMESPACE", "metadata.namespace"}} {
		c.Env = append(c.Env, corev1.EnvVar{Name: field[0], ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{APIVersion: "v1", FieldPath: field[1]}}})
	}
	c.VolumeMounts = append(c.VolumeMounts, corev1.VolumeMount{Name: "data", MountPath: "/home/beagle", SubPath: "home"}, corev1.VolumeMount{Name: "runtime", MountPath: "/run/user/1000"}, corev1.VolumeMount{Name: "shm", MountPath: "/dev/shm"})
	c.Ports = []corev1.ContainerPort{{Name: "control", ContainerPort: 48084}, {Name: "gui", ContainerPort: 48083}, {Name: "files", ContainerPort: 48085}}
	probe := func(path string, period, failures int32) *corev1.Probe {
		return &corev1.Probe{ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: path, Port: intstr.FromString("control")}}, PeriodSeconds: period, TimeoutSeconds: 3, FailureThreshold: failures}
	}
	c.StartupProbe, c.ReadinessProbe, c.LivenessProbe = probe("/readyz", 5, 60), probe("/readyz", 10, 3), probe("/healthz", 20, 3)
	svc.Spec.Ports = []corev1.ServicePort{{Name: "control", Port: 48084, TargetPort: intstr.FromString("control"), Protocol: corev1.ProtocolTCP}, {Name: "gui", Port: 48083, TargetPort: intstr.FromString("gui"), Protocol: corev1.ProtocolTCP}, {Name: "files", Port: 48085, TargetPort: intstr.FromString("files"), Protocol: corev1.ProtocolTCP}}
	raw, _ := json.Marshal(pod.Spec)
	hash := sha256.Sum256(raw)
	pod.Annotations[workerHashKey] = hex.EncodeToString(hash[:])
	return pod, svc, nil
}

func workerPodMatches(actual, desired *corev1.Pod, uid string) bool {
	actual = withProbeDefaults(actual)
	desired = withProbeDefaults(desired)
	if !ownedHelper(actual, desired, uid) || actual.Spec.HostNetwork || actual.Spec.HostPID || actual.Spec.HostIPC ||
		len(actual.Spec.InitContainers) != 0 || len(actual.Spec.EphemeralContainers) != 0 || len(actual.Spec.Containers) != 1 ||
		len(actual.Spec.Volumes) != len(desired.Spec.Volumes) || actual.Spec.RuntimeClassName != nil || actual.Spec.Affinity != nil || actual.Spec.Resources != nil ||
		len(actual.Spec.ResourceClaims) != 0 || len(actual.Spec.SchedulingGates) != 0 || len(actual.Spec.TopologySpreadConstraints) != 0 ||
		actual.Spec.PriorityClassName != desired.Spec.PriorityClassName || actual.Spec.ServiceAccountName != "default" && actual.Spec.ServiceAccountName != "" {
		return false
	}
	for _, tol := range actual.Spec.Tolerations {
		// Kubernetes injects these bounded NoExecute defaults. No extra taint
		// bypass may be added to the approved non-preempting worker template.
		if (tol.Key != "node.kubernetes.io/not-ready" && tol.Key != "node.kubernetes.io/unreachable") || tol.Operator != corev1.TolerationOpExists || tol.Effect != corev1.TaintEffectNoExecute || tol.Value != "" || tol.TolerationSeconds == nil || *tol.TolerationSeconds != 300 {
			return false
		}
	}
	c, d := actual.Spec.Containers[0], desired.Spec.Containers[0]
	return len(c.Command) == 0 && len(c.Args) == 0 && len(c.EnvFrom) == 0 && c.Lifecycle == nil &&
		len(c.VolumeDevices) == 0 && c.SecurityContext != nil &&
		(c.SecurityContext.Capabilities == nil || len(c.SecurityContext.Capabilities.Add) == 0) &&
		equality.Semantic.DeepEqual(c.Resources, d.Resources) && equality.Semantic.DeepEqual(c.Env, d.Env) &&
		equality.Semantic.DeepEqual(actual.Spec.NodeSelector, desired.Spec.NodeSelector) &&
		equality.Semantic.DeepDerivative(desired.Spec, actual.Spec)
}

// EnsureWorker is called only after durable start acceptance, with the create
// operation's pinned Secret UID and a node-scoped execution lock. PodReady is
// transport readiness only; the caller must separately authenticate load proof.
func EnsureWorker(ctx context.Context, client kubernetes.Interface, in Identity, p HelperProfile, b VolumeBinding, startID, secretUID string, source WorkerSource, expected WorkerObservation) (WorkerObservation, error) {
	if secretUID == "" {
		return expected, ErrInvalid
	}
	pod, desiredService, err := workerObjects(in, p, b, startID, source)
	if err != nil {
		return expected, err
	}
	current, err := client.CoreV1().Pods(p.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
	allowed := map[types.UID]bool{}
	if err == nil {
		if !workerPodMatches(current, pod, expected.PodUID) || current.DeletionTimestamp != nil || (current.Spec.NodeName != "" && current.Spec.NodeName != p.NodeName) {
			return expected, ErrBinding
		}
		allowed[current.UID] = true
	} else if !apierrors.IsNotFound(err) {
		return expected, err
	} else if expected.PodUID != "" {
		return expected, ErrBinding
	} else {
		current = nil
	}
	if _, err = Inspect(ctx, client, in, p.StorageProfile, b.PVCName, &b, allowed); err != nil {
		return expected, err
	}
	claim, err := client.CoreV1().PersistentVolumeClaims(p.Namespace).Get(ctx, b.PVCName, metav1.GetOptions{})
	if err != nil {
		return expected, err
	}
	for k, v := range volumeOwner(in) {
		if claim.Annotations[k] != v {
			return expected, ErrBinding
		}
	}
	if _, err = credential(ctx, client, in, p.Namespace, secretUID, false); err != nil {
		return expected, err
	}
	svc, err := client.CoreV1().Services(p.Namespace).Get(ctx, desiredService.Name, metav1.GetOptions{})
	if err == nil {
		if svc.DeletionTimestamp != nil || !helperServiceMatches(svc, desiredService, expected.ServiceUID) || len(svc.Spec.Ports) != 3 {
			return expected, ErrBinding
		}
	} else if !apierrors.IsNotFound(err) {
		return expected, err
	} else if expected.ServiceUID != "" {
		return expected, ErrBinding
	} else {
		svc = nil
	}
	if current == nil {
		if err = AdmitWorker(ctx, client, p.NodeName); err != nil {
			return expected, err
		}
	} else {
		// Recheck exclusivity before accepting an already-created worker's load
		// proof; a device-plugin sharing change must not become a valid binding.
		node, e := client.CoreV1().Nodes().Get(ctx, p.NodeName, metav1.GetOptions{})
		if e != nil {
			return expected, e
		}
		if !exclusiveGPU(node) {
			return expected, ErrComputeUnknown
		}
	}
	if svc == nil {
		svc, err = client.CoreV1().Services(p.Namespace).Create(ctx, desiredService, metav1.CreateOptions{})
		if err != nil {
			return expected, err
		}
	}
	if !helperServiceMatches(svc, desiredService, expected.ServiceUID) || len(svc.Spec.Ports) != 3 {
		return expected, ErrBinding
	}
	if current == nil {
		current, err = client.CoreV1().Pods(p.Namespace).Create(ctx, pod, metav1.CreateOptions{})
		if err != nil {
			return expected, err
		}
	}
	if !workerPodMatches(current, pod, expected.PodUID) {
		return expected, ErrBinding
	}
	base := "http://" + svc.Name + "." + p.Namespace + ".svc.cluster.local:"
	out := WorkerObservation{PodUID: string(current.UID), ServiceUID: string(svc.UID), PodName: current.Name, Namespace: p.Namespace, ControlEndpoint: base + "48084", FileEndpoint: base + "48085", GUIEndpoint: base + "48083"}
	if current.Status.Phase == corev1.PodFailed || current.Status.Phase == corev1.PodSucceeded {
		return out, ErrWorkerExited
	}
	for _, c := range current.Status.Conditions {
		if c.Type == corev1.PodReady && c.Status == corev1.ConditionTrue && current.Status.Phase == corev1.PodRunning && current.Spec.NodeName == p.NodeName {
			out.PodReady = true
		}
	}
	return out, nil
}
