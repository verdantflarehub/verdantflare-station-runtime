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

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
)

const helperHashKey = "station.verdantflare.com/preparer-hash"
const componentKey = "app.kubernetes.io/component"

// HelperProfile is trusted configuration. The image is the registered Blender
// worker image, but its desktop/GPU entrypoint is deliberately not executed.
type HelperProfile struct {
	StorageProfile
	Image        string
	MaxFileBytes int64
}

type HelperObservation struct {
	PodUID       string `json:"pod_uid"`
	ServiceUID   string `json:"service_uid"`
	Ready        bool   `json:"ready"`
	FileEndpoint string `json:"file_endpoint"`
}

var imageVersion = regexp.MustCompile(`^[^\s@]+:(?:[a-z0-9][a-z0-9._-]*-)?v?[0-9]+\.[0-9]+\.[0-9]+$`)

func instanceName(in Identity) string { return "blender-" + strings.ReplaceAll(in.InstanceID, "-", "") }

func helperObjects(in Identity, p HelperProfile, b VolumeBinding) (*corev1.Pod, *corev1.Service, error) {
	if !validIdentity(in) || !p.StorageProfile.valid() || !sameReservation(b, in, p.StorageProfile) || b.Retained || !imageVersion.MatchString(p.Image) || p.MaxFileBytes < 1 || p.MaxFileBytes > b.ReservedBytes {
		return nil, nil, ErrInvalid
	}
	name := instanceName(in) + "-prepare"
	labels := map[string]string{instanceKey: in.InstanceID, componentKey: "blender-workspace-preparer", "app.kubernetes.io/name": "blender-worker", "app.kubernetes.io/part-of": "verdantflare-blender"}
	annotations := volumeOwner(in)
	annotations[stationKey] = in.StationID
	annotations["station.verdantflare.com/profile-hash"] = hex.EncodeToString(p.Hash[:])
	no, yes := false, true
	user := int64(1000)
	mode := int32(0444)
	grace := int64(15)
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: p.Namespace, Labels: labels, Annotations: annotations}, Spec: corev1.PodSpec{
		AutomountServiceAccountToken: &no, RestartPolicy: corev1.RestartPolicyNever, TerminationGracePeriodSeconds: &grace,
		NodeSelector:    map[string]string{corev1.LabelHostname: p.NodeName},
		SecurityContext: &corev1.PodSecurityContext{RunAsUser: &user, RunAsGroup: &user, FSGroup: &user, RunAsNonRoot: &yes, SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault}},
		Containers: []corev1.Container{{Name: "files", Image: p.Image, ImagePullPolicy: corev1.PullAlways, Command: []string{"python3", "/opt/beagle/blender-mcp/file-agent/server.py"},
			Env:             []corev1.EnvVar{{Name: "INSTANCE_ID", Value: in.InstanceID}, {Name: "WORKSPACE_ROOT", Value: "/workspace"}, {Name: "BLENDER_FILE_AGENT_HOST", Value: "0.0.0.0"}, {Name: "BLENDER_MCP_TOKEN_FILE", Value: "/secrets/token"}, {Name: "BLENDER_FILE_MAX_BODY_BYTES", Value: fmt.Sprint(p.MaxFileBytes)}, {Name: "PYTHONDONTWRITEBYTECODE", Value: "1"}, {Name: "NVIDIA_VISIBLE_DEVICES", Value: "void"}},
			Ports:           []corev1.ContainerPort{{Name: "files", ContainerPort: 48085}},
			Resources:       corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m"), corev1.ResourceMemory: resource.MustParse("128Mi")}, Limits: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1"), corev1.ResourceMemory: resource.MustParse("512Mi")}},
			SecurityContext: &corev1.SecurityContext{Privileged: &no, AllowPrivilegeEscalation: &no, ReadOnlyRootFilesystem: &yes, Capabilities: &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}}},
			VolumeMounts:    []corev1.VolumeMount{{Name: "data", MountPath: "/workspace", SubPath: "workspace"}, {Name: "auth", MountPath: "/secrets", ReadOnly: true}},
			ReadinessProbe:  &corev1.Probe{ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/readyz", Port: intstr.FromString("files")}}, PeriodSeconds: 3, TimeoutSeconds: 2},
		}},
		Volumes: []corev1.Volume{{Name: "data", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: b.PVCName}}}, {Name: "auth", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: instanceName(in), DefaultMode: &mode, Items: []corev1.KeyToPath{{Key: "worker-token", Path: "token"}}}}}},
	}}
	pod.Spec.Containers[0].Env = append(pod.Spec.Containers[0].Env, corev1.EnvVar{Name: "BLENDER_POD_UID", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{APIVersion: "v1", FieldPath: "metadata.uid"}}})
	raw, _ := json.Marshal(pod.Spec)
	hash := sha256.Sum256(raw)
	annotations[helperHashKey] = hex.EncodeToString(hash[:])
	svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: p.Namespace, Labels: labels, Annotations: annotations}, Spec: corev1.ServiceSpec{Type: corev1.ServiceTypeClusterIP, Selector: labels, Ports: []corev1.ServicePort{{Name: "files", Port: 48085, TargetPort: intstr.FromString("files"), Protocol: corev1.ProtocolTCP}}}}
	return pod, svc, nil
}

func ownedHelper(actual, desired metav1.Object, uid string) bool {
	if string(actual.GetUID()) == "" || (uid != "" && string(actual.GetUID()) != uid) {
		return false
	}
	for k, v := range desired.GetAnnotations() {
		if actual.GetAnnotations()[k] != v {
			return false
		}
	}
	for k, v := range desired.GetLabels() {
		if actual.GetLabels()[k] != v {
			return false
		}
	}
	return true
}

func helperPodMatches(actual, desired *corev1.Pod, uid string) bool {
	return ownedHelper(actual, desired, uid) && !actual.Spec.HostNetwork && !actual.Spec.HostPID && !actual.Spec.HostIPC &&
		len(actual.Spec.InitContainers) == 0 && len(actual.Spec.EphemeralContainers) == 0 && len(actual.Spec.Containers) == 1 && actual.Spec.RuntimeClassName == nil &&
		len(actual.Spec.Volumes) == 2 && len(actual.Spec.Containers[0].EnvFrom) == 0 &&
		equality.Semantic.DeepEqual(actual.Spec.Containers[0].Resources, desired.Spec.Containers[0].Resources) &&
		equality.Semantic.DeepEqual(actual.Spec.Containers[0].Env, desired.Spec.Containers[0].Env) &&
		len(actual.Spec.Containers[0].Args) == 0 && actual.Spec.Containers[0].Lifecycle == nil && actual.Spec.Containers[0].SecurityContext != nil &&
		(actual.Spec.Containers[0].SecurityContext.Capabilities == nil || len(actual.Spec.Containers[0].SecurityContext.Capabilities.Add) == 0) &&
		equality.Semantic.DeepDerivative(desired.Spec, actual.Spec)
}

func helperServiceMatches(actual, desired *corev1.Service, uid string) bool {
	return ownedHelper(actual, desired, uid) && actual.Spec.Type == corev1.ServiceTypeClusterIP && len(actual.Spec.ExternalIPs) == 0 && actual.Spec.ExternalName == "" &&
		equality.Semantic.DeepEqual(actual.Spec.Selector, desired.Spec.Selector) && equality.Semantic.DeepDerivative(desired.Spec.Ports, actual.Spec.Ports)
}

// EnsureHelper is invoked only after Reserve and Claim have succeeded. Missing
// objects with previously observed UIDs are a conflict, never silently recreated.
// Readiness means the file agent is reachable, not that the project is prepared.
func EnsureHelper(ctx context.Context, client kubernetes.Interface, in Identity, p HelperProfile, b VolumeBinding, expected HelperObservation) (HelperObservation, error) {
	pod, wantedService, err := helperObjects(in, p, b)
	if err != nil {
		return HelperObservation{}, err
	}
	current, err := client.CoreV1().Pods(p.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
	allowed := map[types.UID]bool{}
	if err == nil {
		if !helperPodMatches(current, pod, expected.PodUID) || current.DeletionTimestamp != nil {
			return HelperObservation{}, ErrBinding
		}
		allowed[current.UID] = true
	} else if !apierrors.IsNotFound(err) {
		return HelperObservation{}, err
	} else if expected.PodUID != "" {
		return HelperObservation{}, ErrBinding
	} else {
		current = nil
	}
	if _, err = Inspect(ctx, client, in, p.StorageProfile, b.PVCName, &b, allowed); err != nil {
		return HelperObservation{}, err
	}
	// The durable lease and PVC marker must agree before mounting its bytes.
	claim, err := client.CoreV1().PersistentVolumeClaims(p.Namespace).Get(ctx, b.PVCName, metav1.GetOptions{})
	if err != nil {
		return HelperObservation{}, err
	}
	if string(claim.UID) != b.PVCUID {
		return HelperObservation{}, ErrBinding
	}
	for k, v := range volumeOwner(in) {
		if claim.Annotations[k] != v {
			return HelperObservation{}, ErrBinding
		}
	}
	secret, err := client.CoreV1().Secrets(p.Namespace).Get(ctx, instanceName(in), metav1.GetOptions{})
	if err != nil {
		return HelperObservation{}, err
	}
	if secret.DeletionTimestamp != nil || len(secret.Data["worker-token"]) < 32 || secret.Annotations[stationKey] != in.StationID {
		return HelperObservation{}, ErrBinding
	}
	for k, v := range volumeOwner(in) {
		if secret.Annotations[k] != v {
			return HelperObservation{}, ErrBinding
		}
	}
	svc, err := client.CoreV1().Services(p.Namespace).Get(ctx, wantedService.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		if expected.ServiceUID != "" {
			return HelperObservation{}, ErrBinding
		}
		svc, err = client.CoreV1().Services(p.Namespace).Create(ctx, wantedService, metav1.CreateOptions{})
	}
	if err != nil {
		return HelperObservation{}, err
	}
	if svc.DeletionTimestamp != nil || !helperServiceMatches(svc, wantedService, expected.ServiceUID) {
		return HelperObservation{}, ErrBinding
	}
	if current == nil {
		current, err = client.CoreV1().Pods(p.Namespace).Create(ctx, pod, metav1.CreateOptions{})
		if err != nil {
			return HelperObservation{}, err
		}
	}
	out := HelperObservation{PodUID: string(current.UID), ServiceUID: string(svc.UID), FileEndpoint: "http://" + svc.Name + "." + p.Namespace + ".svc.cluster.local:48085"}
	if current.UID == "" || !helperPodMatches(current, pod, expected.PodUID) {
		return HelperObservation{}, ErrBinding
	}
	if current.Status.Phase == corev1.PodFailed || current.Status.Phase == corev1.PodSucceeded {
		return out, errors.New("WORKSPACE_PREPARER_EXITED")
	}
	for _, condition := range current.Status.Conditions {
		if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue && current.Status.Phase == corev1.PodRunning && current.Spec.NodeName == p.NodeName {
			out.Ready = true
		}
	}
	return out, nil
}

// RemoveHelper is UID-fenced and never deletes the data volume or its Secret.
// true proves both endpoints are absent and no remaining Pod mounts this PVC.
func RemoveHelper(ctx context.Context, client kubernetes.Interface, in Identity, p HelperProfile, b VolumeBinding, expected HelperObservation) (bool, error) {
	pod, svc, err := helperObjects(in, p, b)
	if err != nil {
		return false, err
	}
	if expected.PodUID == "" || expected.ServiceUID == "" {
		return false, ErrInvalid
	}
	currentPod, err := client.CoreV1().Pods(p.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return false, err
	}
	if err == nil && !helperPodMatches(currentPod, pod, expected.PodUID) {
		return false, ErrBinding
	}
	if apierrors.IsNotFound(err) {
		currentPod = nil
	}
	currentService, err := client.CoreV1().Services(p.Namespace).Get(ctx, svc.Name, metav1.GetOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return false, err
	}
	if err == nil && !helperServiceMatches(currentService, svc, expected.ServiceUID) {
		return false, ErrBinding
	}
	if apierrors.IsNotFound(err) {
		currentService = nil
	}
	if currentService != nil {
		uid, rv := currentService.UID, currentService.ResourceVersion
		err = client.CoreV1().Services(p.Namespace).Delete(ctx, svc.Name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid, ResourceVersion: &rv}})
		if err != nil && !apierrors.IsNotFound(err) {
			return false, err
		}
	}
	if currentPod != nil {
		uid, rv := currentPod.UID, currentPod.ResourceVersion
		err = client.CoreV1().Pods(p.Namespace).Delete(ctx, pod.Name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid, ResourceVersion: &rv}})
		if err != nil && !apierrors.IsNotFound(err) {
			return false, err
		}
	}
	if currentPod != nil || currentService != nil {
		return false, nil
	}
	if _, err = Inspect(ctx, client, in, p.StorageProfile, b.PVCName, &b, nil); err != nil {
		return false, err
	}
	return true, nil
}
