package appinstances

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/google/uuid"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

const prefix = "station.verdantflare.com/"
const stationKey = prefix + "station-id"
const poolKey = prefix + "workspace-pool"
const specKey = prefix + "appinstance-spec-hash"

type Identity struct {
	StationID      string `json:"station_id"`
	OrganizationID string `json:"organization_id"`
	UserID         string `json:"user_id"`
	InstallationID string `json:"installation_id"`
	InstanceID     string `json:"instance_id"`
}
type Run struct {
	Identity
	OperationID string `json:"operation_id"`
	Generation  int64  `json:"generation"`
}
type Volume struct {
	Identity
	ProfileHash string `json:"profile_hash"`
	PVCName     string `json:"pvc_name"`
	PVCUID      string `json:"pvc_uid"`
	PVName      string `json:"pv_name"`
	PVUID       string `json:"pv_uid"`
}

func validID(v string) bool {
	id, e := uuid.Parse(v)
	return e == nil && id.Version() == 7 && id.String() == v
}
func validUID(v string) bool {
	id, e := uuid.Parse(v)
	return e == nil && id != uuid.Nil && id.String() == v
}
func (in Identity) valid() bool {
	for _, v := range []string{in.StationID, in.OrganizationID, in.UserID, in.InstallationID, in.InstanceID} {
		if !validID(v) {
			return false
		}
	}
	return true
}
func (in Identity) annotations(p Profile) map[string]string {
	return map[string]string{stationKey: in.StationID, prefix + "organization-id": in.OrganizationID, prefix + "owner-id": in.UserID,
		prefix + "installation-id": in.InstallationID, prefix + "instance-id": in.InstanceID, prefix + "app-id": p.AppID,
		prefix + "app-version": p.AppVersion, prefix + "profile-id": p.ID, prefix + "profile-hash": p.Hash()}
}
func instanceName(in Identity) string { return "appi-" + strings.ReplaceAll(in.InstanceID, "-", "") }
func runName(in Run) string           { return "appr-" + strings.ReplaceAll(in.OperationID, "-", "") }
func (in Run) annotations(p Profile) map[string]string {
	a := in.Identity.annotations(p)
	a[prefix+"start-operation-id"] = in.OperationID
	a[prefix+"generation"] = fmt.Sprint(in.Generation)
	return a
}
func validVolume(in Identity, p Profile, b Volume) bool {
	if !in.valid() || !p.Valid() || b.Identity != in || b.ProfileHash != p.Hash() || !validUID(b.PVCUID) || !validUID(b.PVUID) || b.PVName == "" {
		return false
	}
	for _, name := range p.Claims {
		if name == b.PVCName {
			return true
		}
	}
	return false
}

// WorkerObjects accepts only a durable volume binding, never a raw PodSpec.
func WorkerObjects(in Run, p Profile, b Volume) (*corev1.Pod, *corev1.Service, error) {
	if !validVolume(in.Identity, p, b) || !validID(in.OperationID) || in.Generation < 1 {
		return nil, nil, ErrInvalid
	}
	no, yes := false, true
	zero := int32(0)
	grace := int64(30)
	preempt := corev1.PreemptNever
	tmp := resource.MustParse("256Mi")
	labels := map[string]string{prefix + "instance-id": in.InstanceID, prefix + "start-operation-id": in.OperationID}
	c := corev1.Container{Name: "worker", Image: p.Image, ImagePullPolicy: corev1.PullAlways,
		SecurityContext: &corev1.SecurityContext{AllowPrivilegeEscalation: &no, ReadOnlyRootFilesystem: &yes, Privileged: &no,
			Capabilities: &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}}},
		Ports:        []corev1.ContainerPort{{Name: "control", ContainerPort: p.Port, Protocol: corev1.ProtocolTCP}},
		VolumeMounts: []corev1.VolumeMount{{Name: "workspace", MountPath: "/workspace"}, {Name: "tmp", MountPath: "/tmp"}},
		Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{
			corev1.ResourceCPU: *resource.NewMilliQuantity(p.CPURequestMillis, resource.DecimalSI), corev1.ResourceMemory: *resource.NewQuantity(p.MemoryRequestBytes, resource.BinarySI)},
			Limits: corev1.ResourceList{corev1.ResourceCPU: *resource.NewMilliQuantity(p.CPULimitMillis, resource.DecimalSI), corev1.ResourceMemory: *resource.NewQuantity(p.MemoryLimitBytes, resource.BinarySI)}}}
	c.Env = append(c.Env, corev1.EnvVar{Name: "NVIDIA_VISIBLE_DEVICES", Value: "void"})
	keys := make([]string, 0, len(p.Env))
	for k := range p.Env {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		c.Env = append(c.Env, corev1.EnvVar{Name: k, Value: p.Env[k]})
	}
	keys = keys[:0]
	for k := range p.Bindings {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, key := range keys {
		env := corev1.EnvVar{Name: key}
		switch p.Bindings[key] {
		case "instance_id":
			env.Value = in.InstanceID
		case "start_operation_id":
			env.Value = in.OperationID
		case "generation":
			env.Value = fmt.Sprint(in.Generation)
		case "container":
			env.Value = "worker"
		case "worker_token":
			env.ValueFrom = &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: instanceName(in.Identity)}, Key: "worker-token"}}
		default:
			field := map[string]string{"pod_uid": "metadata.uid", "pod_name": "metadata.name", "namespace": "metadata.namespace"}[p.Bindings[key]]
			env.ValueFrom = &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{APIVersion: "v1", FieldPath: field}}
		}
		c.Env = append(c.Env, env)
	}
	probe := func(period, failures int32) *corev1.Probe {
		return &corev1.Probe{ProbeHandler: corev1.ProbeHandler{TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromString("control")}}, PeriodSeconds: period, TimeoutSeconds: 3, FailureThreshold: failures}
	}
	c.StartupProbe = probe(5, 60)
	c.ReadinessProbe = probe(5, 3)
	c.LivenessProbe = probe(15, 3)
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: runName(in), Namespace: p.Namespace, Labels: labels, Annotations: in.annotations(p)}, Spec: corev1.PodSpec{
		Containers: []corev1.Container{c}, AutomountServiceAccountToken: &no, EnableServiceLinks: &no, RestartPolicy: corev1.RestartPolicyNever,
		TerminationGracePeriodSeconds: &grace, Priority: &zero, PreemptionPolicy: &preempt, SchedulerName: "default-scheduler",
		NodeSelector: map[string]string{corev1.LabelHostname: p.NodeName},
		SecurityContext: &corev1.PodSecurityContext{RunAsNonRoot: &yes, RunAsUser: &p.RunAsUser, RunAsGroup: &p.RunAsUser, FSGroup: &p.RunAsUser,
			SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault}},
		Volumes: []corev1.Volume{{Name: "workspace", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: b.PVCName}}},
			{Name: "tmp", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{SizeLimit: &tmp}}}},
	}}
	svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: pod.Name, Namespace: p.Namespace, Labels: labels, Annotations: in.annotations(p)}, Spec: corev1.ServiceSpec{
		Type: corev1.ServiceTypeClusterIP, Selector: labels, Ports: []corev1.ServicePort{{Name: "control", Port: p.Port, TargetPort: intstr.FromString("control"), Protocol: corev1.ProtocolTCP}}}}
	raw, _ := json.Marshal(struct {
		Pod     corev1.PodSpec
		Service corev1.ServiceSpec
	}{pod.Spec, svc.Spec})
	sum := sha256.Sum256(raw)
	pod.Annotations[specKey] = hex.EncodeToString(sum[:])
	svc.Annotations[specKey] = pod.Annotations[specKey]
	return pod, svc, nil
}
