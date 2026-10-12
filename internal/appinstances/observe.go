package appinstances

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/verdantflarehub/verdantflare-station-runtime/internal/computepolicy"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
)

type Observation struct {
	PodUID     string `json:"pod_uid"`
	ServiceUID string `json:"service_uid"`
	SecretUID  string `json:"secret_uid"`
	PodReady   bool   `json:"pod_ready"`
	BootID     string `json:"boot_id,omitempty"`
	Endpoint   string `json:"-"`
}

func (o Observation) valid() bool {
	if o.BootID != "" && (!validBoot(o.BootID) || o.PodUID == "") {
		return false
	}
	for _, uid := range []string{o.PodUID, o.ServiceUID, o.SecretUID} {
		if uid != "" && !validUID(uid) {
			return false
		}
	}
	return (o.PodUID == "" || o.SecretUID != "" && o.ServiceUID != "") &&
		(o.ServiceUID == "" || o.SecretUID != "") && (!o.PodReady || o.PodUID != "")
}

func validBoot(value string) bool {
	id, e := uuid.Parse(value)
	return e == nil && id.Version() == 4 && id.String() == value
}

func owned(actual, desired metav1.Object, uid string) bool {
	if !validUID(string(actual.GetUID())) || actual.GetDeletionTimestamp() != nil || actual.GetName() != desired.GetName() || actual.GetNamespace() != desired.GetNamespace() ||
		len(actual.GetOwnerReferences()) != 0 || len(actual.GetFinalizers()) != 0 || uid != "" && string(actual.GetUID()) != uid {
		return false
	}
	for key, value := range desired.GetAnnotations() {
		if actual.GetAnnotations()[key] != value {
			return false
		}
	}
	for key, value := range desired.GetLabels() {
		if actual.GetLabels()[key] != value {
			return false
		}
	}
	return true
}
func normalizePod(pod *corev1.Pod, node string) *corev1.PodSpec {
	p := pod.DeepCopy().Spec
	if p.NodeName != "" && p.NodeName != node {
		return nil
	}
	p.NodeName = ""
	if p.DNSPolicy == "" {
		p.DNSPolicy = corev1.DNSClusterFirst
	}
	if p.ServiceAccountName == "" {
		p.ServiceAccountName = "default"
	}
	if p.DeprecatedServiceAccount == "" {
		p.DeprecatedServiceAccount = "default"
	}
	for _, t := range p.Tolerations {
		if (t.Key != "node.kubernetes.io/not-ready" && t.Key != "node.kubernetes.io/unreachable") || t.Operator != corev1.TolerationOpExists ||
			t.Effect != corev1.TaintEffectNoExecute || t.Value != "" || t.TolerationSeconds == nil || *t.TolerationSeconds != 300 {
			return nil
		}
	}
	p.Tolerations = nil
	for i := range p.Containers {
		c := &p.Containers[i]
		if c.TerminationMessagePath == "" {
			c.TerminationMessagePath = "/dev/termination-log"
		}
		if c.TerminationMessagePolicy == "" {
			c.TerminationMessagePolicy = corev1.TerminationMessageReadFile
		}
		for _, probe := range []*corev1.Probe{c.StartupProbe, c.ReadinessProbe, c.LivenessProbe} {
			if probe != nil && probe.SuccessThreshold == 0 {
				probe.SuccessThreshold = 1
			}
		}
	}
	return &p
}
func podMatches(actual, desired *corev1.Pod, uid, node string) bool {
	a, d := normalizePod(actual, node), normalizePod(desired, node)
	if !owned(actual, desired, uid) || a == nil || d == nil {
		return false
	}
	if equality.Semantic.DeepEqual(a, d) {
		return true
	}
	computepolicy.Apply(d)
	return equality.Semantic.DeepEqual(a, d)
}
func serviceMatches(actual, desired *corev1.Service, uid string) bool {
	if !owned(actual, desired, uid) || actual.Spec.ClusterIP == "None" || len(actual.Spec.ExternalIPs) != 0 {
		return false
	}
	a, d := actual.DeepCopy().Spec, desired.DeepCopy().Spec
	// Only API-assigned cluster addressing and standard defaults are normalized.
	a.ClusterIP = ""
	a.ClusterIPs = nil
	a.IPFamilies = nil
	a.IPFamilyPolicy = nil
	if a.SessionAffinity == corev1.ServiceAffinityNone {
		a.SessionAffinity = ""
	}
	if a.InternalTrafficPolicy != nil && *a.InternalTrafficPolicy == corev1.ServiceInternalTrafficPolicyCluster {
		a.InternalTrafficPolicy = nil
	}
	return equality.Semantic.DeepEqual(a, d)
}

// ObserveWorker never mutates resources and never equates PodReady with App
// readiness. The coordinator must additionally verify the worker protocol.
func ObserveWorker(ctx context.Context, client kubernetes.Interface, in Run, p Profile, b Volume, expected Observation) (Observation, error) {
	if !expected.valid() {
		return Observation{}, ErrInvalid
	}
	desired, svc, e := WorkerObjects(in, p, b)
	if e != nil {
		return Observation{}, e
	}
	pod, e := client.CoreV1().Pods(p.Namespace).Get(ctx, desired.Name, metav1.GetOptions{})
	if e != nil {
		return Observation{}, e
	}
	if !podMatches(pod, desired, expected.PodUID, p.NodeName) {
		return Observation{}, ErrBinding
	}
	current, e := client.CoreV1().Services(p.Namespace).Get(ctx, svc.Name, metav1.GetOptions{})
	if e != nil {
		return Observation{}, e
	}
	if !serviceMatches(current, svc, expected.ServiceUID) {
		return Observation{}, ErrBinding
	}
	secret, e := client.CoreV1().Secrets(p.Namespace).Get(ctx, instanceName(in.Identity), metav1.GetOptions{})
	if e != nil {
		return Observation{}, e
	}
	if !secretMatches(secret, in.Identity, p, expected.SecretUID) {
		return Observation{}, ErrBinding
	}
	if _, e = InspectVolume(ctx, client, in.Identity, p, b.PVCName, &b, map[types.UID]bool{pod.UID: true}); e != nil {
		return Observation{}, e
	}
	if e = volumeClaimed(ctx, client, in.Identity, p, b); e != nil {
		return Observation{}, e
	}
	out := Observation{PodUID: string(pod.UID), ServiceUID: string(current.UID), SecretUID: string(secret.UID),
		Endpoint: fmt.Sprintf("http://%s.%s.svc.cluster.local:%d", svc.Name, p.Namespace, p.Port)}
	if pod.Status.Phase == corev1.PodRunning && pod.Spec.NodeName == p.NodeName {
		for _, c := range pod.Status.Conditions {
			if c.Type == corev1.PodReady && c.Status == corev1.ConditionTrue {
				out.PodReady = true
			}
		}
	}
	return out, nil
}
func credentialValid(value []byte) bool {
	if len(value) < 32 || len(value) > 512 {
		return false
	}
	for _, b := range value {
		if b <= 32 || b > 126 {
			return false
		}
	}
	return true
}
