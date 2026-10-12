package executor

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes/fake"
)

func TestPreinstalledAdoptionAllowsKnownDefaultsWithoutSpecMutation(t *testing.T) {
	app := target()
	app.Preinstalled = true
	app.ExpectedUID = "00112233-4455-6677-8899-aabbccddeeff"
	live := app.Deployment.DeepCopy()
	live.UID = types.UID(app.ExpectedUID)
	live.ResourceVersion = "7"
	live.Spec.Template.Spec.RestartPolicy = corev1.RestartPolicyAlways
	live.Spec.Template.Spec.DNSPolicy = corev1.DNSClusterFirst
	live.Spec.Template.Spec.SchedulerName = "default-scheduler"
	live.Spec.Template.Spec.ServiceAccountName = "default"
	live.Spec.Template.Spec.DeprecatedServiceAccount = "default"
	live.Spec.Template.Spec.Containers[0].TerminationMessagePath = "/dev/termination-log"
	live.Spec.Template.Spec.Containers[0].TerminationMessagePolicy = corev1.TerminationMessageReadFile
	client := fake.NewSimpleClientset(live)
	driver := Kubernetes{Client: client}
	out, err := driver.Step(context.Background(), app, "adopt", "op", "station", "org")
	if err != nil || out.Status != "succeeded" {
		t.Fatal(out, err)
	}
	after, err := client.AppsV1().Deployments(live.Namespace).Get(context.Background(), live.Name, metav1.GetOptions{})
	if err != nil || !preinstalledDeploymentCompatible(live, after) {
		t.Fatal("adoption changed execution spec", err)
	}
	for _, action := range client.Actions() {
		if action.GetVerb() != "get" && action.GetVerb() != "patch" {
			t.Fatal("unexpected mutation")
		}
	}
}

func TestPreinstalledAdoptionRejectsUndeclaredExecutionInputs(t *testing.T) {
	for _, kind := range []string{"container-security", "pod-security", "init-container", "volume", "host-alias", "node", "service-account", "extra-probe", "dns-option"} {
		t.Run(kind, func(t *testing.T) {
			app := target()
			app.Preinstalled = true
			app.ExpectedUID = "00112233-4455-6677-8899-aabbccddeeff"
			live := app.Deployment.DeepCopy()
			live.UID = types.UID(app.ExpectedUID)
			live.ResourceVersion = "7"
			spec := &live.Spec.Template.Spec
			truth := true
			uid := int64(0)
			switch kind {
			case "container-security":
				spec.Containers[0].SecurityContext = &corev1.SecurityContext{Privileged: &truth}
			case "pod-security":
				spec.SecurityContext = &corev1.PodSecurityContext{RunAsUser: &uid}
			case "init-container":
				spec.InitContainers = []corev1.Container{{Name: "extra", Image: "extra:1.0.0"}}
			case "volume":
				spec.Volumes = append(spec.Volumes, corev1.Volume{Name: "extra", VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: "/"}}})
			case "host-alias":
				spec.HostAliases = []corev1.HostAlias{{IP: "127.0.0.1", Hostnames: []string{"unexpected.invalid"}}}
			case "node":
				spec.NodeName = "unexpected-node"
			case "service-account":
				spec.ServiceAccountName = "privileged"
			case "extra-probe":
				spec.Containers[0].LivenessProbe = &corev1.Probe{ProbeHandler: corev1.ProbeHandler{Exec: &corev1.ExecAction{Command: []string{"unexpected"}}}}
			case "dns-option":
				spec.DNSConfig = &corev1.PodDNSConfig{Nameservers: []string{"127.0.0.1"}}
			}
			client := fake.NewSimpleClientset(live)
			driver := Kubernetes{Client: client}
			out, err := driver.Step(context.Background(), app, "adopt", "op", "station", "org")
			if err != nil || out.Code != "CONFLICT" {
				t.Fatal("undeclared input accepted", out, err)
			}
			for _, action := range client.Actions() {
				if action.GetVerb() != "get" {
					t.Fatal("failed preflight mutated workload")
				}
			}
		})
	}
}

func TestPreinstalledServiceOnlyAllowsNetworkAllocationDefaults(t *testing.T) {
	want := &corev1.Service{Spec: corev1.ServiceSpec{Selector: map[string]string{"app": "sample"}, Ports: []corev1.ServicePort{{Port: 80}}}}
	live := want.DeepCopy()
	live.Spec.ClusterIP = "10.0.0.2"
	live.Spec.ClusterIPs = []string{"10.0.0.2"}
	live.Spec.IPFamilies = []corev1.IPFamily{corev1.IPv4Protocol}
	live.Spec.Type = corev1.ServiceTypeClusterIP
	live.Spec.Ports[0].TargetPort = intstr.FromInt32(80)
	if !preinstalledServiceCompatible(want, live) {
		t.Fatal("server allocations rejected")
	}
	live.Spec.Ports[0].TargetPort = intstr.FromInt32(8080)
	if preinstalledServiceCompatible(want, live) {
		t.Fatal("different backend port treated as default")
	}
	live.Spec.Ports[0].TargetPort = intstr.FromInt32(80)
	live.Spec.ExternalIPs = []string{"198.51.100.2"}
	if preinstalledServiceCompatible(want, live) {
		t.Fatal("external exposure treated as default")
	}
	live.Spec.ExternalIPs = nil
	live.Spec.ClusterIP = "None"
	live.Spec.ClusterIPs = []string{"None"}
	if preinstalledServiceCompatible(want, live) {
		t.Fatal("headless service treated as allocated ClusterIP")
	}
}
