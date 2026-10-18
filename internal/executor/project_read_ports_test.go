package executor

import (
	"context"
	"errors"
	"reflect"
	"testing"

	pb "github.com/verdantflarehub/verdantflare-station-runtime/api/runtimev1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes/fake"
)

func projectReadFixture(t *testing.T) (*StoppedReplacement, *fake.Clientset, ManagementCheckpoint, *pb.AppRequest) {
	t.Helper()
	r, client, checkpoint := managementFixtureFor(t, func(source, target *Target) {
		source.AppID, target.AppID = "project", "project"
		source.Deployment.Namespace, target.Deployment.Namespace = "verdantflare-station", "verdantflare-station"
		source.Services[0].Name, source.Services[0].Namespace = "app-project", "verdantflare-station"
		source.Services[0].Spec.Type = corev1.ServiceTypeClusterIP
		source.Services[0].Spec.Ports = []corev1.ServicePort{
			{Name: "business", Port: 8443, TargetPort: intstr.FromString("business"), Protocol: corev1.ProtocolTCP},
			{Name: "mcp", Port: 8444, TargetPort: intstr.FromString("mcp"), Protocol: corev1.ProtocolTCP},
		}
		for _, t := range []*Target{source, target} {
			t.Deployment.Name = "app-project"
			t.Deployment.Spec.Template.Spec.Containers[0].Ports = []corev1.ContainerPort{
				{Name: "business", ContainerPort: 8443, Protocol: corev1.ProtocolTCP},
				{Name: "mcp", ContainerPort: 8444, Protocol: corev1.ProtocolTCP},
			}
			t.Deployment.Spec.Template.Spec.Containers[0].Env = append(t.Deployment.Spec.Template.Spec.Containers[0].Env,
				corev1.EnvVar{Name: "PROJECT_TLS_CERT", Value: "/tls/cert"}, corev1.EnvVar{Name: "PROJECT_TLS_KEY", Value: "/tls/key"})
		}
		target.Services = []*corev1.Service{source.Services[0].DeepCopy()}
	})
	r.Target.Services = []*corev1.Service{r.Source.Services[0].DeepCopy()}
	r.Target.Services[0].Spec.Ports = append(r.Target.Services[0].Spec.Ports, corev1.ServicePort{
		Name: "peer-read", Port: 8096, TargetPort: intstr.FromString("peer-read"), Protocol: corev1.ProtocolTCP,
	})
	container := &r.Target.Deployment.Spec.Template.Spec.Containers[0]
	container.Ports = append(container.Ports, corev1.ContainerPort{Name: "peer-read", ContainerPort: 8096, Protocol: corev1.ProtocolTCP})
	container.Env = append(container.Env, corev1.EnvVar{Name: "PROJECT_READ_LISTEN", Value: "0.0.0.0:8096"}, corev1.EnvVar{Name: "PROJECT_READ_TOKEN", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "read-only"}, Key: "token"}}})
	in := &pb.AppRequest{OperationId: r.OperationID, InstallationId: r.Source.InstallationID,
		StationId: r.StationID, OrganizationId: r.OrganizationID, AppId: r.Source.AppID, AppVersion: r.Source.Version}
	r.Source, r.Target = unboundPortsTarget(r.Source), unboundPortsTarget(r.Target)
	driver, err := NewStoppedReplacement(r.Kubernetes, r.Source, r.Target, in)
	if err != nil {
		t.Fatal(err)
	}
	return driver, client, checkpoint, in
}

func TestProjectReadPortsUpgradeRollbackAndReplay(t *testing.T) {
	r, client, checkpoint, _ := projectReadFixture(t)
	ctx := context.Background()
	for _, target := range []bool{true, false} {
		for i := 0; i < 2; i++ {
			if _, err := r.Gate(ctx, checkpoint); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := r.Stop(ctx, checkpoint); err != nil {
			t.Fatal(err)
		}
		stoppedObservation(t, client, checkpoint, false)
		if done, err := r.Stop(ctx, checkpoint); err != nil || !done {
			t.Fatal("stop", done, err)
		}
		if _, err := r.Start(ctx, checkpoint, target); err != nil {
			t.Fatal(err)
		}
		stoppedObservation(t, client, checkpoint, true)
		before, err := client.CoreV1().Services("verdantflare-station").Get(ctx, "app-project", metav1.GetOptions{})
		if err != nil || !reflect.DeepEqual(before.Spec.Selector, r.gatedSelector()) {
			t.Fatal("verification opened routing", err)
		}
		wantBefore := 2
		if !target {
			wantBefore = 3
		}
		if len(before.Spec.Ports) != wantBefore {
			t.Fatal("ports changed before publication")
		}
		if _, err := r.Open(ctx, checkpoint, !target); !errors.Is(err, ErrManagementReplacement) {
			t.Fatal("wrong direction opened routing", err)
		}
		for i := 0; i < 2; i++ {
			if done, err := r.Open(ctx, checkpoint, target); err != nil || done != (i == 1) {
				t.Fatal("open/replay", done, err)
			}
		}
		after, _ := client.CoreV1().Services("verdantflare-station").Get(ctx, "app-project", metav1.GetOptions{})
		want := checkpoint.Services[0].DeepCopy()
		if target {
			want.Spec.Ports = r.Target.Services[0].Spec.Ports
		}
		if after.UID != want.UID || !reflect.DeepEqual(after.Spec, want.Spec) {
			t.Fatal("service identity or unrelated configuration changed")
		}
	}
}

func TestProjectReadPortsRejectUnrelatedChangesAndHotReplacement(t *testing.T) {
	r, _, _, in := projectReadFixture(t)
	if _, err := NewManagementReplacement(r.Kubernetes, unboundPortsTarget(r.Source), unboundPortsTarget(r.Target), in); !errors.Is(err, ErrManagementReplacement) {
		t.Fatal("hot replacement admitted port change", err)
	}
	for _, change := range []func(*Target){
		func(t *Target) { t.Services[0].Spec.Ports[2].Port = 9000 },
		func(t *Target) { t.Services[0].Spec.Ports[2].TargetPort = intstr.FromInt32(9000) },
		func(t *Target) { t.Services[0].Spec.Ports[2].NodePort = 30001 },
		func(t *Target) { t.Services[0].Spec.Type = corev1.ServiceTypeNodePort },
		func(t *Target) { t.Services[0].Spec.Selector = map[string]string{"other": "app"} },
		func(t *Target) { t.Services[0].Spec.ExternalIPs = []string{"203.0.113.1"} },
		func(t *Target) { t.Services = append(t.Services, t.Services[0].DeepCopy()) },
		func(t *Target) { t.Deployment.Spec.Template.Spec.Containers[0].Ports[2].HostPort = 8096 },
		func(t *Target) { t.Deployment.Spec.Template.Spec.Containers[0].Ports[2].ContainerPort = 8097 },
		func(t *Target) { t.Deployment.Spec.Template.Spec.HostNetwork = true },
		func(t *Target) {
			for i := range t.Deployment.Spec.Template.Spec.Containers[0].Env {
				e := &t.Deployment.Spec.Template.Spec.Containers[0].Env[i]
				if e.Name == "PROJECT_TLS_CERT" {
					e.Value = ""
				}
			}
		},
		func(t *Target) {
			t.Deployment.Spec.Template.Spec.Containers[0].Env = append(t.Deployment.Spec.Template.Spec.Containers[0].Env, corev1.EnvVar{Name: "PROJECT_SERVICE_TOKEN", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "read-only"}, Key: "token"}}})
		},
	} {
		target := r.Target
		target.Services = []*corev1.Service{r.Target.Services[0].DeepCopy()}
		target.Deployment = r.Target.Deployment.DeepCopy()
		change(&target)
		if _, err := NewStoppedReplacement(r.Kubernetes, unboundPortsTarget(r.Source), unboundPortsTarget(target), in); !errors.Is(err, ErrManagementReplacement) {
			t.Fatal("unrelated change admitted", err)
		}
	}
}

func TestProjectReadPortsRejectUnmarkedLiveDrift(t *testing.T) {
	r, client, checkpoint, _ := projectReadFixture(t)
	s := checkpoint.Services[0].DeepCopy()
	s.Spec.Ports = r.Target.Services[0].Spec.Ports
	if _, err := client.CoreV1().Services(s.Namespace).Update(context.Background(), s, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Gate(context.Background(), checkpoint); !errors.Is(err, ErrManagementReplacement) {
		t.Fatal("unmarked port drift admitted", err)
	}
}

func TestProjectReadPortsDefaultProtocol(t *testing.T) {
	r, _, _, in := projectReadFixture(t)
	for _, protocol := range []corev1.Protocol{"", corev1.ProtocolTCP, corev1.ProtocolUDP, corev1.ProtocolSCTP} {
		t.Run(string(protocol), func(t *testing.T) {
			source, target := unboundPortsTarget(r.Source), unboundPortsTarget(r.Target)
			source.Services = []*corev1.Service{source.Services[0].DeepCopy()}
			target.Services = []*corev1.Service{target.Services[0].DeepCopy()}
			source.Services[0].Spec.Ports[0].Protocol = protocol
			target.Services[0].Spec.Ports[2].Protocol = ""
			before := source.Services[0].DeepCopy()
			_, err := NewStoppedReplacement(r.Kubernetes, source, target, in)
			allowed := protocol == "" || protocol == corev1.ProtocolTCP
			if (err == nil) != allowed {
				t.Fatalf("protocol %q: %v", protocol, err)
			}
			if !reflect.DeepEqual(source.Services[0], before) || target.Services[0].Spec.Ports[2].Protocol != "" {
				t.Fatal("immutable templates mutated")
			}
		})
	}
}

func TestProjectReadPortsUnifiedAppNamespaceAndMismatchRejection(t *testing.T) {
	r, _, _, in := projectReadFixture(t)
	source, target := unboundPortsTarget(r.Source), unboundPortsTarget(r.Target)
	source.Deployment.Namespace, target.Deployment.Namespace = "verdantflare-app", "verdantflare-app"
	source.Services = []*corev1.Service{source.Services[0].DeepCopy()}
	target.Services = []*corev1.Service{target.Services[0].DeepCopy()}
	source.Services[0].Namespace, target.Services[0].Namespace = "verdantflare-app", "verdantflare-app"
	if !projectReadPorts(source, target) {
		t.Fatal("expected projectReadPorts to admit verdantflare-app")
	}
	if _, err := NewStoppedReplacement(r.Kubernetes, source, target, in); err != nil {
		t.Fatalf("expected NewStoppedReplacement in verdantflare-app to succeed, got %v", err)
	}

	mismatchSvc := target
	mismatchSvc.Services = []*corev1.Service{target.Services[0].DeepCopy()}
	mismatchSvc.Services[0].Namespace = "verdantflare-station"
	if projectReadPorts(source, mismatchSvc) {
		t.Fatal("expected projectReadPorts to reject mismatched service namespace")
	}

	mismatchDep := target
	mismatchDep.Deployment = target.Deployment.DeepCopy()
	mismatchDep.Deployment.Namespace = "verdantflare-station"
	if projectReadPorts(source, mismatchDep) {
		t.Fatal("expected projectReadPorts to reject mismatched deployment namespace")
	}

	defaultNS := source
	defaultNS.Deployment = source.Deployment.DeepCopy()
	defaultNS.Services = []*corev1.Service{source.Services[0].DeepCopy()}
	defaultNS.Deployment.Namespace, defaultNS.Services[0].Namespace = "default", "default"
	defaultTarget := target
	defaultTarget.Deployment = target.Deployment.DeepCopy()
	defaultTarget.Services = []*corev1.Service{target.Services[0].DeepCopy()}
	defaultTarget.Deployment.Namespace, defaultTarget.Services[0].Namespace = "default", "default"
	if projectReadPorts(defaultNS, defaultTarget) {
		t.Fatal("expected projectReadPorts to reject default namespace")
	}
}
