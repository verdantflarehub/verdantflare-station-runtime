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

func unboundPortsTarget(target Target) Target {
	target.Deployment = target.Deployment.DeepCopy()
	for i := range target.Deployment.Spec.Template.Spec.Containers {
		container := &target.Deployment.Spec.Template.Spec.Containers[i]
		var env []corev1.EnvVar
		for _, item := range container.Env {
			bound := false
			for _, binding := range target.InstallationBindings {
				bound = bound || (binding.Container == container.Name && binding.Env == item.Name)
			}
			if !bound {
				env = append(env, item)
			}
		}
		container.Env = env
	}
	return target
}

func chatgptPortsFixture(t *testing.T) (*StoppedReplacement, *fake.Clientset, ManagementCheckpoint, *pb.AppRequest) {
	t.Helper()
	r, client, checkpoint := managementFixtureFor(t, func(source, target *Target) {
		source.AppID, target.AppID = "chatgpt", "chatgpt"
		source.Deployment.Namespace, target.Deployment.Namespace = "verdantflare-chatgpt", "verdantflare-chatgpt"
		source.Services[0].Name, source.Services[0].Namespace = "chatgpt-channel", "verdantflare-chatgpt"
		source.Services[0].Spec.Type = corev1.ServiceTypeClusterIP
		source.Services[0].Spec.Ports = []corev1.ServicePort{{Name: "management", Port: 8000, TargetPort: intstr.FromString("management"), Protocol: corev1.ProtocolTCP}}
		target.Services = []*corev1.Service{source.Services[0].DeepCopy()}
	})
	r.Target.Services = []*corev1.Service{r.Source.Services[0].DeepCopy()}
	r.Target.Services[0].Spec.Ports = append(r.Target.Services[0].Spec.Ports, corev1.ServicePort{
		Name: "execution", Port: 8001, TargetPort: intstr.FromInt32(8001), Protocol: corev1.ProtocolTCP,
	})
	in := &pb.AppRequest{OperationId: r.OperationID, InstallationId: r.Source.InstallationID,
		StationId: r.StationID, OrganizationId: r.OrganizationID, AppId: r.Source.AppID, AppVersion: r.Source.Version}
	r.Source, r.Target = unboundPortsTarget(r.Source), unboundPortsTarget(r.Target)
	driver, err := NewStoppedReplacement(r.Kubernetes, r.Source, r.Target, in)
	if err != nil {
		t.Fatal(err)
	}
	return driver, client, checkpoint, in
}

func TestChatGPTExecutionPortsUpgradeRollbackAndReplay(t *testing.T) {
	r, client, checkpoint, _ := chatgptPortsFixture(t)
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
		before, err := client.CoreV1().Services("verdantflare-chatgpt").Get(ctx, "chatgpt-channel", metav1.GetOptions{})
		if err != nil || !reflect.DeepEqual(before.Spec.Selector, r.gatedSelector()) {
			t.Fatal("verification opened routing", err)
		}
		wantBefore := 1
		if !target {
			wantBefore = 2
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
		after, _ := client.CoreV1().Services("verdantflare-chatgpt").Get(ctx, "chatgpt-channel", metav1.GetOptions{})
		want := checkpoint.Services[0].DeepCopy()
		if target {
			want.Spec.Ports = r.Target.Services[0].Spec.Ports
		}
		if after.UID != want.UID || !reflect.DeepEqual(after.Spec, want.Spec) {
			t.Fatal("service identity or unrelated configuration changed")
		}
	}
}

func TestChatGPTExecutionPortsRejectUnrelatedChangesAndHotReplacement(t *testing.T) {
	r, _, _, in := chatgptPortsFixture(t)
	if _, err := NewManagementReplacement(r.Kubernetes, unboundPortsTarget(r.Source), unboundPortsTarget(r.Target), in); !errors.Is(err, ErrManagementReplacement) {
		t.Fatal("hot replacement admitted port change", err)
	}
	for _, change := range []func(*Target){
		func(t *Target) { t.Services[0].Spec.Ports[1].Port = 9000 },
		func(t *Target) { t.Services[0].Spec.Ports[1].TargetPort = intstr.FromInt32(9000) },
		func(t *Target) { t.Services[0].Spec.Ports[1].NodePort = 30001 },
		func(t *Target) { t.Services[0].Spec.Type = corev1.ServiceTypeNodePort },
		func(t *Target) { t.Services[0].Spec.Selector = map[string]string{"other": "app"} },
		func(t *Target) { t.Services[0].Spec.ExternalIPs = []string{"203.0.113.1"} },
		func(t *Target) { t.Services = append(t.Services, t.Services[0].DeepCopy()) },
	} {
		target := r.Target
		target.Services = []*corev1.Service{r.Target.Services[0].DeepCopy()}
		change(&target)
		if _, err := NewStoppedReplacement(r.Kubernetes, unboundPortsTarget(r.Source), unboundPortsTarget(target), in); !errors.Is(err, ErrManagementReplacement) {
			t.Fatal("unrelated change admitted", err)
		}
	}
}

func TestChatGPTExecutionPortsRejectUnmarkedLiveDrift(t *testing.T) {
	r, client, checkpoint, _ := chatgptPortsFixture(t)
	s := checkpoint.Services[0].DeepCopy()
	s.Spec.Ports = r.Target.Services[0].Spec.Ports
	if _, err := client.CoreV1().Services(s.Namespace).Update(context.Background(), s, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Gate(context.Background(), checkpoint); !errors.Is(err, ErrManagementReplacement) {
		t.Fatal("unmarked port drift admitted", err)
	}
}

func TestChatGPTExecutionPortsDefaultProtocol(t *testing.T) {
	r, _, _, in := chatgptPortsFixture(t)
	for _, protocol := range []corev1.Protocol{"", corev1.ProtocolTCP, corev1.ProtocolUDP, corev1.ProtocolSCTP} {
		t.Run(string(protocol), func(t *testing.T) {
			source, target := unboundPortsTarget(r.Source), unboundPortsTarget(r.Target)
			source.Services = []*corev1.Service{source.Services[0].DeepCopy()}
			target.Services = []*corev1.Service{target.Services[0].DeepCopy()}
			source.Services[0].Spec.Ports[0].Protocol = protocol
			target.Services[0].Spec.Ports[1].Protocol = ""
			before := source.Services[0].DeepCopy()
			_, err := NewStoppedReplacement(r.Kubernetes, source, target, in)
			allowed := protocol == "" || protocol == corev1.ProtocolTCP
			if (err == nil) != allowed {
				t.Fatalf("protocol %q: %v", protocol, err)
			}
			if !reflect.DeepEqual(source.Services[0], before) || target.Services[0].Spec.Ports[1].Protocol != "" {
				t.Fatal("immutable templates mutated")
			}
		})
	}
}
