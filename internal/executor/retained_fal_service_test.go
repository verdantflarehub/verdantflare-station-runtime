package executor

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

func retainedFal(t Target) *corev1.Service {
	return &corev1.Service{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Service"},
		ObjectMeta: metav1.ObjectMeta{Name: t.Deployment.Name, Namespace: t.Deployment.Namespace, UID: "12c63c0e-d2e5-4932-b5c9-c845f0344d91"},
		Spec: corev1.ServiceSpec{Type: corev1.ServiceTypeClusterIP, ClusterIP: "10.0.0.2", Selector: t.Deployment.Spec.Selector.MatchLabels,
			Ports: []corev1.ServicePort{{Port: 8000, Protocol: corev1.ProtocolTCP, TargetPort: intstr.FromInt32(8000)},
				{Port: 8001, Protocol: corev1.ProtocolTCP, TargetPort: intstr.FromInt32(8001)}}}}
}

func TestRetainedFalServiceBoundaries(t *testing.T) {
	for _, name := range []string{"valid", "foreign-app", "foreign-namespace", "selector", "uid", "external", "port", "missing-target", "different-target", "duplicate"} {
		t.Run(name, func(t *testing.T) {
			s := target()
			s.AppID = "fal"
			s.Services = nil
			s.RetainedFalService = retainedFal(s)
			d := s
			d.RetainedFalService = s.RetainedFalService.DeepCopy()
			switch name {
			case "foreign-app":
				s.AppID = "world"
			case "foreign-namespace":
				s.RetainedFalService.Namespace = "foreign"
			case "selector":
				s.RetainedFalService.Spec.Selector = map[string]string{"foreign": "yes"}
			case "uid":
				s.RetainedFalService.UID = ""
			case "external":
				s.RetainedFalService.Spec.Type = corev1.ServiceTypeNodePort
			case "port":
				s.RetainedFalService.Spec.Ports[1].Port = 443
			case "missing-target":
				d.RetainedFalService = nil
			case "different-target":
				d.RetainedFalService.Spec.ClusterIP = "10.0.0.3"
			case "duplicate":
				s.Services = []*corev1.Service{s.RetainedFalService}
			}
			a, b, err := stoppedRetainedServices(s, d)
			if name != "valid" {
				if err == nil {
					t.Fatal("unsafe binding accepted")
				}
				return
			}
			if err != nil || len(a.Services) != 1 || len(b.Services) != 1 || len(s.Services) != 0 || a.Hash != s.Hash {
				t.Fatal("binding changed original registration", err)
			}
			a.Services[0].Spec.ClusterIP = "10.0.0.4"
			if b.Services[0].Spec.ClusterIP != "10.0.0.2" || s.RetainedFalService.Spec.ClusterIP != "10.0.0.2" {
				t.Fatal("shared mutable service")
			}
		})
	}
}

func TestRetainedFalRegistryPreservesOriginalHash(t *testing.T) {
	root := t.TempDir()
	app := target()
	app.AppID = "fal"
	app.Services = nil
	app.Deployment.APIVersion = "apps/v1"
	app.Deployment.Kind = "Deployment"
	write := func(name string, v any) {
		t.Helper()
		raw, e := json.Marshal(v)
		if e != nil {
			t.Fatal(e)
		}
		if e = os.WriteFile(filepath.Join(root, name), raw, 0600); e != nil {
			t.Fatal(e)
		}
	}
	c := app.Deployment.Spec.Template.Spec.Containers[0]
	write("workload.json", app.Deployment)
	write("manifest.json", map[string]any{"app_id": "fal", "version": "0.1.0", "required_models": []any{}, "required_resources": map[string]any{"gpu": 0, "disk_bytes": 0},
		"deployment": map[string]any{"template_ref": "workload.json", "workload_name": app.Deployment.Name, "images": []any{map[string]string{"component": c.Name, "image": c.Image}}}})
	entry := registryEntry{ManifestFile: "manifest.json", Namespace: app.Deployment.Namespace}
	write("registry.json", []registryEntry{entry})
	before, e := LoadRegistry(root, filepath.Join(root, "registry.json"))
	if e != nil {
		t.Fatal(e)
	}
	entry.RetainedFalService = retainedFal(app)
	write("registry.json", []registryEntry{entry})
	after, e := LoadRegistry(root, filepath.Join(root, "registry.json"))
	if e != nil {
		t.Fatal(e)
	}
	if before["fal@0.1.0"].Hash != after["fal@0.1.0"].Hash || len(after["fal@0.1.0"].Services) != 0 || after["fal@0.1.0"].RetainedFalService == nil {
		t.Fatal("original template changed")
	}
}

func TestRetainedFalNamedPorts(t *testing.T) {
	s := target()
	s.AppID = "fal"
	s.Services = nil
	s.RetainedFalService = retainedFal(s)
	s.Deployment.Spec.Template.Spec.Containers[0].Ports = []corev1.ContainerPort{{Name: "management", ContainerPort: 8000}, {Name: "execution", ContainerPort: 8001}}
	s.RetainedFalService.Spec.Ports[0].TargetPort = intstr.FromString("management")
	s.RetainedFalService.Spec.Ports[1].TargetPort = intstr.FromString("execution")
	if !validRetainedFalService(s) {
		t.Fatal("existing named ports rejected")
	}
	s.Deployment.Spec.Template.Spec.Containers[0].Ports[1].ContainerPort = 8002
	if validRetainedFalService(s) {
		t.Fatal("wrong target listener accepted")
	}
}

func TestRetainedServiceRejectsReplacementAndGatesOriginal(t *testing.T) {
	r, k, c := stoppedFixture(t)
	// Capture and checkpoint validation must bind supplemental resource identity.
	r.Source.Services[0] = r.Source.Services[0].DeepCopy()
	r.Source.Services[0].UID = "replaced"
	if _, e := r.Capture(context.Background()); e == nil {
		t.Fatal("replaced service accepted")
	}
	if r.validCheckpoint(c) {
		t.Fatal("replaced checkpoint accepted")
	}
	r.Source.Services[0].UID = c.Services[0].UID
	if _, e := r.Capture(context.Background()); e != nil {
		t.Fatal(e)
	}
	if done, e := r.Gate(context.Background(), c); e != nil || done {
		t.Fatal("gate", done, e)
	}
	live, e := k.CoreV1().Services(c.Services[0].Namespace).Get(context.Background(), c.Services[0].Name, metav1.GetOptions{})
	if e != nil || live.UID != c.Services[0].UID || live.Spec.Selector[stoppedOperationKey] != r.OperationID {
		t.Fatal("original service not gated", e)
	}
}
