package executor

import (
	"context"
	"reflect"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestVideoHistoricalAdoptionPreservesWorkloadAndServices(t *testing.T) {
	for _, version := range []string{"0.17.28", "0.17.30"} {
		for _, scenario := range []string{"success", "unsigned", "version", "app", "owner", "installation", "hash", "uid", "sidecar", "env", "storage", "service-owner", "service-route"} {
			t.Run(version+"/"+scenario, func(t *testing.T) {
				app := target()
				app.AppID, app.Version = "video-mcp", version
				app.Preinstalled, app.InstallationID, app.ExpectedUID = true, "installation", "original"
				old := strings.Repeat("a", 64)
				app.Deployment.Annotations = map[string]string{"station.verdantflare.com/reconciles-template-hash": old}
				app.Deployment.Spec.Template.Spec.Containers = append(app.Deployment.Spec.Template.Spec.Containers,
					corev1.Container{Name: "artifact-authority", Image: "example.invalid/video:v0.17.12"})
				app.Services = []*corev1.Service{{ObjectMeta: metav1.ObjectMeta{Name: "video", Namespace: app.Deployment.Namespace}, Spec: corev1.ServiceSpec{Selector: map[string]string{"app": "test"}, Ports: []corev1.ServicePort{{Port: 8000}}}}}
				live := app.Deployment.DeepCopy()
				live.UID, live.Annotations = "original", annotations(app, "station", "org")
				live.Annotations[hashKey] = old
				svc := app.Services[0].DeepCopy()
				svc.UID, svc.Annotations = "service-original", annotations(app, "station", "org")
				svc.Annotations[hashKey] = old
				switch scenario {
				case "unsigned":
					app.Preinstalled = false
				case "version":
					app.Version = "0.17.29"
				case "app":
					app.AppID = "video-mcp-server"
				case "owner":
					delete(live.Annotations, ownerKey)
				case "installation":
					live.Annotations[installationKey] = "foreign"
				case "hash":
					live.Annotations[hashKey] = strings.Repeat("b", 64)
				case "uid":
					live.UID = "replacement"
				case "sidecar":
					live.Spec.Template.Spec.Containers[1].Image = "example.invalid/video:v0.17.28"
				case "env":
					live.Spec.Template.Spec.Containers[0].Env = []corev1.EnvVar{{Name: "UNDECLARED", Value: "true"}}
				case "storage":
					live.Spec.Template.Spec.Volumes = []corev1.Volume{{Name: "unexpected", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}}}
				case "service-owner":
					svc.Annotations[orgKey] = "foreign"
				case "service-route":
					svc.Spec.Selector["app"] = "other"
				}
				client := fake.NewSimpleClientset(live, svc)
				k := Kubernetes{Client: client}
				result, err := k.adopt(context.Background(), app, live, "station", "org")
				if err != nil {
					t.Fatal(err)
				}
				if scenario != "success" {
					if result.Code != "CONFLICT" {
						t.Fatal(result)
					}
					for _, action := range client.Actions() {
						if action.GetVerb() != "get" {
							t.Fatal("mutated despite failed preflight", action)
						}
					}
					return
				}
				if result.Status != "succeeded" {
					t.Fatal(result)
				}
				after, err := client.AppsV1().Deployments(live.Namespace).Get(context.Background(), live.Name, metav1.GetOptions{})
				if err != nil || after.UID != live.UID || !reflect.DeepEqual(after.Spec, live.Spec) || !owned(after.Annotations, app, "station", "org") {
					t.Fatal("deployment changed or metadata not reconciled", err)
				}
				afterService, err := client.CoreV1().Services(svc.Namespace).Get(context.Background(), svc.Name, metav1.GetOptions{})
				if err != nil || afterService.UID != svc.UID || !reflect.DeepEqual(afterService.Spec, svc.Spec) || !owned(afterService.Annotations, app, "station", "org") {
					t.Fatal("service changed or metadata not reconciled", err)
				}
				before := len(client.Actions())
				result, err = k.adopt(context.Background(), app, after, "station", "org")
				if err != nil || result.Status != "succeeded" {
					t.Fatal(result, err)
				}
				for _, action := range client.Actions()[before:] {
					if action.GetVerb() != "get" {
						t.Fatal("replay mutated objects", action)
					}
				}
			})
		}
	}
}
