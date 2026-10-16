package executor

import (
	"context"
	"reflect"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestImageHistoricalAdoptionMetadataAndGuards(t *testing.T) {
	for _, scenario := range []string{"success", "unsigned", "version", "app", "missing-owner", "foreign-installation", "wrong-hash", "malformed-hash", "uid", "spec"} {
		t.Run(scenario, func(t *testing.T) {
			app := target()
			app.AppID, app.Version = "image-mcp", "0.2.16"
			app.Preinstalled = true
			app.InstallationID, app.ExpectedUID = "installation", "original"
			old := strings.Repeat("a", 64)
			app.Deployment.Annotations = map[string]string{"station.verdantflare.com/reconciles-template-hash": old}
			live := app.Deployment.DeepCopy()
			live.UID = "original"
			live.Annotations = annotations(app, "station", "org")
			live.Annotations[hashKey] = old
			switch scenario {
			case "unsigned":
				app.Preinstalled = false
			case "version":
				app.Version = "0.2.18"
			case "app":
				app.AppID = "other"
			case "missing-owner":
				delete(live.Annotations, ownerKey)
			case "foreign-installation":
				live.Annotations[installationKey] = "foreign"
			case "wrong-hash":
				live.Annotations[hashKey] = strings.Repeat("b", 64)
			case "malformed-hash":
				app.Deployment.Annotations["station.verdantflare.com/reconciles-template-hash"] = "invalid"
			case "uid":
				live.UID = "replaced"
			case "spec":
				live.Spec.Template.Spec.Containers[0].Image = "other:1.0.0"
			}
			client := fake.NewSimpleClientset(live)
			k := Kubernetes{Client: client}
			result, err := k.adopt(context.Background(), app, live, "station", "org")
			if err != nil {
				t.Fatal(err)
			}
			if scenario != "success" {
				if result.Code != "CONFLICT" {
					t.Fatal(result)
				}
				for _, a := range client.Actions() {
					if a.GetVerb() != "get" {
						t.Fatal("mutated on conflict", a)
					}
				}
				return
			}
			if result.Status != "succeeded" {
				t.Fatal(result)
			}
			after, err := client.AppsV1().Deployments(live.Namespace).Get(context.Background(), live.Name, metav1.GetOptions{})
			if err != nil || !reflect.DeepEqual(live.Spec, after.Spec) || after.UID != live.UID || !owned(after.Annotations, app, "station", "org") {
				t.Fatal("workload changed or ownership missing", err)
			}
			before := len(client.Actions())
			result, err = k.adopt(context.Background(), app, after, "station", "org")
			if err != nil || result.Status != "succeeded" || len(client.Actions()) != before {
				t.Fatal("replay mutated workload", result, err)
			}
		})
	}
}
