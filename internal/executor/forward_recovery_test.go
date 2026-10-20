package executor

import (
	"context"
	"encoding/hex"
	"net/http"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestForwardProofRequiresActualReleaseAndNeverMutates(t *testing.T) {
	for _, scenario := range []string{"valid", "deployment uid", "service uid", "marker", "image", "routing", "dashboard", "restart"} {
		t.Run(scenario, func(t *testing.T) {
			f, checker, index := stoppedChecksFixtureFor(t, "world")
			ctx := context.Background()
			for _, entry := range []*Target{&f.d.Source, &f.d.Target} {
				entry.ActivityURL = "https://" + entry.Deployment.Name + "." + entry.Deployment.Namespace + ".svc.cluster.local:8443/healthz"
			}
			d := f.c.Deployment.DeepCopy()
			d.Spec = *f.d.Target.Deployment.Spec.DeepCopy()
			mark(&d.ObjectMeta, f.d.Target, f.b.StationID, f.b.OrganizationID)
			d.Annotations[transitionOperationKey] = f.b.OperationID
			d.Annotations["station.verdantflare.com/external-recovery-operation"] = f.b.OperationID
			if scenario == "deployment uid" {
				d.UID = "replacement"
			}
			if scenario == "marker" {
				delete(d.Annotations, "station.verdantflare.com/external-recovery-operation")
			}
			if _, err := f.k.AppsV1().Deployments(d.Namespace).Update(ctx, d, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
			for _, original := range f.c.Services {
				s := original.DeepCopy()
				mark(&s.ObjectMeta, f.d.Target, f.b.StationID, f.b.OrganizationID)
				s.Annotations[transitionOperationKey] = f.b.OperationID
				s.Annotations["station.verdantflare.com/external-recovery-operation"] = f.b.OperationID
				if scenario == "service uid" {
					s.UID = "replacement"
				}
				if scenario == "routing" {
					s.Spec.Selector = map[string]string{"app": "other"}
				}
				if _, err := f.k.CoreV1().Services(s.Namespace).Update(ctx, s, metav1.UpdateOptions{}); err != nil {
					t.Fatal(err)
				}
			}
			f.pod.Spec = *d.Spec.Template.Spec.DeepCopy()
			if scenario == "image" {
				f.pod.Status.ContainerStatuses[0].ImageID = "repo@sha256:" + strings.Repeat("c", 64)
			}
			f.savePod(t)
			checker.client.Transport = maintenanceRoundTrip(func(req *http.Request) (*http.Response, error) {
				if req.Method != "GET" || req.URL.Host != "10.0.0.9:8443" {
					t.Fatal("unexpected request")
				}
				switch req.URL.Path {
				case "/healthz":
					return proofResponse(204, ``), nil
				case "/internal/dashboard/declaration":
					return proofResponse(200, `{"appId":"world","appVersion":"`+f.b.Target.AppVersion+`","dashboard":{"artifactDigest":"`+f.b.Target.DashboardDigest+`"}}`), nil
				case "/internal/dashboard/index":
					if scenario == "dashboard" {
						return proofResponse(200, `{}`), nil
					}
					if scenario == "restart" {
						f.pod.Status.ContainerStatuses[0].RestartCount++
						f.savePod(t)
					}
					return proofResponse(200, string(index)), nil
				default:
					t.Fatal("unexpected path")
					return nil, ErrManagementReplacement
				}
			})
			f.k.ClearActions()
			proof, err := verifyForwardWorkload(ctx, f.b, forwardBinding{Target: f.b.Target}, &StoppedReplacement{f.d}, f.c, checker)
			if scenario == "valid" {
				if err != nil || proof["deployment_uid"] != string(f.c.Deployment.UID) || proof["template_digest"] != "sha256:"+hex.EncodeToString(f.d.Target.Hash[:]) {
					t.Fatal(proof, err)
				}
			} else if err == nil {
				t.Fatal("invalid evidence accepted", scenario)
			}
			for _, action := range f.k.Actions() {
				// The restart scenario's test harness updates the Pod, never production code.
				if action.GetVerb() != "get" && action.GetVerb() != "list" && !(scenario == "restart" && action.GetVerb() == "update" && action.GetResource().Resource == "pods") {
					t.Fatal("reconciliation mutated Kubernetes", action)
				}
			}
		})
	}
}
