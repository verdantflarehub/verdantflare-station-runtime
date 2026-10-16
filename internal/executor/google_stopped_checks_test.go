package executor

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
)

func TestGoogleStoppedChecks(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		for _, scenario := range []string{"ready", "busy", "missing count", "wrong scope", "failed", "wrong image", "restart"} {
			t.Run(map[bool]string{false: "http", true: "legacy"}[legacy]+"/"+scenario, func(t *testing.T) {
				f, checker, index := stoppedChecksFixtureFor(t, "google")
				for _, target := range []*Target{&f.d.Source, &f.d.Target} {
					target.ActivityURL = "http://" + target.Deployment.Name + "." + target.Deployment.Namespace + ".svc.cluster.local:8000/v1/activity"
				}
				f.b.Source.AppVersion = "0.1.2"
				if legacy {
					f.b.Source.AppVersion = "0.1.0"
					readOnly := true
					f.pod.Spec.Containers[0].Command = nil
					f.pod.Spec.Containers[0].Args = nil
					f.pod.Spec.Containers[0].WorkingDir = "/app"
					f.pod.Spec.Containers[0].SecurityContext = &corev1.SecurityContext{ReadOnlyRootFilesystem: &readOnly}
					f.pod.Spec.Containers[0].VolumeMounts = []corev1.VolumeMount{{Name: "state", MountPath: "/data/google"}}
				}
				if scenario == "wrong image" {
					f.pod.Status.ContainerStatuses[0].ImageID = "repo@sha256:" + strings.Repeat("b", 64)
				}
				f.savePod(t)
				proof := func() ([]byte, error) {
					switch scenario {
					case "busy":
						return []byte(`{"schema_version":1,"scope":"installation","submitted_count":2,"active_count":1}`), nil
					case "missing count":
						return []byte(`{"schema_version":1,"scope":"installation","submitted_count":2}`), nil
					case "wrong scope":
						return []byte(`{"schema_version":1,"scope":"organization","submitted_count":2,"active_count":0}`), nil
					case "failed":
						return nil, ErrManagementMaintenanceUnavailable
					}
					return []byte(`{"schema_version":1,"scope":"installation","submitted_count":2,"active_count":0}`), nil
				}
				execs := 0
				f.d.Kubernetes.googleActivityExec = func(_ context.Context, ns, pod, container string, input []byte) ([]byte, error) {
					execs++
					if !legacy || ns != f.pod.Namespace || pod != f.pod.Name || container != f.pod.Spec.Containers[0].Name || len(input) != 0 {
						t.Fatal("unbound exec")
					}
					return proof()
				}
				checker.client.Transport = maintenanceRoundTrip(func(req *http.Request) (*http.Response, error) {
					f.calls++
					if req.Method != "GET" || req.URL.Host != "10.0.0.9:8000" {
						t.Fatal("unexpected request")
					}
					switch req.URL.Path {
					case "/v1/activity":
						if legacy {
							t.Fatal("legacy HTTP activity must not be attempted")
						}
						raw, err := proof()
						return proofResponse(200, string(raw)), err
					case "/internal/dashboard-declaration":
						return proofResponse(200, `{"appId":"google","appVersion":"`+f.b.Source.AppVersion+`","dashboard":{"artifactDigest":"`+f.b.Source.DashboardDigest+`"}}`), nil
					case "/internal/dashboard-index":
						if scenario == "restart" {
							f.pod.Status.ContainerStatuses[0].RestartCount++
							f.savePod(t)
						}
						return proofResponse(200, string(index)), nil
					}
					t.Fatal("unexpected path")
					return nil, ErrManagementReplacement
				})
				err := checker.Check(context.Background(), f.b, &StoppedReplacement{f.d}, f.c, false, true)
				if scenario == "ready" && err != nil {
					t.Fatal(err)
				}
				if scenario == "busy" && !errors.Is(err, ErrManagementInstallationBusy) {
					t.Fatal("busy proof lost", err)
				}
				if scenario != "ready" && err == nil {
					t.Fatal("invalid proof accepted")
				}
				if scenario == "wrong image" && (execs != 0 || f.calls != 0) {
					t.Fatal("contacted unverified image")
				}
				if !legacy && execs != 0 {
					t.Fatal("HTTP failure fell back to exec")
				}
			})
		}
	}
}
