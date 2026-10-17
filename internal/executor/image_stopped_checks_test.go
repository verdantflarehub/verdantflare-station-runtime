package executor

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
)

func TestImageStoppedChecksBoundReadOnlyEvidence(t *testing.T) {
	for _, version := range []string{"0.2.16", "0.2.18", "0.2.20", "0.2.21", "0.2.22", "0.2.23", "0.2.24"} {
		t.Run(version, func(t *testing.T) {
			for _, scenario := range []string{"ready", "busy", "wrong image", "restart", "redirect", "unsafe target"} {
				t.Run(scenario, func(t *testing.T) {
					f, checker, _ := stoppedChecksFixtureFor(t, "image-mcp")
					f.b.Source.AppVersion, f.b.Target.AppVersion = version, "0.2.24"
					f.b.Source.DashboardDigest = imageDashboardDigest([]byte("image html"))
					for _, target := range []*Target{&f.d.Source, &f.d.Target} {
						target.ActivityURL = "http://" + target.Deployment.Name + "." + target.Deployment.Namespace + ".svc.cluster.local:8000/api/tasks/stats"
					}
					if scenario == "wrong image" {
						f.pod.Status.ContainerStatuses[0].ImageID = "repo@sha256:" + strings.Repeat("b", 64)
						f.savePod(t)
					}
					if scenario == "unsafe target" {
						f.b.Target.AppVersion = "0.2.17"
					}
					calls := 0
					checker.client.Transport = maintenanceRoundTrip(func(req *http.Request) (*http.Response, error) {
						calls++
						if req.Method != "GET" || req.URL.Host != "10.0.0.9:8000" || req.Header.Get("Authorization") != "Bearer "+strings.Repeat("test-only-", 5) {
							t.Fatal("unbound request")
						}
						if scenario == "redirect" {
							return proofResponse(302, ""), nil
						}
						switch req.URL.Path {
						case "/api/tasks/stats":
							if scenario == "busy" {
								return proofResponse(200, `{"total":1,"queued":0,"running":1,"completed":0,"failed":0,"canceled":0}`), nil
							}
							return proofResponse(200, `{"total":0,"queued":0,"running":0,"completed":0,"failed":0,"canceled":0}`), nil
						case "/health":
							return proofResponse(200, `{"status":"ok","service":"image-mcp-server","version":"v`+version+`"}`), nil
						case "/dashboard":
							if scenario == "restart" {
								f.pod.Status.ContainerStatuses[0].RestartCount++
								f.savePod(t)
							}
							return proofResponse(200, "image html"), nil
						}
						t.Fatal("unexpected endpoint")
						return nil, ErrManagementReplacement
					})
					err := checker.Check(context.Background(), f.b, &StoppedReplacement{f.d}, f.c, false, true)
					if scenario == "ready" && (err != nil || calls != 3) {
						t.Fatal(calls, err)
					}
					if scenario != "ready" && err == nil {
						t.Fatal("invalid evidence accepted")
					}
					if scenario == "busy" && !errors.Is(err, ErrManagementInstallationBusy) {
						t.Fatal(err)
					}
					if (scenario == "wrong image" || scenario == "unsafe target") && calls != 0 {
						t.Fatal("contacted invalid target")
					}
				})
			}

		})
	}
}
