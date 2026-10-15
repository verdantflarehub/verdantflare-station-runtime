package executor

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/google/uuid"
)

func TestProjectStoppedChecks(t *testing.T) {
	for _, scenario := range []string{"ready", "health failure", "wrong image", "wrong digest", "restart", "insecure endpoint"} {
		t.Run(scenario, func(t *testing.T) {
			f, s, index := stoppedChecksFixtureFor(t, "project")
			for _, target := range []*Target{&f.d.Source, &f.d.Target} {
				target.ActivityURL = "https://" + target.Deployment.Name + "." + target.Deployment.Namespace + ".svc.cluster.local:8443/healthz"
			}
			if scenario == "wrong image" {
				f.pod.Status.ContainerStatuses[0].ImageID = "repo@sha256:" + strings.Repeat("b", 64)
				f.savePod(t)
			}
			if scenario == "insecure endpoint" {
				f.d.Source.ActivityURL = strings.Replace(f.d.Source.ActivityURL, "https:", "http:", 1)
			}
			s.client.Transport = maintenanceRoundTrip(func(req *http.Request) (*http.Response, error) {
				f.calls++
				requestID, err := uuid.Parse(req.Header.Get("X-Request-Id"))
				if req.Method != "GET" || req.URL.Scheme != "https" || req.URL.Host != "10.0.0.9:8443" || req.Host != f.d.Source.Deployment.Name+"."+f.d.Source.Deployment.Namespace+".svc.cluster.local:8443" || err != nil || requestID.Version() != 7 || req.Header.Get("Authorization") != "Bearer "+strings.Repeat("test-only-", 5) {
					t.Fatal("invalid HTTPS or request identity")
				}
				switch req.URL.Path {
				case "/healthz":
					if scenario == "health failure" {
						return proofResponse(503, ""), nil
					}
					return proofResponse(204, ""), nil
				case "/internal/dashboard/declaration":
					return proofResponse(200, `{"appId":"project","appVersion":"`+f.b.Source.AppVersion+`","dashboard":{"artifactDigest":"`+f.b.Source.DashboardDigest+`"}}`), nil
				case "/internal/dashboard/index":
					if scenario == "wrong digest" {
						return proofResponse(200, `{}`), nil
					}
					if scenario == "restart" {
						f.pod.Status.ContainerStatuses[0].RestartCount++
						f.savePod(t)
					}
					return proofResponse(200, string(index)), nil
				default:
					t.Fatal("unexpected business request")
					return nil, ErrManagementReplacement
				}
			})
			err := s.Check(context.Background(), f.b, &StoppedReplacement{f.d}, f.c, false, true)
			if scenario == "ready" {
				if err != nil || f.calls != 3 {
					t.Fatal("project check failed", err, f.calls)
				}
			} else if err == nil {
				t.Fatal("invalid evidence accepted")
			}
			if (scenario == "wrong image" || scenario == "insecure endpoint") && f.calls != 0 {
				t.Fatal("credential sent before validation")
			}
		})
	}
}
