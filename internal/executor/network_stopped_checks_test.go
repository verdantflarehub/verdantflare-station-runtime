package executor

import (
	"context"
	"net/http"
	"strings"
	"testing"
)

func TestNetworkStoppedChecks(t *testing.T) {
	for _, scenario := range []string{"ready", "unhealthy", "invalid health", "duplicate status", "auth denied", "wrong version", "wrong digest", "restart", "foreign host", "wrong port", "short drain"} {
		t.Run(scenario, func(t *testing.T) {
			f, s, index := stoppedChecksFixtureFor(t, "network")
			for _, entry := range []*Target{&f.d.Source, &f.d.Target} {
				entry.ActivityURL = "http://" + entry.Deployment.Name + "." + entry.Deployment.Namespace + ".svc.cluster.local:8080/healthz"
			}
			switch scenario {
			case "foreign host":
				f.d.Source.ActivityURL = "http://foreign.example:8080/healthz"
			case "wrong port":
				f.d.Source.ActivityURL = strings.Replace(f.d.Source.ActivityURL, ":8080", ":8001", 1)
			case "short drain":
				grace := int64(30)
				f.d.Source.Deployment.Spec.Template.Spec.TerminationGracePeriodSeconds = &grace
			}
			s.client.Transport = maintenanceRoundTrip(func(req *http.Request) (*http.Response, error) {
				f.calls++
				if req.Method != "GET" || req.URL.Host != "10.0.0.9:8080" || req.Header.Get("Authorization") != "Bearer "+strings.Repeat("test-only-", 5) || req.Header.Get("X-Organization-Id") != f.b.OrganizationID || req.Header.Get("X-User-Id") != f.b.UserID || req.Header.Get("X-Station-Id") != f.b.StationID {
					t.Fatal("request identity changed")
				}
				switch req.URL.Path {
				case "/healthz":
					if scenario == "unhealthy" {
						return proofResponse(503, ""), nil
					}
					if scenario == "invalid health" {
						return proofResponse(200, `{"status":"starting"}`), nil
					}
					if scenario == "duplicate status" {
						return proofResponse(200, `{"status":"starting","status":"ready"}`), nil
					}
					return proofResponse(200, `{"status":"ready"}`), nil
				case "/internal/dashboard/declaration":
					if scenario == "auth denied" {
						return proofResponse(403, ""), nil
					}
					version := f.b.Source.AppVersion
					if scenario == "wrong version" {
						version = "9.9.9"
					}
					return proofResponse(200, `{"appId":"network","appVersion":"`+version+`","dashboard":{"artifactDigest":"`+f.b.Source.DashboardDigest+`"}}`), nil
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
					t.Fatal("network check failed", err, f.calls)
				}
			} else if err == nil {
				t.Fatal("invalid evidence accepted")
			}
			if (scenario == "foreign host" || scenario == "wrong port" || scenario == "short drain") && f.calls != 0 {
				t.Fatal("credential sent before validation")
			}
		})
	}
}
