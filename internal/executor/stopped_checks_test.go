package executor

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"strings"
	"testing"
)

func stoppedChecksFixture(t *testing.T) (*maintenanceFixture, *stoppedHTTPChecks, []byte) {
	return stoppedChecksFixtureFor(t, "higgsfield")
}

func stoppedChecksFixtureFor(t *testing.T, app string) (*maintenanceFixture, *stoppedHTTPChecks, []byte) {
	t.Helper()
	f := newMaintenanceFixtureFor(t, func(source, target *Target) {
		source.AppID = app
		target.AppID = app
		if app == "network" {
			for _, entry := range []*Target{source, target} {
				grace := int64(70)
				entry.Deployment.Spec.Template.Spec.TerminationGracePeriodSeconds = &grace
			}
		}
	})
	f.pod.Status.PodIP = "10.0.0.9"
	f.savePod(t)
	index := []byte(`{"schemaVersion":1,"files":{"index.html":"test"}}`)
	hash := sha256.Sum256(index)
	for _, release := range []*ManagementRelease{&f.b.Source, &f.b.Target} {
		release.ReleaseKind = "dashboard-release"
		release.AppImageDigest = "sha256:" + strings.Repeat("a", 64)
		release.ManifestDigest = release.ReleaseDigest
		release.DataContract = ""
		release.DefaultWorkerVersion = ""
		release.CompatibleWorkerVersions = []string{}
		release.DashboardDigest = "sha256:" + hex.EncodeToString(hash[:])
	}
	for _, target := range []*Target{&f.d.Source, &f.d.Target} {
		target.ActivityURL = "http://" + target.Deployment.Name + "." + target.Deployment.Namespace + ".svc.cluster.local:8001/limits"
		target.ActivityTokenEnv = "STATION_APP_TEST_TOKEN"
	}
	t.Setenv("STATION_APP_TEST_TOKEN", strings.Repeat("test-only-", 5))
	checker := newStoppedChecks()
	checker.client.Transport = maintenanceRoundTrip(func(req *http.Request) (*http.Response, error) {
		if req.Method != "GET" || req.URL.Host != "10.0.0.9:8001" || req.Header.Get("X-Organization-Id") != f.b.OrganizationID || req.Header.Get("X-Station-Id") != f.b.StationID || req.Header.Get("X-User-Id") != f.b.UserID || req.Header.Get("Authorization") != "Bearer "+strings.Repeat("test-only-", 5) {
			t.Fatal("request binding changed")
		}
		f.calls++
		switch req.URL.Path {
		case "/limits":
			return proofResponse(200, `{"active_count":0,"submitted_count":3,"binding_id":"fixture"}`), nil
		case "/internal/dashboard-declaration":
			return proofResponse(200, `{"appId":"higgsfield","appVersion":"`+f.b.Source.AppVersion+`","dashboard":{"artifactDigest":"`+f.b.Source.DashboardDigest+`"}}`), nil
		case "/internal/dashboard-index":
			return proofResponse(200, string(index)), nil
		default:
			t.Fatal("unexpected management call")
			return nil, ErrManagementReplacement
		}
	})
	return f, checker, index
}

func TestStoppedChecksUseExistingReadOnlyPaths(t *testing.T) {
	f, s, _ := stoppedChecksFixture(t)
	if err := s.Check(context.Background(), f.b, &StoppedReplacement{f.d}, f.c, false, true); err != nil || f.calls != 3 {
		t.Fatal("read-only check", err, f.calls)
	}
}

func TestStoppedChecksRejectInvalidImageBeforeSendingCredential(t *testing.T) {
	f, s, _ := stoppedChecksFixture(t)
	f.pod.Status.ContainerStatuses[0].ImageID = "repo@sha256:" + strings.Repeat("b", 64)
	f.savePod(t)
	if err := s.Check(context.Background(), f.b, &StoppedReplacement{f.d}, f.c, false, true); !errors.Is(err, ErrManagementReplacement) || f.calls != 0 {
		t.Fatal("credential sent to wrong image", err, f.calls)
	}
}

func TestStoppedChecksReconcileBusyAndRejectMalformedEvidence(t *testing.T) {
	for _, scenario := range []string{"busy", "negative", "missing count", "duplicate count", "wrong dashboard", "restart", "source recovery"} {
		t.Run(scenario, func(t *testing.T) {
			f, s, _ := stoppedChecksFixture(t)
			original := s.client.Transport
			s.client.Transport = maintenanceRoundTrip(func(req *http.Request) (*http.Response, error) {
				if req.URL.Path == "/limits" {
					switch scenario {
					case "busy", "source recovery":
						return proofResponse(200, `{"active_count":1,"submitted_count":3,"binding_id":"fixture"}`), nil
					case "negative":
						return proofResponse(200, `{"active_count":-1,"submitted_count":3,"binding_id":"fixture"}`), nil
					case "missing count":
						return proofResponse(200, `{"submitted_count":3,"binding_id":"fixture"}`), nil
					case "duplicate count":
						return proofResponse(200, `{"active_count":0,"active_count":1,"submitted_count":3,"binding_id":"fixture"}`), nil
					}
				}
				if req.URL.Path == "/internal/dashboard-index" {
					if scenario == "wrong dashboard" {
						return proofResponse(200, `{}`), nil
					}
					if scenario == "restart" {
						f.pod.Status.ContainerStatuses[0].RestartCount++
						f.savePod(t)
					}
				}
				return original.RoundTrip(req)
			})
			err := s.Check(context.Background(), f.b, &StoppedReplacement{f.d}, f.c, false, scenario != "source recovery")
			if scenario == "source recovery" {
				if err != nil {
					t.Fatal(err)
				}
			} else if scenario == "busy" {
				if !errors.Is(err, ErrManagementInstallationBusy) {
					t.Fatal(err)
				}
			} else if err == nil {
				t.Fatal("invalid evidence accepted")
			}
		})
	}
}
