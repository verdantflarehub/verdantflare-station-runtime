package executor

import (
	"errors"
	"strings"
	"testing"
)

func TestImageStoppedEvidence(t *testing.T) {
	for _, version := range []string{"0.2.16", "0.2.18", "0.2.20", "0.2.21", "0.2.22", "0.2.23", "0.2.24"} {
		t.Run(version, func(t *testing.T) {
			for _, scenario := range []string{"idle", "queued", "unknown state", "missing count", "negative", "fraction", "duplicate", "overflow", "wrong version", "wrong service", "changed page", "transport"} {
				t.Run(scenario, func(t *testing.T) {
					html := []byte("<!doctype html><title>Image</title>")
					release := ManagementRelease{AppID: "image-mcp", AppVersion: version, DashboardDigest: imageDashboardDigest(html)}
					stats := `{"total":925,"queued":0,"running":0,"completed":833,"failed":92,"canceled":0}`
					health := `{"status":"ok","service":"image-mcp-server","version":"v` + version + `"}`
					switch scenario {
					case "queued":
						stats = strings.ReplaceAll(strings.ReplaceAll(stats, `"queued":0`, `"queued":1`), `"total":925`, `"total":926`)
					case "unknown state":
						stats = strings.ReplaceAll(stats, `"total":925`, `"total":926`)
					case "missing count":
						stats = strings.ReplaceAll(stats, `,"canceled":0`, "")
					case "negative":
						stats = strings.ReplaceAll(stats, `"running":0`, `"running":-1`)
					case "fraction":
						stats = strings.ReplaceAll(stats, `"running":0`, `"running":0.5`)
					case "duplicate":
						stats = strings.ReplaceAll(stats, `"running":0`, `"running":1,"running":0`)
					case "overflow":
						stats = strings.ReplaceAll(stats, `"total":925`, `"total":9223372036854775807`)
					case "wrong version":
						health = strings.ReplaceAll(health, "v"+version, "v0.0.0")
					case "wrong service":
						health = strings.ReplaceAll(health, "image-mcp-server", "other")
					case "changed page":
						html = append(html, ' ')
					}
					busy, err := imageStoppedEvidence(func(path string) ([]byte, error) {
						if scenario == "transport" {
							return nil, ErrManagementMaintenanceUnavailable
						}
						switch path {
						case "/api/tasks/stats":
							return []byte(stats), nil
						case "/health":
							return []byte(health), nil
						case "/dashboard":
							return html, nil
						}
						t.Fatal("unexpected endpoint", path)
						return nil, ErrManagementReplacement
					}, release)
					if scenario == "idle" || scenario == "queued" {
						if err != nil || busy != (scenario == "queued") {
							t.Fatal(busy, err)
						}
					} else if err == nil {
						t.Fatal("invalid evidence accepted")
					}
					if scenario == "transport" && !errors.Is(err, ErrManagementMaintenanceUnavailable) {
						t.Fatal("transport failure lost")
					}
				})
			}

		})
	}
}
