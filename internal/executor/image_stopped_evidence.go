package executor

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"github.com/verdantflarehub/verdantflare-station-runtime/internal/strictjson"
)

// Image's existing dashboard is a single HTML file. The release material uses
// this deterministic derived index; it does not pretend the old image served a
// dashboard declaration. The caller also verifies process identity and routing.
func imageDashboardDigest(html []byte) string {
	hash := sha256.Sum256(html)
	index := `{"files":{"index.html":"` + hex.EncodeToString(hash[:]) + `"},"schemaVersion":1}`
	digest := sha256.Sum256([]byte(index))
	return "sha256:" + hex.EncodeToString(digest[:])
}

func imageStoppedEvidence(get func(string) ([]byte, error), release ManagementRelease) (bool, error) {
	if release.AppID != "image-mcp" || (release.AppVersion != "0.2.16" && release.AppVersion != "0.2.17" && release.AppVersion != "0.2.18" && release.AppVersion != "0.2.20" && release.AppVersion != "0.2.21" && release.AppVersion != "0.2.22" && release.AppVersion != "0.2.23" && release.AppVersion != "0.2.24" && release.AppVersion != "0.2.25") {
		return false, ErrManagementReplacement
	}
	raw, err := get("/api/tasks/stats")
	if err != nil {
		return false, err
	}
	var stats struct {
		Total     *int64 `json:"total"`
		Queued    *int64 `json:"queued"`
		Running   *int64 `json:"running"`
		Completed *int64 `json:"completed"`
		Failed    *int64 `json:"failed"`
		Canceled  *int64 `json:"canceled"`
	}
	checked, err := strictjson.ReadValue(bytes.NewReader(raw), 1<<20)
	if err != nil || json.Unmarshal(checked, &stats) != nil || stats.Total == nil || *stats.Total < 0 || *stats.Total > 1<<53 {
		return false, ErrManagementReplacement
	}
	remaining := *stats.Total
	for _, count := range []*int64{stats.Queued, stats.Running, stats.Completed, stats.Failed, stats.Canceled} {
		if count == nil || *count < 0 || *count > remaining {
			return false, ErrManagementReplacement
		}
		remaining -= *count
	}
	if remaining != 0 {
		return false, ErrManagementReplacement
	}
	raw, err = get("/health")
	if err != nil {
		return false, err
	}
	var health struct {
		Status  string `json:"status"`
		Service string `json:"service"`
		Version string `json:"version"`
	}
	checked, err = strictjson.ReadValue(bytes.NewReader(raw), 1<<20)
	if err != nil || json.Unmarshal(checked, &health) != nil || health.Status != "ok" || health.Service != "image-mcp-server" || health.Version != "v"+release.AppVersion {
		return false, ErrManagementReplacement
	}
	html, err := get("/dashboard")
	if err != nil {
		return false, err
	}
	if len(html) == 0 || len(html) > 1<<20 || imageDashboardDigest(html) != release.DashboardDigest {
		return false, ErrManagementReplacement
	}
	return *stats.Queued != 0 || *stats.Running != 0, nil
}
