package appinstances

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"io"
	"mime"
	"net/http"
	"regexp"

	"github.com/google/uuid"
	"github.com/verdantflarehub/verdantflare-station-runtime/internal/strictjson"
)

type CoreAuthority struct {
	origin string
	token  string
	appID  string
	client *http.Client
}

type Caller struct {
	AppID     string
	Authority *CoreAuthority
}

type AccessRegistry map[[32]byte]Caller

// The inbound service token is stored only as a digest. The outbound Core token
// is a separate scoped credential and never appears in public projections.
func LoadAccessRegistry(reader io.Reader, lookup func(string) string, coreOrigin string) (AccessRegistry, error) {
	if lookup == nil || !internalOrigin(coreOrigin) {
		return nil, ErrInvalid
	}
	var entries []struct {
		AppID        string `json:"app_id"`
		TokenEnv     string `json:"token_env"`
		CoreTokenEnv string `json:"core_token_env"`
	}
	if strictjson.Decode(reader, 64<<10, &entries) != nil || len(entries) == 0 || len(entries) > 128 {
		return nil, ErrInvalid
	}
	seenApps, seenTokens := map[string]bool{}, map[[32]byte]bool{}
	out := AccessRegistry{}
	for _, entry := range entries {
		if !label(entry.AppID) || seenApps[entry.AppID] ||
			!regexp.MustCompile(`^STATION_APP_INSTANCE_[A-Z0-9_]+_TOKEN$`).MatchString(entry.TokenEnv) ||
			!regexp.MustCompile(`^STATION_APP_INSTALLATION_[A-Z0-9_]+_TOKEN$`).MatchString(entry.CoreTokenEnv) {
			return nil, ErrInvalid
		}
		incoming, outgoing := lookup(entry.TokenEnv), lookup(entry.CoreTokenEnv)
		if !credentialValid([]byte(incoming)) || !credentialValid([]byte(outgoing)) || incoming == outgoing {
			return nil, ErrInvalid
		}
		inHash, outHash := sha256.Sum256([]byte("Bearer "+incoming)), sha256.Sum256([]byte("Bearer "+outgoing))
		if seenTokens[inHash] || seenTokens[outHash] {
			return nil, ErrInvalid
		}
		out[inHash] = Caller{AppID: entry.AppID, Authority: &CoreAuthority{origin: coreOrigin, token: outgoing, appID: entry.AppID, client: InternalHTTPClient()}}
		seenApps[entry.AppID], seenTokens[inHash], seenTokens[outHash] = true, true, true
	}
	return out, nil
}

func (a *CoreAuthority) Check(ctx context.Context, in Run, p Profile) (int64, error) {
	if a == nil || !internalOrigin(a.origin) || !credentialValid([]byte(a.token)) || !in.Identity.valid() ||
		!p.Valid() || p.AppID != a.appID {
		return 0, ErrDenied
	}
	rid, e := uuid.NewV7()
	if e != nil {
		return 0, ErrUnavailable
	}
	body, _ := json.Marshal(map[string]string{"installation_id": in.InstallationID, "app_version": p.AppVersion})
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, a.origin+"/internal/v1/app-installations/authorize", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+a.token)
	req.Header.Set("X-User-Id", in.UserID)
	req.Header.Set("X-Organization-Id", in.OrganizationID)
	req.Header.Set("X-Station-Id", in.StationID)
	req.Header.Set("X-Request-Id", rid.String())
	resp, e := controlledRequest(ctx, a.client, req)
	if e != nil {
		return 0, ErrUnavailable
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusForbidden || resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusNotFound {
		return 0, ErrDenied
	}
	media, _, e := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	if resp.StatusCode != http.StatusOK || e != nil || media != "application/json" || resp.Header.Get("Content-Encoding") != "" || resp.Uncompressed {
		return 0, ErrUnavailable
	}
	var result struct {
		SchemaVersion        int    `json:"schema_version"`
		InstallationID       string `json:"installation_id"`
		AppID                string `json:"app_id"`
		AppVersion           string `json:"app_version"`
		StationID            string `json:"station_id"`
		OrganizationID       string `json:"organization_id"`
		UserID               string `json:"user_id"`
		InstallationRevision int64  `json:"installation_revision"`
		RequestID            string `json:"request_id"`
	}
	if strictjson.Decode(resp.Body, 16<<10, &result) != nil || result.SchemaVersion != 1 || result.InstallationRevision < 1 ||
		result.InstallationID != in.InstallationID || result.AppID != a.appID || result.AppVersion != p.AppVersion ||
		result.StationID != in.StationID || result.OrganizationID != in.OrganizationID || result.UserID != in.UserID || result.RequestID != rid.String() {
		return 0, ErrDenied
	}
	return result.InstallationRevision, nil
}
