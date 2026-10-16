package releasebuilder

import (
	"bytes"
	"encoding/json"
	"os"
	"regexp"

	"github.com/verdantflarehub/verdantflare-station-runtime/internal/executor"
	"github.com/verdantflarehub/verdantflare-station-runtime/internal/strictjson"
)

const provenanceKind = "verdantflare.preinstalled.provenance"

type materialRef struct {
	Ref    string `json:"ref"`
	Digest string `json:"digest"`
}

type provenance struct {
	SchemaVersion int         `json:"schemaVersion"`
	Kind          string      `json:"kind"`
	AppID         string      `json:"appId"`
	AppVersion    string      `json:"appVersion"`
	Manifest      materialRef `json:"manifest"`
	Delivery      materialRef `json:"delivery"`
	Declaration   materialRef `json:"declaration"`
	Index         materialRef `json:"index"`
	Navigation    materialRef `json:"navigation"`
}

type delivery struct {
	SchemaVersion int    `json:"schemaVersion"`
	AppID         string `json:"appId"`
	AppVersion    string `json:"appVersion"`
	Image         string `json:"image"`
	ImageDigest   string `json:"imageDigest"`
	Repository    string `json:"repository"`
	Revision      string `json:"revision"`
}

var materialDigest = regexp.MustCompile(`^sha256:[a-f0-9]{64}$`)
var sourceRevision = regexp.MustCompile(`^[a-f0-9]{40}$`)

// Image predates the app repository naming convention; preserve its real source.
func sourceRepository(appID string) string {
	if appID == "image-mcp" {
		return "verdantflarehub/verdantflare-app-image"
	}
	return "verdantflarehub/verdantflare-app-" + appID
}

// Provenance is specific to the preinstalled delivery slice; the existing v1
// Runtime manifest and shared release binding remain unchanged. All original
// bytes referenced by the package must still exist before signing.
func verifyProvenance(root *os.Root, raw []byte, target executor.Target) error {
	var p provenance
	if strictjson.Decode(bytes.NewReader(raw), 4<<20, &p) != nil || p.SchemaVersion != 1 || p.Kind != provenanceKind || p.AppID != target.AppID || p.AppVersion != target.Version || p.Manifest.Ref != target.ManifestRef {
		return ErrInvalid
	}
	refs := []materialRef{p.Manifest, p.Delivery, p.Declaration, p.Index, p.Navigation}
	seen := map[string]bool{}
	for i, ref := range refs {
		limit := int64(64 << 10)
		if i == 0 {
			limit = 4 << 20
		}
		if seen[ref.Ref] || !materialDigest.MatchString(ref.Digest) {
			return ErrInvalid
		}
		seen[ref.Ref] = true
		content, err := read(root, ref.Ref, limit)
		if err != nil || digest(content) != ref.Digest {
			return ErrInvalid
		}
		// Runtime manifests may use YAML. The other original materials are JSON;
		// their consumer-level closed shapes are checked by the preparation tool.
		if i != 0 && !json.Valid(content) {
			return ErrInvalid
		}
		if i == 1 {
			var d delivery
			if strictjson.Decode(bytes.NewReader(content), 64<<10, &d) != nil || d.SchemaVersion != 1 || d.AppID != p.AppID || d.AppVersion != p.AppVersion || !materialDigest.MatchString(d.ImageDigest) || !sourceRevision.MatchString(d.Revision) || d.Repository != sourceRepository(p.AppID) || len(target.Deployment.Spec.Template.Spec.Containers) != 1 || len(target.Deployment.Spec.Template.Spec.InitContainers) != 0 || d.Image != target.Deployment.Spec.Template.Spec.Containers[0].Image {
				return ErrInvalid
			}
		}
	}
	return nil
}
