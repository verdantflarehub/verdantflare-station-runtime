package executor

import (
	"encoding/hex"
	"os"
	"regexp"
	"slices"

	"github.com/verdantflarehub/verdantflare-station-runtime/internal/strictjson"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
)

type maintenanceRegistration struct {
	Snapshot ManagementRelease `json:"snapshot"`
	Images   map[string]string `json:"images"`
	Service  string            `json:"service"`
	Port     int32             `json:"port"`
	TokenEnv string            `json:"maintenance_token_env"`
}

var managementTokenEnv = regexp.MustCompile(`^[A-Z][A-Z0-9_]{0,127}$`)
var managementReleaseName = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,127}$`)
var managementVersion = regexp.MustCompile(`^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$`)

func validManagementRelease(s ManagementRelease) bool {
	if !managementReleaseName.MatchString(s.AppID) || !managementVersion.MatchString(s.AppVersion) || !managementReleaseName.MatchString(s.ReleaseRef) || !managementReleaseName.MatchString(s.DataContract) ||
		len(s.CompatibleWorkerVersions) == 0 || len(s.CompatibleWorkerVersions) > 128 || !slices.Contains(s.CompatibleWorkerVersions, s.DefaultWorkerVersion) {
		return false
	}
	for _, digest := range []string{s.ReleaseDigest, s.ManifestDigest, s.TemplateDigest, s.DashboardDigest} {
		if !managementDigest.MatchString(digest) {
			return false
		}
	}
	seen := map[string]bool{}
	for _, version := range s.CompatibleWorkerVersions {
		if len(version) > 64 || !managementVersion.MatchString(version) || seen[version] {
			return false
		}
		seen[version] = true
	}
	return len(s.AppVersion) <= 64
}

// The platform verifies publication provenance before mounting this registry.
// Runtime then closes the snapshot/template/image/service/credential binding.
func LoadManagementMaintenance(path, controlToken string, lookup func(string) string, k *Kubernetes, targets map[string]Target) (*HTTPManagementMaintenance, error) {
	if path == "" && controlToken == "" {
		return nil, nil
	}
	if path == "" || lookup == nil || k == nil || k.Client == nil || !validManagementToken(controlToken) {
		return nil, ErrManagementReplacement
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, ErrManagementReplacement
	}
	defer f.Close()
	var registry struct {
		SchemaVersion int                       `json:"schema_version"`
		Releases      []maintenanceRegistration `json:"releases"`
	}
	if strictjson.Decode(f, 1<<20, &registry) != nil || registry.SchemaVersion != 1 || len(registry.Releases) == 0 || len(registry.Releases) > 256 {
		return nil, ErrManagementReplacement
	}
	releases := map[string]ManagementMaintenanceRelease{}
	versions, refs := map[string]bool{}, map[string]bool{}
	tokenApps := map[string]string{}
	for _, entry := range registry.Releases {
		s := entry.Snapshot
		target, exists := targets[s.AppID+"@"+s.AppVersion]
		if !exists || target.Deployment == nil || !oneReplica(target.Deployment) || len(target.InstallationBindings) == 0 || !validManagementRelease(s) || s.TemplateDigest != "sha256:"+hex.EncodeToString(target.Hash[:]) || !validName(entry.Service) || entry.Port < 1 || entry.Port > 65535 || !managementTokenEnv.MatchString(entry.TokenEnv) ||
			versions[s.AppID+"@"+s.AppVersion] || refs[s.ReleaseRef] {
			return nil, ErrManagementReplacement
		}
		if _, exists := releases[s.ReleaseDigest]; exists {
			return nil, ErrManagementReplacement
		}
		versions[s.AppID+"@"+s.AppVersion], refs[s.ReleaseRef] = true, true
		token := lookup(entry.TokenEnv)
		if !validManagementToken(token) || token == controlToken || (tokenApps[token] != "" && tokenApps[token] != s.AppID) {
			return nil, ErrManagementReplacement
		}
		tokenApps[token] = s.AppID
		containers := append([]corev1.Container{}, target.Deployment.Spec.Template.Spec.Containers...)
		containers = append(containers, target.Deployment.Spec.Template.Spec.InitContainers...)
		if len(entry.Images) != len(containers) || len(containers) == 0 {
			return nil, ErrManagementReplacement
		}
		for _, container := range containers {
			if !managementDigest.MatchString(entry.Images[container.Name]) {
				return nil, ErrManagementReplacement
			}
		}
		found := false
		for _, service := range target.Services {
			if service == nil || service.Name != entry.Service || service.Namespace != target.Deployment.Namespace || len(service.Spec.Selector) == 0 || !labels.SelectorFromSet(service.Spec.Selector).Matches(labels.Set(target.Deployment.Spec.Template.Labels)) {
				continue
			}
			for _, port := range service.Spec.Ports {
				if port.Port == entry.Port && (port.Protocol == "" || port.Protocol == corev1.ProtocolTCP) {
					found = true
				}
			}
		}
		if !found {
			return nil, ErrManagementReplacement
		}
		releases[s.ReleaseDigest] = ManagementMaintenanceRelease{Snapshot: s, Images: entry.Images, Service: entry.Service, Port: entry.Port, Token: token}
	}
	return NewHTTPManagementMaintenance(k, targets, releases), nil
}
