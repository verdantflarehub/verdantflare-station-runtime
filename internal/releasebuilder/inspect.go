package releasebuilder

import (
	"encoding/hex"
	"os"
	"path/filepath"
	"sort"

	"github.com/verdantflarehub/verdantflare-station-runtime/internal/executor"
)

type TargetSummary struct {
	AppID          string `json:"appId"`
	AppVersion     string `json:"appVersion"`
	TemplateDigest string `json:"templateDigest"`
}

// InspectTargets performs the same target loading/policy checks as Build without
// writing files or claiming release trust.
func InspectTargets(rootPath, registryRef string) ([]TargetSummary, error) {
	root, err := os.OpenRoot(rootPath)
	if err != nil {
		return nil, ErrInvalid
	}
	defer root.Close()
	if _, err = read(root, registryRef, 2<<20); err != nil {
		return nil, ErrInvalid
	}
	targets, err := executor.LoadRegistry(rootPath, filepath.Join(rootPath, filepath.FromSlash(registryRef)))
	if err != nil {
		return nil, ErrInvalid
	}
	out := []TargetSummary{}
	for _, target := range targets {
		if executor.ValidatePreinstalledTarget(target) != nil {
			return nil, ErrInvalid
		}
		out = append(out, TargetSummary{target.AppID, target.Version, "sha256:" + hex.EncodeToString(target.Hash[:])})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].AppID < out[j].AppID })
	return out, nil
}
