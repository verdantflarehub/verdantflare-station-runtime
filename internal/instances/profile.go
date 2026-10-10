package instances

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"io"
	"os"
)

type profileConfig struct {
	ID           string   `json:"profile_id"`
	Namespace    string   `json:"namespace"`
	Pool         string   `json:"pool"`
	NodeName     string   `json:"node_name"`
	Claims       []string `json:"claims"`
	StorageBytes int64    `json:"storage_bytes"`
	Image        string   `json:"worker_image"`
	MaxFileBytes int64    `json:"max_file_bytes"`
}

func LoadProfiles(path string) (map[string]HelperProfile, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if len(data) > 1<<20 {
		return nil, ErrInvalid
	}
	var entries []profileConfig
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if d.Decode(&entries) != nil || d.Decode(new(any)) != io.EOF || len(entries) == 0 {
		return nil, ErrInvalid
	}
	out := map[string]HelperProfile{}
	for _, entry := range entries {
		raw, _ := json.Marshal(struct {
			Config   profileConfig `json:"config"`
			Template string        `json:"worker_template"`
		}{entry, workerTemplateVersion})
		p := HelperProfile{StorageProfile: StorageProfile{ID: entry.ID, Namespace: entry.Namespace, Pool: entry.Pool, NodeName: entry.NodeName, Claims: entry.Claims, Bytes: entry.StorageBytes, Hash: sha256.Sum256(raw)}, Image: entry.Image, MaxFileBytes: entry.MaxFileBytes}
		if !p.StorageProfile.valid() || !imageVersion.MatchString(p.Image) || p.MaxFileBytes < 1 || p.MaxFileBytes > p.Bytes {
			return nil, ErrInvalid
		}
		if _, exists := out[p.ID]; exists {
			return nil, ErrInvalid
		}
		out[p.ID] = p
	}
	return out, nil
}
