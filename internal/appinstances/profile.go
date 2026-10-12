// Package appinstances implements installation-bound workers independently of
// the legacy Blender workspace preparation protocol.
package appinstances

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"regexp"
	"strings"

	"github.com/verdantflarehub/verdantflare-station-runtime/internal/strictjson"
	"k8s.io/apimachinery/pkg/util/validation"
)

var ErrInvalid = errors.New("INVALID_INSTANCE_PROFILE")
var ErrBinding = errors.New("INSTANCE_RESOURCE_BINDING_CONFLICT")
var ErrBusy = errors.New("INSTANCE_RESOURCE_IN_USE")
var ErrCapacity = errors.New("INSTANCE_STORAGE_CAPACITY_UNAVAILABLE")

const TemplateVersion = "app-worker-cpu-v1"

type Profile struct {
	AppID              string            `json:"app_id"`
	AppVersion         string            `json:"app_version"`
	ID                 string            `json:"profile_id"`
	TemplateVersion    string            `json:"template_version"`
	Namespace          string            `json:"namespace"`
	NodeName           string            `json:"node_name"`
	Pool               string            `json:"pool"`
	Claims             []string          `json:"claims"`
	StorageBytes       int64             `json:"storage_bytes"`
	Image              string            `json:"image"`
	Port               int32             `json:"port"`
	RunAsUser          int64             `json:"run_as_user"`
	CPURequestMillis   int64             `json:"cpu_request_millis"`
	CPULimitMillis     int64             `json:"cpu_limit_millis"`
	MemoryRequestBytes int64             `json:"memory_request_bytes"`
	MemoryLimitBytes   int64             `json:"memory_limit_bytes"`
	Env                map[string]string `json:"env"`
	Bindings           map[string]string `json:"bindings"`
}

var version = regexp.MustCompile(`^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$`)
var imageVersion = regexp.MustCompile(`^[a-z0-9][a-z0-9./:_-]*:(?:[a-z0-9][a-z0-9._-]*-)?v?(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)$`)
var envName = regexp.MustCompile(`^[A-Z_][A-Z0-9_]{0,127}$`)

func label(s string) bool     { return s != "" && len(validation.IsDNS1123Label(s)) == 0 }
func (p Profile) Key() string { return p.AppID + "@" + p.AppVersion + "/" + p.ID }
func (p Profile) Hash() string {
	raw, _ := json.Marshal(p)
	sum := sha256.Sum256(raw)
	const hex = "0123456789abcdef"
	out := make([]byte, 64)
	for i, b := range sum {
		out[i*2] = hex[b>>4]
		out[i*2+1] = hex[b&15]
	}
	return string(out)
}
func (p Profile) Valid() bool {
	if !label(p.AppID) || !version.MatchString(p.AppVersion) || !label(p.ID) || p.TemplateVersion != TemplateVersion ||
		!label(p.Namespace) || p.Namespace == "default" || strings.HasPrefix(p.Namespace, "kube-") ||
		p.NodeName == "" || len(validation.IsDNS1123Subdomain(p.NodeName)) != 0 || !label(p.Pool) || len(p.Claims) == 0 || len(p.Claims) > 1000 ||
		p.StorageBytes < 1 || p.StorageBytes > 1<<50 || !imageVersion.MatchString(p.Image) || len(p.Image) > 512 || p.Port < 1024 || p.Port > 65535 ||
		p.RunAsUser < 1 || p.RunAsUser > 2147483647 || !optionalResourcePair(p.CPURequestMillis, p.CPULimitMillis, 1, 1048576) ||
		!optionalResourcePair(p.MemoryRequestBytes, p.MemoryLimitBytes, 1<<20, 1<<50) ||
		p.Env == nil || len(p.Env) > 64 || len(p.Bindings) != 8 {
		return false
	}
	seen := map[string]bool{}
	for _, claim := range p.Claims {
		if claim == "" || len(validation.IsDNS1123Subdomain(claim)) != 0 || seen[claim] {
			return false
		}
		seen[claim] = true
	}
	sources := map[string]bool{}
	for key, value := range p.Bindings {
		if !envName.MatchString(key) || key == "NVIDIA_VISIBLE_DEVICES" || sources[value] {
			return false
		}
		switch value {
		case "instance_id", "start_operation_id", "generation", "pod_uid", "pod_name", "namespace", "container", "worker_token":
		default:
			return false
		}
		if _, exists := p.Env[key]; exists {
			return false
		}
		sources[value] = true
	}
	if !sources["instance_id"] || !sources["worker_token"] {
		return false
	}
	for key, value := range p.Env {
		if !envName.MatchString(key) || key == "NVIDIA_VISIBLE_DEVICES" || len(value) > 4096 || strings.ContainsAny(value, "\x00\r\n") ||
			strings.Contains(key, "TOKEN") || strings.Contains(key, "PASSWORD") || strings.Contains(key, "SECRET") || strings.Contains(key, "CREDENTIAL") {
			return false
		}
	}
	return true
}

// Omission is canonical zero, never an implicit request or limit. Keep the
// existing profile JSON encoding unchanged for durable legacy identities.
func optionalResourcePair(request, limit, minimum, maximum int64) bool {
	return request == 0 && limit == 0 || request >= minimum && limit >= request && limit <= maximum
}

func LoadProfiles(reader io.Reader) (map[string]Profile, error) {
	// The wire shape permits absent resource fields. The persisted Profile
	// retains its original numeric encoding, including explicit zero values.
	var entries []struct {
		Profile
		CPURequestMillis   *int64 `json:"cpu_request_millis,omitempty"`
		CPULimitMillis     *int64 `json:"cpu_limit_millis,omitempty"`
		MemoryRequestBytes *int64 `json:"memory_request_bytes,omitempty"`
		MemoryLimitBytes   *int64 `json:"memory_limit_bytes,omitempty"`
	}
	if strictjson.Decode(reader, 1<<20, &entries) != nil || len(entries) == 0 || len(entries) > 128 {
		return nil, ErrInvalid
	}
	result := map[string]Profile{}
	for _, entry := range entries {
		p := entry.Profile
		for _, field := range []struct {
			target *int64
			value  *int64
		}{
			{&p.CPURequestMillis, entry.CPURequestMillis}, {&p.CPULimitMillis, entry.CPULimitMillis},
			{&p.MemoryRequestBytes, entry.MemoryRequestBytes}, {&p.MemoryLimitBytes, entry.MemoryLimitBytes},
		} {
			if field.value != nil {
				*field.target = *field.value
			}
		}
		if !p.Valid() {
			return nil, ErrInvalid
		}
		if _, exists := result[p.Key()]; exists {
			return nil, ErrInvalid
		}
		result[p.Key()] = p
	}
	return result, nil
}
