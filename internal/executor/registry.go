package executor

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"regexp"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	"sigs.k8s.io/yaml"
)

type Target struct {
	ExpectedUID      string
	ActivityURL      string
	ActivityTokenEnv string
	AppID, Version   string
	Deployment       *appsv1.Deployment
	Services         []*corev1.Service
	Models           []string
	GPU              int
	DiskBytes        int64
	Hash             [32]byte
}
type registryEntry struct {
	ActivityURL      string `json:"activity_url,omitempty"`
	ActivityTokenEnv string `json:"activity_token_env,omitempty"`
	ManifestFile     string `json:"manifest_file"`
	Namespace        string `json:"namespace"`
}
type manifest struct {
	AppID   string `json:"app_id"`
	Version string `json:"version"`
	Models  []struct {
		ID      string `json:"model_id"`
		Version string `json:"version"`
	} `json:"required_models"`
	Resources *struct {
		GPU  int   `json:"gpu"`
		Disk int64 `json:"disk_bytes"`
	} `json:"required_resources"`
	Deployment struct {
		Template string `json:"template_ref"`
		Name     string `json:"workload_name"`
		Images   []struct {
			Component string `json:"component"`
			Image     string `json:"image"`
		} `json:"images"`
	} `json:"deployment"`
}

var namePattern = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)
var versionPattern = regexp.MustCompile(`^[0-9]+\.[0-9]+\.[0-9]+$`)
var imagePattern = regexp.MustCompile(`^\S+:(?:[a-z0-9][a-z0-9._-]*-)?v?[0-9]+\.[0-9]+\.[0-9]+(@sha256:[a-f0-9]{64})?$`)

func validName(s string) bool { return len(s) > 0 && len(s) <= 63 && namePattern.MatchString(s) }
func fileInRoot(root, ref string) ([]byte, error) {
	if filepath.IsAbs(ref) || ref == "" {
		return nil, errors.New("template reference must be relative")
	}
	root, err := filepath.EvalSymlinks(root)
	if err != nil {
		return nil, err
	}
	root, err = filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	path, err := filepath.EvalSymlinks(filepath.Join(root, ref))
	if err != nil {
		return nil, err
	}
	rel, err := filepath.Rel(root, path)
	if err != nil || rel == ".." || len(rel) > 3 && rel[:3] == "../" {
		return nil, errors.New("template outside registered root")
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, 4<<20+1))
	if err != nil || len(b) > 4<<20 {
		return nil, errors.New("template too large")
	}
	return b, nil
}

// LoadRegistry reads central manifests/templates once; requests cannot override these inputs.
func LoadRegistry(root, path string) (map[string]Target, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var entries []registryEntry
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err = dec.Decode(&entries); err != nil {
		return nil, err
	}
	if dec.Decode(new(any)) != io.EOF || len(entries) == 0 {
		return nil, errors.New("invalid registry")
	}
	targets := map[string]Target{}
	workloads := map[string]bool{}
	for _, e := range entries {
		if !validName(e.Namespace) || e.Namespace == "default" {
			return nil, errors.New("invalid namespace")
		}
		b, err := fileInRoot(root, e.ManifestFile)
		if err != nil {
			return nil, err
		}
		strict, err := yaml.YAMLToJSONStrict(b)
		if err != nil {
			return nil, err
		}
		var m manifest
		if json.Unmarshal(strict, &m) != nil || !validName(m.AppID) || !versionPattern.MatchString(m.Version) || m.Models == nil || m.Resources == nil || m.Resources.GPU < 0 || m.Resources.Disk < 0 || !validName(m.Deployment.Name) || len(m.Deployment.Images) == 0 {
			return nil, errors.New("invalid application manifest")
		}
		template, err := fileInRoot(root, m.Deployment.Template)
		if err != nil {
			return nil, err
		}
		if (e.ActivityURL == "") != (e.ActivityTokenEnv == "") {
			return nil, errors.New("activity endpoint and credential reference must be paired")
		}
		if e.ActivityURL != "" {
			u, err := url.Parse(e.ActivityURL)
			if err != nil || u.Scheme != "http" || u.Hostname() != m.Deployment.Name+"."+e.Namespace+".svc.cluster.local" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Path != "/api/dashboard" || !regexp.MustCompile(`^STATION_APP_[A-Z0-9_]+$`).MatchString(e.ActivityTokenEnv) {
				return nil, errors.New("invalid registered activity endpoint")
			}
		}
		t := Target{ActivityURL: e.ActivityURL, ActivityTokenEnv: e.ActivityTokenEnv, AppID: m.AppID, Version: m.Version, Models: []string{}, GPU: m.Resources.GPU, DiskBytes: m.Resources.Disk}
		for _, model := range m.Models {
			t.Models = append(t.Models, model.ID+"@"+model.Version)
		}
		decoder := utilyaml.NewYAMLOrJSONDecoder(bytes.NewReader(template), 4096)
		for {
			var raw json.RawMessage
			err = decoder.Decode(&raw)
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, err
			}
			if len(raw) == 0 || string(raw) == "null" {
				continue
			}
			var head struct {
				Kind       string `json:"kind"`
				APIVersion string `json:"apiVersion"`
			}
			if json.Unmarshal(raw, &head) != nil {
				return nil, errors.New("invalid template")
			}
			switch head.Kind {
			case "Deployment":
				var d appsv1.Deployment
				if json.Unmarshal(raw, &d) != nil {
					return nil, errors.New("invalid deployment")
				}
				if d.Name != m.Deployment.Name {
					continue
				}
				if head.APIVersion != "apps/v1" || d.Namespace != e.Namespace || t.Deployment != nil || len(d.Spec.Template.Spec.Containers) == 0 {
					return nil, errors.New("deployment target mismatch")
				}
				t.Deployment = &d
			case "Service":
				var svc corev1.Service
				if json.Unmarshal(raw, &svc) != nil || head.APIVersion != "v1" || svc.Namespace != e.Namespace {
					return nil, errors.New("service target mismatch")
				}
				t.Services = append(t.Services, &svc)
			}
		}
		if t.Deployment == nil {
			return nil, errors.New("registered deployment not found")
		}
		if t.Deployment.Spec.Replicas == nil || *t.Deployment.Spec.Replicas == 0 {
			one := int32(1)
			t.Deployment.Spec.Replicas = &one
		}
		images := map[string]string{}
		for _, im := range m.Deployment.Images {
			if !imagePattern.MatchString(im.Image) || images[im.Component] != "" {
				return nil, errors.New("invalid pinned image")
			}
			images[im.Component] = im.Image
		}
		containers := append(append([]corev1.Container{}, t.Deployment.Spec.Template.Spec.Containers...), t.Deployment.Spec.Template.Spec.InitContainers...)
		if len(images) != len(containers) {
			return nil, errors.New("all container images must be pinned")
		}
		for _, c := range containers {
			if c.Resources.Requests.Cpu().Sign() < 0 || c.Resources.Limits.Cpu().Sign() < 0 {
				return nil, errors.New("invalid resources")
			}
			for _, resources := range []corev1.ResourceList{c.Resources.Limits, c.Resources.Requests} {
				for resource, quantity := range resources {
					if (string(resource) == "nvidia.com/gpu" || string(resource) == "amd.com/gpu") && !quantity.IsZero() && m.Resources.GPU == 0 {
						return nil, errors.New("GPU workload missing manifest GPU requirement")
					}
				}
			}
			if images[c.Name] != c.Image {
				return nil, errors.New("manifest/template image mismatch")
			}
		}
		for _, svc := range t.Services {
			if len(svc.Spec.Selector) == 0 {
				return nil, errors.New("service must select registered workload")
			}
			for k, v := range svc.Spec.Selector {
				if t.Deployment.Spec.Template.Labels[k] != v {
					return nil, errors.New("service selects another workload")
				}
			}
		}
		key := m.AppID + "@" + m.Version
		workload := e.Namespace + "/" + m.Deployment.Name
		if _, ok := targets[key]; ok || workloads[workload] {
			return nil, fmt.Errorf("duplicate registered target")
		}
		workloads[workload] = true
		hashInput, _ := json.Marshal(struct {
			Manifest   manifest
			Deployment *appsv1.Deployment
			Services   []*corev1.Service
		}{m, t.Deployment, t.Services})
		t.Hash = sha256.Sum256(hashInput)
		targets[key] = t
	}
	return targets, nil
}
