package executor

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/yaml"
)

type OCIChartMeta struct {
	OCIRef         string
	ChartName      string
	ChartVersion   string
	AppVersion     string
	ManifestDigest string
	PackageDigest  string
	Description    string
	Values         map[string]any
	Package        []byte
}

type OCIChartSource struct {
	RegistryURLs []string
	HTTPClient   *http.Client

	mu    sync.Mutex
	cache map[string]Target
}

func NewOCIChartSourceFromEnv() *OCIChartSource {
	raw := strings.TrimSpace(os.Getenv("STATION_CHART_REGISTRY_URL"))
	var urls []string
	for _, part := range strings.Split(raw, ",") {
		u := strings.TrimRight(strings.TrimSpace(part), "/")
		if u != "" {
			urls = append(urls, u)
		}
	}
	for _, fallback := range []string{"https://registry.verdantflare.ai", "https://registry.verdantflarehub.com"} {
		if !containsString(urls, fallback) {
			urls = append(urls, fallback)
		}
	}
	return &OCIChartSource{
		RegistryURLs: urls,
		HTTPClient:   &http.Client{Timeout: 15 * time.Second},
		cache:        map[string]Target{},
	}
}

func containsString(list []string, target string) bool {
	for _, item := range list {
		if item == target {
			return true
		}
	}
	return false
}

func (s *OCIChartSource) Resolve(ctx context.Context, baseTargets map[string]Target, appID, appVersion, releaseDigest, templateDigest string) (Target, bool) {
	if s == nil || !validName(appID) || !versionPattern.MatchString(appVersion) || !appCenterDigest.MatchString(templateDigest) {
		return Target{}, false
	}
	if releaseDigest != "" && !appCenterDigest.MatchString(releaseDigest) {
		return Target{}, false
	}

	cacheKey := appID + "@" + appVersion + "@" + templateDigest
	s.mu.Lock()
	if s.cache != nil {
		if cached, ok := s.cache[cacheKey]; ok {
			s.mu.Unlock()
			return cloneTarget(cached), true
		}
	}
	s.mu.Unlock()

	base, ok := findBaseTarget(baseTargets, appID, appVersion)
	if !ok {
		return Target{}, false
	}

	meta, err := s.fetchAndVerifyChart(ctx, appID, releaseDigest, templateDigest)
	if err != nil {
		return Target{}, false
	}

	hashBytes, err := hex.DecodeString(strings.TrimPrefix(templateDigest, "sha256:"))
	if err != nil || len(hashBytes) != 32 {
		return Target{}, false
	}

	out := cloneTarget(base)
	out.AppID = appID
	out.Version = appVersion
	out.DiskBytes = 0
	out.Models = nil
	out.OCIChart = meta
	copy(out.Hash[:], hashBytes)

	chartTag := extractChartImageTag(meta.Values)
	if chartTag != "" && out.Deployment != nil {
		for i := range out.Deployment.Spec.Template.Spec.Containers {
			c := &out.Deployment.Spec.Template.Spec.Containers[i]
			if colon := strings.LastIndexByte(c.Image, ':'); colon > strings.LastIndexByte(c.Image, '/') {
				c.Image = c.Image[:colon+1] + chartTag
			}
		}
	}

	s.mu.Lock()
	if s.cache == nil {
		s.cache = map[string]Target{}
	}
	s.cache[cacheKey] = cloneTarget(out)
	s.mu.Unlock()

	return out, true
}

func findBaseTarget(baseTargets map[string]Target, appID, appVersion string) (Target, bool) {
	if exact, ok := baseTargets[appID+"@"+appVersion]; ok && exact.Deployment != nil {
		return exact, true
	}
	var best Target
	found := false
	for _, candidate := range baseTargets {
		if candidate.AppID == appID && candidate.Deployment != nil {
			if !found || candidate.Version > best.Version {
				best = candidate
				found = true
			}
		}
	}
	return best, found
}

func cloneTarget(t Target) Target {
	out := t
	if t.Deployment != nil {
		out.Deployment = t.Deployment.DeepCopy()
	}
	if len(t.Services) > 0 {
		out.Services = make([]*corev1.Service, len(t.Services))
		for i, svc := range t.Services {
			if svc != nil {
				out.Services[i] = svc.DeepCopy()
			}
		}
	}
	if len(t.InstallationBindings) > 0 {
		out.InstallationBindings = append([]InstallationBinding(nil), t.InstallationBindings...)
	}
	return out
}

func extractChartImageTag(values map[string]any) string {
	if values == nil {
		return ""
	}
	img, ok := values["image"].(map[string]any)
	if !ok {
		return ""
	}
	tag, _ := img["tag"].(string)
	return strings.TrimSpace(tag)
}

func (s *OCIChartSource) fetchAndVerifyChart(ctx context.Context, appID, releaseDigest, packageDigest string) (*OCIChartMeta, error) {
	client := s.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 15 * time.Second}
	}
	var lastErr error
	for _, baseURL := range s.RegistryURLs {
		meta, err := fetchChartFromRegistry(ctx, client, baseURL, appID, releaseDigest, packageDigest)
		if err == nil {
			return meta, nil
		}
		lastErr = err
	}
	if lastErr == nil {
		lastErr = errors.New("no registry URLs configured")
	}
	return nil, lastErr
}

func fetchChartFromRegistry(ctx context.Context, client *http.Client, baseURL, appID, releaseDigest, packageDigest string) (*OCIChartMeta, error) {
	baseURL = strings.TrimRight(baseURL, "/")
	if releaseDigest != "" {
		manifestURL := fmt.Sprintf("%s/v2/charts/%s/manifests/%s", baseURL, appID, releaseDigest)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, manifestURL, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Accept", "application/vnd.oci.image.manifest.v1+json")
		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		rawManifest, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("manifest status %d", resp.StatusCode)
		}
		sum := sha256.Sum256(rawManifest)
		if "sha256:"+hex.EncodeToString(sum[:]) != releaseDigest {
			return nil, errors.New("manifest digest mismatch")
		}
		var parsed struct {
			Layers []struct {
				Digest string `json:"digest"`
			} `json:"layers"`
		}
		if err := json.Unmarshal(rawManifest, &parsed); err != nil {
			return nil, err
		}
		layerFound := false
		for _, layer := range parsed.Layers {
			if layer.Digest == packageDigest {
				layerFound = true
				break
			}
		}
		if !layerFound {
			return nil, errors.New("package digest not found in manifest layers")
		}
	}

	blobURL := fmt.Sprintf("%s/v2/charts/%s/blobs/%s", baseURL, appID, packageDigest)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, blobURL, nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	rawBlob, readErr := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
	resp.Body.Close()
	if readErr != nil {
		return nil, readErr
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("blob status %d", resp.StatusCode)
	}
	blobSum := sha256.Sum256(rawBlob)
	if "sha256:"+hex.EncodeToString(blobSum[:]) != packageDigest {
		return nil, errors.New("package digest mismatch")
	}

	return parseChartArchive(rawBlob, appID, releaseDigest, packageDigest)
}

func parseChartArchive(rawTGZ []byte, appID, releaseDigest, packageDigest string) (*OCIChartMeta, error) {
	gz, err := gzip.NewReader(bytes.NewReader(rawTGZ))
	if err != nil {
		return nil, err
	}
	defer gz.Close()

	tr := tar.NewReader(gz)
	var chartYAML, valuesYAML []byte
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		clean := path.Clean(strings.TrimPrefix(hdr.Name, "./"))
		parts := strings.SplitN(clean, "/", 2)
		if len(parts) != 2 {
			continue
		}
		rel := parts[1]
		data, err := io.ReadAll(io.LimitReader(tr, 2<<20))
		if err != nil {
			return nil, err
		}
		switch {
		case rel == "Chart.yaml":
			chartYAML = data
		case rel == "values.yaml":
			valuesYAML = data
		}
	}
	if len(chartYAML) == 0 {
		return nil, errors.New("Chart.yaml missing from archive")
	}
	var chartMeta map[string]any
	if err := yaml.Unmarshal(chartYAML, &chartMeta); err != nil {
		return nil, err
	}
	name, _ := chartMeta["name"].(string)
	ver, _ := chartMeta["version"].(string)
	appVer, _ := chartMeta["appVersion"].(string)
	desc, _ := chartMeta["description"].(string)
	if strings.TrimSpace(name) != appID || strings.TrimSpace(ver) == "" {
		return nil, errors.New("chart identity mismatch")
	}
	var values map[string]any
	if len(valuesYAML) > 0 {
		_ = yaml.Unmarshal(valuesYAML, &values)
	}
	if values == nil {
		values = map[string]any{}
	}
	return &OCIChartMeta{
		OCIRef:         "oci://registry.verdantflare.ai/charts/" + appID,
		ChartName:      strings.TrimSpace(name),
		ChartVersion:   strings.TrimSpace(ver),
		AppVersion:     strings.TrimSpace(appVer),
		ManifestDigest: releaseDigest,
		PackageDigest:  packageDigest,
		Description:    strings.TrimSpace(desc),
		Values:         values,
		Package:        append([]byte(nil), rawTGZ...),
	}, nil
}
