package executor

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"reflect"
	"strings"

	"github.com/verdantflarehub/verdantflare-station-runtime/internal/strictjson"
	corev1 "k8s.io/api/core/v1"
)

const videoAuthorityDigest = "sha256:b4a6eed372cca5ede2a3722a45de210b3bac223050e3e478e89943f997b0a4a4"

func videoStoppedPair(source, target string) bool {
	return (source == "0.17.28" && target == "0.17.29") || (source == "0.17.30" && (target == "0.17.31" || target == "0.17.32")) || (source == "0.17.31" && target == "0.17.32")
}

func videoPodContainerMatches(actual, want corev1.Container, pod corev1.Pod) bool {
	if reflect.DeepEqual(actual, want) {
		return true
	}
	// Kubernetes injects its service-account projection into both containers.
	// Permit only that one extra read-only mount, never an application overlay.
	if len(actual.VolumeMounts) != len(want.VolumeMounts)+1 {
		return false
	}
	copy := actual.DeepCopy()
	mount := copy.VolumeMounts[len(copy.VolumeMounts)-1]
	if !strings.HasPrefix(mount.Name, "kube-api-access-") || !reflect.DeepEqual(mount, corev1.VolumeMount{Name: mount.Name, MountPath: "/var/run/secrets/kubernetes.io/serviceaccount", ReadOnly: true}) {
		return false
	}
	projection := false
	for _, volume := range pod.Spec.Volumes {
		if volume.Name == mount.Name && volume.Projected != nil {
			p := volume.Projected
			if len(p.Sources) != 3 {
				return false
			}
			token, ca, meta := p.Sources[0], p.Sources[1], p.Sources[2]
			if token.ServiceAccountToken == nil || token.ServiceAccountToken.Path != "token" || token.ServiceAccountToken.Audience != "" || ca.ConfigMap == nil || ca.ConfigMap.Name != "kube-root-ca.crt" || len(ca.ConfigMap.Items) != 1 || ca.ConfigMap.Items[0].Key != "ca.crt" || ca.ConfigMap.Items[0].Path != "ca.crt" || meta.DownwardAPI == nil || len(meta.DownwardAPI.Items) != 1 || meta.DownwardAPI.Items[0].Path != "namespace" || meta.DownwardAPI.Items[0].FieldRef == nil || meta.DownwardAPI.Items[0].FieldRef.FieldPath != "metadata.namespace" {
				return false
			}
			projection = true
		}
	}
	copy.VolumeMounts = copy.VolumeMounts[:len(copy.VolumeMounts)-1]
	return projection && reflect.DeepEqual(*copy, want)
}

// The first managed Video transition retains the exact historical authority
// sidecar. It does not grant arbitrary multi-container upgrade support.
func stoppedContainerDigests(r *StoppedReplacement, primary string) ([]string, error) {
	if r.Source.AppID != "video-mcp" {
		return []string{primary}, nil
	}
	if !videoStoppedPair(r.Source.Version, r.Target.Version) || r.Target.AppID != "video-mcp" || r.Source.Deployment == nil || r.Target.Deployment == nil {
		return nil, ErrManagementReplacement
	}
	source, target := r.Source.Deployment.Spec.Template.Spec.Containers, r.Target.Deployment.Spec.Template.Spec.Containers
	if len(source) != 2 || len(target) != 2 || source[0].Name != "api" || target[0].Name != "api" || source[1].Name != "artifact-authority" || !reflect.DeepEqual(source[1], target[1]) {
		return nil, ErrManagementReplacement
	}
	return []string{primary, videoAuthorityDigest}, nil
}

func videoActivityEvidence(raw []byte, release ManagementRelease) (bool, error) {
	var v struct {
		Schema    *int   `json:"schema_version"`
		Scope     string `json:"scope"`
		Active    *int64 `json:"active_count"`
		Submitted *int64 `json:"submitted_count"`
		Version   string `json:"app_version"`
		Dashboard string `json:"dashboard_digest"`
	}
	checked, err := strictjson.ReadValue(bytes.NewReader(raw), 16384)
	if err != nil || json.Unmarshal(checked, &v) != nil || v.Schema == nil || *v.Schema != 1 || v.Scope != "installation" || v.Active == nil || v.Submitted == nil || *v.Active < 0 || *v.Submitted < *v.Active || *v.Submitted > 1<<53 || v.Version != release.AppVersion || v.Dashboard != release.DashboardDigest {
		return false, ErrManagementReplacement
	}
	return *v.Active != 0, nil
}

func (s *stoppedHTTPChecks) checkVideo(ctx context.Context, b ManagementBinding, r *StoppedReplacement, c ManagementCheckpoint, target, idle bool) error {
	release, t := b.Source, r.Source
	if target {
		release, t = b.Target, r.Target
	}
	if !videoStoppedPair(b.Source.AppVersion, b.Target.AppVersion) || release.AppVersion != t.Version {
		return ErrManagementReplacement
	}
	u, err := url.Parse(t.ActivityURL)
	if err != nil || u.Scheme != "http" || u.Host != t.Deployment.Name+"."+t.Deployment.Namespace+".svc.cluster.local:8000" || u.Path != "/health" || u.RawPath != "" || u.RawQuery != "" || u.ForceQuery || u.User != nil || u.Fragment != "" {
		return ErrManagementReplacement
	}
	token := os.Getenv(t.ActivityTokenEnv)
	if !validManagementToken(token) || r.Kubernetes.videoActivityExec == nil {
		return ErrManagementMaintenanceUnavailable
	}
	before, err := stoppedPod(ctx, r, c, target, release.AppImageDigest)
	if err != nil {
		return err
	}
	container := before.Spec.Containers[0]
	if len(container.Command) != 0 || len(container.Args) != 0 || (container.WorkingDir != "" && container.WorkingDir != "/app") || (before.Spec.ShareProcessNamespace != nil && *before.Spec.ShareProcessNamespace) {
		return ErrManagementReplacement
	}
	state := false
	for _, mount := range container.VolumeMounts {
		switch mount.MountPath {
		case "/data/projects":
			state = true
		case "/etc/video-project-ca", "/etc/video-model-catalog", "/var/run/secrets/kubernetes.io/serviceaccount":
		default:
			return ErrManagementReplacement
		}
	}
	if !state {
		return ErrManagementReplacement
	}
	raw, err := r.Kubernetes.videoActivityExec(ctx, before.Namespace, before.Name, "api", nil)
	if err != nil {
		return err
	}
	busy, err := videoActivityEvidence(raw, release)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+net.JoinHostPort(before.Status.PodIP, "8000")+"/health", nil)
	if err != nil {
		return ErrManagementReplacement
	}
	req.Header.Set("Authorization", "Bearer "+token)
	response, err := s.client.Do(req)
	if err != nil {
		return ErrManagementMaintenanceUnavailable
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return ErrManagementReplacement
	}
	raw, err = io.ReadAll(io.LimitReader(response.Body, (1<<20)+1))
	if err != nil {
		return ErrManagementMaintenanceUnavailable
	}
	checked, err := strictjson.ReadValue(bytes.NewReader(raw), 1<<20)
	var health struct {
		Status   string `json:"status"`
		Version  string `json:"version"`
		Contract string `json:"contract_version"`
	}
	if err != nil || json.Unmarshal(checked, &health) != nil || health.Status != "ok" || health.Version != release.AppVersion || health.Contract != "v1" {
		return ErrManagementReplacement
	}
	after, err := stoppedPod(ctx, r, c, target, release.AppImageDigest)
	if err != nil {
		return err
	}
	if before.UID != after.UID || before.Status.PodIP != after.Status.PodIP || !reflect.DeepEqual(before.Status.ContainerStatuses, after.Status.ContainerStatuses) {
		return ErrManagementMaintenanceUnavailable
	}
	if busy && idle {
		return ErrManagementInstallationBusy
	}
	return nil
}
