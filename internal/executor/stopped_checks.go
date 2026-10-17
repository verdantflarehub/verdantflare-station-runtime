package executor

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"reflect"
	"time"

	"github.com/google/uuid"
	"github.com/verdantflarehub/verdantflare-station-runtime/internal/strictjson"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
)

type stoppedChecker interface {
	Check(context.Context, ManagementBinding, *StoppedReplacement, ManagementCheckpoint, bool, bool) error
}

type stoppedHTTPChecks struct{ client *http.Client }

//go:embed fal_010_activity.py
var legacyFalActivityProgram string

//go:embed google_010_activity.py
var legacyGoogleActivityProgram string

//go:embed grok_010_activity.py
var legacyGrokActivityProgram string

//go:embed chatgpt_010_activity.py
var legacyChatGPTActivityProgram string

//go:embed video_01728_activity.py
var legacyVideoActivityProgram string

func supportsStoppedChecks(app string) bool {
	return app == "higgsfield" || app == "project" || app == "world" || app == "fal" || app == "network" || app == "google" || app == "grok" || app == "chatgpt" || app == "image-mcp" || app == "video-mcp"
}

func newStoppedChecks() *stoppedHTTPChecks {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	return &stoppedHTTPChecks{&http.Client{Transport: transport, Timeout: 5 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}
}

func stoppedPod(ctx context.Context, r *StoppedReplacement, c ManagementCheckpoint, target bool, digest string) (corev1.Pod, error) {
	var zero corev1.Pod
	d, isTarget, err := r.liveDeployment(ctx, c)
	if err != nil {
		return zero, err
	}
	digests, err := stoppedContainerDigests(r, digest)
	if err != nil {
		return zero, err
	}
	if isTarget != target || !managementReady(d) || len(d.Spec.Template.Spec.Containers) != len(digests) || len(d.Spec.Template.Spec.InitContainers) != 0 {
		return zero, ErrManagementReplacement
	}
	selector, err := metav1.LabelSelectorAsSelector(d.Spec.Selector)
	if err != nil || selector.Empty() {
		return zero, ErrManagementReplacement
	}
	pods, err := r.Kubernetes.Client.CoreV1().Pods(d.Namespace).List(ctx, metav1.ListOptions{LabelSelector: selector.String()})
	if err != nil {
		return zero, ErrManagementMaintenanceUnavailable
	}
	if len(pods.Items) != 1 {
		return zero, ErrManagementMaintenanceUnavailable
	}
	p := pods.Items[0]
	for _, service := range c.Services {
		if len(service.Spec.Selector) == 0 {
			return zero, ErrManagementReplacement
		}
		selected, e := r.Kubernetes.Client.CoreV1().Pods(d.Namespace).List(ctx, metav1.ListOptions{LabelSelector: labels.SelectorFromSet(service.Spec.Selector).String()})
		if e != nil {
			return zero, ErrManagementMaintenanceUnavailable
		}
		if len(selected.Items) != 1 || selected.Items[0].UID != p.UID {
			return zero, ErrManagementReplacement
		}
	}
	owner := metav1.GetControllerOf(&p)
	if p.UID == "" || p.DeletionTimestamp != nil || p.Status.Phase != corev1.PodRunning || net.ParseIP(p.Status.PodIP) == nil || owner == nil || owner.Kind != "ReplicaSet" || owner.APIVersion != "apps/v1" || len(p.Spec.EphemeralContainers) != 0 || len(p.Spec.InitContainers) != 0 || len(p.Spec.Containers) != len(digests) || len(p.Status.ContainerStatuses) != len(digests) {
		return zero, ErrManagementReplacement
	}
	rs, err := r.Kubernetes.Client.AppsV1().ReplicaSets(d.Namespace).Get(ctx, owner.Name, metav1.GetOptions{})
	if err != nil {
		return zero, ErrManagementMaintenanceUnavailable
	}
	parent := metav1.GetControllerOf(rs)
	if rs.UID != owner.UID || rs.DeletionTimestamp != nil || parent == nil || parent.UID != d.UID || parent.Name != d.Name || parent.Kind != "Deployment" || parent.APIVersion != "apps/v1" {
		return zero, ErrManagementReplacement
	}
	ready := false
	for _, cond := range p.Status.Conditions {
		if cond.Type == corev1.PodReady && cond.Status == corev1.ConditionTrue {
			ready = true
		}
	}
	if !ready {
		return zero, ErrManagementReplacement
	}
	for i, expectedDigest := range digests {
		want, actual := d.Spec.Template.Spec.Containers[i], p.Spec.Containers[i]
		var status *corev1.ContainerStatus
		for j := range p.Status.ContainerStatuses {
			if p.Status.ContainerStatuses[j].Name == want.Name {
				if status != nil {
					return zero, ErrManagementReplacement
				}
				status = &p.Status.ContainerStatuses[j]
			}
		}
		if status == nil || actual.Name != want.Name || actual.Image != want.Image || !status.Ready || status.State.Running == nil || status.ContainerID == "" || !managementDigest.MatchString(expectedDigest) || !managementImageID(status.ImageID, expectedDigest) {
			return zero, ErrManagementReplacement
		}
		if r.Source.AppID == "video-mcp" && !videoPodContainerMatches(actual, want, p) {
			return zero, ErrManagementReplacement
		}
	}
	return p, nil
}

func (s *stoppedHTTPChecks) Check(ctx context.Context, b ManagementBinding, r *StoppedReplacement, c ManagementCheckpoint, target, idle bool) error {
	release, t := b.Source, r.Source
	if target {
		release, t = b.Target, r.Target
	}
	// Other application occupancy formats are not guessed. They can adopt the
	// same ordinary driver when a real read-only business check is available.
	if release.ReleaseKind != "dashboard-release" || !supportsStoppedChecks(release.AppID) || !validManagementRelease(release) {
		return ErrManagementReplacement
	}
	if release.AppID == "image-mcp" {
		return s.checkImage(ctx, b, r, c, target, idle)
	}
	if release.AppID == "video-mcp" {
		return s.checkVideo(ctx, b, r, c, target, idle)
	}
	contentService := release.AppID == "project" || release.AppID == "world"
	fal := release.AppID == "fal"
	google := release.AppID == "google"
	grok := release.AppID == "grok"
	chatgpt := release.AppID == "chatgpt"
	network := release.AppID == "network"
	scheme, port, activityPath := "http", "8001", "/limits"
	declarationPath, indexPath := "/internal/dashboard-declaration", "/internal/dashboard-index"
	if fal || google || grok || chatgpt {
		port, activityPath = "8000", "/v1/activity"
	}
	if contentService {
		scheme, port, activityPath = "https", "8443", "/healthz"
		declarationPath, indexPath = "/internal/dashboard/declaration", "/internal/dashboard/index"
	}
	if network {
		port, activityPath = "8080", "/healthz"
		declarationPath, indexPath = "/internal/dashboard/declaration", "/internal/dashboard/index"
		if t.Deployment.Spec.Template.Spec.TerminationGracePeriodSeconds == nil || *t.Deployment.Spec.Template.Spec.TerminationGracePeriodSeconds < 70 {
			return ErrManagementReplacement
		}
	}
	u, err := url.Parse(t.ActivityURL)
	if err != nil || u.Scheme != scheme || u.Hostname() != t.Deployment.Name+"."+t.Deployment.Namespace+".svc.cluster.local" || u.Port() != port || u.Path != activityPath || u.RawPath != "" || u.RawQuery != "" || u.ForceQuery || u.User != nil || u.Fragment != "" {
		return ErrManagementReplacement
	}
	token := os.Getenv(t.ActivityTokenEnv)
	if !validManagementToken(token) {
		return ErrManagementMaintenanceUnavailable
	}
	before, err := stoppedPod(ctx, r, c, target, release.AppImageDigest)
	if err != nil {
		return err
	}
	client := *s.client
	if contentService {
		if transport, ok := client.Transport.(*http.Transport); ok {
			transport = transport.Clone()
			transport.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12, ServerName: u.Hostname()}
			client.Transport = transport
			defer transport.CloseIdleConnections()
		}
	}
	requestID := uuid.Must(uuid.NewV7()).String()
	get := func(path string) ([]byte, error) {
		endpoint := scheme + "://" + net.JoinHostPort(before.Status.PodIP, port) + path
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			return nil, ErrManagementReplacement
		}
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("X-User-Id", b.UserID)
		req.Header.Set("X-Organization-Id", b.OrganizationID)
		req.Header.Set("X-Station-Id", b.StationID)
		req.Header.Set("X-Request-Id", requestID)
		if contentService {
			req.Host = u.Host
		}
		response, err := client.Do(req)
		if err != nil {
			return nil, ErrManagementMaintenanceUnavailable
		}
		defer response.Body.Close()
		expected := http.StatusOK
		if contentService && path == activityPath {
			expected = http.StatusNoContent
		}
		if response.StatusCode != expected {
			return nil, ErrManagementReplacement
		}
		raw, err := io.ReadAll(io.LimitReader(response.Body, (1<<20)+1))
		if err != nil || len(raw) > 1<<20 {
			return nil, ErrManagementReplacement
		}
		return raw, nil
	}
	var raw []byte
	if google && (release.AppVersion == "0.1.0" || release.AppVersion == "0.1.1") {
		container := before.Spec.Containers[0]
		if r.Kubernetes.googleActivityExec == nil ||
			len(container.Command) != 0 || len(container.Args) != 0 ||
			(container.WorkingDir != "" && container.WorkingDir != "/app") ||
			(before.Spec.ShareProcessNamespace != nil && *before.Spec.ShareProcessNamespace) ||
			container.SecurityContext == nil || container.SecurityContext.ReadOnlyRootFilesystem == nil || !*container.SecurityContext.ReadOnlyRootFilesystem {
			return ErrManagementReplacement
		}
		stateMount := false
		for _, mount := range container.VolumeMounts {
			if mount.MountPath == "/data/google" {
				stateMount = true
			} else if mount.MountPath != "/tmp" {
				return ErrManagementReplacement
			}
		}
		if !stateMount {
			return ErrManagementReplacement
		}
		raw, err = r.Kubernetes.googleActivityExec(ctx, before.Namespace, before.Name, container.Name, nil)
	} else if grok && release.AppVersion == "0.1.0" {
		container := before.Spec.Containers[0]
		if r.Kubernetes.grokActivityExec == nil ||
			len(container.Command) != 0 || len(container.Args) != 0 ||
			(container.WorkingDir != "" && container.WorkingDir != "/app") ||
			(before.Spec.ShareProcessNamespace != nil && *before.Spec.ShareProcessNamespace) ||
			container.SecurityContext == nil || container.SecurityContext.ReadOnlyRootFilesystem == nil || !*container.SecurityContext.ReadOnlyRootFilesystem {
			return ErrManagementReplacement
		}
		stateMount := false
		for _, mount := range container.VolumeMounts {
			if mount.MountPath == "/data/grok" {
				stateMount = true
			} else if mount.MountPath != "/tmp" {
				return ErrManagementReplacement
			}
		}
		if !stateMount {
			return ErrManagementReplacement
		}
		raw, err = r.Kubernetes.grokActivityExec(ctx, before.Namespace, before.Name, container.Name, nil)
	} else if chatgpt && release.AppVersion == "0.1.0" {
		container := before.Spec.Containers[0]
		if r.Kubernetes.chatgptActivityExec == nil ||
			len(container.Command) != 0 || len(container.Args) != 0 ||
			(container.WorkingDir != "" && container.WorkingDir != "/app") ||
			(before.Spec.ShareProcessNamespace != nil && *before.Spec.ShareProcessNamespace) ||
			container.SecurityContext == nil || container.SecurityContext.ReadOnlyRootFilesystem == nil || !*container.SecurityContext.ReadOnlyRootFilesystem {
			return ErrManagementReplacement
		}
		stateMount := false
		for _, mount := range container.VolumeMounts {
			if mount.MountPath == "/data/chatgpt" {
				stateMount = true
			} else if mount.MountPath != "/tmp" {
				return ErrManagementReplacement
			}
		}
		if !stateMount {
			return ErrManagementReplacement
		}
		raw, err = r.Kubernetes.chatgptActivityExec(ctx, before.Namespace, before.Name, container.Name, nil)
	} else if fal && release.AppVersion == "0.1.0" {
		container := before.Spec.Containers[0]
		if r.Kubernetes.falActivityExec == nil || len(container.Command) != 0 || len(container.Args) != 0 ||
			(container.WorkingDir != "" && container.WorkingDir != "/app") ||
			(before.Spec.ShareProcessNamespace != nil && *before.Spec.ShareProcessNamespace) ||
			container.SecurityContext == nil || container.SecurityContext.ReadOnlyRootFilesystem == nil || !*container.SecurityContext.ReadOnlyRootFilesystem {
			return ErrManagementReplacement
		}
		stateMount := false
		for _, mount := range container.VolumeMounts {
			if mount.MountPath == "/data/fal" {
				stateMount = true
			} else if mount.MountPath != "/tmp" {
				return ErrManagementReplacement
			}
		}
		if !stateMount {
			return ErrManagementReplacement
		}
		raw, err = r.Kubernetes.falActivityExec(ctx, before.Namespace, before.Name, container.Name, nil)
	} else {
		raw, err = get(activityPath)
	}
	if err != nil {
		return err
	}
	busy := false
	if fal || google || grok || chatgpt {
		var activity struct {
			Schema    *int   `json:"schema_version"`
			Scope     string `json:"scope"`
			Active    *int   `json:"active_count"`
			Submitted *int   `json:"submitted_count"`
		}
		checked, e := strictjson.ReadValue(bytes.NewReader(raw), 1<<20)
		if e != nil || json.Unmarshal(checked, &activity) != nil || activity.Schema == nil || *activity.Schema != 1 || activity.Scope != "installation" || activity.Active == nil || activity.Submitted == nil || *activity.Active < 0 || *activity.Submitted < *activity.Active {
			return ErrManagementReplacement
		}
		busy = idle && *activity.Active != 0
	} else if network {
		var health struct {
			Status string `json:"status"`
		}
		checked, e := strictjson.ReadValue(bytes.NewReader(raw), 1<<20)
		if e != nil || json.Unmarshal(checked, &health) != nil || health.Status != "ready" {
			return ErrManagementReplacement
		}
	} else if !contentService {
		var limits struct {
			Active    *int   `json:"active_count"`
			Submitted *int   `json:"submitted_count"`
			Binding   string `json:"binding_id"`
		}
		checked, err := strictjson.ReadValue(bytes.NewReader(raw), 1<<20)
		if err != nil || json.Unmarshal(checked, &limits) != nil || limits.Active == nil || limits.Submitted == nil || *limits.Active < 0 || *limits.Submitted < *limits.Active || limits.Binding == "" {
			return ErrManagementReplacement
		}
		busy = idle && *limits.Active != 0
	}
	raw, err = get(declarationPath)
	if err != nil {
		return err
	}
	var declaration struct {
		AppID      string `json:"appId"`
		AppVersion string `json:"appVersion"`
		Dashboard  struct {
			Digest string `json:"artifactDigest"`
		} `json:"dashboard"`
	}
	checked, err := strictjson.ReadValue(bytes.NewReader(raw), 1<<20)
	if err != nil || json.Unmarshal(checked, &declaration) != nil || declaration.AppID != release.AppID || declaration.AppVersion != release.AppVersion || declaration.Dashboard.Digest != release.DashboardDigest {
		return ErrManagementReplacement
	}
	raw, err = get(indexPath)
	if err != nil {
		return err
	}
	hash := sha256.Sum256(raw)
	if "sha256:"+hex.EncodeToString(hash[:]) != release.DashboardDigest {
		return ErrManagementReplacement
	}
	after, err := stoppedPod(ctx, r, c, target, release.AppImageDigest)
	if err != nil {
		return err
	}
	if before.UID != after.UID || before.Status.PodIP != after.Status.PodIP || !reflect.DeepEqual(before.Status.ContainerStatuses, after.Status.ContainerStatuses) {
		return ErrManagementMaintenanceUnavailable
	}
	if busy {
		return ErrManagementInstallationBusy
	}
	return nil
}
