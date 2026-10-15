package executor

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
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

func supportsStoppedChecks(app string) bool {
	return app == "higgsfield" || app == "project" || app == "world"
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
	if isTarget != target || !managementReady(d) || len(d.Spec.Template.Spec.Containers) != 1 || len(d.Spec.Template.Spec.InitContainers) != 0 {
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
	if p.UID == "" || p.DeletionTimestamp != nil || p.Status.Phase != corev1.PodRunning || net.ParseIP(p.Status.PodIP) == nil || owner == nil || owner.Kind != "ReplicaSet" || owner.APIVersion != "apps/v1" || len(p.Spec.EphemeralContainers) != 0 || len(p.Spec.InitContainers) != 0 || len(p.Spec.Containers) != 1 || len(p.Status.ContainerStatuses) != 1 {
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
	want := d.Spec.Template.Spec.Containers[0]
	actual := p.Spec.Containers[0]
	status := p.Status.ContainerStatuses[0]
	ready := false
	for _, cond := range p.Status.Conditions {
		if cond.Type == corev1.PodReady && cond.Status == corev1.ConditionTrue {
			ready = true
		}
	}
	if !ready || actual.Name != want.Name || actual.Image != want.Image || status.Name != want.Name || !status.Ready || status.State.Running == nil || status.ContainerID == "" || !managementDigest.MatchString(digest) || !managementImageID(status.ImageID, digest) {
		return zero, ErrManagementReplacement
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
	contentService := release.AppID == "project" || release.AppID == "world"
	scheme, port, activityPath := "http", "8001", "/limits"
	declarationPath, indexPath := "/internal/dashboard-declaration", "/internal/dashboard-index"
	if contentService {
		scheme, port, activityPath = "https", "8443", "/healthz"
		declarationPath, indexPath = "/internal/dashboard/declaration", "/internal/dashboard/index"
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
	raw, err := get(activityPath)
	if err != nil {
		return err
	}
	busy := false
	if !contentService {
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
