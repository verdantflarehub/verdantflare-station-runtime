package executor

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	pb "github.com/verdantflarehub/verdantflare-station-runtime/api/runtimev1"
	"github.com/verdantflarehub/verdantflare-station-runtime/internal/strictjson"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
)

var ErrManagementMaintenanceUnavailable = errors.New("MANAGEMENT_MAINTENANCE_UNAVAILABLE")
var managementDigest = regexp.MustCompile(`^sha256:[a-f0-9]{64}$`)

// Supplied only by the verified platform release configuration, never by an
// App request or an observed Pod. Images maps every container name (including
// init containers) to its published digest. Token is never persisted or logged.
type ManagementMaintenanceRelease struct {
	Snapshot ManagementRelease
	Images   map[string]string
	Service  string
	Port     int32
	Token    string
}

type HTTPManagementMaintenance struct {
	Kubernetes *Kubernetes
	Targets    map[string]Target
	Releases   map[string]ManagementMaintenanceRelease // immutable release digest
	client     *http.Client
}

func NewHTTPManagementMaintenance(k *Kubernetes, targets map[string]Target, releases map[string]ManagementMaintenanceRelease) *HTTPManagementMaintenance {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	return &HTTPManagementMaintenance{k, targets, releases, &http.Client{
		Transport: transport, Timeout: 10 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}}
}

type managementProof struct {
	SchemaVersion   int    `json:"schema_version"`
	OperationID     string `json:"operation_id"`
	InstallationID  string `json:"installation_id"`
	StationID       string `json:"station_id"`
	OrganizationID  string `json:"organization_id"`
	AppVersion      string `json:"app_version"`
	DataContract    string `json:"data_contract"`
	DashboardDigest string `json:"dashboard_digest"`
}

type managementProofResponse struct {
	managementProof
	State string `json:"maintenance_state"`
}

func (m *HTTPManagementMaintenance) Seal(ctx context.Context, b ManagementBinding, c ManagementCheckpoint) error {
	return m.call(ctx, b, c, false, "seal")
}

func (m *HTTPManagementMaintenance) Verify(ctx context.Context, b ManagementBinding, c ManagementCheckpoint, target bool) error {
	return m.call(ctx, b, c, target, "status")
}

func (m *HTTPManagementMaintenance) Release(ctx context.Context, b ManagementBinding, c ManagementCheckpoint, target bool) error {
	return m.call(ctx, b, c, target, "release")
}

func (m *HTTPManagementMaintenance) call(ctx context.Context, b ManagementBinding, c ManagementCheckpoint, target bool, action string) error {
	if m == nil || m.client == nil || m.Kubernetes == nil || m.Kubernetes.Client == nil || !managementID(b.UserID) || b.ExpectedRevision < 1 {
		return ErrManagementReplacement
	}
	source, ok := m.Targets[b.Source.AppID+"@"+b.Source.AppVersion]
	destination, found := m.Targets[b.Target.AppID+"@"+b.Target.AppVersion]
	if !ok || !found || b.Target.AppID != b.Source.AppID || b.Source.TemplateDigest != "sha256:"+hex.EncodeToString(source.Hash[:]) || b.Target.TemplateDigest != "sha256:"+hex.EncodeToString(destination.Hash[:]) {
		return ErrManagementReplacement
	}
	driver, err := NewManagementReplacement(m.Kubernetes, source, destination, &pb.AppRequest{
		OperationId: b.OperationID, InstallationId: b.InstallationID, StationId: b.StationID, OrganizationId: b.OrganizationID,
		UserId: b.UserID, AppId: b.Source.AppID, AppVersion: b.Source.AppVersion,
	})
	if err != nil || !driver.validCheckpoint(c) {
		return ErrManagementReplacement
	}
	release, want := b.Source, driver.Source
	if target {
		release, want = b.Target, driver.Target
	}
	config, ok := m.Releases[release.ReleaseDigest]
	if !ok || !managementDigest.MatchString(release.ReleaseDigest) || !managementDigest.MatchString(release.DashboardDigest) ||
		!reflect.DeepEqual(config.Snapshot, release) || release.DataContract == "" || !validName(config.Service) ||
		config.Port < 1 || config.Port > 65535 || len(config.Token) < 32 || strings.ContainsAny(config.Token, "\r\n\t ") {
		return ErrManagementReplacement
	}
	serviceFound := false
	for _, s := range want.Services {
		if s.Name == config.Service && s.Namespace == want.Deployment.Namespace && len(s.Spec.Selector) > 0 && labels.SelectorFromSet(s.Spec.Selector).Matches(labels.Set(want.Deployment.Spec.Template.Labels)) {
			for _, port := range s.Spec.Ports {
				if port.Port == config.Port && (port.Protocol == "" || port.Protocol == corev1.ProtocolTCP) {
					serviceFound = true
				}
			}
		}
	}
	if !serviceFound {
		return ErrManagementReplacement
	}
	before, err := m.observe(ctx, driver, c, target, config)
	if err != nil {
		return err
	}
	proof := managementProof{1, b.OperationID, b.InstallationID, b.StationID, b.OrganizationID, release.AppVersion, release.DataContract, release.DashboardDigest}
	raw, err := json.Marshal(proof)
	if err != nil {
		return ErrManagementReplacement
	}
	endpoint := "http://" + config.Service + "." + want.Deployment.Namespace + ".svc:" + strconv.Itoa(int(config.Port)) + "/internal/maintenance/v1/" + action
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(raw))
	if err != nil {
		return ErrManagementReplacement
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+config.Token)
	response, err := m.client.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return ErrManagementMaintenanceUnavailable // Never expose URL/transport errors or response bodies.
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return ErrManagementMaintenanceUnavailable
	}
	wantState := "sealed"
	if action == "release" {
		wantState = "released"
	}
	var actual managementProofResponse
	if strictjson.Decode(response.Body, 16<<10, &actual) != nil || actual.managementProof != proof || actual.State != wantState {
		return ErrManagementReplacement
	}
	after, err := m.observe(ctx, driver, c, target, config)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(before, after) {
		return ErrManagementMaintenanceUnavailable
	}
	return nil
}

// The complete observation is compared across the authenticated request. A
// restart/replacement cannot inherit a prior process's maintenance proof.
type managementProcess struct {
	PodUID     string
	Containers map[string]managementContainer
}
type managementContainer struct {
	ID       string
	Restarts int32
}

func (m *HTTPManagementMaintenance) observe(ctx context.Context, r *ManagementReplacement, c ManagementCheckpoint, target bool, config ManagementMaintenanceRelease) (managementProcess, error) {
	var empty managementProcess
	want := r.Source
	if target {
		want = r.Target
	}
	d, err := m.Kubernetes.Client.AppsV1().Deployments(want.Deployment.Namespace).Get(ctx, want.Deployment.Name, metav1.GetOptions{})
	if err != nil {
		return empty, ErrManagementMaintenanceUnavailable
	}
	if d.UID != c.Deployment.UID || !r.deploymentMatches(want.Deployment, d, want) || !managementReady(d) ||
		(target && !r.targetMarked(d.Annotations)) || (!target && !originalMarker(d.Annotations, c.Deployment.Annotations)) {
		return empty, ErrManagementReplacement
	}
	for i, s := range want.Services {
		live, e := m.Kubernetes.Client.CoreV1().Services(s.Namespace).Get(ctx, s.Name, metav1.GetOptions{})
		if e != nil {
			return empty, ErrManagementMaintenanceUnavailable
		}
		if live.UID != c.Services[i].UID || live.DeletionTimestamp != nil || !owned(live.Annotations, want, r.StationID, r.OrganizationID) || !preinstalledServiceCompatible(s, live) ||
			(target && !r.targetMarked(live.Annotations)) || (!target && !originalMarker(live.Annotations, c.Services[i].Annotations)) {
			return empty, ErrManagementReplacement
		}
	}
	selector, err := metav1.LabelSelectorAsSelector(d.Spec.Selector)
	if err != nil || selector.Empty() {
		return empty, ErrManagementReplacement
	}
	pods, err := m.Kubernetes.Client.CoreV1().Pods(d.Namespace).List(ctx, metav1.ListOptions{LabelSelector: selector.String()})
	if err != nil {
		return empty, ErrManagementMaintenanceUnavailable
	}
	// Reject all competing selected Pods, including terminating ones which may
	// still serve requests. Do not pick an arbitrary Ready Pod during rollout.
	if len(pods.Items) != 1 {
		return empty, ErrManagementMaintenanceUnavailable
	}
	p := pods.Items[0]
	// The maintenance Service may have a broader selector than the Deployment.
	// Verify it cannot route the authenticated call to a second workload.
	for _, service := range want.Services {
		if service.Name != config.Service {
			continue
		}
		selected, err := m.Kubernetes.Client.CoreV1().Pods(d.Namespace).List(ctx, metav1.ListOptions{LabelSelector: labels.SelectorFromSet(service.Spec.Selector).String()})
		if err != nil {
			return empty, ErrManagementMaintenanceUnavailable
		}
		if len(selected.Items) != 1 || selected.Items[0].UID != p.UID {
			return empty, ErrManagementReplacement
		}
	}
	owner := metav1.GetControllerOf(&p)
	if p.UID == "" || p.DeletionTimestamp != nil || p.Status.Phase != corev1.PodRunning || owner == nil || owner.Kind != "ReplicaSet" || owner.APIVersion != "apps/v1" {
		return empty, ErrManagementReplacement
	}
	rs, err := m.Kubernetes.Client.AppsV1().ReplicaSets(d.Namespace).Get(ctx, owner.Name, metav1.GetOptions{})
	if err != nil {
		return empty, ErrManagementMaintenanceUnavailable
	}
	parent := metav1.GetControllerOf(rs)
	if rs.UID != owner.UID || rs.DeletionTimestamp != nil || parent == nil || parent.UID != d.UID || parent.Name != d.Name || parent.Kind != "Deployment" || parent.APIVersion != "apps/v1" {
		return empty, ErrManagementReplacement
	}
	ready := false
	for _, condition := range p.Status.Conditions {
		if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue {
			ready = true
		}
	}
	if !ready || len(p.Spec.EphemeralContainers) > 0 || len(config.Images) != len(d.Spec.Template.Spec.Containers)+len(d.Spec.Template.Spec.InitContainers) {
		return empty, ErrManagementReplacement
	}
	result := managementProcess{string(p.UID), map[string]managementContainer{}}
	for i, group := range [][2][]corev1.Container{{d.Spec.Template.Spec.Containers, p.Spec.Containers}, {d.Spec.Template.Spec.InitContainers, p.Spec.InitContainers}} {
		statuses := p.Status.ContainerStatuses
		if i == 1 {
			statuses = p.Status.InitContainerStatuses
		}
		if len(group[0]) != len(group[1]) || len(statuses) != len(group[0]) {
			return empty, ErrManagementReplacement
		}
		for n, w := range group[0] {
			if group[1][n].Name != w.Name || group[1][n].Image != w.Image {
				return empty, ErrManagementReplacement
			}
			digest, ok := config.Images[w.Name]
			if !ok || !managementDigest.MatchString(digest) {
				return empty, ErrManagementReplacement
			}
			if at := strings.LastIndex(w.Image, "@"); at >= 0 && w.Image[at+1:] != digest {
				return empty, ErrManagementReplacement
			}
			var matched *corev1.ContainerStatus
			for n := range statuses {
				if statuses[n].Name == w.Name {
					if matched != nil {
						return empty, ErrManagementReplacement
					}
					matched = &statuses[n]
				}
			}
			if matched == nil || matched.ContainerID == "" || !managementImageID(matched.ImageID, digest) {
				return empty, ErrManagementReplacement
			}
			if (i == 0 && (!matched.Ready || matched.State.Running == nil)) || (i == 1 && (matched.State.Terminated == nil || matched.State.Terminated.ExitCode != 0)) {
				return empty, ErrManagementReplacement
			}
			if _, exists := result.Containers[w.Name]; exists {
				return empty, ErrManagementReplacement
			}
			result.Containers[w.Name] = managementContainer{matched.ContainerID, matched.RestartCount}
		}
	}
	return result, nil
}

func managementImageID(actual, digest string) bool {
	for _, prefix := range []string{"docker-pullable://", "containerd://", "docker://"} {
		actual = strings.TrimPrefix(actual, prefix)
	}
	if at := strings.LastIndex(actual, "@"); at >= 0 {
		actual = actual[at+1:]
	}
	return managementDigest.MatchString(actual) && actual == digest
}
