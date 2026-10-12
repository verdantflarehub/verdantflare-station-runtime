package executor

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/google/uuid"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

type maintenanceRoundTrip func(*http.Request) (*http.Response, error)

func (f maintenanceRoundTrip) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type maintenanceFixture struct {
	m        *HTTPManagementMaintenance
	b        ManagementBinding
	c        ManagementCheckpoint
	d        *ManagementReplacement
	k        *fake.Clientset
	pod      *corev1.Pod
	calls    int
	response func(*http.Request) (*http.Response, error)
}

func newMaintenanceFixture(t *testing.T) *maintenanceFixture {
	return newMaintenanceFixtureFor(t, nil)
}

func newMaintenanceFixtureFor(t *testing.T, configure func(*Target, *Target)) *maintenanceFixture {
	t.Helper()
	d, k, c := managementFixtureFor(t, configure)
	b := ManagementBinding{OperationID: d.OperationID, InstallationID: d.Source.InstallationID, StationID: d.StationID, OrganizationID: d.OrganizationID, UserID: uuid.Must(uuid.NewV7()).String(), ExpectedRevision: 1}
	makeRelease := func(v Target, digest string) ManagementRelease {
		return ManagementRelease{AppID: v.AppID, AppVersion: v.Version, ReleaseRef: v.Version, ReleaseDigest: "sha256:" + strings.Repeat(digest, 64), TemplateDigest: "sha256:" + hex.EncodeToString(v.Hash[:]), DashboardDigest: "sha256:" + strings.Repeat("b", 64), DataContract: "comfyui-state-v1"}
	}
	b.Source, b.Target = makeRelease(d.Source, "1"), makeRelease(d.Target, "2")
	registry := map[string]Target{}
	for _, v := range []Target{d.Source, d.Target} {
		v.Deployment = v.Deployment.DeepCopy()
		v.Deployment.Spec.Template.Spec.Containers[0].Env = nil
		registry[v.AppID+"@"+v.Version] = v
	}
	releases := map[string]ManagementMaintenanceRelease{}
	for _, s := range []ManagementRelease{b.Source, b.Target} {
		releases[s.ReleaseDigest] = ManagementMaintenanceRelease{Snapshot: s, Images: map[string]string{"server": "sha256:" + strings.Repeat("a", 64)}, Service: "manager", Port: 80, Token: strings.Repeat("test-only-", 5)}
	}
	m := NewHTTPManagementMaintenance(d.Kubernetes, registry, releases)
	f := &maintenanceFixture{m: m, b: b, c: c, d: d, k: k}
	rs := &appsv1.ReplicaSet{ObjectMeta: metav1.ObjectMeta{Name: "manager-rs", Namespace: c.Deployment.Namespace, UID: "rs-uid", OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(c.Deployment, appsv1.SchemeGroupVersion.WithKind("Deployment"))}}}
	p := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "manager-pod", Namespace: rs.Namespace, UID: "pod-uid", Labels: c.Deployment.Spec.Template.Labels, OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(rs, appsv1.SchemeGroupVersion.WithKind("ReplicaSet"))}}, Spec: *c.Deployment.Spec.Template.Spec.DeepCopy(), Status: corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}, ContainerStatuses: []corev1.ContainerStatus{{Name: "server", Ready: true, ContainerID: "containerd://process-1", ImageID: "repo/server@sha256:" + strings.Repeat("a", 64), State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}}}}
	f.pod = p
	if err := k.Tracker().Add(rs); err != nil {
		t.Fatal(err)
	}
	if err := k.Tracker().Add(p); err != nil {
		t.Fatal(err)
	}
	f.response = func(req *http.Request) (*http.Response, error) {
		if req.Method != http.MethodPost || req.Header.Get("Authorization") != "Bearer "+strings.Repeat("test-only-", 5) || req.Header.Get("X-User-Id") != "" || req.URL.Host != "manager."+rs.Namespace+".svc:80" {
			t.Fatal("request scope or authentication mismatch")
		}
		var body managementProof
		if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		raw, _ := json.Marshal(managementProofResponse{body, "sealed"})
		return proofResponse(200, string(raw)), nil
	}
	m.client.Transport = maintenanceRoundTrip(func(req *http.Request) (*http.Response, error) { f.calls++; return f.response(req) })
	return f
}

func proofResponse(code int, body string) *http.Response {
	return &http.Response{StatusCode: code, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}
}
func (f *maintenanceFixture) savePod(t *testing.T) {
	t.Helper()
	if _, err := f.k.CoreV1().Pods(f.pod.Namespace).Update(context.Background(), f.pod, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
}

func TestManagementMaintenanceSealVerifyAndRecovery(t *testing.T) {
	f := newMaintenanceFixture(t)
	ctx := context.Background()
	for i := 0; i < 2; i++ {
		if err := f.m.Seal(ctx, f.b, f.c); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 3; i++ {
		if _, err := f.d.Apply(ctx, f.c); err != nil {
			t.Fatal(err)
		}
	}
	f.pod.Spec = *f.d.Target.Deployment.Spec.Template.Spec.DeepCopy()
	f.savePod(t)
	if err := f.m.Verify(ctx, f.b, f.c, true); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if _, err := f.d.Restore(ctx, f.c); err != nil {
			t.Fatal(err)
		}
	}
	f.pod.Spec = *f.d.Source.Deployment.Spec.Template.Spec.DeepCopy()
	f.savePod(t)
	if err := f.m.Verify(ctx, f.b, f.c, false); err != nil {
		t.Fatal(err)
	}
	if f.calls != 4 {
		t.Fatal("missing fresh maintenance proof")
	}
}

func TestManagementMaintenanceRejectsInvalidRuntimeBeforeSendingToken(t *testing.T) {
	cases := map[string]func(*maintenanceFixture){
		"wrong image": func(f *maintenanceFixture) {
			f.pod.Status.ContainerStatuses[0].ImageID = "repo/server@sha256:" + strings.Repeat("f", 64)
		},
		"tag only":        func(f *maintenanceFixture) { f.pod.Status.ContainerStatuses[0].ImageID = "repo/server:1.0.0" },
		"wrong pod owner": func(f *maintenanceFixture) { f.pod.OwnerReferences[0].UID = "other-rs" },
		"unready":         func(f *maintenanceFixture) { f.pod.Status.ContainerStatuses[0].Ready = false },
		"terminating":     func(f *maintenanceFixture) { now := metav1.Now(); f.pod.DeletionTimestamp = &now },
		"extra container": func(f *maintenanceFixture) {
			f.pod.Spec.Containers = append(f.pod.Spec.Containers, corev1.Container{Name: "extra"})
		},
		"missing process":     func(f *maintenanceFixture) { f.pod.Status.ContainerStatuses[0].ContainerID = "" },
		"wrong installation":  func(f *maintenanceFixture) { f.b.InstallationID = uuid.Must(uuid.NewV7()).String() },
		"changed snapshot":    func(f *maintenanceFixture) { f.b.Source.DashboardDigest = "sha256:" + strings.Repeat("c", 64) },
		"missing release":     func(f *maintenanceFixture) { delete(f.m.Releases, f.b.Source.ReleaseDigest) },
		"missing image proof": func(f *maintenanceFixture) { delete(f.m.Releases[f.b.Source.ReleaseDigest].Images, "server") },
	}
	for name, change := range cases {
		t.Run(name, func(t *testing.T) {
			f := newMaintenanceFixture(t)
			change(f)
			f.savePod(t)
			if err := f.m.Seal(context.Background(), f.b, f.c); err == nil {
				t.Fatal("invalid evidence accepted")
			}
			if f.calls != 0 {
				t.Fatal("token sent before runtime proof")
			}
		})
	}
}

func TestManagementMaintenanceStrictProofAndErrors(t *testing.T) {
	for _, name := range []string{"wrong operation", "wrong scope", "wrong version", "wrong digest", "released", "duplicate", "unknown", "oversized", "404", "401", "503", "lost response"} {
		t.Run(name, func(t *testing.T) {
			f := newMaintenanceFixture(t)
			original := f.response
			f.response = func(req *http.Request) (*http.Response, error) {
				if name == "lost response" {
					return nil, errors.New("secret should never escape")
				}
				res, err := original(req)
				if err != nil {
					return res, err
				}
				raw, _ := io.ReadAll(res.Body)
				res.Body.Close()
				var body map[string]any
				json.Unmarshal(raw, &body)
				switch name {
				case "wrong operation":
					body["operation_id"] = uuid.Must(uuid.NewV7()).String()
				case "wrong scope":
					body["organization_id"] = uuid.Must(uuid.NewV7()).String()
				case "wrong version":
					body["app_version"] = "9.9.9"
				case "wrong digest":
					body["dashboard_digest"] = "sha256:" + strings.Repeat("f", 64)
				case "released":
					body["maintenance_state"] = "released"
				case "unknown":
					body["unknown"] = true
				case "duplicate":
					return proofResponse(200, strings.TrimSuffix(string(raw), "}")+`,"schema_version":1}`), nil
				case "oversized":
					return proofResponse(200, strings.Repeat(" ", 17000)+string(raw)), nil
				case "404":
					return proofResponse(404, "missing legacy endpoint"), nil
				case "401":
					return proofResponse(401, "private error"), nil
				case "503":
					return proofResponse(503, "private error"), nil
				}
				raw, _ = json.Marshal(body)
				return proofResponse(200, string(raw)), nil
			}
			err := f.m.Seal(context.Background(), f.b, f.c)
			if err == nil || strings.Contains(err.Error(), "secret") || strings.Contains(err.Error(), "private") {
				t.Fatal("invalid proof or unsanitized error")
			}
		})
	}
}

func TestManagementMaintenanceRechecksProcessAfterCall(t *testing.T) {
	for _, name := range []string{"restart", "replacement", "image drift", "extra pod"} {
		t.Run(name, func(t *testing.T) {
			f := newMaintenanceFixture(t)
			original := f.response
			f.response = func(req *http.Request) (*http.Response, error) {
				res, err := original(req)
				switch name {
				case "restart":
					f.pod.Status.ContainerStatuses[0].RestartCount++
				case "replacement":
					f.pod.UID = "replacement-pod"
				case "image drift":
					f.pod.Status.ContainerStatuses[0].ImageID = "sha256:" + strings.Repeat("f", 64)
				case "extra pod":
					p := f.pod.DeepCopy()
					p.Name = "competing"
					p.UID = "competing"
					if err := f.k.Tracker().Add(p); err != nil {
						t.Fatal(err)
					}
				}
				f.savePod(t)
				return res, err
			}
			if err := f.m.Verify(context.Background(), f.b, f.c, false); err == nil {
				t.Fatal("stale process proof accepted")
			}
		})
	}
}

func TestManagementMaintenanceDisablesProxyAndRedirects(t *testing.T) {
	f := newMaintenanceFixture(t)
	m := NewHTTPManagementMaintenance(f.d.Kubernetes, f.m.Targets, f.m.Releases)
	if m.client.Transport.(*http.Transport).Proxy != nil || m.client.Timeout <= 0 {
		t.Fatal("unbounded or proxied transport")
	}
	calls := 0
	m.client.Transport = maintenanceRoundTrip(func(*http.Request) (*http.Response, error) {
		calls++
		res := proofResponse(307, "")
		res.Header.Set("Location", "https://untrusted.invalid/")
		return res, nil
	})
	if err := m.Seal(context.Background(), f.b, f.c); err == nil || calls != 1 {
		t.Fatal("redirect followed")
	}
}

func TestManagementMaintenanceLostResponseReusesOperation(t *testing.T) {
	f := newMaintenanceFixture(t)
	original := f.response
	var first managementProof
	f.response = func(req *http.Request) (*http.Response, error) {
		json.NewDecoder(req.Body).Decode(&first)
		return nil, errors.New("connection lost")
	}
	if !errors.Is(f.m.Seal(context.Background(), f.b, f.c), ErrManagementMaintenanceUnavailable) {
		t.Fatal("lost response not retryable")
	}
	f.response = func(req *http.Request) (*http.Response, error) {
		raw, _ := io.ReadAll(req.Body)
		var next managementProof
		json.Unmarshal(raw, &next)
		if next != first {
			t.Fatal("operation changed on retry")
		}
		req.Body = io.NopCloser(strings.NewReader(string(raw)))
		return original(req)
	}
	if err := f.m.Seal(context.Background(), f.b, f.c); err != nil {
		t.Fatal(err)
	}
}

func TestManagementMaintenanceRejectsOwnershipAndServiceDrift(t *testing.T) {
	for _, name := range []string{"deployment UID", "replicaset owner", "service UID", "service route", "unregistered port", "unregistered service", "header injection"} {
		t.Run(name, func(t *testing.T) {
			f := newMaintenanceFixture(t)
			ctx := context.Background()
			switch name {
			case "deployment UID":
				d, _ := f.k.AppsV1().Deployments(f.pod.Namespace).Get(ctx, f.c.Deployment.Name, metav1.GetOptions{})
				d.UID = "different-deployment"
				if _, err := f.k.AppsV1().Deployments(d.Namespace).Update(ctx, d, metav1.UpdateOptions{}); err != nil {
					t.Fatal(err)
				}
			case "replicaset owner":
				r, _ := f.k.AppsV1().ReplicaSets(f.pod.Namespace).Get(ctx, "manager-rs", metav1.GetOptions{})
				r.OwnerReferences[0].UID = "different-deployment"
				if _, err := f.k.AppsV1().ReplicaSets(r.Namespace).Update(ctx, r, metav1.UpdateOptions{}); err != nil {
					t.Fatal(err)
				}
			case "service UID", "service route":
				s, _ := f.k.CoreV1().Services(f.pod.Namespace).Get(ctx, "manager", metav1.GetOptions{})
				if name == "service UID" {
					s.UID = "different-service"
				} else {
					s.Spec.Selector = map[string]string{"app": "other"}
				}
				if _, err := f.k.CoreV1().Services(s.Namespace).Update(ctx, s, metav1.UpdateOptions{}); err != nil {
					t.Fatal(err)
				}
			default:
				c := f.m.Releases[f.b.Source.ReleaseDigest]
				if name == "unregistered port" {
					c.Port = 1234
				}
				if name == "unregistered service" {
					c.Service = "other"
				}
				if name == "header injection" {
					c.Token += "\r\nX-Header: value"
				}
				f.m.Releases[f.b.Source.ReleaseDigest] = c
			}
			if err := f.m.Seal(ctx, f.b, f.c); err == nil || f.calls != 0 {
				t.Fatal("untrusted runtime received credentials")
			}
		})
	}
}

func TestManagementMaintenanceServiceCannotSelectForeignPod(t *testing.T) {
	f := newMaintenanceFixture(t)
	// Narrow the Deployment selector while retaining the registered Service's
	// broader app selector, then add a Pod which only the Service selects.
	for key, v := range f.m.Targets {
		v.Deployment.Spec.Selector.MatchLabels["manager"] = "yes"
		v.Deployment.Spec.Template.Labels["manager"] = "yes"
		f.m.Targets[key] = v
	}
	f.c.Deployment.Spec.Selector.MatchLabels["manager"] = "yes"
	f.c.Deployment.Spec.Template.Labels["manager"] = "yes"
	ctx := context.Background()
	if _, err := f.k.AppsV1().Deployments(f.pod.Namespace).Update(ctx, f.c.Deployment, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	f.pod.Labels["manager"] = "yes"
	f.savePod(t)
	foreign := f.pod.DeepCopy()
	foreign.Name, foreign.UID = "foreign", "foreign-uid"
	delete(foreign.Labels, "manager")
	if err := f.k.Tracker().Add(foreign); err != nil {
		t.Fatal(err)
	}
	if err := f.m.Seal(ctx, f.b, f.c); err == nil || f.calls != 0 {
		t.Fatal("broad Service accepted")
	}
}

func TestManagementMaintenanceCancellation(t *testing.T) {
	f := newMaintenanceFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	f.response = func(req *http.Request) (*http.Response, error) {
		cancel()
		return nil, req.Context().Err()
	}
	if err := f.m.Seal(ctx, f.b, f.c); !errors.Is(err, context.Canceled) {
		t.Fatal("cancellation not propagated")
	}
}
