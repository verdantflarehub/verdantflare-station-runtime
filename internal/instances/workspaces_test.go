package instances

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestWorkspaceHTTPValidationAndRouting(t *testing.T) {
	handler := Handler{Service: nil, Token: "only-core-may-use-this-test-token"}

	r := httptest.NewRequest("GET", "/internal/v1/instances/workspaces?extra=1", nil)
	r.Header.Set("Authorization", "Bearer only-core-may-use-this-test-token")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	if w.Code != 400 {
		t.Fatalf("expected 400 on query string, got %d", w.Code)
	}

	r = httptest.NewRequest("GET", "/internal/v1/instances/workspaces", nil)
	r.Header.Set("Authorization", "Bearer only-core-may-use-this-test-token")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	if w.Code != 503 {
		t.Fatalf("expected 503 when service is nil, got %d", w.Code)
	}

	req := ReleaseWorkspaceRequest{
		IdempotencyKey: uuid.NewString(),
		StationID:      uuid.NewString(),
		OrganizationID: uuid.NewString(),
		UserID:         uuid.NewString(),
		Namespace:      "verdantflare-blender",
		PVCName:        "blender-pool-001",
	}
	invalid := req
	invalid.Namespace = "kube-system"

	raw, _ := json.Marshal(invalid)
	r = httptest.NewRequest("POST", "/internal/v1/instances/workspaces/release", bytes.NewReader(raw))
	r.Header.Set("Authorization", "Bearer only-core-may-use-this-test-token")
	r.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	if w.Code != 503 {
		t.Fatalf("expected 503 when service is nil, got %d", w.Code)
	}
}

func TestWorkspacesAndReleaseRetainedVolume(t *testing.T) {
	s, in, start, client := destroyFixture(t, true)
	ctx := context.Background()
	for range 3 {
		if _, err := s.Destroy(ctx, in); err != nil {
			t.Fatal(err)
		}
	}
	cleaned := false
	s.WorkspaceCleaner = func(ctx context.Context, namespace, pvcName, nodeName string) error {
		cleaned = true
		return nil
	}
	list, err := s.Workspaces(ctx)
	if err != nil || len(list) != 1 || list[0].State != "retained" || list[0].PVCName != s.Profiles[start.ProfileID].Claims[0] {
		t.Fatalf("expected 1 retained workspace, got %+v err=%v", list, err)
	}

	releaseReq := ReleaseWorkspaceRequest{
		IdempotencyKey: uuid.NewString(),
		StationID:      start.StationID,
		OrganizationID: start.OrganizationID,
		UserID:         start.UserID,
		Namespace:      s.Profiles[start.ProfileID].Namespace,
		PVCName:        s.Profiles[start.ProfileID].Claims[0],
	}
	out, err := s.ReleaseWorkspace(ctx, releaseReq)
	if err != nil || !cleaned || out.State != "released" || out.InstanceID != "" {
		t.Fatalf("expected released workspace after release, got %+v cleaned=%v err=%v", out, cleaned, err)
	}
	if _, err := client.CoreV1().PersistentVolumeClaims(releaseReq.Namespace).Get(ctx, releaseReq.PVCName, metav1.GetOptions{}); err == nil {
		t.Fatalf("expected PVC to be deleted after release")
	}
	pv, err := client.CoreV1().PersistentVolumes().Get(ctx, out.PVName, metav1.GetOptions{})
	if err != nil || pv.Spec.ClaimRef != nil || pv.Status.Phase != corev1.VolumeAvailable {
		t.Fatalf("expected PV claimRef cleared and status Available, got %+v err=%v", pv, err)
	}
	afterList, err := s.Workspaces(ctx)
	if err != nil || len(afterList) != 0 {
		t.Fatalf("expected 0 workspaces in table after PVC deleted, got %+v err=%v", afterList, err)
	}

	// Verify a new instance can now reserve the released workspace (recreating the pool PVC automatically)
	create := start.identity()
	create.InstanceID = uuid.NewString()
	create.CreateOperationID = uuid.NewString()
	if _, err := (&WorkspaceStore{s.Pool, client}).Reserve(ctx, create, s.Profiles[start.ProfileID].StorageProfile); err != nil {
		t.Fatalf("expected released workspace to be reservable, got %v", err)
	}
}

func TestCleanWorkspacePodRemovesPodOnSuccess(t *testing.T) {
	ctx := context.Background()
	client := fake.NewClientset()
	go func() {
		for {
			pods, err := client.CoreV1().Pods("verdantflare-blender").List(ctx, metav1.ListOptions{})
			if err == nil && len(pods.Items) > 0 {
				p := pods.Items[0]
				if p.Status.Phase != corev1.PodSucceeded {
					p.Status.Phase = corev1.PodSucceeded
					_, _ = client.CoreV1().Pods("verdantflare-blender").UpdateStatus(ctx, &p, metav1.UpdateOptions{})
				}
				return
			}
		}
	}()
	err := CleanWorkspacePod(ctx, client, "verdantflare-blender", "blender-pool-001", "5090-node", "registry.example.test/worker:v0.1.0", 1000)
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("CleanWorkspacePod failed: %v", err)
	}
}

func TestInferAppMetaAndWorkspaceNamespacesInUnifiedAppNamespace(t *testing.T) {
	cases := []struct {
		namespace   string
		pool        string
		appID       string
		wantID      string
		wantDisplay string
	}{
		{"verdantflare-app", "comfyui-nvme", "comfyui", "comfyui", "ComfyUI"},
		{"verdantflare-app", "comfyui-nvme", "comfyui-workspace-001", "comfyui", "ComfyUI"},
		{"verdantflare-app", "shared-pool", "comfyui-workspace-002", "comfyui", "ComfyUI"},
		{"verdantflare-app", "blender-nvme", "blender", "blender", "Blender"},
		{"verdantflare-app", "shared-pool", "blender-workspace-001", "blender", "Blender"},
		{"verdantflare-comfyui", "nvme-pool", "slot-a", "comfyui", "ComfyUI"},
		{"verdantflare-blender", "nvme-pool", "slot-a", "blender", "Blender"},
	}
	for _, tc := range cases {
		gotID, gotDisplay := inferAppMeta(tc.namespace, tc.pool, tc.appID)
		if gotID != tc.wantID || gotDisplay != tc.wantDisplay {
			t.Fatalf("inferAppMeta(%q, %q, %q) = (%q, %q), want (%q, %q)", tc.namespace, tc.pool, tc.appID, gotID, gotDisplay, tc.wantID, tc.wantDisplay)
		}
	}

	svc := &Service{
		Namespaces: []string{"verdantflare-app", "verdantflare-comfyui"},
		Profiles: map[string]HelperProfile{
			"blender": {StorageProfile: StorageProfile{Namespace: "verdantflare-app"}},
		},
	}
	gotNS := svc.workspaceNamespaces("verdantflare-blender", "verdantflare-app")
	wantNS := []string{"verdantflare-app", "verdantflare-blender", "verdantflare-comfyui"}
	if len(gotNS) != len(wantNS) {
		t.Fatalf("workspaceNamespaces = %v, want %v", gotNS, wantNS)
	}
	for i := range wantNS {
		if gotNS[i] != wantNS[i] {
			t.Fatalf("workspaceNamespaces[%d] = %q, want %q", i, gotNS[i], wantNS[i])
		}
	}

	ctx := context.Background()
	client := fake.NewClientset()
	seenPriorityClass := make(chan string, 1)
	go func() {
		for {
			pods, err := client.CoreV1().Pods("verdantflare-app").List(ctx, metav1.ListOptions{})
			if err == nil && len(pods.Items) > 0 {
				p := pods.Items[0]
				seenPriorityClass <- p.Spec.PriorityClassName
				p.Status.Phase = corev1.PodSucceeded
				_, _ = client.CoreV1().Pods("verdantflare-app").UpdateStatus(ctx, &p, metav1.UpdateOptions{})
				return
			}
		}
	}()
	if err := CleanWorkspacePod(ctx, client, "verdantflare-app", "blender-workspace-001", "5090-node", "registry.example.test/worker:v0.1.0", 1000); err != nil {
		t.Fatalf("CleanWorkspacePod in verdantflare-app failed: %v", err)
	}
	if got := <-seenPriorityClass; got != "station-app-worker" {
		t.Fatalf("CleanWorkspacePod in verdantflare-app PriorityClassName = %q, want station-app-worker", got)
	}
}
