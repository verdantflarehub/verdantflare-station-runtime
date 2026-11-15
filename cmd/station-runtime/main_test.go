package main

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/verdantflarehub/verdantflare-station-runtime/internal/appinstances"
	"github.com/verdantflarehub/verdantflare-station-runtime/internal/instances"
)

func TestSelectCleanerTargetInUnifiedAppNamespace(t *testing.T) {
	profiles := map[string]instances.HelperProfile{
		"blender": {
			StorageProfile: instances.StorageProfile{
				Namespace: "verdantflare-app",
				Claims:    []string{"blender-workspace-001", "blender-workspace-002"},
			},
			Image: "registry.example.test/app:blender-worker-v0.1.0",
		},
	}
	appProfiles := map[string]appinstances.Profile{
		"comfyui": {
			Namespace: "verdantflare-app",
			Claims:    []string{"comfyui-workspace-001", "comfyui-workspace-002"},
			Image:     "registry.example.test/app:comfyui-worker-v0.1.4",
			RunAsUser: 10001,
		},
	}

	img, uid, ok := selectCleanerTarget(profiles, appProfiles, "verdantflare-app", "comfyui-workspace-002")
	if !ok || img != "registry.example.test/app:comfyui-worker-v0.1.4" || uid != 10001 {
		t.Fatalf("expected comfyui cleaner profile for comfyui-workspace-002, got img=%q uid=%d ok=%v", img, uid, ok)
	}

	img, uid, ok = selectCleanerTarget(profiles, appProfiles, "verdantflare-app", "comfyui-dynamic-009")
	if !ok || img != "registry.example.test/app:comfyui-worker-v0.1.4" || uid != 10001 {
		t.Fatalf("expected comfyui cleaner profile by prefix for comfyui-dynamic-009, got img=%q uid=%d ok=%v", img, uid, ok)
	}

	img, uid, ok = selectCleanerTarget(profiles, appProfiles, "verdantflare-app", "blender-workspace-001")
	if !ok || img != "registry.example.test/app:blender-worker-v0.1.0" || uid != 1000 {
		t.Fatalf("expected blender cleaner profile for blender-workspace-001, got img=%q uid=%d ok=%v", img, uid, ok)
	}

	if _, _, ok := selectCleanerTarget(profiles, appProfiles, "verdantflare-unknown", "unknown-001"); ok {
		t.Fatal("expected unknown namespace to return false")
	}
}

func TestMultiplexModelCacheHandlerRoutesCleanly(t *testing.T) {
	mux := multiplexModelCacheHandler(
		http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(201)
		}),
		http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(202)
		}),
	)

	rec1 := httptest.NewRecorder()
	mux.ServeHTTP(rec1, httptest.NewRequest(http.MethodGet, "/internal/v1/model-cache/models", nil))
	if rec1.Code != 202 {
		t.Fatalf("expected /internal/v1/model-cache/* routed to modelHandler (202), got %d", rec1.Code)
	}

	rec2 := httptest.NewRecorder()
	mux.ServeHTTP(rec2, httptest.NewRequest(http.MethodPost, "/internal/v1/app-instances/start", nil))
	if rec2.Code != 201 {
		t.Fatalf("expected /internal/v1/app-instances/* routed to primary (201), got %d", rec2.Code)
	}
}
