package main

import (
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
