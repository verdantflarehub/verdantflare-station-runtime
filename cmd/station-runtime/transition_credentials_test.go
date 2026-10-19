package main

import (
	"crypto/sha256"
	"github.com/verdantflarehub/verdantflare-station-runtime/internal/appinstances"
	"github.com/verdantflarehub/verdantflare-station-runtime/internal/executor"
	"testing"
)

func TestDashboardTransitionCredentialsWithoutMaintenance(t *testing.T) {
	if !transitionCredentialsDistinct("control", "instance", nil, nil) {
		t.Fatal("dashboard-only transition rejected")
	}
	if !transitionCredentialsDistinct("", "", nil, nil) {
		t.Fatal("disabled transition rejected")
	}
	if transitionCredentialsDistinct("shared", "shared", nil, nil) {
		t.Fatal("instance credential reuse accepted")
	}
	callers := appinstances.AccessRegistry{sha256.Sum256([]byte("Bearer control")): {}}
	if transitionCredentialsDistinct("control", "instance", nil, callers) {
		t.Fatal("app caller reuse accepted")
	}
	maintenance := &executor.HTTPManagementMaintenance{Releases: map[string]executor.ManagementMaintenanceRelease{"app": {Token: "instance"}}}
	if transitionCredentialsDistinct("control", "instance", maintenance, nil) {
		t.Fatal("maintenance credential reuse accepted")
	}
}
