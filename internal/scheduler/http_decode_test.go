package scheduler

import (
	"strings"
	"testing"
)

func TestDecodeJSONObjectAllowsNullAndExplicitZeroOptionalFields(t *testing.T) {
	raw := `{
		"instance_id": "01a1dfaf-207f-7af4-a872-736f2fc7867f",
		"node_uid": "28397617-c8a3-4735-ac32-64303f9f1ec4",
		"pod_uid": "6a913182-ccf8-403d-9aba-6b04cbdc435a",
		"container_name": "app",
		"container_id": "",
		"boot_id": "91bb34a4-0c2a-40af-9553-b2a2cee970cf",
		"gpu_epoch": 1,
		"residency_seq": 1,
		"gpu_uuid": "GPU-29a56c24-484e-d51d-fac9-b141be9e24c5",
		"residency_state": "zero_vram_standby",
		"human_protected": false,
		"gui_connection_alive": false,
		"dirty": false,
		"snapshot_in_progress": false,
		"snapshot_required": false,
		"blocking_reason": "standby_zero_vram",
		"pinned_budget_mib": 0,
		"vram_allocated_mib": null,
		"idle_seconds": 5,
		"callback_endpoint": "http://comfyui01a1dfaf207f7af4a872736f2fc7867f.verdantflare-app.svc.cluster.local:80",
		"template_upgraded": null
	}`
	var req ResidencyReportRequest
	if err := DecodeJSONObject(strings.NewReader(raw), maxBodyBytes, &req); err != nil {
		t.Fatalf("expected DecodeJSONObject to accept explicit zero/null optional fields, got %v", err)
	}
	if req.PinnedBudgetMiB != 0 || req.VRAMAllocatedMiB != nil || req.TemplateUpgraded != nil {
		t.Fatalf("unexpected decoded values: %+v", req)
	}

	var acquireReq AcquireRequest
	if err := DecodeJSONObject(strings.NewReader(`{
		"idempotency_key": "idem-1",
		"attempt_id": "att-1",
		"k8s_slots_required": 0,
		"candidate_gpu_uuids": [],
		"headroom_override_mib": null,
		"source_lease_id": ""
	}`), maxBodyBytes, &acquireReq); err != nil {
		t.Fatalf("expected DecodeJSONObject to accept explicit zero/empty/null AcquireRequest fields, got %v", err)
	}

	var admReq AdmissionModeRequest
	if err := DecodeJSONObject(strings.NewReader(`{
		"idempotency_key": "adm-gpu1-drain",
		"node_uid": "28397617-c8a3-4735-ac32-64303f9f1ec4",
		"gpu_uuid": "GPU-798fb508-830f-b538-6733-72ed6d210d40",
		"admission_mode": "drain_only",
		"reason": "t4_s3_single_card_downgrade_proof"
	}`), maxBodyBytes, &admReq); err != nil {
		t.Fatalf("expected DecodeJSONObject to accept per-card AdmissionModeRequest fields, got %v", err)
	}
	if admReq.GPUUUID != "GPU-798fb508-830f-b538-6733-72ed6d210d40" || admReq.AdmissionMode != "drain_only" {
		t.Fatalf("unexpected AdmissionModeRequest values: %+v", admReq)
	}
}

func TestDecodeJSONObjectRejectsMalformedUnknownDuplicateAndTrailingTokens(t *testing.T) {
	badInputs := []string{
		`[]`,
		`null`,
		`"str"`,
		`{"idempotency_key":"a","attempt_id":"1","unknown_field":true}`,
		`{"idempotency_key":"a","idempotency_key":"b","attempt_id":"1"}`,
		`{"idempotency_key":"a","attempt_id":"1"}{}`,
	}
	for _, input := range badInputs {
		var req AcquireRequest
		if err := DecodeJSONObject(strings.NewReader(input), maxBodyBytes, &req); err == nil {
			t.Fatalf("expected DecodeJSONObject to reject %q", input)
		}
	}
}
