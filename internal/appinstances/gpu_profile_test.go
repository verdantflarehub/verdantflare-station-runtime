package appinstances

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/verdantflarehub/verdantflare-station-runtime/internal/computepolicy"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
)

func TestLegacyCPUProfileHashZeroDrift(t *testing.T) {
	_, p, _, _ := fixture()
	type legacyProfileShape struct {
		AppID              string            `json:"app_id"`
		AppVersion         string            `json:"app_version"`
		ID                 string            `json:"profile_id"`
		TemplateVersion    string            `json:"template_version"`
		Namespace          string            `json:"namespace"`
		NodeName           string            `json:"node_name"`
		Pool               string            `json:"pool"`
		Claims             []string          `json:"claims"`
		StorageBytes       int64             `json:"storage_bytes"`
		Image              string            `json:"image"`
		Port               int32             `json:"port"`
		RunAsUser          int64             `json:"run_as_user"`
		CPURequestMillis   int64             `json:"cpu_request_millis"`
		CPULimitMillis     int64             `json:"cpu_limit_millis"`
		MemoryRequestBytes int64             `json:"memory_request_bytes"`
		MemoryLimitBytes   int64             `json:"memory_limit_bytes"`
		Env                map[string]string `json:"env"`
		Bindings           map[string]string `json:"bindings"`
	}
	raw, err := json.Marshal(legacyProfileShape{
		AppID: p.AppID, AppVersion: p.AppVersion, ID: p.ID, TemplateVersion: p.TemplateVersion,
		Namespace: p.Namespace, NodeName: p.NodeName, Pool: p.Pool, Claims: p.Claims,
		StorageBytes: p.StorageBytes, Image: p.Image, Port: p.Port, RunAsUser: p.RunAsUser,
		CPURequestMillis: p.CPURequestMillis, CPULimitMillis: p.CPULimitMillis,
		MemoryRequestBytes: p.MemoryRequestBytes, MemoryLimitBytes: p.MemoryLimitBytes,
		Env: p.Env, Bindings: p.Bindings,
	})
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(raw)
	const hexDigits = "0123456789abcdef"
	want := make([]byte, 64)
	for i, b := range sum {
		want[i*2] = hexDigits[b>>4]
		want[i*2+1] = hexDigits[b&15]
	}
	if got := p.Hash(); got != string(want) {
		t.Fatalf("CPU profile hash drifted: got %s, want %s", got, string(want))
	}
	const wantFixedCPUGoldenHash = "9affdb99229b288f58f3bec122f9fe904d405324c01e657a6f29b24f3ab28bbc"
	if got := p.Hash(); got != wantFixedCPUGoldenHash {
		t.Fatalf("fixed CPU golden profile hash drifted: got %s, want %s", got, wantFixedCPUGoldenHash)
	}

	detRun := Run{
		Identity: Identity{
			StationID:      "0195f000-0000-7000-8000-000000000001",
			OrganizationID: "0195f000-0000-7000-8000-000000000002",
			UserID:         "0195f000-0000-7000-8000-000000000003",
			InstallationID: "0195f000-0000-7000-8000-000000000004",
			InstanceID:     "0195f000-0000-7000-8000-000000000005",
		},
		OperationID: "0195f000-0000-7000-8000-000000000006",
		Generation:  1,
	}
	detVol := Volume{
		Identity:    detRun.Identity,
		ProfileHash: p.Hash(),
		PVCName:     "slot-a",
		PVCUID:      "11111111-1111-4111-8111-111111111111",
		PVName:      "pv-slot-a",
		PVUID:       "22222222-2222-4222-8222-222222222222",
	}
	detPod, detSvc, err := WorkerObjects(detRun, p, detVol)
	if err != nil {
		t.Fatal(err)
	}
	const wantFixedCPUSpecHash = "e538485d82ec5c15a6d5e638979e06815943760b1c5a6b04f1fff7be15c3d5ca"
	if detPod.Annotations[specKey] != wantFixedCPUSpecHash || detSvc.Annotations[specKey] != wantFixedCPUSpecHash {
		t.Fatalf("fixed CPU WorkerObjects specKey drifted: got %s, want %s", detPod.Annotations[specKey], wantFixedCPUSpecHash)
	}
}

func TestSharedGPUReadonlyModelsMountAndMultiInstanceSharing(t *testing.T) {
	in1, baseP, pvcA, pvA := fixture()
	p := baseP
	p.ID = "gpu-standard-v1"
	p.TemplateVersion = GPUTemplateVersion
	p.Image = "registry.example.test/app:comfyui-worker-cuda-v0.1.3"
	p.GPUCount = 1
	p.ShmSizeBytes = 8 << 30
	p.SchedulerSecretName = "station-gpu-scheduler-client"
	p.SchedulerSecretKey = "comfyui-token"
	p.Env = map[string]string{
		"COMFYUI_DEVICE":                "cuda",
		"STATION_RUNTIME_SCHEDULER_URL": "http://station-runtime.verdantflare-station.svc.cluster.local:5056",
	}
	hashWithoutModels := p.Hash()

	p.ReadonlyMounts = []ReadonlyMount{{
		Name:      "models",
		ClaimName: "comfyui-models-ro",
		MountPath: "/models",
	}}
	if !p.Valid() {
		t.Fatal("expected GPU profile with /models readonly_mounts to be valid")
	}
	if p.Hash() == hashWithoutModels {
		t.Fatal("readonly_mounts must participate in GPU Profile.Hash()")
	}

	headlessP := p
	headlessP.ID = "gpu-api-headless-v1"
	raw, err := json.Marshal([]Profile{baseP, p, headlessP})
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := LoadProfiles(strings.NewReader(string(raw)))
	if err != nil || len(loaded) != 3 {
		t.Fatalf("LoadProfiles failed loading CPU + GPU profiles with readonly_mounts: %v", err)
	}

	pvcB := pvcA.DeepCopy()
	pvcB.Name = "slot-b"
	pvcB.UID = types.UID(uuid.NewString())
	pvcB.Spec.VolumeName = "pv-slot-b"
	pvB := pvA.DeepCopy()
	pvB.Name = "pv-slot-b"
	pvB.UID = types.UID(uuid.NewString())
	pvB.Spec.ClaimRef.Name = pvcB.Name
	pvB.Spec.ClaimRef.UID = pvcB.UID

	c := client(pvcA, pvA, pvcB, pvB)
	b1 := bind(t, c, in1, p, "slot-a")
	pod1, _, err := WorkerObjects(in1, p, b1)
	if err != nil {
		t.Fatal(err)
	}

	hasModelsMount, hasModelsVol := false, false
	for _, m := range pod1.Spec.Containers[0].VolumeMounts {
		if m.Name == "models" {
			if m.MountPath != "/models" || !m.ReadOnly {
				t.Fatalf("expected ReadOnly /models VolumeMount, got %+v", m)
			}
			hasModelsMount = true
		}
	}
	for _, v := range pod1.Spec.Volumes {
		if v.Name == "models" {
			if v.PersistentVolumeClaim == nil || v.PersistentVolumeClaim.ClaimName != "comfyui-models-ro" || !v.PersistentVolumeClaim.ReadOnly {
				t.Fatalf("expected ReadOnly comfyui-models-ro PVC VolumeSource, got %+v", v)
			}
			hasModelsVol = true
		}
	}
	if !hasModelsMount || !hasModelsVol {
		t.Fatalf("missing /models readonly mount or volume: mount=%v vol=%v", hasModelsMount, hasModelsVol)
	}

	if _, err := EnsureWorker(context.Background(), c, in1, p, b1, Observation{}, acceptObservation); err != nil {
		t.Fatalf("EnsureWorker 1 failed: %v", err)
	}

	// Second GPU instance in the same station sharing comfyui-models-ro must NOT trigger ErrBusy on workspace inspection.
	in2 := Run{
		Identity: Identity{
			StationID:      in1.StationID,
			OrganizationID: in1.OrganizationID,
			UserID:         id(),
			InstallationID: in1.InstallationID,
			InstanceID:     id(),
		},
		OperationID: id(),
		Generation:  1,
	}
	b2 := bind(t, c, in2, headlessP, "slot-b")
	if _, err := EnsureWorker(context.Background(), c, in2, headlessP, b2, Observation{}, acceptObservation); err != nil {
		t.Fatalf("EnsureWorker 2 sharing comfyui-models-ro failed: %v", err)
	}

	for _, invalid := range []func(*Profile){
		func(cand *Profile) { cand.TemplateVersion = TemplateVersion },
		func(cand *Profile) {
			cand.ReadonlyMounts = []ReadonlyMount{{Name: "models", ClaimName: "comfyui-models-ro", MountPath: "/data/models"}}
		},
		func(cand *Profile) {
			cand.ReadonlyMounts = []ReadonlyMount{{Name: "models", ClaimName: "comfyui-models-ro", MountPath: "/models/../etc"}}
		},
		func(cand *Profile) {
			cand.ReadonlyMounts = []ReadonlyMount{{Name: "workspace", ClaimName: "comfyui-models-ro", MountPath: "/models"}}
		},
		func(cand *Profile) {
			cand.ReadonlyMounts = []ReadonlyMount{{Name: "models", ClaimName: "slot-a", MountPath: "/models"}}
		},
		func(cand *Profile) {
			cand.ReadonlyMounts = []ReadonlyMount{
				{Name: "models", ClaimName: "comfyui-models-ro", MountPath: "/models"},
				{Name: "models-2", ClaimName: "comfyui-models-ro-2", MountPath: "/models"},
			}
		},
	} {
		cand := p
		invalid(&cand)
		if cand.Valid() {
			t.Fatalf("invalid readonly_mounts profile accepted: %+v", cand)
		}
	}
}

func TestSharedGPUProfileObjectsAndValidation(t *testing.T) {
	in, p, pvc, pv := fixture()
	p.ID = "shared-gpu"
	p.TemplateVersion = GPUTemplateVersion
	p.Image = "registry.example.test/app:comfyui-worker-cuda-v0.1.3"
	p.GPUCount = 1
	p.ShmSizeBytes = 4 << 30
	p.SchedulerSecretName = "station-gpu-scheduler-tokens"
	p.SchedulerSecretKey = "comfyui-worker-token"
	p.Env = map[string]string{
		"COMFYUI_DEVICE":                "cuda",
		"STATION_RUNTIME_SCHEDULER_URL": "http://station-runtime.verdantflare-station.svc.cluster.local:5056",
	}

	raw, err := json.Marshal([]Profile{p})
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := LoadProfiles(strings.NewReader(string(raw)))
	if err != nil || len(loaded) != 1 {
		t.Fatalf("LoadProfiles rejected valid GPU profile: %v", err)
	}

	c := client(pvc, pv)
	b := bind(t, c, in, p, pvc.Name)
	pod, svc, err := WorkerObjects(in, p, b)
	if err != nil {
		t.Fatal(err)
	}
	if pod.Labels[gpuClientLabel] != "true" {
		t.Fatalf("missing %s label on GPU worker Pod", gpuClientLabel)
	}
	if _, leaked := svc.Spec.Selector[gpuClientLabel]; leaked {
		t.Fatal("Service selector must stay bound to instance-id + start-operation-id")
	}
	container := pod.Spec.Containers[0]
	gpuReq := container.Resources.Requests[gpuResourceName]
	gpuLim := container.Resources.Limits[gpuResourceName]
	if gpuReq.Cmp(resource.MustParse("1")) != 0 || gpuLim.Cmp(resource.MustParse("1")) != 0 {
		t.Fatalf("expected nvidia.com/gpu=1, got req=%v lim=%v", gpuReq, gpuLim)
	}
	hasSchedToken := false
	hasVFContainerID := false
	for _, ev := range container.Env {
		if ev.Name == "NVIDIA_VISIBLE_DEVICES" && ev.Value == "void" {
			t.Fatal("GPU worker must not inject NVIDIA_VISIBLE_DEVICES=void")
		}
		if ev.Name == "VF_CONTAINER_ID" && ev.Value == "pod-$(COMFYUI_POD_UID)-worker" {
			hasVFContainerID = true
		}
		if ev.Name == "STATION_RUNTIME_SCHEDULER_TOKEN" && ev.ValueFrom != nil && ev.ValueFrom.SecretKeyRef != nil {
			if ev.ValueFrom.SecretKeyRef.Name == "station-gpu-scheduler-tokens" && ev.ValueFrom.SecretKeyRef.Key == "comfyui-worker-token" {
				hasSchedToken = true
			}
		}
	}
	if !hasSchedToken {
		t.Fatal("missing STATION_RUNTIME_SCHEDULER_TOKEN secretKeyRef")
	}
	if !hasVFContainerID {
		t.Fatal("missing VF_CONTAINER_ID downward-API binding on GPU worker")
	}
	hasShmMount, hasShmVol := false, false
	for _, m := range container.VolumeMounts {
		if m.Name == "shm" && m.MountPath == "/dev/shm" {
			hasShmMount = true
		}
	}
	for _, v := range pod.Spec.Volumes {
		if v.Name == "shm" && v.EmptyDir != nil && v.EmptyDir.Medium == corev1.StorageMediumMemory {
			if v.EmptyDir.SizeLimit != nil && v.EmptyDir.SizeLimit.Cmp(resource.MustParse("4Gi")) == 0 {
				hasShmVol = true
			}
		}
	}
	if !hasShmMount || !hasShmVol {
		t.Fatalf("expected 4Gi Memory /dev/shm mount+volume, got mount=%v vol=%v", hasShmMount, hasShmVol)
	}

	t.Setenv(computepolicy.Environment, computepolicy.Unrestricted)
	actual := pod.DeepCopy()
	actual.UID = types.UID(uuid.NewString())
	computepolicy.Apply(&actual.Spec)
	if !podMatches(actual, pod, string(actual.UID), p.NodeName) {
		t.Fatal("unrestricted CPU/memory policy broke GPU worker podMatches")
	}
	if gotGPU := actual.Spec.Containers[0].Resources.Limits[gpuResourceName]; gotGPU.Cmp(resource.MustParse("1")) != 0 {
		t.Fatal("computepolicy stripped nvidia.com/gpu")
	}

	for _, invalid := range []func(*Profile){
		func(cand *Profile) { cand.TemplateVersion = TemplateVersion },
		func(cand *Profile) { cand.GPUCount = 0 },
		func(cand *Profile) { cand.GPUCount = 3 },
		func(cand *Profile) { cand.ShmSizeBytes = 1024 },
		func(cand *Profile) { cand.SchedulerSecretKey = "" },
		func(cand *Profile) { cand.SchedulerSecretName = "BAD_SECRET" },
	} {
		cand := p
		invalid(&cand)
		if cand.Valid() {
			t.Fatalf("invalid GPU profile accepted: %+v", cand)
		}
	}
}

func TestEvaluateTemplateUpgradeDefersProtectedAndDirtyInstances(t *testing.T) {
	j, in, _, _ := journalFixture(t)
	ctx := context.Background()
	podUID := uuid.NewString()
	bootID := uuid.NewString()
	_, err := j.Pool.Exec(ctx, `
		INSERT INTO station.gpu_instance_residency(
			station_id, instance_id, app_id, node_uid, pod_uid, container_name, container_id,
			boot_id, gpu_epoch, residency_seq, gpu_uuid, residency_state,
			human_protected, gui_connection_alive, dirty, snapshot_in_progress, snapshot_required, blocking_reason
		) VALUES ($1, $2, 'blender', 'node-1', $3, 'worker', 'cid-1', $4, 1, 1, 'GPU-0', 'human_protected', true, true, true, false, true, 'human_gui_active')
	`, in.StationID, in.InstanceID, podUID, bootID)
	if err != nil {
		t.Fatal(err)
	}
	gate, err := EvaluateTemplateUpgrade(ctx, j.Pool, in.StationID, in.InstanceID)
	if !errors.Is(err, ErrUpgradeDeferred) || gate.Allowed || !gate.Deferred || !gate.HumanProtected {
		t.Fatalf("expected human_protected instance upgrade to be deferred, got gate=%+v err=%v", gate, err)
	}

	_, err = j.Pool.Exec(ctx, `
		UPDATE station.gpu_instance_residency
		   SET residency_state='idle_evictable', human_protected=false, gui_connection_alive=false,
		       dirty=true, snapshot_required=true, blocking_reason='none'
		 WHERE station_id=$1 AND instance_id=$2
	`, in.StationID, in.InstanceID)
	if err != nil {
		t.Fatal(err)
	}
	gate, err = EvaluateTemplateUpgrade(ctx, j.Pool, in.StationID, in.InstanceID)
	if !errors.Is(err, ErrUpgradeDeferred) || gate.Allowed || !gate.Deferred {
		t.Fatalf("expected dirty uncheckpointed instance upgrade to be deferred, got gate=%+v err=%v", gate, err)
	}

	_, err = j.Pool.Exec(ctx, `
		UPDATE station.gpu_instance_residency
		   SET residency_state='zero_vram_standby', dirty=true, snapshot_required=false, blocking_reason='none'
		 WHERE station_id=$1 AND instance_id=$2
	`, in.StationID, in.InstanceID)
	if err != nil {
		t.Fatal(err)
	}
	gate, err = EvaluateTemplateUpgrade(ctx, j.Pool, in.StationID, in.InstanceID)
	if err != nil || !gate.Allowed || gate.Deferred {
		t.Fatalf("expected checkpointed zero_vram_standby instance to allow upgrade, got gate=%+v err=%v", gate, err)
	}
}
