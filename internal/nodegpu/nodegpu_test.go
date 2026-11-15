package nodegpu

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/verdantflarehub/verdantflare-station-runtime/internal/scheduler"
	"google.golang.org/protobuf/encoding/protowire"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
)

type stubCRISource struct {
	status map[string]CRIContainerStatus
	err    error
}

func (s *stubCRISource) ContainerStatus(_ context.Context, id string) (CRIContainerStatus, error) {
	if s.err != nil {
		return CRIContainerStatus{}, s.err
	}
	st, ok := s.status[id]
	if !ok {
		return CRIContainerStatus{}, ErrContainerNotFound
	}
	return st, nil
}

type stubNVML struct {
	snap NVMLSnapshot
	err  error
}

func (s stubNVML) Sample(_ context.Context) (NVMLSnapshot, error) {
	if s.err != nil {
		return NVMLSnapshot{}, s.err
	}
	return s.snap, nil
}

type fakeProcOpts struct {
	bootID       string
	gpuEpoch     *int64
	hasNvidiaFD  bool
	startTicks   uint64
	containerPID int
	pidNS        string
}

func writeFakeProc(t *testing.T, root string, pid, ppid int, comm, state, cgroup, cmdline string, opts ...fakeProcOpts) {
	t.Helper()
	var opt fakeProcOpts
	if len(opts) > 0 {
		opt = opts[0]
	}
	if opt.startTicks == 0 {
		opt.startTicks = 123456
	}
	dir := filepath.Join(root, strconv.Itoa(pid))
	if err := os.MkdirAll(filepath.Join(dir, "fd"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(dir, "ns"), 0o755); err != nil {
		t.Fatal(err)
	}
	// Fields after ") ": 1=state, 2=ppid, 3..19=zeros, 20=starttimeTicks (field 22 overall).
	zeros := strings.Repeat(" 0", 17)
	statLine := strconv.Itoa(pid) + " (" + comm + ") " + state + " " + strconv.Itoa(ppid) + zeros + " " + strconv.FormatUint(opt.startTicks, 10) + "\n"
	if err := os.WriteFile(filepath.Join(dir, "stat"), []byte(statLine), 0o644); err != nil {
		t.Fatal(err)
	}
	nsPIDLine := "Name:\t" + comm + "\nNSpid:\t" + strconv.Itoa(pid)
	if opt.containerPID > 0 && opt.containerPID != pid {
		nsPIDLine += "\t" + strconv.Itoa(opt.containerPID)
	}
	nsPIDLine += "\n"
	if err := os.WriteFile(filepath.Join(dir, "status"), []byte(nsPIDLine), 0o644); err != nil {
		t.Fatal(err)
	}
	pidNS := opt.pidNS
	if pidNS == "" {
		pidNS = "pid:[4026531836]"
	}
	nsPath := filepath.Join(dir, "ns", "pid")
	_ = os.Remove(nsPath)
	if err := os.Symlink(pidNS, nsPath); err != nil {
		if wErr := os.WriteFile(nsPath, []byte(pidNS+"\n"), 0o644); wErr != nil {
			t.Fatal(wErr)
		}
	}
	if err := os.WriteFile(filepath.Join(dir, "cgroup"), []byte(cgroup), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "cmdline"), []byte(strings.ReplaceAll(cmdline, " ", "\x00")), 0o644); err != nil {
		t.Fatal(err)
	}
	var envParts []string
	if opt.bootID != "" {
		envParts = append(envParts, "VF_BOOT_ID="+opt.bootID)
	}
	if opt.gpuEpoch != nil {
		envParts = append(envParts, "VF_GPU_EPOCH="+strconv.FormatInt(*opt.gpuEpoch, 10))
	}
	if len(envParts) > 0 {
		if err := os.WriteFile(filepath.Join(dir, "environ"), []byte(strings.Join(envParts, "\x00")+"\x00"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	fdPath := filepath.Join(dir, "fd", "3")
	_ = os.Remove(fdPath)
	if opt.hasNvidiaFD {
		if err := os.Symlink("/dev/nvidia0", fdPath); err != nil {
			if wErr := os.WriteFile(fdPath, []byte("/dev/nvidia0\n"), 0o644); wErr != nil {
				t.Fatal(wErr)
			}
		}
	}
}

func TestCRIProxyReadOnlyWhitelistTombstonesAndWireCodec(t *testing.T) {
	cid := strings.Repeat("a", 64)
	podUID := "3a8e11b2-4412-4c9a-9f88-112233445566"

	// Encode ContainerMetadata (field 2) and labels map entry (field 12: io.kubernetes.pod.uid -> podUID).
	var metaField []byte
	metaField = protowire.AppendTag(metaField, 1, protowire.BytesType)
	metaField = protowire.AppendString(metaField, "worker")
	metaField = protowire.AppendTag(metaField, 2, protowire.VarintType)
	metaField = protowire.AppendVarint(metaField, 3)

	var labelEntry []byte
	labelEntry = protowire.AppendTag(labelEntry, 1, protowire.BytesType)
	labelEntry = protowire.AppendString(labelEntry, "io.kubernetes.pod.uid")
	labelEntry = protowire.AppendTag(labelEntry, 2, protowire.BytesType)
	labelEntry = protowire.AppendString(labelEntry, podUID)

	var statusField []byte
	statusField = protowire.AppendTag(statusField, 1, protowire.BytesType)
	statusField = protowire.AppendString(statusField, cid)
	statusField = protowire.AppendTag(statusField, 2, protowire.BytesType)
	statusField = protowire.AppendBytes(statusField, metaField)
	statusField = protowire.AppendTag(statusField, 3, protowire.VarintType)
	statusField = protowire.AppendVarint(statusField, 2) // CONTAINER_EXITED
	statusField = protowire.AppendTag(statusField, 6, protowire.VarintType)
	statusField = protowire.AppendVarint(statusField, 1731600000)
	statusField = protowire.AppendTag(statusField, 7, protowire.VarintType)
	statusField = protowire.AppendVarint(statusField, 0)
	statusField = protowire.AppendTag(statusField, 12, protowire.BytesType)
	statusField = protowire.AppendBytes(statusField, labelEntry)

	var respWire []byte
	respWire = protowire.AppendTag(respWire, 1, protowire.BytesType)
	respWire = protowire.AppendBytes(respWire, statusField)

	decoded, err := decodeContainerStatusResponse(respWire)
	if err != nil {
		t.Fatalf("decodeContainerStatusResponse: %v", err)
	}
	if !decoded.Exited || decoded.State != "CONTAINER_EXITED" || decoded.ContainerID != cid || decoded.PodUID != podUID || decoded.ContainerName != "worker" {
		t.Fatalf("unexpected decoded status: %+v", decoded)
	}
	if len(encodeContainerStatusRequest(cid)) == 0 {
		t.Fatal("expected non-empty encoded request")
	}

	tombDir := t.TempDir()
	criSrc := &stubCRISource{
		status: map[string]CRIContainerStatus{
			cid: decoded,
		},
	}
	srv := httptest.NewServer(CRIProxyHandler{
		Source:     criSrc,
		Tombstones: TombstoneStore{Dir: tombDir},
	})
	defer srv.Close()

	// Forbidden mutating routes/methods must fail 403.
	for _, tc := range []struct {
		method string
		path   string
		want   int
	}{
		{http.MethodPost, "/v1/containers/" + cid + "/exec", http.StatusForbidden},
		{http.MethodDelete, "/v1/containers/" + cid, http.StatusForbidden},
		{http.MethodPost, "/v1/containers/" + cid + "/status", http.StatusForbidden},
		{http.MethodGet, "/v1/containers/bad/nested/status", http.StatusBadRequest},
	} {
		req, _ := http.NewRequest(tc.method, srv.URL+tc.path, nil)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != tc.want {
			t.Fatalf("%s %s: got %d, want %d", tc.method, tc.path, resp.StatusCode, tc.want)
		}
	}

	client := HTTPCRIStatusClient{BaseURL: srv.URL}
	got, err := client.ContainerStatus(context.Background(), "containerd://"+cid)
	if err != nil {
		t.Fatalf("HTTPCRIStatusClient: %v", err)
	}
	if !got.Exited || got.ContainerID != cid || got.PodUID != podUID {
		t.Fatalf("unexpected status via HTTPCRIStatusClient: %+v", got)
	}

	// Simulate CRI garbage-collecting the exited container -> durable tombstone still proves exit!
	delete(criSrc.status, cid)
	gotAfterGC, err := client.ContainerStatus(context.Background(), cid)
	if err != nil {
		t.Fatalf("expected tombstone fallback after CRI GC, got error: %v", err)
	}
	if !gotAfterGC.Exited || !gotAfterGC.Tombstone || gotAfterGC.PodUID != podUID {
		t.Fatalf("unexpected tombstone status: %+v", gotAfterGC)
	}

	// Unknown container without tombstone must return ErrContainerNotFound (404), NOT 503.
	missingCID := strings.Repeat("f", 64)
	_, errMissing := client.ContainerStatus(context.Background(), missingCID)
	if !errors.Is(errMissing, ErrContainerNotFound) {
		t.Fatalf("expected ErrContainerNotFound on missing container, got %v", errMissing)
	}
}

func TestNvidiaSMIXMLAndCSVParser(t *testing.T) {
	// P1-H3: Atomic nvidia-smi -q -x XML captures BOTH Compute ("C") and Graphics ("G") processes!
	xmlSample := []byte(`<?xml version="1.0" ?>
<nvidia_smi_log>
  <gpu id="00000000:01:00.0">
    <product_name>NVIDIA GeForce RTX 5090</product_name>
    <uuid>GPU-0000-alpha</uuid>
    <minor_number>0</minor_number>
    <fb_memory_usage>
      <total>32607 MiB</total>
      <used>18607 MiB</used>
      <free>14000 MiB</free>
    </fb_memory_usage>
    <processes>
      <process_info>
        <pid>4120</pid>
        <type>C</type>
        <used_memory>12288 MiB</used_memory>
      </process_info>
      <process_info>
        <pid>5500</pid>
        <type>G</type>
        <used_memory>6144 MiB</used_memory>
      </process_info>
      <process_info>
        <pid>5500</pid>
        <type>C</type>
        <used_memory>6144 MiB</used_memory>
      </process_info>
    </processes>
  </gpu>
</nvidia_smi_log>`)
	snap, err := parseNvidiaSMIXML(xmlSample)
	if err != nil {
		t.Fatalf("parseNvidiaSMIXML: %v", err)
	}
	if len(snap.Devices) != 1 || snap.Devices[0].MeasuredFreeVRAMMiB != 14000 {
		t.Fatalf("unexpected XML devices: %+v", snap.Devices)
	}
	if len(snap.Processes) != 2 || snap.Processes[0].PID != 4120 || snap.Processes[1].PID != 5500 || snap.Processes[1].ProcessType != "C+G" || snap.Processes[1].UsedVRAMMiB != 6144 {
		t.Fatalf("expected Compute + Graphics processes merged, got %+v", snap.Processes)
	}

	gpuCSV := []byte("GPU-0000-alpha, 0, NVIDIA GeForce RTX 5090, 32607, 29800, 2807\nGPU-0001-beta, 1, NVIDIA GeForce RTX 5090, 32607, 31900, 707\n")
	devs, err := parseNvidiaSMIGPUCSV(gpuCSV)
	if err != nil || len(devs) != 2 {
		t.Fatalf("parseNvidiaSMIGPUCSV: %v (%+v)", err, devs)
	}
	procCSV := []byte("GPU-0000-alpha, 4120, 8192\nGPU-0001-beta, 5011, [N/A]\n")
	procs, err := parseNvidiaSMIProcsCSV(procCSV)
	if err != nil || len(procs) != 2 {
		t.Fatalf("parseNvidiaSMIProcsCSV: %v (%+v)", err, procs)
	}
}

func TestAgentVerifyTerminatedModesAndFailClosed(t *testing.T) {
	procRoot := t.TempDir()
	tombDir := t.TempDir()
	podUID := "3a8e11b2-4412-4c9a-9f88-112233445566"
	cid := strings.Repeat("b", 64)
	systemdCgroup := "0::/kubepods.slice/kubepods-burstable.slice/kubepods-burstable-pod3a8e11b2_4412_4c9a_9f88_112233445566.slice/cri-containerd-" + cid + ".scope\n"

	epoch1 := int64(1)
	// Platform supervisor stays alive without /dev/nvidia* fd; child worker has VF_GPU_EPOCH=1 and /dev/nvidia0 fd!
	// Notice we give child PID 205 a spoofed cmdline "python3 -m worker.server" to prove P1-H3 cmdline bypass is dead!
	writeFakeProc(t, procRoot, 100, 1, "python3", "S", systemdCgroup, "python3 -m worker.server", fakeProcOpts{
		bootID:      "boot-1",
		hasNvidiaFD: false,
	})
	writeFakeProc(t, procRoot, 205, 100, "blender (gpu)", "R", systemdCgroup, "python3 -m worker.server", fakeProcOpts{
		bootID:      "boot-1",
		gpuEpoch:    &epoch1,
		hasNvidiaFD: true,
	})

	token := strings.Repeat("t", 40)
	criSrc := &stubCRISource{
		status: map[string]CRIContainerStatus{
			cid: {ContainerID: cid, PodUID: podUID, State: "CONTAINER_RUNNING", Exited: false},
		},
	}
	agent := &Agent{
		NodeUID:      "node-5090-01",
		NodeName:     "k3s-5090-dev",
		BearerToken:  token,
		TombstoneDir: tombDir,
		NVML: stubNVML{
			snap: NVMLSnapshot{
				SampledAt: time.Now().UTC(),
				Devices: []GPUDeviceObservation{
					{GPUUUID: "GPU-0", GPUIndex: 0, ModelName: "NVIDIA GeForce RTX 5090", TotalVRAMMiB: 32607, MeasuredFreeVRAMMiB: 18000, MeasuredUsedVRAMMiB: 14607, NVMLHealthy: true},
				},
				Processes: []GPUProcessObservation{
					{GPUUUID: "GPU-0", PID: 205, ProcessType: "G", UsedVRAMMiB: 12288},
				},
			},
		},
		Proc: ProcInspector{ProcRoot: procRoot},
		CRI:  criSrc,
	}

	srv := httptest.NewServer(agent)
	defer srv.Close()

	// 1. Unauthorized request must fail 401.
	unauthResp, err := http.Post(srv.URL+"/internal/v1/node-gpu/verify-terminated", "application/json", strings.NewReader(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	_ = unauthResp.Body.Close()
	if unauthResp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("got %d, want 401", unauthResp.StatusCode)
	}

	verifier := scheduler.HTTPNodeVerifier{
		DefaultURL:  srv.URL,
		BearerToken: token,
	}

	// 2. P1-H3: Even with spoofed cmdline "python3 -m worker.server", PID 205 holds GPU-0 + /dev/nvidia0 + VF_GPU_EPOCH=1 -> NOT verified!
	res1, err := verifier.VerifyTerminated(context.Background(), scheduler.VerifyTerminatedRequest{
		NodeUID:                  "node-5090-01",
		GPUUUID:                  "GPU-0",
		PodUID:                   podUID,
		ContainerID:              cid,
		BootID:                   "boot-1",
		GPUEpoch:                 1,
		EpochFenceConfirmed:      true,
		SupervisorPID:            100,
		SupervisorStarttimeTicks: 123456,
		Mode:                     "epoch_unload",
	})
	if err != nil {
		t.Fatal(err)
	}
	if res1.Verified || res1.GPUProcessCount != 1 || res1.CgroupWorkerCount != 1 {
		t.Fatalf("expected unverified while worker active, got %+v", res1)
	}

	// 3. Put PID 205 into unkillable D-state -> UnkillableDState must be true!
	writeFakeProc(t, procRoot, 205, 100, "blender (gpu)", "D", systemdCgroup, "python3 -m worker.server", fakeProcOpts{
		bootID:      "boot-1",
		gpuEpoch:    &epoch1,
		hasNvidiaFD: true,
	})
	resD, err := verifier.VerifyTerminated(context.Background(), scheduler.VerifyTerminatedRequest{
		NodeUID:                  "node-5090-01",
		GPUUUID:                  "GPU-0",
		PodUID:                   podUID,
		ContainerID:              cid,
		BootID:                   "boot-1",
		GPUEpoch:                 1,
		EpochFenceConfirmed:      true,
		SupervisorPID:            100,
		SupervisorStarttimeTicks: 123456,
		Mode:                     "epoch_unload",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !resD.UnkillableDState || resD.Verified {
		t.Fatalf("expected UnkillableDState=true, got %+v", resD)
	}

	// 4. Remove child worker PID 205 while supervisor PID 100 stays alive (no nvidia fd, VF_BOOT_ID=boot-1):
	//    - With EpochFenceConfirmed=false -> MUST refuse Verified (EPOCH_FENCE_UNCONFIRMED) (P1-H2)!
	//    - With EpochFenceConfirmed=true -> Verified=true!
	_ = os.RemoveAll(filepath.Join(procRoot, "205"))
	agent.NVML = stubNVML{
		snap: NVMLSnapshot{
			SampledAt: time.Now().UTC(),
			Devices: []GPUDeviceObservation{
				{GPUUUID: "GPU-0", GPUIndex: 0, ModelName: "NVIDIA GeForce RTX 5090", TotalVRAMMiB: 32607, MeasuredFreeVRAMMiB: 31900, MeasuredUsedVRAMMiB: 707, NVMLHealthy: true},
			},
			Processes: nil,
		},
	}
	resUnconfirmed, err := verifier.VerifyTerminated(context.Background(), scheduler.VerifyTerminatedRequest{
		NodeUID:                  "node-5090-01",
		GPUUUID:                  "GPU-0",
		PodUID:                   podUID,
		ContainerID:              cid,
		BootID:                   "boot-1",
		GPUEpoch:                 1,
		EpochFenceConfirmed:      false,
		SupervisorPID:            100,
		SupervisorStarttimeTicks: 123456,
		Mode:                     "epoch_unload",
	})
	if err != nil || resUnconfirmed.Verified || resUnconfirmed.ErrorCode != "EPOCH_FENCE_UNCONFIRMED" {
		t.Fatalf("expected EPOCH_FENCE_UNCONFIRMED when EpochFenceConfirmed=false, got %+v (%v)", resUnconfirmed, err)
	}
	resSettled, err := verifier.VerifyTerminated(context.Background(), scheduler.VerifyTerminatedRequest{
		NodeUID:                  "node-5090-01",
		GPUUUID:                  "GPU-0",
		PodUID:                   podUID,
		ContainerID:              cid,
		BootID:                   "boot-1",
		GPUEpoch:                 1,
		EpochFenceConfirmed:      true,
		SupervisorPID:            100,
		SupervisorStarttimeTicks: 123456,
		Mode:                     "epoch_unload",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !resSettled.Verified || !resSettled.TombstoneFound || resSettled.MeasuredFreeVRAMMiB != 31900 {
		t.Fatalf("expected settled epoch_unload, got %+v", resSettled)
	}

	// 6. P1-H2: CRI PodUID mismatch must fail closed with ErrNodeStateUnknown!
	criSrc.status[cid] = CRIContainerStatus{ContainerID: cid, PodUID: "00000000-0000-0000-0000-999999999999", State: "CONTAINER_EXITED", Exited: true, FinishedAt: 1731600010}
	_, errMismatch := verifier.VerifyTerminated(context.Background(), scheduler.VerifyTerminatedRequest{
		NodeUID:     "node-5090-01",
		GPUUUID:     "GPU-0",
		PodUID:      podUID,
		ContainerID: cid,
		Mode:        "container_exit",
	})
	if !errors.Is(errMismatch, scheduler.ErrNodeStateUnknown) {
		t.Fatalf("expected ErrNodeStateUnknown on CRI PodUID mismatch, got %v", errMismatch)
	}

	// 7. NVML failure must return 503 UNKNOWN_NODE_STATE (fail-closed).
	agent.NVML = stubNVML{err: errors.New("nvml lost")}
	_, errFail := verifier.VerifyTerminated(context.Background(), scheduler.VerifyTerminatedRequest{
		NodeUID:     "node-5090-01",
		GPUUUID:     "GPU-0",
		PodUID:      podUID,
		ContainerID: cid,
		Mode:        "container_exit",
	})
	if !errors.Is(errFail, scheduler.ErrNodeStateUnknown) {
		t.Fatalf("expected ErrNodeStateUnknown on NVML error, got %v", errFail)
	}
}

func TestObserveNamespaceGPUPodsAndResolveNodeUID(t *testing.T) {
	gpuQty := resource.MustParse("1")
	fakeClient := fake.NewSimpleClientset(
		&corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name: "verdentflare-5090",
				UID:  types.UID("node-real-k8s-uid-5090"),
			},
		},
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "video-running",
				Namespace: "verdantflare-app",
				UID:       types.UID("pod-running-1"),
			},
			Spec: corev1.PodSpec{
				NodeName: "verdentflare-5090",
				Containers: []corev1.Container{
					{Name: "worker", Resources: corev1.ResourceRequirements{Limits: corev1.ResourceList{corev1.ResourceName("nvidia.com/gpu"): gpuQty}}},
				},
			},
			Status: corev1.PodStatus{Phase: corev1.PodRunning},
		},
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "image-unscheduled-pending",
				Namespace: "verdantflare-app",
				UID:       types.UID("pod-unscheduled-2"),
			},
			Spec: corev1.PodSpec{
				NodeName: "", // Unscheduled Pending GPU pod!
				Containers: []corev1.Container{
					{Name: "worker", Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceName("nvidia.com/gpu"): gpuQty}}},
				},
			},
			Status: corev1.PodStatus{Phase: corev1.PodPending},
		},
	)

	uid, err := ResolveNodeUID(context.Background(), fakeClient, "verdentflare-5090")
	if err != nil || uid != "node-real-k8s-uid-5090" {
		t.Fatalf("ResolveNodeUID got %q (%v), want node-real-k8s-uid-5090", uid, err)
	}

	counter := KubeNodeSlotCounter{Client: fakeClient, Namespace: "verdantflare-app"}
	obs, err := counter.ObserveNamespaceGPUPods(context.Background(), "verdentflare-5090")
	if err != nil {
		t.Fatalf("ObserveNamespaceGPUPods: %v", err)
	}
	if obs.AllocatedOnNode != 1 || obs.UnscheduledPending != 1 || len(obs.ActivePods) != 2 {
		t.Fatalf("unexpected K8s slot observation: %+v", obs)
	}
}

func TestCollectorHashChainCrashRecoveryAndStrictAudit(t *testing.T) {
	procRoot := t.TempDir()
	spoolDir := t.TempDir()
	writeFakeProc(t, procRoot, 310, 1, "python3", "R", "0::/kubepods/pod11111111-2222-3333-4444-555555555555\n", "python3 worker.py")

	pinnedDigest := "sha256:12377b916aaf28b7a5c7215eb6f29db95270b425ba011f00266a00c224864433"
	pinnedImageRef := "registry.cn-qingdao.aliyuncs.com/wod/verdantflare-station:station-runtime-v0.2.77@" + pinnedDigest

	// StrictAudit without S3/Spool must fail ValidateConfig().
	badCol := &Collector{NodeUID: "node-5090-01", StrictAudit: true}
	if err := badCol.ValidateConfig(); err == nil {
		t.Fatal("expected ValidateConfig failure when StrictAudit=true and S3/Spool missing")
	}

	var failS3 atomic.Bool
	var gotMethod, gotLockMode, gotAuth, gotMD5 string
	var putBodies [][]byte
	s3Srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if failS3.Load() {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		gotMethod = r.Method
		gotLockMode = r.Header.Get("X-Amz-Object-Lock-Mode")
		gotAuth = r.Header.Get("Authorization")
		gotMD5 = r.Header.Get("Content-MD5")
		b, _ := io.ReadAll(r.Body)
		putBodies = append(putBodies, b)
		w.WriteHeader(http.StatusOK)
	}))
	defer s3Srv.Close()

	newCol := func() *Collector {
		return &Collector{
			NodeUID:             "node-5090-01",
			NodeName:            "k3s-5090-dev",
			SpoolDir:            spoolDir,
			StrictAudit:         true,
			ImageRef:            pinnedImageRef,
			ExpectedImageDigest: pinnedDigest,
			NVML: stubNVML{
				snap: NVMLSnapshot{
					SampledAt: time.Date(2026, 11, 15, 0, 0, 0, 0, time.UTC),
					Devices: []GPUDeviceObservation{
						{GPUUUID: "GPU-0", GPUIndex: 0, ModelName: "RTX 5090", TotalVRAMMiB: 32607, MeasuredFreeVRAMMiB: 30000, MeasuredUsedVRAMMiB: 2607, NVMLHealthy: true},
					},
					Processes: []GPUProcessObservation{{GPUUUID: "GPU-0", PID: 310, UsedVRAMMiB: 2000}},
				},
			},
			Proc: ProcInspector{ProcRoot: procRoot},
			S3: &S3PutOnlyConfig{
				EndpointURL:    s3Srv.URL,
				Bucket:         "vf-t4-evidence",
				Region:         "us-east-1",
				KeyPrefix:      "runs/test-run",
				AccessKey:      "AKIAIOSFODNN7EXAMPLE",
				SecretKey:      "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
				RetentionHours: 168,
			},
			Now: func() time.Time { return time.Date(2026, 11, 15, 1, 0, 0, 0, time.UTC) },
		}
	}

	// Unpinned image tag or mismatched digest must fail ValidateConfig() when StrictAudit=true!
	unpinnedCol := newCol()
	unpinnedCol.ImageRef = "registry.cn-qingdao.aliyuncs.com/wod/verdantflare-station:station-runtime-v0.2.77"
	if err := unpinnedCol.ValidateConfig(); err == nil {
		t.Fatal("expected ValidateConfig failure when ImageRef lacks @sha256 digest in StrictAudit mode")
	}
	mismatchCol := newCol()
	mismatchCol.ExpectedImageDigest = "sha256:" + strings.Repeat("0", 64)
	if err := mismatchCol.ValidateConfig(); err == nil {
		t.Fatal("expected ValidateConfig failure when ExpectedImageDigest mismatches ImageRef")
	}

	col1 := newCol()
	s1, err := col1.CaptureOnce(context.Background())
	if err != nil {
		t.Fatalf("CaptureOnce 1: %v", err)
	}

	// Simulate transient S3 outage on seq=2 -> durable spool writes seq=2, S3 upload fails!
	failS3.Store(true)
	s2, err := col1.CaptureOnce(context.Background())
	if err == nil {
		t.Fatal("expected S3 500 error on CaptureOnce 2")
	}

	// Simulate collector restart (`col2` starts with empty memory) -> RecoverAndReplay restores (seq=2, prevHash=s2.SampleHash) AND replays seq=2 to S3 before seq=3!
	failS3.Store(false)
	col2 := newCol()
	if err := col2.RecoverAndReplay(context.Background()); err != nil {
		t.Fatalf("RecoverAndReplay: %v", err)
	}
	s3Sample, err := col2.CaptureOnce(context.Background())
	if err != nil {
		t.Fatalf("CaptureOnce 3 after recovery: %v", err)
	}
	if s1.PrevHash != "GENESIS" || s2.PrevHash != s1.SampleHash || s3Sample.CollectorSeq != 3 || s3Sample.PrevHash != s2.SampleHash {
		t.Fatalf("broken collector hash chain across restart: s1=%+v s2=%+v s3=%+v", s1, s2, s3Sample)
	}
	if gotMethod != http.MethodPut || gotLockMode != "GOVERNANCE" || !strings.HasPrefix(gotAuth, "AWS4-HMAC-SHA256 ") || gotMD5 == "" || len(putBodies) != 3 {
		t.Fatalf("expected 3 uploaded samples (1 + replayed 2 + new 3), got count=%d method=%s lock=%s", len(putBodies), gotMethod, gotLockMode)
	}
	for idx, wantSeq := range []int64{1, 2, 3} {
		var decoded CollectorSample
		if err := json.Unmarshal(putBodies[idx], &decoded); err != nil || decoded.CollectorSeq != wantSeq {
			t.Fatalf("uploaded[%d] seq=%d want %d (%v)", idx, decoded.CollectorSeq, wantSeq, err)
		}
	}

	// Verify /healthz returns 200 OK when healthy.
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	col2.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("/healthz got %d (%s), want 200", rec.Code, rec.Body.String())
	}
}

func TestAgentEpochTombstoneIntegrityAndTransitiveDescendantFence(t *testing.T) {
	procRoot := t.TempDir()
	tombDir := t.TempDir()
	podUID := "4b9f22c3-5523-4d8b-8a99-223344556677"
	cid := strings.Repeat("c", 64)
	systemdCgroup := "0::/kubepods.slice/kubepods-burstable.slice/kubepods-burstable-pod4b9f22c3_5523_4d8b_8a99_223344556677.slice/cri-containerd-" + cid + ".scope\n"

	// Host PID 45120 has container NSpid=1 and starttimeTicks=987654!
	// Sibling CPU file server has host PID 45101 (container NSpid=2, PPID=45001) + worker child host PID 45205 (PPID=45120, container NSpid=100, cleared environ)
	// + grandchild host PID 45305 (PPID=45205, container NSpid=105, cleared environ)!
	containerNS := "pid:[4026532501]"
	writeFakeProc(t, procRoot, 45120, 45001, "python3", "S", systemdCgroup, "python3 -m worker.supervisor", fakeProcOpts{
		bootID:       "boot-2",
		startTicks:   987654,
		containerPID: 1,
		pidNS:        containerNS,
	})
	writeFakeProc(t, procRoot, 45101, 45001, "files-srv", "S", systemdCgroup, "files-server --port 48085", fakeProcOpts{
		bootID:       "boot-2",
		startTicks:   987600,
		containerPID: 2,
		pidNS:        containerNS,
	})
	writeFakeProc(t, procRoot, 45205, 45120, "sh", "S", systemdCgroup, "sh -c launcher", fakeProcOpts{
		startTicks:   988001,
		containerPID: 100,
		pidNS:        containerNS,
	})
	writeFakeProc(t, procRoot, 45305, 45205, "python3", "R", systemdCgroup, "python3 render_node.py", fakeProcOpts{
		startTicks:   988002,
		containerPID: 105,
		pidNS:        containerNS,
	})

	agent := &Agent{
		NodeUID:      "node-5090-01",
		NodeName:     "k3s-5090-dev",
		BearerToken:  strings.Repeat("k", 40),
		TombstoneDir: tombDir,
		NVML: stubNVML{
			snap: NVMLSnapshot{
				SampledAt: time.Now().UTC(),
				Devices: []GPUDeviceObservation{
					{GPUUUID: "GPU-0", GPUIndex: 0, ModelName: "RTX 5090", TotalVRAMMiB: 32607, MeasuredFreeVRAMMiB: 31900, MeasuredUsedVRAMMiB: 707, NVMLHealthy: true},
				},
			},
		},
		Proc: ProcInspector{ProcRoot: procRoot},
		CRI: &stubCRISource{
			status: map[string]CRIContainerStatus{
				cid: {ContainerID: cid, PodUID: podUID, State: "CONTAINER_RUNNING", Exited: false},
			},
		},
	}
	if err := agent.EnsureStorageAndProcAccess(); err != nil {
		t.Fatalf("EnsureStorageAndProcAccess: %v", err)
	}

	// 1. Container SupervisorPID=1 + StarttimeTicks=987654 maps to host PID 45120 via NSpid,
	//    and cleared-environ child (45205) + grandchild (45305) under host PPID 45120 MUST block Verified!
	resTree, err := agent.VerifyTerminated(context.Background(), scheduler.VerifyTerminatedRequest{
		NodeUID:                  "node-5090-01",
		GPUUUID:                  "GPU-0",
		PodUID:                   podUID,
		ContainerID:              cid,
		BootID:                   "boot-2",
		GPUEpoch:                 2,
		SupervisorPID:            1,
		SupervisorStarttimeTicks: 987654,
		EpochFenceConfirmed:      true,
		Mode:                     "epoch_unload",
	})
	if err != nil {
		t.Fatal(err)
	}
	if resTree.Verified || resTree.CgroupWorkerCount != 2 {
		t.Fatalf("expected cleared-environ child+grandchild (2 workers) under NSpid-mapped host PID 45120 to block epoch_unload, got %+v", resTree)
	}

	// 2. Remove 45205 & 45305; write a corrupt/empty epoch tombstone file -> EpochFenceConfirmed=false MUST reject corrupt tombstone!
	_ = os.RemoveAll(filepath.Join(procRoot, "45205"))
	_ = os.RemoveAll(filepath.Join(procRoot, "45305"))
	corruptTombPath := filepath.Join(tombDir, podUID+"_epoch_2.json")
	if err := os.WriteFile(corruptTombPath, []byte("{corrupt"), 0o600); err != nil {
		t.Fatal(err)
	}
	resCorruptTomb, err := agent.VerifyTerminated(context.Background(), scheduler.VerifyTerminatedRequest{
		NodeUID:                  "node-5090-01",
		GPUUUID:                  "GPU-0",
		PodUID:                   podUID,
		ContainerID:              cid,
		BootID:                   "boot-2",
		GPUEpoch:                 2,
		SupervisorPID:            1,
		SupervisorStarttimeTicks: 987654,
		EpochFenceConfirmed:      false,
		Mode:                     "epoch_unload",
	})
	if err != nil || resCorruptTomb.Verified || resCorruptTomb.ErrorCode != "EPOCH_FENCE_UNCONFIRMED" {
		t.Fatalf("corrupt epoch tombstone must NOT satisfy epoch fence, got %+v (%v)", resCorruptTomb, err)
	}

	// 3a. Missing SupervisorPID (0) or missing/mismatched StarttimeTicks or raw unmapped host PID MUST fail closed with SUPERVISOR_PID_UNVERIFIED!
	for _, badReq := range []scheduler.VerifyTerminatedRequest{
		{NodeUID: "node-5090-01", GPUUUID: "GPU-0", PodUID: podUID, ContainerID: cid, BootID: "boot-2", GPUEpoch: 2, SupervisorPID: 0, SupervisorStarttimeTicks: 987654, EpochFenceConfirmed: true, Mode: "epoch_unload"},
		{NodeUID: "node-5090-01", GPUUUID: "GPU-0", PodUID: podUID, ContainerID: cid, BootID: "boot-2", GPUEpoch: 2, SupervisorPID: 1, SupervisorStarttimeTicks: 0, EpochFenceConfirmed: true, Mode: "epoch_unload"},
		{NodeUID: "node-5090-01", GPUUUID: "GPU-0", PodUID: podUID, ContainerID: cid, BootID: "boot-2", GPUEpoch: 2, SupervisorPID: 1, SupervisorStarttimeTicks: 999999, EpochFenceConfirmed: true, Mode: "epoch_unload"},
		{NodeUID: "node-5090-01", GPUUUID: "GPU-0", PodUID: podUID, ContainerID: cid, BootID: "boot-2", GPUEpoch: 2, SupervisorPID: 45120, SupervisorStarttimeTicks: 987654, EpochFenceConfirmed: true, Mode: "epoch_unload"},
	} {
		resBad, errBad := agent.VerifyTerminated(context.Background(), badReq)
		if errBad != nil || resBad.Verified || resBad.ErrorCode != "SUPERVISOR_PID_UNVERIFIED" {
			t.Fatalf("expected SUPERVISOR_PID_UNVERIFIED for badReq=%+v, got %+v (%v)", badReq, resBad, errBad)
		}
	}

	// 4. Valid container SupervisorPID=1 + StarttimeTicks=987654 + EpochFenceConfirmed=true succeeds (while sibling PID 45101 remains untouched)
	// AND writes an HMAC-SHA256 authenticated EpochTombstone binding HostSupervisorPID=45120 + ContainerSupervisorPID=1!
	resValid, err := agent.VerifyTerminated(context.Background(), scheduler.VerifyTerminatedRequest{
		NodeUID:                  "node-5090-01",
		GPUUUID:                  "GPU-0",
		PodUID:                   podUID,
		ContainerID:              cid,
		BootID:                   "boot-2",
		GPUEpoch:                 2,
		SupervisorPID:            1,
		SupervisorStarttimeTicks: 987654,
		EpochFenceConfirmed:      true,
		Mode:                     "epoch_unload",
	})
	if err != nil || !resValid.Verified || !resValid.TombstoneFound {
		t.Fatalf("expected valid epoch_unload verification, got %+v (%v)", resValid, err)
	}

	// Subsequent query with EpochFenceConfirmed=false + SupervisorPID=0 succeeds from HMAC-verified durable EpochTombstone!
	resFromTomb, err := agent.VerifyTerminated(context.Background(), scheduler.VerifyTerminatedRequest{
		NodeUID:             "node-5090-01",
		GPUUUID:             "GPU-0",
		PodUID:              podUID,
		ContainerID:         cid,
		BootID:              "boot-2",
		GPUEpoch:            2,
		EpochFenceConfirmed: false,
		Mode:                "epoch_unload",
	})
	if err != nil || !resFromTomb.Verified {
		t.Fatalf("expected verified durable EpochTombstone replay to succeed, got %+v (%v)", resFromTomb, err)
	}

	// Tampering with HMACSHA256 on disk MUST reject EpochTombstone!
	rawTomb, err := os.ReadFile(corruptTombPath)
	if err != nil {
		t.Fatal(err)
	}
	var savedTomb EpochTombstone
	if err := json.Unmarshal(rawTomb, &savedTomb); err != nil {
		t.Fatal(err)
	}
	if savedTomb.HostSupervisorPID != 45120 || savedTomb.ContainerSupervisorPID != 1 || savedTomb.HMACSHA256 == "" {
		t.Fatalf("unexpected saved EpochTombstone: %+v", savedTomb)
	}
	savedTomb.HMACSHA256 = strings.Repeat("0", 64)
	tamperedRaw, _ := json.Marshal(savedTomb)
	if err := os.WriteFile(corruptTombPath, tamperedRaw, 0o600); err != nil {
		t.Fatal(err)
	}
	resTamperedMAC, err := agent.VerifyTerminated(context.Background(), scheduler.VerifyTerminatedRequest{
		NodeUID:                  "node-5090-01",
		GPUUUID:                  "GPU-0",
		PodUID:                   podUID,
		ContainerID:              cid,
		BootID:                   "boot-2",
		GPUEpoch:                 2,
		SupervisorPID:            1,
		SupervisorStarttimeTicks: 987654,
		EpochFenceConfirmed:      false,
		Mode:                     "epoch_unload",
	})
	if err != nil || resTamperedMAC.Verified || resTamperedMAC.ErrorCode != "EPOCH_FENCE_UNCONFIRMED" {
		t.Fatalf("forged HMAC EpochTombstone must be rejected, got %+v (%v)", resTamperedMAC, err)
	}
	resWrongBoot, err := agent.VerifyTerminated(context.Background(), scheduler.VerifyTerminatedRequest{
		NodeUID:             "node-5090-01",
		GPUUUID:             "GPU-0",
		PodUID:              podUID,
		ContainerID:         cid,
		BootID:              "boot-different",
		GPUEpoch:            2,
		SupervisorPID:       100,
		EpochFenceConfirmed: false,
		Mode:                "epoch_unload",
	})
	if err != nil || resWrongBoot.Verified {
		t.Fatalf("mismatched BootID must NOT match durable EpochTombstone, got %+v (%v)", resWrongBoot, err)
	}
}

func TestAgentWarmIdleSettledVerification(t *testing.T) {
	procRoot := t.TempDir()
	tombDir := t.TempDir()
	podUID := "5c0f33d4-6634-4e9c-9b00-334455667788"
	cid := strings.Repeat("d", 64)
	systemdCgroup := "0::/kubepods.slice/kubepods-burstable.slice/kubepods-burstable-pod5c0f33d4_6634_4e9c_9b00_334455667788.slice/cri-containerd-" + cid + ".scope\n"
	containerNS := "pid:[4026532601]"

	writeFakeProc(t, procRoot, 51100, 51001, "python3", "S", systemdCgroup, "uvicorn main:app --port 8000", fakeProcOpts{
		bootID:       "boot-warm-1",
		startTicks:   555111,
		containerPID: 1,
		pidNS:        containerNS,
		hasNvidiaFD:  true,
	})
	writeFakeProc(t, procRoot, 51200, 51100, "python3", "R", systemdCgroup, "python3 child_worker.py", fakeProcOpts{
		startTicks:   555999,
		containerPID: 42,
		pidNS:        containerNS,
		hasNvidiaFD:  true,
	})

	agent := &Agent{
		NodeUID:      "node-5090-01",
		NodeName:     "k3s-5090-dev",
		BearerToken:  strings.Repeat("w", 40),
		TombstoneDir: tombDir,
		NVML: stubNVML{
			snap: NVMLSnapshot{
				SampledAt: time.Now().UTC(),
				Devices: []GPUDeviceObservation{
					{GPUUUID: "GPU-0", GPUIndex: 0, ModelName: "RTX 5090", TotalVRAMMiB: 32607, MeasuredFreeVRAMMiB: 31500, MeasuredUsedVRAMMiB: 1107, NVMLHealthy: true},
				},
				Processes: []GPUProcessObservation{
					{GPUUUID: "GPU-0", PID: 51100, UsedVRAMMiB: 500},
				},
			},
		},
		Proc: ProcInspector{ProcRoot: procRoot},
		CRI: &stubCRISource{
			status: map[string]CRIContainerStatus{
				cid: {ContainerID: cid, PodUID: podUID, State: "CONTAINER_RUNNING", Exited: false},
			},
		},
	}

	// Surviving child process (51200) MUST block warm_idle_settled!
	resWithChild, err := agent.VerifyTerminated(context.Background(), scheduler.VerifyTerminatedRequest{
		NodeUID:                  "node-5090-01",
		GPUUUID:                  "GPU-0",
		PodUID:                   podUID,
		ContainerID:              cid,
		BootID:                   "boot-warm-1",
		SupervisorPID:            1,
		SupervisorStarttimeTicks: 555111,
		EpochFenceConfirmed:      true,
		Mode:                     "warm_idle_settled",
	})
	if err != nil || resWithChild.Verified || resWithChild.CgroupWorkerCount != 1 {
		t.Fatalf("expected child worker 51200 to block warm_idle_settled, got %+v (%v)", resWithChild, err)
	}

	// Remove child worker 51200; single warm service PID 51100 (container PID 1) with EpochFenceConfirmed=true MUST verify cleanly and return MeasuredFreeVRAMMiB=31500!
	_ = os.RemoveAll(filepath.Join(procRoot, "51200"))
	resSettled, err := agent.VerifyTerminated(context.Background(), scheduler.VerifyTerminatedRequest{
		NodeUID:                  "node-5090-01",
		GPUUUID:                  "GPU-0",
		PodUID:                   podUID,
		ContainerID:              cid,
		BootID:                   "boot-warm-1",
		SupervisorPID:            1,
		SupervisorStarttimeTicks: 555111,
		EpochFenceConfirmed:      true,
		Mode:                     "warm_idle_settled",
	})
	if err != nil || !resSettled.Verified || resSettled.MeasuredFreeVRAMMiB != 31500 || len(resSettled.SurvivingPIDs) != 0 {
		t.Fatalf("expected warm_idle_settled Verified=true free=31500, got %+v (%v)", resSettled, err)
	}
}

func TestWarmIdleSettledProxyUpstreamProcessTopology(t *testing.T) {
	procRoot := t.TempDir()
	podUID := "99aabbcc-1122-4334-8899-aabbccddeeff"
	cid := strings.Repeat("d", 64)
	cgroup := "0::/kubepods.slice/kubepods-pod99aabbcc_1122_4334_8899_aabbccddeeff.slice/cri-containerd-" + cid + ".scope\n"

	// Realistic dual-process guard-proxy topology:
	// - host PID 60501 (container PID 1): entrypoint.sh
	// - host PID 60512 (container PID 12): guard.proxy (supervisor signing reclaim_proof)
	// - host PID 60508 (container PID 8): upstream CUDA engine (acestep / sgl-omni) holding /dev/nvidia0
	// - host PID 60515 (container PID 15): upstream TP rank worker holding /dev/nvidia0
	writeFakeProc(t, procRoot, 60501, 1, "bash", "S", cgroup, "/bin/bash /entrypoint.sh", fakeProcOpts{
		bootID:       "boot-proxy-1",
		startTicks:   100001,
		containerPID: 1,
		pidNS:        "pid:[4026532200]",
	})
	writeFakeProc(t, procRoot, 60512, 60501, "python3", "S", cgroup, "python3 -m uvicorn guard.proxy:app", fakeProcOpts{
		bootID:       "boot-proxy-1",
		startTicks:   100012,
		containerPID: 12,
		pidNS:        "pid:[4026532200]",
	})
	writeFakeProc(t, procRoot, 60508, 60501, "sgl-omni", "S", cgroup, "sgl-omni serve --port 18000", fakeProcOpts{
		bootID:       "boot-proxy-1",
		hasNvidiaFD:  true,
		startTicks:   100008,
		containerPID: 8,
		pidNS:        "pid:[4026532200]",
	})
	writeFakeProc(t, procRoot, 60515, 60508, "sgl-rank-1", "S", cgroup, "sgl-omni rank 1", fakeProcOpts{
		bootID:       "boot-proxy-1",
		hasNvidiaFD:  true,
		startTicks:   100015,
		containerPID: 15,
		pidNS:        "pid:[4026532200]",
	})

	agent := &Agent{
		NodeUID:  "node-5090-01",
		NodeName: "k3s-5090-dev",
		NVML: stubNVML{
			snap: NVMLSnapshot{
				SampledAt: time.Now().UTC(),
				Devices: []GPUDeviceObservation{
					{GPUUUID: "GPU-0", GPUIndex: 0, ModelName: "NVIDIA GeForce RTX 5090", TotalVRAMMiB: 32607, MeasuredFreeVRAMMiB: 6607, MeasuredUsedVRAMMiB: 26000, NVMLHealthy: true},
				},
				Processes: []GPUProcessObservation{
					{GPUUUID: "GPU-0", PID: 60508, ProcessType: "C", UsedVRAMMiB: 13000},
					{GPUUUID: "GPU-0", PID: 60515, ProcessType: "C", UsedVRAMMiB: 13000},
				},
			},
		},
		Proc: ProcInspector{ProcRoot: procRoot},
		CRI: &stubCRISource{
			status: map[string]CRIContainerStatus{
				cid: {ContainerID: cid, PodUID: podUID, State: "CONTAINER_RUNNING", Exited: false},
			},
		},
	}

	// 1. Without ResidentProcs attestation, warm_idle_settled MUST reject because upstream GPU PIDs are unattested!
	resNoAttest, err := agent.VerifyTerminated(context.Background(), scheduler.VerifyTerminatedRequest{
		NodeUID:                  "node-5090-01",
		GPUUUID:                  "GPU-0",
		PodUID:                   podUID,
		ContainerID:              cid,
		BootID:                   "boot-proxy-1",
		SupervisorPID:            12,
		SupervisorStarttimeTicks: 100012,
		EpochFenceConfirmed:      true,
		Mode:                     "warm_idle_settled",
	})
	if err != nil || resNoAttest.Verified {
		t.Fatalf("expected warm_idle_settled without ResidentProcs to fail, got %+v (%v)", resNoAttest, err)
	}

	// 2. With exact attested ResidentProcs {1, 8, 15} and EpochFenceConfirmed=true -> MUST verify cleanly and preserve MeasuredFreeVRAMMiB=6607!
	attested := []scheduler.ResidentProcIdentity{
		{PID: 1, StarttimeTicks: 100001},
		{PID: 8, StarttimeTicks: 100008},
		{PID: 15, StarttimeTicks: 100015},
	}
	resOK, err := agent.VerifyTerminated(context.Background(), scheduler.VerifyTerminatedRequest{
		NodeUID:                  "node-5090-01",
		GPUUUID:                  "GPU-0",
		PodUID:                   podUID,
		ContainerID:              cid,
		BootID:                   "boot-proxy-1",
		SupervisorPID:            12,
		SupervisorStarttimeTicks: 100012,
		ResidentProcs:            attested,
		EpochFenceConfirmed:      true,
		Mode:                     "warm_idle_settled",
	})
	if err != nil || !resOK.Verified || resOK.MeasuredFreeVRAMMiB != 6607 || len(resOK.SurvivingPIDs) != 0 {
		t.Fatalf("expected attested proxy+upstream topology to verify, got %+v (%v)", resOK, err)
	}

	// 3. Mismatched starttime_ticks on upstream engine (e.g. engine crashed/respawned) -> MUST reject with RESIDENT_PROC_UNVERIFIED!
	resBadTicks, err := agent.VerifyTerminated(context.Background(), scheduler.VerifyTerminatedRequest{
		NodeUID:                  "node-5090-01",
		GPUUUID:                  "GPU-0",
		PodUID:                   podUID,
		ContainerID:              cid,
		BootID:                   "boot-proxy-1",
		SupervisorPID:            12,
		SupervisorStarttimeTicks: 100012,
		ResidentProcs: []scheduler.ResidentProcIdentity{
			{PID: 1, StarttimeTicks: 100001},
			{PID: 8, StarttimeTicks: 999999},
			{PID: 15, StarttimeTicks: 100015},
		},
		EpochFenceConfirmed: true,
		Mode:                "warm_idle_settled",
	})
	if err != nil || resBadTicks.Verified || resBadTicks.ErrorCode != "RESIDENT_PROC_UNVERIFIED" {
		t.Fatalf("expected RESIDENT_PROC_UNVERIFIED on wrong starttime_ticks, got %+v (%v)", resBadTicks, err)
	}

	// 4. Unattested rogue child GPU worker (host PID 60599, container PID 99) -> MUST reject!
	writeFakeProc(t, procRoot, 60599, 60508, "rogue-worker", "R", cgroup, "python3 rogue.py", fakeProcOpts{
		bootID:       "boot-proxy-1",
		hasNvidiaFD:  true,
		startTicks:   100099,
		containerPID: 99,
		pidNS:        "pid:[4026532200]",
	})
	resRogue, err := agent.VerifyTerminated(context.Background(), scheduler.VerifyTerminatedRequest{
		NodeUID:                  "node-5090-01",
		GPUUUID:                  "GPU-0",
		PodUID:                   podUID,
		ContainerID:              cid,
		BootID:                   "boot-proxy-1",
		SupervisorPID:            12,
		SupervisorStarttimeTicks: 100012,
		ResidentProcs:            attested,
		EpochFenceConfirmed:      true,
		Mode:                     "warm_idle_settled",
	})
	if err != nil || resRogue.Verified || len(resRogue.SurvivingPIDs) == 0 {
		t.Fatalf("expected unattested rogue child process to block settlement, got %+v (%v)", resRogue, err)
	}
}
