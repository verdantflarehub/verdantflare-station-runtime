package instances

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

func TestCompatibleProfileAllowsHistoricalCapacityWithoutExpandingCurrentPool(t *testing.T) {
	in, base, pvc, pv := fixture()
	old := HelperProfile{StorageProfile: base, Image: "registry.example.test/blender-worker:v0.1.14", MaxFileBytes: 1 << 30}
	oldHash := hex.EncodeToString(old.Hash[:])

	newer := old
	newer.Image = "registry.example.test/blender-worker:v0.1.24"
	newer.Claims = []string{"workspace-1", "workspace-10", "workspace-11"}
	newer.Hash[0] ^= 0xff

	s := &Service{
		StationID:      in.StationID,
		Profiles:       map[string]HelperProfile{newer.ID: newer},
		ProfileHistory: map[string]HelperProfile{oldHash: old},
		CapacityClaims: CapacityClaims{oldHash: {"workspace-5"}},
	}

	pvc.Name = "workspace-5"
	pvc.UID = types.UID(uuid.NewString())
	pvc.Spec.VolumeName = "workspace-5"
	pvc.Annotations = volumeOwner(in)
	pv.Name = "workspace-5"
	pv.UID = types.UID(uuid.NewString())
	pv.Spec.ClaimRef.Name = pvc.Name
	pv.Spec.ClaimRef.UID = pvc.UID
	pv.Spec.Local.Path = "/test/workspace-5"

	binding := VolumeBinding{
		Identity:      in,
		ProfileID:     old.ID,
		ProfileHash:   append([]byte(nil), old.Hash[:]...),
		Namespace:     old.Namespace,
		Pool:          old.Pool,
		PVCName:       pvc.Name,
		PVCUID:        string(pvc.UID),
		PVName:        pv.Name,
		PVUID:         string(pv.UID),
		NodeName:      old.NodeName,
		ReservedBytes: old.Bytes,
	}

	effective, ok := s.compatibleProfile(binding, in, newer)
	if !ok {
		t.Fatal("expected historical capacity volume to be compatible with newer profile")
	}
	if effective.Hash != newer.Hash {
		t.Fatal("effective profile hash drifted from target profile")
	}
	if !sameWorkspacePool(binding, in, effective.StorageProfile) {
		t.Fatal("effective profile did not admit proven historical volume")
	}
	if !reflect.DeepEqual(newer.Claims, []string{"workspace-1", "workspace-10", "workspace-11"}) {
		t.Fatalf("caller target profile mutated: %v", newer.Claims)
	}
	if !reflect.DeepEqual(s.Profiles[newer.ID].Claims, []string{"workspace-1", "workspace-10", "workspace-11"}) {
		t.Fatalf("current allocation pool expanded: %v", s.Profiles[newer.ID].Claims)
	}

	ctx := context.Background()
	client := fake.NewClientset(pvc, pv)
	if _, err := Inspect(ctx, client, in, effective.StorageProfile, binding.PVCName, &binding, nil); err != nil {
		t.Fatalf("expected Inspect to succeed on proven historical volume: %v", err)
	}

	// Volume UID, ownership, Retain policy, and active Pod occupancy checks remain enforced.
	wrongUID := binding
	wrongUID.PVCUID = uuid.NewString()
	if _, err := Inspect(ctx, client, in, effective.StorageProfile, binding.PVCName, &wrongUID, nil); !errors.Is(err, ErrBinding) {
		t.Fatalf("expected ErrBinding on mismatched PVC UID, got %v", err)
	}
	foreignOwner := pvc.DeepCopy()
	foreignOwner.Annotations[instanceKey] = uuid.NewString()
	if _, err := Inspect(ctx, fake.NewClientset(foreignOwner, pv), in, effective.StorageProfile, binding.PVCName, &binding, nil); !errors.Is(err, ErrBinding) {
		t.Fatalf("expected ErrBinding on foreign owner annotation, got %v", err)
	}
	busyPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "other-pod", Namespace: pvc.Namespace, UID: types.UID(uuid.NewString())},
		Spec: corev1.PodSpec{
			Volumes: []corev1.Volume{{
				Name:         "workspace",
				VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: binding.PVCName}},
			}},
		},
	}
	if _, err := Inspect(ctx, fake.NewClientset(pvc, pv, busyPod), in, effective.StorageProfile, binding.PVCName, &binding, nil); !errors.Is(err, ErrBusy) {
		t.Fatalf("expected ErrBusy on mounted foreign pod, got %v", err)
	}

	// Negative compatibility cases must all be rejected.
	wrongID := in
	wrongID.InstanceID = uuid.NewString()
	if _, ok := s.compatibleProfile(binding, wrongID, newer); ok {
		t.Fatal("accepted mismatched instance identity")
	}

	unregistered := binding
	unregistered.PVCName = "workspace-999"
	if _, ok := s.compatibleProfile(unregistered, in, newer); ok {
		t.Fatal("accepted unregistered claim")
	}

	otherPool := newer
	otherPool.Pool = "other-pool"
	if _, ok := s.compatibleProfile(binding, in, otherPool); ok {
		t.Fatal("accepted different pool")
	}

	otherNode := newer
	otherNode.NodeName = "worker-other"
	if _, ok := s.compatibleProfile(binding, in, otherNode); ok {
		t.Fatal("accepted different node")
	}

	otherNS := newer
	otherNS.Namespace = "other-ns"
	if _, ok := s.compatibleProfile(binding, in, otherNS); ok {
		t.Fatal("accepted different namespace")
	}

	otherBytes := newer
	otherBytes.Bytes = newer.Bytes * 2
	if _, ok := s.compatibleProfile(binding, in, otherBytes); ok {
		t.Fatal("accepted different capacity bytes")
	}

	otherProfile := newer
	otherProfile.ID = "other-profile"
	if _, ok := s.compatibleProfile(binding, in, otherProfile); ok {
		t.Fatal("accepted different profile ID")
	}

	unknownHash := binding
	unknownHash.ProfileHash = make([]byte, 32)
	if _, ok := s.compatibleProfile(unknownHash, in, newer); ok {
		t.Fatal("accepted unknown historical profile hash")
	}

	withoutCapacity := *s
	withoutCapacity.CapacityClaims = nil
	if _, ok := withoutCapacity.compatibleProfile(binding, in, newer); ok {
		t.Fatal("accepted historical claim after capacity policy removal")
	}
}

func TestStopAndTerminalRecoveryRejectMissingClaimOwnerAnnotation(t *testing.T) {
	in, base, pvc, pv := fixture()
	p := HelperProfile{StorageProfile: base, Image: "registry.example.test/blender-worker:v0.1.24", MaxFileBytes: 1 << 30}
	pvc.Annotations = volumeOwner(in)
	delete(pvc.Annotations, projectKey)

	binding := VolumeBinding{
		Identity:      in,
		ProfileID:     p.ID,
		ProfileHash:   append([]byte(nil), p.Hash[:]...),
		Namespace:     p.Namespace,
		Pool:          p.Pool,
		PVCName:       pvc.Name,
		PVCUID:        string(pvc.UID),
		PVName:        pv.Name,
		PVUID:         string(pv.UID),
		NodeName:      p.NodeName,
		ReservedBytes: p.Bytes,
	}
	start := StartRequest{
		OperationID:       uuid.NewString(),
		CreateOperationID: in.CreateOperationID,
		StationID:         in.StationID,
		OrganizationID:    in.OrganizationID,
		UserID:            uuid.NewString(),
		ProjectID:         in.ProjectID,
		InstanceID:        in.InstanceID,
		ProfileID:         p.ID,
		SourceRevisionID:  uuid.NewString(),
		Empty:             true,
	}
	pod, svc, err := workerObjects(in, p, binding, start.OperationID, start.source())
	if err != nil {
		t.Fatal(err)
	}
	pod.UID = types.UID(uuid.NewString())
	pod.ResourceVersion = "1"
	pod.Spec.NodeName = p.NodeName
	pod.Status.Phase = corev1.PodFailed
	svc.UID = types.UID(uuid.NewString())
	svc.ResourceVersion = "1"
	obs := WorkerObservation{PodUID: string(pod.UID), ServiceUID: string(svc.UID), PodName: pod.Name, Namespace: pod.Namespace}

	ctx := context.Background()
	client := fake.NewClientset(pvc, pv, pod, svc)
	if _, err := removeWorker(ctx, client, start, p, binding, obs); !errors.Is(err, ErrBinding) {
		t.Fatalf("removeWorker expected ErrBinding on missing owner annotation, got %v", err)
	}
	for _, a := range client.Actions() {
		if a.GetVerb() == "delete" {
			t.Fatalf("removeWorker issued delete despite missing owner annotation: %s %s", a.GetVerb(), a.GetResource().Resource)
		}
	}

	client.ClearActions()
	if _, err := removeTerminalWorker(ctx, client, start, p, binding, obs); !errors.Is(err, ErrBinding) {
		t.Fatalf("removeTerminalWorker expected ErrBinding on missing owner annotation, got %v", err)
	}
	for _, a := range client.Actions() {
		if a.GetVerb() == "delete" {
			t.Fatalf("removeTerminalWorker issued delete despite missing owner annotation: %s %s", a.GetVerb(), a.GetResource().Resource)
		}
	}

	// Simulate same-name PVC replacement between Inspect and verifyClaimOwner:
	// first Get returns the original UID, second Get returns matching owner annotations with a different UID.
	pvc.Annotations = volumeOwner(in)
	replacedPVC := pvc.DeepCopy()
	replacedPVC.UID = types.UID(uuid.NewString())
	for _, tc := range []struct {
		name string
		fn   func(*fake.Clientset) error
	}{
		{"removeWorker", func(c *fake.Clientset) error {
			_, err := removeWorker(ctx, c, start, p, binding, obs)
			return err
		}},
		{"removeTerminalWorker", func(c *fake.Clientset) error {
			_, err := removeTerminalWorker(ctx, c, start, p, binding, obs)
			return err
		}},
	} {
		t.Run(tc.name+"/replaced-pvc-uid", func(t *testing.T) {
			c := fake.NewClientset(pvc.DeepCopy(), pv.DeepCopy(), pod.DeepCopy(), svc.DeepCopy())
			gets := 0
			c.PrependReactor("get", "persistentvolumeclaims", func(ktesting.Action) (bool, runtime.Object, error) {
				gets++
				if gets == 2 {
					return true, replacedPVC.DeepCopy(), nil
				}
				return false, nil, nil
			})
			if err := tc.fn(c); !errors.Is(err, ErrBinding) {
				t.Fatalf("%s expected ErrBinding on replaced PVC UID, got %v", tc.name, err)
			}
			for _, a := range c.Actions() {
				if a.GetVerb() == "delete" {
					t.Fatalf("%s issued delete after PVC UID replacement: %s %s", tc.name, a.GetVerb(), a.GetResource().Resource)
				}
			}
		})
	}
}

func TestHistoricalCapacityVolumeSupportsCrossVersionStartAccessAndStop(t *testing.T) {
	s, createReq, client, prepObserver := createFixture(t)
	ctx := context.Background()
	old := s.Profiles[createReq.ProfileID]
	oldHash := hex.EncodeToString(old.Hash[:])

	// Occupy the base claim so creation binds to historical extension claim workspace-5.
	basePVC, err := client.CoreV1().PersistentVolumeClaims(old.Namespace).Get(ctx, old.Claims[0], metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	basePVC.Annotations = map[string]string{instanceKey: uuid.NewString()}
	if _, err = client.CoreV1().PersistentVolumeClaims(old.Namespace).Update(ctx, basePVC, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}

	basePV, err := client.CoreV1().PersistentVolumes().Get(ctx, basePVC.Spec.VolumeName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	addPoolClaim := func(name string) {
		t.Helper()
		pvc := basePVC.DeepCopy()
		pvc.Name = name
		pvc.UID = types.UID(uuid.NewString())
		pvc.Spec.VolumeName = name
		pvc.Annotations = nil
		pv := basePV.DeepCopy()
		pv.Name = name
		pv.UID = types.UID(uuid.NewString())
		pv.Spec.ClaimRef.Name = pvc.Name
		pv.Spec.ClaimRef.UID = pvc.UID
		pv.Spec.Local.Path = "/test/" + name
		if err := client.Tracker().Add(pvc); err != nil {
			t.Fatal(err)
		}
		if err := client.Tracker().Add(pv); err != nil {
			t.Fatal(err)
		}
	}
	addPoolClaim("workspace-5")
	// Second idle claim registered only in the old profile's historical capacity policy.
	addPoolClaim("workspace-6")

	policy, err := LoadCapacityClaims(writeCapacity(t, []capacityEntry{{old.ID, oldHash, []string{"workspace-5", "workspace-6"}}}), s.Profiles, nil)
	if err != nil {
		t.Fatal(err)
	}
	s.CapacityClaims = policy

	// 1. Complete Create on old profile with extension claim workspace-5.
	var createdBinding VolumeBinding
	for i := 0; i < 4; i++ {
		out, err := s.Create(ctx, createReq)
		if err != nil {
			t.Fatalf("create step %d: %v", i, err)
		}
		if out.Workspace != nil {
			createdBinding = *out.Workspace
		}
	}
	readyHelper(t, client, createReq, old)
	prepObserver.proof = &PreparationProof{
		PrepareID:        createReq.OperationID,
		InstanceID:       createReq.InstanceID,
		ProjectID:        createReq.ProjectID,
		SourceRevisionID: createReq.SourceRevisionID,
		State:            "prepared",
		PreparedAt:       time.Now().UTC().Format(time.RFC3339Nano),
	}
	for i := 0; i < 3; i++ {
		out, err := s.Create(ctx, createReq)
		if err != nil {
			t.Fatalf("create finish step %d: %v", i, err)
		}
		if out.Workspace != nil {
			createdBinding = *out.Workspace
		}
	}
	if createdBinding.PVCName != "workspace-5" {
		t.Fatalf("expected binding to workspace-5, got %+v", createdBinding)
	}
	if _, err = client.CoreV1().Nodes().Create(ctx, computeNode(old.NodeName), metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	s.WorkerObserver = &testWorkerObserver{ready: true}
	s.DrainObserver = &testDrainObserver{}

	runStart := func(req StartRequest, expectedImage, expectedClaim string) StartProgress {
		t.Helper()
		var out StartProgress
		for i := 0; i < 8; i++ {
			var err error
			out, err = s.Start(ctx, req)
			if err != nil {
				t.Fatalf("start step %d: %v", i, err)
			}
			if out.Worker != nil {
				pod, err := client.CoreV1().Pods(old.Namespace).Get(ctx, out.Worker.PodName, metav1.GetOptions{})
				if err != nil {
					t.Fatal(err)
				}
				if pod.Spec.Containers[0].Image != expectedImage {
					t.Fatalf("expected worker image %s, got %s", expectedImage, pod.Spec.Containers[0].Image)
				}
				mounted := false
				for _, vol := range pod.Spec.Volumes {
					if vol.PersistentVolumeClaim != nil && vol.PersistentVolumeClaim.ClaimName == expectedClaim {
						mounted = true
					}
				}
				if !mounted {
					t.Fatalf("worker pod did not mount original claim %s", expectedClaim)
				}
				pod.Spec.NodeName = old.NodeName
				pod.Status.Phase = corev1.PodRunning
				pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
				if _, err = client.CoreV1().Pods(old.Namespace).Update(ctx, pod, metav1.UpdateOptions{}); err != nil {
					t.Fatalf("start step %d update pod %s ready failed: %v", i, pod.Name, err)
				}
			}
			if out.Status == "succeeded" && out.Phase == "running" {
				return out
			}
		}
		t.Fatalf("start did not finish running: %+v", out)
		return out
	}

	runStop := func(startOpID, assetSuffix, digestChar string, size, sceneVersion int64) StopProgress {
		t.Helper()
		stopReq := StopRequest{
			OperationID:      uuid.NewString(),
			StartOperationID: startOpID,
			StationID:        createReq.StationID,
			OrganizationID:   createReq.OrganizationID,
			UserID:           createReq.UserID,
			ProjectID:        createReq.ProjectID,
			InstanceID:       createReq.InstanceID,
			Generation:       strings.Repeat("a", 32),
			SavedRevisionID:  uuid.NewString(),
			CommitID:         uuid.NewString(),
			StoreID:          uuid.NewString(),
			ArtifactID:       uuid.NewString(),
			VersionID:        uuid.NewString(),
			AssetID:          "checkpoints/checkpoint-" + assetSuffix + "-" + strings.Repeat(digestChar, 32) + ".blend",
			SHA256:           strings.Repeat(digestChar, 64),
			Size:             size,
			SceneVersion:     sceneVersion,
		}
		var out StopProgress
		for i := 0; i < 6; i++ {
			var err error
			out, err = s.Stop(ctx, stopReq)
			if err != nil {
				t.Fatalf("stop step %d: %v", i, err)
			}
			if out.Status == "succeeded" && out.Phase == "stopped" {
				return out
			}
		}
		t.Fatalf("stop did not finish cleanly: %+v", out)
		return out
	}

	// 2. Complete an initial Start + saved Stop on the old profile so the subsequent
	// cross-version Start exercises the saved-source restart branch (stopped.Source == in.source()).
	oldStartReq := StartRequest{
		OperationID:       uuid.NewString(),
		CreateOperationID: createReq.OperationID,
		StationID:         createReq.StationID,
		OrganizationID:    createReq.OrganizationID,
		UserID:            createReq.UserID,
		ProjectID:         createReq.ProjectID,
		InstanceID:        createReq.InstanceID,
		ProfileID:         createReq.ProfileID,
		SourceRevisionID:  createReq.SourceRevisionID,
		SHA256:            createReq.SourceSHA256,
		Size:              createReq.SourceSize,
		Empty:             createReq.EmptySource,
	}
	runStart(oldStartReq, old.Image, "workspace-5")
	oldStopOut := runStop(oldStartReq.OperationID, "1", "b", 128, 1)
	if oldStopOut.Source.Empty || oldStopOut.Source.SHA256 != strings.Repeat("b", 64) || oldStopOut.Source.Size != 128 {
		t.Fatalf("unexpected saved stop source: %+v", oldStopOut.Source)
	}

	// 3. Register newer worker version with a fresh claim list excluding workspace-5 and workspace-6.
	newer := old
	newer.Image = "registry.example.test/blender-worker:v0.1.24"
	newer.Claims = []string{old.Claims[0], "workspace-10"}
	newer.Hash[0] ^= 0xff
	s.Profiles[old.ID] = newer
	s.ProfileHistory = map[string]HelperProfile{oldHash: old}

	// 4. Cross-version Start using the saved Stop source on the original historical capacity volume.
	crossStartReq := oldStartReq
	crossStartReq.OperationID = uuid.NewString()
	crossStartReq.SHA256 = oldStopOut.Source.SHA256
	crossStartReq.Size = oldStopOut.Source.Size
	crossStartReq.Empty = oldStopOut.Source.Empty
	startOut := runStart(crossStartReq, newer.Image, "workspace-5")
	if startOut.Proof == nil || startOut.Proof.Startup.WorkerSource != oldStopOut.Source {
		t.Fatalf("cross-version worker did not load saved stop source: proof=%+v want=%+v", startOut.Proof, oldStopOut.Source)
	}
	if startOut.Workspace == nil || startOut.Workspace.PVCUID != createdBinding.PVCUID || startOut.Workspace.PVUID != createdBinding.PVUID {
		t.Fatalf("cross-version start workspace drifted: got=%+v want=%+v", startOut.Workspace, createdBinding)
	}
	if !reflect.DeepEqual(s.Profiles[old.ID].Claims, []string{old.Claims[0], "workspace-10"}) {
		t.Fatalf("current profile allocation pool mutated: %v", s.Profiles[old.ID].Claims)
	}

	// 5. Access running worker on current profile.
	accessReq := AccessRequest{
		StationID:        crossStartReq.StationID,
		OrganizationID:   crossStartReq.OrganizationID,
		UserID:           crossStartReq.UserID,
		ProjectID:        crossStartReq.ProjectID,
		InstanceID:       crossStartReq.InstanceID,
		StartOperationID: crossStartReq.OperationID,
		Kind:             "worker",
	}
	accessOut, err := s.Access(ctx, accessReq)
	if err != nil || accessOut.Credential == "" || accessOut.Worker.PodUID != startOut.Worker.PodUID {
		t.Fatalf("access failed: err=%v got_pod_uid=%q want_pod_uid=%q credential_empty=%v",
			err, accessOut.Worker.PodUID, startOut.Worker.PodUID, accessOut.Credential == "")
	}

	// 6. Stop and save on current profile.
	stopOut := runStop(crossStartReq.OperationID, "2", "c", 256, 2)
	if stopOut.Workspace == nil || stopOut.Workspace.PVCName != "workspace-5" {
		t.Fatalf("stop did not preserve original workspace: %+v", stopOut)
	}

	// Verify durable binding remained unchanged.
	afterBinding, err := scanBinding(s.Pool.QueryRow(ctx, `SELECT `+bindingColumns+` FROM station.runtime_instance_workspaces WHERE instance_id=$1`, createReq.InstanceID))
	if err != nil {
		t.Fatal(err)
	}
	if afterBinding.PVCName != createdBinding.PVCName || afterBinding.PVCUID != createdBinding.PVCUID ||
		afterBinding.PVUID != createdBinding.PVUID || !bytes.Equal(afterBinding.ProfileHash, old.Hash[:]) {
		t.Fatalf("durable binding drifted: before=%+v after=%+v", createdBinding, afterBinding)
	}

	// 7. Verify a brand-new instance on the current profile cannot allocate idle historical claim workspace-6.
	// Both current claims (basePVC occupied, workspace-10 absent/unprovisioned) are unavailable.
	newInstanceReq := createReq
	newInstanceReq.OperationID = uuid.NewString()
	newInstanceReq.InstanceID = uuid.NewString()
	if out, err := s.Create(ctx, newInstanceReq); err != nil || out.Phase != "accepted" {
		t.Fatalf("new instance accept failed: %+v %v", out, err)
	}
	if _, err := s.Create(ctx, newInstanceReq); err == nil || err.Error() != "STORAGE_CAPACITY_UNAVAILABLE" {
		t.Fatalf("expected STORAGE_CAPACITY_UNAVAILABLE when current pool exhausted, got %v", err)
	}
	var newBindingCount int
	if err := s.Pool.QueryRow(ctx, `SELECT count(*) FROM station.runtime_instance_workspaces WHERE instance_id=$1`, newInstanceReq.InstanceID).Scan(&newBindingCount); err != nil || newBindingCount != 0 {
		t.Fatalf("new instance unexpectedly bound historical volume: count=%d err=%v", newBindingCount, err)
	}
}
