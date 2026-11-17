package instances

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestWorkerRecoveryRequiresExactFailedBindingAndSource(t *testing.T) {
	in, p, b, client, secret := workerFixture(t)
	ctx := context.Background()
	start := StartRequest{OperationID: uuid.NewString(), CreateOperationID: in.CreateOperationID, StationID: in.StationID, OrganizationID: in.OrganizationID, ProjectID: in.ProjectID, InstanceID: in.InstanceID, ProfileID: p.ID, SourceRevisionID: uuid.NewString(), Empty: true}
	obs, err := EnsureWorker(ctx, client, in, p, b, start.OperationID, secret, start.source(), WorkerObservation{})
	if err != nil {
		t.Fatal(err)
	}
	pod, _ := client.CoreV1().Pods(p.Namespace).Get(ctx, obs.PodName, metav1.GetOptions{})
	pod.Spec.NodeName = p.NodeName
	pod.Status.Phase = corev1.PodRunning
	_, _ = client.CoreV1().Pods(p.Namespace).Update(ctx, pod, metav1.UpdateOptions{})
	next := start
	next.OperationID = uuid.NewString()
	raw, _ := json.Marshal(start)
	state, _ := json.Marshal(startState{Worker: obs, Proof: &RunningProof{}})
	if _, err = prepareWorkerRecovery(ctx, client, next, p, b, raw, state); err == nil {
		t.Fatal("running worker recovered")
	}
	pod.Status.Phase = corev1.PodFailed
	_, _ = client.CoreV1().Pods(p.Namespace).Update(ctx, pod, metav1.UpdateOptions{})
	changed := next
	changed.SourceRevisionID = uuid.NewString()
	if _, err = prepareWorkerRecovery(ctx, client, changed, p, b, raw, state); err == nil {
		t.Fatal("changed source accepted")
	}
	recovery, err := prepareWorkerRecovery(ctx, client, next, p, b, raw, state)
	if err != nil {
		t.Fatal(err)
	}
	wrong := *recovery
	wrong.Worker.PodUID = uuid.NewString()
	if err = verifyFailedWorker(ctx, client, p, wrong, false); err == nil {
		t.Fatal("replaced UID accepted")
	}
	if gone, err := removeWorker(ctx, client, start, p, b, obs); err != nil || gone {
		t.Fatal(gone, err)
	}
	if err = verifyFailedWorker(ctx, client, p, *recovery, true); err != nil {
		t.Fatal("committed cleanup replay failed", err)
	}
	if _, err = prepareWorkerRecovery(ctx, client, next, p, b, raw, state); err == nil {
		t.Fatal("missing pod accepted without persisted recovery")
	}
	if gone, err := removeWorker(ctx, client, start, p, b, obs); err != nil || !gone {
		t.Fatal(gone, err)
	}
	if _, err = client.CoreV1().PersistentVolumeClaims(p.Namespace).Get(ctx, b.PVCName, metav1.GetOptions{}); err != nil {
		t.Fatal("workspace lost", err)
	}
	if _, err = EnsureWorker(ctx, client, in, p, b, next.OperationID, secret, next.source(), WorkerObservation{}); err != nil {
		t.Fatal("new attempt could not start", err)
	}
}

func TestRecoveryAcceptsSucceededButRejectsLiveOrUnknownWorker(t *testing.T) {
	in, p, b, client, secret := workerFixture(t)
	ctx := context.Background()
	start := StartRequest{OperationID: uuid.NewString(), CreateOperationID: in.CreateOperationID, StationID: in.StationID, OrganizationID: in.OrganizationID, ProjectID: in.ProjectID, InstanceID: in.InstanceID, ProfileID: p.ID, SourceRevisionID: uuid.NewString(), Empty: true}
	obs, err := EnsureWorker(ctx, client, in, p, b, start.OperationID, secret, start.source(), WorkerObservation{})
	if err != nil {
		t.Fatal(err)
	}
	next := start
	next.OperationID = uuid.NewString()
	raw, _ := json.Marshal(start)
	state, _ := json.Marshal(startState{Worker: obs, Proof: &RunningProof{}})
	pod, _ := client.CoreV1().Pods(p.Namespace).Get(ctx, obs.PodName, metav1.GetOptions{})
	pod.Spec.NodeName = p.NodeName
	for _, phase := range []corev1.PodPhase{corev1.PodPending, corev1.PodRunning, corev1.PodUnknown, corev1.PodSucceeded} {
		pod.Status.Phase = phase
		_, _ = client.CoreV1().Pods(p.Namespace).Update(ctx, pod, metav1.UpdateOptions{})
		_, err := prepareWorkerRecovery(ctx, client, next, p, b, raw, state)
		if (err == nil) != (phase == corev1.PodSucceeded) {
			t.Fatalf("phase %s: %v", phase, err)
		}
	}
}

func TestCrossVersionUnexpectedAdmissionErrorRecoveryUnmodifiedAndModified(t *testing.T) {
	in, oldProfile, b, client, secretUID := workerFixture(t)
	ctx := context.Background()

	// Simulate old v0.1.13 Worker Pod on blender-pool-003 before scheduler env / profile upgrade.
	t.Setenv("STATION_BLENDER_WORKER_SCHEDULER_URL", "")
	t.Setenv("STATION_BLENDER_WORKER_SCHEDULER_SECRET_NAME", "")
	oldProfile.Image = "registry.example.test/blender-worker:v0.1.13"

	origSHA := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	oldStart := StartRequest{
		OperationID:       uuid.NewString(),
		CreateOperationID: in.CreateOperationID,
		StationID:         in.StationID,
		OrganizationID:    in.OrganizationID,
		UserID:            uuid.NewString(),
		ProjectID:         in.ProjectID,
		InstanceID:        in.InstanceID,
		ProfileID:         oldProfile.ID,
		SourceRevisionID:  uuid.NewString(),
		SHA256:            origSHA,
		Size:              4096,
		Empty:             false,
	}
	oldObs, err := EnsureWorker(ctx, client, in, oldProfile, b, oldStart.OperationID, secretUID, oldStart.source(), WorkerObservation{})
	if err != nil {
		t.Fatalf("EnsureWorker(v0.1.13): %v", err)
	}

	// Simulate node reboot -> Kubelet marks bare Pod Failed / UnexpectedAdmissionError with 0 container starts.
	oldPod, err := client.CoreV1().Pods(oldProfile.Namespace).Get(ctx, oldObs.PodName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	oldPod.Spec.NodeName = oldProfile.NodeName
	oldPod.Status.Phase = corev1.PodFailed
	oldPod.Status.Reason = "UnexpectedAdmissionError"
	oldPod.Status.ContainerStatuses = nil
	if _, err = client.CoreV1().Pods(oldProfile.Namespace).Update(ctx, oldPod, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}

	// Upgrade station-runtime active profile to v0.1.25 with GPU scheduler env enabled (blender-desktop-v3).
	t.Setenv("STATION_BLENDER_WORKER_SCHEDULER_URL", "http://station-runtime.verdantflare-station.svc.cluster.local:5056")
	t.Setenv("STATION_BLENDER_WORKER_SCHEDULER_SECRET_NAME", "station-gpu-scheduler-auth")
	newProfile := oldProfile
	newProfile.Image = "registry.example.test/blender-worker:v0.1.25"
	newProfile.Hash[0] ^= 0xff

	oldInputRaw, _ := json.Marshal(oldStart)
	oldStateRaw, _ := json.Marshal(startState{
		SecretUID: secretUID,
		Worker:    oldObs,
		Proof: &RunningProof{
			Startup: StartupProof{
				WorkerSource:      oldStart.source(),
				StartOperationID:  oldStart.OperationID,
				CreateOperationID: oldStart.CreateOperationID,
				InstanceID:        oldStart.InstanceID,
				ProjectID:         oldStart.ProjectID,
				PodUID:            oldObs.PodUID,
			},
			Generation: "11111111111111111111111111111111",
			GPUUUID:    "GPU-11111111-2222-3333-4444-555555555555",
		},
	})

	nextStart := oldStart
	nextStart.OperationID = uuid.NewString()

	// 1. Tampered ownership or privileged spec on terminal Pod MUST still fail ErrBinding.
	tampered := oldPod.DeepCopy()
	tampered.Spec.HostNetwork = true
	_, _ = client.CoreV1().Pods(oldProfile.Namespace).Update(ctx, tampered, metav1.UpdateOptions{})
	if _, err := prepareWorkerRecovery(ctx, client, nextStart, newProfile, b, oldInputRaw, oldStateRaw); err == nil {
		t.Fatal("expected HostNetwork terminal pod to be rejected")
	}
	_, _ = client.CoreV1().Pods(oldProfile.Namespace).Update(ctx, oldPod, metav1.UpdateOptions{})

	// 2. Legitimate v0.1.13 UnexpectedAdmissionError Pod MUST pass prepareWorkerRecovery under upgraded v0.1.25 profile
	//    and accurately record pod_admission_failed / UnexpectedAdmissionError!
	rec, err := prepareWorkerRecovery(ctx, client, nextStart, newProfile, b, oldInputRaw, oldStateRaw)
	if err != nil {
		t.Fatalf("prepareWorkerRecovery across v0.1.13 -> v0.1.25 failed: %v", err)
	}
	if rec.TerminalCode != "pod_admission_failed" || rec.TerminalReason != "UnexpectedAdmissionError" {
		t.Fatalf("expected pod_admission_failed / UnexpectedAdmissionError, got code=%q reason=%q", rec.TerminalCode, rec.TerminalReason)
	}

	// 3. removeTerminalWorker MUST delete old v0.1.13 Pod & Service cleanly while keeping PVC/PV UIDs untouched.
	if gone, err := removeTerminalWorker(ctx, client, oldStart, newProfile, b, oldObs); err != nil || gone {
		t.Fatalf("expected first removeTerminalWorker pass to issue deletes (gone=false), got gone=%v err=%v", gone, err)
	}
	if gone, err := removeTerminalWorker(ctx, client, oldStart, newProfile, b, oldObs); err != nil || !gone {
		t.Fatalf("expected second removeTerminalWorker pass to confirm absence (gone=true), got gone=%v err=%v", gone, err)
	}
	pvcAfterCleanup, err := client.CoreV1().PersistentVolumeClaims(newProfile.Namespace).Get(ctx, b.PVCName, metav1.GetOptions{})
	if err != nil || string(pvcAfterCleanup.UID) != b.PVCUID {
		t.Fatalf("workspace PVC lost or replaced during cross-version recovery: %v", err)
	}

	// 4a. Unmodified working copy (*source == nextStart.source()):
	//     Verify BOTH direct EnsureWorker(v0.1.25) AND SavedRecovery read-only helper verification when *source == in.source()!
	unmodifiedObs := &savedCopyObserver{source: nextStart.source()}
	svc := &Service{
		StationID:           in.StationID,
		Client:              client,
		Profiles:            map[string]HelperProfile{newProfile.ID: newProfile},
		WorkingCopyObserver: unmodifiedObs,
	}
	createdReq := CreateRequest{
		OperationID:      in.CreateOperationID,
		StationID:        in.StationID,
		OrganizationID:   in.OrganizationID,
		UserID:           nextStart.UserID,
		ProjectID:        in.ProjectID,
		InstanceID:       in.InstanceID,
		ProfileID:        newProfile.ID,
		SourceRevisionID: nextStart.SourceRevisionID,
		SourceSHA256:     origSHA,
		SourceSize:       4096,
	}
	stateUnmodified := startState{
		SecretUID: secretUID,
		SavedRecovery: &savedSourceRecovery{
			Stage:          "observing",
			PreviousWorker: oldObs,
		},
	}
	if err := svc.advanceSavedRecovery(ctx, nextStart, createdReq, newProfile, b, &stateUnmodified); err != nil {
		t.Fatalf("advanceSavedRecovery(observing start) unmodified failed: %v", err)
	}
	helperName := instanceName(in) + "-prepare-recover"
	helperPod, err := client.CoreV1().Pods(newProfile.Namespace).Get(ctx, helperName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("expected read-only recovery helper pod: %v", err)
	}
	if !helperPod.Spec.Volumes[0].PersistentVolumeClaim.ReadOnly || !helperPod.Spec.Containers[0].VolumeMounts[0].ReadOnly || helperPod.Spec.Containers[0].Image != newProfile.Image {
		t.Fatalf("expected upgraded read-only helper pod, got %+v", helperPod.Spec)
	}
	helperPod.Spec.NodeName = newProfile.NodeName
	helperPod.Status.Phase = corev1.PodRunning
	helperPod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
	_, _ = client.CoreV1().Pods(newProfile.Namespace).Update(ctx, helperPod, metav1.UpdateOptions{})
	if err := svc.advanceSavedRecovery(ctx, nextStart, createdReq, newProfile, b, &stateUnmodified); err != nil {
		t.Fatalf("advanceSavedRecovery(observing ready) MUST NOT fail ErrBinding on unmodified .blend: %v", err)
	}
	for stateUnmodified.SavedRecovery.Stage != "ready" {
		if err := svc.advanceSavedRecovery(ctx, nextStart, createdReq, newProfile, b, &stateUnmodified); err != nil {
			t.Fatalf("advanceSavedRecovery(removing_helper) unmodified failed: %v", err)
		}
	}
	effUnmod, err := effectiveStart(nextStart, stateUnmodified, newProfile)
	if err != nil || effUnmod.source() != nextStart.source() {
		t.Fatalf("effectiveStart MUST accept unmodified working copy, got %+v (%v)", effUnmod, err)
	}
	if stateUnmodified.sourceRecoveryProof(nextStart) != nil {
		t.Fatal("expected nil sourceRecoveryProof when ready working copy equals original source")
	}

	// 4b. Modified working copy (*source != nextStart.source()):
	//     Verify read-only helper observes modified .blend digest and launches upgraded v0.1.25 scheduled worker with modified SHA256!
	modifiedSource := WorkerSource{
		SourceRevisionID: nextStart.SourceRevisionID,
		SHA256:           "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		Size:             8192,
		Empty:            false,
	}
	svc.WorkingCopyObserver = &savedCopyObserver{source: modifiedSource}
	stateModified := startState{
		SecretUID: secretUID,
		SavedRecovery: &savedSourceRecovery{
			Stage:          "observing",
			PreviousWorker: oldObs,
		},
	}
	if err := svc.advanceSavedRecovery(ctx, nextStart, createdReq, newProfile, b, &stateModified); err != nil {
		t.Fatal(err)
	}
	helperPod2, err := client.CoreV1().Pods(newProfile.Namespace).Get(ctx, helperName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	helperPod2.Spec.NodeName = newProfile.NodeName
	helperPod2.Status.Phase = corev1.PodRunning
	helperPod2.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
	_, _ = client.CoreV1().Pods(newProfile.Namespace).Update(ctx, helperPod2, metav1.UpdateOptions{})
	for stateModified.SavedRecovery.Stage != "ready" {
		if err := svc.advanceSavedRecovery(ctx, nextStart, createdReq, newProfile, b, &stateModified); err != nil {
			t.Fatalf("advanceSavedRecovery modified failed: %v", err)
		}
	}
	effMod, err := effectiveStart(nextStart, stateModified, newProfile)
	if err != nil || effMod.source() != modifiedSource {
		t.Fatalf("expected modified source %+v, got %+v (%v)", modifiedSource, effMod.source(), err)
	}
	if srProof := stateModified.sourceRecoveryProof(nextStart); srProof == nil || srProof.Source == nil || *srProof.Source != modifiedSource {
		t.Fatalf("expected non-nil SourceRecoveryProof with modified source, got %+v", srProof)
	}

	newObs, err := EnsureWorker(ctx, client, in, newProfile, b, nextStart.OperationID, secretUID, effMod.source(), WorkerObservation{})
	if err != nil {
		t.Fatalf("EnsureWorker(v0.1.25 upgraded) failed: %v", err)
	}
	newPod, err := client.CoreV1().Pods(newProfile.Namespace).Get(ctx, newObs.PodName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if newPod.Spec.Containers[0].Image != newProfile.Image || newPod.Annotations["station.verdantflare.com/worker-template"] != scheduledWorkerTemplateVersion {
		t.Fatalf("expected upgraded v0.1.25 scheduled worker template, got image=%q tpl=%q", newPod.Spec.Containers[0].Image, newPod.Annotations["station.verdantflare.com/worker-template"])
	}
}
