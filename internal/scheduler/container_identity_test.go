package scheduler

import "testing"

func TestHolderMatchesAttributedProcess(t *testing.T) {
	podUID := "24762719-e167-47e5-8934-5650debeb98c"
	otherPodUID := "11111111-2222-4333-8444-555555555555"
	hexCID := "ed8620ca5dc7de08630d8e9f13b33f5aecd30a0646546e4e5c24f8cd37d214f1"
	otherHexCID := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

	// 1. Pod-scoped container ID ("pod-<uid>-worker") matches 64-hex host cgroup container ID on the same Pod UID.
	if !holderMatchesAttributedProcess(podUID, "pod-"+podUID+"-worker", podUID, hexCID) {
		t.Fatal("expected pod-scoped container_id to match 64-hex cgroup container_id on matching pod_uid")
	}

	// 2. Spoofed pod-scoped container ID with wrong embedded Pod UID MUST fail closed.
	if holderMatchesAttributedProcess(podUID, "pod-"+otherPodUID+"-worker", podUID, hexCID) {
		t.Fatal("expected mismatched embedded pod_uid to be rejected")
	}
	if holderMatchesAttributedProcess(podUID, "pod-"+podUID+"-worker", otherPodUID, hexCID) {
		t.Fatal("expected different attributed process pod_uid to be rejected")
	}

	// 3. Two distinct 64-hex container IDs inside the same Pod MUST remain strictly separated.
	if holderMatchesAttributedProcess(podUID, hexCID, podUID, otherHexCID) {
		t.Fatal("expected distinct 64-hex container IDs in the same Pod not to cross-match")
	}

	// 4. Exact 64-hex match on matching Pod UID succeeds.
	if !holderMatchesAttributedProcess(podUID, "containerd://"+hexCID, podUID, hexCID) {
		t.Fatal("expected exact 64-hex match after prefix normalization")
	}
}
