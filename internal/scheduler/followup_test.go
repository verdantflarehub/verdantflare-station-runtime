package scheduler

import "testing"

func TestMusic3MeasuredPeakFitsEmpty5090(t *testing.T) {
	spec, ok := DefaultConsumerSpec("music3")
	if !ok || spec.CardCount != 2 || spec.PeakBudgetMiB < 28138 || spec.HeadroomOverrideMiB == nil || spec.PeakBudgetMiB+*spec.HeadroomOverrideMiB > 32000 {
		t.Fatalf("invalid measured Music3 profile: %+v", spec)
	}
}

func TestNeverGrantedUsesAssignmentHistoryNotCandidateIdentity(t *testing.T) {
	row := activeLeaseRow{State: "expired", PodUID: "candidate", ContainerID: "candidate", BootID: "candidate", FencingToken: 12}
	if !decisionFromRow(row, 0, 0).NeverGranted {
		t.Fatal("candidate identity misclassified as grant")
	}
	row.GPUUUIDs = []string{"GPU-allocated"}
	if decisionFromRow(row, 0, 0).NeverGranted {
		t.Fatal("expired execution must remain uncertain")
	}
	row.GPUUUIDs = nil
	row.State = "running"
	if decisionFromRow(row, 0, 0).NeverGranted {
		t.Fatal("invalid running record must fail closed")
	}
}
