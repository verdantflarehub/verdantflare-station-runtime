package scheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"
)

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

func TestResidencyPodAbsenceRequiresFreshCompleteNode(t *testing.T) {
	now := time.Now()
	old := now.Add(-time.Hour)
	r := residencyRow{NodeUID: "node", PodUID: "POD_UID"}
	cards := []cardLockState{{NodeUID: "node", TelemetrySampledAt: &now}, {NodeUID: "node", TelemetrySampledAt: &now}}
	if absent, live := residencyPodAbsent(cards, r, now, time.Minute); !absent || live {
		t.Fatal("idle cards not detected")
	}
	cards[1].TelemetrySampledAt = nil
	if absent, _ := residencyPodAbsent(cards, r, now, time.Minute); absent {
		t.Fatal("missing telemetry accepted")
	}
	cards[1].TelemetrySampledAt = &old
	if absent, _ := residencyPodAbsent(cards, r, now, time.Minute); absent {
		t.Fatal("stale telemetry accepted")
	}
	cards[1].ActiveGPUPods = []ActiveGPUPodInfo{{PodUID: "pod-uid"}}
	if absent, live := residencyPodAbsent(cards, r, now, time.Minute); absent || !live {
		t.Fatal("live normalized Pod not protected")
	}
	if absent, _ := residencyPodAbsent(nil, r, now, time.Minute); absent {
		t.Fatal("missing node accepted")
	}
}

func TestRestartReconcilesExitedResidenciesBeforeMusic3Admission(t *testing.T) {
	for _, live := range []bool{false, true} {
		t.Run(fmt.Sprint("live=", live), func(t *testing.T) {
			fix := setupSchedulerDB(t)
			ctx := context.Background()
			for i, state := range []string{"draining_hard", "idle_evictable"} {
				_, err := fix.pool.Exec(ctx, `INSERT INTO station.gpu_instance_residency(station_id,instance_id,app_id,node_uid,pod_uid,container_id,boot_id,gpu_epoch,residency_seq,gpu_uuid,residency_state,pinned_budget_mib,vram_allocated_mib) VALUES($1,$2,'app-blender',$3,$4,'cid',$5,1,1,$6,$7,6144,6144)`, fix.stationID, fmt.Sprint("orphan-", i), fix.nodeUID, fmt.Sprint("pod-", i), fmt.Sprint("boot-", i), fmt.Sprintf("GPU-%04d", i), state)
				if err != nil {
					t.Fatal(err)
				}
			}
			if live {
				raw, _ := json.Marshal([]ActiveGPUPodInfo{{PodUID: "pod-0"}, {PodUID: "pod-1"}})
				if _, err := fix.pool.Exec(ctx, `UPDATE station.gpu_physical_cards SET active_gpu_pods=$2 WHERE station_id=$1`, fix.stationID, raw); err != nil {
					t.Fatal(err)
				}
			}
			if err := fix.svc.ReconcileRestart(ctx, RestartObservation{}); err != nil {
				t.Fatal(err)
			}
			for i, state := range []string{"draining_hard", "idle_evictable"} {
				var got string
				var seq int64
				if err := fix.pool.QueryRow(ctx, `SELECT residency_state,residency_seq FROM station.gpu_instance_residency WHERE station_id=$1 AND instance_id=$2`, fix.stationID, fmt.Sprint("orphan-", i)).Scan(&got, &seq); err != nil {
					t.Fatal(err)
				}
				if live {
					if got != state || seq != 1 {
						t.Fatal("live residency changed", got, seq)
					}
				} else if got != "zero_vram_standby" || seq != 2 {
					t.Fatal("stale residency not reconciled", got, seq)
				}
			}
			if !live {
				decision, err := fix.svc.Acquire(ctx, CallerIdentity{AppID: "app-music", Kind: CallerConsumer}, AcquireRequest{IdempotencyKey: "music3-after-reconcile", ConsumerKind: "music3", AttemptID: "attempt", PeakBudgetMiB: 29696, CardCount: 2})
				if err != nil || decision.State != "leased" || len(decision.GPUUUIDs) != 2 {
					t.Fatal("empty dual card admission failed", decision, err)
				}
			}
			verifyAllCardHashChains(t, ctx, fix.pool, fix.stationID)
		})
	}
}
