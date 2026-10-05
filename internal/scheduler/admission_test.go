package scheduler

import (
	"context"
	"testing"

	"github.com/verdantflarehub/verdantflare-station-runtime/internal/telemetry"
)

type mockTelemetry struct {
	gpus []telemetry.GPUMetric
	err  error
}

func (m *mockTelemetry) GetGPUs(ctx context.Context) ([]telemetry.GPUMetric, error) {
	return m.gpus, m.err
}

func (m *mockTelemetry) GetNode(ctx context.Context) (*telemetry.NodeMetric, error) {
	return &telemetry.NodeMetric{}, nil
}

func TestAdmissionController_CanAdmit(t *testing.T) {
	mock := &mockTelemetry{
		gpus: []telemetry.GPUMetric{
			{
				Index:       0,
				UUID:        "GPU-0",
				ModelName:   "NVIDIA GeForce RTX 5090",
				FreeVRAMMB:  16000,
				TotalVRAMMB: 32768,
			},
			{
				Index:       1,
				UUID:        "GPU-1",
				ModelName:   "NVIDIA GeForce RTX 5090",
				FreeVRAMMB:  28000,
				TotalVRAMMB: 32768,
			},
		},
	}

	ac := NewAdmissionController(mock, 2048)

	// Case 1: CPU-only / Zero VRAM
	dec, err := ac.CanAdmit(context.Background(), TaskResourceRequest{
		TaskID:         "task-cpu",
		RequiredVRAMMB: 0,
	})
	if err != nil || !dec.Admitted {
		t.Fatalf("expected admitted for cpu task, got %+v, err: %v", dec, err)
	}

	// Case 2: Fits on GPU 1 (needs 20000 + 2048 = 22048MB, GPU 0 has 16000MB, GPU 1 has 28000MB)
	dec, err = ac.CanAdmit(context.Background(), TaskResourceRequest{
		TaskID:         "task-large",
		RequiredVRAMMB: 20000,
	})
	if err != nil || !dec.Admitted {
		t.Fatalf("expected admitted on GPU 1, got %+v, err: %v", dec, err)
	}
	if dec.GPUIndex != 1 || dec.GPUUUID != "GPU-1" {
		t.Errorf("expected GPU 1, got %d", dec.GPUIndex)
	}

	// Case 3: Preferred GPU 0 (needs 12000 + 2048 = 14048MB, GPU 0 has 16000MB)
	pref0 := 0
	dec, err = ac.CanAdmit(context.Background(), TaskResourceRequest{
		TaskID:            "task-pref",
		RequiredVRAMMB:    12000,
		PreferredGPUIndex: &pref0,
	})
	if err != nil || !dec.Admitted {
		t.Fatalf("expected admitted on preferred GPU 0, got %+v", dec)
	}
	if dec.GPUIndex != 0 {
		t.Errorf("expected GPU 0, got %d", dec.GPUIndex)
	}

	// Case 4: Insufficient VRAM (needs 30000 + 2048 = 32048MB, max available is 28000MB)
	dec, err = ac.CanAdmit(context.Background(), TaskResourceRequest{
		TaskID:         "task-too-huge",
		RequiredVRAMMB: 30000,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if dec.Admitted {
		t.Errorf("expected rejection/queueing for task needing 30GB+2GB, but was admitted: %+v", dec)
	}
}
