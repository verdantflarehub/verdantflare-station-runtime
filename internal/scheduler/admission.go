package scheduler

import (
	"context"
	"fmt"

	"github.com/verdantflarehub/verdantflare-station-runtime/internal/telemetry"
)

const (
	// DefaultSafetyHeadroomMB represents the required safety buffer (2GB)
	// to prevent dynamic activation / KV-cache VRAM spikes from triggering CUDA OOM.
	DefaultSafetyHeadroomMB int64 = 2048
)

// TaskResourceRequest specifies resource requirements for task admission.
type TaskResourceRequest struct {
	TaskID            string `json:"task_id"`
	RequiredVRAMMB    int64  `json:"required_vram_mb"`
	PreferredGPUIndex *int   `json:"preferred_gpu_index,omitempty"`
}

// AdmissionDecision contains the result of admission scheduling.
type AdmissionDecision struct {
	Admitted         bool   `json:"admitted"`
	GPUIndex         int    `json:"gpu_index"`
	GPUUUID          string `json:"gpu_uuid"`
	AvailableVRAMMB  int64  `json:"available_vram_mb"`
	SafetyHeadroomMB int64  `json:"safety_headroom_mb"`
	Reason           string `json:"reason"`
}

// AdmissionController manages GPU resource admission with anti-OOM safety headroom.
type AdmissionController struct {
	telemetryClient  telemetry.Client
	safetyHeadroomMB int64
}

// NewAdmissionController creates a new AdmissionController.
func NewAdmissionController(telemetryClient telemetry.Client, safetyHeadroomMB int64) *AdmissionController {
	if safetyHeadroomMB <= 0 {
		safetyHeadroomMB = DefaultSafetyHeadroomMB
	}
	return &AdmissionController{
		telemetryClient:  telemetryClient,
		safetyHeadroomMB: safetyHeadroomMB,
	}
}

// CanAdmit evaluates whether a task can be safely scheduled onto an available GPU.
func (ac *AdmissionController) CanAdmit(ctx context.Context, req TaskResourceRequest) (AdmissionDecision, error) {
	if req.RequiredVRAMMB <= 0 {
		// Zero VRAM requested (CPU-only task or lightweight synchronous task)
		return AdmissionDecision{
			Admitted:         true,
			GPUIndex:         -1,
			GPUUUID:          "",
			AvailableVRAMMB:  0,
			SafetyHeadroomMB: ac.safetyHeadroomMB,
			Reason:           "CPU-only task or zero VRAM requested",
		}, nil
	}

	gpus, err := ac.telemetryClient.GetGPUs(ctx)
	if err != nil {
		return AdmissionDecision{
			Admitted: false,
			Reason:   fmt.Sprintf("telemetry error: %v", err),
		}, err
	}

	if len(gpus) == 0 {
		return AdmissionDecision{
			Admitted: false,
			Reason:   "no GPUs discovered in cluster telemetry",
		}, nil
	}

	totalNeeded := req.RequiredVRAMMB + ac.safetyHeadroomMB

	// Check if preferred GPU satisfies requirement
	if req.PreferredGPUIndex != nil {
		for _, gpu := range gpus {
			if gpu.Index == *req.PreferredGPUIndex {
				if gpu.FreeVRAMMB >= totalNeeded {
					return AdmissionDecision{
						Admitted:         true,
						GPUIndex:         gpu.Index,
						GPUUUID:          gpu.UUID,
						AvailableVRAMMB:  gpu.FreeVRAMMB,
						SafetyHeadroomMB: ac.safetyHeadroomMB,
						Reason:           fmt.Sprintf("preferred GPU %d admitted (free %dMB >= required %dMB + headroom %dMB)", gpu.Index, gpu.FreeVRAMMB, req.RequiredVRAMMB, ac.safetyHeadroomMB),
					}, nil
				}
				break
			}
		}
	}

	// Strategy: select GPU with maximum free VRAM among candidates satisfying totalNeeded
	var bestGPU *telemetry.GPUMetric
	var maxFree int64 = -1

	for i := range gpus {
		gpu := &gpus[i]
		if gpu.FreeVRAMMB > maxFree {
			maxFree = gpu.FreeVRAMMB
		}
		if gpu.FreeVRAMMB >= totalNeeded {
			if bestGPU == nil || gpu.FreeVRAMMB > bestGPU.FreeVRAMMB {
				bestGPU = gpu
			}
		}
	}

	if bestGPU != nil {
		return AdmissionDecision{
			Admitted:         true,
			GPUIndex:         bestGPU.Index,
			GPUUUID:          bestGPU.UUID,
			AvailableVRAMMB:  bestGPU.FreeVRAMMB,
			SafetyHeadroomMB: ac.safetyHeadroomMB,
			Reason:           fmt.Sprintf("admitted to GPU %d (free %dMB >= required %dMB + headroom %dMB)", bestGPU.Index, bestGPU.FreeVRAMMB, req.RequiredVRAMMB, ac.safetyHeadroomMB),
		}, nil
	}

	// Insufficient VRAM: queue task
	return AdmissionDecision{
		Admitted:         false,
		GPUIndex:         -1,
		AvailableVRAMMB:  maxFree,
		SafetyHeadroomMB: ac.safetyHeadroomMB,
		Reason:           fmt.Sprintf("insufficient VRAM: requested %dMB (+%dMB headroom = %dMB needed), but maximum available is %dMB", req.RequiredVRAMMB, ac.safetyHeadroomMB, totalNeeded, maxFree),
	}, nil
}
