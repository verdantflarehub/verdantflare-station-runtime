package telemetry

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestPromClient_GetGPUs(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query().Get("query")
		resp := promResponse{Status: "success"}
		resp.Data.ResultType = "vector"

		switch query {
		case "DCGM_FI_DEV_FB_FREE":
			resp.Data.Result = []struct {
				Metric map[string]string `json:"metric"`
				Value  []interface{}     `json:"value"`
			}{
				{
					Metric: map[string]string{
						"gpu":       "0",
						"device":    "nvidia0",
						"modelName": "NVIDIA GeForce RTX 5090",
						"UUID":      "GPU-001",
					},
					Value: []interface{}{float64(1700000000), "26000"},
				},
				{
					Metric: map[string]string{
						"gpu":       "1",
						"device":    "nvidia1",
						"modelName": "NVIDIA GeForce RTX 5090",
						"UUID":      "GPU-002",
					},
					Value: []interface{}{float64(1700000000), "30000"},
				},
			}
		case "DCGM_FI_DEV_FB_TOTAL":
			resp.Data.Result = []struct {
				Metric map[string]string `json:"metric"`
				Value  []interface{}     `json:"value"`
			}{
				{
					Metric: map[string]string{"UUID": "GPU-001"},
					Value:  []interface{}{float64(1700000000), "32768"},
				},
				{
					Metric: map[string]string{"UUID": "GPU-002"},
					Value:  []interface{}{float64(1700000000), "32768"},
				},
			}
		case "DCGM_FI_DEV_GPU_UTIL":
			resp.Data.Result = []struct {
				Metric map[string]string `json:"metric"`
				Value  []interface{}     `json:"value"`
			}{
				{
					Metric: map[string]string{"UUID": "GPU-001"},
					Value:  []interface{}{float64(1700000000), "15.5"},
				},
			}
		case "DCGM_FI_DEV_GPU_TEMP":
			resp.Data.Result = []struct {
				Metric map[string]string `json:"metric"`
				Value  []interface{}     `json:"value"`
			}{
				{
					Metric: map[string]string{"UUID": "GPU-001"},
					Value:  []interface{}{float64(1700000000), "45"},
				},
			}
		case "DCGM_FI_DEV_POWER_USAGE":
			resp.Data.Result = []struct {
				Metric map[string]string `json:"metric"`
				Value  []interface{}     `json:"value"`
			}{
				{
					Metric: map[string]string{"UUID": "GPU-001"},
					Value:  []interface{}{float64(1700000000), "120.4"},
				},
			}
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}))
	defer ts.Close()

	client := NewPromClient(ts.URL, time.Second)
	gpus, err := client.GetGPUs(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(gpus) != 2 {
		t.Fatalf("expected 2 GPUs, got %d", len(gpus))
	}

	gpu0 := gpus[0]
	if gpu0.Index != 0 || gpu0.FreeVRAMMB != 26000 || gpu0.TotalVRAMMB != 32768 || gpu0.UsedVRAMMB != 6768 {
		t.Errorf("unexpected gpu0 stats: %+v", gpu0)
	}
	if gpu0.Utilization != 15.5 || gpu0.TemperatureC != 45 || gpu0.PowerWatts != 120.4 {
		t.Errorf("unexpected gpu0 extra metrics: %+v", gpu0)
	}

	// Test CachedClient
	cached := NewCachedClient(client, 100*time.Millisecond)
	gpusCached, err := cached.GetGPUs(context.Background())
	if err != nil {
		t.Fatalf("cached client get failed: %v", err)
	}
	if len(gpusCached) != 2 {
		t.Errorf("cached gpus length mismatch: %d", len(gpusCached))
	}
}
