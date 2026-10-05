package telemetry

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"
)

// GPUMetric represents real-time telemetry of a single GPU device.
type GPUMetric struct {
	Index        int     `json:"index"`
	Device       string  `json:"device"`
	ModelName    string  `json:"model_name"`
	UUID         string  `json:"uuid"`
	FreeVRAMMB   int64   `json:"free_vram_mb"`
	TotalVRAMMB  int64   `json:"total_vram_mb"`
	UsedVRAMMB   int64   `json:"used_vram_mb"`
	Utilization  float64 `json:"utilization"`
	TemperatureC float64 `json:"temperature_c"`
	PowerWatts   float64 `json:"power_watts"`
}

// NodeMetric represents real-time host metrics.
type NodeMetric struct {
	CPUUtilizationPercent float64 `json:"cpu_utilization_percent"`
	MemTotalBytes         uint64  `json:"mem_total_bytes"`
	MemAvailableBytes     uint64  `json:"mem_available_bytes"`
	MemUsedPercent        float64 `json:"mem_used_percent"`
}

// Client defines the interface for querying metrics from Prometheus.
type Client interface {
	GetGPUs(ctx context.Context) ([]GPUMetric, error)
	GetNode(ctx context.Context) (*NodeMetric, error)
}

// PromClient implements Client against a Prometheus HTTP API.
type PromClient struct {
	baseURL    string
	httpClient *http.Client
}

// NewPromClient creates a new Prometheus telemetry client.
func NewPromClient(baseURL string, timeout time.Duration) *PromClient {
	if baseURL == "" {
		baseURL = "http://prometheus.verdantflare-station.svc.cluster.local:9090"
	}
	if timeout <= 0 {
		timeout = 3 * time.Second
	}
	return &PromClient{
		baseURL: baseURL,
		httpClient: &http.Client{
			Timeout: timeout,
		},
	}
}

type promResponse struct {
	Status string `json:"status"`
	Data   struct {
		ResultType string `json:"resultType"`
		Result     []struct {
			Metric map[string]string `json:"metric"`
			Value  []interface{}     `json:"value"` // [unix_timestamp, "value_string"]
		} `json:"result"`
	} `json:"data"`
}

// QueryVector executes an instant PromQL query returning vector samples.
func (c *PromClient) QueryVector(ctx context.Context, promQL string) (*promResponse, error) {
	reqURL := fmt.Sprintf("%s/api/v1/query?query=%s", c.baseURL, url.QueryEscape(promQL))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, fmt.Errorf("create prometheus request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("prometheus query failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("prometheus returned status %d", resp.StatusCode)
	}

	var parsed promResponse
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return nil, fmt.Errorf("decode prometheus response: %w", err)
	}
	if parsed.Status != "success" {
		return nil, fmt.Errorf("prometheus returned non-success status: %s", parsed.Status)
	}

	return &parsed, nil
}

// GetGPUs fetches and combines DCGM metrics for all visible GPUs.
func (c *PromClient) GetGPUs(ctx context.Context) ([]GPUMetric, error) {
	freeResp, err := c.QueryVector(ctx, "DCGM_FI_DEV_FB_FREE")
	if err != nil {
		return nil, fmt.Errorf("query DCGM_FI_DEV_FB_FREE: %w", err)
	}

	gpusMap := make(map[string]*GPUMetric)
	for _, r := range freeResp.Data.Result {
		gpuIdx, _ := strconv.Atoi(r.Metric["gpu"])
		freeVal, _ := parsePromValue(r.Value)
		m := &GPUMetric{
			Index:      gpuIdx,
			Device:     r.Metric["device"],
			ModelName:  r.Metric["modelName"],
			UUID:       r.Metric["UUID"],
			FreeVRAMMB: int64(freeVal),
		}
		gpusMap[r.Metric["UUID"]] = m
	}

	// Enrich with total VRAM
	if totalResp, err := c.QueryVector(ctx, "DCGM_FI_DEV_FB_TOTAL"); err == nil {
		for _, r := range totalResp.Data.Result {
			if m, ok := gpusMap[r.Metric["UUID"]]; ok {
				val, _ := parsePromValue(r.Value)
				m.TotalVRAMMB = int64(val)
				m.UsedVRAMMB = m.TotalVRAMMB - m.FreeVRAMMB
				if m.UsedVRAMMB < 0 {
					m.UsedVRAMMB = 0
				}
			}
		}
	}

	// Enrich with GPU Utilization
	if utilResp, err := c.QueryVector(ctx, "DCGM_FI_DEV_GPU_UTIL"); err == nil {
		for _, r := range utilResp.Data.Result {
			if m, ok := gpusMap[r.Metric["UUID"]]; ok {
				val, _ := parsePromValue(r.Value)
				m.Utilization = val
			}
		}
	}

	// Enrich with GPU Temperature
	if tempResp, err := c.QueryVector(ctx, "DCGM_FI_DEV_GPU_TEMP"); err == nil {
		for _, r := range tempResp.Data.Result {
			if m, ok := gpusMap[r.Metric["UUID"]]; ok {
				val, _ := parsePromValue(r.Value)
				m.TemperatureC = val
			}
		}
	}

	// Enrich with Power Usage
	if powerResp, err := c.QueryVector(ctx, "DCGM_FI_DEV_POWER_USAGE"); err == nil {
		for _, r := range powerResp.Data.Result {
			if m, ok := gpusMap[r.Metric["UUID"]]; ok {
				val, _ := parsePromValue(r.Value)
				m.PowerWatts = val
			}
		}
	}

	// Sort / return slice by index
	result := make([]GPUMetric, 0, len(gpusMap))
	for _, m := range gpusMap {
		result = append(result, *m)
	}

	// Sort by Index ascending
	for i := 0; i < len(result)-1; i++ {
		for j := i + 1; j < len(result); j++ {
			if result[i].Index > result[j].Index {
				result[i], result[j] = result[j], result[i]
			}
		}
	}

	return result, nil
}

// GetNode fetches host CPU and memory usage from node-exporter.
func (c *PromClient) GetNode(ctx context.Context) (*NodeMetric, error) {
	node := &NodeMetric{}

	// CPU utilization: 100 - (avg(irate(node_cpu_seconds_total{mode="idle"}[1m])) * 100)
	cpuResp, err := c.QueryVector(ctx, `100 - (avg(irate(node_cpu_seconds_total{mode="idle"}[1m])) * 100)`)
	if err == nil && len(cpuResp.Data.Result) > 0 {
		val, _ := parsePromValue(cpuResp.Data.Result[0].Value)
		node.CPUUtilizationPercent = val
	}

	// Total Memory
	totalMemResp, err := c.QueryVector(ctx, "node_memory_MemTotal_bytes")
	if err == nil && len(totalMemResp.Data.Result) > 0 {
		val, _ := parsePromValue(totalMemResp.Data.Result[0].Value)
		node.MemTotalBytes = uint64(val)
	}

	// Available Memory
	availMemResp, err := c.QueryVector(ctx, "node_memory_MemAvailable_bytes")
	if err == nil && len(availMemResp.Data.Result) > 0 {
		val, _ := parsePromValue(availMemResp.Data.Result[0].Value)
		node.MemAvailableBytes = uint64(val)
	}

	if node.MemTotalBytes > 0 {
		used := float64(node.MemTotalBytes - node.MemAvailableBytes)
		node.MemUsedPercent = (used / float64(node.MemTotalBytes)) * 100.0
	}

	return node, nil
}

func parsePromValue(val []interface{}) (float64, error) {
	if len(val) < 2 {
		return 0, fmt.Errorf("invalid value slice: %v", val)
	}
	strVal, ok := val[1].(string)
	if !ok {
		return 0, fmt.Errorf("value is not string: %v", val[1])
	}
	return strconv.ParseFloat(strVal, 64)
}

// CachedClient provides an in-memory TTL cache around Client.
type CachedClient struct {
	upstream   Client
	ttl        time.Duration
	mu         sync.RWMutex
	lastGPUAt  time.Time
	cachedGPUs []GPUMetric
	lastNodeAt time.Time
	cachedNode *NodeMetric
}

// NewCachedClient wraps an upstream telemetry client with an in-memory TTL cache.
func NewCachedClient(upstream Client, ttl time.Duration) *CachedClient {
	if ttl <= 0 {
		ttl = 2 * time.Second
	}
	return &CachedClient{
		upstream: upstream,
		ttl:      ttl,
	}
}

// GetGPUs returns cached GPUs or fetches from upstream.
func (c *CachedClient) GetGPUs(ctx context.Context) ([]GPUMetric, error) {
	c.mu.RLock()
	if time.Since(c.lastGPUAt) < c.ttl && c.cachedGPUs != nil {
		defer c.mu.RUnlock()
		return c.cachedGPUs, nil
	}
	c.mu.RUnlock()

	c.mu.Lock()
	defer c.mu.Unlock()
	if time.Since(c.lastGPUAt) < c.ttl && c.cachedGPUs != nil {
		return c.cachedGPUs, nil
	}

	gpus, err := c.upstream.GetGPUs(ctx)
	if err != nil {
		if c.cachedGPUs != nil {
			return c.cachedGPUs, nil // Graceful degradation to stale data
		}
		return nil, err
	}
	c.cachedGPUs = gpus
	c.lastGPUAt = time.Now()
	return gpus, nil
}

// GetNode returns cached Node metrics or fetches from upstream.
func (c *CachedClient) GetNode(ctx context.Context) (*NodeMetric, error) {
	c.mu.RLock()
	if time.Since(c.lastNodeAt) < c.ttl && c.cachedNode != nil {
		defer c.mu.RUnlock()
		return c.cachedNode, nil
	}
	c.mu.RUnlock()

	c.mu.Lock()
	defer c.mu.Unlock()
	if time.Since(c.lastNodeAt) < c.ttl && c.cachedNode != nil {
		return c.cachedNode, nil
	}

	node, err := c.upstream.GetNode(ctx)
	if err != nil {
		if c.cachedNode != nil {
			return c.cachedNode, nil // Graceful degradation
		}
		return nil, err
	}
	c.cachedNode = node
	c.lastNodeAt = time.Now()
	return node, nil
}
