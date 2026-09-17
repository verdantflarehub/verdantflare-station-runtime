package executor

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

// Dashboard counts can prove occupancy, never safe admission/drain completion.
func activityGuard(ctx context.Context, t Target) Result {
	unavailable := failed("SERVICE_UNAVAILABLE", "Task occupancy is unavailable; disruptive operation was not executed")
	token := os.Getenv(t.ActivityTokenEnv)
	if t.ActivityURL == "" || token == "" {
		return unavailable
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, t.ActivityURL, nil)
	if err != nil {
		return unavailable
	}
	req.Header.Set("Authorization", "Bearer "+token)
	client := http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	res, err := client.Do(req)
	if err != nil {
		return unavailable
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		return unavailable
	}
	var snapshot struct {
		Counts     map[string]int `json:"counts"`
		SyncedAt   *time.Time     `json:"synced_at"`
		SyncErrors *int           `json:"sync_errors"`
	}
	dec := json.NewDecoder(io.LimitReader(res.Body, 2<<20))
	if dec.Decode(&snapshot) != nil || dec.Decode(new(any)) != io.EOF {
		return unavailable
	}
	if snapshot.SyncedAt == nil || snapshot.SyncErrors == nil || *snapshot.SyncErrors != 0 || time.Since(*snapshot.SyncedAt) > 2*time.Minute || time.Until(*snapshot.SyncedAt) > 10*time.Second {
		return unavailable
	}
	for _, name := range []string{"queued", "running", "succeeded", "failed", "cancelled"} {
		if n, ok := snapshot.Counts[name]; !ok || n < 0 {
			return unavailable
		}
	}
	if snapshot.Counts["queued"] > 0 || snapshot.Counts["running"] > 0 {
		return failed("CONFLICT", fmt.Sprintf("Application has active tasks (queued=%d, running=%d); disruptive operation was not executed", snapshot.Counts["queued"], snapshot.Counts["running"]))
	}
	return failed("SERVICE_UNAVAILABLE", "No active tasks reported, but atomic task admission/drain is unavailable; disruptive operation was not executed")
}
