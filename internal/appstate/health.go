package appstate

import (
	"context"
	"net/http"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"k8s.io/apimachinery/pkg/util/validation"
)

// HealthTarget is operator configuration, never derived from resource labels.
// The Service health endpoint represents the configured management service.
type HealthTarget struct {
	Namespace    string `json:"namespace"`
	WorkloadName string `json:"workloadName"`
	WorkloadUID  string `json:"workloadUID"`
	ServiceName  string `json:"serviceName"`
	Port         int    `json:"port"`
	Path         string `json:"path"`
}

func validHealthTargets(targets []HealthTarget, namespaces []string) bool {
	if len(targets) > 32 {
		return false
	}
	seen := map[string]bool{}
	for _, t := range targets {
		key := t.Namespace + "/" + t.WorkloadName
		uid, err := uuid.Parse(t.WorkloadUID)
		if !slices.Contains(namespaces, t.Namespace) || len(validation.IsDNS1123Label(t.Namespace)) != 0 ||
			len(validation.IsDNS1123Subdomain(t.WorkloadName)) != 0 || len(validation.IsDNS1035Label(t.ServiceName)) != 0 ||
			err != nil || uid.String() != t.WorkloadUID || uid == uuid.Nil || t.Port < 1 || t.Port > 65535 ||
			len(t.Path) > 256 || !regexp.MustCompile(`^/[A-Za-z0-9/_-]*$`).MatchString(t.Path) || strings.Contains(t.Path, "//") || seen[key] {
			return false
		}
		seen[key] = true
	}
	return true
}

func newHealthClient() *http.Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	transport.DisableCompression = true
	transport.MaxConnsPerHost = 4
	transport.MaxResponseHeaderBytes = 8192
	return &http.Client{Transport: transport, Timeout: 2 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
}

func probeHealth(ctx context.Context, client *http.Client, target HealthTarget) string {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	endpoint := "http://" + target.ServiceName + "." + target.Namespace + ".svc:" + strconv.Itoa(target.Port) + target.Path
	r, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return "failed"
	}
	response, err := client.Do(r)
	if err != nil {
		return "failed"
	}
	defer response.Body.Close()
	if response.StatusCode >= 200 && response.StatusCode < 300 {
		return "ready"
	}
	return "failed"
}

type healthResult struct {
	frame Health
	at    time.Time
}

type healthRound struct {
	mu      sync.Mutex
	objects map[string]ResourceObservation
	results chan healthResult
	cancel  context.CancelFunc
	wg      sync.WaitGroup
}

func healthEligible(o ResourceObservation, target HealthTarget) bool {
	return o.Namespace == target.Namespace && o.Name == target.WorkloadName && o.UID == target.WorkloadUID &&
		o.InstanceID == "" && !o.Deleted && !o.Terminating && o.Desired > 0
}

// Every List/Watch round owns its probes; stop joins them before resynchronizing.
// Bounded goroutines and a semaphore prevent slow endpoints from growing a queue.
func startHealthRound(ctx context.Context, targets []HealthTarget, resources []ResourceObservation, client *http.Client) *healthRound {
	ctx, cancel := context.WithCancel(ctx)
	h := &healthRound{objects: map[string]ResourceObservation{}, results: make(chan healthResult, len(targets)), cancel: cancel}
	for _, o := range resources {
		h.objects[o.Namespace+"/"+o.Name] = o
	}
	limit := make(chan struct{}, 4)
	for _, target := range targets {
		h.wg.Add(1)
		go func(target HealthTarget) {
			defer h.wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case limit <- struct{}{}:
				}
				h.mu.Lock()
				o := h.objects[target.Namespace+"/"+target.WorkloadName]
				h.mu.Unlock()
				if ctx.Err() == nil && healthEligible(o, target) {
					state := probeHealth(ctx, client, target)
					result := healthResult{Health{1, "health", o.Namespace, o.Name, o.UID, o.ResourceVersion, state}, time.Now()}
					if ctx.Err() == nil {
						select {
						case h.results <- result:
						default: // Drop rather than queue stale health or block Watch.
						}
					}
				}
				<-limit
				timer := time.NewTimer(10 * time.Second)
				select {
				case <-ctx.Done():
					timer.Stop()
					return
				case <-timer.C:
				}
			}
		}(target)
	}
	return h
}

func (h *healthRound) update(o ResourceObservation) {
	h.mu.Lock()
	defer h.mu.Unlock()
	key := o.Namespace + "/" + o.Name
	if o.Deleted {
		if h.objects[key].UID == o.UID {
			delete(h.objects, key)
		}
	} else {
		h.objects[key] = o
	}
}

func (h *healthRound) current(result healthResult) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	f := result.frame
	o, exists := h.objects[f.Namespace+"/"+f.Name]
	return exists && o.UID == f.UID && o.ResourceVersion == f.ResourceVersion &&
		o.InstanceID == "" && !o.Terminating && !o.Deleted && o.Desired > 0 && time.Since(result.at) < 2*time.Second
}

func (h *healthRound) stop() {
	h.cancel()
	h.wg.Wait()
}
