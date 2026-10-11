package appinstances

import (
	"context"
	"errors"
	"fmt"
	"mime"
	"net/http"

	"github.com/verdantflarehub/verdantflare-station-runtime/internal/strictjson"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var ErrUnavailable = errors.New("INSTANCE_SERVICE_UNAVAILABLE")
var ErrNotReady = errors.New("INSTANCE_WORKER_NOT_READY")

type WorkerObserver interface {
	Observe(context.Context, Run, Profile, Observation, string) (string, error)
}

type HTTPWorkerObserver struct{ Client *http.Client }

func (o HTTPWorkerObserver) Observe(ctx context.Context, in Run, p Profile, observed Observation, token string) (string, error) {
	if !in.Identity.valid() || !validID(in.OperationID) || in.Generation < 1 || !p.Valid() || !observed.valid() ||
		observed.PodUID == "" || !credentialValid([]byte(token)) {
		return "", ErrInvalid
	}
	if !observed.PodReady {
		return "", ErrNotReady
	}
	// Never trust an Endpoint supplied in an observation or response.
	endpoint := fmt.Sprintf("http://%s.%s.svc.cluster.local:%d/internal/v1/app-instance/status", runName(in), p.Namespace, p.Port)
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, e := controlledRequest(ctx, o.Client, req)
	if e != nil {
		return "", ErrUnavailable
	}
	defer resp.Body.Close()
	media, _, e := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	if resp.StatusCode != http.StatusOK || e != nil || media != "application/json" || resp.Header.Get("Content-Encoding") != "" || resp.Uncompressed {
		return "", ErrNotReady
	}
	var proof struct {
		SchemaVersion    int    `json:"schema_version"`
		State            string `json:"state"`
		InstanceID       string `json:"instance_id"`
		StartOperationID string `json:"start_operation_id"`
		Generation       int64  `json:"generation"`
		BootID           string `json:"boot_id"`
		Binding          struct {
			PodUID    string `json:"pod_uid"`
			PodName   string `json:"pod_name"`
			Namespace string `json:"namespace"`
			Container string `json:"container"`
		} `json:"resource_binding"`
	}
	if strictjson.Decode(resp.Body, 16<<10, &proof) != nil || proof.SchemaVersion != 1 || proof.State != "ready" ||
		proof.InstanceID != in.InstanceID || proof.StartOperationID != in.OperationID || proof.Generation != in.Generation ||
		!validBoot(proof.BootID) || observed.BootID != "" && observed.BootID != proof.BootID ||
		proof.Binding.PodUID != observed.PodUID || proof.Binding.PodName != runName(in) || proof.Binding.Namespace != p.Namespace || proof.Binding.Container != "worker" {
		return "", ErrBinding
	}
	return proof.BootID, nil
}

func (j *Journal) verifyWorker(ctx context.Context, in Run, p Profile, r StartRecord) (Observation, error) {
	if j.Observer == nil {
		return Observation{}, ErrUnavailable
	}
	current, e := ObserveWorker(ctx, j.Client, in, p, r.Volume, r.Observation)
	if e != nil {
		return Observation{}, e
	}
	current.BootID = r.Observation.BootID
	secret, e := j.Client.CoreV1().Secrets(p.Namespace).Get(ctx, instanceName(in.Identity), metav1.GetOptions{})
	if e != nil || !secretMatches(secret, in.Identity, p, current.SecretUID) {
		return Observation{}, ErrBinding
	}
	boot, e := j.Observer.Observe(ctx, in, p, current, string(secret.Data["worker-token"]))
	if e != nil {
		return Observation{}, e
	}
	if !validBoot(boot) || current.BootID != "" && current.BootID != boot {
		return Observation{}, ErrBinding
	}
	// Re-read the resources after the HTTP exchange. A probe from an old worker
	// must not certify a replaced or no-longer-ready Pod/Service/volume binding.
	latest, e := ObserveWorker(ctx, j.Client, in, p, r.Volume, current)
	if e != nil {
		return Observation{}, e
	}
	if !latest.PodReady {
		return Observation{}, ErrNotReady
	}
	latest.BootID = boot
	return latest, nil
}
