package appinstances

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// WorkerAccess is a private App-service response, never a Dashboard projection.
type WorkerAccess struct {
	SchemaVersion    int             `json:"schema_version"`
	InstanceID       string          `json:"instance_id"`
	StartOperationID string          `json:"start_operation_id"`
	Generation       int64           `json:"generation"`
	BootID           string          `json:"boot_id"`
	WorkerOrigin     string          `json:"worker_origin"`
	WorkerToken      string          `json:"worker_token"`
	Binding          ResourceBinding `json:"resource_binding"`
}

type ResourceBinding struct {
	PodUID    string `json:"pod_uid"`
	PodName   string `json:"pod_name"`
	Namespace string `json:"namespace"`
	Container string `json:"container"`
}

func workerOrigin(in Run, p Profile) string {
	return fmt.Sprintf("http://%s.%s.svc.cluster.local:%d", runName(in), p.Namespace, p.Port)
}

func (v WorkerAccess) valid(in Run, p Profile) bool {
	return v.SchemaVersion == 1 && v.InstanceID == in.InstanceID && v.StartOperationID == in.OperationID && v.Generation == in.Generation &&
		validBoot(v.BootID) && v.WorkerOrigin == workerOrigin(in, p) && credentialValid([]byte(v.WorkerToken)) &&
		validUID(v.Binding.PodUID) && v.Binding.PodName == runName(in) && v.Binding.Namespace == p.Namespace && v.Binding.Container == "worker"
}

func (j *Journal) Access(ctx context.Context, in Run, p Profile) (WorkerAccess, error) {
	out := WorkerAccess{}
	_, e := j.withRecord(ctx, in, p, func(_ *pgxpool.Conn, r *StartRecord) error {
		if r.Phase != "running" || r.Observation.BootID == "" {
			return ErrNotReady
		}
		current, e := j.verifyWorker(ctx, in, p, *r)
		if e != nil {
			return e
		}
		secret, e := j.Client.CoreV1().Secrets(p.Namespace).Get(ctx, instanceName(in.Identity), metav1.GetOptions{})
		if e != nil || !secretMatches(secret, in.Identity, p, current.SecretUID) {
			return ErrBinding
		}
		out = WorkerAccess{SchemaVersion: 1, InstanceID: in.InstanceID, StartOperationID: in.OperationID, Generation: in.Generation,
			BootID: current.BootID, WorkerOrigin: workerOrigin(in, p), WorkerToken: string(secret.Data["worker-token"]),
			Binding: ResourceBinding{current.PodUID, runName(in), p.Namespace, "worker"}}
		return nil
	})
	if e != nil {
		return WorkerAccess{}, e
	}
	return out, nil
}
