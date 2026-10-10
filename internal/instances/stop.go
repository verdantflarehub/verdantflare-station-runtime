package instances

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"regexp"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
)

type StopRequest struct {
	OperationID      string `json:"operation_id"`
	StartOperationID string `json:"start_operation_id"`
	StationID        string `json:"station_id"`
	OrganizationID   string `json:"organization_id"`
	UserID           string `json:"user_id"`
	ProjectID        string `json:"project_id"`
	InstanceID       string `json:"instance_id"`
	Generation       string `json:"generation"`
	SavedRevisionID  string `json:"saved_revision_id"`
	CommitID         string `json:"commit_id"`
	StoreID          string `json:"store_id"`
	ArtifactID       string `json:"artifact_id"`
	VersionID        string `json:"version_id"`
	AssetID          string `json:"asset_id"`
	SHA256           string `json:"sha256"`
	Size             int64  `json:"size"`
	SceneVersion     int64  `json:"scene_version"`
}

type FrozenProof struct {
	AssetID      string `json:"asset_id"`
	SHA256       string `json:"sha256"`
	Size         int64  `json:"size"`
	SceneVersion int64  `json:"scene_version"`
}

func (r StopRequest) proof() FrozenProof {
	return FrozenProof{r.AssetID, r.SHA256, r.Size, r.SceneVersion}
}
func (r StopRequest) valid() bool {
	for _, value := range []string{r.OperationID, r.StartOperationID, r.StationID, r.OrganizationID, r.UserID, r.ProjectID, r.InstanceID, r.SavedRevisionID, r.CommitID, r.StoreID, r.ArtifactID, r.VersionID} {
		id, err := uuid.Parse(value)
		if err != nil || id == uuid.Nil || id.String() != value {
			return false
		}
	}
	return r.OperationID != r.StartOperationID && regexp.MustCompile(`^[0-9a-f]{32}$`).MatchString(r.Generation) &&
		regexp.MustCompile(`^checkpoints/checkpoint-[0-9]+-[0-9a-f]{32}\.blend$`).MatchString(r.AssetID) &&
		regexp.MustCompile(`^[0-9a-f]{64}$`).MatchString(r.SHA256) && r.Size > 0 && r.SceneVersion >= 0
}

type stopState struct {
	Start     StartRequest `json:"start"`
	Execution startState   `json:"execution"`
	Source    WorkerSource `json:"source"`
	Sealed    bool         `json:"sealed"`
}

type StopProgress struct {
	OperationID     string         `json:"operation_id"`
	InstanceID      string         `json:"instance_id"`
	Status          string         `json:"status"`
	Phase           string         `json:"phase"`
	Workspace       *VolumeBinding `json:"workspace,omitempty"`
	Source          WorkerSource   `json:"source"`
	SavedRevisionID string         `json:"saved_revision_id"`
}

type DrainObserver interface {
	Seal(context.Context, StopRequest, WorkerObservation, string) error
}

// Stop accepts only the application's completed formal Project save. It then
// independently seals authenticated frozen-worker evidence before any deletion.
func (s *Service) Stop(ctx context.Context, in StopRequest) (StopProgress, error) {
	out := StopProgress{OperationID: in.OperationID, InstanceID: in.InstanceID, SavedRevisionID: in.SavedRevisionID}
	if !in.valid() || in.StationID != s.StationID {
		return out, ErrInvalid
	}
	raw, _ := json.Marshal(in)
	hash := sha256.Sum256(raw)
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return out, err
	}
	defer tx.Rollback(context.Background())
	var locked bool
	if err = tx.QueryRow(ctx, `SELECT pg_try_advisory_xact_lock(hashtextextended($1,0))`, "runtime-instance:"+in.StationID+":"+in.InstanceID).Scan(&locked); err != nil {
		return out, err
	}
	if !locked {
		return out, ErrBusy
	}
	var savedHash, profileHash, stateRaw []byte
	err = tx.QueryRow(ctx, `SELECT request_hash,profile_hash,status,phase,state FROM station.runtime_instance_executions WHERE operation_id=$1`, in.OperationID).Scan(&savedHash, &profileHash, &out.Status, &out.Phase, &stateRaw)
	fresh := errors.Is(err, pgx.ErrNoRows)
	if err != nil && !fresh {
		return out, err
	}
	state := stopState{}
	if fresh {
		var operation, action, status, phase string
		var input, execution []byte
		err = tx.QueryRow(ctx, `SELECT operation_id::text,action,status,phase,input,state,profile_hash FROM station.runtime_instance_executions WHERE instance_id=$1 ORDER BY created_at DESC,operation_id DESC LIMIT 1`, in.InstanceID).Scan(&operation, &action, &status, &phase, &input, &execution, &profileHash)
		if errors.Is(err, pgx.ErrNoRows) {
			return out, ErrBinding
		}
		if err != nil {
			return out, err
		}
		if operation != in.StartOperationID || action != "start" || status != "succeeded" || phase != "running" {
			return out, ErrConflict
		}
		if json.Unmarshal(input, &state.Start) != nil || json.Unmarshal(execution, &state.Execution) != nil {
			return out, ErrBinding
		}
		state.Source = WorkerSource{state.Start.SourceRevisionID, in.SHA256, in.Size, false}
	} else {
		if !bytes.Equal(savedHash, hash[:]) {
			return out, ErrConflict
		}
		if json.Unmarshal(stateRaw, &state) != nil {
			return out, ErrBinding
		}
	}
	start := state.Start
	p, ok := s.Profiles[start.ProfileID]
	if !ok || !bytes.Equal(profileHash, p.Hash[:]) || in.Size > p.MaxFileBytes || start.OperationID != in.StartOperationID ||
		start.StationID != in.StationID || start.OrganizationID != in.OrganizationID || start.ProjectID != in.ProjectID || start.InstanceID != in.InstanceID ||
		state.Execution.Proof == nil || state.Execution.Proof.Generation != in.Generation || state.Execution.SecretUID == "" ||
		state.Execution.Worker.PodUID == "" || state.Execution.Worker.ServiceUID == "" {
		return out, ErrBinding
	}
	binding, err := scanBinding(tx.QueryRow(ctx, `SELECT `+bindingColumns+` FROM station.runtime_instance_workspaces WHERE instance_id=$1`, in.InstanceID))
	if err != nil {
		return out, err
	}
	if !sameReservation(binding, start.identity(), p.StorageProfile) || binding.Retained {
		return out, ErrBinding
	}
	out.Workspace = &binding
	out.Source = state.Source
	if fresh {
		stateRaw, _ = json.Marshal(state)
		_, err = tx.Exec(ctx, `INSERT INTO station.runtime_instance_executions(operation_id,instance_id,station_id,organization_id,user_id,project_id,action,request_hash,profile_hash,input,status,phase,state) VALUES($1,$2,$3,$4,$5,$6,'stop',$7,$8,$9,'running','accepted',$10)`, in.OperationID, in.InstanceID, in.StationID, in.OrganizationID, in.UserID, in.ProjectID, hash[:], p.Hash[:], raw, stateRaw)
		if err != nil {
			return out, err
		}
		out.Status, out.Phase = "running", "accepted"
		return out, tx.Commit(ctx)
	}
	if out.Status != "running" {
		return out, tx.Commit(ctx)
	}
	if out.Phase == "accepted" {
		desired, svc, err := workerObjects(start.identity(), p, binding, start.OperationID, start.source())
		if err != nil {
			return out, err
		}
		pod, err := s.Client.CoreV1().Pods(p.Namespace).Get(ctx, desired.Name, metav1.GetOptions{})
		if err != nil {
			return out, err
		}
		service, err := s.Client.CoreV1().Services(p.Namespace).Get(ctx, svc.Name, metav1.GetOptions{})
		if err != nil {
			return out, err
		}
		if !workerPodMatches(pod, desired, state.Execution.Worker.PodUID) || pod.DeletionTimestamp != nil || pod.Spec.NodeName != p.NodeName ||
			!helperServiceMatches(service, svc, state.Execution.Worker.ServiceUID) || service.DeletionTimestamp != nil {
			return out, ErrBinding
		}
		if _, err = Inspect(ctx, s.Client, start.identity(), p.StorageProfile, binding.PVCName, &binding, map[types.UID]bool{pod.UID: true}); err != nil {
			return out, err
		}
		secret, err := credential(ctx, s.Client, start.identity(), p.Namespace, state.Execution.SecretUID, false)
		if err != nil {
			return out, err
		}
		if s.DrainObserver == nil {
			return out, ErrBinding
		}
		if err = s.DrainObserver.Seal(ctx, in, state.Execution.Worker, string(secret.Data["worker-token"])); err != nil {
			return out, &ExecutionError{Code: "WORKER_DRAIN_UNVERIFIED"}
		}
		state.Sealed = true
		out.Phase = "removing"
	} else if out.Phase == "removing" && state.Sealed {
		gone, err := removeWorker(ctx, s.Client, start, p, binding, state.Execution.Worker)
		if err != nil {
			return out, err
		}
		if gone {
			out.Status, out.Phase = "succeeded", "stopped"
		}
	} else {
		return out, ErrBinding
	}
	stateRaw, _ = json.Marshal(state)
	_, err = tx.Exec(ctx, `UPDATE station.runtime_instance_executions SET status=$2,phase=$3,state=$4,updated_at=now() WHERE operation_id=$1`, in.OperationID, out.Status, out.Phase, stateRaw)
	if err != nil {
		return out, err
	}
	return out, tx.Commit(ctx)
}

// Called only after the stop ledger has committed sealed process evidence.
// NotFound after a lost deletion response is reconciled; replaced UIDs are not.
func removeWorker(ctx context.Context, client kubernetes.Interface, start StartRequest, p HelperProfile, b VolumeBinding, expected WorkerObservation) (bool, error) {
	desired, svc, err := workerObjects(start.identity(), p, b, start.OperationID, start.source())
	if err != nil {
		return false, err
	}
	pod, err := client.CoreV1().Pods(p.Namespace).Get(ctx, desired.Name, metav1.GetOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return false, err
	}
	if apierrors.IsNotFound(err) {
		pod = nil
	} else if !workerPodMatches(pod, desired, expected.PodUID) {
		return false, ErrBinding
	}
	service, err := client.CoreV1().Services(p.Namespace).Get(ctx, svc.Name, metav1.GetOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return false, err
	}
	if apierrors.IsNotFound(err) {
		service = nil
	} else if !helperServiceMatches(service, svc, expected.ServiceUID) {
		return false, ErrBinding
	}
	allowed := map[types.UID]bool{}
	if pod != nil {
		allowed[pod.UID] = true
	}
	if _, err = Inspect(ctx, client, start.identity(), p.StorageProfile, b.PVCName, &b, allowed); err != nil {
		return false, err
	}
	if service != nil {
		uid, rv := service.UID, service.ResourceVersion
		if err = client.CoreV1().Services(p.Namespace).Delete(ctx, svc.Name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid, ResourceVersion: &rv}}); err != nil && !apierrors.IsNotFound(err) {
			return false, err
		}
	}
	if pod != nil {
		uid, rv := pod.UID, pod.ResourceVersion
		if err = client.CoreV1().Pods(p.Namespace).Delete(ctx, desired.Name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid, ResourceVersion: &rv}}); err != nil && !apierrors.IsNotFound(err) {
			return false, err
		}
	}
	if pod != nil || service != nil {
		return false, nil
	}
	node, err := client.CoreV1().Nodes().Get(ctx, p.NodeName, metav1.GetOptions{})
	if err != nil {
		return false, err
	}
	for _, condition := range node.Status.Conditions {
		if condition.Type == corev1.NodeReady && condition.Status == corev1.ConditionTrue {
			return true, nil
		}
	}
	return false, ErrComputeUnknown
}
