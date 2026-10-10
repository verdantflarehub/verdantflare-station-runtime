package instances

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type StartRequest struct {
	OperationID       string `json:"operation_id"`
	CreateOperationID string `json:"create_operation_id"`
	StationID         string `json:"station_id"`
	OrganizationID    string `json:"organization_id"`
	UserID            string `json:"user_id"`
	ProjectID         string `json:"project_id"`
	InstanceID        string `json:"instance_id"`
	ProfileID         string `json:"profile_id"`
	SourceRevisionID  string `json:"source_revision_id"`
	SHA256            string `json:"sha256"`
	Size              int64  `json:"size"`
	Empty             bool   `json:"empty"`
}

func (r StartRequest) identity() Identity {
	return Identity{r.StationID, r.OrganizationID, r.ProjectID, r.InstanceID, r.CreateOperationID}
}
func (r StartRequest) source() WorkerSource {
	return WorkerSource{r.SourceRevisionID, r.SHA256, r.Size, r.Empty}
}
func (r StartRequest) valid(p HelperProfile) bool {
	if !validIdentity(r.identity()) || !r.source().valid(p.MaxFileBytes) || r.OperationID == r.CreateOperationID {
		return false
	}
	for _, value := range []string{r.OperationID, r.UserID} {
		id, err := uuid.Parse(value)
		if err != nil || id == uuid.Nil || id.String() != value {
			return false
		}
	}
	return true
}

type startState struct {
	SecretUID string            `json:"secret_uid"`
	Worker    WorkerObservation `json:"worker"`
	Proof     *RunningProof     `json:"proof,omitempty"`
}

type StartProgress struct {
	OperationID string             `json:"operation_id"`
	InstanceID  string             `json:"instance_id"`
	Status      string             `json:"status"`
	Phase       string             `json:"phase"`
	Workspace   *VolumeBinding     `json:"workspace,omitempty"`
	Worker      *WorkerObservation `json:"worker,omitempty"`
	Proof       *RunningProof      `json:"proof,omitempty"`
}

// Start is advanced only by the currently authorized application actor. Each
// replay checks the immutable request and storage profile. A timeout retains
// the original operation and resource names, never triggering a second start.
func (s *Service) Start(ctx context.Context, in StartRequest) (StartProgress, error) {
	out := StartProgress{OperationID: in.OperationID, InstanceID: in.InstanceID}
	p, ok := s.Profiles[in.ProfileID]
	if !ok || !in.valid(p) || in.StationID != s.StationID {
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
	var requestHash, profileHash, stateRaw []byte
	err = tx.QueryRow(ctx, `SELECT request_hash,profile_hash,status,phase,state FROM station.runtime_instance_executions WHERE operation_id=$1`, in.OperationID).Scan(&requestHash, &profileHash, &out.Status, &out.Phase, &stateRaw)
	newOperation := errors.Is(err, pgx.ErrNoRows)
	if err != nil && !newOperation {
		return out, err
	}
	if !newOperation && (!bytes.Equal(requestHash, hash[:]) || !bytes.Equal(profileHash, p.Hash[:])) {
		return out, ErrConflict
	}
	var creationInput, creationState, creationHash []byte
	var creationStatus, creationPhase string
	err = tx.QueryRow(ctx, `SELECT input,state,profile_hash,status,phase FROM station.runtime_instance_executions WHERE operation_id=$1 AND instance_id=$2 AND station_id=$3 AND organization_id=$4 AND project_id=$5 AND action='create'`, in.CreateOperationID, in.InstanceID, in.StationID, in.OrganizationID, in.ProjectID).Scan(&creationInput, &creationState, &creationHash, &creationStatus, &creationPhase)
	if errors.Is(err, pgx.ErrNoRows) {
		return out, ErrBinding
	}
	if err != nil {
		return out, err
	}
	var created CreateRequest
	var prepared createState
	if json.Unmarshal(creationInput, &created) != nil || json.Unmarshal(creationState, &prepared) != nil || prepared.SecretUID == "" || prepared.Proof == nil ||
		created.identity() != in.identity() || created.ProfileID != in.ProfileID || created.SourceRevisionID != in.SourceRevisionID || !bytes.Equal(creationHash, p.Hash[:]) || creationStatus != "succeeded" || creationPhase != "stopped" {
		return out, ErrBinding
	}
	binding, err := scanBinding(tx.QueryRow(ctx, `SELECT `+bindingColumns+` FROM station.runtime_instance_workspaces WHERE instance_id=$1`, in.InstanceID))
	if err != nil {
		return out, err
	}
	if !sameReservation(binding, in.identity(), p.StorageProfile) || binding.Retained {
		return out, ErrBinding
	}
	out.Workspace = &binding
	state := startState{SecretUID: prepared.SecretUID}
	if newOperation {
		var action, status, phase string
		var lastState []byte
		err = tx.QueryRow(ctx, `SELECT action,status,phase,state FROM station.runtime_instance_executions WHERE instance_id=$1 AND action<>'create' ORDER BY created_at DESC,operation_id DESC LIMIT 1`, in.InstanceID).Scan(&action, &status, &phase, &lastState)
		if errors.Is(err, pgx.ErrNoRows) {
			if in.source() != (WorkerSource{created.SourceRevisionID, created.SourceSHA256, created.SourceSize, created.EmptySource}) {
				return out, ErrBinding
			}
		} else if err != nil {
			return out, err
		} else {
			// A restart must load exactly the working copy recorded by the previous
			// completed stop, never silently restore the original Project head.
			var stopped struct {
				Source *WorkerSource `json:"source"`
			}
			if action != "stop" || status != "succeeded" || phase != "stopped" {
				return out, ErrConflict
			}
			if json.Unmarshal(lastState, &stopped) != nil || stopped.Source == nil || *stopped.Source != in.source() {
				return out, ErrBinding
			}
		}
		stateRaw, _ = json.Marshal(state)
		_, err = tx.Exec(ctx, `INSERT INTO station.runtime_instance_executions(operation_id,instance_id,station_id,organization_id,user_id,project_id,action,request_hash,profile_hash,input,status,phase,state) VALUES($1,$2,$3,$4,$5,$6,'start',$7,$8,$9,'running','accepted',$10)`, in.OperationID, in.InstanceID, in.StationID, in.OrganizationID, in.UserID, in.ProjectID, hash[:], p.Hash[:], raw, stateRaw)
		if err != nil {
			return out, err
		}
		out.Status, out.Phase = "running", "accepted"
		return out, tx.Commit(ctx)
	}
	if json.Unmarshal(stateRaw, &state) != nil || state.SecretUID != prepared.SecretUID {
		return out, ErrBinding
	}
	if state.Worker.PodUID != "" {
		out.Worker = &state.Worker
	}
	out.Proof = state.Proof
	if out.Status != "running" {
		return out, tx.Commit(ctx)
	}
	if out.Phase != "accepted" && out.Phase != "launching" && out.Phase != "loading" {
		return out, ErrBinding
	}
	// Serialize capacity observations and new Pod requests across instances on
	// this node. The Kubernetes scheduler still owns the actual allocation.
	if err = tx.QueryRow(ctx, `SELECT pg_try_advisory_xact_lock(hashtextextended($1,0))`, "runtime-instance-node:"+in.StationID+":"+p.NodeName).Scan(&locked); err != nil {
		return out, err
	}
	if !locked {
		return out, ErrBusy
	}
	state.Worker, err = EnsureWorker(ctx, s.Client, in.identity(), p, binding, in.OperationID, state.SecretUID, in.source(), state.Worker)
	if err != nil {
		code := "OPERATION_OUTCOME_UNKNOWN"
		switch {
		case errors.Is(err, ErrComputeCapacity):
			code = "COMPUTE_CAPACITY_UNAVAILABLE"
		case errors.Is(err, ErrComputeUnknown):
			code = "COMPUTE_CAPACITY_UNKNOWN"
		case errors.Is(err, ErrBinding):
			code = "WORKSPACE_BINDING_CONFLICT"
		case errors.Is(err, ErrBusy):
			code = "WORKSPACE_IN_USE"
		case errors.Is(err, ErrWorkerExited):
			code = "WORKER_EXITED"
		}
		return StartProgress{}, &ExecutionError{Code: code}
	}
	out.Worker = &state.Worker
	out.Phase = "launching"
	if state.Worker.PodReady {
		out.Phase = "loading"
		secret, e := credential(ctx, s.Client, in.identity(), p.Namespace, state.SecretUID, false)
		if e != nil {
			return StartProgress{}, e
		}
		if s.WorkerObserver == nil {
			return StartProgress{}, ErrBinding
		}
		state.Proof, err = s.WorkerObserver.ObserveWorker(ctx, in.identity(), in.OperationID, in.source(), state.Worker, string(secret.Data["worker-token"]))
		if err != nil {
			return StartProgress{}, &ExecutionError{Code: "WORKER_LOAD_UNVERIFIED"}
		}
		if state.Proof != nil {
			out.Status, out.Phase = "succeeded", "running"
			out.Proof = state.Proof
		}
	}
	stateRaw, _ = json.Marshal(state)
	_, err = tx.Exec(ctx, `UPDATE station.runtime_instance_executions SET status=$2,phase=$3,state=$4,error_code='',updated_at=now() WHERE operation_id=$1`, in.OperationID, out.Status, out.Phase, stateRaw)
	if err != nil {
		return StartProgress{}, err
	}
	return out, tx.Commit(ctx)
}
