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
	"github.com/jackc/pgx/v5/pgxpool"
	"k8s.io/client-go/kubernetes"
)

var ErrConflict = errors.New("OPERATION_CONFLICT")

type CreateRequest struct {
	OperationID      string `json:"operation_id"`
	StationID        string `json:"station_id"`
	OrganizationID   string `json:"organization_id"`
	UserID           string `json:"user_id"`
	ProjectID        string `json:"project_id"`
	InstanceID       string `json:"instance_id"`
	ProfileID        string `json:"profile_id"`
	SourceRevisionID string `json:"source_revision_id"`
	SourceSHA256     string `json:"source_sha256"`
	SourceSize       int64  `json:"source_size"`
	EmptySource      bool   `json:"empty_source"`
}

func (r CreateRequest) identity() Identity {
	return Identity{r.StationID, r.OrganizationID, r.ProjectID, r.InstanceID, r.OperationID}
}
func (r CreateRequest) valid(p HelperProfile) bool {
	if !validIdentity(r.identity()) {
		return false
	}
	for _, v := range []string{r.UserID, r.SourceRevisionID} {
		id, err := uuid.Parse(v)
		if err != nil || id == uuid.Nil || id.String() != v {
			return false
		}
	}
	if r.EmptySource {
		return r.SourceSHA256 == "" && r.SourceSize == 0
	}
	return regexp.MustCompile(`^[0-9a-f]{64}$`).MatchString(r.SourceSHA256) && r.SourceSize > 0 && r.SourceSize <= p.MaxFileBytes
}

type PreparationProof struct {
	PrepareID        string  `json:"prepare_id"`
	InstanceID       string  `json:"instance_id"`
	ProjectID        string  `json:"project_id"`
	SourceRevisionID string  `json:"source_revision_id"`
	AssetID          *string `json:"asset_id"`
	SHA256           *string `json:"sha256"`
	Size             int64   `json:"size"`
	State            string  `json:"state"`
	PreparedAt       string  `json:"prepared_at"`
}

type createState struct {
	SecretUID string            `json:"secret_uid,omitempty"`
	Helper    HelperObservation `json:"helper"`
	Proof     *PreparationProof `json:"proof,omitempty"`
}

// Access exists only in the authorized service response, never in persisted state.
type PreparationAccess struct {
	FileEndpoint string `json:"file_endpoint"`
	WorkerToken  string `json:"worker_token"`
	PodUID       string `json:"pod_uid"`
}

type CreateProgress struct {
	OperationID string             `json:"operation_id"`
	InstanceID  string             `json:"instance_id"`
	Status      string             `json:"status"`
	Phase       string             `json:"phase"`
	Code        string             `json:"code,omitempty"`
	Workspace   *VolumeBinding     `json:"workspace,omitempty"`
	Access      *PreparationAccess `json:"access,omitempty"`
}

type PreparationObserver interface {
	Observe(context.Context, CreateRequest, HelperObservation, string) (*PreparationProof, error)
}

type Service struct {
	Pool           *pgxpool.Pool
	Client         kubernetes.Interface
	StationID      string
	Profiles       map[string]HelperProfile
	Observer       PreparationObserver
	WorkerObserver WorkerObserver
	DrainObserver  DrainObserver
}

// Create advances one durable stage per authorized call. The Blender coordinator
// repeats the original request after restart; Runtime has no unbounded bearer cache.
func (s *Service) Create(ctx context.Context, in CreateRequest) (CreateProgress, error) {
	out := CreateProgress{OperationID: in.OperationID, InstanceID: in.InstanceID}
	p, exists := s.Profiles[in.ProfileID]
	if !exists || !in.valid(p) || in.StationID != s.StationID {
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
	var priorHash, profileHash, stateRaw []byte
	err = tx.QueryRow(ctx, `SELECT request_hash,profile_hash,status,phase,state,error_code FROM station.runtime_instance_executions WHERE operation_id=$1`, in.OperationID).Scan(&priorHash, &profileHash, &out.Status, &out.Phase, &stateRaw, &out.Code)
	if errors.Is(err, pgx.ErrNoRows) {
		var occupied bool
		if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM station.runtime_instance_executions WHERE instance_id=$1 AND (status='running' OR action='create'))`, in.InstanceID).Scan(&occupied); err != nil {
			return out, err
		}
		if occupied {
			return out, ErrConflict
		}
		_, err = tx.Exec(ctx, `INSERT INTO station.runtime_instance_executions(operation_id,instance_id,station_id,organization_id,user_id,project_id,action,request_hash,profile_hash,input,status,phase) VALUES($1,$2,$3,$4,$5,$6,'create',$7,$8,$9,'running','accepted')`, in.OperationID, in.InstanceID, in.StationID, in.OrganizationID, in.UserID, in.ProjectID, hash[:], p.Hash[:], raw)
		if err != nil {
			return out, err
		}
		out.Status, out.Phase = "running", "accepted"
		return out, tx.Commit(ctx) // No Kubernetes effects before durable acceptance.
	}
	if err != nil {
		return out, err
	}
	if !bytes.Equal(priorHash, hash[:]) || !bytes.Equal(profileHash, p.Hash[:]) {
		return CreateProgress{}, ErrConflict
	}
	var state createState
	if json.Unmarshal(stateRaw, &state) != nil {
		return out, ErrBinding
	}
	store := WorkspaceStore{s.Pool, s.Client}
	var binding VolumeBinding
	if out.Phase != "accepted" {
		binding, err = scanBinding(tx.QueryRow(ctx, `SELECT `+bindingColumns+` FROM station.runtime_instance_workspaces WHERE instance_id=$1`, in.InstanceID))
		if err != nil {
			return out, err
		}
		if !sameReservation(binding, in.identity(), p.StorageProfile) || binding.Retained {
			return out, ErrBinding
		}
		out.Workspace = &binding
	}
	if out.Status != "running" {
		return out, tx.Commit(ctx)
	}
	out.Code = ""
	switch out.Phase {
	case "accepted":
		binding, err = store.reserve(ctx, tx, in.identity(), p.StorageProfile)
		if err == nil {
			out.Workspace = &binding
			out.Phase = "reserved"
		}
	case "reserved":
		err = Claim(ctx, s.Client, in.identity(), p.StorageProfile, binding)
		if err == nil {
			secret, e := credential(ctx, s.Client, in.identity(), p.Namespace, state.SecretUID, true)
			err = e
			if err == nil {
				state.SecretUID = string(secret.UID)
				out.Phase = "claimed"
			}
		}
	case "claimed", "preparing", "awaiting_content":
		var token string
		secret, e := credential(ctx, s.Client, in.identity(), p.Namespace, state.SecretUID, false)
		err = e
		if err == nil {
			token = string(secret.Data["worker-token"])
			state.Helper, err = EnsureHelper(ctx, s.Client, in.identity(), p, binding, state.Helper)
		}
		if err == nil {
			out.Phase = "preparing"
			if state.Helper.Ready {
				out.Phase = "awaiting_content"
				if s.Observer == nil {
					err = ErrBinding
				} else {
					state.Proof, err = s.Observer.Observe(ctx, in, state.Helper, token)
				}
				if err == nil && state.Proof != nil {
					out.Phase = "removing"
				} else if err == nil {
					out.Access = &PreparationAccess{state.Helper.FileEndpoint, token, state.Helper.PodUID}
				}
			}
		}
	case "removing":
		if state.Proof == nil {
			err = ErrBinding
			break
		}
		var removed bool
		removed, err = RemoveHelper(ctx, s.Client, in.identity(), p, binding, state.Helper)
		if err == nil && removed {
			out.Status, out.Phase = "succeeded", "stopped"
		}
	default:
		return out, ErrBinding
	}
	if err != nil {
		// Keep the previously persisted resource IDs on uncertainty. In particular an
		// unsuccessful EnsureHelper must not replace the journal with empty UIDs.
		if errors.Is(err, ErrCapacity) {
			out.Code = "STORAGE_CAPACITY_UNAVAILABLE"
		} else if errors.Is(err, ErrBinding) {
			out.Code = "WORKSPACE_BINDING_CONFLICT"
		} else if errors.Is(err, ErrBusy) {
			out.Code = "WORKSPACE_IN_USE"
		} else {
			out.Code = "OPERATION_OUTCOME_UNKNOWN"
		}
		// Roll back any partial SQL work; side effects are discovered using the same
		// immutable identity on the next authorized call.
		return CreateProgress{}, &ExecutionError{Code: out.Code}
	}
	stateRaw, err = json.Marshal(state)
	if err != nil {
		return out, err
	}
	_, err = tx.Exec(ctx, `UPDATE station.runtime_instance_executions SET status=$2,phase=$3,state=$4,error_code='',updated_at=now() WHERE operation_id=$1`, in.OperationID, out.Status, out.Phase, stateRaw)
	if err != nil {
		return CreateProgress{}, err
	}
	return out, tx.Commit(ctx)
}

type ExecutionError struct{ Code string }

func (e *ExecutionError) Error() string { return e.Code }
