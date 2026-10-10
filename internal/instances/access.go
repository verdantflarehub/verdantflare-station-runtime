package instances

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

type AccessRequest struct {
	StationID        string `json:"station_id"`
	OrganizationID   string `json:"organization_id"`
	UserID           string `json:"user_id"`
	ProjectID        string `json:"project_id"`
	InstanceID       string `json:"instance_id"`
	StartOperationID string `json:"start_operation_id"`
	Kind             string `json:"kind"`
}

type WorkerAccess struct {
	InstanceID       string            `json:"instance_id"`
	StartOperationID string            `json:"start_operation_id"`
	Kind             string            `json:"kind"`
	Worker           WorkerObservation `json:"worker"`
	Credential       string            `json:"credential"`
}

// Access only reads the current, successfully started execution. The trusted
// caller has checked current membership and the application instance/Project
// ACL. No credential is cached in SQL or repaired by creating missing objects.
func (s *Service) Access(ctx context.Context, in AccessRequest) (WorkerAccess, error) {
	out := WorkerAccess{}
	for _, value := range []string{in.StationID, in.OrganizationID, in.UserID, in.ProjectID, in.InstanceID, in.StartOperationID} {
		id, err := uuid.Parse(value)
		if err != nil || id == uuid.Nil || id.String() != value {
			return out, ErrInvalid
		}
	}
	if in.StationID != s.StationID || (in.Kind != "worker" && in.Kind != "gui") {
		return out, ErrInvalid
	}
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
	var operation, action, status, phase string
	var raw, stateRaw, profileHash []byte
	err = tx.QueryRow(ctx, `SELECT operation_id::text,action,status,phase,input,state,profile_hash FROM station.runtime_instance_executions WHERE instance_id=$1 ORDER BY created_at DESC,operation_id DESC LIMIT 1`, in.InstanceID).Scan(&operation, &action, &status, &phase, &raw, &stateRaw, &profileHash)
	if errors.Is(err, pgx.ErrNoRows) {
		return out, ErrBinding
	}
	if err != nil {
		return out, err
	}
	if operation != in.StartOperationID || action != "start" || status != "succeeded" || phase != "running" {
		return out, ErrConflict
	}
	var request StartRequest
	var state startState
	if json.Unmarshal(raw, &request) != nil || json.Unmarshal(stateRaw, &state) != nil || request.OperationID != in.StartOperationID ||
		request.InstanceID != in.InstanceID || request.ProjectID != in.ProjectID || request.OrganizationID != in.OrganizationID || request.StationID != in.StationID ||
		state.SecretUID == "" || state.Proof == nil || state.Worker.PodUID == "" || state.Worker.ServiceUID == "" {
		return out, ErrBinding
	}
	p, ok := s.Profiles[request.ProfileID]
	if !ok || !bytes.Equal(profileHash, p.Hash[:]) {
		return out, ErrBinding
	}
	b, err := scanBinding(tx.QueryRow(ctx, `SELECT `+bindingColumns+` FROM station.runtime_instance_workspaces WHERE instance_id=$1`, in.InstanceID))
	if err != nil {
		return out, err
	}
	if !sameReservation(b, request.identity(), p.StorageProfile) || b.Retained {
		return out, ErrBinding
	}
	desired, desiredService, err := workerObjects(request.identity(), p, b, request.OperationID, request.source())
	if err != nil {
		return out, err
	}
	pod, err := s.Client.CoreV1().Pods(p.Namespace).Get(ctx, desired.Name, metav1.GetOptions{})
	if err != nil {
		return out, err
	}
	if !workerPodMatches(pod, desired, state.Worker.PodUID) || pod.DeletionTimestamp != nil || pod.Spec.NodeName != p.NodeName || pod.Status.Phase != corev1.PodRunning {
		return out, ErrBinding
	}
	ready := false
	for _, c := range pod.Status.Conditions {
		if c.Type == corev1.PodReady && c.Status == corev1.ConditionTrue {
			ready = true
		}
	}
	if !ready {
		return out, ErrBinding
	}
	svc, err := s.Client.CoreV1().Services(p.Namespace).Get(ctx, desiredService.Name, metav1.GetOptions{})
	if err != nil {
		return out, err
	}
	if !helperServiceMatches(svc, desiredService, state.Worker.ServiceUID) || svc.DeletionTimestamp != nil || len(svc.Spec.Ports) != 3 {
		return out, ErrBinding
	}
	if _, err = Inspect(ctx, s.Client, request.identity(), p.StorageProfile, b.PVCName, &b, map[types.UID]bool{pod.UID: true}); err != nil {
		return out, err
	}
	claim, err := s.Client.CoreV1().PersistentVolumeClaims(p.Namespace).Get(ctx, b.PVCName, metav1.GetOptions{})
	if err != nil {
		return out, err
	}
	for k, v := range volumeOwner(request.identity()) {
		if claim.Annotations[k] != v {
			return out, ErrBinding
		}
	}
	secret, err := credential(ctx, s.Client, request.identity(), p.Namespace, state.SecretUID, false)
	if err != nil {
		return out, err
	}
	if err = tx.Commit(ctx); err != nil {
		return out, err
	}
	key := "worker-token"
	if in.Kind == "gui" {
		key = "gui-password"
	}
	return WorkerAccess{in.InstanceID, in.StartOperationID, in.Kind, state.Worker, string(secret.Data[key])}, nil
}
