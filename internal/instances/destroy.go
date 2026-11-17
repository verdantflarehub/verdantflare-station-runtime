package instances

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

type DestroyRequest struct {
	OperationID         string `json:"operation_id"`
	PreviousOperationID string `json:"previous_operation_id"`
	StationID           string `json:"station_id"`
	OrganizationID      string `json:"organization_id"`
	UserID              string `json:"user_id"`
	ProjectID           string `json:"project_id"`
	InstanceID          string `json:"instance_id"`
}
type destroyState struct {
	OrphanStart bool          `json:"orphan_start,omitempty"`
	Create      CreateRequest `json:"create"`
	SecretUID   string        `json:"secret_uid"`
}
type DestroyProgress struct {
	OperationID string         `json:"operation_id"`
	InstanceID  string         `json:"instance_id"`
	Status      string         `json:"status"`
	Phase       string         `json:"phase"`
	Workspace   *VolumeBinding `json:"workspace,omitempty"`
}

func orphanStartResourcesAbsent(ctx context.Context, client kubernetes.Interface, namespace, instance string) error {
	pods, err := client.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}
	for _, pod := range pods.Items {
		if pod.Annotations[instanceKey] == instance {
			return ErrBusy
		}
	}
	services, err := client.CoreV1().Services(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}
	for _, svc := range services.Items {
		if svc.Annotations[instanceKey] == instance {
			return ErrBusy
		}
	}
	return nil
}

// Validate the complete set before deleting anything. Names alone are never ownership.
func reapOrphanStartResources(ctx context.Context, client kubernetes.Interface, binding VolumeBinding) error {
	ns := binding.Namespace
	pods, err := client.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}
	services, err := client.CoreV1().Services(ns).List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}
	owned := func(m metav1.ObjectMeta) bool {
		return strings.HasPrefix(m.Name, "blender-run-") && m.UID != "" && m.ResourceVersion != "" && m.DeletionTimestamp == nil && m.Annotations[instanceKey] == binding.InstanceID && m.Annotations[stationKey] == binding.StationID && m.Annotations[organizationKey] == binding.OrganizationID
	}
	var deadPods []corev1.Pod
	var deadServices []corev1.Service
	for _, pod := range pods.Items {
		relevant := pod.Annotations[instanceKey] == binding.InstanceID
		for _, v := range pod.Spec.Volumes {
			if v.PersistentVolumeClaim != nil && v.PersistentVolumeClaim.ClaimName == binding.PVCName {
				relevant = true
			}
		}
		if !relevant {
			continue
		}
		if !owned(pod.ObjectMeta) || (pod.Status.Phase != corev1.PodSucceeded && pod.Status.Phase != corev1.PodFailed) {
			return ErrBusy
		}
		deadPods = append(deadPods, pod)
	}
	for _, svc := range services.Items {
		if svc.Annotations[instanceKey] == binding.InstanceID {
			if !owned(svc.ObjectMeta) {
				return ErrBusy
			}
			deadServices = append(deadServices, svc)
		}
	}
	for _, pod := range deadPods {
		uid, rv := pod.UID, pod.ResourceVersion
		if err := client.CoreV1().Pods(ns).Delete(ctx, pod.Name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid, ResourceVersion: &rv}}); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	}
	for _, svc := range deadServices {
		uid, rv := svc.UID, svc.ResourceVersion
		if err := client.CoreV1().Services(ns).Delete(ctx, svc.Name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid, ResourceVersion: &rv}}); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	}
	return orphanStartResourcesAbsent(ctx, client, ns, binding.InstanceID)
}

// Destroy revokes access only after a completed stop (or prepared, never-started
// workspace). It deliberately retains the reservation, PVC, PV, and their bytes.
func (s *Service) Destroy(ctx context.Context, in DestroyRequest) (DestroyProgress, error) {
	out := DestroyProgress{OperationID: in.OperationID, InstanceID: in.InstanceID}
	for _, v := range []string{in.OperationID, in.PreviousOperationID, in.StationID, in.OrganizationID, in.UserID, in.ProjectID, in.InstanceID} {
		id, err := uuid.Parse(v)
		if err != nil || id == uuid.Nil || id.String() != v {
			return out, ErrInvalid
		}
	}
	if in.StationID != s.StationID || in.OperationID == in.PreviousOperationID {
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
	err = tx.QueryRow(ctx, `SELECT request_hash,profile_hash,status,phase,state FROM station.runtime_instance_executions WHERE operation_id=$1`, in.OperationID).Scan(&priorHash, &profileHash, &out.Status, &out.Phase, &stateRaw)
	fresh := errors.Is(err, pgx.ErrNoRows)
	if err != nil && !fresh {
		return out, err
	}
	state := destroyState{}
	orphanStart := false
	if fresh {
		var op, action, status, phase string
		var stale bool
		if err = tx.QueryRow(ctx, `SELECT operation_id::text,action,status,phase,created_at < now()-interval '10 minutes' FROM station.runtime_instance_executions WHERE instance_id=$1 ORDER BY created_at DESC,operation_id DESC LIMIT 1`, in.InstanceID).Scan(&op, &action, &status, &phase, &stale); err != nil {
			return out, err
		}
		orphanStart = action == "start" && status == "running" && stale
		state.OrphanStart = orphanStart
		if op != in.PreviousOperationID || (!orphanStart && ((action != "stop" && action != "create") || status != "succeeded" || phase != "stopped")) {
			return out, ErrConflict
		}
		var input, execution []byte
		var created createState
		if err = tx.QueryRow(ctx, `SELECT input,state,profile_hash FROM station.runtime_instance_executions WHERE instance_id=$1 AND action='create' AND status='succeeded' AND phase='stopped'`, in.InstanceID).Scan(&input, &execution, &profileHash); err != nil {
			return out, err
		}
		if json.Unmarshal(input, &state.Create) != nil || json.Unmarshal(execution, &created) != nil {
			return out, ErrBinding
		}
		state.SecretUID = created.SecretUID
	} else {
		if !bytes.Equal(priorHash, hash[:]) {
			return out, ErrConflict
		}
		if json.Unmarshal(stateRaw, &state) != nil {
			return out, ErrBinding
		}
	}
	create := state.Create
	p, ok := s.pinnedProfile(create.ProfileID, profileHash)
	if !ok || !bytes.Equal(profileHash, p.Hash[:]) || state.SecretUID == "" || create.StationID != in.StationID || create.OrganizationID != in.OrganizationID || create.ProjectID != in.ProjectID || create.InstanceID != in.InstanceID {
		return out, ErrBinding
	}
	binding, err := scanBinding(tx.QueryRow(ctx, `SELECT `+bindingColumns+` FROM station.runtime_instance_workspaces WHERE instance_id=$1`, in.InstanceID))
	if err != nil {
		return out, err
	}
	if !sameReservation(binding, create.identity(), p.StorageProfile) {
		return out, ErrBinding
	}
	out.Workspace = &binding
	if fresh {
		if binding.Retained {
			return out, ErrRetained
		}
		if orphanStart {
			// Same instance advisory lock as Start: remove only proven terminal leftovers.
			if err = reapOrphanStartResources(ctx, s.Client, binding); err != nil {
				return out, err
			}
			// Inspect rejects every Pod mounting this workspace, including terminating Pods.
			if _, err = Inspect(ctx, s.Client, create.identity(), p.StorageProfile, binding.PVCName, &binding, nil); err != nil {
				return out, err
			}
			if err = orphanStartResourcesAbsent(ctx, s.Client, p.Namespace, in.InstanceID); err != nil {
				return out, err
			}
			// Retain workspace bytes; never fabricate a saved scene or successful stop.
			if _, err = tx.Exec(ctx, `UPDATE station.runtime_instance_executions SET status='failed',error_code='ORPHAN_START_FENCED',updated_at=now() WHERE operation_id=$1 AND status='running'`, in.PreviousOperationID); err != nil {
				return out, err
			}
		}
		stateRaw, _ = json.Marshal(state)
		_, err = tx.Exec(ctx, `INSERT INTO station.runtime_instance_executions(operation_id,instance_id,station_id,organization_id,user_id,project_id,action,request_hash,profile_hash,input,status,phase,state) VALUES($1,$2,$3,$4,$5,$6,'destroy',$7,$8,$9,'running','accepted',$10)`, in.OperationID, in.InstanceID, in.StationID, in.OrganizationID, in.UserID, in.ProjectID, hash[:], p.Hash[:], raw, stateRaw)
		if err != nil {
			return out, err
		}
		out.Status, out.Phase = "running", "accepted"
		return out, tx.Commit(ctx)
	}
	if out.Status != "running" {
		return out, tx.Commit(ctx)
	}
	if binding.Retained {
		return out, ErrBinding
	}
	if out.Phase == "accepted" || out.Phase == "revoking" {
		if !state.OrphanStart {
			if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM station.runtime_instance_executions WHERE operation_id=$1 AND instance_id=$2 AND error_code='ORPHAN_START_FENCED')`, in.PreviousOperationID, in.InstanceID).Scan(&state.OrphanStart); err != nil {
				return out, err
			}
		}
		if state.OrphanStart {
			if err = reapOrphanStartResources(ctx, s.Client, binding); err != nil {
				return out, err
			}
		}
	}
	if _, err = Inspect(ctx, s.Client, create.identity(), p.StorageProfile, binding.PVCName, &binding, nil); err != nil {
		return out, err
	}
	services, err := s.Client.CoreV1().Services(p.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return out, err
	}
	for _, svc := range services.Items {
		if svc.Annotations[instanceKey] == in.InstanceID {
			return out, ErrBusy
		}
	}
	node, err := s.Client.CoreV1().Nodes().Get(ctx, p.NodeName, metav1.GetOptions{})
	if err != nil {
		return out, err
	}
	ready := false
	for _, c := range node.Status.Conditions {
		if c.Type == corev1.NodeReady && c.Status == corev1.ConditionTrue {
			ready = true
		}
	}
	if !ready {
		return out, ErrComputeUnknown
	}
	switch out.Phase {
	case "accepted":
		if _, err = credential(ctx, s.Client, create.identity(), p.Namespace, state.SecretUID, false); err != nil {
			return out, err
		}
		out.Phase = "revoking"
	case "revoking":
		secret, e := credential(ctx, s.Client, create.identity(), p.Namespace, state.SecretUID, false)
		if e != nil && !apierrors.IsNotFound(e) {
			return out, e
		}
		if secret != nil {
			uid, rv := secret.UID, secret.ResourceVersion
			if err = s.Client.CoreV1().Secrets(p.Namespace).Delete(ctx, secret.Name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid, ResourceVersion: &rv}}); err != nil && !apierrors.IsNotFound(err) {
				return out, err
			}
			return out, tx.Commit(ctx)
		}
		_, err = tx.Exec(ctx, `UPDATE station.runtime_instance_workspaces SET retained_at=COALESCE(retained_at,now()) WHERE instance_id=$1`, in.InstanceID)
		if err != nil {
			return out, err
		}
		binding.Retained = true
		out.Status, out.Phase = "succeeded", "deleted"
	default:
		return out, ErrBinding
	}
	_, err = tx.Exec(ctx, `UPDATE station.runtime_instance_executions SET status=$2,phase=$3,updated_at=now() WHERE operation_id=$1`, in.OperationID, out.Status, out.Phase)
	if err != nil {
		return out, err
	}
	return out, tx.Commit(ctx)
}
