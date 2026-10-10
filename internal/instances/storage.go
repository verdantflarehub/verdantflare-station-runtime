// Package instances manages individual desktop workloads independently of app installation.
package instances

import (
	"context"
	"errors"
	"slices"

	"github.com/google/uuid"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/kubernetes"
)

const (
	stationKey      = "station.verdantflare.com/station-id"
	poolKey         = "station.verdantflare.com/workspace-pool"
	instanceKey     = "station.verdantflare.com/instance-id"
	organizationKey = "station.verdantflare.com/organization-id"
	projectKey      = "station.verdantflare.com/project-id"
	operationKey    = "station.verdantflare.com/create-operation-id"
)

var (
	ErrCapacity = errors.New("STORAGE_CAPACITY_UNAVAILABLE")
	ErrBinding  = errors.New("WORKSPACE_BINDING_CONFLICT")
	ErrBusy     = errors.New("WORKSPACE_IN_USE")
	ErrRetained = errors.New("WORKSPACE_RETAINED")
	ErrInvalid  = errors.New("INVALID_ARGUMENT")
)

// StorageProfile is operator configuration, never supplied by a user request.
// Hash covers the entire registered execution profile, including its templates.
type StorageProfile struct {
	ID, Namespace, Pool, NodeName string
	Claims                        []string
	Bytes                         int64
	Hash                          [32]byte
}

type Identity struct {
	StationID         string `json:"station_id"`
	OrganizationID    string `json:"organization_id"`
	ProjectID         string `json:"project_id"`
	InstanceID        string `json:"instance_id"`
	CreateOperationID string `json:"create_operation_id"`
}

type VolumeBinding struct {
	Identity
	ProfileID     string `json:"profile_id"`
	ProfileHash   []byte `json:"-"`
	Namespace     string `json:"namespace"`
	Pool          string `json:"pool"`
	PVCName       string `json:"pvc_name"`
	PVCUID        string `json:"pvc_uid"`
	PVName        string `json:"pv_name"`
	PVUID         string `json:"pv_uid"`
	NodeName      string `json:"node_name"`
	ReservedBytes int64  `json:"reserved_bytes"`
	Retained      bool   `json:"retained"`
}

func validIdentity(in Identity) bool {
	for _, v := range []string{in.StationID, in.OrganizationID, in.ProjectID, in.InstanceID, in.CreateOperationID} {
		parsed, err := uuid.Parse(v)
		if err != nil || parsed == uuid.Nil || parsed.String() != v {
			return false
		}
	}
	return true
}

func (p StorageProfile) valid() bool {
	if len(validation.IsDNS1123Label(p.ID)) != 0 || len(validation.IsDNS1123Label(p.Namespace)) != 0 || p.Namespace == "default" ||
		len(validation.IsDNS1123Label(p.Pool)) != 0 || len(validation.IsDNS1123Subdomain(p.NodeName)) != 0 || p.Bytes <= 0 || p.Hash == [32]byte{} || len(p.Claims) == 0 {
		return false
	}
	seen := map[string]bool{}
	for _, claim := range p.Claims {
		if len(validation.IsDNS1123Subdomain(claim)) != 0 || seen[claim] {
			return false
		}
		seen[claim] = true
	}
	return true
}

func volumeOwner(in Identity) map[string]string {
	return map[string]string{instanceKey: in.InstanceID, organizationKey: in.OrganizationID, projectKey: in.ProjectID, operationKey: in.CreateOperationID}
}

// Inspect requires both sides of a Bound Retain local-volume pair. A name alone
// is insufficient: replacement PVCs/PVs must never inherit an old reservation.
func Inspect(ctx context.Context, client kubernetes.Interface, in Identity, p StorageProfile, claim string, expected *VolumeBinding, allowedPods map[types.UID]bool) (VolumeBinding, error) {
	var out VolumeBinding
	if !validIdentity(in) || !p.valid() || !slices.Contains(p.Claims, claim) {
		return out, ErrInvalid
	}
	pvc, err := client.CoreV1().PersistentVolumeClaims(p.Namespace).Get(ctx, claim, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return out, ErrCapacity
	}
	if err != nil {
		return out, err
	}
	if pvc.UID == "" || pvc.DeletionTimestamp != nil || pvc.Status.Phase != corev1.ClaimBound || pvc.Spec.VolumeName == "" ||
		pvc.Labels[stationKey] != in.StationID || pvc.Labels[poolKey] != p.Pool ||
		(pvc.Spec.VolumeMode != nil && *pvc.Spec.VolumeMode != corev1.PersistentVolumeFilesystem) {
		return out, ErrBinding
	}
	if pvc.Status.Capacity.Storage().Value() < p.Bytes {
		return out, ErrCapacity
	}
	if len(pvc.Spec.AccessModes) != 1 || pvc.Spec.AccessModes[0] != corev1.ReadWriteOnce {
		return out, ErrBinding
	}
	for k, v := range volumeOwner(in) {
		// A fresh claim must be unowned. An existing binding permits unmarked PVCs
		// only to recover the gap between the SQL commit and Kubernetes update.
		if (expected == nil && pvc.Annotations[k] != "") || (expected != nil && pvc.Annotations[k] != "" && pvc.Annotations[k] != v) {
			return out, ErrBinding
		}
	}
	pv, err := client.CoreV1().PersistentVolumes().Get(ctx, pvc.Spec.VolumeName, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return out, ErrBinding
	}
	if err != nil {
		return out, err
	}
	if pv.UID == "" || pv.DeletionTimestamp != nil || pv.Status.Phase != corev1.VolumeBound || pv.Spec.PersistentVolumeReclaimPolicy != corev1.PersistentVolumeReclaimRetain ||
		pv.Labels[stationKey] != in.StationID || pv.Labels[poolKey] != p.Pool || pv.Spec.Local == nil || pv.Spec.Local.Path == "" ||
		pv.Spec.ClaimRef == nil || pv.Spec.ClaimRef.UID != pvc.UID || pv.Spec.ClaimRef.Name != pvc.Name || pv.Spec.ClaimRef.Namespace != pvc.Namespace ||
		pv.Spec.StorageClassName != value(pvc.Spec.StorageClassName) || (pv.Spec.VolumeMode != nil && *pv.Spec.VolumeMode != corev1.PersistentVolumeFilesystem) ||
		len(pv.Spec.AccessModes) != 1 || pv.Spec.AccessModes[0] != corev1.ReadWriteOnce {
		return out, ErrBinding
	}
	capacity := pv.Spec.Capacity[corev1.ResourceStorage]
	if capacity.Value() < p.Bytes {
		return out, ErrCapacity
	}
	// The registered local pool is tied to one concrete node, not a best-effort
	// selector that might schedule a different machine with unrelated bytes.
	if pv.Spec.NodeAffinity == nil || pv.Spec.NodeAffinity.Required == nil || len(pv.Spec.NodeAffinity.Required.NodeSelectorTerms) != 1 {
		return out, ErrBinding
	}
	term := pv.Spec.NodeAffinity.Required.NodeSelectorTerms[0]
	if len(term.MatchFields) != 0 || len(term.MatchExpressions) != 1 {
		return out, ErrBinding
	}
	expr := term.MatchExpressions[0]
	if expr.Key != corev1.LabelHostname || expr.Operator != corev1.NodeSelectorOpIn || len(expr.Values) != 1 || expr.Values[0] != p.NodeName {
		return out, ErrBinding
	}
	out = VolumeBinding{Identity: in, ProfileID: p.ID, ProfileHash: append([]byte(nil), p.Hash[:]...), Namespace: p.Namespace, Pool: p.Pool, PVCName: pvc.Name, PVCUID: string(pvc.UID), PVName: pv.Name, PVUID: string(pv.UID), NodeName: p.NodeName, ReservedBytes: p.Bytes}
	if expected != nil && (expected.PVCUID != out.PVCUID || expected.PVUID != out.PVUID || expected.PVName != out.PVName || expected.PVCName != out.PVCName || expected.Identity != in) {
		return VolumeBinding{}, ErrBinding
	}
	pods, err := client.CoreV1().Pods(p.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return VolumeBinding{}, err
	}
	for _, pod := range pods.Items {
		for _, volume := range pod.Spec.Volumes {
			if volume.PersistentVolumeClaim == nil || volume.PersistentVolumeClaim.ClaimName != claim {
				continue
			}
			if pod.UID == "" || !allowedPods[pod.UID] || pod.Annotations[instanceKey] != in.InstanceID || pod.Annotations[stationKey] != in.StationID || pod.Annotations[organizationKey] != in.OrganizationID {
				return VolumeBinding{}, ErrBusy
			}
		}
	}
	return out, nil
}

func value(v *string) string {
	if v == nil {
		return ""
	}
	return *v
}

// Claim marks the exact PVC after its durable reservation. Update uses the
// observed resourceVersion, so a concurrent ownership change is never patched over.
func Claim(ctx context.Context, client kubernetes.Interface, in Identity, p StorageProfile, binding VolumeBinding) error {
	if binding.Retained {
		return ErrRetained
	}
	if _, err := Inspect(ctx, client, in, p, binding.PVCName, &binding, nil); err != nil {
		return err
	}
	pvc, err := client.CoreV1().PersistentVolumeClaims(p.Namespace).Get(ctx, binding.PVCName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if string(pvc.UID) != binding.PVCUID || pvc.Spec.VolumeName != binding.PVName || pvc.DeletionTimestamp != nil ||
		pvc.Labels[stationKey] != in.StationID || pvc.Labels[poolKey] != p.Pool || pvc.Status.Phase != corev1.ClaimBound {
		return ErrBinding
	}
	complete := true
	for k, v := range volumeOwner(in) {
		if pvc.Annotations[k] != "" && pvc.Annotations[k] != v {
			return ErrBinding
		}
		complete = complete && pvc.Annotations[k] == v
	}
	if complete {
		return nil
	}
	pvc = pvc.DeepCopy()
	if pvc.Annotations == nil {
		pvc.Annotations = map[string]string{}
	}
	for k, v := range volumeOwner(in) {
		pvc.Annotations[k] = v
	}
	_, err = client.CoreV1().PersistentVolumeClaims(p.Namespace).Update(ctx, pvc, metav1.UpdateOptions{})
	return err
}
