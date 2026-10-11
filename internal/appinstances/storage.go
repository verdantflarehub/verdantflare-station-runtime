package appinstances

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
)

// InspectVolume is read-only. SQL reservation must commit before ClaimVolume.
// allowedPods must come from the durable run record, never a caller's raw UID.
func InspectVolume(ctx context.Context, client kubernetes.Interface, in Identity, p Profile, claim string, expected *Volume, allowedPods map[types.UID]bool) (Volume, error) {
	b, _, err := inspectVolume(ctx, client, in, p, claim, expected, allowedPods)
	return b, err
}

func inspectVolume(ctx context.Context, client kubernetes.Interface, in Identity, p Profile, claim string, expected *Volume, allowedPods map[types.UID]bool) (Volume, *corev1.PersistentVolumeClaim, error) {
	if !in.valid() || !p.Valid() {
		return Volume{}, nil, ErrInvalid
	}
	found := false
	for _, v := range p.Claims {
		found = found || v == claim
	}
	if !found {
		return Volume{}, nil, ErrInvalid
	}
	pvc, e := client.CoreV1().PersistentVolumeClaims(p.Namespace).Get(ctx, claim, metav1.GetOptions{})
	if apierrors.IsNotFound(e) {
		return Volume{}, nil, ErrCapacity
	}
	if e != nil {
		return Volume{}, nil, e
	}
	if !validUID(string(pvc.UID)) || pvc.DeletionTimestamp != nil || pvc.Status.Phase != corev1.ClaimBound || pvc.Spec.VolumeName == "" ||
		pvc.Labels[stationKey] != in.StationID || pvc.Labels[poolKey] != p.Pool ||
		pvc.Spec.VolumeMode != nil && *pvc.Spec.VolumeMode != corev1.PersistentVolumeFilesystem ||
		len(pvc.Spec.AccessModes) != 1 || pvc.Spec.AccessModes[0] != corev1.ReadWriteOnce {
		return Volume{}, nil, ErrBinding
	}
	if pvc.Status.Capacity.Storage().Value() < p.StorageBytes {
		return Volume{}, nil, ErrCapacity
	}
	owner := in.annotations(p)
	for key, v := range owner {
		actual := pvc.Annotations[key]
		if expected == nil && actual != "" || expected != nil && actual != "" && actual != v {
			return Volume{}, nil, ErrBinding
		}
	}
	// Never adopt a legacy Project-bound or retained workspace.
	for _, key := range []string{prefix + "project-id", prefix + "create-operation-id", prefix + "retained-at"} {
		if pvc.Annotations[key] != "" {
			return Volume{}, nil, ErrBinding
		}
	}
	pv, e := client.CoreV1().PersistentVolumes().Get(ctx, pvc.Spec.VolumeName, metav1.GetOptions{})
	if e != nil {
		return Volume{}, nil, e
	}
	// A pool label alone does not make a retained or foreign PV reusable.
	for key, value := range owner {
		if actual := pv.Annotations[key]; actual != "" && (expected == nil || actual != value) {
			return Volume{}, nil, ErrBinding
		}
	}
	for _, key := range []string{prefix + "project-id", prefix + "create-operation-id", prefix + "retained-at"} {
		if pv.Annotations[key] != "" {
			return Volume{}, nil, ErrBinding
		}
	}
	if !validUID(string(pv.UID)) || pv.DeletionTimestamp != nil || pv.Status.Phase != corev1.VolumeBound ||
		pv.Spec.PersistentVolumeReclaimPolicy != corev1.PersistentVolumeReclaimRetain || pv.Labels[stationKey] != in.StationID || pv.Labels[poolKey] != p.Pool ||
		pv.Spec.Local == nil || pv.Spec.Local.Path == "" || pv.Spec.ClaimRef == nil || pv.Spec.ClaimRef.UID != pvc.UID ||
		pv.Spec.ClaimRef.Name != claim || pv.Spec.ClaimRef.Namespace != p.Namespace || pv.Spec.StorageClassName != stringValue(pvc.Spec.StorageClassName) ||
		pv.Spec.VolumeMode != nil && *pv.Spec.VolumeMode != corev1.PersistentVolumeFilesystem ||
		len(pv.Spec.AccessModes) != 1 || pv.Spec.AccessModes[0] != corev1.ReadWriteOnce {
		return Volume{}, nil, ErrBinding
	}
	capacity := pv.Spec.Capacity[corev1.ResourceStorage]
	if capacity.Value() < p.StorageBytes {
		return Volume{}, nil, ErrCapacity
	}
	if pv.Spec.NodeAffinity == nil || pv.Spec.NodeAffinity.Required == nil || len(pv.Spec.NodeAffinity.Required.NodeSelectorTerms) != 1 {
		return Volume{}, nil, ErrBinding
	}
	term := pv.Spec.NodeAffinity.Required.NodeSelectorTerms[0]
	if len(term.MatchExpressions) != 1 || len(term.MatchFields) != 0 {
		return Volume{}, nil, ErrBinding
	}
	expr := term.MatchExpressions[0]
	if expr.Key != corev1.LabelHostname || expr.Operator != corev1.NodeSelectorOpIn || len(expr.Values) != 1 || expr.Values[0] != p.NodeName {
		return Volume{}, nil, ErrBinding
	}
	out := Volume{Identity: in, ProfileHash: p.Hash(), PVCName: claim, PVCUID: string(pvc.UID), PVName: pv.Name, PVUID: string(pv.UID)}
	if expected != nil && *expected != out {
		return Volume{}, nil, ErrBinding
	}
	pods, e := client.CoreV1().Pods(p.Namespace).List(ctx, metav1.ListOptions{})
	if e != nil {
		return Volume{}, nil, e
	}
	for _, pod := range pods.Items {
		for _, v := range pod.Spec.Volumes {
			if v.PersistentVolumeClaim == nil || v.PersistentVolumeClaim.ClaimName != claim {
				continue
			}
			if !validUID(string(pod.UID)) || !allowedPods[pod.UID] {
				return Volume{}, nil, ErrBusy
			}
			for key, value := range owner {
				if pod.Annotations[key] != value {
					return Volume{}, nil, ErrBinding
				}
			}
		}
	}
	return out, pvc, nil
}
func stringValue(v *string) string {
	if v == nil {
		return ""
	}
	return *v
}

// ClaimVolume requires a committed binding. It marks a pool PVC, never replaces
// or deletes it. ResourceVersion prevents overwriting a concurrent owner.
func ClaimVolume(ctx context.Context, client kubernetes.Interface, in Identity, p Profile, b Volume) error {
	if !validVolume(in, p, b) {
		return ErrInvalid
	}
	// Update the exact snapshot that passed inspection. A second GET here would
	// allow a newly retained or reassigned claim to bypass the earlier checks.
	_, pvc, e := inspectVolume(ctx, client, in, p, b.PVCName, &b, nil)
	if e != nil {
		return e
	}
	if string(pvc.UID) != b.PVCUID || pvc.Spec.VolumeName != b.PVName || pvc.DeletionTimestamp != nil || pvc.ResourceVersion == "" ||
		pvc.Labels[stationKey] != in.StationID || pvc.Labels[poolKey] != p.Pool {
		return ErrBinding
	}
	complete := true
	for key, value := range in.annotations(p) {
		if pvc.Annotations[key] != "" && pvc.Annotations[key] != value {
			return ErrBinding
		}
		complete = complete && pvc.Annotations[key] == value
	}
	if complete {
		return nil
	}
	next := pvc.DeepCopy()
	if next.Annotations == nil {
		next.Annotations = map[string]string{}
	}
	for key, value := range in.annotations(p) {
		next.Annotations[key] = value
	}
	_, e = client.CoreV1().PersistentVolumeClaims(p.Namespace).Update(ctx, next, metav1.UpdateOptions{})
	return e
}
