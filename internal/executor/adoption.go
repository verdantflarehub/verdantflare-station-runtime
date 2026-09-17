package executor

import (
	"context"
	"encoding/json"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"reflect"
)

// declaredSubset allows server defaulted fields but requires every declared field
// and exact list membership/order. It never treats an extra container as a default.
func declaredSubset(want, live any) bool {
	switch w := want.(type) {
	case map[string]any:
		l, ok := live.(map[string]any)
		if !ok {
			return false
		}
		for key, v := range w {
			if !declaredSubset(v, l[key]) {
				return false
			}
		}
		return true
	case []any:
		l, ok := live.([]any)
		if !ok || len(w) != len(l) {
			return false
		}
		for i := range w {
			if !declaredSubset(w[i], l[i]) {
				return false
			}
		}
		return true
	default:
		return reflect.DeepEqual(want, live)
	}
}
func compatible(want, live any, ignoreReplicas bool) bool {
	wb, _ := json.Marshal(want)
	lb, _ := json.Marshal(live)
	var w, l map[string]any
	_ = json.Unmarshal(wb, &w)
	_ = json.Unmarshal(lb, &l)
	if ignoreReplicas {
		delete(w, "replicas")
		delete(l, "replicas")
	}
	return declaredSubset(w, l)
}
func adoptable(m metav1.ObjectMeta, t Target, station, org string) bool {
	if m.DeletionTimestamp != nil {
		return false
	}
	for key, value := range annotations(t, station, org) {
		if existing, ok := m.Annotations[key]; ok && existing != value {
			return false
		}
	}
	return true
}
func metadataPatch(m metav1.ObjectMeta, t Target, station, org string) []byte {
	b, _ := json.Marshal(map[string]any{"metadata": map[string]any{"uid": m.UID, "resourceVersion": m.ResourceVersion, "annotations": annotations(t, station, org)}})
	return b
}
func (k *Kubernetes) adopt(ctx context.Context, t Target, live *appsv1.Deployment, station, org string) (Result, error) {
	if t.ExpectedUID == "" || string(live.UID) != t.ExpectedUID || !adoptable(live.ObjectMeta, t, station, org) {
		return failed("CONFLICT", "Workload identity or ownership changed; refresh before adoption"), nil
	}
	if !compatible(t.Deployment.Spec, live.Spec, true) || !sameExecution(t.Deployment, live) {
		return failed("CONFLICT", "Workload differs from the registered template"), nil
	}
	existing := make([]*corev1.Service, 0, len(t.Services))
	// Preflight every object before writing anything. Retries recover metadata-only
	// partial adoption if the API becomes unavailable between object patches.
	for _, wanted := range t.Services {
		svc, err := k.Client.CoreV1().Services(wanted.Namespace).Get(ctx, wanted.Name, metav1.GetOptions{})
		if err != nil {
			return failed("CONFLICT", "Registered Service unavailable for adoption"), nil
		}
		if !adoptable(svc.ObjectMeta, t, station, org) || !compatible(wanted.Spec, svc.Spec, false) {
			return failed("CONFLICT", "Service differs from the registered template or ownership"), nil
		}
		existing = append(existing, svc)
	}
	for _, svc := range existing {
		if owned(svc.Annotations, t, station, org) {
			continue
		}
		if _, err := k.Client.CoreV1().Services(svc.Namespace).Patch(ctx, svc.Name, types.MergePatchType, metadataPatch(svc.ObjectMeta, t, station, org), metav1.PatchOptions{}); err != nil {
			return Result{}, err
		}
	}
	if !owned(live.Annotations, t, station, org) {
		if _, err := k.Client.AppsV1().Deployments(live.Namespace).Patch(ctx, live.Name, types.MergePatchType, metadataPatch(live.ObjectMeta, t, station, org), metav1.PatchOptions{}); err != nil {
			return Result{}, err
		}
	}
	return Result{"succeeded", "completed", "", ""}, nil
}

// API defaults must not disguise additional execution inputs absent from the template.
func sameExecution(want, live *appsv1.Deployment) bool {
	w, l := want.Spec.Template.Spec, live.Spec.Template.Spec
	if len(w.Containers) != len(l.Containers) || len(w.InitContainers) != len(l.InitContainers) || len(w.Volumes) != len(l.Volumes) || w.ServiceAccountName != l.ServiceAccountName || w.HostNetwork != l.HostNetwork || w.HostPID != l.HostPID || w.HostIPC != l.HostIPC {
		return false
	}
	wc := append(append([]corev1.Container{}, w.Containers...), w.InitContainers...)
	lc := append(append([]corev1.Container{}, l.Containers...), l.InitContainers...)
	for i, a := range wc {
		b := lc[i]
		if a.Name != b.Name || a.Image != b.Image || !reflect.DeepEqual(a.Command, b.Command) || !reflect.DeepEqual(a.Args, b.Args) || !reflect.DeepEqual(a.Env, b.Env) || !reflect.DeepEqual(a.EnvFrom, b.EnvFrom) || len(a.VolumeMounts) != len(b.VolumeMounts) || a.WorkingDir != b.WorkingDir {
			return false
		}
	}
	return true
}
