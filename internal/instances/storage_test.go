package instances

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

func fixture() (Identity, StorageProfile, *corev1.PersistentVolumeClaim, *corev1.PersistentVolume) {
	in := Identity{uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString()}
	p := StorageProfile{ID: "blender-standard", Namespace: "blender-test", Pool: "blender", NodeName: "worker-test", Claims: []string{"workspace-1"}, Bytes: 50 << 30, Hash: [32]byte{1}}
	class := "local-test"
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: p.Claims[0], Namespace: p.Namespace, UID: types.UID(uuid.NewString()), ResourceVersion: "1", Labels: map[string]string{stationKey: in.StationID, poolKey: p.Pool}},
		Spec:       corev1.PersistentVolumeClaimSpec{VolumeName: "workspace-1", StorageClassName: &class, AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}},
		Status:     corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound, Capacity: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("50Gi")}},
	}
	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "workspace-1", UID: types.UID(uuid.NewString()), Labels: map[string]string{stationKey: in.StationID, poolKey: p.Pool}},
		Spec: corev1.PersistentVolumeSpec{
			Capacity: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("50Gi")}, PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimRetain, StorageClassName: class,
			AccessModes:            []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			ClaimRef:               &corev1.ObjectReference{Namespace: p.Namespace, Name: pvc.Name, UID: pvc.UID},
			PersistentVolumeSource: corev1.PersistentVolumeSource{Local: &corev1.LocalVolumeSource{Path: "/test/workspace-1"}},
			NodeAffinity:           &corev1.VolumeNodeAffinity{Required: &corev1.NodeSelector{NodeSelectorTerms: []corev1.NodeSelectorTerm{{MatchExpressions: []corev1.NodeSelectorRequirement{{Key: corev1.LabelHostname, Operator: corev1.NodeSelectorOpIn, Values: []string{p.NodeName}}}}}}},
		}, Status: corev1.PersistentVolumeStatus{Phase: corev1.VolumeBound},
	}
	return in, p, pvc, pv
}

func TestPoolVolumeIdentityAndCapacity(t *testing.T) {
	cases := []struct {
		name   string
		change func(*corev1.PersistentVolumeClaim, *corev1.PersistentVolume)
		want   error
	}{
		{"unbound", func(c *corev1.PersistentVolumeClaim, v *corev1.PersistentVolume) {
			c.Status.Phase = corev1.ClaimPending
		}, ErrBinding},
		{"reclaim-delete", func(c *corev1.PersistentVolumeClaim, v *corev1.PersistentVolume) {
			v.Spec.PersistentVolumeReclaimPolicy = corev1.PersistentVolumeReclaimDelete
		}, ErrBinding},
		{"foreign-claim", func(c *corev1.PersistentVolumeClaim, v *corev1.PersistentVolume) {
			v.Spec.ClaimRef.UID = types.UID(uuid.NewString())
		}, ErrBinding},
		{"foreign-station", func(c *corev1.PersistentVolumeClaim, v *corev1.PersistentVolume) {
			c.Labels[stationKey] = uuid.NewString()
		}, ErrBinding},
		{"foreign-pool", func(c *corev1.PersistentVolumeClaim, v *corev1.PersistentVolume) { v.Labels[poolKey] = "other" }, ErrBinding},
		{"claimed", func(c *corev1.PersistentVolumeClaim, v *corev1.PersistentVolume) {
			c.Annotations = map[string]string{instanceKey: uuid.NewString()}
		}, ErrBinding},
		{"claim-too-small", func(c *corev1.PersistentVolumeClaim, v *corev1.PersistentVolume) {
			c.Status.Capacity[corev1.ResourceStorage] = resource.MustParse("49Gi")
		}, ErrCapacity},
		{"volume-too-small", func(c *corev1.PersistentVolumeClaim, v *corev1.PersistentVolume) {
			v.Spec.Capacity[corev1.ResourceStorage] = resource.MustParse("49Gi")
		}, ErrCapacity},
		{"wrong-node", func(c *corev1.PersistentVolumeClaim, v *corev1.PersistentVolume) {
			v.Spec.NodeAffinity.Required.NodeSelectorTerms[0].MatchExpressions[0].Values = []string{"other"}
		}, ErrBinding},
		{"deleting", func(c *corev1.PersistentVolumeClaim, v *corev1.PersistentVolume) {
			now := metav1.Now()
			c.DeletionTimestamp = &now
		}, ErrBinding},
		{"block", func(c *corev1.PersistentVolumeClaim, v *corev1.PersistentVolume) {
			mode := corev1.PersistentVolumeBlock
			v.Spec.VolumeMode = &mode
		}, ErrBinding},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			in, p, pvc, pv := fixture()
			test.change(pvc, pv)
			if _, err := Inspect(context.Background(), fake.NewClientset(pvc, pv), in, p, pvc.Name, nil, nil); !errors.Is(err, test.want) {
				t.Fatalf("want %v, got %v", test.want, err)
			}
		})
	}
	in, p, pvc, pv := fixture()
	client := fake.NewClientset(pvc, pv)
	b, err := Inspect(context.Background(), client, in, p, pvc.Name, nil, nil)
	if err != nil || b.ReservedBytes != 50<<30 || b.PVCUID != string(pvc.UID) || b.PVUID != string(pv.UID) {
		t.Fatal(b, err)
	}
	b.PVCUID = uuid.NewString()
	if _, err = Inspect(context.Background(), client, in, p, pvc.Name, &b, nil); !errors.Is(err, ErrBinding) {
		t.Fatal("replaced claim accepted", err)
	}
}

func volumePod(in Identity, p StorageProfile, pvc string) *corev1.Pod {
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "preparer", Namespace: p.Namespace, UID: types.UID(uuid.NewString()), Annotations: map[string]string{instanceKey: in.InstanceID, stationKey: in.StationID, organizationKey: in.OrganizationID}}, Spec: corev1.PodSpec{Volumes: []corev1.Volume{{Name: "data", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: pvc}}}}}}
}

func TestVolumeOccupancyRequiresExactOwnedPodUID(t *testing.T) {
	in, p, pvc, pv := fixture()
	pod := volumePod(in, p, pvc.Name)
	client := fake.NewClientset(pvc, pv, pod)
	ctx := context.Background()
	if _, err := Inspect(ctx, client, in, p, pvc.Name, nil, nil); !errors.Is(err, ErrBusy) {
		t.Fatal("in-use volume accepted", err)
	}
	if _, err := Inspect(ctx, client, in, p, pvc.Name, nil, map[types.UID]bool{pod.UID: true}); err != nil {
		t.Fatal(err)
	}
	pod.Annotations[organizationKey] = uuid.NewString()
	client = fake.NewClientset(pvc, pv, pod)
	if _, err := Inspect(ctx, client, in, p, pvc.Name, nil, map[types.UID]bool{pod.UID: true}); !errors.Is(err, ErrBusy) {
		t.Fatal("foreign owner accepted", err)
	}
}

func TestClaimRecoversLostResponseAndRefusesOwnershipChange(t *testing.T) {
	in, p, pvc, pv := fixture()
	client := fake.NewClientset(pvc, pv)
	ctx := context.Background()
	b, err := Inspect(ctx, client, in, p, pvc.Name, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	lose := true
	client.PrependReactor("update", "persistentvolumeclaims", func(a ktesting.Action) (bool, runtime.Object, error) {
		if !lose {
			return false, nil, nil
		}
		lose = false
		obj := a.(ktesting.UpdateAction).GetObject()
		if err := client.Tracker().Update(corev1.SchemeGroupVersion.WithResource("persistentvolumeclaims"), obj, p.Namespace); err != nil {
			t.Fatal(err)
		}
		return true, nil, errors.New("response lost after Kubernetes accepted update")
	})
	if err = Claim(ctx, client, in, p, b); err == nil {
		t.Fatal("expected uncertain response")
	}
	if err = Claim(ctx, client, in, p, b); err != nil {
		t.Fatal("same claim did not recover", err)
	}
	current, _ := client.CoreV1().PersistentVolumeClaims(p.Namespace).Get(ctx, pvc.Name, metav1.GetOptions{})
	if current.Annotations[instanceKey] != in.InstanceID {
		t.Fatal("owner not written")
	}
	current.Annotations[instanceKey] = uuid.NewString()
	_, _ = client.CoreV1().PersistentVolumeClaims(p.Namespace).Update(ctx, current, metav1.UpdateOptions{})
	if err = Claim(ctx, client, in, p, b); !errors.Is(err, ErrBinding) {
		t.Fatal("overwrote new owner", err)
	}
}

func TestObservationAndCASFailuresDoNotBecomeCapacitySuccess(t *testing.T) {
	in, p, pvc, pv := fixture()
	client := fake.NewClientset(pvc, pv)
	ctx := context.Background()
	b, err := Inspect(ctx, client, in, p, pvc.Name, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	client.PrependReactor("update", "persistentvolumeclaims", func(a ktesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewConflict(schema.GroupResource{Resource: "persistentvolumeclaims"}, pvc.Name, errors.New("concurrent owner"))
	})
	if err = Claim(ctx, client, in, p, b); !apierrors.IsConflict(err) {
		t.Fatal(err)
	}
	client.PrependReactor("list", "pods", func(a ktesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("observation unavailable")
	})
	if _, err = Inspect(ctx, client, in, p, pvc.Name, nil, nil); err == nil || errors.Is(err, ErrCapacity) {
		t.Fatal("uncertainty was treated as capacity", err)
	}
}
