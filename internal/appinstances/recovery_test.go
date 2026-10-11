package appinstances

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ktesting "k8s.io/client-go/testing"
)

func TestCheckpointFailureStopsBeforeNextResourceAndResumesOriginalRun(t *testing.T) {
	for _, failAt := range []int{1, 2, 3, 4, 5} {
		t.Run(string(rune('0'+failAt)), func(t *testing.T) {
			ctx := context.Background()
			in, p, pvc, pv := fixture()
			c := client(pvc, pv)
			b := bind(t, c, in, p, pvc.Name)
			var saved Observation
			calls := 0
			unavailable := errors.New("journal unavailable")
			checkpoint := func(_ context.Context, next Observation) error {
				calls++
				if calls == failAt {
					return unavailable
				}
				saved = next
				return nil
			}
			if _, e := EnsureWorker(ctx, c, in, p, b, saved, checkpoint); !errors.Is(e, unavailable) {
				t.Fatal("checkpoint failure ignored", e)
			}
			pods, _ := c.CoreV1().Pods(p.Namespace).List(ctx, metav1.ListOptions{})
			services, _ := c.CoreV1().Services(p.Namespace).List(ctx, metav1.ListOptions{})
			secrets, _ := c.CoreV1().Secrets(p.Namespace).List(ctx, metav1.ListOptions{})
			if (len(secrets.Items) != 0) != (failAt >= 2) || (len(services.Items) != 0) != (failAt >= 3) || (len(pods.Items) != 0) != (failAt >= 4) {
				t.Fatal("executed beyond failed checkpoint")
			}
			result, e := EnsureWorker(ctx, c, in, p, b, saved, func(_ context.Context, next Observation) error { saved = next; return nil })
			if e != nil || saved != result || !saved.valid() || saved.PodUID == "" {
				t.Fatal("could not resume original run", e)
			}
			if len(secrets.Items) > 0 && result.SecretUID != string(secrets.Items[0].UID) ||
				len(services.Items) > 0 && result.ServiceUID != string(services.Items[0].UID) ||
				len(pods.Items) > 0 && result.PodUID != string(pods.Items[0].UID) {
				t.Fatal("recovery replaced an existing resource")
			}
		})
	}
}

func TestLostCreateResponsesRecoverAllResourceKinds(t *testing.T) {
	for _, kind := range []string{"secrets", "services", "pods"} {
		t.Run(kind, func(t *testing.T) {
			ctx := context.Background()
			in, p, pvc, pv := fixture()
			c := client(pvc, pv)
			b := bind(t, c, in, p, pvc.Name)
			var saved Observation
			record := func(_ context.Context, next Observation) error { saved = next; return nil }
			var lostUID types.UID
			c.PrependReactor("create", kind, func(a ktesting.Action) (bool, runtime.Object, error) {
				if lostUID != "" {
					return false, nil, nil
				}
				obj := a.(ktesting.CreateAction).GetObject().DeepCopyObject()
				meta := obj.(metav1.Object)
				lostUID = types.UID(uuid.NewString())
				meta.SetUID(lostUID)
				meta.SetResourceVersion("1")
				if e := c.Tracker().Add(obj); e != nil {
					t.Fatal(e)
				}
				return true, nil, errors.New("response lost after creation")
			})
			if _, e := EnsureWorker(ctx, c, in, p, b, saved, record); e == nil {
				t.Fatal("missing transport error")
			}
			recovered, e := EnsureWorker(ctx, c, in, p, b, saved, record)
			if e != nil {
				t.Fatal(e)
			}
			uid := map[string]string{"secrets": recovered.SecretUID, "services": recovered.ServiceUID, "pods": recovered.PodUID}[kind]
			if uid != string(lostUID) {
				t.Fatal("created a replacement after lost response")
			}
			c.ClearActions()
			if _, e = EnsureWorker(ctx, c, in, p, b, saved, record); e != nil {
				t.Fatal(e)
			}
			assertNoEffects(t, c)
		})
	}
}

func TestClaimUsesInspectedResourceVersion(t *testing.T) {
	ctx := context.Background()
	in, p, pvc, pv := fixture()
	c := client(pvc, pv)
	b, e := InspectVolume(ctx, c, in.Identity, p, pvc.Name, nil, nil)
	if e != nil {
		t.Fatal(e)
	}
	changed := false
	c.PrependReactor("list", "pods", func(ktesting.Action) (bool, runtime.Object, error) {
		// Another owner retains the claim after the PVC snapshot was checked.
		if !changed {
			changed = true
			retained := pvc.DeepCopy()
			retained.ResourceVersion = "2"
			retained.Annotations = map[string]string{prefix + "retained-at": "test-retention"}
			if e := c.Tracker().Update(corev1.SchemeGroupVersion.WithResource("persistentvolumeclaims"), retained, p.Namespace); e != nil {
				t.Fatal(e)
			}
		}
		return false, nil, nil
	})
	c.PrependReactor("update", "persistentvolumeclaims", func(a ktesting.Action) (bool, runtime.Object, error) {
		candidate := a.(ktesting.UpdateAction).GetObject().(*corev1.PersistentVolumeClaim)
		if candidate.ResourceVersion != "1" {
			t.Fatal("uninspected new resource version used")
		}
		return true, nil, apierrors.NewConflict(schema.GroupResource{Resource: "persistentvolumeclaims"}, pvc.Name, errors.New("concurrent retention"))
	})
	if e = ClaimVolume(ctx, c, in.Identity, p, b); !apierrors.IsConflict(e) {
		t.Fatal("concurrent ownership change was overwritten", e)
	}
	actual, _ := c.CoreV1().PersistentVolumeClaims(p.Namespace).Get(ctx, pvc.Name, metav1.GetOptions{})
	if actual.Annotations[prefix+"retained-at"] != "test-retention" || actual.Annotations[prefix+"instance-id"] != "" {
		t.Fatal("retained volume adopted")
	}
	c.ClearActions()
	if e = ClaimVolume(ctx, c, in.Identity, p, b); !errors.Is(e, ErrBinding) {
		t.Fatal("retry did not reject retention", e)
	}
	assertNoEffects(t, c)
}

func TestReplacedOrForeignVolumesRejectBeforeWorkerEffects(t *testing.T) {
	for _, change := range []func(*corev1.PersistentVolumeClaim, *corev1.PersistentVolume){
		func(pvc *corev1.PersistentVolumeClaim, _ *corev1.PersistentVolume) {
			pvc.UID = types.UID(uuid.NewString())
		},
		func(_ *corev1.PersistentVolumeClaim, pv *corev1.PersistentVolume) {
			pv.UID = types.UID(uuid.NewString())
		},
		func(_ *corev1.PersistentVolumeClaim, pv *corev1.PersistentVolume) {
			pv.Annotations = map[string]string{prefix + "instance-id": id()}
		},
		func(_ *corev1.PersistentVolumeClaim, pv *corev1.PersistentVolume) {
			pv.Annotations = map[string]string{prefix + "retained-at": "test-retention"}
		},
	} {
		ctx := context.Background()
		in, p, pvc, pv := fixture()
		c := client(pvc, pv)
		b := bind(t, c, in, p, pvc.Name)
		pvc, _ = c.CoreV1().PersistentVolumeClaims(p.Namespace).Get(ctx, pvc.Name, metav1.GetOptions{})
		change(pvc, pv)
		c.CoreV1().PersistentVolumeClaims(p.Namespace).Update(ctx, pvc, metav1.UpdateOptions{})
		c.CoreV1().PersistentVolumes().Update(ctx, pv, metav1.UpdateOptions{})
		c.ClearActions()
		if _, e := EnsureWorker(ctx, c, in, p, b, Observation{}, acceptObservation); !errors.Is(e, ErrBinding) {
			t.Fatal("changed or foreign volume accepted", e)
		}
		assertNoEffects(t, c)
	}
}

func TestMissingOrInvalidCheckpointIsRejectedBeforeIO(t *testing.T) {
	ctx := context.Background()
	in, p, pvc, pv := fixture()
	c := client(pvc, pv)
	b := bind(t, c, in, p, pvc.Name)
	c.ClearActions()
	if _, e := EnsureWorker(ctx, c, in, p, b, Observation{}, nil); !errors.Is(e, ErrInvalid) {
		t.Fatal(e)
	}
	if _, e := EnsureWorker(ctx, c, in, p, b, Observation{PodUID: uuid.NewString()}, acceptObservation); !errors.Is(e, ErrInvalid) {
		t.Fatal(e)
	}
	if len(c.Actions()) != 0 {
		t.Fatal("invalid checkpoint performed IO")
	}
}

func TestActualKubernetesDefaultsAcceptedButExtraPrivilegesRejected(t *testing.T) {
	ctx := context.Background()
	in, p, pvc, pv := fixture()
	c := client(pvc, pv)
	b := bind(t, c, in, p, pvc.Name)
	saved, e := EnsureWorker(ctx, c, in, p, b, Observation{}, acceptObservation)
	if e != nil {
		t.Fatal(e)
	}
	pod, _ := c.CoreV1().Pods(p.Namespace).Get(ctx, runName(in), metav1.GetOptions{})
	pod.Spec.NodeName = p.NodeName
	pod.Spec.DNSPolicy = corev1.DNSClusterFirst
	pod.Spec.ServiceAccountName = "default"
	pod.Spec.DeprecatedServiceAccount = "default"
	seconds := int64(300)
	pod.Spec.Tolerations = []corev1.Toleration{{Key: "node.kubernetes.io/not-ready", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoExecute, TolerationSeconds: &seconds},
		{Key: "node.kubernetes.io/unreachable", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoExecute, TolerationSeconds: &seconds}}
	container := &pod.Spec.Containers[0]
	container.TerminationMessagePath = "/dev/termination-log"
	container.TerminationMessagePolicy = corev1.TerminationMessageReadFile
	container.StartupProbe.SuccessThreshold, container.ReadinessProbe.SuccessThreshold, container.LivenessProbe.SuccessThreshold = 1, 1, 1
	c.CoreV1().Pods(p.Namespace).Update(ctx, pod, metav1.UpdateOptions{})
	svc, _ := c.CoreV1().Services(p.Namespace).Get(ctx, runName(in), metav1.GetOptions{})
	svc.Spec.ClusterIP = "10.96.0.9"
	svc.Spec.ClusterIPs = []string{svc.Spec.ClusterIP}
	svc.Spec.IPFamilies = []corev1.IPFamily{corev1.IPv4Protocol}
	single, cluster := corev1.IPFamilyPolicySingleStack, corev1.ServiceInternalTrafficPolicyCluster
	svc.Spec.IPFamilyPolicy, svc.Spec.InternalTrafficPolicy = &single, &cluster
	svc.Spec.SessionAffinity = corev1.ServiceAffinityNone
	c.CoreV1().Services(p.Namespace).Update(ctx, svc, metav1.UpdateOptions{})
	c.ClearActions()
	if _, e = EnsureWorker(ctx, c, in, p, b, saved, acceptObservation); e != nil {
		t.Fatal("standard defaults rejected", e)
	}
	assertNoEffects(t, c)
	pod.Spec.Tolerations = append(pod.Spec.Tolerations, corev1.Toleration{Operator: corev1.TolerationOpExists})
	c.CoreV1().Pods(p.Namespace).Update(ctx, pod, metav1.UpdateOptions{})
	c.ClearActions()
	if _, e = ObserveWorker(ctx, c, in, p, b, saved); !errors.Is(e, ErrBinding) {
		t.Fatal("unrestricted scheduling accepted", e)
	}
	assertNoEffects(t, c)
}
