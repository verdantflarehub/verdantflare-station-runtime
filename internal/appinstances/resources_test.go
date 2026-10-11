package appinstances

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

// Resource-only tests use an in-memory checkpoint; they do not prove SQL durability.
func acceptObservation(_ context.Context, o Observation) error {
	if !o.valid() {
		return ErrInvalid
	}
	return nil
}

func id() string { return uuid.Must(uuid.NewV7()).String() }
func fixture() (Run, Profile, *corev1.PersistentVolumeClaim, *corev1.PersistentVolume) {
	in := Run{Identity: Identity{StationID: id(), OrganizationID: id(), UserID: id(), InstallationID: id(), InstanceID: id()}, OperationID: id(), Generation: 1}
	p := Profile{AppID: "comfyui", AppVersion: "0.1.2", ID: "base-cpu", TemplateVersion: TemplateVersion,
		Namespace: "app-test", NodeName: "test-node", Pool: "cpu-workspaces", Claims: []string{"slot-a", "slot-b"}, StorageBytes: 1 << 30,
		Image: "registry.example.test/app:comfyui-worker-cpu-v0.1.2", Port: 8188, RunAsUser: 10001,
		CPURequestMillis: 1000, CPULimitMillis: 4000, MemoryRequestBytes: 2 << 30, MemoryLimitBytes: 8 << 30,
		Env: map[string]string{"COMFYUI_DEVICE": "cpu"}, Bindings: map[string]string{"COMFYUI_INSTANCE_ID": "instance_id", "COMFYUI_WORKER_TOKEN": "worker_token",
			"COMFYUI_START_OPERATION_ID": "start_operation_id", "COMFYUI_GENERATION": "generation",
			"COMFYUI_POD_UID": "pod_uid", "COMFYUI_POD_NAME": "pod_name", "COMFYUI_POD_NAMESPACE": "namespace", "COMFYUI_CONTAINER_NAME": "container"}}
	mode := corev1.PersistentVolumeFilesystem
	class := "test-local"
	pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "slot-a", Namespace: p.Namespace, UID: types.UID(uuid.NewString()), ResourceVersion: "1",
		Labels: map[string]string{stationKey: in.StationID, poolKey: p.Pool}}, Spec: corev1.PersistentVolumeClaimSpec{AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
		StorageClassName: &class, VolumeName: "pv-slot-a", VolumeMode: &mode}, Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound, Capacity: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("10Gi")}}}
	pv := &corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: "pv-slot-a", UID: types.UID(uuid.NewString()), ResourceVersion: "1", Labels: map[string]string{stationKey: in.StationID, poolKey: p.Pool}},
		Spec: corev1.PersistentVolumeSpec{AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, StorageClassName: class, VolumeMode: &mode,
			Capacity: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("10Gi")}, PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimRetain,
			ClaimRef:               &corev1.ObjectReference{Name: pvc.Name, Namespace: pvc.Namespace, UID: pvc.UID},
			PersistentVolumeSource: corev1.PersistentVolumeSource{Local: &corev1.LocalVolumeSource{Path: "/test-only/workspace-a"}},
			NodeAffinity:           &corev1.VolumeNodeAffinity{Required: &corev1.NodeSelector{NodeSelectorTerms: []corev1.NodeSelectorTerm{{MatchExpressions: []corev1.NodeSelectorRequirement{{Key: corev1.LabelHostname, Operator: corev1.NodeSelectorOpIn, Values: []string{p.NodeName}}}}}}}},
		Status: corev1.PersistentVolumeStatus{Phase: corev1.VolumeBound}}
	return in, p, pvc, pv
}
func client(objects ...runtime.Object) *fake.Clientset {
	c := fake.NewClientset(objects...)
	c.PrependReactor("create", "*", func(a ktesting.Action) (bool, runtime.Object, error) {
		object := a.(ktesting.CreateAction).GetObject()
		meta := object.(metav1.Object)
		meta.SetUID(types.UID(uuid.NewString()))
		meta.SetResourceVersion("1")
		return false, nil, nil
	})
	return c
}
func bind(t *testing.T, c *fake.Clientset, in Run, p Profile, claim string) Volume {
	t.Helper()
	b, e := InspectVolume(context.Background(), c, in.Identity, p, claim, nil, nil)
	if e != nil {
		t.Fatal(e)
	}
	if e = ClaimVolume(context.Background(), c, in.Identity, p, b); e != nil {
		t.Fatal(e)
	}
	return b
}
func assertNoEffects(t *testing.T, c *fake.Clientset) {
	t.Helper()
	for _, a := range c.Actions() {
		if a.GetVerb() != "get" && a.GetVerb() != "list" {
			t.Fatalf("unexpected mutation: %s %s", a.GetVerb(), a.GetResource().Resource)
		}
	}
}

func TestProfileRejectsExecutableAndCredentialOverrides(t *testing.T) {
	_, p, _, _ := fixture()
	raw, _ := json.Marshal([]Profile{p})
	profiles, e := LoadProfiles(strings.NewReader(string(raw)))
	if e != nil || len(profiles) != 1 {
		t.Fatal(e)
	}
	for _, change := range []func(*Profile){func(p *Profile) { p.Image = "image:latest" }, func(p *Profile) { p.Env["COMFYUI_WORKER_TOKEN"] = "secret" },
		func(p *Profile) { p.Env["NVIDIA_VISIBLE_DEVICES"] = "all" }, func(p *Profile) { p.Bindings["SECOND"] = "instance_id" },
		func(p *Profile) { delete(p.Bindings, "COMFYUI_WORKER_TOKEN") }, func(p *Profile) { p.TemplateVersion = "blender-desktop-v1" },
		func(p *Profile) { p.Namespace = "default" }, func(p *Profile) { p.RunAsUser = 0 }, func(p *Profile) { p.CPULimitMillis = 1 }, func(p *Profile) { p.Claims = append(p.Claims, p.Claims[0]) }} {
		_, candidate, _, _ := fixture()
		change(&candidate)
		raw, _ = json.Marshal([]Profile{candidate})
		if _, e = LoadProfiles(strings.NewReader(string(raw))); e == nil {
			t.Fatal("unsafe profile accepted")
		}
	}
	raw, _ = json.Marshal([]Profile{p})
	for _, bad := range []string{strings.Replace(string(raw), `"port":8188`, `"port":8188,"port":8188`, 1),
		strings.Replace(string(raw), `"port":8188`, `"port":8188,"command":"sh"`, 1)} {
		if _, e = LoadProfiles(strings.NewReader(bad)); e == nil {
			t.Fatal("open profile shape accepted")
		}
	}
	original := p.Hash()
	p.Env["COMFYUI_DEVICE"] = "changed"
	if p.Hash() == original {
		t.Fatal("execution input omitted from profile hash")
	}
}

func TestVolumeInspectionRejectsForeignChangedAndOccupiedVolumes(t *testing.T) {
	ctx := context.Background()
	for _, change := range []func(*corev1.PersistentVolumeClaim, *corev1.PersistentVolume){
		func(p *corev1.PersistentVolumeClaim, _ *corev1.PersistentVolume) {
			p.Annotations = map[string]string{prefix + "instance-id": id()}
		},
		func(p *corev1.PersistentVolumeClaim, _ *corev1.PersistentVolume) {
			p.Annotations = map[string]string{prefix + "project-id": id()}
		},
		func(_ *corev1.PersistentVolumeClaim, p *corev1.PersistentVolume) {
			p.Spec.PersistentVolumeReclaimPolicy = corev1.PersistentVolumeReclaimDelete
		},
		func(_ *corev1.PersistentVolumeClaim, p *corev1.PersistentVolume) {
			p.Spec.ClaimRef.UID = types.UID(uuid.NewString())
		},
		func(_ *corev1.PersistentVolumeClaim, p *corev1.PersistentVolume) {
			p.Spec.NodeAffinity.Required.NodeSelectorTerms[0].MatchExpressions[0].Values = []string{"wrong-node"}
		},
		func(p *corev1.PersistentVolumeClaim, _ *corev1.PersistentVolume) {
			p.Status.Capacity[corev1.ResourceStorage] = resource.MustParse("1Mi")
		},
	} {
		in, p, pvc, pv := fixture()
		change(pvc, pv)
		c := client(pvc, pv)
		if _, e := InspectVolume(ctx, c, in.Identity, p, pvc.Name, nil, nil); e == nil {
			t.Fatal("unsafe volume accepted")
		}
		assertNoEffects(t, c)
	}
	in, p, pvc, pv := fixture()
	other := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "other", Namespace: p.Namespace, UID: types.UID(uuid.NewString())},
		Spec: corev1.PodSpec{Volumes: []corev1.Volume{{Name: "data", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: pvc.Name}}}}}}
	c := client(pvc, pv, other)
	if _, e := InspectVolume(ctx, c, in.Identity, p, pvc.Name, nil, nil); !errors.Is(e, ErrBusy) {
		t.Fatal(e)
	}
	assertNoEffects(t, c)
}

func TestEnsureRequiresClaimThenRecoversSameRunWithoutCredentialsInProjection(t *testing.T) {
	ctx := context.Background()
	in, p, pvc, pv := fixture()
	c := client(pvc, pv)
	b, e := InspectVolume(ctx, c, in.Identity, p, pvc.Name, nil, nil)
	if e != nil {
		t.Fatal(e)
	}
	c.ClearActions()
	if _, e = EnsureWorker(ctx, c, in, p, b, Observation{}, acceptObservation); !errors.Is(e, ErrBinding) {
		t.Fatal("unclaimed volume accepted", e)
	}
	assertNoEffects(t, c)
	if e = ClaimVolume(ctx, c, in.Identity, p, b); e != nil {
		t.Fatal(e)
	}
	first, e := EnsureWorker(ctx, c, in, p, b, Observation{}, acceptObservation)
	if e != nil {
		t.Fatal(e)
	}
	if first.PodReady || first.PodUID == "" || first.SecretUID == "" {
		t.Fatal("invalid pending observation")
	}
	c.ClearActions()
	again, e := EnsureWorker(ctx, c, in, p, b, first, acceptObservation)
	if e != nil || again != first {
		t.Fatal("same run changed", e)
	}
	assertNoEffects(t, c)
	raw, _ := json.Marshal(first)
	if strings.Contains(string(raw), "http") || strings.Contains(string(raw), "worker-token") {
		t.Fatal("private worker material exposed")
	}
	pod, _ := c.CoreV1().Pods(p.Namespace).Get(ctx, runName(in), metav1.GetOptions{})
	if *pod.Spec.AutomountServiceAccountToken || !*pod.Spec.Containers[0].SecurityContext.ReadOnlyRootFilesystem || len(pod.Spec.Containers[0].Resources.Limits) != 2 {
		t.Fatal("unsafe worker shape")
	}
	pod.Status.Phase = corev1.PodRunning
	pod.Spec.NodeName = p.NodeName
	pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
	c.CoreV1().Pods(p.Namespace).Update(ctx, pod, metav1.UpdateOptions{})
	ready, e := ObserveWorker(ctx, c, in, p, b, first)
	if e != nil || !ready.PodReady {
		t.Fatal("ready observation unavailable", e)
	}
}

func TestDifferentInstancesUseIndependentVolumesAndRuntimeIdentity(t *testing.T) {
	ctx := context.Background()
	in, p, pvc, pv := fixture()
	second := in
	second.InstanceID = id()
	second.OperationID = id()
	pvc2, pv2 := pvc.DeepCopy(), pv.DeepCopy()
	pvc2.Name = "slot-b"
	pvc2.UID = types.UID(uuid.NewString())
	pvc2.Spec.VolumeName = "pv-slot-b"
	pv2.Name = "pv-slot-b"
	pv2.UID = types.UID(uuid.NewString())
	pv2.Spec.ClaimRef.Name = pvc2.Name
	pv2.Spec.ClaimRef.UID = pvc2.UID
	c := client(pvc, pv, pvc2, pv2)
	one := bind(t, c, in, p, pvc.Name)
	two := bind(t, c, second, p, pvc2.Name)
	a, e := EnsureWorker(ctx, c, in, p, one, Observation{}, acceptObservation)
	if e != nil {
		t.Fatal(e)
	}
	b, e := EnsureWorker(ctx, c, second, p, two, Observation{}, acceptObservation)
	if e != nil {
		t.Fatal(e)
	}
	if a.PodUID == b.PodUID || a.ServiceUID == b.ServiceUID || a.SecretUID == b.SecretUID || a.Endpoint == b.Endpoint {
		t.Fatal("instances share execution identity")
	}
	c.ClearActions()
	if _, e = EnsureWorker(ctx, c, second, p, one, Observation{}, acceptObservation); e == nil {
		t.Fatal("cross-instance binding accepted")
	}
	assertNoEffects(t, c)
}

func TestTamperedPodOrReplacedUIDNeverAdoptedOrRecreated(t *testing.T) {
	ctx := context.Background()
	for _, change := range []func(*corev1.Pod){func(p *corev1.Pod) { p.UID = types.UID(uuid.NewString()) },
		func(p *corev1.Pod) { p.Spec.HostNetwork = true }, func(p *corev1.Pod) {
			p.Spec.Containers[0].Env = append(p.Spec.Containers[0].Env, corev1.EnvVar{Name: "EXTRA", Value: "bad"})
		},
		func(p *corev1.Pod) { p.Spec.Containers[0].Command = []string{"sh"} }, func(p *corev1.Pod) {
			p.Spec.Containers[0].Resources.Limits[corev1.ResourceName("nvidia.com/gpu")] = resource.MustParse("1")
		},
		func(p *corev1.Pod) { p.Annotations[prefix+"installation-id"] = id() }, func(p *corev1.Pod) { p.Spec.NodeName = "wrong-node" },
	} {
		in, p, pvc, pv := fixture()
		c := client(pvc, pv)
		b := bind(t, c, in, p, pvc.Name)
		saved, e := EnsureWorker(ctx, c, in, p, b, Observation{}, acceptObservation)
		if e != nil {
			t.Fatal(e)
		}
		pod, _ := c.CoreV1().Pods(p.Namespace).Get(ctx, runName(in), metav1.GetOptions{})
		change(pod)
		c.CoreV1().Pods(p.Namespace).Update(ctx, pod, metav1.UpdateOptions{})
		c.ClearActions()
		if _, e = EnsureWorker(ctx, c, in, p, b, saved, acceptObservation); !errors.Is(e, ErrBinding) {
			t.Fatal("tampered pod accepted", e)
		}
		assertNoEffects(t, c)
	}
	in, p, pvc, pv := fixture()
	c := client(pvc, pv)
	b := bind(t, c, in, p, pvc.Name)
	saved, e := EnsureWorker(ctx, c, in, p, b, Observation{}, acceptObservation)
	if e != nil {
		t.Fatal(e)
	}
	c.CoreV1().Pods(p.Namespace).Delete(ctx, runName(in), metav1.DeleteOptions{})
	c.ClearActions()
	if _, e = EnsureWorker(ctx, c, in, p, b, saved, acceptObservation); !errors.Is(e, ErrBinding) {
		t.Fatal("deleted pod recreated", e)
	}
	assertNoEffects(t, c)
}

func TestLostServiceCreateResponseRecoversWithoutNewSecret(t *testing.T) {
	ctx := context.Background()
	in, p, pvc, pv := fixture()
	c := client(pvc, pv)
	b := bind(t, c, in, p, pvc.Name)
	lost := true
	c.PrependReactor("create", "services", func(a ktesting.Action) (bool, runtime.Object, error) {
		if !lost {
			return false, nil, nil
		}
		lost = false
		svc := a.(ktesting.CreateAction).GetObject().(*corev1.Service).DeepCopy()
		svc.UID = types.UID(uuid.NewString())
		svc.ResourceVersion = "1"
		if e := c.Tracker().Add(svc); e != nil {
			t.Fatal(e)
		}
		return true, nil, errors.New("response lost")
	})
	if _, e := EnsureWorker(ctx, c, in, p, b, Observation{}, acceptObservation); e == nil {
		t.Fatal("missing simulated transport failure")
	}
	secret, _ := c.CoreV1().Secrets(p.Namespace).Get(ctx, instanceName(in.Identity), metav1.GetOptions{})
	recovered, e := EnsureWorker(ctx, c, in, p, b, Observation{}, acceptObservation)
	if e != nil || recovered.SecretUID != string(secret.UID) {
		t.Fatal("failed recovery", e)
	}
	pods, _ := c.CoreV1().Pods(p.Namespace).List(ctx, metav1.ListOptions{})
	if len(pods.Items) != 1 {
		t.Fatal("duplicate worker")
	}
}

func TestServiceCredentialAndProfileDriftAreRejected(t *testing.T) {
	ctx := context.Background()
	in, p, pvc, pv := fixture()
	c := client(pvc, pv)
	b := bind(t, c, in, p, pvc.Name)
	saved, e := EnsureWorker(ctx, c, in, p, b, Observation{}, acceptObservation)
	if e != nil {
		t.Fatal(e)
	}
	svc, _ := c.CoreV1().Services(p.Namespace).Get(ctx, runName(in), metav1.GetOptions{})
	svc.Spec.Type = corev1.ServiceTypeNodePort
	c.CoreV1().Services(p.Namespace).Update(ctx, svc, metav1.UpdateOptions{})
	c.ClearActions()
	if _, e = EnsureWorker(ctx, c, in, p, b, saved, acceptObservation); !errors.Is(e, ErrBinding) {
		t.Fatal(e)
	}
	assertNoEffects(t, c)
	svc.Spec.Type = corev1.ServiceTypeClusterIP
	c.CoreV1().Services(p.Namespace).Update(ctx, svc, metav1.UpdateOptions{})
	secret, _ := c.CoreV1().Secrets(p.Namespace).Get(ctx, instanceName(in.Identity), metav1.GetOptions{})
	no := false
	secret.Immutable = &no
	c.CoreV1().Secrets(p.Namespace).Update(ctx, secret, metav1.UpdateOptions{})
	c.ClearActions()
	if _, e = ObserveWorker(ctx, c, in, p, b, saved); !errors.Is(e, ErrBinding) {
		t.Fatal("mutable credential accepted", e)
	}
	assertNoEffects(t, c)
	p.Image = "registry.example.test/app:comfyui-worker-cpu-v0.1.3"
	if _, _, e = WorkerObjects(in, p, b); e == nil {
		t.Fatal("new profile reused old volume binding")
	}
}
