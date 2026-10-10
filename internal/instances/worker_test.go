package instances

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
)

func computeNode(name string) *corev1.Node {
	return &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name, UID: types.UID(uuid.NewString()), Labels: map[string]string{corev1.LabelHostname: name, "nvidia.com/gpu.sharing-strategy": "none"}}, Status: corev1.NodeStatus{
		Allocatable: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("16"), corev1.ResourceMemory: resource.MustParse("32Gi"), gpuResource: resource.MustParse("1"), corev1.ResourcePods: resource.MustParse("100")},
		Conditions:  []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}},
	}}
}

func TestComputeAdmissionProtectsActiveAndPendingWorkloads(t *testing.T) {
	ctx := context.Background()
	for _, phase := range []corev1.PodPhase{corev1.PodRunning, corev1.PodPending} {
		t.Run(string(phase), func(t *testing.T) {
			node := computeNode("worker-test")
			pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "existing-video", Namespace: "video"}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "video", Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{gpuResource: resource.MustParse("1")}}}}}, Status: corev1.PodStatus{Phase: phase}}
			if phase == corev1.PodRunning {
				pod.Spec.NodeName = node.Name
			}
			client := fake.NewClientset(node, pod)
			if err := AdmitWorker(ctx, client, node.Name); !errors.Is(err, ErrComputeCapacity) {
				t.Fatal("occupied GPU admitted", err)
			}
			for _, a := range client.Actions() {
				if a.GetVerb() != "get" && a.GetVerb() != "list" {
					t.Fatal("admission mutated workloads")
				}
			}
			pod.Status.Phase = corev1.PodSucceeded
			_, _ = client.CoreV1().Pods(pod.Namespace).Update(ctx, pod, metav1.UpdateOptions{})
			if err := AdmitWorker(ctx, client, node.Name); err != nil {
				t.Fatal("terminal demand counted", err)
			}
		})
	}
}

func TestComputeAdmissionRequiresHealthyExclusiveNodeAndKnownCapacity(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*corev1.Node)
		want   error
	}{
		{"missing-sharing-evidence", func(n *corev1.Node) { delete(n.Labels, "nvidia.com/gpu.sharing-strategy") }, ErrComputeUnknown},
		{"time-slicing", func(n *corev1.Node) { n.Labels["nvidia.com/gpu.sharing-strategy"] = "time-slicing" }, ErrComputeUnknown},
		{"replicas", func(n *corev1.Node) { n.Labels["nvidia.com/gpu.replicas"] = "2" }, ErrComputeUnknown},
		{"cpu", func(n *corev1.Node) { n.Status.Allocatable[corev1.ResourceCPU] = resource.MustParse("1999m") }, ErrComputeCapacity},
		{"memory", func(n *corev1.Node) { n.Status.Allocatable[corev1.ResourceMemory] = resource.MustParse("3Gi") }, ErrComputeCapacity},
		{"zero-gpu", func(n *corev1.Node) { n.Status.Allocatable[gpuResource] = resource.MustParse("0") }, ErrComputeCapacity},
		{"unknown", func(n *corev1.Node) { delete(n.Status.Allocatable, corev1.ResourceMemory) }, ErrComputeUnknown},
		{"cordon", func(n *corev1.Node) { n.Spec.Unschedulable = true }, ErrComputeCapacity},
		{"not-ready", func(n *corev1.Node) { n.Status.Conditions[0].Status = corev1.ConditionUnknown }, ErrComputeCapacity},
		{"taint", func(n *corev1.Node) {
			n.Spec.Taints = []corev1.Taint{{Key: "reserved", Effect: corev1.TaintEffectNoSchedule}}
		}, ErrComputeCapacity},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			n := computeNode("worker-test")
			test.mutate(n)
			if err := AdmitWorker(context.Background(), fake.NewClientset(n), n.Name); !errors.Is(err, test.want) {
				t.Fatal(err)
			}
		})
	}
}

func TestComputeRequestsIncludeSidecarsInitOverheadAndResize(t *testing.T) {
	always := corev1.ContainerRestartPolicyAlways
	requests := func(cpu string) corev1.ResourceRequirements {
		return corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(cpu)}}
	}
	p := &corev1.Pod{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Resources: requests("2")}}, InitContainers: []corev1.Container{
		{Name: "sidecar", RestartPolicy: &always, Resources: requests("1")}, {Name: "init", Resources: requests("5")},
	}, Overhead: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m")}}}
	q := podRequests(p)[corev1.ResourceCPU]
	if q.Cmp(resource.MustParse("6100m")) != 0 {
		t.Fatal(q.String())
	}
	p.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "main", AllocatedResources: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("8")}}}
	q = podRequests(p)[corev1.ResourceCPU]
	if q.Cmp(resource.MustParse("9100m")) != 0 {
		t.Fatal(q.String())
	}
	p.Spec.Resources = &corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("12")}}
	q = podRequests(p)[corev1.ResourceCPU]
	if q.Cmp(resource.MustParse("12100m")) != 0 {
		t.Fatal(q.String())
	}
}

func workerFixture(t *testing.T) (Identity, HelperProfile, VolumeBinding, *fake.Clientset, string) {
	t.Helper()
	in, p, b, client := helperFixture(t)
	ctx := context.Background()
	if err := client.CoreV1().Secrets(p.Namespace).Delete(ctx, instanceName(in), metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	secret, err := credential(ctx, client, in, p.Namespace, "", true)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = client.CoreV1().Nodes().Create(ctx, computeNode(p.NodeName), metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	return in, p, b, client, string(secret.UID)
}

func TestWorkerCreationReplayAndPodReadinessAreDistinctFromRunning(t *testing.T) {
	in, p, b, client, secretUID := workerFixture(t)
	ctx := context.Background()
	start := uuid.NewString()
	obs, err := EnsureWorker(ctx, client, in, p, b, start, secretUID, WorkerSource{SourceRevisionID: in.CreateOperationID, Empty: true}, WorkerObservation{})
	if err != nil || obs.PodUID == "" || obs.PodReady {
		t.Fatal(obs, err)
	}
	pod, _ := client.CoreV1().Pods(p.Namespace).Get(ctx, obs.PodName, metav1.GetOptions{})
	if pod.Spec.NodeName != "" || pod.Spec.PreemptionPolicy == nil || *pod.Spec.PreemptionPolicy != corev1.PreemptNever || pod.Spec.RestartPolicy != corev1.RestartPolicyNever {
		t.Fatal("worker bypassed scheduler or enabled preemption/restart")
	}
	if pod.Spec.AutomountServiceAccountToken == nil || *pod.Spec.AutomountServiceAccountToken || pod.Spec.Containers[0].Env[0].Value != in.InstanceID {
		t.Fatal("invalid worker isolation")
	}
	replay, err := EnsureWorker(ctx, client, in, p, b, start, secretUID, WorkerSource{SourceRevisionID: in.CreateOperationID, Empty: true}, WorkerObservation{})
	if err != nil || replay != obs {
		t.Fatal("unknown outcome created different binding", replay, err)
	}
	pod.Spec.NodeName = p.NodeName
	pod.Status.Phase = corev1.PodRunning
	pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
	_, _ = client.CoreV1().Pods(p.Namespace).Update(ctx, pod, metav1.UpdateOptions{})
	ready, err := EnsureWorker(ctx, client, in, p, b, start, secretUID, WorkerSource{SourceRevisionID: in.CreateOperationID, Empty: true}, obs)
	if err != nil || !ready.PodReady {
		t.Fatal(ready, err)
	}
	if err = client.CoreV1().Pods(p.Namespace).Delete(ctx, obs.PodName, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err = EnsureWorker(ctx, client, in, p, b, start, secretUID, WorkerSource{SourceRevisionID: in.CreateOperationID, Empty: true}, obs); !errors.Is(err, ErrBinding) {
		t.Fatal("observed missing Pod recreated", err)
	}
}

func TestWorkerRefusesCapacityHelperAndIdentityConflictsBeforeEffects(t *testing.T) {
	for _, kind := range []string{"capacity", "helper", "secret", "retained"} {
		t.Run(kind, func(t *testing.T) {
			in, p, b, client, secretUID := workerFixture(t)
			ctx := context.Background()
			switch kind {
			case "capacity":
				n, _ := client.CoreV1().Nodes().Get(ctx, p.NodeName, metav1.GetOptions{})
				n.Status.Allocatable[gpuResource] = resource.MustParse("0")
				_, _ = client.CoreV1().Nodes().Update(ctx, n, metav1.UpdateOptions{})
			case "helper":
				if _, err := EnsureHelper(ctx, client, in, p, b, HelperObservation{}); err != nil {
					t.Fatal(err)
				}
			case "secret":
				secretUID = uuid.NewString()
			case "retained":
				b.Retained = true
			}
			before := len(client.Actions())
			if _, err := EnsureWorker(ctx, client, in, p, b, uuid.NewString(), secretUID, WorkerSource{SourceRevisionID: in.CreateOperationID, Empty: true}, WorkerObservation{}); err == nil {
				t.Fatal("unsafe creation accepted")
			}
			for _, a := range client.Actions()[before:] {
				if a.GetVerb() != "get" && a.GetVerb() != "list" {
					t.Fatal("effect before validation", a.GetVerb())
				}
			}
		})
	}
}

func TestWorkerRejectsForeignOrModifiedWorkload(t *testing.T) {
	for _, mutate := range []func(*corev1.Pod){
		func(p *corev1.Pod) { p.UID = types.UID(uuid.NewString()) },
		func(p *corev1.Pod) { p.Annotations[startKey] = uuid.NewString() },
		func(p *corev1.Pod) { p.Spec.Containers[0].Command = []string{"sh"} },
		func(p *corev1.Pod) { p.Spec.Containers[0].Resources.Requests[gpuResource] = resource.MustParse("2") },
		func(p *corev1.Pod) { p.Spec.NodeName = "foreign-node" },
		func(p *corev1.Pod) { p.Spec.PreemptionPolicy = nil },
		func(p *corev1.Pod) { p.Spec.Tolerations = []corev1.Toleration{{Operator: corev1.TolerationOpExists}} },
		func(p *corev1.Pod) {
			p.Spec.Resources = &corev1.ResourceRequirements{Requests: workerResources().Requests}
		},
	} {
		in, p, b, client, secretUID := workerFixture(t)
		ctx := context.Background()
		start := uuid.NewString()
		obs, err := EnsureWorker(ctx, client, in, p, b, start, secretUID, WorkerSource{SourceRevisionID: in.CreateOperationID, Empty: true}, WorkerObservation{})
		if err != nil {
			t.Fatal(err)
		}
		pod, _ := client.CoreV1().Pods(p.Namespace).Get(ctx, obs.PodName, metav1.GetOptions{})
		mutate(pod)
		_, _ = client.CoreV1().Pods(p.Namespace).Update(ctx, pod, metav1.UpdateOptions{})
		if _, err = EnsureWorker(ctx, client, in, p, b, start, secretUID, WorkerSource{SourceRevisionID: in.CreateOperationID, Empty: true}, obs); !errors.Is(err, ErrBinding) {
			t.Fatal("modified worker accepted", err)
		}
	}
}
