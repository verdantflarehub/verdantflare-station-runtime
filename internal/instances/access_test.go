package instances

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
)

func accessFixture(t *testing.T) (*Service, AccessRequest, StartRequest, *fake.Clientset) {
	t.Helper()
	s, in, client, observer := startFixture(t)
	ctx := context.Background()
	for range 2 {
		if _, err := s.Start(ctx, in); err != nil {
			t.Fatal(err)
		}
	}
	name := "blender-run-" + strings.ReplaceAll(in.OperationID, "-", "")
	pod, err := client.CoreV1().Pods(s.Profiles[in.ProfileID].Namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	pod.Spec.NodeName = s.Profiles[in.ProfileID].NodeName
	pod.Status.Phase = corev1.PodRunning
	pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
	if _, err = client.CoreV1().Pods(pod.Namespace).Update(ctx, pod, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	observer.ready = true
	out, err := s.Start(ctx, in)
	if err != nil || out.Status != "succeeded" {
		t.Fatal(out, err)
	}
	return s, AccessRequest{in.StationID, in.OrganizationID, in.UserID, in.ProjectID, in.InstanceID, in.OperationID, "worker"}, in, client
}

func TestAccessReturnsOnlyRequestedCredentialWithoutMutations(t *testing.T) {
	s, in, start, client := accessFixture(t)
	ctx := context.Background()
	secret, err := client.CoreV1().Secrets(s.Profiles[start.ProfileID].Namespace).Get(ctx, instanceName(start.identity()), metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	client.ClearActions()
	for _, kind := range []string{"worker", "gui"} {
		in.Kind = kind
		out, err := s.Access(ctx, in)
		key := "worker-token"
		if kind == "gui" {
			key = "gui-password"
		}
		if err != nil || out.Credential != string(secret.Data[key]) || out.Kind != kind || out.StartOperationID != start.OperationID {
			t.Fatal("scoped access failed", err)
		}
		raw, _ := json.Marshal(out)
		other := "worker-token"
		if kind == "worker" {
			other = "gui-password"
		}
		if strings.Contains(string(raw), string(secret.Data[other])) {
			t.Fatal("other credential exposed")
		}
	}
	for _, action := range client.Actions() {
		if action.GetVerb() != "get" && action.GetVerb() != "list" {
			t.Fatal("access performed a Kubernetes mutation", action.GetVerb())
		}
	}
	var state string
	if err = s.Pool.QueryRow(ctx, `SELECT input::text||state::text FROM station.runtime_instance_executions WHERE operation_id=$1`, start.OperationID).Scan(&state); err != nil {
		t.Fatal(err)
	}
	for _, token := range secret.Data {
		if strings.Contains(state, string(token)) {
			t.Fatal("credential persisted")
		}
	}
}

func TestAccessRejectsForeignActorScopeAndObsoleteExecution(t *testing.T) {
	s, in, _, _ := accessFixture(t)
	ctx := context.Background()
	for _, field := range []*string{&in.StationID, &in.OrganizationID, &in.ProjectID, &in.InstanceID, &in.StartOperationID} {
		original := *field
		*field = uuid.NewString()
		if out, err := s.Access(ctx, in); err == nil || out.Credential != "" {
			t.Fatal("foreign binding accepted")
		}
		*field = original
	}
	// The Core checks membership; the Runtime does not equate editor identity with
	// the original starting manager, so authorized read-only editors can observe.
	in.UserID = uuid.NewString()
	if _, err := s.Access(ctx, in); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Pool.Exec(ctx, `UPDATE station.runtime_instance_executions SET phase='loading',status='running' WHERE operation_id=$1`, in.StartOperationID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Access(ctx, in); !errors.Is(err, ErrConflict) {
		t.Fatal("uncompleted execution accepted", err)
	}
}

func TestAccessRejectsReplacedPodServiceSecretAndClaim(t *testing.T) {
	for _, resource := range []string{"pod", "service", "secret", "claim"} {
		t.Run(resource, func(t *testing.T) {
			s, in, start, client := accessFixture(t)
			ctx := context.Background()
			ns := s.Profiles[start.ProfileID].Namespace
			name := "blender-run-" + strings.ReplaceAll(in.StartOperationID, "-", "")
			switch resource {
			case "pod":
				p, _ := client.CoreV1().Pods(ns).Get(ctx, name, metav1.GetOptions{})
				p.UID = types.UID(uuid.NewString())
				_, _ = client.CoreV1().Pods(ns).Update(ctx, p, metav1.UpdateOptions{})
			case "service":
				p, _ := client.CoreV1().Services(ns).Get(ctx, name, metav1.GetOptions{})
				p.UID = types.UID(uuid.NewString())
				_, _ = client.CoreV1().Services(ns).Update(ctx, p, metav1.UpdateOptions{})
			case "secret":
				p, _ := client.CoreV1().Secrets(ns).Get(ctx, instanceName(start.identity()), metav1.GetOptions{})
				p.UID = types.UID(uuid.NewString())
				_, _ = client.CoreV1().Secrets(ns).Update(ctx, p, metav1.UpdateOptions{})
			case "claim":
				p, _ := client.CoreV1().PersistentVolumeClaims(ns).Get(ctx, s.Profiles[start.ProfileID].Claims[0], metav1.GetOptions{})
				p.Annotations[instanceKey] = uuid.NewString()
				_, _ = client.CoreV1().PersistentVolumeClaims(ns).Update(ctx, p, metav1.UpdateOptions{})
			}
			if out, err := s.Access(ctx, in); err == nil || out.Credential != "" {
				t.Fatal("replaced " + resource + " accepted")
			}
		})
	}
}
