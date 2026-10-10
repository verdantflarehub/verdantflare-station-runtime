package instances

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

func destroyFixture(t *testing.T, stopped bool) (*Service, DestroyRequest, StartRequest, *fake.Clientset) {
	t.Helper()
	ctx := context.Background()
	var s *Service
	var start StartRequest
	var client *fake.Clientset
	var previous string
	if stopped {
		var stop StopRequest
		s, stop, start, client, _ = stopFixture(t)
		for range 4 {
			if _, err := s.Stop(ctx, stop); err != nil {
				t.Fatal(err)
			}
		}
		previous = stop.OperationID
	} else {
		s, start, client, _ = startFixture(t)
		previous = start.CreateOperationID
	}
	client.ClearActions()
	return s, DestroyRequest{uuid.NewString(), previous, start.StationID, start.OrganizationID, start.UserID, start.ProjectID, start.InstanceID}, start, client
}

func TestDestroyRevokesCredentialAndRetainsReservationAcrossRestart(t *testing.T) {
	for _, stopped := range []bool{false, true} {
		t.Run(map[bool]string{false: "never_started", true: "saved_stop"}[stopped], func(t *testing.T) {
			s, in, start, client := destroyFixture(t, stopped)
			ctx := context.Background()
			poll := func(phase string) DestroyProgress {
				t.Helper()
				s = &Service{Pool: s.Pool, Client: client, StationID: s.StationID, Profiles: s.Profiles}
				out, err := s.Destroy(ctx, in)
				if err != nil || out.Phase != phase {
					t.Fatal(out, err)
				}
				return out
			}
			poll("accepted")
			if len(client.Actions()) != 0 {
				t.Fatal("side effect before acceptance")
			}
			poll("revoking")
			lost := true
			client.PrependReactor("delete", "secrets", func(action ktesting.Action) (bool, runtime.Object, error) {
				a := action.(ktesting.DeleteAction)
				p := a.GetDeleteOptions().Preconditions
				if p == nil || p.UID == nil || *p.UID == "" || p.ResourceVersion == nil {
					t.Fatal("unscoped credential revocation")
				}
				if lost {
					lost = false
					_ = client.Tracker().Delete(corev1.SchemeGroupVersion.WithResource("secrets"), a.GetNamespace(), a.GetName())
					return true, nil, errors.New("response lost")
				}
				return false, nil, nil
			})
			if _, err := s.Destroy(ctx, in); err == nil {
				t.Fatal("lost response hidden")
			}
			out := poll("deleted")
			if out.Status != "succeeded" || !out.Workspace.Retained {
				t.Fatal(out)
			}
			for _, a := range client.Actions() {
				if a.GetVerb() == "delete" && a.GetResource().Resource != "secrets" {
					t.Fatal("destroy deleted retained resources")
				}
			}
			client.ClearActions()
			poll("deleted")
			if len(client.Actions()) != 0 {
				t.Fatal("completed destroy replay mutated Kubernetes")
			}
			if _, err := s.Start(ctx, start); err == nil {
				t.Fatal("destroyed instance restarted")
			}
			b, err := (&WorkspaceStore{s.Pool, client}).Lookup(ctx, start.identity(), s.Profiles[start.ProfileID].StorageProfile)
			if err != nil || !b.Retained || b.ReservedBytes <= 0 {
				t.Fatal("retained capacity lost", b, err)
			}
			create := start.identity()
			create.InstanceID = uuid.NewString()
			create.CreateOperationID = uuid.NewString()
			if _, err := (&WorkspaceStore{s.Pool, client}).Reserve(ctx, create, s.Profiles[start.ProfileID].StorageProfile); !errors.Is(err, ErrCapacity) {
				t.Fatal("retained storage reused", err)
			}
		})
	}
}

func TestDestroyRejectsRunningReplacedCredentialsAndLateMounts(t *testing.T) {
	ctx := context.Background()
	s, _, start, _ := accessFixture(t)
	in := DestroyRequest{uuid.NewString(), start.OperationID, start.StationID, start.OrganizationID, start.UserID, start.ProjectID, start.InstanceID}
	if _, err := s.Destroy(ctx, in); !errors.Is(err, ErrConflict) {
		t.Fatal("running worker destroyed", err)
	}
	for _, kind := range []string{"secret", "mounted", "service", "identity"} {
		t.Run(kind, func(t *testing.T) {
			s, in, start, client := destroyFixture(t, true)
			ns := s.Profiles[start.ProfileID].Namespace
			if _, err := s.Destroy(ctx, in); err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "secret":
				secret, _ := client.CoreV1().Secrets(ns).Get(ctx, instanceName(start.identity()), metav1.GetOptions{})
				secret.UID = types.UID(uuid.NewString())
				_, _ = client.CoreV1().Secrets(ns).Update(ctx, secret, metav1.UpdateOptions{})
			case "mounted":
				_, _ = client.CoreV1().Pods(ns).Create(ctx, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "foreign", UID: types.UID(uuid.NewString())}, Spec: corev1.PodSpec{Volumes: []corev1.Volume{{Name: "workspace", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: s.Profiles[start.ProfileID].Claims[0]}}}}}}, metav1.CreateOptions{})
			case "service":
				_, _ = client.CoreV1().Services(ns).Create(ctx, &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "late-entry", Annotations: map[string]string{instanceKey: in.InstanceID}}}, metav1.CreateOptions{})
			case "identity":
				in.UserID = uuid.NewString()
			}
			if _, err := s.Destroy(ctx, in); err == nil {
				t.Fatal("unverified destroy accepted")
			}
			for _, a := range client.Actions() {
				if a.GetVerb() == "delete" {
					t.Fatal("destructive side effect despite mismatch")
				}
			}
		})
	}
}
