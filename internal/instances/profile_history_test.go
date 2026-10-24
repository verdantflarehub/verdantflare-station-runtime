package instances

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"github.com/google/uuid"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestProfileHistoryRequiresExactHashAndID(t *testing.T) {
	entry := profileConfig{ID: "blender-standard", Namespace: "blender-test", Pool: "blender", NodeName: "worker-test", Claims: []string{"workspace-1"}, StorageBytes: 50 << 30, Image: "registry.example.test/blender-worker:v0.1.7", MaxFileBytes: 1 << 30}
	next := entry
	next.Image = "registry.example.test/blender-worker:v0.1.8"
	raw, _ := json.Marshal([]profileConfig{entry, next})
	path := filepath.Join(t.TempDir(), "profiles.json")
	os.WriteFile(path, raw, 0600)
	if _, err := LoadProfiles(path); err == nil {
		t.Fatal("ambiguous current profiles accepted")
	}
	history, err := LoadProfileHistory(path)
	if err != nil || len(history) != 2 {
		t.Fatal(err)
	}
	s := Service{ProfileHistory: history}
	for _, p := range history {
		if got, ok := s.pinnedProfile(p.ID, p.Hash[:]); !ok || got.Image != p.Image {
			t.Fatal("pinned image lost")
		}
		if _, ok := s.pinnedProfile("other", p.Hash[:]); ok {
			t.Fatal("foreign ID accepted")
		}
	}
	if _, ok := s.pinnedProfile(entry.ID, make([]byte, 32)); ok {
		t.Fatal("unknown hash accepted")
	}
	if _, ok := s.pinnedProfile(entry.ID, []byte{1}); ok {
		t.Fatal("short hash accepted")
	}
	raw, _ = json.Marshal([]profileConfig{entry, entry})
	os.WriteFile(path, raw, 0600)
	if _, err := LoadProfileHistory(path); err == nil {
		t.Fatal("duplicate historical hash accepted")
	}
}

func TestHistoricalProfilePreservesAccessAndTerminalRecovery(t *testing.T) {
	s, access, start, client := accessFixture(t)
	ctx := context.Background()
	old := s.Profiles[start.ProfileID]
	newer := old
	newer.Image = "registry.example.test/blender-worker:v0.9.0"
	newer.Hash[0] ^= 255
	s.Profiles[start.ProfileID] = newer
	if _, err := s.Access(ctx, access); err == nil {
		t.Fatal("profile drift silently accepted")
	}
	s.ProfileHistory = map[string]HelperProfile{hex.EncodeToString(old.Hash[:]): old}
	if _, err := s.Access(ctx, access); err != nil {
		t.Fatal("historical access", err)
	}
	name := "blender-run-" + strings.ReplaceAll(start.OperationID, "-", "")
	pod, err := client.CoreV1().Pods(old.Namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	pod.Status.Phase = corev1.PodSucceeded
	client.CoreV1().Pods(old.Namespace).Update(ctx, pod, metav1.UpdateOptions{})
	next := start
	next.OperationID = uuid.NewString()
	for i := 0; i < 6; i++ {
		out, err := s.Start(ctx, next)
		if err != nil {
			t.Fatal("recover", err)
		}
		if out.Worker != nil {
			worker, err := client.CoreV1().Pods(old.Namespace).Get(ctx, out.Worker.PodName, metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if worker.Spec.Containers[0].Image != old.Image {
				t.Fatal("worker upgraded during recovery")
			}
			worker.Spec.NodeName = old.NodeName
			worker.Status.Phase = corev1.PodRunning
			worker.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
			client.CoreV1().Pods(old.Namespace).Update(ctx, worker, metav1.UpdateOptions{})
		}
		if out.Status == "succeeded" {
			access.StartOperationID = next.OperationID
			if _, err = s.Access(ctx, access); err != nil {
				t.Fatal("recovered access", err)
			}
			if s.Profiles[start.ProfileID].Image != newer.Image {
				t.Fatal("current profile overwritten")
			}
			return
		}
	}
	t.Fatal("recovery did not complete")
}
