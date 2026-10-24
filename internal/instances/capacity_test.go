package instances

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/google/uuid"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
)

func writeCapacity(t *testing.T, entries any) string {
	t.Helper()
	raw, e := json.Marshal(entries)
	if e != nil {
		t.Fatal(e)
	}
	path := filepath.Join(t.TempDir(), "capacity.json")
	if e = os.WriteFile(path, raw, 0600); e != nil {
		t.Fatal(e)
	}
	return path
}

func TestCapacityPolicyPinsProfileAndPreservesExecution(t *testing.T) {
	_, base, _, _ := fixture()
	old := HelperProfile{StorageProfile: base, Image: "registry.example.test/worker:v0.1.7", MaxFileBytes: 1 << 30}
	newer := old
	newer.Hash[0] = 2
	newer.Image = "registry.example.test/worker:v0.1.8"
	hash := hex.EncodeToString(old.Hash[:])
	s := Service{Profiles: map[string]HelperProfile{old.ID: newer}, ProfileHistory: map[string]HelperProfile{hash: old}}
	var e error
	s.CapacityClaims, e = LoadCapacityClaims(writeCapacity(t, []capacityEntry{{old.ID, hash, []string{"workspace-2"}}}), s.Profiles, s.ProfileHistory)
	if e != nil {
		t.Fatal(e)
	}
	got, ok := s.pinnedProfile(old.ID, old.Hash[:])
	if !ok || got.Hash != old.Hash || got.Image != old.Image || got.MaxFileBytes != old.MaxFileBytes || got.NodeName != old.NodeName || got.Pool != old.Pool || got.Bytes != old.Bytes || !reflect.DeepEqual(got.Claims, []string{"workspace-1", "workspace-2"}) {
		t.Fatal("pinned execution changed", got)
	}
	got.Claims[0] = "mutated"
	again, _ := s.pinnedProfile(old.ID, old.Hash[:])
	if again.Claims[0] != "workspace-1" || !reflect.DeepEqual(s.ProfileHistory[hash].Claims, old.Claims) {
		t.Fatal("shared profile mutated")
	}
	got, _ = s.pinnedProfile(newer.ID, newer.Hash[:])
	if !reflect.DeepEqual(got.Claims, newer.Claims) {
		t.Fatal("extension leaked to new hash")
	}
	if _, ok = s.pinnedProfile("other", old.Hash[:]); ok {
		t.Fatal("foreign profile accepted")
	}
	if _, ok = s.pinnedProfile(old.ID, make([]byte, 32)); ok {
		t.Fatal("unknown hash accepted")
	}
}

func TestCapacityPolicyRejectsAmbiguousOrExistingClaims(t *testing.T) {
	_, base, _, _ := fixture()
	p := HelperProfile{StorageProfile: base}
	h := hex.EncodeToString(p.Hash[:])
	profiles := map[string]HelperProfile{p.ID: p}
	cases := map[string][]capacityEntry{
		"empty": {}, "unknown-hash": {{p.ID, hex.EncodeToString(make([]byte, 32)), []string{"workspace-2"}}},
		"short-hash": {{p.ID, "01", []string{"workspace-2"}}}, "wrong-id": {{"other", h, []string{"workspace-2"}}},
		"old-claim": {{p.ID, h, []string{"workspace-1"}}}, "duplicate-claim": {{p.ID, h, []string{"workspace-2", "workspace-2"}}},
		"empty-claims": {{p.ID, h, nil}}, "invalid-claim": {{p.ID, h, []string{"../other"}}},
		"duplicate-profile": {{p.ID, h, []string{"workspace-2"}}, {p.ID, h, []string{"workspace-3"}}},
	}
	for name, entries := range cases {
		t.Run(name, func(t *testing.T) {
			if _, e := LoadCapacityClaims(writeCapacity(t, entries), profiles, nil); !errors.Is(e, ErrInvalid) {
				t.Fatal("accepted invalid policy", e)
			}
		})
	}
	for _, raw := range []string{`[{"profile_id":"x","profile_hash":"x","claims":[],"worker_image":"changed"}]`, `[] {}`} {
		path := filepath.Join(t.TempDir(), "bad.json")
		os.WriteFile(path, []byte(raw), 0600)
		if _, e := LoadCapacityClaims(path, profiles, nil); !errors.Is(e, ErrInvalid) {
			t.Fatal("invalid document accepted", e)
		}
	}
	other := p
	other.ID = "other"
	other.Hash[0] = 2
	other.Claims = []string{"workspace-2"}
	profiles[other.ID] = other
	if _, e := LoadCapacityClaims(writeCapacity(t, []capacityEntry{{p.ID, h, []string{"workspace-2"}}}), profiles, nil); !errors.Is(e, ErrInvalid) {
		t.Fatal("foreign profile claim accepted", e)
	}
}

func TestCapacityExtensionRetainsVolumeOwnershipChecks(t *testing.T) {
	in, base, pvc, pv := fixture()
	p := HelperProfile{StorageProfile: base}
	h := hex.EncodeToString(p.Hash[:])
	s := Service{Profiles: map[string]HelperProfile{p.ID: p}, CapacityClaims: CapacityClaims{h: {"workspace-2"}}}
	pvc.Name = "workspace-2"
	pv.Spec.ClaimRef.Name = pvc.Name
	client := fake.NewClientset(pvc, pv)
	expanded, _ := s.pinnedProfile(p.ID, p.Hash[:])
	ctx := context.Background()
	b, e := Inspect(ctx, client, in, expanded.StorageProfile, pvc.Name, nil, nil)
	if e != nil || !bytes.Equal(b.ProfileHash, p.Hash[:]) {
		t.Fatal(b, e)
	}
	pvc.Annotations = map[string]string{instanceKey: uuid.NewString()}
	client = fake.NewClientset(pvc, pv)
	if _, e = Inspect(ctx, client, in, expanded.StorageProfile, pvc.Name, nil, nil); !errors.Is(e, ErrBinding) {
		t.Fatal("occupied capacity accepted", e)
	}
	pvc.Annotations = nil
	pv.Labels[poolKey] = "foreign"
	client = fake.NewClientset(pvc, pv)
	if _, e = Inspect(ctx, client, in, expanded.StorageProfile, pvc.Name, nil, nil); !errors.Is(e, ErrBinding) {
		t.Fatal("foreign pool accepted", e)
	}
}

func TestAcceptedCreateResumesWithAdditionalCapacity(t *testing.T) {
	s, in, client, _ := createFixture(t)
	ctx := context.Background()
	p := s.Profiles[in.ProfileID]
	original, e := client.CoreV1().PersistentVolumeClaims(p.Namespace).Get(ctx, p.Claims[0], metav1.GetOptions{})
	if e != nil {
		t.Fatal(e)
	}
	original.Annotations = map[string]string{instanceKey: uuid.NewString()}
	if _, e = client.CoreV1().PersistentVolumeClaims(p.Namespace).Update(ctx, original, metav1.UpdateOptions{}); e != nil {
		t.Fatal(e)
	}
	if out, e := s.Create(ctx, in); e != nil || out.Phase != "accepted" {
		t.Fatal(out, e)
	}
	if _, e := s.Create(ctx, in); e == nil || e.Error() != "STORAGE_CAPACITY_UNAVAILABLE" {
		t.Fatal("expected capacity refusal", e)
	}
	var input, requestHash, profileHash []byte
	if e = s.Pool.QueryRow(ctx, `SELECT input,request_hash,profile_hash FROM station.runtime_instance_executions WHERE operation_id=$1`, in.OperationID).Scan(&input, &requestHash, &profileHash); e != nil {
		t.Fatal(e)
	}
	pvc := original.DeepCopy()
	pvc.Name = "workspace-2"
	pvc.UID = types.UID(uuid.NewString())
	pvc.Spec.VolumeName = "workspace-2"
	pvc.Annotations = nil
	pv, e := client.CoreV1().PersistentVolumes().Get(ctx, original.Spec.VolumeName, metav1.GetOptions{})
	if e != nil {
		t.Fatal(e)
	}
	pv = pv.DeepCopy()
	pv.Name = "workspace-2"
	pv.UID = types.UID(uuid.NewString())
	pv.Spec.ClaimRef.Name = pvc.Name
	pv.Spec.ClaimRef.UID = pvc.UID
	pv.Spec.Local.Path = "/test/workspace-2"
	if e = client.Tracker().Add(pvc); e != nil {
		t.Fatal(e)
	}
	if e = client.Tracker().Add(pv); e != nil {
		t.Fatal(e)
	}
	policy, e := LoadCapacityClaims(writeCapacity(t, []capacityEntry{{p.ID, hex.EncodeToString(p.Hash[:]), []string{pvc.Name}}}), s.Profiles, nil)
	if e != nil {
		t.Fatal(e)
	}
	// Recreate service to simulate deployment/restart; durable request is unchanged.
	s = &Service{Pool: s.Pool, Client: client, StationID: s.StationID, Profiles: s.Profiles, CapacityClaims: policy, Observer: s.Observer}
	out, e := s.Create(ctx, in)
	if e != nil || out.Phase != "reserved" || out.Workspace.PVCName != pvc.Name || out.InstanceID != in.InstanceID || out.OperationID != in.OperationID {
		t.Fatal(out, e)
	}
	if out, e = s.Create(ctx, in); e != nil || out.Phase != "claimed" {
		t.Fatal(out, e)
	}
	var afterInput, afterRequestHash, afterProfileHash []byte
	if e = s.Pool.QueryRow(ctx, `SELECT input,request_hash,profile_hash FROM station.runtime_instance_executions WHERE operation_id=$1`, in.OperationID).Scan(&afterInput, &afterRequestHash, &afterProfileHash); e != nil {
		t.Fatal(e)
	}
	if !bytes.Equal(input, afterInput) || !bytes.Equal(requestHash, afterRequestHash) || !bytes.Equal(profileHash, afterProfileHash) {
		t.Fatal("accepted request rewritten")
	}
	unchanged, e := client.CoreV1().PersistentVolumeClaims(p.Namespace).Get(ctx, original.Name, metav1.GetOptions{})
	if e != nil || !reflect.DeepEqual(unchanged, original) {
		t.Fatal("old workspace modified", e)
	}
	// Removing policy must fail closed, never reserve a replacement for this instance.
	s.CapacityClaims = nil
	if _, e = s.Create(ctx, in); e == nil || e.Error() != "OPERATION_OUTCOME_UNKNOWN" {
		t.Fatal("removed capacity silently rebound", e)
	}
	var count int
	if e = s.Pool.QueryRow(ctx, `SELECT count(*) FROM station.runtime_instance_workspaces WHERE instance_id=$1`, in.InstanceID).Scan(&count); e != nil || count != 1 {
		t.Fatal("workspace identity changed", count, e)
	}
}
