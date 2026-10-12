package executor

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
	pb "github.com/verdantflarehub/verdantflare-station-runtime/api/runtimev1"
	"github.com/verdantflarehub/verdantflare-station-runtime/internal/releasebinding"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

func preinstalledDatabaseRelease(t *testing.T, app Target) (*releasebinding.Source, *pb.PreinstalledRelease) {
	t.Helper()
	dir := t.TempDir()
	digest := func(raw []byte) string { sum := sha256.Sum256(raw); return "sha256:" + hex.EncodeToString(sum[:]) }
	packageBytes, chartBytes := []byte(`{"appId":"test-app","version":"1.0.0"}`), []byte("controlled test chart bytes")
	binding := releasebinding.Binding{ReleaseRef: "test-app-v1", AppID: app.AppID, AppVersion: app.Version, PackageDigest: digest(packageBytes), ChartDigest: digest(chartBytes), TemplateDigest: "sha256:" + hex.EncodeToString(app.Hash[:])}
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(releasebinding.SignedRelease{SchemaVersion: 1, Binding: binding, NotBefore: time.Now().Add(-time.Hour).UTC().Format(time.RFC3339), ExpiresAt: time.Now().Add(time.Hour).UTC().Format(time.RFC3339)})
	if err != nil {
		t.Fatal(err)
	}
	sig := ed25519.Sign(priv, append([]byte("verdantflare.preinstalled.release.v1\n"), payload...))
	raw, err := json.Marshal(map[string]any{"schemaVersion": 1, "keys": map[string]string{"test-key": base64.StdEncoding.EncodeToString(pub)}, "releases": []any{map[string]string{"releaseRef": binding.ReleaseRef, "payload": base64.StdEncoding.EncodeToString(payload), "signature": base64.StdEncoding.EncodeToString(sig), "keyId": "test-key", "packageRef": "package.json", "chartRef": "chart.tgz"}}})
	if err != nil {
		t.Fatal(err)
	}
	for name, content := range map[string][]byte{"package.json": packageBytes, "chart.tgz": chartBytes, "releases.json": raw} {
		if err = os.WriteFile(filepath.Join(dir, name), content, 0600); err != nil {
			t.Fatal(err)
		}
	}
	return &releasebinding.Source{Path: filepath.Join(dir, "releases.json")}, &pb.PreinstalledRelease{ReleaseRef: binding.ReleaseRef, PackageDigest: binding.PackageDigest, ChartDigest: binding.ChartDigest, TemplateDigest: binding.TemplateDigest}
}

// This uses an isolated remote PostgreSQL execution journal and in-memory Kube
// objects. It never launches a container, service listener, or real workload.
func TestPreinstalledDatabaseAdoptionRecoversPartialMetadataFailure(t *testing.T) {
	pool, in := runtimeDB(t)
	ctx := context.Background()
	app := target()
	app.Deployment.Spec.Template.Spec.Containers[0].ImagePullPolicy = corev1.PullAlways
	app.Services = []*corev1.Service{
		{ObjectMeta: metav1.ObjectMeta{Name: "service-one", Namespace: app.Deployment.Namespace}, Spec: corev1.ServiceSpec{Selector: map[string]string{"app": "test"}, Ports: []corev1.ServicePort{{Port: 80}}}},
		{ObjectMeta: metav1.ObjectMeta{Name: "service-two", Namespace: app.Deployment.Namespace}, Spec: corev1.ServiceSpec{Selector: map[string]string{"app": "test"}, Ports: []corev1.ServicePort{{Port: 81}}}},
	}
	// The production loader binds a normalized controlled Target. This fixture
	// likewise fixes the complete target before signing and before any live edit.
	raw, _ := json.Marshal(struct {
		Deployment any
		Services   any
	}{app.Deployment, app.Services})
	app.Hash = sha256.Sum256(raw)
	source, binding := preinstalledDatabaseRelease(t, app)
	in.Action = "adopt"
	in.ExpectedWorkloadUid = uuid.NewString()
	in.PreinstalledRelease = binding
	live := app.Deployment.DeepCopy()
	live.UID = types.UID(in.ExpectedWorkloadUid)
	live.ResourceVersion = "10"
	first, second := app.Services[0].DeepCopy(), app.Services[1].DeepCopy()
	first.UID = types.UID(uuid.NewString())
	first.ResourceVersion = "11"
	second.UID = types.UID(uuid.NewString())
	second.ResourceVersion = "12"
	client := fake.NewSimpleClientset(live, first, second)
	failedOnce := false
	client.PrependReactor("patch", "services", func(action ktesting.Action) (bool, kruntime.Object, error) {
		patch := action.(ktesting.PatchAction)
		if patch.GetName() == "service-two" && !failedOnce {
			failedOnce = true
			return true, nil, errors.New("test response lost after first metadata patch")
		}
		return false, nil, nil
	})
	svc := &Service{Pool: pool, StationID: in.StationId, Targets: map[string]Target{"test-app@1.0.0": app}, Driver: &Kubernetes{Client: client}, Releases: source}
	out, err := svc.Reconcile(ctx, in)
	if err != nil || out.Status != "running" || len(client.Actions()) != 0 {
		t.Fatal("side effect before durable acceptance", out, err)
	}
	out, err = svc.Reconcile(ctx, in)
	if status.Code(err) != codes.Unavailable || !failedOnce {
		t.Fatal("partial mutation uncertainty not retained", out, err)
	}
	var saved string
	if err = pool.QueryRow(ctx, `SELECT status FROM station.runtime_app_executions WHERE operation_id=$1`, in.OperationId).Scan(&saved); err != nil || saved != "running" {
		t.Fatal("partial adoption became terminal", saved, err)
	}
	adoptedFirst, err := client.CoreV1().Services(first.Namespace).Get(ctx, first.Name, metav1.GetOptions{})
	if err != nil || !owned(adoptedFirst.Annotations, app, in.StationId, in.OrganizationId) {
		t.Fatal("partial progress missing", err)
	}
	// A fresh service object has no memory of the interrupted request.
	svc = &Service{Pool: pool, StationID: in.StationId, Targets: map[string]Target{"test-app@1.0.0": app}, Driver: &Kubernetes{Client: client}, Releases: source}
	in.RequestId = "new-trace-after-process-replacement"
	out, err = svc.Reconcile(ctx, in)
	if err != nil || out.Status != "succeeded" {
		t.Fatal("durable adoption did not resume", out, err)
	}
	for _, before := range []*corev1.Service{first, second} {
		after, err := client.CoreV1().Services(before.Namespace).Get(ctx, before.Name, metav1.GetOptions{})
		if err != nil || after.UID != before.UID || !reflect.DeepEqual(after.Spec, before.Spec) || !owned(after.Annotations, app, in.StationId, in.OrganizationId) {
			t.Fatal("service replaced or mutated spec", err)
		}
	}
	after, err := client.AppsV1().Deployments(live.Namespace).Get(ctx, live.Name, metav1.GetOptions{})
	if err != nil || after.UID != live.UID || !reflect.DeepEqual(after.Spec, live.Spec) || !owned(after.Annotations, app, in.StationId, in.OrganizationId) {
		t.Fatal("workload replaced or mutated spec", err)
	}
	patches := map[string]int{}
	for _, action := range client.Actions() {
		if action.GetVerb() == "patch" {
			patch := action.(ktesting.PatchAction)
			patches[patch.GetName()]++
		} else if action.GetVerb() != "get" {
			t.Fatal("unexpected external mutation")
		}
	}
	if patches[first.Name] != 1 || patches[second.Name] != 2 || patches[live.Name] != 1 {
		t.Fatal("recovery repeated successful metadata write", patches)
	}
	if err = os.WriteFile(source.Path, []byte(`{"schemaVersion":1,"keys":{},"releases":[]}`), 0600); err != nil {
		t.Fatal(err)
	}
	beforeActions := len(client.Actions())
	out, err = svc.Reconcile(ctx, in)
	if err != nil || out.Status != "succeeded" || len(client.Actions()) != beforeActions {
		t.Fatal("terminal replay performed side effects", out, err)
	}
	in.OperationId = uuid.NewString()
	if _, err = svc.Reconcile(ctx, in); status.Code(err) != codes.PermissionDenied || len(client.Actions()) != beforeActions {
		t.Fatal("withdrawal allowed new side effect", err)
	}
	var count int
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM station.runtime_app_executions`).Scan(&count); err != nil || count != 1 {
		t.Fatal("withdrawn request persisted an execution", count, err)
	}
}
