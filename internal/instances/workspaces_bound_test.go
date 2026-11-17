package instances

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"reflect"
	"strings"
	"testing"
)

type boundRow []any

func (r boundRow) Scan(out ...any) error {
	for i, v := range r {
		reflect.ValueOf(out[i]).Elem().Set(reflect.ValueOf(v))
	}
	return nil
}

type boundTx struct {
	pgx.Tx
	b                                      VolumeBinding
	locked, committed, deleted, poolLocked bool
}

func (tx *boundTx) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	if strings.Contains(sql, "pg_advisory_xact_lock") {
		tx.poolLocked = true
	}
	if strings.HasPrefix(sql, "DELETE") {
		tx.deleted = true
	}
	return pgconn.NewCommandTag("DELETE 1"), nil
}
func (tx *boundTx) Commit(context.Context) error { tx.committed = true; return nil }
func (tx *boundTx) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	if strings.Contains(sql, "pg_try_advisory") {
		return boundRow{tx.locked}
	}
	b := tx.b
	if strings.Contains(sql, "FOR UPDATE") {
		return boundRow{b.InstanceID, b.StationID, b.OrganizationID, b.ProjectID, b.CreateOperationID, b.ProfileID, b.ProfileHash, b.Namespace, b.Pool, b.PVCName, b.PVCUID, b.PVName, b.PVUID, b.NodeName, b.ReservedBytes, b.Retained}
	}
	return boundRow{b.InstanceID, b.Retained, "succeeded", "deleted", "destroy"}
}
func TestBoundWorkspaceReleasePreservesStaticClaim(t *testing.T) {
	for _, scenario := range []string{"success", "foreign_instance", "foreign_org", "replaced_uid", "wrong_pool", "not_retained", "locked", "mounted", "cleaner_failure"} {
		t.Run(scenario, func(t *testing.T) {
			ctx := context.Background()
			id, p, pvc, pv := fixture()
			p.Pool = "blender-local-v2"
			pvc.Labels[poolKey] = p.Pool
			pv.Labels[poolKey] = p.Pool
			client := fake.NewClientset(pvc, pv)
			b, err := Inspect(ctx, client, id, p, pvc.Name, nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			b.Retained = true
			in := ReleaseWorkspaceRequest{StationID: id.StationID, OrganizationID: id.OrganizationID, InstanceID: id.InstanceID, Namespace: p.Namespace, PVCName: pvc.Name, PVCUID: string(pvc.UID), PVUID: string(pv.UID), PreserveBoundPVC: true}
			tx := &boundTx{b: b, locked: true}
			switch scenario {
			case "foreign_instance":
				in.InstanceID = "foreign"
			case "foreign_org":
				in.OrganizationID = "foreign"
			case "replaced_uid":
				in.PVCUID = "replacement"
			case "wrong_pool":
				tx.b.Pool = "comfyui"
			case "not_retained":
				tx.b.Retained = false
			case "locked":
				tx.locked = false
			case "mounted":
				_, err = client.CoreV1().Pods(p.Namespace).Create(ctx, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "user"}, Spec: corev1.PodSpec{Volumes: []corev1.Volume{{Name: "data", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: pvc.Name}}}}}}, metav1.CreateOptions{})
				if err != nil {
					t.Fatal(err)
				}
			}
			cleaned := false
			s := &Service{StationID: id.StationID, Client: client, WorkspaceCleaner: func(context.Context, string, string, string) error {
				if !tx.poolLocked || !tx.locked || tx.deleted {
					t.Fatal("cleanup outside allocation/instance lock")
				}
				cleaned = true
				if scenario == "cleaner_failure" {
					return errors.New("cleanup failed")
				}
				return nil
			}}
			out, err := s.releaseBoundWorkspaceTx(ctx, in, tx)
			if scenario == "success" {
				if err != nil || out.State != "released" || !tx.committed || !tx.deleted || !cleaned {
					t.Fatal(out, err, tx)
				}
			} else {
				if err == nil || tx.committed || tx.deleted {
					t.Fatal("unsafe release", scenario, err)
				}
				if scenario != "cleaner_failure" && cleaned {
					t.Fatal("cleaned foreign or busy workspace")
				}
			}
			got, _ := client.CoreV1().PersistentVolumeClaims(p.Namespace).Get(ctx, pvc.Name, metav1.GetOptions{})
			gotPV, _ := client.CoreV1().PersistentVolumes().Get(ctx, pv.Name, metav1.GetOptions{})
			if got == nil || got.UID != pvc.UID || got.Status.Phase != corev1.ClaimBound || gotPV == nil || gotPV.Spec.ClaimRef.UID != pvc.UID {
				t.Fatal("static binding lost")
			}
			for _, a := range client.Actions() {
				if a.GetVerb() == "delete" {
					t.Fatal("unexpected delete", a)
				}
			}
		})
	}
}

func TestBoundWorkspaceReleaseDecoding(t *testing.T) {
	old := ReleaseWorkspaceRequest{StationID: "station", OrganizationID: "org", UserID: "user", IdempotencyKey: "key", Namespace: "ns", PVCName: "claim"}
	for _, preserve := range []bool{false, true} {
		old.PreserveBoundPVC = preserve
		if preserve {
			old.InstanceID = "instance"
			old.PVCUID = "pvc"
			old.PVUID = "pv"
		}
		raw, _ := json.Marshal(old)
		var out ReleaseWorkspaceRequest
		if err := decodeRequest(raw, &out); err != nil || out != old {
			t.Fatal(string(raw), out, err)
		}
		raw = append(raw[:len(raw)-1], []byte(`,"pvc_name":"duplicate"}`)...)
		if decodeRequest(raw, &out) == nil {
			t.Fatal("duplicate accepted")
		}
	}
}
