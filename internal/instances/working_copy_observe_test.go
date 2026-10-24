package instances

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestWorkingCopyObservationPreservesSavedBytes(t *testing.T) {
	for _, mode := range []string{"saved-after-empty", "saved-after-import", "wrong-pod", "wrong-revision", "missing", "oversize", "corrupt-digest", "changes-between-reads", "changes-preparation", "empty-file", "compressed-transfer", "redirect", "unready", "wrong-instance", "truncated-body", "missing-digest"} {
		t.Run(mode, func(t *testing.T) {
			content := "BLENDER-v405saved-scene-with-edits"
			original := strings.Repeat("a", 64)
			in := CreateRequest{OperationID: uuid.NewString(), InstanceID: uuid.NewString(), ProjectID: uuid.NewString(), SourceRevisionID: uuid.NewString(), EmptySource: true}
			proof := PreparationProof{PrepareID: in.OperationID, InstanceID: in.InstanceID, ProjectID: in.ProjectID, SourceRevisionID: in.SourceRevisionID, State: "prepared", PreparedAt: time.Now().UTC().Format(time.RFC3339Nano)}
			if mode == "saved-after-import" {
				asset := "inbox/abcdefghijklmnop/restore.blend"
				in.EmptySource = false
				in.SourceSize = 100
				in.SourceSHA256 = original
				proof.Size = 100
				proof.SHA256 = &original
				proof.AssetID = &asset
			}
			reads, statusReads := 0, 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != "GET" || r.Header.Get("Authorization") != "Bearer test-only" {
					t.Error("write or missing authentication")
				}
				switch r.URL.Path {
				case "/internal/workspace/status":
					statusReads++
					p := proof
					pod := "exact-helper-pod"
					if mode == "wrong-pod" {
						pod = "other-pod"
					}
					if mode == "wrong-revision" {
						p.SourceRevisionID = uuid.NewString()
					}
					if mode == "changes-preparation" && statusReads == 2 {
						p.PreparedAt = time.Now().Add(-time.Minute).UTC().Format(time.RFC3339Nano)
					}
					_ = json.NewEncoder(w).Encode(map[string]any{"instance_id": func() string {
						if mode == "wrong-instance" {
							return uuid.NewString()
						}
						return in.InstanceID
					}(), "pod_uid": pod, "preparation": p})
				case "/assets/project/main.blend":
					reads++
					data := content
					if mode == "missing" {
						http.NotFound(w, r)
						return
					}
					if mode == "redirect" {
						http.Redirect(w, r, "/unexpected", http.StatusFound)
						return
					}
					if mode == "empty-file" {
						data = ""
					}
					if mode == "changes-between-reads" && reads == 2 {
						data += "changed"
					}
					sum := sha256.Sum256([]byte(data))
					digest := hex.EncodeToString(sum[:])
					if mode == "corrupt-digest" {
						digest = original
					}
					w.Header().Set("X-Asset-SHA256", digest)
					w.Header().Set("Content-Length", fmt.Sprint(len(data)))
					if mode == "truncated-body" {
						w.Header().Set("Content-Length", fmt.Sprint(len(data)+1))
					}
					if mode == "missing-digest" {
						w.Header().Del("X-Asset-SHA256")
					}
					if mode == "compressed-transfer" {
						w.Header().Set("Content-Encoding", "gzip")
					}
					_, _ = w.Write([]byte(data))
				default:
					t.Error("unexpected or redirected request")
					http.NotFound(w, r)
				}
			}))
			defer server.Close()
			helper := HelperObservation{PodUID: "exact-helper-pod", FileEndpoint: server.URL, Ready: mode != "unready"}
			max := int64(1024)
			if mode == "oversize" {
				max = 12
			}
			observer := HTTPObserver{Client: InternalHTTPClient()}
			result, err := observer.ObserveWorkingCopy(context.Background(), in, helper, "test-only", max)
			if mode != "saved-after-empty" && mode != "saved-after-import" {
				if err == nil || result != nil {
					t.Fatalf("unsafe observation accepted: %v %v", result, err)
				}
				return
			}
			sum := sha256.Sum256([]byte(content))
			if err != nil || result == nil || result.Empty || result.SHA256 != hex.EncodeToString(sum[:]) || result.Size != int64(len(content)) || result.SourceRevisionID != in.SourceRevisionID || reads != 2 || statusReads != 2 {
				t.Fatalf("saved copy not independently observed: %v %v reads=%d status=%d", result, err, reads, statusReads)
			}
		})
	}
}
