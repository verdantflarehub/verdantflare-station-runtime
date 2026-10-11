package executor

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestKubernetesAdmitsTwoLiveObservationReadBursts(t *testing.T) {
	// Exercise the real client-go limiter, which the fake Kubernetes client
	// bypasses. The server has no external dependencies or persisted state.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"apiVersion":"v1","kind":"Pod","metadata":{"name":"worker","namespace":"app-test"}}`)
	}))
	defer server.Close()
	path := filepath.Join(t.TempDir(), "kubeconfig")
	config := fmt.Sprintf("apiVersion: v1\nkind: Config\nclusters:\n- name: test\n  cluster:\n    server: %s\ncontexts:\n- name: test\n  context:\n    cluster: test\n    user: test\nusers:\n- name: test\n  user: {}\n", server.URL)
	if err := os.WriteFile(path, []byte(config), 0600); err != nil {
		t.Fatal(err)
	}
	driver, err := NewKubernetes(path, "test")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	for i := 0; i < 48; i++ {
		if _, err := driver.Client.CoreV1().Pods("app-test").Get(ctx, "worker", metav1.GetOptions{}); err != nil {
			t.Fatalf("live observation read %d exceeded the shared window: %v", i, err)
		}
	}
	canceled, stop := context.WithCancel(context.Background())
	stop()
	if _, err := driver.Client.CoreV1().Pods("app-test").Get(canceled, "worker", metav1.GetOptions{}); err == nil {
		t.Fatal("canceled observation was accepted")
	}
}
