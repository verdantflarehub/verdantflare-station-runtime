package executor

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func buildTestChartTGZ(t *testing.T, name, chartVer, appVer, imageTag string) []byte {
	t.Helper()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	files := map[string]string{
		name + "/Chart.yaml": "apiVersion: v2\nname: " + name + "\nversion: " + chartVer + "\nappVersion: " + appVer + "\n",
		name + "/values.yaml": "image:\n  repository: registry.verdantflare.ai/verdantflare/" + name + "-manager\n  tag: " + imageTag + "\n",
		name + "/templates/deployment.yaml": "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: " + name + "-manager\n",
	}
	for filename, content := range files {
		raw := []byte(content)
		if err := tw.WriteHeader(&tar.Header{Name: filename, Mode: 0644, Size: int64(len(raw))}); err != nil {
			t.Fatal(err)
		}
		if _, err := tw.Write(raw); err != nil {
			t.Fatal(err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func TestOCIChartSourceResolveAndHelmSecretLifecycle(t *testing.T) {
	rawTGZ := buildTestChartTGZ(t, "comfyui", "0.1.0", "0.1.15", "comfyui-v0.1.15")
	pkgSum := sha256.Sum256(rawTGZ)
	pkgDigest := "sha256:" + hex.EncodeToString(pkgSum[:])

	manifestMap := map[string]any{
		"schemaVersion": 2,
		"mediaType":     "application/vnd.oci.image.manifest.v1+json",
		"layers": []map[string]any{
			{
				"mediaType": "application/vnd.cncf.helm.chart.content.v1.tar+gzip",
				"digest":    pkgDigest,
				"size":      len(rawTGZ),
			},
		},
	}
	rawManifest, _ := json.Marshal(manifestMap)
	manSum := sha256.Sum256(rawManifest)
	manDigest := "sha256:" + hex.EncodeToString(manSum[:])

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v2/charts/comfyui/manifests/" + manDigest:
			w.WriteHeader(200)
			_, _ = w.Write(rawManifest)
		case "/v2/charts/comfyui/blobs/" + pkgDigest:
			w.WriteHeader(200)
			_, _ = w.Write(rawTGZ)
		default:
			w.WriteHeader(404)
		}
	}))
	defer srv.Close()

	one := int32(1)
	baseTargets := map[string]Target{
		"comfyui@0.1.12": {
			AppID:   "comfyui",
			Version: "0.1.12",
			Deployment: &appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{Name: "comfyui-manager", Namespace: "verdantflare-app"},
				Spec: appsv1.DeploymentSpec{
					Replicas: &one,
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{Name: "comfyui-manager", Image: "registry.verdantflarehub.com/verdantflare/comfyui-manager:comfyui-v0.1.12"},
							},
						},
					},
				},
			},
		},
	}

	source := &OCIChartSource{
		RegistryURLs: []string{"http://127.0.0.1:1", srv.URL},
		HTTPClient:   srv.Client(),
		cache:        map[string]Target{},
	}

	resolved, ok := source.Resolve(context.Background(), baseTargets, "comfyui", "0.1.15", manDigest, pkgDigest)
	if !ok {
		t.Fatal("expected OCI chart target to resolve via fallback registry URL")
	}
	if resolved.Version != "0.1.15" || "sha256:"+hex.EncodeToString(resolved.Hash[:]) != pkgDigest {
		t.Fatalf("unexpected resolved version/hash: %s / %x", resolved.Version, resolved.Hash)
	}
	if gotImg := resolved.Deployment.Spec.Template.Spec.Containers[0].Image; gotImg != "registry.verdantflarehub.com/verdantflare/comfyui-manager:comfyui-v0.1.15" {
		t.Fatalf("unexpected container image: %s", gotImg)
	}

	k8sClient := fake.NewSimpleClientset()
	recordHelmReleaseSecret(context.Background(), k8sClient, "verdantflare-app", resolved)
	secs, err := k8sClient.CoreV1().Secrets("verdantflare-app").List(context.Background(), metav1.ListOptions{LabelSelector: "owner=helm,name=comfyui"})
	if err != nil || len(secs.Items) != 1 || secs.Items[0].Name != "sh.helm.release.v1.comfyui.v1" {
		t.Fatalf("expected helm release secret v1, got %v (err=%v)", secs, err)
	}

	deleteHelmReleaseSecrets(context.Background(), k8sClient, "verdantflare-app", "comfyui")
	secs, err = k8sClient.CoreV1().Secrets("verdantflare-app").List(context.Background(), metav1.ListOptions{LabelSelector: "owner=helm,name=comfyui"})
	if err != nil || len(secs.Items) != 0 {
		t.Fatalf("expected helm release secrets deleted, got %d (err=%v)", len(secs.Items), err)
	}
}
