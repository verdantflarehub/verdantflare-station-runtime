package executor

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestPreinstalledInstallNeverAdoptsExistingWorkload(t *testing.T) {
	for _, ownedByStation := range []bool{false, true} {
		app := target()
		app.Preinstalled = true
		live := app.Deployment.DeepCopy()
		if ownedByStation {
			mark(&live.ObjectMeta, app, "station", "org")
		}
		client := fake.NewSimpleClientset(live)
		driver := Kubernetes{client}
		out, err := driver.Step(context.Background(), app, "install", "new-operation", "station", "org")
		if err != nil || out.Code != "CONFLICT" {
			t.Fatal("existing deployment accepted", out, err)
		}
		for _, action := range client.Actions() {
			if action.GetVerb() != "get" {
				t.Fatal("existing deployment mutated")
			}
		}
	}
}
func TestPreinstalledLostCreateResponseUsesOriginalOperation(t *testing.T) {
	ctx := context.Background()
	app := target()
	app.Preinstalled = true
	client := fake.NewSimpleClientset()
	driver := Kubernetes{client}
	for i := 0; i < 2; i++ {
		out, err := driver.Step(ctx, app, "install", "original-operation", "station", "org")
		if err != nil || out.Status != "running" {
			t.Fatal(out, err)
		}
	}
	live, err := client.AppsV1().Deployments(app.Deployment.Namespace).Get(ctx, app.Deployment.Name, metav1.GetOptions{})
	if err != nil || live.Annotations[preinstalledOperationKey] != "original-operation" {
		t.Fatal("creation evidence missing", err)
	}
	out, err := driver.Step(ctx, app, "install", "different-operation", "station", "org")
	if err != nil || out.Code != "CONFLICT" {
		t.Fatal("other operation reused deployment", out, err)
	}
	creates := 0
	for _, action := range client.Actions() {
		if action.GetVerb() == "create" {
			creates++
		}
	}
	if creates != 1 {
		t.Fatal("duplicate workload creation", creates)
	}
}

func TestPreinstalledImagesUseFixedSemanticTags(t *testing.T) {
	for _, image := range []string{"registry.invalid/team/app:1.2.3", "registry.invalid:5000/team/app:0.1.0", "app:1.0.0", "app:v1.2.3", "registry.cn-qingdao.aliyuncs.com/wod/verdantflare-app:project-v0.1.0", "registry.cn-qingdao.aliyuncs.com/wod/verdantflare-app:world-v0.1.0", "registry.cn-qingdao.aliyuncs.com/wod/verdantflare-app:network-v0.1.0", "registry.cn-qingdao.aliyuncs.com/wod/verdantflare-app:resources-v0.1.0"} {
		if !preinstalledImage(image) {
			t.Fatal("fixed release rejected", image)
		}
	}
	for _, image := range []string{"app", "app:latest", "app:dev", "app:release", "app:main", "app:sha-deadbeef", "app:1.2.3-deadbeef", "app:01.2.3", "app:project-v01.2.3", "app:project-v1.02.3", "app:project-v1.2.03", "app:project-v1.2.3-sha-deadbeef", "app:project-latest", "app@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "app:project-v1.2.3@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "registry.invalid:5000/app", "app:1.0.0\n"} {
		if preinstalledImage(image) {
			t.Fatal("mutable or unsupported release accepted", image)
		}
	}
}
