package executor

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func videoStoppedFixture(t *testing.T) (*maintenanceFixture, *stoppedHTTPChecks) {
	return videoStoppedFixtureFor(t, "0.17.28", "0.17.29")
}

func videoStoppedFixtureFor(t *testing.T, sourceVersion, targetVersion string) (*maintenanceFixture, *stoppedHTTPChecks) {
	f := newMaintenanceFixtureFor(t, func(source, target *Target) {
		source.Version, target.Version = sourceVersion, targetVersion
		for _, entry := range []*Target{source, target} {
			entry.AppID = "video-mcp"
			entry.InstallationBindings = []InstallationBinding{{"api", "APP_INSTALLATION", "installation_id"}, {"api", "APP_STATION", "station_id"}, {"api", "APP_VERSION", "app_version"}}
			entry.Deployment.Spec.Template.Spec.Containers[0].Name = "api"
			entry.Deployment.Spec.Template.Spec.Containers[0].VolumeMounts = []corev1.VolumeMount{{Name: "state", MountPath: "/data/projects"}}
			entry.Deployment.Spec.Template.Spec.Containers = append(entry.Deployment.Spec.Template.Spec.Containers, corev1.Container{Name: "artifact-authority", Image: "authority:0.17.12"})
		}
	})
	f.pod.Status.PodIP = "10.0.0.9"
	f.pod.Status.ContainerStatuses[0].Name = "api"
	f.pod.Status.ContainerStatuses = append(f.pod.Status.ContainerStatuses, corev1.ContainerStatus{Name: "artifact-authority", Ready: true, ContainerID: "containerd://authority", ImageID: "repo@authority", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}})
	f.pod.Status.ContainerStatuses[1].ImageID = "repo@" + videoAuthorityDigest
	f.savePod(t)
	for _, release := range []*ManagementRelease{&f.b.Source, &f.b.Target} {
		release.ReleaseKind = "dashboard-release"
		release.AppImageDigest = "sha256:" + strings.Repeat("a", 64)
		release.ManifestDigest = release.ReleaseDigest
		release.DataContract = ""
		release.DefaultWorkerVersion = ""
		release.CompatibleWorkerVersions = []string{}
	}
	for _, target := range []*Target{&f.d.Source, &f.d.Target} {
		target.ActivityURL = "http://" + target.Deployment.Name + "." + target.Deployment.Namespace + ".svc.cluster.local:8000/health"
		target.ActivityTokenEnv = "STATION_APP_TEST_TOKEN"
	}
	t.Setenv("STATION_APP_TEST_TOKEN", strings.Repeat("test-only-", 5))
	f.d.Kubernetes.videoActivityExec = func(_ context.Context, ns, pod, container string, input []byte) ([]byte, error) {
		if ns != f.pod.Namespace || pod != f.pod.Name || container != "api" || len(input) != 0 {
			t.Fatal("unbound exec")
		}
		f.calls++
		return []byte(fmt.Sprintf(`{"schema_version":1,"scope":"installation","active_count":0,"submitted_count":191,"app_version":%q,"dashboard_digest":%q}`, sourceVersion, f.b.Source.DashboardDigest)), nil
	}
	s := newStoppedChecks()
	s.client.Transport = maintenanceRoundTrip(func(req *http.Request) (*http.Response, error) {
		if req.Method != "GET" || req.URL.Host != "10.0.0.9:8000" || req.URL.Path != "/health" || req.Header.Get("Authorization") != "Bearer "+strings.Repeat("test-only-", 5) {
			t.Fatal("unbound health request")
		}
		return proofResponse(200, fmt.Sprintf(`{"status":"ok","version":%q,"contract_version":"v1"}`, sourceVersion)), nil
	})
	return f, s
}

func TestVideoStoppedRebasedPair(t *testing.T) {
	for _, pair := range [][2]string{{"0.17.30", "0.17.31"}, {"0.17.30", "0.17.32"}, {"0.17.30", "0.17.33"}, {"0.17.28", "0.17.31"}, {"0.17.30", "0.17.29"}, {"0.17.31", "0.17.32"}, {"0.17.31", "0.17.33"}, {"0.17.35", "0.17.36"}, {"0.17.35", "0.17.37"}} {
		f, s := videoStoppedFixtureFor(t, pair[0], pair[1])
		err := s.Check(context.Background(), f.b, &StoppedReplacement{f.d}, f.c, false, true)
		if (err == nil) != ((pair[0] == "0.17.30" && (pair[1] == "0.17.31" || pair[1] == "0.17.32")) || (pair[0] == "0.17.31" && pair[1] == "0.17.32") || (pair[0] == "0.17.35" && pair[1] == "0.17.36")) {
			t.Fatal("unexpected pair admission", pair, err)
		}
	}
}

func TestVideoStoppedChecksRejectUnsafeEvidenceAndContainers(t *testing.T) {
	for _, scenario := range []string{"ready", "busy", "restore busy", "malformed", "wrong version", "wrong dashboard", "wrong api image", "wrong authority image", "authority restart", "authority drift", "missing authority", "extra container", "unsafe mount", "unsafe target", "redirect"} {
		t.Run(scenario, func(t *testing.T) {
			f, s := videoStoppedFixture(t)
			original := f.d.Kubernetes.videoActivityExec
			f.d.Kubernetes.videoActivityExec = func(ctx context.Context, ns, pod, container string, input []byte) ([]byte, error) {
				raw, err := original(ctx, ns, pod, container, input)
				switch scenario {
				case "busy", "restore busy":
					raw = []byte(strings.Replace(string(raw), `"active_count":0`, `"active_count":1`, 1))
				case "malformed":
					raw = []byte(`{"error":"activity_unavailable"}`)
				case "wrong version":
					raw = []byte(strings.Replace(string(raw), "0.17.28", "0.17.27", 1))
				case "wrong dashboard":
					raw = []byte(strings.Replace(string(raw), f.b.Source.DashboardDigest, "sha256:"+strings.Repeat("c", 64), 1))
				case "authority restart":
					f.pod.Status.ContainerStatuses[1].RestartCount++
					f.savePod(t)
				}
				return raw, err
			}
			blockedBeforeProbe := false
			switch scenario {
			case "wrong api image":
				f.pod.Status.ContainerStatuses[0].ImageID = "repo@sha256:" + strings.Repeat("c", 64)
				blockedBeforeProbe = true
			case "wrong authority image":
				f.pod.Status.ContainerStatuses[1].ImageID = "repo@sha256:" + strings.Repeat("c", 64)
				blockedBeforeProbe = true
			case "authority drift":
				f.d.Target.Deployment.Spec.Template.Spec.Containers[1].Image = "other:1"
				blockedBeforeProbe = true
			case "missing authority":
				f.pod.Spec.Containers = f.pod.Spec.Containers[:1]
				blockedBeforeProbe = true
			case "extra container":
				f.pod.Spec.Containers = append(f.pod.Spec.Containers, corev1.Container{Name: "extra"})
				blockedBeforeProbe = true
			case "unsafe mount":
				f.pod.Spec.Containers[0].VolumeMounts = append(f.pod.Spec.Containers[0].VolumeMounts, corev1.VolumeMount{Name: "overlay", MountPath: "/app"})
				blockedBeforeProbe = true
			case "unsafe target":
				f.b.Target.AppVersion = "0.17.30"
				blockedBeforeProbe = true
			case "redirect":
				s.client.Transport = maintenanceRoundTrip(func(*http.Request) (*http.Response, error) { return proofResponse(302, ""), nil })
			}
			f.savePod(t)
			err := s.Check(context.Background(), f.b, &StoppedReplacement{f.d}, f.c, false, scenario != "restore busy")
			if scenario == "ready" || scenario == "restore busy" {
				if err != nil || f.calls != 1 {
					t.Fatal("read-only check failed", err, f.calls)
				}
			} else if err == nil {
				t.Fatal("unsafe upgrade accepted")
			}
			if scenario == "busy" && !errors.Is(err, ErrManagementInstallationBusy) {
				t.Fatal("busy protection lost", err)
			}
			if blockedBeforeProbe && f.calls != 0 {
				t.Fatal("executed against invalid container")
			}
		})
	}
}

func TestVideoActivityEvidenceRejectsInvalidCountsAndScope(t *testing.T) {
	release := ManagementRelease{AppVersion: "0.17.28", DashboardDigest: "sha256:" + strings.Repeat("a", 64)}
	base := fmt.Sprintf(`{"schema_version":1,"scope":"installation","active_count":0,"submitted_count":1,"app_version":"0.17.28","dashboard_digest":%q}`, release.DashboardDigest)
	for _, raw := range []string{strings.Replace(base, `"active_count":0`, `"active_count":-1`, 1), strings.Replace(base, `"active_count":0`, `"active_count":2`, 1), strings.Replace(base, `"active_count":0`, `"active_count":0,"active_count":1`, 1), strings.Replace(base, "installation", "user", 1)} {
		if _, err := videoActivityEvidence([]byte(raw), release); err == nil {
			t.Fatal("invalid count or scope accepted")
		}
	}
}

func TestVideoStoppedChecksVerifyTargetBeforeOpening(t *testing.T) {
	f, s := videoStoppedFixture(t)
	live := f.c.Deployment.DeepCopy()
	live.Spec = *f.d.Target.Deployment.Spec.DeepCopy()
	mark(&live.ObjectMeta, f.d.Target, f.d.StationID, f.d.OrganizationID)
	live.Annotations[transitionOperationKey] = f.d.OperationID
	if _, err := f.k.AppsV1().Deployments(live.Namespace).Update(context.Background(), live, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	f.pod.Spec = *live.Spec.Template.Spec.DeepCopy()
	f.savePod(t)
	original := f.d.Kubernetes.videoActivityExec
	f.d.Kubernetes.videoActivityExec = func(ctx context.Context, ns, pod, container string, input []byte) ([]byte, error) {
		raw, err := original(ctx, ns, pod, container, input)
		return []byte(strings.ReplaceAll(string(raw), "0.17.28", "0.17.29")), err
	}
	s.client.Transport = maintenanceRoundTrip(func(*http.Request) (*http.Response, error) {
		return proofResponse(200, `{"status":"ok","version":"0.17.29","contract_version":"v1"}`), nil
	})
	if err := s.Check(context.Background(), f.b, &StoppedReplacement{f.d}, f.c, true, true); err != nil {
		t.Fatal(err)
	}
}

func TestVideoPodOnlyAllowsKubernetesServiceAccountMount(t *testing.T) {
	want := corev1.Container{Name: "api", Image: "test:1", VolumeMounts: []corev1.VolumeMount{{Name: "data", MountPath: "/data/projects"}}}
	actual := *want.DeepCopy()
	actual.VolumeMounts = append(actual.VolumeMounts, corev1.VolumeMount{Name: "kube-api-access-test", MountPath: "/var/run/secrets/kubernetes.io/serviceaccount", ReadOnly: true})
	pod := corev1.Pod{Spec: corev1.PodSpec{Volumes: []corev1.Volume{{Name: "kube-api-access-test", VolumeSource: corev1.VolumeSource{Projected: &corev1.ProjectedVolumeSource{Sources: []corev1.VolumeProjection{
		{ServiceAccountToken: &corev1.ServiceAccountTokenProjection{Path: "token"}},
		{ConfigMap: &corev1.ConfigMapProjection{LocalObjectReference: corev1.LocalObjectReference{Name: "kube-root-ca.crt"}, Items: []corev1.KeyToPath{{Key: "ca.crt", Path: "ca.crt"}}}},
		{DownwardAPI: &corev1.DownwardAPIProjection{Items: []corev1.DownwardAPIVolumeFile{{Path: "namespace", FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.namespace"}}}}},
	}}}}}}}
	if !videoPodContainerMatches(actual, want, pod) {
		t.Fatal("standard projection rejected")
	}
	actual.VolumeMounts[1].ReadOnly = false
	if videoPodContainerMatches(actual, want, pod) {
		t.Fatal("writable extra mount accepted")
	}
	actual.VolumeMounts[1].ReadOnly = true
	actual.VolumeMounts[1].MountPath = "/app"
	if videoPodContainerMatches(actual, want, pod) {
		t.Fatal("app overlay accepted")
	}
	actual.VolumeMounts[1].MountPath = "/var/run/secrets/kubernetes.io/serviceaccount"
	pod.Spec.Volumes[0].Projected.Sources[1].ConfigMap.Name = "other"
	if videoPodContainerMatches(actual, want, pod) {
		t.Fatal("arbitrary projection accepted")
	}
}
