package executor

import (
	"reflect"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestNetworkImmutableConsumerConfigReplacement(t *testing.T) {
	for _, scenario := range []string{"allowed", "mutable", "missing", "other app", "writable", "other path", "other volume", "secret", "changed mode", "ordinary driver"} {
		t.Run(scenario, func(t *testing.T) {
			source, request := installationFixture()
			source.AppID, source.Version, source.Hash[0] = "network", "0.1.5", 1
			source.Deployment.Spec.Template.Spec.Volumes = append(source.Deployment.Spec.Template.Spec.Volumes,
				corev1.Volume{Name: "channel-consumers", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: "network-channel-consumers-v0-1-5"}}}})
			source.Deployment.Spec.Template.Spec.Containers[0].VolumeMounts = append(source.Deployment.Spec.Template.Spec.Containers[0].VolumeMounts,
				corev1.VolumeMount{Name: "channel-consumers", MountPath: "/etc/network/channel-consumers", ReadOnly: true})
			target := source
			target.Deployment = source.Deployment.DeepCopy()
			target.Version, target.Hash[0] = "0.1.6", 2
			volumes := target.Deployment.Spec.Template.Spec.Volumes
			volume := &volumes[len(volumes)-1]
			volume.ConfigMap.Name = "network-channel-consumers-v0-1-6"
			mounts := target.Deployment.Spec.Template.Spec.Containers[0].VolumeMounts
			mount := &mounts[len(mounts)-1]
			immutable := true
			old := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "network-channel-consumers-v0-1-5", Namespace: source.Deployment.Namespace}, Immutable: &immutable, Data: map[string]string{"consumers.json": "{}"}}
			next := old.DeepCopy()
			next.Name = "network-channel-consumers-v0-1-6"
			switch scenario {
			case "mutable":
				value := false
				next.Immutable = &value
			case "missing":
				next.Name = "absent"
			case "other app":
				source.AppID = "other"
				target.AppID = "other"
			case "writable":
				mount.ReadOnly = false
			case "other path":
				mount.MountPath = "/unapproved"
			case "other volume":
				volume.Name = "other-config"
			case "secret":
				volume.ConfigMap = nil
				volume.Secret = &corev1.SecretVolumeSource{SecretName: "secret"}
			case "changed mode":
				value := int32(0777)
				volume.ConfigMap.DefaultMode = &value
			}
			request.AppId, request.AppVersion = source.AppID, source.Version
			before := target.Deployment.DeepCopy()
			client := fake.NewSimpleClientset(old, next)
			driver, err := newManagementReplacement(&Kubernetes{Client: client}, source, target, request, scenario != "ordinary driver")
			if (err == nil) != (scenario == "allowed") {
				t.Fatalf("acceptance=%v, scenario=%s", err, scenario)
			}
			if !reflect.DeepEqual(before, target.Deployment) {
				t.Fatal("registered target mutated")
			}
			if scenario == "allowed" && driver.Target.Hash != target.Hash {
				t.Fatal("template hash changed")
			}
			for _, action := range client.Actions() {
				if action.GetVerb() != "get" || action.GetResource().Resource != "configmaps" {
					t.Fatal("unexpected API action")
				}
			}
		})
	}
}
