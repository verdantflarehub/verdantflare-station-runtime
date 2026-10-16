package executor

import (
	"context"
	"reflect"
	"regexp"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Normalize only the comparison copy. Registered templates and their hashes
// remain unchanged; data, Secret and arbitrary config volumes stay protected.
func normalizeNetworkConsumerConfig(k *Kubernetes, source Target, compare *appsv1.Deployment) error {
	if source.AppID != "network" {
		return nil
	}
	original := source.Deployment.Spec.Template.Spec
	candidate := &compare.Spec.Template.Spec
	if len(original.Volumes) != len(candidate.Volumes) {
		return ErrManagementReplacement
	}
	pattern := regexp.MustCompile(`^network-channel-consumers-v[0-9]+-[0-9]+-[0-9]+$`)
	for i, volume := range original.Volumes {
		other := &candidate.Volumes[i]
		if reflect.DeepEqual(volume, *other) {
			continue
		}
		if volume.Name != "channel-consumers" || other.Name != volume.Name || volume.ConfigMap == nil || other.ConfigMap == nil ||
			!pattern.MatchString(volume.ConfigMap.Name) || !pattern.MatchString(other.ConfigMap.Name) {
			return ErrManagementReplacement
		}
		normalized := other.DeepCopy()
		normalized.ConfigMap.Name = volume.ConfigMap.Name
		if !reflect.DeepEqual(volume, *normalized) || len(original.InitContainers) != 0 || len(original.Containers) != 1 {
			return ErrManagementReplacement
		}
		mounted := false
		for _, mount := range original.Containers[0].VolumeMounts {
			if mount.Name == volume.Name {
				if !mount.ReadOnly || mount.MountPath != "/etc/network/channel-consumers" || mount.SubPath != "" || mount.SubPathExpr != "" {
					return ErrManagementReplacement
				}
				mounted = true
			}
		}
		if !mounted {
			return ErrManagementReplacement
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		for _, name := range []string{volume.ConfigMap.Name, other.ConfigMap.Name} {
			config, err := k.Client.CoreV1().ConfigMaps(compare.Namespace).Get(ctx, name, metav1.GetOptions{})
			if err != nil || config.Immutable == nil || !*config.Immutable || config.DeletionTimestamp != nil || len(config.BinaryData) != 0 || len(config.Data) != 1 || config.Data["consumers.json"] == "" {
				cancel()
				return ErrManagementReplacement
			}
		}
		cancel()
		other.ConfigMap.Name = volume.ConfigMap.Name
	}
	return nil
}
