package executor

import (
	"encoding/hex"
	"regexp"
	"strings"

	pb "github.com/verdantflarehub/verdantflare-station-runtime/api/runtimev1"
	"github.com/verdantflarehub/verdantflare-station-runtime/internal/releasebinding"
	corev1 "k8s.io/api/core/v1"
)

func (s *Service) verifyPreinstalled(in *pb.AppRequest, target Target) error {
	if in.PreinstalledRelease == nil {
		return nil
	}
	p := in.PreinstalledRelease
	b := releasebinding.Binding{ReleaseRef: p.ReleaseRef, AppID: in.AppId, AppVersion: in.AppVersion, PackageDigest: p.PackageDigest, ChartDigest: p.ChartDigest, TemplateDigest: p.TemplateDigest}
	if (in.Action != "install" && in.Action != "adopt") || b.TemplateDigest != "sha256:"+hex.EncodeToString(target.Hash[:]) || s.Releases == nil {
		return releasebinding.ErrUntrusted
	}
	if ValidatePreinstalledTarget(target) != nil {
		return releasebinding.ErrUntrusted
	}
	return s.Releases.Verify(b)
}

// ValidatePreinstalledTarget is shared by the release registry builder and the
// runtime consumer, so registration cannot bypass the fixed deployment policy.
func ValidatePreinstalledTarget(target Target) error {
	if target.Deployment == nil || len(target.Deployment.Spec.Template.Spec.Containers) == 0 {
		return releasebinding.ErrUntrusted
	}
	for _, containers := range [][]corev1.Container{target.Deployment.Spec.Template.Spec.Containers, target.Deployment.Spec.Template.Spec.InitContainers} {
		for _, container := range containers {
			if !preinstalledImage(container.Image) || container.ImagePullPolicy != corev1.PullAlways {
				return releasebinding.ErrUntrusted
			}
		}
	}
	for _, service := range target.Services {
		if service == nil || (service.Spec.Type != "" && service.Spec.Type != corev1.ServiceTypeClusterIP) || len(service.Spec.ExternalIPs) != 0 || service.Spec.ExternalName != "" || service.Spec.LoadBalancerIP != "" || len(service.Spec.LoadBalancerSourceRanges) != 0 {
			return releasebinding.ErrUntrusted
		}
		for _, port := range service.Spec.Ports {
			if port.NodePort != 0 {
				return releasebinding.ErrUntrusted
			}
		}
	}
	return nil
}

// The active deployment policy requires fixed semantic release tags and
// Always. Trust binds this exact image reference in the registered template; a tag
// alone is never reported as evidence of the running OCI content digest.
var preinstalledTag = regexp.MustCompile(`^(?:[a-z0-9][a-z0-9._-]*-)?v?(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$`)

func preinstalledImage(image string) bool {
	if strings.ContainsAny(image, "@\\ \t\r\n") {
		return false
	}
	colon := strings.LastIndexByte(image, ':')
	return colon > strings.LastIndexByte(image, '/') && colon > 0 && preinstalledTag.MatchString(image[colon+1:])
}
