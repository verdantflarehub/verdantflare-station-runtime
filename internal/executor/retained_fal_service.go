package executor

import (
	"reflect"

	"github.com/google/uuid"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// This controlled registration only supplements old deployment-only releases
// during a stopped transition. It never changes their immutable template hash
// or grants ordinary install operations permission to create a Service.
func validRetainedFalService(t Target) bool {
	s := t.RetainedFalService
	if s == nil {
		return true
	}
	if t.AppID != "fal" || t.Deployment == nil || len(t.Services) != 0 ||
		s.APIVersion != "v1" || s.Kind != "Service" || s.Name != t.Deployment.Name || s.Namespace != t.Deployment.Namespace ||
		s.ResourceVersion != "" || s.DeletionTimestamp != nil || len(s.Annotations) != 0 || len(s.OwnerReferences) != 0 ||
		s.Spec.Type != corev1.ServiceTypeClusterIP || s.Spec.ClusterIP == "" || s.Spec.ClusterIP == "None" ||
		s.Spec.ExternalName != "" || len(s.Spec.ExternalIPs) != 0 || len(s.Spec.LoadBalancerSourceRanges) != 0 ||
		len(s.Spec.Selector) == 0 || !reflect.DeepEqual(s.Spec.Selector, t.Deployment.Spec.Selector.MatchLabels) || len(s.Spec.Ports) != 2 {
		return false
	}
	id, err := uuid.Parse(string(s.UID))
	if err != nil || id.String() != string(s.UID) {
		return false
	}
	seen := map[int32]bool{}
	for _, p := range s.Spec.Ports {
		if (p.Port != 8000 && p.Port != 8001) || seen[p.Port] || p.Protocol != corev1.ProtocolTCP ||
			p.NodePort != 0 {
			return false
		}
		if p.TargetPort.Type == intstr.String {
			matches := 0
			for _, container := range t.Deployment.Spec.Template.Spec.Containers {
				for _, port := range container.Ports {
					if port.Name == p.TargetPort.StrVal && port.ContainerPort == p.Port && (port.Protocol == "" || port.Protocol == corev1.ProtocolTCP) {
						matches++
					}
				}
			}
			if p.TargetPort.StrVal == "" || matches != 1 {
				return false
			}
		} else if p.TargetPort.Type != intstr.Int || p.TargetPort.IntVal != p.Port {
			return false
		}
		seen[p.Port] = true
	}
	return true
}

func stoppedRetainedServices(source, target Target) (Target, Target, error) {
	if !validRetainedFalService(source) || !validRetainedFalService(target) ||
		!reflect.DeepEqual(source.RetainedFalService, target.RetainedFalService) {
		return source, target, ErrManagementReplacement
	}
	if source.RetainedFalService != nil {
		source.Services = []*corev1.Service{source.RetainedFalService.DeepCopy()}
		target.Services = []*corev1.Service{target.RetainedFalService.DeepCopy()}
	}
	return source, target, nil
}
