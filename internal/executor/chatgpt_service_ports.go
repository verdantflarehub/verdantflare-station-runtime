package executor

import (
	"reflect"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// The stopped transition may evolve only the existing ChatGPT internal Service.
// Its selector fences both ports; ports change atomically with reopening, never
// while the new process is being verified. No extra Service is created.
func chatgptExecutionPorts(source, target Target) bool {
	if source.AppID != "chatgpt" || target.AppID != "chatgpt" || len(source.Services) != 1 || len(target.Services) != 1 {
		return false
	}
	a, b := source.Services[0], target.Services[0]
	if a == nil || b == nil {
		return false
	}
	// YAML may omit protocol; the API server defaults it to TCP. Compare
	// effective protocols without modifying immutable release templates.
	a, b = a.DeepCopy(), b.DeepCopy()
	for _, s := range []*corev1.Service{a, b} {
		for i := range s.Spec.Ports {
			if s.Spec.Ports[i].Protocol == "" {
				s.Spec.Ports[i].Protocol = corev1.ProtocolTCP
			}
		}
	}
	for _, s := range []*corev1.Service{a, b} {
		if s == nil || s.Name != "chatgpt-channel" || !validName(s.Namespace) || s.Namespace == "default" || s.Namespace != a.Namespace ||
			s.Spec.Type != corev1.ServiceTypeClusterIP || s.Spec.ExternalName != "" || len(s.Spec.ExternalIPs) != 0 ||
			len(s.Spec.Ports) < 1 || len(s.Spec.Ports) > 2 {
			return false
		}
		management := s.Spec.Ports[0]
		if management.Name != "management" || management.Port != 8000 || management.Protocol != corev1.ProtocolTCP ||
			management.NodePort != 0 || management.TargetPort != intstr.FromString("management") {
			return false
		}
		if len(s.Spec.Ports) == 2 && !reflect.DeepEqual(s.Spec.Ports[1], corev1.ServicePort{
			Name: "execution", Port: 8001, TargetPort: intstr.FromInt32(8001), Protocol: corev1.ProtocolTCP,
		}) {
			return false
		}
	}
	if len(a.Spec.Ports) == len(b.Spec.Ports) || !reflect.DeepEqual(a.Spec.Ports[0], b.Spec.Ports[0]) {
		return false
	}
	compare := b.DeepCopy()
	compare.Spec.Ports = a.Spec.Ports
	return reflect.DeepEqual(a.Spec, compare.Spec)
}
