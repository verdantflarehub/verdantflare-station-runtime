package executor

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"reflect"
)

// Project's optional TLS peer reader changes only its existing internal Service.
// The source checkpoint and stopped routing fence remain authoritative.
func projectReadPorts(source, target Target) bool {
	if source.AppID != "project" || target.AppID != "project" || len(source.Services) != 1 || len(target.Services) != 1 {
		return false
	}
	a, b := source.Services[0], target.Services[0]
	if a == nil || b == nil {
		return false
	}
	a, b = a.DeepCopy(), b.DeepCopy()
	for _, s := range []*corev1.Service{a, b} {
		for i := range s.Spec.Ports {
			if s.Spec.Ports[i].Protocol == "" {
				s.Spec.Ports[i].Protocol = corev1.ProtocolTCP
			}
		}
		if s.Name != "app-project" || s.Namespace != "verdantflare-station" || s.Spec.Type != corev1.ServiceTypeClusterIP || s.Spec.ExternalName != "" || len(s.Spec.ExternalIPs) != 0 || (len(s.Spec.Ports) != 2 && len(s.Spec.Ports) != 3) {
			return false
		}
		want := []corev1.ServicePort{{Name: "business", Port: 8443, TargetPort: intstr.FromString("business"), Protocol: corev1.ProtocolTCP}, {Name: "mcp", Port: 8444, TargetPort: intstr.FromString("mcp"), Protocol: corev1.ProtocolTCP}}
		if len(s.Spec.Ports) == 3 {
			want = append(want, corev1.ServicePort{Name: "peer-read", Port: 8096, TargetPort: intstr.FromString("peer-read"), Protocol: corev1.ProtocolTCP})
		}
		if !reflect.DeepEqual(s.Spec.Ports, want) {
			return false
		}
	}
	if len(a.Spec.Ports) == len(b.Spec.Ports) {
		return false
	}
	compare := b.DeepCopy()
	compare.Spec.Ports = a.Spec.Ports
	if !reflect.DeepEqual(a.Spec, compare.Spec) {
		return false
	}
	for _, t := range []Target{source, target} {
		d := t.Deployment
		if d == nil || d.Name != "app-project" || d.Namespace != "verdantflare-station" || len(d.Spec.Template.Spec.Containers) != 1 || len(d.Spec.Template.Spec.InitContainers) != 0 {
			return false
		}
		c := d.Spec.Template.Spec.Containers[0]
		want := []corev1.ContainerPort{{Name: "business", ContainerPort: 8443, Protocol: corev1.ProtocolTCP}, {Name: "mcp", ContainerPort: 8444, Protocol: corev1.ProtocolTCP}}
		reader := len(t.Services[0].Spec.Ports) == 3
		if reader {
			want = append(want, corev1.ContainerPort{Name: "peer-read", ContainerPort: 8096, Protocol: corev1.ProtocolTCP})
		}
		ports := append([]corev1.ContainerPort(nil), c.Ports...)
		for i := range ports {
			if ports[i].Protocol == "" {
				ports[i].Protocol = corev1.ProtocolTCP
			}
		}
		if !reflect.DeepEqual(ports, want) {
			return false
		}
		if reader {
			env := map[string]corev1.EnvVar{}
			for _, e := range c.Env {
				if _, exists := env[e.Name]; exists {
					return false
				}
				env[e.Name] = e
			}
			if env["PROJECT_READ_LISTEN"].Value != "0.0.0.0:8096" || env["PROJECT_TLS_CERT"].Value == "" || env["PROJECT_TLS_KEY"].Value == "" {
				return false
			}
			ref := env["PROJECT_READ_TOKEN"]
			if ref.Value != "" || ref.ValueFrom == nil || ref.ValueFrom.SecretKeyRef == nil || ref.ValueFrom.SecretKeyRef.Name == "" || ref.ValueFrom.SecretKeyRef.Key == "" || (ref.ValueFrom.SecretKeyRef.Optional != nil && *ref.ValueFrom.SecretKeyRef.Optional) {
				return false
			}
			for _, key := range []string{"PROJECT_SERVICE_TOKEN", "PROJECT_AUTHORITY_TOKEN", "PROJECT_COORD_TOKEN", "ARTIFACT_SERVICE_TOKEN", "WORLD_SERVICE_TOKEN", "WORLD_COORD_TOKEN"} {
				if env[key].ValueFrom != nil && reflect.DeepEqual(ref.ValueFrom, env[key].ValueFrom) {
					return false
				}
			}
		}
	}
	return true
}

func stoppedServicePortsChange(source, target Target) bool {
	return chatgptExecutionPorts(source, target) || projectReadPorts(source, target)
}
