package executor

import (
	"encoding/json"
	"net"
	"reflect"

	"github.com/verdantflarehub/verdantflare-station-runtime/internal/computepolicy"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
)

// Only documented API defaults are normalized. Extra execution/security fields
// remain part of the strict comparison; unknown future defaults fail closed.
func preinstalledObject(value any) map[string]any {
	raw, _ := json.Marshal(value)
	var out map[string]any
	_ = json.Unmarshal(raw, &out)
	return out
}
func defaultValue(m map[string]any, key string, value any) {
	if _, exists := m[key]; !exists {
		m[key] = value
	}
}
func objectAt(m map[string]any, key string) map[string]any {
	child, ok := m[key].(map[string]any)
	if !ok {
		child = map[string]any{}
		m[key] = child
	}
	return child
}
func normalizePreinstalledPod(pod map[string]any) {
	for key, value := range map[string]any{"restartPolicy": "Always", "terminationGracePeriodSeconds": float64(30), "dnsPolicy": "ClusterFirst", "serviceAccountName": "default", "schedulerName": "default-scheduler", "securityContext": map[string]any{}, "enableServiceLinks": true, "preemptionPolicy": "PreemptLowerPriority"} {
		defaultValue(pod, key, value)
	}
	defaultValue(pod, "serviceAccount", pod["serviceAccountName"])
	for _, field := range []string{"containers", "initContainers"} {
		list, _ := pod[field].([]any)
		for _, entry := range list {
			c, ok := entry.(map[string]any)
			if !ok {
				continue
			}
			defaultValue(c, "terminationMessagePath", "/dev/termination-log")
			defaultValue(c, "terminationMessagePolicy", "File")
			ports, _ := c["ports"].([]any)
			for _, entry := range ports {
				if port, ok := entry.(map[string]any); ok {
					defaultValue(port, "protocol", "TCP")
				}
			}
			for _, name := range []string{"livenessProbe", "readinessProbe", "startupProbe"} {
				probe, ok := c[name].(map[string]any)
				if !ok {
					continue
				}
				for key, value := range map[string]any{"timeoutSeconds": float64(1), "periodSeconds": float64(10), "successThreshold": float64(1), "failureThreshold": float64(3)} {
					defaultValue(probe, key, value)
				}
				if http, ok := probe["httpGet"].(map[string]any); ok {
					defaultValue(http, "scheme", "HTTP")
				}
			}
			envs, _ := c["env"].([]any)
			for _, entry := range envs {
				env, ok := entry.(map[string]any)
				if !ok {
					continue
				}
				from, _ := env["valueFrom"].(map[string]any)
				if ref, ok := from["fieldRef"].(map[string]any); ok {
					defaultValue(ref, "apiVersion", "v1")
				}
			}
		}
	}
	volumes, _ := pod["volumes"].([]any)
	for _, entry := range volumes {
		volume, ok := entry.(map[string]any)
		if !ok {
			continue
		}
		for _, field := range []string{"secret", "configMap", "downwardAPI", "projected"} {
			if source, ok := volume[field].(map[string]any); ok {
				defaultValue(source, "defaultMode", float64(420))
			}
		}
	}
}
func normalizePreinstalledDeployment(spec map[string]any) {
	delete(spec, "replicas") // Adoption/start does not redefine scale ownership.
	defaultValue(spec, "revisionHistoryLimit", float64(10))
	defaultValue(spec, "progressDeadlineSeconds", float64(600))
	strategy := objectAt(spec, "strategy")
	defaultValue(strategy, "type", "RollingUpdate")
	if strategy["type"] == "RollingUpdate" {
		rolling := objectAt(strategy, "rollingUpdate")
		defaultValue(rolling, "maxUnavailable", "25%")
		defaultValue(rolling, "maxSurge", "25%")
	}
	normalizePreinstalledPod(objectAt(objectAt(spec, "template"), "spec"))
}
func preinstalledDeploymentCompatible(want, live *appsv1.Deployment) bool {
	if preinstalledDeploymentExact(want, live) {
		return true
	}
	if !computepolicy.Enabled() {
		return false
	}
	copy := want.DeepCopy()
	computepolicy.Apply(&copy.Spec.Template.Spec)
	return preinstalledDeploymentExact(copy, live)
}
func preinstalledDeploymentExact(want, live *appsv1.Deployment) bool {
	w, l := preinstalledObject(want.Spec), preinstalledObject(live.Spec)
	normalizePreinstalledDeployment(w)
	normalizePreinstalledDeployment(l)
	return reflect.DeepEqual(w, l)
}
func preinstalledServiceCompatible(want, live *corev1.Service) bool {
	w, l := preinstalledObject(want.Spec), preinstalledObject(live.Spec)
	for _, spec := range []map[string]any{w, l} {
		defaultValue(spec, "type", "ClusterIP")
		defaultValue(spec, "sessionAffinity", "None")
		defaultValue(spec, "internalTrafficPolicy", "Cluster")
		defaultValue(spec, "ipFamilyPolicy", "SingleStack")
		ports, _ := spec["ports"].([]any)
		for _, entry := range ports {
			if port, ok := entry.(map[string]any); ok {
				defaultValue(port, "protocol", "TCP")
				defaultValue(port, "targetPort", port["port"])
				// IntOrString's zero value serializes as 0; Kubernetes defaults it
				// to the Service port just like an omitted targetPort.
				if port["targetPort"] == float64(0) {
					port["targetPort"] = port["port"]
				}
			}
		}
	}
	// Only these server-assigned network fields may be absent from the template.
	// Explicit headless/family/allocation declarations remain exact.
	if _, declared := w["clusterIP"]; !declared {
		if value, exists := l["clusterIP"]; exists && net.ParseIP(value.(string)) == nil {
			return false
		}
	}
	if _, declared := w["clusterIPs"]; !declared {
		if values, exists := l["clusterIPs"].([]any); exists {
			if len(values) == 0 || len(values) > 2 || values[0] != l["clusterIP"] {
				return false
			}
			for _, value := range values {
				if net.ParseIP(value.(string)) == nil {
					return false
				}
			}
		}
	}
	for _, field := range []string{"clusterIP", "clusterIPs", "ipFamilies"} {
		if _, exists := w[field]; !exists {
			delete(l, field)
		}
	}
	return reflect.DeepEqual(w, l)
}
