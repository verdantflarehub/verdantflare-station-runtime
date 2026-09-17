package executor

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

const ownerKey = "station.verdantflare.com/station-id"
const orgKey = "station.verdantflare.com/organization-id"
const appKey = "station.verdantflare.com/app-id"
const hashKey = "station.verdantflare.com/template-hash"

type Result struct{ Status, Phase, Code, Message string }

func failed(code, message string) Result { return Result{"failed", "checking", code, message} }

type Driver interface {
	Step(context.Context, Target, string, string, string, string) (Result, error)
}
type Kubernetes struct{ Client kubernetes.Interface }

func NewKubernetes(path, contextName string) (*Kubernetes, error) {
	var cfg *rest.Config
	var err error
	if path != "" {
		if contextName == "" {
			return nil, errors.New("explicit Kubernetes context required")
		}
		rules := &clientcmd.ClientConfigLoadingRules{ExplicitPath: path}
		cfg, err = clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, &clientcmd.ConfigOverrides{CurrentContext: contextName}).ClientConfig()
	} else {
		cfg, err = rest.InClusterConfig()
	}
	if err != nil {
		return nil, err
	}
	cfg.Timeout = 3 * time.Second
	cfg.ContentType = "application/json"
	cfg.AcceptContentTypes = "application/json"
	c, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	return &Kubernetes{c}, nil
}
func annotations(t Target, station, org string) map[string]string {
	return map[string]string{ownerKey: station, orgKey: org, appKey: t.AppID, hashKey: hex.EncodeToString(t.Hash[:])}
}
func owned(a map[string]string, t Target, station, org string) bool {
	for k, v := range annotations(t, station, org) {
		if a[k] != v {
			return false
		}
	}
	return true
}
func mark(meta *metav1.ObjectMeta, t Target, station, org string) {
	if meta.Annotations == nil {
		meta.Annotations = map[string]string{}
	}
	for k, v := range annotations(t, station, org) {
		meta.Annotations[k] = v
	}
}
func (k *Kubernetes) Step(ctx context.Context, t Target, action, operation, station, org string) (Result, error) {
	// No task/lease API exists yet: disruptive commands must fail closed.
	if action == "stop" || action == "restart" || action == "delete" {
		return activityGuard(ctx, t), nil
	}
	if action != "adopt" && t.DiskBytes > 0 {
		return failed("SERVICE_UNAVAILABLE", "Storage capacity preflight is not connected"), nil
	}
	if action != "adopt" && (len(t.Models) > 0 || t.GPU > 0) {
		return failed("SERVICE_UNAVAILABLE", "Model preparation and GPU readiness adapters are not connected"), nil
	}
	deployments := k.Client.AppsV1().Deployments(t.Deployment.Namespace)
	live, err := deployments.Get(ctx, t.Deployment.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		if action != "install" {
			return failed("NOT_FOUND", "Application is not installed"), nil
		}
		if err = k.dependencies(ctx, t); err != nil {
			return failed("SERVICE_UNAVAILABLE", err.Error()), nil
		}
		d := t.Deployment.DeepCopy()
		mark(&d.ObjectMeta, t, station, org)
		_, err = deployments.Create(ctx, d, metav1.CreateOptions{})
		if apierrors.IsAlreadyExists(err) {
			return Result{"running", "installing", "", ""}, nil
		}
		if err != nil {
			return Result{}, err
		}
		return Result{"running", "installing", "", ""}, nil
	}
	if err != nil {
		return Result{}, err
	}
	if action == "adopt" {
		return k.adopt(ctx, t, live, station, org)
	}
	if !owned(live.Annotations, t, station, org) {
		return failed("CONFLICT", "Existing workload is not owned by this registered installation; explicit adoption is required"), nil
	}
	if live.DeletionTimestamp != nil {
		return failed("CONFLICT", "Workload is being deleted"), nil
	}
	if !sameImages(live, t.Deployment) {
		return failed("CONFLICT", "Workload images differ from the registered version"), nil
	}
	if live.Spec.Replicas != nil && *live.Spec.Replicas == 0 {
		live.Spec.Replicas = t.Deployment.Spec.Replicas
		_, err = deployments.Update(ctx, live, metav1.UpdateOptions{})
		return Result{"running", "starting", "", ""}, err
	}
	for _, wanted := range t.Services {
		services := k.Client.CoreV1().Services(wanted.Namespace)
		existing, e := services.Get(ctx, wanted.Name, metav1.GetOptions{})
		if apierrors.IsNotFound(e) {
			svc := wanted.DeepCopy()
			mark(&svc.ObjectMeta, t, station, org)
			_, e = services.Create(ctx, svc, metav1.CreateOptions{})
			if apierrors.IsAlreadyExists(e) {
				return Result{"running", "installing", "", ""}, nil
			}
		} else if e == nil && !owned(existing.Annotations, t, station, org) {
			return failed("CONFLICT", "Existing Service is not owned by this installation"), nil
		}
		if e != nil {
			return Result{}, e
		}
	}
	for _, c := range live.Status.Conditions {
		if c.Type == appsv1.DeploymentProgressing && c.Status == corev1.ConditionFalse && c.Reason == "ProgressDeadlineExceeded" || c.Type == appsv1.DeploymentReplicaFailure && c.Status == corev1.ConditionTrue {
			return Result{"failed", "starting", "SERVICE_UNAVAILABLE", "Deployment failed to become ready"}, nil
		}
	}
	desired := int32(1)
	if live.Spec.Replicas != nil {
		desired = *live.Spec.Replicas
	}
	if desired > 0 && live.Status.ObservedGeneration >= live.Generation && live.Status.Replicas == desired && live.Status.UpdatedReplicas == desired && live.Status.ReadyReplicas == desired && live.Status.AvailableReplicas == desired {
		return Result{"succeeded", "completed", "", ""}, nil
	}
	return Result{"running", "starting", "", ""}, nil
}
func sameImages(a, b *appsv1.Deployment) bool {
	ac := append(append([]corev1.Container{}, a.Spec.Template.Spec.Containers...), a.Spec.Template.Spec.InitContainers...)
	bc := append(append([]corev1.Container{}, b.Spec.Template.Spec.Containers...), b.Spec.Template.Spec.InitContainers...)
	if len(ac) != len(bc) {
		return false
	}
	images := map[string]string{}
	for _, c := range bc {
		images[c.Name] = c.Image
	}
	for _, c := range ac {
		if images[c.Name] != c.Image {
			return false
		}
	}
	return true
}
func (k *Kubernetes) dependencies(ctx context.Context, t Target) error {
	ns := t.Deployment.Namespace
	for _, v := range t.Deployment.Spec.Template.Spec.Volumes {
		if v.PersistentVolumeClaim != nil {
			p, e := k.Client.CoreV1().PersistentVolumeClaims(ns).Get(ctx, v.PersistentVolumeClaim.ClaimName, metav1.GetOptions{})
			if e != nil || p.Status.Phase != corev1.ClaimBound {
				return fmt.Errorf("Required PVC is unavailable or unbound")
			}
		}
		if v.Secret != nil && (v.Secret.Optional == nil || !*v.Secret.Optional) {
			if _, e := k.Client.CoreV1().Secrets(ns).Get(ctx, v.Secret.SecretName, metav1.GetOptions{}); e != nil {
				return fmt.Errorf("Required Secret is unavailable")
			}
		}
		if v.ConfigMap != nil && (v.ConfigMap.Optional == nil || !*v.ConfigMap.Optional) {
			if _, e := k.Client.CoreV1().ConfigMaps(ns).Get(ctx, v.ConfigMap.Name, metav1.GetOptions{}); e != nil {
				return fmt.Errorf("Required ConfigMap is unavailable")
			}
		}
	}
	return nil
}
