package executor

import (
	"context"
	"reflect"

	pb "github.com/verdantflarehub/verdantflare-station-runtime/api/runtimev1"
	"github.com/verdantflarehub/verdantflare-station-runtime/internal/computepolicy"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const stoppedOperationKey = "station.verdantflare.com/stopped-transition"

// StoppedReplacement is the ordinary downtime deployment driver. It reuses
// the original checkpoint, Kubernetes ownership and resourceVersion checks.
// Business checks and Core's version commit belong to the coordinator; only
// the coordinator may restore routing after those checks have succeeded.
type StoppedReplacement struct{ *ManagementReplacement }

func NewStoppedReplacement(k *Kubernetes, source, target Target, in *pb.AppRequest) (*StoppedReplacement, error) {
	r, err := newManagementReplacement(k, source, target, in, true)
	if err != nil {
		return nil, err
	}
	return &StoppedReplacement{r}, nil
}

func (r *StoppedReplacement) gatedSelector() map[string]string {
	return map[string]string{stoppedOperationKey: r.OperationID}
}

func (r *StoppedReplacement) liveDeployment(ctx context.Context, c ManagementCheckpoint) (*appsv1.Deployment, bool, error) {
	if !r.validCheckpoint(c) {
		return nil, false, ErrManagementReplacement
	}
	d, err := r.Kubernetes.Client.AppsV1().Deployments(c.Deployment.Namespace).Get(ctx, c.Deployment.Name, metav1.GetOptions{})
	if err != nil {
		return nil, false, err
	}
	if d.UID != c.Deployment.UID || d.ResourceVersion == "" || d.DeletionTimestamp != nil || d.Spec.Replicas == nil || (*d.Spec.Replicas != 0 && *d.Spec.Replicas != 1) {
		return nil, false, ErrManagementReplacement
	}
	compare := d.DeepCopy()
	*compare.Spec.Replicas = 1
	if r.deploymentMatches(c.Deployment, compare, r.Source) && (originalMarker(d.Annotations, c.Deployment.Annotations) || d.Annotations[transitionOperationKey] == r.OperationID) {
		return d, false, nil
	}
	if r.deploymentMatches(r.Target.Deployment, compare, r.Target) && r.targetMarked(d.Annotations) {
		return d, true, nil
	}
	return nil, false, ErrManagementReplacement
}

func (r *StoppedReplacement) liveService(ctx context.Context, original *corev1.Service) (*corev1.Service, bool, error) {
	live, err := r.Kubernetes.Client.CoreV1().Services(original.Namespace).Get(ctx, original.Name, metav1.GetOptions{})
	if err != nil {
		return nil, false, err
	}
	if live.UID != original.UID || live.ResourceVersion == "" || live.DeletionTimestamp != nil {
		return nil, false, ErrManagementReplacement
	}
	gated := reflect.DeepEqual(live.Spec.Selector, r.gatedSelector()) && live.Annotations[stoppedOperationKey] == r.OperationID
	if !gated && live.Annotations[stoppedOperationKey] != "" {
		return nil, false, ErrManagementReplacement
	}
	compare := live.DeepCopy()
	if gated {
		compare.Spec.Selector = original.Spec.Selector
	}
	want := original
	if r.targetMarked(live.Annotations) && stoppedServicePortsChange(r.Source, r.Target) {
		want = original.DeepCopy()
		want.Spec.Ports = r.Target.Services[0].Spec.Ports
	}
	if !preinstalledServiceCompatible(want, compare) || !((owned(live.Annotations, r.Source, r.StationID, r.OrganizationID) && originalMarker(live.Annotations, original.Annotations)) || r.targetMarked(live.Annotations)) {
		return nil, false, ErrManagementReplacement
	}
	return live, gated, nil
}

func (r *StoppedReplacement) routesGated(ctx context.Context, c ManagementCheckpoint) (bool, error) {
	if len(c.Services) == 0 {
		return false, ErrManagementReplacement
	}
	for _, original := range c.Services {
		_, gated, err := r.liveService(ctx, original)
		if err != nil || !gated {
			return false, err
		}
		// A Service update is not yet a routing barrier: wait for the
		// EndpointSlice controller to remove the old routable addresses.
		slices, err := r.Kubernetes.Client.DiscoveryV1().EndpointSlices(original.Namespace).List(ctx, metav1.ListOptions{LabelSelector: "kubernetes.io/service-name=" + original.Name})
		if err != nil {
			return false, err
		}
		for _, slice := range slices.Items {
			if len(slice.Endpoints) != 0 {
				return false, nil
			}
		}
	}
	return true, nil
}

// Gate changes at most one Service, before stopping any process. Existing
// requests are drained by termination; routing isolation alone is not idle.
func (r *StoppedReplacement) Gate(ctx context.Context, c ManagementCheckpoint) (bool, error) {
	if _, _, err := r.liveDeployment(ctx, c); err != nil {
		return false, err
	}
	if len(c.Services) == 0 {
		return false, ErrManagementReplacement
	}
	// The fencing selector must not match either registered application template.
	for _, target := range []Target{r.Source, r.Target} {
		if target.Deployment.Spec.Template.Labels[stoppedOperationKey] != "" {
			return false, ErrManagementReplacement
		}
	}
	for _, original := range c.Services {
		live, gated, err := r.liveService(ctx, original)
		if err != nil {
			return false, err
		}
		if gated {
			continue
		}
		live.Spec.Selector = r.gatedSelector()
		live.Annotations[stoppedOperationKey] = r.OperationID
		_, err = r.Kubernetes.Client.CoreV1().Services(live.Namespace).Update(ctx, live, metav1.UpdateOptions{})
		return false, err
	}
	return r.routesGated(ctx, c)
}

func (r *StoppedReplacement) gone(ctx context.Context, d *appsv1.Deployment) (bool, error) {
	selector, err := metav1.LabelSelectorAsSelector(d.Spec.Selector)
	if err != nil || selector.Empty() {
		return false, ErrManagementReplacement
	}
	pods, err := r.Kubernetes.Client.CoreV1().Pods(d.Namespace).List(ctx, metav1.ListOptions{LabelSelector: selector.String()})
	if err != nil {
		return false, err
	}
	return *d.Spec.Replicas == 0 && d.Status.ObservedGeneration >= d.Generation && d.Status.Replicas == 0 && len(pods.Items) == 0, nil
}

// Stop never force-deletes Pods. Even a terminating Pod is a possible writer.
func (r *StoppedReplacement) Stop(ctx context.Context, c ManagementCheckpoint) (bool, error) {
	d, _, err := r.liveDeployment(ctx, c)
	if err != nil {
		return false, err
	}
	if gated, err := r.routesGated(ctx, c); err != nil || !gated {
		if err == nil {
			err = ErrManagementReplacement
		}
		return false, err
	}
	if *d.Spec.Replicas != 0 {
		*d.Spec.Replicas = 0
		d.Annotations[transitionOperationKey] = r.OperationID
		_, err = r.Kubernetes.Client.AppsV1().Deployments(d.Namespace).Update(ctx, d, metav1.UpdateOptions{})
		return false, err
	}
	return r.gone(ctx, d)
}

// Start is direction-bound and requires a fully stopped previous process.
// Routing stays closed while the coordinator verifies the started release.
func (r *StoppedReplacement) Start(ctx context.Context, c ManagementCheckpoint, target bool) (bool, error) {
	d, isTarget, err := r.liveDeployment(ctx, c)
	if err != nil {
		return false, err
	}
	if gated, err := r.routesGated(ctx, c); err != nil || !gated {
		if err == nil {
			err = ErrManagementReplacement
		}
		return false, err
	}
	if *d.Spec.Replicas == 1 {
		if isTarget != target {
			return false, ErrManagementReplacement
		}
		// Reuse Kubernetes' rollout deadline instead of introducing another
		// timeout record. Ignore a condition left over from an older generation.
		if d.Status.ObservedGeneration >= d.Generation {
			for _, condition := range d.Status.Conditions {
				if condition.Type == appsv1.DeploymentProgressing && condition.Status == corev1.ConditionFalse && condition.Reason == "ProgressDeadlineExceeded" {
					return false, ErrManagementReplacement
				}
			}
		}
		return managementReady(d), nil
	}
	if gone, err := r.gone(ctx, d); err != nil || !gone {
		return false, err
	}
	if target {
		d.Spec = *r.Target.Deployment.Spec.DeepCopy()
		mark(&d.ObjectMeta, r.Target, r.StationID, r.OrganizationID)
	} else {
		d.Spec = *c.Deployment.Spec.DeepCopy()
		restoreManagementMarks(d.Annotations, c.Deployment.Annotations)
	}
	computepolicy.Apply(&d.Spec.Template.Spec)
	d.Annotations[transitionOperationKey] = r.OperationID
	_, err = r.Kubernetes.Client.AppsV1().Deployments(d.Namespace).Update(ctx, d, metav1.UpdateOptions{})
	return false, err
}

// Open is called only after the coordinator's version commit. Replays keep
// the same direction and do not touch application data or credentials.
func (r *StoppedReplacement) Open(ctx context.Context, c ManagementCheckpoint, target bool) (bool, error) {
	d, isTarget, err := r.liveDeployment(ctx, c)
	if err != nil {
		return false, err
	}
	if isTarget != target || !managementReady(d) {
		return false, ErrManagementReplacement
	}
	for _, original := range c.Services {
		live, gated, err := r.liveService(ctx, original)
		if err != nil {
			return false, err
		}
		if !gated {
			if target != r.targetMarked(live.Annotations) {
				return false, ErrManagementReplacement
			}
			continue
		}
		live.Spec.Selector = original.Spec.Selector
		if stoppedServicePortsChange(r.Source, r.Target) {
			live.Spec.Ports = original.DeepCopy().Spec.Ports
			if target {
				live.Spec.Ports = r.Target.Services[0].DeepCopy().Spec.Ports
			}
		}
		delete(live.Annotations, stoppedOperationKey)
		if target {
			mark(&live.ObjectMeta, r.Target, r.StationID, r.OrganizationID)
			live.Annotations[transitionOperationKey] = r.OperationID
		} else {
			restoreManagementMarks(live.Annotations, original.Annotations)
		}
		_, err = r.Kubernetes.Client.CoreV1().Services(live.Namespace).Update(ctx, live, metav1.UpdateOptions{})
		return false, err
	}
	return true, nil
}
