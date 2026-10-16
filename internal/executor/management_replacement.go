package executor

import (
	"context"
	"errors"
	"reflect"

	"github.com/google/uuid"
	pb "github.com/verdantflarehub/verdantflare-station-runtime/api/runtimev1"
	"github.com/verdantflarehub/verdantflare-station-runtime/internal/computepolicy"
	"google.golang.org/protobuf/proto"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var ErrManagementReplacement = errors.New("MANAGEMENT_REPLACEMENT_CONFLICT")

const transitionOperationKey = "station.verdantflare.com/installation-transition"

// ManagementCheckpoint must be durably saved BEFORE sealing or applying. The
// coordinator owns authentication, maintenance proofs and the persistent phase.
// This driver has no public endpoint and never creates or deletes resources.
type ManagementCheckpoint struct {
	Deployment *appsv1.Deployment `json:"deployment"`
	Services   []*corev1.Service  `json:"services"`
}

type ManagementReplacement struct {
	Source, Target                         Target
	OperationID, StationID, OrganizationID string
	Kubernetes                             *Kubernetes
}

func NewManagementReplacement(k *Kubernetes, source, target Target, in *pb.AppRequest) (*ManagementReplacement, error) {
	return newManagementReplacement(k, source, target, in, false)
}

func newManagementReplacement(k *Kubernetes, source, target Target, in *pb.AppRequest, stopped bool) (*ManagementReplacement, error) {
	if k == nil || k.Client == nil || in == nil {
		return nil, ErrManagementReplacement
	}
	for _, value := range []string{in.OperationId, in.InstallationId, in.StationId, in.OrganizationId} {
		id, err := uuid.Parse(value)
		if err != nil || id.Version() != 7 || id.Variant() != uuid.RFC4122 || id.String() != value {
			return nil, ErrManagementReplacement
		}
	}
	if source.AppID != in.AppId || source.Version != in.AppVersion || target.AppID != source.AppID || target.Version == source.Version ||
		!versionPattern.MatchString(target.Version) || (!stopped && (len(source.InstallationBindings) == 0 || len(target.InstallationBindings) == 0)) ||
		source.Deployment == nil || target.Deployment == nil || source.Deployment.Name != target.Deployment.Name || source.Deployment.Namespace != target.Deployment.Namespace ||
		!oneReplica(source.Deployment) || !oneReplica(target.Deployment) || source.Hash == target.Hash {
		return nil, ErrManagementReplacement
	}
	// Management-only compatibility deliberately permits image/env changes only.
	// All volume, command, resource and security fields must remain equivalent.
	compare := target.Deployment.DeepCopy()
	for _, pair := range [][2][]corev1.Container{{source.Deployment.Spec.Template.Spec.Containers, compare.Spec.Template.Spec.Containers},
		{source.Deployment.Spec.Template.Spec.InitContainers, compare.Spec.Template.Spec.InitContainers}} {
		if len(pair[0]) != len(pair[1]) {
			return nil, ErrManagementReplacement
		}
		for i := range pair[0] {
			if pair[0][i].Name != pair[1][i].Name {
				return nil, ErrManagementReplacement
			}
			pair[1][i].Image, pair[1][i].Env = pair[0][i].Image, pair[0][i].Env
		}
	}
	if !preinstalledDeploymentCompatible(source.Deployment, compare) || len(source.Services) != len(target.Services) {
		return nil, ErrManagementReplacement
	}
	for i, service := range source.Services {
		other := target.Services[i]
		if service == nil || other == nil || service.Name != other.Name || service.Namespace != other.Namespace ||
			!reflect.DeepEqual(service.Spec, other.Spec) {
			return nil, ErrManagementReplacement
		}
	}
	boundSource, err := bindInstallation(source, in)
	if err != nil {
		return nil, ErrManagementReplacement
	}
	request := proto.Clone(in).(*pb.AppRequest)
	request.AppVersion = target.Version
	boundTarget, err := bindInstallation(target, request)
	if err != nil {
		return nil, ErrManagementReplacement
	}
	if stopped {
		boundSource.InstallationID, boundTarget.InstallationID = in.InstallationId, in.InstallationId
	}
	return &ManagementReplacement{boundSource, boundTarget, in.OperationId, in.StationId, in.OrganizationId, k}, nil
}

func oneReplica(d *appsv1.Deployment) bool { return d.Spec.Replicas != nil && *d.Spec.Replicas == 1 }
func managementReady(d *appsv1.Deployment) bool {
	return oneReplica(d) && d.DeletionTimestamp == nil && d.Status.ObservedGeneration >= d.Generation &&
		d.Status.Replicas == 1 && d.Status.UpdatedReplicas == 1 && d.Status.ReadyReplicas == 1 && d.Status.AvailableReplicas == 1
}

func (r *ManagementReplacement) deploymentMatches(want, live *appsv1.Deployment, target Target) bool {
	return live != nil && live.DeletionTimestamp == nil && oneReplica(live) && live.Name == want.Name && live.Namespace == want.Namespace &&
		owned(live.Annotations, target, r.StationID, r.OrganizationID) && preinstalledDeploymentCompatible(want, live)
}

func (r *ManagementReplacement) Capture(ctx context.Context) (ManagementCheckpoint, error) {
	var checkpoint ManagementCheckpoint
	d, err := r.Kubernetes.Client.AppsV1().Deployments(r.Source.Deployment.Namespace).Get(ctx, r.Source.Deployment.Name, metav1.GetOptions{})
	if err != nil {
		return checkpoint, err
	}
	if d.UID == "" || d.ResourceVersion == "" || !r.deploymentMatches(r.Source.Deployment, d, r.Source) || !managementReady(d) {
		return checkpoint, ErrManagementReplacement
	}
	checkpoint.Deployment, checkpoint.Services = d.DeepCopy(), []*corev1.Service{}
	for _, want := range r.Source.Services {
		live, err := r.Kubernetes.Client.CoreV1().Services(want.Namespace).Get(ctx, want.Name, metav1.GetOptions{})
		if err != nil {
			return ManagementCheckpoint{}, err
		}
		if live.UID == "" || (want.UID != "" && want.UID != live.UID) || live.ResourceVersion == "" || live.DeletionTimestamp != nil ||
			!owned(live.Annotations, r.Source, r.StationID, r.OrganizationID) || !preinstalledServiceCompatible(want, live) {
			return ManagementCheckpoint{}, ErrManagementReplacement
		}
		checkpoint.Services = append(checkpoint.Services, live.DeepCopy())
	}
	return checkpoint, nil
}

func (r *ManagementReplacement) validCheckpoint(c ManagementCheckpoint) bool {
	if c.Deployment == nil || c.Deployment.UID == "" || c.Deployment.ResourceVersion == "" ||
		!r.deploymentMatches(r.Source.Deployment, c.Deployment, r.Source) || len(c.Services) != len(r.Source.Services) {
		return false
	}
	for i, service := range c.Services {
		want := r.Source.Services[i]
		if service == nil || service.UID == "" || (want.UID != "" && want.UID != service.UID) || service.ResourceVersion == "" || service.DeletionTimestamp != nil ||
			service.Name != want.Name || service.Namespace != want.Namespace || !owned(service.Annotations, r.Source, r.StationID, r.OrganizationID) ||
			!preinstalledServiceCompatible(want, service) {
			return false
		}
	}
	return true
}

func (r *ManagementReplacement) targetMarked(annotations map[string]string) bool {
	return owned(annotations, r.Target, r.StationID, r.OrganizationID) && annotations[transitionOperationKey] == r.OperationID
}

func originalMarker(live, original map[string]string) bool {
	return live[transitionOperationKey] == original[transitionOperationKey]
}

// Apply updates at most one object per call. true means Kubernetes is ready,
// not that App proof passed or that Core may skip its publication transaction.
func (r *ManagementReplacement) Apply(ctx context.Context, c ManagementCheckpoint) (bool, error) {
	if !r.validCheckpoint(c) {
		return false, ErrManagementReplacement
	}
	// Inspect Deployment before touching Services, including partial retries.
	d, err := r.Kubernetes.Client.AppsV1().Deployments(c.Deployment.Namespace).Get(ctx, c.Deployment.Name, metav1.GetOptions{})
	if err != nil {
		return false, err
	}
	if d.UID != c.Deployment.UID || d.ResourceVersion == "" ||
		!((r.deploymentMatches(c.Deployment, d, r.Source) && originalMarker(d.Annotations, c.Deployment.Annotations)) ||
			(r.deploymentMatches(r.Target.Deployment, d, r.Target) && r.targetMarked(d.Annotations))) {
		return false, ErrManagementReplacement
	}
	for _, original := range c.Services {
		client := r.Kubernetes.Client.CoreV1().Services(original.Namespace)
		live, err := client.Get(ctx, original.Name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		if live.UID != original.UID || live.ResourceVersion == "" || live.DeletionTimestamp != nil || !preinstalledServiceCompatible(original, live) {
			return false, ErrManagementReplacement
		}
		if r.targetMarked(live.Annotations) {
			continue
		}
		if !owned(live.Annotations, r.Source, r.StationID, r.OrganizationID) || !originalMarker(live.Annotations, original.Annotations) {
			return false, ErrManagementReplacement
		}
		mark(&live.ObjectMeta, r.Target, r.StationID, r.OrganizationID)
		live.Annotations[transitionOperationKey] = r.OperationID
		_, err = client.Update(ctx, live, metav1.UpdateOptions{})
		return false, err
	}
	if r.targetMarked(d.Annotations) {
		return managementReady(d), nil
	}
	d.Spec = *r.Target.Deployment.Spec.DeepCopy()
	computepolicy.Apply(&d.Spec.Template.Spec)
	mark(&d.ObjectMeta, r.Target, r.StationID, r.OrganizationID)
	d.Annotations[transitionOperationKey] = r.OperationID
	_, err = r.Kubernetes.Client.AppsV1().Deployments(d.Namespace).Update(ctx, d, metav1.UpdateOptions{})
	return false, err
}

func restoreManagementMarks(live, original map[string]string) {
	for _, key := range []string{ownerKey, orgKey, appKey, hashKey, installationKey, transitionOperationKey} {
		if value, exists := original[key]; exists {
			live[key] = value
		} else {
			delete(live, key)
		}
	}
}

// Restore does not release maintenance. Recovery stops on external drift and
// never substitutes a newly created resource for a saved UID.
func (r *ManagementReplacement) Restore(ctx context.Context, c ManagementCheckpoint) (bool, error) {
	if !r.validCheckpoint(c) {
		return false, ErrManagementReplacement
	}
	client := r.Kubernetes.Client.AppsV1().Deployments(c.Deployment.Namespace)
	d, err := client.Get(ctx, c.Deployment.Name, metav1.GetOptions{})
	if err != nil {
		return false, err
	}
	if d.UID != c.Deployment.UID || d.ResourceVersion == "" {
		return false, ErrManagementReplacement
	}
	if r.deploymentMatches(c.Deployment, d, r.Source) && originalMarker(d.Annotations, c.Deployment.Annotations) {
		if !managementReady(d) {
			return false, nil
		}
	} else {
		if !r.deploymentMatches(r.Target.Deployment, d, r.Target) || !r.targetMarked(d.Annotations) {
			return false, ErrManagementReplacement
		}
		d.Spec = *c.Deployment.Spec.DeepCopy()
		computepolicy.Apply(&d.Spec.Template.Spec)
		restoreManagementMarks(d.Annotations, c.Deployment.Annotations)
		_, err = client.Update(ctx, d, metav1.UpdateOptions{})
		return false, err
	}
	for _, original := range c.Services {
		client := r.Kubernetes.Client.CoreV1().Services(original.Namespace)
		live, err := client.Get(ctx, original.Name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		if live.UID != original.UID || live.ResourceVersion == "" || live.DeletionTimestamp != nil || !preinstalledServiceCompatible(original, live) {
			return false, ErrManagementReplacement
		}
		if owned(live.Annotations, r.Source, r.StationID, r.OrganizationID) && originalMarker(live.Annotations, original.Annotations) {
			continue
		}
		if !r.targetMarked(live.Annotations) {
			return false, ErrManagementReplacement
		}
		restoreManagementMarks(live.Annotations, original.Annotations)
		_, err = client.Update(ctx, live, metav1.UpdateOptions{})
		return false, err
	}
	return true, nil
}
