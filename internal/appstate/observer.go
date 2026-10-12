package appstate

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/google/uuid"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
)

type Observer struct {
	Client     kubernetes.Interface
	Core       Reporter
	Namespaces []string
}

func observation(d *appsv1.Deployment, deleted bool) ResourceObservation {
	a := d.Annotations
	identityValue := func(key string) string {
		v := a["station.verdantflare.com/"+key]
		if len(v) > 128 {
			return "!invalid"
		}
		return v
	}
	o := ResourceObservation{Namespace: d.Namespace, Name: d.Name, UID: string(d.UID), ResourceVersion: d.ResourceVersion,
		StationID: identityValue("station-id"), OrganizationID: identityValue("organization-id"), InstallationID: identityValue("installation-id"), InstanceID: identityValue("instance-id"), AppID: identityValue("app-id"),
		Generation: d.Generation, ObservedGeneration: d.Status.ObservedGeneration, Desired: 1, Replicas: d.Status.Replicas, Ready: d.Status.ReadyReplicas, Updated: d.Status.UpdatedReplicas, Available: d.Status.AvailableReplicas, Terminating: d.DeletionTimestamp != nil, Deleted: deleted, Health: "not_checked"}
	if d.Spec.Replicas != nil {
		o.Desired = *d.Spec.Replicas
	}
	for _, condition := range d.Status.Conditions {
		if condition.Type == appsv1.DeploymentProgressing && condition.Status == corev1.ConditionFalse && condition.Reason == "ProgressDeadlineExceeded" {
			o.ProgressFailed = true
		}
		if condition.Type == appsv1.DeploymentReplicaFailure && condition.Status == corev1.ConditionTrue {
			o.ReplicaFailed = true
		}
	}
	return o
}

func (o *Observer) inventory(ctx context.Context) ([]ResourceObservation, map[string]string, error) {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	resources := []ResourceObservation{}
	versions := map[string]string{}
	seen := map[string]bool{}
	for _, ns := range o.Namespaces {
		next := ""
		pages := map[string]bool{}
		for {
			list, err := o.Client.AppsV1().Deployments(ns).List(ctx, metav1.ListOptions{Limit: 500, Continue: next})
			if err != nil {
				return nil, nil, ErrUnavailable
			}
			if list.ResourceVersion == "" || (versions[ns] != "" && versions[ns] != list.ResourceVersion) {
				return nil, nil, ErrResync
			}
			versions[ns] = list.ResourceVersion
			for i := range list.Items {
				d := &list.Items[i]
				key := ns + "/" + d.Name
				if d.Namespace != ns || d.Name == "" || d.UID == "" || d.ResourceVersion == "" || seen[key] {
					return nil, nil, ErrResync
				}
				seen[key] = true
				resources = append(resources, observation(d, false))
				if len(resources) > 10000 {
					return nil, nil, ErrInvalid
				}
			}
			next = list.Continue
			if next == "" {
				break
			}
			if pages[next] {
				return nil, nil, ErrResync
			}
			pages[next] = true
		}
	}
	return resources, versions, nil
}

type resourceEvent struct{ Object ResourceObservation }

func (o *Observer) watch(ctx context.Context, versions map[string]string) (<-chan resourceEvent, <-chan error, func(), error) {
	ctx, cancel := context.WithTimeout(ctx, 75*time.Second)
	updates := make(chan resourceEvent, 256)
	failures := make(chan error, 1)
	var streams []watch.Interface
	var wg sync.WaitGroup
	stop := func() {
		cancel()
		for _, stream := range streams {
			stream.Stop()
		}
		wg.Wait()
	}
	for _, ns := range o.Namespaces {
		timeout := int64(70)
		stream, err := o.Client.AppsV1().Deployments(ns).Watch(ctx, metav1.ListOptions{ResourceVersion: versions[ns], AllowWatchBookmarks: true, TimeoutSeconds: &timeout})
		if err != nil {
			stop()
			return nil, nil, func() {}, ErrResync
		}
		streams = append(streams, stream)
		wg.Add(1)
		go func(ns string, stream watch.Interface) {
			defer wg.Done()
			defer stream.Stop()
			fail := func() {
				select {
				case failures <- ErrResync:
				default:
				}
			}
			for {
				select {
				case <-ctx.Done():
					return
				case event, ok := <-stream.ResultChan():
					if !ok {
						fail()
						return
					}
					if event.Type == watch.Bookmark {
						continue
					}
					if event.Type != watch.Added && event.Type != watch.Modified && event.Type != watch.Deleted {
						fail()
						return
					}
					d, valid := event.Object.(*appsv1.Deployment)
					if !valid || d.Namespace != ns || d.UID == "" || d.ResourceVersion == "" {
						fail()
						return
					}
					select {
					case updates <- resourceEvent{observation(d, event.Type == watch.Deleted)}:
					case <-ctx.Done():
						return
					default:
						fail()
						return
					}
				}
			}
		}(ns, stream)
	}
	return updates, failures, stop, nil
}

func (o *Observer) Session(ctx context.Context) error {
	stream, err := o.Core.Open(ctx)
	if err != nil {
		return err
	}
	defer stream.Close()
	for {
		resources, versions, err := o.inventory(ctx)
		if err != nil {
			return err
		}
		syncID := uuid.NewString()
		if err = stream.Send(SnapshotBegin{1, "snapshot-begin", syncID}); err != nil {
			return err
		}
		if err = sendSnapshotObjects(stream, syncID, resources); err != nil {
			return err
		}
		if err = stream.Send(SnapshotEnd{1, "snapshot-end", syncID, len(resources)}); err != nil {
			return err
		}
		updates, failures, stop, err := o.watch(ctx, versions)
		if err != nil {
			return err
		}
		timer := time.NewTimer(60 * time.Second)
		heartbeat := time.NewTicker(10 * time.Second)
		resync := false
		var result error
		for !resync && result == nil {
			select {
			case <-failures:
				resync = true
			default:
			}
			if resync {
				break
			}
			select {
			case <-ctx.Done():
				result = ctx.Err()
			case <-timer.C:
				resync = true
			case <-failures:
				resync = true
			case event, ok := <-stream.Events():
				if !ok {
					result = ErrUnavailable
				} else if event.Err != nil {
					result = event.Err
				} else {
					resync = true
				}
			case <-heartbeat.C:
				select {
				case <-failures:
					resync = true
				default:
				}
				if !resync {
					result = stream.Send(Heartbeat{1, "heartbeat"})
				}
			case update := <-updates:
				fact := update.Object
				if fact.Deleted {
					result = stream.Send(Delete{1, "delete", fact.Namespace, fact.Name, fact.UID})
				} else {
					result = stream.Send(Upsert{1, "upsert", fact})
				}
			}
		}
		timer.Stop()
		heartbeat.Stop()
		stop()
		if result != nil {
			return result
		}
	}
}

func sendSnapshotObjects(stream Stream, syncID string, resources []ResourceObservation) error {
	// Annotation strings can expand when JSON-escaped. Bound bytes as well as
	// object count so one valid but unusual annotation cannot stop collection.
	start, size := 0, 0
	for index, o := range resources {
		raw, err := json.Marshal(o)
		if err != nil || len(raw) > (1<<20)-512 {
			return ErrInvalid
		}
		if index-start == 500 || size+len(raw)+1 > (1<<20)-512 {
			if err = stream.Send(SnapshotObjects{1, "snapshot-objects", syncID, resources[start:index]}); err != nil {
				return err
			}
			start, size = index, 0
		}
		size += len(raw) + 1
	}
	if start < len(resources) {
		return stream.Send(SnapshotObjects{1, "snapshot-objects", syncID, resources[start:]})
	}
	return nil
}

func (o *Observer) Run(ctx context.Context, logger *slog.Logger) error {
	if o == nil || o.Client == nil || o.Core == nil || len(o.Namespaces) == 0 {
		return ErrInvalid
	}
	delay := time.Second
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		started := time.Now()
		err := o.Session(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if errors.Is(err, ErrInvalid) {
			return err
		}
		if time.Since(started) >= 30*time.Second {
			delay = time.Second
		}
		wait := delay
		var busy Busy
		if errors.As(err, &busy) {
			wait = busy.Delay
		} else {
			delay = min(2*delay, 10*time.Second)
		}
		wait += time.Duration(rand.IntN(250)) * time.Millisecond
		if logger != nil {
			logger.Warn("app_state_observer_resynchronizing")
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}
