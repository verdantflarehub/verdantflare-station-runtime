package instances

import (
	"context"
	"encoding/json"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

type WorkingCopyObserver interface {
	ObserveWorkingCopy(context.Context, CreateRequest, HelperObservation, string, int64) (*WorkerSource, error)
}

type SourceRecoveryProof struct {
	PreviousWorker WorkerObservation `json:"previous_worker"`
	Source         *WorkerSource     `json:"source,omitempty"`
}

type savedSourceRecovery struct {
	Stage          string            `json:"stage"`
	PreviousWorker WorkerObservation `json:"previous_worker"`
	Helper         HelperObservation `json:"helper"`
	Source         *WorkerSource     `json:"source,omitempty"`
}

func (s startState) sourceRecoveryProof() *SourceRecoveryProof {
	if s.SavedRecovery == nil {
		return nil
	}
	return &SourceRecoveryProof{PreviousWorker: s.SavedRecovery.PreviousWorker, Source: s.SavedRecovery.Source}
}

// The immutable request remains the original source. Only a recorded, complete
// read-only observation can provide the effective source for this execution.
func effectiveStart(in StartRequest, state startState, p HelperProfile) (StartRequest, error) {
	r := state.SavedRecovery
	if r == nil {
		return in, nil
	}
	if r.Stage != "ready" || r.Source == nil || !r.Source.valid(p.MaxFileBytes) || r.Source.Empty || r.Source.SourceRevisionID != in.SourceRevisionID || *r.Source == in.source() || r.PreviousWorker.PodUID == "" || r.PreviousWorker.ServiceUID == "" || r.Helper.PodUID == "" || r.Helper.ServiceUID == "" {
		return in, ErrBinding
	}
	in.SHA256, in.Size, in.Empty = r.Source.SHA256, r.Source.Size, r.Source.Empty
	return in, nil
}

func validateRecoveryOrigin(next StartRequest, p HelperProfile, input, state []byte) error {
	var prior StartRequest
	var execution startState
	if json.Unmarshal(input, &prior) != nil || json.Unmarshal(state, &execution) != nil || execution.Proof == nil || prior.identity() != next.identity() || prior.ProfileID != next.ProfileID || prior.OperationID == next.OperationID {
		return ErrBinding
	}
	effective, err := effectiveStart(prior, execution, p)
	if err != nil {
		return err
	}
	proof := execution.Proof.Startup
	if effective.source() != next.source() || proof.WorkerSource != effective.source() || proof.StartOperationID != prior.OperationID || proof.CreateOperationID != prior.CreateOperationID || proof.InstanceID != prior.InstanceID || proof.ProjectID != prior.ProjectID || proof.PodUID != execution.Worker.PodUID {
		return ErrBinding
	}
	return nil
}

func verifyCurrentTerminal(ctx context.Context, client kubernetes.Interface, in StartRequest, p HelperProfile, b VolumeBinding, worker WorkerObservation, allowMissing bool) error {
	desired, _, err := workerObjects(in.identity(), p, b, in.OperationID, in.source())
	if err != nil {
		return err
	}
	pod, err := client.CoreV1().Pods(p.Namespace).Get(ctx, desired.Name, metav1.GetOptions{})
	if !(allowMissing && apierrors.IsNotFound(err)) {
		if err != nil {
			return err
		}
		if worker.PodName != desired.Name || !workerPodMatches(pod, desired, worker.PodUID) {
			return ErrBinding
		}
	}
	return verifyFailedWorker(ctx, client, p, workerRecovery{Start: in, Worker: worker}, allowMissing)
}

// Each call advances only one durable stage. File helpers cannot write to the
// volume and are removed before relaunch; no second GPU worker is concurrent.
func (s *Service) advanceSavedRecovery(ctx context.Context, in StartRequest, created CreateRequest, p HelperProfile, b VolumeBinding, state *startState) error {
	r := state.SavedRecovery
	if r == nil {
		return ErrBinding
	}
	switch r.Stage {
	case "removing_worker":
		if err := verifyCurrentTerminal(ctx, s.Client, in, p, b, r.PreviousWorker, true); err != nil {
			return err
		}
		gone, err := removeWorker(ctx, s.Client, in, p, b, r.PreviousWorker)
		if err != nil {
			return err
		}
		if gone {
			r.Stage = "observing"
			state.Worker = WorkerObservation{}
		}
	case "observing":
		helper, err := EnsureHelper(ctx, s.Client, in.identity(), p, b, r.Helper, true)
		if err != nil {
			return err
		}
		r.Helper = helper
		if !helper.Ready {
			return nil
		}
		secret, err := credential(ctx, s.Client, in.identity(), p.Namespace, state.SecretUID, false)
		if err != nil {
			return err
		}
		if s.WorkingCopyObserver == nil {
			return ErrBinding
		}
		source, err := s.WorkingCopyObserver.ObserveWorkingCopy(ctx, created, helper, string(secret.Data["worker-token"]), p.MaxFileBytes)
		if err != nil {
			return err
		}
		if source == nil || !source.valid(p.MaxFileBytes) || source.Empty || source.SourceRevisionID != in.SourceRevisionID || *source == in.source() {
			return ErrBinding
		}
		r.Source, r.Stage = source, "removing_helper"
	case "removing_helper":
		gone, err := RemoveHelper(ctx, s.Client, in.identity(), p, b, r.Helper, true)
		if err != nil {
			return err
		}
		if gone {
			r.Stage = "ready"
		}
	default:
		return ErrBinding
	}
	return nil
}
