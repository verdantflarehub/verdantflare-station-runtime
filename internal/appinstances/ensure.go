package appinstances

import (
	"context"
	"crypto/rand"
	"encoding/base64"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
)

func secretMatches(actual *corev1.Secret, in Identity, p Profile, uid string) bool {
	desired := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: instanceName(in), Namespace: p.Namespace, Annotations: in.annotations(p)}}
	return owned(actual, desired, uid) && actual.Type == corev1.SecretTypeOpaque && actual.Immutable != nil && *actual.Immutable &&
		len(actual.Data) == 1 && credentialValid(actual.Data["worker-token"])
}
func volumeClaimed(ctx context.Context, client kubernetes.Interface, in Identity, p Profile, b Volume) error {
	pvc, e := client.CoreV1().PersistentVolumeClaims(p.Namespace).Get(ctx, b.PVCName, metav1.GetOptions{})
	if e != nil {
		return e
	}
	if string(pvc.UID) != b.PVCUID || pvc.DeletionTimestamp != nil {
		return ErrBinding
	}
	for k, v := range in.annotations(p) {
		if pvc.Annotations[k] != v {
			return ErrBinding
		}
	}
	return nil
}

// EnsureWorker is only called after durable authorization, start acceptance and
// volume reservation. It never adopts a changed existing UID or deletes data.
// Read and validate every existing object before making the first mutation.
// checkpoint must durably bind each observed UID to this exact accepted run.
// It runs before the next mutation, including when recovering a lost response.
// A checkpoint failure leaves the accepted operation resumable, not failed or
// rolled back. The caller must serialize all execution of this instance.
func EnsureWorker(ctx context.Context, client kubernetes.Interface, in Run, p Profile, b Volume, expected Observation, checkpoint func(context.Context, Observation) error) (Observation, error) {
	if checkpoint == nil || !expected.valid() {
		return Observation{}, ErrInvalid
	}
	pod, svc, e := WorkerObjects(in, p, b)
	if e != nil {
		return Observation{}, e
	}
	currentPod, e := client.CoreV1().Pods(p.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
	if e != nil && !apierrors.IsNotFound(e) {
		return Observation{}, e
	}
	allowed := map[types.UID]bool{}
	if apierrors.IsNotFound(e) {
		if expected.PodUID != "" {
			return Observation{}, ErrBinding
		}
		currentPod = nil
	} else {
		if !podMatches(currentPod, pod, expected.PodUID, p.NodeName) {
			return Observation{}, ErrBinding
		}
		allowed[currentPod.UID] = true
	}
	currentService, e := client.CoreV1().Services(p.Namespace).Get(ctx, svc.Name, metav1.GetOptions{})
	if e != nil && !apierrors.IsNotFound(e) {
		return Observation{}, e
	}
	if apierrors.IsNotFound(e) {
		if expected.ServiceUID != "" || currentPod != nil {
			return Observation{}, ErrBinding
		}
		currentService = nil
	} else if !serviceMatches(currentService, svc, expected.ServiceUID) {
		return Observation{}, ErrBinding
	}
	secret, e := client.CoreV1().Secrets(p.Namespace).Get(ctx, instanceName(in.Identity), metav1.GetOptions{})
	if e != nil && !apierrors.IsNotFound(e) {
		return Observation{}, e
	}
	if apierrors.IsNotFound(e) {
		if expected.SecretUID != "" || currentPod != nil || currentService != nil {
			return Observation{}, ErrBinding
		}
		secret = nil
	} else if !secretMatches(secret, in.Identity, p, expected.SecretUID) {
		return Observation{}, ErrBinding
	}
	if _, e = InspectVolume(ctx, client, in.Identity, p, b.PVCName, &b, allowed); e != nil {
		return Observation{}, e
	}
	if e = volumeClaimed(ctx, client, in.Identity, p, b); e != nil {
		return Observation{}, e
	}
	progress := Observation{}
	if secret != nil {
		progress.SecretUID = string(secret.UID)
	}
	if currentService != nil {
		progress.ServiceUID = string(currentService.UID)
	}
	if currentPod != nil {
		progress.PodUID = string(currentPod.UID)
	}
	if e = checkpoint(ctx, progress); e != nil {
		return progress, e
	}
	if secret == nil {
		random := make([]byte, 32)
		if _, e = rand.Read(random); e != nil {
			return Observation{}, e
		}
		yes := true
		desired := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: instanceName(in.Identity), Namespace: p.Namespace, Annotations: in.Identity.annotations(p)},
			Type: corev1.SecretTypeOpaque, Immutable: &yes, Data: map[string][]byte{"worker-token": []byte(base64.RawURLEncoding.EncodeToString(random))}}
		secret, e = client.CoreV1().Secrets(p.Namespace).Create(ctx, desired, metav1.CreateOptions{})
		if e != nil {
			return Observation{}, e
		}
		if !secretMatches(secret, in.Identity, p, "") {
			return Observation{}, ErrBinding
		}
		progress.SecretUID = string(secret.UID)
		if e = checkpoint(ctx, progress); e != nil {
			return progress, e
		}
	}
	if currentService == nil {
		currentService, e = client.CoreV1().Services(p.Namespace).Create(ctx, svc, metav1.CreateOptions{})
		if e != nil {
			return Observation{}, e
		}
		if !serviceMatches(currentService, svc, "") {
			return Observation{}, ErrBinding
		}
		progress.ServiceUID = string(currentService.UID)
		if e = checkpoint(ctx, progress); e != nil {
			return progress, e
		}
	}
	if currentPod == nil {
		currentPod, e = client.CoreV1().Pods(p.Namespace).Create(ctx, pod, metav1.CreateOptions{})
		if e != nil {
			return Observation{}, e
		}
		if !podMatches(currentPod, pod, "", p.NodeName) {
			return Observation{}, ErrBinding
		}
		progress.PodUID = string(currentPod.UID)
		if e = checkpoint(ctx, progress); e != nil {
			return progress, e
		}
	}
	observed, e := ObserveWorker(ctx, client, in, p, b, progress)
	if e != nil {
		return progress, e
	}
	if e = checkpoint(ctx, observed); e != nil {
		return observed, e
	}
	return observed, nil
}
