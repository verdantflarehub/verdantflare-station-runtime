package instances

import (
	"context"
	"crypto/rand"
	"encoding/base64"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

func credential(ctx context.Context, client kubernetes.Interface, in Identity, namespace, expectedUID string, create bool) (*corev1.Secret, error) {
	item, err := client.CoreV1().Secrets(namespace).Get(ctx, instanceName(in), metav1.GetOptions{})
	if apierrors.IsNotFound(err) && create && expectedUID == "" {
		data := map[string][]byte{}
		for _, key := range []string{"worker-token", "gui-password"} {
			raw := make([]byte, 32)
			if _, err = rand.Read(raw); err != nil {
				return nil, err
			}
			data[key] = []byte(base64.RawURLEncoding.EncodeToString(raw))
		}
		immutable := true
		annotations := volumeOwner(in)
		annotations[stationKey] = in.StationID
		item, err = client.CoreV1().Secrets(namespace).Create(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: instanceName(in), Namespace: namespace, Annotations: annotations}, Immutable: &immutable, Type: corev1.SecretTypeOpaque, Data: data}, metav1.CreateOptions{})
	}
	if err != nil {
		return nil, err
	}
	if item.UID == "" || (expectedUID != "" && string(item.UID) != expectedUID) || item.DeletionTimestamp != nil || item.Immutable == nil || !*item.Immutable || item.Type != corev1.SecretTypeOpaque || item.Annotations[stationKey] != in.StationID {
		return nil, ErrBinding
	}
	for k, v := range volumeOwner(in) {
		if item.Annotations[k] != v {
			return nil, ErrBinding
		}
	}
	for _, key := range []string{"worker-token", "gui-password"} {
		raw, err := base64.RawURLEncoding.DecodeString(string(item.Data[key]))
		if err != nil || len(raw) != 32 {
			return nil, ErrBinding
		}
	}
	return item, nil
}
