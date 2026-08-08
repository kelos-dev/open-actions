package webhook

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func readSecretValue(ctx context.Context, reader client.Reader, namespace string, selector corev1.SecretKeySelector) ([]byte, error) {
	secret := &corev1.Secret{}
	if err := reader.Get(ctx, client.ObjectKey{Namespace: namespace, Name: selector.Name}, secret); err != nil {
		return nil, err
	}
	value := secret.Data[selector.Key]
	if len(value) == 0 {
		return nil, fmt.Errorf("Secret %q key %q is empty", selector.Name, selector.Key)
	}
	return value, nil
}
