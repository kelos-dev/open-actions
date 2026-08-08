package controller

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func secretValue(ctx context.Context, reader client.Reader, namespace string, selector corev1.SecretKeySelector) ([]byte, error) {
	secret := &corev1.Secret{}
	if err := reader.Get(ctx, client.ObjectKey{Namespace: namespace, Name: selector.Name}, secret); err != nil {
		return nil, fmt.Errorf("get Secret %q: %w", selector.Name, err)
	}
	value, found := secret.Data[selector.Key]
	if !found || len(value) == 0 {
		return nil, fmt.Errorf("Secret %q does not contain non-empty key %q", selector.Name, selector.Key)
	}
	return value, nil
}
