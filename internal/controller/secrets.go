package controller

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"unicode/utf8"

	actionsv1alpha1 "github.com/kelos-dev/open-actions/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	maxEnvironmentSecrets     = 100
	maxEnvironmentSecretBytes = 8 << 10
)

var environmentSecretNamePattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

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

func environmentSecretValues(ctx context.Context, reader client.Reader, namespace string, reference *actionsv1alpha1.EnvironmentSecretReference) (map[string]string, error) {
	if reference == nil {
		return map[string]string{}, nil
	}
	secret := &corev1.Secret{}
	if err := reader.Get(ctx, client.ObjectKey{Namespace: namespace, Name: reference.Name}, secret); err != nil {
		return nil, fmt.Errorf("get environment Secret %q: %w", reference.Name, err)
	}
	if len(secret.Data) > maxEnvironmentSecrets {
		return nil, fmt.Errorf("environment Secret %q contains %d keys; maximum is %d", secret.Name, len(secret.Data), maxEnvironmentSecrets)
	}
	names := make([]string, 0, len(secret.Data))
	for name := range secret.Data {
		names = append(names, name)
	}
	sort.Strings(names)
	values := make(map[string]string, len(names))
	canonicalNames := make(map[string]string, len(names))
	for _, name := range names {
		value := secret.Data[name]
		canonical := strings.ToUpper(name)
		if !environmentSecretNamePattern.MatchString(name) || strings.HasPrefix(canonical, "GITHUB_") {
			return nil, fmt.Errorf("environment Secret %q contains invalid GitHub secret name %q", secret.Name, name)
		}
		if other := canonicalNames[canonical]; other != "" {
			return nil, fmt.Errorf("environment Secret %q contains case-insensitive duplicate keys %q and %q", secret.Name, other, name)
		}
		if len(value) > maxEnvironmentSecretBytes || !utf8.Valid(value) {
			return nil, fmt.Errorf("environment Secret %q value %q must be valid UTF-8 no larger than %d bytes", secret.Name, name, maxEnvironmentSecretBytes)
		}
		canonicalNames[canonical] = name
		values[name] = string(value)
	}
	return values, nil
}
