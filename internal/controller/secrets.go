package controller

import (
	"context"
	"fmt"
	"sort"
	"unicode/utf8"

	actionsv1alpha1 "github.com/kelos-dev/open-actions/api/v1alpha1"
	"github.com/kelos-dev/open-actions/internal/projectvalue"
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

func validateProjectSecretValues(ctx context.Context, reader client.Reader, project *actionsv1alpha1.Project) error {
	if project.Spec.Secrets == nil {
		return nil
	}
	secret := &corev1.Secret{}
	secretName := project.Spec.Secrets.SecretRef.Name
	if err := reader.Get(ctx, client.ObjectKey{Namespace: project.Namespace, Name: secretName}, secret); err != nil {
		return fmt.Errorf("Project %q: get Secret %q: %w", project.Name, secretName, err)
	}
	if len(secret.Data) > projectvalue.MaxSecretCount {
		return fmt.Errorf("Project %q Secret %q contains more than %d values", project.Name, secretName, projectvalue.MaxSecretCount)
	}
	names := sortedSecretNames(secret.Data)
	for _, name := range names {
		if err := projectvalue.ValidateName(name); err != nil {
			return fmt.Errorf("Project %q Secret %q key %q %s", project.Name, secretName, name, err)
		}
		value := secret.Data[name]
		if len(value) > projectvalue.MaxValueBytes {
			return fmt.Errorf("Project %q Secret %q key %q exceeds %d bytes", project.Name, secretName, name, projectvalue.MaxValueBytes)
		}
		if !utf8.Valid(value) {
			return fmt.Errorf("Project %q Secret %q key %q must contain valid UTF-8", project.Name, secretName, name)
		}
	}
	return nil
}

func validateProjectVariableValues(ctx context.Context, reader client.Reader, project *actionsv1alpha1.Project) error {
	if project.Spec.Variables == nil {
		return nil
	}
	configMap := &corev1.ConfigMap{}
	configMapName := project.Spec.Variables.ConfigMapRef.Name
	if err := reader.Get(ctx, client.ObjectKey{Namespace: project.Namespace, Name: configMapName}, configMap); err != nil {
		return fmt.Errorf("Project %q: get ConfigMap %q: %w", project.Name, configMapName, err)
	}
	if len(configMap.Data) > projectvalue.MaxVariableCount {
		return fmt.Errorf("Project %q ConfigMap %q contains more than %d values", project.Name, configMapName, projectvalue.MaxVariableCount)
	}
	if len(configMap.BinaryData) > 0 {
		return fmt.Errorf("Project %q ConfigMap %q must not contain binary data", project.Name, configMapName)
	}
	names := sortedVariableNames(configMap.Data)
	for _, name := range names {
		if err := projectvalue.ValidateName(name); err != nil {
			return fmt.Errorf("Project %q ConfigMap %q key %q %s", project.Name, configMapName, name, err)
		}
		if len(configMap.Data[name]) > projectvalue.MaxValueBytes {
			return fmt.Errorf("Project %q ConfigMap %q key %q exceeds %d bytes", project.Name, configMapName, name, projectvalue.MaxValueBytes)
		}
	}
	return nil
}

func sortedSecretNames(values map[string][]byte) []string {
	names := make([]string, 0, len(values))
	for name := range values {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func sortedVariableNames(values map[string]string) []string {
	names := make([]string, 0, len(values))
	for name := range values {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
