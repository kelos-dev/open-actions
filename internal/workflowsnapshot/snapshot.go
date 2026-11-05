// Package workflowsnapshot defines the storage contract for workflow files
// retained with WorkflowRuns.
package workflowsnapshot

import (
	"context"
	"crypto/sha256"
	"encoding/base32"
	"fmt"
	"regexp"
	"strings"

	actionsv1alpha1 "github.com/kelos-dev/open-actions/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const DataKey = "workflow.yaml"

var nameSeparators = regexp.MustCompile(`[^a-z0-9]+`)

// ConfigMapName identifies a run's workflow file with a readable prefix and digest.
func ConfigMapName(runName string) string {
	digest := sha256.Sum256([]byte(runName + "\x00workflow-file"))
	suffix := strings.ToLower(base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(digest[:]))
	prefix := strings.Trim(nameSeparators.ReplaceAllString(strings.ToLower(runName+"-workflow-file"), "-"), "-")
	maxPrefix := 63 - len(suffix) - 1
	if len(prefix) > maxPrefix {
		prefix = strings.Trim(prefix[:maxPrefix], "-")
	}
	return prefix + "-" + suffix
}

// InvalidSnapshotError reports a snapshot that does not satisfy its storage contract.
type InvalidSnapshotError struct {
	Name        string
	WorkflowRun string
}

func (e *InvalidSnapshotError) Error() string {
	return fmt.Sprintf("workflow file ConfigMap %q does not contain the expected immutable snapshot for WorkflowRun %q", e.Name, e.WorkflowRun)
}

// Validate checks that a snapshot contains workflow bytes and belongs to the run.
func Validate(object *corev1.ConfigMap, run *actionsv1alpha1.WorkflowRun) error {
	_, found := object.Data[DataKey]
	if !metav1.IsControlledBy(object, run) || object.Immutable == nil || !*object.Immutable || !found {
		return &InvalidSnapshotError{Name: object.Name, WorkflowRun: run.Name}
	}
	return nil
}

// Ensure retains the selected bytes and records their immutable snapshot on the run.
func Ensure(ctx context.Context, writer client.Client, reader client.Reader, run *actionsv1alpha1.WorkflowRun, data []byte) error {
	name := run.Annotations[actionsv1alpha1.AnnotationWorkflowFile]
	if name == "" {
		name = ConfigMapName(run.Name)
	}
	immutable := true
	object := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: run.Namespace, Labels: map[string]string{actionsv1alpha1.LabelWorkflowRunUID: string(run.UID)}},
		Immutable:  &immutable, Data: map[string]string{DataKey: string(data)},
	}
	if err := controllerutil.SetControllerReference(run, object, writer.Scheme()); err != nil {
		return fmt.Errorf("set WorkflowRun %q as owner of workflow file ConfigMap %q: %w", run.Name, name, err)
	}
	if err := writer.Create(ctx, object); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return fmt.Errorf("create workflow file ConfigMap %q for WorkflowRun %q: %w", name, run.Name, err)
		}
		existing := &corev1.ConfigMap{}
		if err := reader.Get(ctx, client.ObjectKeyFromObject(object), existing); err != nil {
			return fmt.Errorf("get workflow file ConfigMap %q for WorkflowRun %q: %w", name, run.Name, err)
		}
		if err := Validate(existing, run); err != nil {
			return err
		}
		if existing.Data[DataKey] != string(data) {
			return &InvalidSnapshotError{Name: existing.Name, WorkflowRun: run.Name}
		}
	}
	if run.Annotations[actionsv1alpha1.AnnotationWorkflowFile] == name {
		return nil
	}
	before := run.DeepCopy()
	if run.Annotations == nil {
		run.Annotations = map[string]string{}
	}
	run.Annotations[actionsv1alpha1.AnnotationWorkflowFile] = name
	if err := writer.Patch(ctx, run, client.MergeFrom(before)); err != nil {
		return fmt.Errorf("record workflow file ConfigMap %q on WorkflowRun %q: %w", name, run.Name, err)
	}
	return nil
}
