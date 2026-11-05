package controller

import (
	"testing"

	actionsv1alpha1 "github.com/kelos-dev/open-actions/api/v1alpha1"
	"github.com/kelos-dev/open-actions/internal/workflowsnapshot"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

func TestLoadWorkflowFileSnapshot(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*corev1.ConfigMap)
		absent bool
		want   planningFailureDisposition
	}{
		{name: "valid"},
		{name: "another owner", mutate: func(object *corev1.ConfigMap) { object.OwnerReferences[0].UID = "other" }, want: planningFailureTerminal},
		{name: "mutable", mutate: func(object *corev1.ConfigMap) { object.Immutable = pointerTo(false) }, want: planningFailureTerminal},
		{name: "unspecified immutability", mutate: func(object *corev1.ConfigMap) { object.Immutable = nil }, want: planningFailureTerminal},
		{name: "missing data", mutate: func(object *corev1.ConfigMap) { object.Data = nil }, want: planningFailureTerminal},
		{name: "not yet created", absent: true, want: planningFailureRetry},
	} {
		t.Run(test.name, func(t *testing.T) {
			scheme := runnerTestScheme(t)
			run := &actionsv1alpha1.WorkflowRun{ObjectMeta: metav1.ObjectMeta{
				Name: "ci", Namespace: "default", UID: "run-uid", Annotations: map[string]string{actionsv1alpha1.AnnotationWorkflowFile: "workflow-snapshot"},
			}}
			object := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{Name: "workflow-snapshot", Namespace: run.Namespace},
				Immutable:  pointerTo(true), Data: map[string]string{workflowsnapshot.DataKey: "on: push\njobs: {}\n"},
			}
			if err := controllerutil.SetControllerReference(run, object, scheme); err != nil {
				t.Fatal(err)
			}
			if test.mutate != nil {
				test.mutate(object)
			}
			objects := []client.Object{run}
			if !test.absent {
				objects = append(objects, object)
			}
			cluster := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()
			r := &WorkflowRunReconciler{Client: cluster, APIReader: cluster}
			data, err := r.loadWorkflowFileSnapshot(t.Context(), run)
			if test.mutate == nil && !test.absent {
				if err != nil || string(data) != object.Data[workflowsnapshot.DataKey] {
					t.Fatalf("snapshot = %q, %v", data, err)
				}
			} else if err == nil || childCreationFailureDisposition(err) != test.want {
				t.Fatalf("snapshot error = %v, want disposition %v", err, test.want)
			}
		})
	}
}
