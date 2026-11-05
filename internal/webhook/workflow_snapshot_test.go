package webhook

import (
	"context"
	"errors"
	"testing"

	actionsv1alpha1 "github.com/kelos-dev/open-actions/api/v1alpha1"
	"github.com/kelos-dev/open-actions/internal/workflowsnapshot"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

func TestWorkflowSnapshotCreationRetries(t *testing.T) {
	run := &actionsv1alpha1.WorkflowRun{ObjectMeta: metav1.ObjectMeta{Name: "ci", Namespace: "default", UID: "run-uid",
		Annotations: map[string]string{actionsv1alpha1.AnnotationWorkflowFile: workflowsnapshot.ConfigMapName("ci")},
	}}
	data := []byte("on: pull_request\njobs: {}\n")
	cluster := fake.NewClientBuilder().WithScheme(deliveryTestScheme(t)).WithObjects(run).Build()
	failCreate := true
	r := &DeliveryReconciler{APIReader: cluster, Client: interceptor.NewClient(cluster, interceptor.Funcs{
		Create: func(ctx context.Context, c client.WithWatch, object client.Object, opts ...client.CreateOption) error {
			if failCreate {
				return errors.New("snapshot storage unavailable")
			}
			return c.Create(ctx, object, opts...)
		},
	})}
	if err := r.ensureWorkflowRunSnapshots(t.Context(), run, data); err == nil {
		t.Fatal("snapshot creation failure was ignored")
	}
	key := client.ObjectKey{Namespace: run.Namespace, Name: run.Annotations[actionsv1alpha1.AnnotationWorkflowFile]}
	if err := cluster.Get(t.Context(), key, &corev1.ConfigMap{}); !apierrors.IsNotFound(err) {
		t.Fatalf("snapshot after failed creation: %v", err)
	}
	failCreate = false
	for range 2 {
		if err := r.ensureWorkflowRunSnapshots(t.Context(), run, data); err != nil {
			t.Fatal(err)
		}
	}
	object := &corev1.ConfigMap{}
	if err := cluster.Get(t.Context(), key, object); err != nil {
		t.Fatal(err)
	}
	if !metav1.IsControlledBy(object, run) || object.Immutable == nil || !*object.Immutable || object.Data[workflowsnapshot.DataKey] != string(data) {
		t.Fatalf("retained snapshot = %#v", object)
	}
}

func TestWorkflowSnapshotCreationRejectsConflictingObjects(t *testing.T) {
	for _, reason := range []string{"owner", "mutable", "data"} {
		t.Run(reason, func(t *testing.T) {
			scheme := deliveryTestScheme(t)
			run := &actionsv1alpha1.WorkflowRun{ObjectMeta: metav1.ObjectMeta{Name: "ci", Namespace: "default", UID: "run-uid"}}
			immutable := true
			object := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{Name: workflowsnapshot.ConfigMapName(run.Name), Namespace: run.Namespace},
				Immutable:  &immutable, Data: map[string]string{workflowsnapshot.DataKey: "workflow"},
			}
			if err := controllerutil.SetControllerReference(run, object, scheme); err != nil {
				t.Fatal(err)
			}
			switch reason {
			case "owner":
				object.OwnerReferences[0].UID = "other"
			case "mutable":
				object.Immutable = nil
			case "data":
				object.Data[workflowsnapshot.DataKey] = "different"
			}
			cluster := fake.NewClientBuilder().WithScheme(scheme).WithObjects(run, object).Build()
			r := &DeliveryReconciler{Client: cluster, APIReader: cluster}
			if err := r.ensureWorkflowRunSnapshots(t.Context(), run, []byte("workflow")); !apierrors.IsConflict(err) {
				t.Fatalf("conflicting snapshot error = %v", err)
			}
			stored := &actionsv1alpha1.WorkflowRun{}
			if err := cluster.Get(t.Context(), client.ObjectKeyFromObject(run), stored); err != nil {
				t.Fatal(err)
			}
			if stored.Annotations[actionsv1alpha1.AnnotationWorkflowFile] != "" {
				t.Fatal("conflicting snapshot was attached to the WorkflowRun")
			}
		})
	}
}
