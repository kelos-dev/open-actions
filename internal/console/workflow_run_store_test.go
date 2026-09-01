package console

import (
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	actionsv1alpha1 "github.com/kelos-dev/open-actions/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	toolscache "k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllertest"
)

func TestWorkflowRunCacheTransformKeepsRunListFields(t *testing.T) {
	run := testWorkflowRun("default", "ci", time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC))
	run.Labels = map[string]string{"large": strings.Repeat("x", 1000)}
	run.Annotations = map[string]string{"large": strings.Repeat("x", 1000)}
	run.ManagedFields = []metav1.ManagedFieldsEntry{{Manager: "controller"}}
	run.Spec.Source.GitHub.Actor = "octocat"
	run.Spec.Source.GitHub.Event.DeliveryID = "delivery"
	run.Spec.Rerun = &actionsv1alpha1.WorkflowRunRerun{JobIDs: []string{"build"}}
	run.Status.Identity = &actionsv1alpha1.WorkflowRunIdentityStatus{ID: 1}
	run.Status.Conditions = []metav1.Condition{{
		Type: actionsv1alpha1.WorkflowRunConditionSucceeded, Status: metav1.ConditionTrue, Reason: "JobsSucceeded", Message: strings.Repeat("x", 1000),
	}}
	wantName := run.Name
	wantNamespace := run.Namespace
	wantUID := run.UID
	wantResourceVersion := run.ResourceVersion
	wantCreated := run.CreationTimestamp
	wantRepository := run.Spec.Source.GitHub.Repository
	wantRevision := run.Spec.Source.GitHub.Revision.SHA
	wantWorkflowName := run.Status.WorkflowName

	object, err := WorkflowRunCacheTransform(run)
	if err != nil {
		t.Fatal(err)
	}
	transformed := object.(*actionsv1alpha1.WorkflowRun)
	if transformed.Name != wantName || transformed.Namespace != wantNamespace || transformed.UID != wantUID || transformed.ResourceVersion != wantResourceVersion || !transformed.CreationTimestamp.Equal(&wantCreated) {
		t.Fatalf("transformed metadata = %#v", transformed.ObjectMeta)
	}
	if transformed.Spec.Source.GitHub == nil || transformed.Spec.Source.GitHub.Repository != wantRepository || transformed.Spec.Source.GitHub.Revision.SHA != wantRevision || transformed.Status.WorkflowName != wantWorkflowName {
		t.Fatalf("transformed run list fields = %#v", transformed)
	}
	if transformed.Labels != nil || transformed.Annotations != nil || transformed.ManagedFields != nil || transformed.Spec.Rerun != nil || transformed.Spec.Source.GitHub.Actor != "" || transformed.Spec.Source.GitHub.Event.DeliveryID != "" || transformed.Status.Identity != nil || transformed.Status.Conditions[0].Message != "" {
		t.Fatalf("transformed run retained unused fields = %#v", transformed)
	}
}

func TestWorkflowRunStoreMaintainsRecentRuns(t *testing.T) {
	store := readyWorkflowRunStore(slog.New(slog.NewTextHandler(io.Discard, nil)))
	created := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	older := testWorkflowRun("default", "older", created.Add(-time.Hour))
	tieSecond := testWorkflowRun("team-b", "run", created)
	tieFirst := testWorkflowRun("team-a", "run", created)
	store.upsert(older)
	store.upsert(tieSecond)
	store.upsert(tieFirst)

	runs, truncated := store.Recent(2)
	if !truncated || len(runs) != 2 || runs[0].Namespace != "team-a" || runs[1].Namespace != "team-b" {
		t.Fatalf("recent runs = %#v, truncated = %t", runs, truncated)
	}

	tieFirst.Status.WorkflowName = "Updated"
	store.upsert(tieFirst)
	runs, _ = store.Recent(1)
	if runs[0].WorkflowName != "Updated" {
		t.Fatalf("updated run = %#v", runs[0])
	}

	store.deleteObject(toolscache.DeletedFinalStateUnknown{Key: "team-a/run", Obj: tieFirst})
	runs, truncated = store.Recent(2)
	if truncated || len(runs) != 2 || runs[0].Namespace != "team-b" || runs[1].WorkflowName != "older" {
		t.Fatalf("recent runs after deletion = %#v, truncated = %t", runs, truncated)
	}

	tieSecond.Spec.Source.Type = "Unsupported"
	tieSecond.Spec.Source.GitHub = nil
	store.upsert(tieSecond)
	runs, truncated = store.Recent(2)
	if truncated || len(runs) != 1 || runs[0].WorkflowName != "older" {
		t.Fatalf("recent runs after unsupported update = %#v, truncated = %t", runs, truncated)
	}
}

func TestWorkflowRunStoreReportsInformerSync(t *testing.T) {
	store := newWorkflowRunStore(slog.New(slog.NewTextHandler(io.Discard, nil)))
	if store.Synced() {
		t.Fatal("new WorkflowRun store reports synced")
	}
	store.synced = func() bool { return true }
	if !store.Synced() {
		t.Fatal("WorkflowRun store does not report informer sync")
	}
}

func TestWorkflowRunStoreConsumesInformerEvents(t *testing.T) {
	informer := &controllertest.FakeInformer{}
	store, err := NewWorkflowRunStore(informer, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	run := testWorkflowRun("default", "ci", time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC))
	informer.Add(run)
	runs, _ := store.Recent(1)
	if len(runs) != 1 || runs[0].WorkflowName != "ci" {
		t.Fatalf("runs after informer add = %#v", runs)
	}

	updated := run.DeepCopy()
	updated.Status.WorkflowName = "CI"
	informer.Update(run, updated)
	runs, _ = store.Recent(1)
	if len(runs) != 1 || runs[0].WorkflowName != "CI" {
		t.Fatalf("runs after informer update = %#v", runs)
	}

	informer.Delete(updated)
	runs, _ = store.Recent(1)
	if len(runs) != 0 {
		t.Fatalf("runs after informer delete = %#v", runs)
	}

	informer.SyncedLock.Lock()
	informer.Synced = true
	informer.SyncedLock.Unlock()
	if !store.Synced() {
		t.Fatal("WorkflowRun store does not report handler sync")
	}
}

func testWorkflowRun(namespace, name string, createdAt time.Time) *actionsv1alpha1.WorkflowRun {
	return &actionsv1alpha1.WorkflowRun{
		TypeMeta: metav1.TypeMeta{APIVersion: actionsv1alpha1.GroupVersion.String(), Kind: "WorkflowRun"},
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: namespace, UID: types.UID(namespace + "-" + name), ResourceVersion: "1", CreationTimestamp: metav1.NewTime(createdAt),
		},
		Spec: actionsv1alpha1.WorkflowRunSpec{
			ProjectRef: corev1.LocalObjectReference{Name: "project"}, WorkflowPath: ".open-actions/workflows/ci.yaml",
			Source: actionsv1alpha1.WorkflowRunSource{Type: actionsv1alpha1.SourceTypeGitHub, GitHub: &actionsv1alpha1.GitHubWorkflowRunSource{
				Repository: actionsv1alpha1.GitHubRepository{ID: 123, Owner: "acme", Name: "example"},
				Event:      actionsv1alpha1.GitHubEvent{Name: actionsv1alpha1.GitHubEventNamePush},
				Revision:   actionsv1alpha1.GitRevision{SHA: strings.Repeat("a", 40), Ref: "refs/heads/main"},
			}},
		},
		Status: actionsv1alpha1.WorkflowRunStatus{WorkflowName: name},
	}
}
