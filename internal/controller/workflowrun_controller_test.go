package controller

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	actionsv1alpha1 "github.com/kelos-dev/open-actions/api/v1alpha1"
	githubclient "github.com/kelos-dev/open-actions/internal/github"
	"github.com/kelos-dev/open-actions/internal/runner"
	"github.com/kelos-dev/open-actions/internal/workflow"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

func TestJobPlanCoversSupportedSteps(t *testing.T) {
	reconciler := &WorkflowRunReconciler{GitHubServerURL: "https://github.com", GitHubAPIBase: "https://api.github.com", ActionCloneBaseURL: "https://github.com/git"}
	run := &actionsv1alpha1.WorkflowRun{Spec: actionsv1alpha1.WorkflowRunSpec{
		Source: actionsv1alpha1.WorkflowRunSource{
			Type: actionsv1alpha1.SourceTypeGitHub,
			GitHub: &actionsv1alpha1.GitHubWorkflowRunSource{
				Repository: actionsv1alpha1.GitHubRepository{ID: 1, Owner: "example", Name: "project"},
				Event:      actionsv1alpha1.GitHubEvent{Name: "push", DeliveryID: "delivery"},
				Revision:   actionsv1alpha1.GitRevision{SHA: strings.Repeat("a", 40), Ref: "refs/heads/main"},
			},
		},
	}}
	job := workflow.Job{RunsOn: workflow.StringList{"ubuntu-latest"}, Steps: []workflow.Step{
		{Uses: "actions/checkout@v4"},
		{Uses: "actions/setup-go@v5", With: map[string]any{"go-version-file": "go.mod"}},
		{Name: "Build", Run: "make build"},
	}}
	plan, err := reconciler.jobPlan(run, "CI", "build", job)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Repository.ServerURL != "https://github.com" || plan.Repository.APIURL != "https://api.github.com" || plan.Repository.ActionCloneBaseURL != "https://github.com/git" {
		t.Errorf("repository endpoints = %#v", plan.Repository)
	}
	if plan.Version != runner.PlanVersion || plan.Repository.ID != 1 || plan.Event.DeliveryID != "delivery" {
		t.Errorf("plan identity = %#v", plan)
	}
	if len(plan.Steps) != 3 {
		t.Errorf("steps = %d", len(plan.Steps))
	}
}

func TestConcurrencyWaitsForOlderUnevaluatedRun(t *testing.T) {
	older := concurrencyRun("older", "older", 1, time.Unix(1, 0), "", nil)
	current := concurrencyRun("current", "current", 1, time.Unix(2, 0), "", nil)
	reconciler, _ := concurrencyReconciler(t, older, current)

	waiting, err := reconciler.handleConcurrency(context.Background(), current, "deploy", false)
	if err != nil {
		t.Fatal(err)
	}
	if !waiting {
		t.Fatal("newer run did not wait for an older unevaluated run")
	}
}

func TestConcurrencyIsRepositoryScoped(t *testing.T) {
	condition := plannedCondition(metav1.ConditionTrue, "JobsPlanned")
	otherRepository := concurrencyRun("other", "other", 2, time.Unix(1, 0), "deploy", &condition)
	current := concurrencyRun("current", "current", 1, time.Unix(2, 0), "", nil)
	reconciler, clusterClient := concurrencyReconciler(t, otherRepository, current)

	waiting, err := reconciler.handleConcurrency(context.Background(), current, "deploy", true)
	if err != nil {
		t.Fatal(err)
	}
	if waiting {
		t.Fatal("run waited for a concurrency group in another repository")
	}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(otherRepository), &actionsv1alpha1.WorkflowRun{}); err != nil {
		t.Fatalf("run in another repository was deleted: %v", err)
	}
}

func TestConcurrencySupersedesPendingRun(t *testing.T) {
	runningCondition := plannedCondition(metav1.ConditionTrue, "JobsPlanned")
	pendingCondition := plannedCondition(metav1.ConditionUnknown, "WaitingForConcurrency")
	running := concurrencyRun("running", "running", 1, time.Unix(1, 0), "deploy", &runningCondition)
	pending := concurrencyRun("pending", "pending", 1, time.Unix(2, 0), "deploy", &pendingCondition)
	current := concurrencyRun("current", "current", 1, time.Unix(3, 0), "", nil)
	reconciler, clusterClient := concurrencyReconciler(t, running, pending, current)

	waiting, err := reconciler.handleConcurrency(context.Background(), current, "deploy", false)
	if err != nil {
		t.Fatal(err)
	}
	if !waiting {
		t.Fatal("current run did not wait for the running member")
	}
	storedPending := &actionsv1alpha1.WorkflowRun{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(pending), storedPending); err != nil {
		t.Fatal(err)
	}
	if storedPending.DeletionTimestamp.IsZero() || !controllerutil.ContainsFinalizer(storedPending, workflowRunCancellationFinalizer) {
		t.Fatalf("superseded pending run is not held for foreground cleanup: %#v", storedPending.ObjectMeta)
	}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(running), &actionsv1alpha1.WorkflowRun{}); err != nil {
		t.Fatalf("running member was deleted: %v", err)
	}
}

func TestConcurrencyCancelInProgressDeletesRunningMember(t *testing.T) {
	condition := plannedCondition(metav1.ConditionTrue, "JobsPlanned")
	running := concurrencyRun("running", "running", 1, time.Unix(1, 0), "deploy", &condition)
	current := concurrencyRun("current", "current", 1, time.Unix(2, 0), "", nil)
	reconciler, clusterClient := concurrencyReconciler(t, running, current)

	waiting, err := reconciler.handleConcurrency(context.Background(), current, "deploy", true)
	if err != nil {
		t.Fatal(err)
	}
	if !waiting {
		t.Fatal("replacement run did not wait for cancellation")
	}
	storedRunning := &actionsv1alpha1.WorkflowRun{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(running), storedRunning); err != nil {
		t.Fatal(err)
	}
	if storedRunning.DeletionTimestamp.IsZero() || !controllerutil.ContainsFinalizer(storedRunning, workflowRunCancellationFinalizer) {
		t.Fatalf("running member is not held for foreground cleanup: %#v", storedRunning.ObjectMeta)
	}
}

func TestCanceledWorkflowRunWaitsForPodsBeforeFinalizing(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := actionsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := batchv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	deletionTime := metav1.Now()
	run := &actionsv1alpha1.WorkflowRun{ObjectMeta: metav1.ObjectMeta{
		Name: "canceling", Namespace: "default", UID: types.UID("run-uid"),
		DeletionTimestamp: &deletionTime, Finalizers: []string{workflowRunCancellationFinalizer},
	}}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: "canceling-job", Namespace: "default", Labels: map[string]string{actionsv1alpha1.LabelWorkflowRunUID: string(run.UID)},
	}}
	clusterClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(run, pod).Build()
	reconciler := &WorkflowRunReconciler{Client: clusterClient, APIReader: clusterClient}
	result, err := reconciler.finalizeCanceledWorkflowRun(context.Background(), run)
	if err != nil {
		t.Fatal(err)
	}
	if result.RequeueAfter == 0 {
		t.Fatal("canceled WorkflowRun did not wait for its Pod")
	}
	stored := &actionsv1alpha1.WorkflowRun{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(run), stored); err != nil {
		t.Fatal(err)
	}
	if !controllerutil.ContainsFinalizer(stored, workflowRunCancellationFinalizer) {
		t.Fatal("cancellation finalizer was removed while a Pod remained")
	}
	if err := clusterClient.Delete(context.Background(), pod); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.finalizeCanceledWorkflowRun(context.Background(), stored); err != nil {
		t.Fatal(err)
	}
	stored = &actionsv1alpha1.WorkflowRun{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(run), stored); err != nil {
		if client.IgnoreNotFound(err) != nil {
			t.Fatal(err)
		}
	} else if controllerutil.ContainsFinalizer(stored, workflowRunCancellationFinalizer) {
		t.Fatal("cancellation finalizer remained after the Pod was deleted")
	}
}

func concurrencyRun(name string, uid types.UID, repositoryID int64, created time.Time, group string, condition *metav1.Condition) *actionsv1alpha1.WorkflowRun {
	run := &actionsv1alpha1.WorkflowRun{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", UID: uid, CreationTimestamp: metav1.NewTime(created)},
		Spec: actionsv1alpha1.WorkflowRunSpec{
			ProjectRef: corev1.LocalObjectReference{Name: "project"},
			Source: actionsv1alpha1.WorkflowRunSource{
				Type:   actionsv1alpha1.SourceTypeGitHub,
				GitHub: &actionsv1alpha1.GitHubWorkflowRunSource{Repository: actionsv1alpha1.GitHubRepository{ID: repositoryID}},
			},
		},
		Status: actionsv1alpha1.WorkflowRunStatus{ConcurrencyGroup: group},
	}
	if condition != nil {
		run.Status.Conditions = []metav1.Condition{*condition}
	}
	return run
}

func plannedCondition(status metav1.ConditionStatus, reason string) metav1.Condition {
	return metav1.Condition{Type: actionsv1alpha1.WorkflowRunConditionPlanned, Status: status, Reason: reason, Message: reason, LastTransitionTime: metav1.Now()}
}

func concurrencyReconciler(t *testing.T, runs ...client.Object) (*WorkflowRunReconciler, client.Client) {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := actionsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	clusterClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(runs...).Build()
	return &WorkflowRunReconciler{Client: clusterClient, APIReader: clusterClient}, clusterClient
}

func TestChildNameIsStableAndBounded(t *testing.T) {
	name := childName(strings.Repeat("run", 30), strings.Repeat("job", 100))
	if len(name) > 63 {
		t.Errorf("name has %d characters", len(name))
	}
	if name != childName(strings.Repeat("run", 30), strings.Repeat("job", 100)) {
		t.Error("name is not stable")
	}
}

func TestChildNameSeparatesCollidingJobIDsAndTheirPlans(t *testing.T) {
	runName := strings.Repeat("workflow", 8)
	first := childName(runName, "job_1230")
	second := childName(runName, "job_103711")
	if first == second {
		t.Fatalf("colliding job IDs produced %q", first)
	}
	if childName(first, "plan") == childName(second, "plan") {
		t.Fatal("distinct jobs produced the same plan name")
	}
}

func TestWorkflowJobIdentityRequiresOwnerSpecAndLabels(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := actionsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	run := &actionsv1alpha1.WorkflowRun{
		TypeMeta:   metav1.TypeMeta{APIVersion: actionsv1alpha1.GroupVersion.String(), Kind: "WorkflowRun"},
		ObjectMeta: metav1.ObjectMeta{Name: "ci", Namespace: "default", UID: types.UID("run-uid")},
	}
	desired := &actionsv1alpha1.WorkflowJob{
		ObjectMeta: metav1.ObjectMeta{Name: "build", Namespace: "default", Labels: map[string]string{
			actionsv1alpha1.LabelProjectUID:     "project-uid",
			actionsv1alpha1.LabelWorkflowRunUID: string(run.UID),
			actionsv1alpha1.LabelWorkflowJob:    "build",
		}},
		Spec: actionsv1alpha1.WorkflowJobSpec{
			WorkflowRunRef: corev1.LocalObjectReference{Name: run.Name},
			JobID:          "build",
			RunsOn:         []string{"linux"},
		},
	}
	if err := controllerutil.SetControllerReference(run, desired, scheme); err != nil {
		t.Fatal(err)
	}
	existing := desired.DeepCopy()
	if !workflowJobIdentityMatches(existing, desired, run) {
		t.Fatal("matching WorkflowJob identity was rejected")
	}
	existing.Spec.JobID = "release"
	if workflowJobIdentityMatches(existing, desired, run) {
		t.Fatal("mismatched WorkflowJob spec was accepted")
	}
	existing = desired.DeepCopy()
	existing.Labels[actionsv1alpha1.LabelWorkflowJob] = "release"
	if workflowJobIdentityMatches(existing, desired, run) {
		t.Fatal("mismatched WorkflowJob labels were accepted")
	}
	existing = desired.DeepCopy()
	existing.OwnerReferences = nil
	if workflowJobIdentityMatches(existing, desired, run) {
		t.Fatal("unowned WorkflowJob was accepted")
	}
}

func TestPlannedWorkflowRunIsObservedWithoutPlanningDependencies(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := actionsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	run := &actionsv1alpha1.WorkflowRun{
		ObjectMeta: metav1.ObjectMeta{Name: "ci", Namespace: "default", UID: types.UID("run-uid")},
		Status: actionsv1alpha1.WorkflowRunStatus{
			WorkflowName: "CI",
			Jobs:         &actionsv1alpha1.WorkflowRunJobStatus{Total: 1},
			Conditions:   []metav1.Condition{plannedCondition(metav1.ConditionTrue, "JobsPlanned")},
		},
	}
	job := &actionsv1alpha1.WorkflowJob{
		TypeMeta: metav1.TypeMeta{APIVersion: actionsv1alpha1.GroupVersion.String(), Kind: "WorkflowJob"},
		ObjectMeta: metav1.ObjectMeta{
			Name: "build", Namespace: "default", UID: types.UID("job-uid"), Labels: map[string]string{actionsv1alpha1.LabelWorkflowRunUID: string(run.UID)},
		},
	}
	plan := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: childName(job.Name, "plan"), Namespace: job.Namespace}}
	if err := controllerutil.SetControllerReference(job, plan, scheme); err != nil {
		t.Fatal(err)
	}
	clusterClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&actionsv1alpha1.WorkflowRun{}).
		WithObjects(run, job, plan).
		Build()
	reconciler := &WorkflowRunReconciler{Client: clusterClient, APIReader: clusterClient}
	if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(run)}); err != nil {
		t.Fatal(err)
	}
	stored := &actionsv1alpha1.WorkflowRun{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(run), stored); err != nil {
		t.Fatal(err)
	}
	if stored.Status.Jobs == nil || stored.Status.Jobs.Queued != 1 {
		t.Fatalf("job summary = %#v", stored.Status.Jobs)
	}
}

func TestPlannedWorkflowRunFailsWhenAChildIsMissing(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := actionsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := batchv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	run := &actionsv1alpha1.WorkflowRun{
		ObjectMeta: metav1.ObjectMeta{Name: "ci", Namespace: "default", UID: types.UID("run-uid")},
		Status: actionsv1alpha1.WorkflowRunStatus{
			WorkflowName: "CI",
			Jobs:         &actionsv1alpha1.WorkflowRunJobStatus{Total: 1},
			Conditions:   []metav1.Condition{plannedCondition(metav1.ConditionTrue, "JobsPlanned")},
		},
	}
	clusterClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&actionsv1alpha1.WorkflowRun{}).WithObjects(run).Build()
	reconciler := &WorkflowRunReconciler{Client: clusterClient, APIReader: clusterClient}
	if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(run)}); err != nil {
		t.Fatal(err)
	}
	stored := &actionsv1alpha1.WorkflowRun{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(run), stored); err != nil {
		t.Fatal(err)
	}
	condition := meta.FindStatusCondition(stored.Status.Conditions, actionsv1alpha1.WorkflowRunConditionSucceeded)
	if condition == nil || condition.Status != metav1.ConditionFalse || condition.Reason != "ExecutionStateLost" {
		t.Fatalf("succeeded condition = %#v", condition)
	}
}

func TestPlannedWorkflowRunWaitsForActiveWorkloadAfterChildIsLost(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := actionsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := batchv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	run := &actionsv1alpha1.WorkflowRun{
		ObjectMeta: metav1.ObjectMeta{Name: "ci", Namespace: "default", UID: types.UID("run-uid")},
		Status: actionsv1alpha1.WorkflowRunStatus{
			WorkflowName: "CI",
			Jobs:         &actionsv1alpha1.WorkflowRunJobStatus{Total: 1},
			Conditions:   []metav1.Condition{plannedCondition(metav1.ConditionTrue, "JobsPlanned")},
		},
	}
	nativeJob := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
		Name: "build", Namespace: run.Namespace,
		Labels: map[string]string{actionsv1alpha1.LabelWorkflowRunUID: string(run.UID)},
	}}
	clusterClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&actionsv1alpha1.WorkflowRun{}, &batchv1.Job{}).
		WithObjects(run, nativeJob).
		Build()
	reconciler := &WorkflowRunReconciler{Client: clusterClient, APIReader: clusterClient}
	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(run)})
	if err != nil {
		t.Fatal(err)
	}
	if result.RequeueAfter == 0 {
		t.Fatal("WorkflowRun did not wait for the active native Job")
	}
	stored := &actionsv1alpha1.WorkflowRun{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(run), stored); err != nil {
		t.Fatal(err)
	}
	condition := meta.FindStatusCondition(stored.Status.Conditions, actionsv1alpha1.WorkflowRunConditionSucceeded)
	if condition == nil || condition.Status != metav1.ConditionUnknown || condition.Reason != "JobsRunning" {
		t.Fatalf("succeeded condition while native Job exists = %#v", condition)
	}
	if err := clusterClient.Delete(context.Background(), nativeJob); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(run)}); err != nil {
		t.Fatal(err)
	}
	stored = &actionsv1alpha1.WorkflowRun{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(run), stored); err != nil {
		t.Fatal(err)
	}
	condition = meta.FindStatusCondition(stored.Status.Conditions, actionsv1alpha1.WorkflowRunConditionSucceeded)
	if condition == nil || condition.Status != metav1.ConditionFalse || condition.Reason != "ExecutionStateLost" {
		t.Fatalf("succeeded condition after native Job deletion = %#v", condition)
	}
}

func TestInvalidWorkflowPlanningDoesNotRetry(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := actionsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	run := &actionsv1alpha1.WorkflowRun{ObjectMeta: metav1.ObjectMeta{Name: "ci", Namespace: "default"}}
	clusterClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&actionsv1alpha1.WorkflowRun{}).WithObjects(run).Build()
	reconciler := &WorkflowRunReconciler{Client: clusterClient, APIReader: clusterClient}
	if _, err := reconciler.planningFailed(context.Background(), run, "WorkflowInvalid", errors.New("invalid workflow"), planningFailureTerminal); err != nil {
		t.Fatalf("terminal planning failure returned an error: %v", err)
	}
	stored := &actionsv1alpha1.WorkflowRun{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(run), stored); err != nil {
		t.Fatal(err)
	}
	condition := meta.FindStatusCondition(stored.Status.Conditions, actionsv1alpha1.WorkflowRunConditionPlanned)
	if condition == nil || condition.Status != metav1.ConditionFalse || condition.Reason != "WorkflowInvalid" {
		t.Fatalf("planned condition = %#v", condition)
	}
	succeeded := meta.FindStatusCondition(stored.Status.Conditions, actionsv1alpha1.WorkflowRunConditionSucceeded)
	if succeeded == nil || succeeded.Status != metav1.ConditionFalse || succeeded.Reason != "WorkflowInvalid" {
		t.Fatalf("succeeded condition = %#v", succeeded)
	}
}

func TestChildCreationFailureRemainsRetryable(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := actionsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	run := &actionsv1alpha1.WorkflowRun{ObjectMeta: metav1.ObjectMeta{Name: "ci", Namespace: "default"}}
	clusterClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&actionsv1alpha1.WorkflowRun{}).WithObjects(run).Build()
	reconciler := &WorkflowRunReconciler{Client: clusterClient, APIReader: clusterClient}
	cause := errors.New("admission unavailable")
	if _, err := reconciler.planningFailed(context.Background(), run, "ChildCreationFailed", cause, planningFailureRetry); !errors.Is(err, cause) {
		t.Fatalf("child creation failure error = %v", err)
	}
	stored := &actionsv1alpha1.WorkflowRun{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(run), stored); err != nil {
		t.Fatal(err)
	}
	condition := meta.FindStatusCondition(stored.Status.Conditions, actionsv1alpha1.WorkflowRunConditionPlanned)
	if condition == nil || condition.Status != metav1.ConditionUnknown || condition.Reason != "ChildCreationFailed" {
		t.Fatalf("planned condition = %#v", condition)
	}
}

func TestTerminalPlanningFailuresAreClassifiedByCause(t *testing.T) {
	notFound := fmt.Errorf("get workflow: %w", &githubclient.APIError{StatusCode: 404, Status: "404 Not Found"})
	if !githubAPIStatus(notFound, 404) {
		t.Fatal("GitHub 404 was not classified as terminal")
	}
	invalid := apierrors.NewInvalid(actionsv1alpha1.GroupVersion.WithKind("WorkflowJob").GroupKind(), "build", nil)
	if childCreationFailureDisposition(invalid) != planningFailureTerminal {
		t.Fatal("invalid child resource was not classified as terminal")
	}
	if childCreationFailureDisposition(errors.New("admission unavailable")) != planningFailureRetry {
		t.Fatal("transient child creation failure was classified as terminal")
	}
}

func TestWaitingWorkflowRunResumesWithoutRefetchingWorkflow(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := actionsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	olderCondition := plannedCondition(metav1.ConditionTrue, "JobsPlanned")
	waitingCondition := plannedCondition(metav1.ConditionUnknown, "WaitingForConcurrency")
	older := concurrencyRun("older", "run-older", 1, time.Now().Add(-time.Minute), "deploy", &olderCondition)
	current := concurrencyRun("current", "run-current", 1, time.Now(), "deploy", &waitingCondition)
	current.Status.WorkflowName = "CI"
	current.Status.Jobs = &actionsv1alpha1.WorkflowRunJobStatus{Total: 1, Queued: 1}
	clusterClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&actionsv1alpha1.WorkflowRun{}).WithObjects(older, current).Build()
	reconciler := &WorkflowRunReconciler{Client: clusterClient, APIReader: clusterClient}

	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(current)})
	if err != nil {
		t.Fatal(err)
	}
	if result.RequeueAfter != 2*time.Second {
		t.Fatalf("requeue after = %v, want 2s", result.RequeueAfter)
	}
}

func TestWaitingWorkflowRunPersistsCancellationPolicy(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := actionsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	run := concurrencyRun("current", "run-current", 1, time.Now(), "", nil)
	clusterClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&actionsv1alpha1.WorkflowRun{}).WithObjects(run).Build()
	reconciler := &WorkflowRunReconciler{Client: clusterClient, APIReader: clusterClient}
	if _, err := reconciler.waitingForConcurrency(context.Background(), run, "CI", "deploy", 1, true); err != nil {
		t.Fatal(err)
	}
	stored := &actionsv1alpha1.WorkflowRun{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(run), stored); err != nil {
		t.Fatal(err)
	}
	condition := meta.FindStatusCondition(stored.Status.Conditions, actionsv1alpha1.WorkflowRunConditionPlanned)
	if condition == nil || condition.Status != metav1.ConditionUnknown || condition.Reason != "WaitingForConcurrencyCancellation" {
		t.Fatalf("planned condition = %#v", condition)
	}
}

func TestWorkflowJobLabelsEncodeInvalidLabelValues(t *testing.T) {
	run := &actionsv1alpha1.WorkflowRun{ObjectMeta: metav1.ObjectMeta{UID: "run-uid"}}
	project := &actionsv1alpha1.Project{ObjectMeta: metav1.ObjectMeta{UID: "project-uid"}}

	for _, jobID := range []string{"_build", strings.Repeat("a", 64)} {
		t.Run(jobID[:1], func(t *testing.T) {
			digest := sha256.Sum256([]byte(jobID))
			want := strings.ToLower(digestEncoding.EncodeToString(digest[:]))
			got := workflowJobLabels(run, project, jobID)[actionsv1alpha1.LabelWorkflowJob]
			if got != want {
				t.Errorf("workflow job label = %q, want %q", got, want)
			}
			if problems := validation.IsValidLabelValue(got); len(problems) > 0 {
				t.Errorf("workflow job label is invalid: %v", problems)
			}
		})
	}
}

func TestWorkflowRunCountsAssignedJobAsActiveBeforeExecution(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := actionsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	run := &actionsv1alpha1.WorkflowRun{
		ObjectMeta: metav1.ObjectMeta{Name: "ci", Namespace: "default", UID: types.UID("run-uid")},
	}
	job := &actionsv1alpha1.WorkflowJob{
		TypeMeta: metav1.TypeMeta{APIVersion: actionsv1alpha1.GroupVersion.String(), Kind: "WorkflowJob"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "build",
			Namespace: "default",
			UID:       types.UID("job-uid"),
			Labels:    map[string]string{actionsv1alpha1.LabelWorkflowRunUID: string(run.UID)},
		},
		Status: actionsv1alpha1.WorkflowJobStatus{
			RunnerRef: &corev1.LocalObjectReference{Name: "runner-1"},
		},
	}
	plan := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: childName(job.Name, "plan"), Namespace: job.Namespace}}
	if err := controllerutil.SetControllerReference(job, plan, scheme); err != nil {
		t.Fatal(err)
	}
	clusterClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&actionsv1alpha1.WorkflowRun{}).
		WithObjects(run, job, plan).
		Build()
	reconciler := &WorkflowRunReconciler{Client: clusterClient, APIReader: clusterClient}
	if _, err := reconciler.observeWorkflowJobs(context.Background(), run, "CI", "", 1); err != nil {
		t.Fatal(err)
	}
	stored := &actionsv1alpha1.WorkflowRun{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(run), stored); err != nil {
		t.Fatal(err)
	}
	if stored.Status.Jobs == nil || stored.Status.Jobs.Active != 1 || stored.Status.Jobs.Queued != 0 {
		t.Fatalf("job summary = %#v", stored.Status.Jobs)
	}
	condition := meta.FindStatusCondition(stored.Status.Conditions, actionsv1alpha1.WorkflowRunConditionSucceeded)
	if condition == nil || condition.Status != metav1.ConditionUnknown || condition.Reason != "JobsRunning" {
		t.Fatalf("succeeded condition = %#v", condition)
	}
}
