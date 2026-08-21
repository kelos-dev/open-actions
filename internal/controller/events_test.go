package controller

import (
	"context"
	"errors"
	"testing"

	actionsv1alpha1 "github.com/kelos-dev/open-actions/api/v1alpha1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestProjectFailureEmitsWarningEventOnce(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := actionsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	project := &actionsv1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: "default", UID: types.UID("project-uid")},
		Spec: actionsv1alpha1.ProjectSpec{Source: actionsv1alpha1.ProjectSource{
			Type: actionsv1alpha1.SourceTypeGitHub,
			GitHub: &actionsv1alpha1.GitHubAppConfiguration{
				InstallationID:      1,
				PrivateKeySecretRef: corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "github"}, Key: "private-key"},
			},
		}},
	}
	clusterClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&actionsv1alpha1.Project{}).WithObjects(project).Build()
	recorder := events.NewFakeRecorder(2)
	reconciler := &ProjectReconciler{Client: clusterClient, APIReader: clusterClient, Recorder: recorder}
	request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(project)}
	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	requireEvent(t, recorder, `Warning CredentialsUnavailable get Secret "github": secrets "github" not found`)
	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	requireNoEvent(t, recorder)
}

func TestWorkflowRunPlanningFailureEmitsWarningEventOnce(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := actionsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	run := &actionsv1alpha1.WorkflowRun{ObjectMeta: metav1.ObjectMeta{Name: "ci", Namespace: "default"}}
	clusterClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&actionsv1alpha1.WorkflowRun{}).WithObjects(run).Build()
	recorder := events.NewFakeRecorder(2)
	reconciler := &WorkflowRunReconciler{Client: clusterClient, Recorder: recorder}
	cause := errors.New("admission unavailable")
	if _, err := reconciler.planningFailed(context.Background(), run, "ChildCreationFailed", cause, planningFailureRetry); !errors.Is(err, cause) {
		t.Fatalf("planning failure error = %v", err)
	}
	requireEvent(t, recorder, "Warning ChildCreationFailed admission unavailable")
	if _, err := reconciler.planningFailed(context.Background(), run, "ChildCreationFailed", cause, planningFailureRetry); !errors.Is(err, cause) {
		t.Fatalf("planning failure error = %v", err)
	}
	requireNoEvent(t, recorder)
}

func TestWorkflowRunJobFailureEmitsWarningEvent(t *testing.T) {
	scheme := runnerTestScheme(t)
	run := &actionsv1alpha1.WorkflowRun{ObjectMeta: metav1.ObjectMeta{Name: "ci", Namespace: "default", UID: types.UID("run-uid")}}
	job := &actionsv1alpha1.WorkflowJob{
		ObjectMeta: metav1.ObjectMeta{Name: "build", Namespace: "default", Labels: map[string]string{actionsv1alpha1.LabelWorkflowRunUID: string(run.UID)}},
		Status: actionsv1alpha1.WorkflowJobStatus{
			Result: actionsv1alpha1.WorkflowJobResultFailure,
			Conditions: []metav1.Condition{{
				Type: actionsv1alpha1.WorkflowJobConditionSucceeded, Status: metav1.ConditionFalse, Reason: "JobFailed",
			}},
		},
	}
	clusterClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&actionsv1alpha1.WorkflowRun{}, &actionsv1alpha1.WorkflowJob{}).WithObjects(run, job).Build()
	recorder := events.NewFakeRecorder(1)
	reconciler := &WorkflowRunReconciler{Client: clusterClient, APIReader: clusterClient, Recorder: recorder}
	if _, err := reconciler.observeWorkflowJobs(context.Background(), run, "CI", 1); err != nil {
		t.Fatal(err)
	}
	requireEvent(t, recorder, "Warning JobFailed At least one WorkflowJob failed")
}

func TestUnscheduledWorkflowJobFailureEmitsWarningEvent(t *testing.T) {
	scheme := runnerTestScheme(t)
	workflowJob := &actionsv1alpha1.WorkflowJob{ObjectMeta: metav1.ObjectMeta{Name: "build", Namespace: "default"}}
	clusterClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&actionsv1alpha1.WorkflowJob{}).WithObjects(workflowJob).Build()
	recorder := events.NewFakeRecorder(1)
	reconciler := &WorkflowRunReconciler{Client: clusterClient, Recorder: recorder}
	if err := reconciler.completeUnscheduledWorkflowJob(context.Background(), workflowJob, actionsv1alpha1.WorkflowJobResultFailure, "ConditionEvaluationFailed", "evaluate job condition: invalid expression"); err != nil {
		t.Fatal(err)
	}
	requireEvent(t, recorder, "Warning ConditionEvaluationFailed evaluate job condition: invalid expression")
}

func TestWorkflowJobFailureEmitsWarningEventOnce(t *testing.T) {
	scheme := runnerTestScheme(t)
	workflowJob := &actionsv1alpha1.WorkflowJob{
		ObjectMeta: metav1.ObjectMeta{Name: "build", Namespace: "default"},
		Status:     actionsv1alpha1.WorkflowJobStatus{RunnerRef: &corev1.LocalObjectReference{Name: "runner"}},
	}
	completion := metav1.Now()
	nativeJob := &batchv1.Job{Status: batchv1.JobStatus{
		CompletionTime: &completion,
		Conditions:     []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue}},
	}}
	clusterClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&actionsv1alpha1.WorkflowJob{}).WithObjects(workflowJob).Build()
	recorder := events.NewFakeRecorder(2)
	reconciler := &RunnerReconciler{Client: clusterClient, Recorder: recorder}
	if err := reconciler.updateWorkflowJobStatus(context.Background(), workflowJob, nativeJob, nil, false); err != nil {
		t.Fatal(err)
	}
	requireEvent(t, recorder, "Warning JobFailed The workflow job failed")
	if err := reconciler.updateWorkflowJobStatus(context.Background(), workflowJob, nativeJob, nil, false); err != nil {
		t.Fatal(err)
	}
	requireNoEvent(t, recorder)
}

func TestRunnerFailureEmitsWarningEventOnce(t *testing.T) {
	scheme := runnerTestScheme(t)
	runnerObject := &actionsv1alpha1.Runner{ObjectMeta: metav1.ObjectMeta{Name: "runner", Namespace: "default"}}
	clusterClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&actionsv1alpha1.Runner{}).WithObjects(runnerObject).Build()
	recorder := events.NewFakeRecorder(2)
	reconciler := &RunnerReconciler{Client: clusterClient, Recorder: recorder}
	if err := reconciler.updateRunnerStatus(context.Background(), runnerObject, metav1.ConditionFalse, "ProjectNotConfigured", "Project is not configured", nil); err != nil {
		t.Fatal(err)
	}
	requireEvent(t, recorder, "Warning ProjectNotConfigured Project is not configured")
	if err := reconciler.updateRunnerStatus(context.Background(), runnerObject, metav1.ConditionFalse, "ProjectNotConfigured", "Project is not configured", nil); err != nil {
		t.Fatal(err)
	}
	requireNoEvent(t, recorder)
}

func requireEvent(t *testing.T, recorder *events.FakeRecorder, expected string) {
	t.Helper()
	select {
	case event := <-recorder.Events:
		if event != expected {
			t.Fatalf("event = %q, want %q", event, expected)
		}
	default:
		t.Fatalf("event was not recorded, want %q", expected)
	}
}

func requireNoEvent(t *testing.T, recorder *events.FakeRecorder) {
	t.Helper()
	select {
	case event := <-recorder.Events:
		t.Fatalf("unexpected event %q", event)
	default:
	}
}
