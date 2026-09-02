package controller

import (
	"context"
	"testing"
	"time"

	actionsv1alpha1 "github.com/kelos-dev/open-actions/api/v1alpha1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestWorkflowJobQueueStartedAt(t *testing.T) {
	created := time.Date(2026, 8, 28, 10, 0, 0, 0, time.UTC)
	plannedAt := metav1.NewTime(created.Add(5 * time.Second))
	readyAt := metav1.NewTime(created.Add(20 * time.Second))
	planned := &metav1.Condition{Type: actionsv1alpha1.WorkflowRunConditionPlanned, Status: metav1.ConditionTrue, LastTransitionTime: plannedAt}
	job := &actionsv1alpha1.WorkflowJob{ObjectMeta: metav1.ObjectMeta{CreationTimestamp: metav1.NewTime(created)}}
	if got := workflowJobQueueStartedAt(job, planned); !got.Equal(plannedAt.Time) {
		t.Fatalf("unconditional job queue start = %s, want %s", got, plannedAt.Time)
	}
	job.Status.Conditions = []metav1.Condition{{
		Type: actionsv1alpha1.WorkflowJobConditionReady, Status: metav1.ConditionTrue, LastTransitionTime: readyAt,
	}}
	if got := workflowJobQueueStartedAt(job, planned); !got.Equal(readyAt.Time) {
		t.Fatalf("ready job queue start = %s, want %s", got, readyAt.Time)
	}
}

func TestWorkflowJobAssignmentRecordsMetricsAfterPatch(t *testing.T) {
	scheme := runnerTestScheme(t)
	created := time.Date(2026, 8, 28, 10, 0, 0, 0, time.UTC)
	workflowJob := &actionsv1alpha1.WorkflowJob{
		ObjectMeta: metav1.ObjectMeta{Name: "build", Namespace: "default", CreationTimestamp: metav1.NewTime(created)},
	}
	clusterClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&actionsv1alpha1.WorkflowJob{}).
		WithObjects(workflowJob).
		Build()
	metrics := &recordingDurationMetrics{}
	reconciler := &RunnerReconciler{Client: clusterClient, Metrics: metrics}
	first := &actionsv1alpha1.WorkflowJob{}
	second := &actionsv1alpha1.WorkflowJob{}
	key := client.ObjectKeyFromObject(workflowJob)
	if err := clusterClient.Get(context.Background(), key, first); err != nil {
		t.Fatal(err)
	}
	if err := clusterClient.Get(context.Background(), key, second); err != nil {
		t.Fatal(err)
	}
	if err := reconciler.assignWorkflowJob(context.Background(), first, "runner-1", created); err != nil {
		t.Fatal(err)
	}
	if err := reconciler.assignWorkflowJob(context.Background(), second, "runner-2", created); err == nil {
		t.Fatal("conflicting assignment succeeded")
	}
	if metrics.jobSchedules != 1 {
		t.Fatalf("workflow job metric schedules = %d, want 1", metrics.jobSchedules)
	}
}

func TestWorkflowJobStatusRecordsMetricsOnce(t *testing.T) {
	scheme := runnerTestScheme(t)
	base := time.Date(2026, 8, 28, 10, 0, 0, 0, time.UTC)
	scheduledAt := metav1.NewTime(base)
	start := metav1.NewTime(base.Add(10 * time.Second))
	completion := metav1.NewTime(base.Add(time.Minute))
	workflowJob := &actionsv1alpha1.WorkflowJob{
		TypeMeta:   metav1.TypeMeta{APIVersion: actionsv1alpha1.GroupVersion.String(), Kind: "WorkflowJob"},
		ObjectMeta: metav1.ObjectMeta{Name: "build", Namespace: "default"},
		Status: actionsv1alpha1.WorkflowJobStatus{
			RunnerRef: &corev1.LocalObjectReference{Name: "runner"},
			Conditions: []metav1.Condition{{
				Type: actionsv1alpha1.WorkflowJobConditionScheduled, Status: metav1.ConditionTrue, LastTransitionTime: scheduledAt,
			}},
		},
	}
	nativeJob := &batchv1.Job{Status: batchv1.JobStatus{
		StartTime: &start, CompletionTime: &completion,
		Conditions: []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}},
	}}
	clusterClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&actionsv1alpha1.WorkflowJob{}).WithObjects(workflowJob).Build()
	metrics := &recordingDurationMetrics{}
	reconciler := &RunnerReconciler{Client: clusterClient, Metrics: metrics}
	if err := reconciler.updateWorkflowJobStatus(context.Background(), workflowJob, nativeJob, &start, nil, false); err != nil {
		t.Fatal(err)
	}
	stored := &actionsv1alpha1.WorkflowJob{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(workflowJob), stored); err != nil {
		t.Fatal(err)
	}
	if err := reconciler.updateWorkflowJobStatus(context.Background(), stored, nativeJob, &start, nil, false); err != nil {
		t.Fatal(err)
	}
	if metrics.jobUpdates != 1 {
		t.Fatalf("workflow job metric updates = %d, want 1", metrics.jobUpdates)
	}
}

func TestWorkflowRunCompletionRecordsMetricsOnce(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := actionsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	base := time.Date(2026, 8, 28, 10, 0, 0, 0, time.UTC)
	start := metav1.NewTime(base)
	completion := metav1.NewTime(base.Add(time.Minute))
	run := &actionsv1alpha1.WorkflowRun{ObjectMeta: metav1.ObjectMeta{Name: "ci", Namespace: "default", UID: types.UID("run-uid")}}
	job := &actionsv1alpha1.WorkflowJob{
		ObjectMeta: metav1.ObjectMeta{
			Name: "build", Namespace: run.Namespace,
			Labels: map[string]string{actionsv1alpha1.LabelWorkflowRunUID: string(run.UID)},
		},
		Spec: actionsv1alpha1.WorkflowJobSpec{JobID: "build"},
		Status: actionsv1alpha1.WorkflowJobStatus{
			Result: actionsv1alpha1.WorkflowJobResultSuccess, StartTime: &start, CompletionTime: &completion,
		},
	}
	clusterClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&actionsv1alpha1.WorkflowRun{}).WithObjects(run, job).Build()
	metrics := &recordingDurationMetrics{}
	reconciler := &WorkflowRunReconciler{Client: clusterClient, APIReader: clusterClient, Metrics: metrics}
	if _, err := reconciler.observeWorkflowJobs(context.Background(), run, "CI", 1); err != nil {
		t.Fatal(err)
	}
	stored := &actionsv1alpha1.WorkflowRun{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(run), stored); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.observeWorkflowJobs(context.Background(), stored, "CI", 1); err != nil {
		t.Fatal(err)
	}
	if metrics.runCompletions != 1 {
		t.Fatalf("workflow run metric completions = %d, want 1", metrics.runCompletions)
	}
}

type recordingDurationMetrics struct {
	runCompletions int
	jobSchedules   int
	jobUpdates     int
}

func (m *recordingDurationMetrics) WorkflowRunCompleted(previous *actionsv1alpha1.WorkflowRunStatus, _ *actionsv1alpha1.WorkflowRun) {
	if previous.CompletionTime == nil {
		m.runCompletions++
	}
}

func (m *recordingDurationMetrics) WorkflowJobScheduled(_ *actionsv1alpha1.WorkflowJob, _ time.Time) {
	m.jobSchedules++
}

func (m *recordingDurationMetrics) WorkflowJobUpdated(_ *actionsv1alpha1.WorkflowJobStatus, _ *actionsv1alpha1.WorkflowJob) {
	m.jobUpdates++
}

func (m *recordingDurationMetrics) WebhookRequest(_, _ string, _ time.Duration) {}

func (m *recordingDurationMetrics) WebhookDelivery(_, _, _, _ string, _ time.Duration) {}
