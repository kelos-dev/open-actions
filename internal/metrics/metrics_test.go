package metrics

import (
	"testing"
	"time"

	actionsv1alpha1 "github.com/kelos-dev/open-actions/api/v1alpha1"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestRecorderPublishesLifecycleDurations(t *testing.T) {
	registry := prometheus.NewRegistry()
	recorder, err := New(registry)
	if err != nil {
		t.Fatal(err)
	}
	base := time.Date(2026, 8, 28, 10, 0, 0, 0, time.UTC)
	start := metav1.NewTime(base.Add(30 * time.Second))
	completion := metav1.NewTime(base.Add(2 * time.Minute))
	run := &actionsv1alpha1.WorkflowRun{
		ObjectMeta: metav1.ObjectMeta{Name: "run", Namespace: "ci"},
		Spec:       actionsv1alpha1.WorkflowRunSpec{ProjectRef: localReference("project")},
		Status: actionsv1alpha1.WorkflowRunStatus{
			StartTime: &start, CompletionTime: &completion,
			Conditions: []metav1.Condition{{
				Type: actionsv1alpha1.WorkflowRunConditionSucceeded, Status: metav1.ConditionTrue, Reason: "JobsSucceeded",
			}},
		},
	}
	recorder.WorkflowRunCompleted(&actionsv1alpha1.WorkflowRunStatus{StartTime: &start}, run)
	recorder.WorkflowRunCompleted(run.Status.DeepCopy(), run)

	readyAt := base.Add(10 * time.Second)
	scheduledAt := metav1.NewTime(base.Add(20 * time.Second))
	jobStart := metav1.NewTime(base.Add(35 * time.Second))
	jobCompletion := metav1.NewTime(base.Add(95 * time.Second))
	job := &actionsv1alpha1.WorkflowJob{
		ObjectMeta: metav1.ObjectMeta{
			Name: "job", Namespace: "ci", Annotations: map[string]string{actionsv1alpha1.AnnotationProjectName: "project"},
		},
		Status: actionsv1alpha1.WorkflowJobStatus{
			StartTime: &jobStart, CompletionTime: &jobCompletion, Result: actionsv1alpha1.WorkflowJobResultFailure,
			Conditions: []metav1.Condition{
				{Type: actionsv1alpha1.WorkflowJobConditionScheduled, Status: metav1.ConditionTrue, LastTransitionTime: scheduledAt},
				{Type: actionsv1alpha1.WorkflowJobConditionSucceeded, Status: metav1.ConditionFalse, Reason: "JobTimedOut"},
			},
		},
	}
	recorder.WorkflowJobScheduled(job, readyAt)
	recorder.WorkflowJobUpdated(&actionsv1alpha1.WorkflowJobStatus{}, job)
	recorder.WebhookRequest("push", "accepted", 2*time.Second)
	recorder.WebhookDelivery("ci", "project", "push", "completed", 12*time.Second)

	want := []struct {
		name   string
		labels map[string]string
		count  uint64
		sum    float64
	}{
		{name: "open_actions_workflow_run_duration_seconds", labels: map[string]string{"namespace": "ci", "project": "project", "conclusion": "success"}, count: 1, sum: 90},
		{name: "open_actions_workflow_job_queue_duration_seconds", labels: map[string]string{"namespace": "ci", "project": "project"}, count: 1, sum: 10},
		{name: "open_actions_workflow_job_startup_duration_seconds", labels: map[string]string{"namespace": "ci", "project": "project"}, count: 1, sum: 15},
		{name: "open_actions_workflow_job_execution_duration_seconds", labels: map[string]string{"namespace": "ci", "project": "project", "conclusion": "timed_out"}, count: 1, sum: 60},
		{name: "open_actions_webhook_request_duration_seconds", labels: map[string]string{"event": "push", "result": "accepted"}, count: 1, sum: 2},
		{name: "open_actions_webhook_delivery_duration_seconds", labels: map[string]string{"namespace": "ci", "project": "project", "event": "push", "result": "completed"}, count: 1, sum: 12},
	}
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, expected := range want {
		histogram := findHistogram(t, families, expected.name, expected.labels)
		if histogram.GetSampleCount() != expected.count || histogram.GetSampleSum() != expected.sum {
			t.Errorf("%s count = %d, sum = %v; want count %d, sum %v", expected.name, histogram.GetSampleCount(), histogram.GetSampleSum(), expected.count, expected.sum)
		}
	}
}

func TestRecorderRejectsInvalidLifecycleIntervals(t *testing.T) {
	registry := prometheus.NewRegistry()
	recorder, err := New(registry)
	if err != nil {
		t.Fatal(err)
	}
	base := time.Date(2026, 8, 28, 10, 0, 0, 0, time.UTC)
	start := metav1.NewTime(base.Add(time.Minute))
	completion := metav1.NewTime(base)
	run := &actionsv1alpha1.WorkflowRun{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ci"},
		Spec:       actionsv1alpha1.WorkflowRunSpec{ProjectRef: localReference("project")},
		Status:     actionsv1alpha1.WorkflowRunStatus{StartTime: &start, CompletionTime: &completion},
	}
	recorder.WorkflowRunCompleted(&actionsv1alpha1.WorkflowRunStatus{}, run)

	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() == "open_actions_workflow_run_duration_seconds" {
			t.Fatalf("invalid interval published a metric: %#v", family)
		}
	}
}

func TestWebhookEventUsesBoundedLabels(t *testing.T) {
	if got := webhookEvent("user-controlled-event"); got != "unknown" {
		t.Fatalf("webhook event label = %q, want unknown", got)
	}
	if got := webhookEvent("check_run"); got != "check_run" {
		t.Fatalf("check run event label = %q", got)
	}
}

func TestWorkflowJobConclusionUsesCancelledResult(t *testing.T) {
	job := &actionsv1alpha1.WorkflowJob{Status: actionsv1alpha1.WorkflowJobStatus{
		Result: actionsv1alpha1.WorkflowJobResultCancelled,
		Conditions: []metav1.Condition{{
			Type: actionsv1alpha1.WorkflowJobConditionSucceeded, Status: metav1.ConditionFalse, Reason: "CancellationRequested",
		}},
	}}
	if got := workflowJobConclusion(job); got != "cancelled" {
		t.Fatalf("workflow job conclusion = %q, want cancelled", got)
	}
}

func findHistogram(t *testing.T, families []*dto.MetricFamily, name string, labels map[string]string) *dto.Histogram {
	t.Helper()
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.Metric {
			matched := len(metric.Label) == len(labels)
			for _, pair := range metric.Label {
				if labels[pair.GetName()] != pair.GetValue() {
					matched = false
					break
				}
			}
			if matched {
				return metric.Histogram
			}
		}
	}
	t.Fatalf("metric %q with labels %#v was not gathered", name, labels)
	return nil
}

func localReference(name string) corev1.LocalObjectReference {
	return corev1.LocalObjectReference{Name: name}
}
