package workflowstatus

import (
	"testing"

	actionsv1alpha1 "github.com/kelos-dev/open-actions/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestRun(t *testing.T) {
	started := metav1.Now()
	for _, test := range []struct {
		name string
		run  *actionsv1alpha1.WorkflowRun
		want string
	}{
		{name: "no condition", run: &actionsv1alpha1.WorkflowRun{}, want: "Queued"},
		{name: "waiting", run: workflowRunWithCondition(metav1.ConditionUnknown, nil), want: "Queued"},
		{name: "running", run: workflowRunWithCondition(metav1.ConditionUnknown, &started), want: "Running"},
		{name: "succeeded", run: workflowRunWithCondition(metav1.ConditionTrue, &started), want: "Succeeded"},
		{name: "failed", run: workflowRunWithCondition(metav1.ConditionFalse, &started), want: "Failed"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := Run(test.run); got != test.want {
				t.Fatalf("Run() = %q, want %q", got, test.want)
			}
		})
	}
}

func TestJob(t *testing.T) {
	for _, test := range []struct {
		name string
		job  *actionsv1alpha1.WorkflowJob
		want string
	}{
		{name: "waiting", job: &actionsv1alpha1.WorkflowJob{}, want: "Queued"},
		{name: "waiting for approval", job: workflowJobWaitingForApproval(), want: "Waiting for approval"},
		{name: "running", job: &actionsv1alpha1.WorkflowJob{Status: actionsv1alpha1.WorkflowJobStatus{RunnerRef: &corev1.LocalObjectReference{Name: "runner"}}}, want: "Running"},
		{name: "succeeded", job: workflowJobWithCondition(metav1.ConditionTrue), want: "Succeeded"},
		{name: "failed", job: workflowJobWithCondition(metav1.ConditionFalse), want: "Failed"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := Job(test.job); got != test.want {
				t.Fatalf("Job() = %q, want %q", got, test.want)
			}
		})
	}
}

func workflowJobWaitingForApproval() *actionsv1alpha1.WorkflowJob {
	job := &actionsv1alpha1.WorkflowJob{Spec: actionsv1alpha1.WorkflowJobSpec{Environment: &actionsv1alpha1.WorkflowJobEnvironment{Name: "production"}}}
	meta.SetStatusCondition(&job.Status.Conditions, metav1.Condition{Type: actionsv1alpha1.WorkflowJobConditionEnvironmentApproved, Status: metav1.ConditionFalse, Reason: "ApprovalRequired"})
	return job
}

func TestJobTerminal(t *testing.T) {
	for _, test := range []struct {
		status metav1.ConditionStatus
		want   bool
	}{
		{status: metav1.ConditionUnknown, want: false},
		{status: metav1.ConditionTrue, want: true},
		{status: metav1.ConditionFalse, want: true},
	} {
		if got := JobTerminal(workflowJobWithCondition(test.status)); got != test.want {
			t.Errorf("JobTerminal(%s) = %t, want %t", test.status, got, test.want)
		}
	}
}

func workflowRunWithCondition(status metav1.ConditionStatus, startTime *metav1.Time) *actionsv1alpha1.WorkflowRun {
	return &actionsv1alpha1.WorkflowRun{Status: actionsv1alpha1.WorkflowRunStatus{
		StartTime: startTime,
		Conditions: []metav1.Condition{{
			Type: actionsv1alpha1.WorkflowRunConditionSucceeded, Status: status,
		}},
	}}
}

func workflowJobWithCondition(status metav1.ConditionStatus) *actionsv1alpha1.WorkflowJob {
	return &actionsv1alpha1.WorkflowJob{Status: actionsv1alpha1.WorkflowJobStatus{Conditions: []metav1.Condition{{
		Type: actionsv1alpha1.WorkflowJobConditionSucceeded, Status: status,
	}}}}
}
