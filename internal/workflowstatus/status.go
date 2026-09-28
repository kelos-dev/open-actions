package workflowstatus

import (
	actionsv1alpha1 "github.com/kelos-dev/open-actions/api/v1alpha1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// The user-facing statuses reported for WorkflowRuns and WorkflowJobs.
const (
	Queued           = "Queued"
	Running          = "Running"
	Cancelling       = "Cancelling"
	Succeeded        = "Succeeded"
	Failed           = "Failed"
	Skipped          = "Skipped"
	Cancelled        = "Cancelled"
	TimedOut         = "Timed out"
	Waiting          = "Waiting"
	AwaitingApproval = "Awaiting approval"
)

// Run returns the user-facing status derived from a WorkflowRun's conditions.
func Run(run *actionsv1alpha1.WorkflowRun) string {
	condition := meta.FindStatusCondition(run.Status.Conditions, actionsv1alpha1.WorkflowRunConditionSucceeded)
	if run.Spec.CancelRequested && (condition == nil || condition.Status == metav1.ConditionUnknown) {
		return Cancelling
	}
	if condition == nil {
		approved := meta.FindStatusCondition(run.Status.Conditions, actionsv1alpha1.WorkflowRunConditionApproved)
		if approved != nil && approved.Status == metav1.ConditionFalse && approved.Reason == "ApprovalRequired" {
			return AwaitingApproval
		}
		return Queued
	}
	switch condition.Status {
	case metav1.ConditionTrue:
		return Succeeded
	case metav1.ConditionFalse:
		if condition.Reason == "JobCancelled" || condition.Reason == "RevisionSuperseded" {
			return Cancelled
		}
		if condition.Reason == "JobTimedOut" {
			return TimedOut
		}
		return Failed
	default:
		if run.Status.StartTime != nil {
			return Running
		}
		return Queued
	}
}

// Job returns the user-facing status derived from a WorkflowJob's conditions and runner assignment.
func Job(job *actionsv1alpha1.WorkflowJob) string {
	condition := meta.FindStatusCondition(job.Status.Conditions, actionsv1alpha1.WorkflowJobConditionSucceeded)
	if condition != nil && condition.Status == metav1.ConditionFalse && condition.Reason == "JobTimedOut" {
		return TimedOut
	}
	switch job.Status.Result {
	case actionsv1alpha1.WorkflowJobResultSuccess:
		return Succeeded
	case actionsv1alpha1.WorkflowJobResultFailure:
		return Failed
	case actionsv1alpha1.WorkflowJobResultSkipped:
		return Skipped
	case actionsv1alpha1.WorkflowJobResultCancelled:
		return Cancelled
	}
	if condition != nil {
		switch condition.Status {
		case metav1.ConditionTrue:
			return Succeeded
		case metav1.ConditionFalse:
			return Failed
		}
	}
	cancellation := meta.FindStatusCondition(job.Status.Conditions, actionsv1alpha1.WorkflowJobConditionCancellationRequested)
	if cancellation != nil && cancellation.Status == metav1.ConditionTrue {
		return Cancelling
	}
	if job.Status.RunnerRef != nil {
		return Running
	}
	ready := meta.FindStatusCondition(job.Status.Conditions, actionsv1alpha1.WorkflowJobConditionReady)
	if ready != nil && ready.Status != metav1.ConditionTrue {
		return Waiting
	}
	return Queued
}

// JobTerminal reports whether a WorkflowJob has a terminal succeeded condition.
func JobTerminal(job *actionsv1alpha1.WorkflowJob) bool {
	if job.Status.Result != "" {
		return true
	}
	condition := meta.FindStatusCondition(job.Status.Conditions, actionsv1alpha1.WorkflowJobConditionSucceeded)
	return condition != nil && condition.Status != metav1.ConditionUnknown
}
