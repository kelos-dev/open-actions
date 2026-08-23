package workflowstatus

import (
	actionsv1alpha1 "github.com/kelos-dev/open-actions/api/v1alpha1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	queued     = "Queued"
	running    = "Running"
	cancelling = "Cancelling"
	succeeded  = "Succeeded"
	failed     = "Failed"
	skipped    = "Skipped"
	cancelled  = "Cancelled"
	timedOut   = "Timed out"
	waiting    = "Waiting"
	approval   = "Awaiting approval"
)

// Run returns the user-facing status derived from a WorkflowRun's conditions.
func Run(run *actionsv1alpha1.WorkflowRun) string {
	condition := meta.FindStatusCondition(run.Status.Conditions, actionsv1alpha1.WorkflowRunConditionSucceeded)
	if run.Spec.CancelRequested && (condition == nil || condition.Status == metav1.ConditionUnknown) {
		return cancelling
	}
	if condition == nil {
		approved := meta.FindStatusCondition(run.Status.Conditions, actionsv1alpha1.WorkflowRunConditionApproved)
		if approved != nil && approved.Status == metav1.ConditionFalse && approved.Reason == "ApprovalRequired" {
			return approval
		}
		return queued
	}
	switch condition.Status {
	case metav1.ConditionTrue:
		return succeeded
	case metav1.ConditionFalse:
		if condition.Reason == "JobCancelled" || condition.Reason == "RevisionSuperseded" {
			return cancelled
		}
		if condition.Reason == "JobTimedOut" {
			return timedOut
		}
		return failed
	default:
		if run.Status.StartTime != nil {
			return running
		}
		return queued
	}
}

// Job returns the user-facing status derived from a WorkflowJob's conditions and runner assignment.
func Job(job *actionsv1alpha1.WorkflowJob) string {
	condition := meta.FindStatusCondition(job.Status.Conditions, actionsv1alpha1.WorkflowJobConditionSucceeded)
	if condition != nil && condition.Status == metav1.ConditionFalse && condition.Reason == "JobTimedOut" {
		return timedOut
	}
	switch job.Status.Result {
	case actionsv1alpha1.WorkflowJobResultSuccess:
		return succeeded
	case actionsv1alpha1.WorkflowJobResultFailure:
		return failed
	case actionsv1alpha1.WorkflowJobResultSkipped:
		return skipped
	case actionsv1alpha1.WorkflowJobResultCancelled:
		return cancelled
	}
	if condition != nil {
		switch condition.Status {
		case metav1.ConditionTrue:
			return succeeded
		case metav1.ConditionFalse:
			return failed
		}
	}
	cancellation := meta.FindStatusCondition(job.Status.Conditions, actionsv1alpha1.WorkflowJobConditionCancellationRequested)
	if cancellation != nil && cancellation.Status == metav1.ConditionTrue {
		return cancelling
	}
	if job.Status.RunnerRef != nil {
		return running
	}
	ready := meta.FindStatusCondition(job.Status.Conditions, actionsv1alpha1.WorkflowJobConditionReady)
	if ready != nil && ready.Status != metav1.ConditionTrue {
		return waiting
	}
	return queued
}

// JobTerminal reports whether a WorkflowJob has a terminal succeeded condition.
func JobTerminal(job *actionsv1alpha1.WorkflowJob) bool {
	if job.Status.Result != "" {
		return true
	}
	condition := meta.FindStatusCondition(job.Status.Conditions, actionsv1alpha1.WorkflowJobConditionSucceeded)
	return condition != nil && condition.Status != metav1.ConditionUnknown
}
