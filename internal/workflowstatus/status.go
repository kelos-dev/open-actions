package workflowstatus

import (
	actionsv1alpha1 "github.com/kelos-dev/open-actions/api/v1alpha1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	queued    = "Queued"
	running   = "Running"
	succeeded = "Succeeded"
	failed    = "Failed"
)

// Run returns the user-facing status derived from a WorkflowRun's conditions.
func Run(run *actionsv1alpha1.WorkflowRun) string {
	condition := meta.FindStatusCondition(run.Status.Conditions, actionsv1alpha1.WorkflowRunConditionSucceeded)
	if condition == nil {
		return queued
	}
	switch condition.Status {
	case metav1.ConditionTrue:
		return succeeded
	case metav1.ConditionFalse:
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
	if condition != nil {
		switch condition.Status {
		case metav1.ConditionTrue:
			return succeeded
		case metav1.ConditionFalse:
			return failed
		}
	}
	if job.Status.RunnerRef != nil {
		return running
	}
	return queued
}

// JobTerminal reports whether a WorkflowJob has a terminal succeeded condition.
func JobTerminal(job *actionsv1alpha1.WorkflowJob) bool {
	condition := meta.FindStatusCondition(job.Status.Conditions, actionsv1alpha1.WorkflowJobConditionSucceeded)
	return condition != nil && condition.Status != metav1.ConditionUnknown
}
