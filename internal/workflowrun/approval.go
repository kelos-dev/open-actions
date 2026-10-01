package workflowrun

import actionsv1alpha1 "github.com/kelos-dev/open-actions/api/v1alpha1"

// RequiresApproval reports whether a pull request run needs explicit approval.
func RequiresApproval(run *actionsv1alpha1.WorkflowRun) bool {
	return run.Spec.Approval != nil || run.Spec.ForkPullRequest != nil && run.Spec.ForkPullRequest.RequireApproval
}

// AwaitingApproval reports whether any required approval has not been granted.
func AwaitingApproval(run *actionsv1alpha1.WorkflowRun) bool {
	return run.Spec.Approval != nil && !run.Spec.Approval.Approved ||
		run.Spec.ForkPullRequest != nil && run.Spec.ForkPullRequest.RequireApproval && !run.Spec.ForkPullRequest.Approved
}

// Approve grants all approvals required for this run's pinned revision.
func Approve(run *actionsv1alpha1.WorkflowRun) {
	if run.Spec.Approval != nil {
		run.Spec.Approval.Approved = true
	}
	if policy := run.Spec.ForkPullRequest; policy != nil && policy.RequireApproval {
		policy.Approved = true
	}
}
