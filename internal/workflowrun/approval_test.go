package workflowrun

import (
	"testing"

	actionsv1alpha1 "github.com/kelos-dev/open-actions/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestPullRequestApprovalRequirements(t *testing.T) {
	for _, test := range []struct {
		name     string
		fork     *actionsv1alpha1.WorkflowRunForkPullRequest
		approval *actionsv1alpha1.WorkflowRunApproval
		required bool
		waiting  bool
	}{
		{name: "ordinary run"},
		{name: "fork approval disabled", fork: &actionsv1alpha1.WorkflowRunForkPullRequest{}},
		{name: "fork pending", fork: &actionsv1alpha1.WorkflowRunForkPullRequest{RequireApproval: true}, required: true, waiting: true},
		{name: "App pending", approval: &actionsv1alpha1.WorkflowRunApproval{}, required: true, waiting: true},
		{name: "App approved", approval: &actionsv1alpha1.WorkflowRunApproval{Approved: true}, required: true},
		{name: "App approval cannot waive fork approval", fork: &actionsv1alpha1.WorkflowRunForkPullRequest{RequireApproval: true}, approval: &actionsv1alpha1.WorkflowRunApproval{Approved: true}, required: true, waiting: true},
		{name: "fork approval cannot waive App approval", fork: &actionsv1alpha1.WorkflowRunForkPullRequest{RequireApproval: true, Approved: true}, approval: &actionsv1alpha1.WorkflowRunApproval{}, required: true, waiting: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			run := &actionsv1alpha1.WorkflowRun{
				ObjectMeta: metav1.ObjectMeta{Name: "ci", UID: "run-uid"},
				Spec:       actionsv1alpha1.WorkflowRunSpec{ForkPullRequest: test.fork, Approval: test.approval},
			}
			if RequiresApproval(run) != test.required || AwaitingApproval(run) != test.waiting {
				t.Fatalf("required = %t, waiting = %t", RequiresApproval(run), AwaitingApproval(run))
			}
			rerun := NewRerun(run, run, 2, nil)
			if RequiresApproval(rerun) != test.required || AwaitingApproval(rerun) != test.waiting {
				t.Fatal("rerun did not preserve the approval decision")
			}
			Approve(rerun)
			if AwaitingApproval(rerun) || RequiresApproval(rerun) != test.required || AwaitingApproval(run) != test.waiting {
				t.Fatal("approval did not grant exactly the rerun's required approvals")
			}
		})
	}
}
