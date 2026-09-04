package workflowrun

import (
	"slices"
	"testing"

	actionsv1alpha1 "github.com/kelos-dev/open-actions/api/v1alpha1"
	"github.com/kelos-dev/open-actions/internal/eventsnapshot"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestNewRerunPreservesGitHubEventSnapshot(t *testing.T) {
	root := &actionsv1alpha1.WorkflowRun{
		ObjectMeta: metav1.ObjectMeta{
			Name: "ci", Namespace: "default", UID: "root-uid",
			Annotations: map[string]string{eventsnapshot.Annotation: "event-snapshot"},
		},
	}
	desired := NewRerun(root, root, 2, nil)
	if desired.Annotations[eventsnapshot.Annotation] != "event-snapshot" {
		t.Fatalf("rerun annotations = %#v", desired.Annotations)
	}
}

func TestFailedJobIDsIncludesMatrixFailFastCancellations(t *testing.T) {
	run := &actionsv1alpha1.WorkflowRun{
		ObjectMeta: metav1.ObjectMeta{Name: "ci"},
		Status: actionsv1alpha1.WorkflowRunStatus{
			Jobs: &actionsv1alpha1.WorkflowRunJobStatus{Total: 5},
			Conditions: []metav1.Condition{{
				Type: actionsv1alpha1.WorkflowRunConditionSucceeded, Status: metav1.ConditionFalse, Reason: "JobFailed",
			}},
		},
	}
	matrix := func(id string, result actionsv1alpha1.WorkflowJobResult, reason string) actionsv1alpha1.WorkflowJob {
		job := actionsv1alpha1.WorkflowJob{
			Spec: actionsv1alpha1.WorkflowJobSpec{
				JobID:  id,
				Matrix: &actionsv1alpha1.WorkflowJobMatrix{LogicalJobID: "build", JobTotal: 3},
			},
			Status: actionsv1alpha1.WorkflowJobStatus{Result: result},
		}
		if reason != "" {
			job.Status.Conditions = []metav1.Condition{{
				Type: actionsv1alpha1.WorkflowJobConditionSucceeded, Status: metav1.ConditionFalse, Reason: reason,
			}}
		}
		return job
	}
	jobs := []actionsv1alpha1.WorkflowJob{
		matrix("build-matrix-1", actionsv1alpha1.WorkflowJobResultFailure, "JobFailed"),
		matrix("build-matrix-2", actionsv1alpha1.WorkflowJobResultCancelled, matrixFailFastReason),
		matrix("build-matrix-3", actionsv1alpha1.WorkflowJobResultSuccess, ""),
		{Spec: actionsv1alpha1.WorkflowJobSpec{JobID: "report", Needs: []string{"build"}}, Status: actionsv1alpha1.WorkflowJobStatus{Result: actionsv1alpha1.WorkflowJobResultSkipped}},
		{Spec: actionsv1alpha1.WorkflowJobSpec{JobID: "unrelated"}, Status: actionsv1alpha1.WorkflowJobStatus{Result: actionsv1alpha1.WorkflowJobResultCancelled}},
	}

	jobIDs, err := FailedJobIDs(run, jobs)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"build-matrix-1", "build-matrix-2", "report"}
	if !slices.Equal(jobIDs, want) {
		t.Fatalf("failed job IDs = %v, want %v", jobIDs, want)
	}
}
