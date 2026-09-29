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

// https://docs.github.com/en/rest/actions/workflow-runs#re-run-a-job-from-a-workflow-run
func TestJobAndDependentIDs(t *testing.T) {
	jobs := []actionsv1alpha1.WorkflowJob{
		{Spec: actionsv1alpha1.WorkflowJobSpec{JobID: "deploy", Needs: []string{"report"}}},
		{Spec: actionsv1alpha1.WorkflowJobSpec{JobID: "report", Needs: []string{"build"}}},
		{Spec: actionsv1alpha1.WorkflowJobSpec{JobID: "build-matrix-1", Needs: []string{"prepare"}, Matrix: &actionsv1alpha1.WorkflowJobMatrix{LogicalJobID: "build", JobTotal: 2}}},
		{Spec: actionsv1alpha1.WorkflowJobSpec{JobID: "build-matrix-2", Needs: []string{"prepare"}, Matrix: &actionsv1alpha1.WorkflowJobMatrix{LogicalJobID: "build", JobTotal: 2}}},
		{Spec: actionsv1alpha1.WorkflowJobSpec{JobID: "prepare"}},
		{Spec: actionsv1alpha1.WorkflowJobSpec{JobID: "lint"}},
	}
	for _, test := range []struct {
		jobID string
		want  []string
	}{
		{jobID: "build-matrix-1", want: []string{"build-matrix-1", "deploy", "report"}},
		{jobID: "prepare", want: []string{"build-matrix-1", "build-matrix-2", "deploy", "prepare", "report"}},
		{jobID: "report", want: []string{"deploy", "report"}},
		{jobID: "lint", want: []string{"lint"}},
	} {
		t.Run(test.jobID, func(t *testing.T) {
			if got := JobAndDependentIDs(jobs, test.jobID); !slices.Equal(got, test.want) {
				t.Fatalf("selected job IDs = %v, want %v", got, test.want)
			}
		})
	}
}

func TestJobHistoryReplacesDeferredExpansions(t *testing.T) {
	matrix := func(id string, total int32) actionsv1alpha1.WorkflowJob {
		return actionsv1alpha1.WorkflowJob{Spec: actionsv1alpha1.WorkflowJobSpec{JobID: id, Matrix: &actionsv1alpha1.WorkflowJobMatrix{LogicalJobID: "build", JobTotal: total}}}
	}
	root := &actionsv1alpha1.WorkflowRun{}
	tests := []struct {
		name     string
		current  []actionsv1alpha1.WorkflowJob
		selected []string
		previous []actionsv1alpha1.WorkflowJob
		want     []string
	}{
		{name: "partial matrix retains sibling", current: []actionsv1alpha1.WorkflowJob{matrix("build-matrix-2", 2)}, selected: []string{"build-matrix-2"}, previous: []actionsv1alpha1.WorkflowJob{matrix("build-matrix-1", 2), matrix("build-matrix-2", 2)}, want: []string{"build-matrix-1"}},
		{name: "complete matrix replaces placeholder", current: []actionsv1alpha1.WorkflowJob{matrix("build-matrix-1", 1)}, selected: []string{"build"}, previous: []actionsv1alpha1.WorkflowJob{{Spec: actionsv1alpha1.WorkflowJobSpec{JobID: "build"}}}},
		{name: "smaller matrix replaces removed children", current: []actionsv1alpha1.WorkflowJob{matrix("build-matrix-1", 1)}, previous: []actionsv1alpha1.WorkflowJob{matrix("build-matrix-1", 2), matrix("build-matrix-2", 2)}},
		{name: "pending selection excludes previous execution", selected: []string{"build-matrix-2"}, previous: []actionsv1alpha1.WorkflowJob{matrix("build-matrix-1", 2), matrix("build-matrix-2", 2)}, want: []string{"build-matrix-1"}},
		{name: "logical selection excludes previous expansion", selected: []string{"build"}, previous: []actionsv1alpha1.WorkflowJob{matrix("build-matrix-1", 2), matrix("build-matrix-2", 2)}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			history := &JobHistory{}
			current := &actionsv1alpha1.WorkflowRun{Spec: actionsv1alpha1.WorkflowRunSpec{Rerun: &actionsv1alpha1.WorkflowRunRerun{JobIDs: test.selected}}}
			history.Add(current, test.current)
			var ids []string
			for _, job := range history.Add(root, test.previous) {
				ids = append(ids, job.Spec.JobID)
			}
			if !slices.Equal(ids, test.want) {
				t.Fatalf("retained jobs = %v, want %v", ids, test.want)
			}
		})
	}
}
