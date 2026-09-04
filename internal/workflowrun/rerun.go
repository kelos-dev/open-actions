package workflowrun

import (
	"crypto/sha256"
	"encoding/base32"
	"fmt"
	"sort"
	"strings"

	actionsv1alpha1 "github.com/kelos-dev/open-actions/api/v1alpha1"
	"github.com/kelos-dev/open-actions/internal/eventsnapshot"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	resourceNameMaxLength = 63
	matrixFailFastReason  = "MatrixFailFast"
)

var digestEncoding = base32.StdEncoding.WithPadding(base32.NoPadding)

// LatestAttempt returns the highest valid attempt in a WorkflowRun lineage.
func LatestAttempt(root *actionsv1alpha1.WorkflowRun, runs []actionsv1alpha1.WorkflowRun) (*actionsv1alpha1.WorkflowRun, error) {
	latest := root
	attempts := map[int32]struct{}{1: {}}
	for index := range runs {
		candidate := &runs[index]
		rerun := candidate.Spec.Rerun
		if rerun == nil || rerun.OriginalRunRef.Name != root.Name || rerun.OriginalRunRef.UID != root.UID {
			continue
		}
		if candidate.Spec.ProjectRef != root.Spec.ProjectRef || candidate.Spec.WorkflowPath != root.Spec.WorkflowPath || !apiequality.Semantic.DeepEqual(candidate.Spec.Source, root.Spec.Source) {
			continue
		}
		planned := meta.FindStatusCondition(candidate.Status.Conditions, actionsv1alpha1.WorkflowRunConditionPlanned)
		if planned != nil && planned.Status == metav1.ConditionFalse && planned.Reason == "RerunInvalid" {
			continue
		}
		if _, found := attempts[rerun.Attempt]; found {
			return nil, fmt.Errorf("WorkflowRun rerun lineage has multiple attempt %d objects", rerun.Attempt)
		}
		attempts[rerun.Attempt] = struct{}{}
		if latest.Spec.Rerun == nil || rerun.Attempt > latest.Spec.Rerun.Attempt {
			latest = candidate
		}
	}
	return latest, nil
}

// Terminal reports whether a WorkflowRun has a terminal succeeded condition.
func Terminal(run *actionsv1alpha1.WorkflowRun) bool {
	condition := meta.FindStatusCondition(run.Status.Conditions, actionsv1alpha1.WorkflowRunConditionSucceeded)
	return condition != nil && (condition.Status == metav1.ConditionTrue || condition.Status == metav1.ConditionFalse)
}

// Failed reports whether a WorkflowRun completed because one or more jobs failed or timed out.
func Failed(run *actionsv1alpha1.WorkflowRun) bool {
	condition := meta.FindStatusCondition(run.Status.Conditions, actionsv1alpha1.WorkflowRunConditionSucceeded)
	return condition != nil && condition.Status == metav1.ConditionFalse && (condition.Reason == "JobFailed" || condition.Reason == "JobTimedOut")
}

// FailedJobIDs selects failed expanded jobs and their transitive dependents.
func FailedJobIDs(run *actionsv1alpha1.WorkflowRun, jobs []actionsv1alpha1.WorkflowJob) ([]string, error) {
	if !Failed(run) {
		return nil, nil
	}
	if run.Status.Jobs == nil || int32(len(jobs)) != run.Status.Jobs.Total {
		return nil, fmt.Errorf("WorkflowRun %q does not have its complete WorkflowJob history", run.Name)
	}
	selected := make(map[string]struct{})
	selectedLogicalIDs := make(map[string]struct{})
	for index := range jobs {
		job := &jobs[index]
		succeeded := meta.FindStatusCondition(job.Status.Conditions, actionsv1alpha1.WorkflowJobConditionSucceeded)
		failed := job.Status.Result == actionsv1alpha1.WorkflowJobResultFailure || (job.Status.Result == "" && succeeded != nil && succeeded.Status == metav1.ConditionFalse)
		if failed || matrixFailFastCancelled(job) {
			selected[job.Spec.JobID] = struct{}{}
			selectedLogicalIDs[logicalJobID(job)] = struct{}{}
		}
	}
	if len(selected) == 0 {
		return nil, fmt.Errorf("WorkflowRun %q reports failed jobs but no failed WorkflowJobs are available", run.Name)
	}
	for changed := true; changed; {
		changed = false
		for index := range jobs {
			job := &jobs[index]
			if _, found := selected[job.Spec.JobID]; found || !needsSelectedJob(job.Spec.Needs, selectedLogicalIDs) {
				continue
			}
			selected[job.Spec.JobID] = struct{}{}
			selectedLogicalIDs[logicalJobID(job)] = struct{}{}
			changed = true
		}
	}
	jobIDs := make([]string, 0, len(selected))
	for id := range selected {
		jobIDs = append(jobIDs, id)
	}
	sort.Strings(jobIDs)
	return jobIDs, nil
}

func matrixFailFastCancelled(job *actionsv1alpha1.WorkflowJob) bool {
	if job.Spec.Matrix == nil || job.Status.Result != actionsv1alpha1.WorkflowJobResultCancelled {
		return false
	}
	for _, condition := range job.Status.Conditions {
		if condition.Reason == matrixFailFastReason {
			return true
		}
	}
	return false
}

// NewRerun creates the immutable object for the next WorkflowRun attempt.
func NewRerun(root, previous *actionsv1alpha1.WorkflowRun, attempt int32, jobIDs []string) *actionsv1alpha1.WorkflowRun {
	desired := &actionsv1alpha1.WorkflowRun{
		ObjectMeta: metav1.ObjectMeta{
			Name: RerunName(root, attempt), Namespace: root.Namespace,
			Labels: map[string]string{actionsv1alpha1.LabelWorkflowRunRootUID: string(root.UID)},
		},
		Spec: *previous.Spec.DeepCopy(),
	}
	if snapshotName := root.Annotations[eventsnapshot.Annotation]; snapshotName != "" {
		desired.Annotations = map[string]string{eventsnapshot.Annotation: snapshotName}
	}
	desired.Spec.CancelRequested = false
	desired.Spec.Rerun = &actionsv1alpha1.WorkflowRunRerun{
		OriginalRunRef: actionsv1alpha1.WorkflowRunReference{Name: root.Name, UID: root.UID},
		PreviousRunRef: actionsv1alpha1.WorkflowRunReference{Name: previous.Name, UID: previous.UID},
		Attempt:        attempt,
		JobIDs:         append([]string(nil), jobIDs...),
	}
	return desired
}

// RerunName returns the deterministic resource name for a WorkflowRun attempt.
func RerunName(root *actionsv1alpha1.WorkflowRun, attempt int32) string {
	digest := sha256.Sum256([]byte(root.UID))
	suffix := fmt.Sprintf("-attempt-%d-%s", attempt, strings.ToLower(digestEncoding.EncodeToString(digest[:]))[:8])
	base := sanitizeName(root.Name)
	if len(base) > resourceNameMaxLength-len(suffix) {
		base = strings.Trim(base[:resourceNameMaxLength-len(suffix)], "-")
	}
	return base + suffix
}

func sanitizeName(value string) string {
	value = strings.ToLower(value)
	var builder strings.Builder
	lastDash := false
	for _, character := range value {
		valid := character >= 'a' && character <= 'z' || character >= '0' && character <= '9'
		if valid {
			builder.WriteRune(character)
			lastDash = false
		} else if !lastDash {
			builder.WriteByte('-')
			lastDash = true
		}
	}
	return strings.Trim(builder.String(), "-")
}

func logicalJobID(job *actionsv1alpha1.WorkflowJob) string {
	if job.Spec.Matrix != nil {
		return job.Spec.Matrix.LogicalJobID
	}
	return job.Spec.JobID
}

func needsSelectedJob(needs []string, selectedLogicalIDs map[string]struct{}) bool {
	for _, dependency := range needs {
		if _, found := selectedLogicalIDs[dependency]; found {
			return true
		}
	}
	return false
}
