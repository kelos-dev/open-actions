package workflowrun

import (
	"context"
	"crypto/sha256"
	"encoding/base32"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	actionsv1alpha1 "github.com/kelos-dev/open-actions/api/v1alpha1"
	"github.com/kelos-dev/open-actions/internal/eventsnapshot"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
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

// JobHistory filters attempts in newest-first order. Selected jobs cannot fall
// back to older executions, and complete logical jobs replace older expansions.
type JobHistory struct {
	jobIDs     map[string]struct{}
	logicalIDs map[string]struct{}
}

// Add returns the jobs from this attempt that remain effective in the lineage.
func (h *JobHistory) Add(run *actionsv1alpha1.WorkflowRun, jobs []actionsv1alpha1.WorkflowJob) []actionsv1alpha1.WorkflowJob {
	if h.jobIDs == nil {
		h.jobIDs = make(map[string]struct{})
		h.logicalIDs = make(map[string]struct{})
	}
	retained := make([]actionsv1alpha1.WorkflowJob, 0, len(jobs))
	groups := make(map[string][]actionsv1alpha1.WorkflowJob)
	for _, job := range jobs {
		logicalID := logicalJobID(&job)
		groups[logicalID] = append(groups[logicalID], job)
		_, replacedJob := h.jobIDs[job.Spec.JobID]
		_, replacedLogical := h.logicalIDs[logicalID]
		_, selectedLogical := h.jobIDs[logicalID]
		if !replacedJob && !replacedLogical && !selectedLogical {
			retained = append(retained, job)
		}
	}
	for _, job := range jobs {
		h.jobIDs[job.Spec.JobID] = struct{}{}
	}
	for id, group := range groups {
		if group[0].Spec.Matrix == nil || len(group) == int(group[0].Spec.Matrix.JobTotal) {
			h.logicalIDs[id] = struct{}{}
		}
	}
	if run.Spec.Rerun != nil {
		for _, id := range run.Spec.Rerun.JobIDs {
			h.jobIDs[id] = struct{}{}
		}
	}
	return retained
}

// MaxAttempt is the highest rerun attempt a WorkflowRun lineage can reach.
const MaxAttempt = int32(2147483647)

// JobSelection selects which jobs the next WorkflowRun attempt executes.
type JobSelection string

const (
	// AllJobs runs the whole workflow again.
	AllJobs JobSelection = "all"
	// FailedJobs runs the failed jobs and their dependents again.
	FailedJobs JobSelection = "failed"
)

// ConflictError reports that a WorkflowRun lineage does not allow the
// requested rerun. Its message is safe to show to the requester.
type ConflictError struct {
	message string
}

func (e *ConflictError) Error() string {
	return e.message
}

func conflictf(format string, arguments ...any) *ConflictError {
	return &ConflictError{message: fmt.Sprintf(format, arguments...)}
}

// Lineage returns the original WorkflowRun and the latest valid attempt of the
// lineage that run belongs to.
func Lineage(ctx context.Context, reader client.Reader, run *actionsv1alpha1.WorkflowRun) (*actionsv1alpha1.WorkflowRun, *actionsv1alpha1.WorkflowRun, error) {
	root := run
	if run.Spec.Rerun != nil {
		root = &actionsv1alpha1.WorkflowRun{}
		key := client.ObjectKey{Namespace: run.Namespace, Name: run.Spec.Rerun.OriginalRunRef.Name}
		if err := reader.Get(ctx, key, root); err != nil {
			return nil, nil, fmt.Errorf("load original WorkflowRun %q for WorkflowRun %q: %w", key.Name, run.Name, err)
		}
		if root.UID != run.Spec.Rerun.OriginalRunRef.UID || root.Spec.Rerun != nil {
			return nil, nil, fmt.Errorf("WorkflowRun %q does not have a valid original WorkflowRun", run.Name)
		}
	}
	runs := &actionsv1alpha1.WorkflowRunList{}
	if err := reader.List(ctx, runs, client.InNamespace(root.Namespace), client.MatchingLabels{actionsv1alpha1.LabelWorkflowRunRootUID: string(root.UID)}); err != nil {
		return nil, nil, fmt.Errorf("load rerun attempts for WorkflowRun %q: %w", root.Name, err)
	}
	if run.Spec.Rerun != nil {
		found := false
		for index := range runs.Items {
			found = runs.Items[index].UID == run.UID
			if found {
				break
			}
		}
		if !found {
			runs.Items = append(runs.Items, *run.DeepCopy())
		}
	}
	latest, err := LatestAttempt(root, runs.Items)
	if err != nil {
		return nil, nil, fmt.Errorf("resolve rerun attempts for WorkflowRun %q: %w", root.Name, err)
	}
	return root, latest, nil
}

// CreateRerun creates the next attempt of the lineage that run belongs to and
// returns the created WorkflowRun. It returns a ConflictError when the lineage
// state does not allow the requested rerun. The returned WorkflowRun is the
// persisted attempt when an ambiguous create error hid a successful create.
func CreateRerun(ctx context.Context, kube client.Client, run *actionsv1alpha1.WorkflowRun, jobs JobSelection) (*actionsv1alpha1.WorkflowRun, error) {
	root, latest, err := Lineage(ctx, kube, run)
	if err != nil {
		return nil, err
	}
	if !Terminal(latest) {
		return nil, conflictf("the latest workflow attempt is not complete")
	}
	attempt := int32(2)
	if latest.Spec.Rerun != nil {
		if latest.Spec.Rerun.Attempt == MaxAttempt {
			return nil, conflictf("workflow rerun attempt limit reached")
		}
		attempt = latest.Spec.Rerun.Attempt + 1
	}
	var jobIDs []string
	if jobs == FailedJobs {
		if !Failed(latest) {
			return nil, conflictf("the latest workflow attempt did not fail because of a job")
		}
		workflowJobs := &actionsv1alpha1.WorkflowJobList{}
		if err := kube.List(ctx, workflowJobs, client.InNamespace(latest.Namespace), client.MatchingLabels{actionsv1alpha1.LabelWorkflowRunUID: string(latest.UID)}); err != nil {
			return nil, fmt.Errorf("load WorkflowJobs for WorkflowRun %q: %w", latest.Name, err)
		}
		jobIDs, err = FailedJobIDs(latest, workflowJobs.Items)
		if err != nil {
			return nil, &ConflictError{message: err.Error()}
		}
	}
	desired := NewRerun(root, latest, attempt, jobIDs)
	if desired.Annotations[eventsnapshot.Annotation] != "" {
		if err := protectRerunEventSnapshot(ctx, kube, root, desired.Name); err != nil {
			return nil, fmt.Errorf("protect event snapshot for WorkflowRun %q rerun: %w", root.Name, err)
		}
	}
	if err := kube.Create(ctx, desired); err != nil {
		if apierrors.IsAlreadyExists(err) {
			return nil, conflictf("another workflow rerun was requested")
		}
		persisted := &actionsv1alpha1.WorkflowRun{}
		getErr := kube.Get(ctx, client.ObjectKeyFromObject(desired), persisted)
		switch {
		case getErr == nil && MatchingRerun(persisted, desired):
			return persisted, nil
		case apierrors.IsNotFound(getErr):
			if desired.Annotations[eventsnapshot.Annotation] != "" {
				err = errors.Join(err, releaseRerunEventSnapshotProtection(ctx, kube, root, desired.Name))
			}
		case getErr != nil:
			err = errors.Join(err, fmt.Errorf("check rerun WorkflowRun %q after create error: %w", desired.Name, getErr))
		default:
			err = errors.Join(err, fmt.Errorf("WorkflowRun %q does not match the requested rerun", persisted.Name))
		}
		return nil, fmt.Errorf("create rerun WorkflowRun %q for WorkflowRun %q: %w", desired.Name, latest.Name, err)
	}
	return desired, nil
}

// MatchingRerun reports whether an existing WorkflowRun is the requested rerun attempt.
func MatchingRerun(existing, desired *actionsv1alpha1.WorkflowRun) bool {
	return apiequality.Semantic.DeepEqual(existing.Spec, desired.Spec) &&
		existing.Labels[actionsv1alpha1.LabelWorkflowRunRootUID] == desired.Labels[actionsv1alpha1.LabelWorkflowRunRootUID] &&
		existing.Annotations[eventsnapshot.Annotation] == desired.Annotations[eventsnapshot.Annotation]
}

// protectRerunEventSnapshot keeps the original run's GitHub event snapshot
// available until the controller plans the named rerun attempt.
func protectRerunEventSnapshot(ctx context.Context, kube client.Client, root *actionsv1alpha1.WorkflowRun, rerunName string) error {
	if !root.DeletionTimestamp.IsZero() {
		return fmt.Errorf("WorkflowRun %q is being deleted", root.Name)
	}
	secret := &corev1.Secret{}
	name := root.Annotations[eventsnapshot.Annotation]
	if err := kube.Get(ctx, client.ObjectKey{Namespace: root.Namespace, Name: name}, secret); err != nil {
		return err
	}
	data, found := secret.Data[eventsnapshot.DataKey]
	if !secret.DeletionTimestamp.IsZero() || !ownsEventSnapshot(secret, root.Name, root.UID) || secret.Immutable == nil || !*secret.Immutable || !found {
		return fmt.Errorf("GitHub event snapshot Secret %q is invalid for WorkflowRun %q", name, root.Name)
	}
	if _, err := eventsnapshot.Decode(data); err != nil {
		return fmt.Errorf("GitHub event snapshot Secret %q is invalid for WorkflowRun %q: %w", name, root.Name, err)
	}
	if target := root.Annotations[eventsnapshot.RerunTargetAnnotation]; target != "" && target != rerunName {
		return fmt.Errorf("WorkflowRun %q is already creating rerun %q", root.Name, target)
	}
	if controllerutil.ContainsFinalizer(root, eventsnapshot.RerunProtectionFinalizer) && root.Annotations[eventsnapshot.RerunTargetAnnotation] == rerunName {
		return nil
	}
	if root.Annotations == nil {
		root.Annotations = map[string]string{}
	}
	root.Annotations[eventsnapshot.RerunTargetAnnotation] = rerunName
	root.Annotations[eventsnapshot.RerunDeadlineAnnotation] = time.Now().UTC().Add(time.Minute).Format(time.RFC3339Nano)
	controllerutil.AddFinalizer(root, eventsnapshot.RerunProtectionFinalizer)
	return kube.Update(ctx, root)
}

func releaseRerunEventSnapshotProtection(ctx context.Context, kube client.Client, root *actionsv1alpha1.WorkflowRun, rerunName string) error {
	if root.Annotations[eventsnapshot.RerunTargetAnnotation] != rerunName {
		return nil
	}
	delete(root.Annotations, eventsnapshot.RerunTargetAnnotation)
	delete(root.Annotations, eventsnapshot.RerunDeadlineAnnotation)
	controllerutil.RemoveFinalizer(root, eventsnapshot.RerunProtectionFinalizer)
	return kube.Update(ctx, root)
}

func ownsEventSnapshot(secret *corev1.Secret, name string, uid types.UID) bool {
	for _, owner := range secret.OwnerReferences {
		if owner.APIVersion == actionsv1alpha1.GroupVersion.String() && owner.Kind == "WorkflowRun" && owner.Name == name && owner.UID == uid {
			return true
		}
	}
	return false
}
