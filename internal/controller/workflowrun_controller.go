package controller

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base32"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	actionsv1alpha1 "github.com/kelos-dev/open-actions/api/v1alpha1"
	"github.com/kelos-dev/open-actions/internal/eventsnapshot"
	workflowexpression "github.com/kelos-dev/open-actions/internal/expression"
	githubclient "github.com/kelos-dev/open-actions/internal/github"
	"github.com/kelos-dev/open-actions/internal/gitrepository"
	actionmetrics "github.com/kelos-dev/open-actions/internal/metrics"
	"github.com/kelos-dev/open-actions/internal/projectvalue"
	"github.com/kelos-dev/open-actions/internal/runner"
	"github.com/kelos-dev/open-actions/internal/workflow"
	"github.com/kelos-dev/open-actions/internal/workflowcontext"
	"github.com/kelos-dev/open-actions/internal/workflowsnapshot"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apiEquality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/tools/events"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	jobPlanKey  = "job.json"
	jobNeedsKey = "needs.json"
	// deferredJobPlanKey is persisted in planning ConfigMaps and must remain
	// stable for stored WorkflowRuns.
	deferredJobPlanKey               = "matrix.json"
	workflowPlanKey                  = "workflow.json"
	maxJobPlanBytes                  = 900_000
	resourceNameMaxLength            = 63
	workflowJobNameDigestLength      = 16
	workflowJobDisplayNameMaxLength  = 256
	workflowJobIDMaxLength           = 256
	workflowRunCancellationFinalizer = "actions.kelos.dev/concurrency-cancellation"
	workflowRunGitHubStatusFinalizer = "actions.kelos.dev/github-status"
	workflowRunScheduleFinalizer     = "actions.kelos.dev/schedule-idempotency"
	defaultJobTimeout                = time.Duration(workflow.DefaultJobTimeoutMinutes) * time.Minute
	defaultMaxJobTimeout             = 6 * time.Hour
	workflowRunSequencePrefix        = "open-actions-run-sequence-"
	workflowRunSequenceScopeKey      = "scope"
	workflowRunSequenceNextKey       = "next"
	maxGitHubCompatibleNumber        = int64(9_007_199_254_740_991)
	maxCommitStatusDescriptionRunes  = 140
	maxCommitStatusContextRunes      = 100
	githubStatusOwnerPrefix          = "ghso-"
	githubStatusOwnerDataKey         = "owner.json"
	githubStatusLeaseDuration        = 5 * time.Minute
	deferredJobResultRunsOn          = "deferred-planning"
	matrixEvaluationResultRunsOn     = "matrix-evaluation"
)

var digestEncoding = base32.StdEncoding.WithPadding(base32.NoPadding)

var errGitHubProjectIdentityMismatch = errors.New("GitHub reporting Project identity changed")

type planningFailureDisposition uint8

const (
	planningFailureRetry planningFailureDisposition = iota
	planningFailureTerminal
)

type terminalPlanningError struct {
	cause error
}

func (e *terminalPlanningError) Error() string {
	return e.cause.Error()
}

func (e *terminalPlanningError) Unwrap() error {
	return e.cause
}

type terminalNeedsContextError struct {
	cause error
}

func (e *terminalNeedsContextError) Error() string {
	return e.cause.Error()
}

func (e *terminalNeedsContextError) Unwrap() error {
	return e.cause
}

type projectValuesUnavailableError struct {
	cause error
}

func (e *projectValuesUnavailableError) Error() string {
	return e.cause.Error()
}

func (e *projectValuesUnavailableError) Unwrap() error {
	return e.cause
}

type WorkflowRunReconciler struct {
	client.Client
	APIReader          client.Reader
	GitHub             *githubclient.Client
	GitRepository      *gitrepository.Client
	GitHubAPIBase      string
	GitHubServerURL    string
	ActionCloneBaseURL string
	ConsoleURL         string
	MaxJobTimeout      time.Duration
	Now                func() time.Time
	Recorder           events.EventRecorder
	Metrics            actionmetrics.DurationRecorder
}

func (r *WorkflowRunReconciler) Reconcile(ctx context.Context, request ctrl.Request) (result ctrl.Result, reconcileErr error) {
	defer func() {
		result, reconcileErr = requeueAfterGitHubRateLimit(ctx, result, reconcileErr, r.now())
	}()
	run := &actionsv1alpha1.WorkflowRun{}
	if err := r.Get(ctx, request.NamespacedName, run); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	var rerunPrevious *actionsv1alpha1.WorkflowRun
	if run.Spec.Rerun != nil && !terminalRun(run) {
		var err error
		rerunPrevious, err = r.validateWorkflowRunRerun(ctx, run)
		if err != nil {
			terminal := &terminalPlanningError{}
			if !errors.As(err, &terminal) {
				return ctrl.Result{}, err
			}
			if releaseErr := r.releaseRerunEventSnapshotProtection(ctx, run); releaseErr != nil {
				return ctrl.Result{}, errors.Join(err, releaseErr)
			}
			return r.planningFailed(ctx, run, "RerunInvalid", err, planningFailureTerminal)
		}
		planned := meta.FindStatusCondition(run.Status.Conditions, actionsv1alpha1.WorkflowRunConditionPlanned)
		if run.Annotations[eventsnapshot.Annotation] != "" && run.Status.Jobs == nil && (planned == nil || planned.Status != metav1.ConditionTrue) {
			if _, err := r.githubEventSnapshot(ctx, run); err != nil {
				disposition := planningFailureRetry
				terminal := &terminalPlanningError{}
				if errors.As(err, &terminal) {
					disposition = planningFailureTerminal
				}
				return r.planningFailed(ctx, run, "EventSnapshotUnavailable", err, disposition)
			}
		}
	}
	if run.DeletionTimestamp.IsZero() {
		if err := r.ensureWorkflowRunLineageLabel(ctx, run); err != nil {
			return ctrl.Result{}, err
		}
		if !terminalRun(run) {
			project := &actionsv1alpha1.Project{}
			projectKey := client.ObjectKey{Namespace: run.Namespace, Name: run.Spec.ProjectRef.Name}
			if err := r.APIReader.Get(ctx, projectKey, project); err == nil {
				if err := r.ensureWorkflowRunIdentity(ctx, run, project, rerunPrevious); err != nil {
					return ctrl.Result{}, err
				}
			} else if !apierrors.IsNotFound(err) {
				return ctrl.Result{}, err
			}
		}
	}
	if err := r.ensureWorkflowRunGitHubStatusFinalizer(ctx, run); err != nil {
		return ctrl.Result{}, err
	}
	if !run.DeletionTimestamp.IsZero() {
		return r.finalizeCanceledWorkflowRun(ctx, run)
	}
	reportError := errors.Join(r.reconcileGitHubStatus(ctx, run), r.reconcileGitHubJobStatuses(ctx, run))
	result, reconcileError := r.reconcileWorkflowRun(ctx, run)
	if reconcileError != nil {
		return result, errors.Join(reportError, reconcileError)
	}
	if reportError != nil {
		if !terminalRun(run) && (result.Requeue || result.RequeueAfter > 0) {
			ctrl.LoggerFrom(ctx).Error(reportError, "GitHub reporting failed while workflow reconciliation is pending")
			if delay, limited := githubclient.RetryDelay(reportError, r.now()); limited {
				result.Requeue = false
				result.RequeueAfter = max(result.RequeueAfter, delay)
			}
			return result, nil
		}
		return result, reportError
	}
	return result, nil
}

func (r *WorkflowRunReconciler) ensureWorkflowRunGitHubStatusFinalizer(ctx context.Context, run *actionsv1alpha1.WorkflowRun) error {
	if !run.DeletionTimestamp.IsZero() || !r.githubStatusEnabled(run) || controllerutil.ContainsFinalizer(run, workflowRunGitHubStatusFinalizer) {
		return nil
	}
	before := run.DeepCopy()
	controllerutil.AddFinalizer(run, workflowRunGitHubStatusFinalizer)
	return r.Patch(ctx, run, client.MergeFrom(before))
}

func (r *WorkflowRunReconciler) ensureWorkflowRunIdentity(ctx context.Context, run *actionsv1alpha1.WorkflowRun, project *actionsv1alpha1.Project, rerunPrevious *actionsv1alpha1.WorkflowRun) error {
	if run.Status.Identity != nil {
		return nil
	}
	attempt := int32(1)
	var id, number int64
	if run.Spec.Rerun == nil {
		var err error
		id, err = r.allocateWorkflowRunSequence(ctx, project, "id")
		if err != nil {
			return fmt.Errorf("allocate ID for WorkflowRun %q: %w", run.Name, err)
		}
		numberScope := strings.Join([]string{
			"number",
			strconv.FormatInt(run.Spec.Source.GitHub.Repository.ID, 10),
			run.Spec.WorkflowPath,
		}, "\x00")
		number, err = r.allocateWorkflowRunSequence(ctx, project, numberScope)
		if err != nil {
			return fmt.Errorf("allocate number for WorkflowRun %q: %w", run.Name, err)
		}
	} else {
		attempt = run.Spec.Rerun.Attempt
		previousRef := run.Spec.Rerun.PreviousRunRef
		if rerunPrevious == nil {
			return fmt.Errorf("previous WorkflowRun %q is required to allocate identity for WorkflowRun %q", previousRef.Name, run.Name)
		}
		if rerunPrevious.Name != previousRef.Name || rerunPrevious.UID != previousRef.UID {
			return fmt.Errorf("previous WorkflowRun %q does not match the rerun reference for WorkflowRun %q", rerunPrevious.Name, run.Name)
		}
		if rerunPrevious.Status.Identity == nil {
			return fmt.Errorf("previous WorkflowRun %q has no run identity for WorkflowRun %q", rerunPrevious.Name, run.Name)
		}
		id = rerunPrevious.Status.Identity.ID
		number = rerunPrevious.Status.Identity.Number
	}
	before := run.DeepCopy()
	run.Status.Identity = &actionsv1alpha1.WorkflowRunIdentityStatus{
		ID: id, Number: number, Attempt: attempt,
	}
	if r.ConsoleURL != "" {
		run.Status.Identity.URL = workflowRunConsoleURL(r.ConsoleURL, run)
	}
	return r.Status().Patch(ctx, run, client.MergeFrom(before))
}

func (r *WorkflowRunReconciler) allocateWorkflowRunSequence(ctx context.Context, project *actionsv1alpha1.Project, scope string) (int64, error) {
	identity := project.Name + "\x00" + scope
	digest := sha256.Sum256([]byte(identity))
	name := workflowRunSequencePrefix + strings.ToLower(digestEncoding.EncodeToString(digest[:]))[:20]
	var allocated int64
	err := retry.OnError(retry.DefaultRetry, func(err error) bool {
		return apierrors.IsAlreadyExists(err) || apierrors.IsConflict(err)
	}, func() error {
		sequence := &corev1.ConfigMap{}
		key := client.ObjectKey{Namespace: project.Namespace, Name: name}
		if err := r.APIReader.Get(ctx, key, sequence); err != nil {
			if !apierrors.IsNotFound(err) {
				return err
			}
			sequence = &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: project.Namespace},
				Data: map[string]string{
					workflowRunSequenceScopeKey: identity,
					workflowRunSequenceNextKey:  "2",
				},
			}
			if err := r.Create(ctx, sequence); err != nil {
				return err
			}
			allocated = 1
			return nil
		}
		if sequence.Data[workflowRunSequenceScopeKey] != identity {
			return fmt.Errorf("run sequence ConfigMap %q does not belong to Project %q", sequence.Name, project.Name)
		}
		next, err := strconv.ParseInt(sequence.Data[workflowRunSequenceNextKey], 10, 64)
		if err != nil || next < 1 || next > maxGitHubCompatibleNumber {
			return fmt.Errorf("run sequence ConfigMap %q contains an invalid next value", sequence.Name)
		}
		if next == maxGitHubCompatibleNumber {
			return fmt.Errorf("run sequence ConfigMap %q exhausted its numeric range", sequence.Name)
		}
		sequence.Data[workflowRunSequenceNextKey] = strconv.FormatInt(next+1, 10)
		if err := r.Update(ctx, sequence); err != nil {
			return err
		}
		allocated = next
		return nil
	})
	return allocated, err
}

func (r *WorkflowRunReconciler) ensureWorkflowRunLineageLabel(ctx context.Context, run *actionsv1alpha1.WorkflowRun) error {
	rootUID := run.UID
	if run.Spec.Rerun != nil {
		rootUID = run.Spec.Rerun.OriginalRunRef.UID
	}
	if run.Labels[actionsv1alpha1.LabelWorkflowRunRootUID] == string(rootUID) {
		return nil
	}
	before := run.DeepCopy()
	if run.Labels == nil {
		run.Labels = map[string]string{}
	}
	run.Labels[actionsv1alpha1.LabelWorkflowRunRootUID] = string(rootUID)
	return r.Patch(ctx, run, client.MergeFrom(before))
}

func (r *WorkflowRunReconciler) githubStatusEnabled(run *actionsv1alpha1.WorkflowRun) bool {
	planned := meta.FindStatusCondition(run.Status.Conditions, actionsv1alpha1.WorkflowRunConditionPlanned)
	if run.Spec.Rerun != nil && planned != nil && planned.Status == metav1.ConditionFalse && planned.Reason == "RerunInvalid" {
		return false
	}
	if r.GitHub == nil || run.Spec.Source.Type != actionsv1alpha1.SourceTypeGitHub || run.Spec.Source.GitHub == nil || run.UID == "" {
		return false
	}
	switch run.Spec.Source.GitHub.Event.Name {
	case actionsv1alpha1.GitHubEventNamePush, actionsv1alpha1.GitHubEventNamePullRequest, actionsv1alpha1.GitHubEventNameMergeGroup:
		return true
	default:
		return false
	}
}

func (r *WorkflowRunReconciler) validateWorkflowRunRerun(ctx context.Context, run *actionsv1alpha1.WorkflowRun) (*actionsv1alpha1.WorkflowRun, error) {
	rerun := run.Spec.Rerun
	previous := &actionsv1alpha1.WorkflowRun{}
	key := client.ObjectKey{Namespace: run.Namespace, Name: rerun.PreviousRunRef.Name}
	if err := r.APIReader.Get(ctx, key, previous); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, &terminalPlanningError{cause: fmt.Errorf("previous WorkflowRun %q does not exist", key.Name)}
		}
		return nil, err
	}
	if previous.UID != rerun.PreviousRunRef.UID {
		return nil, &terminalPlanningError{cause: fmt.Errorf("previous WorkflowRun %q has a different UID", previous.Name)}
	}
	if !terminalRun(previous) {
		return nil, &terminalPlanningError{cause: fmt.Errorf("previous WorkflowRun %q is not complete", previous.Name)}
	}
	if previous.Status.Identity == nil {
		return nil, &terminalPlanningError{cause: fmt.Errorf("previous WorkflowRun %q has no run identity", previous.Name)}
	}
	if !apiEquality.Semantic.DeepEqual(run.Spec.ProjectRef, previous.Spec.ProjectRef) ||
		!apiEquality.Semantic.DeepEqual(run.Spec.Source, previous.Spec.Source) || run.Spec.WorkflowPath != previous.Spec.WorkflowPath {
		return nil, &terminalPlanningError{cause: fmt.Errorf("WorkflowRun %q does not match previous WorkflowRun %q", run.Name, previous.Name)}
	}

	previousAttempt := int32(1)
	if previous.Spec.Rerun == nil {
		if rerun.OriginalRunRef.Name != previous.Name || rerun.OriginalRunRef.UID != previous.UID {
			return nil, &terminalPlanningError{cause: fmt.Errorf("original WorkflowRun reference does not identify previous WorkflowRun %q", previous.Name)}
		}
	} else {
		previousAttempt = previous.Spec.Rerun.Attempt
		if rerun.OriginalRunRef != previous.Spec.Rerun.OriginalRunRef {
			return nil, &terminalPlanningError{cause: fmt.Errorf("original WorkflowRun reference does not match previous WorkflowRun %q", previous.Name)}
		}
	}
	if rerun.Attempt != previousAttempt+1 {
		return nil, &terminalPlanningError{cause: fmt.Errorf("rerun attempt %d must follow attempt %d", rerun.Attempt, previousAttempt)}
	}
	return previous, nil
}

func (r *WorkflowRunReconciler) reconcileWorkflowRun(ctx context.Context, run *actionsv1alpha1.WorkflowRun) (ctrl.Result, error) {
	if terminalRun(run) {
		waiting, err := r.releaseTerminalWorkflowJobConcurrency(ctx, run)
		if err != nil {
			return ctrl.Result{}, err
		}
		if waiting {
			return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
		}
		if concurrency := workflowRunConcurrencyDecision(run); concurrency != nil {
			scope, err := workflowRunConcurrencyScope(run)
			if err != nil {
				return ctrl.Result{}, err
			}
			if err := r.releaseConcurrency(ctx, run.Namespace, scope, concurrency.Group, workflowRunConcurrencyMember(run)); err != nil {
				return ctrl.Result{}, err
			}
		}
		return r.reconcileCompletedWorkflowRunTTL(ctx, run)
	}
	planned := meta.FindStatusCondition(run.Status.Conditions, actionsv1alpha1.WorkflowRunConditionPlanned)
	if planned != nil && planned.Status == metav1.ConditionTrue {
		if run.Status.Jobs == nil {
			return ctrl.Result{}, fmt.Errorf("planned WorkflowRun %q has no job summary", run.Name)
		}
		return r.observeWorkflowJobs(ctx, run, run.Status.WorkflowName, run.Status.Jobs.Total)
	}
	if workflowRunConcurrencyWaitCondition(planned) && run.Status.Jobs != nil {
		if concurrency := workflowRunConcurrencyDecision(run); concurrency != nil {
			if run.Status.Concurrency == nil {
				if err := r.persistWorkflowRunConcurrencyDecision(ctx, run, concurrency.Group, concurrency.CancelInProgress); err != nil {
					return ctrl.Result{}, err
				}
			}
			waiting, waitingForPlanning, err := r.handleConcurrency(ctx, run)
			if err != nil {
				return ctrl.Result{}, err
			}
			if waiting {
				return r.waitingForConcurrency(ctx, run, run.Status.WorkflowName, run.Status.Jobs.Total, waitingForPlanning)
			}
			return r.observeWorkflowJobs(ctx, run, run.Status.WorkflowName, run.Status.Jobs.Total)
		}
	}
	if policy := run.Spec.ForkPullRequest; policy != nil {
		if run.Spec.CancelRequested {
			return r.completeUnplannedWorkflowRun(ctx, run, "JobCancelled", "Workflow cancellation was requested before jobs were created")
		}
		if policy.RequireApproval && !policy.Approved {
			superseding, err := r.supersedingForkPullRequestRevision(ctx, run)
			if err != nil {
				return ctrl.Result{}, err
			}
			if superseding != nil {
				message := fmt.Sprintf("Pull request head %s superseded unapproved WorkflowRun revision %s", superseding.Spec.Source.GitHub.Event.PullRequest.HeadSHA, run.Spec.Source.GitHub.Event.PullRequest.HeadSHA)
				return r.completeUnplannedWorkflowRun(ctx, run, "RevisionSuperseded", message)
			}
			return r.waitingForApproval(ctx, run)
		}
		approved := meta.FindStatusCondition(run.Status.Conditions, actionsv1alpha1.WorkflowRunConditionApproved)
		if !policy.RequireApproval && (approved == nil || approved.Status != metav1.ConditionTrue || approved.ObservedGeneration != run.Generation) {
			return r.recordApproval(ctx, run, "ApprovalNotRequired", "The fork pull request policy does not require approval")
		}
	}

	project := &actionsv1alpha1.Project{}
	if err := r.APIReader.Get(ctx, client.ObjectKey{Namespace: run.Namespace, Name: run.Spec.ProjectRef.Name}, project); err != nil {
		disposition := planningFailureRetry
		if apierrors.IsNotFound(err) {
			disposition = planningFailureTerminal
		}
		return r.planningFailed(ctx, run, "ProjectUnavailable", err, disposition)
	}
	githubConfig := project.Spec.Source.GitHub
	githubSource := run.Spec.Source.GitHub
	eventPayload, err := r.githubEventSnapshot(ctx, run)
	if err != nil {
		disposition := planningFailureRetry
		terminal := &terminalPlanningError{}
		if errors.As(err, &terminal) {
			disposition = planningFailureTerminal
		}
		return r.planningFailed(ctx, run, "EventSnapshotUnavailable", err, disposition)
	}
	privateKey, err := secretValue(ctx, r.APIReader, project.Namespace, githubConfig.PrivateKeySecretRef)
	if err != nil {
		return r.planningFailed(ctx, run, "CredentialsUnavailable", err, planningFailureRetry)
	}
	installation, err := r.GitHub.CachedInstallation(ctx, githubConfig.AppID, githubConfig.InstallationID, privateKey, githubSource.Repository.Name, githubclient.InstallationPermissions{"contents": "read"})
	if err != nil {
		return r.planningFailed(ctx, run, "GitHubAuthenticationFailed", err, planningFailureRetry)
	}
	if policy := run.Spec.ForkPullRequest; policy != nil && policy.RequireApproval {
		pullRequest := githubSource.Event.PullRequest
		if pullRequest == nil {
			return r.planningFailed(ctx, run, "ApprovalValidationFailed", fmt.Errorf("WorkflowRun %q has no pull request metadata", run.Name), planningFailureTerminal)
		}
		currentHead, resolveErr := installation.ResolveRevision(ctx, githubSource.Repository.Owner, githubSource.Repository.Name, fmt.Sprintf("refs/pull/%d/head", pullRequest.Number))
		if resolveErr != nil {
			return r.planningFailed(ctx, run, "ApprovalValidationFailed", fmt.Errorf("validate approved pull request head for WorkflowRun %q: %w", run.Name, resolveErr), planningFailureRetry)
		}
		if currentHead != pullRequest.HeadSHA {
			message := fmt.Sprintf("Pull request head %s superseded approved WorkflowRun revision %s", currentHead, pullRequest.HeadSHA)
			return r.completeUnplannedWorkflowRun(ctx, run, "RevisionSuperseded", message)
		}
		approved := meta.FindStatusCondition(run.Status.Conditions, actionsv1alpha1.WorkflowRunConditionApproved)
		if approved == nil || approved.Status != metav1.ConditionTrue || approved.ObservedGeneration != run.Generation {
			return r.recordApproval(ctx, run, "ApprovalGranted", "An administrator approved this fork pull request revision")
		}
	}
	var workflowData []byte
	if githubSource.Revision.BaseSHA != "" {
		mergedRepository, mergeErr := r.GitRepository.Merge(ctx, githubSource.Repository.Owner, githubSource.Repository.Name, installation.Token(), gitrepository.Revision{
			BaseSHA: githubSource.Revision.BaseSHA, HeadSHA: githubSource.Revision.HeadSHA, MergeBaseSHA: githubSource.Revision.MergeBaseSHA,
		})
		if mergeErr != nil {
			disposition := planningFailureRetry
			conflict := &gitrepository.ConflictError{}
			if errors.As(mergeErr, &conflict) {
				disposition = planningFailureTerminal
			}
			return r.planningFailed(ctx, run, "IntegrationRevisionFailed", fmt.Errorf("construct integration revision for WorkflowRun %q: %w", run.Name, mergeErr), disposition)
		}
		defer mergedRepository.Close()
		if mergedRepository.SHA != githubSource.Revision.SHA {
			return r.planningFailed(ctx, run, "IntegrationRevisionFailed", fmt.Errorf("constructed integration revision for WorkflowRun %q is %s, want %s", run.Name, mergedRepository.SHA, githubSource.Revision.SHA), planningFailureTerminal)
		}
		workflowData, err = mergedRepository.ReadFile(ctx, run.Spec.WorkflowPath)
		if err != nil {
			disposition := planningFailureRetry
			if errors.Is(err, gitrepository.ErrPathNotFound) {
				disposition = planningFailureTerminal
			}
			return r.planningFailed(ctx, run, "WorkflowFetchFailed", fmt.Errorf("read workflow for WorkflowRun %q: %w", run.Name, err), disposition)
		}
	} else {
		workflowData, err = installation.GetFile(ctx, githubSource.Repository.Owner, githubSource.Repository.Name, run.Spec.WorkflowPath, githubSource.Revision.SHA)
		if err != nil {
			disposition := planningFailureRetry
			if githubAPIStatus(err, 404) {
				disposition = planningFailureTerminal
			}
			return r.planningFailed(ctx, run, "WorkflowFetchFailed", err, disposition)
		}
	}
	definition, err := workflow.Parse(workflowData)
	if err != nil {
		return r.planningFailed(ctx, run, "WorkflowInvalid", err, planningFailureTerminal)
	}
	if err := r.ensureWorkflowFileSnapshot(ctx, run, workflowData); err != nil {
		return r.planningFailed(ctx, run, "ChildCreationFailed", err, childCreationFailureDisposition(err))
	}
	planningRun, planningEvent, err := resolvePlanningEvent(run, definition, eventPayload)
	if err != nil {
		return r.planningFailed(ctx, run, "TriggerInvalid", err, planningFailureTerminal)
	}
	variables := r.projectVariableContext(ctx, project)
	if concurrency := workflowRunConcurrencyDecision(run); concurrency == nil {
		expressionContext := r.jobExpressionContext(planningRun, definition.Name, planningEvent.InputValues, variables, eventPayload)
		concurrencyGroup, cancelInProgress, err := workflow.EvaluateConcurrencyContext(definition, expressionContext)
		if err != nil {
			return r.planningEvaluationFailed(ctx, run, err)
		}
		if concurrencyGroup != "" {
			if err := r.persistWorkflowRunConcurrencyDecision(ctx, run, concurrencyGroup, cancelInProgress); err != nil {
				return ctrl.Result{}, err
			}
		}
	} else if run.Status.Concurrency == nil {
		if err := r.persistWorkflowRunConcurrencyDecision(ctx, run, concurrency.Group, concurrency.CancelInProgress); err != nil {
			return ctrl.Result{}, err
		}
	}
	plannedJobs, deferredJobs, err := r.planWorkflowJobs(planningRun, definition, planningEvent.InputValues, variables, eventPayload)
	if err != nil {
		return r.planningEvaluationFailed(ctx, run, err)
	}
	if len(deferredJobs) > 0 && run.Spec.Rerun != nil && len(run.Spec.Rerun.JobIDs) > 0 {
		return r.planningFailed(ctx, run, "RerunInvalid", fmt.Errorf("WorkflowRun %q does not support selective reruns with deferred job planning", run.Name), planningFailureTerminal)
	}
	plannedJobs, err = selectRerunWorkflowJobs(run, plannedJobs)
	if err != nil {
		return r.planningFailed(ctx, run, "RerunInvalid", err, planningFailureTerminal)
	}
	if run.Spec.Rerun != nil && len(run.Spec.Rerun.JobIDs) > 0 {
		if _, err := r.rerunDependencyWorkflowJobs(ctx, run, plannedWorkflowJobsForDependencyGraph(plannedJobs)); err != nil {
			disposition := planningFailureRetry
			terminal := &terminalPlanningError{}
			if errors.As(err, &terminal) {
				disposition = planningFailureTerminal
			}
			return r.planningFailed(ctx, run, "RerunInvalid", err, disposition)
		}
	}
	jobCount := int32(len(plannedJobs) + len(deferredJobs))
	run.Status.WorkflowName = definition.Name
	run.Status.Jobs = &actionsv1alpha1.WorkflowRunJobStatus{Total: jobCount}
	if len(deferredJobs) > 0 {
		if err := r.ensureWorkflowPlan(ctx, run, project, plannedJobs, deferredJobs); err != nil {
			return r.planningFailed(ctx, run, "ChildCreationFailed", err, childCreationFailureDisposition(err))
		}
	}
	if err := r.ensureWorkflowJobs(ctx, run, project, plannedJobs); err != nil {
		return r.planningFailed(ctx, run, "ChildCreationFailed", err, childCreationFailureDisposition(err))
	}
	waiting, waitingForPlanning, err := r.handleConcurrency(ctx, run)
	if err != nil {
		return r.planningFailed(ctx, run, "ConcurrencyCheckFailed", err, planningFailureRetry)
	}
	if waiting {
		return r.waitingForConcurrency(ctx, run, definition.Name, jobCount, waitingForPlanning)
	}
	return r.observeWorkflowJobs(ctx, run, definition.Name, jobCount)
}

func (r *WorkflowRunReconciler) persistWorkflowRunConcurrencyDecision(ctx context.Context, run *actionsv1alpha1.WorkflowRun, group string, cancelInProgress bool) error {
	before := run.Status.DeepCopy()
	run.Status.Concurrency = &actionsv1alpha1.ConcurrencyStatus{Group: group, CancelInProgress: cancelInProgress}
	run.Status.ConcurrencyGroup = group
	if apiEquality.Semantic.DeepEqual(before, &run.Status) {
		return nil
	}
	if err := r.Status().Update(ctx, run); err != nil {
		return fmt.Errorf("persist concurrency decision for WorkflowRun %q: %w", run.Name, err)
	}
	return nil
}

func (r *WorkflowRunReconciler) releaseTerminalWorkflowJobConcurrency(ctx context.Context, run *actionsv1alpha1.WorkflowRun) (bool, error) {
	jobs := &actionsv1alpha1.WorkflowJobList{}
	if err := r.APIReader.List(ctx, jobs, client.InNamespace(run.Namespace), client.MatchingLabels{actionsv1alpha1.LabelWorkflowRunUID: string(run.UID)}); err != nil {
		return false, fmt.Errorf("list WorkflowJobs for terminal WorkflowRun %q: %w", run.Name, err)
	}
	waiting := false
	var scope concurrencyScope
	scopeResolved := false
	for index := range jobs.Items {
		job := &jobs.Items[index]
		if job.Status.Concurrency == nil {
			continue
		}
		active, err := r.workflowJobExecutionActive(ctx, job)
		if err != nil {
			return false, fmt.Errorf("check concurrency workload for WorkflowJob %q: %w", job.Name, err)
		}
		if active {
			waiting = true
			continue
		}
		if !scopeResolved {
			scope, err = workflowRunConcurrencyScope(run)
			if err != nil {
				return false, err
			}
			scopeResolved = true
		}
		if err := r.releaseConcurrency(ctx, job.Namespace, scope, job.Status.Concurrency.Group, workflowJobConcurrencyMember(job)); err != nil {
			return false, fmt.Errorf("release concurrency for WorkflowJob %q: %w", job.Name, err)
		}
	}
	return waiting, nil
}

func (r *WorkflowRunReconciler) reconcileCompletedWorkflowRunTTL(ctx context.Context, run *actionsv1alpha1.WorkflowRun) (ctrl.Result, error) {
	remaining, configured := r.workflowRunTTLRemaining(run)
	if !configured {
		return ctrl.Result{}, nil
	}
	if remaining > 0 {
		return ctrl.Result{RequeueAfter: remaining}, nil
	}

	current := &actionsv1alpha1.WorkflowRun{}
	if err := r.APIReader.Get(ctx, client.ObjectKeyFromObject(run), current); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	remaining, configured = r.workflowRunTTLRemaining(current)
	if !configured {
		return ctrl.Result{}, nil
	}
	if remaining > 0 {
		return ctrl.Result{RequeueAfter: remaining}, nil
	}

	completedAt := current.Status.CompletionTime
	if completedAt == nil {
		condition := meta.FindStatusCondition(current.Status.Conditions, actionsv1alpha1.WorkflowRunConditionSucceeded)
		completedAt = &condition.LastTransitionTime
	}
	ctrl.LoggerFrom(ctx).Info("Deleting completed WorkflowRun after its TTL expired", "completion_time", completedAt.Time, "ttl_seconds_after_finished", *current.Spec.TTLSecondsAfterFinished)
	policy := metav1.DeletePropagationBackground
	resourceVersion := current.ResourceVersion
	return ctrl.Result{}, client.IgnoreNotFound(r.Delete(ctx, current, &client.DeleteOptions{
		Preconditions:     &metav1.Preconditions{ResourceVersion: &resourceVersion},
		PropagationPolicy: &policy,
	}))
}

func (r *WorkflowRunReconciler) workflowRunTTLRemaining(run *actionsv1alpha1.WorkflowRun) (time.Duration, bool) {
	if run.Spec.TTLSecondsAfterFinished == nil || !terminalRun(run) {
		return 0, false
	}
	retention := time.Duration(*run.Spec.TTLSecondsAfterFinished) * time.Second
	completedAt := run.Status.CompletionTime
	if completedAt == nil {
		condition := meta.FindStatusCondition(run.Status.Conditions, actionsv1alpha1.WorkflowRunConditionSucceeded)
		completedAt = &condition.LastTransitionTime
	}
	remaining := retention - r.now().Sub(completedAt.Time)
	if remaining > retention {
		// Poll at least once per second when completionTime is ahead of this controller's clock.
		remaining = max(retention, time.Second)
	}
	return remaining, true
}

func (r *WorkflowRunReconciler) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

func resolvePlanningEvent(run *actionsv1alpha1.WorkflowRun, definition *workflow.Definition, eventPayload map[string]any) (*actionsv1alpha1.WorkflowRun, workflow.Event, error) {
	githubSource := run.Spec.Source.GitHub
	event := workflowEvent(githubSource)
	event.Payload = eventPayload
	if githubSource.Event.Name != actionsv1alpha1.GitHubEventNameWorkflowDispatch && githubSource.Event.Name != actionsv1alpha1.GitHubEventNameWorkflowCall && githubSource.Event.Name != actionsv1alpha1.GitHubEventNameSchedule {
		return run, event, nil
	}
	matchedEvent, matched, err := workflow.Match(definition.On, event)
	if err != nil {
		return nil, workflow.Event{}, err
	}
	if !matched {
		return nil, workflow.Event{}, fmt.Errorf("workflow does not declare matching %s trigger", githubSource.Event.Name)
	}
	if githubSource.Event.Name == actionsv1alpha1.GitHubEventNameSchedule {
		return run, matchedEvent, nil
	}
	planningRun := run.DeepCopy()
	matchedEvent.Inputs = githubEventInputValues(matchedEvent.InputValues)
	planningRun.Spec.Source.GitHub.Event.Inputs = matchedEvent.Inputs
	return planningRun, matchedEvent, nil
}

type commitStatusReport struct {
	State       string
	Description string
}

func (r *WorkflowRunReconciler) shouldReportGitHubJobStatuses(ctx context.Context, run *actionsv1alpha1.WorkflowRun) (bool, error) {
	rootUID, attempt := workflowRunLineage(run)
	rootName := run.Name
	if run.Spec.Rerun != nil {
		rootName = run.Spec.Rerun.OriginalRunRef.Name
	}
	runs := &actionsv1alpha1.WorkflowRunList{}
	if err := r.APIReader.List(ctx, runs, client.InNamespace(run.Namespace), client.MatchingLabels{actionsv1alpha1.LabelWorkflowRunRootUID: string(rootUID)}); err != nil {
		return false, fmt.Errorf("list rerun attempts for WorkflowRun %q: %w", run.Name, err)
	}
	for index := range runs.Items {
		candidate := &runs.Items[index]
		candidateRoot, candidateAttempt := workflowRunLineage(candidate)
		if candidateRoot != rootUID {
			continue
		}
		if candidate.UID != rootUID && (candidate.Spec.Rerun == nil || candidate.Spec.Rerun.OriginalRunRef.Name != rootName) {
			continue
		}
		if candidateAttempt > attempt && candidate.Spec.ProjectRef == run.Spec.ProjectRef && candidate.Spec.WorkflowPath == run.Spec.WorkflowPath && apiEquality.Semantic.DeepEqual(candidate.Spec.Source, run.Spec.Source) {
			return false, nil
		}
	}
	return true, nil
}

type githubStatusOwner struct {
	UID        string `json:"uid"`
	RootUID    string `json:"rootUID"`
	Attempt    int32  `json:"attempt"`
	CreatedAt  int64  `json:"createdAt"`
	IdentityID int64  `json:"identityID,omitempty"`
	LeaseToken string `json:"leaseToken,omitempty"`
	LeaseUntil int64  `json:"leaseUntil,omitempty"`
}

func (r *WorkflowRunReconciler) reconcileGitHubStatus(ctx context.Context, run *actionsv1alpha1.WorkflowRun) error {
	githubSource := run.Spec.Source.GitHub
	if !r.githubStatusEnabled(run) {
		return nil
	}
	project := &actionsv1alpha1.Project{}
	projectKey := client.ObjectKey{Namespace: run.Namespace, Name: run.Spec.ProjectRef.Name}
	if err := r.APIReader.Get(ctx, projectKey, project); err != nil {
		return fmt.Errorf("get Project %q for GitHub status: %w", projectKey.Name, err)
	}
	statusKey := githubStatusKey(project.UID, run)
	projectMatches, err := r.ensureGitHubStatusIdentityLabels(ctx, run, project.UID, statusKey)
	if err != nil {
		return err
	}
	if !projectMatches {
		return nil
	}
	report, targetRun, err := r.aggregateGitHubCommitStatusReport(ctx, run, statusKey)
	if err != nil {
		return err
	}
	githubConfig := project.Spec.Source.GitHub
	revision := githubStatusRevision(githubSource)
	requestFor := func(report commitStatusReport, targetRun *actionsv1alpha1.WorkflowRun) githubclient.CreateCommitStatusRequest {
		targetURL := ""
		if r.ConsoleURL != "" {
			targetURL = workflowRunConsoleURL(r.ConsoleURL, targetRun)
		}
		return githubclient.CreateCommitStatusRequest{
			State:       report.State,
			TargetURL:   targetURL,
			Description: report.Description,
			Context:     githubStatusContext(run.Spec.WorkflowPath),
		}
	}
	request := requestFor(report, targetRun)
	reportDigest := commitStatusReportDigest(request)
	current := workflowRunCommitStatus(run)
	if current != nil && current.State == actionsv1alpha1.GitHubCommitStatusState(report.State) && current.ReportDigest == reportDigest {
		owned, err := r.githubStatusOwnershipMatchesRun(ctx, run, project, statusKey)
		if err != nil {
			return err
		}
		if owned {
			return nil
		}
	}
	currentOwner, leaseToken, err := r.githubStatusCurrentOwner(ctx, run, project, statusKey)
	if err != nil {
		return err
	}
	if !currentOwner {
		return nil
	}
	report, targetRun, err = r.aggregateGitHubCommitStatusReport(ctx, run, statusKey)
	if err != nil {
		return errors.Join(err, r.releaseGitHubStatusLease(ctx, run.Namespace, statusKey, leaseToken))
	}
	request = requestFor(report, targetRun)
	reportDigest = commitStatusReportDigest(request)
	reportError := func() error {
		privateKey, err := secretValue(ctx, r.APIReader, project.Namespace, githubConfig.PrivateKeySecretRef)
		if err != nil {
			return fmt.Errorf("read credentials for GitHub status: %w", err)
		}
		installation, err := r.GitHub.CachedInstallation(ctx, githubConfig.AppID, githubConfig.InstallationID, privateKey, githubSource.Repository.Name, githubclient.InstallationPermissions{"statuses": "write"})
		if err != nil {
			return fmt.Errorf("authenticate GitHub status reporter: %w", err)
		}
		statuses, err := installation.ListCommitStatuses(ctx, githubSource.Repository.Owner, githubSource.Repository.Name, revision)
		if err != nil {
			return err
		}
		for index := range statuses {
			status := &statuses[index]
			if !strings.EqualFold(status.Context, request.Context) {
				continue
			}
			if !commitStatusMatches(status, request) {
				break
			}
			appBotLogin, err := r.GitHub.AppBotLogin(ctx, githubConfig.AppID, privateKey)
			if err != nil {
				return fmt.Errorf("identify GitHub status reporter: %w", err)
			}
			if !strings.EqualFold(status.Creator.Login, appBotLogin) {
				break
			}
			if status.ID < 1 {
				return errors.New("GitHub returned an invalid commit-status ID")
			}
			return r.recordGitHubCommitStatus(ctx, run, report.State, reportDigest)
		}
		status, err := installation.CreateCommitStatus(ctx, githubSource.Repository.Owner, githubSource.Repository.Name, revision, request)
		if err != nil {
			return err
		}
		if status == nil || status.ID < 1 {
			return errors.New("GitHub returned an invalid commit-status ID")
		}
		return r.recordGitHubCommitStatus(ctx, run, report.State, reportDigest)
	}()
	return errors.Join(reportError, r.releaseGitHubStatusLease(ctx, run.Namespace, statusKey, leaseToken))
}

func (r *WorkflowRunReconciler) reconcileGitHubJobStatuses(ctx context.Context, run *actionsv1alpha1.WorkflowRun) error {
	if !r.githubStatusEnabled(run) {
		return nil
	}
	jobs := &actionsv1alpha1.WorkflowJobList{}
	if err := r.APIReader.List(ctx, jobs, client.InNamespace(run.Namespace), client.MatchingLabels{actionsv1alpha1.LabelWorkflowRunUID: string(run.UID)}); err != nil {
		return fmt.Errorf("list WorkflowJobs for GitHub reporting on WorkflowRun %q: %w", run.Name, err)
	}
	sort.Slice(jobs.Items, func(left, right int) bool {
		return jobs.Items[left].Name < jobs.Items[right].Name
	})
	type jobCommitStatusUpdate struct {
		job          *actionsv1alpha1.WorkflowJob
		request      githubclient.CreateCommitStatusRequest
		report       commitStatusReport
		reportDigest string
	}
	desired := make([]jobCommitStatusUpdate, 0, len(jobs.Items))
	updates := make([]jobCommitStatusUpdate, 0, len(jobs.Items))
	for index := range jobs.Items {
		job := &jobs.Items[index]
		report := workflowJobCommitStatusReport(run, job)
		request := workflowJobCommitStatusRequest(r.ConsoleURL, run, job, report)
		digest := commitStatusReportDigest(request)
		current := workflowJobCommitStatus(job)
		item := jobCommitStatusUpdate{job: job, request: request, report: report, reportDigest: digest}
		desired = append(desired, item)
		if current != nil && current.State == actionsv1alpha1.GitHubCommitStatusState(report.State) && current.ReportDigest == digest {
			continue
		}
		updates = append(updates, item)
	}
	if len(desired) == 0 {
		return nil
	}

	project := &actionsv1alpha1.Project{}
	projectKey := client.ObjectKey{Namespace: run.Namespace, Name: run.Spec.ProjectRef.Name}
	if err := r.APIReader.Get(ctx, projectKey, project); err != nil {
		return fmt.Errorf("get Project %q for GitHub job statuses: %w", projectKey.Name, err)
	}
	statusKey := githubStatusKey(project.UID, run)
	projectMatches, err := r.ensureGitHubStatusIdentityLabels(ctx, run, project.UID, statusKey)
	if err != nil {
		return err
	}
	if !projectMatches {
		return nil
	}
	owned, err := r.githubStatusOwnershipMatchesRun(ctx, run, project, statusKey)
	if err != nil {
		return fmt.Errorf("check GitHub status ownership for WorkflowRun %q: %w", run.Name, err)
	}
	if owned && len(updates) == 0 {
		return nil
	}
	currentAttempt, err := r.shouldReportGitHubJobStatuses(ctx, run)
	if err != nil {
		return err
	}
	if !currentAttempt {
		return nil
	}
	if !owned {
		updates = desired
	}
	currentOwner, leaseToken, err := r.githubStatusCurrentOwner(ctx, run, project, statusKey)
	if err != nil {
		return fmt.Errorf("claim GitHub status ownership for WorkflowRun %q: %w", run.Name, err)
	}
	if !currentOwner {
		return nil
	}

	githubConfig := project.Spec.Source.GitHub
	githubSource := run.Spec.Source.GitHub
	reportError := func() error {
		privateKey, err := secretValue(ctx, r.APIReader, project.Namespace, githubConfig.PrivateKeySecretRef)
		if err != nil {
			return fmt.Errorf("read credentials for GitHub job statuses: %w", err)
		}
		installation, err := r.GitHub.CachedInstallation(ctx, githubConfig.AppID, githubConfig.InstallationID, privateKey, githubSource.Repository.Name, githubclient.InstallationPermissions{"statuses": "write"})
		if err != nil {
			return fmt.Errorf("authenticate GitHub job status reporter: %w", err)
		}

		statuses, err := installation.ListCommitStatuses(ctx, githubSource.Repository.Owner, githubSource.Repository.Name, githubStatusRevision(githubSource))
		if err != nil {
			return err
		}
		recovered := make(map[string]*githubclient.CommitStatus, len(statuses))
		for index := range statuses {
			status := &statuses[index]
			key := strings.ToLower(status.Context)
			if _, found := recovered[key]; !found {
				recovered[key] = status
			}
		}
		appBotLogin := ""
		for _, item := range updates {
			status := recovered[strings.ToLower(item.request.Context)]
			if status != nil && commitStatusMatches(status, item.request) {
				if appBotLogin == "" {
					appBotLogin, err = r.GitHub.AppBotLogin(ctx, githubConfig.AppID, privateKey)
					if err != nil {
						return fmt.Errorf("identify GitHub job status reporter: %w", err)
					}
				}
				if strings.EqualFold(status.Creator.Login, appBotLogin) {
					if status.ID < 1 {
						return fmt.Errorf("GitHub returned an invalid commit-status ID for WorkflowJob %q", item.job.Name)
					}
					if err := r.recordGitHubJobCommitStatus(ctx, item.job, item.report.State, item.reportDigest); err != nil {
						return fmt.Errorf("record GitHub status for WorkflowJob %q: %w", item.job.Name, err)
					}
					continue
				}
			}
			status, err := installation.CreateCommitStatus(ctx, githubSource.Repository.Owner, githubSource.Repository.Name, githubStatusRevision(githubSource), item.request)
			if err != nil {
				return fmt.Errorf("report GitHub status for WorkflowJob %q: %w", item.job.Name, err)
			}
			if status == nil || status.ID < 1 {
				return fmt.Errorf("GitHub returned an invalid commit-status ID for WorkflowJob %q", item.job.Name)
			}
			if err := r.recordGitHubJobCommitStatus(ctx, item.job, item.report.State, item.reportDigest); err != nil {
				return fmt.Errorf("record GitHub status for WorkflowJob %q: %w", item.job.Name, err)
			}
		}
		return nil
	}()
	return errors.Join(reportError, r.releaseGitHubStatusLease(ctx, run.Namespace, statusKey, leaseToken))
}

func commitStatusMatches(status *githubclient.CommitStatus, request githubclient.CreateCommitStatusRequest) bool {
	return strings.EqualFold(status.Context, request.Context) && status.State == request.State && status.TargetURL == request.TargetURL && status.Description == request.Description
}

func (r *WorkflowRunReconciler) aggregateGitHubCommitStatusReport(ctx context.Context, run *actionsv1alpha1.WorkflowRun, statusKey string) (commitStatusReport, *actionsv1alpha1.WorkflowRun, error) {
	runs := &actionsv1alpha1.WorkflowRunList{}
	if err := r.APIReader.List(ctx, runs, client.InNamespace(run.Namespace), client.MatchingLabels{actionsv1alpha1.LabelGitHubStatusKey: statusKey}); err != nil {
		return commitStatusReport{}, nil, err
	}
	report, targetRun := aggregateGitHubCommitStatusReports(run, runs.Items)
	return report, targetRun, nil
}

func aggregateGitHubCommitStatusReports(run *actionsv1alpha1.WorkflowRun, runs []actionsv1alpha1.WorkflowRun) (commitStatusReport, *actionsv1alpha1.WorkflowRun) {
	latestByExecution := map[string]*actionsv1alpha1.WorkflowRun{}
	for index := range runs {
		candidate := &runs[index]
		if candidate.UID == run.UID {
			candidate = run
		}
		if !githubStatusKeyMatches(candidate, run) {
			continue
		}
		execution := githubStatusExecutionIdentity(candidate)
		if current := latestByExecution[execution]; current == nil || workflowRunIsNewer(candidate, current) {
			latestByExecution[execution] = candidate
		}
	}
	if len(latestByExecution) == 0 {
		return workflowRunCommitStatusReport(run), run
	}
	if len(latestByExecution) == 1 {
		for _, selected := range latestByExecution {
			return workflowRunCommitStatusReport(selected), selected
		}
	}

	state := "success"
	targetRun := run
	targetPriority := -1
	for _, selected := range latestByExecution {
		selectedState := workflowRunCommitStatusReport(selected).State
		priority := 0
		switch selectedState {
		case "error":
			state = "error"
			priority = 3
		case "failure":
			if state != "error" {
				state = "failure"
			}
			priority = 2
		case "pending":
			if state == "success" {
				state = "pending"
			}
			priority = 1
		}
		if priority > targetPriority || priority == targetPriority && workflowRunIsNewer(selected, targetRun) {
			targetRun = selected
			targetPriority = priority
		}
	}
	count := len(latestByExecution)
	description := fmt.Sprintf("All %d matching workflow runs succeeded", count)
	switch state {
	case "error":
		description = fmt.Sprintf("%d matching workflow runs include an error", count)
	case "failure":
		description = fmt.Sprintf("%d matching workflow runs include a failure", count)
	case "pending":
		description = fmt.Sprintf("%d matching workflow runs are pending", count)
	}
	return commitStatusReport{State: state, Description: description}, targetRun
}

func githubStatusExecutionIdentity(run *actionsv1alpha1.WorkflowRun) string {
	source := run.Spec.Source.GitHub
	if source.Event.Name == actionsv1alpha1.GitHubEventNamePullRequest && source.Event.PullRequest != nil {
		return fmt.Sprintf("%s\x00%d", source.Event.Name, source.Event.PullRequest.Number)
	}
	return fmt.Sprintf("%s\x00%s", source.Event.Name, source.Revision.Ref)
}

func (r *WorkflowRunReconciler) ensureGitHubStatusIdentityLabels(ctx context.Context, run *actionsv1alpha1.WorkflowRun, projectUID types.UID, statusKey string) (bool, error) {
	if labeledProjectUID := run.Labels[actionsv1alpha1.LabelProjectUID]; labeledProjectUID != "" && labeledProjectUID != string(projectUID) {
		return false, fmt.Errorf("%w for WorkflowRun %q", errGitHubProjectIdentityMismatch, run.Name)
	}
	if run.Labels[actionsv1alpha1.LabelProjectUID] == string(projectUID) && run.Labels[actionsv1alpha1.LabelGitHubStatusKey] == statusKey {
		return true, nil
	}
	before := run.DeepCopy()
	if run.Labels == nil {
		run.Labels = map[string]string{}
	}
	run.Labels[actionsv1alpha1.LabelProjectUID] = string(projectUID)
	run.Labels[actionsv1alpha1.LabelGitHubStatusKey] = statusKey
	return true, r.Patch(ctx, run, client.MergeFrom(before))
}

func (r *WorkflowRunReconciler) githubStatusCurrentOwner(ctx context.Context, run *actionsv1alpha1.WorkflowRun, project *actionsv1alpha1.Project, statusKey string) (bool, string, error) {
	runs := &actionsv1alpha1.WorkflowRunList{}
	if err := r.APIReader.List(ctx, runs, client.InNamespace(run.Namespace), client.MatchingLabels{actionsv1alpha1.LabelGitHubStatusKey: statusKey}); err != nil {
		return false, "", err
	}
	for index := range runs.Items {
		candidate := &runs.Items[index]
		if githubStatusKeyMatches(candidate, run) && workflowRunIsNewer(candidate, run) {
			return false, "", nil
		}
	}
	return r.claimGitHubStatusOwnership(ctx, run, project, statusKey)
}

func (r *WorkflowRunReconciler) claimGitHubStatusOwnership(ctx context.Context, run *actionsv1alpha1.WorkflowRun, project *actionsv1alpha1.Project, statusKey string) (bool, string, error) {
	desiredOwner := githubStatusOwnerForRun(run)
	leaseToken, err := newGitHubStatusLeaseToken()
	if err != nil {
		return false, "", err
	}
	desiredOwner.LeaseToken = leaseToken
	desiredOwner.LeaseUntil = r.now().Add(githubStatusLeaseDuration).UnixNano()
	data, err := json.Marshal(desiredOwner)
	if err != nil {
		return false, "", err
	}
	key := client.ObjectKey{Namespace: run.Namespace, Name: githubStatusOwnerPrefix + statusKey}
	current := false
	err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
		object := &corev1.ConfigMap{}
		if err := r.APIReader.Get(ctx, key, object); err != nil {
			if !apierrors.IsNotFound(err) {
				return err
			}
			object = &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: key.Name, Namespace: key.Namespace}, Data: map[string]string{githubStatusOwnerDataKey: string(data)}}
			if err := controllerutil.SetControllerReference(project, object, r.Scheme()); err != nil {
				return err
			}
			if err := r.Create(ctx, object); err != nil {
				if apierrors.IsAlreadyExists(err) {
					return apierrors.NewConflict(corev1.Resource("configmaps"), key.Name, err)
				}
				return err
			}
			current = true
			return nil
		}
		if !metav1.IsControlledBy(object, project) {
			return fmt.Errorf("GitHub status ownership ConfigMap %q is not owned by Project %q", object.Name, project.Name)
		}
		storedOwner := githubStatusOwner{}
		if err := json.Unmarshal([]byte(object.Data[githubStatusOwnerDataKey]), &storedOwner); err != nil || storedOwner.UID == "" || storedOwner.RootUID == "" || storedOwner.Attempt < 1 {
			return fmt.Errorf("GitHub status ownership ConfigMap %q is invalid", object.Name)
		}
		if storedOwner.UID != desiredOwner.UID && githubStatusOwnerIsNewer(storedOwner, desiredOwner) {
			live, err := r.githubStatusOwnerHasLiveRun(ctx, run, statusKey, storedOwner.UID)
			if err != nil {
				return err
			}
			if live {
				current = false
				return nil
			}
		}
		if storedOwner.LeaseToken != "" && storedOwner.LeaseUntil > r.now().UnixNano() {
			return apierrors.NewConflict(corev1.Resource("configmaps"), object.Name, errors.New("GitHub status reporting is already in progress"))
		}
		before := object.DeepCopy()
		object.Data[githubStatusOwnerDataKey] = string(data)
		if err := r.Patch(ctx, object, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
			return err
		}
		current = true
		return nil
	})
	if err != nil || !current {
		return current, "", err
	}
	return true, leaseToken, nil
}

func (r *WorkflowRunReconciler) githubStatusOwnerHasLiveRun(ctx context.Context, run *actionsv1alpha1.WorkflowRun, statusKey, ownerUID string) (bool, error) {
	runs := &actionsv1alpha1.WorkflowRunList{}
	if err := r.APIReader.List(ctx, runs, client.InNamespace(run.Namespace), client.MatchingLabels{actionsv1alpha1.LabelGitHubStatusKey: statusKey}); err != nil {
		return false, err
	}
	for index := range runs.Items {
		candidate := &runs.Items[index]
		if string(candidate.UID) == ownerUID && candidate.DeletionTimestamp.IsZero() && githubStatusKeyMatches(candidate, run) {
			return true, nil
		}
	}
	return false, nil
}

func (r *WorkflowRunReconciler) githubStatusOwnershipMatchesRun(ctx context.Context, run *actionsv1alpha1.WorkflowRun, project *actionsv1alpha1.Project, statusKey string) (bool, error) {
	object := &corev1.ConfigMap{}
	key := client.ObjectKey{Namespace: run.Namespace, Name: githubStatusOwnerPrefix + statusKey}
	if err := r.APIReader.Get(ctx, key, object); err != nil {
		return false, client.IgnoreNotFound(err)
	}
	if !metav1.IsControlledBy(object, project) {
		return false, fmt.Errorf("GitHub status ownership ConfigMap %q is not owned by Project %q", object.Name, project.Name)
	}
	owner := githubStatusOwner{}
	if err := json.Unmarshal([]byte(object.Data[githubStatusOwnerDataKey]), &owner); err != nil || owner.UID == "" || owner.RootUID == "" || owner.Attempt < 1 {
		return false, fmt.Errorf("GitHub status ownership ConfigMap %q is invalid", object.Name)
	}
	return owner.UID == string(run.UID), nil
}

func newGitHubStatusLeaseToken() (string, error) {
	value := [16]byte{}
	if _, err := rand.Read(value[:]); err != nil {
		return "", fmt.Errorf("create GitHub status lease token: %w", err)
	}
	return hex.EncodeToString(value[:]), nil
}

func (r *WorkflowRunReconciler) releaseGitHubStatusLease(ctx context.Context, namespace, statusKey, leaseToken string) error {
	key := client.ObjectKey{Namespace: namespace, Name: githubStatusOwnerPrefix + statusKey}
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		object := &corev1.ConfigMap{}
		if err := r.APIReader.Get(ctx, key, object); err != nil {
			return client.IgnoreNotFound(err)
		}
		owner := githubStatusOwner{}
		if err := json.Unmarshal([]byte(object.Data[githubStatusOwnerDataKey]), &owner); err != nil {
			return fmt.Errorf("decode GitHub status ownership ConfigMap %q: %w", object.Name, err)
		}
		if owner.LeaseToken != leaseToken {
			return nil
		}
		owner.LeaseToken = ""
		owner.LeaseUntil = 0
		data, err := json.Marshal(owner)
		if err != nil {
			return err
		}
		before := object.DeepCopy()
		object.Data[githubStatusOwnerDataKey] = string(data)
		return r.Patch(ctx, object, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{}))
	})
}

func (r *WorkflowRunReconciler) releaseGitHubStatusLeaseForRun(ctx context.Context, run *actionsv1alpha1.WorkflowRun, statusKey string) error {
	object := &corev1.ConfigMap{}
	key := client.ObjectKey{Namespace: run.Namespace, Name: githubStatusOwnerPrefix + statusKey}
	if err := r.APIReader.Get(ctx, key, object); err != nil {
		return client.IgnoreNotFound(err)
	}
	owner := githubStatusOwner{}
	if err := json.Unmarshal([]byte(object.Data[githubStatusOwnerDataKey]), &owner); err != nil {
		return fmt.Errorf("decode GitHub status ownership ConfigMap %q: %w", object.Name, err)
	}
	if owner.UID != string(run.UID) || owner.LeaseToken == "" {
		return nil
	}
	return r.releaseGitHubStatusLease(ctx, run.Namespace, statusKey, owner.LeaseToken)
}

func githubStatusOwnerForRun(run *actionsv1alpha1.WorkflowRun) githubStatusOwner {
	rootUID, attempt := workflowRunLineage(run)
	owner := githubStatusOwner{UID: string(run.UID), RootUID: string(rootUID), Attempt: attempt, CreatedAt: run.CreationTimestamp.UnixNano()}
	if run.Status.Identity != nil {
		owner.IdentityID = run.Status.Identity.ID
	}
	return owner
}

func githubStatusOwnerIsNewer(candidate, current githubStatusOwner) bool {
	if candidate.UID == current.UID {
		return false
	}
	if candidate.RootUID == current.RootUID && candidate.Attempt != current.Attempt {
		return candidate.Attempt > current.Attempt
	}
	if candidate.CreatedAt != current.CreatedAt {
		return candidate.CreatedAt > current.CreatedAt
	}
	if candidate.IdentityID != 0 && current.IdentityID != 0 && candidate.IdentityID != current.IdentityID {
		return candidate.IdentityID > current.IdentityID
	}
	return candidate.UID > current.UID
}

func (r *WorkflowRunReconciler) releaseGitHubStatusOwnershipIfUnused(ctx context.Context, run *actionsv1alpha1.WorkflowRun) error {
	statusKey := run.Labels[actionsv1alpha1.LabelGitHubStatusKey]
	if statusKey == "" {
		return nil
	}
	matchingRunRemains := func() (bool, error) {
		runs := &actionsv1alpha1.WorkflowRunList{}
		if err := r.APIReader.List(ctx, runs, client.InNamespace(run.Namespace), client.MatchingLabels{actionsv1alpha1.LabelGitHubStatusKey: statusKey}); err != nil {
			return false, err
		}
		for index := range runs.Items {
			candidate := &runs.Items[index]
			if candidate.UID != run.UID && candidate.DeletionTimestamp.IsZero() && githubStatusKeyMatches(candidate, run) {
				return true, nil
			}
		}
		return false, nil
	}
	remaining, err := matchingRunRemains()
	if err != nil || remaining {
		return err
	}
	object := &corev1.ConfigMap{}
	key := client.ObjectKey{Namespace: run.Namespace, Name: githubStatusOwnerPrefix + statusKey}
	if err := r.APIReader.Get(ctx, key, object); err != nil {
		return client.IgnoreNotFound(err)
	}
	owner := githubStatusOwner{}
	if err := json.Unmarshal([]byte(object.Data[githubStatusOwnerDataKey]), &owner); err != nil {
		return fmt.Errorf("decode GitHub status ownership ConfigMap %q: %w", object.Name, err)
	}
	if owner.UID == "" {
		return fmt.Errorf("GitHub status ownership ConfigMap %q is invalid", object.Name)
	}
	if owner.UID != string(run.UID) && owner.LeaseToken != "" && owner.LeaseUntil > r.now().UnixNano() {
		return nil
	}
	remaining, err = matchingRunRemains()
	if err != nil || remaining {
		return err
	}
	resourceVersion := object.ResourceVersion
	return client.IgnoreNotFound(r.Delete(ctx, object, &client.DeleteOptions{
		Preconditions: &metav1.Preconditions{ResourceVersion: &resourceVersion},
	}))
}

func githubStatusKeyMatches(left, right *actionsv1alpha1.WorkflowRun) bool {
	leftSource := left.Spec.Source.GitHub
	rightSource := right.Spec.Source.GitHub
	return leftSource != nil && rightSource != nil &&
		left.Namespace == right.Namespace && left.Spec.ProjectRef == right.Spec.ProjectRef &&
		left.Labels[actionsv1alpha1.LabelProjectUID] != "" && left.Labels[actionsv1alpha1.LabelProjectUID] == right.Labels[actionsv1alpha1.LabelProjectUID] &&
		leftSource.Repository.ID == rightSource.Repository.ID &&
		githubStatusRevision(leftSource) == githubStatusRevision(rightSource) &&
		strings.EqualFold(githubStatusContext(left.Spec.WorkflowPath), githubStatusContext(right.Spec.WorkflowPath))
}

func workflowRunIsNewer(candidate, current *actionsv1alpha1.WorkflowRun) bool {
	if candidate.UID == current.UID {
		return false
	}
	candidateRoot, candidateAttempt := workflowRunLineage(candidate)
	currentRoot, currentAttempt := workflowRunLineage(current)
	if candidateRoot != "" && candidateRoot == currentRoot {
		return candidateAttempt > currentAttempt
	}
	if !candidate.CreationTimestamp.Equal(&current.CreationTimestamp) {
		return candidate.CreationTimestamp.After(current.CreationTimestamp.Time)
	}
	if candidate.Namespace == current.Namespace && candidate.Spec.ProjectRef == current.Spec.ProjectRef &&
		candidate.Status.Identity != nil && current.Status.Identity != nil && candidate.Status.Identity.ID != current.Status.Identity.ID {
		return candidate.Status.Identity.ID > current.Status.Identity.ID
	}
	return string(candidate.UID) > string(current.UID)
}

func workflowRunLineage(run *actionsv1alpha1.WorkflowRun) (types.UID, int32) {
	if run.Spec.Rerun != nil {
		return run.Spec.Rerun.OriginalRunRef.UID, run.Spec.Rerun.Attempt
	}
	return run.UID, 1
}

func githubStatusRevision(source *actionsv1alpha1.GitHubWorkflowRunSource) string {
	if source.Event.Name == actionsv1alpha1.GitHubEventNamePullRequest && source.Revision.HeadSHA != "" {
		return source.Revision.HeadSHA
	}
	return source.Revision.SHA
}

func githubStatusContext(workflowPath string) string {
	context := "Open Actions / " + workflowPath
	digest := sha256.Sum256([]byte(context))
	suffix := fmt.Sprintf(" / %x", digest[:8])
	runes := []rune(context)
	if workflowPath == strings.ToLower(workflowPath) && len(runes) <= maxCommitStatusContextRunes {
		return context
	}
	if len(runes)+len(suffix) <= maxCommitStatusContextRunes {
		return context + suffix
	}
	return string(runes[:maxCommitStatusContextRunes-len(suffix)]) + suffix
}

func githubJobStatusContext(run *actionsv1alpha1.WorkflowRun, job *actionsv1alpha1.WorkflowJob) string {
	displayName := job.Spec.DisplayName
	if displayName == "" {
		displayName = job.Spec.JobID
	}
	jobID := job.Spec.JobID
	if job.Spec.Matrix != nil {
		jobID = job.Spec.Matrix.LogicalJobID
	}
	context := "Open Actions / " + run.Spec.WorkflowPath + " / " + displayName
	if displayName != jobID {
		context += " / " + jobID
	}
	digest := sha256.Sum256([]byte(context))
	suffix := fmt.Sprintf(" / %x", digest[:8])
	runes := []rune(context)
	caseIdentity := run.Spec.WorkflowPath + "\x00" + jobID
	if caseIdentity == strings.ToLower(caseIdentity) && len(runes) <= maxCommitStatusContextRunes {
		return context
	}
	if len(runes)+len(suffix) <= maxCommitStatusContextRunes {
		return context + suffix
	}
	return string(runes[:maxCommitStatusContextRunes-len(suffix)]) + suffix
}

func githubStatusKey(projectUID types.UID, run *actionsv1alpha1.WorkflowRun) string {
	source := run.Spec.Source.GitHub
	if source == nil {
		return ""
	}
	switch source.Event.Name {
	case actionsv1alpha1.GitHubEventNamePush, actionsv1alpha1.GitHubEventNamePullRequest, actionsv1alpha1.GitHubEventNameMergeGroup:
	default:
		return ""
	}
	key := fmt.Sprintf("%s\x00%d\x00%s\x00%s", projectUID, source.Repository.ID, githubStatusRevision(source), strings.ToLower(githubStatusContext(run.Spec.WorkflowPath)))
	digest := sha256.Sum256([]byte(key))
	return strings.ToLower(digestEncoding.EncodeToString(digest[:]))
}

func commitStatusReportDigest(request githubclient.CreateCommitStatusRequest) string {
	data, _ := json.Marshal(request)
	digest := sha256.Sum256(data)
	return fmt.Sprintf("%x", digest)
}

func workflowRunCommitStatusReport(run *actionsv1alpha1.WorkflowRun) commitStatusReport {
	report := commitStatusReport{State: "pending", Description: "The workflow is queued"}
	if policy := run.Spec.ForkPullRequest; policy != nil && policy.RequireApproval && !policy.Approved {
		report.Description = "The workflow is waiting for approval"
	}
	succeeded := meta.FindStatusCondition(run.Status.Conditions, actionsv1alpha1.WorkflowRunConditionSucceeded)
	switch {
	case succeeded != nil && succeeded.Status == metav1.ConditionTrue:
		report.State = "success"
		report.Description = commitStatusDescription(succeeded.Message, "The workflow succeeded")
	case succeeded != nil && succeeded.Status == metav1.ConditionFalse:
		report.State = "failure"
		switch succeeded.Reason {
		case "JobCancelled", "JobTimedOut", "RevisionSuperseded":
			report.State = "error"
		}
		report.Description = commitStatusDescription(succeeded.Message, "The workflow failed")
	case !run.DeletionTimestamp.IsZero():
		report.State = "error"
		report.Description = "The workflow was cancelled"
	case run.Status.StartTime != nil:
		report.Description = "The workflow is running"
	default:
		planned := meta.FindStatusCondition(run.Status.Conditions, actionsv1alpha1.WorkflowRunConditionPlanned)
		if planned != nil && planned.Message != "" {
			report.Description = commitStatusDescription(planned.Message, report.Description)
		}
	}
	if run.Spec.Rerun != nil {
		report.Description = commitStatusDescription(fmt.Sprintf("Attempt %d: %s", run.Spec.Rerun.Attempt, report.Description), report.Description)
	}
	return report
}

func workflowJobCommitStatusReport(run *actionsv1alpha1.WorkflowRun, job *actionsv1alpha1.WorkflowJob) commitStatusReport {
	report := commitStatusReport{State: "pending", Description: "The workflow job is queued"}
	succeeded := meta.FindStatusCondition(job.Status.Conditions, actionsv1alpha1.WorkflowJobConditionSucceeded)
	switch workflowJobResult(job) {
	case actionsv1alpha1.WorkflowJobResultSuccess:
		report.State = "success"
		report.Description = commitStatusDescription(conditionMessage(succeeded), "The workflow job succeeded")
	case actionsv1alpha1.WorkflowJobResultFailure:
		report.State = "failure"
		report.Description = "The workflow job failed"
		if workflowJobTimedOut(job) {
			report.State = "error"
			report.Description = "The workflow job timed out"
		} else if succeeded != nil && succeeded.Reason == "JobCancelled" {
			report.State = "error"
			report.Description = "The workflow job was cancelled"
		}
		report.Description = commitStatusDescription(conditionMessage(succeeded), report.Description)
	case actionsv1alpha1.WorkflowJobResultSkipped:
		report.State = "success"
		report.Description = "The workflow job was skipped"
	case actionsv1alpha1.WorkflowJobResultCancelled:
		report.State = "error"
		report.Description = commitStatusDescription(conditionMessage(succeeded), "The workflow job was cancelled")
	case "":
		switch {
		case !run.DeletionTimestamp.IsZero():
			report.State = "error"
			report.Description = "The workflow job was cancelled"
		case job.Status.StartTime != nil:
			report.Description = "The workflow job is running"
		}
	}
	if run.Spec.Rerun != nil {
		report.Description = commitStatusDescription(fmt.Sprintf("Attempt %d: %s", run.Spec.Rerun.Attempt, report.Description), report.Description)
	}
	return report
}

func conditionMessage(condition *metav1.Condition) string {
	if condition == nil {
		return ""
	}
	return condition.Message
}

func workflowJobCommitStatusRequest(consoleURL string, run *actionsv1alpha1.WorkflowRun, job *actionsv1alpha1.WorkflowJob, report commitStatusReport) githubclient.CreateCommitStatusRequest {
	targetURL := ""
	if consoleURL != "" {
		targetURL = workflowJobConsoleURL(consoleURL, run, job)
	}
	return githubclient.CreateCommitStatusRequest{
		State:       report.State,
		TargetURL:   targetURL,
		Description: report.Description,
		Context:     githubJobStatusContext(run, job),
	}
}

func commitStatusDescription(description, fallback string) string {
	if description == "" {
		description = fallback
	}
	runes := []rune(description)
	if len(runes) > maxCommitStatusDescriptionRunes {
		description = string(runes[:maxCommitStatusDescriptionRunes])
	}
	return description
}

func workflowRunCommitStatus(run *actionsv1alpha1.WorkflowRun) *actionsv1alpha1.GitHubCommitStatus {
	if run.Status.Source == nil || run.Status.Source.GitHub == nil {
		return nil
	}
	return run.Status.Source.GitHub.CommitStatus
}

func workflowJobCommitStatus(job *actionsv1alpha1.WorkflowJob) *actionsv1alpha1.GitHubCommitStatus {
	if job.Status.Source == nil || job.Status.Source.GitHub == nil {
		return nil
	}
	return job.Status.Source.GitHub.CommitStatus
}

func (r *WorkflowRunReconciler) recordGitHubCommitStatus(ctx context.Context, run *actionsv1alpha1.WorkflowRun, state, reportDigest string) error {
	before := run.DeepCopy()
	if run.Status.Source == nil {
		run.Status.Source = &actionsv1alpha1.WorkflowRunSourceStatus{}
	}
	if run.Status.Source.GitHub == nil {
		run.Status.Source.GitHub = &actionsv1alpha1.GitHubWorkflowRunStatus{}
	}
	run.Status.Source.GitHub.CommitStatus = &actionsv1alpha1.GitHubCommitStatus{State: actionsv1alpha1.GitHubCommitStatusState(state), ReportDigest: reportDigest}
	return r.Status().Patch(ctx, run, client.MergeFrom(before))
}

func (r *WorkflowRunReconciler) recordGitHubJobCommitStatus(ctx context.Context, job *actionsv1alpha1.WorkflowJob, state, reportDigest string) error {
	before := job.DeepCopy()
	if job.Status.Source == nil {
		job.Status.Source = &actionsv1alpha1.WorkflowJobSourceStatus{}
	}
	if job.Status.Source.GitHub == nil {
		job.Status.Source.GitHub = &actionsv1alpha1.GitHubWorkflowJobStatus{}
	}
	job.Status.Source.GitHub.CommitStatus = &actionsv1alpha1.GitHubCommitStatus{State: actionsv1alpha1.GitHubCommitStatusState(state), ReportDigest: reportDigest}
	return r.Status().Patch(ctx, job, client.MergeFrom(before))
}

func workflowRunConsoleURL(baseURL string, run *actionsv1alpha1.WorkflowRun) string {
	parsed, err := url.Parse(baseURL)
	if err != nil {
		return ""
	}
	parsed.Path = strings.TrimRight(parsed.Path, "/") + "/runs/" + url.PathEscape(run.Namespace) + "/" + url.PathEscape(run.Name)
	return parsed.String()
}

func workflowJobConsoleURL(baseURL string, run *actionsv1alpha1.WorkflowRun, job *actionsv1alpha1.WorkflowJob) string {
	runURL := workflowRunConsoleURL(baseURL, run)
	if runURL == "" {
		return ""
	}
	parsed, err := url.Parse(runURL)
	if err != nil {
		return ""
	}
	parsed.Path = strings.TrimRight(parsed.Path, "/") + "/jobs/" + url.PathEscape(job.Name)
	return parsed.String()
}

func workflowRunQueryURL(baseURL string, run *actionsv1alpha1.WorkflowRun) string {
	if baseURL == "" {
		return ""
	}
	parsed, err := url.Parse(baseURL)
	if err != nil {
		return ""
	}
	parsed.Path = strings.TrimRight(parsed.Path, "/") + "/api/v1/runs/" + url.PathEscape(run.Namespace) + "/" + url.PathEscape(run.Name) + "/newer"
	return parsed.String()
}

type plannedWorkflowJob struct {
	id             string
	displayName    string
	runsOn         []string
	needs          []string
	condition      string
	concurrency    *actionsv1alpha1.WorkflowJobConcurrency
	matrix         *actionsv1alpha1.WorkflowJobMatrix
	plan           string
	resultVersion  string
	timeoutSeconds int64
}

type deferredJobPlan struct {
	JobID        string            `json:"jobID"`
	WorkflowName string            `json:"workflowName"`
	WorkflowEnv  map[string]string `json:"workflowEnv,omitempty"`
	EventPayload map[string]any    `json:"eventPayload,omitempty"`
	Variables    map[string]any    `json:"variables,omitempty"`
	Job          workflow.Job      `json:"job"`
	InputValues  map[string]any    `json:"inputValues,omitempty"`
}

type workflowPlanManifest struct {
	JobIDs    []string `json:"jobIDs"`
	SourceIDs []string `json:"sourceIDs"`
	// DeferredJobs keeps its JSON name for stored WorkflowRun plan compatibility.
	DeferredJobs map[string]string `json:"matrices"`
}

func selectRerunWorkflowJobs(run *actionsv1alpha1.WorkflowRun, plannedJobs []plannedWorkflowJob) ([]plannedWorkflowJob, error) {
	if run.Spec.Rerun == nil || len(run.Spec.Rerun.JobIDs) == 0 {
		return plannedJobs, nil
	}
	selectedIDs := make(map[string]struct{}, len(run.Spec.Rerun.JobIDs))
	for _, id := range run.Spec.Rerun.JobIDs {
		selectedIDs[id] = struct{}{}
	}
	plannedByID := make(map[string]*plannedWorkflowJob, len(plannedJobs))
	for index := range plannedJobs {
		job := &plannedJobs[index]
		plannedByID[job.id] = job
	}
	missing := make([]string, 0)
	for id := range selectedIDs {
		if plannedByID[id] == nil {
			missing = append(missing, id)
		}
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		return nil, fmt.Errorf("selected jobs are not present in the workflow: %s", strings.Join(missing, ", "))
	}

	selected := make([]plannedWorkflowJob, 0, len(selectedIDs))
	for _, job := range plannedJobs {
		if _, found := selectedIDs[job.id]; found {
			selected = append(selected, job)
		}
	}
	return selected, nil
}

func plannedWorkflowJobsForDependencyGraph(plannedJobs []plannedWorkflowJob) []actionsv1alpha1.WorkflowJob {
	jobs := make([]actionsv1alpha1.WorkflowJob, 0, len(plannedJobs))
	for _, planned := range plannedJobs {
		jobs = append(jobs, actionsv1alpha1.WorkflowJob{Spec: actionsv1alpha1.WorkflowJobSpec{
			JobID:  planned.id,
			Needs:  append([]string(nil), planned.needs...),
			Matrix: planned.matrix.DeepCopy(),
		}})
	}
	return jobs
}

func (r *WorkflowRunReconciler) rerunDependencyWorkflowJobs(ctx context.Context, run *actionsv1alpha1.WorkflowRun, currentJobs []actionsv1alpha1.WorkflowJob) ([]actionsv1alpha1.WorkflowJob, error) {
	if run.Spec.Rerun == nil || len(run.Spec.Rerun.JobIDs) == 0 {
		return nil, nil
	}

	historicalByID := make(map[string]actionsv1alpha1.WorkflowJob)
	dependencies, dependencyErr := resolvedRerunDependencyWorkflowJobs(currentJobs, historicalByID)
	if dependencyErr == nil {
		return dependencies, nil
	}

	ref := run.Spec.Rerun.PreviousRunRef
	visited := make(map[types.UID]struct{})
	for {
		previous := &actionsv1alpha1.WorkflowRun{}
		key := client.ObjectKey{Namespace: run.Namespace, Name: ref.Name}
		if err := r.APIReader.Get(ctx, key, previous); err != nil {
			if apierrors.IsNotFound(err) {
				return nil, &terminalPlanningError{cause: fmt.Errorf("WorkflowRun %q cannot resolve rerun dependencies because WorkflowRun %q is unavailable: %w", run.Name, ref.Name, dependencyErr)}
			}
			return nil, fmt.Errorf("get dependency WorkflowRun %q for WorkflowRun %q: %w", ref.Name, run.Name, err)
		}
		if previous.UID != ref.UID {
			return nil, &terminalPlanningError{cause: fmt.Errorf("WorkflowRun %q cannot resolve rerun dependencies because WorkflowRun %q has a different UID", run.Name, previous.Name)}
		}
		if _, found := visited[previous.UID]; found {
			return nil, &terminalPlanningError{cause: fmt.Errorf("WorkflowRun %q rerun lineage contains a cycle at WorkflowRun %q", run.Name, previous.Name)}
		}
		visited[previous.UID] = struct{}{}

		jobs := &actionsv1alpha1.WorkflowJobList{}
		if err := r.APIReader.List(ctx, jobs, client.InNamespace(run.Namespace), client.MatchingLabels{actionsv1alpha1.LabelWorkflowRunUID: string(previous.UID)}); err != nil {
			return nil, fmt.Errorf("list dependency WorkflowJobs for WorkflowRun %q: %w", previous.Name, err)
		}
		for index := range jobs.Items {
			job := jobs.Items[index]
			if _, found := historicalByID[job.Spec.JobID]; !found {
				historicalByID[job.Spec.JobID] = job
			}
		}
		dependencies, dependencyErr = resolvedRerunDependencyWorkflowJobs(currentJobs, historicalByID)
		if dependencyErr == nil {
			return dependencies, nil
		}
		if previous.Spec.Rerun == nil {
			break
		}
		if previous.Spec.Rerun.OriginalRunRef != run.Spec.Rerun.OriginalRunRef {
			return nil, &terminalPlanningError{cause: fmt.Errorf("WorkflowRun %q dependency lineage does not match WorkflowRun %q", previous.Name, run.Name)}
		}
		ref = previous.Spec.Rerun.PreviousRunRef
	}

	return nil, &terminalPlanningError{cause: fmt.Errorf("WorkflowRun %q cannot resolve rerun dependencies: %w", run.Name, dependencyErr)}
}

func resolvedRerunDependencyWorkflowJobs(currentJobs []actionsv1alpha1.WorkflowJob, historicalByID map[string]actionsv1alpha1.WorkflowJob) ([]actionsv1alpha1.WorkflowJob, error) {
	effectiveByID := make(map[string]*actionsv1alpha1.WorkflowJob, len(historicalByID)+len(currentJobs))
	for id, historical := range historicalByID {
		effectiveByID[id] = historical.DeepCopy()
	}
	currentIDs := make(map[string]struct{}, len(currentJobs))
	for index := range currentJobs {
		job := &currentJobs[index]
		if _, found := currentIDs[job.Spec.JobID]; found {
			return nil, fmt.Errorf("rerun contains multiple jobs with ID %q", job.Spec.JobID)
		}
		currentIDs[job.Spec.JobID] = struct{}{}
		effectiveByID[job.Spec.JobID] = job
	}

	jobsByLogicalID := make(map[string][]*actionsv1alpha1.WorkflowJob)
	for _, job := range effectiveByID {
		logicalID := job.Spec.JobID
		if job.Spec.Matrix != nil {
			logicalID = job.Spec.Matrix.LogicalJobID
		}
		jobsByLogicalID[logicalID] = append(jobsByLogicalID[logicalID], job)
	}
	for logicalID := range jobsByLogicalID {
		sort.Slice(jobsByLogicalID[logicalID], func(left, right int) bool {
			return jobsByLogicalID[logicalID][left].Spec.JobID < jobsByLogicalID[logicalID][right].Spec.JobID
		})
	}

	requiredHistorical := make(map[string]struct{})
	visited := make(map[string]struct{})
	visiting := make(map[string]struct{})
	var visit func(*actionsv1alpha1.WorkflowJob) error
	visit = func(job *actionsv1alpha1.WorkflowJob) error {
		if _, found := visited[job.Spec.JobID]; found {
			return nil
		}
		if _, found := visiting[job.Spec.JobID]; found {
			return fmt.Errorf("job %q has a cyclic dependency", job.Spec.JobID)
		}
		visiting[job.Spec.JobID] = struct{}{}
		defer delete(visiting, job.Spec.JobID)

		for _, dependencyID := range job.Spec.Needs {
			dependencyJobs := jobsByLogicalID[dependencyID]
			if err := validateRerunDependencyGroup(dependencyID, dependencyJobs); err != nil {
				return fmt.Errorf("job %q: %w", job.Spec.JobID, err)
			}
			for _, dependencyJob := range dependencyJobs {
				if _, current := currentIDs[dependencyJob.Spec.JobID]; !current {
					if !workflowJobTerminal(dependencyJob) {
						return fmt.Errorf("dependency job %q has no terminal result", dependencyJob.Spec.JobID)
					}
					requiredHistorical[dependencyJob.Spec.JobID] = struct{}{}
				}
				if err := visit(dependencyJob); err != nil {
					return err
				}
			}
		}
		visited[job.Spec.JobID] = struct{}{}
		return nil
	}
	for index := range currentJobs {
		if workflowJobTerminal(&currentJobs[index]) {
			continue
		}
		if err := visit(&currentJobs[index]); err != nil {
			return nil, err
		}
	}

	dependencies := make([]actionsv1alpha1.WorkflowJob, 0, len(requiredHistorical))
	for id := range requiredHistorical {
		dependencies = append(dependencies, historicalByID[id])
	}
	sort.Slice(dependencies, func(left, right int) bool {
		return dependencies[left].Spec.JobID < dependencies[right].Spec.JobID
	})
	return dependencies, nil
}

func validateRerunDependencyGroup(logicalID string, jobs []*actionsv1alpha1.WorkflowJob) error {
	if len(jobs) == 0 {
		return fmt.Errorf("needs missing job %q", logicalID)
	}
	first := jobs[0]
	if first.Spec.Matrix == nil {
		if len(jobs) != 1 {
			return fmt.Errorf("dependency %q has %d non-matrix jobs", logicalID, len(jobs))
		}
		return nil
	}
	expected := first.Spec.Matrix.JobTotal
	if expected < 1 {
		return fmt.Errorf("matrix dependency %q has no job total", logicalID)
	}
	for _, job := range jobs {
		if job.Spec.Matrix == nil || job.Spec.Matrix.LogicalJobID != logicalID || job.Spec.Matrix.JobTotal != expected {
			return fmt.Errorf("matrix dependency %q has inconsistent expansion metadata", logicalID)
		}
	}
	if len(jobs) != int(expected) {
		return fmt.Errorf("matrix dependency %q requires %d expanded jobs, found %d", logicalID, expected, len(jobs))
	}
	return nil
}

func (r *WorkflowRunReconciler) ensureWorkflowJobs(ctx context.Context, run *actionsv1alpha1.WorkflowRun, project *actionsv1alpha1.Project, plannedJobs []plannedWorkflowJob) error {
	existingJobs := &actionsv1alpha1.WorkflowJobList{}
	if err := r.APIReader.List(ctx, existingJobs, client.InNamespace(run.Namespace), client.MatchingLabels{actionsv1alpha1.LabelWorkflowRunUID: string(run.UID)}); err != nil {
		return err
	}
	existingByJobID := make(map[string]*actionsv1alpha1.WorkflowJob, len(existingJobs.Items))
	for index := range existingJobs.Items {
		existing := &existingJobs.Items[index]
		if other := existingByJobID[existing.Spec.JobID]; other != nil {
			return &terminalPlanningError{cause: fmt.Errorf("WorkflowJobs %q and %q both represent job %q in WorkflowRun %q", other.Name, existing.Name, existing.Spec.JobID, run.Name)}
		}
		existingByJobID[existing.Spec.JobID] = existing
	}

	for _, item := range plannedJobs {
		id := item.id
		labels := workflowJobLabels(run, project, id)
		annotations := map[string]string{actionsv1alpha1.AnnotationProjectName: project.Name}
		if item.resultVersion != "" {
			annotations[actionsv1alpha1.AnnotationRunnerResultVersion] = item.resultVersion
		}
		workflowJob := &actionsv1alpha1.WorkflowJob{
			ObjectMeta: metav1.ObjectMeta{
				Name:        workflowJobName(run.Name, id),
				Namespace:   run.Namespace,
				Labels:      labels,
				Annotations: annotations,
			},
			Spec: actionsv1alpha1.WorkflowJobSpec{
				WorkflowRunRef: corev1.LocalObjectReference{Name: run.Name},
				JobID:          id,
				DisplayName:    item.displayName,
				RunsOn:         append([]string(nil), item.runsOn...),
				Needs:          append([]string(nil), item.needs...),
				If:             item.condition,
				Concurrency:    item.concurrency.DeepCopy(),
				Matrix:         item.matrix.DeepCopy(),
				TimeoutSeconds: item.timeoutSeconds,
			},
		}
		if err := controllerutil.SetControllerReference(run, workflowJob, r.Scheme()); err != nil {
			return &terminalPlanningError{cause: err}
		}
		if existing := existingByJobID[id]; existing != nil {
			if !workflowJobIdentityMatches(existing, workflowJob, run) {
				return &terminalPlanningError{cause: fmt.Errorf("WorkflowJob %q does not match job %q in WorkflowRun %q", existing.Name, id, run.Name)}
			}
			workflowJob = existing
		} else if err := r.Create(ctx, workflowJob); err != nil {
			if !apierrors.IsAlreadyExists(err) {
				return err
			}
			existing := &actionsv1alpha1.WorkflowJob{}
			if err := r.APIReader.Get(ctx, client.ObjectKeyFromObject(workflowJob), existing); err != nil {
				return err
			}
			if !workflowJobIdentityMatches(existing, workflowJob, run) {
				return &terminalPlanningError{cause: fmt.Errorf("WorkflowJob %q does not match job %q in WorkflowRun %q", existing.Name, id, run.Name)}
			}
			workflowJob = existing
		}

		if err := r.ensurePlanConfigMap(ctx, workflowJob, item.plan); err != nil {
			return err
		}
	}
	return nil
}

func (r *WorkflowRunReconciler) planWorkflowJobs(run *actionsv1alpha1.WorkflowRun, definition *workflow.Definition, inputValues map[string]any, variables any, eventPayload map[string]any) ([]plannedWorkflowJob, []deferredJobPlan, error) {
	workflowEnv, err := stringMap(definition.Env)
	if err != nil {
		return nil, nil, fmt.Errorf("workflow env: %w", err)
	}
	var variableSnapshot map[string]any
	variablesSnapshotted := false
	jobIDs := make([]string, 0, len(definition.Jobs))
	for id := range definition.Jobs {
		jobIDs = append(jobIDs, id)
	}
	sort.Strings(jobIDs)
	plannedJobs := make([]plannedWorkflowJob, 0, len(jobIDs))
	plannedIDs := make(map[string]struct{})
	deferredJobs := make([]deferredJobPlan, 0)
	sourceIDs := make(map[string]struct{}, len(jobIDs))
	for _, id := range jobIDs {
		sourceIDs[id] = struct{}{}
	}
	for _, id := range jobIDs {
		definitionJob := definition.Jobs[id]
		definitionJob.Permissions = workflow.EffectivePermissions(definition.Permissions, definitionJob.Permissions)
		if workflow.JobPlanningUsesNeeds(definitionJob) {
			if !variablesSnapshotted {
				variableSnapshot, err = snapshotExpressionVariables(variables)
				if err != nil {
					return nil, nil, err
				}
				variablesSnapshotted = true
			}
			deferredJobs = append(deferredJobs, deferredJobPlan{
				JobID: id, WorkflowName: definition.Name, WorkflowEnv: workflowEnv, EventPayload: eventPayload, Variables: variableSnapshot, Job: definitionJob, InputValues: inputValues,
			})
			if len(plannedJobs)+len(deferredJobs) > workflow.MaxJobs {
				return nil, nil, fmt.Errorf("workflow expands to more than %d jobs", workflow.MaxJobs)
			}
			continue
		}
		expressionContext := r.jobExpressionContext(run, definition.Name, inputValues, variables, eventPayload)
		combinations, err := workflow.EvaluateMatrix(id, definitionJob.Strategy, expressionContext)
		if err != nil {
			return nil, nil, err
		}
		if len(combinations) == 0 {
			combinations = []map[string]any{nil}
		}
		expanded, err := r.expandPlannedWorkflowJob(run, definition.Name, id, workflowEnv, definitionJob, inputValues, expressionContext, combinations, sourceIDs, plannedIDs)
		if err != nil {
			return nil, nil, err
		}
		plannedJobs = append(plannedJobs, expanded...)
		if len(plannedJobs)+len(deferredJobs) > workflow.MaxJobs {
			return nil, nil, fmt.Errorf("workflow expands to more than %d jobs", workflow.MaxJobs)
		}
	}
	return plannedJobs, deferredJobs, nil
}

func (r *WorkflowRunReconciler) expandPlannedWorkflowJob(run *actionsv1alpha1.WorkflowRun, workflowName, id string, workflowEnv map[string]string, definitionJob workflow.Job, inputValues map[string]any, expressionContext workflowexpression.Context, combinations []map[string]any, sourceIDs, plannedIDs map[string]struct{}) ([]plannedWorkflowJob, error) {
	plannedJobs := make([]plannedWorkflowJob, 0, len(combinations))
	for index, matrix := range combinations {
		expandedID := id
		var matrixSpec *actionsv1alpha1.WorkflowJobMatrix
		if matrix != nil {
			expandedID = uniqueMatrixWorkflowJobID(id, index, sourceIDs, plannedIDs)
			matrixSpec = &actionsv1alpha1.WorkflowJobMatrix{
				LogicalJobID: id,
				Values:       matrixStringValues(matrix),
				JobIndex:     int32(index),
				JobTotal:     int32(len(combinations)),
				MaxParallel:  definitionJob.Strategy.MaxParallel,
				FailFast:     pointerTo(definitionJob.Strategy.FailFast),
			}
		}
		if _, found := plannedIDs[expandedID]; found {
			return nil, fmt.Errorf("expanded job ID %q is not unique", expandedID)
		}
		plannedIDs[expandedID] = struct{}{}

		jobContext := expressionContext
		jobContext.Values = maps.Clone(expressionContext.Values)
		if matrix != nil {
			jobContext.Values["matrix"] = matrix
			jobContext.Values["strategy"] = workflowJobStrategyContext(matrixSpec)
		}
		resolvedJob, err := workflow.EvaluateJob(id, definitionJob, jobContext)
		if err != nil {
			return nil, err
		}
		displayName := resolvedJob.Name
		if displayName == "" {
			displayName = id
		}
		if matrix != nil {
			displayName = matrixDisplayName(displayName, matrix)
		}
		timeoutSeconds := r.effectiveJobTimeoutSeconds(resolvedJob.TimeoutMinutes.Minutes())
		plan, err := r.jobPlan(run, workflowName, id, workflowEnv, resolvedJob, matrix, inputValues, timeoutSeconds)
		if err != nil {
			return nil, err
		}
		if matrixSpec != nil {
			plan.Strategy = workflowJobStrategyContext(matrixSpec)
		}
		data, err := json.Marshal(plan)
		if err != nil {
			return nil, fmt.Errorf("encode job plan: %w", err)
		}
		if len(data) > maxJobPlanBytes {
			return nil, fmt.Errorf("job plan for %q exceeds %d bytes", expandedID, maxJobPlanBytes)
		}
		var concurrency *actionsv1alpha1.WorkflowJobConcurrency
		if definitionJob.Concurrency.Group != "" {
			concurrency = &actionsv1alpha1.WorkflowJobConcurrency{
				Group:            definitionJob.Concurrency.Group,
				CancelInProgress: workflowJobConcurrencyCancellation(definitionJob.Concurrency.CancelInProgress),
			}
		}
		plannedJobs = append(plannedJobs, plannedWorkflowJob{
			id:             expandedID,
			displayName:    displayName,
			runsOn:         append([]string(nil), resolvedJob.RunsOn...),
			needs:          append([]string(nil), resolvedJob.Needs...),
			condition:      resolvedJob.If,
			concurrency:    concurrency,
			matrix:         matrixSpec,
			plan:           string(data),
			resultVersion:  jobResultVersion,
			timeoutSeconds: timeoutSeconds,
		})
	}
	return plannedJobs, nil
}

func workflowJobConcurrencyCancellation(input workflow.BooleanExpression) *actionsv1alpha1.WorkflowJobConcurrencyCancellation {
	if input.Expression != "" {
		return &actionsv1alpha1.WorkflowJobConcurrencyCancellation{Expression: input.Expression}
	}
	if !input.Value {
		return nil
	}
	value := true
	return &actionsv1alpha1.WorkflowJobConcurrencyCancellation{Value: &value}
}

func workflowJobCancellationExpression(input *actionsv1alpha1.WorkflowJobConcurrencyCancellation) workflow.BooleanExpression {
	if input == nil {
		return workflow.BooleanExpression{}
	}
	result := workflow.BooleanExpression{Expression: input.Expression}
	if input.Value != nil {
		result.Value = *input.Value
	}
	return result
}

func (r *WorkflowRunReconciler) effectiveJobTimeoutSeconds(requestedMinutes int64) int64 {
	maximum := configuredMaxJobTimeout(r.MaxJobTimeout)
	maximumMinutes := int64(maximum / time.Minute)
	if requestedMinutes > maximumMinutes {
		requestedMinutes = maximumMinutes
	}
	return requestedMinutes * int64(time.Minute/time.Second)
}

func configuredMaxJobTimeout(value time.Duration) time.Duration {
	if value == 0 {
		return defaultMaxJobTimeout
	}
	return value
}

func matrixWorkflowJobID(logicalJobID string, index int) string {
	suffix := fmt.Sprintf("-matrix-%d", index+1)
	return boundedWorkflowJobID(logicalJobID, suffix)
}

func uniqueMatrixWorkflowJobID(logicalJobID string, index int, sourceIDs, plannedIDs map[string]struct{}) string {
	candidate := matrixWorkflowJobID(logicalJobID, index)
	for attempt := 0; ; attempt++ {
		_, sourceCollision := sourceIDs[candidate]
		_, plannedCollision := plannedIDs[candidate]
		if !sourceCollision && !plannedCollision {
			return candidate
		}
		digest := sha256.Sum256([]byte(fmt.Sprintf("%s\x00%d\x00%d", logicalJobID, index, attempt)))
		suffix := fmt.Sprintf("-matrix-%d-%s", index+1, strings.ToLower(digestEncoding.EncodeToString(digest[:]))[:workflowJobNameDigestLength])
		candidate = boundedWorkflowJobID(logicalJobID, suffix)
	}
}

func boundedWorkflowJobID(logicalJobID, suffix string) string {
	if len(logicalJobID)+len(suffix) <= workflowJobIDMaxLength {
		return logicalJobID + suffix
	}
	digest := sha256.Sum256([]byte(logicalJobID))
	digestSuffix := "-" + strings.ToLower(digestEncoding.EncodeToString(digest[:]))[:workflowJobNameDigestLength] + suffix
	return logicalJobID[:workflowJobIDMaxLength-len(digestSuffix)] + digestSuffix
}

func matrixStringValues(matrix map[string]any) map[string]string {
	values := make(map[string]string, len(matrix))
	for name, value := range matrix {
		values[name] = fmt.Sprint(value)
	}
	return values
}

func matrixDescription(matrix map[string]any) string {
	names := make([]string, 0, len(matrix))
	for name := range matrix {
		names = append(names, name)
	}
	sort.Strings(names)
	parts := make([]string, 0, len(names))
	for _, name := range names {
		parts = append(parts, name+"="+fmt.Sprint(matrix[name]))
	}
	return strings.Join(parts, ", ")
}

func matrixDisplayName(name string, matrix map[string]any) string {
	description := matrixDescription(matrix)
	suffix := " (" + description + ")"
	if len([]rune(suffix)) >= workflowJobDisplayNameMaxLength {
		digest := sha256.Sum256([]byte(description))
		suffix = fmt.Sprintf(" (matrix %x)", digest[:8])
	}
	nameRunes := []rune(name)
	maximumNameLength := workflowJobDisplayNameMaxLength - len([]rune(suffix))
	if len(nameRunes) > maximumNameLength {
		nameRunes = nameRunes[:maximumNameLength]
	}
	return string(nameRunes) + suffix
}

func workflowEvent(source *actionsv1alpha1.GitHubWorkflowRunSource) workflow.Event {
	headRef, baseRef := githubSourcePullRequestRefs(source)
	return workflow.Event{
		Name:        string(source.Event.Name),
		Action:      source.Event.Action,
		SHA:         source.Revision.SHA,
		BaseSHA:     source.Revision.BaseSHA,
		Ref:         source.Revision.Ref,
		RefName:     githubclient.RefName(source.Revision.Ref),
		HeadRef:     headRef,
		BaseRef:     baseRef,
		Inputs:      source.Event.Inputs,
		Schedule:    source.Event.Schedule,
		PullRequest: workflowPullRequest(source.Event.PullRequest),
		WorkflowRun: workflowWorkflowRunEvent(source.Event.WorkflowRun),
		Issue:       workflowIssueEvent(source.Event.Issue),
		Comment:     workflowCommentEvent(source.Event.Comment),
		Review:      workflowReviewEvent(source.Event.Review),
	}
}

func workflowPullRequest(pullRequest *actionsv1alpha1.GitHubPullRequest) *workflow.PullRequest {
	if pullRequest == nil {
		return nil
	}
	return &workflow.PullRequest{
		Number: pullRequest.Number, Body: pullRequest.Body, HTMLURL: pullRequest.HTMLURL,
		HeadRef: pullRequest.HeadRef, HeadSHA: pullRequest.HeadSHA, BaseRef: pullRequest.BaseRef,
		HeadRepository: workflow.Repository{ID: pullRequest.HeadRepository.ID, Owner: pullRequest.HeadRepository.Owner, Name: pullRequest.HeadRepository.Name},
	}
}

func workflowWorkflowRunEvent(event *actionsv1alpha1.GitHubWorkflowRunEvent) *workflow.WorkflowRunEvent {
	if event == nil {
		return nil
	}
	return &workflow.WorkflowRunEvent{Conclusion: event.Conclusion, HeadSHA: event.HeadSHA}
}

func workflowIssueEvent(event *actionsv1alpha1.GitHubIssueEvent) *workflow.IssueEvent {
	if event == nil {
		return nil
	}
	return &workflow.IssueEvent{Number: event.Number, Body: event.Body}
}

func workflowCommentEvent(event *actionsv1alpha1.GitHubCommentEvent) *workflow.CommentEvent {
	if event == nil {
		return nil
	}
	return &workflow.CommentEvent{Body: event.Body}
}

func workflowReviewEvent(event *actionsv1alpha1.GitHubReviewEvent) *workflow.ReviewEvent {
	if event == nil {
		return nil
	}
	return &workflow.ReviewEvent{Body: event.Body}
}

func githubSourcePullRequestRefs(source *actionsv1alpha1.GitHubWorkflowRunSource) (string, string) {
	if source.Event.Name == actionsv1alpha1.GitHubEventNamePullRequestTarget && source.Event.PullRequest != nil {
		return source.Event.PullRequest.HeadRef, source.Event.PullRequest.BaseRef
	}
	return source.Revision.HeadRef, source.Revision.BaseRef
}

func githubSourceActor(source *actionsv1alpha1.GitHubWorkflowRunSource) string {
	if source.Actor == "" {
		return "open-actions"
	}
	return source.Actor
}

func (r *WorkflowRunReconciler) jobExpressionContext(run *actionsv1alpha1.WorkflowRun, workflowName string, inputValues map[string]any, variables any, eventPayload map[string]any) workflowexpression.Context {
	githubSource := run.Spec.Source.GitHub
	headRef, baseRef := githubSourcePullRequestRefs(githubSource)
	eventValues := githubEventExpressionValue(githubSource, inputValues, eventPayload)
	identity := run.Status.Identity
	var runID, runNumber int64
	var runAttempt int32
	runURL, runQueryURL := "", ""
	if identity != nil {
		runID = identity.ID
		runNumber = identity.Number
		runAttempt = identity.Attempt
		runURL = identity.URL
		if r.ConsoleURL != "" {
			runQueryURL = workflowRunQueryURL(r.ConsoleURL, run)
		}
	}
	github := workflowcontext.GitHub(workflowcontext.GitHubValues{
		Actor:             githubSourceActor(githubSource),
		ActorID:           workflowcontext.EventID(eventValues, "sender", "id"),
		APIURL:            r.GitHubAPIBase,
		BaseRef:           baseRef,
		Event:             eventValues,
		EventName:         string(githubSource.Event.Name),
		HeadRef:           headRef,
		Ref:               githubSource.Revision.Ref,
		RefName:           githubclient.RefName(githubSource.Revision.Ref),
		RepositoryID:      githubSource.Repository.ID,
		RepositoryName:    githubSource.Repository.Name,
		RepositoryOwner:   githubSource.Repository.Owner,
		RepositoryOwnerID: workflowcontext.EventID(eventValues, "repository", "owner", "id"),
		RepositoryURL:     workflowcontext.EventString(eventValues, "repository", "git_url"),
		RunAttempt:        runAttempt,
		RunID:             runID,
		RunNumber:         runNumber,
		ServerURL:         r.GitHubServerURL,
		SHA:               githubSource.Revision.SHA,
		TriggeringActor:   githubSourceActor(githubSource),
		WorkflowName:      workflowName,
		WorkflowPath:      run.Spec.WorkflowPath,
	})
	return workflowexpression.Context{
		Availability: workflowexpression.NewAvailability("github", "open_actions", "inputs", "vars"),
		Values: map[string]any{
			"inputs":   inputValues,
			"vars":     variables,
			"github":   github,
			"matrix":   map[string]any{},
			"needs":    map[string]any{},
			"strategy": map[string]any{},
			"open_actions": map[string]any{
				"run_url":       runURL,
				"run_query_url": runQueryURL,
			},
		},
	}
}

func (r *WorkflowRunReconciler) projectVariableContext(ctx context.Context, project *actionsv1alpha1.Project) workflowexpression.DeferredObjectMap {
	var values map[string]string
	var loadError error
	loaded := false
	load := func() {
		if loaded {
			return
		}
		loaded = true
		if project.Spec.Variables == nil {
			values = map[string]string{}
			return
		}
		configMapName := project.Spec.Variables.ConfigMapRef.Name
		configMap := &corev1.ConfigMap{}
		if err := r.APIReader.Get(ctx, client.ObjectKey{Namespace: project.Namespace, Name: configMapName}, configMap); err != nil {
			loadError = fmt.Errorf("Project %q: get ConfigMap %q: %w", project.Name, configMapName, err)
		} else {
			values = configMap.Data
		}
	}
	resolve := func(name string) (any, bool, error) {
		name = strings.ToUpper(name)
		load()
		if loadError != nil {
			return nil, true, &projectValuesUnavailableError{cause: loadError}
		}
		value, found := values[name]
		if !found {
			return nil, false, nil
		}
		if len(value) > projectvalue.MaxValueBytes {
			configMapName := project.Spec.Variables.ConfigMapRef.Name
			cause := fmt.Errorf("Project %q ConfigMap %q key %q exceeds %d bytes", project.Name, configMapName, name, projectvalue.MaxValueBytes)
			return nil, true, &projectValuesUnavailableError{cause: cause}
		}
		return value, true, nil
	}
	all := func() (map[string]any, error) {
		load()
		if loadError != nil {
			return nil, &projectValuesUnavailableError{cause: loadError}
		}
		result := make(map[string]any, len(values))
		for name, value := range values {
			if len(value) > projectvalue.MaxValueBytes {
				configMapName := project.Spec.Variables.ConfigMapRef.Name
				cause := fmt.Errorf("Project %q ConfigMap %q key %q exceeds %d bytes", project.Name, configMapName, name, projectvalue.MaxValueBytes)
				return nil, &projectValuesUnavailableError{cause: cause}
			}
			result[name] = value
		}
		return result, nil
	}
	return workflowexpression.DeferredObjectMap{Resolve: resolve, Values: all}
}

func snapshotExpressionVariables(variables any) (map[string]any, error) {
	switch values := variables.(type) {
	case nil:
		return nil, nil
	case workflowexpression.DeferredObjectMap:
		return values.Values()
	case map[string]any:
		return maps.Clone(values), nil
	case map[string]string:
		result := make(map[string]any, len(values))
		for name, value := range values {
			result[name] = value
		}
		return result, nil
	default:
		return nil, fmt.Errorf("snapshot vars context: unsupported value %T", variables)
	}
}

func (r *WorkflowRunReconciler) workflowRunVariableContext(ctx context.Context, run *actionsv1alpha1.WorkflowRun) workflowexpression.DeferredObjectMap {
	var variables workflowexpression.DeferredObjectMap
	var loadError error
	loaded := false
	load := func() {
		if !loaded {
			loaded = true
			project := &actionsv1alpha1.Project{}
			key := client.ObjectKey{Namespace: run.Namespace, Name: run.Spec.ProjectRef.Name}
			if err := r.APIReader.Get(ctx, key, project); err != nil {
				loadError = fmt.Errorf("get Project %q: %w", key.Name, err)
			} else {
				variables = r.projectVariableContext(ctx, project)
			}
		}
	}
	resolve := func(name string) (any, bool, error) {
		load()
		if loadError != nil {
			return nil, true, &projectValuesUnavailableError{cause: loadError}
		}
		return variables.Resolve(name)
	}
	all := func() (map[string]any, error) {
		load()
		if loadError != nil {
			return nil, &projectValuesUnavailableError{cause: loadError}
		}
		return variables.Values()
	}
	return workflowexpression.DeferredObjectMap{Resolve: resolve, Values: all}
}

func (r *WorkflowRunReconciler) githubEventSnapshot(ctx context.Context, run *actionsv1alpha1.WorkflowRun) (map[string]any, error) {
	name := run.Annotations[eventsnapshot.Annotation]
	if name == "" {
		return nil, nil
	}
	secret := &corev1.Secret{}
	if err := r.APIReader.Get(ctx, client.ObjectKey{Namespace: run.Namespace, Name: name}, secret); err != nil {
		return nil, fmt.Errorf("get GitHub event snapshot Secret %q for WorkflowRun %q: %w", name, run.Name, err)
	}
	data, found := secret.Data[eventsnapshot.DataKey]
	owned := eventSnapshotOwnedByWorkflowRun(secret, run.Name, run.UID)
	if !owned && run.Spec.Rerun != nil && eventSnapshotOwnedByWorkflowRun(secret, run.Spec.Rerun.OriginalRunRef.Name, run.Spec.Rerun.OriginalRunRef.UID) {
		if secret.Immutable == nil || !*secret.Immutable || !found {
			return nil, &terminalPlanningError{cause: fmt.Errorf("GitHub event snapshot Secret %q is invalid for WorkflowRun %q", name, run.Name)}
		}
		before := secret.DeepCopy()
		secret.OwnerReferences = append(secret.OwnerReferences, metav1.OwnerReference{
			APIVersion: actionsv1alpha1.GroupVersion.String(), Kind: "WorkflowRun", Name: run.Name, UID: run.UID,
		})
		if err := r.Patch(ctx, secret, client.MergeFrom(before)); err != nil {
			return nil, fmt.Errorf("preserve GitHub event snapshot Secret %q for WorkflowRun %q: %w", name, run.Name, err)
		}
		owned = true
	}
	if !owned {
		return nil, fmt.Errorf("GitHub event snapshot Secret %q is not owned by WorkflowRun %q", name, run.Name)
	}
	if err := r.releaseRerunEventSnapshotProtection(ctx, run); err != nil {
		return nil, err
	}
	if secret.Immutable == nil || !*secret.Immutable || !found {
		return nil, &terminalPlanningError{cause: fmt.Errorf("GitHub event snapshot Secret %q is invalid for WorkflowRun %q", name, run.Name)}
	}
	payload, err := eventsnapshot.Decode(data)
	if err != nil {
		return nil, &terminalPlanningError{cause: fmt.Errorf("GitHub event snapshot Secret %q is invalid for WorkflowRun %q: %w", name, run.Name, err)}
	}
	return payload, nil
}

func eventSnapshotOwnedByWorkflowRun(secret *corev1.Secret, name string, uid types.UID) bool {
	for _, owner := range secret.OwnerReferences {
		if owner.APIVersion == actionsv1alpha1.GroupVersion.String() && owner.Kind == "WorkflowRun" && owner.Name == name && owner.UID == uid {
			return true
		}
	}
	return false
}

func (r *WorkflowRunReconciler) releaseRerunEventSnapshotProtection(ctx context.Context, run *actionsv1alpha1.WorkflowRun) error {
	if run.Spec.Rerun == nil {
		return nil
	}
	root := &actionsv1alpha1.WorkflowRun{}
	key := client.ObjectKey{Namespace: run.Namespace, Name: run.Spec.Rerun.OriginalRunRef.Name}
	if err := r.APIReader.Get(ctx, key, root); err != nil {
		return client.IgnoreNotFound(err)
	}
	if root.UID != run.Spec.Rerun.OriginalRunRef.UID || root.Annotations[eventsnapshot.RerunTargetAnnotation] != run.Name || !controllerutil.ContainsFinalizer(root, eventsnapshot.RerunProtectionFinalizer) {
		return nil
	}
	return r.clearRerunEventSnapshotProtection(ctx, root)
}

func (r *WorkflowRunReconciler) clearRerunEventSnapshotProtection(ctx context.Context, root *actionsv1alpha1.WorkflowRun) error {
	before := root.DeepCopy()
	delete(root.Annotations, eventsnapshot.RerunTargetAnnotation)
	delete(root.Annotations, eventsnapshot.RerunDeadlineAnnotation)
	controllerutil.RemoveFinalizer(root, eventsnapshot.RerunProtectionFinalizer)
	return r.Patch(ctx, root, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{}))
}

func githubEventExpressionValue(source *actionsv1alpha1.GitHubWorkflowRunSource, inputValues map[string]any, eventPayload map[string]any) map[string]any {
	if eventPayload != nil {
		return eventPayload
	}
	event := source.Event
	eventValues := map[string]any{"action": event.Action}
	if len(inputValues) > 0 {
		eventValues["inputs"] = githubEventInputValues(inputValues)
	}
	if event.Schedule != "" {
		eventValues["schedule"] = event.Schedule
	}
	if event.PullRequest != nil {
		pullRequest := githubPullRequestExpressionValue(event.PullRequest)
		if event.Name == actionsv1alpha1.GitHubEventNamePullRequest {
			pullRequest["merge_commit_sha"] = source.Revision.SHA
			if source.Revision.BaseSHA != "" {
				pullRequest["base"].(map[string]any)["sha"] = source.Revision.BaseSHA
			}
		} else if event.Name == actionsv1alpha1.GitHubEventNamePullRequestTarget {
			pullRequest["base"].(map[string]any)["sha"] = source.Revision.SHA
		}
		eventValues["pull_request"] = pullRequest
	} else if event.Name == actionsv1alpha1.GitHubEventNamePullRequest {
		eventValues["pull_request"] = map[string]any{
			"merge_commit_sha": source.Revision.SHA,
			"head":             map[string]any{"ref": source.Revision.HeadRef},
			"base":             map[string]any{"ref": source.Revision.BaseRef},
		}
	}
	if event.WorkflowRun != nil {
		eventValues["workflow_run"] = map[string]any{"conclusion": event.WorkflowRun.Conclusion, "head_sha": event.WorkflowRun.HeadSHA}
	}
	if event.Issue != nil {
		eventValues["issue"] = map[string]any{"number": event.Issue.Number, "body": event.Issue.Body}
	}
	if event.Comment != nil {
		eventValues["comment"] = map[string]any{"body": event.Comment.Body}
	}
	if event.Review != nil {
		eventValues["review"] = map[string]any{"body": event.Review.Body}
	}
	if event.Name == actionsv1alpha1.GitHubEventNameRelease {
		eventValues["release"] = map[string]any{"tag_name": githubclient.RefName(source.Revision.Ref)}
	}
	return eventValues
}

func githubEventInputValues(inputs map[string]any) map[string]string {
	values := make(map[string]string, len(inputs))
	for name, value := range inputs {
		values[name] = fmt.Sprint(value)
	}
	return values
}

func githubPullRequestExpressionValue(pullRequest *actionsv1alpha1.GitHubPullRequest) map[string]any {
	return map[string]any{
		"number": pullRequest.Number, "body": pullRequest.Body, "html_url": pullRequest.HTMLURL,
		"merge_ref": fmt.Sprintf("refs/pull/%d/merge", pullRequest.Number),
		"head": map[string]any{
			"ref": pullRequest.HeadRef,
			"sha": pullRequest.HeadSHA,
			"repo": map[string]any{
				"id":        pullRequest.HeadRepository.ID,
				"name":      pullRequest.HeadRepository.Name,
				"full_name": pullRequest.HeadRepository.Owner + "/" + pullRequest.HeadRepository.Name,
				"owner":     map[string]any{"login": pullRequest.HeadRepository.Owner},
			},
		},
		"base": map[string]any{"ref": pullRequest.BaseRef},
	}
}

func workflowJobIdentityMatches(existing, desired *actionsv1alpha1.WorkflowJob, run *actionsv1alpha1.WorkflowRun) bool {
	existingSpec := existing.Spec
	if existingSpec.DisplayName == "" {
		existingSpec.DisplayName = desired.Spec.DisplayName
	}
	if !metav1.IsControlledBy(existing, run) || !apiEquality.Semantic.DeepEqual(existingSpec, desired.Spec) {
		return false
	}
	for key, value := range desired.Labels {
		if existing.Labels[key] != value {
			return false
		}
	}
	for key, value := range desired.Annotations {
		if existing.Annotations[key] != value {
			return false
		}
	}
	return true
}

func (r *WorkflowRunReconciler) ensurePlanConfigMap(ctx context.Context, workflowJob *actionsv1alpha1.WorkflowJob, data string) error {
	if len(data) > maxJobPlanBytes {
		return fmt.Errorf("job plan for %q exceeds %d bytes", workflowJob.Spec.JobID, maxJobPlanBytes)
	}
	configMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      childName(workflowJob.Name, "plan"),
			Namespace: workflowJob.Namespace,
			Labels: map[string]string{
				actionsv1alpha1.LabelWorkflowRunUID: workflowJob.Labels[actionsv1alpha1.LabelWorkflowRunUID],
				actionsv1alpha1.LabelWorkflowJobUID: string(workflowJob.UID),
			},
		},
		Immutable: pointerTo(true),
		Data:      map[string]string{jobPlanKey: data},
	}
	if err := controllerutil.SetControllerReference(workflowJob, configMap, r.Scheme()); err != nil {
		return &terminalPlanningError{cause: err}
	}
	if err := r.Create(ctx, configMap); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return err
		}
		existing := &corev1.ConfigMap{}
		if err := r.APIReader.Get(ctx, client.ObjectKeyFromObject(configMap), existing); err != nil {
			return err
		}
		if !metav1.IsControlledBy(existing, workflowJob) {
			return &terminalPlanningError{cause: fmt.Errorf("job plan ConfigMap %q is not controlled by WorkflowJob %q", existing.Name, workflowJob.Name)}
		}
		if existing.Immutable == nil || !*existing.Immutable || existing.Data[jobPlanKey] != data {
			return &terminalPlanningError{cause: fmt.Errorf("job plan ConfigMap %q does not contain the expected immutable plan", existing.Name)}
		}
	}
	return nil
}

func (r *WorkflowRunReconciler) ensureNeedsContextConfigMap(ctx context.Context, workflowJob *actionsv1alpha1.WorkflowJob, needs runner.Needs) error {
	data, err := runner.EncodeNeedsContext(needs)
	if err != nil {
		return &terminalNeedsContextError{cause: fmt.Errorf("encode needs context for WorkflowJob %q: %w", workflowJob.Name, err)}
	}
	configMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      childName(workflowJob.Name, "needs"),
			Namespace: workflowJob.Namespace,
			Labels: map[string]string{
				actionsv1alpha1.LabelWorkflowRunUID: workflowJob.Labels[actionsv1alpha1.LabelWorkflowRunUID],
				actionsv1alpha1.LabelWorkflowJobUID: string(workflowJob.UID),
			},
		},
		Immutable: pointerTo(true),
		Data:      map[string]string{jobNeedsKey: string(data)},
	}
	if err := controllerutil.SetControllerReference(workflowJob, configMap, r.Scheme()); err != nil {
		return &terminalNeedsContextError{cause: fmt.Errorf("set WorkflowJob %q as owner of needs context ConfigMap %q: %w", workflowJob.Name, configMap.Name, err)}
	}
	if err := r.Create(ctx, configMap); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return fmt.Errorf("create needs context ConfigMap %q for WorkflowJob %q: %w", configMap.Name, workflowJob.Name, err)
		}
		existing := &corev1.ConfigMap{}
		if err := r.APIReader.Get(ctx, client.ObjectKeyFromObject(configMap), existing); err != nil {
			return fmt.Errorf("get existing needs context ConfigMap %q for WorkflowJob %q: %w", configMap.Name, workflowJob.Name, err)
		}
		if !metav1.IsControlledBy(existing, workflowJob) {
			return &terminalNeedsContextError{cause: fmt.Errorf("needs context ConfigMap %q is not controlled by WorkflowJob %q", existing.Name, workflowJob.Name)}
		}
		if existing.Immutable == nil || !*existing.Immutable || existing.Data[jobNeedsKey] != string(data) {
			return &terminalNeedsContextError{cause: fmt.Errorf("needs context ConfigMap %q does not contain the expected immutable snapshot", existing.Name)}
		}
	}
	return nil
}

func (r *WorkflowRunReconciler) ensureWorkflowFileSnapshot(ctx context.Context, run *actionsv1alpha1.WorkflowRun, data []byte) error {
	configMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      childName(run.Name, "workflow-file"),
			Namespace: run.Namespace,
			Labels:    map[string]string{actionsv1alpha1.LabelWorkflowRunUID: string(run.UID)},
		},
		Immutable: pointerTo(true),
		Data:      map[string]string{workflowsnapshot.DataKey: string(data)},
	}
	if err := controllerutil.SetControllerReference(run, configMap, r.Scheme()); err != nil {
		return &terminalPlanningError{cause: fmt.Errorf("set WorkflowRun %q as owner of workflow file ConfigMap %q: %w", run.Name, configMap.Name, err)}
	}
	if err := r.Create(ctx, configMap); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return fmt.Errorf("create workflow file ConfigMap %q for WorkflowRun %q: %w", configMap.Name, run.Name, err)
		}
		existing := &corev1.ConfigMap{}
		if err := r.APIReader.Get(ctx, client.ObjectKeyFromObject(configMap), existing); err != nil {
			return fmt.Errorf("get workflow file ConfigMap %q for WorkflowRun %q: %w", configMap.Name, run.Name, err)
		}
		if !metav1.IsControlledBy(existing, run) || existing.Immutable == nil || !*existing.Immutable || existing.Data[workflowsnapshot.DataKey] != string(data) {
			return &terminalPlanningError{cause: fmt.Errorf("workflow file ConfigMap %q does not contain the expected immutable snapshot for WorkflowRun %q", existing.Name, run.Name)}
		}
	}
	if run.Annotations[actionsv1alpha1.AnnotationWorkflowFile] == configMap.Name {
		return nil
	}
	before := run.DeepCopy()
	if run.Annotations == nil {
		run.Annotations = map[string]string{}
	}
	run.Annotations[actionsv1alpha1.AnnotationWorkflowFile] = configMap.Name
	if err := r.Patch(ctx, run, client.MergeFrom(before)); err != nil {
		return fmt.Errorf("record workflow file ConfigMap %q on WorkflowRun %q: %w", configMap.Name, run.Name, err)
	}
	return nil
}

func (r *WorkflowRunReconciler) ensureWorkflowPlan(ctx context.Context, run *actionsv1alpha1.WorkflowRun, project *actionsv1alpha1.Project, plannedJobs []plannedWorkflowJob, deferredJobs []deferredJobPlan) error {
	manifest := workflowPlanManifest{
		JobIDs:       make([]string, 0, len(plannedJobs)),
		SourceIDs:    make([]string, 0, len(plannedJobs)+len(deferredJobs)),
		DeferredJobs: make(map[string]string, len(deferredJobs)),
	}
	sourceIDs := map[string]struct{}{}
	for _, job := range plannedJobs {
		manifest.JobIDs = append(manifest.JobIDs, job.id)
		sourceID := job.id
		if job.matrix != nil {
			sourceID = job.matrix.LogicalJobID
		}
		sourceIDs[sourceID] = struct{}{}
	}
	sort.Strings(manifest.JobIDs)
	for index := range deferredJobs {
		plan := &deferredJobs[index]
		sourceIDs[plan.JobID] = struct{}{}
		name := deferredJobPlanConfigMapName(run.Name, plan.JobID)
		manifest.DeferredJobs[plan.JobID] = name
		data, err := json.Marshal(plan)
		if err != nil {
			return &terminalPlanningError{cause: fmt.Errorf("encode deferred job plan for job %q: %w", plan.JobID, err)}
		}
		if len(data) > maxJobPlanBytes {
			return &terminalPlanningError{cause: fmt.Errorf("deferred job plan for job %q exceeds %d bytes", plan.JobID, maxJobPlanBytes)}
		}
		configMap := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: run.Namespace,
				Labels: map[string]string{
					actionsv1alpha1.LabelProjectUID:     string(project.UID),
					actionsv1alpha1.LabelWorkflowRunUID: string(run.UID),
				},
				Annotations: map[string]string{
					actionsv1alpha1.AnnotationProjectName:     project.Name,
					actionsv1alpha1.AnnotationDeferredJobPlan: plan.JobID,
				},
			},
			Immutable: pointerTo(true),
			Data:      map[string]string{deferredJobPlanKey: string(data)},
		}
		if err := r.ensureWorkflowOwnedConfigMap(ctx, run, configMap, deferredJobPlanKey); err != nil {
			return err
		}
	}
	for id := range sourceIDs {
		manifest.SourceIDs = append(manifest.SourceIDs, id)
	}
	sort.Strings(manifest.SourceIDs)
	data, err := json.Marshal(manifest)
	if err != nil {
		return &terminalPlanningError{cause: fmt.Errorf("encode WorkflowRun %q plan: %w", run.Name, err)}
	}
	configMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      workflowPlanConfigMapName(run.Name),
			Namespace: run.Namespace,
			Labels:    map[string]string{actionsv1alpha1.LabelWorkflowRunUID: string(run.UID)},
		},
		Immutable: pointerTo(true),
		Data:      map[string]string{workflowPlanKey: string(data)},
	}
	if err := r.ensureWorkflowOwnedConfigMap(ctx, run, configMap, workflowPlanKey); err != nil {
		return err
	}
	if run.Annotations[actionsv1alpha1.AnnotationWorkflowPlan] == configMap.Name {
		return nil
	}
	before := run.DeepCopy()
	if run.Annotations == nil {
		run.Annotations = map[string]string{}
	}
	run.Annotations[actionsv1alpha1.AnnotationWorkflowPlan] = configMap.Name
	return r.Patch(ctx, run, client.MergeFrom(before))
}

func (r *WorkflowRunReconciler) ensureWorkflowOwnedConfigMap(ctx context.Context, run *actionsv1alpha1.WorkflowRun, desired *corev1.ConfigMap, dataKey string) error {
	if err := controllerutil.SetControllerReference(run, desired, r.Scheme()); err != nil {
		return &terminalPlanningError{cause: err}
	}
	if err := r.Create(ctx, desired); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return err
		}
		existing := &corev1.ConfigMap{}
		if err := r.APIReader.Get(ctx, client.ObjectKeyFromObject(desired), existing); err != nil {
			return err
		}
		if !metav1.IsControlledBy(existing, run) || existing.Immutable == nil || !*existing.Immutable || existing.Data[dataKey] != desired.Data[dataKey] {
			return &terminalPlanningError{cause: fmt.Errorf("planning ConfigMap %q does not contain the expected immutable data for WorkflowRun %q", existing.Name, run.Name)}
		}
	}
	return nil
}

func workflowPlanConfigMapName(runName string) string {
	return childName(runName, "workflow-plan")
}

func deferredJobPlanConfigMapName(runName, jobID string) string {
	// The matrix-plan suffix is part of the persisted WorkflowRun plan manifest.
	return childName(workflowJobName(runName, jobID), "matrix-plan")
}

func (r *WorkflowRunReconciler) jobPlan(run *actionsv1alpha1.WorkflowRun, workflowName, id string, workflowEnv map[string]string, job workflow.Job, matrix, inputValues map[string]any, timeoutSeconds int64) (*runner.Plan, error) {
	githubSource := run.Spec.Source.GitHub
	identity := run.Status.Identity
	if identity == nil {
		return nil, fmt.Errorf("WorkflowRun %q has no run identity", run.Name)
	}
	headRef, baseRef := githubSourcePullRequestRefs(githubSource)
	jobEnv, err := stringMap(job.Env)
	if err != nil {
		return nil, fmt.Errorf("job %q env: %w", id, err)
	}
	jobEnv = mergeStringMaps(workflowEnv, jobEnv)
	outputs, err := stringMap(job.Outputs)
	if err != nil {
		return nil, fmt.Errorf("job %q outputs: %w", id, err)
	}
	steps := make([]runner.Step, 0, len(job.Steps))
	for index, step := range job.Steps {
		with, err := stringMap(step.With)
		if err != nil {
			return nil, fmt.Errorf("job %q step %d with: %w", id, index+1, err)
		}
		environment, err := stringMap(step.Env)
		if err != nil {
			return nil, fmt.Errorf("job %q step %d env: %w", id, index+1, err)
		}
		steps = append(steps, runner.Step{
			ID:               step.ID,
			Name:             step.Name,
			Uses:             step.Uses,
			Run:              step.Run,
			WorkingDirectory: step.WorkingDirectory,
			With:             with,
			Env:              environment,
			If:               step.If,
			ContinueOnError:  step.ContinueOnError,
		})
	}
	return &runner.Plan{
		Version: runner.PlanVersion,
		Inputs:  inputValues,
		Run: runner.Run{
			ID: identity.ID, Number: identity.Number, Attempt: identity.Attempt,
			Actor: githubSourceActor(githubSource), TriggeringActor: githubSourceActor(githubSource), URL: identity.URL,
			QueryURL: workflowRunQueryURL(r.ConsoleURL, run),
		},
		Repository: runner.Repository{
			ID:                 githubSource.Repository.ID,
			Owner:              githubSource.Repository.Owner,
			Name:               githubSource.Repository.Name,
			ServerURL:          strings.TrimSuffix(r.GitHubServerURL, "/"),
			APIURL:             strings.TrimSuffix(r.GitHubAPIBase, "/"),
			ActionCloneBaseURL: strings.TrimSuffix(r.ActionCloneBaseURL, "/"),
		},
		Event: runner.Event{
			Name:        string(githubSource.Event.Name),
			Action:      githubSource.Event.Action,
			DeliveryID:  githubSource.Event.DeliveryID,
			Schedule:    githubSource.Event.Schedule,
			PullRequest: runnerPullRequest(githubSource.Event.PullRequest),
			WorkflowRun: runnerWorkflowRunEvent(githubSource.Event.WorkflowRun),
			Issue:       runnerIssueEvent(githubSource.Event.Issue),
			Comment:     runnerCommentEvent(githubSource.Event.Comment),
			Review:      runnerReviewEvent(githubSource.Event.Review),
		},
		WorkflowName: workflowName,
		WorkflowPath: run.Spec.WorkflowPath,
		Revision: runner.Revision{
			SHA:          githubSource.Revision.SHA,
			HeadSHA:      githubSource.Revision.HeadSHA,
			BaseSHA:      githubSource.Revision.BaseSHA,
			MergeBaseSHA: githubSource.Revision.MergeBaseSHA,
			Ref:          githubSource.Revision.Ref,
			RefName:      githubclient.RefName(githubSource.Revision.Ref),
			HeadRef:      headRef,
			BaseRef:      baseRef,
		},
		JobID:                  id,
		Matrix:                 matrix,
		Env:                    jobEnv,
		Outputs:                outputs,
		GitHubTokenPermissions: githubTokenPermissions(run, workflow.EffectivePermissions(nil, job.Permissions)),
		TimeoutSeconds:         timeoutSeconds,
		CleanupTimeoutSeconds:  int64(runner.CleanupTimeout / time.Second),
		Steps:                  steps,
	}, nil
}

func githubTokenPermissions(run *actionsv1alpha1.WorkflowRun, permissions workflow.Permissions) map[string]string {
	restrictWrites := false
	githubSource := run.Spec.Source.GitHub
	if policy := run.Spec.ForkPullRequest; policy != nil {
		restrictWrites = !policy.SendWriteTokens
	} else if pullRequest := githubSource.Event.PullRequest; pullRequest != nil && githubSource.Event.Name != actionsv1alpha1.GitHubEventNamePullRequestTarget {
		restrictWrites = pullRequest.HeadRepository.ID != githubSource.Repository.ID
	}
	result := make(map[string]string, len(permissions))
	for name, level := range permissions {
		if restrictWrites && level == "write" {
			level = "read"
		}
		result[strings.ReplaceAll(name, "-", "_")] = level
	}
	return result
}

func (r *WorkflowRunReconciler) waitingForApproval(ctx context.Context, run *actionsv1alpha1.WorkflowRun) (ctrl.Result, error) {
	before := run.Status.DeepCopy()
	run.Status.ObservedGeneration = run.Generation
	meta.SetStatusCondition(&run.Status.Conditions, metav1.Condition{
		Type: actionsv1alpha1.WorkflowRunConditionApproved, Status: metav1.ConditionFalse,
		ObservedGeneration: run.Generation, Reason: "ApprovalRequired", Message: "An administrator must approve this fork pull request revision",
	})
	meta.SetStatusCondition(&run.Status.Conditions, metav1.Condition{
		Type: actionsv1alpha1.WorkflowRunConditionPlanned, Status: metav1.ConditionUnknown,
		ObservedGeneration: run.Generation, Reason: "ApprovalRequired", Message: "The workflow is waiting for approval before jobs are created",
	})
	if apiEquality.Semantic.DeepEqual(before, &run.Status) {
		return ctrl.Result{}, nil
	}
	return ctrl.Result{}, r.Status().Update(ctx, run)
}

func (r *WorkflowRunReconciler) supersedingForkPullRequestRevision(ctx context.Context, run *actionsv1alpha1.WorkflowRun) (*actionsv1alpha1.WorkflowRun, error) {
	runs := &actionsv1alpha1.WorkflowRunList{}
	if err := r.APIReader.List(ctx, runs, client.InNamespace(run.Namespace)); err != nil {
		return nil, err
	}
	var superseding *actionsv1alpha1.WorkflowRun
	for index := range runs.Items {
		candidate := &runs.Items[index]
		if !candidate.CreationTimestamp.After(run.CreationTimestamp.Time) || !sameForkPullRequest(run, candidate) || forkPullRequestHeadSHA(run) == forkPullRequestHeadSHA(candidate) {
			continue
		}
		if superseding == nil || candidate.CreationTimestamp.After(superseding.CreationTimestamp.Time) {
			superseding = candidate
		}
	}
	return superseding, nil
}

func sameForkPullRequest(left, right *actionsv1alpha1.WorkflowRun) bool {
	leftSource := left.Spec.Source.GitHub
	rightSource := right.Spec.Source.GitHub
	if left.Spec.ForkPullRequest == nil || right.Spec.ForkPullRequest == nil || leftSource == nil || rightSource == nil || leftSource.Event.PullRequest == nil || rightSource.Event.PullRequest == nil {
		return false
	}
	return left.Spec.ProjectRef.Name == right.Spec.ProjectRef.Name &&
		leftSource.Repository.ID == rightSource.Repository.ID &&
		leftSource.Event.PullRequest.Number == rightSource.Event.PullRequest.Number
}

func forkPullRequestHeadSHA(run *actionsv1alpha1.WorkflowRun) string {
	if run.Spec.Source.GitHub == nil || run.Spec.Source.GitHub.Event.PullRequest == nil {
		return ""
	}
	return run.Spec.Source.GitHub.Event.PullRequest.HeadSHA
}

func (r *WorkflowRunReconciler) recordApproval(ctx context.Context, run *actionsv1alpha1.WorkflowRun, reason, message string) (ctrl.Result, error) {
	before := run.Status.DeepCopy()
	run.Status.ObservedGeneration = run.Generation
	meta.SetStatusCondition(&run.Status.Conditions, metav1.Condition{
		Type: actionsv1alpha1.WorkflowRunConditionApproved, Status: metav1.ConditionTrue,
		ObservedGeneration: run.Generation, Reason: reason, Message: message,
	})
	if !apiEquality.Semantic.DeepEqual(before, &run.Status) {
		if err := r.Status().Update(ctx, run); err != nil {
			return ctrl.Result{}, err
		}
	}
	return ctrl.Result{Requeue: true}, nil
}

func (r *WorkflowRunReconciler) completeUnplannedWorkflowRun(ctx context.Context, run *actionsv1alpha1.WorkflowRun, reason, message string) (ctrl.Result, error) {
	before := run.Status.DeepCopy()
	now := metav1.Now()
	run.Status.ObservedGeneration = run.Generation
	run.Status.CompletionTime = &now
	if run.Spec.ForkPullRequest != nil {
		meta.SetStatusCondition(&run.Status.Conditions, metav1.Condition{
			Type: actionsv1alpha1.WorkflowRunConditionApproved, Status: metav1.ConditionFalse,
			ObservedGeneration: run.Generation, Reason: reason, Message: message,
		})
	}
	meta.SetStatusCondition(&run.Status.Conditions, metav1.Condition{
		Type: actionsv1alpha1.WorkflowRunConditionPlanned, Status: metav1.ConditionFalse,
		ObservedGeneration: run.Generation, Reason: reason, Message: message,
	})
	meta.SetStatusCondition(&run.Status.Conditions, metav1.Condition{
		Type: actionsv1alpha1.WorkflowRunConditionSucceeded, Status: metav1.ConditionFalse,
		ObservedGeneration: run.Generation, Reason: reason, Message: message,
	})
	if apiEquality.Semantic.DeepEqual(before, &run.Status) {
		return ctrl.Result{}, nil
	}
	if err := r.Status().Update(ctx, run); err != nil {
		return ctrl.Result{}, err
	}
	if r.Metrics != nil {
		r.Metrics.WorkflowRunCompleted(before, run)
	}
	recordConditionWarning(r.Recorder, run, before.Conditions, run.Status.Conditions, actionsv1alpha1.WorkflowRunConditionSucceeded)
	return ctrl.Result{}, nil
}

func runnerPullRequest(pullRequest *actionsv1alpha1.GitHubPullRequest) *runner.PullRequest {
	if pullRequest == nil {
		return nil
	}
	return &runner.PullRequest{
		Number: pullRequest.Number, Body: pullRequest.Body, HTMLURL: pullRequest.HTMLURL,
		HeadRef: pullRequest.HeadRef, HeadSHA: pullRequest.HeadSHA, BaseRef: pullRequest.BaseRef,
		HeadRepository: runner.EventRepository{ID: pullRequest.HeadRepository.ID, Owner: pullRequest.HeadRepository.Owner, Name: pullRequest.HeadRepository.Name},
	}
}

func runnerWorkflowRunEvent(event *actionsv1alpha1.GitHubWorkflowRunEvent) *runner.WorkflowRunEvent {
	if event == nil {
		return nil
	}
	return &runner.WorkflowRunEvent{Conclusion: event.Conclusion, HeadSHA: event.HeadSHA}
}

func runnerIssueEvent(event *actionsv1alpha1.GitHubIssueEvent) *runner.IssueEvent {
	if event == nil {
		return nil
	}
	return &runner.IssueEvent{Number: event.Number, Body: event.Body}
}

func runnerCommentEvent(event *actionsv1alpha1.GitHubCommentEvent) *runner.CommentEvent {
	if event == nil {
		return nil
	}
	return &runner.CommentEvent{Body: event.Body}
}

func runnerReviewEvent(event *actionsv1alpha1.GitHubReviewEvent) *runner.ReviewEvent {
	if event == nil {
		return nil
	}
	return &runner.ReviewEvent{Body: event.Body}
}

type workflowPlanState struct {
	active   bool
	changed  bool
	pending  map[string]struct{}
	expected map[string]struct{}
}

func (r *WorkflowRunReconciler) reconcileDeferredJobs(ctx context.Context, run *actionsv1alpha1.WorkflowRun, jobs *actionsv1alpha1.WorkflowJobList) (workflowPlanState, error) {
	state := workflowPlanState{}
	planName := run.Annotations[actionsv1alpha1.AnnotationWorkflowPlan]
	if planName == "" {
		return state, nil
	}
	configMap := &corev1.ConfigMap{}
	key := client.ObjectKey{Namespace: run.Namespace, Name: planName}
	if err := r.APIReader.Get(ctx, key, configMap); err != nil {
		if apierrors.IsNotFound(err) {
			return state, nil
		}
		return state, err
	}
	if !metav1.IsControlledBy(configMap, run) || configMap.Immutable == nil || !*configMap.Immutable {
		return state, &terminalPlanningError{cause: fmt.Errorf("planning ConfigMap %q is not immutable and controlled by WorkflowRun %q", configMap.Name, run.Name)}
	}
	manifest := workflowPlanManifest{}
	if err := json.Unmarshal([]byte(configMap.Data[workflowPlanKey]), &manifest); err != nil {
		return state, &terminalPlanningError{cause: fmt.Errorf("decode planning ConfigMap %q for WorkflowRun %q: %w", configMap.Name, run.Name, err)}
	}
	state.active = true
	state.pending = map[string]struct{}{}
	state.expected = make(map[string]struct{}, len(manifest.JobIDs)+len(manifest.DeferredJobs))
	for _, id := range manifest.JobIDs {
		if _, found := state.expected[id]; found {
			return state, &terminalPlanningError{cause: fmt.Errorf("WorkflowRun %q plan repeats job %q", run.Name, id)}
		}
		state.expected[id] = struct{}{}
	}

	jobsByID := make(map[string]*actionsv1alpha1.WorkflowJob, len(jobs.Items))
	jobsByLogicalID := make(map[string][]*actionsv1alpha1.WorkflowJob, len(jobs.Items))
	for index := range jobs.Items {
		job := &jobs.Items[index]
		if existing := jobsByID[job.Spec.JobID]; existing != nil {
			return state, fmt.Errorf("WorkflowJobs %q and %q both represent job %q in WorkflowRun %q", existing.Name, job.Name, job.Spec.JobID, run.Name)
		}
		jobsByID[job.Spec.JobID] = job
		logicalID := job.Spec.JobID
		if job.Spec.Matrix != nil {
			logicalID = job.Spec.Matrix.LogicalJobID
		}
		jobsByLogicalID[logicalID] = append(jobsByLogicalID[logicalID], job)
	}

	sourceIDs := make(map[string]struct{}, len(manifest.SourceIDs))
	for _, id := range manifest.SourceIDs {
		sourceIDs[id] = struct{}{}
	}
	deferredJobIDs := make([]string, 0, len(manifest.DeferredJobs))
	for id := range manifest.DeferredJobs {
		deferredJobIDs = append(deferredJobIDs, id)
	}
	sort.Strings(deferredJobIDs)
	for _, id := range deferredJobIDs {
		planConfigMap := &corev1.ConfigMap{}
		planKey := client.ObjectKey{Namespace: run.Namespace, Name: manifest.DeferredJobs[id]}
		if err := r.APIReader.Get(ctx, planKey, planConfigMap); err != nil {
			if apierrors.IsNotFound(err) {
				return state, &terminalPlanningError{cause: fmt.Errorf("deferred job plan ConfigMap %q for job %q is missing from WorkflowRun %q", planKey.Name, id, run.Name)}
			}
			return state, err
		}
		if !metav1.IsControlledBy(planConfigMap, run) || planConfigMap.Immutable == nil || !*planConfigMap.Immutable || planConfigMap.Annotations[actionsv1alpha1.AnnotationDeferredJobPlan] != id {
			return state, &terminalPlanningError{cause: fmt.Errorf("deferred job plan ConfigMap %q does not match job %q in WorkflowRun %q", planConfigMap.Name, id, run.Name)}
		}
		plan := deferredJobPlan{}
		decoder := json.NewDecoder(strings.NewReader(planConfigMap.Data[deferredJobPlanKey]))
		decoder.UseNumber()
		if err := decoder.Decode(&plan); err != nil {
			return state, &terminalPlanningError{cause: fmt.Errorf("decode deferred job plan ConfigMap %q: %w", planConfigMap.Name, err)}
		}
		if plan.JobID != id {
			return state, &terminalPlanningError{cause: fmt.Errorf("deferred job plan ConfigMap %q identifies job %q, want %q", planConfigMap.Name, plan.JobID, id)}
		}
		if resultJob := jobsByID[id]; resultJob != nil && workflowJobTerminal(resultJob) {
			state.expected[id] = struct{}{}
			continue
		}
		resultPlaceholder, err := r.deferredJobResultPlaceholder(run, planConfigMap, plan)
		if err != nil {
			return state, &terminalPlanningError{cause: err}
		}
		jobPlanned := false
		for _, job := range jobsByLogicalID[id] {
			if !deferredJobResultPlaceholderMatches(job, resultPlaceholder, run) {
				jobPlanned = true
				break
			}
		}

		dependenciesReady := true
		for _, dependency := range plan.Job.Needs {
			needed := jobsByLogicalID[dependency]
			if len(needed) == 0 {
				dependenciesReady = false
				break
			}
			for _, dependencyJob := range needed {
				if !workflowJobTerminal(dependencyJob) {
					dependenciesReady = false
					break
				}
			}
		}
		if !dependenciesReady {
			state.pending[id] = struct{}{}
			continue
		}

		logicalJob := &actionsv1alpha1.WorkflowJob{Spec: actionsv1alpha1.WorkflowJobSpec{JobID: id, Needs: append([]string(nil), plan.Job.Needs...), If: plan.Job.If}}
		expressionContext := r.jobExpressionContext(run, plan.WorkflowName, plan.InputValues, plan.Variables, plan.EventPayload)
		expressionContext.Values["needs"] = workflowNeedsContext(logicalJob, jobsByLogicalID).ExpressionValues()
		expressionContext.Status = workflowJobAncestorStatus(logicalJob, jobsByLogicalID, run.Spec.CancelRequested)
		if !jobPlanned {
			runnable, err := workflow.EvaluateJobCondition(id, plan.Job.If, expressionContext)
			if err != nil {
				changed, resultErr := r.completeDeferredJobPlanning(ctx, run, planConfigMap, plan, jobsByID[id], actionsv1alpha1.WorkflowJobResultFailure, "ConditionEvaluationFailed", err.Error())
				state.changed = state.changed || changed
				state.expected[id] = struct{}{}
				return state, resultErr
			}
			if !runnable {
				result := actionsv1alpha1.WorkflowJobResultSkipped
				reason := "ConditionFalse"
				message := "The workflow job condition evaluated to false before deferred planning"
				if run.Spec.CancelRequested {
					result = actionsv1alpha1.WorkflowJobResultCancelled
					reason = "CancellationRequested"
					message = "The workflow job was cancelled before deferred planning"
				}
				changed, resultErr := r.completeDeferredJobPlanning(ctx, run, planConfigMap, plan, jobsByID[id], result, reason, message)
				state.changed = state.changed || changed
				state.expected[id] = struct{}{}
				return state, resultErr
			}
		}

		combinations, err := workflow.EvaluateMatrix(id, plan.Job.Strategy, expressionContext)
		if err != nil {
			var unavailable *projectValuesUnavailableError
			if errors.As(err, &unavailable) {
				return state, err
			}
			changed, resultErr := r.completeDeferredJobPlanning(ctx, run, planConfigMap, plan, jobsByID[id], actionsv1alpha1.WorkflowJobResultFailure, "JobPlanningFailed", err.Error())
			state.changed = state.changed || changed
			state.expected[id] = struct{}{}
			return state, resultErr
		}
		if len(combinations) == 0 {
			combinations = []map[string]any{nil}
		}
		if projectedWorkflowJobCount(len(jobs.Items), jobsByLogicalID, deferredJobIDs, id, len(combinations)) > workflow.MaxJobs {
			message := fmt.Sprintf("workflow expands to more than %d jobs", workflow.MaxJobs)
			changed, resultErr := r.completeDeferredJobPlanning(ctx, run, planConfigMap, plan, jobsByID[id], actionsv1alpha1.WorkflowJobResultFailure, "JobPlanningFailed", message)
			state.changed = state.changed || changed
			state.expected[id] = struct{}{}
			return state, resultErr
		}

		plannedIDs := make(map[string]struct{}, len(jobsByID)+len(manifest.JobIDs))
		for existingID, existing := range jobsByID {
			if existing.Spec.JobID != id && (existing.Spec.Matrix == nil || existing.Spec.Matrix.LogicalJobID != id) {
				plannedIDs[existingID] = struct{}{}
			}
		}
		for expectedID := range state.expected {
			plannedIDs[expectedID] = struct{}{}
		}
		expanded, err := r.expandPlannedWorkflowJob(run, plan.WorkflowName, id, plan.WorkflowEnv, plan.Job, plan.InputValues, expressionContext, combinations, sourceIDs, plannedIDs)
		if err != nil {
			var unavailable *projectValuesUnavailableError
			if errors.As(err, &unavailable) {
				return state, err
			}
			changed, resultErr := r.completeDeferredJobPlanning(ctx, run, planConfigMap, plan, jobsByID[id], actionsv1alpha1.WorkflowJobResultFailure, "JobPlanningFailed", err.Error())
			state.changed = state.changed || changed
			state.expected[id] = struct{}{}
			return state, resultErr
		}
		for _, job := range expanded {
			state.expected[job.id] = struct{}{}
			if jobsByID[job.id] == nil {
				state.changed = true
			}
		}
		project := &actionsv1alpha1.Project{ObjectMeta: metav1.ObjectMeta{
			Name: planConfigMap.Annotations[actionsv1alpha1.AnnotationProjectName],
			UID:  types.UID(planConfigMap.Labels[actionsv1alpha1.LabelProjectUID]),
		}}
		if err := r.ensureWorkflowJobs(ctx, run, project, expanded); err != nil {
			return state, err
		}
		if state.changed {
			return state, nil
		}
	}
	return state, nil
}

func projectedWorkflowJobCount(existingJobs int, jobsByLogicalID map[string][]*actionsv1alpha1.WorkflowJob, deferredJobIDs []string, currentID string, combinations int) int {
	result := existingJobs - len(jobsByLogicalID[currentID]) + combinations
	for _, id := range deferredJobIDs {
		if id != currentID && len(jobsByLogicalID[id]) == 0 {
			result++
		}
	}
	return result
}

func (r *WorkflowRunReconciler) completeDeferredJobPlanning(ctx context.Context, run *actionsv1alpha1.WorkflowRun, planConfigMap *corev1.ConfigMap, plan deferredJobPlan, existing *actionsv1alpha1.WorkflowJob, result actionsv1alpha1.WorkflowJobResult, reason, message string) (bool, error) {
	desired, err := r.deferredJobResultPlaceholder(run, planConfigMap, plan)
	if err != nil {
		return false, &terminalPlanningError{cause: err}
	}
	job := desired
	created := false
	if existing != nil {
		if !deferredJobResultPlaceholderMatches(existing, desired, run) {
			return false, &terminalPlanningError{cause: fmt.Errorf("WorkflowJob %q does not match deferred job %q in WorkflowRun %q", existing.Name, plan.JobID, run.Name)}
		}
		job = existing
	} else if err := r.Create(ctx, desired); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return false, err
		}
		job = &actionsv1alpha1.WorkflowJob{}
		if err := r.APIReader.Get(ctx, client.ObjectKey{Namespace: run.Namespace, Name: workflowJobName(run.Name, plan.JobID)}, job); err != nil {
			return false, err
		}
		if !deferredJobResultPlaceholderMatches(job, desired, run) {
			return false, &terminalPlanningError{cause: fmt.Errorf("WorkflowJob %q does not match deferred job %q in WorkflowRun %q", job.Name, plan.JobID, run.Name)}
		}
	} else {
		created = true
	}
	if workflowJobTerminal(job) {
		return created, nil
	}
	if err := r.completeUnscheduledWorkflowJob(ctx, job, result, reason, message); err != nil {
		return created, err
	}
	return true, nil
}

func (r *WorkflowRunReconciler) deferredJobResultPlaceholder(run *actionsv1alpha1.WorkflowRun, planConfigMap *corev1.ConfigMap, plan deferredJobPlan) (*actionsv1alpha1.WorkflowJob, error) {
	project := &actionsv1alpha1.Project{ObjectMeta: metav1.ObjectMeta{UID: types.UID(planConfigMap.Labels[actionsv1alpha1.LabelProjectUID])}}
	desired := &actionsv1alpha1.WorkflowJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:        workflowJobName(run.Name, plan.JobID),
			Namespace:   run.Namespace,
			Labels:      workflowJobLabels(run, project, plan.JobID),
			Annotations: map[string]string{actionsv1alpha1.AnnotationProjectName: planConfigMap.Annotations[actionsv1alpha1.AnnotationProjectName]},
		},
		Spec: actionsv1alpha1.WorkflowJobSpec{
			WorkflowRunRef: corev1.LocalObjectReference{Name: run.Name},
			JobID:          plan.JobID,
			DisplayName:    plan.JobID,
			RunsOn:         []string{deferredJobResultRunsOn},
			Needs:          append([]string(nil), plan.Job.Needs...),
			If:             plan.Job.If,
		},
	}
	if err := controllerutil.SetControllerReference(run, desired, r.Scheme()); err != nil {
		return nil, err
	}
	return desired, nil
}

func deferredJobResultPlaceholderMatches(existing, desired *actionsv1alpha1.WorkflowJob, run *actionsv1alpha1.WorkflowRun) bool {
	// Stored result placeholders can use either sentinel and must remain
	// reconcilable across controller upgrades.
	for _, runsOn := range []string{deferredJobResultRunsOn, matrixEvaluationResultRunsOn} {
		candidate := desired.DeepCopy()
		candidate.Spec.RunsOn = []string{runsOn}
		if workflowJobIdentityMatches(existing, candidate, run) {
			return true
		}
	}
	return false
}

func (r *WorkflowRunReconciler) observeWorkflowJobs(ctx context.Context, run *actionsv1alpha1.WorkflowRun, workflowName string, total int32) (ctrl.Result, error) {
	reader := r.APIReader
	jobs := &actionsv1alpha1.WorkflowJobList{}
	if err := reader.List(ctx, jobs, client.InNamespace(run.Namespace), client.MatchingLabels{actionsv1alpha1.LabelWorkflowRunUID: string(run.UID)}); err != nil {
		return ctrl.Result{}, err
	}
	planState, err := r.reconcileDeferredJobs(ctx, run, jobs)
	if err != nil {
		return ctrl.Result{}, err
	}
	if planState.changed {
		return ctrl.Result{Requeue: true}, nil
	}
	expectedObjects := int(total)
	pendingJobs := map[string]struct{}{}
	if planState.active {
		expectedObjects = len(planState.expected)
		pendingJobs = planState.pending
		total = int32(expectedObjects + len(pendingJobs))
	}
	failFastPending, err := r.reconcileMatrixFailFast(ctx, jobs)
	if err != nil {
		return ctrl.Result{}, err
	}
	status := &actionsv1alpha1.WorkflowRunJobStatus{Total: total, Waiting: int32(len(pendingJobs))}
	var startTime *metav1.Time
	lostState := ""
	waitingForRuntimeState := false
	hasNonFailFastCancellation := false
	if len(jobs.Items) != expectedObjects {
		active, err := activeRuntimeWorkloads(ctx, reader, run)
		if err != nil {
			return ctrl.Result{}, err
		}
		if active {
			waitingForRuntimeState = true
		} else {
			lostState = fmt.Sprintf("expected %d WorkflowJobs, found %d", expectedObjects, len(jobs.Items))
		}
	}
	if len(jobs.Items) == expectedObjects {
		dependencyJobs, dependencyErr := r.rerunDependencyWorkflowJobs(ctx, run, jobs.Items)
		if dependencyErr != nil {
			terminal := &terminalPlanningError{}
			if !errors.As(dependencyErr, &terminal) {
				return ctrl.Result{}, dependencyErr
			}
			lostState = dependencyErr.Error()
		}
		if lostState == "" {
			var inputValues map[string]any
			var eventPayload map[string]any
			if workflowJobGraphNeedsExpressionContext(run, jobs.Items) {
				inputValues, err = r.workflowJobGraphInputValues(ctx, jobs.Items)
				if err != nil {
					return ctrl.Result{}, err
				}
				eventPayload, err = r.githubEventSnapshot(ctx, run)
				if err != nil {
					return ctrl.Result{}, err
				}
			}
			variables := r.workflowRunVariableContext(ctx, run)
			if err := r.reconcileWorkflowJobGraphWithDependencies(ctx, run, workflowName, inputValues, variables, eventPayload, jobs.Items, dependencyJobs, pendingJobs); err != nil {
				return ctrl.Result{}, err
			}
		}
	}
	concurrencyPending := false
	for index := range jobs.Items {
		job := &jobs.Items[index]
		if !workflowJobTerminal(job) {
			condition := meta.FindStatusCondition(job.Status.Conditions, actionsv1alpha1.WorkflowJobConditionConcurrencyAcquired)
			if condition != nil && condition.Status == metav1.ConditionUnknown {
				concurrencyPending = true
			}
		}
		if !workflowJobTerminal(job) {
			plan := &corev1.ConfigMap{}
			planKey := client.ObjectKey{Namespace: job.Namespace, Name: childName(job.Name, "plan")}
			planError := ""
			if err := reader.Get(ctx, planKey, plan); err != nil {
				if !apierrors.IsNotFound(err) {
					return ctrl.Result{}, err
				}
				planError = fmt.Sprintf("WorkflowJob %q is missing its plan ConfigMap", job.Name)
			} else if !metav1.IsControlledBy(plan, job) {
				planError = fmt.Sprintf("WorkflowJob %q does not control plan ConfigMap %q", job.Name, plan.Name)
			}
			if planError != "" {
				exists, err := workflowJobRuntimeStateExists(ctx, reader, job)
				if err != nil {
					return ctrl.Result{}, err
				}
				if exists {
					waitingForRuntimeState = true
				} else {
					lostState = planError
				}
			}
		}
		if job.Status.StartTime != nil && (startTime == nil || job.Status.StartTime.Before(startTime)) {
			startTime = job.Status.StartTime.DeepCopy()
		}
		result := workflowJobResult(job)
		if result != "" && job.Status.Concurrency != nil {
			active, err := r.workflowJobExecutionActive(ctx, job)
			if err != nil {
				return ctrl.Result{}, err
			}
			if active {
				waitingForRuntimeState = true
			} else {
				scope, err := workflowRunConcurrencyScope(run)
				if err != nil {
					return ctrl.Result{}, err
				}
				if err := r.releaseConcurrency(ctx, job.Namespace, scope, job.Status.Concurrency.Group, workflowJobConcurrencyMember(job)); err != nil {
					return ctrl.Result{}, err
				}
			}
		}
		switch {
		case result == actionsv1alpha1.WorkflowJobResultSuccess:
			status.Succeeded++
		case workflowJobTimedOut(job):
			status.TimedOut++
		case result == actionsv1alpha1.WorkflowJobResultFailure:
			status.Failed++
		case result == actionsv1alpha1.WorkflowJobResultSkipped:
			status.Skipped++
		case result == actionsv1alpha1.WorkflowJobResultCancelled:
			status.Cancelled++
			if !workflowJobCancelledByMatrixFailFast(job) {
				hasNonFailFastCancellation = true
			}
		case job.Status.RunnerRef != nil:
			status.Active++
		case workflowJobReady(job):
			status.Queued++
		default:
			status.Waiting++
		}
	}
	if lostState != "" {
		active, err := activeRuntimeWorkloads(ctx, reader, run)
		if err != nil {
			return ctrl.Result{}, err
		}
		if active {
			lostState = ""
			waitingForRuntimeState = true
		}
	}

	before := run.Status.DeepCopy()
	run.Status.ObservedGeneration = run.Generation
	run.Status.WorkflowName = workflowName
	run.Status.Jobs = status
	if run.Status.StartTime == nil && startTime != nil {
		run.Status.StartTime = startTime
	}
	if lostState != "" {
		now := metav1.Now()
		run.Status.CompletionTime = &now
		meta.SetStatusCondition(&run.Status.Conditions, metav1.Condition{
			Type: actionsv1alpha1.WorkflowRunConditionPlanned, Status: metav1.ConditionFalse, ObservedGeneration: run.Generation,
			Reason: "ExecutionStateLost", Message: lostState,
		})
		meta.SetStatusCondition(&run.Status.Conditions, metav1.Condition{
			Type: actionsv1alpha1.WorkflowRunConditionSucceeded, Status: metav1.ConditionFalse, ObservedGeneration: run.Generation,
			Reason: "ExecutionStateLost", Message: lostState,
		})
		if !apiEquality.Semantic.DeepEqual(before, &run.Status) {
			if err := r.Status().Update(ctx, run); err != nil {
				return ctrl.Result{}, err
			}
			if r.Metrics != nil {
				r.Metrics.WorkflowRunCompleted(before, run)
			}
			recordConditionWarning(r.Recorder, run, before.Conditions, run.Status.Conditions, actionsv1alpha1.WorkflowRunConditionSucceeded)
		}
		return ctrl.Result{}, nil
	}
	plannedMessage := "All WorkflowJobs have been created"
	if planState.active {
		plannedMessage = "The workflow plan is ready"
	}
	meta.SetStatusCondition(&run.Status.Conditions, metav1.Condition{
		Type:               actionsv1alpha1.WorkflowRunConditionPlanned,
		Status:             metav1.ConditionTrue,
		ObservedGeneration: run.Generation,
		Reason:             "JobsPlanned",
		Message:            plannedMessage,
	})
	terminal := len(pendingJobs) == 0 && len(jobs.Items) == expectedObjects && status.Succeeded+status.Failed+status.TimedOut+status.Skipped+status.Cancelled == status.Total
	if terminal && status.Cancelled > 0 {
		active, err := activeRuntimeWorkloads(ctx, reader, run)
		if err != nil {
			return ctrl.Result{}, err
		}
		if active {
			terminal = false
			waitingForRuntimeState = true
		}
	}
	switch {
	case terminal && hasNonFailFastCancellation:
		now := metav1.Now()
		run.Status.CompletionTime = &now
		meta.SetStatusCondition(&run.Status.Conditions, metav1.Condition{Type: actionsv1alpha1.WorkflowRunConditionSucceeded, Status: metav1.ConditionFalse, ObservedGeneration: run.Generation, Reason: "JobCancelled", Message: "At least one WorkflowJob was cancelled"})
	case terminal && (status.Failed > 0 || status.TimedOut > 0) && run.Spec.CancelRequested:
		now := metav1.Now()
		run.Status.CompletionTime = &now
		meta.SetStatusCondition(&run.Status.Conditions, metav1.Condition{Type: actionsv1alpha1.WorkflowRunConditionSucceeded, Status: metav1.ConditionFalse, ObservedGeneration: run.Generation, Reason: "JobCancelled", Message: "Workflow cancellation was requested"})
	case terminal && status.TimedOut > 0:
		now := metav1.Now()
		run.Status.CompletionTime = &now
		meta.SetStatusCondition(&run.Status.Conditions, metav1.Condition{Type: actionsv1alpha1.WorkflowRunConditionSucceeded, Status: metav1.ConditionFalse, ObservedGeneration: run.Generation, Reason: "JobTimedOut", Message: "At least one WorkflowJob timed out"})
	case terminal && status.Failed > 0:
		now := metav1.Now()
		run.Status.CompletionTime = &now
		meta.SetStatusCondition(&run.Status.Conditions, metav1.Condition{Type: actionsv1alpha1.WorkflowRunConditionSucceeded, Status: metav1.ConditionFalse, ObservedGeneration: run.Generation, Reason: "JobFailed", Message: "At least one WorkflowJob failed"})
	case terminal && status.Cancelled > 0:
		now := metav1.Now()
		run.Status.CompletionTime = &now
		meta.SetStatusCondition(&run.Status.Conditions, metav1.Condition{Type: actionsv1alpha1.WorkflowRunConditionSucceeded, Status: metav1.ConditionFalse, ObservedGeneration: run.Generation, Reason: "JobCancelled", Message: "At least one WorkflowJob was cancelled"})
	case terminal:
		now := metav1.Now()
		run.Status.CompletionTime = &now
		meta.SetStatusCondition(&run.Status.Conditions, metav1.Condition{Type: actionsv1alpha1.WorkflowRunConditionSucceeded, Status: metav1.ConditionTrue, ObservedGeneration: run.Generation, Reason: "JobsSucceeded", Message: "All required WorkflowJobs succeeded"})
	case status.Waiting > 0:
		meta.SetStatusCondition(&run.Status.Conditions, metav1.Condition{Type: actionsv1alpha1.WorkflowRunConditionSucceeded, Status: metav1.ConditionUnknown, ObservedGeneration: run.Generation, Reason: "JobsWaiting", Message: "WorkflowJobs are waiting for dependencies"})
	case status.Queued > 0:
		meta.SetStatusCondition(&run.Status.Conditions, metav1.Condition{Type: actionsv1alpha1.WorkflowRunConditionSucceeded, Status: metav1.ConditionUnknown, ObservedGeneration: run.Generation, Reason: "JobsQueued", Message: "WorkflowJobs are waiting for matching Runners"})
	default:
		meta.SetStatusCondition(&run.Status.Conditions, metav1.Condition{Type: actionsv1alpha1.WorkflowRunConditionSucceeded, Status: metav1.ConditionUnknown, ObservedGeneration: run.Generation, Reason: "JobsRunning", Message: "WorkflowJobs are still running"})
	}
	if !apiEquality.Semantic.DeepEqual(before, &run.Status) {
		if err := r.Status().Update(ctx, run); err != nil {
			return ctrl.Result{}, err
		}
		if r.Metrics != nil {
			r.Metrics.WorkflowRunCompleted(before, run)
		}
		if condition := meta.FindStatusCondition(run.Status.Conditions, actionsv1alpha1.WorkflowRunConditionSucceeded); condition != nil && condition.Status == metav1.ConditionFalse {
			recordConditionWarning(r.Recorder, run, before.Conditions, run.Status.Conditions, actionsv1alpha1.WorkflowRunConditionSucceeded)
		}
	}
	if waitingForRuntimeState || failFastPending || concurrencyPending {
		return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
	}
	return ctrl.Result{}, nil
}

func workflowJobGraphNeedsExpressionContext(run *actionsv1alpha1.WorkflowRun, jobs []actionsv1alpha1.WorkflowJob) bool {
	for index := range jobs {
		job := &jobs[index]
		if workflowJobTerminal(job) {
			continue
		}
		needsCondition := strings.TrimSpace(job.Spec.If) != ""
		needsConcurrency := job.Spec.Concurrency != nil && job.Status.Concurrency == nil
		if !needsCondition && !needsConcurrency {
			continue
		}
		if !run.Spec.CancelRequested && (job.Status.RunnerRef != nil || workflowJobReadyCondition(job)) && !needsConcurrency {
			continue
		}
		return true
	}
	return false
}

func (r *WorkflowRunReconciler) workflowJobGraphInputValues(ctx context.Context, jobs []actionsv1alpha1.WorkflowJob) (map[string]any, error) {
	for index := range jobs {
		job := &jobs[index]
		if workflowJobTerminal(job) || strings.TrimSpace(job.Spec.If) == "" && (job.Spec.Concurrency == nil || job.Status.Concurrency != nil) {
			continue
		}
		plan := &corev1.ConfigMap{}
		key := client.ObjectKey{Namespace: job.Namespace, Name: childName(job.Name, "plan")}
		if err := r.APIReader.Get(ctx, key, plan); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return nil, err
		}
		if !metav1.IsControlledBy(plan, job) {
			continue
		}
		var decoded struct {
			Inputs map[string]any `json:"inputs"`
		}
		if err := json.Unmarshal([]byte(plan.Data[jobPlanKey]), &decoded); err != nil {
			return nil, fmt.Errorf("decode plan for WorkflowJob %q: %w", job.Name, err)
		}
		return decoded.Inputs, nil
	}
	return nil, nil
}

func (r *WorkflowRunReconciler) reconcileWorkflowJobGraph(ctx context.Context, run *actionsv1alpha1.WorkflowRun, workflowName string, inputValues map[string]any, variables any, eventPayload map[string]any, jobs []actionsv1alpha1.WorkflowJob, pendingJobSets ...map[string]struct{}) error {
	return r.reconcileWorkflowJobGraphWithDependencies(ctx, run, workflowName, inputValues, variables, eventPayload, jobs, nil, pendingJobSets...)
}

func (r *WorkflowRunReconciler) reconcileWorkflowJobGraphWithDependencies(ctx context.Context, run *actionsv1alpha1.WorkflowRun, workflowName string, inputValues map[string]any, variables any, eventPayload map[string]any, jobs, dependencyJobs []actionsv1alpha1.WorkflowJob, pendingJobSets ...map[string]struct{}) error {
	pendingJobs := map[string]struct{}{}
	if len(pendingJobSets) > 0 {
		pendingJobs = pendingJobSets[0]
	}
	jobsByID := make(map[string]*actionsv1alpha1.WorkflowJob, len(jobs)+len(dependencyJobs))
	for index := range dependencyJobs {
		job := &dependencyJobs[index]
		if existing := jobsByID[job.Spec.JobID]; existing != nil {
			return fmt.Errorf("WorkflowJobs %q and %q both represent inherited job %q in WorkflowRun %q", existing.Name, job.Name, job.Spec.JobID, run.Name)
		}
		jobsByID[job.Spec.JobID] = job
	}
	currentByID := make(map[string]*actionsv1alpha1.WorkflowJob, len(jobs))
	for index := range jobs {
		job := &jobs[index]
		if existing := currentByID[job.Spec.JobID]; existing != nil {
			return fmt.Errorf("WorkflowJobs %q and %q both represent job %q in WorkflowRun %q", existing.Name, job.Name, job.Spec.JobID, run.Name)
		}
		currentByID[job.Spec.JobID] = job
		jobsByID[job.Spec.JobID] = job
	}
	jobsByLogicalID := make(map[string][]*actionsv1alpha1.WorkflowJob, len(jobsByID))
	for _, job := range jobsByID {
		logicalID := job.Spec.JobID
		if job.Spec.Matrix != nil {
			logicalID = job.Spec.Matrix.LogicalJobID
		}
		jobsByLogicalID[logicalID] = append(jobsByLogicalID[logicalID], job)
	}

	for index := range jobs {
		job := &jobs[index]
		if workflowJobTerminal(job) {
			continue
		}
		if job.Status.RunnerRef != nil {
			if run.Spec.CancelRequested {
				if err := r.reconcileAssignedWorkflowJobCancellation(ctx, run, workflowName, inputValues, variables, eventPayload, job, jobsByLogicalID); err != nil {
					return err
				}
			}
			continue
		}
		if workflowJobReadyCondition(job) && !run.Spec.CancelRequested {
			continue
		}
		dependenciesReady := true
		for _, dependency := range job.Spec.Needs {
			needed := jobsByLogicalID[dependency]
			if len(needed) == 0 {
				if _, pending := pendingJobs[dependency]; pending {
					dependenciesReady = false
					continue
				}
				return fmt.Errorf("WorkflowJob %q needs missing job %q in WorkflowRun %q", job.Name, dependency, run.Name)
			}
			for _, dependencyJob := range needed {
				if !workflowJobTerminal(dependencyJob) {
					dependenciesReady = false
				}
			}
		}
		if !dependenciesReady {
			if err := r.setWorkflowJobWaiting(ctx, job); err != nil {
				return err
			}
			continue
		}

		needsContext := workflowNeedsContext(job, jobsByLogicalID)
		expressionContext := workflowexpression.Context{Status: workflowJobAncestorStatus(job, jobsByLogicalID, run.Spec.CancelRequested)}
		runnable := true
		if !workflowJobConcurrencyRegistered(job) || run.Spec.CancelRequested {
			if strings.TrimSpace(job.Spec.If) != "" {
				expressionContext = r.jobExpressionContext(run, workflowName, inputValues, variables, eventPayload)
				expressionContext.Values["needs"] = needsContext.ExpressionValues()
				expressionContext.Status = workflowJobAncestorStatus(job, jobsByLogicalID, run.Spec.CancelRequested)
			}
			var err error
			runnable, err = workflow.EvaluateJobCondition(job.Spec.JobID, job.Spec.If, expressionContext)
			if err != nil {
				var unavailable *projectValuesUnavailableError
				if errors.As(err, &unavailable) {
					return err
				}
				if statusErr := r.completeUnscheduledWorkflowJob(ctx, job, actionsv1alpha1.WorkflowJobResultFailure, "ConditionEvaluationFailed", err.Error()); statusErr != nil {
					return statusErr
				}
				continue
			}
		}
		if !runnable {
			result := actionsv1alpha1.WorkflowJobResultSkipped
			reason := "ConditionFalse"
			message := "The workflow job condition evaluated to false"
			if run.Spec.CancelRequested {
				result = actionsv1alpha1.WorkflowJobResultCancelled
				reason = "CancellationRequested"
				message = "The workflow job was cancelled before Runner assignment"
			}
			if err := r.completeUnscheduledWorkflowJob(ctx, job, result, reason, message); err != nil {
				return err
			}
			continue
		}
		if len(job.Spec.Needs) > 0 {
			if err := r.ensureNeedsContextConfigMap(ctx, job, needsContext); err != nil {
				terminal := &terminalNeedsContextError{}
				if !errors.As(err, &terminal) {
					return err
				}
				if statusErr := r.completeUnscheduledWorkflowJob(ctx, job, actionsv1alpha1.WorkflowJobResultFailure, "PlanUnavailable", err.Error()); statusErr != nil {
					return statusErr
				}
				continue
			}
		}
		if job.Spec.Concurrency != nil {
			if job.Status.Concurrency == nil {
				expressionContext = r.jobExpressionContext(run, workflowName, inputValues, variables, eventPayload)
				expressionContext.Values["needs"] = needsContext.ExpressionValues()
				if job.Spec.Matrix != nil {
					matrix, err := r.workflowJobMatrixExpressionContext(ctx, job)
					if err != nil {
						return err
					}
					expressionContext.Values["matrix"] = matrix
					expressionContext.Values["strategy"] = workflowJobStrategyContext(job.Spec.Matrix)
				}
				group, cancelInProgress, err := workflow.EvaluateJobConcurrency(job.Spec.JobID, workflow.Concurrency{
					Group:            job.Spec.Concurrency.Group,
					CancelInProgress: workflowJobCancellationExpression(job.Spec.Concurrency.CancelInProgress),
				}, expressionContext)
				if err != nil {
					var unavailable *projectValuesUnavailableError
					if errors.As(err, &unavailable) {
						return err
					}
					if statusErr := r.completeUnscheduledWorkflowJob(ctx, job, actionsv1alpha1.WorkflowJobResultFailure, "ConcurrencyEvaluationFailed", err.Error()); statusErr != nil {
						return statusErr
					}
					continue
				}
				if err := r.persistWorkflowJobConcurrencyDecision(ctx, job, group, cancelInProgress); err != nil {
					return err
				}
			}
			group := job.Status.Concurrency.Group
			if runConcurrency := workflowRunConcurrencyDecision(run); runConcurrency != nil && strings.EqualFold(group, runConcurrency.Group) {
				message := fmt.Sprintf("WorkflowJob %q concurrency group %q conflicts with its WorkflowRun %q concurrency group", job.Name, group, run.Name)
				if err := r.completeUnscheduledWorkflowJob(ctx, job, actionsv1alpha1.WorkflowJobResultFailure, "ConcurrencyEvaluationFailed", message); err != nil {
					return err
				}
				continue
			}
			scope, err := workflowRunConcurrencyScope(run)
			if err != nil {
				return err
			}
			member := workflowJobConcurrencyMember(job)
			gate, err := r.acquireConcurrency(ctx, job.Namespace, scope, group, member, workflowJobConcurrencyRegistered(job))
			if err != nil {
				return err
			}
			if gate.displaced != nil {
				if err := r.requestConcurrencyCancellation(ctx, job.Namespace, *gate.displaced, true); err != nil {
					return err
				}
			}
			if gate.cancelOwner != nil {
				if err := r.requestConcurrencyCancellation(ctx, job.Namespace, *gate.cancelOwner, false); err != nil {
					return err
				}
			}
			switch gate.state {
			case concurrencyGateSuperseded:
				if err := r.completeUnscheduledWorkflowJob(ctx, job, actionsv1alpha1.WorkflowJobResultCancelled, concurrencySupersededReason, "A newer pending member replaced this workflow job"); err != nil {
					return err
				}
				continue
			case concurrencyGateWaiting:
				if err := r.setWorkflowJobConcurrencyState(ctx, job, false); err != nil {
					return err
				}
				continue
			case concurrencyGateAcquired:
				if err := r.setWorkflowJobConcurrencyState(ctx, job, true); err != nil {
					return err
				}
				continue
			}
		}
		if err := r.setWorkflowJobReady(ctx, job); err != nil {
			return err
		}
	}
	return nil
}

func (r *WorkflowRunReconciler) workflowJobMatrixExpressionContext(ctx context.Context, job *actionsv1alpha1.WorkflowJob) (map[string]any, error) {
	plan := &corev1.ConfigMap{}
	key := client.ObjectKey{Namespace: job.Namespace, Name: childName(job.Name, "plan")}
	if err := r.APIReader.Get(ctx, key, plan); err != nil {
		return nil, err
	}
	if !metav1.IsControlledBy(plan, job) {
		return nil, fmt.Errorf("job plan ConfigMap %q is not controlled by WorkflowJob %q", plan.Name, job.Name)
	}
	var decoded struct {
		Matrix map[string]any `json:"matrix"`
	}
	if err := json.Unmarshal([]byte(plan.Data[jobPlanKey]), &decoded); err != nil {
		return nil, fmt.Errorf("decode plan for WorkflowJob %q: %w", job.Name, err)
	}
	return decoded.Matrix, nil
}

func workflowJobStrategyContext(matrix *actionsv1alpha1.WorkflowJobMatrix) map[string]any {
	return workflowcontext.Strategy(matrix.JobIndex, matrix.JobTotal, matrix.MaxParallel, matrix.FailFast == nil || *matrix.FailFast)
}

func (r *WorkflowRunReconciler) reconcileAssignedWorkflowJobCancellation(ctx context.Context, run *actionsv1alpha1.WorkflowRun, workflowName string, inputValues map[string]any, variables any, eventPayload map[string]any, job *actionsv1alpha1.WorkflowJob, jobs map[string][]*actionsv1alpha1.WorkflowJob) error {
	expressionContext := workflowexpression.Context{Status: workflowJobAncestorStatus(job, jobs, true)}
	if strings.TrimSpace(job.Spec.If) != "" {
		expressionContext = r.jobExpressionContext(run, workflowName, inputValues, variables, eventPayload)
		expressionContext.Values["needs"] = workflowNeedsContext(job, jobs).ExpressionValues()
		expressionContext.Status = workflowJobAncestorStatus(job, jobs, true)
	}
	continueRunning, err := workflow.EvaluateJobCondition(job.Spec.JobID, job.Spec.If, expressionContext)
	if err != nil {
		var unavailable *projectValuesUnavailableError
		if errors.As(err, &unavailable) {
			return err
		}
		return r.setWorkflowJobCancellationRequested(ctx, job, true, "ConditionEvaluationFailed", err.Error())
	}
	if continueRunning {
		return r.setWorkflowJobCancellationRequested(ctx, job, false, "ConditionPassed", "The workflow job condition permits execution during cancellation")
	}
	return r.setWorkflowJobCancellationRequested(ctx, job, true, "CancellationRequested", "The workflow job condition does not permit execution during cancellation")
}

func workflowNeedsContext(job *actionsv1alpha1.WorkflowJob, jobs map[string][]*actionsv1alpha1.WorkflowJob) runner.Needs {
	needs := make(runner.Needs, len(job.Spec.Needs))
	for _, dependency := range job.Spec.Needs {
		dependencyJobs := append([]*actionsv1alpha1.WorkflowJob(nil), jobs[dependency]...)
		sort.Slice(dependencyJobs, func(left, right int) bool {
			return dependencyJobs[left].Spec.JobID < dependencyJobs[right].Spec.JobID
		})
		outputs := map[string]string{}
		for _, dependencyJob := range dependencyJobs {
			for name, value := range dependencyJob.Status.Outputs {
				outputs[name] = value
			}
		}
		needs[dependency] = runner.Need{Result: string(workflowJobGroupResult(dependencyJobs)), Outputs: outputs}
	}
	return needs
}

func workflowJobGroupResult(jobs []*actionsv1alpha1.WorkflowJob) actionsv1alpha1.WorkflowJobResult {
	result := actionsv1alpha1.WorkflowJobResultSuccess
	for _, job := range jobs {
		switch workflowJobResult(job) {
		case actionsv1alpha1.WorkflowJobResultFailure:
			return actionsv1alpha1.WorkflowJobResultFailure
		case actionsv1alpha1.WorkflowJobResultCancelled:
			result = actionsv1alpha1.WorkflowJobResultCancelled
		case actionsv1alpha1.WorkflowJobResultSkipped:
			if result == actionsv1alpha1.WorkflowJobResultSuccess {
				result = actionsv1alpha1.WorkflowJobResultSkipped
			}
		case actionsv1alpha1.WorkflowJobResultSuccess:
		default:
			return ""
		}
	}
	return result
}

func workflowJobAncestorStatus(job *actionsv1alpha1.WorkflowJob, jobs map[string][]*actionsv1alpha1.WorkflowJob, cancellationRequested bool) *workflowexpression.Status {
	status := &workflowexpression.Status{Success: !cancellationRequested, Cancelled: cancellationRequested}
	visited := map[string]bool{}
	var visit func(string)
	visit = func(id string) {
		if visited[id] {
			return
		}
		visited[id] = true
		dependencies := jobs[id]
		if len(dependencies) == 0 {
			return
		}
		for _, dependency := range dependencies {
			switch workflowJobResult(dependency) {
			case actionsv1alpha1.WorkflowJobResultFailure:
				status.Success = false
				status.Failure = true
			case actionsv1alpha1.WorkflowJobResultCancelled:
				status.Success = false
				status.Cancelled = true
			case actionsv1alpha1.WorkflowJobResultSkipped:
				status.Success = false
			}
			for _, ancestor := range dependency.Spec.Needs {
				visit(ancestor)
			}
		}
	}
	for _, dependency := range job.Spec.Needs {
		visit(dependency)
	}
	return status
}

func (r *WorkflowRunReconciler) setWorkflowJobCancellationRequested(ctx context.Context, job *actionsv1alpha1.WorkflowJob, requested bool, reason, message string) error {
	before := job.Status.DeepCopy()
	job.Status.ObservedGeneration = job.Generation
	conditionStatus := metav1.ConditionFalse
	if requested {
		conditionStatus = metav1.ConditionTrue
	}
	meta.SetStatusCondition(&job.Status.Conditions, metav1.Condition{
		Type: actionsv1alpha1.WorkflowJobConditionCancellationRequested, Status: conditionStatus,
		ObservedGeneration: job.Generation, Reason: reason, Message: message,
	})
	if apiEquality.Semantic.DeepEqual(before, &job.Status) {
		return nil
	}
	return r.Status().Update(ctx, job)
}

func (r *WorkflowRunReconciler) setWorkflowJobWaiting(ctx context.Context, job *actionsv1alpha1.WorkflowJob) error {
	before := job.Status.DeepCopy()
	job.Status.ObservedGeneration = job.Generation
	meta.SetStatusCondition(&job.Status.Conditions, metav1.Condition{
		Type: actionsv1alpha1.WorkflowJobConditionReady, Status: metav1.ConditionUnknown,
		ObservedGeneration: job.Generation, Reason: "DependenciesPending", Message: "Required WorkflowJobs have not reached terminal results",
	})
	if apiEquality.Semantic.DeepEqual(before, &job.Status) {
		return nil
	}
	return r.Status().Update(ctx, job)
}

func (r *WorkflowRunReconciler) setWorkflowJobReady(ctx context.Context, job *actionsv1alpha1.WorkflowJob) error {
	before := job.Status.DeepCopy()
	job.Status.ObservedGeneration = job.Generation
	meta.SetStatusCondition(&job.Status.Conditions, metav1.Condition{
		Type: actionsv1alpha1.WorkflowJobConditionReady, Status: metav1.ConditionTrue,
		ObservedGeneration: job.Generation, Reason: "ConditionPassed", Message: "The workflow job is ready for Runner assignment",
	})
	if apiEquality.Semantic.DeepEqual(before, &job.Status) {
		return nil
	}
	return r.Status().Update(ctx, job)
}

func (r *WorkflowRunReconciler) persistWorkflowJobConcurrencyDecision(ctx context.Context, job *actionsv1alpha1.WorkflowJob, group string, cancelInProgress bool) error {
	before := job.Status.DeepCopy()
	job.Status.ObservedGeneration = job.Generation
	job.Status.Concurrency = &actionsv1alpha1.ConcurrencyStatus{Group: group, CancelInProgress: cancelInProgress}
	if apiEquality.Semantic.DeepEqual(before, &job.Status) {
		return nil
	}
	if err := r.Status().Update(ctx, job); err != nil {
		return fmt.Errorf("persist concurrency decision for WorkflowJob %q: %w", job.Name, err)
	}
	return nil
}

func (r *WorkflowRunReconciler) setWorkflowJobConcurrencyState(ctx context.Context, job *actionsv1alpha1.WorkflowJob, acquired bool) error {
	before := job.Status.DeepCopy()
	if job.Status.Concurrency == nil {
		return fmt.Errorf("WorkflowJob %q has no evaluated concurrency policy", job.Name)
	}
	job.Status.ObservedGeneration = job.Generation
	status := metav1.ConditionUnknown
	reason := concurrencyWaitingReason
	message := "The workflow job is waiting for the concurrency group"
	if acquired {
		status = metav1.ConditionTrue
		reason = concurrencyAcquiredReason
		message = "The workflow job owns the concurrency group"
	}
	meta.SetStatusCondition(&job.Status.Conditions, metav1.Condition{
		Type: actionsv1alpha1.WorkflowJobConditionConcurrencyAcquired, Status: status,
		ObservedGeneration: job.Generation, Reason: reason, Message: message,
	})
	meta.SetStatusCondition(&job.Status.Conditions, metav1.Condition{
		Type: actionsv1alpha1.WorkflowJobConditionReady, Status: status,
		ObservedGeneration: job.Generation, Reason: reason, Message: message,
	})
	if apiEquality.Semantic.DeepEqual(before, &job.Status) {
		return nil
	}
	return r.Status().Update(ctx, job)
}

func (r *WorkflowRunReconciler) completeUnscheduledWorkflowJob(ctx context.Context, job *actionsv1alpha1.WorkflowJob, result actionsv1alpha1.WorkflowJobResult, reason, message string) error {
	before := job.Status.DeepCopy()
	now := metav1.Now()
	job.Status.ObservedGeneration = job.Generation
	job.Status.Result = result
	job.Status.CompletionTime = &now
	meta.SetStatusCondition(&job.Status.Conditions, metav1.Condition{
		Type: actionsv1alpha1.WorkflowJobConditionReady, Status: metav1.ConditionFalse,
		ObservedGeneration: job.Generation, Reason: reason, Message: message,
	})
	if reason == concurrencySupersededReason {
		meta.SetStatusCondition(&job.Status.Conditions, metav1.Condition{
			Type: actionsv1alpha1.WorkflowJobConditionConcurrencyAcquired, Status: metav1.ConditionFalse,
			ObservedGeneration: job.Generation, Reason: reason, Message: message,
		})
	}
	meta.SetStatusCondition(&job.Status.Conditions, metav1.Condition{
		Type: actionsv1alpha1.WorkflowJobConditionScheduled, Status: metav1.ConditionFalse,
		ObservedGeneration: job.Generation, Reason: reason, Message: "The workflow job completed without Runner assignment",
	})
	if result == actionsv1alpha1.WorkflowJobResultFailure {
		meta.SetStatusCondition(&job.Status.Conditions, metav1.Condition{
			Type: actionsv1alpha1.WorkflowJobConditionSucceeded, Status: metav1.ConditionFalse,
			ObservedGeneration: job.Generation, Reason: reason, Message: message,
		})
	}
	if apiEquality.Semantic.DeepEqual(before, &job.Status) {
		return nil
	}
	if err := r.Status().Update(ctx, job); err != nil {
		return err
	}
	if r.Metrics != nil {
		r.Metrics.WorkflowJobUpdated(before, job)
	}
	if result == actionsv1alpha1.WorkflowJobResultFailure {
		recordConditionWarning(r.Recorder, job, before.Conditions, job.Status.Conditions, actionsv1alpha1.WorkflowJobConditionSucceeded)
	}
	return nil
}

func workflowJobReady(job *actionsv1alpha1.WorkflowJob) bool {
	if workflowJobReadyCondition(job) {
		return true
	}
	return job.Spec.Concurrency == nil && len(job.Spec.Needs) == 0 && strings.TrimSpace(job.Spec.If) == ""
}

func workflowJobReadyCondition(job *actionsv1alpha1.WorkflowJob) bool {
	condition := meta.FindStatusCondition(job.Status.Conditions, actionsv1alpha1.WorkflowJobConditionReady)
	return condition != nil && condition.Status == metav1.ConditionTrue
}

func workflowJobResult(job *actionsv1alpha1.WorkflowJob) actionsv1alpha1.WorkflowJobResult {
	if job == nil {
		return ""
	}
	if job.Status.Result != "" {
		return job.Status.Result
	}
	condition := meta.FindStatusCondition(job.Status.Conditions, actionsv1alpha1.WorkflowJobConditionSucceeded)
	if condition == nil {
		return ""
	}
	switch condition.Status {
	case metav1.ConditionTrue:
		return actionsv1alpha1.WorkflowJobResultSuccess
	case metav1.ConditionFalse:
		return actionsv1alpha1.WorkflowJobResultFailure
	default:
		return ""
	}
}

func workflowJobTerminal(job *actionsv1alpha1.WorkflowJob) bool {
	return workflowJobResult(job) != ""
}

func workflowJobCancelledByMatrixFailFast(job *actionsv1alpha1.WorkflowJob) bool {
	if workflowJobResult(job) != actionsv1alpha1.WorkflowJobResultCancelled {
		return false
	}
	for _, condition := range job.Status.Conditions {
		if condition.Reason == matrixFailFastReason {
			return true
		}
	}
	return false
}

func workflowJobTimedOut(job *actionsv1alpha1.WorkflowJob) bool {
	condition := meta.FindStatusCondition(job.Status.Conditions, actionsv1alpha1.WorkflowJobConditionSucceeded)
	return condition != nil && condition.Status == metav1.ConditionFalse && condition.Reason == "JobTimedOut"
}

func (r *WorkflowRunReconciler) reconcileMatrixFailFast(ctx context.Context, jobs *actionsv1alpha1.WorkflowJobList) (bool, error) {
	failedGroups := map[string]struct{}{}
	for index := range jobs.Items {
		job := &jobs.Items[index]
		if workflowJobFailureTriggersMatrixFailFast(job) {
			failedGroups[matrixJobGroup(job)] = struct{}{}
		}
	}
	if len(failedGroups) == 0 {
		return false, nil
	}

	pending := false
	for index := range jobs.Items {
		job := &jobs.Items[index]
		if job.Spec.Matrix == nil || workflowJobTerminal(job) {
			continue
		}
		if _, found := failedGroups[matrixJobGroup(job)]; !found {
			continue
		}

		if job.Status.RunnerRef == nil {
			if err := r.completeUnscheduledWorkflowJob(ctx, job, actionsv1alpha1.WorkflowJobResultCancelled, matrixFailFastReason, matrixFailFastMessage); err != nil {
				return false, err
			}
		} else {
			if err := r.setWorkflowJobCancellationRequested(ctx, job, true, matrixFailFastReason, matrixFailFastMessage); err != nil {
				return false, err
			}
		}
		pending = true
	}
	return pending, nil
}

func workflowJobRuntimeStateExists(ctx context.Context, reader client.Reader, workflowJob *actionsv1alpha1.WorkflowJob) (bool, error) {
	nativeJob := &batchv1.Job{}
	key := client.ObjectKey{Namespace: workflowJob.Namespace, Name: workflowJob.Name}
	if err := reader.Get(ctx, key, nativeJob); err != nil {
		if !apierrors.IsNotFound(err) {
			return false, err
		}
	} else {
		if !metav1.IsControlledBy(nativeJob, workflowJob) {
			return false, fmt.Errorf("native Job %q is not controlled by WorkflowJob %q", nativeJob.Name, workflowJob.Name)
		}
		return true, nil
	}
	pods := &corev1.PodList{}
	selector := client.MatchingLabels{actionsv1alpha1.LabelWorkflowJobUID: string(workflowJob.UID)}
	if err := reader.List(ctx, pods, client.InNamespace(workflowJob.Namespace), selector); err != nil {
		return false, err
	}
	for index := range pods.Items {
		if podActive(&pods.Items[index]) {
			return true, nil
		}
	}
	return false, nil
}

func activeRuntimeWorkloads(ctx context.Context, reader client.Reader, run *actionsv1alpha1.WorkflowRun) (bool, error) {
	selector := client.MatchingLabels{actionsv1alpha1.LabelWorkflowRunUID: string(run.UID)}
	nativeJobs := &batchv1.JobList{}
	if err := reader.List(ctx, nativeJobs, client.InNamespace(run.Namespace), selector); err != nil {
		return false, err
	}
	for index := range nativeJobs.Items {
		if !jobTerminal(&nativeJobs.Items[index]) {
			return true, nil
		}
	}
	pods := &corev1.PodList{}
	if err := reader.List(ctx, pods, client.InNamespace(run.Namespace), selector); err != nil {
		return false, err
	}
	for index := range pods.Items {
		if podActive(&pods.Items[index]) {
			return true, nil
		}
	}
	return false, nil
}

func podActive(pod *corev1.Pod) bool {
	return pod.Status.Phase != corev1.PodSucceeded && pod.Status.Phase != corev1.PodFailed
}

func (r *WorkflowRunReconciler) handleConcurrency(ctx context.Context, run *actionsv1alpha1.WorkflowRun) (bool, bool, error) {
	decision := workflowRunConcurrencyDecision(run)
	if decision == nil {
		return false, false, nil
	}
	group := decision.Group
	planning, err := r.olderWorkflowRunPlanning(ctx, run)
	if err != nil {
		return false, false, err
	}
	if planning {
		return true, true, nil
	}
	scope, err := workflowRunConcurrencyScope(run)
	if err != nil {
		return false, false, err
	}
	planned := meta.FindStatusCondition(run.Status.Conditions, actionsv1alpha1.WorkflowRunConditionPlanned)
	registered := waitingForConcurrencyCondition(planned) || planned != nil && planned.Status == metav1.ConditionTrue
	result, err := r.acquireConcurrency(ctx, run.Namespace, scope, group, workflowRunConcurrencyMember(run), registered)
	if err != nil {
		return false, false, err
	}
	if result.displaced != nil {
		if err := r.requestConcurrencyCancellation(ctx, run.Namespace, *result.displaced, true); err != nil {
			return false, false, err
		}
	}
	if result.cancelOwner != nil {
		if err := r.requestConcurrencyCancellation(ctx, run.Namespace, *result.cancelOwner, false); err != nil {
			return false, false, err
		}
	}
	if result.state == concurrencyGateSuperseded {
		if err := r.cancelWorkflowRun(ctx, run); err != nil {
			return false, false, err
		}
		return true, false, nil
	}
	return result.state == concurrencyGateWaiting, false, nil
}

func (r *WorkflowRunReconciler) cancelWorkflowRun(ctx context.Context, run *actionsv1alpha1.WorkflowRun) error {
	if run.Spec.CancelRequested {
		return nil
	}
	before := run.DeepCopy()
	run.Spec.CancelRequested = true
	return r.Patch(ctx, run, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{}))
}

func (r *WorkflowRunReconciler) finalizeCanceledWorkflowRun(ctx context.Context, run *actionsv1alpha1.WorkflowRun) (ctrl.Result, error) {
	if controllerutil.ContainsFinalizer(run, eventsnapshot.RerunProtectionFinalizer) {
		return r.finalizeRerunEventSnapshotProtection(ctx, run)
	}
	cancellationFinalizer := controllerutil.ContainsFinalizer(run, workflowRunCancellationFinalizer)
	statusFinalizer := controllerutil.ContainsFinalizer(run, workflowRunGitHubStatusFinalizer)
	scheduleFinalizer := controllerutil.ContainsFinalizer(run, workflowRunScheduleFinalizer)
	if !cancellationFinalizer && !statusFinalizer && !scheduleFinalizer {
		return ctrl.Result{}, nil
	}
	var reportError error
	if statusFinalizer {
		reportError = errors.Join(r.reconcileGitHubStatus(ctx, run), r.reconcileGitHubJobStatuses(ctx, run))
	}
	if statusFinalizer && reportError == nil {
		reportError = r.releaseGitHubStatusOwnershipIfUnused(ctx, run)
	}
	if cancellationFinalizer {
		remaining, err := r.executionWorkloadsRemain(ctx, run)
		if err != nil {
			return ctrl.Result{}, err
		}
		if remaining {
			if reportError != nil {
				ctrl.LoggerFrom(ctx).Error(reportError, "GitHub reporting failed while workflow cleanup is pending")
			}
			return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
		}
	}
	before := run.DeepCopy()
	controllerutil.RemoveFinalizer(run, workflowRunCancellationFinalizer)
	scheduleRemaining := time.Duration(0)
	if scheduleFinalizer {
		scheduleRemaining = r.scheduleFinalizerRemaining(run)
		if scheduleRemaining <= 0 {
			controllerutil.RemoveFinalizer(run, workflowRunScheduleFinalizer)
		}
	}
	retryReport := false
	if statusFinalizer {
		switch {
		case reportError == nil:
			controllerutil.RemoveFinalizer(run, workflowRunGitHubStatusFinalizer)
		case r.githubReportPermanentlyUnavailable(ctx, run, reportError):
			if err := r.releaseGitHubStatusOwnershipIfUnused(ctx, run); err != nil {
				reportError = errors.Join(reportError, fmt.Errorf("release GitHub status ownership: %w", err))
				retryReport = true
				break
			}
			ctrl.LoggerFrom(ctx).Info("Skipping terminal GitHub report because reporting is unavailable", "error", reportError)
			controllerutil.RemoveFinalizer(run, workflowRunGitHubStatusFinalizer)
		default:
			retryReport = true
		}
	}
	finalizersChanged := cancellationFinalizer || (scheduleFinalizer && scheduleRemaining <= 0) || (statusFinalizer && !retryReport)
	if finalizersChanged {
		if err := r.Patch(ctx, run, client.MergeFrom(before)); err != nil {
			return ctrl.Result{}, err
		}
	}
	if retryReport {
		return ctrl.Result{}, reportError
	}
	if scheduleRemaining > 0 {
		return ctrl.Result{RequeueAfter: scheduleRemaining}, nil
	}
	return ctrl.Result{}, nil
}

func (r *WorkflowRunReconciler) finalizeRerunEventSnapshotProtection(ctx context.Context, root *actionsv1alpha1.WorkflowRun) (ctrl.Result, error) {
	targetName := root.Annotations[eventsnapshot.RerunTargetAnnotation]
	deadline, deadlineErr := time.Parse(time.RFC3339Nano, root.Annotations[eventsnapshot.RerunDeadlineAnnotation])
	if targetName == "" || deadlineErr != nil {
		if err := r.clearRerunEventSnapshotProtection(ctx, root); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}
	target := &actionsv1alpha1.WorkflowRun{}
	if err := r.APIReader.Get(ctx, client.ObjectKey{Namespace: root.Namespace, Name: targetName}, target); err != nil {
		if apierrors.IsNotFound(err) {
			now := r.now()
			if !now.Before(deadline) {
				if err := r.clearRerunEventSnapshotProtection(ctx, root); err != nil {
					return ctrl.Result{}, err
				}
				return ctrl.Result{Requeue: true}, nil
			}
			return ctrl.Result{RequeueAfter: min(2*time.Second, deadline.Sub(now))}, nil
		}
		return ctrl.Result{}, err
	}
	rerun := target.Spec.Rerun
	if rerun == nil || rerun.OriginalRunRef.Name != root.Name || rerun.OriginalRunRef.UID != root.UID || target.Annotations[eventsnapshot.Annotation] != root.Annotations[eventsnapshot.Annotation] || !target.DeletionTimestamp.IsZero() {
		if err := r.clearRerunEventSnapshotProtection(ctx, root); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}
	if _, err := r.githubEventSnapshot(ctx, target); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{Requeue: true}, nil
}

func (r *WorkflowRunReconciler) scheduleFinalizerRemaining(run *actionsv1alpha1.WorkflowRun) time.Duration {
	if run.CreationTimestamp.IsZero() {
		return 0
	}
	deadline := run.CreationTimestamp.UTC().Truncate(time.Minute).Add(time.Minute)
	return deadline.Sub(r.now().UTC())
}

func (r *WorkflowRunReconciler) githubReportPermanentlyUnavailable(ctx context.Context, run *actionsv1alpha1.WorkflowRun, reportError error) bool {
	if _, limited := githubclient.RetryDelay(reportError, r.now()); limited {
		return false
	}
	if errors.Is(reportError, errGitHubProjectIdentityMismatch) {
		return true
	}
	apiError := &githubclient.APIError{}
	if errors.As(reportError, &apiError) && apiError.StatusCode >= 400 && apiError.StatusCode < 500 && apiError.StatusCode != 408 && apiError.StatusCode != 409 && apiError.StatusCode != 429 {
		return true
	}
	project := &actionsv1alpha1.Project{}
	projectKey := client.ObjectKey{Namespace: run.Namespace, Name: run.Spec.ProjectRef.Name}
	if err := r.APIReader.Get(ctx, projectKey, project); err != nil {
		return apierrors.IsNotFound(err)
	}
	if project.Spec.Source.GitHub == nil {
		return true
	}
	selector := project.Spec.Source.GitHub.PrivateKeySecretRef
	if selector.Name == "" || selector.Key == "" {
		return true
	}
	secret := &corev1.Secret{}
	if err := r.APIReader.Get(ctx, client.ObjectKey{Namespace: project.Namespace, Name: selector.Name}, secret); err != nil {
		return apierrors.IsNotFound(err)
	}
	privateKey := secret.Data[selector.Key]
	return len(privateKey) == 0 || githubclient.ValidatePrivateKey(privateKey) != nil
}

func (r *WorkflowRunReconciler) executionWorkloadsRemain(ctx context.Context, run *actionsv1alpha1.WorkflowRun) (bool, error) {
	reader := r.APIReader
	selector := client.MatchingLabels{actionsv1alpha1.LabelWorkflowRunUID: string(run.UID)}
	workflowJobs := &actionsv1alpha1.WorkflowJobList{}
	if err := reader.List(ctx, workflowJobs, client.InNamespace(run.Namespace), selector); err != nil {
		return false, err
	}
	if len(workflowJobs.Items) > 0 {
		return true, nil
	}
	nativeJobs := &batchv1.JobList{}
	if err := reader.List(ctx, nativeJobs, client.InNamespace(run.Namespace), selector); err != nil {
		return false, err
	}
	if len(nativeJobs.Items) > 0 {
		return true, nil
	}
	pods := &corev1.PodList{}
	if err := reader.List(ctx, pods, client.InNamespace(run.Namespace), selector); err != nil {
		return false, err
	}
	return len(pods.Items) > 0, nil
}

func (r *WorkflowRunReconciler) waitingForConcurrency(ctx context.Context, run *actionsv1alpha1.WorkflowRun, workflowName string, total int32, waitingForPlanning bool) (ctrl.Result, error) {
	before := run.Status.DeepCopy()
	run.Status.ObservedGeneration = run.Generation
	run.Status.WorkflowName = workflowName
	run.Status.Jobs = &actionsv1alpha1.WorkflowRunJobStatus{Total: total, Queued: total}
	reason := "WaitingForConcurrency"
	message := "An earlier WorkflowRun still owns the concurrency group"
	if waitingForPlanning {
		reason = workflowRunPlanningWaitReason
		message = "An earlier WorkflowRun is still planning"
	} else if concurrency := workflowRunConcurrencyDecision(run); concurrency != nil && concurrency.CancelInProgress {
		reason = "WaitingForConcurrencyCancellation"
	}
	meta.SetStatusCondition(&run.Status.Conditions, metav1.Condition{
		Type:               actionsv1alpha1.WorkflowRunConditionPlanned,
		Status:             metav1.ConditionUnknown,
		ObservedGeneration: run.Generation,
		Reason:             reason,
		Message:            message,
	})
	if !apiEquality.Semantic.DeepEqual(before, &run.Status) {
		if err := r.Status().Update(ctx, run); err != nil {
			return ctrl.Result{}, err
		}
	}
	return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
}

func waitingForConcurrencyCondition(condition *metav1.Condition) bool {
	return condition != nil && condition.Status == metav1.ConditionUnknown &&
		(condition.Reason == "WaitingForConcurrency" || condition.Reason == "WaitingForConcurrencyCancellation")
}

func workflowRunConcurrencyWaitCondition(condition *metav1.Condition) bool {
	if waitingForConcurrencyCondition(condition) {
		return true
	}
	return condition != nil && condition.Status == metav1.ConditionUnknown && condition.Reason == workflowRunPlanningWaitReason
}

func (r *WorkflowRunReconciler) planningFailed(ctx context.Context, run *actionsv1alpha1.WorkflowRun, reason string, cause error, disposition planningFailureDisposition) (ctrl.Result, error) {
	before := run.Status.DeepCopy()
	run.Status.ObservedGeneration = run.Generation
	status := metav1.ConditionUnknown
	if disposition == planningFailureTerminal {
		status = metav1.ConditionFalse
		now := metav1.Now()
		run.Status.CompletionTime = &now
		meta.SetStatusCondition(&run.Status.Conditions, metav1.Condition{
			Type:               actionsv1alpha1.WorkflowRunConditionSucceeded,
			Status:             metav1.ConditionFalse,
			ObservedGeneration: run.Generation,
			Reason:             reason,
			Message:            cause.Error(),
		})
	}
	meta.SetStatusCondition(&run.Status.Conditions, metav1.Condition{
		Type:               actionsv1alpha1.WorkflowRunConditionPlanned,
		Status:             status,
		ObservedGeneration: run.Generation,
		Reason:             reason,
		Message:            cause.Error(),
	})
	if !apiEquality.Semantic.DeepEqual(before, &run.Status) {
		if err := r.Status().Update(ctx, run); err != nil {
			return ctrl.Result{}, err
		}
		if r.Metrics != nil {
			r.Metrics.WorkflowRunCompleted(before, run)
		}
		recordConditionWarning(r.Recorder, run, before.Conditions, run.Status.Conditions, actionsv1alpha1.WorkflowRunConditionPlanned)
	}
	if disposition == planningFailureTerminal {
		return ctrl.Result{}, nil
	}
	return ctrl.Result{}, cause
}

func (r *WorkflowRunReconciler) planningEvaluationFailed(ctx context.Context, run *actionsv1alpha1.WorkflowRun, cause error) (ctrl.Result, error) {
	var unavailable *projectValuesUnavailableError
	if errors.As(cause, &unavailable) {
		return r.planningFailed(ctx, run, "ProjectValuesUnavailable", cause, planningFailureRetry)
	}
	return r.planningFailed(ctx, run, "WorkflowInvalid", cause, planningFailureTerminal)
}

func childCreationFailureDisposition(err error) planningFailureDisposition {
	terminal := &terminalPlanningError{}
	if apierrors.IsInvalid(err) || errors.As(err, &terminal) {
		return planningFailureTerminal
	}
	return planningFailureRetry
}

func githubAPIStatus(err error, status int) bool {
	apiError := &githubclient.APIError{}
	return errors.As(err, &apiError) && apiError.StatusCode == status
}

func (r *WorkflowRunReconciler) SetupWithManager(manager ctrl.Manager) error {
	if r.GitRepository == nil {
		return errors.New("Git repository client must be specified")
	}
	return ctrl.NewControllerManagedBy(manager).
		For(&actionsv1alpha1.WorkflowRun{}).
		Watches(&actionsv1alpha1.WorkflowRun{}, handler.EnqueueRequestsFromMapFunc(r.workflowRunsSupersededByForkPullRequestRevision), builder.WithPredicates(predicate.Funcs{
			CreateFunc:  func(event.CreateEvent) bool { return true },
			UpdateFunc:  func(event.UpdateEvent) bool { return false },
			DeleteFunc:  func(event.DeleteEvent) bool { return false },
			GenericFunc: func(event.GenericEvent) bool { return false },
		})).
		Watches(&actionsv1alpha1.WorkflowRun{}, handler.EnqueueRequestsFromMapFunc(r.workflowRunsSharingGitHubStatus)).
		Owns(&actionsv1alpha1.WorkflowJob{}).
		Complete(r)
}

func (r *WorkflowRunReconciler) workflowRunsSharingGitHubStatus(ctx context.Context, object client.Object) []reconcile.Request {
	run, ok := object.(*actionsv1alpha1.WorkflowRun)
	if !ok {
		return nil
	}
	statusKey := run.Labels[actionsv1alpha1.LabelGitHubStatusKey]
	if statusKey == "" {
		return nil
	}
	runs := &actionsv1alpha1.WorkflowRunList{}
	if err := r.APIReader.List(ctx, runs, client.InNamespace(run.Namespace), client.MatchingLabels{actionsv1alpha1.LabelGitHubStatusKey: statusKey}); err != nil {
		ctrl.LoggerFrom(ctx).Error(err, "List WorkflowRuns sharing GitHub status", "workflow_run", run.Name)
		return nil
	}
	requests := make([]reconcile.Request, 0, len(runs.Items))
	for index := range runs.Items {
		candidate := &runs.Items[index]
		if candidate.UID == run.UID || !candidate.DeletionTimestamp.IsZero() || !githubStatusKeyMatches(candidate, run) {
			continue
		}
		requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(candidate)})
	}
	return requests
}

func (r *WorkflowRunReconciler) workflowRunsSupersededByForkPullRequestRevision(ctx context.Context, object client.Object) []reconcile.Request {
	run, ok := object.(*actionsv1alpha1.WorkflowRun)
	if !ok || run.Spec.ForkPullRequest == nil {
		return nil
	}
	runs := &actionsv1alpha1.WorkflowRunList{}
	if err := r.APIReader.List(ctx, runs, client.InNamespace(run.Namespace)); err != nil {
		ctrl.LoggerFrom(ctx).Error(err, "List WorkflowRuns for fork pull request revision", "workflow_run", run.Name)
		return nil
	}
	requests := make([]reconcile.Request, 0)
	for index := range runs.Items {
		candidate := &runs.Items[index]
		policy := candidate.Spec.ForkPullRequest
		if policy == nil || !policy.RequireApproval || policy.Approved || terminalRun(candidate) ||
			!run.CreationTimestamp.After(candidate.CreationTimestamp.Time) || !sameForkPullRequest(candidate, run) || forkPullRequestHeadSHA(candidate) == forkPullRequestHeadSHA(run) {
			continue
		}
		requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(candidate)})
	}
	return requests
}

func terminalRun(run *actionsv1alpha1.WorkflowRun) bool {
	condition := meta.FindStatusCondition(run.Status.Conditions, actionsv1alpha1.WorkflowRunConditionSucceeded)
	return condition != nil && (condition.Status == metav1.ConditionTrue || condition.Status == metav1.ConditionFalse)
}

func workflowJobName(runName, jobID string) string {
	digest := sha256.Sum256([]byte(runName + "\x00" + jobID))
	suffix := strings.ToLower(digestEncoding.EncodeToString(digest[:]))[:workflowJobNameDigestLength]
	parentPrefix := sanitizeName(runName)
	childPrefix := sanitizeName(jobID)
	if childPrefix == "" {
		childPrefix = "job"
	}
	maxPrefix := resourceNameMaxLength - len(suffix) - 1
	if len(childPrefix) >= maxPrefix {
		childPrefix = strings.Trim(childPrefix[:maxPrefix], "-")
		return childPrefix + "-" + suffix
	}
	maxParentLength := maxPrefix - len(childPrefix) - 1
	if len(parentPrefix) > maxParentLength {
		parentPrefix = strings.Trim(parentPrefix[:maxParentLength], "-")
	}
	prefix := childPrefix
	if parentPrefix != "" {
		prefix = parentPrefix + "-" + childPrefix
	}
	return prefix + "-" + suffix
}

func childName(parentName, childID string) string {
	digest := sha256.Sum256([]byte(parentName + "\x00" + childID))
	suffix := strings.ToLower(digestEncoding.EncodeToString(digest[:]))
	prefix := sanitizeName(parentName + "-" + childID)
	maxPrefix := resourceNameMaxLength - len(suffix) - 1
	if len(prefix) > maxPrefix {
		prefix = strings.Trim(prefix[:maxPrefix], "-")
	}
	if prefix == "" {
		prefix = "job"
	}
	return prefix + "-" + suffix
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

func workflowJobLabels(run *actionsv1alpha1.WorkflowRun, project *actionsv1alpha1.Project, jobID string) map[string]string {
	jobLabel := jobID
	if len(validation.IsValidLabelValue(jobLabel)) > 0 {
		digest := sha256.Sum256([]byte(jobID))
		jobLabel = strings.ToLower(digestEncoding.EncodeToString(digest[:]))
	}
	return map[string]string{
		actionsv1alpha1.LabelProjectUID:     string(project.UID),
		actionsv1alpha1.LabelWorkflowRunUID: string(run.UID),
		actionsv1alpha1.LabelWorkflowJob:    jobLabel,
	}
}

func stringMap(values map[string]any) (map[string]string, error) {
	if len(values) == 0 {
		return nil, nil
	}
	result := make(map[string]string, len(values))
	for key, value := range values {
		switch typed := value.(type) {
		case string:
			result[key] = typed
		case json.Number:
			result[key] = typed.String()
		case bool, int, int64, uint64, float64:
			result[key] = fmt.Sprint(typed)
		default:
			return nil, fmt.Errorf("value %q must be a scalar", key)
		}
	}
	return result, nil
}

func mergeStringMaps(base, override map[string]string) map[string]string {
	if len(base) == 0 && len(override) == 0 {
		return nil
	}
	result := make(map[string]string, len(base)+len(override))
	for key, value := range base {
		result[key] = value
	}
	for key, value := range override {
		result[key] = value
	}
	return result
}
