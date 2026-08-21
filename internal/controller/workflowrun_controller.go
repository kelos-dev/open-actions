package controller

import (
	"context"
	"crypto/sha256"
	"encoding/base32"
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
	"github.com/kelos-dev/open-actions/internal/projectvalue"
	"github.com/kelos-dev/open-actions/internal/runner"
	"github.com/kelos-dev/open-actions/internal/workflow"
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
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const (
	jobPlanKey                       = "job.json"
	jobNeedsKey                      = "needs.json"
	matrixPlanKey                    = "matrix.json"
	workflowPlanKey                  = "workflow.json"
	maxJobPlanBytes                  = 900_000
	resourceNameMaxLength            = 63
	workflowJobNameDigestLength      = 16
	workflowJobDisplayNameMaxLength  = 256
	workflowJobIDMaxLength           = 256
	workflowRunCancellationFinalizer = "actions.kelos.dev/concurrency-cancellation"
	workflowRunCheckFinalizer        = "actions.kelos.dev/github-check"
	workflowRunScheduleFinalizer     = "actions.kelos.dev/schedule-idempotency"
	defaultJobTimeout                = time.Duration(workflow.DefaultJobTimeoutMinutes) * time.Minute
	defaultMaxJobTimeout             = 6 * time.Hour
	workflowRunSequencePrefix        = "open-actions-run-sequence-"
	workflowRunSequenceScopeKey      = "scope"
	workflowRunSequenceNextKey       = "next"
	maxGitHubCompatibleNumber        = int64(9_007_199_254_740_991)
)

var digestEncoding = base32.StdEncoding.WithPadding(base32.NoPadding)

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
}

func (r *WorkflowRunReconciler) Reconcile(ctx context.Context, request ctrl.Request) (ctrl.Result, error) {
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
			return r.planningFailed(ctx, run, "RerunInvalid", err, planningFailureTerminal)
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
	if r.githubCheckEnabled(run) && run.DeletionTimestamp.IsZero() && !controllerutil.ContainsFinalizer(run, workflowRunCheckFinalizer) {
		before := run.DeepCopy()
		controllerutil.AddFinalizer(run, workflowRunCheckFinalizer)
		if err := r.Patch(ctx, run, client.MergeFrom(before)); err != nil {
			return ctrl.Result{}, err
		}
	}
	if !run.DeletionTimestamp.IsZero() {
		return r.finalizeCanceledWorkflowRun(ctx, run)
	}
	reportError := r.reconcileGitHubCheck(ctx, run)
	result, reconcileError := r.reconcileWorkflowRun(ctx, run)
	if reconcileError != nil {
		return result, errors.Join(reportError, reconcileError)
	}
	if reportError != nil {
		if !terminalRun(run) && (result.Requeue || result.RequeueAfter > 0) {
			ctrl.LoggerFrom(ctx).Error(reportError, "GitHub Check reporting failed while workflow reconciliation is pending")
			return result, nil
		}
		return result, reportError
	}
	return result, nil
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

func (r *WorkflowRunReconciler) githubCheckEnabled(run *actionsv1alpha1.WorkflowRun) bool {
	planned := meta.FindStatusCondition(run.Status.Conditions, actionsv1alpha1.WorkflowRunConditionPlanned)
	if run.Spec.Rerun != nil && planned != nil && planned.Status == metav1.ConditionFalse && planned.Reason == "RerunInvalid" {
		return false
	}
	return r.GitHub != nil && run.Spec.Source.Type == actionsv1alpha1.SourceTypeGitHub && run.Spec.Source.GitHub != nil && run.UID != ""
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
	installation, err := r.GitHub.Installation(ctx, githubConfig.AppID, githubConfig.InstallationID, privateKey, githubSource.Repository.Name, githubclient.InstallationPermissions{"contents": "read"})
	if err != nil {
		return r.planningFailed(ctx, run, "GitHubAuthenticationFailed", err, planningFailureRetry)
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
	planningRun, planningEvent, err := resolvePlanningEvent(run, definition, eventPayload)
	if err != nil {
		return r.planningFailed(ctx, run, "TriggerInvalid", err, planningFailureTerminal)
	}
	variables := r.projectVariableContext(ctx, project)
	if concurrency := workflowRunConcurrencyDecision(run); concurrency == nil {
		concurrencyGroup, cancelInProgress, err := workflow.EvaluateConcurrency(definition, planningEvent, variables)
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
	plannedJobs, deferredMatrices, err := r.planWorkflowJobs(planningRun, definition, planningEvent.InputValues, variables, eventPayload)
	if err != nil {
		return r.planningEvaluationFailed(ctx, run, err)
	}
	if len(deferredMatrices) > 0 && run.Spec.Rerun != nil && len(run.Spec.Rerun.JobIDs) > 0 {
		return r.planningFailed(ctx, run, "RerunInvalid", fmt.Errorf("WorkflowRun %q does not support selective reruns with dynamic matrices", run.Name), planningFailureTerminal)
	}
	plannedJobs, err = selectRerunWorkflowJobs(run, plannedJobs)
	if err != nil {
		return r.planningFailed(ctx, run, "RerunInvalid", err, planningFailureTerminal)
	}
	jobCount := int32(len(plannedJobs) + len(deferredMatrices))
	run.Status.WorkflowName = definition.Name
	run.Status.Jobs = &actionsv1alpha1.WorkflowRunJobStatus{Total: jobCount}
	if len(deferredMatrices) > 0 {
		if err := r.ensureWorkflowPlan(ctx, run, project, plannedJobs, deferredMatrices); err != nil {
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

type checkRunReport struct {
	Status      string
	Conclusion  string
	StartedAt   string
	CompletedAt string
	Output      githubclient.CheckRunOutput
}

func (r *WorkflowRunReconciler) reconcileGitHubCheck(ctx context.Context, run *actionsv1alpha1.WorkflowRun) error {
	githubSource := run.Spec.Source.GitHub
	if !r.githubCheckEnabled(run) {
		return nil
	}
	report := workflowRunCheckReport(run)
	project := &actionsv1alpha1.Project{}
	projectKey := client.ObjectKey{Namespace: run.Namespace, Name: run.Spec.ProjectRef.Name}
	if err := r.APIReader.Get(ctx, projectKey, project); err != nil {
		return fmt.Errorf("get Project %q for GitHub check: %w", projectKey.Name, err)
	}
	githubConfig := project.Spec.Source.GitHub
	checkHeadSHA := githubSource.Revision.SHA
	if githubSource.Event.Name == actionsv1alpha1.GitHubEventNamePullRequest && githubSource.Revision.HeadSHA != "" {
		checkHeadSHA = githubSource.Revision.HeadSHA
	}
	detailsURL := ""
	if r.ConsoleURL != "" {
		detailsURL = workflowRunConsoleURL(r.ConsoleURL, run)
	}
	name := "Open Actions / " + run.Spec.WorkflowPath
	externalID := workflowRunCheckExternalID(run)
	createRequest := githubclient.CreateCheckRunRequest{
		Name:        name,
		HeadSHA:     checkHeadSHA,
		DetailsURL:  detailsURL,
		ExternalID:  externalID,
		Status:      report.Status,
		Conclusion:  report.Conclusion,
		StartedAt:   report.StartedAt,
		CompletedAt: report.CompletedAt,
		Output:      &report.Output,
	}
	reportDigest := checkRunReportDigest(createRequest)
	current := workflowRunCheckRunStatus(run)
	if current != nil && current.Status == report.Status && current.Conclusion == report.Conclusion && current.ReportDigest == reportDigest {
		return nil
	}
	currentAttempt, err := r.githubCheckCurrentAttempt(ctx, run)
	if err != nil {
		return err
	}
	if !currentAttempt {
		return nil
	}

	privateKey, err := secretValue(ctx, r.APIReader, project.Namespace, githubConfig.PrivateKeySecretRef)
	if err != nil {
		return fmt.Errorf("read credentials for GitHub check: %w", err)
	}
	installation, err := r.GitHub.Installation(ctx, githubConfig.AppID, githubConfig.InstallationID, privateKey, githubSource.Repository.Name, githubclient.InstallationPermissions{"checks": "write"})
	if err != nil {
		return fmt.Errorf("authenticate GitHub check reporter: %w", err)
	}

	var checkRun *githubclient.CheckRun
	if current == nil {
		checkRun, err = installation.FindCheckRun(ctx, githubSource.Repository.Owner, githubSource.Repository.Name, checkHeadSHA, githubConfig.AppID, externalID)
		if err != nil {
			return err
		}
	}
	if current == nil && checkRun == nil {
		checkRun, err = installation.CreateCheckRun(ctx, githubSource.Repository.Owner, githubSource.Repository.Name, createRequest)
		if err != nil {
			return err
		}
	} else {
		var id int64
		if current != nil {
			id = current.ID
		} else {
			id = checkRun.ID
		}
		if id < 1 {
			return errors.New("GitHub returned an invalid check-run ID")
		}
		checkRun, err = installation.UpdateCheckRun(ctx, githubSource.Repository.Owner, githubSource.Repository.Name, id, githubclient.UpdateCheckRunRequest{
			Name:        name,
			DetailsURL:  detailsURL,
			ExternalID:  externalID,
			Status:      report.Status,
			Conclusion:  report.Conclusion,
			StartedAt:   report.StartedAt,
			CompletedAt: report.CompletedAt,
			Output:      &report.Output,
		})
		if err != nil {
			return err
		}
	}
	if checkRun == nil || checkRun.ID < 1 {
		return errors.New("GitHub returned an invalid check-run ID")
	}
	return r.recordGitHubCheckRun(ctx, run, checkRun.ID, report.Status, report.Conclusion, reportDigest)
}

func (r *WorkflowRunReconciler) githubCheckCurrentAttempt(ctx context.Context, run *actionsv1alpha1.WorkflowRun) (bool, error) {
	rootUID := run.UID
	rootName := run.Name
	attempt := int32(1)
	if run.Spec.Rerun != nil {
		rootUID = run.Spec.Rerun.OriginalRunRef.UID
		rootName = run.Spec.Rerun.OriginalRunRef.Name
		attempt = run.Spec.Rerun.Attempt
	}
	runs := &actionsv1alpha1.WorkflowRunList{}
	if err := r.APIReader.List(ctx, runs, client.InNamespace(run.Namespace), client.MatchingLabels{actionsv1alpha1.LabelWorkflowRunRootUID: string(rootUID)}); err != nil {
		return false, err
	}
	for index := range runs.Items {
		candidate := &runs.Items[index]
		candidateAttempt := int32(1)
		if candidate.UID != rootUID {
			if candidate.Spec.Rerun == nil || candidate.Spec.Rerun.OriginalRunRef.Name != rootName || candidate.Spec.Rerun.OriginalRunRef.UID != rootUID {
				continue
			}
			candidateAttempt = candidate.Spec.Rerun.Attempt
		}
		if candidateAttempt > attempt && candidate.Spec.ProjectRef == run.Spec.ProjectRef && candidate.Spec.WorkflowPath == run.Spec.WorkflowPath && apiEquality.Semantic.DeepEqual(candidate.Spec.Source, run.Spec.Source) {
			return false, nil
		}
	}
	return true, nil
}

func checkRunReportDigest(request githubclient.CreateCheckRunRequest) string {
	data, _ := json.Marshal(request)
	digest := sha256.Sum256(data)
	return fmt.Sprintf("%x", digest)
}

func workflowRunCheckExternalID(run *actionsv1alpha1.WorkflowRun) string {
	if run.Spec.Rerun != nil {
		return string(run.Spec.Rerun.OriginalRunRef.UID)
	}
	return string(run.UID)
}

func workflowRunCheckReport(run *actionsv1alpha1.WorkflowRun) checkRunReport {
	title := run.Status.WorkflowName
	if title == "" {
		title = run.Spec.WorkflowPath
	}
	report := checkRunReport{
		Status: "queued",
		Output: githubclient.CheckRunOutput{
			Title:   title,
			Summary: "The workflow is queued.",
		},
	}
	succeeded := meta.FindStatusCondition(run.Status.Conditions, actionsv1alpha1.WorkflowRunConditionSucceeded)
	switch {
	case succeeded != nil && succeeded.Status == metav1.ConditionTrue:
		report.Status = "completed"
		report.Conclusion = "success"
		report.summaryFromCondition(succeeded)
	case succeeded != nil && succeeded.Status == metav1.ConditionFalse:
		report.Status = "completed"
		switch succeeded.Reason {
		case "JobCancelled":
			report.Conclusion = "cancelled"
		case "JobTimedOut":
			report.Conclusion = "timed_out"
		default:
			report.Conclusion = "failure"
		}
		report.summaryFromCondition(succeeded)
	case !run.DeletionTimestamp.IsZero():
		report.Status = "completed"
		report.Conclusion = "cancelled"
		report.Output.Summary = "The workflow was cancelled."
	case run.Status.StartTime != nil:
		report.Status = "in_progress"
		report.Output.Summary = "The workflow is running."
	default:
		planned := meta.FindStatusCondition(run.Status.Conditions, actionsv1alpha1.WorkflowRunConditionPlanned)
		if planned != nil && planned.Message != "" {
			report.Output.Summary = planned.Message
		}
	}
	if run.Status.StartTime != nil {
		report.StartedAt = run.Status.StartTime.UTC().Format(time.RFC3339)
	}
	if run.Status.CompletionTime != nil {
		report.CompletedAt = run.Status.CompletionTime.UTC().Format(time.RFC3339)
	} else if !run.DeletionTimestamp.IsZero() {
		report.CompletedAt = run.DeletionTimestamp.UTC().Format(time.RFC3339)
	} else if report.Status == "completed" && succeeded != nil {
		report.CompletedAt = succeeded.LastTransitionTime.UTC().Format(time.RFC3339)
	}
	if run.Status.Jobs != nil {
		jobs := run.Status.Jobs
		report.Output.Text = fmt.Sprintf("Jobs: %d total, %d waiting, %d queued, %d active, %d succeeded, %d failed, %d timed out, %d skipped, %d cancelled.", jobs.Total, jobs.Waiting, jobs.Queued, jobs.Active, jobs.Succeeded, jobs.Failed, jobs.TimedOut, jobs.Skipped, jobs.Cancelled)
	}
	if run.Spec.Rerun != nil {
		report.Output.Title = fmt.Sprintf("%s (attempt %d)", title, run.Spec.Rerun.Attempt)
		selection := "all jobs"
		if count := len(run.Spec.Rerun.JobIDs); count > 0 {
			selection = fmt.Sprintf("%d requested jobs plus required dependencies", count)
		}
		attempt := fmt.Sprintf("Attempt %d reruns %s.", run.Spec.Rerun.Attempt, selection)
		if report.Output.Text == "" {
			report.Output.Text = attempt
		} else {
			report.Output.Text = attempt + "\n\n" + report.Output.Text
		}
	}
	return report
}

func (r *checkRunReport) summaryFromCondition(condition *metav1.Condition) {
	if condition.Message != "" {
		r.Output.Summary = condition.Message
	}
}

func workflowRunCheckRunStatus(run *actionsv1alpha1.WorkflowRun) *actionsv1alpha1.GitHubCheckRunStatus {
	if run.Status.Source == nil || run.Status.Source.GitHub == nil {
		return nil
	}
	return run.Status.Source.GitHub.CheckRun
}

func (r *WorkflowRunReconciler) recordGitHubCheckRun(ctx context.Context, run *actionsv1alpha1.WorkflowRun, id int64, status, conclusion, reportDigest string) error {
	before := run.DeepCopy()
	if run.Status.Source == nil {
		run.Status.Source = &actionsv1alpha1.WorkflowRunSourceStatus{}
	}
	if run.Status.Source.GitHub == nil {
		run.Status.Source.GitHub = &actionsv1alpha1.GitHubWorkflowRunStatus{}
	}
	run.Status.Source.GitHub.CheckRun = &actionsv1alpha1.GitHubCheckRunStatus{ID: id, Status: status, Conclusion: conclusion, ReportDigest: reportDigest}
	return r.Status().Patch(ctx, run, client.MergeFrom(before))
}

func workflowRunConsoleURL(baseURL string, run *actionsv1alpha1.WorkflowRun) string {
	parsed, err := url.Parse(baseURL)
	if err != nil {
		return ""
	}
	parsed.Path = strings.TrimRight(parsed.Path, "/") + "/runs/" + url.PathEscape(run.Namespace) + "/" + url.PathEscape(run.Name)
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

type deferredMatrixPlan struct {
	JobID        string            `json:"jobID"`
	WorkflowName string            `json:"workflowName"`
	WorkflowEnv  map[string]string `json:"workflowEnv,omitempty"`
	EventPayload map[string]any    `json:"eventPayload,omitempty"`
	Variables    map[string]any    `json:"variables,omitempty"`
	Job          workflow.Job      `json:"job"`
	InputValues  map[string]any    `json:"inputValues,omitempty"`
}

type workflowPlanManifest struct {
	JobIDs    []string          `json:"jobIDs"`
	SourceIDs []string          `json:"sourceIDs"`
	Matrices  map[string]string `json:"matrices"`
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
	plannedByLogicalID := make(map[string][]*plannedWorkflowJob)
	for index := range plannedJobs {
		job := &plannedJobs[index]
		plannedByID[job.id] = job
		logicalID := job.id
		if job.matrix != nil {
			logicalID = job.matrix.LogicalJobID
		}
		plannedByLogicalID[logicalID] = append(plannedByLogicalID[logicalID], job)
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

	for changed := true; changed; {
		changed = false
		for id := range selectedIDs {
			for _, dependency := range plannedByID[id].needs {
				needed := plannedByLogicalID[dependency]
				if len(needed) == 0 {
					return nil, fmt.Errorf("selected job %q needs missing job %q", id, dependency)
				}
				for _, job := range needed {
					if _, found := selectedIDs[job.id]; found {
						continue
					}
					selectedIDs[job.id] = struct{}{}
					changed = true
				}
			}
		}
	}

	selected := make([]plannedWorkflowJob, 0, len(selectedIDs))
	for _, job := range plannedJobs {
		if _, found := selectedIDs[job.id]; found {
			selected = append(selected, job)
		}
	}
	return selected, nil
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

func (r *WorkflowRunReconciler) planWorkflowJobs(run *actionsv1alpha1.WorkflowRun, definition *workflow.Definition, inputValues map[string]any, variables any, eventPayload map[string]any) ([]plannedWorkflowJob, []deferredMatrixPlan, error) {
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
	deferredMatrices := make([]deferredMatrixPlan, 0)
	sourceIDs := make(map[string]struct{}, len(jobIDs))
	for _, id := range jobIDs {
		sourceIDs[id] = struct{}{}
	}
	for _, id := range jobIDs {
		definitionJob := definition.Jobs[id]
		definitionJob.Permissions = workflow.EffectivePermissions(definition.Permissions, definitionJob.Permissions)
		if workflow.MatrixUsesNeeds(definitionJob.Strategy) {
			if !variablesSnapshotted {
				variableSnapshot, err = snapshotExpressionVariables(variables)
				if err != nil {
					return nil, nil, err
				}
				variablesSnapshotted = true
			}
			deferredMatrices = append(deferredMatrices, deferredMatrixPlan{
				JobID: id, WorkflowName: definition.Name, WorkflowEnv: workflowEnv, EventPayload: eventPayload, Variables: variableSnapshot, Job: definitionJob, InputValues: inputValues,
			})
			if len(plannedJobs)+len(deferredMatrices) > workflow.MaxJobs {
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
		if len(plannedJobs)+len(deferredMatrices) > workflow.MaxJobs {
			return nil, nil, fmt.Errorf("workflow expands to more than %d jobs", workflow.MaxJobs)
		}
	}
	return plannedJobs, deferredMatrices, nil
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
			contexts := []string{"github", "open_actions", "matrix", "inputs", "vars"}
			if _, found := jobContext.Values["needs"]; found {
				contexts = append(contexts, "needs")
			}
			jobContext.Availability = workflowexpression.NewAvailability(contexts...)
			jobContext.Values["matrix"] = matrix
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
			displayName = matrixDisplayName(displayName, matrix, index)
		}
		timeoutSeconds := r.effectiveJobTimeoutSeconds(resolvedJob.TimeoutMinutes.Minutes())
		plan, err := r.jobPlan(run, workflowName, id, workflowEnv, resolvedJob, matrix, inputValues, timeoutSeconds)
		if err != nil {
			return nil, err
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

func matrixDisplayName(name string, matrix map[string]any, index int) string {
	suffix := " (" + matrixDescription(matrix) + ")"
	if len([]rune(suffix)) >= workflowJobDisplayNameMaxLength {
		suffix = fmt.Sprintf(" (matrix %d)", index+1)
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
	runID, runNumber, runAttempt, runURL, runQueryURL := "", "", "", "", ""
	if identity != nil {
		runID = strconv.FormatInt(identity.ID, 10)
		runNumber = strconv.FormatInt(identity.Number, 10)
		runAttempt = strconv.FormatInt(int64(identity.Attempt), 10)
		runURL = identity.URL
		if r.ConsoleURL != "" {
			runQueryURL = workflowRunQueryURL(r.ConsoleURL, run)
		}
	}
	return workflowexpression.Context{
		Availability: workflowexpression.NewAvailability("github", "open_actions", "inputs", "vars"),
		Values: map[string]any{
			"inputs": inputValues,
			"vars":   variables,
			"github": map[string]any{
				"actor":       githubSourceActor(githubSource),
				"workflow":    workflowName,
				"event_name":  string(githubSource.Event.Name),
				"event":       eventValues,
				"repository":  githubSource.Repository.Owner + "/" + githubSource.Repository.Name,
				"sha":         githubSource.Revision.SHA,
				"ref":         githubSource.Revision.Ref,
				"ref_name":    githubclient.RefName(githubSource.Revision.Ref),
				"head_ref":    headRef,
				"base_ref":    baseRef,
				"server_url":  strings.TrimSuffix(r.GitHubServerURL, "/"),
				"api_url":     strings.TrimSuffix(r.GitHubAPIBase, "/"),
				"run_id":      runID,
				"run_number":  runNumber,
				"run_attempt": runAttempt,
			},
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
	owned := false
	for _, owner := range secret.OwnerReferences {
		if owner.APIVersion == actionsv1alpha1.GroupVersion.String() && owner.Kind == "WorkflowRun" && owner.Name == run.Name && owner.UID == run.UID {
			owned = true
			break
		}
	}
	data, found := secret.Data[eventsnapshot.DataKey]
	if !owned {
		return nil, fmt.Errorf("GitHub event snapshot Secret %q is not owned by WorkflowRun %q", name, run.Name)
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

func (r *WorkflowRunReconciler) ensureWorkflowPlan(ctx context.Context, run *actionsv1alpha1.WorkflowRun, project *actionsv1alpha1.Project, plannedJobs []plannedWorkflowJob, deferredMatrices []deferredMatrixPlan) error {
	manifest := workflowPlanManifest{
		JobIDs:    make([]string, 0, len(plannedJobs)),
		SourceIDs: make([]string, 0, len(plannedJobs)+len(deferredMatrices)),
		Matrices:  make(map[string]string, len(deferredMatrices)),
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
	for index := range deferredMatrices {
		plan := &deferredMatrices[index]
		sourceIDs[plan.JobID] = struct{}{}
		name := matrixPlanConfigMapName(run.Name, plan.JobID)
		manifest.Matrices[plan.JobID] = name
		data, err := json.Marshal(plan)
		if err != nil {
			return &terminalPlanningError{cause: fmt.Errorf("encode dynamic matrix plan for job %q: %w", plan.JobID, err)}
		}
		if len(data) > maxJobPlanBytes {
			return &terminalPlanningError{cause: fmt.Errorf("dynamic matrix plan for job %q exceeds %d bytes", plan.JobID, maxJobPlanBytes)}
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
					actionsv1alpha1.AnnotationProjectName: project.Name,
					actionsv1alpha1.AnnotationMatrixPlan:  plan.JobID,
				},
			},
			Immutable: pointerTo(true),
			Data:      map[string]string{matrixPlanKey: string(data)},
		}
		if err := r.ensureWorkflowOwnedConfigMap(ctx, run, configMap, matrixPlanKey); err != nil {
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

func matrixPlanConfigMapName(runName, jobID string) string {
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
			Actor: githubSourceActor(githubSource), URL: identity.URL,
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
	if pullRequest := githubSource.Event.PullRequest; pullRequest != nil && githubSource.Event.Name != actionsv1alpha1.GitHubEventNamePullRequestTarget {
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

func (r *WorkflowRunReconciler) reconcileDynamicMatrices(ctx context.Context, run *actionsv1alpha1.WorkflowRun, jobs *actionsv1alpha1.WorkflowJobList) (workflowPlanState, error) {
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
	state.expected = make(map[string]struct{}, len(manifest.JobIDs)+len(manifest.Matrices))
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
	matrixIDs := make([]string, 0, len(manifest.Matrices))
	for id := range manifest.Matrices {
		matrixIDs = append(matrixIDs, id)
	}
	sort.Strings(matrixIDs)
	for _, id := range matrixIDs {
		planConfigMap := &corev1.ConfigMap{}
		planKey := client.ObjectKey{Namespace: run.Namespace, Name: manifest.Matrices[id]}
		if err := r.APIReader.Get(ctx, planKey, planConfigMap); err != nil {
			if apierrors.IsNotFound(err) {
				return state, &terminalPlanningError{cause: fmt.Errorf("dynamic matrix plan ConfigMap %q for job %q is missing from WorkflowRun %q", planKey.Name, id, run.Name)}
			}
			return state, err
		}
		if !metav1.IsControlledBy(planConfigMap, run) || planConfigMap.Immutable == nil || !*planConfigMap.Immutable || planConfigMap.Annotations[actionsv1alpha1.AnnotationMatrixPlan] != id {
			return state, &terminalPlanningError{cause: fmt.Errorf("dynamic matrix plan ConfigMap %q does not match job %q in WorkflowRun %q", planConfigMap.Name, id, run.Name)}
		}
		plan := deferredMatrixPlan{}
		decoder := json.NewDecoder(strings.NewReader(planConfigMap.Data[matrixPlanKey]))
		decoder.UseNumber()
		if err := decoder.Decode(&plan); err != nil {
			return state, &terminalPlanningError{cause: fmt.Errorf("decode dynamic matrix plan ConfigMap %q: %w", planConfigMap.Name, err)}
		}
		if plan.JobID != id {
			return state, &terminalPlanningError{cause: fmt.Errorf("dynamic matrix plan ConfigMap %q identifies job %q, want %q", planConfigMap.Name, plan.JobID, id)}
		}
		if resultJob := jobsByID[id]; resultJob != nil && workflowJobTerminal(resultJob) {
			state.expected[id] = struct{}{}
			continue
		}
		matrixExpanded := false
		for _, job := range jobsByLogicalID[id] {
			if job.Spec.Matrix != nil && job.Spec.Matrix.LogicalJobID == id {
				matrixExpanded = true
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
		if !matrixExpanded {
			runnable, err := workflow.EvaluateJobCondition(id, plan.Job.If, expressionContext)
			if err != nil {
				changed, resultErr := r.completeDynamicMatrix(ctx, run, planConfigMap, plan, jobsByID[id], actionsv1alpha1.WorkflowJobResultFailure, "ConditionEvaluationFailed", err.Error())
				state.changed = state.changed || changed
				state.expected[id] = struct{}{}
				return state, resultErr
			}
			if !runnable {
				result := actionsv1alpha1.WorkflowJobResultSkipped
				reason := "ConditionFalse"
				message := "The workflow job condition evaluated to false before matrix expansion"
				if run.Spec.CancelRequested {
					result = actionsv1alpha1.WorkflowJobResultCancelled
					reason = "CancellationRequested"
					message = "The workflow job was cancelled before matrix expansion"
				}
				changed, resultErr := r.completeDynamicMatrix(ctx, run, planConfigMap, plan, jobsByID[id], result, reason, message)
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
			changed, resultErr := r.completeDynamicMatrix(ctx, run, planConfigMap, plan, jobsByID[id], actionsv1alpha1.WorkflowJobResultFailure, "MatrixEvaluationFailed", err.Error())
			state.changed = state.changed || changed
			state.expected[id] = struct{}{}
			return state, resultErr
		}
		if projectedWorkflowJobCount(len(jobs.Items), jobsByLogicalID, matrixIDs, id, len(combinations)) > workflow.MaxJobs {
			message := fmt.Sprintf("workflow expands to more than %d jobs", workflow.MaxJobs)
			changed, resultErr := r.completeDynamicMatrix(ctx, run, planConfigMap, plan, jobsByID[id], actionsv1alpha1.WorkflowJobResultFailure, "MatrixEvaluationFailed", message)
			state.changed = state.changed || changed
			state.expected[id] = struct{}{}
			return state, resultErr
		}

		plannedIDs := make(map[string]struct{}, len(jobsByID)+len(manifest.JobIDs))
		for existingID, existing := range jobsByID {
			if existing.Spec.Matrix == nil || existing.Spec.Matrix.LogicalJobID != id {
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
			changed, resultErr := r.completeDynamicMatrix(ctx, run, planConfigMap, plan, jobsByID[id], actionsv1alpha1.WorkflowJobResultFailure, "MatrixEvaluationFailed", err.Error())
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

func projectedWorkflowJobCount(existingJobs int, jobsByLogicalID map[string][]*actionsv1alpha1.WorkflowJob, matrixIDs []string, currentID string, combinations int) int {
	result := existingJobs - len(jobsByLogicalID[currentID]) + combinations
	for _, id := range matrixIDs {
		if id != currentID && len(jobsByLogicalID[id]) == 0 {
			result++
		}
	}
	return result
}

func (r *WorkflowRunReconciler) completeDynamicMatrix(ctx context.Context, run *actionsv1alpha1.WorkflowRun, planConfigMap *corev1.ConfigMap, plan deferredMatrixPlan, existing *actionsv1alpha1.WorkflowJob, result actionsv1alpha1.WorkflowJobResult, reason, message string) (bool, error) {
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
			RunsOn:         []string{"matrix-evaluation"},
			Needs:          append([]string(nil), plan.Job.Needs...),
			If:             plan.Job.If,
		},
	}
	if err := controllerutil.SetControllerReference(run, desired, r.Scheme()); err != nil {
		return false, &terminalPlanningError{cause: err}
	}
	job := desired
	created := false
	if existing != nil {
		if !workflowJobIdentityMatches(existing, desired, run) {
			return false, &terminalPlanningError{cause: fmt.Errorf("WorkflowJob %q does not match dynamic matrix job %q in WorkflowRun %q", existing.Name, plan.JobID, run.Name)}
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
		if !workflowJobIdentityMatches(job, desired, run) {
			return false, &terminalPlanningError{cause: fmt.Errorf("WorkflowJob %q does not match dynamic matrix job %q in WorkflowRun %q", job.Name, plan.JobID, run.Name)}
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

func (r *WorkflowRunReconciler) observeWorkflowJobs(ctx context.Context, run *actionsv1alpha1.WorkflowRun, workflowName string, total int32) (ctrl.Result, error) {
	reader := r.APIReader
	jobs := &actionsv1alpha1.WorkflowJobList{}
	if err := reader.List(ctx, jobs, client.InNamespace(run.Namespace), client.MatchingLabels{actionsv1alpha1.LabelWorkflowRunUID: string(run.UID)}); err != nil {
		return ctrl.Result{}, err
	}
	planState, err := r.reconcileDynamicMatrices(ctx, run, jobs)
	if err != nil {
		return ctrl.Result{}, err
	}
	if planState.changed {
		return ctrl.Result{Requeue: true}, nil
	}
	expectedObjects := int(total)
	pendingMatrices := map[string]struct{}{}
	if planState.active {
		expectedObjects = len(planState.expected)
		pendingMatrices = planState.pending
		total = int32(expectedObjects + len(pendingMatrices))
	}
	failFastPending, err := r.reconcileMatrixFailFast(ctx, jobs)
	if err != nil {
		return ctrl.Result{}, err
	}
	status := &actionsv1alpha1.WorkflowRunJobStatus{Total: total, Waiting: int32(len(pendingMatrices))}
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
		if err := r.reconcileWorkflowJobGraph(ctx, run, workflowName, inputValues, variables, eventPayload, jobs.Items, pendingMatrices); err != nil {
			return ctrl.Result{}, err
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
	terminal := len(pendingMatrices) == 0 && len(jobs.Items) == expectedObjects && status.Succeeded+status.Failed+status.TimedOut+status.Skipped+status.Cancelled == status.Total
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

func (r *WorkflowRunReconciler) reconcileWorkflowJobGraph(ctx context.Context, run *actionsv1alpha1.WorkflowRun, workflowName string, inputValues map[string]any, variables any, eventPayload map[string]any, jobs []actionsv1alpha1.WorkflowJob, pendingMatrixSets ...map[string]struct{}) error {
	pendingMatrices := map[string]struct{}{}
	if len(pendingMatrixSets) > 0 {
		pendingMatrices = pendingMatrixSets[0]
	}
	jobsByID := make(map[string]*actionsv1alpha1.WorkflowJob, len(jobs))
	jobsByLogicalID := make(map[string][]*actionsv1alpha1.WorkflowJob, len(jobs))
	for index := range jobs {
		job := &jobs[index]
		if existing := jobsByID[job.Spec.JobID]; existing != nil {
			return fmt.Errorf("WorkflowJobs %q and %q both represent job %q in WorkflowRun %q", existing.Name, job.Name, job.Spec.JobID, run.Name)
		}
		jobsByID[job.Spec.JobID] = job
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
				if _, pending := pendingMatrices[dependency]; pending {
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
	maxParallel := matrix.MaxParallel
	if maxParallel == 0 {
		maxParallel = matrix.JobTotal
	}
	result := map[string]any{
		"job-index":    matrix.JobIndex,
		"job-total":    matrix.JobTotal,
		"fail-fast":    matrix.FailFast == nil || *matrix.FailFast,
		"max-parallel": maxParallel,
	}
	return result
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
	cancellationFinalizer := controllerutil.ContainsFinalizer(run, workflowRunCancellationFinalizer)
	checkFinalizer := controllerutil.ContainsFinalizer(run, workflowRunCheckFinalizer)
	scheduleFinalizer := controllerutil.ContainsFinalizer(run, workflowRunScheduleFinalizer)
	if !cancellationFinalizer && !checkFinalizer && !scheduleFinalizer {
		return ctrl.Result{}, nil
	}
	var reportError error
	if checkFinalizer {
		reportError = r.reconcileGitHubCheck(ctx, run)
	}
	if cancellationFinalizer {
		remaining, err := r.executionWorkloadsRemain(ctx, run)
		if err != nil {
			return ctrl.Result{}, err
		}
		if remaining {
			if reportError != nil {
				ctrl.LoggerFrom(ctx).Error(reportError, "GitHub Check reporting failed while workflow cleanup is pending")
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
	if checkFinalizer {
		switch {
		case reportError == nil:
			controllerutil.RemoveFinalizer(run, workflowRunCheckFinalizer)
		case r.githubCheckReportPermanentlyUnavailable(ctx, run, reportError):
			ctrl.LoggerFrom(ctx).Info("Skipping terminal GitHub Check report because reporting is unavailable", "error", reportError)
			controllerutil.RemoveFinalizer(run, workflowRunCheckFinalizer)
		default:
			retryReport = true
		}
	}
	finalizersChanged := cancellationFinalizer || (scheduleFinalizer && scheduleRemaining <= 0) || (checkFinalizer && !retryReport)
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

func (r *WorkflowRunReconciler) scheduleFinalizerRemaining(run *actionsv1alpha1.WorkflowRun) time.Duration {
	if run.CreationTimestamp.IsZero() {
		return 0
	}
	deadline := run.CreationTimestamp.UTC().Truncate(time.Minute).Add(time.Minute)
	return deadline.Sub(r.now().UTC())
}

func (r *WorkflowRunReconciler) githubCheckReportPermanentlyUnavailable(ctx context.Context, run *actionsv1alpha1.WorkflowRun, reportError error) bool {
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
		Owns(&actionsv1alpha1.WorkflowJob{}).
		Complete(r)
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
