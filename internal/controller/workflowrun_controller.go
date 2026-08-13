package controller

import (
	"context"
	"crypto/sha256"
	"encoding/base32"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"time"

	actionsv1alpha1 "github.com/kelos-dev/open-actions/api/v1alpha1"
	workflowexpression "github.com/kelos-dev/open-actions/internal/expression"
	githubclient "github.com/kelos-dev/open-actions/internal/github"
	"github.com/kelos-dev/open-actions/internal/runner"
	"github.com/kelos-dev/open-actions/internal/workflow"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apiEquality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const (
	jobPlanKey                       = "job.json"
	maxJobPlanBytes                  = 900_000
	resourceNameMaxLength            = 63
	workflowJobNameDigestLength      = 16
	workflowJobDisplayNameMaxLength  = 256
	workflowJobIDMaxLength           = 256
	workflowRunCancellationFinalizer = "actions.kelos.dev/concurrency-cancellation"
	workflowRunCheckFinalizer        = "actions.kelos.dev/github-check"
	workflowRunScheduleFinalizer     = "actions.kelos.dev/schedule-idempotency"
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

type WorkflowRunReconciler struct {
	client.Client
	APIReader          client.Reader
	GitHub             *githubclient.Client
	GitHubAPIBase      string
	GitHubServerURL    string
	ActionCloneBaseURL string
	ConsoleURL         string
	Now                func() time.Time
	Recorder           events.EventRecorder
}

func (r *WorkflowRunReconciler) Reconcile(ctx context.Context, request ctrl.Request) (ctrl.Result, error) {
	run := &actionsv1alpha1.WorkflowRun{}
	if err := r.Get(ctx, request.NamespacedName, run); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if run.Spec.Rerun != nil && !terminalRun(run) {
		if err := r.validateWorkflowRunRerun(ctx, run); err != nil {
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

func (r *WorkflowRunReconciler) validateWorkflowRunRerun(ctx context.Context, run *actionsv1alpha1.WorkflowRun) error {
	rerun := run.Spec.Rerun
	previous := &actionsv1alpha1.WorkflowRun{}
	key := client.ObjectKey{Namespace: run.Namespace, Name: rerun.PreviousRunRef.Name}
	if err := r.APIReader.Get(ctx, key, previous); err != nil {
		if apierrors.IsNotFound(err) {
			return &terminalPlanningError{cause: fmt.Errorf("previous WorkflowRun %q does not exist", key.Name)}
		}
		return err
	}
	if previous.UID != rerun.PreviousRunRef.UID {
		return &terminalPlanningError{cause: fmt.Errorf("previous WorkflowRun %q has a different UID", previous.Name)}
	}
	if !terminalRun(previous) {
		return &terminalPlanningError{cause: fmt.Errorf("previous WorkflowRun %q is not complete", previous.Name)}
	}
	if !apiEquality.Semantic.DeepEqual(run.Spec.ProjectRef, previous.Spec.ProjectRef) ||
		!apiEquality.Semantic.DeepEqual(run.Spec.Source, previous.Spec.Source) || run.Spec.WorkflowPath != previous.Spec.WorkflowPath {
		return &terminalPlanningError{cause: fmt.Errorf("WorkflowRun %q does not match previous WorkflowRun %q", run.Name, previous.Name)}
	}

	previousAttempt := int32(1)
	if previous.Spec.Rerun == nil {
		if rerun.OriginalRunRef.Name != previous.Name || rerun.OriginalRunRef.UID != previous.UID {
			return &terminalPlanningError{cause: fmt.Errorf("original WorkflowRun reference does not identify previous WorkflowRun %q", previous.Name)}
		}
	} else {
		previousAttempt = previous.Spec.Rerun.Attempt
		if rerun.OriginalRunRef != previous.Spec.Rerun.OriginalRunRef {
			return &terminalPlanningError{cause: fmt.Errorf("original WorkflowRun reference does not match previous WorkflowRun %q", previous.Name)}
		}
	}
	if rerun.Attempt != previousAttempt+1 {
		return &terminalPlanningError{cause: fmt.Errorf("rerun attempt %d must follow attempt %d", rerun.Attempt, previousAttempt)}
	}
	return nil
}

func (r *WorkflowRunReconciler) reconcileWorkflowRun(ctx context.Context, run *actionsv1alpha1.WorkflowRun) (ctrl.Result, error) {
	if terminalRun(run) {
		return r.reconcileCompletedWorkflowRunTTL(ctx, run)
	}
	planned := meta.FindStatusCondition(run.Status.Conditions, actionsv1alpha1.WorkflowRunConditionPlanned)
	if planned != nil && planned.Status == metav1.ConditionTrue {
		if run.Status.Jobs == nil {
			return ctrl.Result{}, fmt.Errorf("planned WorkflowRun %q has no job summary", run.Name)
		}
		return r.observeWorkflowJobs(ctx, run, run.Status.WorkflowName, run.Status.ConcurrencyGroup, run.Status.Jobs.Total)
	}
	if waitingForConcurrencyCondition(planned) && run.Status.Jobs != nil {
		cancelInProgress := planned.Reason == "WaitingForConcurrencyCancellation"
		waiting, err := r.handleConcurrency(ctx, run, run.Status.ConcurrencyGroup, cancelInProgress)
		if err != nil {
			return ctrl.Result{}, err
		}
		if waiting {
			return r.waitingForConcurrency(ctx, run, run.Status.WorkflowName, run.Status.ConcurrencyGroup, run.Status.Jobs.Total, cancelInProgress)
		}
		return r.observeWorkflowJobs(ctx, run, run.Status.WorkflowName, run.Status.ConcurrencyGroup, run.Status.Jobs.Total)
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
	privateKey, err := secretValue(ctx, r.APIReader, project.Namespace, githubConfig.PrivateKeySecretRef)
	if err != nil {
		return r.planningFailed(ctx, run, "CredentialsUnavailable", err, planningFailureRetry)
	}
	installation, err := r.GitHub.Installation(ctx, githubConfig.AppID, githubConfig.InstallationID, privateKey, githubSource.Repository.Name, githubclient.InstallationPermissions{ContentsRead: true})
	if err != nil {
		return r.planningFailed(ctx, run, "GitHubAuthenticationFailed", err, planningFailureRetry)
	}
	workflowData, err := installation.GetFile(ctx, githubSource.Repository.Owner, githubSource.Repository.Name, run.Spec.WorkflowPath, githubSource.Revision.SHA)
	if err != nil {
		disposition := planningFailureRetry
		if githubAPIStatus(err, 404) {
			disposition = planningFailureTerminal
		}
		return r.planningFailed(ctx, run, "WorkflowFetchFailed", err, disposition)
	}
	definition, err := workflow.Parse(workflowData)
	if err != nil {
		return r.planningFailed(ctx, run, "WorkflowInvalid", err, planningFailureTerminal)
	}
	planningRun, planningEvent, err := resolvePlanningEvent(run, definition)
	if err != nil {
		return r.planningFailed(ctx, run, "TriggerInvalid", err, planningFailureTerminal)
	}
	concurrencyGroup, cancelInProgress, err := workflow.EvaluateConcurrency(definition, planningEvent)
	if err != nil {
		return r.planningFailed(ctx, run, "WorkflowInvalid", err, planningFailureTerminal)
	}
	plannedJobs, err := r.planWorkflowJobs(planningRun, definition, planningEvent.InputValues)
	if err != nil {
		return r.planningFailed(ctx, run, "WorkflowInvalid", err, planningFailureTerminal)
	}
	plannedJobs, err = selectRerunWorkflowJobs(run, plannedJobs)
	if err != nil {
		return r.planningFailed(ctx, run, "RerunInvalid", err, planningFailureTerminal)
	}
	jobCount := int32(len(plannedJobs))
	run.Status.WorkflowName = definition.Name
	run.Status.ConcurrencyGroup = concurrencyGroup
	run.Status.Jobs = &actionsv1alpha1.WorkflowRunJobStatus{Total: jobCount}
	if err := r.ensureWorkflowJobs(ctx, run, project, plannedJobs); err != nil {
		return r.planningFailed(ctx, run, "ChildCreationFailed", err, childCreationFailureDisposition(err))
	}
	waiting, err := r.handleConcurrency(ctx, run, concurrencyGroup, cancelInProgress)
	if err != nil {
		return r.planningFailed(ctx, run, "ConcurrencyCheckFailed", err, planningFailureRetry)
	}
	if waiting {
		return r.waitingForConcurrency(ctx, run, definition.Name, concurrencyGroup, jobCount, cancelInProgress)
	}
	return r.observeWorkflowJobs(ctx, run, definition.Name, concurrencyGroup, jobCount)
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

func resolvePlanningEvent(run *actionsv1alpha1.WorkflowRun, definition *workflow.Definition) (*actionsv1alpha1.WorkflowRun, workflow.Event, error) {
	githubSource := run.Spec.Source.GitHub
	event := workflowEvent(githubSource)
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
	installation, err := r.GitHub.Installation(ctx, githubConfig.AppID, githubConfig.InstallationID, privateKey, githubSource.Repository.Name, githubclient.InstallationPermissions{ChecksWrite: true})
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
		if succeeded.Reason == "JobCancelled" {
			report.Conclusion = "cancelled"
		} else {
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
		report.Output.Text = fmt.Sprintf("Jobs: %d total, %d waiting, %d queued, %d active, %d succeeded, %d failed, %d skipped, %d cancelled.", jobs.Total, jobs.Waiting, jobs.Queued, jobs.Active, jobs.Succeeded, jobs.Failed, jobs.Skipped, jobs.Cancelled)
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

type plannedWorkflowJob struct {
	id            string
	displayName   string
	runsOn        []string
	needs         []string
	condition     string
	matrix        *actionsv1alpha1.WorkflowJobMatrix
	plan          string
	resultVersion string
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
				Matrix:         item.matrix.DeepCopy(),
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

func (r *WorkflowRunReconciler) planWorkflowJobs(run *actionsv1alpha1.WorkflowRun, definition *workflow.Definition, inputValues map[string]any) ([]plannedWorkflowJob, error) {
	jobIDs := make([]string, 0, len(definition.Jobs))
	for id := range definition.Jobs {
		jobIDs = append(jobIDs, id)
	}
	sort.Strings(jobIDs)
	plannedJobs := make([]plannedWorkflowJob, 0, len(jobIDs))
	plannedIDs := make(map[string]struct{})
	sourceIDs := make(map[string]struct{}, len(jobIDs))
	for _, id := range jobIDs {
		sourceIDs[id] = struct{}{}
	}
	for _, id := range jobIDs {
		definitionJob := definition.Jobs[id]
		combinations := workflow.MatrixCombinations(definitionJob.Strategy)
		if len(combinations) == 0 {
			combinations = []map[string]any{nil}
		}
		for index, matrix := range combinations {
			expandedID := id
			var matrixSpec *actionsv1alpha1.WorkflowJobMatrix
			if matrix != nil {
				expandedID = uniqueMatrixWorkflowJobID(id, index, sourceIDs, plannedIDs)
				matrixSpec = &actionsv1alpha1.WorkflowJobMatrix{
					LogicalJobID: id,
					Values:       matrixStringValues(matrix),
					MaxParallel:  definitionJob.Strategy.MaxParallel,
					FailFast:     pointerTo(definitionJob.Strategy.FailFast),
				}
			}
			if _, found := plannedIDs[expandedID]; found {
				return nil, fmt.Errorf("expanded job ID %q is not unique", expandedID)
			}
			plannedIDs[expandedID] = struct{}{}

			expressionContext := r.jobExpressionContext(run, definition.Name, inputValues)
			if matrix != nil {
				expressionContext.Availability = workflowexpression.NewAvailability("github", "matrix", "inputs")
				expressionContext.Values["matrix"] = matrix
			}
			resolvedJob, err := workflow.EvaluateJob(id, definitionJob, expressionContext)
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
			plan, err := r.jobPlan(run, definition.Name, id, resolvedJob, matrix, inputValues)
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
			resultVersion := ""
			if len(resolvedJob.Outputs) > 0 {
				resultVersion = jobResultVersion
			}
			plannedJobs = append(plannedJobs, plannedWorkflowJob{
				id:            expandedID,
				displayName:   displayName,
				runsOn:        append([]string(nil), resolvedJob.RunsOn...),
				needs:         append([]string(nil), resolvedJob.Needs...),
				condition:     resolvedJob.If,
				matrix:        matrixSpec,
				plan:          string(data),
				resultVersion: resultVersion,
			})
		}
	}
	return plannedJobs, nil
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

func (r *WorkflowRunReconciler) jobExpressionContext(run *actionsv1alpha1.WorkflowRun, workflowName string, inputValues map[string]any) workflowexpression.Context {
	githubSource := run.Spec.Source.GitHub
	headRef, baseRef := githubSourcePullRequestRefs(githubSource)
	eventValues := githubEventExpressionValue(githubSource, inputValues)
	return workflowexpression.Context{
		Availability: workflowexpression.NewAvailability("github", "inputs"),
		Values: map[string]any{
			"inputs": inputValues,
			"github": map[string]any{
				"workflow":   workflowName,
				"event_name": string(githubSource.Event.Name),
				"event":      eventValues,
				"repository": githubSource.Repository.Owner + "/" + githubSource.Repository.Name,
				"sha":        githubSource.Revision.SHA,
				"ref":        githubSource.Revision.Ref,
				"ref_name":   githubclient.RefName(githubSource.Revision.Ref),
				"head_ref":   headRef,
				"base_ref":   baseRef,
				"server_url": strings.TrimSuffix(r.GitHubServerURL, "/"),
				"api_url":    strings.TrimSuffix(r.GitHubAPIBase, "/"),
			},
		},
	}
}

func githubEventExpressionValue(source *actionsv1alpha1.GitHubWorkflowRunSource, inputValues map[string]any) map[string]any {
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

func (r *WorkflowRunReconciler) jobPlan(run *actionsv1alpha1.WorkflowRun, workflowName, id string, job workflow.Job, matrix, inputValues map[string]any) (*runner.Plan, error) {
	githubSource := run.Spec.Source.GitHub
	headRef, baseRef := githubSourcePullRequestRefs(githubSource)
	jobEnv, err := stringMap(job.Env)
	if err != nil {
		return nil, fmt.Errorf("job %q env: %w", id, err)
	}
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
		})
	}
	return &runner.Plan{
		Version: runner.PlanVersion,
		Inputs:  inputValues,
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
			SHA:     githubSource.Revision.SHA,
			Ref:     githubSource.Revision.Ref,
			RefName: githubclient.RefName(githubSource.Revision.Ref),
			HeadRef: headRef,
			BaseRef: baseRef,
		},
		JobID:   id,
		Matrix:  matrix,
		Env:     jobEnv,
		Outputs: outputs,
		Steps:   steps,
	}, nil
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

func (r *WorkflowRunReconciler) observeWorkflowJobs(ctx context.Context, run *actionsv1alpha1.WorkflowRun, workflowName, concurrencyGroup string, total int32) (ctrl.Result, error) {
	reader := r.APIReader
	jobs := &actionsv1alpha1.WorkflowJobList{}
	if err := reader.List(ctx, jobs, client.InNamespace(run.Namespace), client.MatchingLabels{actionsv1alpha1.LabelWorkflowRunUID: string(run.UID)}); err != nil {
		return ctrl.Result{}, err
	}
	failFastPending, err := r.reconcileMatrixFailFast(ctx, jobs)
	if err != nil {
		return ctrl.Result{}, err
	}
	status := &actionsv1alpha1.WorkflowRunJobStatus{Total: total}
	var startTime *metav1.Time
	lostState := ""
	waitingForRuntimeState := false
	hasNonFailFastCancellation := false
	if int32(len(jobs.Items)) != total {
		active, err := activeRuntimeWorkloads(ctx, reader, run)
		if err != nil {
			return ctrl.Result{}, err
		}
		if active {
			waitingForRuntimeState = true
		} else {
			lostState = fmt.Sprintf("expected %d WorkflowJobs, found %d", total, len(jobs.Items))
		}
	}
	if int32(len(jobs.Items)) == total {
		inputValues, err := r.workflowJobGraphInputValues(ctx, jobs.Items)
		if err != nil {
			return ctrl.Result{}, err
		}
		if err := r.reconcileWorkflowJobGraph(ctx, run, workflowName, inputValues, jobs.Items); err != nil {
			return ctrl.Result{}, err
		}
	}
	for index := range jobs.Items {
		job := &jobs.Items[index]
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
		switch {
		case result == actionsv1alpha1.WorkflowJobResultSuccess:
			status.Succeeded++
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
	run.Status.ConcurrencyGroup = concurrencyGroup
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
	meta.SetStatusCondition(&run.Status.Conditions, metav1.Condition{
		Type:               actionsv1alpha1.WorkflowRunConditionPlanned,
		Status:             metav1.ConditionTrue,
		ObservedGeneration: run.Generation,
		Reason:             "JobsPlanned",
		Message:            "All WorkflowJobs have been created",
	})
	terminal := int32(len(jobs.Items)) == total && status.Succeeded+status.Failed+status.Skipped+status.Cancelled == status.Total
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
	case terminal && status.Failed > 0 && run.Spec.CancelRequested:
		now := metav1.Now()
		run.Status.CompletionTime = &now
		meta.SetStatusCondition(&run.Status.Conditions, metav1.Condition{Type: actionsv1alpha1.WorkflowRunConditionSucceeded, Status: metav1.ConditionFalse, ObservedGeneration: run.Generation, Reason: "JobCancelled", Message: "Workflow cancellation was requested"})
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
	if waitingForRuntimeState || failFastPending {
		return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
	}
	return ctrl.Result{}, nil
}

func (r *WorkflowRunReconciler) workflowJobGraphInputValues(ctx context.Context, jobs []actionsv1alpha1.WorkflowJob) (map[string]any, error) {
	for index := range jobs {
		job := &jobs[index]
		if workflowJobTerminal(job) || strings.TrimSpace(job.Spec.If) == "" {
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

func (r *WorkflowRunReconciler) reconcileWorkflowJobGraph(ctx context.Context, run *actionsv1alpha1.WorkflowRun, workflowName string, inputValues map[string]any, jobs []actionsv1alpha1.WorkflowJob) error {
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
				if err := r.reconcileAssignedWorkflowJobCancellation(ctx, run, workflowName, inputValues, job, jobsByLogicalID); err != nil {
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

		expressionContext := workflowexpression.Context{Status: workflowJobAncestorStatus(job, jobsByLogicalID, run.Spec.CancelRequested)}
		if strings.TrimSpace(job.Spec.If) != "" {
			expressionContext = r.jobExpressionContext(run, workflowName, inputValues)
			expressionContext.Values["needs"] = workflowNeedsContext(job, jobsByLogicalID)
			expressionContext.Status = workflowJobAncestorStatus(job, jobsByLogicalID, run.Spec.CancelRequested)
		}
		runnable, err := workflow.EvaluateJobCondition(job.Spec.JobID, job.Spec.If, expressionContext)
		if err != nil {
			if statusErr := r.completeUnscheduledWorkflowJob(ctx, job, actionsv1alpha1.WorkflowJobResultFailure, "ConditionEvaluationFailed", err.Error()); statusErr != nil {
				return statusErr
			}
			continue
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
		if err := r.setWorkflowJobReady(ctx, job); err != nil {
			return err
		}
	}
	return nil
}

func (r *WorkflowRunReconciler) reconcileAssignedWorkflowJobCancellation(ctx context.Context, run *actionsv1alpha1.WorkflowRun, workflowName string, inputValues map[string]any, job *actionsv1alpha1.WorkflowJob, jobs map[string][]*actionsv1alpha1.WorkflowJob) error {
	expressionContext := workflowexpression.Context{Status: workflowJobAncestorStatus(job, jobs, true)}
	if strings.TrimSpace(job.Spec.If) != "" {
		expressionContext = r.jobExpressionContext(run, workflowName, inputValues)
		expressionContext.Values["needs"] = workflowNeedsContext(job, jobs)
		expressionContext.Status = workflowJobAncestorStatus(job, jobs, true)
	}
	continueRunning, err := workflow.EvaluateJobCondition(job.Spec.JobID, job.Spec.If, expressionContext)
	if err != nil {
		return r.setWorkflowJobCancellationRequested(ctx, job, true, "ConditionEvaluationFailed", err.Error())
	}
	if continueRunning {
		return r.setWorkflowJobCancellationRequested(ctx, job, false, "ConditionPassed", "The workflow job condition permits execution during cancellation")
	}
	return r.setWorkflowJobCancellationRequested(ctx, job, true, "CancellationRequested", "The workflow job condition does not permit execution during cancellation")
}

func workflowNeedsContext(job *actionsv1alpha1.WorkflowJob, jobs map[string][]*actionsv1alpha1.WorkflowJob) map[string]any {
	needs := make(map[string]any, len(job.Spec.Needs))
	for _, dependency := range job.Spec.Needs {
		dependencyJobs := append([]*actionsv1alpha1.WorkflowJob(nil), jobs[dependency]...)
		sort.Slice(dependencyJobs, func(left, right int) bool {
			return dependencyJobs[left].Spec.JobID < dependencyJobs[right].Spec.JobID
		})
		outputs := map[string]any{}
		for _, dependencyJob := range dependencyJobs {
			for name, value := range dependencyJob.Status.Outputs {
				outputs[name] = value
			}
		}
		needs[dependency] = map[string]any{
			"result":  string(workflowJobGroupResult(dependencyJobs)),
			"outputs": outputs,
		}
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
	return len(job.Spec.Needs) == 0 && strings.TrimSpace(job.Spec.If) == ""
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

func (r *WorkflowRunReconciler) handleConcurrency(ctx context.Context, run *actionsv1alpha1.WorkflowRun, group string, cancelInProgress bool) (bool, error) {
	if group == "" {
		return false, nil
	}
	runs := &actionsv1alpha1.WorkflowRunList{}
	if err := r.List(ctx, runs, client.InNamespace(run.Namespace)); err != nil {
		return false, err
	}
	waiting := false
	for index := range runs.Items {
		other := &runs.Items[index]
		if other.UID == run.UID || terminalRun(other) || !sameConcurrencyScope(other, run) || !olderThan(other, run) {
			continue
		}
		planned := meta.FindStatusCondition(other.Status.Conditions, actionsv1alpha1.WorkflowRunConditionPlanned)
		if other.Status.ConcurrencyGroup == "" {
			if planned == nil || planned.Status == metav1.ConditionUnknown {
				waiting = true
			}
			continue
		}
		if !strings.EqualFold(other.Status.ConcurrencyGroup, group) {
			continue
		}
		if planned == nil {
			waiting = true
			continue
		}
		if planned.Status == metav1.ConditionFalse {
			continue
		}
		waiting = true
		pending := waitingForConcurrencyCondition(planned)
		if (cancelInProgress || pending) && other.DeletionTimestamp.IsZero() {
			if err := r.cancelWorkflowRun(ctx, other); err != nil {
				return false, err
			}
		}
	}
	return waiting, nil
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

func sameConcurrencyScope(left, right *actionsv1alpha1.WorkflowRun) bool {
	if left.Spec.ProjectRef.Name != right.Spec.ProjectRef.Name || left.Spec.Source.Type != right.Spec.Source.Type {
		return false
	}
	if left.Spec.Source.Type != actionsv1alpha1.SourceTypeGitHub || left.Spec.Source.GitHub == nil || right.Spec.Source.GitHub == nil {
		return false
	}
	return left.Spec.Source.GitHub.Repository.ID == right.Spec.Source.GitHub.Repository.ID
}

func (r *WorkflowRunReconciler) waitingForConcurrency(ctx context.Context, run *actionsv1alpha1.WorkflowRun, workflowName, group string, total int32, cancelInProgress bool) (ctrl.Result, error) {
	before := run.Status.DeepCopy()
	run.Status.ObservedGeneration = run.Generation
	run.Status.WorkflowName = workflowName
	run.Status.ConcurrencyGroup = group
	run.Status.Jobs = &actionsv1alpha1.WorkflowRunJobStatus{Total: total, Queued: total}
	reason := "WaitingForConcurrency"
	if cancelInProgress {
		reason = "WaitingForConcurrencyCancellation"
	}
	meta.SetStatusCondition(&run.Status.Conditions, metav1.Condition{
		Type:               actionsv1alpha1.WorkflowRunConditionPlanned,
		Status:             metav1.ConditionUnknown,
		ObservedGeneration: run.Generation,
		Reason:             reason,
		Message:            "An earlier WorkflowRun still owns the concurrency group",
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
	return ctrl.NewControllerManagedBy(manager).
		For(&actionsv1alpha1.WorkflowRun{}).
		Owns(&actionsv1alpha1.WorkflowJob{}).
		Complete(r)
}

func terminalRun(run *actionsv1alpha1.WorkflowRun) bool {
	condition := meta.FindStatusCondition(run.Status.Conditions, actionsv1alpha1.WorkflowRunConditionSucceeded)
	return condition != nil && (condition.Status == metav1.ConditionTrue || condition.Status == metav1.ConditionFalse)
}

func olderThan(left, right *actionsv1alpha1.WorkflowRun) bool {
	if left.CreationTimestamp.Time.Before(right.CreationTimestamp.Time) {
		return true
	}
	if right.CreationTimestamp.Time.Before(left.CreationTimestamp.Time) {
		return false
	}
	return left.Name < right.Name
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
		case bool, int, int64, uint64, float64:
			result[key] = fmt.Sprint(typed)
		default:
			return nil, fmt.Errorf("value %q must be a scalar", key)
		}
	}
	return result, nil
}
