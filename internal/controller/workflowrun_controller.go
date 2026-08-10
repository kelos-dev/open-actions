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
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const (
	jobPlanKey                       = "job.json"
	maxJobPlanBytes                  = 900_000
	resourceNameMaxLength            = 63
	workflowJobNameDigestLength      = 16
	workflowRunCancellationFinalizer = "actions.kelos.dev/concurrency-cancellation"
	workflowRunCheckFinalizer        = "actions.kelos.dev/github-check"
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
}

func (r *WorkflowRunReconciler) Reconcile(ctx context.Context, request ctrl.Request) (ctrl.Result, error) {
	run := &actionsv1alpha1.WorkflowRun{}
	if err := r.Get(ctx, request.NamespacedName, run); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
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
		if result.Requeue || result.RequeueAfter > 0 {
			ctrl.LoggerFrom(ctx).Error(reportError, "GitHub Check reporting failed while workflow reconciliation is pending")
			return result, nil
		}
		return result, reportError
	}
	return result, nil
}

func (r *WorkflowRunReconciler) githubCheckEnabled(run *actionsv1alpha1.WorkflowRun) bool {
	return r.GitHub != nil && run.Spec.Source.Type == actionsv1alpha1.SourceTypeGitHub && run.Spec.Source.GitHub != nil && run.UID != ""
}

func (r *WorkflowRunReconciler) reconcileWorkflowRun(ctx context.Context, run *actionsv1alpha1.WorkflowRun) (ctrl.Result, error) {
	if terminalRun(run) {
		return ctrl.Result{}, nil
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
	concurrencyGroup, cancelInProgress, err := workflow.EvaluateConcurrency(definition, workflow.Event{
		Name: string(githubSource.Event.Name), Action: githubSource.Event.Action, RefName: githubclient.RefName(githubSource.Revision.Ref), HeadRef: githubSource.Revision.HeadRef,
	})
	if err != nil {
		return r.planningFailed(ctx, run, "WorkflowInvalid", err, planningFailureTerminal)
	}
	plannedJobs, err := r.planWorkflowJobs(run, definition)
	if err != nil {
		return r.planningFailed(ctx, run, "WorkflowInvalid", err, planningFailureTerminal)
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
	detailsURL := ""
	if r.ConsoleURL != "" {
		detailsURL = workflowRunConsoleURL(r.ConsoleURL, run)
	}
	name := "Open Actions / " + run.Spec.WorkflowPath
	externalID := string(run.UID)
	createRequest := githubclient.CreateCheckRunRequest{
		Name:        name,
		HeadSHA:     githubSource.Revision.SHA,
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
		checkRun, err = installation.FindCheckRun(ctx, githubSource.Repository.Owner, githubSource.Repository.Name, githubSource.Revision.SHA, githubConfig.AppID, externalID)
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

func checkRunReportDigest(request githubclient.CreateCheckRunRequest) string {
	data, _ := json.Marshal(request)
	digest := sha256.Sum256(data)
	return fmt.Sprintf("%x", digest)
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
		report.Conclusion = "failure"
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
		report.Output.Text = fmt.Sprintf("Jobs: %d total, %d queued, %d active, %d succeeded, %d failed.", jobs.Total, jobs.Queued, jobs.Active, jobs.Succeeded, jobs.Failed)
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
	id          string
	displayName string
	runsOn      []string
	plan        string
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
		workflowJob := &actionsv1alpha1.WorkflowJob{
			ObjectMeta: metav1.ObjectMeta{
				Name:        workflowJobName(run.Name, id),
				Namespace:   run.Namespace,
				Labels:      labels,
				Annotations: map[string]string{actionsv1alpha1.AnnotationProjectName: project.Name},
			},
			Spec: actionsv1alpha1.WorkflowJobSpec{
				WorkflowRunRef: corev1.LocalObjectReference{Name: run.Name},
				JobID:          id,
				DisplayName:    item.displayName,
				RunsOn:         append([]string(nil), item.runsOn...),
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

func (r *WorkflowRunReconciler) planWorkflowJobs(run *actionsv1alpha1.WorkflowRun, definition *workflow.Definition) ([]plannedWorkflowJob, error) {
	jobIDs := make([]string, 0, len(definition.Jobs))
	for id := range definition.Jobs {
		jobIDs = append(jobIDs, id)
	}
	sort.Strings(jobIDs)
	plannedJobs := make([]plannedWorkflowJob, 0, len(jobIDs))
	for _, id := range jobIDs {
		definitionJob := definition.Jobs[id]
		displayName := definitionJob.Name
		if displayName == "" {
			displayName = id
		}
		plan, err := r.jobPlan(run, definition.Name, id, definitionJob)
		if err != nil {
			return nil, err
		}
		data, err := json.Marshal(plan)
		if err != nil {
			return nil, fmt.Errorf("encode job plan: %w", err)
		}
		if len(data) > maxJobPlanBytes {
			return nil, fmt.Errorf("job plan for %q exceeds %d bytes", id, maxJobPlanBytes)
		}
		plannedJobs = append(plannedJobs, plannedWorkflowJob{id: id, displayName: displayName, runsOn: append([]string(nil), definitionJob.RunsOn...), plan: string(data)})
	}
	return plannedJobs, nil
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

func (r *WorkflowRunReconciler) jobPlan(run *actionsv1alpha1.WorkflowRun, workflowName, id string, job workflow.Job) (*runner.Plan, error) {
	githubSource := run.Spec.Source.GitHub
	jobEnv, err := stringMap(job.Env)
	if err != nil {
		return nil, fmt.Errorf("job %q env: %w", id, err)
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
			Name:             step.Name,
			Uses:             step.Uses,
			Run:              step.Run,
			WorkingDirectory: step.WorkingDirectory,
			With:             with,
			Env:              environment,
		})
	}
	return &runner.Plan{
		Version: runner.PlanVersion,
		Repository: runner.Repository{
			ID:                 githubSource.Repository.ID,
			Owner:              githubSource.Repository.Owner,
			Name:               githubSource.Repository.Name,
			ServerURL:          strings.TrimSuffix(r.GitHubServerURL, "/"),
			APIURL:             strings.TrimSuffix(r.GitHubAPIBase, "/"),
			ActionCloneBaseURL: strings.TrimSuffix(r.ActionCloneBaseURL, "/"),
		},
		Event: runner.Event{
			Name:       string(githubSource.Event.Name),
			Action:     githubSource.Event.Action,
			DeliveryID: githubSource.Event.DeliveryID,
		},
		WorkflowName: workflowName,
		Revision: runner.Revision{
			SHA:     githubSource.Revision.SHA,
			Ref:     githubSource.Revision.Ref,
			RefName: githubclient.RefName(githubSource.Revision.Ref),
			HeadRef: githubSource.Revision.HeadRef,
		},
		JobID: id,
		Env:   jobEnv,
		Steps: steps,
	}, nil
}

func (r *WorkflowRunReconciler) observeWorkflowJobs(ctx context.Context, run *actionsv1alpha1.WorkflowRun, workflowName, concurrencyGroup string, total int32) (ctrl.Result, error) {
	reader := r.APIReader
	jobs := &actionsv1alpha1.WorkflowJobList{}
	if err := reader.List(ctx, jobs, client.InNamespace(run.Namespace), client.MatchingLabels{actionsv1alpha1.LabelWorkflowRunUID: string(run.UID)}); err != nil {
		return ctrl.Result{}, err
	}
	status := &actionsv1alpha1.WorkflowRunJobStatus{Total: total}
	var startTime *metav1.Time
	lostState := ""
	waitingForRuntimeState := false
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
	for index := range jobs.Items {
		job := &jobs.Items[index]
		condition := meta.FindStatusCondition(job.Status.Conditions, actionsv1alpha1.WorkflowJobConditionSucceeded)
		if condition == nil || (condition.Status != metav1.ConditionTrue && condition.Status != metav1.ConditionFalse) {
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
		switch {
		case condition != nil && condition.Status == metav1.ConditionTrue:
			status.Succeeded++
		case condition != nil && condition.Status == metav1.ConditionFalse:
			status.Failed++
		case job.Status.RunnerRef != nil:
			status.Active++
		default:
			status.Queued++
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
	terminal := int32(len(jobs.Items)) == total && status.Succeeded+status.Failed == status.Total
	switch {
	case terminal && status.Failed > 0:
		now := metav1.Now()
		run.Status.CompletionTime = &now
		meta.SetStatusCondition(&run.Status.Conditions, metav1.Condition{Type: actionsv1alpha1.WorkflowRunConditionSucceeded, Status: metav1.ConditionFalse, ObservedGeneration: run.Generation, Reason: "JobFailed", Message: "At least one WorkflowJob failed"})
	case terminal && status.Succeeded == status.Total:
		now := metav1.Now()
		run.Status.CompletionTime = &now
		meta.SetStatusCondition(&run.Status.Conditions, metav1.Condition{Type: actionsv1alpha1.WorkflowRunConditionSucceeded, Status: metav1.ConditionTrue, ObservedGeneration: run.Generation, Reason: "JobsSucceeded", Message: "All WorkflowJobs succeeded"})
	case status.Queued > 0:
		meta.SetStatusCondition(&run.Status.Conditions, metav1.Condition{Type: actionsv1alpha1.WorkflowRunConditionSucceeded, Status: metav1.ConditionUnknown, ObservedGeneration: run.Generation, Reason: "JobsQueued", Message: "WorkflowJobs are waiting for matching Runners"})
	default:
		meta.SetStatusCondition(&run.Status.Conditions, metav1.Condition{Type: actionsv1alpha1.WorkflowRunConditionSucceeded, Status: metav1.ConditionUnknown, ObservedGeneration: run.Generation, Reason: "JobsRunning", Message: "WorkflowJobs are still running"})
	}
	if !apiEquality.Semantic.DeepEqual(before, &run.Status) {
		if err := r.Status().Update(ctx, run); err != nil {
			return ctrl.Result{}, err
		}
	}
	if waitingForRuntimeState {
		return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
	}
	return ctrl.Result{}, nil
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
	if !controllerutil.ContainsFinalizer(run, workflowRunCancellationFinalizer) {
		before := run.DeepCopy()
		controllerutil.AddFinalizer(run, workflowRunCancellationFinalizer)
		if err := r.Patch(ctx, run, client.MergeFrom(before)); err != nil {
			return err
		}
	}
	policy := metav1.DeletePropagationForeground
	if err := r.Delete(ctx, run, &client.DeleteOptions{PropagationPolicy: &policy}); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	return nil
}

func (r *WorkflowRunReconciler) finalizeCanceledWorkflowRun(ctx context.Context, run *actionsv1alpha1.WorkflowRun) (ctrl.Result, error) {
	cancellationFinalizer := controllerutil.ContainsFinalizer(run, workflowRunCancellationFinalizer)
	checkFinalizer := controllerutil.ContainsFinalizer(run, workflowRunCheckFinalizer)
	if !cancellationFinalizer && !checkFinalizer {
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
	finalizersChanged := cancellationFinalizer || (checkFinalizer && !retryReport)
	if finalizersChanged {
		if err := r.Patch(ctx, run, client.MergeFrom(before)); err != nil {
			return ctrl.Result{}, err
		}
	}
	if retryReport {
		return ctrl.Result{}, reportError
	}
	return ctrl.Result{}, nil
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
