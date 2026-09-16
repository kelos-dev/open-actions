package controller

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	actionsv1alpha1 "github.com/kelos-dev/open-actions/api/v1alpha1"
	"github.com/kelos-dev/open-actions/internal/eventsnapshot"
	githubclient "github.com/kelos-dev/open-actions/internal/github"
	corev1 "k8s.io/api/core/v1"
	apiEquality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	workflowRunGitHubStatusFinalizer = "actions.kelos.dev/github-status"
	maxCommitStatusContextRunes      = 100
	githubStatusOwnerPrefix          = "ghso-"
	githubStatusOwnerDataKey         = "owner.json"
	githubStatusLeaseDuration        = 5 * time.Minute
	workflowRunGitHubStatusKeyIndex  = "actions.kelos.dev/workflow-run-github-status-key"
)

var errGitHubProjectIdentityMismatch = errors.New("GitHub reporting Project identity changed")

// GitHubStatusReconciler publishes job statuses and workflow validation results
// independently of workflow execution.
type GitHubStatusReconciler struct {
	client.Client
	APIReader  client.Reader
	GitHub     *githubclient.Client
	ConsoleURL string
	Now        func() time.Time
	Recorder   events.EventRecorder
}

func (r *GitHubStatusReconciler) Reconcile(ctx context.Context, request ctrl.Request) (result ctrl.Result, reconcileErr error) {
	defer func() {
		result, reconcileErr = requeueAfterGitHubRateLimit(ctx, result, reconcileErr, r.now())
	}()
	run := &actionsv1alpha1.WorkflowRun{}
	if err := r.APIReader.Get(ctx, request.NamespacedName, run); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !controllerutil.ContainsFinalizer(run, workflowRunGitHubStatusFinalizer) {
		return ctrl.Result{}, nil
	}
	if !run.DeletionTimestamp.IsZero() {
		return r.finalizeGitHubStatus(ctx, run)
	}
	return ctrl.Result{}, r.reconcileGitHubStatuses(ctx, run)
}

func (r *GitHubStatusReconciler) finalizeGitHubStatus(ctx context.Context, run *actionsv1alpha1.WorkflowRun) (ctrl.Result, error) {
	if controllerutil.ContainsFinalizer(run, eventsnapshot.RerunProtectionFinalizer) {
		return ctrl.Result{}, nil
	}
	reportError := r.reconcileGitHubStatuses(ctx, run)
	if reportError != nil && !r.githubReportPermanentlyUnavailable(ctx, run, reportError) {
		return ctrl.Result{}, reportError
	}
	if controllerutil.ContainsFinalizer(run, workflowRunCancellationFinalizer) {
		return ctrl.Result{}, nil
	}
	if err := r.releaseGitHubStatusOwnershipIfUnused(ctx, run); err != nil {
		return ctrl.Result{}, errors.Join(reportError, fmt.Errorf("release GitHub status ownership for WorkflowRun %q: %w", run.Name, err))
	}
	if reportError != nil {
		ctrl.LoggerFrom(ctx).Info("Skipping terminal GitHub report because reporting is unavailable", "workflow_run", run.Name, "error", reportError)
	}
	before := run.DeepCopy()
	controllerutil.RemoveFinalizer(run, workflowRunGitHubStatusFinalizer)
	return ctrl.Result{}, r.Patch(ctx, run, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{}))
}

func (r *GitHubStatusReconciler) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

func (r *GitHubStatusReconciler) SetupWithManager(manager ctrl.Manager) error {
	if r.Client == nil || r.APIReader == nil || r.GitHub == nil {
		return errors.New("Kubernetes client, API reader, and GitHub client must be specified")
	}
	if err := manager.GetFieldIndexer().IndexField(context.Background(), &actionsv1alpha1.WorkflowRun{}, workflowRunGitHubStatusKeyIndex, indexWorkflowRunGitHubStatusKey); err != nil {
		return err
	}
	return ctrl.NewControllerManagedBy(manager).
		Named("githubstatus").
		For(&actionsv1alpha1.WorkflowRun{}).
		Owns(&actionsv1alpha1.WorkflowJob{}, builder.WithPredicates(predicate.Funcs{UpdateFunc: workflowJobExecutionChanged})).
		Watches(&actionsv1alpha1.WorkflowRun{}, handler.EnqueueRequestsFromMapFunc(r.workflowRunsSharingGitHubStatus)).
		Complete(r)
}

func indexWorkflowRunGitHubStatusKey(object client.Object) []string {
	if key := object.GetLabels()[actionsv1alpha1.LabelGitHubStatusKey]; key != "" {
		return []string{key}
	}
	return nil
}

func (r *GitHubStatusReconciler) workflowRunsSharingGitHubStatus(ctx context.Context, object client.Object) []reconcile.Request {
	run, ok := object.(*actionsv1alpha1.WorkflowRun)
	if !ok {
		return nil
	}
	statusKey := run.Labels[actionsv1alpha1.LabelGitHubStatusKey]
	if statusKey == "" {
		return nil
	}
	runs := &actionsv1alpha1.WorkflowRunList{}
	if err := r.List(ctx, runs, client.InNamespace(run.Namespace), client.MatchingFields{workflowRunGitHubStatusKeyIndex: statusKey}); err != nil {
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

func githubStatusEnabled(run *actionsv1alpha1.WorkflowRun) bool {
	planned := meta.FindStatusCondition(run.Status.Conditions, actionsv1alpha1.WorkflowRunConditionPlanned)
	if run.Spec.Rerun != nil && planned != nil && planned.Status == metav1.ConditionFalse && planned.Reason == "RerunInvalid" {
		return false
	}
	if run.Spec.Source.Type != actionsv1alpha1.SourceTypeGitHub || run.Spec.Source.GitHub == nil || run.UID == "" {
		return false
	}
	switch run.Spec.Source.GitHub.Event.Name {
	case actionsv1alpha1.GitHubEventNamePush, actionsv1alpha1.GitHubEventNamePullRequest, actionsv1alpha1.GitHubEventNameMergeGroup:
		return true
	default:
		return false
	}
}

type commitStatusReport struct {
	State       string
	Description string
}

func (r *GitHubStatusReconciler) shouldReportGitHubStatuses(ctx context.Context, run *actionsv1alpha1.WorkflowRun) (bool, error) {
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

func (r *GitHubStatusReconciler) reconcileGitHubStatuses(ctx context.Context, run *actionsv1alpha1.WorkflowRun) error {
	validationReport := workflowValidationCommitStatusReport(run)
	if r.GitHub == nil || !githubStatusEnabled(run) || run.Status.WorkflowName == "" {
		return nil
	}
	jobs := &actionsv1alpha1.WorkflowJobList{}
	if err := r.APIReader.List(ctx, jobs, client.InNamespace(run.Namespace), client.MatchingLabels{actionsv1alpha1.LabelWorkflowRunUID: string(run.UID)}); err != nil {
		return fmt.Errorf("list WorkflowJobs for GitHub reporting on WorkflowRun %q: %w", run.Name, err)
	}
	sort.Slice(jobs.Items, func(left, right int) bool {
		return jobs.Items[left].Name < jobs.Items[right].Name
	})
	type commitStatusUpdate struct {
		resource     string
		job          *actionsv1alpha1.WorkflowJob
		request      githubclient.CreateCommitStatusRequest
		report       commitStatusReport
		reportDigest string
	}
	desired := make([]commitStatusUpdate, 0, len(jobs.Items))
	updates := make([]commitStatusUpdate, 0, len(jobs.Items))
	checkContextCollisions := false
	var validationRecovery *commitStatusUpdate
	for index := range jobs.Items {
		job := &jobs.Items[index]
		report := workflowJobCommitStatusReport(run, job)
		request := workflowJobCommitStatusRequest(r.ConsoleURL, run, job, report)
		digest := commitStatusReportDigest(request)
		current := workflowJobCommitStatus(job)
		if current == nil {
			checkContextCollisions = true
		}
		item := commitStatusUpdate{resource: fmt.Sprintf("WorkflowJob %q", job.Name), job: job, request: request, report: report, reportDigest: digest}
		desired = append(desired, item)
		if current != nil && current.State == actionsv1alpha1.GitHubCommitStatusState(report.State) && current.ReportDigest == digest {
			continue
		}
		updates = append(updates, item)
	}
	if validationReport != nil && (validationReport.State == "success" || len(jobs.Items) == 0) {
		report := *validationReport
		targetURL := ""
		if r.ConsoleURL != "" {
			targetURL = workflowRunConsoleURL(r.ConsoleURL, run)
		}
		request := githubclient.CreateCommitStatusRequest{
			State: report.State, Description: report.Description,
			Context:   githubWorkflowValidationContext(run.Spec.WorkflowPath),
			TargetURL: targetURL,
		}
		digest := commitStatusReportDigest(request)
		item := commitStatusUpdate{resource: fmt.Sprintf("WorkflowRun %q", run.Name), request: request, report: report, reportDigest: digest}
		current := workflowRunValidationStatus(run)
		if current == nil && report.State == "success" {
			validationRecovery = &item
		} else {
			desired = append(desired, item)
			if current == nil || string(current.State) != report.State || current.ReportDigest != digest {
				updates = append(updates, item)
			}
		}
	}
	if len(desired) == 0 {
		return nil
	}
	if checkContextCollisions {
		if err := r.warnGitHubJobStatusContextCollision(ctx, run, jobs.Items); err != nil {
			return err
		}
	}

	project := &actionsv1alpha1.Project{}
	projectKey := client.ObjectKey{Namespace: run.Namespace, Name: run.Spec.ProjectRef.Name}
	if err := r.APIReader.Get(ctx, projectKey, project); err != nil {
		return fmt.Errorf("get Project %q for GitHub statuses: %w", projectKey.Name, err)
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
	currentAttempt, err := r.shouldReportGitHubStatuses(ctx, run)
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
			return fmt.Errorf("read credentials for GitHub statuses on WorkflowRun %q: %w", run.Name, err)
		}
		installation, err := r.GitHub.CachedInstallation(ctx, githubConfig.AppID, githubConfig.InstallationID, privateKey, githubSource.Repository.Name, githubclient.InstallationPermissions{"statuses": "write"})
		if err != nil {
			return fmt.Errorf("authenticate GitHub status reporter for WorkflowRun %q: %w", run.Name, err)
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
		if validationRecovery != nil {
			status := recovered[strings.ToLower(validationRecovery.request.Context)]
			if status != nil && (status.State == "failure" || commitStatusMatches(status, validationRecovery.request)) {
				appBotLogin, err = r.GitHub.AppBotLogin(ctx, githubConfig.AppID, privateKey)
				if err != nil {
					return fmt.Errorf("identify GitHub validation status reporter for WorkflowRun %q: %w", run.Name, err)
				}
				if strings.EqualFold(status.Creator.Login, appBotLogin) {
					// Complete validation recovery before recording job updates so a failed
					// report remains retryable even when every job has finished.
					updates = append([]commitStatusUpdate{*validationRecovery}, updates...)
				}
			}
		}
		for _, item := range updates {
			status := recovered[strings.ToLower(item.request.Context)]
			if status != nil && commitStatusMatches(status, item.request) {
				if appBotLogin == "" {
					appBotLogin, err = r.GitHub.AppBotLogin(ctx, githubConfig.AppID, privateKey)
					if err != nil {
						return fmt.Errorf("identify GitHub status reporter for WorkflowRun %q: %w", run.Name, err)
					}
				}
				if strings.EqualFold(status.Creator.Login, appBotLogin) {
					if status.ID < 1 {
						return fmt.Errorf("GitHub returned an invalid commit-status ID for %s", item.resource)
					}
					if err := r.recordGitHubCommitStatus(ctx, run, item.job, item.report.State, item.reportDigest); err != nil {
						return fmt.Errorf("record GitHub status for %s: %w", item.resource, err)
					}
					continue
				}
			}
			status, err := installation.CreateCommitStatus(ctx, githubSource.Repository.Owner, githubSource.Repository.Name, githubStatusRevision(githubSource), item.request)
			if err != nil {
				return fmt.Errorf("report GitHub status for %s: %w", item.resource, err)
			}
			if status == nil || status.ID < 1 {
				return fmt.Errorf("GitHub returned an invalid commit-status ID for %s", item.resource)
			}
			if err := r.recordGitHubCommitStatus(ctx, run, item.job, item.report.State, item.reportDigest); err != nil {
				return fmt.Errorf("record GitHub status for %s: %w", item.resource, err)
			}
		}
		return nil
	}()
	return errors.Join(reportError, r.releaseGitHubStatusLease(ctx, run.Namespace, statusKey, leaseToken))
}

func commitStatusMatches(status *githubclient.CommitStatus, request githubclient.CreateCommitStatusRequest) bool {
	return strings.EqualFold(status.Context, request.Context) && status.State == request.State && status.TargetURL == request.TargetURL && status.Description == request.Description
}

func (r *GitHubStatusReconciler) ensureGitHubStatusIdentityLabels(ctx context.Context, run *actionsv1alpha1.WorkflowRun, projectUID types.UID, statusKey string) (bool, error) {
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

func (r *GitHubStatusReconciler) githubStatusCurrentOwner(ctx context.Context, run *actionsv1alpha1.WorkflowRun, project *actionsv1alpha1.Project, statusKey string) (bool, string, error) {
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

func (r *GitHubStatusReconciler) claimGitHubStatusOwnership(ctx context.Context, run *actionsv1alpha1.WorkflowRun, project *actionsv1alpha1.Project, statusKey string) (bool, string, error) {
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

func (r *GitHubStatusReconciler) githubStatusOwnerHasLiveRun(ctx context.Context, run *actionsv1alpha1.WorkflowRun, statusKey, ownerUID string) (bool, error) {
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

func (r *GitHubStatusReconciler) githubStatusOwnershipMatchesRun(ctx context.Context, run *actionsv1alpha1.WorkflowRun, project *actionsv1alpha1.Project, statusKey string) (bool, error) {
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

func (r *GitHubStatusReconciler) releaseGitHubStatusLease(ctx context.Context, namespace, statusKey, leaseToken string) error {
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

func (r *GitHubStatusReconciler) releaseGitHubStatusLeaseForRun(ctx context.Context, run *actionsv1alpha1.WorkflowRun, statusKey string) error {
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

func (r *GitHubStatusReconciler) releaseGitHubStatusOwnershipIfUnused(ctx context.Context, run *actionsv1alpha1.WorkflowRun) error {
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
		strings.EqualFold(githubWorkflowPathIdentity(left.Spec.WorkflowPath), githubWorkflowPathIdentity(right.Spec.WorkflowPath))
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

func githubWorkflowPathIdentity(workflowPath string) string {
	context := "Open Actions / " + workflowPath
	return boundedGitHubStatusContext(context, workflowPath != strings.ToLower(workflowPath))
}

func githubWorkflowValidationContext(workflowPath string) string {
	context := "Open Actions validation / " + workflowPath
	return boundedGitHubStatusContext(context, workflowPath != strings.ToLower(workflowPath))
}

func githubWorkflowDisplayName(run *actionsv1alpha1.WorkflowRun) string {
	if run.Status.WorkflowName != "" {
		return run.Status.WorkflowName
	}
	return run.Spec.WorkflowPath
}

func (r *GitHubStatusReconciler) warnGitHubJobStatusContextCollision(ctx context.Context, run *actionsv1alpha1.WorkflowRun, jobs []actionsv1alpha1.WorkflowJob) error {
	if r.Recorder == nil {
		return nil
	}
	runs := &actionsv1alpha1.WorkflowRunList{}
	if err := r.APIReader.List(ctx, runs, client.InNamespace(run.Namespace)); err != nil {
		return fmt.Errorf("list WorkflowRuns for GitHub job status contexts on WorkflowRun %q: %w", run.Name, err)
	}
	source := run.Spec.Source.GitHub
	for index := range runs.Items {
		candidate := &runs.Items[index]
		candidateSource := candidate.Spec.Source.GitHub
		if candidate.UID == run.UID || candidate.Spec.ProjectRef != run.Spec.ProjectRef || candidateSource == nil || !githubStatusEnabled(candidate) ||
			candidateSource.Repository.ID != source.Repository.ID || githubStatusRevision(candidateSource) != githubStatusRevision(source) ||
			candidate.Spec.WorkflowPath == run.Spec.WorkflowPath || candidate.Status.WorkflowName == "" {
			continue
		}
		candidateJobs := &actionsv1alpha1.WorkflowJobList{}
		if err := r.APIReader.List(ctx, candidateJobs, client.InNamespace(run.Namespace), client.MatchingLabels{actionsv1alpha1.LabelWorkflowRunUID: string(candidate.UID)}); err != nil {
			return fmt.Errorf("list WorkflowJobs for GitHub status context collision with WorkflowRun %q: %w", candidate.Name, err)
		}
		for currentIndex := range jobs {
			contextName := githubJobStatusContext(run, &jobs[currentIndex])
			for candidateIndex := range candidateJobs.Items {
				candidateJob := &candidateJobs.Items[candidateIndex]
				if !strings.EqualFold(githubJobStatusContext(candidate, candidateJob), contextName) {
					continue
				}
				r.Recorder.Eventf(run, candidateJob, corev1.EventTypeWarning, "GitHubStatusContextCollision", "ReportStatus",
					"GitHub job status context %q also identifies WorkflowJob %q from workflow %q; workflow and job names that report the same commit must produce unique contexts ignoring case", contextName, candidateJob.Name, candidate.Spec.WorkflowPath)
				return nil
			}
		}
	}
	return nil
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
	context := "Open Actions / " + githubWorkflowDisplayName(run) + " / " + displayName
	if displayName != jobID {
		context += " / " + jobID
	}
	caseIdentity := run.Spec.WorkflowPath + "\x00" + jobID
	return boundedGitHubStatusContext(context, caseIdentity != strings.ToLower(caseIdentity))
}

func boundedGitHubStatusContext(context string, disambiguateCase bool) string {
	digest := sha256.Sum256([]byte(context))
	suffix := fmt.Sprintf(" / %x", digest[:8])
	runes := []rune(context)
	if !disambiguateCase && len(runes) <= maxCommitStatusContextRunes {
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
	key := fmt.Sprintf("%s\x00%d\x00%s\x00%s", projectUID, source.Repository.ID, githubStatusRevision(source), strings.ToLower(githubWorkflowPathIdentity(run.Spec.WorkflowPath)))
	digest := sha256.Sum256([]byte(key))
	return strings.ToLower(digestEncoding.EncodeToString(digest[:]))
}

func commitStatusReportDigest(request githubclient.CreateCommitStatusRequest) string {
	data, _ := json.Marshal(request)
	digest := sha256.Sum256(data)
	return fmt.Sprintf("%x", digest)
}

func workflowJobCommitStatusReport(run *actionsv1alpha1.WorkflowRun, job *actionsv1alpha1.WorkflowJob) commitStatusReport {
	report := commitStatusReport{State: "pending", Description: "Queued"}
	succeeded := meta.FindStatusCondition(job.Status.Conditions, actionsv1alpha1.WorkflowJobConditionSucceeded)
	durationPrefix := "after"
	switch workflowJobResult(job) {
	case actionsv1alpha1.WorkflowJobResultSuccess:
		report.State = "success"
		report.Description = "Successful"
		durationPrefix = "in"
	case actionsv1alpha1.WorkflowJobResultFailure:
		report.State = "failure"
		report.Description = "Failing"
		if workflowJobTimedOut(job) {
			report.State = "error"
			report.Description = "Timed out"
		} else if succeeded != nil && succeeded.Reason == "JobCancelled" {
			report.State = "error"
			report.Description = "Cancelled"
		}
	case actionsv1alpha1.WorkflowJobResultSkipped:
		return commitStatusReport{State: "success", Description: "Skipped"}
	case actionsv1alpha1.WorkflowJobResultCancelled:
		report.State = "error"
		report.Description = "Cancelled"
	case "":
		switch {
		case !run.DeletionTimestamp.IsZero():
			report.State = "error"
			report.Description = "Cancelled"
		case job.Status.StartTime != nil:
			report.Description = "In progress"
		}
	}
	if report.State != "pending" && job.Status.StartTime != nil && job.Status.CompletionTime != nil {
		duration := job.Status.CompletionTime.Sub(job.Status.StartTime.Time)
		if duration >= 0 {
			report.Description += " " + durationPrefix + " " + githubStatusDuration(duration)
		}
	}
	return report
}

func githubStatusDuration(duration time.Duration) string {
	seconds := int64(duration / time.Second)
	parts := []string{}
	if hours := seconds / 3600; hours > 0 {
		parts = append(parts, fmt.Sprintf("%dh", hours))
	}
	if minutes := seconds / 60 % 60; minutes > 0 {
		parts = append(parts, fmt.Sprintf("%dm", minutes))
	}
	if seconds%60 > 0 || len(parts) == 0 {
		parts = append(parts, fmt.Sprintf("%ds", seconds%60))
	}
	return strings.Join(parts, " ")
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

func workflowValidationCommitStatusReport(run *actionsv1alpha1.WorkflowRun) *commitStatusReport {
	planned := meta.FindStatusCondition(run.Status.Conditions, actionsv1alpha1.WorkflowRunConditionPlanned)
	if planned == nil {
		return nil
	}
	if planned.Status == metav1.ConditionTrue {
		return &commitStatusReport{State: "success", Description: "Workflow validation passed"}
	}
	if planned.Status == metav1.ConditionFalse && planned.Reason == "WorkflowInvalid" {
		return &commitStatusReport{State: "failure", Description: "Workflow validation failed"}
	}
	return nil
}

func workflowRunValidationStatus(run *actionsv1alpha1.WorkflowRun) *actionsv1alpha1.GitHubCommitStatus {
	if run.Status.Source == nil || run.Status.Source.GitHub == nil {
		return nil
	}
	return run.Status.Source.GitHub.ValidationStatus
}

func (r *GitHubStatusReconciler) recordGitHubCommitStatus(ctx context.Context, run *actionsv1alpha1.WorkflowRun, job *actionsv1alpha1.WorkflowJob, state, reportDigest string) error {
	if job != nil {
		return r.recordGitHubJobCommitStatus(ctx, job, state, reportDigest)
	}
	before := run.DeepCopy()
	if run.Status.Source == nil {
		run.Status.Source = &actionsv1alpha1.WorkflowRunSourceStatus{}
	}
	if run.Status.Source.GitHub == nil {
		run.Status.Source.GitHub = &actionsv1alpha1.GitHubWorkflowRunStatus{}
	}
	run.Status.Source.GitHub.ValidationStatus = &actionsv1alpha1.GitHubCommitStatus{State: actionsv1alpha1.GitHubCommitStatusState(state), ReportDigest: reportDigest}
	return r.Status().Patch(ctx, run, client.MergeFrom(before))
}

func workflowJobCommitStatus(job *actionsv1alpha1.WorkflowJob) *actionsv1alpha1.GitHubCommitStatus {
	if job.Status.Source == nil || job.Status.Source.GitHub == nil {
		return nil
	}
	return job.Status.Source.GitHub.CommitStatus
}

func (r *GitHubStatusReconciler) recordGitHubJobCommitStatus(ctx context.Context, job *actionsv1alpha1.WorkflowJob, state, reportDigest string) error {
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

func (r *GitHubStatusReconciler) githubReportPermanentlyUnavailable(ctx context.Context, run *actionsv1alpha1.WorkflowRun, reportError error) bool {
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
