package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"time"

	actionsv1alpha1 "github.com/kelos-dev/open-actions/api/v1alpha1"
	githubclient "github.com/kelos-dev/open-actions/internal/github"
	"github.com/kelos-dev/open-actions/internal/workflow"
	"github.com/robfig/cron/v3"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	maxScheduledRepositories = 1000
	maxScheduledWorkflows    = 100
	scheduleFailureRetry     = 5 * time.Second
)

type ScheduleReconciler struct {
	client.Client
	APIReader                          client.Reader
	GitHub                             *githubclient.Client
	Logger                             *slog.Logger
	WorkflowRunTTLSecondsAfterFinished *int32
	Now                                func() time.Time
	cacheMutex                         sync.Mutex
	cache                              map[scheduleCacheKey]scheduleCacheEntry
}

type scheduleCacheKey struct {
	Project      client.ObjectKey
	RepositoryID int64
}

type scheduleCacheEntry struct {
	ProjectUID        string
	WorkflowDirectory string
	DefaultBranch     string
	PushedAt          time.Time
	Revision          string
	Workflows         []scheduledWorkflow
	DiscoveryError    string
	Empty             bool
}

type scheduleRepositoryError struct {
	cause error
}

func (e scheduleRepositoryError) Error() string {
	return e.cause.Error()
}

func (e scheduleRepositoryError) Unwrap() error {
	return e.cause
}

func permanentScheduleRepositoryError(err error) error {
	return scheduleRepositoryError{cause: err}
}

func retryScheduleRepository(err error) bool {
	permanent := scheduleRepositoryError{}
	return !errors.As(err, &permanent)
}

type scheduledWorkflow struct {
	Path     string
	Triggers []scheduledTrigger
}

type scheduledTrigger struct {
	Expression string
	Schedule   cron.Schedule
}

func (r *ScheduleReconciler) Reconcile(ctx context.Context, request ctrl.Request) (ctrl.Result, error) {
	project := &actionsv1alpha1.Project{}
	if err := r.Get(ctx, request.NamespacedName, project); err != nil {
		if apierrors.IsNotFound(err) {
			r.clearScheduleCache(request.NamespacedName)
		}
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	now := r.now().UTC().Truncate(time.Minute)
	configured := meta.FindStatusCondition(project.Status.Conditions, actionsv1alpha1.ProjectConditionConfigured)
	if configured == nil || configured.Status != metav1.ConditionTrue || configured.ObservedGeneration != project.Generation {
		return ctrl.Result{RequeueAfter: r.nextMinute()}, nil
	}
	githubConfig := project.Spec.Source.GitHub
	privateKey, err := secretValue(ctx, r.APIReader, project.Namespace, githubConfig.PrivateKeySecretRef)
	if err != nil {
		return ctrl.Result{}, err
	}
	installation, err := r.GitHub.InstallationForAllRepositories(ctx, githubConfig.AppID, githubConfig.InstallationID, privateKey, githubclient.InstallationPermissions{ContentsRead: true})
	if err != nil {
		return ctrl.Result{}, err
	}
	repositories, err := installation.ListRepositories(ctx, maxScheduledRepositories+1)
	if err != nil {
		return ctrl.Result{}, err
	}
	if len(repositories) > maxScheduledRepositories {
		return ctrl.Result{}, fmt.Errorf("GitHub App installation contains more than %d repositories; scheduled discovery supports at most %d", maxScheduledRepositories, maxScheduledRepositories)
	}
	r.pruneScheduleCache(project, repositories)
	created := 0
	repositoryFailed := false
	for _, repository := range repositories {
		count, err := r.reconcileRepository(ctx, project, installation, repository, now)
		if err != nil {
			r.Logger.Warn("skipping repository during scheduled workflow discovery", "repository", repository.Owner.Login+"/"+repository.Name, "error", err)
			if retryScheduleRepository(err) {
				repositoryFailed = true
			}
			continue
		}
		created += count
	}
	if created > 0 {
		r.Logger.Info("created scheduled workflow runs", "project", project.Name, "namespace", project.Namespace, "workflow_runs", created)
	}
	if repositoryFailed {
		return r.scheduleFailureRequeue(now), nil
	}
	return r.scheduleRequeue(now), nil
}

func (r *ScheduleReconciler) reconcileRepository(ctx context.Context, project *actionsv1alpha1.Project, installation *githubclient.InstallationClient, repository githubclient.Repository, now time.Time) (int, error) {
	if repository.ID < 1 || repository.Owner.Login == "" || repository.Name == "" || repository.DefaultBranch == "" {
		return 0, permanentScheduleRepositoryError(errors.New("GitHub returned an incomplete installation repository"))
	}
	revision, scheduledWorkflows, err := r.scheduledWorkflows(ctx, project, installation, repository)
	if err != nil {
		return 0, err
	}
	created := 0
	for _, item := range scheduledWorkflows {
		for _, trigger := range item.Triggers {
			if !workflow.ScheduleMatches(trigger.Schedule, now) {
				continue
			}
			wasCreated, err := r.createWorkflowRun(ctx, project, repository, item.Path, trigger.Expression, revision, now)
			if err != nil {
				return 0, err
			}
			if wasCreated {
				created++
			}
		}
	}
	return created, nil
}

func (r *ScheduleReconciler) scheduledWorkflows(ctx context.Context, project *actionsv1alpha1.Project, installation *githubclient.InstallationClient, repository githubclient.Repository) (string, []scheduledWorkflow, error) {
	key := scheduleCacheKey{Project: client.ObjectKeyFromObject(project), RepositoryID: repository.ID}
	cached, found := r.loadScheduleCache(key)
	if found && cached.matches(project, repository) && cached.PushedAt.Equal(repository.PushedAt) && (cached.Empty || cached.DiscoveryError != "" || !repository.PushedAt.IsZero()) {
		if cached.DiscoveryError != "" {
			return "", nil, permanentScheduleRepositoryError(errors.New(cached.DiscoveryError))
		}
		return cached.Revision, cached.Workflows, nil
	}
	revision, err := installation.ResolveRevision(ctx, repository.Owner.Login, repository.Name, repository.DefaultBranch)
	if err != nil {
		if emptyRepositoryError(err) {
			r.storeScheduleCache(key, scheduleCacheEntry{
				ProjectUID: string(project.UID), WorkflowDirectory: project.Spec.WorkflowDirectory,
				DefaultBranch: repository.DefaultBranch, PushedAt: repository.PushedAt, Empty: true,
			})
			return "", nil, nil
		}
		return "", nil, err
	}
	if found && cached.matches(project, repository) && cached.Revision == revision {
		cached.PushedAt = repository.PushedAt
		r.storeScheduleCache(key, cached)
		return cached.Revision, cached.Workflows, nil
	}
	workflows, err := r.discoverScheduledWorkflows(ctx, project, installation, repository, revision)
	if err != nil {
		if !retryScheduleRepository(err) {
			r.storeScheduleCache(key, scheduleCacheEntry{
				ProjectUID: string(project.UID), WorkflowDirectory: project.Spec.WorkflowDirectory,
				DefaultBranch: repository.DefaultBranch, PushedAt: repository.PushedAt,
				Revision: revision, DiscoveryError: err.Error(),
			})
		}
		return "", nil, err
	}
	r.storeScheduleCache(key, scheduleCacheEntry{
		ProjectUID: string(project.UID), WorkflowDirectory: project.Spec.WorkflowDirectory,
		DefaultBranch: repository.DefaultBranch, PushedAt: repository.PushedAt,
		Revision: revision, Workflows: workflows,
	})
	return revision, workflows, nil
}

func emptyRepositoryError(err error) bool {
	apiError := &githubclient.APIError{}
	if errors.As(err, &apiError) && apiError.StatusCode == http.StatusConflict && strings.EqualFold(strings.TrimSpace(apiError.Message), "Git Repository is empty.") {
		return true
	}
	return strings.Contains(err.Error(), "GitHub returned no commits")
}

func (entry scheduleCacheEntry) matches(project *actionsv1alpha1.Project, repository githubclient.Repository) bool {
	return entry.ProjectUID == string(project.UID) && entry.WorkflowDirectory == project.Spec.WorkflowDirectory && entry.DefaultBranch == repository.DefaultBranch
}

func (r *ScheduleReconciler) discoverScheduledWorkflows(ctx context.Context, project *actionsv1alpha1.Project, installation *githubclient.InstallationClient, repository githubclient.Repository, revision string) ([]scheduledWorkflow, error) {
	contents, err := installation.ListDirectory(ctx, repository.Owner.Login, repository.Name, project.Spec.WorkflowDirectory, revision)
	if err != nil {
		apiError := &githubclient.APIError{}
		if errors.As(err, &apiError) && apiError.StatusCode == http.StatusNotFound && apiError.Message == "Not Found" {
			return nil, nil
		}
		return nil, err
	}
	workflowPaths := make([]string, 0, len(contents))
	for _, content := range contents {
		if content.Type == "file" {
			extension := filepath.Ext(content.Path)
			if extension == ".yaml" || extension == ".yml" {
				workflowPaths = append(workflowPaths, content.Path)
			}
		}
	}
	if len(workflowPaths) > maxScheduledWorkflows {
		return nil, permanentScheduleRepositoryError(fmt.Errorf("repository %s/%s contains %d workflow files; scheduled discovery supports at most %d", repository.Owner.Login, repository.Name, len(workflowPaths), maxScheduledWorkflows))
	}
	result := make([]scheduledWorkflow, 0, len(workflowPaths))
	for _, workflowPath := range workflowPaths {
		data, err := installation.GetFile(ctx, repository.Owner.Login, repository.Name, workflowPath, revision)
		if err != nil {
			return nil, err
		}
		definition, err := workflow.Parse(data)
		if err != nil {
			r.Logger.Warn("skipping invalid scheduled workflow", "repository", repository.Owner.Login+"/"+repository.Name, "path", workflowPath, "error", err)
			continue
		}
		filter, found := definition.On.Events["schedule"]
		if !found {
			continue
		}
		item := scheduledWorkflow{Path: workflowPath, Triggers: make([]scheduledTrigger, 0, len(filter.Schedules))}
		for _, configuredSchedule := range filter.Schedules {
			parsed, err := workflow.ParseCron(configuredSchedule.Cron)
			if err != nil {
				return nil, permanentScheduleRepositoryError(fmt.Errorf("parse validated schedule %q: %w", configuredSchedule.Cron, err))
			}
			item.Triggers = append(item.Triggers, scheduledTrigger{Expression: configuredSchedule.Cron, Schedule: parsed})
		}
		result = append(result, item)
	}
	return result, nil
}

func (r *ScheduleReconciler) loadScheduleCache(key scheduleCacheKey) (scheduleCacheEntry, bool) {
	r.cacheMutex.Lock()
	defer r.cacheMutex.Unlock()
	entry, found := r.cache[key]
	return entry, found
}

func (r *ScheduleReconciler) storeScheduleCache(key scheduleCacheKey, entry scheduleCacheEntry) {
	r.cacheMutex.Lock()
	defer r.cacheMutex.Unlock()
	if r.cache == nil {
		r.cache = map[scheduleCacheKey]scheduleCacheEntry{}
	}
	r.cache[key] = entry
}

func (r *ScheduleReconciler) pruneScheduleCache(project *actionsv1alpha1.Project, repositories []githubclient.Repository) {
	active := make(map[int64]struct{}, len(repositories))
	for _, repository := range repositories {
		active[repository.ID] = struct{}{}
	}
	projectKey := client.ObjectKeyFromObject(project)
	r.cacheMutex.Lock()
	defer r.cacheMutex.Unlock()
	for key := range r.cache {
		if key.Project == projectKey {
			if _, found := active[key.RepositoryID]; !found {
				delete(r.cache, key)
			}
		}
	}
}

func (r *ScheduleReconciler) clearScheduleCache(project client.ObjectKey) {
	r.cacheMutex.Lock()
	defer r.cacheMutex.Unlock()
	for key := range r.cache {
		if key.Project == project {
			delete(r.cache, key)
		}
	}
}

func (r *ScheduleReconciler) createWorkflowRun(ctx context.Context, project *actionsv1alpha1.Project, repository githubclient.Repository, workflowPath, schedule, revision string, scheduledAt time.Time) (bool, error) {
	identity := strings.Join([]string{string(project.UID), fmt.Sprint(repository.ID), workflowPath, schedule, scheduledAt.Format(time.RFC3339)}, "\x00")
	digest := sha256.Sum256([]byte(identity))
	desired := &actionsv1alpha1.WorkflowRun{
		ObjectMeta: metav1.ObjectMeta{
			Name: "schedule-" + hex.EncodeToString(digest[:10]), Namespace: project.Namespace,
			Finalizers: []string{workflowRunScheduleFinalizer},
		},
		Spec: actionsv1alpha1.WorkflowRunSpec{
			ProjectRef:              corev1.LocalObjectReference{Name: project.Name},
			TTLSecondsAfterFinished: r.WorkflowRunTTLSecondsAfterFinished,
			Source: actionsv1alpha1.WorkflowRunSource{Type: actionsv1alpha1.SourceTypeGitHub, GitHub: &actionsv1alpha1.GitHubWorkflowRunSource{
				Repository: actionsv1alpha1.GitHubRepository{ID: repository.ID, Owner: repository.Owner.Login, Name: repository.Name},
				Event:      actionsv1alpha1.GitHubEvent{Name: actionsv1alpha1.GitHubEventNameSchedule, Schedule: schedule},
				Revision:   actionsv1alpha1.GitRevision{SHA: revision, Ref: "refs/heads/" + repository.DefaultBranch},
			}},
			WorkflowPath: workflowPath,
		},
	}
	if err := r.Create(ctx, desired); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return false, err
		}
		existing := &actionsv1alpha1.WorkflowRun{}
		if err := r.APIReader.Get(ctx, client.ObjectKeyFromObject(desired), existing); err != nil {
			return false, err
		}
		if !apiequality.Semantic.DeepEqual(existing.Spec, desired.Spec) {
			return false, apierrors.NewConflict(actionsv1alpha1.GroupVersion.WithResource("workflowruns").GroupResource(), existing.Name, fmt.Errorf("existing WorkflowRun does not match scheduled run"))
		}
		return false, nil
	}
	return true, nil
}

func (r *ScheduleReconciler) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

func (r *ScheduleReconciler) nextMinute() time.Duration {
	now := r.now().UTC()
	return now.Truncate(time.Minute).Add(time.Minute).Sub(now)
}

func (r *ScheduleReconciler) scheduleRequeue(processedMinute time.Time) ctrl.Result {
	now := r.now().UTC()
	if now.Truncate(time.Minute).After(processedMinute) {
		return ctrl.Result{Requeue: true}
	}
	return ctrl.Result{RequeueAfter: now.Truncate(time.Minute).Add(time.Minute).Sub(now)}
}

func (r *ScheduleReconciler) scheduleFailureRequeue(processedMinute time.Time) ctrl.Result {
	now := r.now().UTC()
	if now.Truncate(time.Minute).After(processedMinute) {
		return ctrl.Result{Requeue: true}
	}
	remaining := now.Truncate(time.Minute).Add(time.Minute).Sub(now)
	retryAfter := min(scheduleFailureRetry, remaining/2)
	if retryAfter <= 0 {
		return ctrl.Result{Requeue: true}
	}
	return ctrl.Result{RequeueAfter: retryAfter}
}

func (r *ScheduleReconciler) SetupWithManager(manager ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(manager).Named("workflow-schedule").For(&actionsv1alpha1.Project{}).Complete(r)
}
