package console

import (
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	actionsv1alpha1 "github.com/kelos-dev/open-actions/api/v1alpha1"
	"github.com/kelos-dev/open-actions/internal/workflowrun"
	"github.com/kelos-dev/open-actions/internal/workflowstatus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	toolscache "k8s.io/client-go/tools/cache"
	ctrlcache "sigs.k8s.io/controller-runtime/pkg/cache"
)

// WorkflowRunSummary contains the fields used by the Console run list.
type WorkflowRunSummary struct {
	URL           string
	Repository    string
	RepositoryID  int64
	WorkflowPath  string
	RunLabel      string
	WorkflowName  string
	Namespace     string
	Name          string
	Project       string
	Event         string
	RefName       string
	Revision      string
	ShortRevision string
	Status        string
	StatusClass   string
	Created       string
	Duration      executionDuration
	Active        bool
	start         *metav1.Time
	completion    *metav1.Time
}

// RecentWorkflowRuns provides recent and selected WorkflowRuns for the Console.
type RecentWorkflowRuns interface {
	List(filter WorkflowRunFilter, limit int) WorkflowRunList
	Find(keys []types.NamespacedName) []WorkflowRunSummary
	Synced() bool
}

type workflowRunEntry struct {
	key       types.NamespacedName
	createdAt time.Time
	summary   WorkflowRunSummary
}

// WorkflowRunStore maintains a compact, ordered projection of WorkflowRuns.
type WorkflowRunStore struct {
	mu      sync.RWMutex
	byKey   map[types.NamespacedName]*workflowRunEntry
	ordered []*workflowRunEntry
	synced  func() bool
	logger  *slog.Logger
}

// NewWorkflowRunStore registers an ordered WorkflowRun projection with an informer.
func NewWorkflowRunStore(informer ctrlcache.Informer, logger *slog.Logger) (*WorkflowRunStore, error) {
	if informer == nil {
		return nil, errors.New("WorkflowRun informer is required")
	}
	if logger == nil {
		return nil, errors.New("WorkflowRun store logger is required")
	}
	store := newWorkflowRunStore(logger)
	registration, err := informer.AddEventHandler(toolscache.ResourceEventHandlerDetailedFuncs{
		AddFunc: func(object any, _ bool) {
			store.upsertObject(object)
		},
		UpdateFunc: func(_, object any) {
			store.upsertObject(object)
		},
		DeleteFunc: func(object any) {
			store.deleteObject(object)
		},
	})
	if err != nil {
		return nil, fmt.Errorf("register WorkflowRun informer handler: %w", err)
	}
	store.synced = registration.HasSynced
	return store, nil
}

func newWorkflowRunStore(logger *slog.Logger) *WorkflowRunStore {
	return &WorkflowRunStore{
		byKey:  map[types.NamespacedName]*workflowRunEntry{},
		synced: func() bool { return false },
		logger: logger,
	}
}

// WorkflowRunCacheTransform removes fields that the run list does not use.
func WorkflowRunCacheTransform(object any) (any, error) {
	run, ok := object.(*actionsv1alpha1.WorkflowRun)
	if !ok {
		return nil, fmt.Errorf("transform WorkflowRun cache object of type %T", object)
	}
	metadata := metav1.ObjectMeta{
		Name:              run.Name,
		Namespace:         run.Namespace,
		UID:               run.UID,
		ResourceVersion:   run.ResourceVersion,
		CreationTimestamp: run.CreationTimestamp,
	}
	source := actionsv1alpha1.WorkflowRunSource{Type: run.Spec.Source.Type}
	if github := run.Spec.Source.GitHub; github != nil {
		source.GitHub = &actionsv1alpha1.GitHubWorkflowRunSource{
			Repository: github.Repository,
			Event:      actionsv1alpha1.GitHubEvent{Name: github.Event.Name},
			Revision: actionsv1alpha1.GitRevision{
				SHA: github.Revision.SHA, Ref: github.Revision.Ref, HeadRef: github.Revision.HeadRef,
			},
		}
	}
	conditions := make([]metav1.Condition, len(run.Status.Conditions))
	for index := range run.Status.Conditions {
		condition := run.Status.Conditions[index]
		conditions[index] = metav1.Condition{Type: condition.Type, Status: condition.Status, Reason: condition.Reason}
	}
	projectRef := run.Spec.ProjectRef
	workflowPath := run.Spec.WorkflowPath
	cancelRequested := run.Spec.CancelRequested
	workflowName := run.Status.WorkflowName
	var identity *actionsv1alpha1.WorkflowRunIdentityStatus
	if run.Status.Identity != nil {
		identity = &actionsv1alpha1.WorkflowRunIdentityStatus{Number: run.Status.Identity.Number, Attempt: run.Status.Identity.Attempt}
	}
	startTime := run.Status.StartTime
	completionTime := run.Status.CompletionTime
	run.ObjectMeta = metadata
	run.Spec = actionsv1alpha1.WorkflowRunSpec{
		ProjectRef: projectRef, Source: source, WorkflowPath: workflowPath, CancelRequested: cancelRequested,
	}
	run.Status = actionsv1alpha1.WorkflowRunStatus{
		WorkflowName: workflowName, Identity: identity, StartTime: startTime, CompletionTime: completionTime, Conditions: conditions,
	}
	return run, nil
}

// List filters before limiting results and derives cascading choices from all retained runs.
func (s *WorkflowRunStore) List(filter WorkflowRunFilter, limit int) WorkflowRunList {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := WorkflowRunList{Runs: make([]WorkflowRunSummary, 0, min(max(limit, 0), len(s.ordered)))}
	projects, repositories, workflows := map[string]string{}, map[string]string{}, map[string]string{}
	now := time.Now()
	for _, entry := range s.ordered {
		run := entry.summary
		project := namespacedValue(run.Namespace, run.Project)
		projects[project] = project
		if filter.Project != "" && filter.Project != project {
			continue
		}
		repository := strconv.FormatInt(run.RepositoryID, 10)
		if _, exists := repositories[repository]; !exists {
			repositories[repository] = run.Repository
		}
		if filter.RepositoryID != 0 && filter.RepositoryID != run.RepositoryID {
			continue
		}
		if _, exists := workflows[run.WorkflowPath]; !exists {
			label := run.WorkflowPath
			if run.WorkflowName != run.WorkflowPath {
				label = run.WorkflowName + " · " + run.WorkflowPath
			}
			workflows[run.WorkflowPath] = label
		}
		if filter.WorkflowPath != "" && filter.WorkflowPath != run.WorkflowPath {
			continue
		}
		if len(result.Runs) >= limit {
			result.Truncated = true
			continue
		}
		run.Duration = elapsedDuration(run.start, run.completion, run.Active, now)
		result.Runs = append(result.Runs, run)
	}
	// Preserve selections when their last retained run disappears.
	if filter.Project != "" {
		projects[filter.Project] = filter.Project
	}
	if filter.RepositoryID != 0 {
		value := strconv.FormatInt(filter.RepositoryID, 10)
		if _, exists := repositories[value]; !exists {
			repositories[value] = "Repository " + value
		}
	}
	if filter.WorkflowPath != "" {
		if _, exists := workflows[filter.WorkflowPath]; !exists {
			workflows[filter.WorkflowPath] = filter.WorkflowPath
		}
	}
	result.Projects = runFilterOptions(projects)
	result.Repositories = runFilterOptions(repositories)
	result.Workflows = runFilterOptions(workflows)
	return result
}

// Find returns summaries for the requested WorkflowRuns that are still cached.
func (s *WorkflowRunStore) Find(keys []types.NamespacedName) []WorkflowRunSummary {
	s.mu.RLock()
	defer s.mu.RUnlock()
	runs := make([]WorkflowRunSummary, 0, len(keys))
	now := time.Now()
	for _, key := range keys {
		if entry := s.byKey[key]; entry != nil {
			summary := entry.summary
			summary.Duration = elapsedDuration(summary.start, summary.completion, summary.Active, now)
			runs = append(runs, summary)
		}
	}
	return runs
}

// Synced reports whether the store has consumed the informer's initial list.
func (s *WorkflowRunStore) Synced() bool {
	return s.synced()
}

func (s *WorkflowRunStore) upsertObject(object any) {
	run, ok := object.(*actionsv1alpha1.WorkflowRun)
	if !ok {
		s.logger.Error("received invalid WorkflowRun informer object", "type", fmt.Sprintf("%T", object))
		return
	}
	s.upsert(run)
}

func (s *WorkflowRunStore) upsert(run *actionsv1alpha1.WorkflowRun) {
	key := types.NamespacedName{Namespace: run.Namespace, Name: run.Name}
	summary, supported := workflowRunSummary(run)
	s.mu.Lock()
	defer s.mu.Unlock()
	existing := s.byKey[key]
	if !supported {
		s.removeLocked(existing)
		return
	}
	createdAt := run.CreationTimestamp.Time
	if existing != nil && existing.createdAt.Equal(createdAt) {
		existing.summary = summary
		return
	}
	s.removeLocked(existing)
	entry := &workflowRunEntry{key: key, createdAt: createdAt, summary: summary}
	index := sort.Search(len(s.ordered), func(index int) bool {
		return workflowRunEntryBefore(entry, s.ordered[index])
	})
	s.ordered = append(s.ordered, nil)
	copy(s.ordered[index+1:], s.ordered[index:])
	s.ordered[index] = entry
	s.byKey[key] = entry
}

func (s *WorkflowRunStore) deleteObject(object any) {
	key, err := toolscache.DeletionHandlingMetaNamespaceKeyFunc(object)
	if err != nil {
		s.logger.Error("received invalid WorkflowRun informer deletion", "error", err)
		return
	}
	namespace, name, err := toolscache.SplitMetaNamespaceKey(key)
	if err != nil {
		s.logger.Error("received invalid WorkflowRun informer key", "key", key, "error", err)
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.removeLocked(s.byKey[types.NamespacedName{Namespace: namespace, Name: name}])
}

func (s *WorkflowRunStore) removeLocked(entry *workflowRunEntry) {
	if entry == nil {
		return
	}
	delete(s.byKey, entry.key)
	for index := range s.ordered {
		if s.ordered[index] != entry {
			continue
		}
		copy(s.ordered[index:], s.ordered[index+1:])
		s.ordered[len(s.ordered)-1] = nil
		s.ordered = s.ordered[:len(s.ordered)-1]
		return
	}
}

func workflowRunEntryBefore(left, right *workflowRunEntry) bool {
	if !left.createdAt.Equal(right.createdAt) {
		return left.createdAt.After(right.createdAt)
	}
	if left.key.Namespace != right.key.Namespace {
		return left.key.Namespace < right.key.Namespace
	}
	return left.key.Name < right.key.Name
}

func workflowRunSummary(run *actionsv1alpha1.WorkflowRun) (WorkflowRunSummary, bool) {
	if run.Spec.Source.Type != actionsv1alpha1.SourceTypeGitHub || run.Spec.Source.GitHub == nil {
		return WorkflowRunSummary{}, false
	}
	github := run.Spec.Source.GitHub
	workflowName := run.Status.WorkflowName
	if workflowName == "" {
		workflowName = run.Spec.WorkflowPath
	}
	refName := github.Revision.HeadRef
	if refName == "" {
		refName = shortRef(github.Revision.Ref)
	}
	status := workflowstatus.Run(run)
	summary := WorkflowRunSummary{
		URL: runPath(run), Repository: github.Repository.Owner + "/" + github.Repository.Name,
		RepositoryID: github.Repository.ID, WorkflowPath: run.Spec.WorkflowPath, RunLabel: workflowRunLabel(run),
		WorkflowName: workflowName, Namespace: run.Namespace, Name: run.Name, Project: run.Spec.ProjectRef.Name,
		Event: strings.ReplaceAll(string(github.Event.Name), "_", " "), RefName: refName,
		Revision: github.Revision.SHA, ShortRevision: shortRevision(github.Revision.SHA),
		Status: status, StatusClass: statusClass(status), Active: !workflowrun.Terminal(run) && run.Status.CompletionTime == nil,
		start: run.Status.StartTime.DeepCopy(), completion: run.Status.CompletionTime.DeepCopy(),
	}
	if !run.CreationTimestamp.IsZero() {
		summary.Created = run.CreationTimestamp.UTC().Format(time.RFC3339)
	}
	return summary, true
}
