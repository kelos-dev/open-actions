package console

import (
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	actionsv1alpha1 "github.com/kelos-dev/open-actions/api/v1alpha1"
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
	WorkflowName  string
	Namespace     string
	Project       string
	Event         string
	RefName       string
	Revision      string
	ShortRevision string
	Status        string
	StatusClass   string
	Created       string
	Duration      string
}

// RecentWorkflowRuns provides the Console's ordered WorkflowRun list.
type RecentWorkflowRuns interface {
	Recent(limit int) ([]WorkflowRunSummary, bool)
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
	startTime := run.Status.StartTime
	completionTime := run.Status.CompletionTime
	run.ObjectMeta = metadata
	run.Spec = actionsv1alpha1.WorkflowRunSpec{
		ProjectRef: projectRef, Source: source, WorkflowPath: workflowPath, CancelRequested: cancelRequested,
	}
	run.Status = actionsv1alpha1.WorkflowRunStatus{
		WorkflowName: workflowName, StartTime: startTime, CompletionTime: completionTime, Conditions: conditions,
	}
	return run, nil
}

// Recent returns at most limit WorkflowRuns, newest first, and reports whether more exist.
func (s *WorkflowRunStore) Recent(limit int) ([]WorkflowRunSummary, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if limit < 0 {
		limit = 0
	}
	count := min(limit, len(s.ordered))
	runs := make([]WorkflowRunSummary, count)
	for index := 0; index < count; index++ {
		runs[index] = s.ordered[index].summary
	}
	return runs, len(s.ordered) > limit
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
		WorkflowName: workflowName, Namespace: run.Namespace, Project: run.Spec.ProjectRef.Name,
		Event: strings.ReplaceAll(string(github.Event.Name), "_", " "), RefName: refName,
		Revision: github.Revision.SHA, ShortRevision: shortRevision(github.Revision.SHA),
		Status: status, StatusClass: statusClass(status), Duration: elapsedTime(run.Status.StartTime, run.Status.CompletionTime),
	}
	if !run.CreationTimestamp.IsZero() {
		summary.Created = run.CreationTimestamp.UTC().Format(time.RFC3339)
	}
	return summary, true
}
