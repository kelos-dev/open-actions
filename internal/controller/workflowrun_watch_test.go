package controller

import (
	"context"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	actionsv1alpha1 "github.com/kelos-dev/open-actions/api/v1alpha1"
	"github.com/kelos-dev/open-actions/internal/gitrepository"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/rest"
	toolscache "k8s.io/client-go/tools/cache"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

func TestPullRequestRevisionLookupUsesPendingApprovalIndex(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := actionsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	revision := pullRequestApprovalTestRun("job token", false)
	revision.Spec.Approval = nil
	revision.CreationTimestamp = metav1.NewTime(time.Unix(2, 0))
	revision.Spec.Source.GitHub.Event.PullRequest.HeadSHA = strings.Repeat("c", 40)

	objects := []client.Object{}
	for _, test := range []struct {
		name   string
		mutate func(*actionsv1alpha1.WorkflowRun)
	}{
		{name: "pending"},
		{name: "same-head", mutate: func(run *actionsv1alpha1.WorkflowRun) {
			run.Spec.Source.GitHub.Event.PullRequest.HeadSHA = pullRequestHeadSHA(revision)
		}},
		{name: "newer", mutate: func(run *actionsv1alpha1.WorkflowRun) {
			run.CreationTimestamp = metav1.NewTime(time.Unix(3, 0))
		}},
		{name: "other-namespace", mutate: func(run *actionsv1alpha1.WorkflowRun) { run.Namespace = "other" }},
		{name: "other-project", mutate: func(run *actionsv1alpha1.WorkflowRun) { run.Spec.ProjectRef.Name = "other" }},
		{name: "other-repository", mutate: func(run *actionsv1alpha1.WorkflowRun) { run.Spec.Source.GitHub.Repository.ID = 2 }},
		{name: "other-pr", mutate: func(run *actionsv1alpha1.WorkflowRun) { run.Spec.Source.GitHub.Event.PullRequest.Number++ }},
		{name: "approved", mutate: func(run *actionsv1alpha1.WorkflowRun) { run.Spec.Approval.Approved = true }},
		{name: "approval-not-required", mutate: func(run *actionsv1alpha1.WorkflowRun) { run.Spec.Approval = nil }},
		{name: "push", mutate: func(run *actionsv1alpha1.WorkflowRun) {
			run.Spec.Source.GitHub.Event.Name = actionsv1alpha1.GitHubEventNamePush
		}},
		{name: "no-source", mutate: func(run *actionsv1alpha1.WorkflowRun) { run.Spec.Source.GitHub = nil }},
		{name: "no-pr", mutate: func(run *actionsv1alpha1.WorkflowRun) { run.Spec.Source.GitHub.Event.PullRequest = nil }},
	} {
		run := pullRequestApprovalTestRun("job token", false)
		run.Name = test.name
		run.CreationTimestamp = metav1.NewTime(time.Unix(1, 0))
		if test.mutate != nil {
			test.mutate(run)
		}
		objects = append(objects, run)
	}
	for i := range 403 {
		run := pullRequestApprovalTestRun("job token", false)
		run.Name = fmt.Sprintf("completed-%d", i)
		run.Status.Conditions = []metav1.Condition{{Type: actionsv1alpha1.WorkflowRunConditionSucceeded, Status: metav1.ConditionFalse}}
		if i%2 == 0 {
			run.Status.Conditions[0].Status = metav1.ConditionTrue
		}
		objects = append(objects, run)
	}
	listedNames := []string{}
	clusterClient := fake.NewClientBuilder().WithScheme(scheme).
		WithIndex(&actionsv1alpha1.WorkflowRun{}, workflowRunPendingPullRequestIndex, indexPendingPullRequestWorkflowRun).
		WithObjects(objects...).
		WithInterceptorFuncs(interceptor.Funcs{List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if err := c.List(ctx, list, opts...); err != nil {
				return err
			}
			for _, run := range list.(*actionsv1alpha1.WorkflowRunList).Items {
				listedNames = append(listedNames, run.Name)
			}
			return nil
		}}).Build()
	// A watch lookup must work entirely from the cache without an API reader.
	reconciler := &WorkflowRunReconciler{Client: clusterClient}
	requests := reconciler.workflowRunsSupersededByPullRequestRevision(t.Context(), revision)
	if len(requests) != 1 || requests[0].NamespacedName != (types.NamespacedName{Namespace: revision.Namespace, Name: "pending"}) {
		t.Fatalf("superseded requests = %#v", requests)
	}
	slices.Sort(listedNames)
	if !slices.Equal(listedNames, []string{"newer", "pending", "same-head"}) {
		t.Fatalf("lookup candidates = %v", listedNames)
	}
}

func TestRetainedPullRequestRevisionSupersedesPendingApproval(t *testing.T) {
	for _, origin := range []string{"fork", "job token", "fork and job token"} {
		t.Run(origin, func(t *testing.T) {
			scheme := runtime.NewScheme()
			if err := actionsv1alpha1.AddToScheme(scheme); err != nil {
				t.Fatal(err)
			}
			pending := pullRequestApprovalTestRun(origin, false)
			pending.CreationTimestamp = metav1.NewTime(time.Unix(1, 0))
			revision := pending.DeepCopy()
			revision.Name = "retained-revision"
			revision.UID = "retained-revision"
			revision.CreationTimestamp = metav1.NewTime(time.Unix(2, 0))
			revision.Spec.Source.GitHub.Event.PullRequest.HeadSHA = strings.Repeat("c", 40)
			revision.Spec.Approval = nil
			revision.Spec.ForkPullRequest = nil
			revision.Status.Conditions = []metav1.Condition{{Type: actionsv1alpha1.WorkflowRunConditionSucceeded, Status: metav1.ConditionTrue}}
			clusterClient := fake.NewClientBuilder().WithScheme(scheme).
				WithStatusSubresource(&actionsv1alpha1.WorkflowRun{}).
				WithObjects(pending, revision).Build()
			reconciler := &WorkflowRunReconciler{Client: clusterClient, APIReader: clusterClient}
			if _, err := reconciler.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(pending)}); err != nil {
				t.Fatal(err)
			}
			stored := &actionsv1alpha1.WorkflowRun{}
			if err := clusterClient.Get(t.Context(), client.ObjectKeyFromObject(pending), stored); err != nil {
				t.Fatal(err)
			}
			condition := meta.FindStatusCondition(stored.Status.Conditions, actionsv1alpha1.WorkflowRunConditionSucceeded)
			if condition == nil || condition.Status != metav1.ConditionFalse || condition.Reason != "RevisionSuperseded" {
				t.Fatalf("pending run condition = %#v", condition)
			}
		})
	}
}

func TestWorkflowRunControllerStartsWithRetainedPullRequests(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := actionsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	runs := &actionsv1alpha1.WorkflowRunList{ListMeta: metav1.ListMeta{ResourceVersion: "1"}}
	objects := []client.Object{}
	for i := range 817 {
		run := pullRequestApprovalTestRun("job token", false)
		run.Name = fmt.Sprintf("retained-%d", i)
		run.UID = types.UID(run.Name)
		run.ResourceVersion = "1"
		run.Labels = map[string]string{actionsv1alpha1.LabelWorkflowRunRootUID: string(run.UID)}
		run.Spec.Approval = nil
		if i >= 403 {
			run.Spec.Source.GitHub.Event.Name = actionsv1alpha1.GitHubEventNamePush
			run.Spec.Source.GitHub.Event.PullRequest = nil
		}
		run.Status.Conditions = []metav1.Condition{{Type: actionsv1alpha1.WorkflowRunConditionSucceeded, Status: metav1.ConditionTrue}}
		runs.Items = append(runs.Items, *run)
		objects = append(objects, run)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	reconciled := make(chan string, len(objects))
	lookups := make(chan error, len(objects)+1)
	runWatch := watch.NewRaceFreeFake()
	var apiLists, cacheLists atomic.Int32
	apiClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).
		WithInterceptorFuncs(interceptor.Funcs{List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			switch list.(type) {
			case *actionsv1alpha1.WorkflowRunList:
				apiLists.Add(1)
			case *actionsv1alpha1.WorkflowJobList:
				options := (&client.ListOptions{}).ApplyOptions(opts)
				uid, _ := options.LabelSelector.RequiresExactMatch(actionsv1alpha1.LabelWorkflowRunUID)
				select {
				case reconciled <- uid:
				case <-ctx.Done():
				}
			}
			return c.List(ctx, list, opts...)
		}}).Build()
	mapper := meta.NewDefaultRESTMapper([]schema.GroupVersion{actionsv1alpha1.GroupVersion})
	for _, kind := range []string{"WorkflowRun", "WorkflowJob"} {
		mapper.Add(actionsv1alpha1.GroupVersion.WithKind(kind), meta.RESTScopeNamespace)
	}
	manager, err := ctrl.NewManager(&rest.Config{Host: "http://localhost"}, ctrl.Options{
		Scheme:                 scheme,
		MapperProvider:         func(*rest.Config, *http.Client) (meta.RESTMapper, error) { return mapper, nil },
		Metrics:                metricsserver.Options{BindAddress: "0"},
		HealthProbeBindAddress: "0",
		Cache: cache.Options{NewInformer: func(_ toolscache.ListerWatcher, object runtime.Object, resync time.Duration, indexers toolscache.Indexers) toolscache.SharedIndexInformer {
			// Exercise real informer initial events and handler synchronization with an in-memory API source.
			return toolscache.NewSharedIndexInformer(&toolscache.ListWatch{
				ListFunc: func(metav1.ListOptions) (runtime.Object, error) {
					if _, ok := object.(*actionsv1alpha1.WorkflowRun); ok {
						return runs.DeepCopy(), nil
					}
					return &actionsv1alpha1.WorkflowJobList{ListMeta: metav1.ListMeta{ResourceVersion: "1"}}, nil
				},
				WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
					if options.SendInitialEvents != nil && *options.SendInitialEvents {
						return nil, apierrors.NewBadRequest("streaming lists are not supported")
					}
					if _, ok := object.(*actionsv1alpha1.WorkflowRun); ok {
						return runWatch, nil
					}
					return watch.NewRaceFreeFake(), nil
				},
			}, object, resync, indexers)
		}},
		NewClient: func(_ *rest.Config, options client.Options) (client.Client, error) {
			return interceptor.NewClient(apiClient, interceptor.Funcs{
				Get: func(ctx context.Context, _ client.WithWatch, key client.ObjectKey, object client.Object, opts ...client.GetOption) error {
					return options.Cache.Reader.Get(ctx, key, object, opts...)
				},
				List: func(ctx context.Context, _ client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
					err := options.Cache.Reader.List(ctx, list, opts...)
					if _, ok := list.(*actionsv1alpha1.WorkflowRunList); ok {
						cacheLists.Add(1)
						lookups <- err
					}
					return err
				},
			}), nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	gitRepository, err := gitrepository.NewClient("https://github.com")
	if err != nil {
		t.Fatal(err)
	}
	reconciler := &WorkflowRunReconciler{Client: manager.GetClient(), APIReader: apiClient, GitRepository: gitRepository}
	if err := reconciler.SetupWithManager(manager); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- manager.Start(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("manager shutdown: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("manager did not stop after cancellation")
		}
	})
	seen := map[string]bool{}
	for len(seen) < len(objects) {
		select {
		case name := <-reconciled:
			seen[name] = true
		case <-ctx.Done():
			t.Fatalf("reconciled %d of %d retained runs: %v", len(seen), len(objects), ctx.Err())
		}
	}
	if apiLists.Load() != 0 || cacheLists.Load() != 0 {
		t.Fatalf("startup WorkflowRun lists: API=%d, cache=%d", apiLists.Load(), cacheLists.Load())
	}
	live := runs.Items[0].DeepCopy()
	live.Name = "live-revision"
	live.UID = "live-revision"
	live.ResourceVersion = "2"
	live.Labels[actionsv1alpha1.LabelWorkflowRunRootUID] = string(live.UID)
	runWatch.Add(live)
	select {
	case err := <-lookups:
		if err != nil {
			t.Fatalf("live pull request lookup: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("live pull request creation did not trigger a supersession lookup")
	}
	select {
	case name := <-reconciled:
		if name != live.Name {
			t.Fatalf("reconciled run = %q, want %q", name, live.Name)
		}
	case <-ctx.Done():
		t.Fatal("live pull request was not reconciled")
	}
	if apiLists.Load() != 0 || cacheLists.Load() != 1 {
		t.Fatalf("live WorkflowRun lists: API=%d, cache=%d", apiLists.Load(), cacheLists.Load())
	}
}
