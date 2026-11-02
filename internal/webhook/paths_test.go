package webhook

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	actionsv1alpha1 "github.com/kelos-dev/open-actions/api/v1alpha1"
	"github.com/kelos-dev/open-actions/internal/eventsnapshot"
	githubclient "github.com/kelos-dev/open-actions/internal/github"
	"github.com/kelos-dev/open-actions/internal/gitrepository"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestDeliveryPushPathFilters(t *testing.T) {
	now := time.Date(2026, 11, 2, 12, 0, 0, 0, time.UTC)
	gitRoot, revision := testPullRequestGitRepository(t, []byte("name: Base\n"), nil, false)
	for _, tt := range []struct {
		name, before, after, firstCommit, filter, ref string
		commitCount                                   int
		deleted                                       bool
		want                                          int
	}{
		{name: "matching files", before: revision.BaseSHA, after: revision.HeadSHA, filter: "paths: ['head.txt']", want: 2},
		{name: "two dot force push deletion", before: revision.BaseSHA, after: revision.HeadSHA, filter: "paths: ['.open-actions/workflows/ci.yaml']", want: 2},
		{name: "nonmatching files", before: revision.BaseSHA, after: revision.HeadSHA, filter: "paths: ['src/**']"},
		{name: "ignored files", before: revision.MergeBaseSHA, after: revision.HeadSHA, filter: "paths-ignore: ['head.txt']"},
		{name: "nonignored files", before: revision.BaseSHA, after: revision.HeadSHA, filter: "paths-ignore: ['head.txt']", want: 2},
		{name: "empty diff", before: revision.HeadSHA, after: revision.HeadSHA, filter: "paths-ignore: ['docs/**']"},
		{name: "new branch", before: zeroGitSHA, after: revision.HeadSHA, filter: "paths: ['head.txt']", commitCount: 1, want: 2},
		{name: "new branch excludes parent files", before: zeroGitSHA, after: revision.HeadSHA, filter: "paths: ['shared.txt']", commitCount: 1},
		{name: "new repository", before: zeroGitSHA, after: revision.MergeBaseSHA, filter: "paths: ['shared.txt']", commitCount: 1, want: 2},
		{name: "new branch without commits", before: zeroGitSHA, after: revision.HeadSHA, filter: "paths: ['**']"},
		{name: "1000 commit push still filters", before: revision.BaseSHA, after: revision.HeadSHA, filter: "paths: ['src/**']", commitCount: 1000},
		{name: "multiple commits on new branch", before: zeroGitSHA, after: revision.HeadSHA, firstCommit: revision.MergeBaseSHA, filter: "paths: ['shared.txt']", commitCount: 2, want: 2},
		{name: "large push", before: revision.BaseSHA, after: revision.HeadSHA, filter: "paths: ['src/**']", commitCount: 1001, want: 2},
		{name: "large push still enforces branches", before: revision.BaseSHA, after: revision.HeadSHA, filter: "branches: [release]\n    paths: ['**']", commitCount: 1001},
		{name: "tag bypasses path filters", after: revision.HeadSHA, filter: "paths: ['src/**']", ref: "refs/tags/v1", want: 2},
		{name: "deleted branch", before: revision.BaseSHA, after: zeroGitSHA, filter: "paths: ['**']", deleted: true},
		{name: "nonmatching branch needs no diff", after: revision.HeadSHA, filter: "branches: [release]\n    paths: ['**']"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			workflowData := []byte("on:\n  push:\n    " + tt.filter + "\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo test\n")
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				switch req.URL.Path {
				case "/app/installations/2/access_tokens":
					fmt.Fprint(w, `{"token":"installation-token"}`)
				case "/repos/acme/example/commits":
					fmt.Fprintf(w, `[{"sha":%q}]`, revision.BaseSHA)
				case "/repos/acme/example/contents/.open-actions/workflows":
					fmt.Fprint(w, `[{"path":".open-actions/workflows/ci.yaml","type":"file"},{"path":".open-actions/workflows/test.yaml","type":"file"}]`)
				case "/repos/acme/example/contents/.open-actions/workflows/ci.yaml", "/repos/acme/example/contents/.open-actions/workflows/test.yaml":
					fmt.Fprintf(w, `{"encoding":"base64","content":%q}`, base64.StdEncoding.EncodeToString(workflowData))
				default:
					http.NotFound(w, req)
				}
			}))
			defer server.Close()
			cluster, reconciler, handler, project := newPullRequestDeliveryTest(t, server, now)
			reconciler.GitRepository, _ = gitrepository.NewClient(gitRoot)
			event := &payload{Ref: tt.ref, After: tt.after, Deleted: tt.deleted}
			if event.Ref == "" {
				event.Ref = "refs/heads/main"
			}
			event.Repository.ID, event.Repository.Name, event.Repository.Owner.Login, event.Repository.DefaultBranch = 1, "example", "acme", "main"
			commits := make([]map[string]string, tt.commitCount)
			for i := range commits {
				commits[i] = map[string]string{"id": tt.after}
			}
			if tt.firstCommit != "" {
				commits[0]["id"] = tt.firstCommit
			}
			body, err := json.Marshal(map[string]any{"before": tt.before, "after": tt.after, "deleted": tt.deleted, "commits": commits})
			if err != nil {
				t.Fatal(err)
			}
			normalized, accepted, err := normalize("push", event)
			if err != nil || !accepted {
				t.Fatalf("normalize() = %v, %v", accepted, err)
			}
			if err := handler.enqueueDelivery(context.Background(), project, event, normalized, "delivery", body); err != nil {
				t.Fatal(err)
			}
			key := client.ObjectKey{Namespace: project.Namespace, Name: webhookDeliveryName(body)}
			if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: key}); err != nil {
				t.Fatal(err)
			}
			stored := &corev1.ConfigMap{}
			if err := cluster.Get(context.Background(), key, stored); err != nil {
				t.Fatal(err)
			}
			if stored.Data[deliveryStateKey] != deliveryStateCompleted {
				t.Fatalf("delivery = %#v", stored.Data)
			}
			runs := &actionsv1alpha1.WorkflowRunList{}
			if err := cluster.List(context.Background(), runs); err != nil {
				t.Fatal(err)
			}
			if len(runs.Items) != tt.want {
				t.Fatalf("WorkflowRuns = %d, want %d", len(runs.Items), tt.want)
			}
			for _, run := range runs.Items {
				if run.Spec.Source.GitHub.Revision.SHA != normalized.SHA {
					t.Fatalf("run revision = %#v", run.Spec.Source.GitHub.Revision)
				}
			}
		})
	}
}

func TestDeliveryPullRequestPathFilters(t *testing.T) {
	for _, tt := range []struct {
		name, filter   string
		want           int
		sameRepository bool
	}{
		{"head changes", "paths: ['head.txt']", 2, false},
		{"same repository head changes", "paths: ['head.txt']", 2, true},
		{"base changes excluded", "paths: ['.open-actions/workflows/ci.yaml']", 0, false},
		{"ignore head changes", "paths-ignore: ['head.txt']", 0, false},
		{"branch and path filters", "branches: [release]\n    paths: ['head.txt']", 0, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			now := time.Date(2026, 11, 2, 12, 0, 0, 0, time.UTC)
			workflowData := []byte("on:\n  pull_request:\n    " + tt.filter + "\n  pull_request_target:\n    " + tt.filter + "\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo test\n")
			gitRoot, revision := testPullRequestGitRepository(t, workflowData, nil, false)
			compareCalls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				switch req.URL.Path {
				case "/app/installations/2/access_tokens":
					fmt.Fprint(w, `{"token":"installation-token"}`)
				case "/repos/acme/example/compare/" + revision.BaseSHA + "..." + revision.HeadSHA:
					compareCalls++
					fmt.Fprintf(w, `{"merge_base_commit":{"sha":%q}}`, revision.MergeBaseSHA)
				case "/repos/acme/example/contents/.open-actions/workflows":
					fmt.Fprint(w, `[{"path":".open-actions/workflows/ci.yaml","type":"file"}]`)
				case "/repos/acme/example/contents/.open-actions/workflows/ci.yaml":
					fmt.Fprintf(w, `{"encoding":"base64","content":%q}`, base64.StdEncoding.EncodeToString(workflowData))
				default:
					http.NotFound(w, req)
				}
			}))
			defer server.Close()
			cluster, reconciler, handler, project := newPullRequestDeliveryTest(t, server, now)
			reconciler.GitRepository, _ = gitrepository.NewClient(gitRoot)
			key := enqueuePullRequestDelivery(t, handler, cluster, project, now, revision.BaseSHA, revision.HeadSHA, []byte(`{"delivery":"path-filter"}`))
			if tt.sameRepository {
				object := &corev1.ConfigMap{}
				if err := cluster.Get(context.Background(), key, object); err != nil {
					t.Fatal(err)
				}
				delivery := queuedDelivery{}
				if err := json.Unmarshal([]byte(object.Data[deliveryDataKey]), &delivery); err != nil {
					t.Fatal(err)
				}
				delivery.Event.Fork = false
				delivery.Event.PullRequest.HeadRepository = normalizedRepository{ID: 1, Owner: "acme", Name: "example"}
				data, err := json.Marshal(delivery)
				if err != nil {
					t.Fatal(err)
				}
				object.Data[deliveryDataKey] = string(data)
				if err := cluster.Update(context.Background(), object); err != nil {
					t.Fatal(err)
				}
			}

			if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: key}); err != nil {
				t.Fatal(err)
			}
			runs := &actionsv1alpha1.WorkflowRunList{}
			if err := cluster.List(context.Background(), runs); err != nil {
				t.Fatal(err)
			}
			if len(runs.Items) != tt.want {
				t.Fatalf("WorkflowRuns = %d, want %d", len(runs.Items), tt.want)
			}
			if compareCalls > 2 || tt.sameRepository && compareCalls != 1 {
				t.Fatalf("merge base fetched %d times", compareCalls)
			}
			for _, run := range runs.Items {
				github := run.Spec.Source.GitHub
				switch github.Event.Name {
				case actionsv1alpha1.GitHubEventNamePullRequestTarget:
					if github.Revision.SHA != revision.BaseSHA {
						t.Fatalf("target revision = %s", github.Revision.SHA)
					}
				case actionsv1alpha1.GitHubEventNamePullRequest:
					if github.Revision.HeadSHA != revision.HeadSHA || !strings.HasPrefix(github.Revision.Ref, "refs/pull/") {
						t.Fatalf("pull request revision = %#v", github.Revision)
					}
				default:
					t.Fatalf("unexpected event %s", github.Event.Name)
				}
			}
		})
	}
}

func TestPathDiffTimeoutAndCancellation(t *testing.T) {
	for _, parentCanceled := range []bool{false, true} {
		t.Run(fmt.Sprintf("parent canceled=%v", parentCanceled), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				if parentCanceled {
					cancel()
				}
				reconciler := &DeliveryReconciler{
					APIReader: &waitingPathReader{},
					Logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
				}
				files, err := reconciler.changedFiles(ctx, nil, nil, &corev1.ConfigMap{}, &queuedDelivery{EventSnapshot: "snapshot"}, normalizedEvent{Name: "push"})
				if parentCanceled {
					if !errors.Is(err, context.Canceled) || files != nil {
						t.Fatalf("files = %#v, error = %v", files, err)
					}
				} else if err != nil || files == nil || !files.AlwaysRun {
					t.Fatalf("diff timeout did not bypass path filters: files = %#v, error = %v", files, err)
				}
			})
		})
	}
}

type waitingPathReader struct{ client.Reader }

func (*waitingPathReader) Get(ctx context.Context, _ client.ObjectKey, _ client.Object, _ ...client.GetOption) error {
	<-ctx.Done()
	return ctx.Err()
}

func TestPushPathDiffRejectsUnavailableMetadata(t *testing.T) {
	for _, tt := range []struct {
		name, snapshot string
		owned          bool
	}{
		{"no snapshot", "", false},
		{"unowned snapshot", `{"before":"` + strings.Repeat("a", 40) + `"}`, false},
		{"invalid snapshot", `{`, true},
		{"missing before revision", `{}`, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			object := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "delivery", Namespace: "default", UID: "delivery-uid"}}
			immutable := true
			snapshot := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "snapshot", Namespace: "default"}, Immutable: &immutable, Data: map[string][]byte{eventsnapshot.DataKey: []byte(tt.snapshot)}}
			if tt.owned {
				snapshot.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(object, corev1.SchemeGroupVersion.WithKind("ConfigMap"))}
			}
			cluster := fake.NewClientBuilder().WithScheme(deliveryTestScheme(t)).WithObjects(snapshot).Build()
			reconciler := &DeliveryReconciler{APIReader: cluster}
			reconciler.GitRepository, _ = gitrepository.NewClient(t.TempDir())
			delivery := &queuedDelivery{}
			if tt.snapshot != "" {
				delivery.EventSnapshot = snapshot.Name
			}
			files, err := reconciler.diffFiles(context.Background(), &githubclient.InstallationClient{}, nil, object, delivery, normalizedEvent{Name: "push", SHA: strings.Repeat("b", 40)})
			if err == nil || files != nil {
				t.Fatalf("files = %#v, error = %v", files, err)
			}
		})
	}
}
