package webhook

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/cgi"
	"net/http/httptest"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	actionsv1alpha1 "github.com/kelos-dev/open-actions/api/v1alpha1"
	"github.com/kelos-dev/open-actions/internal/controller"
	"github.com/kelos-dev/open-actions/internal/gitrepository"
	"github.com/kelos-dev/open-actions/internal/workflowsnapshot"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

func TestPullRequestGitRequestsFromDiscoveryThroughPlanning(t *testing.T) {
	for _, count := range []int{1, 10, 50} {
		t.Run(fmt.Sprintf("workflows=%d", count), func(t *testing.T) {
			data := []byte("name: CI\non: pull_request\njobs:\n  build:\n    runs-on: linux\n    steps:\n      - run: echo hello\n")
			baseData := strings.Replace(string(data), "on: pull_request", "on: push", 1)
			gitRoot, revision := testPullRequestGitRepository(t, []byte(baseData), nil, false)
			work := filepath.Join(gitRoot, "work")
			workflowData := make(map[string]string, count)
			for index := range count {
				path := fmt.Sprintf(".open-actions/workflows/head-%d.yaml", index)
				workflowData[path] = strings.Replace(string(data), "name: CI", fmt.Sprintf("name: CI %d", index), 1)
				writeTestFile(t, work, path, workflowData[path])
			}
			runTestGit(t, work, "add", ".")
			runTestGit(t, work, "commit", "--quiet", "-m", "workflows")
			revision.HeadSHA = strings.TrimSpace(runTestGit(t, work, "rev-parse", "HEAD"))
			runTestGit(t, work, "push", "--quiet", filepath.Join(gitRoot, "acme", "example"), "head")
			remote := filepath.Join(gitRoot, "acme", "example")
			runTestGit(t, remote, "config", "uploadpack.allowFilter", "true")
			runTestGit(t, remote, "config", "uploadpack.allowReachableSHA1InWant", "true")
			gitPath, err := exec.LookPath("git")
			if err != nil {
				t.Fatal(err)
			}
			backend := &cgi.Handler{Path: gitPath, Args: []string{"http-backend"}, Env: []string{"GIT_PROJECT_ROOT=" + gitRoot, "GIT_HTTP_EXPORT_ALL=1"}}
			var gitCalls atomic.Int32
			gitServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gitCalls.Add(1)
				backend.ServeHTTP(w, r)
			}))
			defer gitServer.Close()
			var apiCalls atomic.Int32
			apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				apiCalls.Add(1)
				switch r.URL.Path {
				case "/app/installations/2/access_tokens":
					_ = json.NewEncoder(w).Encode(map[string]any{"token": "installation-token", "expires_at": time.Now().Add(time.Hour)})
				case "/repos/acme/example/compare/" + revision.BaseSHA + "..." + revision.HeadSHA:
					_ = json.NewEncoder(w).Encode(map[string]any{"merge_base_commit": map[string]string{"sha": revision.MergeBaseSHA}})
				case "/repos/acme/example/contents/.open-actions/workflows":
					contents := []map[string]string{{"name": "ci.yaml", "path": ".open-actions/workflows/ci.yaml", "type": "file"}}
					_ = json.NewEncoder(w).Encode(contents)
				default:
					if r.URL.Path == "/repos/acme/example/contents/.open-actions/workflows/ci.yaml" {
						_ = json.NewEncoder(w).Encode(map[string]string{"encoding": "base64", "content": base64.StdEncoding.EncodeToString([]byte(baseData))})
						return
					}
					t.Errorf("unexpected GitHub API request: %s", r.URL)
					http.NotFound(w, r)
				}
			}))
			defer apiServer.Close()
			now := time.Now()
			initialClient, delivery, handler, project := newPullRequestDeliveryTest(t, apiServer, now)
			requireApproval := false
			project.Spec.Source.GitHub.ForkPullRequests = &actionsv1alpha1.GitHubForkPullRequestPolicy{RequireApproval: &requireApproval}
			secret := &corev1.Secret{}
			if err := initialClient.Get(t.Context(), client.ObjectKey{Namespace: project.Namespace, Name: "github"}, secret); err != nil {
				t.Fatal(err)
			}
			cluster := fake.NewClientBuilder().WithScheme(initialClient.Scheme()).WithObjects(project, secret).
				WithStatusSubresource(&actionsv1alpha1.WorkflowRun{}, &actionsv1alpha1.WorkflowJob{}).
				WithInterceptorFuncs(interceptor.Funcs{Create: func(ctx context.Context, c client.WithWatch, object client.Object, opts ...client.CreateOption) error {
					if object.GetUID() == "" {
						object.SetUID(types.UID(object.GetName()))
					}
					return c.Create(ctx, object, opts...)
				}}).Build()
			delivery.Client, delivery.APIReader = cluster, cluster
			handler.Client, handler.APIReader = cluster, cluster
			delivery.GitRepository, err = gitrepository.NewClient(gitServer.URL)
			if err != nil {
				t.Fatal(err)
			}
			key := enqueuePullRequestDelivery(t, handler, cluster, project, now, revision.BaseSHA, revision.HeadSHA, []byte(`{"delivery":"pull-request"}`))
			if _, err := delivery.Reconcile(t.Context(), ctrl.Request{NamespacedName: key}); err != nil {
				t.Fatal(err)
			}
			afterDiscovery := gitCalls.Load()
			runs := &actionsv1alpha1.WorkflowRunList{}
			if err := cluster.List(t.Context(), runs); err != nil || len(runs.Items) != count {
				t.Fatalf("discovered WorkflowRuns = %d, want %d: %v", len(runs.Items), count, err)
			}
			for index := range runs.Items {
				run := &runs.Items[index]
				planner := &controller.WorkflowRunReconciler{
					Client: cluster, APIReader: cluster, GitHub: delivery.GitHub, GitRepository: delivery.GitRepository,
					GitHubAPIBase: apiServer.URL, GitHubServerURL: gitServer.URL, ActionCloneBaseURL: gitServer.URL,
				}
				for range 2 {
					if _, err := planner.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(run)}); err != nil {
						t.Fatal(err)
					}
				}
				if err := cluster.Get(t.Context(), client.ObjectKeyFromObject(run), run); err != nil {
					t.Fatal(err)
				}
				if !meta.IsStatusConditionTrue(run.Status.Conditions, actionsv1alpha1.WorkflowRunConditionPlanned) || run.Status.Jobs == nil || run.Status.Jobs.Total != 1 {
					t.Fatalf("workflow was not planned: %#v", run.Status)
				}
				object := &corev1.ConfigMap{}
				if err := cluster.Get(t.Context(), client.ObjectKey{Namespace: run.Namespace, Name: run.Annotations[actionsv1alpha1.AnnotationWorkflowFile]}, object); err != nil {
					t.Fatal(err)
				}
				if object.Data[workflowsnapshot.DataKey] != workflowData[run.Spec.WorkflowPath] {
					t.Fatalf("workflow snapshot data = %q", object.Data[workflowsnapshot.DataKey])
				}
			}
			t.Logf("workflows=%d: Git HTTP requests after discovery=%d, after planning=%d, GitHub API requests=%d", count, afterDiscovery, gitCalls.Load(), apiCalls.Load())
			if afterDiscovery != 4 || gitCalls.Load() != afterDiscovery {
				t.Fatalf("Git HTTP requests: discovery=%d, after planning=%d; want 4", afterDiscovery, gitCalls.Load())
			}
			if apiCalls.Load() != 4 {
				t.Fatalf("GitHub API requests = %d, want 4", apiCalls.Load())
			}
		})
	}
}
