package webhook

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	actionsv1alpha1 "github.com/kelos-dev/open-actions/api/v1alpha1"
	"github.com/kelos-dev/open-actions/internal/controller"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

func TestWorkflowFileRequestsFromDiscoveryThroughPlanning(t *testing.T) {
	for _, count := range []int{1, 10} {
		t.Run(fmt.Sprintf("workflows=%d", count), func(t *testing.T) {
			const directory = ".open-actions/workflows"
			const data = "on: push\njobs:\n  test:\n    runs-on: linux\n    steps:\n      - run: echo hello\n"
			var fileCalls, directoryCalls, tokenCalls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				switch request.URL.Path {
				case "/app/installations/2/access_tokens":
					tokenCalls.Add(1)
					fmt.Fprintf(writer, `{"token":"installation-token","expires_at":%q}`, time.Now().Add(time.Hour).UTC().Format(time.RFC3339))
				case "/repos/acme/example/contents/" + directory:
					directoryCalls.Add(1)
					contents := make([]map[string]string, count)
					for index := range contents {
						name := fmt.Sprintf("ci-%d.yaml", index)
						contents[index] = map[string]string{"name": name, "path": directory + "/" + name, "type": "file"}
					}
					_ = json.NewEncoder(writer).Encode(contents)
				default:
					if !strings.HasPrefix(request.URL.Path, "/repos/acme/example/contents/"+directory+"/ci-") {
						t.Errorf("unexpected GitHub request: %s", request.URL)
						http.NotFound(writer, request)
						return
					}
					fileCalls.Add(1)
					fmt.Fprintf(writer, `{"encoding":"base64","content":%q}`, base64.StdEncoding.EncodeToString([]byte(data)))
				}
			}))
			defer server.Close()
			now := time.Now()
			initialClient, delivery, handler, project := newPullRequestDeliveryTest(t, server, now)
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
			planner := &controller.WorkflowRunReconciler{
				Client: cluster, APIReader: cluster, GitHub: delivery.GitHub,
				GitHubAPIBase: server.URL, GitHubServerURL: "https://github.com", ActionCloneBaseURL: "https://github.com",
			}
			event := &payload{}
			event.Repository.ID = 1
			event.Repository.Name = "example"
			event.Repository.Owner.Login = "acme"
			for round, revision := range []string{strings.Repeat("a", 40), strings.Repeat("a", 40), strings.Repeat("b", 40)} {
				body := []byte(fmt.Sprintf(`{"sender":{"login":"octocat"},"round":%d}`, round))
				normalized := normalizedEvent{Name: "push", Actor: "octocat", SHA: revision, Ref: "refs/heads/main"}
				if err := handler.enqueueDelivery(t.Context(), project, event, normalized, fmt.Sprint(round), body); err != nil {
					t.Fatal(err)
				}
				request := ctrl.Request{NamespacedName: client.ObjectKey{Namespace: project.Namespace, Name: webhookDeliveryName(body)}}
				if _, err := delivery.Reconcile(t.Context(), request); err != nil {
					t.Fatal(err)
				}
				for index := range count {
					path := fmt.Sprintf("%s/ci-%d.yaml", directory, index)
					key := client.ObjectKey{Namespace: project.Namespace, Name: workflowRunName(path, string(project.UID), webhookReplayID(body))}
					if _, err := planner.Reconcile(t.Context(), ctrl.Request{NamespacedName: key}); err != nil {
						t.Fatal(err)
					}
					run := &actionsv1alpha1.WorkflowRun{}
					if err := cluster.Get(t.Context(), key, run); err != nil {
						t.Fatal(err)
					}
					if !meta.IsStatusConditionTrue(run.Status.Conditions, actionsv1alpha1.WorkflowRunConditionPlanned) || run.Status.Jobs == nil || run.Status.Jobs.Total != 1 {
						t.Fatalf("workflow was not planned: %#v", run.Status)
					}
				}
				wantFiles, wantDirectories := int32(count), int32(1)
				if round == 2 {
					wantFiles, wantDirectories = 2*int32(count), 2
				}
				t.Logf("round=%d workflows=%d: file GETs=%d, directory GETs=%d, token POSTs=%d", round, count, fileCalls.Load(), directoryCalls.Load(), tokenCalls.Load())
				if fileCalls.Load() != wantFiles || directoryCalls.Load() != wantDirectories || tokenCalls.Load() != 1 {
					t.Errorf("GitHub requests: files=%d, directories=%d, tokens=%d; want %d, %d, 1", fileCalls.Load(), directoryCalls.Load(), tokenCalls.Load(), wantFiles, wantDirectories)
				}
			}
		})
	}
}
