package webhook

import (
	"encoding/base64"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	actionsv1alpha1 "github.com/kelos-dev/open-actions/api/v1alpha1"
	"github.com/kelos-dev/open-actions/internal/gitrepository"
	corev1 "k8s.io/api/core/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// GitHub's token trigger rules are documented at
// https://docs.github.com/en/actions/concepts/security/github_token#when-github_token-triggers-workflow-runs.
func TestJobTokenTriggerRules(t *testing.T) {
	for _, test := range []struct {
		event, action string
		allowed       bool
	}{
		{"issue_comment", "created", false},
		{"issue_comment", "edited", false},
		{"issue_comment", "deleted", false},
		{"push", "", false},
		{"issues", "labeled", false},
		{"release", "published", false},
		{"pull_request_review", "submitted", false},
		{"pull_request_review_comment", "created", false},
		{"pull_request", "opened", true},
		{"pull_request", "synchronize", true},
		{"pull_request", "reopened", true},
		{"pull_request", "labeled", false},
		{"pull_request", "edited", false},
		{"pull_request", "closed", false},
		{"pull_request_target", "opened", false},
		{"workflow_dispatch", "", true},
		{"repository_dispatch", "custom", true},
		{"workflow_run", "completed", false},
		{"schedule", "", true},
		{"workflow_call", "", true},
	} {
		t.Run(test.event+"/"+test.action, func(t *testing.T) {
			if got := allowJobTokenEvent(normalizedEvent{Name: test.event, Action: test.action}); got != test.allowed {
				t.Fatalf("allowed = %t, want %t", got, test.allowed)
			}
		})
	}
}

func TestJobTokenWorkflowRunTriggerRules(t *testing.T) {
	for _, test := range []struct {
		event   string
		allowed bool
	}{
		{"push", false},
		{"issue_comment", false},
		{"pull_request", false},
		{"pull_request_target", false},
		{"workflow_run", false},
		{"", false},
		{"workflow_dispatch", true},
		{"repository_dispatch", true},
	} {
		for _, action := range []string{"requested", "in_progress", "completed"} {
			t.Run(test.event+"/"+action, func(t *testing.T) {
				event := normalizedEvent{Name: "workflow_run", Action: action, WorkflowRun: &normalizedWorkflowRun{Event: test.event}}
				if got := allowJobTokenEvent(event); got != test.allowed {
					t.Fatalf("allowed = %t, want %t", got, test.allowed)
				}
			})
		}
	}
}

func TestDeliverySuppressesJobTokenEvents(t *testing.T) {
	for _, test := range []struct {
		name, event, action, actor string
		workflowRunEvent           string
		wantRuns                   int
		identityFailure            bool
	}{
		{name: "own comment", event: "issue_comment", action: "created", actor: "open-actions[bot]"},
		{name: "own edited comment", event: "issue_comment", action: "edited", actor: "open-actions[bot]"},
		{name: "own deleted comment", event: "issue_comment", action: "deleted", actor: "open-actions[bot]"},
		{name: "own push", event: "push", actor: "open-actions[bot]"},
		{name: "own label", event: "issues", action: "labeled", actor: "open-actions[bot]"},
		{name: "own release", event: "release", action: "published", actor: "open-actions[bot]"},
		{name: "case insensitive identity", event: "issue_comment", action: "created", actor: "Open-Actions[Bot]"},
		{name: "human comment", event: "issue_comment", action: "created", actor: "octocat", wantRuns: 1},
		{name: "unrelated App comment", event: "issue_comment", action: "created", actor: "another-app[bot]", wantRuns: 1},
		{name: "App comment edited by human", event: "issue_comment", action: "edited", actor: "octocat", wantRuns: 1},
		{name: "own push native workflow", event: "workflow_run", action: "completed", actor: "open-actions[bot]", workflowRunEvent: "push"},
		{name: "own native workflow with missing trigger", event: "workflow_run", action: "completed", actor: "open-actions[bot]"},
		{name: "own native workflow dispatch", event: "workflow_run", action: "completed", actor: "open-actions[bot]", workflowRunEvent: "workflow_dispatch", wantRuns: 1},
		{name: "own native repository dispatch", event: "workflow_run", action: "completed", actor: "open-actions[bot]", workflowRunEvent: "repository_dispatch", wantRuns: 1},
		{name: "human native workflow", event: "workflow_run", action: "completed", actor: "octocat", workflowRunEvent: "push", wantRuns: 1},
		{name: "unrelated App native workflow", event: "workflow_run", action: "completed", actor: "another-app[bot]", workflowRunEvent: "push", wantRuns: 1},
		{name: "native workflow identity lookup fails closed", event: "workflow_run", action: "completed", actor: "open-actions[bot]", workflowRunEvent: "push", identityFailure: true},
		{name: "identity lookup fails closed", event: "issue_comment", action: "created", actor: "open-actions[bot]", identityFailure: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			const workflowPath = ".open-actions/workflows/respond.yaml"
			workflowData := []byte("on: [issue_comment, push, issues, release]\njobs:\n  respond:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo response\n")
			if test.event == "workflow_run" {
				workflowData = []byte("on:\n  workflow_run:\n    workflows: [CI]\n    types: [completed]\njobs:\n  respond:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo response\n")
			}
			identityRequests, discoveryRequests := 0, 0
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				switch request.URL.Path {
				case "/app":
					identityRequests++
					if test.identityFailure {
						http.Error(writer, "unavailable", http.StatusServiceUnavailable)
						return
					}
					fmt.Fprint(writer, `{"id":1,"slug":"open-actions"}`)
				case "/app/installations/2/access_tokens":
					fmt.Fprint(writer, `{"token":"installation-token"}`)
				case "/repos/acme/example/contents/.open-actions/workflows":
					discoveryRequests++
					fmt.Fprintf(writer, `[{"name":"respond.yaml","path":%q,"type":"file"}]`, workflowPath)
				case "/repos/acme/example/contents/" + workflowPath:
					fmt.Fprintf(writer, `{"encoding":"base64","content":%q}`, base64.StdEncoding.EncodeToString(workflowData))
				default:
					http.NotFound(writer, request)
				}
			}))
			defer server.Close()
			cluster, reconciler, handler, project := newPullRequestDeliveryTest(t, server, time.Now())
			body := []byte(`{"comment":{"user":{"login":"open-actions[bot]"}}}`)
			delivery := queuedDelivery{
				ProjectName: project.Name, ProjectUID: string(project.UID),
				Repository: deliveryRepository{ID: 1, Owner: "acme", Name: "example"},
				Event: normalizedEvent{
					Name: test.event, Action: test.action, Actor: test.actor, WorkflowName: "CI",
					SHA: strings.Repeat("a", 40), Ref: "refs/heads/main",
					Issue: &normalizedIssue{Number: 17}, Comment: &normalizedComment{Body: "response"},
				},
				ReplayID: webhookReplayID(body), DeliveryID: "delivery",
			}
			if test.event == "workflow_run" {
				delivery.Event.WorkflowRun = &normalizedWorkflowRun{Event: test.workflowRunEvent, Conclusion: "success", HeadSHA: strings.Repeat("a", 40)}
			}
			if err := handler.enqueueQueuedDelivery(t.Context(), project, delivery, body, nil); err != nil {
				t.Fatal(err)
			}
			key := client.ObjectKey{Namespace: project.Namespace, Name: webhookDeliveryName(body)}
			_, err := reconciler.Reconcile(t.Context(), ctrl.Request{NamespacedName: key})
			if (err != nil) != test.identityFailure {
				t.Fatalf("Reconcile() error = %v", err)
			}
			if err != nil && !strings.Contains(err.Error(), `Project "default"`) {
				t.Fatalf("error does not identify Project: %v", err)
			}
			stored := &corev1.ConfigMap{}
			if err := cluster.Get(t.Context(), key, stored); err != nil {
				t.Fatal(err)
			}
			if test.identityFailure {
				if stored.Data[deliveryStateKey] != "" {
					t.Fatal("identity failure permanently completed delivery")
				}
			} else if stored.Data[deliveryStateKey] != deliveryStateCompleted || stored.Data[deliveryRunCountKey] != fmt.Sprint(test.wantRuns) {
				t.Fatalf("delivery data = %#v", stored.Data)
			}
			runs := &actionsv1alpha1.WorkflowRunList{}
			if err := cluster.List(t.Context(), runs); err != nil {
				t.Fatal(err)
			}
			if len(runs.Items) != test.wantRuns || discoveryRequests != test.wantRuns {
				t.Fatalf("runs = %d, discovery requests = %d, want %d", len(runs.Items), discoveryRequests, test.wantRuns)
			}
			if test.actor == "octocat" && identityRequests != 0 {
				t.Fatal("human event required an App identity lookup")
			}
			if test.wantRuns == 1 && (runs.Items[0].Spec.Source.GitHub.Actor != test.actor || runs.Items[0].Spec.WorkflowPath != workflowPath) {
				t.Fatalf("created WorkflowRun = %#v", runs.Items[0].Spec)
			}
		})
	}
}

func TestJobTokenPullRequestRequiresApproval(t *testing.T) {
	for _, action := range []string{"opened", "synchronize", "reopened"} {
		t.Run(action, func(t *testing.T) {
			workflowData := []byte("on: [pull_request, pull_request_target]\njobs:\n  test:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo test\n")
			gitRoot, revision := testPullRequestGitRepository(t, workflowData, nil, false)
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				switch request.URL.Path {
				case "/app":
					fmt.Fprint(writer, `{"id":1,"slug":"open-actions"}`)
				case "/app/installations/2/access_tokens":
					fmt.Fprint(writer, `{"token":"installation-token"}`)
				case "/repos/acme/example/compare/" + revision.BaseSHA + "..." + revision.HeadSHA:
					fmt.Fprintf(writer, `{"merge_base_commit":{"sha":%q}}`, revision.MergeBaseSHA)
				default:
					http.NotFound(writer, request)
				}
			}))
			defer server.Close()
			cluster, reconciler, handler, project := newPullRequestDeliveryTest(t, server, time.Now())
			reconciler.GitRepository, _ = gitrepository.NewClient(gitRoot)
			body := []byte(`{"delivery":"job-token-pr"}`)
			delivery := queuedDelivery{
				ProjectName: project.Name, ProjectUID: string(project.UID),
				Repository: deliveryRepository{ID: 1, Owner: "acme", Name: "example"},
				Event: normalizedEvent{
					Name: "pull_request", Action: action, Actor: "open-actions[bot]", Ref: "refs/pull/9/merge",
					HeadRef: "feature", BaseRef: "main", HeadSHA: revision.HeadSHA, MergeRevision: true,
					PullRequest: &normalizedPullRequest{
						Number: 9, HTMLURL: "https://github.com/acme/example/pull/9",
						HeadRef: "feature", HeadSHA: revision.HeadSHA, BaseRef: "main", BaseSHA: revision.BaseSHA,
						HeadRepository: normalizedRepository{ID: 1, Owner: "acme", Name: "example"},
					},
				},
				ReplayID: webhookReplayID(body), DeliveryID: "delivery",
			}
			if err := handler.enqueueQueuedDelivery(t.Context(), project, delivery, body, nil); err != nil {
				t.Fatal(err)
			}
			key := client.ObjectKey{Namespace: project.Namespace, Name: webhookDeliveryName(body)}
			if _, err := reconciler.Reconcile(t.Context(), ctrl.Request{NamespacedName: key}); err != nil {
				t.Fatal(err)
			}
			runs := &actionsv1alpha1.WorkflowRunList{}
			if err := cluster.List(t.Context(), runs); err != nil {
				t.Fatal(err)
			}
			if len(runs.Items) != 1 {
				t.Fatalf("WorkflowRuns = %d, want only ordinary pull_request", len(runs.Items))
			}
			run := &runs.Items[0]
			if run.Spec.Source.GitHub.Event.Name != actionsv1alpha1.GitHubEventNamePullRequest || run.Spec.Approval == nil || run.Spec.Approval.Approved || run.Spec.ForkPullRequest != nil {
				t.Fatalf("pull request spec = %#v", run.Spec)
			}
			run.Spec.Approval.Approved = true
			if err := cluster.Update(t.Context(), run); err != nil {
				t.Fatal(err)
			}
			stored := &corev1.ConfigMap{}
			if err := cluster.Get(t.Context(), key, stored); err != nil {
				t.Fatal(err)
			}
			delete(stored.Data, deliveryStateKey)
			if err := cluster.Update(t.Context(), stored); err != nil {
				t.Fatal(err)
			}
			if _, err := reconciler.Reconcile(t.Context(), ctrl.Request{NamespacedName: key}); err != nil {
				t.Fatalf("replay after approval: %v", err)
			}
			if err := cluster.Get(t.Context(), client.ObjectKeyFromObject(run), run); err != nil || !run.Spec.Approval.Approved {
				t.Fatalf("replay changed approval: %v", err)
			}
		})
	}
}
