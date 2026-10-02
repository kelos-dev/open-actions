package console

import (
	"context"
	"fmt"
	"html"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os/exec"
	"strings"
	"testing"
	"time"

	actionsv1alpha1 "github.com/kelos-dev/open-actions/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

func TestConsoleRunFilterInteraction(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("node is not installed")
	}
	handler := newTestHandler(t, false)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("run list status = %d, want %d", response.Code, http.StatusOK)
	}
	command := exec.Command(node, "testdata/run_filters.cjs")
	command.Stdin = strings.NewReader(response.Body.String())
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("run filter interaction: %v\n%s", err, output)
	}
}

func TestWorkflowRunListFiltersBeforeLimit(t *testing.T) {
	store := readyWorkflowRunStore(slog.New(slog.NewTextHandler(io.Discard, nil)))
	now := time.Now()
	for index := 0; index < mainPageRunLimit+1; index++ {
		run := testWorkflowRun("default", fmt.Sprintf("busy-%d", index), now.Add(time.Duration(index)*time.Minute))
		store.upsert(run)
	}
	target := testWorkflowRun("team", "target", now.Add(-time.Hour))
	target.Spec.ProjectRef.Name = "other-project"
	target.Spec.Source.GitHub.Repository = actionsv1alpha1.GitHubRepository{ID: 456, Owner: "other", Name: "example"}
	target.Spec.WorkflowPath = ".open-actions/workflows/security.yaml"
	target.Status.WorkflowName = "CI"
	store.upsert(target)
	result := store.List(WorkflowRunFilter{}, mainPageRunLimit)
	if len(result.Runs) != mainPageRunLimit || !result.Truncated || len(result.Projects) != 2 || len(result.Repositories) != 2 || len(result.Workflows) != 2 {
		t.Fatalf("unfiltered list = %#v", result)
	}
	for _, filter := range []WorkflowRunFilter{
		{Project: "team/other-project"}, {RepositoryID: 456}, {WorkflowPath: target.Spec.WorkflowPath},
		{Project: "team/other-project", RepositoryID: 456, WorkflowPath: target.Spec.WorkflowPath},
	} {
		result = store.List(filter, mainPageRunLimit)
		if len(result.Runs) != 1 || result.Runs[0].Name != "target" || result.Truncated {
			t.Fatalf("filter %#v: %#v", filter, result)
		}
	}
	scoped := store.List(WorkflowRunFilter{Project: "team/other-project", RepositoryID: 456}, 1)
	if len(scoped.Projects) != 2 || len(scoped.Repositories) != 1 || scoped.Repositories[0].Label != "other/example" || len(scoped.Workflows) != 1 || scoped.Workflows[0].Value != target.Spec.WorkflowPath {
		t.Fatalf("cascading choices = %#v", scoped)
	}
}

func TestWorkflowRunListUsesIdentityAndExactWorkflowPath(t *testing.T) {
	store := readyWorkflowRunStore(slog.New(slog.NewTextHandler(io.Discard, nil)))
	now := time.Now()
	original := testWorkflowRun("team", "original", now)
	original.Status.WorkflowName = "CI"
	original.Spec.Source.GitHub.Repository = actionsv1alpha1.GitHubRepository{ID: 123, Owner: "acme", Name: "example"}
	store.upsert(original)
	for index, mutate := range []func(*actionsv1alpha1.WorkflowRun){
		func(run *actionsv1alpha1.WorkflowRun) {
			run.Spec.Source.GitHub.Repository.Owner = "other"
			run.Spec.Source.GitHub.Repository.ID = 456
		},
		func(run *actionsv1alpha1.WorkflowRun) { run.Spec.WorkflowPath = ".open-actions/workflows/test.yaml" },
		func(run *actionsv1alpha1.WorkflowRun) { run.Spec.WorkflowPath = ".open-actions/workflows/CI.yaml" },
		func(run *actionsv1alpha1.WorkflowRun) { run.Spec.ProjectRef.Name = "other" },
		func(run *actionsv1alpha1.WorkflowRun) { run.Namespace = "other" },
	} {
		run := original.DeepCopy()
		run.Name = fmt.Sprintf("sibling-%d", index)
		mutate(run)
		store.upsert(run)
	}
	filter := WorkflowRunFilter{Project: "team/project", RepositoryID: 123, WorkflowPath: original.Spec.WorkflowPath}
	result := store.List(filter, 100)
	if len(result.Runs) != 1 || result.Runs[0].Name != "original" || len(result.Workflows) != 3 {
		t.Fatalf("identity filter = %#v", result)
	}
	renamed := original.DeepCopy()
	renamed.Name = "renamed"
	renamed.Spec.Source.GitHub.Repository.Name = "renamed"
	renamed.CreationTimestamp = metav1.NewTime(now.Add(time.Minute))
	store.upsert(renamed)
	result = store.List(filter, 100)
	if len(result.Runs) != 2 {
		t.Fatalf("repository rename lost history: %#v", result.Runs)
	}
	if result.Repositories[0].Value != "123" || result.Repositories[0].Label != "acme/renamed" {
		t.Fatalf("repository choices = %#v", result.Repositories)
	}
	store.deleteObject(renamed)
	result = store.List(filter, 100)
	if result.Repositories[0].Label != "acme/example" {
		t.Fatalf("deleted run remains in choices: %#v", result.Repositories)
	}
}

func TestWorkflowRunListRetainsEmptySelection(t *testing.T) {
	store := readyWorkflowRunStore(slog.New(slog.NewTextHandler(io.Discard, nil)))
	filter := WorkflowRunFilter{Project: "team/project", RepositoryID: 123, WorkflowPath: ".open-actions/workflows/ci.yaml"}
	result := store.List(filter, 100)
	if len(result.Runs) != 0 || result.Truncated || len(result.Projects) != 1 || len(result.Repositories) != 1 || len(result.Workflows) != 1 || result.Workflows[0].Value != filter.WorkflowPath {
		t.Fatalf("empty filtered list = %#v", result)
	}
}

func TestConsoleFiltersRunsAndDurationRequests(t *testing.T) {
	h := newTestHandler(t, false)
	store := h.workflowRuns.(*WorkflowRunStore)
	target := testWorkflowRun("team", "target", time.Now().Add(-time.Hour))
	target.Spec.Source.GitHub.Repository.ID = 456
	target.Spec.Source.GitHub.Repository.Owner = "other"
	target.Spec.WorkflowPath = ".open-actions/workflows/test.yaml"
	store.upsert(target)
	query := url.Values{"project": {"team/project"}, "repository": {"456"}, "workflow": {target.Spec.WorkflowPath}}
	response := httptest.NewRecorder()
	h.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/?"+query.Encode(), nil))
	body := response.Body.String()
	for _, expected := range []string{
		`href="/runs/team/target"`, `aria-label="Workflow run count">1</span>`,
		`<option value="team/project" selected>team/project</option>`,
		`<option value="456" selected>other/example</option>`,
		`<option value=".open-actions/workflows/test.yaml" selected>`,
		`data-duration-url="/durations?run=team%2Ftarget"`,
		`<script id="run-durations" type="application/json">{"values":{"/runs/team/target":`,
	} {
		if !strings.Contains(body, expected) {
			t.Fatalf("filtered page missing %q: %d %s", expected, response.Code, body)
		}
	}
	if response.Code != 200 || strings.Contains(body, `href="/runs/default/ci"`) {
		t.Fatalf("filtered page = %d %s", response.Code, body)
	}
	durationURL, err := url.Parse(durationURLFromPage(t, body))
	if err != nil {
		t.Fatal(err)
	}
	if runs := durationURL.Query()["run"]; len(runs) != 1 || runs[0] != "team/target" {
		t.Fatalf("filtered duration request = %s", durationURL)
	}
	for _, repository := range []string{"abc", "-1", "0", "9223372036854775808"} {
		response := httptest.NewRecorder()
		h.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/?repository="+repository, nil))
		if response.Code != http.StatusBadRequest {
			t.Fatalf("repository %q: status %d", repository, response.Code)
		}
	}
	response = httptest.NewRecorder()
	h.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/?repository=789", nil))
	if response.Code != 200 || !strings.Contains(response.Body.String(), "No matching workflow runs") || !strings.Contains(response.Body.String(), `<option value="789" selected>Repository 789</option>`) {
		t.Fatalf("no matching runs = %d %s", response.Code, response.Body.String())
	}
}

func TestConsoleRunAndJobNavigation(t *testing.T) {
	h := newTestHandler(t, false)
	cluster := h.client.(client.Client)
	run := &actionsv1alpha1.WorkflowRun{}
	if err := cluster.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "ci"}, run); err != nil {
		t.Fatal(err)
	}
	run.Status.Identity.Number = 142
	run.Status.Identity.Attempt = 2
	if err := cluster.Update(context.Background(), run); err != nil {
		t.Fatal(err)
	}
	h.workflowRuns.(*WorkflowRunStore).upsert(run)
	for index, id := range []string{"test_linux", "test_macos"} {
		job := &actionsv1alpha1.WorkflowJob{
			ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("matrix-%d", index), Namespace: run.Namespace, Labels: map[string]string{actionsv1alpha1.LabelWorkflowRunUID: string(run.UID)}},
			Spec:       actionsv1alpha1.WorkflowJobSpec{WorkflowRunRef: corev1.LocalObjectReference{Name: run.Name}, JobID: id, DisplayName: "Test"},
		}
		if err := controllerutil.SetControllerReference(run, job, cluster.Scheme()); err != nil {
			t.Fatal(err)
		}
		if err := cluster.Create(context.Background(), job); err != nil {
			t.Fatal(err)
		}
	}
	for _, path := range []string{"/runs/default/ci", "/runs/default/ci/jobs/matrix-0"} {
		response := httptest.NewRecorder()
		h.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
		body := response.Body.String()
		for _, expected := range []string{
			`href="/?project=default%2Fproject"`,
			`href="/?project=default%2Fproject&amp;repository=123"`,
			`href="/?project=default%2Fproject&amp;repository=123&amp;workflow=.open-actions%2Fworkflows%2Fci.yaml"`,
			".open-actions/workflows/ci.yaml", "#142 · attempt 2", "test_linux", "test_macos",
		} {
			if !strings.Contains(body, expected) {
				t.Fatalf("%s missing %q: %d %s", path, expected, response.Code, body)
			}
		}
		if response.Code != 200 {
			t.Fatalf("%s: %d", path, response.Code)
		}
		title := "<title>CI · #142 · attempt 2 · acme/example · .open-actions/workflows/ci.yaml · default/project · Open Actions</title>"
		if strings.Contains(path, "/jobs/") {
			title = "<title>Test · CI · #142 · attempt 2 · acme/example · .open-actions/workflows/ci.yaml · default/project · Open Actions</title>"
		}
		if !strings.Contains(body, title) {
			t.Fatalf("page title missing %s", title)
		}
	}
}

func TestConsoleNavigationEscapesWorkflowMetadata(t *testing.T) {
	h := newTestHandler(t, false)
	run := testWorkflowRun("team", "pending", time.Now())
	run.Status.WorkflowName = `<script>alert("workflow")</script>`
	h.workflowRuns.(*WorkflowRunStore).upsert(run)
	response := httptest.NewRecorder()
	h.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/", nil))
	if body := response.Body.String(); strings.Contains(body, run.Status.WorkflowName) || !strings.Contains(body, html.EscapeString(run.Status.WorkflowName)) || !strings.Contains(body, `class="run-number">pending</span>`) {
		t.Fatalf("escaped pending run = %s", body)
	}
}
