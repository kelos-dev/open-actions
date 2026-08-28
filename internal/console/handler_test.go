package console

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	actionsv1alpha1 "github.com/kelos-dev/open-actions/api/v1alpha1"
	githubclient "github.com/kelos-dev/open-actions/internal/github"
	"github.com/kelos-dev/open-actions/internal/projectvalue"
	"github.com/kelos-dev/open-actions/internal/workflowrun"
	"github.com/kelos-dev/open-actions/internal/workflowsnapshot"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const testConsoleToken = "test-console-token"

type testLogSource struct {
	pod  *corev1.Pod
	logs string
}

type testRepositoryResolver struct {
	repository actionsv1alpha1.GitHubRepository
	err        error
}

func (r *testRepositoryResolver) Resolve(_ context.Context, _ *actionsv1alpha1.Project, owner, name string) (actionsv1alpha1.GitHubRepository, error) {
	if r.err != nil {
		return actionsv1alpha1.GitHubRepository{}, r.err
	}
	if r.repository.ID != 0 {
		return r.repository, nil
	}
	return actionsv1alpha1.GitHubRepository{ID: 123, Owner: owner, Name: name}, nil
}

func TestConsoleTopbarsUseConsistentSpacing(t *testing.T) {
	pages := map[string]string{
		"workflow runs": mainPageTemplate,
		"projects":      projectsPageTemplate,
		"project":       projectPageTemplate,
		"dispatch":      dispatchPageTemplate,
		"workflow run":  runPageTemplate,
		"runner logs":   logPageTemplate,
	}

	for name, page := range pages {
		t.Run(name, func(t *testing.T) {
			if !strings.Contains(page, ".topbar{height:64px;display:flex;align-items:center;gap:24px;") {
				t.Fatal("topbar does not separate navigation links")
			}
			if !strings.Contains(page, "@media(max-width:800px){.topbar{padding:0 16px}") {
				t.Fatal("topbar does not use mobile padding")
			}
		})
	}
}

func (s *testLogSource) ListPods(context.Context, string, string) (*corev1.PodList, error) {
	return &corev1.PodList{Items: []corev1.Pod{*s.pod.DeepCopy()}}, nil
}

func (s *testLogSource) Stream(context.Context, string, string) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader(s.logs)), nil
}

func TestConsoleServesWorkflowRunsAndLogsWithoutAuthentication(t *testing.T) {
	handler := newTestHandler(t, true)
	runURL := "/runs/default/ci"

	runResponse := httptest.NewRecorder()
	handler.ServeHTTP(runResponse, httptest.NewRequest(http.MethodGet, runURL, nil))
	runBody := runResponse.Body.String()
	if runResponse.Code != http.StatusOK || !strings.Contains(runBody, "CI") || !strings.Contains(runBody, "build") || !strings.Contains(runBody, "Workflow run Queued") || !strings.Contains(runBody, "name: CI\non:\n  push:") || !strings.Contains(runBody, "&lt;script&gt;alert(&#39;unsafe&#39;)&lt;/script&gt;") || strings.Contains(runBody, "<script>alert('unsafe')</script>") {
		t.Fatalf("run page = %d, %q", runResponse.Code, runResponse.Body.String())
	}

	logResponse := httptest.NewRecorder()
	handler.ServeHTTP(logResponse, httptest.NewRequest(http.MethodGet, runURL+"/jobs/build", nil))
	if logResponse.Code != http.StatusOK || strings.Contains(logResponse.Body.String(), "Show debug") || !strings.Contains(logResponse.Body.String(), "Show timestamps") {
		t.Fatalf("log page = %d, %q", logResponse.Code, logResponse.Body.String())
	}

	streamResponse := httptest.NewRecorder()
	handler.ServeHTTP(streamResponse, httptest.NewRequest(http.MethodGet, runURL+"/jobs/build/stream", nil))
	if streamResponse.Code != http.StatusOK || !strings.Contains(streamResponse.Body.String(), "id: 1\nevent: log") || !strings.Contains(streamResponse.Body.String(), "build output") {
		t.Fatalf("log stream = %d, %q", streamResponse.Code, streamResponse.Body.String())
	}
}

func TestConsoleReportsUnavailableWorkflowFile(t *testing.T) {
	for _, test := range []struct {
		name  string
		setup func(*testing.T, *Handler)
	}{
		{name: "missing annotation", setup: func(t *testing.T, handler *Handler) {
			run := &actionsv1alpha1.WorkflowRun{}
			key := client.ObjectKey{Namespace: "default", Name: "ci"}
			if err := handler.client.Get(context.Background(), key, run); err != nil {
				t.Fatal(err)
			}
			delete(run.Annotations, actionsv1alpha1.AnnotationWorkflowFile)
			if err := handler.client.Update(context.Background(), run); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "missing ConfigMap", setup: func(t *testing.T, handler *Handler) {
			configMap := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "ci-workflow-file", Namespace: "default"}}
			if err := handler.client.Delete(context.Background(), configMap); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			handler := newTestHandler(t, false)
			test.setup(t, handler)
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/runs/default/ci", nil))
			if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), "The workflow file is not available for this run.") {
				t.Fatalf("run page = %d, %q", response.Code, response.Body.String())
			}
		})
	}
}

func TestConsoleRejectsWorkflowFileNotOwnedByRun(t *testing.T) {
	handler := newTestHandler(t, false)
	configMap := &corev1.ConfigMap{}
	key := client.ObjectKey{Namespace: "default", Name: "ci-workflow-file"}
	if err := handler.client.Get(context.Background(), key, configMap); err != nil {
		t.Fatal(err)
	}
	configMap.OwnerReferences = nil
	if err := handler.client.Update(context.Background(), configMap); err != nil {
		t.Fatal(err)
	}
	run := &actionsv1alpha1.WorkflowRun{}
	if err := handler.client.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "ci"}, run); err != nil {
		t.Fatal(err)
	}
	if _, _, err := handler.loadWorkflowFile(context.Background(), run); err == nil || !strings.Contains(err.Error(), `ConfigMap "ci-workflow-file"`) || !strings.Contains(err.Error(), `WorkflowRun "ci"`) {
		t.Fatalf("loadWorkflowFile() error = %v", err)
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/runs/default/ci/jobs/build", nil))
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `body data-stream-url="/runs/default/ci/jobs/build/stream"`) {
		t.Fatalf("job log page = %d, %q", response.Code, response.Body.String())
	}
}

func TestConsoleCreatesAdministratorSession(t *testing.T) {
	handler := newTestHandler(t, true)
	loginPage := httptest.NewRecorder()
	handler.ServeHTTP(loginPage, httptest.NewRequest(http.MethodGet, "/login", nil))
	if loginPage.Code != http.StatusOK || !strings.Contains(loginPage.Body.String(), "administrator token") {
		t.Fatalf("login page = %d, %q", loginPage.Code, loginPage.Body.String())
	}

	invalidLogin := httptest.NewRecorder()
	handler.ServeHTTP(invalidLogin, httptest.NewRequest(http.MethodPost, "/api/login", strings.NewReader(`{"token":"wrong"}`)))
	if invalidLogin.Code != http.StatusUnauthorized {
		t.Fatalf("invalid login status = %d", invalidLogin.Code)
	}

	login := httptest.NewRecorder()
	handler.ServeHTTP(login, httptest.NewRequest(http.MethodPost, "/api/login", strings.NewReader(`{"token":"`+testConsoleToken+`"}`)))
	if login.Code != http.StatusOK {
		t.Fatalf("login response = %d, %q", login.Code, login.Body.String())
	}
	sessionCookie := responseCookie(t, login.Result(), sessionCookieName)
	if sessionCookie.Value == testConsoleToken || !sessionCookie.HttpOnly || !sessionCookie.Secure || sessionCookie.SameSite != http.SameSiteLaxMode {
		t.Fatalf("session cookie = %#v", sessionCookie)
	}

	projectRequest := httptest.NewRequest(http.MethodGet, "/projects/default/project", nil)
	projectRequest.AddCookie(sessionCookie)
	projectResponse := httptest.NewRecorder()
	handler.ServeHTTP(projectResponse, projectRequest)
	if projectResponse.Code != http.StatusOK || !strings.Contains(projectResponse.Body.String(), "Add or replace") || !strings.Contains(projectResponse.Body.String(), ">Delete</button>") {
		t.Fatalf("authenticated Project page = %d, %q", projectResponse.Code, projectResponse.Body.String())
	}
}

func TestConsoleKeepsTimestampedLogContentOnOneGridRow(t *testing.T) {
	handler := newTestHandler(t, false)
	request := httptest.NewRequest(http.MethodGet, "/runs/default/ci/jobs/build", nil)
	request.Header.Set("Authorization", "Bearer "+testConsoleToken)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("log page status = %d, want %d", response.Code, http.StatusOK)
	}
	if !strings.Contains(response.Body.String(), ".show-time .log-line{grid-template-columns:auto auto auto minmax(0,1fr)}") {
		t.Fatal("log page does not provide a fourth grid column for timestamps")
	}
}

func TestConsoleKeepsFourDigitLineNumbersOnOneRow(t *testing.T) {
	handler := newTestHandler(t, false)
	request := httptest.NewRequest(http.MethodGet, "/runs/default/ci/jobs/build", nil)
	request.Header.Set("Authorization", "Bearer "+testConsoleToken)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("log page status = %d, want %d", response.Code, http.StatusOK)
	}
	if !strings.Contains(response.Body.String(), ".line-number{width:42px;padding-right:14px;color:#484f58;text-align:right;white-space:nowrap;user-select:none}") {
		t.Fatal("log page allows line numbers to wrap")
	}
}

func TestConsoleRendersSkippedWorkflowSteps(t *testing.T) {
	handler := newTestHandler(t, false)
	request := httptest.NewRequest(http.MethodGet, "/runs/default/ci/jobs/build", nil)
	request.Header.Set("Authorization", "Bearer "+testConsoleToken)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("log page status = %d, want %d", response.Code, http.StatusOK)
	}
	for _, expected := range []string{
		`.group-status.skipped{color:#8b949e}`,
		`skipped:'Skipped'`,
		`details.open=!entry.conclusion`,
		`if(entry.conclusion){setGroupConclusion(container,entry.conclusion);return}`,
	} {
		if !strings.Contains(response.Body.String(), expected) {
			t.Fatalf("log page does not support skipped workflow steps: missing %q", expected)
		}
	}
}

func TestConsoleResumesReconnectedLogStream(t *testing.T) {
	handler := newTestHandler(t, false)
	source := handler.logs.(*testLogSource)
	source.logs = "first\nsecond\nthird\n"

	request := httptest.NewRequest(http.MethodGet, "/runs/default/ci/jobs/build/stream", nil)
	request.Header.Set("Authorization", "Bearer "+testConsoleToken)
	request.Header.Set("Last-Event-ID", "2")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	body := response.Body.String()
	if response.Code != http.StatusOK || strings.Contains(body, `"text":"first"`) || strings.Contains(body, `"text":"second"`) || !strings.Contains(body, "id: 3\nevent: log") || !strings.Contains(body, `"text":"third"`) {
		t.Fatalf("resumed log stream = %d, %q", response.Code, body)
	}
}

func TestConsoleBatchesLogStream(t *testing.T) {
	tests := []struct {
		name               string
		logs               string
		entryCount         int
		maxEntriesPerBatch int
	}{
		{
			name:               "entry limit",
			logs:               strings.Repeat("output\n", logBatchEntryLimit+1),
			entryCount:         logBatchEntryLimit + 1,
			maxEntriesPerBatch: logBatchEntryLimit,
		},
		{
			name:               "byte limit",
			logs:               strings.Repeat("a", 40<<10) + "\n" + strings.Repeat("b", 40<<10) + "\n",
			entryCount:         2,
			maxEntriesPerBatch: 1,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			handler := newTestHandler(t, false)
			handler.logs.(*testLogSource).logs = test.logs
			request := httptest.NewRequest(http.MethodGet, "/runs/default/ci/jobs/build/stream", nil)
			request.Header.Set("Authorization", "Bearer "+testConsoleToken)
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			body := response.Body.String()

			if response.Code != http.StatusOK {
				t.Fatalf("log stream status = %d, body %q", response.Code, body)
			}
			batches := parseTestLogBatches(t, body)
			if len(batches) < 2 {
				t.Fatalf("log stream contains %d batches, want at least 2: %q", len(batches), body)
			}
			entries := 0
			for _, batch := range batches {
				entries += len(batch.entries)
				if len(batch.entries) > test.maxEntriesPerBatch {
					t.Fatalf("batch contains %d entries, want at most %d", len(batch.entries), test.maxEntriesPerBatch)
				}
				if batch.id != uint64(entries) {
					t.Fatalf("batch ID = %d, want %d", batch.id, entries)
				}
				if batch.encodedBytes > logBatchByteLimit+1 {
					t.Fatalf("batch contains %d encoded bytes, want at most %d", batch.encodedBytes, logBatchByteLimit+1)
				}
			}
			if entries != test.entryCount {
				t.Fatalf("log stream contains %d entries, want %d", entries, test.entryCount)
			}
		})
	}
}

type testLogBatch struct {
	id           uint64
	entries      []logEntry
	encodedBytes int
}

func parseTestLogBatches(t *testing.T, body string) []testLogBatch {
	t.Helper()
	var batches []testLogBatch
	for _, event := range strings.Split(body, "\n\n") {
		if !strings.Contains(event, "event: log\n") {
			continue
		}
		batch := testLogBatch{}
		for _, line := range strings.Split(event, "\n") {
			if value, found := strings.CutPrefix(line, "id: "); found {
				id, err := strconv.ParseUint(value, 10, 64)
				if err != nil {
					t.Fatalf("parse batch ID %q: %v", value, err)
				}
				batch.id = id
			}
			if value, found := strings.CutPrefix(line, "data: "); found {
				batch.encodedBytes = len(value)
				if err := json.Unmarshal([]byte(value), &batch.entries); err != nil {
					t.Fatalf("parse log batch: %v", err)
				}
			}
		}
		batches = append(batches, batch)
	}
	return batches
}

func TestConsoleMainPageListsWorkflowRunsNewestFirst(t *testing.T) {
	handler := newTestHandler(t, false)

	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/", nil))
	body := response.Body.String()
	if response.Code != http.StatusOK || !strings.Contains(body, "Workflow runs") || !strings.Contains(body, `href="/runs/default/ci"`) || !strings.Contains(body, "acme/example") || !strings.Contains(body, "default/project") {
		t.Fatalf("main page = %d, %q", response.Code, body)
	}
	newer := strings.Index(body, "<strong>Lint</strong>")
	older := strings.Index(body, "<strong>CI</strong>")
	if newer == -1 || older == -1 || newer >= older {
		t.Fatalf("main page is not newest first: %s", body)
	}
}

func TestNewerRunQueryDetectsConcurrencyReplacementAndRerun(t *testing.T) {
	handler := newTestHandler(t, false)
	clusterClient := handler.client.(client.Client)
	current := &actionsv1alpha1.WorkflowRun{}
	if err := clusterClient.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "ci"}, current); err != nil {
		t.Fatal(err)
	}

	query := func(name string) newerRunResponse {
		t.Helper()
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/v1/runs/default/"+name+"/newer", nil))
		if response.Code != http.StatusOK {
			t.Fatalf("newer run query = %d, %q", response.Code, response.Body.String())
		}
		result := newerRunResponse{}
		if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
			t.Fatal(err)
		}
		return result
	}
	if response := query(current.Name); response.Newer || response.Superseding != nil ||
		response.Current.ID != "1" || response.Current.Actor != "octocat" {
		t.Fatalf("initial query = %#v", response)
	}

	replacement := current.DeepCopy()
	replacement.ResourceVersion = ""
	replacement.Name = "ci-replacement"
	replacement.UID = "replacement-uid"
	replacement.Status.Identity = &actionsv1alpha1.WorkflowRunIdentityStatus{
		ID: 2, Number: 2, Attempt: 1, URL: "https://actions.example/runs/default/ci-replacement",
	}
	replacement.Status.Concurrency = &actionsv1alpha1.ConcurrencyStatus{Group: "ci-main"}
	if err := clusterClient.Create(context.Background(), replacement); err != nil {
		t.Fatal(err)
	}
	if response := query(current.Name); !response.Newer || response.Superseding == nil || response.Superseding.ID != "2" || response.Superseding.Attempt != "1" {
		t.Fatalf("replacement query = %#v", response)
	}

	rerun := replacement.DeepCopy()
	rerun.ResourceVersion = ""
	rerun.Name = "ci-replacement-attempt-2"
	rerun.UID = "rerun-uid"
	rerun.Spec.Rerun = &actionsv1alpha1.WorkflowRunRerun{
		OriginalRunRef: actionsv1alpha1.WorkflowRunReference{Name: replacement.Name, UID: replacement.UID},
		PreviousRunRef: actionsv1alpha1.WorkflowRunReference{Name: replacement.Name, UID: replacement.UID},
		Attempt:        2,
	}
	rerun.Status.Identity = &actionsv1alpha1.WorkflowRunIdentityStatus{
		ID: 2, Number: 2, Attempt: 2, URL: "https://actions.example/runs/default/ci-replacement-attempt-2",
	}
	if err := clusterClient.Create(context.Background(), rerun); err != nil {
		t.Fatal(err)
	}
	if response := query(replacement.Name); !response.Newer || response.Superseding == nil || response.Superseding.ID != "2" || response.Superseding.Attempt != "2" {
		t.Fatalf("rerun query = %#v", response)
	}
}

func TestConsoleMainPageLimitsWorkflowRuns(t *testing.T) {
	handler := newTestHandler(t, false)
	clusterClient, ok := handler.client.(client.Client)
	if !ok {
		t.Fatal("test Console reader is not a client")
	}
	base := &actionsv1alpha1.WorkflowRun{}
	if err := clusterClient.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "ci"}, base); err != nil {
		t.Fatal(err)
	}
	oldestName := ""
	for index := 0; index < mainPageRunLimit-1; index++ {
		run := base.DeepCopy()
		run.Name = fmt.Sprintf("older-%03d", index)
		run.UID = types.UID(run.Name)
		run.ResourceVersion = ""
		run.CreationTimestamp = metav1.NewTime(time.Date(2026, 8, 10, 11, 0, 0, 0, time.UTC).Add(-time.Duration(index) * time.Minute))
		run.Status.WorkflowName = run.Name
		if err := clusterClient.Create(context.Background(), run); err != nil {
			t.Fatal(err)
		}
		oldestName = run.Name
	}

	request := httptest.NewRequest(http.MethodGet, "/", nil)
	request.Header.Set("Authorization", "Bearer "+testConsoleToken)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	body := response.Body.String()
	if response.Code != http.StatusOK || !strings.Contains(body, `aria-label="Workflow run count">100</span>`) || !strings.Contains(body, "Showing 100 most recent runs") {
		t.Fatalf("main page = %d, %q", response.Code, body)
	}
	if strings.Contains(body, oldestName) {
		t.Fatalf("main page contains truncated WorkflowRun %q", oldestName)
	}
}

func TestConsoleCancelsActiveWorkflow(t *testing.T) {
	handler := newTestHandler(t, false)
	runURL := "/runs/default/ci"
	cancelURL := runURL + "/cancel"

	page := httptest.NewRecorder()
	handler.ServeHTTP(page, httptest.NewRequest(http.MethodGet, runURL, nil))
	if page.Code != http.StatusOK || !strings.Contains(page.Body.String(), "Sign in to cancel") || strings.Contains(page.Body.String(), handler.csrfToken) {
		t.Fatalf("unauthenticated run page = %d, %q", page.Code, page.Body.String())
	}

	form := url.Values{"csrf": {handler.csrfToken}}
	unauthenticated := httptest.NewRequest(http.MethodPost, cancelURL, strings.NewReader(form.Encode()))
	unauthenticated.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	unauthenticatedResponse := httptest.NewRecorder()
	handler.ServeHTTP(unauthenticatedResponse, unauthenticated)
	if unauthenticatedResponse.Code != http.StatusFound || unauthenticatedResponse.Header().Get("Location") != "/login?next=%2Fruns%2Fdefault%2Fci" {
		t.Fatalf("unauthenticated cancellation response = %d, %q", unauthenticatedResponse.Code, unauthenticatedResponse.Header().Get("Location"))
	}

	authenticatedPageRequest := httptest.NewRequest(http.MethodGet, runURL, nil)
	authenticatedPageRequest.Header.Set("Authorization", "Bearer "+testConsoleToken)
	authenticatedPage := httptest.NewRecorder()
	handler.ServeHTTP(authenticatedPage, authenticatedPageRequest)
	if authenticatedPage.Code != http.StatusOK || !strings.Contains(authenticatedPage.Body.String(), `action="`+cancelURL+`"`) || !strings.Contains(authenticatedPage.Body.String(), "Cancel workflow") || !strings.Contains(authenticatedPage.Body.String(), handler.csrfToken) {
		t.Fatalf("authenticated run page = %d, %q", authenticatedPage.Code, authenticatedPage.Body.String())
	}

	invalidRequest := httptest.NewRequest(http.MethodPost, cancelURL, strings.NewReader("csrf=invalid"))
	invalidRequest.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	invalidRequest.Header.Set("Authorization", "Bearer "+testConsoleToken)
	invalidResponse := httptest.NewRecorder()
	handler.ServeHTTP(invalidResponse, invalidRequest)
	if invalidResponse.Code != http.StatusForbidden {
		t.Fatalf("invalid CSRF response = %d, want %d", invalidResponse.Code, http.StatusForbidden)
	}

	request := httptest.NewRequest(http.MethodPost, cancelURL, strings.NewReader(form.Encode()))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	request.Header.Set("Authorization", "Bearer "+testConsoleToken)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusSeeOther || response.Header().Get("Location") != runURL {
		t.Fatalf("cancellation response = %d, %q", response.Code, response.Header().Get("Location"))
	}
	run := &actionsv1alpha1.WorkflowRun{}
	if err := handler.client.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "ci"}, run); err != nil {
		t.Fatal(err)
	}
	if !run.Spec.CancelRequested {
		t.Fatal("WorkflowRun cancellation was not requested")
	}

	cancellingPageRequest := httptest.NewRequest(http.MethodGet, runURL, nil)
	cancellingPageRequest.Header.Set("Authorization", "Bearer "+testConsoleToken)
	cancellingPage := httptest.NewRecorder()
	handler.ServeHTTP(cancellingPage, cancellingPageRequest)
	if cancellingPage.Code != http.StatusOK || !strings.Contains(cancellingPage.Body.String(), "Workflow run Cancelling") || strings.Contains(cancellingPage.Body.String(), "Cancel workflow") {
		t.Fatalf("cancelling run page = %d, %q", cancellingPage.Code, cancellingPage.Body.String())
	}

	repeatedRequest := httptest.NewRequest(http.MethodPost, cancelURL, strings.NewReader(form.Encode()))
	repeatedRequest.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	repeatedRequest.Header.Set("Authorization", "Bearer "+testConsoleToken)
	repeatedResponse := httptest.NewRecorder()
	handler.ServeHTTP(repeatedResponse, repeatedRequest)
	if repeatedResponse.Code != http.StatusSeeOther || repeatedResponse.Header().Get("Location") != runURL {
		t.Fatalf("repeated cancellation response = %d, %q", repeatedResponse.Code, repeatedResponse.Header().Get("Location"))
	}
}

func TestConsoleApprovesForkPullRequestRevision(t *testing.T) {
	handler := newTestHandler(t, false)
	run := &actionsv1alpha1.WorkflowRun{}
	key := client.ObjectKey{Namespace: "default", Name: "ci"}
	if err := handler.client.Get(context.Background(), key, run); err != nil {
		t.Fatal(err)
	}
	run.Spec.ForkPullRequest = &actionsv1alpha1.WorkflowRunForkPullRequest{RequireApproval: true}
	run.Spec.Source.GitHub.Event.Name = actionsv1alpha1.GitHubEventNamePullRequest
	run.Spec.Source.GitHub.Event.Action = "synchronize"
	run.Spec.Source.GitHub.Event.PullRequest = &actionsv1alpha1.GitHubPullRequest{
		Number: 7, Body: "Pull request body", HTMLURL: "https://github.com/acme/example/pull/7",
		HeadRepository: actionsv1alpha1.GitHubRepository{ID: 456, Owner: "contributor", Name: "example"},
		HeadRef:        "feature", HeadSHA: strings.Repeat("b", 40), BaseRef: "main",
	}
	if err := handler.client.Update(context.Background(), run); err != nil {
		t.Fatal(err)
	}
	runURL := "/runs/default/ci"
	approveURL := runURL + "/approve"

	page := httptest.NewRecorder()
	handler.ServeHTTP(page, httptest.NewRequest(http.MethodGet, runURL, nil))
	if page.Code != http.StatusOK || !strings.Contains(page.Body.String(), "Sign in to approve") || strings.Contains(page.Body.String(), handler.csrfToken) {
		t.Fatalf("unauthenticated run page = %d, %q", page.Code, page.Body.String())
	}

	authenticatedPageRequest := httptest.NewRequest(http.MethodGet, runURL, nil)
	authenticatedPageRequest.Header.Set("Authorization", "Bearer "+testConsoleToken)
	authenticatedPage := httptest.NewRecorder()
	handler.ServeHTTP(authenticatedPage, authenticatedPageRequest)
	if authenticatedPage.Code != http.StatusOK || !strings.Contains(authenticatedPage.Body.String(), `action="`+approveURL+`"`) || !strings.Contains(authenticatedPage.Body.String(), "Approve workflow") {
		t.Fatalf("authenticated run page = %d, %q", authenticatedPage.Code, authenticatedPage.Body.String())
	}

	form := url.Values{"csrf": {handler.csrfToken}}
	request := httptest.NewRequest(http.MethodPost, approveURL, strings.NewReader(form.Encode()))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	request.Header.Set("Authorization", "Bearer "+testConsoleToken)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusSeeOther || response.Header().Get("Location") != runURL {
		t.Fatalf("approval response = %d, %q", response.Code, response.Header().Get("Location"))
	}
	if err := handler.client.Get(context.Background(), key, run); err != nil {
		t.Fatal(err)
	}
	if run.Spec.ForkPullRequest == nil || !run.Spec.ForkPullRequest.Approved {
		t.Fatalf("fork pull request policy = %#v", run.Spec.ForkPullRequest)
	}
}

func TestConsoleRejectsCancellationAfterWorkflowCompletes(t *testing.T) {
	handler := newTestHandler(t, false)
	run := &actionsv1alpha1.WorkflowRun{}
	if err := handler.client.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "ci"}, run); err != nil {
		t.Fatal(err)
	}
	run.Status.Conditions = []metav1.Condition{{
		Type: actionsv1alpha1.WorkflowRunConditionSucceeded, Status: metav1.ConditionTrue, Reason: "JobsSucceeded",
	}}
	if err := handler.client.Update(context.Background(), run); err != nil {
		t.Fatal(err)
	}

	form := url.Values{"csrf": {handler.csrfToken}}
	request := httptest.NewRequest(http.MethodPost, "/runs/default/ci/cancel", strings.NewReader(form.Encode()))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	request.Header.Set("Authorization", "Bearer "+testConsoleToken)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusConflict || !strings.Contains(response.Body.String(), `WorkflowRun "ci" is already complete`) {
		t.Fatalf("cancellation response = %d, %q", response.Code, response.Body.String())
	}
	if err := handler.client.Get(context.Background(), client.ObjectKeyFromObject(run), run); err != nil {
		t.Fatal(err)
	}
	if run.Spec.CancelRequested {
		t.Fatal("completed WorkflowRun cancellation was requested")
	}
}

func TestConsoleRerunsCompletedWorkflow(t *testing.T) {
	handler := newTestHandler(t, false)
	run := &actionsv1alpha1.WorkflowRun{}
	if err := handler.client.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "ci"}, run); err != nil {
		t.Fatal(err)
	}
	run.Status.Conditions = []metav1.Condition{{
		Type: actionsv1alpha1.WorkflowRunConditionSucceeded, Status: metav1.ConditionTrue, Reason: "JobsSucceeded",
	}}
	if err := handler.client.Update(context.Background(), run); err != nil {
		t.Fatal(err)
	}

	page := httptest.NewRecorder()
	handler.ServeHTTP(page, httptest.NewRequest(http.MethodGet, "/runs/default/ci", nil))
	if page.Code != http.StatusOK || !strings.Contains(page.Body.String(), "Sign in to re-run") {
		t.Fatalf("unauthenticated run page = %d, %q", page.Code, page.Body.String())
	}

	form := url.Values{"csrf": {handler.csrfToken}, "jobs": {"all"}}
	unauthenticated := httptest.NewRequest(http.MethodPost, "/runs/default/ci/rerun", strings.NewReader(form.Encode()))
	unauthenticated.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	unauthenticatedResponse := httptest.NewRecorder()
	handler.ServeHTTP(unauthenticatedResponse, unauthenticated)
	if unauthenticatedResponse.Code != http.StatusFound || unauthenticatedResponse.Header().Get("Location") != "/login?next=%2Fruns%2Fdefault%2Fci" {
		t.Fatalf("unauthenticated rerun response = %d, %q", unauthenticatedResponse.Code, unauthenticatedResponse.Header().Get("Location"))
	}

	authenticatedPageRequest := httptest.NewRequest(http.MethodGet, "/runs/default/ci", nil)
	authenticatedPageRequest.Header.Set("Authorization", "Bearer "+testConsoleToken)
	authenticatedPage := httptest.NewRecorder()
	handler.ServeHTTP(authenticatedPage, authenticatedPageRequest)
	if authenticatedPage.Code != http.StatusOK || !strings.Contains(authenticatedPage.Body.String(), "Re-run all jobs") || strings.Contains(authenticatedPage.Body.String(), "Re-run failed jobs") || !strings.Contains(authenticatedPage.Body.String(), handler.csrfToken) {
		t.Fatalf("authenticated run page = %d, %q", authenticatedPage.Code, authenticatedPage.Body.String())
	}
	failedForm := url.Values{"csrf": {handler.csrfToken}, "jobs": {"failed"}}
	failedRequest := httptest.NewRequest(http.MethodPost, "/runs/default/ci/rerun", strings.NewReader(failedForm.Encode()))
	failedRequest.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	failedRequest.Header.Set("Authorization", "Bearer "+testConsoleToken)
	failedResponse := httptest.NewRecorder()
	handler.ServeHTTP(failedResponse, failedRequest)
	if failedResponse.Code != http.StatusConflict || !strings.Contains(failedResponse.Body.String(), "did not fail because of a job") {
		t.Fatalf("failed-job rerun response = %d, %q", failedResponse.Code, failedResponse.Body.String())
	}

	invalidRequest := httptest.NewRequest(http.MethodPost, "/runs/default/ci/rerun", strings.NewReader("csrf=invalid"))
	invalidRequest.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	invalidRequest.Header.Set("Authorization", "Bearer "+testConsoleToken)
	invalidResponse := httptest.NewRecorder()
	handler.ServeHTTP(invalidResponse, invalidRequest)
	if invalidResponse.Code != http.StatusForbidden {
		t.Fatalf("invalid CSRF response = %d, want %d", invalidResponse.Code, http.StatusForbidden)
	}

	request := httptest.NewRequest(http.MethodPost, "/runs/default/ci/rerun", strings.NewReader(form.Encode()))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	request.Header.Set("Authorization", "Bearer "+testConsoleToken)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	rerunName := workflowrun.RerunName(run, 2)
	if response.Code != http.StatusSeeOther || response.Header().Get("Location") != "/runs/default/"+rerunName {
		t.Fatalf("rerun response = %d, %q", response.Code, response.Header().Get("Location"))
	}
	rerun := &actionsv1alpha1.WorkflowRun{}
	if err := handler.client.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: rerunName}, rerun); err != nil {
		t.Fatal(err)
	}
	if rerun.Spec.Rerun == nil || rerun.Spec.Rerun.Attempt != 2 || rerun.Spec.Rerun.OriginalRunRef.Name != run.Name || rerun.Spec.Rerun.OriginalRunRef.UID != run.UID || rerun.Spec.Rerun.PreviousRunRef.Name != run.Name || rerun.Spec.Rerun.PreviousRunRef.UID != run.UID || len(rerun.Spec.Rerun.JobIDs) != 0 {
		t.Fatalf("rerun spec = %#v", rerun.Spec.Rerun)
	}
	if rerun.Spec.CancelRequested || rerun.Labels[actionsv1alpha1.LabelWorkflowRunRootUID] != string(run.UID) {
		t.Fatalf("rerun = %#v", rerun)
	}

	rerun.UID = "rerun-uid"
	rerun.Status.Conditions = []metav1.Condition{{
		Type: actionsv1alpha1.WorkflowRunConditionSucceeded, Status: metav1.ConditionFalse, Reason: "JobFailed",
	}}
	if err := handler.client.Update(context.Background(), rerun); err != nil {
		t.Fatal(err)
	}
	request = httptest.NewRequest(http.MethodPost, "/runs/default/ci/rerun", strings.NewReader(form.Encode()))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	request.Header.Set("Authorization", "Bearer "+testConsoleToken)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	thirdName := workflowrun.RerunName(run, 3)
	if response.Code != http.StatusSeeOther || response.Header().Get("Location") != "/runs/default/"+thirdName {
		t.Fatalf("third attempt response = %d, %q", response.Code, response.Header().Get("Location"))
	}
	third := &actionsv1alpha1.WorkflowRun{}
	if err := handler.client.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: thirdName}, third); err != nil {
		t.Fatal(err)
	}
	if third.Spec.Rerun == nil || third.Spec.Rerun.Attempt != 3 || third.Spec.Rerun.PreviousRunRef.Name != rerun.Name || third.Spec.Rerun.PreviousRunRef.UID != rerun.UID {
		t.Fatalf("third attempt spec = %#v", third.Spec.Rerun)
	}
}

func TestConsoleRerunsFailedWorkflowJobs(t *testing.T) {
	handler := newTestHandler(t, false)
	run := &actionsv1alpha1.WorkflowRun{}
	if err := handler.client.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "ci"}, run); err != nil {
		t.Fatal(err)
	}
	run.Status.Jobs = &actionsv1alpha1.WorkflowRunJobStatus{Total: 3}
	run.Status.Conditions = []metav1.Condition{{
		Type: actionsv1alpha1.WorkflowRunConditionSucceeded, Status: metav1.ConditionFalse, Reason: "JobFailed",
	}}
	if err := handler.client.Update(context.Background(), run); err != nil {
		t.Fatal(err)
	}
	failed := &actionsv1alpha1.WorkflowJob{}
	if err := handler.client.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "build"}, failed); err != nil {
		t.Fatal(err)
	}
	failed.Status.Result = actionsv1alpha1.WorkflowJobResultFailure
	if err := handler.client.Update(context.Background(), failed); err != nil {
		t.Fatal(err)
	}
	succeeded := &actionsv1alpha1.WorkflowJob{
		ObjectMeta: metav1.ObjectMeta{Name: "lint", Namespace: run.Namespace, Labels: map[string]string{actionsv1alpha1.LabelWorkflowRunUID: string(run.UID)}},
		Spec:       actionsv1alpha1.WorkflowJobSpec{JobID: "lint"},
		Status:     actionsv1alpha1.WorkflowJobStatus{Result: actionsv1alpha1.WorkflowJobResultSuccess},
	}
	dependent := &actionsv1alpha1.WorkflowJob{
		ObjectMeta: metav1.ObjectMeta{Name: "report", Namespace: run.Namespace, Labels: map[string]string{actionsv1alpha1.LabelWorkflowRunUID: string(run.UID)}},
		Spec:       actionsv1alpha1.WorkflowJobSpec{JobID: "report", Needs: []string{"build"}},
		Status:     actionsv1alpha1.WorkflowJobStatus{Result: actionsv1alpha1.WorkflowJobResultSkipped},
	}
	if err := handler.client.Create(context.Background(), succeeded); err != nil {
		t.Fatal(err)
	}
	if err := handler.client.Create(context.Background(), dependent); err != nil {
		t.Fatal(err)
	}

	pageRequest := httptest.NewRequest(http.MethodGet, "/runs/default/ci", nil)
	pageRequest.Header.Set("Authorization", "Bearer "+testConsoleToken)
	page := httptest.NewRecorder()
	handler.ServeHTTP(page, pageRequest)
	if page.Code != http.StatusOK || !strings.Contains(page.Body.String(), "Re-run failed jobs") || !strings.Contains(page.Body.String(), "Re-run all jobs") {
		t.Fatalf("failed run page = %d, %q", page.Code, page.Body.String())
	}

	form := url.Values{"csrf": {handler.csrfToken}, "jobs": {"failed"}}
	request := httptest.NewRequest(http.MethodPost, "/runs/default/ci/rerun", strings.NewReader(form.Encode()))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	request.Header.Set("Authorization", "Bearer "+testConsoleToken)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusSeeOther {
		t.Fatalf("failed-job rerun response = %d, %q", response.Code, response.Body.String())
	}
	rerun := &actionsv1alpha1.WorkflowRun{}
	if err := handler.client.Get(context.Background(), client.ObjectKey{Namespace: run.Namespace, Name: workflowrun.RerunName(run, 2)}, rerun); err != nil {
		t.Fatal(err)
	}
	if rerun.Spec.Rerun == nil || len(rerun.Spec.Rerun.JobIDs) != 2 || rerun.Spec.Rerun.JobIDs[0] != "build" || rerun.Spec.Rerun.JobIDs[1] != "report" {
		t.Fatalf("failed-job rerun spec = %#v", rerun.Spec.Rerun)
	}
}

func TestConsoleRejectsRerunBeforeWorkflowCompletes(t *testing.T) {
	handler := newTestHandler(t, false)
	form := url.Values{"csrf": {handler.csrfToken}, "jobs": {"all"}}
	request := httptest.NewRequest(http.MethodPost, "/runs/default/ci/rerun", strings.NewReader(form.Encode()))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	request.Header.Set("Authorization", "Bearer "+testConsoleToken)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusConflict || !strings.Contains(response.Body.String(), "not complete") {
		t.Fatalf("rerun response = %d, %q", response.Code, response.Body.String())
	}
}

func TestConsoleShowsRerunWhenOriginalWorkflowRunIsGone(t *testing.T) {
	handler := newTestHandler(t, false)
	root := &actionsv1alpha1.WorkflowRun{}
	if err := handler.client.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "ci"}, root); err != nil {
		t.Fatal(err)
	}
	rerun := workflowrun.NewRerun(root, root, 2, "", nil)
	rerun.UID = "rerun-uid"
	rerun.Status.WorkflowName = "CI rerun"
	if err := handler.client.Create(context.Background(), rerun); err != nil {
		t.Fatal(err)
	}
	if err := handler.client.Delete(context.Background(), root); err != nil {
		t.Fatal(err)
	}

	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, runPath(rerun), nil))
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), "CI rerun") || strings.Contains(response.Body.String(), "Sign in to re-run") {
		t.Fatalf("rerun page = %d, %q", response.Code, response.Body.String())
	}
}

func TestConsoleCreatesWorkflowDispatch(t *testing.T) {
	handler := newTestHandler(t, false)
	handler.repositories = &testRepositoryResolver{repository: actionsv1alpha1.GitHubRepository{ID: 456, Owner: "canonical-acme", Name: "canonical-example"}}
	sourceRun := &actionsv1alpha1.WorkflowRun{}
	if err := handler.client.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "ci"}, sourceRun); err != nil {
		t.Fatal(err)
	}
	sourceRun.Spec.Source.GitHub.Event = actionsv1alpha1.GitHubEvent{
		Name: actionsv1alpha1.GitHubEventNameWorkflowDispatch,
		Inputs: map[string]string{
			"environment": "production",
		},
	}
	if err := handler.client.Update(context.Background(), sourceRun); err != nil {
		t.Fatal(err)
	}

	unauthenticated := httptest.NewRecorder()
	handler.ServeHTTP(unauthenticated, httptest.NewRequest(http.MethodGet, "/dispatch?source=default%2Fci", nil))
	if unauthenticated.Code != http.StatusFound || !strings.HasPrefix(unauthenticated.Header().Get("Location"), "/login?next=") {
		t.Fatalf("unauthenticated dispatch page = %d, %q", unauthenticated.Code, unauthenticated.Header().Get("Location"))
	}
	invalidSourceRequest := httptest.NewRequest(http.MethodGet, "/dispatch?source=invalid", nil)
	invalidSourceRequest.Header.Set("Authorization", "Bearer "+testConsoleToken)
	invalidSource := httptest.NewRecorder()
	handler.ServeHTTP(invalidSource, invalidSourceRequest)
	if invalidSource.Code != http.StatusBadRequest {
		t.Fatalf("invalid workflow dispatch source = %d, %q", invalidSource.Code, invalidSource.Body.String())
	}

	pageRequest := httptest.NewRequest(http.MethodGet, "/dispatch?source=default%2Fci", nil)
	pageRequest.Header.Set("Authorization", "Bearer "+testConsoleToken)
	page := httptest.NewRecorder()
	handler.ServeHTTP(page, pageRequest)
	pageBody := page.Body.String()
	for _, expected := range []string{`value="default/project" selected`, `value="acme"`, `value="example"`, `value="main"`, `value="` + strings.Repeat("a", 40) + `"`, `value=".open-actions/workflows/ci.yaml"`} {
		if page.Code != http.StatusOK || !strings.Contains(pageBody, expected) {
			t.Fatalf("dispatch page does not contain %q: %d, %s", expected, page.Code, pageBody)
		}
	}
	if strings.Contains(pageBody, `name="repository-id"`) {
		t.Fatalf("dispatch page asks for a repository ID: %s", pageBody)
	}
	for _, expected := range []string{
		`<code>dry-run</code><span class="input-type">boolean</span>`,
		`Dry run without applying changes`,
		`<option value="false" selected>false</option>`,
		`<code>environment</code><span class="input-type">choice</span><span aria-label="required">Required</span>`,
		`<option value="staging">staging</option>`,
		`<option value="production" selected>production</option>`,
		`value="notes" data-input-field disabled`,
	} {
		if !strings.Contains(pageBody, expected) {
			t.Fatalf("dispatch page does not contain declared input %q: %s", expected, pageBody)
		}
	}
	if strings.Contains(pageBody, `id="add-input"`) {
		t.Fatalf("dispatch page permits undeclared inputs: %s", pageBody)
	}
	dryRunStart := strings.Index(pageBody, `<code>dry-run</code>`)
	environmentStart := strings.Index(pageBody, `<code>environment</code>`)
	if dryRunStart == -1 || environmentStart <= dryRunStart {
		t.Fatalf("dispatch page input order is invalid: %s", pageBody)
	}
	dryRunInput := pageBody[dryRunStart:environmentStart]
	if !strings.Contains(dryRunInput, `data-input-toggle checked`) || !strings.Contains(dryRunInput, `value="dry-run" data-input-field>`) {
		t.Fatalf("defaulted optional input is not included: %s", dryRunInput)
	}

	requestID := "0123456789abcdefabcd"
	form := url.Values{
		"csrf":             {handler.csrfToken},
		"request-id":       {requestID},
		"project":          {"default/project"},
		"repository-owner": {"acme"},
		"repository-name":  {"example"},
		"ref-type":         {"branch"},
		"ref-name":         {"main"},
		"revision":         {strings.Repeat("b", 40)},
		"workflow-path":    {".open-actions/workflows/deploy.yaml"},
		"input-name":       {"environment", "retries"},
		"input-value":      {"staging", "2"},
	}
	dispatch := func(values url.Values) *httptest.ResponseRecorder {
		request := httptest.NewRequest(http.MethodPost, "/dispatch", strings.NewReader(values.Encode()))
		request.Header.Set("Authorization", "Bearer "+testConsoleToken)
		request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		return response
	}
	response := dispatch(form)
	wantLocation := "/runs/default/dispatch-" + requestID
	if response.Code != http.StatusSeeOther || response.Header().Get("Location") != wantLocation {
		t.Fatalf("workflow dispatch response = %d, %q, want %q", response.Code, response.Header().Get("Location"), wantLocation)
	}
	created := &actionsv1alpha1.WorkflowRun{}
	if err := handler.client.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "dispatch-" + requestID}, created); err != nil {
		t.Fatal(err)
	}
	github := created.Spec.Source.GitHub
	if created.Spec.ProjectRef.Name != "project" || created.Spec.WorkflowPath != ".open-actions/workflows/deploy.yaml" || created.Spec.TTLSecondsAfterFinished == nil || *created.Spec.TTLSecondsAfterFinished != 604800 || github == nil ||
		github.Repository.ID != 456 || github.Repository.Owner != "canonical-acme" || github.Repository.Name != "canonical-example" ||
		github.Event.Name != actionsv1alpha1.GitHubEventNameWorkflowDispatch || github.Event.DeliveryID != "" || github.Event.Inputs["environment"] != "staging" || github.Event.Inputs["retries"] != "2" ||
		github.Revision.Ref != "refs/heads/main" || github.Revision.SHA != strings.Repeat("b", 40) {
		t.Fatalf("created WorkflowRun = %#v", created)
	}
	changedTTL := int32(1)
	created.Spec.CancelRequested = true
	created.Spec.TTLSecondsAfterFinished = &changedTTL
	if err := handler.client.Update(context.Background(), created); err != nil {
		t.Fatal(err)
	}
	if retryResponse := dispatch(form); retryResponse.Code != http.StatusSeeOther || retryResponse.Header().Get("Location") != wantLocation {
		t.Fatalf("workflow dispatch retry = %d, %q", retryResponse.Code, retryResponse.Header().Get("Location"))
	}
	form.Set("revision", strings.Repeat("c", 40))
	if conflict := dispatch(form); conflict.Code != http.StatusConflict {
		t.Fatalf("conflicting workflow dispatch = %d, %q", conflict.Code, conflict.Body.String())
	}
	form.Set("request-id", "abcdef0123456789abcd")
	form.Set("ref-type", "tag")
	form.Set("ref-name", "v1.2.3")
	if tagResponse := dispatch(form); tagResponse.Code != http.StatusSeeOther {
		t.Fatalf("tag workflow dispatch = %d, %q", tagResponse.Code, tagResponse.Body.String())
	}
	tagRun := &actionsv1alpha1.WorkflowRun{}
	if err := handler.client.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "dispatch-abcdef0123456789abcd"}, tagRun); err != nil {
		t.Fatal(err)
	}
	if tagRun.Spec.Source.GitHub.Revision.Ref != "refs/tags/v1.2.3" {
		t.Fatalf("tag workflow dispatch ref = %q", tagRun.Spec.Source.GitHub.Revision.Ref)
	}
}

func TestWorkflowDispatchValidation(t *testing.T) {
	for _, test := range []struct {
		name  string
		value string
		valid bool
	}{
		{name: "branch", value: "refs/heads/feature/dispatch", valid: true},
		{name: "tag", value: "refs/tags/v1.2.3", valid: true},
		{name: "empty branch", value: "refs/heads/"},
		{name: "space", value: "refs/heads/release candidate"},
		{name: "empty component", value: "refs/heads/feature//dispatch"},
		{name: "parent component", value: "refs/heads/feature/../dispatch"},
		{name: "lock suffix", value: "refs/tags/release.lock"},
		{name: "reflog syntax", value: "refs/heads/main@{1}"},
	} {
		t.Run("ref "+test.name, func(t *testing.T) {
			if got := validGitRef(test.value); got != test.valid {
				t.Fatalf("validGitRef(%q) = %t, want %t", test.value, got, test.valid)
			}
		})
	}
	for _, test := range []struct {
		name  string
		value string
		valid bool
	}{
		{name: "yaml", value: ".open-actions/workflows/deploy.yaml", valid: true},
		{name: "yml", value: "workflows/release.yml", valid: true},
		{name: "absolute", value: "/workflows/deploy.yaml"},
		{name: "current directory", value: "./workflows/deploy.yaml"},
		{name: "parent directory", value: "../workflows/deploy.yaml"},
		{name: "embedded parent", value: "workflows/../deploy.yaml"},
		{name: "empty component", value: "workflows//deploy.yaml"},
		{name: "extension", value: "workflows/deploy.json"},
	} {
		t.Run("path "+test.name, func(t *testing.T) {
			if got := validWorkflowPath(test.value); got != test.valid {
				t.Fatalf("validWorkflowPath(%q) = %t, want %t", test.value, got, test.valid)
			}
		})
	}
	for _, test := range []struct {
		name  string
		value string
		valid bool
	}{
		{name: "full lowercase", value: strings.Repeat("a", 40), valid: true},
		{name: "uppercase", value: strings.Repeat("A", 40)},
		{name: "short", value: strings.Repeat("a", 39)},
		{name: "non hexadecimal", value: strings.Repeat("g", 40)},
	} {
		t.Run("SHA "+test.name, func(t *testing.T) {
			if got := validGitSHA(test.value); got != test.valid {
				t.Fatalf("validGitSHA(%q) = %t, want %t", test.value, got, test.valid)
			}
		})
	}
}

func TestConsoleDoesNotCreateWorkflowDispatchWhenRepositoryResolutionFails(t *testing.T) {
	handler := newTestHandler(t, false)
	handler.repositories = &testRepositoryResolver{err: fmt.Errorf("resolve repository: %w", &githubclient.APIError{StatusCode: http.StatusNotFound})}
	form := url.Values{
		"csrf":             {handler.csrfToken},
		"request-id":       {"0123456789abcdefabcd"},
		"project":          {"default/project"},
		"repository-owner": {"acme"},
		"repository-name":  {"missing"},
		"ref-type":         {"branch"},
		"ref-name":         {"main"},
		"revision":         {strings.Repeat("b", 40)},
		"workflow-path":    {".open-actions/workflows/deploy.yaml"},
	}
	request := httptest.NewRequest(http.MethodPost, "/dispatch", strings.NewReader(form.Encode()))
	request.Header.Set("Authorization", "Bearer "+testConsoleToken)
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest || !strings.Contains(response.Body.String(), `GitHub repository "acme/missing" is not accessible to Project "project"`) {
		t.Fatalf("workflow dispatch response = %d, %q", response.Code, response.Body.String())
	}
	run := &actionsv1alpha1.WorkflowRun{}
	err := handler.client.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "dispatch-0123456789abcdefabcd"}, run)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("WorkflowRun exists after repository resolution failure: %#v, %v", run, err)
	}
}

func TestWorkflowDispatchInputValidation(t *testing.T) {
	inputs, err := dispatchInputs([]string{"environment", "retries"}, []string{"staging", "2"})
	if err != nil || inputs["environment"] != "staging" || inputs["retries"] != "2" {
		t.Fatalf("dispatchInputs() = %#v, %v", inputs, err)
	}
	manyNames := make([]string, maxDispatchInputs+1)
	manyValues := make([]string, len(manyNames))
	for index := range manyNames {
		manyNames[index] = fmt.Sprintf("input_%d", index)
	}
	for _, test := range []struct {
		name   string
		names  []string
		values []string
		want   string
	}{
		{name: "mismatched fields", names: []string{"one"}, want: "do not match"},
		{name: "too many", names: manyNames, values: manyValues, want: "at most 25"},
		{name: "invalid name", names: []string{"invalid.name"}, values: []string{"value"}, want: "invalid workflow input name"},
		{name: "case insensitive duplicate", names: []string{"Target", "target"}, values: []string{"one", "two"}, want: "duplicated"},
		{name: "invalid UTF-8", names: []string{"target"}, values: []string{string([]byte{0xff})}, want: "valid UTF-8"},
		{name: "value too long", names: []string{"target"}, values: []string{strings.Repeat("x", maxDispatchPayload+1)}, want: "exceeds"},
		{name: "payload too long", names: []string{"first", "second"}, values: []string{strings.Repeat("x", maxDispatchPayload/2), strings.Repeat("y", maxDispatchPayload/2)}, want: "names and values exceed"},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := dispatchInputs(test.names, test.values)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("dispatchInputs() error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestConsoleRequiresAuthenticationForWorkflowDispatch(t *testing.T) {
	handler := newTestHandler(t, false)
	form := url.Values{"csrf": {handler.csrfToken}, "request-id": {"0123456789abcdefabcd"}}
	request := httptest.NewRequest(http.MethodPost, "/dispatch", strings.NewReader(form.Encode()))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusFound || !strings.HasPrefix(response.Header().Get("Location"), "/login?next=") {
		t.Fatalf("unauthenticated workflow dispatch = %d, %q", response.Code, response.Header().Get("Location"))
	}
	runs := &actionsv1alpha1.WorkflowRunList{}
	if err := handler.client.List(context.Background(), runs); err != nil {
		t.Fatal(err)
	}
	if len(runs.Items) != 2 {
		t.Fatalf("WorkflowRuns after unauthenticated dispatch = %d, want 2", len(runs.Items))
	}
}

func TestConsoleManagesReferencedProjectSecretWithoutDisplayingValues(t *testing.T) {
	handler := newTestHandler(t, false)

	projectsRequest := httptest.NewRequest(http.MethodGet, "/projects", nil)
	projectsResponse := httptest.NewRecorder()
	handler.ServeHTTP(projectsResponse, projectsRequest)
	if projectsResponse.Code != http.StatusOK || !strings.Contains(projectsResponse.Body.String(), `href="/projects/default/project"`) {
		t.Fatalf("projects page = %d, %q", projectsResponse.Code, projectsResponse.Body.String())
	}

	readRequest := httptest.NewRequest(http.MethodGet, "/projects/default/project", nil)
	readResponse := httptest.NewRecorder()
	handler.ServeHTTP(readResponse, readRequest)
	readBody := readResponse.Body.String()
	if readResponse.Code != http.StatusOK || !strings.Contains(readBody, "DEPLOY_TOKEN") || !strings.Contains(readBody, "Sign in as an administrator") {
		t.Fatalf("read-only Project page = %d, %q", readResponse.Code, readBody)
	}
	for _, privateContent := range []string{"existing-secret-value", "Add or replace", ">Delete</button>", handler.csrfToken} {
		if strings.Contains(readBody, privateContent) {
			t.Fatalf("read-only Project page exposed %q: %s", privateContent, readBody)
		}
	}

	detailsRequest := httptest.NewRequest(http.MethodGet, "/projects/default/project", nil)
	detailsRequest.Header.Set("Authorization", "Bearer "+testConsoleToken)
	detailsResponse := httptest.NewRecorder()
	handler.ServeHTTP(detailsResponse, detailsRequest)
	detailsBody := detailsResponse.Body.String()
	if detailsResponse.Code != http.StatusOK || !strings.Contains(detailsBody, "DEPLOY_TOKEN") || !strings.Contains(detailsBody, "Add or replace") {
		t.Fatalf("Project page = %d, %q", detailsResponse.Code, detailsBody)
	}
	if strings.Contains(detailsBody, "existing-secret-value") {
		t.Fatalf("Project page exposed a secret value: %s", detailsBody)
	}

	invalid := url.Values{"csrf": {"invalid"}, "action": {"set"}, "name": {"release_token"}, "value": {"replacement-value"}}
	invalidRequest := httptest.NewRequest(http.MethodPost, "/projects/default/project/secrets", strings.NewReader(invalid.Encode()))
	invalidRequest.Header.Set("Authorization", "Bearer "+testConsoleToken)
	invalidRequest.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	invalidResponse := httptest.NewRecorder()
	handler.ServeHTTP(invalidResponse, invalidRequest)
	if invalidResponse.Code != http.StatusForbidden {
		t.Fatalf("invalid CSRF response = %d, %q", invalidResponse.Code, invalidResponse.Body.String())
	}

	set := url.Values{"csrf": {handler.csrfToken}, "action": {"set"}, "name": {"release_token"}, "value": {"replacement-value"}}
	setRequest := httptest.NewRequest(http.MethodPost, "/projects/default/project/secrets", strings.NewReader(set.Encode()))
	setRequest.Header.Set("Authorization", "Bearer "+testConsoleToken)
	setRequest.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	setResponse := httptest.NewRecorder()
	handler.ServeHTTP(setResponse, setRequest)
	if setResponse.Code != http.StatusSeeOther || setResponse.Header().Get("Location") != "/projects/default/project" {
		t.Fatalf("set response = %d, %q", setResponse.Code, setResponse.Header().Get("Location"))
	}
	secret := &corev1.Secret{}
	if err := handler.client.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "project-secrets"}, secret); err != nil {
		t.Fatal(err)
	}
	if string(secret.Data["RELEASE_TOKEN"]) != "replacement-value" {
		t.Fatalf("updated Secret data = %#v", secret.Data)
	}

	deleteForm := url.Values{"csrf": {handler.csrfToken}, "action": {"delete"}, "name": {"DEPLOY_TOKEN"}}
	deleteRequest := httptest.NewRequest(http.MethodPost, "/projects/default/project/secrets", strings.NewReader(deleteForm.Encode()))
	deleteRequest.Header.Set("Authorization", "Bearer "+testConsoleToken)
	deleteRequest.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	deleteResponse := httptest.NewRecorder()
	handler.ServeHTTP(deleteResponse, deleteRequest)
	if deleteResponse.Code != http.StatusSeeOther {
		t.Fatalf("delete response = %d, %q", deleteResponse.Code, deleteResponse.Body.String())
	}
	if err := handler.client.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "project-secrets"}, secret); err != nil {
		t.Fatal(err)
	}
	if _, found := secret.Data["DEPLOY_TOKEN"]; found {
		t.Fatalf("deleted secret remains: %#v", secret.Data)
	}

	if err := handler.client.Delete(context.Background(), secret); err != nil {
		t.Fatal(err)
	}
	create := url.Values{"csrf": {handler.csrfToken}, "action": {"set"}, "name": {"FIRST_TOKEN"}, "value": {"created-value"}}
	createRequest := httptest.NewRequest(http.MethodPost, "/projects/default/project/secrets", strings.NewReader(create.Encode()))
	createRequest.Header.Set("Authorization", "Bearer "+testConsoleToken)
	createRequest.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	createResponse := httptest.NewRecorder()
	handler.ServeHTTP(createResponse, createRequest)
	if createResponse.Code != http.StatusSeeOther {
		t.Fatalf("create response = %d, %q", createResponse.Code, createResponse.Body.String())
	}
	if err := handler.client.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "project-secrets"}, secret); err != nil {
		t.Fatal(err)
	}
	if string(secret.Data["FIRST_TOKEN"]) != "created-value" {
		t.Fatalf("created Secret data = %#v", secret.Data)
	}

	otherNamespaceRequest := httptest.NewRequest(http.MethodPost, "/projects/other/project/secrets", strings.NewReader(set.Encode()))
	otherNamespaceRequest.Header.Set("Authorization", "Bearer "+testConsoleToken)
	otherNamespaceRequest.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	otherNamespaceResponse := httptest.NewRecorder()
	handler.ServeHTTP(otherNamespaceResponse, otherNamespaceRequest)
	if otherNamespaceResponse.Code != http.StatusForbidden {
		t.Fatalf("other namespace response = %d, %q", otherNamespaceResponse.Code, otherNamespaceResponse.Body.String())
	}
}

func TestConsoleAcceptsEncodingHeavySecretAtValueLimit(t *testing.T) {
	handler := newTestHandler(t, false)
	value := strings.Repeat("%", projectvalue.MaxValueBytes)
	form := url.Values{"csrf": {handler.csrfToken}, "action": {"set"}, "name": {"ENCODED_TOKEN"}, "value": {value}}
	encoded := form.Encode()
	if len(encoded) <= projectvalue.MaxValueBytes+(8<<10) {
		t.Fatalf("encoded request does not exercise form expansion: %d bytes", len(encoded))
	}
	request := httptest.NewRequest(http.MethodPost, "/projects/default/project/secrets", strings.NewReader(encoded))
	request.Header.Set("Authorization", "Bearer "+testConsoleToken)
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusSeeOther {
		t.Fatalf("set response = %d, %q", response.Code, response.Body.String())
	}
	secret := &corev1.Secret{}
	if err := handler.client.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "project-secrets"}, secret); err != nil {
		t.Fatal(err)
	}
	if string(secret.Data["ENCODED_TOKEN"]) != value {
		t.Fatalf("stored value length = %d, want %d", len(secret.Data["ENCODED_TOKEN"]), len(value))
	}
}

func TestConsoleReportsProjectSecretCountLimit(t *testing.T) {
	handler := newTestHandler(t, false)
	secret := &corev1.Secret{}
	key := client.ObjectKey{Namespace: "default", Name: "project-secrets"}
	if err := handler.client.Get(context.Background(), key, secret); err != nil {
		t.Fatal(err)
	}
	for index := len(secret.Data); index < projectvalue.MaxSecretCount; index++ {
		secret.Data[fmt.Sprintf("TOKEN_%03d", index)] = []byte("value")
	}
	if err := handler.client.Update(context.Background(), secret); err != nil {
		t.Fatal(err)
	}

	form := url.Values{"csrf": {handler.csrfToken}, "action": {"set"}, "name": {"NEW_TOKEN"}, "value": {"value"}}
	request := httptest.NewRequest(http.MethodPost, "/projects/default/project/secrets", strings.NewReader(form.Encode()))
	request.Header.Set("Authorization", "Bearer "+testConsoleToken)
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusConflict || !strings.Contains(response.Body.String(), "already contains 100 values") {
		t.Fatalf("limit response = %d, %q", response.Code, response.Body.String())
	}
	if err := handler.client.Get(context.Background(), key, secret); err != nil {
		t.Fatal(err)
	}
	if _, found := secret.Data["NEW_TOKEN"]; found {
		t.Fatalf("Secret contains rejected value: %#v", secret.Data)
	}
}

func TestConsoleRedirectsAuthenticatedLoginToProjectsPage(t *testing.T) {
	handler := newTestHandler(t, false)
	request := httptest.NewRequest(http.MethodGet, "/login", nil)
	request.Header.Set("Authorization", "Bearer "+testConsoleToken)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusFound || response.Header().Get("Location") != "/projects" {
		t.Fatalf("response = %d, %q", response.Code, response.Header().Get("Location"))
	}
}

func TestConsoleStructuresGitHubActionsLogs(t *testing.T) {
	handler := newTestHandler(t, false)
	source := handler.logs.(*testLogSource)
	source.logs = strings.Join([]string{
		`{"time":"2026-08-10T12:34:56Z","level":"INFO","msg":"starting workflow step","open_actions_runner":true,"job":"build","step":1,"name":"Build"}`,
		`{"time":"2026-08-10T12:34:56Z","level":"INFO","msg":"workflow step input","open_actions_runner":true,"name":"target","value":"sensitive-input-value"}`,
		"::debug::resolved%20input",
		"::warning file=main.go,line=7,title=Compiler::check failed",
		"[command]/usr/bin/go test ./...",
		"##[add-matcher]/tmp/matcher.json",
		"::set-output name=artifact::private-value",
		"ordinary output",
		"\x1b[38;5;243mcolored output\x1b[0m",
		`{"time":"2026-08-10T12:34:57Z","level":"INFO","msg":"workflow step output","open_actions_runner":true,"name":"artifact","value":"sensitive-output-value"}`,
		`{"time":"2026-08-10T12:34:57Z","level":"INFO","msg":"completed workflow step","open_actions_runner":true,"job":"build","step":1,"name":"Build"}`,
		`{"time":"2026-08-10T12:34:58Z","level":"INFO","msg":"skipping workflow step","open_actions_runner":true,"job":"build","step":2,"name":"Deploy"}`,
		`{"time":"2026-08-10T12:34:58Z","level":"INFO","msg":"starting post action","open_actions_runner":true,"action":"actions/example@v1"}`,
		`{"time":"2026-08-10T12:34:59Z","level":"INFO","msg":"completed post action","open_actions_runner":true,"action":"actions/example@v1"}`,
	}, "\n") + "\n"

	request := httptest.NewRequest(http.MethodGet, "/runs/default/ci/jobs/build/stream", nil)
	request.Header.Set("Authorization", "Bearer "+testConsoleToken)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	body := response.Body.String()
	for _, expected := range []string{
		`"kind":"group","text":"1. Build"`,
		`"kind":"input","text":"target"`,
		`"kind":"debug","text":"resolved%20input"`,
		`"kind":"warning","text":"check failed"`,
		`"kind":"command","text":"/usr/bin/go test ./..."`,
		`"kind":"command","text":"Set output artifact"`,
		`"kind":"output","text":"ordinary output"`,
		`"kind":"output","text":"colored output","parts":[{"text":"colored output","foreground":"#767676"}]`,
		`"kind":"step-output","text":"artifact"`,
		`"scope":"workflow","conclusion":"success"`,
		`"kind":"group","text":"2. Deploy","time":"2026-08-10T12:34:58Z","scope":"workflow","conclusion":"skipped"`,
		`"kind":"group","text":"Post actions/example@v1"`,
	} {
		if !strings.Contains(body, expected) {
			t.Fatalf("stream does not contain %q: %s", expected, body)
		}
	}
	if strings.Contains(body, "private-value") || strings.Contains(body, "sensitive-input-value") || strings.Contains(body, "sensitive-output-value") {
		t.Fatalf("stream exposed workflow command value: %s", body)
	}
	if strings.Contains(body, "add-matcher") {
		t.Fatalf("stream exposed matcher command: %s", body)
	}
}

func TestConsoleRequiresAuthenticationForProjectSecretUpdates(t *testing.T) {
	handler := newTestHandler(t, false)
	form := url.Values{"csrf": {handler.csrfToken}, "action": {"set"}, "name": {"DEPLOY_TOKEN"}, "value": {"replacement-value"}}
	request := httptest.NewRequest(http.MethodPost, "/projects/default/project/secrets", strings.NewReader(form.Encode()))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusFound || response.Header().Get("Location") != "/login?next=%2Fprojects%2Fdefault%2Fproject" {
		t.Fatalf("response = %d, %q", response.Code, response.Header().Get("Location"))
	}
	secret := &corev1.Secret{}
	if err := handler.client.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "project-secrets"}, secret); err != nil {
		t.Fatal(err)
	}
	if string(secret.Data["DEPLOY_TOKEN"]) != "existing-secret-value" {
		t.Fatalf("unauthenticated request updated Secret: %#v", secret.Data)
	}
}

func TestNewRequiresToken(t *testing.T) {
	scheme := runtime.NewScheme()
	handler, err := New(Config{
		Client: fake.NewClientBuilder().WithScheme(scheme).Build(),
		Logs:   &testLogSource{pod: &corev1.Pod{}}, Repositories: &testRepositoryResolver{},
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err == nil || handler != nil || !strings.Contains(err.Error(), "token is required") {
		t.Fatalf("New() = %#v, %v", handler, err)
	}
}

func TestNewRejectsNegativeWorkflowRunTTL(t *testing.T) {
	scheme := runtime.NewScheme()
	negative := int32(-1)
	handler, err := New(Config{
		Client: fake.NewClientBuilder().WithScheme(scheme).Build(),
		Logs:   &testLogSource{pod: &corev1.Pod{}}, Repositories: &testRepositoryResolver{}, Token: testConsoleToken,
		WorkflowRunTTLSecondsAfterFinished: &negative,
		Logger:                             slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err == nil || handler != nil || !strings.Contains(err.Error(), "TTL must not be negative") {
		t.Fatalf("New() = %#v, %v", handler, err)
	}
}

func TestSafeNextRejectsExternalURLs(t *testing.T) {
	for _, value := range []string{"https://example.com/projects/default/project", "//example.com/projects/default/project", "/", "/login"} {
		if next := safeNext(value); next != "" {
			t.Fatalf("safeNext(%q) = %q", value, next)
		}
	}
	for _, value := range []string{"/projects/default/project?tab=secrets", "/runs/default/ci", "/dispatch?source=default%2Fci"} {
		if next := safeNext(value); next != value {
			t.Fatalf("safeNext(%q) = %q", value, next)
		}
	}
}

func newTestHandler(t *testing.T, secureCookie bool) *Handler {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := actionsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	run := &actionsv1alpha1.WorkflowRun{
		TypeMeta:   metav1.TypeMeta{APIVersion: actionsv1alpha1.GroupVersion.String(), Kind: "WorkflowRun"},
		ObjectMeta: metav1.ObjectMeta{Name: "ci", Namespace: "default", UID: "run-uid", CreationTimestamp: metav1.NewTime(time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC))},
		Spec: actionsv1alpha1.WorkflowRunSpec{
			ProjectRef: corev1.LocalObjectReference{Name: "project"}, WorkflowPath: ".open-actions/workflows/ci.yaml",
			Source: actionsv1alpha1.WorkflowRunSource{Type: actionsv1alpha1.SourceTypeGitHub, GitHub: &actionsv1alpha1.GitHubWorkflowRunSource{
				Actor:      "octocat",
				Repository: actionsv1alpha1.GitHubRepository{ID: 123, Owner: "acme", Name: "example"},
				Event:      actionsv1alpha1.GitHubEvent{Name: actionsv1alpha1.GitHubEventNamePush, DeliveryID: "delivery-id"},
				Revision:   actionsv1alpha1.GitRevision{SHA: strings.Repeat("a", 40), Ref: "refs/heads/main"},
			}},
		},
		Status: actionsv1alpha1.WorkflowRunStatus{
			WorkflowName: "CI",
			Identity: &actionsv1alpha1.WorkflowRunIdentityStatus{
				ID: 1, Number: 1, Attempt: 1, URL: "https://actions.example/runs/default/ci",
			},
		},
	}
	job := &actionsv1alpha1.WorkflowJob{
		TypeMeta:   metav1.TypeMeta{APIVersion: actionsv1alpha1.GroupVersion.String(), Kind: "WorkflowJob"},
		ObjectMeta: metav1.ObjectMeta{Name: "build", Namespace: "default", UID: "job-uid", Labels: map[string]string{actionsv1alpha1.LabelWorkflowRunUID: string(run.UID)}},
		Spec:       actionsv1alpha1.WorkflowJobSpec{WorkflowRunRef: corev1.LocalObjectReference{Name: run.Name}, JobID: "build"},
	}
	if err := controllerutil.SetControllerReference(run, job, scheme); err != nil {
		t.Fatal(err)
	}
	workflowFileName := "ci-workflow-file"
	immutable := true
	workflowFile := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: workflowFileName, Namespace: run.Namespace},
		Immutable:  &immutable,
		Data: map[string]string{
			workflowsnapshot.DataKey: `name: CI
on:
  push:
  workflow_dispatch:
    inputs:
      environment:
        description: Deployment environment
        required: true
        type: choice
        default: staging
        options: [staging, production]
      dry-run:
        description: Dry run without applying changes
        type: boolean
        default: false
      notes:
        description: Optional release notes
        type: string
# <script>alert('unsafe')</script>
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: make test
`,
		},
	}
	if err := controllerutil.SetControllerReference(run, workflowFile, scheme); err != nil {
		t.Fatal(err)
	}
	newerRun := run.DeepCopy()
	newerRun.Name = "lint"
	newerRun.UID = "lint-run-uid"
	newerRun.CreationTimestamp = metav1.NewTime(time.Date(2026, 8, 10, 13, 0, 0, 0, time.UTC))
	newerRun.Status.WorkflowName = "Lint"
	run.Annotations = map[string]string{actionsv1alpha1.AnnotationWorkflowFile: workflowFileName}
	project := &actionsv1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: "project", Namespace: "default", Generation: 1},
		Spec: actionsv1alpha1.ProjectSpec{
			Source:  actionsv1alpha1.ProjectSource{Type: actionsv1alpha1.SourceTypeGitHub, GitHub: &actionsv1alpha1.GitHubAppConfiguration{AppID: 1, InstallationID: 2}},
			Secrets: &actionsv1alpha1.ProjectSecretSource{SecretRef: corev1.LocalObjectReference{Name: "project-secrets"}},
		},
		Status: actionsv1alpha1.ProjectStatus{ObservedGeneration: 1, Conditions: []metav1.Condition{{
			Type: actionsv1alpha1.ProjectConditionConfigured, Status: metav1.ConditionTrue, ObservedGeneration: 1, Reason: "ConfigurationValid", Message: "Configuration is valid",
		}}},
	}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "project-secrets", Namespace: "default"}, Data: map[string][]byte{"DEPLOY_TOKEN": []byte("existing-secret-value")}}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "build", Namespace: "default", Labels: map[string]string{actionsv1alpha1.LabelWorkflowJobUID: string(job.UID)}}}
	clusterClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(run, newerRun, job, workflowFile, project, secret).Build()
	workflowRunTTLSecondsAfterFinished := int32(604800)
	handler, err := New(Config{
		Client: clusterClient, Logs: &testLogSource{pod: pod, logs: "build output\n"}, Repositories: &testRepositoryResolver{}, Token: testConsoleToken,
		SecretManagementNamespace: "default", WorkflowRunTTLSecondsAfterFinished: &workflowRunTTLSecondsAfterFinished,
		SecureCookie: secureCookie, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	return handler
}

func responseCookie(t *testing.T, response *http.Response, name string) *http.Cookie {
	t.Helper()
	for _, cookie := range response.Cookies() {
		if cookie.Name == name {
			return cookie
		}
	}
	t.Fatalf("response has no %s cookie", name)
	return nil
}
