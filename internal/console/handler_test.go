package console

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	actionsv1alpha1 "github.com/kelos-dev/open-actions/api/v1alpha1"
	"github.com/kelos-dev/open-actions/internal/projectvalue"
	corev1 "k8s.io/api/core/v1"
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

func (s *testLogSource) ListPods(context.Context, string, string) (*corev1.PodList, error) {
	return &corev1.PodList{Items: []corev1.Pod{*s.pod.DeepCopy()}}, nil
}

func (s *testLogSource) Stream(context.Context, string, string) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader(s.logs)), nil
}

func TestConsoleAuthenticatesWithStaticTokenAndStreamsLogs(t *testing.T) {
	handler := newTestHandler(t, true)
	runURL := "/runs/default/ci"

	unauthenticated := httptest.NewRecorder()
	handler.ServeHTTP(unauthenticated, httptest.NewRequest(http.MethodGet, runURL, nil))
	if unauthenticated.Code != http.StatusFound || unauthenticated.Header().Get("Location") != "/login?next=%2Fruns%2Fdefault%2Fci" {
		t.Fatalf("unauthenticated response = %d, %q", unauthenticated.Code, unauthenticated.Header().Get("Location"))
	}

	loginPage := httptest.NewRecorder()
	handler.ServeHTTP(loginPage, httptest.NewRequest(http.MethodGet, unauthenticated.Header().Get("Location"), nil))
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

	runRequest := httptest.NewRequest(http.MethodGet, runURL, nil)
	runRequest.AddCookie(sessionCookie)
	runResponse := httptest.NewRecorder()
	handler.ServeHTTP(runResponse, runRequest)
	if runResponse.Code != http.StatusOK || !strings.Contains(runResponse.Body.String(), "CI") || !strings.Contains(runResponse.Body.String(), "build") || !strings.Contains(runResponse.Body.String(), "Workflow run Queued") {
		t.Fatalf("run page = %d, %q", runResponse.Code, runResponse.Body.String())
	}

	logRequest := httptest.NewRequest(http.MethodGet, runURL+"/jobs/build", nil)
	logRequest.AddCookie(sessionCookie)
	logResponse := httptest.NewRecorder()
	handler.ServeHTTP(logResponse, logRequest)
	if logResponse.Code != http.StatusOK || !strings.Contains(logResponse.Body.String(), "Show debug") || !strings.Contains(logResponse.Body.String(), "Show timestamps") {
		t.Fatalf("log page = %d, %q", logResponse.Code, logResponse.Body.String())
	}

	streamRequest := httptest.NewRequest(http.MethodGet, runURL+"/jobs/build/stream", nil)
	streamRequest.AddCookie(sessionCookie)
	streamResponse := httptest.NewRecorder()
	handler.ServeHTTP(streamResponse, streamRequest)
	if streamResponse.Code != http.StatusOK || !strings.Contains(streamResponse.Body.String(), "id: 1\nevent: log") || !strings.Contains(streamResponse.Body.String(), "build output") {
		t.Fatalf("log stream = %d, %q", streamResponse.Code, streamResponse.Body.String())
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

func TestConsoleMainPageListsWorkflowRunsNewestFirst(t *testing.T) {
	handler := newTestHandler(t, false)

	unauthenticated := httptest.NewRecorder()
	handler.ServeHTTP(unauthenticated, httptest.NewRequest(http.MethodGet, "/", nil))
	if unauthenticated.Code != http.StatusFound || unauthenticated.Header().Get("Location") != "/login?next=%2F" {
		t.Fatalf("unauthenticated response = %d, %q", unauthenticated.Code, unauthenticated.Header().Get("Location"))
	}

	request := httptest.NewRequest(http.MethodGet, "/", nil)
	request.Header.Set("Authorization", "Bearer "+testConsoleToken)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
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

func TestConsoleManagesReferencedProjectSecretWithoutDisplayingValues(t *testing.T) {
	handler := newTestHandler(t, false)

	projectsRequest := httptest.NewRequest(http.MethodGet, "/projects", nil)
	projectsRequest.Header.Set("Authorization", "Bearer "+testConsoleToken)
	projectsResponse := httptest.NewRecorder()
	handler.ServeHTTP(projectsResponse, projectsRequest)
	if projectsResponse.Code != http.StatusOK || !strings.Contains(projectsResponse.Body.String(), `href="/projects/default/project"`) {
		t.Fatalf("projects page = %d, %q", projectsResponse.Code, projectsResponse.Body.String())
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

func TestConsoleRedirectsAuthenticatedLoginToMainPage(t *testing.T) {
	handler := newTestHandler(t, false)
	request := httptest.NewRequest(http.MethodGet, "/login", nil)
	request.Header.Set("Authorization", "Bearer "+testConsoleToken)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusFound || response.Header().Get("Location") != "/" {
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

func TestConsoleAcceptsBearerToken(t *testing.T) {
	handler := newTestHandler(t, false)
	request := httptest.NewRequest(http.MethodGet, "/runs/default/ci", nil)
	request.Header.Set("Authorization", "Bearer "+testConsoleToken)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("bearer response = %d, %q", response.Code, response.Body.String())
	}
}

func TestConsoleRejectsInvalidSessionCookie(t *testing.T) {
	handler := newTestHandler(t, false)
	request := httptest.NewRequest(http.MethodGet, "/runs/default/ci", nil)
	request.AddCookie(&http.Cookie{Name: sessionCookieName, Value: "invalid"})
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusFound || !strings.HasPrefix(response.Header().Get("Location"), "/login?next=") {
		t.Fatalf("response = %d, %q", response.Code, response.Header().Get("Location"))
	}
}

func TestNewRequiresToken(t *testing.T) {
	scheme := runtime.NewScheme()
	handler, err := New(Config{
		Client: fake.NewClientBuilder().WithScheme(scheme).Build(),
		Logs:   &testLogSource{pod: &corev1.Pod{}},
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err == nil || handler != nil || !strings.Contains(err.Error(), "token is required") {
		t.Fatalf("New() = %#v, %v", handler, err)
	}
}

func TestSafeNextRejectsExternalURLs(t *testing.T) {
	for _, value := range []string{"https://example.com/runs/default/ci", "//example.com/runs/default/ci", "/login"} {
		if next := safeNext(value); next != "" {
			t.Fatalf("safeNext(%q) = %q", value, next)
		}
	}
	if next := safeNext("/runs/default/ci?tab=logs"); next != "/runs/default/ci?tab=logs" {
		t.Fatalf("safeNext() = %q", next)
	}
	if next := safeNext("/"); next != "/" {
		t.Fatalf("safeNext() = %q", next)
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
				Repository: actionsv1alpha1.GitHubRepository{ID: 123, Owner: "acme", Name: "example"},
				Event:      actionsv1alpha1.GitHubEvent{Name: actionsv1alpha1.GitHubEventNamePush, DeliveryID: "delivery-id"},
				Revision:   actionsv1alpha1.GitRevision{SHA: strings.Repeat("a", 40), Ref: "refs/heads/main"},
			}},
		},
		Status: actionsv1alpha1.WorkflowRunStatus{WorkflowName: "CI"},
	}
	job := &actionsv1alpha1.WorkflowJob{
		TypeMeta:   metav1.TypeMeta{APIVersion: actionsv1alpha1.GroupVersion.String(), Kind: "WorkflowJob"},
		ObjectMeta: metav1.ObjectMeta{Name: "build", Namespace: "default", UID: "job-uid", Labels: map[string]string{actionsv1alpha1.LabelWorkflowRunUID: string(run.UID)}},
		Spec:       actionsv1alpha1.WorkflowJobSpec{WorkflowRunRef: corev1.LocalObjectReference{Name: run.Name}, JobID: "build"},
	}
	if err := controllerutil.SetControllerReference(run, job, scheme); err != nil {
		t.Fatal(err)
	}
	newerRun := run.DeepCopy()
	newerRun.Name = "lint"
	newerRun.UID = "lint-run-uid"
	newerRun.CreationTimestamp = metav1.NewTime(time.Date(2026, 8, 10, 13, 0, 0, 0, time.UTC))
	newerRun.Status.WorkflowName = "Lint"
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
	clusterClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(run, newerRun, job, project, secret).Build()
	handler, err := New(Config{
		Client: clusterClient, Logs: &testLogSource{pod: pod, logs: "build output\n"}, Token: testConsoleToken,
		SecretManagementNamespace: "default", SecureCookie: secureCookie, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
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
