package console

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	actionsv1alpha1 "github.com/kelos-dev/open-actions/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
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
	if runResponse.Code != http.StatusOK || !strings.Contains(runResponse.Body.String(), "CI") || !strings.Contains(runResponse.Body.String(), "build") {
		t.Fatalf("run page = %d, %q", runResponse.Code, runResponse.Body.String())
	}

	streamRequest := httptest.NewRequest(http.MethodGet, runURL+"/jobs/build/stream", nil)
	streamRequest.AddCookie(sessionCookie)
	streamResponse := httptest.NewRecorder()
	handler.ServeHTTP(streamResponse, streamRequest)
	if streamResponse.Code != http.StatusOK || !strings.Contains(streamResponse.Body.String(), "event: log") || !strings.Contains(streamResponse.Body.String(), "build output") {
		t.Fatalf("log stream = %d, %q", streamResponse.Code, streamResponse.Body.String())
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
		ObjectMeta: metav1.ObjectMeta{Name: "ci", Namespace: "default", UID: "run-uid"},
		Spec: actionsv1alpha1.WorkflowRunSpec{
			ProjectRef: corev1.LocalObjectReference{Name: "project"}, WorkflowPath: ".open-actions/workflows/ci.yaml",
			Source: actionsv1alpha1.WorkflowRunSource{Type: actionsv1alpha1.SourceTypeGitHub, GitHub: &actionsv1alpha1.GitHubWorkflowRunSource{
				Repository: actionsv1alpha1.GitHubRepository{ID: 123, Owner: "acme", Name: "example"},
				Revision:   actionsv1alpha1.GitRevision{SHA: strings.Repeat("a", 40)},
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
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "build", Namespace: "default", Labels: map[string]string{actionsv1alpha1.LabelWorkflowJobUID: string(job.UID)}}}
	clusterClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(run, job).Build()
	handler, err := New(Config{
		Client: clusterClient, Logs: &testLogSource{pod: pod, logs: "build output\n"}, Token: testConsoleToken,
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
