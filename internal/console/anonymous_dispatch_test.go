package console

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"regexp"
	"strings"
	"testing"

	actionsv1alpha1 "github.com/kelos-dev/open-actions/api/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestConsoleAnonymousDispatch(t *testing.T) {
	for _, test := range []struct {
		name, path    string
		authenticated bool
	}{
		{name: "anonymous", path: "/dispatch"},
		{name: "anonymous with source", path: "/dispatch?source=default%2Fci"},
		{name: "administrator", path: "/dispatch", authenticated: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			handler := newTestHandler(t, false, func(config *Config) { config.AllowAnonymousWorkflowRuns = true })
			resolver := &testRepositoryResolver{workflowFile: testDispatchWorkflow}
			handler.repositories = resolver
			request := httptest.NewRequest(http.MethodGet, test.path, nil)
			cookie := &http.Cookie{Name: sessionCookieName, Value: handler.sessionValue}
			if test.authenticated {
				request.AddCookie(cookie)
			}
			page := httptest.NewRecorder()
			handler.ServeHTTP(page, request)
			if page.Code != http.StatusOK {
				t.Fatalf("dispatch page = %d, %s", page.Code, page.Body.String())
			}
			if !test.authenticated {
				cookie = responseCookie(t, page.Result(), anonymousCookieName)
				if strings.Contains(page.Body.String(), handler.csrfToken) {
					t.Fatal("anonymous dispatch page exposes administrator CSRF token")
				}
			}
			form := dispatchForm(handler)
			form.Set("csrf", hiddenFormValue(t, page.Body.String(), "csrf"))
			form.Set("request-id", hiddenFormValue(t, page.Body.String(), "request-id"))
			post := func() *httptest.ResponseRecorder {
				request := httptest.NewRequest(http.MethodPost, "/dispatch", strings.NewReader(form.Encode()))
				request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
				request.Header.Set("Sec-Fetch-Site", "same-origin")
				request.AddCookie(cookie)
				response := httptest.NewRecorder()
				handler.ServeHTTP(response, request)
				return response
			}
			form.Set("action", "load")
			loaded := post()
			if loaded.Code != http.StatusOK || !strings.Contains(loaded.Body.String(), `id="run-workflow" type="submit">Run workflow</button>`) {
				t.Fatalf("load workflow = %d, %s", loaded.Code, loaded.Body.String())
			}
			if hiddenFormValue(t, loaded.Body.String(), "csrf") != form.Get("csrf") {
				t.Fatal("loading the workflow did not preserve the form CSRF token")
			}
			assertDispatchNotCreated(t, handler, form)
			form.Del("action")
			form.Set("loaded-selection", hiddenFormValue(t, loaded.Body.String(), "loaded-selection"))
			form["input-name"] = []string{"environment", "dry-run"}
			form["input-value"] = []string{"production", "true"}
			for attempt := 0; attempt < 2; attempt++ {
				response := post()
				if response.Code != http.StatusSeeOther || response.Header().Get("Location") != "/runs/default/dispatch-"+form.Get("request-id") {
					t.Fatalf("dispatch = %d, %s", response.Code, response.Body.String())
				}
			}
			run := &actionsv1alpha1.WorkflowRun{}
			if err := handler.client.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "dispatch-" + form.Get("request-id")}, run); err != nil {
				t.Fatal(err)
			}
			source := run.Spec.Source.GitHub
			if run.Spec.ProjectRef.Name != "project" || run.Spec.WorkflowPath != form.Get("workflow-path") || source == nil || source.Event.Name != actionsv1alpha1.GitHubEventNameWorkflowDispatch || source.Revision.SHA != form.Get("revision") || source.Revision.Ref != "refs/heads/main" || source.Repository.ID != 123 || !reflect.DeepEqual(source.Event.Inputs, map[string]string{"environment": "production", "dry-run": "true"}) {
				t.Fatalf("dispatched WorkflowRun = %#v", run.Spec)
			}
			if run.Spec.TTLSecondsAfterFinished == nil || *run.Spec.TTLSecondsAfterFinished != 604800 {
				t.Fatalf("dispatch TTL = %v", run.Spec.TTLSecondsAfterFinished)
			}
			wantRequest := testWorkflowFileRequest{client.ObjectKey{Namespace: "default", Name: "project"}, "acme", "example", form.Get("workflow-path"), form.Get("revision")}
			if !reflect.DeepEqual(resolver.workflowRequests, []testWorkflowFileRequest{wantRequest, wantRequest, wantRequest}) {
				t.Fatalf("workflow requests = %#v", resolver.workflowRequests)
			}
			runs := &actionsv1alpha1.WorkflowRunList{}
			if err := handler.client.List(context.Background(), runs); err != nil {
				t.Fatal(err)
			}
			if len(runs.Items) != 3 {
				t.Fatalf("WorkflowRuns after duplicate dispatch = %d, want 3", len(runs.Items))
			}
		})
	}
}

func TestConsoleAnonymousDispatchValidatesWorkflowAndInputs(t *testing.T) {
	for _, test := range []struct {
		name, workflow, input, message string
	}{
		{name: "missing dispatch trigger", workflow: "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo done\n", input: "production", message: "does not declare workflow_dispatch"},
		{name: "invalid choice", workflow: testDispatchWorkflow, input: "unknown", message: "invalid inputs"},
	} {
		t.Run(test.name, func(t *testing.T) {
			handler := newTestHandler(t, false, func(config *Config) { config.AllowAnonymousWorkflowRuns = true })
			handler.repositories = &testRepositoryResolver{workflowFile: test.workflow}
			page := httptest.NewRecorder()
			handler.ServeHTTP(page, httptest.NewRequest(http.MethodGet, "/dispatch?source=default%2Fci", nil))
			cookie := responseCookie(t, page.Result(), anonymousCookieName)
			form := dispatchForm(handler)
			form.Set("workflow-path", ".open-actions/workflows/ci.yaml")
			form.Set("revision", strings.Repeat("a", 40))
			for _, name := range []string{"csrf", "request-id", "loaded-selection"} {
				form.Set(name, hiddenFormValue(t, page.Body.String(), name))
			}
			form["input-name"] = []string{"environment"}
			form["input-value"] = []string{test.input}
			request := httptest.NewRequest(http.MethodPost, "/dispatch", strings.NewReader(form.Encode()))
			request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			request.AddCookie(cookie)
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != http.StatusBadRequest || !strings.Contains(response.Body.String(), test.message) {
				t.Fatalf("dispatch = %d, %s", response.Code, response.Body.String())
			}
			assertDispatchNotCreated(t, handler, form)
		})
	}
}

func TestConsoleAnonymousWorkflowRunsRequireOptIn(t *testing.T) {
	for _, path := range []string{"/dispatch", "/runs/default/ci/rerun"} {
		handler := newTestHandler(t, false)
		cookie := &http.Cookie{Name: anonymousCookieName, Value: "browser-cookie"}
		form := url.Values{"csrf": {handler.anonymousCSRFValue(cookie.Value)}}
		request := httptest.NewRequest(http.MethodPost, path, strings.NewReader(form.Encode()))
		request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		request.AddCookie(cookie)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusFound || !strings.HasPrefix(response.Header().Get("Location"), "/login?next=") {
			t.Fatalf("disabled anonymous action %s = %d, %s", path, response.Code, response.Body.String())
		}
	}
}

func hiddenFormValue(t *testing.T, page, name string) string {
	t.Helper()
	match := regexp.MustCompile(`name="` + regexp.QuoteMeta(name) + `" value="([^"]+)"`).FindStringSubmatch(page)
	if len(match) != 2 {
		t.Fatalf("page has no %s field: %s", name, page)
	}
	return match[1]
}
