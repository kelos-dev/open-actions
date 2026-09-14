package console

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"testing"

	actionsv1alpha1 "github.com/kelos-dev/open-actions/api/v1alpha1"
	"github.com/kelos-dev/open-actions/internal/workflowrun"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestConsoleAnonymousReruns(t *testing.T) {
	for _, test := range []struct {
		name, selection string
		authenticated   bool
	}{
		{name: "anonymous all jobs", selection: "all"},
		{name: "anonymous failed jobs", selection: "failed"},
		{name: "administrator all jobs", selection: "all", authenticated: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			handler, run := anonymousWorkflowFixture(t, true)
			cookie, token := anonymousRerunPage(t, handler)
			if cookie.Value == "" || !cookie.HttpOnly || !cookie.Secure || cookie.SameSite != http.SameSiteLaxMode || cookie.Path != "/" {
				t.Fatalf("anonymous cookie = %#v", cookie)
			}
			if test.authenticated {
				cookie = &http.Cookie{Name: sessionCookieName, Value: handler.sessionValue}
				token = handler.csrfToken
			}
			form := url.Values{"csrf": {token}, "jobs": {test.selection}}
			request := httptest.NewRequest(http.MethodPost, "/runs/default/ci/rerun", strings.NewReader(form.Encode()))
			request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			request.Header.Set("Sec-Fetch-Site", "same-origin")
			request.AddCookie(cookie)
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			rerunName := workflowrun.RerunName(run, 2)
			if response.Code != http.StatusSeeOther || response.Header().Get("Location") != "/runs/default/"+rerunName {
				t.Fatalf("anonymous rerun = %d, %q", response.Code, response.Body.String())
			}
			rerun := &actionsv1alpha1.WorkflowRun{}
			if err := handler.client.Get(context.Background(), client.ObjectKey{Namespace: run.Namespace, Name: rerunName}, rerun); err != nil {
				t.Fatal(err)
			}
			want := run.Spec.DeepCopy()
			want.CancelRequested = false
			want.Rerun = &actionsv1alpha1.WorkflowRunRerun{
				OriginalRunRef: actionsv1alpha1.WorkflowRunReference{Name: run.Name, UID: run.UID},
				PreviousRunRef: actionsv1alpha1.WorkflowRunReference{Name: run.Name, UID: run.UID},
				Attempt:        2,
			}
			if test.selection == "failed" {
				want.Rerun.JobIDs = []string{"build"}
			}
			if !reflect.DeepEqual(rerun.Spec, *want) {
				t.Fatalf("rerun spec = %#v, want %#v", rerun.Spec, *want)
			}

			request = httptest.NewRequest(http.MethodPost, "/runs/default/ci/rerun", strings.NewReader(form.Encode()))
			request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			request.AddCookie(cookie)
			response = httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != http.StatusConflict || !strings.Contains(response.Body.String(), "not complete") {
				t.Fatalf("overlapping rerun = %d, %q", response.Code, response.Body.String())
			}
			runs := &actionsv1alpha1.WorkflowRunList{}
			if err := handler.client.List(context.Background(), runs); err != nil {
				t.Fatal(err)
			}
			if len(runs.Items) != 3 {
				t.Fatalf("WorkflowRuns = %d, want 3", len(runs.Items))
			}
		})
	}
}

func TestConsoleAnonymousWorkflowCSRF(t *testing.T) {
	handler, _ := anonymousWorkflowFixture(t, false)
	cookie, token := anonymousRerunPage(t, handler)
	otherCookie, otherToken := anonymousRerunPage(t, handler)
	if cookie.Secure || cookie.Value == otherCookie.Value || token == otherToken || token == cookie.Value {
		t.Fatal("anonymous CSRF tokens must be bound to separate browser cookies")
	}
	for _, path := range []string{"/dispatch", "/runs/default/ci/rerun"} {
		for _, test := range []struct {
			name          string
			cookie        *http.Cookie
			token         string
			origin        string
			fetchSite     string
			authenticated bool
		}{
			{name: "missing token", cookie: cookie},
			{name: "invalid token", cookie: cookie, token: "invalid"},
			{name: "missing cookie", token: token},
			{name: "empty cookie", cookie: &http.Cookie{Name: cookie.Name}, token: token},
			{name: "another browser", cookie: otherCookie, token: token},
			{name: "administrator token", cookie: cookie, token: handler.csrfToken},
			{name: "anonymous token with administrator session", cookie: cookie, token: token, authenticated: true},
			{name: "cross origin", cookie: cookie, token: token, origin: "https://untrusted.example"},
			{name: "opaque origin", cookie: cookie, token: token, origin: "null"},
			{name: "cross site", cookie: cookie, token: token, fetchSite: "cross-site"},
			{name: "same site different origin", cookie: cookie, token: token, fetchSite: "same-site"},
		} {
			t.Run(path+"/"+test.name, func(t *testing.T) {
				form := url.Values{"csrf": {test.token}, "jobs": {"all"}}
				request := httptest.NewRequest(http.MethodPost, path, strings.NewReader(form.Encode()))
				request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
				request.Header.Set("Origin", test.origin)
				request.Header.Set("Sec-Fetch-Site", test.fetchSite)
				if test.cookie != nil {
					request.AddCookie(test.cookie)
				}
				if test.authenticated {
					request.AddCookie(&http.Cookie{Name: sessionCookieName, Value: handler.sessionValue})
				}
				response := httptest.NewRecorder()
				handler.ServeHTTP(response, request)
				if response.Code != http.StatusForbidden {
					t.Fatalf("workflow action = %d, %q", response.Code, response.Body.String())
				}
			})
		}
	}
	runs := &actionsv1alpha1.WorkflowRunList{}
	if err := handler.client.List(context.Background(), runs); err != nil {
		t.Fatal(err)
	}
	if len(runs.Items) != 2 {
		t.Fatalf("rejected requests created WorkflowRuns: %d", len(runs.Items))
	}

	replica, _ := anonymousWorkflowFixture(t, false)
	request := httptest.NewRequest(http.MethodGet, "/runs/default/ci", nil)
	request.AddCookie(cookie)
	response := httptest.NewRecorder()
	replica.ServeHTTP(response, request)
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `name="csrf" value="`+token+`"`) || len(response.Result().Cookies()) != 0 {
		t.Fatalf("anonymous page on another replica = %d, %q", response.Code, response.Body.String())
	}
}

func TestConsoleAnonymousWorkflowActionsDoNotAuthorizeAdministration(t *testing.T) {
	handler, _ := anonymousWorkflowFixture(t, false)
	cookie, token := anonymousRerunPage(t, handler)
	for _, path := range []string{"/runs/default/ci/cancel", "/runs/default/ci/approve", "/projects/default/project/secrets"} {
		for _, authenticated := range []bool{false, true} {
			t.Run(path+"/authenticated="+strconv.FormatBool(authenticated), func(t *testing.T) {
				form := url.Values{"csrf": {token}}
				request := httptest.NewRequest(http.MethodPost, path, strings.NewReader(form.Encode()))
				request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
				request.AddCookie(cookie)
				want := http.StatusFound
				if authenticated {
					request.AddCookie(&http.Cookie{Name: sessionCookieName, Value: handler.sessionValue})
					want = http.StatusForbidden
				}
				response := httptest.NewRecorder()
				handler.ServeHTTP(response, request)
				if response.Code != want {
					t.Fatalf("administration = %d, %q, want %d", response.Code, response.Body.String(), want)
				}
			})
		}
	}
}

func anonymousWorkflowFixture(t *testing.T, secureCookie bool) (*Handler, *actionsv1alpha1.WorkflowRun) {
	t.Helper()
	handler := newTestHandler(t, secureCookie, func(config *Config) {
		config.AllowAnonymousWorkflowRuns = true
	})
	run := &actionsv1alpha1.WorkflowRun{}
	if err := handler.client.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "ci"}, run); err != nil {
		t.Fatal(err)
	}
	run.Spec.CancelRequested = true
	run.Spec.Source.GitHub.Event.Name = actionsv1alpha1.GitHubEventNameWorkflowDispatch
	run.Spec.Source.GitHub.Event.Inputs = map[string]string{"environment": "staging"}
	run.Status.Jobs = &actionsv1alpha1.WorkflowRunJobStatus{Total: 1}
	run.Status.Conditions = []metav1.Condition{{
		Type: actionsv1alpha1.WorkflowRunConditionSucceeded, Status: metav1.ConditionFalse, Reason: "JobFailed",
	}}
	if err := handler.client.Update(context.Background(), run); err != nil {
		t.Fatal(err)
	}
	job := &actionsv1alpha1.WorkflowJob{}
	if err := handler.client.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "build"}, job); err != nil {
		t.Fatal(err)
	}
	job.Status.Result = actionsv1alpha1.WorkflowJobResultFailure
	if err := handler.client.Update(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	return handler, run
}

func anonymousRerunPage(t *testing.T, handler *Handler) (*http.Cookie, string) {
	t.Helper()
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/runs/default/ci", nil))
	body := response.Body.String()
	if response.Code != http.StatusOK || !strings.Contains(body, "Re-run all jobs") || !strings.Contains(body, "Re-run failed jobs") || strings.Contains(body, "Sign in to re-run") || strings.Contains(body, handler.csrfToken) {
		t.Fatalf("anonymous run page = %d, %q", response.Code, body)
	}
	match := regexp.MustCompile(`name="csrf" value="([^"]+)"`).FindStringSubmatch(body)
	if len(match) != 2 {
		t.Fatal("anonymous rerun form has no CSRF token")
	}
	return responseCookie(t, response.Result(), anonymousCookieName), match[1]
}
