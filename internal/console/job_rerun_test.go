package console

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"html"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"regexp"
	"slices"
	"strings"
	"testing"

	actionsv1alpha1 "github.com/kelos-dev/open-actions/api/v1alpha1"
	"github.com/kelos-dev/open-actions/internal/workflowrun"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// https://docs.github.com/en/rest/actions/workflow-runs#re-run-a-job-from-a-workflow-run
func TestConsoleRerunsSelectedJob(t *testing.T) {
	for _, name := range []string{"successful job", "failed job", "retained job", "matrix combination", "sidebar job"} {
		t.Run(name, func(t *testing.T) {
			handler, root, job := jobRerunFixture(t)
			latest := root
			wantIDs := []string{"build", "deploy", "report"}
			if name == "failed job" {
				job.Status.Result = actionsv1alpha1.WorkflowJobResultFailure
				root.Status.Conditions[0].Status = metav1.ConditionFalse
				root.Status.Conditions[0].Reason = "JobFailed"
			}
			if name == "matrix combination" {
				job.Spec.JobID = "build-matrix-1"
				job.Spec.Matrix = &actionsv1alpha1.WorkflowJobMatrix{LogicalJobID: "build", JobTotal: 2, Values: map[string]string{"os": "linux"}}
				sibling := job.DeepCopy()
				sibling.Name, sibling.UID, sibling.ResourceVersion = "build-sibling", "sibling-uid", ""
				sibling.Spec.JobID = "build-matrix-2"
				sibling.Spec.Matrix.Values["os"] = "windows"
				if err := handler.client.Create(context.Background(), sibling); err != nil {
					t.Fatal(err)
				}
				root.Status.Jobs.Total++
				wantIDs[0] = "build-matrix-1"
			}
			if err := handler.client.Update(context.Background(), job); err != nil {
				t.Fatal(err)
			}
			if err := handler.client.Update(context.Background(), root); err != nil {
				t.Fatal(err)
			}
			attempt := int32(2)
			if name == "retained job" {
				latest = completedJobRerun(t, handler, root, "report")
				attempt = 3
			}
			pagePath := runPath(latest) + "/jobs/" + job.Name
			pageRequest := httptest.NewRequest(http.MethodGet, pagePath, nil)
			pageRequest.Header.Set("Authorization", "Bearer "+testConsoleToken)
			page := httptest.NewRecorder()
			handler.ServeHTTP(page, pageRequest)
			if page.Code != http.StatusOK {
				t.Fatalf("job page = %d, %s", page.Code, page.Body.String())
			}
			for _, want := range []string{`action="` + runPath(latest) + `/rerun"`, `name="jobs" value="selected"`, `name="job" value="build"`, `name="csrf" value="` + handler.csrfToken + `"`, `aria-label="Re-run job: ` + job.Spec.JobID + `"`} {
				if !strings.Contains(page.Body.String(), want) {
					t.Fatalf("job page is missing %q", want)
				}
			}
			selectedName := job.Name
			if name == "sidebar job" {
				selectedName = "report"
				wantIDs = []string{"deploy", "report"}
			}
			form := renderedRerunForm(t, page.Body.String(), runPath(latest)+"/rerun", "selected", selectedName)
			response := submitJobRerun(handler, runPath(latest), form, true)
			if response.Code != http.StatusSeeOther || response.Header().Get("Location") != "/runs/default/"+workflowrun.RerunName(root, attempt) {
				t.Fatalf("selected job rerun = %d, %s", response.Code, response.Body.String())
			}
			rerun := &actionsv1alpha1.WorkflowRun{}
			if err := handler.client.Get(context.Background(), client.ObjectKey{Namespace: root.Namespace, Name: workflowrun.RerunName(root, attempt)}, rerun); err != nil {
				t.Fatal(err)
			}
			want := workflowrun.NewRerun(root, latest, attempt, wantIDs)
			if !reflect.DeepEqual(rerun.Spec, want.Spec) {
				t.Fatalf("rerun spec = %#v, want %#v", rerun.Spec, want.Spec)
			}
		})
	}
}

// https://docs.github.com/en/actions/how-tos/manage-workflow-runs/re-run-workflows-and-jobs
func TestConsoleWorkflowRerunControls(t *testing.T) {
	for _, view := range []string{"summary", "job logs"} {
		for _, state := range []string{"succeeded", "failed", "latest attempt failed", "active"} {
			for _, selection := range []string{"all", "failed"} {
				if selection == "failed" && (state == "succeeded" || state == "active") {
					continue
				}
				t.Run(view+"/"+state+"/"+selection, func(t *testing.T) {
					handler, root, job := jobRerunFixture(t)
					latest, attempt := root, int32(2)
					if state == "latest attempt failed" {
						latest = completedJobRerun(t, handler, root, "build")
						attempt = 3
						if err := handler.client.Get(context.Background(), client.ObjectKey{Namespace: root.Namespace, Name: "build-attempt-2"}, job); err != nil {
							t.Fatal(err)
						}
					}
					failed := strings.Contains(state, "failed")
					if failed {
						latest.Status.Conditions[0].Status = metav1.ConditionFalse
						latest.Status.Conditions[0].Reason = "JobFailed"
						job.Status.Result = actionsv1alpha1.WorkflowJobResultFailure
						if err := handler.client.Update(context.Background(), job); err != nil {
							t.Fatal(err)
						}
					} else if state == "active" {
						latest.Status.Conditions = nil
					}
					if err := handler.client.Update(context.Background(), latest); err != nil {
						t.Fatal(err)
					}
					path := runPath(root)
					if view == "job logs" {
						path += "/jobs/build"
					}
					request := httptest.NewRequest(http.MethodGet, path, nil)
					request.Header.Set("Authorization", "Bearer "+testConsoleToken)
					page := httptest.NewRecorder()
					handler.ServeHTTP(page, request)
					if page.Code != http.StatusOK {
						t.Fatalf("page = %d, %s", page.Code, page.Body.String())
					}
					body := page.Body.String()
					if got := strings.Contains(body, `<details class="rerun-menu">`); got != failed {
						t.Fatalf("rerun menu present = %t, want %t", got, failed)
					}
					if got := strings.Contains(body, `>Re-run failed jobs</button>`); got != failed {
						t.Fatalf("failed jobs action present = %t, want %t", got, failed)
					}
					if state == "active" {
						if strings.Contains(body, `action="`+runPath(root)+`/rerun"`) {
							t.Fatal("active attempt offers rerun actions")
						}
						return
					}
					form := renderedRerunForm(t, body, runPath(root)+"/rerun", selection, "")
					response := submitJobRerun(handler, runPath(root), form, true)
					if response.Code != http.StatusSeeOther {
						t.Fatalf("rerun = %d, %s", response.Code, response.Body.String())
					}
					rerun := &actionsv1alpha1.WorkflowRun{}
					if err := handler.client.Get(context.Background(), client.ObjectKey{Namespace: root.Namespace, Name: workflowrun.RerunName(root, attempt)}, rerun); err != nil {
						t.Fatal(err)
					}
					var wantIDs []string
					if selection == "failed" {
						wantIDs = []string{"build", "deploy", "report"}
						if state == "latest attempt failed" {
							wantIDs = []string{"build"}
						}
					}
					want := workflowrun.NewRerun(root, latest, attempt, wantIDs)
					if !reflect.DeepEqual(rerun.Spec, want.Spec) {
						t.Fatalf("rerun spec = %#v, want %#v", rerun.Spec, want.Spec)
					}
				})
			}
		}
	}
}

func renderedRerunForm(t *testing.T, body, action, selection, jobName string) url.Values {
	t.Helper()
	forms := regexp.MustCompile(`(?s)<form[^>]*action="([^"]+)"[^>]*>(.*?)</form>`)
	inputs := regexp.MustCompile(`name="([^"]+)" value="([^"]*)"`)
	for _, match := range forms.FindAllStringSubmatch(body, -1) {
		if html.UnescapeString(match[1]) != action {
			continue
		}
		form := url.Values{}
		for _, input := range inputs.FindAllStringSubmatch(match[2], -1) {
			form.Add(html.UnescapeString(input[1]), html.UnescapeString(input[2]))
		}
		if form.Get("jobs") == selection && form.Get("job") == jobName {
			return form
		}
	}
	t.Fatalf("missing rerun form: action=%q jobs=%q job=%q", action, selection, jobName)
	return nil
}

func TestConsoleJobRerunAuthorization(t *testing.T) {
	for _, anonymous := range []bool{false, true} {
		t.Run(map[bool]string{false: "sign in required", true: "anonymous enabled"}[anonymous], func(t *testing.T) {
			handler, _, _ := jobRerunFixture(t)
			handler.allowAnonymousWorkflowRuns = anonymous
			page := httptest.NewRecorder()
			handler.ServeHTTP(page, httptest.NewRequest(http.MethodGet, "/runs/default/ci/jobs/build", nil))
			if page.Code != http.StatusOK {
				t.Fatalf("job page = %d, %s", page.Code, page.Body.String())
			}
			form := url.Values{"jobs": {"selected"}, "job": {"build"}}
			if !anonymous {
				want := `href="/login?next=%2Fruns%2Fdefault%2Fci%2Fjobs%2Fbuild">Sign in to re-run</a>`
				if !strings.Contains(page.Body.String(), want) || strings.Contains(page.Body.String(), `aria-label="Re-run job: build"`) {
					t.Fatal("job page does not offer sign-in returning to the job")
				}
				response := submitJobRerun(handler, "/runs/default/ci", form, false)
				if response.Code != http.StatusFound || !strings.HasPrefix(response.Header().Get("Location"), "/login?") {
					t.Fatalf("unauthenticated rerun = %d, %s", response.Code, response.Body.String())
				}
				return
			}
			match := regexp.MustCompile(`name="csrf" value="([^"]+)"`).FindStringSubmatch(page.Body.String())
			if len(match) != 2 || match[1] == handler.csrfToken || !strings.Contains(page.Body.String(), `aria-label="Re-run job: build"`) {
				t.Fatal("job page does not offer an anonymous rerun form")
			}
			form.Set("csrf", match[1])
			cookie := responseCookie(t, page.Result(), anonymousCookieName)
			for _, fetchSite := range []string{"cross-site", "same-origin"} {
				request := httptest.NewRequest(http.MethodPost, "/runs/default/ci/rerun", strings.NewReader(form.Encode()))
				request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
				request.Header.Set("Sec-Fetch-Site", fetchSite)
				request.AddCookie(cookie)
				response := httptest.NewRecorder()
				handler.ServeHTTP(response, request)
				want := http.StatusForbidden
				if fetchSite == "same-origin" {
					want = http.StatusSeeOther
				}
				if response.Code != want {
					t.Fatalf("anonymous rerun from %s = %d, %s", fetchSite, response.Code, response.Body.String())
				}
			}
		})
	}
}

func TestConsoleRejectsUnavailableJobRerun(t *testing.T) {
	for _, test := range []struct {
		name string
		code int
	}{
		{name: "missing csrf", code: http.StatusForbidden},
		{name: "missing job name", code: http.StatusBadRequest},
		{name: "missing job", code: http.StatusNotFound},
		{name: "foreign job", code: http.StatusServiceUnavailable},
		{name: "active attempt", code: http.StatusConflict},
		{name: "attempt limit", code: http.StatusConflict},
		{name: "superseded job", code: http.StatusConflict},
		{name: "incomplete job history", code: http.StatusConflict},
		{name: "incomplete run history", code: http.StatusConflict},
		{name: "mismatched previous run UID", code: http.StatusConflict},
	} {
		t.Run(test.name, func(t *testing.T) {
			handler, root, job := jobRerunFixture(t)
			form := url.Values{"csrf": {handler.csrfToken}, "jobs": {"selected"}, "job": {job.Name}}
			path := runPath(root)
			switch test.name {
			case "missing csrf":
				form.Del("csrf")
			case "missing job name":
				form.Del("job")
			case "missing job":
				form.Set("job", "absent")
			case "foreign job":
				job.OwnerReferences = nil
				if err := handler.client.Update(context.Background(), job); err != nil {
					t.Fatal(err)
				}
			case "active attempt", "attempt limit":
				latest := completedJobRerun(t, handler, root, "report")
				if test.name == "active attempt" {
					latest.Status.Conditions = nil
				} else {
					latest.Spec.Rerun.Attempt = maxRerunAttempt
				}
				if err := handler.client.Update(context.Background(), latest); err != nil {
					t.Fatal(err)
				}
			case "superseded job":
				completedJobRerun(t, handler, root, "build")
			case "incomplete job history":
				dependent := &actionsv1alpha1.WorkflowJob{ObjectMeta: metav1.ObjectMeta{Name: "report", Namespace: root.Namespace}}
				if err := handler.client.Delete(context.Background(), dependent); err != nil {
					t.Fatal(err)
				}
			case "incomplete run history", "mismatched previous run UID":
				latest := completedJobRerun(t, handler, root, "build")
				if test.name == "incomplete run history" {
					latest.Spec.Rerun.PreviousRunRef.Name = "missing-attempt"
				} else {
					latest.Spec.Rerun.PreviousRunRef.UID = "replaced-run-uid"
				}
				if err := handler.client.Update(context.Background(), latest); err != nil {
					t.Fatal(err)
				}
				path = runPath(latest)
				form.Set("job", "build-attempt-2")
			}
			if slices.Contains([]string{"active attempt", "attempt limit", "superseded job"}, test.name) {
				page := httptest.NewRecorder()
				request := httptest.NewRequest(http.MethodGet, path+"/jobs/build", nil)
				request.Header.Set("Authorization", "Bearer "+testConsoleToken)
				handler.ServeHTTP(page, request)
				if page.Code != http.StatusOK || strings.Contains(page.Body.String(), `aria-label="Re-run job: build"`) {
					t.Fatalf("unavailable rerun page = %d, %s", page.Code, page.Body.String())
				}
			}
			before := &actionsv1alpha1.WorkflowRunList{}
			if err := handler.client.List(context.Background(), before); err != nil {
				t.Fatal(err)
			}
			response := submitJobRerun(handler, path, form, true)
			if response.Code != test.code {
				t.Fatalf("rejected rerun = %d, %s, want %d", response.Code, response.Body.String(), test.code)
			}
			after := &actionsv1alpha1.WorkflowRunList{}
			if err := handler.client.List(context.Background(), after); err != nil {
				t.Fatal(err)
			}
			if len(before.Items) != len(after.Items) {
				t.Fatal("rejected request created a WorkflowRun")
			}
		})
	}
}

func TestConsoleJobRerunHistoryAPIErrors(t *testing.T) {
	for _, operation := range []string{"list jobs", "load previous run"} {
		resource := "workflowjobs"
		if operation == "load previous run" {
			resource = "workflowruns"
		}
		for _, cause := range []error{
			apierrors.NewForbidden(schema.GroupResource{Group: actionsv1alpha1.GroupVersion.Group, Resource: resource}, "ci", errors.New("user system:serviceaccount:open-actions-system:open-actions-console cannot read history")),
			apierrors.NewTimeoutError("history request timed out", 1),
		} {
			t.Run(operation+"/"+string(apierrors.ReasonForError(cause)), func(t *testing.T) {
				handler, run, job := jobRerunFixture(t)
				handler.allowAnonymousWorkflowRuns = true
				jobName := job.Name
				failing := &rerunHistoryErrorClient{Client: handler.client}
				if operation == "list jobs" {
					failing.listError = cause
				} else {
					run = completedJobRerun(t, handler, run, "build")
					run.Spec.Rerun.PreviousRunRef.Name = "unavailable-previous"
					if err := handler.client.Update(context.Background(), run); err != nil {
						t.Fatal(err)
					}
					jobName = "build-attempt-2"
					failing.getRunName, failing.getError = run.Spec.Rerun.PreviousRunRef.Name, cause
				}
				path := runPath(run)
				page := httptest.NewRecorder()
				handler.ServeHTTP(page, httptest.NewRequest(http.MethodGet, path+"/jobs/"+jobName, nil))
				if page.Code != http.StatusOK {
					t.Fatalf("job page = %d, %s", page.Code, page.Body.String())
				}
				form := renderedRerunForm(t, page.Body.String(), path+"/rerun", "selected", jobName)
				before := &actionsv1alpha1.WorkflowRunList{}
				if err := handler.client.List(context.Background(), before); err != nil {
					t.Fatal(err)
				}
				var logs bytes.Buffer
				handler.logger = slog.New(slog.NewJSONHandler(&logs, nil))
				handler.client = failing
				request := httptest.NewRequest(http.MethodPost, path+"/rerun", strings.NewReader(form.Encode()))
				request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
				request.AddCookie(responseCookie(t, page.Result(), anonymousCookieName))
				response := httptest.NewRecorder()
				handler.ServeHTTP(response, request)
				if response.Code != http.StatusServiceUnavailable || response.Body.String() != "Console request failed\n" {
					t.Fatalf("API failure response = %d, %q", response.Code, response.Body.String())
				}
				var entry map[string]string
				if err := json.Unmarshal(logs.Bytes(), &entry); err != nil {
					t.Fatal(err)
				}
				if entry["level"] != "WARN" || entry["msg"] != "Console request failed" || entry["path"] != path+"/rerun" || !strings.Contains(entry["error"], cause.Error()) {
					t.Fatalf("API failure log = %v", entry)
				}
				after := &actionsv1alpha1.WorkflowRunList{}
				if err := handler.client.List(context.Background(), after); err != nil {
					t.Fatal(err)
				}
				if len(before.Items) != len(after.Items) {
					t.Fatal("failed history lookup created a WorkflowRun")
				}
			})
		}
	}
}

type rerunHistoryErrorClient struct {
	client.Client
	listError  error
	getError   error
	getRunName string
}

func (c *rerunHistoryErrorClient) List(ctx context.Context, list client.ObjectList, options ...client.ListOption) error {
	if _, ok := list.(*actionsv1alpha1.WorkflowJobList); ok && c.listError != nil {
		return c.listError
	}
	return c.Client.List(ctx, list, options...)
}

func (c *rerunHistoryErrorClient) Get(ctx context.Context, key client.ObjectKey, object client.Object, options ...client.GetOption) error {
	if _, ok := object.(*actionsv1alpha1.WorkflowRun); ok && key.Name == c.getRunName && c.getError != nil {
		return c.getError
	}
	return c.Client.Get(ctx, key, object, options...)
}

func submitJobRerun(handler *Handler, path string, form url.Values, authenticated bool) *httptest.ResponseRecorder {
	request := httptest.NewRequest(http.MethodPost, path+"/rerun", strings.NewReader(form.Encode()))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	if authenticated {
		request.Header.Set("Authorization", "Bearer "+testConsoleToken)
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func jobRerunFixture(t *testing.T) (*Handler, *actionsv1alpha1.WorkflowRun, *actionsv1alpha1.WorkflowJob) {
	t.Helper()
	handler := newTestHandler(t, false)
	run := &actionsv1alpha1.WorkflowRun{}
	if err := handler.client.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "ci"}, run); err != nil {
		t.Fatal(err)
	}
	run.Status.Jobs = &actionsv1alpha1.WorkflowRunJobStatus{Total: 4}
	run.Status.Conditions = []metav1.Condition{{Type: actionsv1alpha1.WorkflowRunConditionSucceeded, Status: metav1.ConditionTrue, Reason: "JobsSucceeded"}}
	if err := handler.client.Update(context.Background(), run); err != nil {
		t.Fatal(err)
	}
	job := &actionsv1alpha1.WorkflowJob{}
	if err := handler.client.Get(context.Background(), client.ObjectKey{Namespace: run.Namespace, Name: "build"}, job); err != nil {
		t.Fatal(err)
	}
	job.Status.Result = actionsv1alpha1.WorkflowJobResultSuccess
	if err := handler.client.Update(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	for name, needs := range map[string][]string{"report": {"build"}, "deploy": {"report"}, "lint": nil} {
		dependent := &actionsv1alpha1.WorkflowJob{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: run.Namespace, Labels: map[string]string{actionsv1alpha1.LabelWorkflowRunUID: string(run.UID)}},
			Spec:       actionsv1alpha1.WorkflowJobSpec{WorkflowRunRef: corev1.LocalObjectReference{Name: run.Name}, JobID: name, Needs: needs},
			Status:     actionsv1alpha1.WorkflowJobStatus{Result: actionsv1alpha1.WorkflowJobResultSuccess},
		}
		if err := controllerutil.SetControllerReference(run, dependent, handler.client.Scheme()); err != nil {
			t.Fatal(err)
		}
		if err := handler.client.Create(context.Background(), dependent); err != nil {
			t.Fatal(err)
		}
	}
	return handler, run, job
}

func completedJobRerun(t *testing.T, handler *Handler, root *actionsv1alpha1.WorkflowRun, jobID string) *actionsv1alpha1.WorkflowRun {
	t.Helper()
	run := workflowrun.NewRerun(root, root, 2, []string{jobID})
	run.UID = "attempt-2-uid"
	run.Status.Jobs = &actionsv1alpha1.WorkflowRunJobStatus{Total: 1}
	run.Status.Conditions = root.DeepCopy().Status.Conditions
	if err := handler.client.Create(context.Background(), run); err != nil {
		t.Fatal(err)
	}
	job := &actionsv1alpha1.WorkflowJob{}
	if err := handler.client.Get(context.Background(), client.ObjectKey{Namespace: root.Namespace, Name: jobID}, job); err != nil {
		t.Fatal(err)
	}
	job.Name, job.UID, job.ResourceVersion = jobID+"-attempt-2", "attempt-2-job-uid", ""
	job.OwnerReferences = nil
	job.Labels[actionsv1alpha1.LabelWorkflowRunUID] = string(run.UID)
	job.Spec.WorkflowRunRef.Name = run.Name
	if err := controllerutil.SetControllerReference(run, job, handler.client.Scheme()); err != nil {
		t.Fatal(err)
	}
	if err := handler.client.Create(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	return run
}
