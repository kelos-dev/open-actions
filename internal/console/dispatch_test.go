package console

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"regexp"
	"strings"
	"testing"

	actionsv1alpha1 "github.com/kelos-dev/open-actions/api/v1alpha1"
	githubclient "github.com/kelos-dev/open-actions/internal/github"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const testDispatchWorkflow = `name: Deploy
on:
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
      retries:
        type: number
        default: 0
      notes:
        type: string
      message:
        type: string
        default: ''
jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - run: make deploy
`

func dispatchForm(handler *Handler) url.Values {
	return url.Values{
		"csrf": {handler.csrfToken}, "request-id": {"0123456789abcdefabcd"},
		"project": {"default/project"}, "repository-owner": {"acme"}, "repository-name": {"example"},
		"ref-type": {"branch"}, "ref-name": {"main"}, "revision": {strings.Repeat("b", 40)},
		"workflow-path": {".open-actions/workflows/deploy.yaml"},
	}
}

func postDispatchForm(handler *Handler, form url.Values) *httptest.ResponseRecorder {
	request := httptest.NewRequest(http.MethodPost, "/dispatch", strings.NewReader(form.Encode()))
	request.Header.Set("Authorization", "Bearer "+testConsoleToken)
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func loadDispatchForm(t *testing.T, handler *Handler, form url.Values) string {
	t.Helper()
	form.Set("action", "load")
	response := postDispatchForm(handler, form)
	form.Del("action")
	if response.Code != http.StatusOK {
		t.Fatalf("load workflow = %d, %s", response.Code, response.Body.String())
	}
	selection := regexp.MustCompile(`name="loaded-selection" value="([a-f0-9]{64})"`).FindStringSubmatch(response.Body.String())
	if len(selection) != 2 {
		t.Fatalf("loaded workflow has no selection: %s", response.Body.String())
	}
	form.Set("loaded-selection", selection[1])
	return response.Body.String()
}

func assertDispatchNotCreated(t *testing.T, handler *Handler, form url.Values) {
	t.Helper()
	run := &actionsv1alpha1.WorkflowRun{}
	err := handler.client.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "dispatch-" + form.Get("request-id")}, run)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("unexpected WorkflowRun after dispatch request: %#v, %v", run, err)
	}
}

func TestConsoleLoadsDispatchInputsWithoutPreviousRuns(t *testing.T) {
	handler := newTestHandler(t, false)
	for _, object := range []client.Object{&actionsv1alpha1.WorkflowRun{}, &corev1.ConfigMap{}} {
		if err := handler.client.DeleteAllOf(context.Background(), object); err != nil {
			t.Fatal(err)
		}
	}
	resolver := &testRepositoryResolver{
		repository:   actionsv1alpha1.GitHubRepository{ID: 456, Owner: "Acme", Name: "Example"},
		workflowFile: testDispatchWorkflow,
	}
	handler.repositories = resolver
	request := httptest.NewRequest(http.MethodGet, "/dispatch", nil)
	request.Header.Set("Authorization", "Bearer "+testConsoleToken)
	initial := httptest.NewRecorder()
	handler.ServeHTTP(initial, request)
	if initial.Code != http.StatusOK || !strings.Contains(initial.Body.String(), `id="run-workflow" type="submit" disabled`) {
		t.Fatalf("initial dispatch page = %d, %s", initial.Code, initial.Body.String())
	}
	form := dispatchForm(handler)
	page := loadDispatchForm(t, handler, form)
	for _, expected := range []string{
		`<code>environment</code><span class="input-type">choice</span><span aria-label="required">Required</span>`,
		`<p class="input-description">Deployment environment</p>`,
		`<option value="staging" selected>staging</option>`,
		`<option value="production">production</option>`,
		`<option value="false" selected>false</option>`,
		`id="workflow-input-2" name="input-value" maxlength="65535" data-input-field></textarea>`,
		`value="notes" data-input-field disabled`,
		`id="workflow-input-4" name="input-value" maxlength="65535" data-input-field>0</textarea>`,
		`id="run-workflow" type="submit">Run workflow</button>`,
	} {
		if !strings.Contains(page, expected) {
			t.Fatalf("loaded workflow does not contain %q: %s", expected, page)
		}
	}
	wantRequest := testWorkflowFileRequest{client.ObjectKey{Namespace: "default", Name: "project"}, "Acme", "Example", form.Get("workflow-path"), form.Get("revision")}
	if !reflect.DeepEqual(resolver.workflowRequests, []testWorkflowFileRequest{wantRequest}) {
		t.Fatalf("workflow file requests = %#v, want %#v", resolver.workflowRequests, wantRequest)
	}
	if len(resolver.revisionRequests) != 0 {
		t.Fatalf("pinned commit resolved the ref: %#v", resolver.revisionRequests)
	}
	assertDispatchNotCreated(t, handler, form)
}

func TestConsoleDispatchesRefHeadWithoutPinnedCommit(t *testing.T) {
	// https://docs.github.com/en/actions/how-tos/manage-workflow-runs/manually-run-a-workflow
	for _, test := range []struct {
		name, refType, refName            string
		wantRefType, wantRefName, wantRef string
	}{
		{name: "default branch", refType: "branch", wantRefType: "branch", wantRefName: "trunk", wantRef: "refs/heads/trunk"},
		{name: "default branch with tag selected", refType: "tag", wantRefType: "branch", wantRefName: "trunk", wantRef: "refs/heads/trunk"},
		{name: "branch", refType: "branch", refName: "feature/deploy", wantRefType: "branch", wantRefName: "feature/deploy", wantRef: "refs/heads/feature/deploy"},
		{name: "tag", refType: "tag", refName: "v1.2.3", wantRefType: "tag", wantRefName: "v1.2.3", wantRef: "refs/tags/v1.2.3"},
	} {
		t.Run(test.name, func(t *testing.T) {
			handler := newTestHandler(t, false)
			resolver := &testRepositoryResolver{defaultBranch: "trunk", workflowFile: testDispatchWorkflow}
			handler.repositories = resolver
			form := dispatchForm(handler)
			form.Set("ref-type", test.refType)
			form.Set("ref-name", test.refName)
			form.Set("revision", "")
			page := loadDispatchForm(t, handler, form)
			for _, expected := range []string{
				`<option value="` + test.wantRefType + `" selected>`,
				`id="ref-name" name="ref-name" value="` + test.wantRefName + `"`,
				`id="revision" name="revision" value=""`,
			} {
				if !strings.Contains(page, expected) {
					t.Fatalf("loaded workflow does not contain %q: %s", expected, page)
				}
			}
			form.Set("ref-type", test.wantRefType)
			form.Set("ref-name", test.wantRefName)
			if response := postDispatchForm(handler, form); response.Code != http.StatusSeeOther {
				t.Fatalf("dispatch = %d, %s", response.Code, response.Body.String())
			}
			run := &actionsv1alpha1.WorkflowRun{}
			if err := handler.client.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "dispatch-" + form.Get("request-id")}, run); err != nil {
				t.Fatal(err)
			}
			if want := (actionsv1alpha1.GitRevision{SHA: testHeadRevision, Ref: test.wantRef}); run.Spec.Source.GitHub.Revision != want {
				t.Fatalf("dispatched revision = %#v, want %#v", run.Spec.Source.GitHub.Revision, want)
			}
			if !reflect.DeepEqual(resolver.revisionRequests, []string{test.wantRef, test.wantRef}) {
				t.Fatalf("revision requests = %#v, want %q for load and run", resolver.revisionRequests, test.wantRef)
			}
			wantRequest := testWorkflowFileRequest{client.ObjectKey{Namespace: "default", Name: "project"}, "acme", "example", form.Get("workflow-path"), testHeadRevision}
			if !reflect.DeepEqual(resolver.workflowRequests, []testWorkflowFileRequest{wantRequest, wantRequest}) {
				t.Fatalf("workflow requests = %#v, want %#v for load and run", resolver.workflowRequests, wantRequest)
			}
		})
	}
}

func TestConsoleResubmittedRefHeadDispatchIsIdempotentAfterRefMoves(t *testing.T) {
	handler := newTestHandler(t, false)
	resolver := &testRepositoryResolver{workflowFile: testDispatchWorkflow}
	handler.repositories = resolver
	form := dispatchForm(handler)
	form.Set("revision", "")
	loadDispatchForm(t, handler, form)
	wantLocation := "/runs/default/dispatch-" + form.Get("request-id")
	if response := postDispatchForm(handler, form); response.Code != http.StatusSeeOther || response.Header().Get("Location") != wantLocation {
		t.Fatalf("dispatch = %d, %s", response.Code, response.Body.String())
	}
	resolver.revision = strings.Repeat("e", 40)
	if response := postDispatchForm(handler, form); response.Code != http.StatusSeeOther || response.Header().Get("Location") != wantLocation {
		t.Fatalf("resubmitted dispatch after the ref moved = %d, %s", response.Code, response.Body.String())
	}
	run := &actionsv1alpha1.WorkflowRun{}
	if err := handler.client.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "dispatch-" + form.Get("request-id")}, run); err != nil {
		t.Fatal(err)
	}
	if run.Spec.Source.GitHub.Revision.SHA != testHeadRevision {
		t.Fatalf("resubmission changed the dispatched revision to %q", run.Spec.Source.GitHub.Revision.SHA)
	}
	form["input-name"], form["input-value"] = []string{"environment"}, []string{"production"}
	if response := postDispatchForm(handler, form); response.Code != http.StatusConflict || !strings.Contains(response.Body.String(), "already exists with different parameters") {
		t.Fatalf("resubmitted dispatch with different inputs = %d, %s", response.Code, response.Body.String())
	}
}

func TestConsoleRejectsUnavailableDispatchRefs(t *testing.T) {
	for _, test := range []struct {
		name, message string
		err           error
		status        int
	}{
		{name: "missing ref", err: &githubclient.APIError{StatusCode: http.StatusNotFound}, status: http.StatusBadRequest, message: `branch "main" in repository acme/example is not accessible to Project "project"`},
		{name: "unavailable provider", err: errors.New("GitHub unavailable"), status: http.StatusServiceUnavailable, message: "Console request failed"},
	} {
		t.Run(test.name, func(t *testing.T) {
			handler := newTestHandler(t, false)
			resolver := &testRepositoryResolver{workflowFile: testDispatchWorkflow}
			handler.repositories = resolver
			form := dispatchForm(handler)
			form.Set("revision", "")
			loadDispatchForm(t, handler, form)
			resolver.revisionErr = test.err
			for _, action := range []string{"load", ""} {
				form.Set("action", action)
				response := postDispatchForm(handler, form)
				if response.Code != test.status || !strings.Contains(response.Body.String(), test.message) {
					t.Fatalf("action %q = %d, %s, want %d containing %q", action, response.Code, response.Body.String(), test.status, test.message)
				}
				assertDispatchNotCreated(t, handler, form)
			}
			if len(resolver.workflowRequests) != 1 {
				t.Fatalf("unresolved ref fetched a workflow: %#v", resolver.workflowRequests)
			}
		})
	}
}

func TestConsoleDispatchesSnapshotFormAtRefHeadWithoutLoading(t *testing.T) {
	for _, refType := range []string{"branch", "tag"} {
		t.Run(refType, func(t *testing.T) {
			handler := newTestHandler(t, false)
			resolver := &testRepositoryResolver{workflowFile: testDispatchWorkflow}
			handler.repositories = resolver
			source := &actionsv1alpha1.WorkflowRun{}
			if err := handler.client.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "ci"}, source); err != nil {
				t.Fatal(err)
			}
			github := source.Spec.Source.GitHub
			github.Repository.Owner, github.Repository.Name = "Acme", "Example"
			refName := "feature/deploy"
			github.Revision.Ref = "refs/heads/" + refName
			if refType == "tag" {
				refName = "v1.2.3"
				github.Revision.Ref = "refs/tags/" + refName
			}
			github.Event = actionsv1alpha1.GitHubEvent{Name: actionsv1alpha1.GitHubEventNameWorkflowDispatch, Inputs: map[string]string{"environment": "production"}}
			if err := handler.client.Update(context.Background(), source); err != nil {
				t.Fatal(err)
			}
			request := httptest.NewRequest(http.MethodGet, "/dispatch?source=default%2Fci", nil)
			request.Header.Set("Authorization", "Bearer "+testConsoleToken)
			page := httptest.NewRecorder()
			handler.ServeHTTP(page, request)
			if page.Code != http.StatusOK || !strings.Contains(page.Body.String(), `value="dry-run" data-input-field disabled>`) {
				t.Fatalf("source dispatch page = %d, %s", page.Code, page.Body.String())
			}
			form := url.Values{
				"project":          {namespacedValue(source.Namespace, source.Spec.ProjectRef.Name)},
				"repository-owner": {github.Repository.Owner}, "repository-name": {github.Repository.Name},
				"ref-type": {refType}, "ref-name": {refName}, "revision": {""},
				"workflow-path": {source.Spec.WorkflowPath},
				"input-name":    {"environment"}, "input-value": {"production"},
			}
			for _, name := range []string{"csrf", "request-id", "loaded-selection"} {
				field := regexp.MustCompile(`name="` + regexp.QuoteMeta(name) + `" value="([^"]+)"`).FindStringSubmatch(page.Body.String())
				if len(field) != 2 {
					t.Fatalf("source dispatch page has no %s: %s", name, page.Body.String())
				}
				form.Set(name, field[1])
			}
			response := postDispatchForm(handler, form)
			if response.Code != http.StatusSeeOther || response.Header().Get("Location") != "/runs/default/dispatch-"+form.Get("request-id") {
				t.Fatalf("dispatch source form = %d, %s", response.Code, response.Body.String())
			}
			created := &actionsv1alpha1.WorkflowRun{}
			if err := handler.client.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "dispatch-" + form.Get("request-id")}, created); err != nil {
				t.Fatal(err)
			}
			wantRevision := actionsv1alpha1.GitRevision{SHA: testHeadRevision, Ref: github.Revision.Ref}
			if created.Spec.Source.GitHub.Revision != wantRevision || !reflect.DeepEqual(created.Spec.Source.GitHub.Event.Inputs, map[string]string{"environment": "production"}) {
				t.Fatalf("created source = %#v", created.Spec.Source.GitHub)
			}
			if !reflect.DeepEqual(resolver.revisionRequests, []string{github.Revision.Ref}) {
				t.Fatalf("revision requests = %#v, want %q", resolver.revisionRequests, github.Revision.Ref)
			}
			wantRequest := testWorkflowFileRequest{client.ObjectKey{Namespace: source.Namespace, Name: source.Spec.ProjectRef.Name}, github.Repository.Owner, github.Repository.Name, source.Spec.WorkflowPath, testHeadRevision}
			if !reflect.DeepEqual(resolver.workflowRequests, []testWorkflowFileRequest{wantRequest}) {
				t.Fatalf("workflow requests = %#v, want %#v", resolver.workflowRequests, wantRequest)
			}
		})
	}
}

func TestConsoleReloadPreservesSuppliedDispatchInputs(t *testing.T) {
	handler := newTestHandler(t, false)
	handler.repositories = &testRepositoryResolver{workflowFile: testDispatchWorkflow}
	form := dispatchForm(handler)
	loadDispatchForm(t, handler, form)
	form["input-name"] = []string{"environment", "dry-run", "retries", "notes"}
	form["input-value"] = []string{"production", "true", "2.5", "Release notes"}
	page := loadDispatchForm(t, handler, form)
	for _, expected := range []string{
		`<option value="production" selected>production</option>`,
		`<option value="true" selected>true</option>`,
		`value="message" data-input-field disabled`,
		`value="notes" data-input-field>`,
		`id="workflow-input-3" name="input-value" maxlength="65535" data-input-field>Release notes</textarea>`,
		`id="workflow-input-4" name="input-value" maxlength="65535" data-input-field>2.5</textarea>`,
	} {
		if !strings.Contains(page, expected) {
			t.Fatalf("reloaded workflow does not contain %q: %s", expected, page)
		}
	}
	assertDispatchNotCreated(t, handler, form)
}

func TestConsoleDispatchRequiresReloadAfterSelectionChanges(t *testing.T) {
	for field, value := range map[string]string{
		"project": "default/another", "repository-owner": "other", "repository-name": "other",
		"ref-type": "tag", "ref-name": "release", "revision": strings.Repeat("c", 40),
		"workflow-path": ".open-actions/workflows/another.yaml", "loaded-selection": "",
	} {
		t.Run(field, func(t *testing.T) {
			handler := newTestHandler(t, false)
			resolver := &testRepositoryResolver{workflowFile: testDispatchWorkflow}
			handler.repositories = resolver
			project := &actionsv1alpha1.Project{}
			if err := handler.client.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "project"}, project); err != nil {
				t.Fatal(err)
			}
			project.Name, project.ResourceVersion = "another", ""
			if err := handler.client.Create(context.Background(), project); err != nil {
				t.Fatal(err)
			}
			form := dispatchForm(handler)
			loadDispatchForm(t, handler, form)
			form["input-name"], form["input-value"] = []string{"notes"}, []string{"Notes for the loaded workflow"}
			form.Set(field, value)
			response := postDispatchForm(handler, form)
			if response.Code != http.StatusConflict || !strings.Contains(response.Body.String(), "load the selected workflow") {
				t.Fatalf("changed selection = %d, %s", response.Code, response.Body.String())
			}
			assertDispatchNotCreated(t, handler, form)
			if len(resolver.workflowRequests) != 1 {
				t.Fatalf("stale form fetched workflow: %#v", resolver.workflowRequests)
			}
			resolver.workflowFile = strings.Replace(testDispatchWorkflow, "default: staging", "default: production", 1)
			page := loadDispatchForm(t, handler, form)
			if !strings.Contains(page, `<option value="production" selected>production</option>`) {
				t.Fatalf("reload did not use selected workflow: %s", page)
			}
			if !strings.Contains(page, `value="notes" data-input-field disabled`) {
				t.Fatalf("reload carried inputs from another selection: %s", page)
			}
			form.Del("input-name")
			form.Del("input-value")
			if response := postDispatchForm(handler, form); response.Code != http.StatusSeeOther {
				t.Fatalf("dispatch after reload = %d, %s", response.Code, response.Body.String())
			}
		})
	}
}

func TestConsoleDispatchInputConformance(t *testing.T) {
	// https://docs.github.com/en/actions/reference/workflows-and-actions/workflow-syntax#onworkflow_dispatchinputs
	for _, test := range []struct {
		name          string
		workflow      string
		names, values []string
		want          map[string]string
		failure       string
	}{
		{name: "omitted defaults", workflow: testDispatchWorkflow},
		{name: "explicit false zero and empty", workflow: testDispatchWorkflow, names: []string{"dry-run", "retries", "notes"}, values: []string{"false", "0", ""}, want: map[string]string{"dry-run": "false", "retries": "0", "notes": ""}},
		{name: "choice", workflow: testDispatchWorkflow, names: []string{"environment"}, values: []string{"production"}, want: map[string]string{"environment": "production"}},
		{name: "unknown input", workflow: testDispatchWorkflow, names: []string{"unknown"}, values: []string{"value"}, failure: `unknown input "unknown"`},
		{name: "invalid choice", workflow: testDispatchWorkflow, names: []string{"environment"}, values: []string{"unknown"}, failure: "invalid for type choice"},
		{name: "invalid boolean", workflow: testDispatchWorkflow, names: []string{"dry-run"}, values: []string{"yes"}, failure: "invalid for type boolean"},
		{name: "invalid number", workflow: testDispatchWorkflow, names: []string{"retries"}, values: []string{"abc"}, failure: "invalid for type number"},
		{name: "required input", workflow: strings.Replace(testDispatchWorkflow, "        default: staging\n", "", 1), failure: `missing required input "environment"`},
		{name: "no inputs", workflow: "name: Manual\non: workflow_dispatch\njobs:\n  run:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo done\n"},
	} {
		t.Run(test.name, func(t *testing.T) {
			handler := newTestHandler(t, false)
			resolver := &testRepositoryResolver{workflowFile: test.workflow}
			handler.repositories = resolver
			form := dispatchForm(handler)
			page := loadDispatchForm(t, handler, form)
			if test.name == "no inputs" && !strings.Contains(page, "This workflow does not declare any inputs.") {
				t.Fatalf("missing empty input state: %s", page)
			}
			form["input-name"], form["input-value"] = test.names, test.values
			response := postDispatchForm(handler, form)
			if test.failure != "" {
				if response.Code != http.StatusBadRequest || !strings.Contains(response.Body.String(), test.failure) {
					t.Fatalf("invalid inputs = %d, %s, want %q", response.Code, response.Body.String(), test.failure)
				}
				assertDispatchNotCreated(t, handler, form)
				return
			}
			if response.Code != http.StatusSeeOther {
				t.Fatalf("dispatch = %d, %s", response.Code, response.Body.String())
			}
			run := &actionsv1alpha1.WorkflowRun{}
			if err := handler.client.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "dispatch-" + form.Get("request-id")}, run); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(run.Spec.Source.GitHub.Event.Inputs, test.want) {
				t.Fatalf("submitted inputs = %#v, want %#v", run.Spec.Source.GitHub.Event.Inputs, test.want)
			}
			if len(resolver.workflowRequests) != 2 || resolver.workflowRequests[0] != resolver.workflowRequests[1] {
				t.Fatalf("dispatch did not validate the loaded revision: %#v", resolver.workflowRequests)
			}
		})
	}
}

func TestConsoleRejectsUnavailableDispatchWorkflows(t *testing.T) {
	for _, test := range []struct {
		name, workflow, message string
		err                     error
		status                  int
	}{
		{name: "missing file", err: &githubclient.APIError{StatusCode: http.StatusNotFound}, status: http.StatusBadRequest, message: `workflow ".open-actions/workflows/deploy.yaml"`},
		{name: "unavailable provider", err: errors.New("GitHub unavailable"), status: http.StatusServiceUnavailable, message: "Console request failed"},
		{name: "invalid YAML", workflow: "on: [", status: http.StatusBadRequest, message: `parse workflow ".open-actions/workflows/deploy.yaml"`},
		{name: "unsupported input type", workflow: strings.Replace(testDispatchWorkflow, "type: choice", "type: object", 1), status: http.StatusBadRequest, message: "unsupported type"},
		{name: "push only", workflow: "on: push\njobs:\n  run:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo done\n", status: http.StatusBadRequest, message: "does not declare workflow_dispatch"},
	} {
		t.Run(test.name, func(t *testing.T) {
			handler := newTestHandler(t, false)
			resolver := &testRepositoryResolver{workflowFile: testDispatchWorkflow}
			handler.repositories = resolver
			form := dispatchForm(handler)
			loadDispatchForm(t, handler, form)
			resolver.workflowFile, resolver.workflowErr = test.workflow, test.err
			for _, action := range []string{"load", ""} {
				form.Set("action", action)
				response := postDispatchForm(handler, form)
				if response.Code != test.status || !strings.Contains(response.Body.String(), test.message) {
					t.Fatalf("action %q = %d, %s, want %d containing %q", action, response.Code, response.Body.String(), test.status, test.message)
				}
				assertDispatchNotCreated(t, handler, form)
			}
		})
	}
}
