package console

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	actionsv1alpha1 "github.com/kelos-dev/open-actions/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestWorkflowJobDuration(t *testing.T) {
	now := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	start := metav1.NewTime(now.Add(-125 * time.Second))
	completion := metav1.NewTime(start.Add(61 * time.Second))
	future := metav1.NewTime(now.Add(time.Second))
	tests := []struct {
		name       string
		start, end *metav1.Time
		result     actionsv1alpha1.WorkflowJobResult
		want       string
		running    bool
	}{
		{name: "queued", want: "—"},
		{name: "running", start: &start, want: "2m 5s", running: true},
		{name: "succeeded", start: &start, end: &completion, result: actionsv1alpha1.WorkflowJobResultSuccess, want: "1m 1s"},
		{name: "failed", start: &start, end: &completion, result: actionsv1alpha1.WorkflowJobResultFailure, want: "1m 1s"},
		{name: "cancelled", start: &start, end: &completion, result: actionsv1alpha1.WorkflowJobResultCancelled, want: "1m 1s"},
		{name: "skipped", result: actionsv1alpha1.WorkflowJobResultSkipped, want: "—"},
		{name: "cancelled before starting", end: &completion, result: actionsv1alpha1.WorkflowJobResultCancelled, want: "—"},
		{name: "completed without timestamp", start: &start, result: actionsv1alpha1.WorkflowJobResultSuccess, want: "—"},
		{name: "completion before result", start: &start, end: &completion, want: "1m 1s"},
		{name: "clock skew", start: &future, want: "0s", running: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			job := &actionsv1alpha1.WorkflowJob{Status: actionsv1alpha1.WorkflowJobStatus{
				StartTime: test.start, CompletionTime: test.end, Result: test.result,
			}}
			got := workflowJobDuration(job, now)
			if got.String() != test.want || got.Running != test.running {
				t.Fatalf("duration = %q, running = %t; want %q, %t", got.String(), got.Running, test.want, test.running)
			}
		})
	}

	job := &actionsv1alpha1.WorkflowJob{Status: actionsv1alpha1.WorkflowJobStatus{
		StartTime:  &start,
		Conditions: []metav1.Condition{{Type: actionsv1alpha1.WorkflowJobConditionSucceeded, Status: metav1.ConditionFalse, Reason: "JobTimedOut"}},
	}}
	if got := workflowJobDuration(job, now); got.Running || got.Seconds != nil {
		t.Fatalf("terminal condition without completion timestamp = %#v", got)
	}
}

func TestFormatDuration(t *testing.T) {
	for _, test := range []struct {
		seconds int64
		want    string
	}{
		{0, "0s"}, {1, "1s"}, {59, "59s"}, {60, "1m 0s"}, {3599, "59m 59s"},
		{3600, "1h 0m 0s"}, {3723, "1h 2m 3s"}, {90061, "25h 1m 1s"},
	} {
		if got := formatDuration(test.seconds); got != test.want {
			t.Errorf("formatDuration(%d) = %q, want %q", test.seconds, got, test.want)
		}
	}
}

func TestConsoleWorkflowRunListDurations(t *testing.T) {
	handler := newTestHandler(t, false)
	store := readyWorkflowRunStore(handler.logger)
	handler.workflowRuns = store
	now := time.Now().Truncate(time.Second)
	start := metav1.NewTime(now.Add(-125 * time.Second))
	completion := metav1.NewTime(start.Add(75 * time.Second))
	for _, name := range []string{"queued", "running", "succeeded", "failed", "cancelled", "missing-completion"} {
		run := testWorkflowRun("default", name, now)
		if name != "queued" {
			run.Status.StartTime = &start
		}
		condition := metav1.Condition{Type: actionsv1alpha1.WorkflowRunConditionSucceeded, Status: metav1.ConditionUnknown}
		if name != "running" && name != "queued" {
			condition.Status = metav1.ConditionFalse
			run.Status.CompletionTime = &completion
		}
		if name == "succeeded" {
			condition.Status = metav1.ConditionTrue
		}
		if name == "cancelled" {
			condition.Reason = "JobCancelled"
		}
		if name == "missing-completion" {
			run.Status.CompletionTime = nil
		}
		run.Status.Conditions = []metav1.Condition{condition}
		store.upsert(run)
	}

	page := httptest.NewRecorder()
	handler.ServeHTTP(page, httptest.NewRequest(http.MethodGet, "/", nil))
	pollURL := durationURLFromPage(t, page.Body.String())
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, pollURL, nil))
	var durations durationsResponse
	if err := json.Unmarshal(response.Body.Bytes(), &durations); err != nil {
		t.Fatal(err)
	}
	if response.Code != http.StatusOK || response.Header().Get("Cache-Control") != "no-store" || !durations.Active || len(durations.Values) != 6 {
		t.Fatalf("run list durations = %d, %s", response.Code, response.Body.String())
	}
	for _, name := range []string{"succeeded", "failed", "cancelled"} {
		duration := durations.Values["/runs/default/"+name]
		if duration.Running || duration.String() != "1m 15s" {
			t.Errorf("%s duration = %#v", name, duration)
		}
	}
	for _, name := range []string{"queued", "missing-completion"} {
		duration := durations.Values["/runs/default/"+name]
		if duration.Running || duration.Seconds != nil {
			t.Errorf("%s duration = %#v", name, duration)
		}
	}
	duration := durations.Values["/runs/default/running"]
	if !duration.Running || duration.Seconds == nil || *duration.Seconds < 125 || *duration.Seconds > int64(time.Since(start.Time)/time.Second) {
		t.Fatalf("running workflow duration = %#v", duration)
	}
	for _, expected := range []string{
		`data-duration="/runs/default/succeeded" title="Workflow run duration">1m 15s</small>`,
		`data-duration="/runs/default/queued" title="Workflow run duration">—</small>`,
	} {
		if !strings.Contains(page.Body.String(), expected) {
			t.Errorf("run list is missing %q", expected)
		}
	}

	store.synced = func() bool { return false }
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/durations", nil))
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("unsynced run durations = %d", response.Code)
	}
}

func durationURLFromPage(t *testing.T, body string) string {
	t.Helper()
	_, rest, found := strings.Cut(body, "const durationURL = ")
	if !found {
		t.Fatal("page is missing the duration polling URL")
	}
	encoded, _, _ := strings.Cut(rest, ";")
	var value string
	if err := json.Unmarshal([]byte(encoded), &value); err != nil {
		t.Fatalf("decode duration polling URL: %v", err)
	}
	return value
}

func TestConsoleWorkflowRunListPollsDisplayedRuns(t *testing.T) {
	handler := newTestHandler(t, false)
	store := readyWorkflowRunStore(handler.logger)
	handler.workflowRuns = store
	now := time.Now().Truncate(time.Second)
	start := metav1.NewTime(now.Add(-125 * time.Second))
	completion := metav1.NewTime(start.Add(75 * time.Second))
	var active *actionsv1alpha1.WorkflowRun
	for index := 0; index < mainPageRunLimit; index++ {
		run := testWorkflowRun("default", fmt.Sprintf("run-%03d", index), now.Add(time.Duration(index)*time.Second))
		run.Status.StartTime = &start
		run.Status.CompletionTime = &completion
		run.Status.Conditions = []metav1.Condition{{Type: actionsv1alpha1.WorkflowRunConditionSucceeded, Status: metav1.ConditionTrue}}
		if index == 0 {
			run.Status.CompletionTime = nil
			run.Status.Conditions[0].Status = metav1.ConditionUnknown
			active = run
		}
		store.upsert(run)
	}
	page := httptest.NewRecorder()
	handler.ServeHTTP(page, httptest.NewRequest(http.MethodGet, "/", nil))
	pollURL := durationURLFromPage(t, page.Body.String())
	parsed, err := url.Parse(pollURL)
	if err != nil {
		t.Fatal(err)
	}
	if len(parsed.Query()["run"]) != mainPageRunLimit {
		t.Fatalf("polling URL does not select the displayed runs: %s", pollURL)
	}
	for index := 0; index < 2; index++ {
		store.upsert(testWorkflowRun("default", fmt.Sprintf("incoming-%d", index), now.Add(time.Hour)))
	}
	poll := func() durationsResponse {
		t.Helper()
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, pollURL, nil))
		var data durationsResponse
		if response.Code != http.StatusOK {
			t.Fatalf("durations response = %d, %s", response.Code, response.Body.String())
		}
		if err := json.Unmarshal(response.Body.Bytes(), &data); err != nil {
			t.Fatal(err)
		}
		if len(data.Values) != mainPageRunLimit {
			t.Fatalf("durations contain %d runs, want %d", len(data.Values), mainPageRunLimit)
		}
		if _, found := data.Values["/runs/default/incoming-0"]; found {
			t.Fatal("durations include a run that was not displayed")
		}
		if data.Values["/runs/default/run-001"].String() != "1m 15s" {
			t.Fatal("displayed completed run lost its duration after new runs arrived")
		}
		return data
	}
	data := poll()
	if duration := data.Values[runPath(active)]; !data.Active || !duration.Running || duration.Seconds == nil || *duration.Seconds < 125 {
		t.Fatalf("displayed active run lost its timing: %#v", duration)
	}
	active.Status.CompletionTime = &completion
	active.Status.Conditions[0].Status = metav1.ConditionTrue
	store.upsert(active)
	data = poll()
	if duration := data.Values[runPath(active)]; data.Active || duration.Running || duration.String() != "1m 15s" {
		t.Fatalf("displayed run did not receive its final duration: %#v", data)
	}
}

func TestConsoleWorkflowRunDurationSelection(t *testing.T) {
	handler := newTestHandler(t, false)
	for _, test := range []struct {
		name  string
		runs  []string
		code  int
		count int
	}{
		{name: "empty", code: http.StatusOK},
		{name: "selected", runs: []string{"default/ci"}, code: http.StatusOK, count: 1},
		{name: "missing", runs: []string{"default/missing"}, code: http.StatusOK},
		{name: "malformed", runs: []string{"default/ci/extra"}, code: http.StatusBadRequest},
		{name: "missing namespace", runs: []string{"ci"}, code: http.StatusBadRequest},
		{name: "too many", runs: strings.Split(strings.Repeat("default/ci,", mainPageRunLimit)+"default/ci", ","), code: http.StatusBadRequest},
	} {
		t.Run(test.name, func(t *testing.T) {
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/durations?"+url.Values{"run": test.runs}.Encode(), nil))
			if response.Code != test.code {
				t.Fatalf("status = %d, want %d: %s", response.Code, test.code, response.Body.String())
			}
			if test.code == http.StatusOK {
				var data durationsResponse
				if err := json.Unmarshal(response.Body.Bytes(), &data); err != nil {
					t.Fatal(err)
				}
				if len(data.Values) != test.count {
					t.Fatalf("received %d durations, want %d", len(data.Values), test.count)
				}
			}
		})
	}
}

func TestConsoleJobDurations(t *testing.T) {
	handler := newTestHandler(t, false)
	clusterClient := handler.client.(client.Client)
	ctx := context.Background()
	job := &actionsv1alpha1.WorkflowJob{}
	if err := clusterClient.Get(ctx, client.ObjectKey{Namespace: "default", Name: "build"}, job); err != nil {
		t.Fatal(err)
	}
	readDurations := func() durationsResponse {
		t.Helper()
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/runs/default/ci/durations", nil))
		if response.Code != http.StatusOK || response.Header().Get("Cache-Control") != "no-store" || response.Header().Get("Content-Type") != "application/json" {
			t.Fatalf("durations response = %d, %v, %s", response.Code, response.Header(), response.Body.String())
		}
		var result durationsResponse
		if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
			t.Fatal(err)
		}
		return result
	}
	queued := readDurations()
	if !queued.Active || len(queued.Values) != 2 || queued.Values["build"].Seconds != nil || queued.Values["build"].Running {
		t.Fatalf("queued job durations = %#v", queued)
	}

	start := metav1.NewTime(time.Now().Add(-125 * time.Second).Truncate(time.Second))
	job.Status.StartTime = &start
	if err := clusterClient.Update(ctx, job); err != nil {
		t.Fatal(err)
	}
	run := &actionsv1alpha1.WorkflowRun{}
	if err := clusterClient.Get(ctx, client.ObjectKey{Namespace: "default", Name: "ci"}, run); err != nil {
		t.Fatal(err)
	}
	run.Status.StartTime = &start
	if err := clusterClient.Update(ctx, run); err != nil {
		t.Fatal(err)
	}
	running := readDurations()
	duration := running.Values["build"]
	if !running.Active || !duration.Running || duration.Seconds == nil || *duration.Seconds < 125 || *duration.Seconds > 135 {
		t.Fatalf("running job duration = %#v", duration)
	}
	if runDuration := running.Values["/runs/default/ci"]; !runDuration.Running || runDuration.Seconds == nil || *runDuration.Seconds != *duration.Seconds {
		t.Fatalf("running workflow duration = %#v", runDuration)
	}
	for _, path := range []string{"/runs/default/ci", "/runs/default/ci/jobs/build"} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
		if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `data-duration="build" title="Job duration">2m `) || !strings.Contains(response.Body.String(), `"running":true`) {
			t.Fatalf("running job page %s = %d, %s", path, response.Code, response.Body.String())
		}
	}

	completion := metav1.NewTime(start.Add(61 * time.Second))
	job.Status.CompletionTime = &completion
	job.Status.Result = actionsv1alpha1.WorkflowJobResultSuccess
	if err := clusterClient.Update(ctx, job); err != nil {
		t.Fatal(err)
	}
	if err := clusterClient.Get(ctx, client.ObjectKey{Namespace: "default", Name: "ci"}, run); err != nil {
		t.Fatal(err)
	}
	run.Status.Conditions = []metav1.Condition{{Type: actionsv1alpha1.WorkflowRunConditionSucceeded, Status: metav1.ConditionTrue}}
	run.Status.CompletionTime = &completion
	if err := clusterClient.Update(ctx, run); err != nil {
		t.Fatal(err)
	}
	completed := readDurations()
	duration = completed.Values["build"]
	if completed.Active || duration.Running || duration.Seconds == nil || *duration.Seconds != 61 {
		t.Fatalf("completed job durations = %#v", completed)
	}
	if runDuration := completed.Values["/runs/default/ci"]; runDuration.Running || runDuration.String() != "1m 1s" {
		t.Fatalf("completed workflow duration = %#v", runDuration)
	}
	for path, count := range map[string]int{"/runs/default/ci": 1, "/runs/default/ci/jobs/build": 2} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
		if response.Code != http.StatusOK || strings.Count(response.Body.String(), `data-duration="build" title="Job duration">1m 1s</span>`) != count {
			t.Fatalf("completed job page %s = %d, %s", path, response.Code, response.Body.String())
		}
		if path == "/runs/default/ci" && !strings.Contains(response.Body.String(), `data-duration="/runs/default/ci" title="Workflow run duration">1m 1s</span>`) {
			t.Fatal("run summary is missing the completed duration")
		}
	}
}
