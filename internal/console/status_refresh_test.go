package console

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"strings"
	"testing"
	"time"

	actionsv1alpha1 "github.com/kelos-dev/open-actions/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestConsoleStatusRefresh(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("node is not installed")
	}
	for _, path := range []string{"/", "/runs/default/ci", "/runs/default/ci/jobs/build"} {
		t.Run(path, func(t *testing.T) {
			h := newTestHandler(t, false)
			cluster := h.client.(client.Client)
			ctx := context.Background()
			run := &actionsv1alpha1.WorkflowRun{}
			job := &actionsv1alpha1.WorkflowJob{}
			if err := cluster.Get(ctx, client.ObjectKey{Namespace: "default", Name: "ci"}, run); err != nil {
				t.Fatal(err)
			}
			if err := cluster.Get(ctx, client.ObjectKey{Namespace: "default", Name: "build"}, job); err != nil {
				t.Fatal(err)
			}
			store := readyWorkflowRunStore(h.logger)
			h.workflowRuns = store
			store.upsert(run)
			read := func(target string) string {
				t.Helper()
				response := httptest.NewRecorder()
				h.ServeHTTP(response, httptest.NewRequest(http.MethodGet, target, nil))
				if response.Code != http.StatusOK {
					t.Fatalf("GET %s: %d %s", target, response.Code, response.Body.String())
				}
				return response.Body.String()
			}
			type snapshot struct {
				HTML string            `json:"html"`
				Data durationsResponse `json:"data"`
			}
			var snapshots []snapshot
			snapshotPage := func(status string) {
				t.Helper()
				page := read(path)
				var data durationsResponse
				if err := json.Unmarshal([]byte(read(durationURLFromPage(t, page))), &data); err != nil {
					t.Fatal(err)
				}
				if got := data.Statuses[runPath(run)]; got.Label != status || got.Class != strings.ToLower(status) {
					t.Fatalf("run status = %#v, want %s", got, status)
				}
				if path != "/" && data.Statuses[job.Name].Label != status {
					t.Fatalf("job status = %#v, want %s", data.Statuses[job.Name], status)
				}
				snapshots = append(snapshots, snapshot{HTML: page, Data: data})
			}
			snapshotPage("Queued")
			start := metav1.NewTime(time.Now().Add(-time.Minute).Truncate(time.Second))
			run.Status.StartTime = &start
			run.Status.Conditions = []metav1.Condition{{Type: actionsv1alpha1.WorkflowRunConditionSucceeded, Status: metav1.ConditionUnknown}}
			job.Status.StartTime = &start
			job.Status.RunnerRef = &corev1.LocalObjectReference{Name: "runner"}
			update := func() {
				t.Helper()
				if err := cluster.Update(ctx, run); err != nil {
					t.Fatal(err)
				}
				if err := cluster.Update(ctx, job); err != nil {
					t.Fatal(err)
				}
				store.upsert(run)
			}
			update()
			snapshotPage("Running")
			completion := metav1.NewTime(start.Add(65 * time.Second))
			run.Status.CompletionTime = &completion
			run.Status.Conditions[0].Status = metav1.ConditionTrue
			job.Status.CompletionTime = &completion
			job.Status.Result = actionsv1alpha1.WorkflowJobResultSuccess
			update()
			snapshotPage("Succeeded")
			input, err := json.Marshal(snapshots)
			if err != nil {
				t.Fatal(err)
			}
			command := exec.Command(node, "testdata/status_refresh.cjs")
			command.Stdin = strings.NewReader(string(input))
			if output, err := command.CombinedOutput(); err != nil {
				t.Fatalf("status refresh: %v\n%s", err, output)
			}
		})
	}
}
