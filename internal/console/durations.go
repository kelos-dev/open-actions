package console

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	actionsv1alpha1 "github.com/kelos-dev/open-actions/api/v1alpha1"
	"github.com/kelos-dev/open-actions/internal/workflowrun"
	"github.com/kelos-dev/open-actions/internal/workflowstatus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

type executionDuration struct {
	Seconds *int64 `json:"seconds"`
	Running bool   `json:"running"`
}

func (d executionDuration) String() string {
	if d.Seconds == nil {
		return "—"
	}
	return formatDuration(*d.Seconds)
}

func formatDuration(seconds int64) string {
	if seconds >= 3600 {
		return fmt.Sprintf("%dh %dm %ds", seconds/3600, seconds%3600/60, seconds%60)
	}
	if seconds >= 60 {
		return fmt.Sprintf("%dm %ds", seconds/60, seconds%60)
	}
	return fmt.Sprintf("%ds", seconds)
}

func workflowJobDuration(job *actionsv1alpha1.WorkflowJob, now time.Time) executionDuration {
	return elapsedDuration(job.Status.StartTime, job.Status.CompletionTime, !workflowstatus.JobTerminal(job), now)
}

func elapsedDuration(start, completion *metav1.Time, active bool, now time.Time) executionDuration {
	if start == nil {
		return executionDuration{}
	}
	running := completion == nil && active
	end := now
	if completion != nil {
		end = completion.Time
	} else if !running {
		return executionDuration{}
	}
	seconds := max(int64(end.Sub(start.Time)/time.Second), 0)
	return executionDuration{Seconds: &seconds, Running: running}
}

type durationsResponse struct {
	Values map[string]executionDuration `json:"values"`
	Active bool                         `json:"active"`
}

func workflowRunDurations(run *actionsv1alpha1.WorkflowRun, jobs []effectiveWorkflowJob, now time.Time) durationsResponse {
	data := durationsResponse{Values: make(map[string]executionDuration, len(jobs)+1), Active: !workflowrun.Terminal(run)}
	data.Values[runPath(run)] = elapsedDuration(run.Status.StartTime, run.Status.CompletionTime, data.Active, now)
	for _, item := range jobs {
		data.Values[item.job.Name] = workflowJobDuration(&item.job, now)
		if !workflowstatus.JobTerminal(&item.job) && item.job.Status.CompletionTime == nil {
			data.Active = true
		}
	}
	return data
}

func (h *Handler) runDurations(writer http.ResponseWriter, request *http.Request, run *actionsv1alpha1.WorkflowRun) {
	jobs, err := h.effectiveWorkflowJobs(request.Context(), run)
	if err != nil {
		h.writeResolutionError(writer, request, err)
		return
	}
	writer.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(writer).Encode(workflowRunDurations(run, jobs, time.Now())); err != nil {
		h.logger.Error("failed to encode workflow run durations", "workflow_run", run.Name, "error", err)
	}
}

func (h *Handler) runListDurations(writer http.ResponseWriter, request *http.Request) {
	if !h.workflowRuns.Synced() {
		http.Error(writer, "Console run index is not ready", http.StatusServiceUnavailable)
		return
	}
	names := request.URL.Query()["run"]
	if len(names) > mainPageRunLimit {
		http.Error(writer, fmt.Sprintf("at most %d WorkflowRuns may be requested", mainPageRunLimit), http.StatusBadRequest)
		return
	}
	keys := make([]types.NamespacedName, 0, len(names))
	for _, value := range names {
		namespace, name, valid := splitNamespacedValue(value)
		if !valid {
			http.Error(writer, fmt.Sprintf("invalid WorkflowRun name %q", value), http.StatusBadRequest)
			return
		}
		keys = append(keys, types.NamespacedName{Namespace: namespace, Name: name})
	}
	writer.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(writer).Encode(workflowRunListDurations(h.workflowRuns.Find(keys))); err != nil {
		h.logger.Error("failed to encode workflow run list durations", "error", err)
	}
}

func workflowRunListDurations(runs []WorkflowRunSummary) durationsResponse {
	data := durationsResponse{Values: make(map[string]executionDuration, len(runs))}
	for _, run := range runs {
		data.Values[run.URL] = run.Duration
		data.Active = data.Active || run.Active
	}
	return data
}
