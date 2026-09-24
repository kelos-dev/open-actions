package main

import (
	"fmt"
	"io"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	actionsv1alpha1 "github.com/kelos-dev/open-actions/api/v1alpha1"
	"github.com/kelos-dev/open-actions/internal/workflowstatus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/duration"
)

// Status markers follow the GitHub CLI: a completed success, a completed
// failure, a completed run that executed nothing, and work still in progress.
const (
	markerSucceeded = "✓"
	markerFailed    = "X"
	markerInactive  = "-"
	markerActive    = "*"
)

// clearScreen moves the cursor home and erases the screen so a watched run
// redraws in place.
const clearScreen = "\033[H\033[2J"

func statusMarker(status string) string {
	switch status {
	case workflowstatus.Succeeded:
		return markerSucceeded
	case workflowstatus.Failed, workflowstatus.TimedOut:
		return markerFailed
	case workflowstatus.Cancelled, workflowstatus.Skipped:
		return markerInactive
	default:
		return markerActive
	}
}

func markedStatus(status string) string {
	return statusMarker(status) + " " + status
}

// terminalWriter reports whether output goes to an interactive terminal.
func terminalWriter(writer io.Writer) bool {
	file, isFile := writer.(*os.File)
	if !isFile {
		return false
	}
	information, err := file.Stat()
	return err == nil && information.Mode()&os.ModeCharDevice != 0
}

func writeWorkflowRunList(writer io.Writer, runs []actionsv1alpha1.WorkflowRun, allNamespaces bool, now time.Time) error {
	table := tabwriter.NewWriter(writer, 0, 4, 2, ' ', 0)
	columns := []string{"STATUS", "NAME", "WORKFLOW", "REPOSITORY", "BRANCH", "EVENT", "ELAPSED", "AGE"}
	if allNamespaces {
		columns = append([]string{"NAMESPACE"}, columns...)
	}
	fmt.Fprintln(table, strings.Join(columns, "\t"))
	for index := range runs {
		run := &runs[index]
		values := []string{
			markedStatus(workflowstatus.Run(run)),
			tableCell(run.Name),
			tableCell(workflowRunName(run)),
			tableCell(workflowRunRepository(run)),
			tableCell(workflowRunBranch(run)),
			tableCell(workflowRunEvent(run)),
			workflowRunElapsed(run, now),
			workflowRunAge(run, now),
		}
		if allNamespaces {
			values = append([]string{tableCell(run.Namespace)}, values...)
		}
		fmt.Fprintln(table, strings.Join(values, "\t"))
	}
	return table.Flush()
}

func writeWorkflowRun(writer io.Writer, run *actionsv1alpha1.WorkflowRun, jobs []actionsv1alpha1.WorkflowJob, now time.Time) error {
	status := workflowstatus.Run(run)
	headline := []string{statusMarker(status)}
	if branch := workflowRunBranch(run); branch != "-" {
		headline = append(headline, tableCell(branch))
	}
	headline = append(headline, tableCell(workflowRunName(run)), "·", run.Name)
	fmt.Fprintln(writer, strings.Join(headline, " "))
	fmt.Fprintf(writer, "Triggered via %s %s ago\n\n", workflowRunEvent(run), workflowRunAge(run, now))

	table := tabwriter.NewWriter(writer, 0, 4, 2, ' ', 0)
	fmt.Fprintf(table, "Namespace:\t%s\n", run.Namespace)
	fmt.Fprintf(table, "Repository:\t%s\n", workflowRunRepository(run))
	fmt.Fprintf(table, "Workflow:\t%s\n", tableCell(run.Spec.WorkflowPath))
	fmt.Fprintf(table, "Revision:\t%s\n", workflowRunRevision(run))
	if run.Spec.Rerun != nil {
		fmt.Fprintf(table, "Attempt:\t%d\n", run.Spec.Rerun.Attempt)
	}
	fmt.Fprintf(table, "Status:\t%s\n", status)
	fmt.Fprintf(table, "Started:\t%s\n", optionalTime(run.Status.StartTime))
	fmt.Fprintf(table, "Completed:\t%s\n", optionalTime(run.Status.CompletionTime))
	fmt.Fprintf(table, "Elapsed:\t%s\n", workflowRunElapsed(run, now))
	if err := table.Flush(); err != nil {
		return err
	}

	fmt.Fprintln(writer, "\nJOBS")
	if len(jobs) == 0 {
		fmt.Fprintln(writer, "No jobs have been created yet")
		return nil
	}
	for index := range jobs {
		fmt.Fprintln(writer, workflowJobLine(&jobs[index], now))
	}
	return nil
}

// workflowJobLine describes one job as a GitHub CLI style line, naming the
// WorkflowJob so that `open-actions run logs --job` can select it.
func workflowJobLine(job *actionsv1alpha1.WorkflowJob, now time.Time) string {
	label := job.Spec.JobID
	if displayName := job.Spec.DisplayName; displayName != "" && displayName != job.Spec.JobID {
		label = fmt.Sprintf("%s (%s)", displayName, job.Spec.JobID)
	}
	line := markedStatus(workflowstatus.Job(job)) + " · " + tableCell(label)
	if elapsed := workflowJobElapsed(job, now); elapsed != "-" {
		line += " in " + elapsed
	}
	line += " · WorkflowJob " + job.Name
	if job.Status.RunnerRef != nil {
		line += " · runner " + job.Status.RunnerRef.Name
	}
	return line
}

func workflowRunName(run *actionsv1alpha1.WorkflowRun) string {
	if run.Status.WorkflowName != "" {
		return run.Status.WorkflowName
	}
	return run.Spec.WorkflowPath
}

func workflowRunRepository(run *actionsv1alpha1.WorkflowRun) string {
	if run.Spec.Source.GitHub == nil {
		return "-"
	}
	return run.Spec.Source.GitHub.Repository.Owner + "/" + run.Spec.Source.GitHub.Repository.Name
}

func workflowRunEvent(run *actionsv1alpha1.WorkflowRun) string {
	if run.Spec.Source.GitHub == nil {
		return "-"
	}
	event := string(run.Spec.Source.GitHub.Event.Name)
	if run.Spec.Source.GitHub.Event.Action != "" {
		event += "/" + run.Spec.Source.GitHub.Event.Action
	}
	return event
}

func workflowRunRevision(run *actionsv1alpha1.WorkflowRun) string {
	if run.Spec.Source.GitHub == nil {
		return "-"
	}
	return run.Spec.Source.GitHub.Revision.SHA
}

// workflowRunBranch returns the branch or tag name the run executed.
func workflowRunBranch(run *actionsv1alpha1.WorkflowRun) string {
	if run.Spec.Source.GitHub == nil {
		return "-"
	}
	revision := run.Spec.Source.GitHub.Revision
	ref := revision.Ref
	if revision.HeadRef != "" {
		ref = revision.HeadRef
	}
	for _, prefix := range []string{"refs/heads/", "refs/tags/"} {
		if value, found := strings.CutPrefix(ref, prefix); found {
			return value
		}
	}
	if ref == "" {
		return "-"
	}
	return ref
}

func workflowRunAge(run *actionsv1alpha1.WorkflowRun, now time.Time) string {
	if run.CreationTimestamp.IsZero() {
		return "-"
	}
	return duration.HumanDuration(nonNegative(now.Sub(run.CreationTimestamp.Time)))
}

func workflowRunElapsed(run *actionsv1alpha1.WorkflowRun, now time.Time) string {
	return elapsed(run.Status.StartTime, run.Status.CompletionTime, now)
}

func workflowJobElapsed(job *actionsv1alpha1.WorkflowJob, now time.Time) string {
	return elapsed(job.Status.StartTime, job.Status.CompletionTime, now)
}

// elapsed reports how long execution took, or how long it has been running
// when it has not completed yet.
func elapsed(start, completion *metav1.Time, now time.Time) string {
	if start == nil {
		return "-"
	}
	end := now
	if completion != nil {
		end = completion.Time
	}
	return duration.HumanDuration(nonNegative(end.Sub(start.Time)))
}

func nonNegative(value time.Duration) time.Duration {
	return max(value, 0)
}

func optionalTime(value *metav1.Time) string {
	if value == nil {
		return "-"
	}
	return value.UTC().Format(time.RFC3339)
}

func tableCell(value string) string {
	return strings.NewReplacer("\t", " ", "\r", " ", "\n", " ").Replace(value)
}
