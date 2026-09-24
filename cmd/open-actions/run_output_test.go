package main

import (
	"bytes"
	"regexp"
	"strings"
	"testing"
	"time"

	actionsv1alpha1 "github.com/kelos-dev/open-actions/api/v1alpha1"
	"github.com/kelos-dev/open-actions/internal/workflowstatus"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var outputNow = time.Date(2026, 8, 10, 12, 30, 0, 0, time.UTC)

func TestStatusMarkerFollowsGitHubCLI(t *testing.T) {
	for status, want := range map[string]string{
		workflowstatus.Succeeded:        markerSucceeded,
		workflowstatus.Failed:           markerFailed,
		workflowstatus.TimedOut:         markerFailed,
		workflowstatus.Cancelled:        markerInactive,
		workflowstatus.Skipped:          markerInactive,
		workflowstatus.Running:          markerActive,
		workflowstatus.Queued:           markerActive,
		workflowstatus.Waiting:          markerActive,
		workflowstatus.Cancelling:       markerActive,
		workflowstatus.AwaitingApproval: markerActive,
	} {
		if marker := statusMarker(status); marker != want {
			t.Errorf("statusMarker(%q) = %q, want %q", status, marker, want)
		}
	}
}

func TestWriteWorkflowRunListRendersRunColumns(t *testing.T) {
	succeeded := outputTestRun("ci-abc123", "team-ci")
	succeeded.Status.Conditions = []metav1.Condition{{
		Type: actionsv1alpha1.WorkflowRunConditionSucceeded, Status: metav1.ConditionTrue, Reason: "JobsSucceeded",
	}}
	succeeded.Status.CompletionTime = &metav1.Time{Time: outputNow.Add(-20 * time.Minute)}
	queued := outputTestRun("ci-def456", "other-ci")
	queued.Status.StartTime = nil
	queued.Status.Conditions = nil
	queued.Spec.Source.GitHub.Revision.Ref = "refs/tags/v1.2.3"

	var stdout bytes.Buffer
	if err := writeWorkflowRunList(&stdout, []actionsv1alpha1.WorkflowRun{*succeeded, *queued}, false, outputNow); err != nil {
		t.Fatalf("writeWorkflowRunList error = %v", err)
	}
	want := []string{
		"STATUS       NAME       WORKFLOW  REPOSITORY    BRANCH  EVENT  ELAPSED  AGE",
		"✓ Succeeded  ci-abc123  CI        acme/example  main    push   10m      120m",
		"* Queued     ci-def456  CI        acme/example  v1.2.3  push   -        120m",
	}
	if lines := outputLines(stdout.String()); !equalLines(lines, want) {
		t.Fatalf("workflow run list =\n%s\nwant\n%s", strings.Join(lines, "\n"), strings.Join(want, "\n"))
	}
}

func TestWriteWorkflowRunListAddsNamespaceColumn(t *testing.T) {
	run := outputTestRun("ci-abc123", "team-ci")
	var stdout bytes.Buffer
	if err := writeWorkflowRunList(&stdout, []actionsv1alpha1.WorkflowRun{*run}, true, outputNow); err != nil {
		t.Fatalf("writeWorkflowRunList error = %v", err)
	}
	want := []string{
		"NAMESPACE  STATUS     NAME       WORKFLOW  REPOSITORY    BRANCH  EVENT  ELAPSED  AGE",
		"team-ci    * Running  ci-abc123  CI        acme/example  main    push   30m      120m",
	}
	if lines := outputLines(stdout.String()); !equalLines(lines, want) {
		t.Fatalf("workflow run list =\n%s\nwant\n%s", strings.Join(lines, "\n"), strings.Join(want, "\n"))
	}
}

func TestWriteWorkflowRunRendersHeadlineAndJobs(t *testing.T) {
	run := outputTestRun("ci-abc123", "team-ci")
	run.Spec.Rerun = &actionsv1alpha1.WorkflowRunRerun{Attempt: 2}
	build := outputTestJob("ci-build", "build", "Build")
	build.Status.Result = actionsv1alpha1.WorkflowJobResultSuccess
	build.Status.StartTime = &metav1.Time{Time: outputNow.Add(-70 * time.Second)}
	build.Status.CompletionTime = &metav1.Time{Time: outputNow.Add(-10 * time.Second)}
	build.Status.RunnerRef = &corev1.LocalObjectReference{Name: "linux-1"}
	unit := outputTestJob("ci-test", "test", "test")
	unit.Status.StartTime = &metav1.Time{Time: outputNow.Add(-10 * time.Second)}
	unit.Status.RunnerRef = &corev1.LocalObjectReference{Name: "linux-2"}
	pending := outputTestJob("ci-deploy", "deploy", "Deploy")

	var stdout bytes.Buffer
	jobs := []actionsv1alpha1.WorkflowJob{*build, *unit, *pending}
	if err := writeWorkflowRun(&stdout, run, jobs, outputNow); err != nil {
		t.Fatalf("writeWorkflowRun error = %v", err)
	}
	want := []string{
		"* main CI · ci-abc123",
		"Triggered via push 120m ago",
		"",
		"Namespace:   team-ci",
		"Repository:  acme/example",
		"Workflow:    .open-actions/workflows/ci.yaml",
		"Revision:    " + strings.Repeat("a", 40),
		"Attempt:     2",
		"Status:      Running",
		"Started:     2026-08-10T12:00:00Z",
		"Completed:   -",
		"Elapsed:     30m",
		"",
		"JOBS",
		"✓ Succeeded · Build (build) in 60s · WorkflowJob ci-build · runner linux-1",
		"* Running · test in 10s · WorkflowJob ci-test · runner linux-2",
		"* Queued · Deploy (deploy) · WorkflowJob ci-deploy",
	}
	if lines := outputLines(stdout.String()); !equalLines(lines, want) {
		t.Fatalf("workflow run =\n%s\nwant\n%s", strings.Join(lines, "\n"), strings.Join(want, "\n"))
	}
}

func TestWriteWorkflowRunReportsMissingJobsAndRevision(t *testing.T) {
	run := outputTestRun("ci-abc123", "team-ci")
	run.Spec.Source = actionsv1alpha1.WorkflowRunSource{Type: actionsv1alpha1.SourceTypeGitHub}
	run.Status.StartTime = nil
	run.Status.Conditions = nil

	var stdout bytes.Buffer
	if err := writeWorkflowRun(&stdout, run, nil, outputNow); err != nil {
		t.Fatalf("writeWorkflowRun error = %v", err)
	}
	lines := outputLines(stdout.String())
	if lines[0] != "* CI · ci-abc123" || lines[1] != "Triggered via - 120m ago" {
		t.Fatalf("workflow run headline = %q", strings.Join(lines[:2], "\n"))
	}
	for _, want := range []string{"Repository:  -", "Revision:    -", "Status:      Queued", "Elapsed:     -"} {
		if !containsLine(lines, want) {
			t.Fatalf("workflow run =\n%s\nwant line %q", strings.Join(lines, "\n"), want)
		}
	}
	if last := lines[len(lines)-1]; last != "No jobs have been created yet" {
		t.Fatalf("workflow run jobs = %q", last)
	}
}

func TestWorkflowRunBranchPrefersPullRequestHeadRef(t *testing.T) {
	run := outputTestRun("ci-abc123", "team-ci")
	run.Spec.Source.GitHub.Revision.HeadRef = "feature/login"
	if branch := workflowRunBranch(run); branch != "feature/login" {
		t.Fatalf("workflowRunBranch() = %q", branch)
	}
	run.Spec.Source.GitHub.Revision = actionsv1alpha1.GitRevision{}
	if branch := workflowRunBranch(run); branch != "-" {
		t.Fatalf("workflowRunBranch() without a ref = %q", branch)
	}
}

func outputTestRun(name, namespace string) *actionsv1alpha1.WorkflowRun {
	return &actionsv1alpha1.WorkflowRun{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: namespace,
			CreationTimestamp: metav1.NewTime(outputNow.Add(-2 * time.Hour)),
		},
		Spec: actionsv1alpha1.WorkflowRunSpec{
			WorkflowPath: ".open-actions/workflows/ci.yaml",
			Source: actionsv1alpha1.WorkflowRunSource{Type: actionsv1alpha1.SourceTypeGitHub, GitHub: &actionsv1alpha1.GitHubWorkflowRunSource{
				Repository: actionsv1alpha1.GitHubRepository{Owner: "acme", Name: "example"},
				Event:      actionsv1alpha1.GitHubEvent{Name: actionsv1alpha1.GitHubEventNamePush},
				Revision:   actionsv1alpha1.GitRevision{SHA: strings.Repeat("a", 40), Ref: "refs/heads/main"},
			}},
		},
		Status: actionsv1alpha1.WorkflowRunStatus{
			WorkflowName: "CI",
			StartTime:    &metav1.Time{Time: outputNow.Add(-30 * time.Minute)},
			Conditions: []metav1.Condition{{
				Type: actionsv1alpha1.WorkflowRunConditionSucceeded, Status: metav1.ConditionUnknown, Reason: "JobsRunning",
			}},
		},
	}
}

func outputTestJob(name, jobID, displayName string) *actionsv1alpha1.WorkflowJob {
	return &actionsv1alpha1.WorkflowJob{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "team-ci"},
		Spec:       actionsv1alpha1.WorkflowJobSpec{JobID: jobID, DisplayName: displayName},
	}
}

func outputLines(value string) []string {
	return strings.Split(strings.TrimRight(value, "\n"), "\n")
}

func equalLines(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for index := range got {
		if got[index] != want[index] {
			return false
		}
	}
	return true
}

func containsLine(lines []string, want string) bool {
	for _, line := range lines {
		if line == want {
			return true
		}
	}
	return false
}

func matchesLine(lines []string, pattern *regexp.Regexp) bool {
	for _, line := range lines {
		if pattern.MatchString(line) {
			return true
		}
	}
	return false
}
