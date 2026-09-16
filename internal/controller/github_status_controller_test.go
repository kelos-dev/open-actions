package controller

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	actionsv1alpha1 "github.com/kelos-dev/open-actions/api/v1alpha1"
	githubclient "github.com/kelos-dev/open-actions/internal/github"
	"github.com/kelos-dev/open-actions/internal/runner"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

func TestGitHubStatusEnabledOnlyForCommitScopedEvents(t *testing.T) {
	for _, test := range []struct {
		event actionsv1alpha1.GitHubEventName
		want  bool
	}{
		{event: actionsv1alpha1.GitHubEventNamePush, want: true},
		{event: actionsv1alpha1.GitHubEventNamePullRequest, want: true},
		{event: actionsv1alpha1.GitHubEventNameMergeGroup, want: true},
		{event: actionsv1alpha1.GitHubEventNameSchedule},
		{event: actionsv1alpha1.GitHubEventNameWorkflowDispatch},
		{event: actionsv1alpha1.GitHubEventNamePullRequestTarget},
	} {
		run := &actionsv1alpha1.WorkflowRun{
			ObjectMeta: metav1.ObjectMeta{UID: "run-uid"},
			Spec: actionsv1alpha1.WorkflowRunSpec{Source: actionsv1alpha1.WorkflowRunSource{
				Type:   actionsv1alpha1.SourceTypeGitHub,
				GitHub: &actionsv1alpha1.GitHubWorkflowRunSource{Event: actionsv1alpha1.GitHubEvent{Name: test.event}},
			}},
		}
		if got := githubStatusEnabled(run); got != test.want {
			t.Errorf("githubStatusEnabled(%q) = %t, want %t", test.event, got, test.want)
		}
	}
}

func TestGitHubWorkflowJobCommitStatusLifecycle(t *testing.T) {
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	privateKeyData := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(privateKey)})
	revision := strings.Repeat("b", 40)
	reports := []githubclient.CreateCommitStatusRequest{}
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch {
		case request.Method == http.MethodGet && request.URL.Path == "/app":
			fmt.Fprint(writer, `{"id":1,"slug":"open-actions"}`)
		case request.URL.Path == "/app/installations/2/access_tokens":
			body := struct {
				Permissions map[string]string `json:"permissions"`
			}{}
			if err := json.NewDecoder(request.Body).Decode(&body); err != nil || body.Permissions["statuses"] != "write" || len(body.Permissions) != 1 {
				http.Error(writer, "unexpected permissions", http.StatusBadRequest)
				return
			}
			fmt.Fprint(writer, `{"token":"statuses-token"}`)
		case request.Method == http.MethodGet && request.URL.Path == "/repos/acme/example/commits/"+revision+"/statuses":
			if request.URL.Query().Get("page") != "1" || request.URL.Query().Get("per_page") != "100" {
				http.Error(writer, "unexpected pagination", http.StatusBadRequest)
				return
			}
			statuses := make([]githubclient.CommitStatus, 0, len(reports))
			for index := len(reports) - 1; index >= 0; index-- {
				report := reports[index]
				status := githubclient.CommitStatus{ID: int64(index + 21), State: report.State, TargetURL: report.TargetURL, Description: report.Description, Context: report.Context}
				status.Creator.Login = "open-actions[bot]"
				statuses = append(statuses, status)
			}
			if err := json.NewEncoder(writer).Encode(statuses); err != nil {
				t.Fatal(err)
			}
		case request.Method == http.MethodPost && request.URL.Path == "/repos/acme/example/statuses/"+revision:
			body := githubclient.CreateCommitStatusRequest{}
			if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
				http.Error(writer, "unexpected status report", http.StatusBadRequest)
				return
			}
			reports = append(reports, body)
			fmt.Fprintf(writer, `{"id":%d,"state":%q}`, 20+len(reports), body.State)
		default:
			http.NotFound(writer, request)
		}
	}))
	defer server.Close()
	github, err := githubclient.NewClient(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	scheme := runtime.NewScheme()
	if err := actionsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	project := &actionsv1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: "project", Namespace: "default", UID: "project-uid"},
		Spec: actionsv1alpha1.ProjectSpec{Source: actionsv1alpha1.ProjectSource{Type: actionsv1alpha1.SourceTypeGitHub, GitHub: &actionsv1alpha1.GitHubAppConfiguration{
			AppID: 1, InstallationID: 2,
			PrivateKeySecretRef: corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "github"}, Key: "private-key"},
			WebhookSecretRef:    corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "github"}, Key: "webhook-secret"},
		}}},
	}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "github", Namespace: "default"}, Data: map[string][]byte{"private-key": privateKeyData}}
	run := &actionsv1alpha1.WorkflowRun{
		ObjectMeta: metav1.ObjectMeta{Name: "ci", Namespace: "default", UID: "run-uid", Labels: map[string]string{actionsv1alpha1.LabelWorkflowRunRootUID: "run-uid"}},
		Spec: actionsv1alpha1.WorkflowRunSpec{
			ProjectRef: corev1.LocalObjectReference{Name: project.Name}, WorkflowPath: ".open-actions/workflows/ci.yaml",
			Source: actionsv1alpha1.WorkflowRunSource{Type: actionsv1alpha1.SourceTypeGitHub, GitHub: &actionsv1alpha1.GitHubWorkflowRunSource{
				Repository: actionsv1alpha1.GitHubRepository{ID: 3, Owner: "acme", Name: "example"},
				Event:      actionsv1alpha1.GitHubEvent{Name: actionsv1alpha1.GitHubEventNamePush},
				Revision:   actionsv1alpha1.GitRevision{SHA: revision},
			}},
		},
		Status: actionsv1alpha1.WorkflowRunStatus{WorkflowName: "CI"},
	}
	build := &actionsv1alpha1.WorkflowJob{
		ObjectMeta: metav1.ObjectMeta{Name: "ci-build", Namespace: "default", UID: "build-uid", Labels: map[string]string{actionsv1alpha1.LabelWorkflowRunUID: "run-uid"}},
		Spec:       actionsv1alpha1.WorkflowJobSpec{WorkflowRunRef: corev1.LocalObjectReference{Name: run.Name}, JobID: "build", DisplayName: "Build", RunsOn: []string{"linux"}},
	}
	lint := &actionsv1alpha1.WorkflowJob{
		ObjectMeta: metav1.ObjectMeta{Name: "ci-lint", Namespace: "default", UID: "lint-uid", Labels: map[string]string{actionsv1alpha1.LabelWorkflowRunUID: "run-uid"}},
		Spec:       actionsv1alpha1.WorkflowJobSpec{WorkflowRunRef: corev1.LocalObjectReference{Name: run.Name}, JobID: "lint", DisplayName: "Lint", RunsOn: []string{"linux"}},
	}
	clusterClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&actionsv1alpha1.WorkflowRun{}, &actionsv1alpha1.WorkflowJob{}).WithObjects(project, secret, run, build, lint).Build()
	reconciler := &GitHubStatusReconciler{Client: clusterClient, APIReader: clusterClient, GitHub: github, ConsoleURL: "https://console.example"}
	if err := reconciler.reconcileGitHubStatuses(context.Background(), run); err != nil {
		t.Fatal(err)
	}
	if len(reports) != 2 {
		t.Fatalf("job status reports = %d, want 2", len(reports))
	}
	wantQueued := map[string]string{
		"Open Actions / CI / Build / build": "https://console.example/runs/default/ci/jobs/ci-build",
		"Open Actions / CI / Lint / lint":   "https://console.example/runs/default/ci/jobs/ci-lint",
	}
	for _, report := range reports {
		if report.State != "pending" || report.Description != "Queued" || report.TargetURL != wantQueued[report.Context] {
			t.Fatalf("queued job report = %#v", report)
		}
		delete(wantQueued, report.Context)
	}
	if len(wantQueued) != 0 {
		t.Fatalf("missing queued reports: %#v", wantQueued)
	}
	for _, job := range []*actionsv1alpha1.WorkflowJob{build, lint} {
		stored := &actionsv1alpha1.WorkflowJob{}
		if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(job), stored); err != nil {
			t.Fatal(err)
		}
		status := workflowJobCommitStatus(stored)
		if status == nil || status.State != actionsv1alpha1.GitHubCommitStatusStatePending || status.ReportDigest == "" {
			t.Fatalf("queued status for WorkflowJob %q = %#v", job.Name, status)
		}
	}
	listReader := &workflowRunAppearsReader{Reader: clusterClient}
	reconciler.APIReader = listReader
	if err := reconciler.reconcileGitHubStatuses(context.Background(), run); err != nil {
		t.Fatal(err)
	}
	if listReader.listCount != 0 {
		t.Fatalf("unchanged job statuses listed WorkflowRuns %d times", listReader.listCount)
	}
	reconciler.APIReader = clusterClient

	storedBuild := &actionsv1alpha1.WorkflowJob{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(build), storedBuild); err != nil {
		t.Fatal(err)
	}
	start := metav1.NewTime(time.Date(2026, time.September, 5, 12, 0, 0, 0, time.UTC))
	storedBuild.Status.StartTime = &start
	storedBuild.Status.RunnerRef = &corev1.LocalObjectReference{Name: "runner-1"}
	if err := clusterClient.Status().Update(context.Background(), storedBuild); err != nil {
		t.Fatal(err)
	}
	if err := reconciler.reconcileGitHubStatuses(context.Background(), run); err != nil {
		t.Fatal(err)
	}
	if len(reports) != 3 || reports[2].State != "pending" || reports[2].Description != "In progress" || reports[2].TargetURL != "https://console.example/runs/default/ci/jobs/ci-build" {
		t.Fatalf("running job report = %#v, reports = %d", reports[len(reports)-1], len(reports))
	}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(build), storedBuild); err != nil {
		t.Fatal(err)
	}
	completion := metav1.NewTime(start.Add(22 * time.Second))
	storedBuild.Status.CompletionTime = &completion
	storedBuild.Status.Result = actionsv1alpha1.WorkflowJobResultSuccess
	meta.SetStatusCondition(&storedBuild.Status.Conditions, metav1.Condition{Type: actionsv1alpha1.WorkflowJobConditionSucceeded, Status: metav1.ConditionTrue, Reason: "JobSucceeded", Message: "All steps passed"})
	if err := clusterClient.Status().Update(context.Background(), storedBuild); err != nil {
		t.Fatal(err)
	}
	if err := reconciler.reconcileGitHubStatuses(context.Background(), run); err != nil {
		t.Fatal(err)
	}
	if len(reports) != 4 || reports[3].State != "success" || reports[3].Description != "Successful in 22s" {
		t.Fatalf("completed job report = %#v, reports = %d", reports[len(reports)-1], len(reports))
	}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(build), storedBuild); err != nil {
		t.Fatal(err)
	}
	buildRequest := workflowJobCommitStatusRequest(reconciler.ConsoleURL, run, storedBuild, workflowJobCommitStatusReport(run, storedBuild))
	if status := workflowJobCommitStatus(storedBuild); status == nil || status.ReportDigest != commitStatusReportDigest(buildRequest) {
		t.Fatalf("stored completed job status = %#v, request = %#v", status, buildRequest)
	}
	storedBuild.Status.Source.GitHub.CommitStatus.ReportDigest = ""
	if err := clusterClient.Status().Update(context.Background(), storedBuild); err != nil {
		t.Fatal(err)
	}
	if err := reconciler.reconcileGitHubStatuses(context.Background(), run); err != nil {
		t.Fatal(err)
	}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(build), storedBuild); err != nil {
		t.Fatal(err)
	}
	if status := workflowJobCommitStatus(storedBuild); status == nil || status.ReportDigest != commitStatusReportDigest(buildRequest) || len(reports) != 4 {
		t.Fatalf("recovered completed job status = %#v, reports = %#v", status, reports)
	}

	storedLint := &actionsv1alpha1.WorkflowJob{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(lint), storedLint); err != nil {
		t.Fatal(err)
	}
	storedLint.Status.Source = nil
	if err := clusterClient.Status().Update(context.Background(), storedLint); err != nil {
		t.Fatal(err)
	}
	if err := reconciler.reconcileGitHubStatuses(context.Background(), run); err != nil {
		t.Fatal(err)
	}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(lint), storedLint); err != nil {
		t.Fatal(err)
	}
	if status := workflowJobCommitStatus(storedLint); status == nil || status.State != actionsv1alpha1.GitHubCommitStatusStatePending || status.ReportDigest == "" || len(reports) != 4 {
		t.Fatalf("recovered job status = %#v, reports = %#v", status, reports)
	}

	retry := &actionsv1alpha1.WorkflowRun{
		ObjectMeta: metav1.ObjectMeta{Name: "ci-attempt-2", Namespace: run.Namespace, UID: "retry-uid", Labels: map[string]string{actionsv1alpha1.LabelWorkflowRunRootUID: "run-uid"}},
		Spec:       *run.Spec.DeepCopy(),
	}
	retry.Spec.Rerun = &actionsv1alpha1.WorkflowRunRerun{
		OriginalRunRef: actionsv1alpha1.WorkflowRunReference{Name: run.Name, UID: run.UID},
		PreviousRunRef: actionsv1alpha1.WorkflowRunReference{Name: run.Name, UID: run.UID},
		Attempt:        2,
	}
	if err := clusterClient.Create(context.Background(), retry); err != nil {
		t.Fatal(err)
	}
	storedLint.Status.StartTime = &start
	if err := clusterClient.Status().Update(context.Background(), storedLint); err != nil {
		t.Fatal(err)
	}
	if err := reconciler.reconcileGitHubStatuses(context.Background(), run); err != nil {
		t.Fatal(err)
	}
	if len(reports) != 4 {
		t.Fatalf("older attempt updated a job status; reports = %#v", reports)
	}
	if err := clusterClient.Delete(context.Background(), retry); err != nil {
		t.Fatal(err)
	}
	sharedRun := run.DeepCopy()
	sharedRun.Name = "ci-pull-request"
	sharedRun.UID = "shared-run-uid"
	sharedRun.ResourceVersion = ""
	sharedRun.CreationTimestamp = metav1.NewTime(time.Date(2026, time.September, 5, 13, 0, 0, 0, time.UTC))
	sharedRun.Labels = map[string]string{
		actionsv1alpha1.LabelWorkflowRunRootUID: string(sharedRun.UID),
		actionsv1alpha1.LabelProjectUID:         string(project.UID),
	}
	sharedRun.Finalizers = []string{workflowRunGitHubStatusFinalizer}
	sharedRun.Spec.Source.GitHub.Event = actionsv1alpha1.GitHubEvent{
		Name: actionsv1alpha1.GitHubEventNamePullRequest,
		PullRequest: &actionsv1alpha1.GitHubPullRequest{
			Number:         42,
			HeadRepository: sharedRun.Spec.Source.GitHub.Repository,
		},
	}
	sharedRun.Spec.Source.GitHub.Revision.HeadSHA = revision
	sharedRun.Labels[actionsv1alpha1.LabelGitHubStatusKey] = githubStatusKey(project.UID, sharedRun)
	if err := clusterClient.Create(context.Background(), sharedRun); err != nil {
		t.Fatal(err)
	}
	sharedBuild := &actionsv1alpha1.WorkflowJob{
		ObjectMeta: metav1.ObjectMeta{Name: "ci-pull-request-build", Namespace: run.Namespace, UID: "shared-build-uid", Labels: map[string]string{actionsv1alpha1.LabelWorkflowRunUID: string(sharedRun.UID)}},
		Spec:       actionsv1alpha1.WorkflowJobSpec{WorkflowRunRef: corev1.LocalObjectReference{Name: sharedRun.Name}, JobID: "build", DisplayName: "Build", RunsOn: []string{"linux"}},
		Status:     actionsv1alpha1.WorkflowJobStatus{Result: actionsv1alpha1.WorkflowJobResultSuccess},
	}
	sharedBuildReport := workflowJobCommitStatusReport(sharedRun, sharedBuild)
	sharedBuildRequest := workflowJobCommitStatusRequest(reconciler.ConsoleURL, sharedRun, sharedBuild, sharedBuildReport)
	sharedBuild.Status.Source = &actionsv1alpha1.WorkflowJobSourceStatus{GitHub: &actionsv1alpha1.GitHubWorkflowJobStatus{CommitStatus: &actionsv1alpha1.GitHubCommitStatus{
		State: actionsv1alpha1.GitHubCommitStatusState(sharedBuildReport.State), ReportDigest: commitStatusReportDigest(sharedBuildRequest),
	}}}
	sharedLint := &actionsv1alpha1.WorkflowJob{
		ObjectMeta: metav1.ObjectMeta{Name: "ci-pull-request-lint", Namespace: run.Namespace, UID: "shared-lint-uid", Labels: map[string]string{actionsv1alpha1.LabelWorkflowRunUID: string(sharedRun.UID)}},
		Spec:       actionsv1alpha1.WorkflowJobSpec{WorkflowRunRef: corev1.LocalObjectReference{Name: sharedRun.Name}, JobID: "lint", DisplayName: "Lint", RunsOn: []string{"linux"}},
	}
	if err := clusterClient.Create(context.Background(), sharedBuild); err != nil {
		t.Fatal(err)
	}
	if err := clusterClient.Create(context.Background(), sharedLint); err != nil {
		t.Fatal(err)
	}
	if err := reconciler.reconcileGitHubStatuses(context.Background(), run); err != nil {
		t.Fatal(err)
	}
	if len(reports) != 4 {
		t.Fatalf("older shared-revision run updated a job status; reports = %#v", reports)
	}
	if err := reconciler.reconcileGitHubStatuses(context.Background(), sharedRun); err != nil {
		t.Fatal(err)
	}
	wantTakeover := map[string]string{
		"Open Actions / CI / Build / build": "https://console.example/runs/default/ci-pull-request/jobs/ci-pull-request-build",
		"Open Actions / CI / Lint / lint":   "https://console.example/runs/default/ci-pull-request/jobs/ci-pull-request-lint",
	}
	if len(reports) != 6 {
		t.Fatalf("newer shared-revision run reports = %#v", reports)
	}
	for _, report := range reports[4:] {
		if report.TargetURL != wantTakeover[report.Context] {
			t.Fatalf("takeover job report = %#v", report)
		}
		delete(wantTakeover, report.Context)
	}
	if len(wantTakeover) != 0 {
		t.Fatalf("missing takeover reports: %#v", wantTakeover)
	}

	if err := clusterClient.Delete(context.Background(), sharedRun); err != nil {
		t.Fatal(err)
	}
	deletingRun := &actionsv1alpha1.WorkflowRun{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(sharedRun), deletingRun); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.finalizeGitHubStatus(context.Background(), deletingRun); err != nil {
		t.Fatal(err)
	}
	if len(reports) != 7 {
		t.Fatalf("deleting WorkflowRun reports = %#v", reports)
	}
	if report := reports[6]; report.Context != "Open Actions / CI / Lint / lint" || report.State != "error" || report.Description != "Cancelled" || report.TargetURL != "https://console.example/runs/default/ci-pull-request/jobs/ci-pull-request-lint" {
		t.Fatalf("canceled job report = %#v", report)
	}
}

func TestGitHubWorkflowValidationFailureReporting(t *testing.T) {
	for _, test := range []struct {
		name, reason string
		event        actionsv1alpha1.GitHubEventName
		consoleURL   string
		wantReport   bool
	}{
		{name: "invalid workflow", reason: "WorkflowInvalid", event: actionsv1alpha1.GitHubEventNamePush, consoleURL: "https://console.example", wantReport: true},
		{name: "pull request head", reason: "WorkflowInvalid", event: actionsv1alpha1.GitHubEventNamePullRequest, consoleURL: "https://console.example", wantReport: true},
		{name: "invalid workflow without console", reason: "WorkflowInvalid", event: actionsv1alpha1.GitHubEventNamePush, wantReport: true},
		{name: "unrelated planning failure", reason: "ChildCreationFailed", event: actionsv1alpha1.GitHubEventNamePush},
		{name: "schedule", reason: "WorkflowInvalid", event: actionsv1alpha1.GitHubEventNameSchedule},
	} {
		t.Run(test.name, func(t *testing.T) {
			var reports []githubclient.CreateCommitStatusRequest
			unavailable := true
			var statusRevision string
			reporter, _, clusterClient, run, build, dependent := githubStatusTestControllers(t, func(writer http.ResponseWriter, request *http.Request) {
				if unavailable {
					http.Error(writer, "temporarily unavailable", http.StatusServiceUnavailable)
					return
				}
				statusRevision = strings.TrimPrefix(request.URL.Path, "/repos/acme/example/statuses/")
				report := githubclient.CreateCommitStatusRequest{}
				if err := json.NewDecoder(request.Body).Decode(&report); err != nil {
					t.Error(err)
				}
				reports = append(reports, report)
				fmt.Fprint(writer, `{"id":42}`)
			}, func() []githubclient.CommitStatus {
				statuses := make([]githubclient.CommitStatus, 0, len(reports))
				for index := len(reports) - 1; index >= 0; index-- {
					report := reports[index]
					status := githubclient.CommitStatus{ID: int64(index + 1), State: report.State, Context: report.Context, TargetURL: report.TargetURL, Description: report.Description}
					status.Creator.Login = "open-actions[bot]"
					statuses = append(statuses, status)
				}
				return statuses
			})
			reporter.ConsoleURL = test.consoleURL
			for _, job := range []*actionsv1alpha1.WorkflowJob{build, dependent} {
				if err := clusterClient.Delete(t.Context(), job); err != nil {
					t.Fatal(err)
				}
			}
			key := client.ObjectKeyFromObject(run)
			if err := clusterClient.Get(t.Context(), key, run); err != nil {
				t.Fatal(err)
			}
			run.Spec.Source.GitHub.Event.Name = test.event
			if test.event == actionsv1alpha1.GitHubEventNamePullRequest {
				run.Spec.Source.GitHub.Revision.HeadSHA = strings.Repeat("b", 40)
			}
			if err := clusterClient.Update(t.Context(), run); err != nil {
				t.Fatal(err)
			}
			completion := metav1.NewTime(time.Now().Truncate(time.Second))
			run.Status = actionsv1alpha1.WorkflowRunStatus{WorkflowName: run.Spec.WorkflowPath, CompletionTime: &completion, Conditions: []metav1.Condition{
				plannedCondition(metav1.ConditionFalse, test.reason),
				{Type: actionsv1alpha1.WorkflowRunConditionSucceeded, Status: metav1.ConditionFalse, Reason: test.reason},
			}}
			if err := clusterClient.Status().Update(t.Context(), run); err != nil {
				t.Fatal(err)
			}
			request := ctrl.Request{NamespacedName: key}
			_, err := reporter.Reconcile(t.Context(), request)
			if !test.wantReport {
				if err != nil || len(reports) != 0 {
					t.Fatalf("unexpected report: %#v, error = %v", reports, err)
				}
				return
			}
			if err == nil {
				t.Fatal("transient reporting failure did not request a retry")
			}
			if err := clusterClient.Get(t.Context(), key, run); err != nil {
				t.Fatal(err)
			}
			if workflowRunValidationStatus(run) != nil || !run.Status.CompletionTime.Equal(&completion) {
				t.Fatalf("status after reporting failure = %#v", run.Status)
			}
			unavailable = false
			for range 2 {
				if _, err := reporter.Reconcile(t.Context(), request); err != nil {
					t.Fatal(err)
				}
			}
			if len(reports) != 1 {
				t.Fatalf("reports = %#v, want exactly one", reports)
			}
			wantURL := ""
			if test.consoleURL != "" {
				wantURL = test.consoleURL + "/runs/default/ci"
			}
			report := reports[0]
			if report.State != "failure" || report.Description != "Workflow validation failed" || report.Context != "Open Actions validation / .open-actions/workflows/ci.yaml" || report.TargetURL != wantURL || statusRevision != githubStatusRevision(run.Spec.Source.GitHub) {
				t.Fatalf("validation failure report = %#v, revision = %q", report, statusRevision)
			}
			if err := clusterClient.Get(t.Context(), key, run); err != nil {
				t.Fatal(err)
			}
			status := workflowRunValidationStatus(run)
			if status == nil || status.State != actionsv1alpha1.GitHubCommitStatusStateFailure || status.ReportDigest != commitStatusReportDigest(report) {
				t.Fatalf("recorded workflow status = %#v", status)
			}

			later := run.DeepCopy()
			later.Name, later.UID, later.ResourceVersion = "ci-later", "later-uid", ""
			later.CreationTimestamp = metav1.Now()
			later.Labels = map[string]string{actionsv1alpha1.LabelWorkflowRunRootUID: string(later.UID)}
			later.Status = actionsv1alpha1.WorkflowRunStatus{WorkflowName: "CI", Conditions: []metav1.Condition{plannedCondition(metav1.ConditionTrue, "JobsPlanned")}}
			if err := clusterClient.Create(t.Context(), later); err != nil {
				t.Fatal(err)
			}
			job := &actionsv1alpha1.WorkflowJob{
				ObjectMeta: metav1.ObjectMeta{Name: "later-build", Namespace: later.Namespace, UID: "later-build-uid", Labels: map[string]string{actionsv1alpha1.LabelWorkflowRunUID: string(later.UID)}},
				Spec:       actionsv1alpha1.WorkflowJobSpec{WorkflowRunRef: corev1.LocalObjectReference{Name: later.Name}, JobID: "build"},
			}
			if err := clusterClient.Create(t.Context(), job); err != nil {
				t.Fatal(err)
			}
			for range 2 {
				if _, err := reporter.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(later)}); err != nil {
					t.Fatal(err)
				}
				if _, err := reporter.Reconcile(t.Context(), request); err != nil {
					t.Fatal(err)
				}
			}
			if len(reports) != 3 || reports[2].State != "pending" || reports[2].Context != "Open Actions / CI / build" || reports[1].State != "success" || reports[1].Context != report.Context || reports[1].Description != "Workflow validation passed" {
				t.Fatalf("validation failure was not superseded: %#v", reports)
			}
		})
	}
}

func TestGitHubValidationSuccessRequiresAnExistingAppReport(t *testing.T) {
	for _, test := range []struct {
		name, state, creator, targetURL string
		wantWrite, wantRecorded, retry  bool
	}{
		{name: "no validation history"},
		{name: "app failure", state: "failure", creator: "open-actions[bot]", wantWrite: true, wantRecorded: true, retry: true},
		{name: "another reporter failure", state: "failure", creator: "another-app[bot]"},
		{name: "app success for another run", state: "success", creator: "open-actions[bot]", targetURL: "https://console.example/runs/default/earlier"},
		{name: "recover accepted success", state: "success", creator: "open-actions[bot]", targetURL: "https://console.example/runs/default/ci", wantRecorded: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			const validationContext = "Open Actions validation / .open-actions/workflows/ci.yaml"
			var reports []githubclient.CreateCommitStatusRequest
			listCalls := 0
			failValidation := test.retry
			reporter, _, clusterClient, run, build, dependent := githubStatusTestControllers(t, func(writer http.ResponseWriter, request *http.Request) {
				var report githubclient.CreateCommitStatusRequest
				if err := json.NewDecoder(request.Body).Decode(&report); err != nil {
					t.Error(err)
				}
				if failValidation && report.Context == validationContext {
					http.Error(writer, "temporarily unavailable", http.StatusServiceUnavailable)
					return
				}
				reports = append(reports, report)
				fmt.Fprint(writer, `{"id":42}`)
			}, func() []githubclient.CommitStatus {
				listCalls++
				if test.state == "" {
					return []githubclient.CommitStatus{}
				}
				status := githubclient.CommitStatus{ID: 1, Context: validationContext, State: test.state, TargetURL: test.targetURL, Description: "Workflow validation passed"}
				status.Creator.Login = test.creator
				return []githubclient.CommitStatus{status}
			})
			if err := clusterClient.Get(t.Context(), client.ObjectKeyFromObject(run), run); err != nil {
				t.Fatal(err)
			}
			completion := metav1.NewTime(time.Now().Truncate(time.Second))
			run.Status.CompletionTime = &completion
			meta.SetStatusCondition(&run.Status.Conditions, metav1.Condition{Type: actionsv1alpha1.WorkflowRunConditionSucceeded, Status: metav1.ConditionTrue, Reason: "JobsSucceeded"})
			if err := clusterClient.Status().Update(t.Context(), run); err != nil {
				t.Fatal(err)
			}
			for _, job := range []*actionsv1alpha1.WorkflowJob{build, dependent} {
				if err := clusterClient.Get(t.Context(), client.ObjectKeyFromObject(job), job); err != nil {
					t.Fatal(err)
				}
				job.Status.Result = actionsv1alpha1.WorkflowJobResultSuccess
				if err := clusterClient.Status().Update(t.Context(), job); err != nil {
					t.Fatal(err)
				}
			}
			request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(run)}
			if test.retry {
				if _, err := reporter.Reconcile(t.Context(), request); err == nil || len(reports) != 0 {
					t.Fatalf("validation recovery failure did not preserve pending job reports: %#v, %v", reports, err)
				}
				failValidation = false
			}
			if _, err := reporter.Reconcile(t.Context(), request); err != nil {
				t.Fatal(err)
			}
			wantReports := 2
			if test.wantWrite {
				wantReports++
			}
			if len(reports) != wantReports {
				t.Fatalf("reports = %#v, want %d", reports, wantReports)
			}
			if test.wantWrite && (reports[0].Context != validationContext || reports[0].State != "success") {
				t.Fatalf("validation recovery = %#v", reports)
			}
			if err := clusterClient.Get(t.Context(), request.NamespacedName, run); err != nil {
				t.Fatal(err)
			}
			if status := workflowRunValidationStatus(run); (status != nil) != test.wantRecorded || (status != nil && status.State != actionsv1alpha1.GitHubCommitStatusStateSuccess) {
				t.Fatalf("recorded validation status = %#v", status)
			}
			calls := listCalls
			restarted := &GitHubStatusReconciler{Client: clusterClient, APIReader: clusterClient, GitHub: reporter.GitHub, ConsoleURL: reporter.ConsoleURL}
			if _, err := restarted.Reconcile(t.Context(), request); err != nil {
				t.Fatal(err)
			}
			if listCalls != calls || len(reports) != wantReports {
				t.Fatalf("completed run repeated reporting after restart: lists %d -> %d, reports %#v", calls, listCalls, reports)
			}
		})
	}
}

func TestWorkflowJobCommitStatusReportMapsLifecycle(t *testing.T) {
	completion := metav1.Now()
	tests := []struct {
		name        string
		configure   func(*actionsv1alpha1.WorkflowRun, *actionsv1alpha1.WorkflowJob)
		state       string
		description string
	}{
		{name: "queued", state: "pending", description: "Queued"},
		{name: "running", state: "pending", description: "In progress", configure: func(_ *actionsv1alpha1.WorkflowRun, job *actionsv1alpha1.WorkflowJob) {
			job.Status.StartTime = &completion
		}},
		{name: "success", state: "success", description: "Successful", configure: func(_ *actionsv1alpha1.WorkflowRun, job *actionsv1alpha1.WorkflowJob) {
			job.Status.Result = actionsv1alpha1.WorkflowJobResultSuccess
		}},
		{name: "failure", state: "failure", description: "Failing", configure: func(_ *actionsv1alpha1.WorkflowRun, job *actionsv1alpha1.WorkflowJob) {
			job.Status.Result = actionsv1alpha1.WorkflowJobResultFailure
		}},
		{name: "timeout", state: "error", description: "Timed out", configure: func(_ *actionsv1alpha1.WorkflowRun, job *actionsv1alpha1.WorkflowJob) {
			job.Status.Result = actionsv1alpha1.WorkflowJobResultFailure
			meta.SetStatusCondition(&job.Status.Conditions, metav1.Condition{Type: actionsv1alpha1.WorkflowJobConditionSucceeded, Status: metav1.ConditionFalse, Reason: "JobTimedOut"})
		}},
		{name: "skipped", state: "success", description: "Skipped", configure: func(_ *actionsv1alpha1.WorkflowRun, job *actionsv1alpha1.WorkflowJob) {
			job.Status.Result = actionsv1alpha1.WorkflowJobResultSkipped
		}},
		{name: "cancelled", state: "error", description: "Cancelled", configure: func(_ *actionsv1alpha1.WorkflowRun, job *actionsv1alpha1.WorkflowJob) {
			job.Status.Result = actionsv1alpha1.WorkflowJobResultCancelled
		}},
		{name: "deleting run", state: "error", description: "Cancelled", configure: func(run *actionsv1alpha1.WorkflowRun, _ *actionsv1alpha1.WorkflowJob) {
			run.DeletionTimestamp = &completion
		}},
		{name: "rerun", state: "pending", description: "Queued", configure: func(run *actionsv1alpha1.WorkflowRun, _ *actionsv1alpha1.WorkflowJob) {
			run.Spec.Rerun = &actionsv1alpha1.WorkflowRunRerun{Attempt: 2}
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			run := &actionsv1alpha1.WorkflowRun{}
			job := &actionsv1alpha1.WorkflowJob{Spec: actionsv1alpha1.WorkflowJobSpec{JobID: "build", DisplayName: "Build"}}
			if tt.configure != nil {
				tt.configure(run, job)
			}
			report := workflowJobCommitStatusReport(run, job)
			if report.State != tt.state || report.Description != tt.description {
				t.Fatalf("report = %#v, want state %q and description %q", report, tt.state, tt.description)
			}
		})
	}
}

func TestWorkflowJobCommitStatusReportIncludesDuration(t *testing.T) {
	start := metav1.NewTime(time.Date(2026, time.September, 7, 12, 0, 0, 0, time.UTC))
	for _, test := range []struct {
		name        string
		result      actionsv1alpha1.WorkflowJobResult
		reason      string
		duration    time.Duration
		state       string
		description string
	}{
		{name: "success seconds", result: actionsv1alpha1.WorkflowJobResultSuccess, duration: 22 * time.Second, state: "success", description: "Successful in 22s"},
		{name: "success minutes", result: actionsv1alpha1.WorkflowJobResultSuccess, duration: 3*time.Minute + 2*time.Second, state: "success", description: "Successful in 3m 2s"},
		{name: "success hours", result: actionsv1alpha1.WorkflowJobResultSuccess, duration: time.Hour + 2*time.Minute + 3*time.Second, state: "success", description: "Successful in 1h 2m 3s"},
		{name: "whole minute", result: actionsv1alpha1.WorkflowJobResultSuccess, duration: time.Minute, state: "success", description: "Successful in 1m"},
		{name: "zero duration", result: actionsv1alpha1.WorkflowJobResultSuccess, state: "success", description: "Successful in 0s"},
		{name: "fractional seconds", result: actionsv1alpha1.WorkflowJobResultSuccess, duration: 22900 * time.Millisecond, state: "success", description: "Successful in 22s"},
		{name: "failure", result: actionsv1alpha1.WorkflowJobResultFailure, duration: 8*time.Minute + 23*time.Second, state: "failure", description: "Failing after 8m 23s"},
		{name: "timeout", result: actionsv1alpha1.WorkflowJobResultFailure, reason: "JobTimedOut", duration: 6 * time.Hour, state: "error", description: "Timed out after 6h"},
		{name: "cancelled", result: actionsv1alpha1.WorkflowJobResultCancelled, duration: 22 * time.Second, state: "error", description: "Cancelled after 22s"},
		{name: "condition success", reason: "JobSucceeded", duration: 22 * time.Second, state: "success", description: "Successful in 22s"},
		{name: "condition cancellation", reason: "JobCancelled", duration: 22 * time.Second, state: "error", description: "Cancelled after 22s"},
	} {
		t.Run(test.name, func(t *testing.T) {
			completion := metav1.NewTime(start.Add(test.duration))
			conditionStatus := metav1.ConditionFalse
			if test.state == "success" {
				conditionStatus = metav1.ConditionTrue
			}
			job := &actionsv1alpha1.WorkflowJob{Status: actionsv1alpha1.WorkflowJobStatus{
				Result: test.result, StartTime: &start, CompletionTime: &completion,
				Conditions: []metav1.Condition{{
					Type: actionsv1alpha1.WorkflowJobConditionSucceeded, Status: conditionStatus,
					Reason: test.reason, Message: "A controller diagnostic",
				}},
			}}
			run := &actionsv1alpha1.WorkflowRun{}
			for attempt := int32(1); attempt <= 2; attempt++ {
				if attempt > 1 {
					run.Spec.Rerun = &actionsv1alpha1.WorkflowRunRerun{Attempt: attempt}
				}
				report := workflowJobCommitStatusReport(run, job)
				if report.State != test.state || report.Description != test.description {
					t.Fatalf("attempt %d report = %#v, want state %q and description %q", attempt, report, test.state, test.description)
				}
			}
		})
	}
}

func TestWorkflowJobCommitStatusReportWithoutExecutionDuration(t *testing.T) {
	start := metav1.NewTime(time.Date(2026, time.September, 7, 12, 0, 0, 0, time.UTC))
	completion := metav1.NewTime(start.Add(22 * time.Second))
	for _, test := range []struct {
		name       string
		start      *metav1.Time
		completion *metav1.Time
	}{
		{name: "missing start", completion: &completion},
		{name: "missing completion", start: &start},
		{name: "completion precedes start", start: &completion, completion: &start},
	} {
		t.Run(test.name, func(t *testing.T) {
			job := &actionsv1alpha1.WorkflowJob{Status: actionsv1alpha1.WorkflowJobStatus{
				Result: actionsv1alpha1.WorkflowJobResultSuccess, StartTime: test.start, CompletionTime: test.completion,
			}}
			if report := workflowJobCommitStatusReport(&actionsv1alpha1.WorkflowRun{}, job); report.State != "success" || report.Description != "Successful" {
				t.Fatalf("report without execution duration = %#v", report)
			}
		})
	}
}

func TestGitHubJobStatusContextIsStableAndBounded(t *testing.T) {
	run := &actionsv1alpha1.WorkflowRun{
		Spec:   actionsv1alpha1.WorkflowRunSpec{WorkflowPath: ".open-actions/workflows/ci.yaml"},
		Status: actionsv1alpha1.WorkflowRunStatus{WorkflowName: "CI"},
	}
	job := &actionsv1alpha1.WorkflowJob{Spec: actionsv1alpha1.WorkflowJobSpec{JobID: "build", DisplayName: "Build"}}
	if got, want := githubJobStatusContext(run, job), "Open Actions / CI / Build / build"; got != want {
		t.Fatalf("job context = %q, want %q", got, want)
	}
	retry := run.DeepCopy()
	retry.Spec.Rerun = &actionsv1alpha1.WorkflowRunRerun{Attempt: 2}
	if first, second := githubJobStatusContext(run, job), githubJobStatusContext(retry, job); first != second {
		t.Fatalf("rerun job contexts = %q and %q", first, second)
	}
	matrixFirst := job.DeepCopy()
	matrixFirst.Spec.JobID = "build-matrix-1"
	matrixFirst.Spec.DisplayName = "Build (os=ubuntu)"
	matrixFirst.Spec.Matrix = &actionsv1alpha1.WorkflowJobMatrix{LogicalJobID: "build", Values: map[string]string{"os": "ubuntu"}, JobIndex: 0, JobTotal: 2}
	matrixSecond := matrixFirst.DeepCopy()
	matrixSecond.Spec.JobID = "build-matrix-2"
	matrixSecond.Spec.Matrix.JobIndex = 1
	if first, second := githubJobStatusContext(run, matrixFirst), githubJobStatusContext(run, matrixSecond); first != second || first != "Open Actions / CI / Build (os=ubuntu) / build" {
		t.Fatalf("reordered matrix job contexts = %q and %q", first, second)
	}
	longFirst := job.DeepCopy()
	longFirst.Spec.JobID = "first"
	longFirst.Spec.DisplayName = strings.Repeat("한", 256)
	longSecond := longFirst.DeepCopy()
	longSecond.Spec.JobID = "second"
	if first, second := githubJobStatusContext(run, longFirst), githubJobStatusContext(run, longSecond); utf8.RuneCountInString(first) != maxCommitStatusContextRunes || first == second {
		t.Fatalf("bounded job status contexts = %q and %q", first, second)
	}
	uppercaseJob := job.DeepCopy()
	uppercaseJob.Spec.JobID = "Build"
	if strings.EqualFold(githubJobStatusContext(run, job), githubJobStatusContext(run, uppercaseJob)) {
		t.Fatalf("case-distinct job IDs produced equivalent contexts %q and %q", githubJobStatusContext(run, job), githubJobStatusContext(run, uppercaseJob))
	}
	uppercasePath := run.DeepCopy()
	uppercasePath.Spec.WorkflowPath = ".open-actions/workflows/CI.yaml"
	uppercaseContext := "Open Actions / CI / Build / build"
	uppercaseDigest := sha256.Sum256([]byte(uppercaseContext))
	wantUppercaseContext := fmt.Sprintf("%s / %x", uppercaseContext, uppercaseDigest[:8])
	if got := githubJobStatusContext(uppercasePath, job); strings.EqualFold(githubJobStatusContext(run, job), got) || got != wantUppercaseContext {
		t.Fatalf("uppercase workflow path context = %q, want %q", got, wantUppercaseContext)
	}
}

func TestGitHubJobStatusWarnsAboutCaseInsensitiveContextCollision(t *testing.T) {
	revision := strings.Repeat("c", 40)
	newRun := func(name, path, workflowName string) *actionsv1alpha1.WorkflowRun {
		return &actionsv1alpha1.WorkflowRun{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", UID: types.UID(name + "-uid")},
			Spec: actionsv1alpha1.WorkflowRunSpec{
				ProjectRef: corev1.LocalObjectReference{Name: "project"}, WorkflowPath: path,
				Source: actionsv1alpha1.WorkflowRunSource{Type: actionsv1alpha1.SourceTypeGitHub, GitHub: &actionsv1alpha1.GitHubWorkflowRunSource{
					Repository: actionsv1alpha1.GitHubRepository{ID: 3, Owner: "acme", Name: "example"},
					Event:      actionsv1alpha1.GitHubEvent{Name: actionsv1alpha1.GitHubEventNamePush},
					Revision:   actionsv1alpha1.GitRevision{SHA: revision},
				}},
			},
			Status: actionsv1alpha1.WorkflowRunStatus{WorkflowName: workflowName},
		}
	}
	run := newRun("ci", ".open-actions/workflows/ci.yaml", "CI")
	collision := newRun("release", ".open-actions/workflows/release.yaml", "ci")
	job := actionsv1alpha1.WorkflowJob{
		ObjectMeta: metav1.ObjectMeta{Name: "ci-build", Namespace: "default", UID: "build-uid", Labels: map[string]string{actionsv1alpha1.LabelWorkflowRunUID: string(run.UID)}},
		Spec:       actionsv1alpha1.WorkflowJobSpec{WorkflowRunRef: corev1.LocalObjectReference{Name: run.Name}, JobID: "build"},
	}
	collisionJob := &actionsv1alpha1.WorkflowJob{
		ObjectMeta: metav1.ObjectMeta{Name: "release-build", Namespace: "default", UID: "release-build-uid", Labels: map[string]string{actionsv1alpha1.LabelWorkflowRunUID: string(collision.UID)}},
		Spec:       actionsv1alpha1.WorkflowJobSpec{WorkflowRunRef: corev1.LocalObjectReference{Name: collision.Name}, JobID: "build"},
	}
	scheme := runtime.NewScheme()
	if err := actionsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	clusterClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(run, collision, collisionJob).Build()
	recorder := events.NewFakeRecorder(1)
	reconciler := &GitHubStatusReconciler{APIReader: clusterClient, GitHub: &githubclient.Client{}, Recorder: recorder}

	if err := reconciler.warnGitHubJobStatusContextCollision(context.Background(), run, []actionsv1alpha1.WorkflowJob{job}); err != nil {
		t.Fatal(err)
	}
	requireEvent(t, recorder, `Warning GitHubStatusContextCollision GitHub job status context "Open Actions / CI / build" also identifies WorkflowJob "release-build" from workflow ".open-actions/workflows/release.yaml"; workflow and job names that report the same commit must produce unique contexts ignoring case`)
}

func TestGitHubStatusOwnershipUsesNewestRunWithinProjectBoundary(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := actionsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	project := &actionsv1alpha1.Project{ObjectMeta: metav1.ObjectMeta{Name: "project", Namespace: "team-a", UID: "project-uid"}}
	older := &actionsv1alpha1.WorkflowRun{
		ObjectMeta: metav1.ObjectMeta{Name: "older", Namespace: "team-a", UID: "older-uid", CreationTimestamp: metav1.NewTime(time.Unix(100, 0))},
		Spec: actionsv1alpha1.WorkflowRunSpec{
			ProjectRef:   corev1.LocalObjectReference{Name: "project"},
			WorkflowPath: ".open-actions/workflows/ci.yaml",
			Source: actionsv1alpha1.WorkflowRunSource{Type: actionsv1alpha1.SourceTypeGitHub, GitHub: &actionsv1alpha1.GitHubWorkflowRunSource{
				Repository: actionsv1alpha1.GitHubRepository{ID: 3, Owner: "acme", Name: "example"},
				Event:      actionsv1alpha1.GitHubEvent{Name: actionsv1alpha1.GitHubEventNamePush},
				Revision:   actionsv1alpha1.GitRevision{SHA: strings.Repeat("a", 40)},
			}},
		},
	}
	newer := older.DeepCopy()
	newer.Name = "newer"
	newer.UID = "newer-uid"
	newer.CreationTimestamp = metav1.NewTime(time.Unix(200, 0))
	statusKey := githubStatusKey("project-uid", older)
	older.Labels = map[string]string{actionsv1alpha1.LabelProjectUID: "project-uid", actionsv1alpha1.LabelGitHubStatusKey: statusKey}
	newer.Labels = map[string]string{actionsv1alpha1.LabelProjectUID: "project-uid", actionsv1alpha1.LabelGitHubStatusKey: statusKey}
	otherNamespace := newer.DeepCopy()
	otherNamespace.Name = "other-namespace"
	otherNamespace.Namespace = "team-b"
	otherNamespace.UID = "other-namespace-uid"
	otherNamespace.CreationTimestamp = metav1.NewTime(time.Unix(300, 0))
	otherProject := newer.DeepCopy()
	otherProject.Name = "other-project"
	otherProject.UID = "other-project-uid"
	otherProject.CreationTimestamp = metav1.NewTime(time.Unix(400, 0))
	otherProject.Spec.ProjectRef.Name = "other-project"
	clusterClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(project, older, newer, otherNamespace, otherProject).Build()
	reconciler := &GitHubStatusReconciler{Client: clusterClient, APIReader: clusterClient}

	current, _, err := reconciler.githubStatusCurrentOwner(context.Background(), older, project, statusKey)
	if err != nil {
		t.Fatal(err)
	}
	if current {
		t.Fatal("older run retained ownership of the shared status context")
	}
	current, leaseToken, err := reconciler.githubStatusCurrentOwner(context.Background(), newer, project, statusKey)
	if err != nil {
		t.Fatal(err)
	}
	if !current {
		t.Fatal("a run outside the Project boundary suppressed the current owner")
	}
	if err := reconciler.releaseGitHubStatusLease(context.Background(), newer.Namespace, statusKey, leaseToken); err != nil {
		t.Fatal(err)
	}
	if err := clusterClient.Delete(context.Background(), newer); err != nil {
		t.Fatal(err)
	}
	current, leaseToken, err = reconciler.githubStatusCurrentOwner(context.Background(), older, project, statusKey)
	if err != nil {
		t.Fatal(err)
	}
	if !current {
		t.Fatal("surviving run did not replace the deleted status owner")
	}
	if err := reconciler.releaseGitHubStatusLease(context.Background(), older.Namespace, statusKey, leaseToken); err != nil {
		t.Fatal(err)
	}
	appearing := newer.DeepCopy()
	appearing.Name = "appearing"
	appearing.UID = "appearing-uid"
	recordingClient := &recordingDeleteClient{Client: clusterClient}
	reconciler.Client = recordingClient
	reconciler.APIReader = &workflowRunAppearsReader{Reader: clusterClient, run: *appearing}
	if err := reconciler.releaseGitHubStatusOwnershipIfUnused(context.Background(), older); err != nil {
		t.Fatal(err)
	}
	if recordingClient.deleteOptions != nil {
		t.Fatal("status owner was deleted after a matching run appeared")
	}
	reconciler.Client = clusterClient
	reconciler.APIReader = clusterClient
	if err := clusterClient.Delete(context.Background(), older); err != nil {
		t.Fatal(err)
	}
	recordingClient = &recordingDeleteClient{Client: clusterClient}
	reconciler.Client = recordingClient
	if err := reconciler.releaseGitHubStatusOwnershipIfUnused(context.Background(), older); err != nil {
		t.Fatal(err)
	}
	if recordingClient.deleteOptions == nil || recordingClient.deleteOptions.Preconditions == nil || recordingClient.deleteOptions.Preconditions.ResourceVersion == nil {
		t.Fatalf("status owner delete options = %#v", recordingClient.deleteOptions)
	}
	owner := &corev1.ConfigMap{}
	if err := clusterClient.Get(context.Background(), client.ObjectKey{Namespace: older.Namespace, Name: githubStatusOwnerPrefix + statusKey}, owner); !apierrors.IsNotFound(err) {
		t.Fatalf("status owner cleanup error = %v", err)
	}
}

func TestGitHubStatusOwnershipLeaseSerializesReporters(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := actionsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	project := &actionsv1alpha1.Project{ObjectMeta: metav1.ObjectMeta{Name: "project", Namespace: "default", UID: "project-uid"}}
	older := &actionsv1alpha1.WorkflowRun{ObjectMeta: metav1.ObjectMeta{Name: "older", Namespace: project.Namespace, UID: "older-uid", CreationTimestamp: metav1.NewTime(time.Unix(100, 0))}}
	older.Spec.Rerun = nil
	newer := older.DeepCopy()
	newer.Name = "newer"
	newer.UID = "newer-uid"
	newer.CreationTimestamp = metav1.NewTime(time.Unix(200, 0))
	clusterClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(project).Build()
	reconciler := &GitHubStatusReconciler{Client: clusterClient, APIReader: clusterClient}

	current, olderLease, err := reconciler.claimGitHubStatusOwnership(context.Background(), older, project, "status-key")
	if err != nil || !current || olderLease == "" {
		t.Fatalf("older claim = %t, %q, %v", current, olderLease, err)
	}
	if _, _, err := reconciler.claimGitHubStatusOwnership(context.Background(), newer, project, "status-key"); !apierrors.IsConflict(err) {
		t.Fatalf("concurrent newer claim error = %v", err)
	}
	if err := reconciler.releaseGitHubStatusLease(context.Background(), project.Namespace, "status-key", olderLease); err != nil {
		t.Fatal(err)
	}
	current, newerLease, err := reconciler.claimGitHubStatusOwnership(context.Background(), newer, project, "status-key")
	if err != nil || !current || newerLease == "" {
		t.Fatalf("newer claim = %t, %q, %v", current, newerLease, err)
	}
}

func TestCanceledWorkflowRunRetainsGitHubReportFinalizerWhenReportingFails(t *testing.T) {
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	privateKeyData := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(privateKey)})
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		http.Error(writer, "GitHub unavailable", http.StatusServiceUnavailable)
	}))
	defer server.Close()
	github, err := githubclient.NewClient(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	scheme := runtime.NewScheme()
	if err := actionsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	project := &actionsv1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: "project", Namespace: "default", UID: "project-uid"},
		Spec: actionsv1alpha1.ProjectSpec{Source: actionsv1alpha1.ProjectSource{Type: actionsv1alpha1.SourceTypeGitHub, GitHub: &actionsv1alpha1.GitHubAppConfiguration{
			AppID: 1, InstallationID: 2,
			PrivateKeySecretRef: corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "github"}, Key: "private-key"},
		}}},
	}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "github", Namespace: "default"}, Data: map[string][]byte{"private-key": privateKeyData}}
	deletionTime := metav1.Now()
	run := &actionsv1alpha1.WorkflowRun{
		ObjectMeta: metav1.ObjectMeta{Name: "canceling", Namespace: "default", UID: types.UID("run-uid"), DeletionTimestamp: &deletionTime, Finalizers: []string{workflowRunGitHubStatusFinalizer}},
		Spec: actionsv1alpha1.WorkflowRunSpec{
			ProjectRef: corev1.LocalObjectReference{Name: project.Name}, WorkflowPath: ".open-actions/workflows/ci.yaml",
			Source: actionsv1alpha1.WorkflowRunSource{Type: actionsv1alpha1.SourceTypeGitHub, GitHub: &actionsv1alpha1.GitHubWorkflowRunSource{
				Repository: actionsv1alpha1.GitHubRepository{ID: 3, Owner: "acme", Name: "example"},
				Event:      actionsv1alpha1.GitHubEvent{Name: actionsv1alpha1.GitHubEventNamePush},
				Revision:   actionsv1alpha1.GitRevision{SHA: strings.Repeat("a", 40)},
			}},
		},
		Status: actionsv1alpha1.WorkflowRunStatus{WorkflowName: "CI"},
	}
	job := &actionsv1alpha1.WorkflowJob{
		ObjectMeta: metav1.ObjectMeta{Name: "canceling-build", Namespace: run.Namespace, UID: "job-uid", Labels: map[string]string{actionsv1alpha1.LabelWorkflowRunUID: string(run.UID)}},
		Spec:       actionsv1alpha1.WorkflowJobSpec{WorkflowRunRef: corev1.LocalObjectReference{Name: run.Name}, JobID: "build"},
	}
	clusterClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&actionsv1alpha1.WorkflowRun{}, &actionsv1alpha1.WorkflowJob{}).WithObjects(project, secret, run, job).Build()
	reconciler := &GitHubStatusReconciler{Client: clusterClient, APIReader: clusterClient, GitHub: github}
	if _, err := reconciler.finalizeGitHubStatus(context.Background(), run); err == nil {
		t.Fatal("terminal GitHub status reporting failure did not request a retry")
	}
	stored := &actionsv1alpha1.WorkflowRun{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(run), stored); err != nil {
		t.Fatal(err)
	}
	if !controllerutil.ContainsFinalizer(stored, workflowRunGitHubStatusFinalizer) {
		t.Fatal("GitHub report finalizer was removed after a transient reporting failure")
	}
}

func TestCanceledWorkflowRunRemovesGitHubReportFinalizerWhenProjectIsGone(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := actionsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	github, err := githubclient.NewClient("https://api.github.test", nil)
	if err != nil {
		t.Fatal(err)
	}
	deletionTime := metav1.Now()
	run := &actionsv1alpha1.WorkflowRun{
		ObjectMeta: metav1.ObjectMeta{Name: "canceling", Namespace: "default", UID: types.UID("run-uid"), DeletionTimestamp: &deletionTime, Finalizers: []string{workflowRunGitHubStatusFinalizer}},
		Spec: actionsv1alpha1.WorkflowRunSpec{
			ProjectRef: corev1.LocalObjectReference{Name: "deleted-project"}, WorkflowPath: ".open-actions/workflows/ci.yaml",
			Source: actionsv1alpha1.WorkflowRunSource{Type: actionsv1alpha1.SourceTypeGitHub, GitHub: &actionsv1alpha1.GitHubWorkflowRunSource{
				Repository: actionsv1alpha1.GitHubRepository{ID: 3, Owner: "acme", Name: "example"},
				Event:      actionsv1alpha1.GitHubEvent{Name: actionsv1alpha1.GitHubEventNamePush},
				Revision:   actionsv1alpha1.GitRevision{SHA: strings.Repeat("a", 40)},
			}},
		},
		Status: actionsv1alpha1.WorkflowRunStatus{WorkflowName: "CI"},
	}
	job := &actionsv1alpha1.WorkflowJob{
		ObjectMeta: metav1.ObjectMeta{Name: "canceling-build", Namespace: run.Namespace, UID: "job-uid", Labels: map[string]string{actionsv1alpha1.LabelWorkflowRunUID: string(run.UID)}},
		Spec:       actionsv1alpha1.WorkflowJobSpec{WorkflowRunRef: corev1.LocalObjectReference{Name: run.Name}, JobID: "build"},
	}
	clusterClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(run, job).Build()
	reconciler := &GitHubStatusReconciler{Client: clusterClient, APIReader: clusterClient, GitHub: github}
	if _, err := reconciler.finalizeGitHubStatus(context.Background(), run); err != nil {
		t.Fatal(err)
	}
	stored := &actionsv1alpha1.WorkflowRun{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(run), stored); client.IgnoreNotFound(err) != nil {
		t.Fatal(err)
	} else if err == nil && controllerutil.ContainsFinalizer(stored, workflowRunGitHubStatusFinalizer) {
		t.Fatal("GitHub report finalizer blocked WorkflowRun deletion")
	}
}

func TestCanceledWorkflowRunRemovesGitHubStatusOwnershipWhenCredentialsAreGone(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := actionsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	github, err := githubclient.NewClient("https://api.github.test", nil)
	if err != nil {
		t.Fatal(err)
	}
	project := &actionsv1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: "project", Namespace: "default", UID: "project-uid"},
		Spec: actionsv1alpha1.ProjectSpec{Source: actionsv1alpha1.ProjectSource{Type: actionsv1alpha1.SourceTypeGitHub, GitHub: &actionsv1alpha1.GitHubAppConfiguration{
			AppID: 1, InstallationID: 2,
			PrivateKeySecretRef: corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "missing"}, Key: "private-key"},
		}}},
	}
	deletionTime := metav1.Now()
	run := &actionsv1alpha1.WorkflowRun{
		ObjectMeta: metav1.ObjectMeta{Name: "canceling", Namespace: "default", UID: "run-uid", DeletionTimestamp: &deletionTime, Finalizers: []string{workflowRunGitHubStatusFinalizer}},
		Spec: actionsv1alpha1.WorkflowRunSpec{
			ProjectRef: corev1.LocalObjectReference{Name: project.Name}, WorkflowPath: ".open-actions/workflows/ci.yaml",
			Source: actionsv1alpha1.WorkflowRunSource{Type: actionsv1alpha1.SourceTypeGitHub, GitHub: &actionsv1alpha1.GitHubWorkflowRunSource{
				Repository: actionsv1alpha1.GitHubRepository{ID: 3, Owner: "acme", Name: "example"},
				Event:      actionsv1alpha1.GitHubEvent{Name: actionsv1alpha1.GitHubEventNamePush},
				Revision:   actionsv1alpha1.GitRevision{SHA: strings.Repeat("a", 40)},
			}},
		},
		Status: actionsv1alpha1.WorkflowRunStatus{WorkflowName: "CI"},
	}
	statusKey := githubStatusKey(project.UID, run)
	job := &actionsv1alpha1.WorkflowJob{
		ObjectMeta: metav1.ObjectMeta{Name: "canceling-build", Namespace: run.Namespace, UID: "job-uid", Labels: map[string]string{actionsv1alpha1.LabelWorkflowRunUID: string(run.UID)}},
		Spec:       actionsv1alpha1.WorkflowJobSpec{WorkflowRunRef: corev1.LocalObjectReference{Name: run.Name}, JobID: "build"},
	}
	clusterClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(project, run, job).Build()
	reconciler := &GitHubStatusReconciler{Client: clusterClient, APIReader: clusterClient, GitHub: github}
	if _, err := reconciler.finalizeGitHubStatus(context.Background(), run); err != nil {
		t.Fatal(err)
	}
	owner := &corev1.ConfigMap{}
	err = clusterClient.Get(context.Background(), client.ObjectKey{Namespace: run.Namespace, Name: githubStatusOwnerPrefix + statusKey}, owner)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("GitHub status ownership cleanup error = %v", err)
	}
}

func TestWorkflowRunProgressesDuringGitHubStatusRequest(t *testing.T) {
	for _, cancelRun := range []bool{false, true} {
		t.Run(fmt.Sprintf("cancel=%t", cancelRun), func(t *testing.T) {
			started := make(chan struct{})
			release := make(chan struct{})
			var startedOnce, releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			defer unblock()
			reporter, workflow, clusterClient, run, build, dependent := githubStatusTestControllers(t, func(writer http.ResponseWriter, request *http.Request) {
				startedOnce.Do(func() { close(started) })
				select {
				case <-release:
					fmt.Fprint(writer, `{"id":1}`)
				case <-request.Context().Done():
				}
			})
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(run)}
			reportDone := make(chan error, 1)
			go func() {
				_, err := reporter.Reconcile(ctx, request)
				reportDone <- err
			}()
			select {
			case <-started:
			case err := <-reportDone:
				t.Fatalf("reporter returned before publishing a status: %v", err)
			case <-ctx.Done():
				t.Fatal("reporter did not reach the GitHub status request")
			}

			build.Status.Result = actionsv1alpha1.WorkflowJobResultSuccess
			build.Status.Outputs = map[string]string{"artifact": "ready"}
			if err := clusterClient.Status().Update(ctx, build); err != nil {
				t.Fatal(err)
			}
			if cancelRun {
				if err := clusterClient.Get(ctx, request.NamespacedName, run); err != nil {
					t.Fatal(err)
				}
				run.Spec.CancelRequested = true
				if err := clusterClient.Update(ctx, run); err != nil {
					t.Fatal(err)
				}
			}
			workflowDone := make(chan error, 1)
			go func() {
				_, err := workflow.Reconcile(ctx, request)
				workflowDone <- err
			}()
			select {
			case err := <-workflowDone:
				if err != nil {
					t.Fatal(err)
				}
			case <-ctx.Done():
				t.Fatal("workflow reconciliation waited for GitHub reporting")
			}
			if err := clusterClient.Get(ctx, client.ObjectKeyFromObject(dependent), dependent); err != nil {
				t.Fatal(err)
			}
			if cancelRun {
				if dependent.Status.Result != actionsv1alpha1.WorkflowJobResultCancelled {
					t.Fatalf("dependent job result = %q, want cancelled", dependent.Status.Result)
				}
			} else {
				if !workflowJobReady(dependent) {
					t.Fatalf("dependent job did not become ready: %#v", dependent.Status)
				}
				needsObject := &corev1.ConfigMap{}
				if err := clusterClient.Get(ctx, client.ObjectKey{Namespace: run.Namespace, Name: childName(dependent.Name, "needs")}, needsObject); err != nil {
					t.Fatal(err)
				}
				needs, err := runner.DecodeNeedsContext([]byte(needsObject.Data[jobNeedsKey]))
				if err != nil || needs["build"].Outputs["artifact"] != "ready" {
					t.Fatalf("dependent job needs = %#v, %v", needs, err)
				}
			}
			unblock()
			select {
			case err := <-reportDone:
				if err != nil {
					t.Fatal(err)
				}
			case <-ctx.Done():
				t.Fatal("reporter did not finish after GitHub responded")
			}

			// A fresh reconciler recovers the latest result from persisted job state.
			restarted := &GitHubStatusReconciler{Client: clusterClient, APIReader: clusterClient, GitHub: reporter.GitHub, ConsoleURL: reporter.ConsoleURL}
			if _, err := restarted.Reconcile(ctx, request); err != nil {
				t.Fatal(err)
			}
			if err := clusterClient.Get(ctx, client.ObjectKeyFromObject(build), build); err != nil {
				t.Fatal(err)
			}
			if build.Status.Result != actionsv1alpha1.WorkflowJobResultSuccess || build.Status.Outputs["artifact"] != "ready" {
				t.Fatalf("reporting overwrote execution state: %#v", build.Status)
			}
			if status := workflowJobCommitStatus(build); status == nil || status.State != actionsv1alpha1.GitHubCommitStatusStateSuccess {
				t.Fatalf("recovered build status = %#v", status)
			}
		})
	}
}

func TestGitHubStatusRetriesDoNotDelayWorkflowReadiness(t *testing.T) {
	for _, statusCode := range []int{http.StatusServiceUnavailable, http.StatusTooManyRequests} {
		t.Run(http.StatusText(statusCode), func(t *testing.T) {
			reporter, workflow, clusterClient, run, build, dependent := githubStatusTestControllers(t, func(writer http.ResponseWriter, request *http.Request) {
				if statusCode == http.StatusTooManyRequests {
					writer.Header().Set("Retry-After", "30")
				}
				http.Error(writer, "reporting unavailable", statusCode)
			})
			request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(run)}
			result, err := reporter.Reconcile(t.Context(), request)
			if statusCode == http.StatusTooManyRequests {
				if err != nil || result.RequeueAfter != 30*time.Second {
					t.Fatalf("rate-limited reporter result = %#v, %v", result, err)
				}
			} else if err == nil {
				t.Fatal("transient reporting failure did not request a retry")
			}
			build.Status.Result = actionsv1alpha1.WorkflowJobResultSuccess
			if err := clusterClient.Status().Update(t.Context(), build); err != nil {
				t.Fatal(err)
			}
			if _, err := workflow.Reconcile(t.Context(), request); err != nil {
				t.Fatal(err)
			}
			if err := clusterClient.Get(t.Context(), client.ObjectKeyFromObject(dependent), dependent); err != nil {
				t.Fatal(err)
			}
			if !workflowJobReady(dependent) {
				t.Fatalf("dependent job did not become ready: %#v", dependent.Status)
			}
		})
	}
}

func githubStatusTestControllers(t *testing.T, publish http.HandlerFunc, statusHistory ...func() []githubclient.CommitStatus) (*GitHubStatusReconciler, *WorkflowRunReconciler, client.Client, *actionsv1alpha1.WorkflowRun, *actionsv1alpha1.WorkflowJob, *actionsv1alpha1.WorkflowJob) {
	t.Helper()
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch {
		case request.URL.Path == "/app":
			fmt.Fprint(writer, `{"id":1,"slug":"open-actions"}`)
		case request.URL.Path == "/app/installations/2/access_tokens":
			fmt.Fprint(writer, `{"token":"statuses-token"}`)
		case request.Method == http.MethodGet && strings.HasSuffix(request.URL.Path, "/statuses"):
			if len(statusHistory) != 0 {
				_ = json.NewEncoder(writer).Encode(statusHistory[0]())
			} else {
				fmt.Fprint(writer, `[]`)
			}
		case request.Method == http.MethodPost && strings.Contains(request.URL.Path, "/statuses/"):
			publish(writer, request)
		default:
			http.NotFound(writer, request)
		}
	}))
	t.Cleanup(server.Close)
	github, err := githubclient.NewClient(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	scheme := runnerTestScheme(t)
	project := &actionsv1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: "project", Namespace: "default", UID: "project-uid"},
		Spec: actionsv1alpha1.ProjectSpec{Source: actionsv1alpha1.ProjectSource{Type: actionsv1alpha1.SourceTypeGitHub, GitHub: &actionsv1alpha1.GitHubAppConfiguration{
			AppID: 1, InstallationID: 2,
			PrivateKeySecretRef: corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "github"}, Key: "private-key"},
		}}},
	}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "github", Namespace: "default"}, Data: map[string][]byte{
		"private-key": pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(privateKey)}),
	}}
	run := &actionsv1alpha1.WorkflowRun{
		ObjectMeta: metav1.ObjectMeta{Name: "ci", Namespace: "default", UID: "run-uid", Finalizers: []string{workflowRunGitHubStatusFinalizer}, Labels: map[string]string{actionsv1alpha1.LabelWorkflowRunRootUID: "run-uid"}},
		Spec: actionsv1alpha1.WorkflowRunSpec{
			ProjectRef: corev1.LocalObjectReference{Name: project.Name}, WorkflowPath: ".open-actions/workflows/ci.yaml",
			Source: actionsv1alpha1.WorkflowRunSource{Type: actionsv1alpha1.SourceTypeGitHub, GitHub: &actionsv1alpha1.GitHubWorkflowRunSource{
				Repository: actionsv1alpha1.GitHubRepository{ID: 3, Owner: "acme", Name: "example"},
				Event:      actionsv1alpha1.GitHubEvent{Name: actionsv1alpha1.GitHubEventNamePush},
				Revision:   actionsv1alpha1.GitRevision{SHA: strings.Repeat("a", 40)},
			}},
		},
		Status: actionsv1alpha1.WorkflowRunStatus{WorkflowName: "CI", Jobs: &actionsv1alpha1.WorkflowRunJobStatus{Total: 2}, Conditions: []metav1.Condition{plannedCondition(metav1.ConditionTrue, "JobsPlanned")}},
	}
	setTestWorkflowRunIdentity(run)
	build := &actionsv1alpha1.WorkflowJob{
		ObjectMeta: metav1.ObjectMeta{Name: "ci-build", Namespace: run.Namespace, UID: "build-uid", Labels: map[string]string{actionsv1alpha1.LabelWorkflowRunUID: string(run.UID)}},
		Spec:       actionsv1alpha1.WorkflowJobSpec{WorkflowRunRef: corev1.LocalObjectReference{Name: run.Name}, JobID: "build", RunsOn: []string{"linux"}},
	}
	dependent := &actionsv1alpha1.WorkflowJob{
		ObjectMeta: metav1.ObjectMeta{Name: "ci-deploy", Namespace: run.Namespace, UID: "deploy-uid", Labels: map[string]string{actionsv1alpha1.LabelWorkflowRunUID: string(run.UID)}},
		Spec:       actionsv1alpha1.WorkflowJobSpec{WorkflowRunRef: corev1.LocalObjectReference{Name: run.Name}, JobID: "deploy", Needs: []string{"build"}, RunsOn: []string{"linux"}},
	}
	objects := []client.Object{project, secret, run}
	for _, job := range []*actionsv1alpha1.WorkflowJob{build, dependent} {
		if err := controllerutil.SetControllerReference(run, job, scheme); err != nil {
			t.Fatal(err)
		}
		plan := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: childName(job.Name, "plan"), Namespace: job.Namespace}}
		if err := controllerutil.SetControllerReference(job, plan, scheme); err != nil {
			t.Fatal(err)
		}
		objects = append(objects, job, plan)
	}
	clusterClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&actionsv1alpha1.WorkflowRun{}, &actionsv1alpha1.WorkflowJob{}).WithObjects(objects...).Build()
	// Fake clients assign resource versions when storing the initial fixtures.
	if err := clusterClient.Get(t.Context(), client.ObjectKeyFromObject(build), build); err != nil {
		t.Fatal(err)
	}
	return &GitHubStatusReconciler{Client: clusterClient, APIReader: clusterClient, GitHub: github, ConsoleURL: "https://console.example"},
		&WorkflowRunReconciler{Client: clusterClient, APIReader: clusterClient, GitHub: github}, clusterClient, run, build, dependent
}

func TestGitHubStatusFinalizerPreservesConcurrentWorkflowCleanup(t *testing.T) {
	now := metav1.Now()
	run := &actionsv1alpha1.WorkflowRun{ObjectMeta: metav1.ObjectMeta{
		Name: "ci", Namespace: "default", UID: "run-uid", DeletionTimestamp: &now,
		Finalizers: []string{workflowRunCancellationFinalizer, workflowRunGitHubStatusFinalizer, "example.com/hold"},
	}}
	clusterClient := fake.NewClientBuilder().WithScheme(runnerTestScheme(t)).WithObjects(run).Build()
	if err := clusterClient.Get(t.Context(), client.ObjectKeyFromObject(run), run); err != nil {
		t.Fatal(err)
	}
	reporter := &GitHubStatusReconciler{Client: clusterClient, APIReader: clusterClient}
	if _, err := reporter.finalizeGitHubStatus(t.Context(), run); err != nil {
		t.Fatal(err)
	}
	stored := &actionsv1alpha1.WorkflowRun{}
	if err := clusterClient.Get(t.Context(), client.ObjectKeyFromObject(run), stored); err != nil {
		t.Fatal(err)
	}
	if !controllerutil.ContainsFinalizer(stored, workflowRunGitHubStatusFinalizer) {
		t.Fatal("reporter removed its finalizer before cancellation cleanup")
	}
	workflow := &WorkflowRunReconciler{Client: clusterClient, APIReader: clusterClient}
	if _, err := workflow.finalizeCanceledWorkflowRun(t.Context(), stored); err != nil {
		t.Fatal(err)
	}
	if err := clusterClient.Get(t.Context(), client.ObjectKeyFromObject(run), run); err != nil {
		t.Fatal(err)
	}
	stale := run.DeepCopy()
	controllerutil.RemoveFinalizer(run, "example.com/hold")
	if err := clusterClient.Update(t.Context(), run); err != nil {
		t.Fatal(err)
	}
	if _, err := reporter.finalizeGitHubStatus(t.Context(), stale); !apierrors.IsConflict(err) {
		t.Fatalf("stale reporting finalizer removal = %v, want conflict", err)
	}
	if err := clusterClient.Get(t.Context(), client.ObjectKeyFromObject(run), stored); err != nil {
		t.Fatal(err)
	}
	if len(stored.Finalizers) != 1 || stored.Finalizers[0] != workflowRunGitHubStatusFinalizer {
		t.Fatalf("finalizers after concurrent cleanup = %v", stored.Finalizers)
	}
	if _, err := reporter.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(run)}); err != nil {
		t.Fatal(err)
	}
	if err := clusterClient.Get(t.Context(), client.ObjectKeyFromObject(run), stored); !apierrors.IsNotFound(err) {
		t.Fatalf("completed deletion = %v, want not found", err)
	}
}

func TestGitHubStatusEventsUseCachedIndex(t *testing.T) {
	run := &actionsv1alpha1.WorkflowRun{
		ObjectMeta: metav1.ObjectMeta{Name: "ci", Namespace: "default", UID: "run-uid", Labels: map[string]string{actionsv1alpha1.LabelGitHubStatusKey: "key", actionsv1alpha1.LabelProjectUID: "project-uid"}},
		Spec: actionsv1alpha1.WorkflowRunSpec{ProjectRef: corev1.LocalObjectReference{Name: "project"}, WorkflowPath: "ci.yaml",
			Source: actionsv1alpha1.WorkflowRunSource{Type: actionsv1alpha1.SourceTypeGitHub, GitHub: &actionsv1alpha1.GitHubWorkflowRunSource{
				Repository: actionsv1alpha1.GitHubRepository{ID: 3}, Revision: actionsv1alpha1.GitRevision{SHA: "revision"},
			}},
		},
	}
	peer := run.DeepCopy()
	peer.Name, peer.UID = "peer", "peer-uid"
	otherNamespace := peer.DeepCopy()
	otherNamespace.Namespace = "other"
	otherProject := peer.DeepCopy()
	otherProject.Name, otherProject.UID = "other-project", "other-project-uid"
	otherProject.Spec.ProjectRef.Name = "other"
	otherKey := peer.DeepCopy()
	otherKey.Name, otherKey.UID = "other-key", "other-key-uid"
	otherKey.Labels[actionsv1alpha1.LabelGitHubStatusKey] = "other"
	clusterClient := fake.NewClientBuilder().WithScheme(runnerTestScheme(t)).
		WithIndex(&actionsv1alpha1.WorkflowRun{}, workflowRunGitHubStatusKeyIndex, indexWorkflowRunGitHubStatusKey).
		WithObjects(run, peer, otherNamespace, otherProject, otherKey).Build()
	reporter := &GitHubStatusReconciler{Client: clusterClient}
	requests := reporter.workflowRunsSharingGitHubStatus(t.Context(), run)
	if len(requests) != 1 || requests[0].NamespacedName != client.ObjectKeyFromObject(peer) {
		t.Fatalf("shared status requests = %#v", requests)
	}
	if keys := indexWorkflowRunGitHubStatusKey(&actionsv1alpha1.WorkflowRun{}); len(keys) != 0 {
		t.Fatalf("index for run without status identity = %v", keys)
	}
}
