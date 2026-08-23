package controller

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	actionsv1alpha1 "github.com/kelos-dev/open-actions/api/v1alpha1"
	"github.com/kelos-dev/open-actions/internal/eventsnapshot"
	workflowexpression "github.com/kelos-dev/open-actions/internal/expression"
	githubclient "github.com/kelos-dev/open-actions/internal/github"
	"github.com/kelos-dev/open-actions/internal/gitrepository"
	"github.com/kelos-dev/open-actions/internal/runner"
	"github.com/kelos-dev/open-actions/internal/workflow"
	batchv1 "k8s.io/api/batch/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

type countingReader struct {
	client.Reader
	getCount int
}

func setTestWorkflowRunIdentity(run *actionsv1alpha1.WorkflowRun) {
	run.Status.Identity = &actionsv1alpha1.WorkflowRunIdentityStatus{
		ID: 1, Number: 1, Attempt: 1, URL: "https://actions.example/runs/default/ci",
	}
}

func (r *countingReader) Get(ctx context.Context, key client.ObjectKey, object client.Object, options ...client.GetOption) error {
	r.getCount++
	return r.Reader.Get(ctx, key, object, options...)
}

func TestJobPlanCoversSupportedSteps(t *testing.T) {
	reconciler := &WorkflowRunReconciler{GitHubServerURL: "https://github.com", GitHubAPIBase: "https://api.github.com", ActionCloneBaseURL: "https://github.com/git"}
	run := &actionsv1alpha1.WorkflowRun{Spec: actionsv1alpha1.WorkflowRunSpec{
		Source: actionsv1alpha1.WorkflowRunSource{
			Type: actionsv1alpha1.SourceTypeGitHub,
			GitHub: &actionsv1alpha1.GitHubWorkflowRunSource{
				Actor:      "octocat",
				Repository: actionsv1alpha1.GitHubRepository{ID: 1, Owner: "example", Name: "project"},
				Event:      actionsv1alpha1.GitHubEvent{Name: "push", DeliveryID: "delivery"},
				Revision:   actionsv1alpha1.GitRevision{SHA: strings.Repeat("a", 40), Ref: "refs/heads/main", BaseRef: "target"},
			},
		},
	}, Status: actionsv1alpha1.WorkflowRunStatus{Identity: &actionsv1alpha1.WorkflowRunIdentityStatus{
		ID: 101, Number: 7, Attempt: 1, URL: "https://actions.example/runs/default/ci",
	}}}
	job := workflow.Job{RunsOn: workflow.StringList{"ubuntu-latest"}, Outputs: map[string]any{"artifact": "${{ steps.build.outputs.value }}"}, Steps: []workflow.Step{
		{Uses: "actions/checkout@v4"},
		{Uses: "actions/setup-go@v5", With: map[string]any{"go-version-file": "go.mod"}},
		{ID: "build", Name: "Build", Run: "make build", ContinueOnError: "${{ inputs.enabled }}"},
	}}
	plan, err := reconciler.jobPlan(run, "CI", "build", nil, job, nil, map[string]any{"enabled": false, "retries": float64(2)}, 90*60)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Repository.ServerURL != "https://github.com" || plan.Repository.APIURL != "https://api.github.com" || plan.Repository.ActionCloneBaseURL != "https://github.com/git" {
		t.Errorf("repository endpoints = %#v", plan.Repository)
	}
	if plan.Version != runner.PlanVersion || plan.Repository.ID != 1 || plan.Event.DeliveryID != "delivery" || plan.Revision.BaseRef != "target" {
		t.Errorf("plan identity = %#v", plan)
	}
	if plan.TimeoutSeconds != 90*60 || plan.CleanupTimeoutSeconds != int64(runner.CleanupTimeout/time.Second) {
		t.Errorf("plan timeouts = execution %d, cleanup %d", plan.TimeoutSeconds, plan.CleanupTimeoutSeconds)
	}
	if plan.Run.ID != 101 || plan.Run.Number != 7 || plan.Run.Attempt != 1 || plan.Run.Actor != "octocat" || plan.Run.URL != "https://actions.example/runs/default/ci" {
		t.Errorf("run identity = %#v", plan.Run)
	}
	if plan.Inputs["enabled"] != false || plan.Inputs["retries"] != float64(2) {
		t.Errorf("plan inputs = %#v", plan.Inputs)
	}
	if !maps.Equal(plan.GitHubTokenPermissions, map[string]string{"contents": "read"}) {
		t.Errorf("GitHub token permissions = %#v", plan.GitHubTokenPermissions)
	}
	if len(plan.Steps) != 3 {
		t.Errorf("steps = %d", len(plan.Steps))
	}
	if plan.Steps[2].ID != "build" || plan.Steps[2].ContinueOnError != "${{ inputs.enabled }}" || plan.Outputs["artifact"] == "" {
		t.Errorf("output plan = %#v", plan)
	}
}

func TestGitHubTokenPermissionsRestrictsForkWrites(t *testing.T) {
	requested := workflow.Permissions{"contents": "write", "issues": "read", "pull-requests": "write", "statuses": "write"}
	for _, tt := range []struct {
		name      string
		eventName actionsv1alpha1.GitHubEventName
		headID    int64
		policy    *actionsv1alpha1.WorkflowRunForkPullRequest
		want      map[string]string
	}{
		{
			name: "fork pull request", eventName: actionsv1alpha1.GitHubEventNamePullRequest, headID: 2,
			want: map[string]string{"contents": "read", "issues": "read", "pull_requests": "read", "statuses": "read"},
		},
		{
			name: "same repository pull request", eventName: actionsv1alpha1.GitHubEventNamePullRequest, headID: 1,
			want: map[string]string{"contents": "write", "issues": "read", "pull_requests": "write", "statuses": "write"},
		},
		{
			name: "pull request target", eventName: actionsv1alpha1.GitHubEventNamePullRequestTarget, headID: 2,
			want: map[string]string{"contents": "write", "issues": "read", "pull_requests": "write", "statuses": "write"},
		},
		{
			name: "Dependabot policy", eventName: actionsv1alpha1.GitHubEventNamePullRequest, headID: 1,
			policy: &actionsv1alpha1.WorkflowRunForkPullRequest{},
			want:   map[string]string{"contents": "read", "issues": "read", "pull_requests": "read", "statuses": "read"},
		},
		{
			name: "configured write tokens", eventName: actionsv1alpha1.GitHubEventNamePullRequest, headID: 2,
			policy: &actionsv1alpha1.WorkflowRunForkPullRequest{SendWriteTokens: true},
			want:   map[string]string{"contents": "write", "issues": "read", "pull_requests": "write", "statuses": "write"},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			run := &actionsv1alpha1.WorkflowRun{Spec: actionsv1alpha1.WorkflowRunSpec{
				ForkPullRequest: tt.policy,
				Source: actionsv1alpha1.WorkflowRunSource{GitHub: &actionsv1alpha1.GitHubWorkflowRunSource{
					Repository: actionsv1alpha1.GitHubRepository{ID: 1},
					Event: actionsv1alpha1.GitHubEvent{Name: tt.eventName, PullRequest: &actionsv1alpha1.GitHubPullRequest{
						HeadRepository: actionsv1alpha1.GitHubRepository{ID: tt.headID},
					}},
				}},
			}}
			if got := githubTokenPermissions(run, requested); !maps.Equal(got, tt.want) {
				t.Fatalf("permissions = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestForkPullRequestWaitsForApprovalWithoutCreatingJobs(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := actionsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	run := forkApprovalTestRun(false)
	clusterClient := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&actionsv1alpha1.WorkflowRun{}).
		WithObjects(run).Build()
	reconciler := &WorkflowRunReconciler{Client: clusterClient, APIReader: clusterClient}
	if result, err := reconciler.reconcileWorkflowRun(context.Background(), run); err != nil || result.Requeue || result.RequeueAfter != 0 {
		t.Fatalf("reconcileWorkflowRun() = %#v, %v", result, err)
	}
	stored := &actionsv1alpha1.WorkflowRun{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(run), stored); err != nil {
		t.Fatal(err)
	}
	approved := meta.FindStatusCondition(stored.Status.Conditions, actionsv1alpha1.WorkflowRunConditionApproved)
	planned := meta.FindStatusCondition(stored.Status.Conditions, actionsv1alpha1.WorkflowRunConditionPlanned)
	if approved == nil || approved.Status != metav1.ConditionFalse || approved.Reason != "ApprovalRequired" || planned == nil || planned.Status != metav1.ConditionUnknown || planned.Reason != "ApprovalRequired" {
		t.Fatalf("approval conditions = %#v", stored.Status.Conditions)
	}
	jobs := &actionsv1alpha1.WorkflowJobList{}
	if err := clusterClient.List(context.Background(), jobs); err != nil {
		t.Fatal(err)
	}
	if len(jobs.Items) != 0 {
		t.Fatalf("WorkflowJobs = %#v", jobs.Items)
	}
}

func TestNewForkPullRequestRevisionSupersedesUnapprovedRun(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := actionsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	oldRun := forkApprovalTestRun(false)
	oldRun.CreationTimestamp = metav1.NewTime(time.Unix(1, 0))
	clusterClient := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&actionsv1alpha1.WorkflowRun{}).
		WithObjects(oldRun).Build()
	reconciler := &WorkflowRunReconciler{Client: clusterClient, APIReader: clusterClient}
	if _, err := reconciler.reconcileWorkflowRun(context.Background(), oldRun); err != nil {
		t.Fatal(err)
	}

	newRun := forkApprovalTestRun(false)
	newRun.Name = "ci-new-head"
	newRun.UID = "new-run-uid"
	newRun.CreationTimestamp = metav1.NewTime(time.Unix(2, 0))
	newRun.Spec.Source.GitHub.Event.PullRequest.HeadSHA = strings.Repeat("c", 40)
	newRun.Spec.Source.GitHub.Revision.HeadSHA = strings.Repeat("c", 40)
	if err := clusterClient.Create(context.Background(), newRun); err != nil {
		t.Fatal(err)
	}
	requests := reconciler.workflowRunsSupersededByForkPullRequestRevision(context.Background(), newRun)
	if len(requests) != 1 || requests[0].NamespacedName != client.ObjectKeyFromObject(oldRun) {
		t.Fatalf("superseded requests = %#v", requests)
	}
	stored := &actionsv1alpha1.WorkflowRun{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(oldRun), stored); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.reconcileWorkflowRun(context.Background(), stored); err != nil {
		t.Fatal(err)
	}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(oldRun), stored); err != nil {
		t.Fatal(err)
	}
	approved := meta.FindStatusCondition(stored.Status.Conditions, actionsv1alpha1.WorkflowRunConditionApproved)
	succeeded := meta.FindStatusCondition(stored.Status.Conditions, actionsv1alpha1.WorkflowRunConditionSucceeded)
	if approved == nil || approved.Status != metav1.ConditionFalse || approved.Reason != "RevisionSuperseded" || succeeded == nil || succeeded.Status != metav1.ConditionFalse || succeeded.Reason != "RevisionSuperseded" {
		t.Fatalf("superseded status = %#v", stored.Status)
	}
}

func TestForkPullRequestApprovalValidatesCurrentHead(t *testing.T) {
	approvedHead := strings.Repeat("b", 40)
	for _, test := range []struct {
		name        string
		currentHead string
		wantReason  string
		wantRequeue bool
	}{
		{name: "current revision", currentHead: approvedHead, wantReason: "ApprovalGranted", wantRequeue: true},
		{name: "superseded revision", currentHead: strings.Repeat("c", 40), wantReason: "RevisionSuperseded"},
	} {
		t.Run(test.name, func(t *testing.T) {
			privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
			if err != nil {
				t.Fatal(err)
			}
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				switch request.URL.Path {
				case "/app/installations/2/access_tokens":
					fmt.Fprint(writer, `{"token":"installation-token"}`)
				case "/repos/acme/example/commits":
					if request.URL.Query().Get("sha") != "refs/pull/7/head" {
						http.Error(writer, "unexpected pull request ref", http.StatusBadRequest)
						return
					}
					fmt.Fprintf(writer, `[{"sha":%q}]`, test.currentHead)
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
				ObjectMeta: metav1.ObjectMeta{Name: "project", Namespace: "default"},
				Spec: actionsv1alpha1.ProjectSpec{Source: actionsv1alpha1.ProjectSource{Type: actionsv1alpha1.SourceTypeGitHub, GitHub: &actionsv1alpha1.GitHubAppConfiguration{
					AppID: 1, InstallationID: 2,
					PrivateKeySecretRef: corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "github"}, Key: "private-key"},
				}}},
			}
			secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "github", Namespace: "default"}, Data: map[string][]byte{
				"private-key": pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(privateKey)}),
			}}
			run := forkApprovalTestRun(true)
			clusterClient := fake.NewClientBuilder().WithScheme(scheme).
				WithStatusSubresource(&actionsv1alpha1.WorkflowRun{}).
				WithObjects(project, secret, run).Build()
			reconciler := &WorkflowRunReconciler{Client: clusterClient, APIReader: clusterClient, GitHub: github}
			result, err := reconciler.reconcileWorkflowRun(context.Background(), run)
			if err != nil || result.Requeue != test.wantRequeue {
				t.Fatalf("reconcileWorkflowRun() = %#v, %v", result, err)
			}
			stored := &actionsv1alpha1.WorkflowRun{}
			if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(run), stored); err != nil {
				t.Fatal(err)
			}
			approved := meta.FindStatusCondition(stored.Status.Conditions, actionsv1alpha1.WorkflowRunConditionApproved)
			if approved == nil || approved.Reason != test.wantReason {
				t.Fatalf("Approved condition = %#v", approved)
			}
			if test.wantRequeue {
				if approved.Status != metav1.ConditionTrue || terminalRun(stored) {
					t.Fatalf("current revision status = %#v", stored.Status)
				}
			} else {
				succeeded := meta.FindStatusCondition(stored.Status.Conditions, actionsv1alpha1.WorkflowRunConditionSucceeded)
				if approved.Status != metav1.ConditionFalse || succeeded == nil || succeeded.Status != metav1.ConditionFalse || succeeded.Reason != "RevisionSuperseded" {
					t.Fatalf("superseded revision status = %#v", stored.Status)
				}
			}
			jobs := &actionsv1alpha1.WorkflowJobList{}
			if err := clusterClient.List(context.Background(), jobs); err != nil {
				t.Fatal(err)
			}
			if len(jobs.Items) != 0 {
				t.Fatalf("WorkflowJobs = %#v", jobs.Items)
			}
		})
	}
}

func forkApprovalTestRun(approved bool) *actionsv1alpha1.WorkflowRun {
	headSHA := strings.Repeat("b", 40)
	return &actionsv1alpha1.WorkflowRun{
		ObjectMeta: metav1.ObjectMeta{Name: "ci", Namespace: "default", UID: "run-uid"},
		Spec: actionsv1alpha1.WorkflowRunSpec{
			ProjectRef: corev1.LocalObjectReference{Name: "project"},
			Source: actionsv1alpha1.WorkflowRunSource{Type: actionsv1alpha1.SourceTypeGitHub, GitHub: &actionsv1alpha1.GitHubWorkflowRunSource{
				Repository: actionsv1alpha1.GitHubRepository{ID: 1, Owner: "acme", Name: "example"},
				Event: actionsv1alpha1.GitHubEvent{Name: actionsv1alpha1.GitHubEventNamePullRequest, Action: "synchronize", DeliveryID: "delivery", PullRequest: &actionsv1alpha1.GitHubPullRequest{
					Number: 7, Body: "Pull request body", HTMLURL: "https://github.com/acme/example/pull/7",
					HeadRepository: actionsv1alpha1.GitHubRepository{ID: 2, Owner: "contributor", Name: "example"}, HeadRef: "feature", HeadSHA: headSHA, BaseRef: "main",
				}},
				Revision: actionsv1alpha1.GitRevision{SHA: strings.Repeat("a", 40), HeadSHA: headSHA, BaseSHA: strings.Repeat("9", 40), MergeBaseSHA: strings.Repeat("8", 40), Ref: "refs/pull/7/merge", HeadRef: "feature", BaseRef: "main"},
			}},
			WorkflowPath: ".open-actions/workflows/ci.yaml",
			ForkPullRequest: &actionsv1alpha1.WorkflowRunForkPullRequest{
				RequireApproval: true, Approved: approved,
			},
		},
	}
}

func TestPlanWorkflowJobsInheritsWorkflowEnvironment(t *testing.T) {
	reconciler := &WorkflowRunReconciler{}
	run := &actionsv1alpha1.WorkflowRun{Spec: actionsv1alpha1.WorkflowRunSpec{Source: actionsv1alpha1.WorkflowRunSource{
		Type: actionsv1alpha1.SourceTypeGitHub,
		GitHub: &actionsv1alpha1.GitHubWorkflowRunSource{
			Repository: actionsv1alpha1.GitHubRepository{ID: 1, Owner: "acme", Name: "example"},
			Event:      actionsv1alpha1.GitHubEvent{Name: "push", DeliveryID: "delivery"},
			Revision:   actionsv1alpha1.GitRevision{SHA: strings.Repeat("a", 40), Ref: "refs/heads/main"},
		},
	}}}
	setTestWorkflowRunIdentity(run)
	definition := &workflow.Definition{
		Name: "CI",
		Env: map[string]any{
			"GLOBAL":   "${{ vars.GLOBAL }}",
			"OVERRIDE": "workflow",
			"TOKEN":    "${{ secrets.TOKEN }}",
		},
		Jobs: map[string]workflow.Job{
			"build": {
				RunsOn: workflow.StringList{"ubuntu-latest"},
				Env:    map[string]any{"OVERRIDE": "job"},
				Steps: []workflow.Step{{
					Uses: "actions/example@v1",
					With: map[string]any{"message": "${{ env.GLOBAL }}-${{ env.OVERRIDE }}"},
					Env:  map[string]any{"OVERRIDE": "step"},
				}},
			},
			"lint": {
				RunsOn: workflow.StringList{"ubuntu-latest"},
				Steps:  []workflow.Step{{Run: "true"}},
			},
		},
	}

	planned, _, err := reconciler.planWorkflowJobs(run, definition, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(planned) != 2 {
		t.Fatalf("planned jobs = %d, want 2", len(planned))
	}
	build := &runner.Plan{}
	if err := json.Unmarshal([]byte(planned[0].plan), build); err != nil {
		t.Fatal(err)
	}
	if build.Env["GLOBAL"] != "${{ vars.GLOBAL }}" || build.Env["OVERRIDE"] != "job" || build.Env["TOKEN"] != "${{ secrets.TOKEN }}" {
		t.Fatalf("build environment = %#v", build.Env)
	}
	if build.Steps[0].Env["OVERRIDE"] != "step" || build.Steps[0].With["message"] != "${{ env.GLOBAL }}-${{ env.OVERRIDE }}" {
		t.Fatalf("build step = %#v", build.Steps[0])
	}
	lint := &runner.Plan{}
	if err := json.Unmarshal([]byte(planned[1].plan), lint); err != nil {
		t.Fatal(err)
	}
	if lint.Env["GLOBAL"] != "${{ vars.GLOBAL }}" || lint.Env["OVERRIDE"] != "workflow" || lint.Env["TOKEN"] != "${{ secrets.TOKEN }}" {
		t.Fatalf("lint environment = %#v", lint.Env)
	}
	replanned, _, err := reconciler.planWorkflowJobs(run, definition, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if replanned[0].plan != planned[0].plan || replanned[1].plan != planned[1].plan {
		t.Fatal("workflow environment planning is not deterministic")
	}
}

func TestPlanWorkflowJobsAppliesPermissionPrecedence(t *testing.T) {
	definition, err := workflow.Parse([]byte("name: CI\non: push\npermissions:\n  contents: write\n  issues: read\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: true\n  publish:\n    permissions:\n      packages: write\n    runs-on: ubuntu-latest\n    steps:\n      - run: true\n"))
	if err != nil {
		t.Fatal(err)
	}
	run := &actionsv1alpha1.WorkflowRun{Spec: actionsv1alpha1.WorkflowRunSpec{Source: actionsv1alpha1.WorkflowRunSource{
		Type: actionsv1alpha1.SourceTypeGitHub,
		GitHub: &actionsv1alpha1.GitHubWorkflowRunSource{
			Actor:      "octocat",
			Repository: actionsv1alpha1.GitHubRepository{ID: 1, Owner: "acme", Name: "example"},
			Event:      actionsv1alpha1.GitHubEvent{Name: "push", DeliveryID: "delivery"},
			Revision:   actionsv1alpha1.GitRevision{SHA: strings.Repeat("a", 40), Ref: "refs/heads/main"},
		},
	}}}
	setTestWorkflowRunIdentity(run)
	reconciler := &WorkflowRunReconciler{GitHubServerURL: "https://github.com", GitHubAPIBase: "https://api.github.com", ActionCloneBaseURL: "https://github.com"}
	planned, _, err := reconciler.planWorkflowJobs(run, definition, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(planned) != 2 {
		t.Fatalf("planned jobs = %d", len(planned))
	}
	want := []map[string]string{{"contents": "write", "issues": "read"}, {"packages": "write"}}
	for index := range planned {
		plan, err := runner.DecodePlan([]byte(planned[index].plan))
		if err != nil {
			t.Fatal(err)
		}
		if !maps.Equal(plan.GitHubTokenPermissions, want[index]) {
			t.Errorf("job %q permissions = %#v, want %#v", planned[index].id, plan.GitHubTokenPermissions, want[index])
		}
	}
}

func TestWorkflowRunPlansFromLocalPullRequestIntegration(t *testing.T) {
	serverRoot, baseSHA, headSHA, mergeBaseSHA := createControllerTestRepository(t)
	gitRepository, err := gitrepository.NewClient(serverRoot)
	if err != nil {
		t.Fatal(err)
	}
	mergedRepository, err := gitRepository.Merge(context.Background(), "acme", "example", "token", gitrepository.Revision{
		BaseSHA: baseSHA, HeadSHA: headSHA, MergeBaseSHA: mergeBaseSHA,
	})
	if err != nil {
		t.Fatal(err)
	}
	integrationSHA := mergedRepository.SHA
	if err := mergedRepository.Close(); err != nil {
		t.Fatal(err)
	}

	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	privateKeyData := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(privateKey)})
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method == http.MethodPost && request.URL.Path == "/app/installations/2/access_tokens" {
			fmt.Fprint(writer, `{"token":"contents-token"}`)
			return
		}
		http.NotFound(writer, request)
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
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "github", Namespace: "default"},
		Data:       map[string][]byte{"private-key": privateKeyData},
	}
	run := &actionsv1alpha1.WorkflowRun{
		ObjectMeta: metav1.ObjectMeta{Name: "ci", Namespace: "default", UID: "run-uid"},
		Spec: actionsv1alpha1.WorkflowRunSpec{
			ProjectRef:   corev1.LocalObjectReference{Name: project.Name},
			WorkflowPath: ".open-actions/workflows/ci.yaml",
			Source: actionsv1alpha1.WorkflowRunSource{Type: actionsv1alpha1.SourceTypeGitHub, GitHub: &actionsv1alpha1.GitHubWorkflowRunSource{
				Repository: actionsv1alpha1.GitHubRepository{ID: 3, Owner: "acme", Name: "example"},
				Event: actionsv1alpha1.GitHubEvent{
					Name: "pull_request", Action: "synchronize", DeliveryID: "delivery",
					PullRequest: &actionsv1alpha1.GitHubPullRequest{Number: 42, HeadRef: "feature", HeadSHA: headSHA, BaseRef: "main"},
				},
				Revision: actionsv1alpha1.GitRevision{
					SHA: integrationSHA, BaseSHA: baseSHA, HeadSHA: headSHA, MergeBaseSHA: mergeBaseSHA,
					Ref: "refs/pull/42/merge", HeadRef: "feature", BaseRef: "main",
				},
			}},
		},
	}
	clusterClient := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&actionsv1alpha1.WorkflowRun{}, &actionsv1alpha1.WorkflowJob{}).
		WithObjects(project, secret, run).
		Build()
	reconciler := &WorkflowRunReconciler{
		Client: clusterClient, APIReader: clusterClient, GitHub: github, GitRepository: gitRepository,
		GitHubAPIBase: server.URL, GitHubServerURL: "https://github.example", ActionCloneBaseURL: "https://github.example",
	}
	setTestWorkflowRunIdentity(run)
	if _, err := reconciler.reconcileWorkflowRun(context.Background(), run); err != nil {
		t.Fatal(err)
	}

	jobs := &actionsv1alpha1.WorkflowJobList{}
	if err := clusterClient.List(context.Background(), jobs); err != nil {
		t.Fatal(err)
	}
	if len(jobs.Items) != 1 || jobs.Items[0].Spec.JobID != "build" {
		t.Fatalf("WorkflowJobs = %#v", jobs.Items)
	}
	plans := &corev1.ConfigMapList{}
	if err := clusterClient.List(context.Background(), plans, client.MatchingLabels{actionsv1alpha1.LabelWorkflowRunUID: string(run.UID)}); err != nil {
		t.Fatal(err)
	}
	if len(plans.Items) != 1 {
		t.Fatalf("plan ConfigMaps = %d", len(plans.Items))
	}
	plan := &runner.Plan{}
	if err := json.Unmarshal([]byte(plans.Items[0].Data[jobPlanKey]), plan); err != nil {
		t.Fatal(err)
	}
	if plan.Revision.SHA != integrationSHA || plan.Revision.BaseSHA != baseSHA || plan.Revision.HeadSHA != headSHA || plan.Revision.MergeBaseSHA != mergeBaseSHA {
		t.Fatalf("plan revision = %#v", plan.Revision)
	}
}

func TestPlanWorkflowJobsSetsDisplayNames(t *testing.T) {
	reconciler := &WorkflowRunReconciler{}
	run := &actionsv1alpha1.WorkflowRun{Spec: actionsv1alpha1.WorkflowRunSpec{Source: actionsv1alpha1.WorkflowRunSource{
		Type: actionsv1alpha1.SourceTypeGitHub,
		GitHub: &actionsv1alpha1.GitHubWorkflowRunSource{
			Repository: actionsv1alpha1.GitHubRepository{ID: 1},
			Event: actionsv1alpha1.GitHubEvent{
				Name: "pull_request", Action: "synchronize", DeliveryID: "delivery", Inputs: map[string]string{"environment": "staging"},
				PullRequest: &actionsv1alpha1.GitHubPullRequest{
					Number: 7, HeadRef: "feature", HeadSHA: strings.Repeat("b", 40), BaseRef: "main",
					HeadRepository: actionsv1alpha1.GitHubRepository{ID: 2, Owner: "contributor", Name: "project"},
				},
			},
			Revision: actionsv1alpha1.GitRevision{SHA: strings.Repeat("a", 40), Ref: "refs/pull/7/merge", HeadRef: "feature", BaseRef: "main"},
		},
	}}}
	definition := &workflow.Definition{Name: "CI", Jobs: map[string]workflow.Job{
		"build": {Name: "Build ${{ github.base_ref }} PR ${{ github.event.pull_request.number }} from ${{ github.event.pull_request.head.repo.full_name }} at ${{ github.event.pull_request.merge_commit_sha }} for ${{ inputs.environment }} (${{ github.event.inputs.environment }})", RunsOn: workflow.StringList{"ubuntu-latest"}, Outputs: map[string]any{"artifact": "ready"}},
		"lint":  {RunsOn: workflow.StringList{"ubuntu-latest"}, Needs: workflow.StringList{"build"}, If: "always()"},
	}}

	setTestWorkflowRunIdentity(run)
	planned, _, err := reconciler.planWorkflowJobs(run, definition, map[string]any{"environment": "staging"}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(planned) != 2 {
		t.Fatalf("planned jobs = %d, want 2", len(planned))
	}
	if planned[0].id != "build" || planned[0].displayName != "Build main PR 7 from contributor/project at "+strings.Repeat("a", 40)+" for staging (staging)" || planned[0].resultVersion != jobResultVersion || planned[0].timeoutSeconds != int64(defaultMaxJobTimeout/time.Second) {
		t.Errorf("build job = %#v", planned[0])
	}
	plan := &runner.Plan{}
	if err := json.Unmarshal([]byte(planned[0].plan), plan); err != nil {
		t.Fatal(err)
	}
	if plan.Event.PullRequest == nil || plan.Event.PullRequest.Number != 7 || plan.Event.PullRequest.HeadRepository.Owner != "contributor" || plan.Event.PullRequest.HeadSHA != strings.Repeat("b", 40) {
		t.Fatalf("plan pull request = %#v", plan.Event.PullRequest)
	}
	if planned[1].id != "lint" || planned[1].displayName != "lint" || planned[1].resultVersion != jobResultVersion {
		t.Errorf("lint job = %#v", planned[1])
	}
	if len(planned[1].needs) != 1 || planned[1].needs[0] != "build" || planned[1].condition != "always()" {
		t.Errorf("lint graph = %#v", planned[1])
	}
}

func TestPlanWorkflowJobsCapsTimeout(t *testing.T) {
	definition, err := workflow.Parse([]byte("name: CI\non: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    timeout-minutes: 120\n    steps:\n      - run: true\n"))
	if err != nil {
		t.Fatal(err)
	}
	run := &actionsv1alpha1.WorkflowRun{Spec: actionsv1alpha1.WorkflowRunSpec{Source: actionsv1alpha1.WorkflowRunSource{
		Type: actionsv1alpha1.SourceTypeGitHub,
		GitHub: &actionsv1alpha1.GitHubWorkflowRunSource{
			Repository: actionsv1alpha1.GitHubRepository{ID: 1, Owner: "acme", Name: "example"},
			Event:      actionsv1alpha1.GitHubEvent{Name: "push", DeliveryID: "delivery"},
			Revision:   actionsv1alpha1.GitRevision{SHA: strings.Repeat("a", 40), Ref: "refs/heads/main"},
		},
	}}}
	setTestWorkflowRunIdentity(run)
	reconciler := &WorkflowRunReconciler{MaxJobTimeout: 90 * time.Minute}
	planned, _, err := reconciler.planWorkflowJobs(run, definition, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(planned) != 1 || planned[0].timeoutSeconds != 90*60 {
		t.Fatalf("planned timeout = %#v", planned)
	}
	plan := &runner.Plan{}
	if err := json.Unmarshal([]byte(planned[0].plan), plan); err != nil {
		t.Fatal(err)
	}
	if plan.TimeoutSeconds != 90*60 {
		t.Fatalf("runner timeout = %d, want %d", plan.TimeoutSeconds, 90*60)
	}
}

func TestWorkflowJobGraphSchedulesAndPropagatesResults(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := actionsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	run := &actionsv1alpha1.WorkflowRun{
		ObjectMeta: metav1.ObjectMeta{Name: "ci", Namespace: "default", UID: types.UID("run-uid")},
		Spec: actionsv1alpha1.WorkflowRunSpec{Source: actionsv1alpha1.WorkflowRunSource{
			Type: actionsv1alpha1.SourceTypeGitHub,
			GitHub: &actionsv1alpha1.GitHubWorkflowRunSource{
				Repository: actionsv1alpha1.GitHubRepository{ID: 1, Owner: "example", Name: "project"},
				Event:      actionsv1alpha1.GitHubEvent{Name: "push", DeliveryID: "delivery"},
				Revision:   actionsv1alpha1.GitRevision{SHA: strings.Repeat("a", 40), Ref: "refs/heads/main"},
			},
		}},
	}
	job := func(id string, needs []string, condition string, result actionsv1alpha1.WorkflowJobResult) *actionsv1alpha1.WorkflowJob {
		return &actionsv1alpha1.WorkflowJob{
			ObjectMeta: metav1.ObjectMeta{Name: id, Namespace: run.Namespace, Labels: map[string]string{actionsv1alpha1.LabelWorkflowRunUID: string(run.UID)}},
			Spec:       actionsv1alpha1.WorkflowJobSpec{JobID: id, Needs: needs, If: condition},
			Status:     actionsv1alpha1.WorkflowJobStatus{Result: result},
		}
	}
	build := job("build", nil, "", actionsv1alpha1.WorkflowJobResultSuccess)
	build.Status.Outputs = map[string]string{"artifact": "ready"}
	left := job("left", []string{"build"}, "", actionsv1alpha1.WorkflowJobResultSuccess)
	right := job("right", []string{"build"}, "", actionsv1alpha1.WorkflowJobResultSuccess)
	failing := job("failing", nil, "", actionsv1alpha1.WorkflowJobResultFailure)
	cancelled := job("cancelled", nil, "", actionsv1alpha1.WorkflowJobResultCancelled)
	running := job("running", nil, "", "")
	objects := []client.Object{
		build,
		left,
		right,
		failing,
		cancelled,
		running,
		job("diamond-report", []string{"left", "right"}, "needs.left.result == 'success' && needs.right.result == 'success'", ""),
		job("input-condition", []string{"build"}, "inputs.deploy && needs.build.outputs.artifact == 'ready'", ""),
		job("variable-condition", []string{"build"}, "vars.DEPLOY == 'true'", ""),
		job("condition-false", []string{"build"}, "false", ""),
		job("default-after-failure", []string{"failing"}, "", ""),
		job("always-report", []string{"failing"}, "always()", ""),
		job("failure-report", []string{"failing"}, "failure()", ""),
		job("cancel-report", []string{"cancelled"}, "cancelled()", ""),
		job("waiting", []string{"running"}, "", ""),
	}
	clusterClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&actionsv1alpha1.WorkflowJob{}).WithObjects(objects...).Build()
	jobs := &actionsv1alpha1.WorkflowJobList{}
	if err := clusterClient.List(context.Background(), jobs, client.InNamespace(run.Namespace)); err != nil {
		t.Fatal(err)
	}
	reconciler := &WorkflowRunReconciler{Client: clusterClient, APIReader: clusterClient}
	if err := reconciler.reconcileWorkflowJobGraph(context.Background(), run, "CI", map[string]any{"deploy": true}, map[string]string{"DEPLOY": "true"}, nil, jobs.Items); err != nil {
		t.Fatal(err)
	}

	for _, test := range []struct {
		name   string
		result actionsv1alpha1.WorkflowJobResult
		ready  metav1.ConditionStatus
	}{
		{name: "diamond-report", ready: metav1.ConditionTrue},
		{name: "input-condition", ready: metav1.ConditionTrue},
		{name: "variable-condition", ready: metav1.ConditionTrue},
		{name: "condition-false", result: actionsv1alpha1.WorkflowJobResultSkipped, ready: metav1.ConditionFalse},
		{name: "default-after-failure", result: actionsv1alpha1.WorkflowJobResultSkipped, ready: metav1.ConditionFalse},
		{name: "always-report", ready: metav1.ConditionTrue},
		{name: "failure-report", ready: metav1.ConditionTrue},
		{name: "cancel-report", ready: metav1.ConditionTrue},
		{name: "waiting", ready: metav1.ConditionUnknown},
	} {
		t.Run(test.name, func(t *testing.T) {
			stored := &actionsv1alpha1.WorkflowJob{}
			if err := clusterClient.Get(context.Background(), client.ObjectKey{Namespace: run.Namespace, Name: test.name}, stored); err != nil {
				t.Fatal(err)
			}
			if stored.Status.Result != test.result {
				t.Errorf("result = %q, want %q", stored.Status.Result, test.result)
			}
			ready := meta.FindStatusCondition(stored.Status.Conditions, actionsv1alpha1.WorkflowJobConditionReady)
			if ready == nil || ready.Status != test.ready {
				t.Errorf("ready condition = %#v, want %s", ready, test.ready)
			}
			if test.result == actionsv1alpha1.WorkflowJobResultSkipped && stored.Status.RunnerRef != nil {
				t.Errorf("skipped job runnerRef = %#v", stored.Status.RunnerRef)
			}
		})
	}

	inputNeeds := &corev1.ConfigMap{}
	if err := clusterClient.Get(context.Background(), client.ObjectKey{Namespace: run.Namespace, Name: childName("input-condition", "needs")}, inputNeeds); err != nil {
		t.Fatal(err)
	}
	if inputNeeds.Immutable == nil || !*inputNeeds.Immutable {
		t.Fatal("needs context ConfigMap is mutable")
	}
	inputSnapshot, err := runner.DecodeNeedsContext([]byte(inputNeeds.Data[jobNeedsKey]))
	if err != nil {
		t.Fatal(err)
	}
	if len(inputSnapshot) != 1 || inputSnapshot["build"].Result != "success" || inputSnapshot["build"].Outputs["artifact"] != "ready" {
		t.Fatalf("input-condition needs = %#v", inputSnapshot)
	}

	diamondNeeds := &corev1.ConfigMap{}
	if err := clusterClient.Get(context.Background(), client.ObjectKey{Namespace: run.Namespace, Name: childName("diamond-report", "needs")}, diamondNeeds); err != nil {
		t.Fatal(err)
	}
	diamondSnapshot, err := runner.DecodeNeedsContext([]byte(diamondNeeds.Data[jobNeedsKey]))
	if err != nil {
		t.Fatal(err)
	}
	if len(diamondSnapshot) != 2 || diamondSnapshot["left"].Result != "success" || diamondSnapshot["right"].Result != "success" {
		t.Fatalf("diamond-report needs = %#v", diamondSnapshot)
	}
	if _, found := diamondSnapshot["build"]; found {
		t.Fatalf("diamond-report includes transitive dependency: %#v", diamondSnapshot)
	}
}

func TestWorkflowJobGraphRetriesUnavailableVariables(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := actionsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	run := &actionsv1alpha1.WorkflowRun{
		ObjectMeta: metav1.ObjectMeta{Name: "ci", Namespace: "default", UID: types.UID("run-uid")},
		Spec: actionsv1alpha1.WorkflowRunSpec{
			Source: actionsv1alpha1.WorkflowRunSource{Type: actionsv1alpha1.SourceTypeGitHub, GitHub: &actionsv1alpha1.GitHubWorkflowRunSource{
				Repository: actionsv1alpha1.GitHubRepository{ID: 1, Owner: "example", Name: "project"},
				Event:      actionsv1alpha1.GitHubEvent{Name: "push", DeliveryID: "delivery"},
				Revision:   actionsv1alpha1.GitRevision{SHA: strings.Repeat("a", 40), Ref: "refs/heads/main"},
			}},
		},
	}
	readError := errors.New("API server unavailable")
	variables := workflowexpression.DeferredObject(func(string) (any, bool, error) {
		return nil, true, &projectValuesUnavailableError{cause: readError}
	})

	for _, assigned := range []bool{false, true} {
		t.Run(fmt.Sprintf("assigned=%t", assigned), func(t *testing.T) {
			testRun := run.DeepCopy()
			testRun.Spec.CancelRequested = assigned
			job := &actionsv1alpha1.WorkflowJob{
				ObjectMeta: metav1.ObjectMeta{Name: "deploy", Namespace: run.Namespace},
				Spec:       actionsv1alpha1.WorkflowJobSpec{JobID: "deploy", If: "always() && vars.DEPLOY == 'true'"},
			}
			if assigned {
				job.Status.RunnerRef = &corev1.LocalObjectReference{Name: "runner"}
			}
			clusterClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&actionsv1alpha1.WorkflowJob{}).WithObjects(job).Build()
			reconciler := &WorkflowRunReconciler{Client: clusterClient, APIReader: clusterClient}
			err := reconciler.reconcileWorkflowJobGraph(context.Background(), testRun, "CI", nil, variables, nil, []actionsv1alpha1.WorkflowJob{*job})
			if !errors.Is(err, readError) {
				t.Fatalf("reconcileWorkflowJobGraph() error = %v", err)
			}
			stored := &actionsv1alpha1.WorkflowJob{}
			if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(job), stored); err != nil {
				t.Fatal(err)
			}
			if stored.Status.Result != "" || len(stored.Status.Conditions) != 0 {
				t.Fatalf("WorkflowJob was completed after transient value failure: %#v", stored.Status)
			}
		})
	}
}

func TestWorkflowJobGraphWaitsForMatrixDependencies(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := actionsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	run := &actionsv1alpha1.WorkflowRun{
		ObjectMeta: metav1.ObjectMeta{Name: "release", Namespace: "default", UID: types.UID("run-uid")},
		Spec: actionsv1alpha1.WorkflowRunSpec{Source: actionsv1alpha1.WorkflowRunSource{
			Type: actionsv1alpha1.SourceTypeGitHub,
			GitHub: &actionsv1alpha1.GitHubWorkflowRunSource{
				Repository: actionsv1alpha1.GitHubRepository{ID: 1, Owner: "example", Name: "project"},
				Event:      actionsv1alpha1.GitHubEvent{Name: "push", DeliveryID: "delivery"},
				Revision:   actionsv1alpha1.GitRevision{SHA: strings.Repeat("a", 40), Ref: "refs/heads/main"},
			},
		}},
	}
	matrixJob := func(id, arch string, result actionsv1alpha1.WorkflowJobResult) *actionsv1alpha1.WorkflowJob {
		outputs := map[string]string{"artifact": arch}
		for index := 0; index < 51; index++ {
			outputs[fmt.Sprintf("%s_%d", arch, index)] = "available"
		}
		return &actionsv1alpha1.WorkflowJob{
			ObjectMeta: metav1.ObjectMeta{Name: id, Namespace: run.Namespace},
			Spec: actionsv1alpha1.WorkflowJobSpec{
				JobID: id,
				Matrix: &actionsv1alpha1.WorkflowJobMatrix{
					LogicalJobID: "build",
					Values:       map[string]string{"arch": arch},
				},
			},
			Status: actionsv1alpha1.WorkflowJobStatus{Result: result, Outputs: outputs},
		}
	}
	amd64 := matrixJob("build-matrix-1", "amd64", actionsv1alpha1.WorkflowJobResultSuccess)
	arm64 := matrixJob("build-matrix-2", "arm64", "")
	report := &actionsv1alpha1.WorkflowJob{
		ObjectMeta: metav1.ObjectMeta{Name: "report", Namespace: run.Namespace},
		Spec: actionsv1alpha1.WorkflowJobSpec{
			JobID: "report",
			Needs: []string{"build"},
			If:    "needs.build.result == 'success' && needs.build.outputs.artifact == 'arm64'",
		},
	}
	clusterClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&actionsv1alpha1.WorkflowJob{}).
		WithObjects(amd64, arm64, report).
		Build()
	reconciler := &WorkflowRunReconciler{Client: clusterClient, APIReader: clusterClient}

	jobs := &actionsv1alpha1.WorkflowJobList{}
	if err := clusterClient.List(context.Background(), jobs); err != nil {
		t.Fatal(err)
	}
	if err := reconciler.reconcileWorkflowJobGraph(context.Background(), run, "Release", nil, nil, nil, jobs.Items); err != nil {
		t.Fatal(err)
	}
	storedReport := &actionsv1alpha1.WorkflowJob{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(report), storedReport); err != nil {
		t.Fatal(err)
	}
	ready := meta.FindStatusCondition(storedReport.Status.Conditions, actionsv1alpha1.WorkflowJobConditionReady)
	if ready == nil || ready.Status != metav1.ConditionUnknown {
		t.Fatalf("ready condition while matrix runs = %#v", ready)
	}

	storedArm64 := &actionsv1alpha1.WorkflowJob{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(arm64), storedArm64); err != nil {
		t.Fatal(err)
	}
	storedArm64.Status.Result = actionsv1alpha1.WorkflowJobResultSuccess
	if err := clusterClient.Status().Update(context.Background(), storedArm64); err != nil {
		t.Fatal(err)
	}
	if err := clusterClient.List(context.Background(), jobs); err != nil {
		t.Fatal(err)
	}
	if err := reconciler.reconcileWorkflowJobGraph(context.Background(), run, "Release", nil, nil, nil, jobs.Items); err != nil {
		t.Fatal(err)
	}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(report), storedReport); err != nil {
		t.Fatal(err)
	}
	ready = meta.FindStatusCondition(storedReport.Status.Conditions, actionsv1alpha1.WorkflowJobConditionReady)
	if ready == nil || ready.Status != metav1.ConditionTrue {
		t.Fatalf("ready condition after matrix completion = %#v", ready)
	}
	needsConfigMap := &corev1.ConfigMap{}
	if err := clusterClient.Get(context.Background(), client.ObjectKey{Namespace: run.Namespace, Name: childName(report.Name, "needs")}, needsConfigMap); err != nil {
		t.Fatal(err)
	}
	needs, err := runner.DecodeNeedsContext([]byte(needsConfigMap.Data[jobNeedsKey]))
	if err != nil {
		t.Fatal(err)
	}
	if len(needs) != 1 || needs["build"].Result != "success" || needs["build"].Outputs["artifact"] != "arm64" || len(needs["build"].Outputs) != 103 {
		t.Fatalf("matrix needs = %#v", needs)
	}
	if err := reconciler.ensureNeedsContextConfigMap(context.Background(), storedReport, needs); err != nil {
		t.Fatalf("reconcile persisted needs context after restart: %v", err)
	}
}

func TestWorkflowJobAncestorStatusIncludesTransitiveFailure(t *testing.T) {
	failed := &actionsv1alpha1.WorkflowJob{Spec: actionsv1alpha1.WorkflowJobSpec{JobID: "failed"}, Status: actionsv1alpha1.WorkflowJobStatus{Result: actionsv1alpha1.WorkflowJobResultFailure}}
	cleanup := &actionsv1alpha1.WorkflowJob{Spec: actionsv1alpha1.WorkflowJobSpec{JobID: "cleanup", Needs: []string{"failed"}}, Status: actionsv1alpha1.WorkflowJobStatus{Result: actionsv1alpha1.WorkflowJobResultSuccess}}
	report := &actionsv1alpha1.WorkflowJob{Spec: actionsv1alpha1.WorkflowJobSpec{JobID: "report", Needs: []string{"cleanup"}}}
	status := workflowJobAncestorStatus(report, map[string][]*actionsv1alpha1.WorkflowJob{"failed": {failed}, "cleanup": {cleanup}, "report": {report}}, false)
	if status.Success || !status.Failure || status.Cancelled {
		t.Fatalf("ancestor status = %#v", status)
	}
}

func TestWorkflowJobGraphInputValuesReadsPersistedPlan(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := actionsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	job := &actionsv1alpha1.WorkflowJob{
		TypeMeta:   metav1.TypeMeta{APIVersion: actionsv1alpha1.GroupVersion.String(), Kind: "WorkflowJob"},
		ObjectMeta: metav1.ObjectMeta{Name: "deploy", Namespace: "default", UID: types.UID("job-uid")},
		Spec:       actionsv1alpha1.WorkflowJobSpec{JobID: "deploy", If: "inputs.deploy"},
	}
	plan := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: childName(job.Name, "plan"), Namespace: job.Namespace},
		Data:       map[string]string{jobPlanKey: `{"inputs":{"deploy":true}}`},
	}
	if err := controllerutil.SetControllerReference(job, plan, scheme); err != nil {
		t.Fatal(err)
	}
	clusterClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(job, plan).Build()
	reconciler := &WorkflowRunReconciler{Client: clusterClient, APIReader: clusterClient}

	values, err := reconciler.workflowJobGraphInputValues(context.Background(), []actionsv1alpha1.WorkflowJob{*job})
	if err != nil {
		t.Fatal(err)
	}
	if deploy, found := values["deploy"]; !found || deploy != true {
		t.Fatalf("input values = %#v", values)
	}
}

func TestWorkflowCancellationPreservesEligibleCleanupJobs(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := actionsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	run := &actionsv1alpha1.WorkflowRun{
		ObjectMeta: metav1.ObjectMeta{Name: "ci", Namespace: "default", UID: types.UID("run-uid")},
		Spec: actionsv1alpha1.WorkflowRunSpec{
			CancelRequested: true,
			Source: actionsv1alpha1.WorkflowRunSource{Type: actionsv1alpha1.SourceTypeGitHub, GitHub: &actionsv1alpha1.GitHubWorkflowRunSource{
				Repository: actionsv1alpha1.GitHubRepository{ID: 1, Owner: "example", Name: "project"},
				Event:      actionsv1alpha1.GitHubEvent{Name: "push", DeliveryID: "delivery"},
				Revision:   actionsv1alpha1.GitRevision{SHA: strings.Repeat("a", 40), Ref: "refs/heads/main"},
			}},
		},
	}
	ready := []metav1.Condition{{Type: actionsv1alpha1.WorkflowJobConditionReady, Status: metav1.ConditionTrue}}
	job := func(id, condition string, assigned bool) *actionsv1alpha1.WorkflowJob {
		item := &actionsv1alpha1.WorkflowJob{
			ObjectMeta: metav1.ObjectMeta{Name: id, Namespace: run.Namespace},
			Spec:       actionsv1alpha1.WorkflowJobSpec{JobID: id, If: condition},
			Status:     actionsv1alpha1.WorkflowJobStatus{Conditions: append([]metav1.Condition(nil), ready...)},
		}
		if assigned {
			item.Status.RunnerRef = &corev1.LocalObjectReference{Name: "runner-" + id}
		}
		return item
	}
	ordinary := job("ordinary", "", true)
	runningCleanup := job("running-cleanup", "always()", true)
	queued := job("queued", "", false)
	queuedCleanup := job("queued-cleanup", "cancelled()", false)
	objects := []client.Object{ordinary, runningCleanup, queued, queuedCleanup}
	clusterClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&actionsv1alpha1.WorkflowJob{}).WithObjects(objects...).Build()
	jobs := &actionsv1alpha1.WorkflowJobList{}
	if err := clusterClient.List(context.Background(), jobs, client.InNamespace(run.Namespace)); err != nil {
		t.Fatal(err)
	}
	reconciler := &WorkflowRunReconciler{Client: clusterClient, APIReader: clusterClient}
	if err := reconciler.reconcileWorkflowJobGraph(context.Background(), run, "CI", nil, nil, nil, jobs.Items); err != nil {
		t.Fatal(err)
	}

	assertJob := func(name string, result actionsv1alpha1.WorkflowJobResult, cancellation metav1.ConditionStatus, readyStatus metav1.ConditionStatus) {
		t.Helper()
		stored := &actionsv1alpha1.WorkflowJob{}
		if err := clusterClient.Get(context.Background(), client.ObjectKey{Namespace: run.Namespace, Name: name}, stored); err != nil {
			t.Fatal(err)
		}
		if stored.Status.Result != result {
			t.Errorf("WorkflowJob %q result = %q, want %q", name, stored.Status.Result, result)
		}
		if cancellation != "" {
			condition := meta.FindStatusCondition(stored.Status.Conditions, actionsv1alpha1.WorkflowJobConditionCancellationRequested)
			if condition == nil || condition.Status != cancellation {
				t.Errorf("WorkflowJob %q cancellation = %#v, want %s", name, condition, cancellation)
			}
		}
		if readyStatus != "" {
			condition := meta.FindStatusCondition(stored.Status.Conditions, actionsv1alpha1.WorkflowJobConditionReady)
			if condition == nil || condition.Status != readyStatus {
				t.Errorf("WorkflowJob %q ready = %#v, want %s", name, condition, readyStatus)
			}
		}
	}
	assertJob("ordinary", "", metav1.ConditionTrue, "")
	assertJob("running-cleanup", "", metav1.ConditionFalse, "")
	assertJob("queued", actionsv1alpha1.WorkflowJobResultCancelled, "", metav1.ConditionFalse)
	assertJob("queued-cleanup", "", "", metav1.ConditionTrue)
}

func TestPlanWorkflowJobsResolvesOnlyPlanningVariables(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	variables := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "project-variables", Namespace: "default"}, Data: map[string]string{
		"ENVIRONMENT": "production",
		"RUNNER":      "ubuntu-latest",
	}}
	reader := &countingReader{Reader: fake.NewClientBuilder().WithScheme(scheme).WithObjects(variables).Build()}
	reconciler := &WorkflowRunReconciler{APIReader: reader}
	project := &actionsv1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: "default"},
		Spec:       actionsv1alpha1.ProjectSpec{Variables: &actionsv1alpha1.ProjectVariableSource{ConfigMapRef: corev1.LocalObjectReference{Name: variables.Name}}},
	}
	run := &actionsv1alpha1.WorkflowRun{Spec: actionsv1alpha1.WorkflowRunSpec{Source: actionsv1alpha1.WorkflowRunSource{
		Type: actionsv1alpha1.SourceTypeGitHub,
		GitHub: &actionsv1alpha1.GitHubWorkflowRunSource{
			Repository: actionsv1alpha1.GitHubRepository{ID: 1, Owner: "acme", Name: "example"},
			Event:      actionsv1alpha1.GitHubEvent{Name: "push", DeliveryID: "delivery"},
			Revision:   actionsv1alpha1.GitRevision{SHA: strings.Repeat("a", 40), Ref: "refs/heads/main"},
		},
	}}}
	definition := &workflow.Definition{Name: "Deploy", Jobs: map[string]workflow.Job{
		"deploy": {
			Name:   "Deploy ${{ vars.ENVIRONMENT }}",
			RunsOn: workflow.StringList{"${{ vars.RUNNER }}"},
			Env:    map[string]any{"ENVIRONMENT": "${{ vars.ENVIRONMENT }}"},
			Steps:  []workflow.Step{{Run: "deploy '${{ secrets.DEPLOY_TOKEN }}' to '$ENVIRONMENT'"}},
		},
	}}
	variablesContext := reconciler.projectVariableContext(context.Background(), project)
	setTestWorkflowRunIdentity(run)
	planned, _, err := reconciler.planWorkflowJobs(run, definition, nil, variablesContext, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(planned) != 1 || planned[0].displayName != "Deploy production" || !slices.Equal(planned[0].runsOn, []string{"ubuntu-latest"}) {
		t.Fatalf("planned jobs = %#v", planned)
	}
	plan := &runner.Plan{}
	if err := json.Unmarshal([]byte(planned[0].plan), plan); err != nil {
		t.Fatal(err)
	}
	if plan.Env["ENVIRONMENT"] != "${{ vars.ENVIRONMENT }}" || plan.Steps[0].Run != "deploy '${{ secrets.DEPLOY_TOKEN }}' to '$ENVIRONMENT'" {
		t.Fatalf("runtime expressions were resolved in the plan: %#v", plan)
	}
	if strings.Contains(planned[0].plan, "production") {
		t.Fatalf("runtime variable value was persisted in the plan: %s", planned[0].plan)
	}
	for range 2 {
		if _, found, err := variablesContext.Resolve("MISSING"); err != nil || found {
			t.Fatalf("missing variable = found %t, error %v", found, err)
		}
	}
	allVariables, err := variablesContext.Values()
	if err != nil {
		t.Fatal(err)
	}
	if len(allVariables) != 2 || allVariables["ENVIRONMENT"] != "production" || allVariables["RUNNER"] != "ubuntu-latest" {
		t.Fatalf("variables = %#v", allVariables)
	}
	if reader.getCount != 1 {
		t.Fatalf("ConfigMap reads = %d, want 1", reader.getCount)
	}
}

func TestJobExpressionsUseCanonicalEventInputs(t *testing.T) {
	reconciler := &WorkflowRunReconciler{}
	run := &actionsv1alpha1.WorkflowRun{Spec: actionsv1alpha1.WorkflowRunSpec{Source: actionsv1alpha1.WorkflowRunSource{
		Type: actionsv1alpha1.SourceTypeGitHub,
		GitHub: &actionsv1alpha1.GitHubWorkflowRunSource{
			Repository: actionsv1alpha1.GitHubRepository{ID: 1, Owner: "acme", Name: "example"},
			Event:      actionsv1alpha1.GitHubEvent{Name: "workflow_dispatch", Inputs: map[string]string{"retries": "1.10"}},
			Revision:   actionsv1alpha1.GitRevision{SHA: strings.Repeat("a", 40), Ref: "refs/heads/main"},
		},
	}}}
	definition := &workflow.Definition{Name: "Manual", Jobs: map[string]workflow.Job{
		"check": {Name: "${{ inputs.retries }}-${{ github.event.inputs.retries }}", RunsOn: workflow.StringList{"ubuntu-latest"}},
	}}
	setTestWorkflowRunIdentity(run)
	planned, _, err := reconciler.planWorkflowJobs(run, definition, map[string]any{"retries": float64(1.1)}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(planned) != 1 || planned[0].displayName != "1.1-1.1" {
		t.Fatalf("planned jobs = %#v", planned)
	}
}

func TestJobPlanFitsBoundedEncodedContexts(t *testing.T) {
	reconciler := &WorkflowRunReconciler{}
	job := workflow.Job{RunsOn: workflow.StringList{"ubuntu-latest"}, Steps: []workflow.Step{{Run: strings.Repeat("\\", workflow.MaxRunScriptBytes)}}}
	t.Run("event bodies", func(t *testing.T) {
		body := strings.Repeat("\x01", 48_000)
		run := &actionsv1alpha1.WorkflowRun{Spec: actionsv1alpha1.WorkflowRunSpec{Source: actionsv1alpha1.WorkflowRunSource{
			Type: actionsv1alpha1.SourceTypeGitHub,
			GitHub: &actionsv1alpha1.GitHubWorkflowRunSource{
				Repository: actionsv1alpha1.GitHubRepository{ID: 1, Owner: "acme", Name: "example"},
				Event: actionsv1alpha1.GitHubEvent{
					Name: "pull_request_review", Action: "submitted", DeliveryID: "delivery",
					PullRequest: &actionsv1alpha1.GitHubPullRequest{
						Number: 42, Body: body, HTMLURL: "https://github.com/acme/example/pull/42",
						HeadRepository: actionsv1alpha1.GitHubRepository{ID: 2, Owner: "contributor", Name: "example"},
						HeadRef:        "feature", HeadSHA: strings.Repeat("a", 40), BaseRef: "main",
					},
					Review: &actionsv1alpha1.GitHubReviewEvent{Body: body},
				},
				Revision: actionsv1alpha1.GitRevision{SHA: strings.Repeat("b", 40), Ref: "refs/heads/main"},
			},
		}}}
		setTestWorkflowRunIdentity(run)
		planned, _, err := reconciler.planWorkflowJobs(run, &workflow.Definition{Name: "Review", Jobs: map[string]workflow.Job{"check": job}}, nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(planned) != 1 {
			t.Fatalf("planned jobs = %d, want 1", len(planned))
		}
		if len(planned[0].plan) > maxJobPlanBytes {
			t.Fatalf("job plan size = %d, maximum = %d", len(planned[0].plan), maxJobPlanBytes)
		}
	})
	t.Run("escaped input", func(t *testing.T) {
		value := strings.Repeat("\x01", 65_534)
		run := &actionsv1alpha1.WorkflowRun{Spec: actionsv1alpha1.WorkflowRunSpec{Source: actionsv1alpha1.WorkflowRunSource{
			Type: actionsv1alpha1.SourceTypeGitHub,
			GitHub: &actionsv1alpha1.GitHubWorkflowRunSource{
				Repository: actionsv1alpha1.GitHubRepository{ID: 1, Owner: "acme", Name: "example"},
				Event:      actionsv1alpha1.GitHubEvent{Name: "workflow_dispatch", Inputs: map[string]string{"a": value}},
				Revision:   actionsv1alpha1.GitRevision{SHA: strings.Repeat("b", 40), Ref: "refs/heads/main"},
			},
		}}}
		setTestWorkflowRunIdentity(run)
		planned, _, err := reconciler.planWorkflowJobs(run, &workflow.Definition{Name: "Manual", Jobs: map[string]workflow.Job{"check": job}}, map[string]any{"a": value}, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(planned) != 1 {
			t.Fatalf("planned jobs = %d, want 1", len(planned))
		}
		if len(planned[0].plan) > maxJobPlanBytes {
			t.Fatalf("job plan size = %d, maximum = %d", len(planned[0].plan), maxJobPlanBytes)
		}
		document := map[string]any{}
		if err := json.Unmarshal([]byte(planned[0].plan), &document); err != nil {
			t.Fatal(err)
		}
		if _, found := document["event"].(map[string]any)["inputs"]; found {
			t.Fatal("job plan serialized resolved inputs twice")
		}
	})
}

func TestJobExpressionsIncludeBoundedEventMetadata(t *testing.T) {
	reconciler := &WorkflowRunReconciler{}
	for _, tt := range []struct {
		name       string
		event      actionsv1alpha1.GitHubEvent
		expression string
		want       string
	}{
		{name: "workflow run", event: actionsv1alpha1.GitHubEvent{Name: "workflow_run", WorkflowRun: &actionsv1alpha1.GitHubWorkflowRunEvent{Conclusion: "success", HeadSHA: strings.Repeat("a", 40)}}, expression: "${{ github.event.workflow_run.conclusion }}-${{ github.event.workflow_run.head_sha }}", want: "success-" + strings.Repeat("a", 40)},
		{name: "issue", event: actionsv1alpha1.GitHubEvent{Name: "issues", Issue: &actionsv1alpha1.GitHubIssueEvent{Number: 17, Body: "/kind bug"}}, expression: "${{ github.event.issue.number }}-${{ github.event.issue.body }}", want: "17-/kind bug"},
		{name: "comment", event: actionsv1alpha1.GitHubEvent{Name: "issue_comment", Comment: &actionsv1alpha1.GitHubCommentEvent{Body: "/priority important-soon"}}, expression: "${{ github.event.comment.body }}", want: "/priority important-soon"},
		{name: "review", event: actionsv1alpha1.GitHubEvent{Name: "pull_request_review", Review: &actionsv1alpha1.GitHubReviewEvent{Body: "/kind api"}}, expression: "${{ github.event.review.body }}", want: "/kind api"},
		{name: "release", event: actionsv1alpha1.GitHubEvent{Name: "release"}, expression: "${{ github.event.release.tag_name }}", want: "v1.2.3"},
		{name: "pull request", event: actionsv1alpha1.GitHubEvent{Name: "pull_request_target", PullRequest: &actionsv1alpha1.GitHubPullRequest{Number: 42, Body: "Pull request body", HTMLURL: "https://github.com/contributor/example/pull/42"}}, expression: "${{ github.event.pull_request.number }}-${{ github.event.pull_request.body }}-${{ github.event.pull_request.html_url }}", want: "42-Pull request body-https://github.com/contributor/example/pull/42"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ref := "refs/heads/main"
			if tt.event.Name == actionsv1alpha1.GitHubEventNameRelease {
				ref = "refs/tags/v1.2.3"
			}
			run := &actionsv1alpha1.WorkflowRun{Spec: actionsv1alpha1.WorkflowRunSpec{Source: actionsv1alpha1.WorkflowRunSource{
				Type: actionsv1alpha1.SourceTypeGitHub,
				GitHub: &actionsv1alpha1.GitHubWorkflowRunSource{
					Event: tt.event, Revision: actionsv1alpha1.GitRevision{SHA: strings.Repeat("b", 40), Ref: ref},
				},
			}}}
			job, err := workflow.EvaluateJob("test", workflow.Job{Name: tt.expression, RunsOn: workflow.StringList{"ubuntu-latest"}}, reconciler.jobExpressionContext(run, "CI", nil, nil, nil))
			if err != nil {
				t.Fatal(err)
			}
			if job.Name != tt.want {
				t.Fatalf("evaluated job name = %q, want %q", job.Name, tt.want)
			}
		})
	}
}

func TestGitHubEventExpressionUsesImmutableSnapshot(t *testing.T) {
	source := &actionsv1alpha1.GitHubWorkflowRunSource{
		Event: actionsv1alpha1.GitHubEvent{
			Name: actionsv1alpha1.GitHubEventNamePullRequest,
			PullRequest: &actionsv1alpha1.GitHubPullRequest{
				Number: 42, HTMLURL: "https://github.com/acme/example/pull/42", HeadSHA: strings.Repeat("2", 40), BaseRef: "main",
			},
		},
		Revision: actionsv1alpha1.GitRevision{SHA: strings.Repeat("9", 40), BaseSHA: strings.Repeat("8", 40)},
	}
	payload := map[string]any{
		"number": float64(42),
		"pull_request": map[string]any{
			"number":           float64(42),
			"html_url":         "https://github.com/acme/example/pull/42",
			"merge_commit_sha": strings.Repeat("3", 40),
			"head": map[string]any{
				"sha":  strings.Repeat("2", 40),
				"repo": map[string]any{"full_name": "acme/example"},
			},
			"base": map[string]any{"sha": strings.Repeat("1", 40)},
		},
	}
	event := githubEventExpressionValue(source, nil, payload)
	pullRequest := event["pull_request"].(map[string]any)
	if pullRequest["merge_commit_sha"] != strings.Repeat("3", 40) ||
		pullRequest["base"].(map[string]any)["sha"] != strings.Repeat("1", 40) ||
		pullRequest["head"].(map[string]any)["repo"].(map[string]any)["full_name"] != "acme/example" {
		t.Fatalf("github.event.pull_request = %#v", pullRequest)
	}
}

func TestWorkflowRunReadsOwnedImmutableEventSnapshot(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := actionsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join("..", "webhook", "testdata", "github", "push.json"))
	if err != nil {
		t.Fatal(err)
	}
	run := &actionsv1alpha1.WorkflowRun{ObjectMeta: metav1.ObjectMeta{
		Name: "ci", Namespace: "default", UID: "run-uid",
		Annotations: map[string]string{eventsnapshot.Annotation: "event-snapshot"},
	}}
	immutable := true
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name: "event-snapshot", Namespace: run.Namespace,
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: actionsv1alpha1.GroupVersion.String(), Kind: "WorkflowRun", Name: run.Name, UID: run.UID,
			}},
		},
		Immutable: &immutable,
		Data:      map[string][]byte{eventsnapshot.DataKey: data},
	}
	clusterClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(secret).Build()
	reconciler := &WorkflowRunReconciler{APIReader: clusterClient}
	payload, err := reconciler.githubEventSnapshot(context.Background(), run)
	if err != nil {
		t.Fatal(err)
	}
	if payload["sender"].(map[string]any)["email"] != "private@example.com" {
		t.Fatalf("event snapshot = %#v", payload)
	}
}

func TestJobExpressionsIncludeRunIdentity(t *testing.T) {
	reconciler := &WorkflowRunReconciler{
		GitHubServerURL: "https://github.com", GitHubAPIBase: "https://api.github.com", ConsoleURL: "https://actions.example",
	}
	run := &actionsv1alpha1.WorkflowRun{
		ObjectMeta: metav1.ObjectMeta{Name: "ci", Namespace: "default"},
		Spec: actionsv1alpha1.WorkflowRunSpec{Source: actionsv1alpha1.WorkflowRunSource{
			Type: actionsv1alpha1.SourceTypeGitHub,
			GitHub: &actionsv1alpha1.GitHubWorkflowRunSource{
				Actor: "octocat", Event: actionsv1alpha1.GitHubEvent{Name: "push", DeliveryID: "delivery"},
				Revision: actionsv1alpha1.GitRevision{SHA: strings.Repeat("a", 40), Ref: "refs/heads/main"},
			},
		}},
		Status: actionsv1alpha1.WorkflowRunStatus{Identity: &actionsv1alpha1.WorkflowRunIdentityStatus{
			ID: 101, Number: 7, Attempt: 2, URL: "https://actions.example/runs/default/ci",
		}},
	}
	job, err := workflow.EvaluateJob("test", workflow.Job{
		Name:   "${{ github.run_id }}-${{ github.run_number }}-${{ github.run_attempt }}-${{ github.actor }}-${{ open_actions.run_url }}-${{ open_actions.run_query_url }}",
		RunsOn: workflow.StringList{"ubuntu-latest"},
	}, reconciler.jobExpressionContext(run, "CI", nil, nil, nil))
	if err != nil {
		t.Fatal(err)
	}
	want := "101-7-2-octocat-https://actions.example/runs/default/ci-https://actions.example/api/v1/runs/default/ci/newer"
	if job.Name != want {
		t.Fatalf("job name = %q, want %q", job.Name, want)
	}
}

func TestPlanWorkflowJobsExpandsArchitectureMatrix(t *testing.T) {
	reconciler := &WorkflowRunReconciler{}
	run := &actionsv1alpha1.WorkflowRun{Spec: actionsv1alpha1.WorkflowRunSpec{Source: actionsv1alpha1.WorkflowRunSource{
		Type: actionsv1alpha1.SourceTypeGitHub,
		GitHub: &actionsv1alpha1.GitHubWorkflowRunSource{
			Repository: actionsv1alpha1.GitHubRepository{ID: 1, Owner: "kelos-dev", Name: "kelos"},
			Event:      actionsv1alpha1.GitHubEvent{Name: "push", DeliveryID: "delivery"},
			Revision:   actionsv1alpha1.GitRevision{SHA: strings.Repeat("a", 40), Ref: "refs/heads/main"},
		},
	}}}
	definition, err := workflow.Parse([]byte("name: Release\non: push\njobs:\n  build-images:\n    strategy:\n      max-parallel: 1\n      matrix:\n        arch: [amd64, arm64]\n    concurrency: images-${{ matrix.arch }}-${{ strategy['job-index'] }}\n    runs-on: ${{ matrix.arch == 'arm64' && 'ubuntu-24.04-arm' || 'ubuntu-latest' }}\n    outputs:\n      image: ${{ matrix.arch }}-${{ steps.build.outputs.image }}\n    steps:\n      - id: build\n        run: make image IMAGE_PLATFORMS=linux/${{ matrix.arch }}\n"))
	if err != nil {
		t.Fatal(err)
	}

	setTestWorkflowRunIdentity(run)
	planned, _, err := reconciler.planWorkflowJobs(run, definition, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(planned) != 2 {
		t.Fatalf("planned jobs = %d, want 2", len(planned))
	}
	for index, arch := range []string{"amd64", "arm64"} {
		job := planned[index]
		if job.id != fmt.Sprintf("build-images-matrix-%d", index+1) || job.matrix == nil || job.matrix.LogicalJobID != "build-images" || job.matrix.Values["arch"] != arch || job.matrix.JobIndex != int32(index) || job.matrix.JobTotal != 2 || job.matrix.MaxParallel != 1 || job.matrix.FailFast == nil || !*job.matrix.FailFast || job.concurrency == nil || job.concurrency.Group != "images-${{ matrix.arch }}-${{ strategy['job-index'] }}" || job.resultVersion != jobResultVersion {
			t.Errorf("planned job %d = %#v", index, job)
		}
		wantRunner := "ubuntu-latest"
		if arch == "arm64" {
			wantRunner = "ubuntu-24.04-arm"
		}
		if len(job.runsOn) != 1 || job.runsOn[0] != wantRunner {
			t.Errorf("runs-on for %s = %v, want %q", arch, job.runsOn, wantRunner)
		}
		plan := &runner.Plan{}
		if err := json.Unmarshal([]byte(job.plan), plan); err != nil {
			t.Fatal(err)
		}
		if plan.JobID != "build-images" || plan.Matrix["arch"] != arch || plan.Outputs["image"] != "${{ matrix.arch }}-${{ steps.build.outputs.image }}" {
			t.Errorf("plan for %s = %#v", arch, plan)
		}
	}
}

func TestPlanWorkflowJobsEnforcesExpandedWorkflowLimit(t *testing.T) {
	values := make([]int, 256)
	for index := range values {
		values[index] = index
	}
	matrix, err := json.Marshal(map[string]any{"value": values})
	if err != nil {
		t.Fatal(err)
	}
	data := strings.Builder{}
	data.WriteString("name: Large\non: push\njobs:\n")
	for index := range 4 {
		fmt.Fprintf(&data, "  build-%d:\n    strategy:\n      matrix: ${{ fromJSON('%s') }}\n    runs-on: ubuntu-latest\n    steps:\n      - run: build\n", index, matrix)
	}
	definition, err := workflow.Parse([]byte(data.String()))
	if err != nil {
		t.Fatal(err)
	}
	run := &actionsv1alpha1.WorkflowRun{Spec: actionsv1alpha1.WorkflowRunSpec{Source: actionsv1alpha1.WorkflowRunSource{GitHub: &actionsv1alpha1.GitHubWorkflowRunSource{}}}}
	setTestWorkflowRunIdentity(run)
	_, _, err = (&WorkflowRunReconciler{}).planWorkflowJobs(run, definition, nil, nil, nil)
	if err == nil || !strings.Contains(err.Error(), "more than 1000 jobs") {
		t.Fatalf("planning error = %v, want workflow size limit", err)
	}
}

func TestStaticMatrixJobCannotUseUnavailableNeedsContext(t *testing.T) {
	definition, err := workflow.Parse([]byte("name: CI\non: push\njobs:\n  prepare:\n    runs-on: ubuntu-latest\n    steps:\n      - run: prepare\n  build:\n    needs: prepare\n    strategy:\n      matrix:\n        arch: [amd64]\n    name: Build ${{ needs.prepare.result }}\n    runs-on: ubuntu-latest\n    steps:\n      - run: build\n"))
	if err != nil {
		t.Fatal(err)
	}
	run := &actionsv1alpha1.WorkflowRun{Spec: actionsv1alpha1.WorkflowRunSpec{Source: actionsv1alpha1.WorkflowRunSource{GitHub: &actionsv1alpha1.GitHubWorkflowRunSource{}}}}
	setTestWorkflowRunIdentity(run)
	_, _, err = (&WorkflowRunReconciler{}).planWorkflowJobs(run, definition, nil, nil, nil)
	if err == nil || !strings.Contains(err.Error(), `context "needs" is unavailable`) {
		t.Fatalf("planning error = %v, want unavailable needs context", err)
	}
}

func TestProjectedWorkflowJobCountIncludesExistingAndPendingMatrices(t *testing.T) {
	matrixIDs := []string{"a", "m", "z"}
	jobsByLogicalID := map[string][]*actionsv1alpha1.WorkflowJob{
		"z": make([]*actionsv1alpha1.WorkflowJob, 50),
	}
	if got := projectedWorkflowJobCount(950, jobsByLogicalID, matrixIDs, "a", 50); got != 1001 {
		t.Fatalf("projected jobs = %d, want 1001", got)
	}

	jobsByLogicalID["a"] = make([]*actionsv1alpha1.WorkflowJob, 50)
	if got := projectedWorkflowJobCount(1000, jobsByLogicalID, []string{"a", "z"}, "a", 50); got != 1000 {
		t.Fatalf("reprojected jobs = %d, want 1000", got)
	}
}

func TestPlanWorkflowJobsPreservesDisabledMatrixFailFast(t *testing.T) {
	reconciler := &WorkflowRunReconciler{}
	run := &actionsv1alpha1.WorkflowRun{Spec: actionsv1alpha1.WorkflowRunSpec{Source: actionsv1alpha1.WorkflowRunSource{
		Type: actionsv1alpha1.SourceTypeGitHub,
		GitHub: &actionsv1alpha1.GitHubWorkflowRunSource{
			Repository: actionsv1alpha1.GitHubRepository{ID: 1},
			Event:      actionsv1alpha1.GitHubEvent{Name: "push", DeliveryID: "delivery"},
			Revision:   actionsv1alpha1.GitRevision{SHA: strings.Repeat("a", 40), Ref: "refs/heads/main"},
		},
	}}}
	definition, err := workflow.Parse([]byte("name: Release\non: push\njobs:\n  build:\n    strategy:\n      fail-fast: false\n      matrix:\n        arch: [amd64]\n    runs-on: ubuntu-latest\n    steps:\n      - run: make build\n"))
	if err != nil {
		t.Fatal(err)
	}
	setTestWorkflowRunIdentity(run)
	planned, _, err := reconciler.planWorkflowJobs(run, definition, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(planned) != 1 || planned[0].matrix == nil || planned[0].matrix.FailFast == nil || *planned[0].matrix.FailFast {
		t.Fatalf("planned matrix strategy = %#v", planned)
	}
}

func TestReconcileDynamicMatrixFromDependencyOutput(t *testing.T) {
	scheme := runnerTestScheme(t)
	run := &actionsv1alpha1.WorkflowRun{
		ObjectMeta: metav1.ObjectMeta{Name: "dynamic", Namespace: "default", UID: types.UID("run-uid")},
		Spec: actionsv1alpha1.WorkflowRunSpec{
			ProjectRef: corev1.LocalObjectReference{Name: "default"},
			Source: actionsv1alpha1.WorkflowRunSource{Type: actionsv1alpha1.SourceTypeGitHub, GitHub: &actionsv1alpha1.GitHubWorkflowRunSource{
				Repository: actionsv1alpha1.GitHubRepository{ID: 1, Owner: "acme", Name: "example"},
				Event:      actionsv1alpha1.GitHubEvent{Name: actionsv1alpha1.GitHubEventNamePush, DeliveryID: "delivery"},
				Revision:   actionsv1alpha1.GitRevision{SHA: strings.Repeat("a", 40), Ref: "refs/heads/main"},
			}},
		},
	}
	setTestWorkflowRunIdentity(run)
	project := &actionsv1alpha1.Project{ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: "default", UID: types.UID("project-uid")}}
	definition, err := workflow.Parse([]byte("name: Dynamic\non: push\npermissions:\n  issues: write\njobs:\n  prepare:\n    runs-on: ubuntu-latest\n    outputs:\n      arches: ${{ steps.prepare.outputs.arches }}\n    steps:\n      - id: prepare\n        run: prepare\n  build:\n    needs: prepare\n    strategy:\n      max-parallel: 1\n      matrix:\n        arch: ${{ fromJSON(needs.prepare.outputs.arches) }}\n        runner: ['${{ vars.RUNNER }}']\n        build-date: [20260820]\n        run-url: ['${{ open_actions.run_url }}']\n        include:\n          - arch: ppc64le\n            runner: self-hosted\n    name: Build ${{ matrix.arch }} in ${{ vars.ENVIRONMENT }} from ${{ github.event.channel }}\n    runs-on: ${{ matrix.runner }}\n    env:\n      BUILD_DATE: 20260820\n    steps:\n      - run: build ${{ matrix.arch }}\n"))
	if err != nil {
		t.Fatal(err)
	}
	clusterClient := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&actionsv1alpha1.WorkflowRun{}, &actionsv1alpha1.WorkflowJob{}).
		WithObjects(run).
		Build()
	reconciler := &WorkflowRunReconciler{Client: clusterClient, APIReader: clusterClient, GitHubAPIBase: "https://api.github.example", GitHubServerURL: "https://github.example"}
	variables := map[string]any{"ENVIRONMENT": "production", "RUNNER": "ubuntu-latest"}
	eventPayload := map[string]any{"channel": "stable"}
	planned, deferred, err := reconciler.planWorkflowJobs(run, definition, nil, variables, eventPayload)
	if err != nil {
		t.Fatal(err)
	}
	if len(planned) != 1 || len(deferred) != 1 || deferred[0].JobID != "build" {
		t.Fatalf("planned = %#v, deferred = %#v", planned, deferred)
	}
	if err := reconciler.ensureWorkflowPlan(context.Background(), run, project, planned, deferred); err != nil {
		t.Fatal(err)
	}
	variables["ENVIRONMENT"] = "staging"
	variables["RUNNER"] = "windows"
	eventPayload["channel"] = "changed"
	if err := reconciler.ensureWorkflowJobs(context.Background(), run, project, planned); err != nil {
		t.Fatal(err)
	}

	jobs := &actionsv1alpha1.WorkflowJobList{}
	if err := clusterClient.List(context.Background(), jobs, client.InNamespace(run.Namespace), client.MatchingLabels{actionsv1alpha1.LabelWorkflowRunUID: string(run.UID)}); err != nil {
		t.Fatal(err)
	}
	state, err := reconciler.reconcileDynamicMatrices(context.Background(), run, jobs)
	if err != nil {
		t.Fatal(err)
	}
	if _, pending := state.pending["build"]; !pending || state.changed {
		t.Fatalf("matrix state before dependency completion = %#v", state)
	}

	prepare := &jobs.Items[0]
	prepare.Status.Result = actionsv1alpha1.WorkflowJobResultSuccess
	prepare.Status.Outputs = map[string]string{"arches": `["amd64","arm64"]`}
	if err := clusterClient.Status().Update(context.Background(), prepare); err != nil {
		t.Fatal(err)
	}
	jobs = &actionsv1alpha1.WorkflowJobList{}
	if err := clusterClient.List(context.Background(), jobs, client.InNamespace(run.Namespace), client.MatchingLabels{actionsv1alpha1.LabelWorkflowRunUID: string(run.UID)}); err != nil {
		t.Fatal(err)
	}
	state, err = reconciler.reconcileDynamicMatrices(context.Background(), run, jobs)
	if err != nil {
		t.Fatal(err)
	}
	if !state.changed {
		t.Fatal("dynamic matrix creation did not report a change")
	}

	jobs = &actionsv1alpha1.WorkflowJobList{}
	if err := clusterClient.List(context.Background(), jobs, client.InNamespace(run.Namespace), client.MatchingLabels{actionsv1alpha1.LabelWorkflowRunUID: string(run.UID)}); err != nil {
		t.Fatal(err)
	}
	if len(jobs.Items) != 4 {
		t.Fatalf("WorkflowJobs = %d, want producer and three matrix children", len(jobs.Items))
	}
	values := map[string]map[string]string{}
	firstMatrixJobName := ""
	for index := range jobs.Items {
		job := &jobs.Items[index]
		if job.Spec.Matrix != nil {
			values[job.Spec.JobID] = job.Spec.Matrix.Values
			if job.Spec.JobID == "build-matrix-1" {
				firstMatrixJobName = job.Name
			}
			if !strings.Contains(job.Spec.DisplayName, "production from stable") || strings.Contains(job.Spec.DisplayName, "staging") || strings.Contains(job.Spec.DisplayName, "changed") {
				t.Errorf("job %q display name = %q, want snapshotted contexts", job.Spec.JobID, job.Spec.DisplayName)
			}
			if job.Spec.Matrix.MaxParallel != 1 {
				t.Errorf("job %q maxParallel = %d", job.Spec.JobID, job.Spec.Matrix.MaxParallel)
			}
		}
	}
	if len(values) != 3 || values["build-matrix-1"]["arch"] != "amd64" || values["build-matrix-1"]["runner"] != "ubuntu-latest" || values["build-matrix-1"]["build-date"] != "20260820" || values["build-matrix-1"]["run-url"] != "https://actions.example/runs/default/ci" || values["build-matrix-2"]["arch"] != "arm64" || values["build-matrix-3"]["runner"] != "self-hosted" {
		t.Fatalf("matrix values = %#v", values)
	}
	planConfigMap := &corev1.ConfigMap{}
	if err := clusterClient.Get(context.Background(), client.ObjectKey{Namespace: run.Namespace, Name: childName(firstMatrixJobName, "plan")}, planConfigMap); err != nil {
		t.Fatal(err)
	}
	jobPlan := &runner.Plan{}
	if err := json.Unmarshal([]byte(planConfigMap.Data[jobPlanKey]), jobPlan); err != nil {
		t.Fatal(err)
	}
	if jobPlan.Env["BUILD_DATE"] != "20260820" {
		t.Fatalf("BUILD_DATE = %q, want exact integer", jobPlan.Env["BUILD_DATE"])
	}
	if !maps.Equal(jobPlan.GitHubTokenPermissions, map[string]string{"issues": "write"}) {
		t.Fatalf("GitHub token permissions = %#v", jobPlan.GitHubTokenPermissions)
	}

	restarted := &WorkflowRunReconciler{Client: clusterClient, APIReader: clusterClient, GitHubAPIBase: "https://api.github.example", GitHubServerURL: "https://github.example"}
	state, err = restarted.reconcileDynamicMatrices(context.Background(), run, jobs)
	if err != nil {
		t.Fatal(err)
	}
	if state.changed || len(state.expected) != 4 || len(state.pending) != 0 {
		t.Fatalf("matrix state after restart = %#v", state)
	}

	run.Spec.CancelRequested = true
	state, err = restarted.reconcileDynamicMatrices(context.Background(), run, jobs)
	if err != nil {
		t.Fatal(err)
	}
	if state.changed || len(state.expected) != 4 {
		t.Fatalf("matrix state after cancellation = %#v", state)
	}
	if err := restarted.reconcileWorkflowJobGraph(context.Background(), run, definition.Name, nil, nil, nil, jobs.Items, state.pending); err != nil {
		t.Fatal(err)
	}
	jobs = &actionsv1alpha1.WorkflowJobList{}
	if err := clusterClient.List(context.Background(), jobs, client.InNamespace(run.Namespace), client.MatchingLabels{actionsv1alpha1.LabelWorkflowRunUID: string(run.UID)}); err != nil {
		t.Fatal(err)
	}
	for index := range jobs.Items {
		job := &jobs.Items[index]
		if job.Spec.JobID == "build" {
			t.Fatalf("cancellation created duplicate logical job %#v", job)
		}
		if job.Spec.Matrix != nil && job.Status.Result != actionsv1alpha1.WorkflowJobResultCancelled {
			t.Errorf("matrix job %q result = %q, want cancelled", job.Spec.JobID, job.Status.Result)
		}
	}
}

func TestReconcileDynamicMatricesTreatsMissingPlanAsTerminal(t *testing.T) {
	scheme := runnerTestScheme(t)
	run := &actionsv1alpha1.WorkflowRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "missing-plan",
			Namespace:   "default",
			UID:         types.UID("run-uid"),
			Annotations: map[string]string{actionsv1alpha1.AnnotationWorkflowPlan: "missing-plan-workflow-plan"},
		},
	}
	data, err := json.Marshal(workflowPlanManifest{
		SourceIDs: []string{"build"},
		Matrices:  map[string]string{"build": "missing-plan-build-matrix-plan"},
	})
	if err != nil {
		t.Fatal(err)
	}
	immutable := true
	workflowPlan := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: run.Annotations[actionsv1alpha1.AnnotationWorkflowPlan], Namespace: run.Namespace},
		Immutable:  &immutable,
		Data:       map[string]string{workflowPlanKey: string(data)},
	}
	if err := controllerutil.SetControllerReference(run, workflowPlan, scheme); err != nil {
		t.Fatal(err)
	}
	clusterClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(run, workflowPlan).Build()
	reconciler := &WorkflowRunReconciler{Client: clusterClient, APIReader: clusterClient}
	_, err = reconciler.reconcileDynamicMatrices(context.Background(), run, &actionsv1alpha1.WorkflowJobList{})
	terminal := &terminalPlanningError{}
	if !errors.As(err, &terminal) || !strings.Contains(err.Error(), "missing-plan-build-matrix-plan") || !strings.Contains(err.Error(), run.Name) {
		t.Fatalf("reconciliation error = %v, want terminal missing-plan error naming the ConfigMap and WorkflowRun", err)
	}
}

func TestReconcileDynamicMatricesRefreshesJobsBetweenExpansions(t *testing.T) {
	scheme := runnerTestScheme(t)
	run := &actionsv1alpha1.WorkflowRun{
		ObjectMeta: metav1.ObjectMeta{Name: "dynamic", Namespace: "default", UID: types.UID("run-uid")},
		Spec:       actionsv1alpha1.WorkflowRunSpec{Source: actionsv1alpha1.WorkflowRunSource{GitHub: &actionsv1alpha1.GitHubWorkflowRunSource{}}},
	}
	setTestWorkflowRunIdentity(run)
	project := &actionsv1alpha1.Project{ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: "default", UID: types.UID("project-uid")}}
	definition, err := workflow.Parse([]byte("name: Dynamic\non: push\njobs:\n  prepare:\n    runs-on: ubuntu-latest\n    steps:\n      - run: prepare\n  alpha:\n    needs: prepare\n    strategy:\n      matrix:\n        value: ${{ fromJSON(needs.prepare.outputs.values) }}\n    runs-on: ubuntu-latest\n    steps:\n      - run: alpha\n  zulu:\n    needs: prepare\n    strategy:\n      matrix:\n        value: ${{ fromJSON(needs.prepare.outputs.values) }}\n    runs-on: ubuntu-latest\n    steps:\n      - run: zulu\n"))
	if err != nil {
		t.Fatal(err)
	}
	clusterClient := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&actionsv1alpha1.WorkflowRun{}, &actionsv1alpha1.WorkflowJob{}).
		WithObjects(run).
		Build()
	reconciler := &WorkflowRunReconciler{Client: clusterClient, APIReader: clusterClient}
	planned, deferred, err := reconciler.planWorkflowJobs(run, definition, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := reconciler.ensureWorkflowPlan(context.Background(), run, project, planned, deferred); err != nil {
		t.Fatal(err)
	}
	if err := reconciler.ensureWorkflowJobs(context.Background(), run, project, planned); err != nil {
		t.Fatal(err)
	}

	jobs := &actionsv1alpha1.WorkflowJobList{}
	if err := clusterClient.List(context.Background(), jobs, client.InNamespace(run.Namespace), client.MatchingLabels{actionsv1alpha1.LabelWorkflowRunUID: string(run.UID)}); err != nil {
		t.Fatal(err)
	}
	prepare := &jobs.Items[0]
	prepare.Status.Result = actionsv1alpha1.WorkflowJobResultSuccess
	prepare.Status.Outputs = map[string]string{"values": `["one"]`}
	if err := clusterClient.Status().Update(context.Background(), prepare); err != nil {
		t.Fatal(err)
	}

	for wantJobs := 2; wantJobs <= 3; wantJobs++ {
		jobs = &actionsv1alpha1.WorkflowJobList{}
		if err := clusterClient.List(context.Background(), jobs, client.InNamespace(run.Namespace), client.MatchingLabels{actionsv1alpha1.LabelWorkflowRunUID: string(run.UID)}); err != nil {
			t.Fatal(err)
		}
		state, err := reconciler.reconcileDynamicMatrices(context.Background(), run, jobs)
		if err != nil {
			t.Fatal(err)
		}
		if !state.changed {
			t.Fatalf("expansion pass for %d jobs did not report a change", wantJobs)
		}
		stored := &actionsv1alpha1.WorkflowJobList{}
		if err := clusterClient.List(context.Background(), stored, client.InNamespace(run.Namespace), client.MatchingLabels{actionsv1alpha1.LabelWorkflowRunUID: string(run.UID)}); err != nil {
			t.Fatal(err)
		}
		if len(stored.Items) != wantJobs {
			t.Fatalf("WorkflowJobs after expansion pass = %d, want %d", len(stored.Items), wantJobs)
		}
	}
}

func TestReconcileDynamicMatrixFailsInvalidOutput(t *testing.T) {
	scheme := runnerTestScheme(t)
	run := &actionsv1alpha1.WorkflowRun{
		ObjectMeta: metav1.ObjectMeta{Name: "invalid-matrix", Namespace: "default", UID: types.UID("run-uid")},
		Spec: actionsv1alpha1.WorkflowRunSpec{Source: actionsv1alpha1.WorkflowRunSource{Type: actionsv1alpha1.SourceTypeGitHub, GitHub: &actionsv1alpha1.GitHubWorkflowRunSource{
			Repository: actionsv1alpha1.GitHubRepository{ID: 1, Owner: "acme", Name: "example"},
			Event:      actionsv1alpha1.GitHubEvent{Name: actionsv1alpha1.GitHubEventNamePush}, Revision: actionsv1alpha1.GitRevision{SHA: strings.Repeat("a", 40), Ref: "refs/heads/main"},
		}}},
	}
	setTestWorkflowRunIdentity(run)
	project := &actionsv1alpha1.Project{ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: "default", UID: types.UID("project-uid")}}
	definition, err := workflow.Parse([]byte("name: Dynamic\non: push\njobs:\n  prepare:\n    runs-on: ubuntu-latest\n    steps:\n      - run: prepare\n  build:\n    needs: prepare\n    strategy:\n      matrix: ${{ fromJSON(needs.prepare.outputs.matrix) }}\n    runs-on: ubuntu-latest\n    steps:\n      - run: build\n"))
	if err != nil {
		t.Fatal(err)
	}
	prepare := &actionsv1alpha1.WorkflowJob{
		ObjectMeta: metav1.ObjectMeta{Name: "prepare", Namespace: run.Namespace, Labels: map[string]string{actionsv1alpha1.LabelWorkflowRunUID: string(run.UID)}},
		Spec:       actionsv1alpha1.WorkflowJobSpec{WorkflowRunRef: corev1.LocalObjectReference{Name: run.Name}, JobID: "prepare", RunsOn: []string{"ubuntu-latest"}},
		Status:     actionsv1alpha1.WorkflowJobStatus{Result: actionsv1alpha1.WorkflowJobResultSuccess, Outputs: map[string]string{"matrix": "not-json"}},
	}
	clusterClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&actionsv1alpha1.WorkflowJob{}).WithObjects(run, prepare).Build()
	reconciler := &WorkflowRunReconciler{Client: clusterClient, APIReader: clusterClient}
	_, deferred, err := reconciler.planWorkflowJobs(run, definition, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := reconciler.ensureWorkflowPlan(context.Background(), run, project, nil, deferred); err != nil {
		t.Fatal(err)
	}
	jobs := &actionsv1alpha1.WorkflowJobList{}
	if err := clusterClient.List(context.Background(), jobs, client.InNamespace(run.Namespace), client.MatchingLabels{actionsv1alpha1.LabelWorkflowRunUID: string(run.UID)}); err != nil {
		t.Fatal(err)
	}
	state, err := reconciler.reconcileDynamicMatrices(context.Background(), run, jobs)
	if err != nil {
		t.Fatal(err)
	}
	if !state.changed {
		t.Fatal("invalid matrix did not create a terminal result job")
	}
	failed := &actionsv1alpha1.WorkflowJob{}
	if err := clusterClient.Get(context.Background(), client.ObjectKey{Namespace: run.Namespace, Name: workflowJobName(run.Name, "build")}, failed); err != nil {
		t.Fatal(err)
	}
	if failed.Status.Result != actionsv1alpha1.WorkflowJobResultFailure {
		t.Fatalf("result = %q, want failure", failed.Status.Result)
	}
	condition := meta.FindStatusCondition(failed.Status.Conditions, actionsv1alpha1.WorkflowJobConditionSucceeded)
	if condition == nil || condition.Reason != "MatrixEvaluationFailed" || !strings.Contains(condition.Message, "parse JSON") {
		t.Fatalf("Succeeded condition = %#v", condition)
	}
}

func TestWorkflowEventIncludesRevisionValues(t *testing.T) {
	event := workflowEvent(&actionsv1alpha1.GitHubWorkflowRunSource{
		Event: actionsv1alpha1.GitHubEvent{
			Name: "pull_request_target", Action: "synchronize", Inputs: map[string]string{"environment": "staging"},
			PullRequest: &actionsv1alpha1.GitHubPullRequest{
				Number: 7, Body: "Pull request body", HTMLURL: "https://github.com/contributor/project/pull/7",
				HeadRef: "feature", HeadSHA: strings.Repeat("a", 40), BaseRef: "target",
				HeadRepository: actionsv1alpha1.GitHubRepository{ID: 2, Owner: "contributor", Name: "project"},
			},
		},
		Revision: actionsv1alpha1.GitRevision{Ref: "refs/heads/main"},
	})
	if event.Name != "pull_request_target" || event.Action != "synchronize" || event.Ref != "refs/heads/main" || event.RefName != "main" || event.HeadRef != "feature" || event.BaseRef != "target" || event.Inputs["environment"] != "staging" || event.PullRequest == nil || event.PullRequest.Number != 7 || event.PullRequest.Body != "Pull request body" || event.PullRequest.HTMLURL != "https://github.com/contributor/project/pull/7" || event.PullRequest.HeadRepository.Owner != "contributor" {
		t.Fatalf("workflow event = %#v", event)
	}
}

func TestPullRequestTargetExpressionSeparatesTrustedBaseFromForkHead(t *testing.T) {
	baseSHA := strings.Repeat("a", 40)
	headSHA := strings.Repeat("b", 40)
	source := &actionsv1alpha1.GitHubWorkflowRunSource{
		Repository: actionsv1alpha1.GitHubRepository{ID: 1, Owner: "acme", Name: "example"},
		Event: actionsv1alpha1.GitHubEvent{
			Name: actionsv1alpha1.GitHubEventNamePullRequestTarget,
			PullRequest: &actionsv1alpha1.GitHubPullRequest{
				Number: 42, HeadRef: "feature", HeadSHA: headSHA, BaseRef: "main",
				HeadRepository: actionsv1alpha1.GitHubRepository{ID: 2, Owner: "contributor", Name: "example"},
			},
		},
		Revision: actionsv1alpha1.GitRevision{SHA: baseSHA, Ref: "refs/heads/main"},
	}
	context := (&WorkflowRunReconciler{}).jobExpressionContext(&actionsv1alpha1.WorkflowRun{
		Spec: actionsv1alpha1.WorkflowRunSpec{Source: actionsv1alpha1.WorkflowRunSource{GitHub: source}},
	}, "Fork checks", nil, nil, nil)
	github := context.Values["github"].(map[string]any)
	pullRequest := github["event"].(map[string]any)["pull_request"].(map[string]any)
	base := pullRequest["base"].(map[string]any)
	head := pullRequest["head"].(map[string]any)

	if github["sha"] != baseSHA || github["ref"] != "refs/heads/main" || base["sha"] != baseSHA || base["ref"] != "main" {
		t.Fatalf("trusted base context = %#v", github)
	}
	if head["sha"] != headSHA || head["ref"] != "feature" || pullRequest["merge_ref"] != "refs/pull/42/merge" {
		t.Fatalf("untrusted pull request context = %#v", pullRequest)
	}
}

func TestResolvePlanningEventInputs(t *testing.T) {
	definition, err := workflow.Parse([]byte("name: Manual\non:\n  workflow_dispatch:\n    inputs:\n      namespace:\n        required: true\n        type: string\n      dry-run:\n        type: boolean\n        default: false\n      retries:\n        type: number\njobs:\n  run:\n    runs-on: ubuntu-latest\n    steps:\n      - run: make deploy\n"))
	if err != nil {
		t.Fatal(err)
	}
	run := &actionsv1alpha1.WorkflowRun{Spec: actionsv1alpha1.WorkflowRunSpec{Source: actionsv1alpha1.WorkflowRunSource{
		Type: actionsv1alpha1.SourceTypeGitHub,
		GitHub: &actionsv1alpha1.GitHubWorkflowRunSource{Event: actionsv1alpha1.GitHubEvent{
			Name: actionsv1alpha1.GitHubEventNameWorkflowDispatch, Inputs: map[string]string{"namespace": "default", "retries": "1.10"},
		}},
	}}}
	planningRun, event, err := resolvePlanningEvent(run, definition, nil)
	if err != nil {
		t.Fatal(err)
	}
	if planningRun.Spec.Source.GitHub.Event.Inputs["namespace"] != "default" || planningRun.Spec.Source.GitHub.Event.Inputs["dry-run"] != "false" || planningRun.Spec.Source.GitHub.Event.Inputs["retries"] != "1.1" {
		t.Fatalf("resolved inputs = %#v", planningRun.Spec.Source.GitHub.Event.Inputs)
	}
	if _, found := run.Spec.Source.GitHub.Event.Inputs["dry-run"]; found {
		t.Fatal("input defaults mutated the WorkflowRun spec")
	}
	if event.InputValues["namespace"] != "default" || event.InputValues["dry-run"] != false || event.InputValues["retries"] != float64(1.1) {
		t.Fatalf("expression inputs = %#v", event.InputValues)
	}

	run.Spec.Source.GitHub.Event.Name = actionsv1alpha1.GitHubEventNameWorkflowCall
	if _, _, err := resolvePlanningEvent(run, definition, nil); err == nil {
		t.Fatal("workflow_call matched a workflow_dispatch declaration")
	}
}

func TestResolvePlanningEventValidatesSchedule(t *testing.T) {
	definition, err := workflow.Parse([]byte("name: Scheduled\non:\n  schedule:\n    - cron: '0 6 * * *'\njobs:\n  run:\n    runs-on: ubuntu-latest\n    steps:\n      - run: make refresh\n"))
	if err != nil {
		t.Fatal(err)
	}
	run := &actionsv1alpha1.WorkflowRun{Spec: actionsv1alpha1.WorkflowRunSpec{Source: actionsv1alpha1.WorkflowRunSource{
		Type: actionsv1alpha1.SourceTypeGitHub,
		GitHub: &actionsv1alpha1.GitHubWorkflowRunSource{Event: actionsv1alpha1.GitHubEvent{
			Name: actionsv1alpha1.GitHubEventNameSchedule, Schedule: "0 6 * * *",
		}},
	}}}
	if _, _, err := resolvePlanningEvent(run, definition, nil); err != nil {
		t.Fatalf("matching schedule was rejected: %v", err)
	}
	run.Spec.Source.GitHub.Event.Schedule = "x x x x x"
	if _, _, err := resolvePlanningEvent(run, definition, nil); err == nil {
		t.Fatal("invalid schedule was accepted")
	}
}

func TestGitHubCheckRunIsCreatedAndRecorded(t *testing.T) {
	executionSHA := strings.Repeat("a", 40)
	tests := []struct {
		name         string
		eventName    actionsv1alpha1.GitHubEventName
		headSHA      string
		checkHeadSHA string
	}{
		{
			name:         "pull request head SHA",
			eventName:    actionsv1alpha1.GitHubEventNamePullRequest,
			headSHA:      strings.Repeat("b", 40),
			checkHeadSHA: strings.Repeat("b", 40),
		},
		{
			name:         "execution SHA fallback",
			eventName:    actionsv1alpha1.GitHubEventNamePush,
			checkHeadSHA: executionSHA,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			testGitHubCheckRunLifecycle(t, executionSHA, test.headSHA, test.checkHeadSHA, test.eventName)
		})
	}
}

func testGitHubCheckRunLifecycle(t *testing.T, executionSHA, headSHA, checkHeadSHA string, eventName actionsv1alpha1.GitHubEventName) {
	t.Helper()
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	privateKeyData := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(privateKey)})
	created := 0
	updated := 0
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch {
		case request.URL.Path == "/app/installations/2/access_tokens":
			body := struct {
				Permissions map[string]string `json:"permissions"`
			}{}
			if err := json.NewDecoder(request.Body).Decode(&body); err != nil || body.Permissions["checks"] != "write" || len(body.Permissions) != 1 {
				http.Error(writer, "unexpected permissions", http.StatusBadRequest)
				return
			}
			fmt.Fprint(writer, `{"token":"checks-token"}`)
		case request.Method == http.MethodGet && request.URL.Path == "/repos/acme/example/commits/"+checkHeadSHA+"/check-runs":
			if created == 0 {
				fmt.Fprint(writer, `{"total_count":0,"check_runs":[]}`)
			} else {
				fmt.Fprint(writer, `{"total_count":1,"check_runs":[{"id":17,"external_id":"run-uid","status":"completed","conclusion":"success"}]}`)
			}
		case request.Method == http.MethodPost && request.URL.Path == "/repos/acme/example/check-runs":
			body := githubclient.CreateCheckRunRequest{}
			if err := json.NewDecoder(request.Body).Decode(&body); err != nil || body.ExternalID != "run-uid" || body.Status != "queued" || body.HeadSHA != checkHeadSHA {
				http.Error(writer, "unexpected check run", http.StatusBadRequest)
				return
			}
			created++
			fmt.Fprint(writer, `{"id":17,"external_id":"run-uid","status":"queued"}`)
		case request.Method == http.MethodPatch && request.URL.Path == "/repos/acme/example/check-runs/17":
			body := githubclient.UpdateCheckRunRequest{}
			if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
				http.Error(writer, "unexpected check update", http.StatusBadRequest)
				return
			}
			switch updated {
			case 0:
				if body.Status != "queued" || body.Output == nil || body.Output.Title != "CI" {
					http.Error(writer, "unexpected queued check update", http.StatusBadRequest)
					return
				}
			case 1:
				if body.Status != "completed" || body.Conclusion != "success" {
					http.Error(writer, "unexpected completed check update", http.StatusBadRequest)
					return
				}
			case 2:
				if body.Status != "queued" || body.ExternalID != "run-uid" || body.Output == nil || body.Output.Title != "CI (attempt 2)" {
					http.Error(writer, "unexpected rerun check update", http.StatusBadRequest)
					return
				}
			default:
				http.Error(writer, "unexpected extra check update", http.StatusBadRequest)
				return
			}
			updated++
			fmt.Fprintf(writer, `{"id":17,"external_id":"run-uid","status":%q,"conclusion":%q}`, body.Status, body.Conclusion)
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
			ProjectRef:   corev1.LocalObjectReference{Name: project.Name},
			WorkflowPath: ".open-actions/workflows/ci.yaml",
			Source: actionsv1alpha1.WorkflowRunSource{Type: actionsv1alpha1.SourceTypeGitHub, GitHub: &actionsv1alpha1.GitHubWorkflowRunSource{
				Repository: actionsv1alpha1.GitHubRepository{ID: 3, Owner: "acme", Name: "example"},
				Event:      actionsv1alpha1.GitHubEvent{Name: eventName},
				Revision:   actionsv1alpha1.GitRevision{SHA: executionSHA, HeadSHA: headSHA},
			}},
		},
	}
	clusterClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&actionsv1alpha1.WorkflowRun{}).WithObjects(project, secret, run).Build()
	reconciler := &WorkflowRunReconciler{Client: clusterClient, APIReader: clusterClient, GitHub: github}
	if err := reconciler.reconcileGitHubCheck(context.Background(), run); err != nil {
		t.Fatal(err)
	}
	stored := &actionsv1alpha1.WorkflowRun{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(run), stored); err != nil {
		t.Fatal(err)
	}
	checkRun := workflowRunCheckRunStatus(stored)
	if checkRun == nil || checkRun.ID != 17 || checkRun.Status != "queued" || checkRun.ReportDigest == "" || created != 1 {
		t.Fatalf("check run status = %#v, creates = %d", checkRun, created)
	}
	stored.Status.WorkflowName = "CI"
	if err := reconciler.reconcileGitHubCheck(context.Background(), stored); err != nil {
		t.Fatal(err)
	}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(run), stored); err != nil {
		t.Fatal(err)
	}
	completionTime := metav1.Now()
	stored.Status.CompletionTime = &completionTime
	meta.SetStatusCondition(&stored.Status.Conditions, metav1.Condition{Type: actionsv1alpha1.WorkflowRunConditionSucceeded, Status: metav1.ConditionTrue, Reason: "JobsSucceeded", Message: "All jobs succeeded", LastTransitionTime: completionTime})
	if err := reconciler.reconcileGitHubCheck(context.Background(), stored); err != nil {
		t.Fatal(err)
	}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(run), stored); err != nil {
		t.Fatal(err)
	}
	if updated != 2 || workflowRunCheckRunStatus(stored).Conclusion != "success" {
		t.Fatalf("updates = %d, check run = %#v", updated, workflowRunCheckRunStatus(stored))
	}

	retry := &actionsv1alpha1.WorkflowRun{
		ObjectMeta: metav1.ObjectMeta{Name: "ci-attempt-2", Namespace: stored.Namespace, UID: "retry-uid", Labels: map[string]string{actionsv1alpha1.LabelWorkflowRunRootUID: "run-uid"}},
		Spec:       *stored.Spec.DeepCopy(),
		Status:     actionsv1alpha1.WorkflowRunStatus{WorkflowName: "CI"},
	}
	retry.Spec.Rerun = &actionsv1alpha1.WorkflowRunRerun{
		OriginalRunRef: actionsv1alpha1.WorkflowRunReference{Name: stored.Name, UID: stored.UID},
		PreviousRunRef: actionsv1alpha1.WorkflowRunReference{Name: stored.Name, UID: stored.UID},
		Attempt:        2,
		JobIDs:         []string{"unit"},
	}
	if err := clusterClient.Create(context.Background(), retry); err != nil {
		t.Fatal(err)
	}
	if err := reconciler.reconcileGitHubCheck(context.Background(), retry); err != nil {
		t.Fatal(err)
	}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(retry), retry); err != nil {
		t.Fatal(err)
	}
	if created != 1 || updated != 3 || workflowRunCheckRunStatus(retry) == nil || workflowRunCheckRunStatus(retry).ID != 17 {
		t.Fatalf("rerun creates = %d, updates = %d, check run = %#v", created, updated, workflowRunCheckRunStatus(retry))
	}
	reconciler.APIReader = &workflowRunListErrorReader{Reader: clusterClient, err: errors.New("unexpected WorkflowRun list")}
	if err := reconciler.reconcileGitHubCheck(context.Background(), retry); err != nil {
		t.Fatalf("unchanged check report listed WorkflowRuns: %v", err)
	}
	reconciler.APIReader = clusterClient
	stale := stored.DeepCopy()
	stale.Status.Source.GitHub.CheckRun.ReportDigest = ""
	if err := reconciler.reconcileGitHubCheck(context.Background(), stale); err != nil {
		t.Fatal(err)
	}
	if updated != 3 {
		t.Fatalf("older attempt updated the shared check; updates = %d", updated)
	}
}

func TestWorkflowRunCheckReportMapsLifecycle(t *testing.T) {
	run := &actionsv1alpha1.WorkflowRun{Spec: actionsv1alpha1.WorkflowRunSpec{WorkflowPath: ".open-actions/workflows/ci.yaml"}}
	if report := workflowRunCheckReport(run); report.Status != "queued" || report.Conclusion != "" {
		t.Fatalf("queued report = %#v", report)
	}
	run.Spec.ForkPullRequest = &actionsv1alpha1.WorkflowRunForkPullRequest{RequireApproval: true}
	if report := workflowRunCheckReport(run); report.Status != "queued" || report.Output.Summary != "The workflow is waiting for approval before jobs are created." {
		t.Fatalf("approval report = %#v", report)
	}
	run.Spec.ForkPullRequest.Approved = true
	start := metav1.Now()
	run.Status.StartTime = &start
	if report := workflowRunCheckReport(run); report.Status != "in_progress" || report.StartedAt == "" {
		t.Fatalf("running report = %#v", report)
	}
	completion := metav1.Now()
	run.Status.CompletionTime = &completion
	meta.SetStatusCondition(&run.Status.Conditions, metav1.Condition{Type: actionsv1alpha1.WorkflowRunConditionSucceeded, Status: metav1.ConditionFalse, Reason: "JobFailed", Message: "A job failed", LastTransitionTime: completion})
	if report := workflowRunCheckReport(run); report.Status != "completed" || report.Conclusion != "failure" || report.CompletedAt == "" {
		t.Fatalf("failed report = %#v", report)
	}
	meta.SetStatusCondition(&run.Status.Conditions, metav1.Condition{Type: actionsv1alpha1.WorkflowRunConditionSucceeded, Status: metav1.ConditionFalse, Reason: "JobCancelled", Message: "A job was cancelled", LastTransitionTime: completion})
	if report := workflowRunCheckReport(run); report.Status != "completed" || report.Conclusion != "cancelled" {
		t.Fatalf("cancelled report = %#v", report)
	}
	meta.SetStatusCondition(&run.Status.Conditions, metav1.Condition{Type: actionsv1alpha1.WorkflowRunConditionSucceeded, Status: metav1.ConditionFalse, Reason: "RevisionSuperseded", Message: "The approved revision was superseded", LastTransitionTime: completion})
	if report := workflowRunCheckReport(run); report.Status != "completed" || report.Conclusion != "cancelled" {
		t.Fatalf("superseded report = %#v", report)
	}
	meta.SetStatusCondition(&run.Status.Conditions, metav1.Condition{Type: actionsv1alpha1.WorkflowRunConditionSucceeded, Status: metav1.ConditionFalse, Reason: "JobTimedOut", Message: "A job timed out", LastTransitionTime: completion})
	if report := workflowRunCheckReport(run); report.Status != "completed" || report.Conclusion != "timed_out" {
		t.Fatalf("timed-out report = %#v", report)
	}
	deletionTime := metav1.NewTime(time.Unix(1_700_000_000, 0))
	canceled := &actionsv1alpha1.WorkflowRun{ObjectMeta: metav1.ObjectMeta{DeletionTimestamp: &deletionTime}}
	if report := workflowRunCheckReport(canceled); report.Status != "completed" || report.Conclusion != "cancelled" || report.CompletedAt != deletionTime.UTC().Format(time.RFC3339) {
		t.Fatalf("canceled report = %#v", report)
	}
}

func TestRerunWorkflowJobSelectionAndCheckIdentity(t *testing.T) {
	run := &actionsv1alpha1.WorkflowRun{Spec: actionsv1alpha1.WorkflowRunSpec{
		WorkflowPath: ".open-actions/workflows/ci.yaml",
		Rerun: &actionsv1alpha1.WorkflowRunRerun{
			OriginalRunRef: actionsv1alpha1.WorkflowRunReference{Name: "ci", UID: "original-uid"},
			PreviousRunRef: actionsv1alpha1.WorkflowRunReference{Name: "ci", UID: "original-uid"},
			Attempt:        2,
			JobIDs:         []string{"unit-matrix-2", "integration"},
		},
	}}
	planned := []plannedWorkflowJob{
		{id: "setup"},
		{id: "integration", needs: []string{"setup"}},
		{id: "unit-matrix-1", matrix: &actionsv1alpha1.WorkflowJobMatrix{LogicalJobID: "unit"}},
		{id: "unit-matrix-2", needs: []string{"setup"}, matrix: &actionsv1alpha1.WorkflowJobMatrix{LogicalJobID: "unit"}},
	}

	selected, err := selectRerunWorkflowJobs(run, planned)
	if err != nil {
		t.Fatal(err)
	}
	if len(selected) != 3 || selected[0].id != "setup" || selected[1].id != "integration" || selected[2].id != "unit-matrix-2" {
		t.Fatalf("selected jobs = %#v", selected)
	}
	if externalID := workflowRunCheckExternalID(run); externalID != "original-uid" {
		t.Fatalf("check external ID = %q", externalID)
	}
	report := workflowRunCheckReport(run)
	if report.Output.Title != ".open-actions/workflows/ci.yaml (attempt 2)" || !strings.Contains(report.Output.Text, "2 requested jobs plus required dependencies") {
		t.Fatalf("rerun check report = %#v", report)
	}

	run.Spec.Rerun.JobIDs = []string{"missing"}
	if _, err := selectRerunWorkflowJobs(run, planned); err == nil || !strings.Contains(err.Error(), "missing") {
		t.Fatalf("missing job selection error = %v", err)
	}
}

func TestValidateWorkflowRunRerun(t *testing.T) {
	terminalConditions := []metav1.Condition{{
		Type: actionsv1alpha1.WorkflowRunConditionSucceeded, Status: metav1.ConditionTrue, Reason: "JobsSucceeded",
	}}
	tests := []struct {
		name         string
		omitPrevious bool
		mutate       func(*actionsv1alpha1.WorkflowRun, *actionsv1alpha1.WorkflowRun)
		wantError    string
	}{
		{name: "valid"},
		{name: "missing previous", omitPrevious: true, wantError: "does not exist"},
		{name: "UID mismatch", mutate: func(_ *actionsv1alpha1.WorkflowRun, run *actionsv1alpha1.WorkflowRun) {
			run.Spec.Rerun.PreviousRunRef.UID = "recreated-uid"
		}, wantError: "different UID"},
		{name: "previous still running", mutate: func(previous, _ *actionsv1alpha1.WorkflowRun) {
			previous.Status.Conditions = nil
		}, wantError: "is not complete"},
		{name: "previous without identity", mutate: func(previous, _ *actionsv1alpha1.WorkflowRun) {
			previous.Status.Identity = nil
		}, wantError: "has no run identity"},
		{name: "spec mismatch", mutate: func(_ *actionsv1alpha1.WorkflowRun, run *actionsv1alpha1.WorkflowRun) {
			run.Spec.WorkflowPath = ".open-actions/workflows/other.yaml"
		}, wantError: "does not match previous"},
		{name: "original mismatch", mutate: func(_ *actionsv1alpha1.WorkflowRun, run *actionsv1alpha1.WorkflowRun) {
			run.Spec.Rerun.OriginalRunRef.Name = "other"
		}, wantError: "does not identify previous"},
		{name: "previous rerun lineage mismatch", mutate: func(previous, run *actionsv1alpha1.WorkflowRun) {
			previous.Spec.Rerun = &actionsv1alpha1.WorkflowRunRerun{
				OriginalRunRef: actionsv1alpha1.WorkflowRunReference{Name: "root", UID: "root-uid"},
				PreviousRunRef: actionsv1alpha1.WorkflowRunReference{Name: "root", UID: "root-uid"},
				Attempt:        2,
			}
			run.Spec.Rerun.Attempt = 3
		}, wantError: "does not match previous"},
		{name: "wrong attempt", mutate: func(_ *actionsv1alpha1.WorkflowRun, run *actionsv1alpha1.WorkflowRun) {
			run.Spec.Rerun.Attempt = 3
		}, wantError: "must follow attempt 1"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			if err := actionsv1alpha1.AddToScheme(scheme); err != nil {
				t.Fatal(err)
			}
			previous := &actionsv1alpha1.WorkflowRun{
				ObjectMeta: metav1.ObjectMeta{Name: "ci", Namespace: "default", UID: "previous-uid"},
				Spec: actionsv1alpha1.WorkflowRunSpec{
					ProjectRef: corev1.LocalObjectReference{Name: "project"}, WorkflowPath: ".open-actions/workflows/ci.yaml",
				},
				Status: actionsv1alpha1.WorkflowRunStatus{
					Identity:   &actionsv1alpha1.WorkflowRunIdentityStatus{ID: 1, Number: 1, Attempt: 1},
					Conditions: append([]metav1.Condition(nil), terminalConditions...),
				},
			}
			run := &actionsv1alpha1.WorkflowRun{
				ObjectMeta: metav1.ObjectMeta{Name: "ci-attempt-2", Namespace: previous.Namespace, UID: "attempt-uid"},
				Spec:       *previous.Spec.DeepCopy(),
			}
			run.Spec.Rerun = &actionsv1alpha1.WorkflowRunRerun{
				OriginalRunRef: actionsv1alpha1.WorkflowRunReference{Name: previous.Name, UID: previous.UID},
				PreviousRunRef: actionsv1alpha1.WorkflowRunReference{Name: previous.Name, UID: previous.UID},
				Attempt:        2,
			}
			if test.mutate != nil {
				test.mutate(previous, run)
			}
			objects := []client.Object{}
			if !test.omitPrevious {
				objects = append(objects, previous)
			}
			clusterClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()
			reconciler := &WorkflowRunReconciler{Client: clusterClient, APIReader: clusterClient}

			_, err := reconciler.validateWorkflowRunRerun(context.Background(), run)
			if test.wantError == "" && err != nil {
				t.Fatalf("valid rerun rejected: %v", err)
			}
			if test.wantError != "" && (err == nil || !strings.Contains(err.Error(), test.wantError)) {
				t.Fatalf("validation error = %v, want substring %q", err, test.wantError)
			}
		})
	}
}

func TestWorkflowRunIdentityPersistsAcrossRerunsAndControllerRestarts(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := actionsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	project := &actionsv1alpha1.Project{
		TypeMeta:   metav1.TypeMeta{APIVersion: actionsv1alpha1.GroupVersion.String(), Kind: "Project"},
		ObjectMeta: metav1.ObjectMeta{Name: "project", Namespace: "default", UID: "project-uid"},
	}
	root := &actionsv1alpha1.WorkflowRun{
		ObjectMeta: metav1.ObjectMeta{Name: "ci", Namespace: "default", UID: "root-uid"},
		Spec: actionsv1alpha1.WorkflowRunSpec{
			ProjectRef:   corev1.LocalObjectReference{Name: project.Name},
			WorkflowPath: ".open-actions/workflows/ci.yaml",
			Source: actionsv1alpha1.WorkflowRunSource{Type: actionsv1alpha1.SourceTypeGitHub, GitHub: &actionsv1alpha1.GitHubWorkflowRunSource{
				Actor:      "octocat",
				Repository: actionsv1alpha1.GitHubRepository{ID: 1, Owner: "acme", Name: "example"},
				Event:      actionsv1alpha1.GitHubEvent{Name: actionsv1alpha1.GitHubEventNamePush, DeliveryID: "delivery"},
				Revision:   actionsv1alpha1.GitRevision{SHA: strings.Repeat("a", 40), Ref: "refs/heads/main"},
			}},
		},
	}
	clusterClient := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&actionsv1alpha1.WorkflowRun{}).
		WithObjects(project, root).Build()
	reconciler := &WorkflowRunReconciler{
		Client: clusterClient, APIReader: clusterClient, ConsoleURL: "https://actions.example",
	}
	if err := reconciler.ensureWorkflowRunIdentity(context.Background(), root, project, nil); err != nil {
		t.Fatal(err)
	}
	if root.Status.Identity == nil || root.Status.Identity.ID != 1 || root.Status.Identity.Number != 1 || root.Status.Identity.Attempt != 1 || root.Status.Identity.URL != "https://actions.example/runs/default/ci" {
		t.Fatalf("root identity = %#v", root.Status.Identity)
	}

	reloadedRoot := &actionsv1alpha1.WorkflowRun{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(root), reloadedRoot); err != nil {
		t.Fatal(err)
	}
	restarted := &WorkflowRunReconciler{Client: clusterClient, APIReader: clusterClient, ConsoleURL: "https://actions.example"}
	if err := restarted.ensureWorkflowRunIdentity(context.Background(), reloadedRoot, project, nil); err != nil {
		t.Fatal(err)
	}
	if reloadedRoot.Status.Identity.ID != root.Status.Identity.ID || reloadedRoot.Status.Identity.Number != root.Status.Identity.Number {
		t.Fatalf("reloaded root identity = %#v", reloadedRoot.Status.Identity)
	}

	rerun := root.DeepCopy()
	rerun.ResourceVersion = ""
	rerun.Name = "ci-attempt-2"
	rerun.UID = "rerun-uid"
	rerun.Status = actionsv1alpha1.WorkflowRunStatus{}
	rerun.Spec.Rerun = &actionsv1alpha1.WorkflowRunRerun{
		OriginalRunRef: actionsv1alpha1.WorkflowRunReference{Name: root.Name, UID: root.UID},
		PreviousRunRef: actionsv1alpha1.WorkflowRunReference{Name: root.Name, UID: root.UID},
		Attempt:        2,
	}
	if err := clusterClient.Create(context.Background(), rerun); err != nil {
		t.Fatal(err)
	}
	if err := restarted.ensureWorkflowRunIdentity(context.Background(), rerun, project, root); err != nil {
		t.Fatal(err)
	}
	if rerun.Status.Identity == nil || rerun.Status.Identity.ID != root.Status.Identity.ID || rerun.Status.Identity.Number != root.Status.Identity.Number || rerun.Status.Identity.Attempt != 2 || rerun.Status.Identity.URL != "https://actions.example/runs/default/ci-attempt-2" {
		t.Fatalf("rerun identity = %#v", rerun.Status.Identity)
	}

	recreatedProject := project.DeepCopy()
	recreatedProject.UID = "recreated-project-uid"
	nextRun := root.DeepCopy()
	nextRun.ResourceVersion = ""
	nextRun.Name = "ci-next"
	nextRun.UID = "next-run-uid"
	nextRun.Status = actionsv1alpha1.WorkflowRunStatus{}
	if err := clusterClient.Create(context.Background(), nextRun); err != nil {
		t.Fatal(err)
	}
	if err := restarted.ensureWorkflowRunIdentity(context.Background(), nextRun, recreatedProject, nil); err != nil {
		t.Fatal(err)
	}
	if nextRun.Status.Identity == nil || nextRun.Status.Identity.ID != 2 || nextRun.Status.Identity.Number != 2 || nextRun.Status.Identity.Attempt != 1 {
		t.Fatalf("identity after Project recreation = %#v", nextRun.Status.Identity)
	}

	otherWorkflow := root.DeepCopy()
	otherWorkflow.ResourceVersion = ""
	otherWorkflow.Name = "release"
	otherWorkflow.UID = "release-uid"
	otherWorkflow.Spec.WorkflowPath = ".open-actions/workflows/release.yaml"
	otherWorkflow.Status = actionsv1alpha1.WorkflowRunStatus{}
	if err := clusterClient.Create(context.Background(), otherWorkflow); err != nil {
		t.Fatal(err)
	}
	if err := restarted.ensureWorkflowRunIdentity(context.Background(), otherWorkflow, recreatedProject, nil); err != nil {
		t.Fatal(err)
	}
	if otherWorkflow.Status.Identity == nil || otherWorkflow.Status.Identity.ID != 3 || otherWorkflow.Status.Identity.Number != 1 {
		t.Fatalf("identity for second workflow path = %#v", otherWorkflow.Status.Identity)
	}

	meta.SetStatusCondition(&rerun.Status.Conditions, metav1.Condition{
		Type: actionsv1alpha1.WorkflowRunConditionSucceeded, Status: metav1.ConditionTrue, Reason: "JobsSucceeded",
	})
	if err := clusterClient.Status().Update(context.Background(), rerun); err != nil {
		t.Fatal(err)
	}
	attemptThree := rerun.DeepCopy()
	attemptThree.ResourceVersion = ""
	attemptThree.Name = "ci-attempt-3"
	attemptThree.UID = "third-attempt-uid"
	attemptThree.Status = actionsv1alpha1.WorkflowRunStatus{}
	attemptThree.Spec.Rerun = &actionsv1alpha1.WorkflowRunRerun{
		OriginalRunRef: actionsv1alpha1.WorkflowRunReference{Name: root.Name, UID: root.UID},
		PreviousRunRef: actionsv1alpha1.WorkflowRunReference{Name: rerun.Name, UID: rerun.UID},
		Attempt:        3,
	}
	if err := clusterClient.Create(context.Background(), attemptThree); err != nil {
		t.Fatal(err)
	}
	if err := clusterClient.Delete(context.Background(), root); err != nil {
		t.Fatal(err)
	}
	previous, err := restarted.validateWorkflowRunRerun(context.Background(), attemptThree)
	if err != nil {
		t.Fatalf("validate third attempt after root deletion: %v", err)
	}
	if err := restarted.ensureWorkflowRunIdentity(context.Background(), attemptThree, recreatedProject, previous); err != nil {
		t.Fatal(err)
	}
	if attemptThree.Status.Identity == nil || attemptThree.Status.Identity.ID != 1 || attemptThree.Status.Identity.Number != 1 || attemptThree.Status.Identity.Attempt != 3 {
		t.Fatalf("third attempt identity after root deletion = %#v", attemptThree.Status.Identity)
	}
}

func TestInvalidWorkflowRunRerunBecomesTerminal(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := actionsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	project := &actionsv1alpha1.Project{ObjectMeta: metav1.ObjectMeta{Name: "project", Namespace: "default"}}
	run := &actionsv1alpha1.WorkflowRun{
		ObjectMeta: metav1.ObjectMeta{Name: "ci-attempt-2", Namespace: "default", UID: "attempt-uid"},
		Spec: actionsv1alpha1.WorkflowRunSpec{
			ProjectRef: corev1.LocalObjectReference{Name: project.Name},
			Source: actionsv1alpha1.WorkflowRunSource{Type: actionsv1alpha1.SourceTypeGitHub, GitHub: &actionsv1alpha1.GitHubWorkflowRunSource{
				Actor:      "octocat",
				Repository: actionsv1alpha1.GitHubRepository{ID: 1, Owner: "acme", Name: "example"},
				Event:      actionsv1alpha1.GitHubEvent{Name: actionsv1alpha1.GitHubEventNameWorkflowDispatch},
				Revision:   actionsv1alpha1.GitRevision{SHA: strings.Repeat("a", 40), Ref: "refs/heads/main"},
			}},
			Rerun: &actionsv1alpha1.WorkflowRunRerun{
				OriginalRunRef: actionsv1alpha1.WorkflowRunReference{Name: "ci", UID: "root-uid"},
				PreviousRunRef: actionsv1alpha1.WorkflowRunReference{Name: "ci", UID: "root-uid"},
				Attempt:        2,
			},
		},
	}
	clusterClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&actionsv1alpha1.WorkflowRun{}).WithObjects(project, run).Build()
	reconciler := &WorkflowRunReconciler{Client: clusterClient, APIReader: clusterClient}
	if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(run)}); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(run)}); err != nil {
		t.Fatal(err)
	}
	stored := &actionsv1alpha1.WorkflowRun{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(run), stored); err != nil {
		t.Fatal(err)
	}
	planned := meta.FindStatusCondition(stored.Status.Conditions, actionsv1alpha1.WorkflowRunConditionPlanned)
	succeeded := meta.FindStatusCondition(stored.Status.Conditions, actionsv1alpha1.WorkflowRunConditionSucceeded)
	if planned == nil || planned.Status != metav1.ConditionFalse || planned.Reason != "RerunInvalid" || succeeded == nil || succeeded.Status != metav1.ConditionFalse || succeeded.Reason != "RerunInvalid" {
		t.Fatalf("invalid rerun conditions = %#v", stored.Status.Conditions)
	}
	if stored.Status.Identity != nil {
		t.Fatalf("invalid rerun identity = %#v", stored.Status.Identity)
	}
	sequences := &corev1.ConfigMapList{}
	if err := clusterClient.List(context.Background(), sequences); err != nil {
		t.Fatal(err)
	}
	if len(sequences.Items) != 0 {
		t.Fatalf("invalid rerun created %d run sequence ConfigMaps", len(sequences.Items))
	}
}

func TestWorkflowRunLineageLabelUsesOriginalUID(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := actionsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	run := &actionsv1alpha1.WorkflowRun{
		ObjectMeta: metav1.ObjectMeta{Name: "ci-attempt-2", Namespace: "default", UID: "attempt-uid"},
		Spec: actionsv1alpha1.WorkflowRunSpec{Rerun: &actionsv1alpha1.WorkflowRunRerun{
			OriginalRunRef: actionsv1alpha1.WorkflowRunReference{Name: "ci", UID: "original-uid"},
			PreviousRunRef: actionsv1alpha1.WorkflowRunReference{Name: "ci", UID: "original-uid"},
			Attempt:        2,
		}},
	}
	clusterClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(run).Build()
	reconciler := &WorkflowRunReconciler{Client: clusterClient}

	if err := reconciler.ensureWorkflowRunLineageLabel(context.Background(), run); err != nil {
		t.Fatal(err)
	}
	stored := &actionsv1alpha1.WorkflowRun{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(run), stored); err != nil {
		t.Fatal(err)
	}
	if value := stored.Labels[actionsv1alpha1.LabelWorkflowRunRootUID]; value != "original-uid" {
		t.Fatalf("lineage label = %q", value)
	}
}

func TestCompletedWorkflowRunTTL(t *testing.T) {
	now := time.Date(2026, time.August, 10, 12, 0, 0, 0, time.UTC)
	completedAt := metav1.NewTime(now.Add(-2 * time.Hour))
	tests := []struct {
		name       string
		ttl        *int32
		completion *metav1.Time
		transition metav1.Time
		wantExists bool
		wantAfter  time.Duration
	}{
		{name: "omitted", completion: &completedAt, wantExists: true},
		{name: "zero", ttl: pointerTo(int32(0)), completion: &completedAt},
		{name: "retained", ttl: pointerTo(int32(3 * 60 * 60)), completion: &completedAt, transition: completedAt, wantExists: true, wantAfter: time.Hour},
		{name: "expired", ttl: pointerTo(int32(60 * 60)), completion: &completedAt, transition: completedAt},
		{name: "condition transition fallback", ttl: pointerTo(int32(60 * 60)), transition: completedAt},
		{name: "future completion", ttl: pointerTo(int32(60 * 60)), completion: pointerTo(metav1.NewTime(now.Add(time.Hour))), transition: completedAt, wantExists: true, wantAfter: time.Hour},
		{name: "future completion with zero TTL", ttl: pointerTo(int32(0)), completion: pointerTo(metav1.NewTime(now.Add(time.Hour))), transition: completedAt, wantExists: true, wantAfter: time.Second},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			if err := actionsv1alpha1.AddToScheme(scheme); err != nil {
				t.Fatal(err)
			}
			run := &actionsv1alpha1.WorkflowRun{
				ObjectMeta: metav1.ObjectMeta{Name: "ci", Namespace: "default"},
				Spec:       actionsv1alpha1.WorkflowRunSpec{TTLSecondsAfterFinished: test.ttl},
				Status: actionsv1alpha1.WorkflowRunStatus{
					CompletionTime: test.completion,
					Conditions: []metav1.Condition{{
						Type: actionsv1alpha1.WorkflowRunConditionSucceeded, Status: metav1.ConditionTrue,
						Reason: "JobsSucceeded", LastTransitionTime: test.transition,
					}},
				},
			}
			clusterClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(run).Build()
			writeClient := &recordingDeleteClient{Client: clusterClient}
			reconciler := &WorkflowRunReconciler{Client: writeClient, APIReader: clusterClient, Now: func() time.Time { return now }}
			storedBefore := &actionsv1alpha1.WorkflowRun{}
			if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(run), storedBefore); err != nil {
				t.Fatal(err)
			}

			result, err := reconciler.reconcileCompletedWorkflowRunTTL(context.Background(), run)
			if err != nil {
				t.Fatal(err)
			}
			if result.RequeueAfter != test.wantAfter {
				t.Errorf("requeue after = %s, want %s", result.RequeueAfter, test.wantAfter)
			}
			stored := &actionsv1alpha1.WorkflowRun{}
			err = clusterClient.Get(context.Background(), client.ObjectKeyFromObject(run), stored)
			if test.wantExists && err != nil {
				t.Fatalf("retained WorkflowRun lookup failed: %v", err)
			}
			if !test.wantExists && !apierrors.IsNotFound(err) {
				t.Fatalf("expired WorkflowRun lookup error = %v, want not found", err)
			}
			if !test.wantExists {
				if writeClient.deleteOptions == nil || writeClient.deleteOptions.Preconditions == nil || writeClient.deleteOptions.Preconditions.ResourceVersion == nil {
					t.Fatal("expired WorkflowRun delete has no resourceVersion precondition")
				}
				if got := *writeClient.deleteOptions.Preconditions.ResourceVersion; got != storedBefore.ResourceVersion {
					t.Errorf("delete resourceVersion precondition = %q, want %q", got, storedBefore.ResourceVersion)
				}
			}
		})
	}
}

func TestCompletedWorkflowRunTTLUsesCurrentSpecBeforeDeleting(t *testing.T) {
	now := time.Date(2026, time.August, 10, 12, 0, 0, 0, time.UTC)
	completedAt := metav1.NewTime(now.Add(-2 * time.Hour))
	expiredTTL := int32(time.Hour / time.Second)
	extendedTTL := int32(3 * time.Hour / time.Second)
	cachedRun := &actionsv1alpha1.WorkflowRun{
		ObjectMeta: metav1.ObjectMeta{Name: "ci", Namespace: "default"},
		Spec:       actionsv1alpha1.WorkflowRunSpec{TTLSecondsAfterFinished: &expiredTTL},
		Status: actionsv1alpha1.WorkflowRunStatus{
			CompletionTime: &completedAt,
			Conditions: []metav1.Condition{{
				Type: actionsv1alpha1.WorkflowRunConditionSucceeded, Status: metav1.ConditionTrue,
				Reason: "JobsSucceeded", LastTransitionTime: completedAt,
			}},
		},
	}
	currentRun := cachedRun.DeepCopy()
	currentRun.Spec.TTLSecondsAfterFinished = &extendedTTL
	scheme := runtime.NewScheme()
	if err := actionsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	clusterClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(currentRun).Build()
	reconciler := &WorkflowRunReconciler{Client: clusterClient, APIReader: clusterClient, Now: func() time.Time { return now }}

	result, err := reconciler.reconcileCompletedWorkflowRunTTL(context.Background(), cachedRun)
	if err != nil {
		t.Fatal(err)
	}
	if result.RequeueAfter != time.Hour {
		t.Errorf("requeue after = %s, want %s", result.RequeueAfter, time.Hour)
	}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(currentRun), &actionsv1alpha1.WorkflowRun{}); err != nil {
		t.Fatalf("WorkflowRun with extended TTL was deleted: %v", err)
	}
}

type recordingDeleteClient struct {
	client.Client
	deleteOptions *client.DeleteOptions
}

type workflowRunListErrorReader struct {
	client.Reader
	err error
}

func (r *workflowRunListErrorReader) List(ctx context.Context, list client.ObjectList, options ...client.ListOption) error {
	if _, ok := list.(*actionsv1alpha1.WorkflowRunList); ok {
		return r.err
	}
	return r.Reader.List(ctx, list, options...)
}

func (c *recordingDeleteClient) Delete(ctx context.Context, object client.Object, options ...client.DeleteOption) error {
	c.deleteOptions = (&client.DeleteOptions{}).ApplyOptions(options)
	return c.Client.Delete(ctx, object, options...)
}

func TestConcurrencyWaitsForOlderUnevaluatedRun(t *testing.T) {
	older := concurrencyRun("older", "older", 1, time.Unix(1, 0), "", nil)
	current := concurrencyRun("current", "current", 1, time.Unix(2, 0), "deploy", nil)
	reconciler, _ := concurrencyReconciler(t, older, current)

	waiting, waitingForPlanning, err := reconciler.handleConcurrency(context.Background(), current)
	if err != nil {
		t.Fatal(err)
	}
	if !waiting {
		t.Fatal("newer run did not wait for an older unevaluated run")
	}
	if !waitingForPlanning {
		t.Fatal("newer run did not distinguish the older run's planning wait")
	}
}

func TestConcurrencyWaitsForLegacyEvaluatedRunBeforeRegistration(t *testing.T) {
	older := concurrencyRun("older", "older", 1, time.Unix(1, 0), "deploy", nil)
	older.Status.Concurrency = nil
	older.Status.ConcurrencyGroup = "deploy"
	current := concurrencyRun("current", "current", 1, time.Unix(2, 0), "deploy", nil)
	reconciler, _ := concurrencyReconciler(t, older, current)

	waiting, waitingForPlanning, err := reconciler.handleConcurrency(context.Background(), current)
	if err != nil {
		t.Fatal(err)
	}
	if !waiting {
		t.Fatal("newer run did not wait for an older run before gate registration")
	}
	if !waitingForPlanning {
		t.Fatal("newer run did not distinguish the older run's planning wait")
	}
}

func TestWorkflowRunPlanningWaitDoesNotRegisterConcurrency(t *testing.T) {
	runningCondition := plannedCondition(metav1.ConditionTrue, "JobsPlanned")
	pendingCondition := plannedCondition(metav1.ConditionUnknown, "WaitingForConcurrency")
	running := concurrencyRun("running", "running", 1, time.Unix(1, 0), "deploy", &runningCondition)
	pending := concurrencyRun("pending", "pending", 1, time.Unix(2, 0), "deploy", &pendingCondition)
	planning := concurrencyRun("planning", "planning", 1, time.Unix(3, 0), "", nil)
	current := concurrencyRun("current", "current", 1, time.Unix(4, 0), "deploy", nil)
	reconciler, clusterClient := concurrencyReconciler(t, running, pending, planning, current)

	waiting, waitingForPlanning, err := reconciler.handleConcurrency(context.Background(), current)
	if err != nil {
		t.Fatal(err)
	}
	if !waiting || !waitingForPlanning {
		t.Fatalf("initial concurrency wait = (%t, %t), want (true, true)", waiting, waitingForPlanning)
	}
	if _, err := reconciler.waitingForConcurrency(context.Background(), current, "CI", 1, waitingForPlanning); err != nil {
		t.Fatal(err)
	}
	storedCurrent := &actionsv1alpha1.WorkflowRun{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(current), storedCurrent); err != nil {
		t.Fatal(err)
	}
	planned := meta.FindStatusCondition(storedCurrent.Status.Conditions, actionsv1alpha1.WorkflowRunConditionPlanned)
	if planned == nil || planned.Reason != workflowRunPlanningWaitReason {
		t.Fatalf("planning wait condition = %#v", planned)
	}

	planning.Status.Conditions = []metav1.Condition{plannedCondition(metav1.ConditionTrue, "JobsPlanned")}
	if err := clusterClient.Status().Update(context.Background(), planning); err != nil {
		t.Fatal(err)
	}
	waiting, waitingForPlanning, err = reconciler.handleConcurrency(context.Background(), storedCurrent)
	if err != nil {
		t.Fatal(err)
	}
	if !waiting || waitingForPlanning {
		t.Fatalf("gate wait = (%t, %t), want (true, false)", waiting, waitingForPlanning)
	}
	storedPending := &actionsv1alpha1.WorkflowRun{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(pending), storedPending); err != nil {
		t.Fatal(err)
	}
	if !storedPending.Spec.CancelRequested {
		t.Fatal("newest run did not replace the pending concurrency member")
	}
	lease := &coordinationv1.Lease{}
	scope, err := workflowRunConcurrencyScope(current)
	if err != nil {
		t.Fatal(err)
	}
	if err := clusterClient.Get(context.Background(), client.ObjectKey{Namespace: current.Namespace, Name: concurrencyLeaseName(scope, "deploy")}, lease); err != nil {
		t.Fatal(err)
	}
	storedMember, err := concurrencyPending(lease)
	if err != nil {
		t.Fatal(err)
	}
	if storedMember == nil || storedMember.identity() != workflowRunConcurrencyMember(storedCurrent).identity() {
		t.Fatalf("pending concurrency member = %#v", storedMember)
	}
}

func TestWorkflowRunPlanningBarrierReleasesInCreationOrder(t *testing.T) {
	runningCondition := plannedCondition(metav1.ConditionTrue, "JobsPlanned")
	planningWaitCondition := plannedCondition(metav1.ConditionUnknown, workflowRunPlanningWaitReason)
	running := concurrencyRun("running", "running", 1, time.Unix(1, 0), "deploy", &runningCondition)
	olderWaiter := concurrencyRun("older-waiter", "older-waiter", 1, time.Unix(2, 0), "deploy", &planningWaitCondition)
	newerWaiter := concurrencyRun("newer-waiter", "newer-waiter", 1, time.Unix(3, 0), "deploy", &planningWaitCondition)
	reconciler, clusterClient := concurrencyReconciler(t, running, olderWaiter, newerWaiter)

	waiting, waitingForPlanning, err := reconciler.handleConcurrency(context.Background(), newerWaiter)
	if err != nil {
		t.Fatal(err)
	}
	if !waiting || !waitingForPlanning {
		t.Fatalf("newer run wait = (%t, %t), want (true, true)", waiting, waitingForPlanning)
	}
	scope, err := workflowRunConcurrencyScope(running)
	if err != nil {
		t.Fatal(err)
	}
	key := client.ObjectKey{Namespace: running.Namespace, Name: concurrencyLeaseName(scope, "deploy")}
	if err := clusterClient.Get(context.Background(), key, &coordinationv1.Lease{}); !apierrors.IsNotFound(err) {
		t.Fatalf("Lease lookup before older run registration = %v, want not found", err)
	}

	waiting, waitingForPlanning, err = reconciler.handleConcurrency(context.Background(), olderWaiter)
	if err != nil {
		t.Fatal(err)
	}
	if !waiting || waitingForPlanning {
		t.Fatalf("older run wait = (%t, %t), want (true, false)", waiting, waitingForPlanning)
	}
	if _, err := reconciler.waitingForConcurrency(context.Background(), olderWaiter, "CI", 1, waitingForPlanning); err != nil {
		t.Fatal(err)
	}
	storedNewer := &actionsv1alpha1.WorkflowRun{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(newerWaiter), storedNewer); err != nil {
		t.Fatal(err)
	}
	waiting, waitingForPlanning, err = reconciler.handleConcurrency(context.Background(), storedNewer)
	if err != nil {
		t.Fatal(err)
	}
	if !waiting || waitingForPlanning {
		t.Fatalf("newer run gate wait = (%t, %t), want (true, false)", waiting, waitingForPlanning)
	}
	storedOlder := &actionsv1alpha1.WorkflowRun{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(olderWaiter), storedOlder); err != nil {
		t.Fatal(err)
	}
	if !storedOlder.Spec.CancelRequested {
		t.Fatal("newer run did not replace the older pending member")
	}
	lease := &coordinationv1.Lease{}
	if err := clusterClient.Get(context.Background(), key, lease); err != nil {
		t.Fatal(err)
	}
	pending, err := concurrencyPending(lease)
	if err != nil {
		t.Fatal(err)
	}
	if pending == nil || pending.identity() != workflowRunConcurrencyMember(storedNewer).identity() {
		t.Fatalf("pending concurrency member = %#v", pending)
	}
}

func TestConcurrencyIsRepositoryScoped(t *testing.T) {
	condition := plannedCondition(metav1.ConditionTrue, "JobsPlanned")
	otherRepository := concurrencyRun("other", "other", 2, time.Unix(1, 0), "deploy", &condition)
	current := concurrencyRun("current", "current", 1, time.Unix(2, 0), "deploy", nil)
	reconciler, clusterClient := concurrencyReconciler(t, otherRepository, current)

	waiting, _, err := reconciler.handleConcurrency(context.Background(), current)
	if err != nil {
		t.Fatal(err)
	}
	if waiting {
		t.Fatal("run waited for a concurrency group in another repository")
	}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(otherRepository), &actionsv1alpha1.WorkflowRun{}); err != nil {
		t.Fatalf("run in another repository was deleted: %v", err)
	}
}

func TestConcurrencySupersedesPendingRun(t *testing.T) {
	runningCondition := plannedCondition(metav1.ConditionTrue, "JobsPlanned")
	pendingCondition := plannedCondition(metav1.ConditionUnknown, "WaitingForConcurrency")
	running := concurrencyRun("running", "running", 1, time.Unix(1, 0), "deploy", &runningCondition)
	pending := concurrencyRun("pending", "pending", 1, time.Unix(2, 0), "deploy", &pendingCondition)
	current := concurrencyRun("current", "current", 1, time.Unix(3, 0), "deploy", nil)
	reconciler, clusterClient := concurrencyReconciler(t, running, pending, current)

	waiting, _, err := reconciler.handleConcurrency(context.Background(), current)
	if err != nil {
		t.Fatal(err)
	}
	if !waiting {
		t.Fatal("current run did not wait for the running member")
	}
	storedPending := &actionsv1alpha1.WorkflowRun{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(pending), storedPending); err != nil {
		t.Fatal(err)
	}
	if !storedPending.Spec.CancelRequested || !storedPending.DeletionTimestamp.IsZero() {
		t.Fatalf("superseded pending run cancellation = %#v", storedPending)
	}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(running), &actionsv1alpha1.WorkflowRun{}); err != nil {
		t.Fatalf("running member was deleted: %v", err)
	}
}

func TestSupersededWorkflowRunRejoinsAvailablePendingSlot(t *testing.T) {
	runningCondition := plannedCondition(metav1.ConditionTrue, "JobsPlanned")
	pendingCondition := plannedCondition(metav1.ConditionUnknown, "WaitingForConcurrency")
	running := concurrencyRun("running", "running", 1, time.Unix(1, 0), "deploy", &runningCondition)
	pending := concurrencyRun("pending", "pending", 1, time.Unix(2, 0), "deploy", &pendingCondition)
	current := concurrencyRun("current", "current", 1, time.Unix(3, 0), "deploy", nil)
	pending.Status.Concurrency.CancelInProgress = true
	current.Status.Concurrency.CancelInProgress = true
	reconciler, clusterClient := concurrencyReconciler(t, running, pending, current)

	if waiting, _, err := reconciler.handleConcurrency(context.Background(), current); err != nil {
		t.Fatal(err)
	} else if !waiting {
		t.Fatal("current run did not wait for the running member")
	}
	storedPending := &actionsv1alpha1.WorkflowRun{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(pending), storedPending); err != nil {
		t.Fatal(err)
	}
	if !storedPending.Spec.CancelRequested {
		t.Fatal("superseded pending run was not cancelled")
	}
	scope, err := workflowRunConcurrencyScope(running)
	if err != nil {
		t.Fatal(err)
	}
	if err := reconciler.releaseConcurrency(context.Background(), running.Namespace, scope, "deploy", workflowRunConcurrencyMember(running)); err != nil {
		t.Fatal(err)
	}
	if waiting, _, err := reconciler.handleConcurrency(context.Background(), storedPending); err != nil {
		t.Fatal(err)
	} else if !waiting {
		t.Fatal("superseded run did not rejoin the available pending slot")
	}
	storedCurrent := &actionsv1alpha1.WorkflowRun{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(current), storedCurrent); err != nil {
		t.Fatal(err)
	}
	if storedCurrent.Spec.CancelRequested {
		t.Fatal("superseded run requested cancellation of the newer holder")
	}
	lease := &coordinationv1.Lease{}
	key := client.ObjectKey{Namespace: pending.Namespace, Name: concurrencyLeaseName(scope, "deploy")}
	if err := clusterClient.Get(context.Background(), key, lease); err != nil {
		t.Fatal(err)
	}
	rejoined, err := concurrencyPending(lease)
	if err != nil {
		t.Fatal(err)
	}
	if rejoined == nil || rejoined.identity() != workflowRunConcurrencyMember(storedPending).identity() {
		t.Fatalf("pending concurrency member = %#v", rejoined)
	}
	if err := reconciler.releaseConcurrency(context.Background(), current.Namespace, scope, "deploy", workflowRunConcurrencyMember(current)); err != nil {
		t.Fatal(err)
	}
	if waiting, _, err := reconciler.handleConcurrency(context.Background(), storedPending); err != nil {
		t.Fatal(err)
	} else if waiting {
		t.Fatal("superseded run did not acquire the group after the newer holder completed")
	}
}

func TestConcurrencyCancelInProgressRequestsRunningMemberCancellation(t *testing.T) {
	condition := plannedCondition(metav1.ConditionTrue, "JobsPlanned")
	running := concurrencyRun("running", "running", 1, time.Unix(1, 0), "deploy", &condition)
	current := concurrencyRun("current", "current", 1, time.Unix(2, 0), "deploy", nil)
	current.Status.Concurrency.CancelInProgress = true
	reconciler, clusterClient := concurrencyReconciler(t, running, current)

	waiting, _, err := reconciler.handleConcurrency(context.Background(), current)
	if err != nil {
		t.Fatal(err)
	}
	if !waiting {
		t.Fatal("replacement run did not wait for cancellation")
	}
	storedRunning := &actionsv1alpha1.WorkflowRun{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(running), storedRunning); err != nil {
		t.Fatal(err)
	}
	if !storedRunning.Spec.CancelRequested || !storedRunning.DeletionTimestamp.IsZero() {
		t.Fatalf("running member cancellation = %#v", storedRunning)
	}
}

func TestJobConcurrencyAcquiresReplacesAndReleasesPendingMember(t *testing.T) {
	scheme := runtime.NewScheme()
	for name, add := range map[string]func(*runtime.Scheme) error{
		"actions": actionsv1alpha1.AddToScheme, "batch": batchv1.AddToScheme, "coordination": coordinationv1.AddToScheme, "core": corev1.AddToScheme,
	} {
		if err := add(scheme); err != nil {
			t.Fatalf("add %s types: %v", name, err)
		}
	}
	run := concurrencyRun("run", "run-uid", 1, time.Unix(1, 0), "", nil)
	job := func(name string) *actionsv1alpha1.WorkflowJob {
		return &actionsv1alpha1.WorkflowJob{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: run.Namespace, UID: types.UID(name + "-uid")},
			Spec: actionsv1alpha1.WorkflowJobSpec{
				WorkflowRunRef: corev1.LocalObjectReference{Name: run.Name}, JobID: name,
				Concurrency: &actionsv1alpha1.WorkflowJobConcurrency{Group: "Deploy"},
			},
		}
	}
	first := job("first")
	second := job("second")
	clusterClient := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&actionsv1alpha1.WorkflowJob{}).
		WithObjects(run, first, second).Build()
	reconciler := &WorkflowRunReconciler{Client: clusterClient, APIReader: clusterClient}
	jobs := []actionsv1alpha1.WorkflowJob{*first, *second}
	if err := reconciler.reconcileWorkflowJobGraph(context.Background(), run, "CI", nil, nil, nil, jobs); err != nil {
		t.Fatal(err)
	}
	storedJob := func(name string) *actionsv1alpha1.WorkflowJob {
		stored := &actionsv1alpha1.WorkflowJob{}
		if err := clusterClient.Get(context.Background(), client.ObjectKey{Namespace: run.Namespace, Name: name}, stored); err != nil {
			t.Fatal(err)
		}
		return stored
	}
	if condition := meta.FindStatusCondition(storedJob(first.Name).Status.Conditions, actionsv1alpha1.WorkflowJobConditionConcurrencyAcquired); condition == nil || condition.Status != metav1.ConditionTrue {
		t.Fatalf("first concurrency condition = %#v", condition)
	}
	if condition := meta.FindStatusCondition(storedJob(second.Name).Status.Conditions, actionsv1alpha1.WorkflowJobConditionConcurrencyAcquired); condition == nil || condition.Status != metav1.ConditionUnknown {
		t.Fatalf("second concurrency condition = %#v", condition)
	}

	third := job("third")
	if err := clusterClient.Create(context.Background(), third); err != nil {
		t.Fatal(err)
	}
	jobList := &actionsv1alpha1.WorkflowJobList{}
	if err := clusterClient.List(context.Background(), jobList, client.InNamespace(run.Namespace)); err != nil {
		t.Fatal(err)
	}
	slices.SortFunc(jobList.Items, func(left, right actionsv1alpha1.WorkflowJob) int { return strings.Compare(left.Name, right.Name) })
	if err := reconciler.reconcileWorkflowJobGraph(context.Background(), run, "CI", nil, nil, nil, jobList.Items); err != nil {
		t.Fatal(err)
	}
	if stored := storedJob(second.Name); stored.Status.Result != actionsv1alpha1.WorkflowJobResultCancelled || meta.FindStatusCondition(stored.Status.Conditions, actionsv1alpha1.WorkflowJobConditionConcurrencyAcquired).Reason != concurrencySupersededReason {
		t.Fatalf("superseded job status = %#v", stored.Status)
	}

	first = storedJob(first.Name)
	first.Status.Result = actionsv1alpha1.WorkflowJobResultSuccess
	if err := clusterClient.Status().Update(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	scope, err := workflowRunConcurrencyScope(run)
	if err != nil {
		t.Fatal(err)
	}
	restarted := &WorkflowRunReconciler{Client: clusterClient, APIReader: clusterClient}
	if err := restarted.releaseConcurrency(context.Background(), run.Namespace, scope, "Deploy", workflowJobConcurrencyMember(first)); err != nil {
		t.Fatal(err)
	}
	jobList = &actionsv1alpha1.WorkflowJobList{}
	if err := clusterClient.List(context.Background(), jobList, client.InNamespace(run.Namespace)); err != nil {
		t.Fatal(err)
	}
	slices.SortFunc(jobList.Items, func(left, right actionsv1alpha1.WorkflowJob) int { return strings.Compare(left.Name, right.Name) })
	if err := restarted.reconcileWorkflowJobGraph(context.Background(), run, "CI", nil, nil, nil, jobList.Items); err != nil {
		t.Fatal(err)
	}
	if condition := meta.FindStatusCondition(storedJob(third.Name).Status.Conditions, actionsv1alpha1.WorkflowJobConditionConcurrencyAcquired); condition == nil || condition.Status != metav1.ConditionTrue {
		t.Fatalf("promoted concurrency condition = %#v", condition)
	}
}

func TestJobConcurrencyRejectsParentWorkflowRunGroup(t *testing.T) {
	scheme := runtime.NewScheme()
	for name, add := range map[string]func(*runtime.Scheme) error{
		"actions": actionsv1alpha1.AddToScheme, "coordination": coordinationv1.AddToScheme, "core": corev1.AddToScheme,
	} {
		if err := add(scheme); err != nil {
			t.Fatalf("add %s types: %v", name, err)
		}
	}
	planned := plannedCondition(metav1.ConditionTrue, "JobsPlanned")
	run := concurrencyRun("run", "run-uid", 1, time.Unix(1, 0), "Deploy", &planned)
	cancelInProgress := true
	job := &actionsv1alpha1.WorkflowJob{
		ObjectMeta: metav1.ObjectMeta{Name: "deploy", Namespace: run.Namespace, UID: "deploy-uid"},
		Spec: actionsv1alpha1.WorkflowJobSpec{
			WorkflowRunRef: corev1.LocalObjectReference{Name: run.Name}, JobID: "deploy",
			Concurrency: &actionsv1alpha1.WorkflowJobConcurrency{
				Group: "deploy", CancelInProgress: &actionsv1alpha1.WorkflowJobConcurrencyCancellation{Value: &cancelInProgress},
			},
		},
	}
	clusterClient := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&actionsv1alpha1.WorkflowJob{}).
		WithObjects(run, job).Build()
	reconciler := &WorkflowRunReconciler{Client: clusterClient, APIReader: clusterClient}

	if err := reconciler.reconcileWorkflowJobGraph(context.Background(), run, "CI", nil, nil, nil, []actionsv1alpha1.WorkflowJob{*job}); err != nil {
		t.Fatal(err)
	}
	storedJob := &actionsv1alpha1.WorkflowJob{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(job), storedJob); err != nil {
		t.Fatal(err)
	}
	if storedJob.Status.Result != actionsv1alpha1.WorkflowJobResultFailure {
		t.Fatalf("job result = %q, want %q", storedJob.Status.Result, actionsv1alpha1.WorkflowJobResultFailure)
	}
	succeeded := meta.FindStatusCondition(storedJob.Status.Conditions, actionsv1alpha1.WorkflowJobConditionSucceeded)
	if succeeded == nil || succeeded.Status != metav1.ConditionFalse || succeeded.Reason != "ConcurrencyEvaluationFailed" ||
		!strings.Contains(succeeded.Message, `WorkflowJob "deploy"`) || !strings.Contains(succeeded.Message, `WorkflowRun "run"`) {
		t.Fatalf("job success condition = %#v", succeeded)
	}
	storedRun := &actionsv1alpha1.WorkflowRun{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(run), storedRun); err != nil {
		t.Fatal(err)
	}
	if storedRun.Spec.CancelRequested {
		t.Fatal("job concurrency requested cancellation of its parent WorkflowRun")
	}
	scope, err := workflowRunConcurrencyScope(run)
	if err != nil {
		t.Fatal(err)
	}
	key := client.ObjectKey{Namespace: run.Namespace, Name: concurrencyLeaseName(scope, "deploy")}
	if err := clusterClient.Get(context.Background(), key, &coordinationv1.Lease{}); !apierrors.IsNotFound(err) {
		t.Fatalf("conflicting concurrency Lease lookup = %v, want not found", err)
	}
}

func TestJobConcurrencyCancellationDoesNotCancelWorkflowRun(t *testing.T) {
	scheme := runtime.NewScheme()
	for name, add := range map[string]func(*runtime.Scheme) error{
		"actions": actionsv1alpha1.AddToScheme, "coordination": coordinationv1.AddToScheme, "core": corev1.AddToScheme,
	} {
		if err := add(scheme); err != nil {
			t.Fatalf("add %s types: %v", name, err)
		}
	}
	run := concurrencyRun("run", "run-uid", 1, time.Unix(1, 0), "", nil)
	running := &actionsv1alpha1.WorkflowJob{
		ObjectMeta: metav1.ObjectMeta{Name: "running", Namespace: run.Namespace, UID: "running-uid"},
		Spec:       actionsv1alpha1.WorkflowJobSpec{WorkflowRunRef: corev1.LocalObjectReference{Name: run.Name}, JobID: "running", Concurrency: &actionsv1alpha1.WorkflowJobConcurrency{Group: "deploy"}},
	}
	replacement := &actionsv1alpha1.WorkflowJob{
		ObjectMeta: metav1.ObjectMeta{Name: "replacement", Namespace: run.Namespace, UID: "replacement-uid"},
		Spec: actionsv1alpha1.WorkflowJobSpec{
			WorkflowRunRef: corev1.LocalObjectReference{Name: run.Name}, JobID: "replacement",
			Concurrency: &actionsv1alpha1.WorkflowJobConcurrency{Group: "DEPLOY", CancelInProgress: &actionsv1alpha1.WorkflowJobConcurrencyCancellation{Expression: "${{ true }}"}},
		},
	}
	clusterClient := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&actionsv1alpha1.WorkflowJob{}).
		WithObjects(run, running).Build()
	reconciler := &WorkflowRunReconciler{Client: clusterClient, APIReader: clusterClient}
	jobs := []actionsv1alpha1.WorkflowJob{*running}
	if err := reconciler.reconcileWorkflowJobGraph(context.Background(), run, "CI", nil, nil, nil, jobs); err != nil {
		t.Fatal(err)
	}
	stored := &actionsv1alpha1.WorkflowJob{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(running), stored); err != nil {
		t.Fatal(err)
	}
	stored.Status.RunnerRef = &corev1.LocalObjectReference{Name: "runner"}
	if err := clusterClient.Status().Update(context.Background(), stored); err != nil {
		t.Fatal(err)
	}
	if err := clusterClient.Create(context.Background(), replacement); err != nil {
		t.Fatal(err)
	}
	jobs = []actionsv1alpha1.WorkflowJob{*stored, *replacement}
	if err := reconciler.reconcileWorkflowJobGraph(context.Background(), run, "CI", nil, nil, nil, jobs); err != nil {
		t.Fatal(err)
	}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(running), stored); err != nil {
		t.Fatal(err)
	}
	cancellation := meta.FindStatusCondition(stored.Status.Conditions, actionsv1alpha1.WorkflowJobConditionCancellationRequested)
	if cancellation == nil || cancellation.Status != metav1.ConditionTrue || cancellation.Reason != concurrencyCancelledReason {
		t.Fatalf("job cancellation condition = %#v", cancellation)
	}
	storedReplacement := &actionsv1alpha1.WorkflowJob{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(replacement), storedReplacement); err != nil {
		t.Fatal(err)
	}
	if storedReplacement.Status.Concurrency == nil || storedReplacement.Status.Concurrency.Group != "DEPLOY" || !storedReplacement.Status.Concurrency.CancelInProgress {
		t.Fatalf("replacement concurrency decision = %#v", storedReplacement.Status.Concurrency)
	}
	storedRun := &actionsv1alpha1.WorkflowRun{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(run), storedRun); err != nil {
		t.Fatal(err)
	}
	if storedRun.Spec.CancelRequested {
		t.Fatal("job concurrency cancelled the WorkflowRun")
	}
}

func TestJobConcurrencyRestoresEvaluatedCancellationAfterRestart(t *testing.T) {
	scheme := runtime.NewScheme()
	for name, add := range map[string]func(*runtime.Scheme) error{
		"actions": actionsv1alpha1.AddToScheme, "coordination": coordinationv1.AddToScheme, "core": corev1.AddToScheme,
	} {
		if err := add(scheme); err != nil {
			t.Fatalf("add %s types: %v", name, err)
		}
	}
	run := concurrencyRun("run", "run-uid", 1, time.Unix(1, 0), "", nil)
	running := &actionsv1alpha1.WorkflowJob{
		ObjectMeta: metav1.ObjectMeta{Name: "running", Namespace: run.Namespace, UID: "running-uid"},
		Spec: actionsv1alpha1.WorkflowJobSpec{
			WorkflowRunRef: corev1.LocalObjectReference{Name: run.Name}, JobID: "running",
			Concurrency: &actionsv1alpha1.WorkflowJobConcurrency{Group: "deploy"},
		},
		Status: actionsv1alpha1.WorkflowJobStatus{
			RunnerRef:   &corev1.LocalObjectReference{Name: "runner"},
			Concurrency: &actionsv1alpha1.ConcurrencyStatus{Group: "deploy"},
		},
	}
	replacement := &actionsv1alpha1.WorkflowJob{
		ObjectMeta: metav1.ObjectMeta{Name: "replacement", Namespace: run.Namespace, UID: "replacement-uid"},
		Spec: actionsv1alpha1.WorkflowJobSpec{
			WorkflowRunRef: corev1.LocalObjectReference{Name: run.Name}, JobID: "replacement",
			Concurrency: &actionsv1alpha1.WorkflowJobConcurrency{
				Group: "deploy", CancelInProgress: &actionsv1alpha1.WorkflowJobConcurrencyCancellation{Expression: "${{ false }}"},
			},
		},
		Status: actionsv1alpha1.WorkflowJobStatus{
			Concurrency: &actionsv1alpha1.ConcurrencyStatus{Group: "deploy", CancelInProgress: true},
		},
	}
	clusterClient := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&actionsv1alpha1.WorkflowJob{}).
		WithObjects(run, running, replacement).Build()
	scope, err := workflowRunConcurrencyScope(run)
	if err != nil {
		t.Fatal(err)
	}
	initial := &WorkflowRunReconciler{Client: clusterClient, APIReader: clusterClient}
	if _, err := initial.acquireConcurrency(context.Background(), run.Namespace, scope, "deploy", workflowJobConcurrencyMember(running), false); err != nil {
		t.Fatal(err)
	}
	restarted := &WorkflowRunReconciler{Client: clusterClient, APIReader: clusterClient}
	jobs := []actionsv1alpha1.WorkflowJob{*running, *replacement}
	if err := restarted.reconcileWorkflowJobGraph(context.Background(), run, "CI", nil, nil, nil, jobs); err != nil {
		t.Fatal(err)
	}
	stored := &actionsv1alpha1.WorkflowJob{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(running), stored); err != nil {
		t.Fatal(err)
	}
	cancellation := meta.FindStatusCondition(stored.Status.Conditions, actionsv1alpha1.WorkflowJobConditionCancellationRequested)
	if cancellation == nil || cancellation.Status != metav1.ConditionTrue || cancellation.Reason != concurrencyCancelledReason {
		t.Fatalf("restored cancellation decision condition = %#v", cancellation)
	}
}

func TestJobConcurrencySharesWorkflowRunNamespaceAcrossRuns(t *testing.T) {
	scheme := runtime.NewScheme()
	for name, add := range map[string]func(*runtime.Scheme) error{
		"actions": actionsv1alpha1.AddToScheme, "coordination": coordinationv1.AddToScheme, "core": corev1.AddToScheme,
	} {
		if err := add(scheme); err != nil {
			t.Fatalf("add %s types: %v", name, err)
		}
	}
	planned := plannedCondition(metav1.ConditionTrue, "JobsPlanned")
	holderRun := concurrencyRun("workflow-holder", "workflow-holder-uid", 1, time.Unix(1, 0), "deploy", &planned)
	jobRun := concurrencyRun("job-run", "job-run-uid", 1, time.Unix(2, 0), "", nil)
	job := &actionsv1alpha1.WorkflowJob{
		ObjectMeta: metav1.ObjectMeta{Name: "deploy", Namespace: jobRun.Namespace, UID: "deploy-uid"},
		Spec: actionsv1alpha1.WorkflowJobSpec{
			WorkflowRunRef: corev1.LocalObjectReference{Name: jobRun.Name}, JobID: "deploy",
			Concurrency: &actionsv1alpha1.WorkflowJobConcurrency{Group: "DEPLOY"},
		},
	}
	clusterClient := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&actionsv1alpha1.WorkflowJob{}).
		WithObjects(holderRun, jobRun, job).Build()
	reconciler := &WorkflowRunReconciler{Client: clusterClient, APIReader: clusterClient}
	jobs := []actionsv1alpha1.WorkflowJob{*job}
	if err := reconciler.reconcileWorkflowJobGraph(context.Background(), jobRun, "CI", nil, nil, nil, jobs); err != nil {
		t.Fatal(err)
	}
	stored := &actionsv1alpha1.WorkflowJob{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(job), stored); err != nil {
		t.Fatal(err)
	}
	condition := meta.FindStatusCondition(stored.Status.Conditions, actionsv1alpha1.WorkflowJobConditionConcurrencyAcquired)
	if condition == nil || condition.Status != metav1.ConditionUnknown {
		t.Fatalf("job concurrency condition = %#v", condition)
	}
}

func TestJobConcurrencyWaitsForCancellationCleanup(t *testing.T) {
	scheme := runtime.NewScheme()
	for name, add := range map[string]func(*runtime.Scheme) error{
		"actions": actionsv1alpha1.AddToScheme, "batch": batchv1.AddToScheme, "coordination": coordinationv1.AddToScheme, "core": corev1.AddToScheme,
	} {
		if err := add(scheme); err != nil {
			t.Fatalf("add %s types: %v", name, err)
		}
	}
	run := concurrencyRun("run", "run-uid", 1, time.Unix(1, 0), "", nil)
	job := func(name string) *actionsv1alpha1.WorkflowJob {
		return &actionsv1alpha1.WorkflowJob{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: run.Namespace, UID: types.UID(name + "-uid")},
			Spec: actionsv1alpha1.WorkflowJobSpec{
				WorkflowRunRef: corev1.LocalObjectReference{Name: run.Name}, JobID: name,
				Concurrency: &actionsv1alpha1.WorkflowJobConcurrency{Group: "deploy"},
			},
		}
	}
	holder := job("holder")
	waiter := job("waiter")
	clusterClient := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&actionsv1alpha1.WorkflowJob{}).
		WithObjects(run, holder, waiter).Build()
	reconciler := &WorkflowRunReconciler{Client: clusterClient, APIReader: clusterClient}
	jobs := []actionsv1alpha1.WorkflowJob{*holder, *waiter}
	if err := reconciler.reconcileWorkflowJobGraph(context.Background(), run, "CI", nil, nil, nil, jobs); err != nil {
		t.Fatal(err)
	}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(holder), holder); err != nil {
		t.Fatal(err)
	}
	holder.Status.Result = actionsv1alpha1.WorkflowJobResultCancelled
	if err := clusterClient.Status().Update(context.Background(), holder); err != nil {
		t.Fatal(err)
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "holder-pod", Namespace: run.Namespace, Labels: map[string]string{actionsv1alpha1.LabelWorkflowJobUID: string(holder.UID)}},
		Status:     corev1.PodStatus{Phase: corev1.PodRunning},
	}
	if err := clusterClient.Create(context.Background(), pod); err != nil {
		t.Fatal(err)
	}
	jobsList := &actionsv1alpha1.WorkflowJobList{}
	if err := clusterClient.List(context.Background(), jobsList, client.InNamespace(run.Namespace)); err != nil {
		t.Fatal(err)
	}
	slices.SortFunc(jobsList.Items, func(left, right actionsv1alpha1.WorkflowJob) int { return strings.Compare(left.Name, right.Name) })
	if err := reconciler.reconcileWorkflowJobGraph(context.Background(), run, "CI", nil, nil, nil, jobsList.Items); err != nil {
		t.Fatal(err)
	}
	storedWaiter := &actionsv1alpha1.WorkflowJob{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(waiter), storedWaiter); err != nil {
		t.Fatal(err)
	}
	if condition := meta.FindStatusCondition(storedWaiter.Status.Conditions, actionsv1alpha1.WorkflowJobConditionConcurrencyAcquired); condition == nil || condition.Status != metav1.ConditionUnknown {
		t.Fatalf("condition during cleanup = %#v", condition)
	}
	if err := clusterClient.Delete(context.Background(), pod); err != nil {
		t.Fatal(err)
	}
	jobsList = &actionsv1alpha1.WorkflowJobList{}
	if err := clusterClient.List(context.Background(), jobsList, client.InNamespace(run.Namespace)); err != nil {
		t.Fatal(err)
	}
	slices.SortFunc(jobsList.Items, func(left, right actionsv1alpha1.WorkflowJob) int { return strings.Compare(left.Name, right.Name) })
	if err := reconciler.reconcileWorkflowJobGraph(context.Background(), run, "CI", nil, nil, nil, jobsList.Items); err != nil {
		t.Fatal(err)
	}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(waiter), storedWaiter); err != nil {
		t.Fatal(err)
	}
	if condition := meta.FindStatusCondition(storedWaiter.Status.Conditions, actionsv1alpha1.WorkflowJobConditionConcurrencyAcquired); condition == nil || condition.Status != metav1.ConditionTrue {
		t.Fatalf("condition after cleanup = %#v", condition)
	}
}

func TestTerminalWorkflowRunReleasesJobConcurrencyAfterWorkloadStops(t *testing.T) {
	scheme := runtime.NewScheme()
	for name, add := range map[string]func(*runtime.Scheme) error{
		"actions": actionsv1alpha1.AddToScheme, "batch": batchv1.AddToScheme, "coordination": coordinationv1.AddToScheme, "core": corev1.AddToScheme,
	} {
		if err := add(scheme); err != nil {
			t.Fatalf("add %s types: %v", name, err)
		}
	}
	run := concurrencyRun("run", "run-uid", 1, time.Unix(1, 0), "", nil)
	run.Status.Conditions = []metav1.Condition{{
		Type: actionsv1alpha1.WorkflowRunConditionSucceeded, Status: metav1.ConditionTrue,
		Reason: "JobsSucceeded", Message: "JobsSucceeded", LastTransitionTime: metav1.Now(),
	}}
	job := &actionsv1alpha1.WorkflowJob{
		ObjectMeta: metav1.ObjectMeta{
			Name: "deploy", Namespace: run.Namespace, UID: "deploy-uid",
			Labels: map[string]string{actionsv1alpha1.LabelWorkflowRunUID: string(run.UID)},
		},
		Spec: actionsv1alpha1.WorkflowJobSpec{WorkflowRunRef: corev1.LocalObjectReference{Name: run.Name}, JobID: "deploy"},
		Status: actionsv1alpha1.WorkflowJobStatus{
			Result: actionsv1alpha1.WorkflowJobResultSuccess,
			Concurrency: &actionsv1alpha1.ConcurrencyStatus{
				Group: "deploy",
			},
		},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "deploy-pod", Namespace: run.Namespace,
			Labels: map[string]string{actionsv1alpha1.LabelWorkflowJobUID: string(job.UID)},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
	scope, err := workflowRunConcurrencyScope(run)
	if err != nil {
		t.Fatal(err)
	}
	holder := workflowJobConcurrencyMember(job).identity()
	lease := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Name: concurrencyLeaseName(scope, "deploy"), Namespace: run.Namespace,
			Annotations: map[string]string{
				concurrencyGroupAnnotation: "deploy", concurrencyProjectAnnotation: scope.project,
				concurrencyRepositoryAnnotation: fmt.Sprint(scope.repositoryID),
			},
		},
		Spec: coordinationv1.LeaseSpec{HolderIdentity: &holder},
	}
	clusterClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(run, job, pod, lease).Build()
	reconciler := &WorkflowRunReconciler{Client: clusterClient, APIReader: clusterClient}

	result, err := reconciler.reconcileWorkflowRun(context.Background(), run)
	if err != nil {
		t.Fatal(err)
	}
	if result.RequeueAfter != 2*time.Second {
		t.Fatalf("requeue after = %v, want 2s", result.RequeueAfter)
	}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(lease), &coordinationv1.Lease{}); err != nil {
		t.Fatalf("concurrency Lease was released while workload remained: %v", err)
	}
	if err := clusterClient.Delete(context.Background(), pod); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.reconcileWorkflowRun(context.Background(), run); err != nil {
		t.Fatal(err)
	}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(lease), &coordinationv1.Lease{}); !apierrors.IsNotFound(err) {
		t.Fatalf("concurrency Lease after workload stopped = %v, want not found", err)
	}
}

func TestCanceledWorkflowRunWaitsForPodsBeforeFinalizing(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := actionsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := batchv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	deletionTime := metav1.Now()
	run := &actionsv1alpha1.WorkflowRun{ObjectMeta: metav1.ObjectMeta{
		Name: "canceling", Namespace: "default", UID: types.UID("run-uid"),
		DeletionTimestamp: &deletionTime, Finalizers: []string{workflowRunCancellationFinalizer, workflowRunCheckFinalizer},
	}}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: "canceling-job", Namespace: "default", Labels: map[string]string{actionsv1alpha1.LabelWorkflowRunUID: string(run.UID)},
	}}
	clusterClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(run, pod).Build()
	reconciler := &WorkflowRunReconciler{Client: clusterClient, APIReader: clusterClient}
	result, err := reconciler.finalizeCanceledWorkflowRun(context.Background(), run)
	if err != nil {
		t.Fatal(err)
	}
	if result.RequeueAfter == 0 {
		t.Fatal("canceled WorkflowRun did not wait for its Pod")
	}
	stored := &actionsv1alpha1.WorkflowRun{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(run), stored); err != nil {
		t.Fatal(err)
	}
	if !controllerutil.ContainsFinalizer(stored, workflowRunCancellationFinalizer) {
		t.Fatal("cancellation finalizer was removed while a Pod remained")
	}
	if !controllerutil.ContainsFinalizer(stored, workflowRunCheckFinalizer) {
		t.Fatal("check finalizer was removed while cancellation remained")
	}
	if err := clusterClient.Delete(context.Background(), pod); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.finalizeCanceledWorkflowRun(context.Background(), stored); err != nil {
		t.Fatal(err)
	}
	stored = &actionsv1alpha1.WorkflowRun{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(run), stored); err != nil {
		if client.IgnoreNotFound(err) != nil {
			t.Fatal(err)
		}
	} else if controllerutil.ContainsFinalizer(stored, workflowRunCancellationFinalizer) || controllerutil.ContainsFinalizer(stored, workflowRunCheckFinalizer) {
		t.Fatal("WorkflowRun finalizer remained after the Pod was deleted")
	}
}

func TestScheduledWorkflowRunDeletionRetainsIdempotencyMarker(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := actionsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	created := time.Date(2026, 8, 11, 6, 0, 10, 0, time.UTC)
	deleted := metav1.NewTime(created.Add(10 * time.Second))
	run := &actionsv1alpha1.WorkflowRun{ObjectMeta: metav1.ObjectMeta{
		Name: "schedule-example", Namespace: "default", CreationTimestamp: metav1.NewTime(created),
		DeletionTimestamp: &deleted, Finalizers: []string{workflowRunScheduleFinalizer},
	}}
	clusterClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(run).Build()
	reconciler := &WorkflowRunReconciler{
		Client: clusterClient, APIReader: clusterClient,
		Now: func() time.Time { return time.Date(2026, 8, 11, 6, 0, 30, 0, time.UTC) },
	}
	result, err := reconciler.finalizeCanceledWorkflowRun(context.Background(), run)
	if err != nil {
		t.Fatal(err)
	}
	if result.RequeueAfter != 30*time.Second {
		t.Fatalf("requeue after = %v, want 30s", result.RequeueAfter)
	}
	stored := &actionsv1alpha1.WorkflowRun{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(run), stored); err != nil {
		t.Fatal(err)
	}
	if !controllerutil.ContainsFinalizer(stored, workflowRunScheduleFinalizer) {
		t.Fatal("schedule idempotency finalizer was removed during the due minute")
	}
	reconciler.Now = func() time.Time { return time.Date(2026, 8, 11, 6, 1, 0, 0, time.UTC) }
	if _, err := reconciler.finalizeCanceledWorkflowRun(context.Background(), stored); err != nil {
		t.Fatal(err)
	}
	stored = &actionsv1alpha1.WorkflowRun{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(run), stored); client.IgnoreNotFound(err) != nil {
		t.Fatal(err)
	} else if err == nil && controllerutil.ContainsFinalizer(stored, workflowRunScheduleFinalizer) {
		t.Fatal("schedule idempotency finalizer remained after the due minute")
	}
}

func TestCanceledWorkflowRunRetainsCheckFinalizerWhenReportingFails(t *testing.T) {
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
		ObjectMeta: metav1.ObjectMeta{Name: "project", Namespace: "default"},
		Spec: actionsv1alpha1.ProjectSpec{Source: actionsv1alpha1.ProjectSource{Type: actionsv1alpha1.SourceTypeGitHub, GitHub: &actionsv1alpha1.GitHubAppConfiguration{
			AppID: 1, InstallationID: 2,
			PrivateKeySecretRef: corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "github"}, Key: "private-key"},
		}}},
	}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "github", Namespace: "default"}, Data: map[string][]byte{"private-key": privateKeyData}}
	deletionTime := metav1.Now()
	run := &actionsv1alpha1.WorkflowRun{
		ObjectMeta: metav1.ObjectMeta{Name: "canceling", Namespace: "default", UID: types.UID("run-uid"), DeletionTimestamp: &deletionTime, Finalizers: []string{workflowRunCheckFinalizer}},
		Spec: actionsv1alpha1.WorkflowRunSpec{
			ProjectRef: corev1.LocalObjectReference{Name: project.Name}, WorkflowPath: ".open-actions/workflows/ci.yaml",
			Source: actionsv1alpha1.WorkflowRunSource{Type: actionsv1alpha1.SourceTypeGitHub, GitHub: &actionsv1alpha1.GitHubWorkflowRunSource{
				Repository: actionsv1alpha1.GitHubRepository{ID: 3, Owner: "acme", Name: "example"},
				Revision:   actionsv1alpha1.GitRevision{SHA: strings.Repeat("a", 40)},
			}},
		},
	}
	clusterClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(project, secret, run).Build()
	reconciler := &WorkflowRunReconciler{Client: clusterClient, APIReader: clusterClient, GitHub: github}
	if _, err := reconciler.finalizeCanceledWorkflowRun(context.Background(), run); err == nil {
		t.Fatal("terminal Check reporting failure did not request a retry")
	}
	stored := &actionsv1alpha1.WorkflowRun{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(run), stored); err != nil {
		t.Fatal(err)
	}
	if !controllerutil.ContainsFinalizer(stored, workflowRunCheckFinalizer) {
		t.Fatal("check finalizer was removed after a transient reporting failure")
	}
}

func TestCanceledWorkflowRunRemovesCheckFinalizerWhenProjectIsGone(t *testing.T) {
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
		ObjectMeta: metav1.ObjectMeta{Name: "canceling", Namespace: "default", UID: types.UID("run-uid"), DeletionTimestamp: &deletionTime, Finalizers: []string{workflowRunCheckFinalizer}},
		Spec: actionsv1alpha1.WorkflowRunSpec{
			ProjectRef: corev1.LocalObjectReference{Name: "deleted-project"}, WorkflowPath: ".open-actions/workflows/ci.yaml",
			Source: actionsv1alpha1.WorkflowRunSource{Type: actionsv1alpha1.SourceTypeGitHub, GitHub: &actionsv1alpha1.GitHubWorkflowRunSource{
				Repository: actionsv1alpha1.GitHubRepository{ID: 3, Owner: "acme", Name: "example"},
				Revision:   actionsv1alpha1.GitRevision{SHA: strings.Repeat("a", 40)},
			}},
		},
	}
	clusterClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(run).Build()
	reconciler := &WorkflowRunReconciler{Client: clusterClient, APIReader: clusterClient, GitHub: github}
	if _, err := reconciler.finalizeCanceledWorkflowRun(context.Background(), run); err != nil {
		t.Fatal(err)
	}
	stored := &actionsv1alpha1.WorkflowRun{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(run), stored); client.IgnoreNotFound(err) != nil {
		t.Fatal(err)
	} else if err == nil && controllerutil.ContainsFinalizer(stored, workflowRunCheckFinalizer) {
		t.Fatal("check finalizer blocked WorkflowRun deletion")
	}
}

func concurrencyRun(name string, uid types.UID, repositoryID int64, created time.Time, group string, condition *metav1.Condition) *actionsv1alpha1.WorkflowRun {
	run := &actionsv1alpha1.WorkflowRun{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", UID: uid, CreationTimestamp: metav1.NewTime(created)},
		Spec: actionsv1alpha1.WorkflowRunSpec{
			ProjectRef: corev1.LocalObjectReference{Name: "project"},
			Source: actionsv1alpha1.WorkflowRunSource{
				Type:   actionsv1alpha1.SourceTypeGitHub,
				GitHub: &actionsv1alpha1.GitHubWorkflowRunSource{Repository: actionsv1alpha1.GitHubRepository{ID: repositoryID}},
			},
		},
		Status: actionsv1alpha1.WorkflowRunStatus{},
	}
	if group != "" {
		run.Status.Concurrency = &actionsv1alpha1.ConcurrencyStatus{Group: group}
	}
	if condition != nil {
		run.Status.Conditions = []metav1.Condition{*condition}
	}
	return run
}

func plannedCondition(status metav1.ConditionStatus, reason string) metav1.Condition {
	return metav1.Condition{Type: actionsv1alpha1.WorkflowRunConditionPlanned, Status: status, Reason: reason, Message: reason, LastTransitionTime: metav1.Now()}
}

func concurrencyReconciler(t *testing.T, runs ...client.Object) (*WorkflowRunReconciler, client.Client) {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := actionsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := coordinationv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	clusterClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&actionsv1alpha1.WorkflowRun{}).WithObjects(runs...).Build()
	return &WorkflowRunReconciler{Client: clusterClient, APIReader: clusterClient}, clusterClient
}

func TestChildNameIsStableAndBounded(t *testing.T) {
	name := childName(strings.Repeat("run", 30), strings.Repeat("job", 100))
	if len(name) > 63 {
		t.Errorf("name has %d characters", len(name))
	}
	if name != childName(strings.Repeat("run", 30), strings.Repeat("job", 100)) {
		t.Error("name is not stable")
	}
}

func TestWorkflowJobNameIsStableReadableAndBounded(t *testing.T) {
	runName := "ci-m4z2c6h5t3k7w2n4r6qa"
	readable := workflowJobName(runName, "build")
	if !strings.HasPrefix(readable, "ci-m4z2c6h5t3k7w2n4r6qa-build-") {
		t.Errorf("readable child name = %q", readable)
	}
	if len(readable) > 63 {
		t.Errorf("name has %d characters", len(readable))
	}
	if readable != workflowJobName(runName, "build") {
		t.Error("name is not stable")
	}
}

func TestWorkflowJobNameSeparatesCollidingJobIDs(t *testing.T) {
	runName := strings.Repeat("workflow", 8)
	first := workflowJobName(runName, "build_test")
	second := workflowJobName(runName, "build-test")
	if first == second {
		t.Fatalf("colliding job IDs produced %q", first)
	}
}

func TestMatrixWorkflowJobIDsAreStableUniqueAndBounded(t *testing.T) {
	logicalID := strings.Repeat("a", workflowJobIDMaxLength)
	first := matrixWorkflowJobID(logicalID, 0)
	second := matrixWorkflowJobID(logicalID, 1)
	if first == second || len(first) > workflowJobIDMaxLength || len(second) > workflowJobIDMaxLength {
		t.Fatalf("matrix IDs = %q, %q", first, second)
	}
	if first != matrixWorkflowJobID(logicalID, 0) {
		t.Fatal("matrix job ID is not stable")
	}
	sourceIDs := map[string]struct{}{"build-matrix-1": {}}
	collisionFree := uniqueMatrixWorkflowJobID("build", 0, sourceIDs, nil)
	if collisionFree == "build-matrix-1" || len(collisionFree) > workflowJobIDMaxLength {
		t.Fatalf("collision-free matrix ID = %q", collisionFree)
	}
}

func TestEnsureWorkflowJobsCreatesReadableNames(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := actionsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	run := &actionsv1alpha1.WorkflowRun{ObjectMeta: metav1.ObjectMeta{Name: "ci-m4z2c6h5t3k7w2n4r6qa", Namespace: "default", UID: "run-uid"}}
	project := &actionsv1alpha1.Project{ObjectMeta: metav1.ObjectMeta{Name: "project", Namespace: "default", UID: "project-uid"}}
	clusterClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(run, project).Build()
	reconciler := &WorkflowRunReconciler{Client: clusterClient, APIReader: clusterClient}

	if err := reconciler.ensureWorkflowJobs(context.Background(), run, project, []plannedWorkflowJob{{
		id: "build", displayName: "Build and test", runsOn: []string{"linux"}, plan: "{}", resultVersion: jobResultVersion, timeoutSeconds: 5400,
	}}); err != nil {
		t.Fatal(err)
	}
	jobs := &actionsv1alpha1.WorkflowJobList{}
	if err := clusterClient.List(context.Background(), jobs); err != nil {
		t.Fatal(err)
	}
	if len(jobs.Items) != 1 {
		t.Fatalf("WorkflowJobs = %d, want 1", len(jobs.Items))
	}
	job := &jobs.Items[0]
	if job.Name != workflowJobName(run.Name, "build") || job.Spec.DisplayName != "Build and test" {
		t.Errorf("WorkflowJob = %#v", job)
	}
	if job.Spec.TimeoutSeconds != 5400 {
		t.Errorf("WorkflowJob timeout = %d, want 5400", job.Spec.TimeoutSeconds)
	}
	if job.Annotations[actionsv1alpha1.AnnotationRunnerResultVersion] != jobResultVersion {
		t.Errorf("runner result version = %q", job.Annotations[actionsv1alpha1.AnnotationRunnerResultVersion])
	}
}

func TestEnsureWorkflowJobsPreservesMatrixIdentity(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := actionsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	run := &actionsv1alpha1.WorkflowRun{ObjectMeta: metav1.ObjectMeta{Name: "release", Namespace: "default", UID: "run-uid"}}
	project := &actionsv1alpha1.Project{ObjectMeta: metav1.ObjectMeta{Name: "project", Namespace: "default", UID: "project-uid"}}
	clusterClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(run, project).Build()
	reconciler := &WorkflowRunReconciler{Client: clusterClient, APIReader: clusterClient}
	planned := []plannedWorkflowJob{{
		id: "build-matrix-1", displayName: "build (arch=amd64)", runsOn: []string{"ubuntu-latest"}, plan: "{}",
		matrix: &actionsv1alpha1.WorkflowJobMatrix{LogicalJobID: "build", Values: map[string]string{"arch": "amd64"}, MaxParallel: 1},
	}}

	if err := reconciler.ensureWorkflowJobs(context.Background(), run, project, planned); err != nil {
		t.Fatal(err)
	}
	if err := reconciler.ensureWorkflowJobs(context.Background(), run, project, planned); err != nil {
		t.Fatalf("reconcile persisted matrix job: %v", err)
	}
	jobs := &actionsv1alpha1.WorkflowJobList{}
	if err := clusterClient.List(context.Background(), jobs); err != nil {
		t.Fatal(err)
	}
	if len(jobs.Items) != 1 || jobs.Items[0].Spec.Matrix == nil || jobs.Items[0].Spec.Matrix.Values["arch"] != "amd64" || jobs.Items[0].Spec.Matrix.MaxParallel != 1 {
		t.Fatalf("WorkflowJobs = %#v", jobs.Items)
	}
}

func TestEnsureWorkflowJobsAdoptsExistingName(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := actionsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	run := &actionsv1alpha1.WorkflowRun{ObjectMeta: metav1.ObjectMeta{Name: "ci", Namespace: "default", UID: "run-uid"}}
	project := &actionsv1alpha1.Project{ObjectMeta: metav1.ObjectMeta{Name: "project", Namespace: "default", UID: "project-uid"}}
	existing := &actionsv1alpha1.WorkflowJob{
		ObjectMeta: metav1.ObjectMeta{
			Name: childName(run.Name, "build"), Namespace: run.Namespace,
			Labels:      workflowJobLabels(run, project, "build"),
			Annotations: map[string]string{actionsv1alpha1.AnnotationProjectName: project.Name},
		},
		Spec: actionsv1alpha1.WorkflowJobSpec{
			WorkflowRunRef: corev1.LocalObjectReference{Name: run.Name}, JobID: "build", RunsOn: []string{"linux"},
		},
	}
	if err := controllerutil.SetControllerReference(run, existing, scheme); err != nil {
		t.Fatal(err)
	}
	clusterClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(run, project, existing).Build()
	reconciler := &WorkflowRunReconciler{Client: clusterClient, APIReader: clusterClient}

	if err := reconciler.ensureWorkflowJobs(context.Background(), run, project, []plannedWorkflowJob{{
		id: "build", displayName: "Build and test", runsOn: []string{"linux"}, plan: "{}",
	}}); err != nil {
		t.Fatal(err)
	}
	jobs := &actionsv1alpha1.WorkflowJobList{}
	if err := clusterClient.List(context.Background(), jobs); err != nil {
		t.Fatal(err)
	}
	if len(jobs.Items) != 1 || jobs.Items[0].Name != existing.Name {
		t.Fatalf("WorkflowJobs = %#v", jobs.Items)
	}
}

func TestWorkflowJobIdentityRequiresOwnerSpecAndLabels(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := actionsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	run := &actionsv1alpha1.WorkflowRun{
		TypeMeta:   metav1.TypeMeta{APIVersion: actionsv1alpha1.GroupVersion.String(), Kind: "WorkflowRun"},
		ObjectMeta: metav1.ObjectMeta{Name: "ci", Namespace: "default", UID: types.UID("run-uid")},
	}
	desired := &actionsv1alpha1.WorkflowJob{
		ObjectMeta: metav1.ObjectMeta{Name: "build", Namespace: "default", Labels: map[string]string{
			actionsv1alpha1.LabelProjectUID:     "project-uid",
			actionsv1alpha1.LabelWorkflowRunUID: string(run.UID),
			actionsv1alpha1.LabelWorkflowJob:    "build",
		}},
		Spec: actionsv1alpha1.WorkflowJobSpec{
			WorkflowRunRef: corev1.LocalObjectReference{Name: run.Name},
			JobID:          "build",
			DisplayName:    "Build and test",
			RunsOn:         []string{"linux"},
		},
	}
	if err := controllerutil.SetControllerReference(run, desired, scheme); err != nil {
		t.Fatal(err)
	}
	existing := desired.DeepCopy()
	if !workflowJobIdentityMatches(existing, desired, run) {
		t.Fatal("matching WorkflowJob identity was rejected")
	}
	existing.Spec.DisplayName = ""
	if !workflowJobIdentityMatches(existing, desired, run) {
		t.Fatal("WorkflowJob created before display names was rejected")
	}
	existing.Spec.DisplayName = "Release"
	if workflowJobIdentityMatches(existing, desired, run) {
		t.Fatal("mismatched WorkflowJob display name was accepted")
	}
	existing = desired.DeepCopy()
	existing.Spec.JobID = "release"
	if workflowJobIdentityMatches(existing, desired, run) {
		t.Fatal("mismatched WorkflowJob spec was accepted")
	}
	existing = desired.DeepCopy()
	existing.Labels[actionsv1alpha1.LabelWorkflowJob] = "release"
	if workflowJobIdentityMatches(existing, desired, run) {
		t.Fatal("mismatched WorkflowJob labels were accepted")
	}
	existing = desired.DeepCopy()
	existing.OwnerReferences = nil
	if workflowJobIdentityMatches(existing, desired, run) {
		t.Fatal("unowned WorkflowJob was accepted")
	}
}

func TestReconcileAllocatesWorkflowRunIdentityBeforeObservingJobs(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := actionsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	project := &actionsv1alpha1.Project{ObjectMeta: metav1.ObjectMeta{Name: "project", Namespace: "default"}}
	run := &actionsv1alpha1.WorkflowRun{
		ObjectMeta: metav1.ObjectMeta{Name: "ci", Namespace: "default", UID: types.UID("run-uid")},
		Spec: actionsv1alpha1.WorkflowRunSpec{
			ProjectRef:   corev1.LocalObjectReference{Name: project.Name},
			WorkflowPath: ".open-actions/workflows/ci.yaml",
			Source: actionsv1alpha1.WorkflowRunSource{Type: actionsv1alpha1.SourceTypeGitHub, GitHub: &actionsv1alpha1.GitHubWorkflowRunSource{
				Actor:      "octocat",
				Repository: actionsv1alpha1.GitHubRepository{ID: 1, Owner: "acme", Name: "example"},
				Event:      actionsv1alpha1.GitHubEvent{Name: actionsv1alpha1.GitHubEventNamePush, DeliveryID: "delivery"},
				Revision:   actionsv1alpha1.GitRevision{SHA: strings.Repeat("a", 40), Ref: "refs/heads/main"},
			}},
		},
		Status: actionsv1alpha1.WorkflowRunStatus{
			WorkflowName: "CI",
			Jobs:         &actionsv1alpha1.WorkflowRunJobStatus{Total: 1},
			Conditions:   []metav1.Condition{plannedCondition(metav1.ConditionTrue, "JobsPlanned")},
		},
	}
	job := &actionsv1alpha1.WorkflowJob{
		TypeMeta: metav1.TypeMeta{APIVersion: actionsv1alpha1.GroupVersion.String(), Kind: "WorkflowJob"},
		ObjectMeta: metav1.ObjectMeta{
			Name: "build", Namespace: run.Namespace, UID: types.UID("job-uid"),
			Labels: map[string]string{actionsv1alpha1.LabelWorkflowRunUID: string(run.UID)},
		},
	}
	plan := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: childName(job.Name, "plan"), Namespace: job.Namespace}}
	if err := controllerutil.SetControllerReference(job, plan, scheme); err != nil {
		t.Fatal(err)
	}
	clusterClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&actionsv1alpha1.WorkflowRun{}, &actionsv1alpha1.WorkflowJob{}).
		WithObjects(project, run, job, plan).
		Build()
	reconciler := &WorkflowRunReconciler{Client: clusterClient, APIReader: clusterClient}
	if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(run)}); err != nil {
		t.Fatal(err)
	}
	stored := &actionsv1alpha1.WorkflowRun{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(run), stored); err != nil {
		t.Fatal(err)
	}
	if stored.Status.Identity == nil || stored.Status.Identity.ID != 1 || stored.Status.Identity.Number != 1 || stored.Status.Identity.Attempt != 1 {
		t.Fatalf("reconciled identity = %#v", stored.Status.Identity)
	}
	if stored.Status.Jobs == nil || stored.Status.Jobs.Queued != 1 {
		t.Fatalf("job summary = %#v", stored.Status.Jobs)
	}
}

func TestPlannedWorkflowRunIsObservedWithoutPlanningDependencies(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := actionsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	run := &actionsv1alpha1.WorkflowRun{
		ObjectMeta: metav1.ObjectMeta{Name: "ci", Namespace: "default", UID: types.UID("run-uid")},
		Status: actionsv1alpha1.WorkflowRunStatus{
			WorkflowName: "CI",
			Jobs:         &actionsv1alpha1.WorkflowRunJobStatus{Total: 1},
			Conditions:   []metav1.Condition{plannedCondition(metav1.ConditionTrue, "JobsPlanned")},
		},
	}
	job := &actionsv1alpha1.WorkflowJob{
		TypeMeta: metav1.TypeMeta{APIVersion: actionsv1alpha1.GroupVersion.String(), Kind: "WorkflowJob"},
		ObjectMeta: metav1.ObjectMeta{
			Name: "build", Namespace: "default", UID: types.UID("job-uid"), Labels: map[string]string{actionsv1alpha1.LabelWorkflowRunUID: string(run.UID)},
		},
	}
	plan := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: childName(job.Name, "plan"), Namespace: job.Namespace}}
	if err := controllerutil.SetControllerReference(job, plan, scheme); err != nil {
		t.Fatal(err)
	}
	clusterClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&actionsv1alpha1.WorkflowRun{}, &actionsv1alpha1.WorkflowJob{}).
		WithObjects(run, job, plan).
		Build()
	reconciler := &WorkflowRunReconciler{Client: clusterClient, APIReader: clusterClient}
	if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(run)}); err != nil {
		t.Fatal(err)
	}
	stored := &actionsv1alpha1.WorkflowRun{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(run), stored); err != nil {
		t.Fatal(err)
	}
	if stored.Status.Jobs == nil || stored.Status.Jobs.Queued != 1 {
		t.Fatalf("job summary = %#v", stored.Status.Jobs)
	}
}

func TestPlannedWorkflowRunFailsWhenAChildIsMissing(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := actionsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := batchv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	run := &actionsv1alpha1.WorkflowRun{
		ObjectMeta: metav1.ObjectMeta{Name: "ci", Namespace: "default", UID: types.UID("run-uid")},
		Status: actionsv1alpha1.WorkflowRunStatus{
			WorkflowName: "CI",
			Jobs:         &actionsv1alpha1.WorkflowRunJobStatus{Total: 2},
			Conditions:   []metav1.Condition{plannedCondition(metav1.ConditionTrue, "JobsPlanned")},
		},
	}
	job := &actionsv1alpha1.WorkflowJob{
		TypeMeta: metav1.TypeMeta{APIVersion: actionsv1alpha1.GroupVersion.String(), Kind: "WorkflowJob"},
		ObjectMeta: metav1.ObjectMeta{
			Name: "report", Namespace: run.Namespace, UID: types.UID("job-uid"),
			Labels: map[string]string{actionsv1alpha1.LabelWorkflowRunUID: string(run.UID)},
		},
		Spec: actionsv1alpha1.WorkflowJobSpec{JobID: "report", Needs: []string{"build"}},
	}
	if err := controllerutil.SetControllerReference(run, job, scheme); err != nil {
		t.Fatal(err)
	}
	plan := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: childName(job.Name, "plan"), Namespace: job.Namespace}}
	if err := controllerutil.SetControllerReference(job, plan, scheme); err != nil {
		t.Fatal(err)
	}
	clusterClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&actionsv1alpha1.WorkflowRun{}, &actionsv1alpha1.WorkflowJob{}).
		WithObjects(run, job, plan).
		Build()
	reconciler := &WorkflowRunReconciler{Client: clusterClient, APIReader: clusterClient}
	if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(run)}); err != nil {
		t.Fatal(err)
	}
	stored := &actionsv1alpha1.WorkflowRun{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(run), stored); err != nil {
		t.Fatal(err)
	}
	condition := meta.FindStatusCondition(stored.Status.Conditions, actionsv1alpha1.WorkflowRunConditionSucceeded)
	if condition == nil || condition.Status != metav1.ConditionFalse || condition.Reason != "ExecutionStateLost" {
		t.Fatalf("succeeded condition = %#v", condition)
	}
}

func TestPlannedWorkflowRunWaitsForActiveWorkloadAfterChildIsLost(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := actionsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := batchv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	run := &actionsv1alpha1.WorkflowRun{
		ObjectMeta: metav1.ObjectMeta{Name: "ci", Namespace: "default", UID: types.UID("run-uid")},
		Status: actionsv1alpha1.WorkflowRunStatus{
			WorkflowName: "CI",
			Jobs:         &actionsv1alpha1.WorkflowRunJobStatus{Total: 1},
			Conditions:   []metav1.Condition{plannedCondition(metav1.ConditionTrue, "JobsPlanned")},
		},
	}
	nativeJob := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
		Name: "build", Namespace: run.Namespace,
		Labels: map[string]string{actionsv1alpha1.LabelWorkflowRunUID: string(run.UID)},
	}}
	clusterClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&actionsv1alpha1.WorkflowRun{}, &batchv1.Job{}).
		WithObjects(run, nativeJob).
		Build()
	reconciler := &WorkflowRunReconciler{Client: clusterClient, APIReader: clusterClient}
	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(run)})
	if err != nil {
		t.Fatal(err)
	}
	if result.RequeueAfter == 0 {
		t.Fatal("WorkflowRun did not wait for the active native Job")
	}
	stored := &actionsv1alpha1.WorkflowRun{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(run), stored); err != nil {
		t.Fatal(err)
	}
	condition := meta.FindStatusCondition(stored.Status.Conditions, actionsv1alpha1.WorkflowRunConditionSucceeded)
	if condition == nil || condition.Status != metav1.ConditionUnknown || condition.Reason != "JobsRunning" {
		t.Fatalf("succeeded condition while native Job exists = %#v", condition)
	}
	if err := clusterClient.Delete(context.Background(), nativeJob); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(run)}); err != nil {
		t.Fatal(err)
	}
	stored = &actionsv1alpha1.WorkflowRun{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(run), stored); err != nil {
		t.Fatal(err)
	}
	condition = meta.FindStatusCondition(stored.Status.Conditions, actionsv1alpha1.WorkflowRunConditionSucceeded)
	if condition == nil || condition.Status != metav1.ConditionFalse || condition.Reason != "ExecutionStateLost" {
		t.Fatalf("succeeded condition after native Job deletion = %#v", condition)
	}
}

func TestInvalidWorkflowPlanningDoesNotRetry(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := actionsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	run := &actionsv1alpha1.WorkflowRun{ObjectMeta: metav1.ObjectMeta{Name: "ci", Namespace: "default"}}
	clusterClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&actionsv1alpha1.WorkflowRun{}).WithObjects(run).Build()
	reconciler := &WorkflowRunReconciler{Client: clusterClient, APIReader: clusterClient}
	if _, err := reconciler.planningFailed(context.Background(), run, "WorkflowInvalid", errors.New("invalid workflow"), planningFailureTerminal); err != nil {
		t.Fatalf("terminal planning failure returned an error: %v", err)
	}
	stored := &actionsv1alpha1.WorkflowRun{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(run), stored); err != nil {
		t.Fatal(err)
	}
	condition := meta.FindStatusCondition(stored.Status.Conditions, actionsv1alpha1.WorkflowRunConditionPlanned)
	if condition == nil || condition.Status != metav1.ConditionFalse || condition.Reason != "WorkflowInvalid" {
		t.Fatalf("planned condition = %#v", condition)
	}
	succeeded := meta.FindStatusCondition(stored.Status.Conditions, actionsv1alpha1.WorkflowRunConditionSucceeded)
	if succeeded == nil || succeeded.Status != metav1.ConditionFalse || succeeded.Reason != "WorkflowInvalid" {
		t.Fatalf("succeeded condition = %#v", succeeded)
	}
}

func TestProjectValuePlanningFailureRemainsRetryable(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := actionsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	run := &actionsv1alpha1.WorkflowRun{ObjectMeta: metav1.ObjectMeta{Name: "deploy", Namespace: "default"}}
	clusterClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&actionsv1alpha1.WorkflowRun{}).WithObjects(run).Build()
	reconciler := &WorkflowRunReconciler{Client: clusterClient, APIReader: clusterClient}
	project := &actionsv1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: "default"},
		Spec:       actionsv1alpha1.ProjectSpec{Variables: &actionsv1alpha1.ProjectVariableSource{ConfigMapRef: corev1.LocalObjectReference{Name: "missing"}}},
	}
	definition := &workflow.Definition{Name: "Deploy", Concurrency: workflow.Concurrency{Group: "deploy-${{ vars.ENVIRONMENT }}"}}
	_, _, cause := workflow.EvaluateConcurrency(definition, workflow.Event{}, reconciler.projectVariableContext(context.Background(), project))
	var unavailable *projectValuesUnavailableError
	if !errors.As(cause, &unavailable) {
		t.Fatalf("EvaluateConcurrency() error = %v", cause)
	}
	if _, err := reconciler.planningEvaluationFailed(context.Background(), run, cause); !errors.Is(err, cause) {
		t.Fatalf("planningEvaluationFailed() error = %v", err)
	}
	stored := &actionsv1alpha1.WorkflowRun{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(run), stored); err != nil {
		t.Fatal(err)
	}
	condition := meta.FindStatusCondition(stored.Status.Conditions, actionsv1alpha1.WorkflowRunConditionPlanned)
	if condition == nil || condition.Status != metav1.ConditionUnknown || condition.Reason != "ProjectValuesUnavailable" {
		t.Fatalf("planned condition = %#v", condition)
	}
}

func TestChildCreationFailureRemainsRetryable(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := actionsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	run := &actionsv1alpha1.WorkflowRun{ObjectMeta: metav1.ObjectMeta{Name: "ci", Namespace: "default"}}
	clusterClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&actionsv1alpha1.WorkflowRun{}).WithObjects(run).Build()
	reconciler := &WorkflowRunReconciler{Client: clusterClient, APIReader: clusterClient}
	cause := errors.New("admission unavailable")
	if _, err := reconciler.planningFailed(context.Background(), run, "ChildCreationFailed", cause, planningFailureRetry); !errors.Is(err, cause) {
		t.Fatalf("child creation failure error = %v", err)
	}
	stored := &actionsv1alpha1.WorkflowRun{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(run), stored); err != nil {
		t.Fatal(err)
	}
	condition := meta.FindStatusCondition(stored.Status.Conditions, actionsv1alpha1.WorkflowRunConditionPlanned)
	if condition == nil || condition.Status != metav1.ConditionUnknown || condition.Reason != "ChildCreationFailed" {
		t.Fatalf("planned condition = %#v", condition)
	}
}

func TestTerminalPlanningFailuresAreClassifiedByCause(t *testing.T) {
	notFound := fmt.Errorf("get workflow: %w", &githubclient.APIError{StatusCode: 404, Status: "404 Not Found"})
	if !githubAPIStatus(notFound, 404) {
		t.Fatal("GitHub 404 was not classified as terminal")
	}
	invalid := apierrors.NewInvalid(actionsv1alpha1.GroupVersion.WithKind("WorkflowJob").GroupKind(), "build", nil)
	if childCreationFailureDisposition(invalid) != planningFailureTerminal {
		t.Fatal("invalid child resource was not classified as terminal")
	}
	if childCreationFailureDisposition(errors.New("admission unavailable")) != planningFailureRetry {
		t.Fatal("transient child creation failure was classified as terminal")
	}
}

func TestWaitingWorkflowRunResumesWithoutRefetchingWorkflow(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := actionsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := coordinationv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	olderCondition := plannedCondition(metav1.ConditionTrue, "JobsPlanned")
	waitingCondition := plannedCondition(metav1.ConditionUnknown, "WaitingForConcurrency")
	older := concurrencyRun("older", "run-older", 1, time.Now().Add(-time.Minute), "deploy", &olderCondition)
	current := concurrencyRun("current", "run-current", 1, time.Now(), "deploy", &waitingCondition)
	current.Status.Concurrency = nil
	current.Status.ConcurrencyGroup = "deploy"
	current.Status.WorkflowName = "CI"
	current.Status.Jobs = &actionsv1alpha1.WorkflowRunJobStatus{Total: 1, Queued: 1}
	clusterClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&actionsv1alpha1.WorkflowRun{}).WithObjects(older, current).Build()
	reconciler := &WorkflowRunReconciler{Client: clusterClient, APIReader: clusterClient}

	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(current)})
	if err != nil {
		t.Fatal(err)
	}
	if result.RequeueAfter != 2*time.Second {
		t.Fatalf("requeue after = %v, want 2s", result.RequeueAfter)
	}
	stored := &actionsv1alpha1.WorkflowRun{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(current), stored); err != nil {
		t.Fatal(err)
	}
	if stored.Status.Concurrency == nil || stored.Status.Concurrency.Group != "deploy" || stored.Status.ConcurrencyGroup != "deploy" {
		t.Fatalf("migrated concurrency status = %#v, legacy group = %q", stored.Status.Concurrency, stored.Status.ConcurrencyGroup)
	}
}

func TestCancelledWaitingWorkflowRunDoesNotBypassConcurrencyGroup(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := actionsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := coordinationv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	olderCondition := plannedCondition(metav1.ConditionTrue, "JobsPlanned")
	waitingCondition := plannedCondition(metav1.ConditionUnknown, "WaitingForConcurrency")
	older := concurrencyRun("older", "run-older", 1, time.Now().Add(-time.Minute), "deploy", &olderCondition)
	current := concurrencyRun("current", "run-current", 1, time.Now(), "deploy", &waitingCondition)
	current.TypeMeta = metav1.TypeMeta{APIVersion: actionsv1alpha1.GroupVersion.String(), Kind: "WorkflowRun"}
	current.Spec.CancelRequested = true
	current.Status.WorkflowName = "CI"
	current.Status.Jobs = &actionsv1alpha1.WorkflowRunJobStatus{Total: 1, Queued: 1}
	cleanup := &actionsv1alpha1.WorkflowJob{
		TypeMeta: metav1.TypeMeta{APIVersion: actionsv1alpha1.GroupVersion.String(), Kind: "WorkflowJob"},
		ObjectMeta: metav1.ObjectMeta{
			Name: "cleanup", Namespace: current.Namespace, UID: types.UID("cleanup-uid"),
			Labels: map[string]string{actionsv1alpha1.LabelWorkflowRunUID: string(current.UID)},
		},
		Spec: actionsv1alpha1.WorkflowJobSpec{
			WorkflowRunRef: corev1.LocalObjectReference{Name: current.Name},
			JobID:          "cleanup",
			If:             "cancelled()",
		},
	}
	if err := controllerutil.SetControllerReference(current, cleanup, scheme); err != nil {
		t.Fatal(err)
	}
	plan := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: childName(cleanup.Name, "plan"), Namespace: cleanup.Namespace}}
	if err := controllerutil.SetControllerReference(cleanup, plan, scheme); err != nil {
		t.Fatal(err)
	}
	clusterClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&actionsv1alpha1.WorkflowRun{}, &actionsv1alpha1.WorkflowJob{}).
		WithObjects(older, current, cleanup, plan).
		Build()
	reconciler := &WorkflowRunReconciler{Client: clusterClient, APIReader: clusterClient}

	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(current)})
	if err != nil {
		t.Fatal(err)
	}
	if result.RequeueAfter != 2*time.Second {
		t.Fatalf("requeue after = %v, want 2s", result.RequeueAfter)
	}
	storedRun := &actionsv1alpha1.WorkflowRun{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(current), storedRun); err != nil {
		t.Fatal(err)
	}
	planned := meta.FindStatusCondition(storedRun.Status.Conditions, actionsv1alpha1.WorkflowRunConditionPlanned)
	if planned == nil || planned.Status != metav1.ConditionUnknown || planned.Reason != "WaitingForConcurrency" {
		t.Fatalf("planned condition = %#v", planned)
	}
	storedJob := &actionsv1alpha1.WorkflowJob{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(cleanup), storedJob); err != nil {
		t.Fatal(err)
	}
	if ready := meta.FindStatusCondition(storedJob.Status.Conditions, actionsv1alpha1.WorkflowJobConditionReady); ready != nil {
		t.Fatalf("cleanup job became ready while concurrency group was occupied: %#v", ready)
	}
}

func TestGitHubCheckFailurePreservesWorkflowRequeue(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := actionsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := coordinationv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	github, err := githubclient.NewClient("https://api.github.test", nil)
	if err != nil {
		t.Fatal(err)
	}
	olderCondition := plannedCondition(metav1.ConditionTrue, "JobsPlanned")
	waitingCondition := plannedCondition(metav1.ConditionUnknown, "WaitingForConcurrency")
	older := concurrencyRun("older", "run-older", 1, time.Now().Add(-time.Minute), "deploy", &olderCondition)
	current := concurrencyRun("current", "run-current", 1, time.Now(), "deploy", &waitingCondition)
	current.Status.WorkflowName = "CI"
	current.Status.Jobs = &actionsv1alpha1.WorkflowRunJobStatus{Total: 1, Queued: 1}
	clusterClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&actionsv1alpha1.WorkflowRun{}).WithObjects(older, current).Build()
	reconciler := &WorkflowRunReconciler{Client: clusterClient, APIReader: clusterClient, GitHub: github}

	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(current)})
	if err != nil {
		t.Fatalf("Check reporting failure replaced the workflow requeue: %v", err)
	}
	if result.RequeueAfter != 2*time.Second {
		t.Fatalf("requeue after = %v, want 2s", result.RequeueAfter)
	}
}

func TestWaitingWorkflowRunPersistsCancellationPolicy(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := actionsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	run := concurrencyRun("current", "run-current", 1, time.Now(), "deploy", nil)
	run.Status.Concurrency.CancelInProgress = true
	clusterClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&actionsv1alpha1.WorkflowRun{}).WithObjects(run).Build()
	reconciler := &WorkflowRunReconciler{Client: clusterClient, APIReader: clusterClient}
	if _, err := reconciler.waitingForConcurrency(context.Background(), run, "CI", 1, false); err != nil {
		t.Fatal(err)
	}
	stored := &actionsv1alpha1.WorkflowRun{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(run), stored); err != nil {
		t.Fatal(err)
	}
	condition := meta.FindStatusCondition(stored.Status.Conditions, actionsv1alpha1.WorkflowRunConditionPlanned)
	if condition == nil || condition.Status != metav1.ConditionUnknown || condition.Reason != "WaitingForConcurrencyCancellation" {
		t.Fatalf("planned condition = %#v", condition)
	}
	if stored.Status.Concurrency == nil || stored.Status.Concurrency.Group != "deploy" || !stored.Status.Concurrency.CancelInProgress {
		t.Fatalf("persisted concurrency decision = %#v", stored.Status.Concurrency)
	}
}

func TestPersistWorkflowRunConcurrencyDecisionStoresFalse(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := actionsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	run := concurrencyRun("current", "run-current", 1, time.Now(), "", nil)
	clusterClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&actionsv1alpha1.WorkflowRun{}).WithObjects(run).Build()
	reconciler := &WorkflowRunReconciler{Client: clusterClient, APIReader: clusterClient}
	if err := reconciler.persistWorkflowRunConcurrencyDecision(context.Background(), run, "deploy", false); err != nil {
		t.Fatal(err)
	}
	stored := &actionsv1alpha1.WorkflowRun{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(run), stored); err != nil {
		t.Fatal(err)
	}
	if stored.Status.Concurrency == nil || stored.Status.Concurrency.Group != "deploy" || stored.Status.Concurrency.CancelInProgress || stored.Status.ConcurrencyGroup != "deploy" {
		t.Fatalf("persisted concurrency decision = %#v, legacy group = %q", stored.Status.Concurrency, stored.Status.ConcurrencyGroup)
	}
}

func TestWaitingWorkflowRunRestoresCancellationDecision(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := actionsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := coordinationv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	olderCondition := plannedCondition(metav1.ConditionTrue, "JobsPlanned")
	waitingCondition := plannedCondition(metav1.ConditionUnknown, "WaitingForConcurrencyCancellation")
	older := concurrencyRun("older", "run-older", 1, time.Now().Add(-time.Minute), "deploy", &olderCondition)
	current := concurrencyRun("current", "run-current", 1, time.Now(), "deploy", &waitingCondition)
	current.Status.Concurrency = nil
	current.Status.ConcurrencyGroup = "deploy"
	current.Status.WorkflowName = "CI"
	current.Status.Jobs = &actionsv1alpha1.WorkflowRunJobStatus{Total: 1, Queued: 1}
	clusterClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&actionsv1alpha1.WorkflowRun{}).WithObjects(older, current).Build()
	reconciler := &WorkflowRunReconciler{Client: clusterClient, APIReader: clusterClient}

	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(current)})
	if err != nil {
		t.Fatal(err)
	}
	if result.RequeueAfter != 2*time.Second {
		t.Fatalf("requeue after = %v, want 2s", result.RequeueAfter)
	}
	storedOlder := &actionsv1alpha1.WorkflowRun{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(older), storedOlder); err != nil {
		t.Fatal(err)
	}
	if !storedOlder.Spec.CancelRequested {
		t.Fatal("restored cancellation decision did not cancel the executing run")
	}
	storedCurrent := &actionsv1alpha1.WorkflowRun{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(current), storedCurrent); err != nil {
		t.Fatal(err)
	}
	if storedCurrent.Status.Concurrency == nil || !storedCurrent.Status.Concurrency.CancelInProgress {
		t.Fatalf("restored concurrency decision = %#v", storedCurrent.Status.Concurrency)
	}
}

func TestWorkflowJobLabelsEncodeInvalidLabelValues(t *testing.T) {
	run := &actionsv1alpha1.WorkflowRun{ObjectMeta: metav1.ObjectMeta{UID: "run-uid"}}
	project := &actionsv1alpha1.Project{ObjectMeta: metav1.ObjectMeta{UID: "project-uid"}}

	for _, jobID := range []string{"_build", strings.Repeat("a", 64)} {
		t.Run(jobID[:1], func(t *testing.T) {
			digest := sha256.Sum256([]byte(jobID))
			want := strings.ToLower(digestEncoding.EncodeToString(digest[:]))
			got := workflowJobLabels(run, project, jobID)[actionsv1alpha1.LabelWorkflowJob]
			if got != want {
				t.Errorf("workflow job label = %q, want %q", got, want)
			}
			if problems := validation.IsValidLabelValue(got); len(problems) > 0 {
				t.Errorf("workflow job label is invalid: %v", problems)
			}
		})
	}
}

func TestWorkflowRunCountsAssignedJobWithoutReloadingEventSnapshot(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := actionsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	run := &actionsv1alpha1.WorkflowRun{
		ObjectMeta: metav1.ObjectMeta{
			Name: "ci", Namespace: "default", UID: types.UID("run-uid"),
			Annotations: map[string]string{eventsnapshot.Annotation: "missing-snapshot"},
		},
	}
	job := &actionsv1alpha1.WorkflowJob{
		TypeMeta: metav1.TypeMeta{APIVersion: actionsv1alpha1.GroupVersion.String(), Kind: "WorkflowJob"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "build",
			Namespace: "default",
			UID:       types.UID("job-uid"),
			Labels:    map[string]string{actionsv1alpha1.LabelWorkflowRunUID: string(run.UID)},
		},
		Spec: actionsv1alpha1.WorkflowJobSpec{JobID: "build", If: "github.event.sender.login == 'octocat'"},
		Status: actionsv1alpha1.WorkflowJobStatus{
			RunnerRef: &corev1.LocalObjectReference{Name: "runner-1"},
		},
	}
	plan := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: childName(job.Name, "plan"), Namespace: job.Namespace}}
	if err := controllerutil.SetControllerReference(job, plan, scheme); err != nil {
		t.Fatal(err)
	}
	clusterClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&actionsv1alpha1.WorkflowRun{}).
		WithObjects(run, job, plan).
		Build()
	reconciler := &WorkflowRunReconciler{Client: clusterClient, APIReader: clusterClient}
	if _, err := reconciler.observeWorkflowJobs(context.Background(), run, "CI", 1); err != nil {
		t.Fatal(err)
	}
	stored := &actionsv1alpha1.WorkflowRun{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(run), stored); err != nil {
		t.Fatal(err)
	}
	if stored.Status.Jobs == nil || stored.Status.Jobs.Active != 1 || stored.Status.Jobs.Queued != 0 {
		t.Fatalf("job summary = %#v", stored.Status.Jobs)
	}
	condition := meta.FindStatusCondition(stored.Status.Conditions, actionsv1alpha1.WorkflowRunConditionSucceeded)
	if condition == nil || condition.Status != metav1.ConditionUnknown || condition.Reason != "JobsRunning" {
		t.Fatalf("succeeded condition = %#v", condition)
	}
}

func TestWorkflowRunAggregatesMatrixFailure(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := actionsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	run := &actionsv1alpha1.WorkflowRun{ObjectMeta: metav1.ObjectMeta{Name: "release", Namespace: "default", UID: "run-uid"}}
	jobs := []client.Object{run}
	for index, status := range []metav1.ConditionStatus{metav1.ConditionTrue, metav1.ConditionFalse} {
		arch := []string{"amd64", "arm64"}[index]
		jobs = append(jobs, &actionsv1alpha1.WorkflowJob{
			ObjectMeta: metav1.ObjectMeta{Name: "build-" + arch, Namespace: run.Namespace, Labels: map[string]string{actionsv1alpha1.LabelWorkflowRunUID: string(run.UID)}},
			Spec: actionsv1alpha1.WorkflowJobSpec{
				WorkflowRunRef: corev1.LocalObjectReference{Name: run.Name}, JobID: fmt.Sprintf("build-matrix-%d", index+1),
				Matrix: &actionsv1alpha1.WorkflowJobMatrix{LogicalJobID: "build", Values: map[string]string{"arch": arch}, MaxParallel: 1},
			},
			Status: actionsv1alpha1.WorkflowJobStatus{Conditions: []metav1.Condition{{Type: actionsv1alpha1.WorkflowJobConditionSucceeded, Status: status, Reason: "Completed", Message: "Completed"}}},
		})
	}
	clusterClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&actionsv1alpha1.WorkflowRun{}).WithObjects(jobs...).Build()
	reconciler := &WorkflowRunReconciler{Client: clusterClient, APIReader: clusterClient}

	if _, err := reconciler.observeWorkflowJobs(context.Background(), run, "Release", 2); err != nil {
		t.Fatal(err)
	}
	stored := &actionsv1alpha1.WorkflowRun{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(run), stored); err != nil {
		t.Fatal(err)
	}
	if stored.Status.Jobs == nil || stored.Status.Jobs.Succeeded != 1 || stored.Status.Jobs.Failed != 1 {
		t.Fatalf("job summary = %#v", stored.Status.Jobs)
	}
	condition := meta.FindStatusCondition(stored.Status.Conditions, actionsv1alpha1.WorkflowRunConditionSucceeded)
	if condition == nil || condition.Status != metav1.ConditionFalse || condition.Reason != "JobFailed" {
		t.Fatalf("succeeded condition = %#v", condition)
	}
}

func TestWorkflowRunTerminalConclusions(t *testing.T) {
	for _, test := range []struct {
		name            string
		cancelRequested bool
		results         []actionsv1alpha1.WorkflowJobResult
		failFast        bool
		timedOut        bool
		wantStatus      metav1.ConditionStatus
		wantReason      string
		wantMessage     string
	}{
		{name: "success with skipped job", results: []actionsv1alpha1.WorkflowJobResult{actionsv1alpha1.WorkflowJobResultSuccess, actionsv1alpha1.WorkflowJobResultSkipped}, wantStatus: metav1.ConditionTrue, wantReason: "JobsSucceeded", wantMessage: "All required WorkflowJobs succeeded"},
		{name: "cancellation after successful completion", cancelRequested: true, results: []actionsv1alpha1.WorkflowJobResult{actionsv1alpha1.WorkflowJobResultSuccess, actionsv1alpha1.WorkflowJobResultSuccess}, wantStatus: metav1.ConditionTrue, wantReason: "JobsSucceeded", wantMessage: "All required WorkflowJobs succeeded"},
		{name: "failure", results: []actionsv1alpha1.WorkflowJobResult{actionsv1alpha1.WorkflowJobResultSuccess, actionsv1alpha1.WorkflowJobResultFailure}, wantStatus: metav1.ConditionFalse, wantReason: "JobFailed", wantMessage: "At least one WorkflowJob failed"},
		{name: "timeout", results: []actionsv1alpha1.WorkflowJobResult{actionsv1alpha1.WorkflowJobResultSuccess, actionsv1alpha1.WorkflowJobResultFailure}, timedOut: true, wantStatus: metav1.ConditionFalse, wantReason: "JobTimedOut", wantMessage: "At least one WorkflowJob timed out"},
		{name: "cancellation request takes precedence over failure", cancelRequested: true, results: []actionsv1alpha1.WorkflowJobResult{actionsv1alpha1.WorkflowJobResultSuccess, actionsv1alpha1.WorkflowJobResultFailure}, wantStatus: metav1.ConditionFalse, wantReason: "JobCancelled", wantMessage: "Workflow cancellation was requested"},
		{name: "cancelled job takes precedence over failure", results: []actionsv1alpha1.WorkflowJobResult{actionsv1alpha1.WorkflowJobResultFailure, actionsv1alpha1.WorkflowJobResultCancelled}, wantStatus: metav1.ConditionFalse, wantReason: "JobCancelled", wantMessage: "At least one WorkflowJob was cancelled"},
		{name: "matrix fail-fast preserves failure", results: []actionsv1alpha1.WorkflowJobResult{actionsv1alpha1.WorkflowJobResultFailure, actionsv1alpha1.WorkflowJobResultCancelled}, failFast: true, wantStatus: metav1.ConditionFalse, wantReason: "JobFailed", wantMessage: "At least one WorkflowJob failed"},
	} {
		t.Run(test.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			if err := actionsv1alpha1.AddToScheme(scheme); err != nil {
				t.Fatal(err)
			}
			if err := corev1.AddToScheme(scheme); err != nil {
				t.Fatal(err)
			}
			if err := batchv1.AddToScheme(scheme); err != nil {
				t.Fatal(err)
			}
			run := &actionsv1alpha1.WorkflowRun{
				ObjectMeta: metav1.ObjectMeta{Name: "ci", Namespace: "default", UID: types.UID("run-uid")},
				Spec:       actionsv1alpha1.WorkflowRunSpec{CancelRequested: test.cancelRequested},
			}
			objects := []client.Object{run}
			for index, result := range test.results {
				name := fmt.Sprintf("job-%d", index)
				job := &actionsv1alpha1.WorkflowJob{
					ObjectMeta: metav1.ObjectMeta{
						Name: name, Namespace: run.Namespace,
						Labels: map[string]string{actionsv1alpha1.LabelWorkflowRunUID: string(run.UID)},
					},
					Spec:   actionsv1alpha1.WorkflowJobSpec{JobID: name},
					Status: actionsv1alpha1.WorkflowJobStatus{Result: result},
				}
				if test.timedOut && result == actionsv1alpha1.WorkflowJobResultFailure {
					job.Status.Conditions = []metav1.Condition{{Type: actionsv1alpha1.WorkflowJobConditionSucceeded, Status: metav1.ConditionFalse, Reason: "JobTimedOut"}}
				}
				if test.failFast && result == actionsv1alpha1.WorkflowJobResultCancelled {
					job.Status.Conditions = []metav1.Condition{{Type: actionsv1alpha1.WorkflowJobConditionScheduled, Status: metav1.ConditionFalse, Reason: matrixFailFastReason}}
				}
				objects = append(objects, job)
			}
			clusterClient := fake.NewClientBuilder().
				WithScheme(scheme).
				WithStatusSubresource(&actionsv1alpha1.WorkflowRun{}, &actionsv1alpha1.WorkflowJob{}).
				WithObjects(objects...).
				Build()
			reconciler := &WorkflowRunReconciler{Client: clusterClient, APIReader: clusterClient}
			if _, err := reconciler.observeWorkflowJobs(context.Background(), run, "CI", int32(len(test.results))); err != nil {
				t.Fatal(err)
			}
			stored := &actionsv1alpha1.WorkflowRun{}
			if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(run), stored); err != nil {
				t.Fatal(err)
			}
			condition := meta.FindStatusCondition(stored.Status.Conditions, actionsv1alpha1.WorkflowRunConditionSucceeded)
			if condition == nil || condition.Status != test.wantStatus || condition.Reason != test.wantReason || condition.Message != test.wantMessage {
				t.Fatalf("succeeded condition = %#v, want status %s reason %q message %q", condition, test.wantStatus, test.wantReason, test.wantMessage)
			}
		})
	}
}

func TestTerminalConcurrencyWaitDoesNotRequeueWorkflowRun(t *testing.T) {
	scheme := runtime.NewScheme()
	for name, add := range map[string]func(*runtime.Scheme) error{
		"actions": actionsv1alpha1.AddToScheme, "batch": batchv1.AddToScheme, "coordination": coordinationv1.AddToScheme, "core": corev1.AddToScheme,
	} {
		if err := add(scheme); err != nil {
			t.Fatalf("add %s types: %v", name, err)
		}
	}
	run := concurrencyRun("ci", "run-uid", 1, time.Unix(1, 0), "", nil)
	job := &actionsv1alpha1.WorkflowJob{
		ObjectMeta: metav1.ObjectMeta{
			Name: "deploy", Namespace: run.Namespace,
			Labels: map[string]string{actionsv1alpha1.LabelWorkflowRunUID: string(run.UID)},
		},
		Spec: actionsv1alpha1.WorkflowJobSpec{JobID: "deploy"},
		Status: actionsv1alpha1.WorkflowJobStatus{
			Result:      actionsv1alpha1.WorkflowJobResultCancelled,
			Concurrency: &actionsv1alpha1.ConcurrencyStatus{Group: "deploy"},
			Conditions: []metav1.Condition{{
				Type: actionsv1alpha1.WorkflowJobConditionConcurrencyAcquired, Status: metav1.ConditionUnknown,
				Reason: concurrencyWaitingReason, Message: concurrencyWaitingReason, LastTransitionTime: metav1.Now(),
			}},
		},
	}
	clusterClient := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&actionsv1alpha1.WorkflowRun{}, &actionsv1alpha1.WorkflowJob{}).
		WithObjects(run, job).Build()
	reconciler := &WorkflowRunReconciler{Client: clusterClient, APIReader: clusterClient}

	result, err := reconciler.observeWorkflowJobs(context.Background(), run, "CI", 1)
	if err != nil {
		t.Fatal(err)
	}
	if result.Requeue || result.RequeueAfter != 0 {
		t.Fatalf("terminal WorkflowRun requeue = %#v", result)
	}
	stored := &actionsv1alpha1.WorkflowRun{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(run), stored); err != nil {
		t.Fatal(err)
	}
	condition := meta.FindStatusCondition(stored.Status.Conditions, actionsv1alpha1.WorkflowRunConditionSucceeded)
	if condition == nil || condition.Status != metav1.ConditionFalse || condition.Reason != "JobCancelled" {
		t.Fatalf("terminal WorkflowRun condition = %#v", condition)
	}
}

func TestCancelledWorkflowRunWaitsForTerminatingWorkloads(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := actionsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := batchv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	run := &actionsv1alpha1.WorkflowRun{ObjectMeta: metav1.ObjectMeta{Name: "ci", Namespace: "default", UID: types.UID("run-uid")}}
	job := &actionsv1alpha1.WorkflowJob{
		ObjectMeta: metav1.ObjectMeta{
			Name: "build", Namespace: run.Namespace,
			Labels: map[string]string{actionsv1alpha1.LabelWorkflowRunUID: string(run.UID)},
		},
		Spec:   actionsv1alpha1.WorkflowJobSpec{JobID: "build"},
		Status: actionsv1alpha1.WorkflowJobStatus{Result: actionsv1alpha1.WorkflowJobResultCancelled},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "build-runner", Namespace: run.Namespace,
			Labels: map[string]string{actionsv1alpha1.LabelWorkflowRunUID: string(run.UID)},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
	clusterClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&actionsv1alpha1.WorkflowRun{}, &actionsv1alpha1.WorkflowJob{}, &corev1.Pod{}).
		WithObjects(run, job, pod).
		Build()
	reconciler := &WorkflowRunReconciler{Client: clusterClient, APIReader: clusterClient}

	result, err := reconciler.observeWorkflowJobs(context.Background(), run, "CI", 1)
	if err != nil {
		t.Fatal(err)
	}
	if result.RequeueAfter != 2*time.Second {
		t.Fatalf("requeue after = %v, want 2s", result.RequeueAfter)
	}
	stored := &actionsv1alpha1.WorkflowRun{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(run), stored); err != nil {
		t.Fatal(err)
	}
	condition := meta.FindStatusCondition(stored.Status.Conditions, actionsv1alpha1.WorkflowRunConditionSucceeded)
	if condition == nil || condition.Status != metav1.ConditionUnknown || condition.Reason != "JobsRunning" {
		t.Fatalf("succeeded condition while Pod terminates = %#v", condition)
	}
	if stored.Status.CompletionTime != nil {
		t.Fatalf("completion time while Pod terminates = %v", stored.Status.CompletionTime)
	}

	if err := clusterClient.Delete(context.Background(), pod); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.observeWorkflowJobs(context.Background(), stored, "CI", 1); err != nil {
		t.Fatal(err)
	}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(run), stored); err != nil {
		t.Fatal(err)
	}
	condition = meta.FindStatusCondition(stored.Status.Conditions, actionsv1alpha1.WorkflowRunConditionSucceeded)
	if condition == nil || condition.Status != metav1.ConditionFalse || condition.Reason != "JobCancelled" {
		t.Fatalf("succeeded condition after Pod termination = %#v", condition)
	}
}

func createControllerTestRepository(t *testing.T) (string, string, string, string) {
	t.Helper()
	root := t.TempDir()
	work := filepath.Join(root, "work")
	if err := os.Mkdir(work, 0o755); err != nil {
		t.Fatal(err)
	}
	runControllerTestGit(t, work, "init", "--quiet")
	runControllerTestGit(t, work, "config", "user.name", "Open Actions Test")
	runControllerTestGit(t, work, "config", "user.email", "test@example.com")
	if err := os.WriteFile(filepath.Join(work, "README.md"), []byte("test\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	runControllerTestGit(t, work, "add", "README.md")
	runControllerTestGit(t, work, "commit", "--quiet", "-m", "common")
	mergeBaseSHA := runControllerTestGit(t, work, "rev-parse", "HEAD")

	runControllerTestGit(t, work, "checkout", "--quiet", "-b", "base")
	workflowDirectory := filepath.Join(work, ".open-actions", "workflows")
	if err := os.MkdirAll(workflowDirectory, 0o755); err != nil {
		t.Fatal(err)
	}
	workflowData := "name: CI\non:\n  pull_request:\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo ok\n"
	if err := os.WriteFile(filepath.Join(workflowDirectory, "ci.yaml"), []byte(workflowData), 0o644); err != nil {
		t.Fatal(err)
	}
	runControllerTestGit(t, work, "add", ".open-actions/workflows/ci.yaml")
	runControllerTestGit(t, work, "commit", "--quiet", "-m", "base")
	baseSHA := runControllerTestGit(t, work, "rev-parse", "HEAD")

	runControllerTestGit(t, work, "checkout", "--quiet", "-b", "head", mergeBaseSHA)
	if err := os.WriteFile(filepath.Join(work, "feature.txt"), []byte("feature\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	runControllerTestGit(t, work, "add", "feature.txt")
	runControllerTestGit(t, work, "commit", "--quiet", "-m", "head")
	headSHA := runControllerTestGit(t, work, "rev-parse", "HEAD")

	remote := filepath.Join(root, "acme", "example")
	if err := os.MkdirAll(filepath.Dir(remote), 0o755); err != nil {
		t.Fatal(err)
	}
	runControllerTestGit(t, "", "clone", "--quiet", "--bare", work, remote)
	return root, baseSHA, headSHA, mergeBaseSHA
}

func runControllerTestGit(t *testing.T, directory string, arguments ...string) string {
	t.Helper()
	command := exec.Command("git", arguments...)
	command.Dir = directory
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("git %s: %v: %s", strings.Join(arguments, " "), err, output)
	}
	return strings.TrimSpace(string(output))
}

func TestMatrixFailFastCancelsOnlyUnfinishedSiblings(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := actionsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	run := &actionsv1alpha1.WorkflowRun{ObjectMeta: metav1.ObjectMeta{Name: "release", Namespace: "default", UID: "run-uid"}}
	job := func(name, logicalID string, failFast *bool, runnerName string, result metav1.ConditionStatus, reason string) *actionsv1alpha1.WorkflowJob {
		workflowJob := &actionsv1alpha1.WorkflowJob{
			ObjectMeta: metav1.ObjectMeta{
				Name: name, Namespace: run.Namespace,
				Labels: map[string]string{actionsv1alpha1.LabelWorkflowRunUID: string(run.UID)},
			},
			Spec: actionsv1alpha1.WorkflowJobSpec{
				WorkflowRunRef: corev1.LocalObjectReference{Name: run.Name}, JobID: name,
				Matrix: &actionsv1alpha1.WorkflowJobMatrix{LogicalJobID: logicalID, Values: map[string]string{"case": name}, FailFast: failFast},
			},
		}
		if runnerName != "" {
			workflowJob.Status.RunnerRef = &corev1.LocalObjectReference{Name: runnerName}
		}
		if result != "" {
			workflowJob.Status.Conditions = []metav1.Condition{{
				Type: actionsv1alpha1.WorkflowJobConditionSucceeded, Status: result, Reason: reason, Message: reason,
			}}
		}
		return workflowJob
	}

	failed := job("build-failed", "build", nil, "runner-1", metav1.ConditionFalse, "JobFailed")
	queued := job("build-queued", "build", nil, "", "", "")
	active := job("build-active", "build", nil, "runner-2", metav1.ConditionUnknown, "JobRunning")
	completed := job("build-completed", "build", nil, "runner-3", metav1.ConditionTrue, "JobSucceeded")
	otherGroup := job("test-queued", "test", pointerTo(true), "", "", "")
	otherRun := job("other-run-build-queued", "build", pointerTo(true), "", "", "")
	otherRun.Labels[actionsv1alpha1.LabelWorkflowRunUID] = "other-run-uid"
	disabledFailure := job("lint-failed", "lint", pointerTo(false), "runner-4", metav1.ConditionFalse, "JobFailed")
	disabledQueued := job("lint-queued", "lint", pointerTo(false), "", "", "")
	cancelledFailure := job("cancelled-failed", "cancelled", pointerTo(true), "runner-5", metav1.ConditionFalse, "CancellationRequested")
	cancelledQueued := job("cancelled-queued", "cancelled", pointerTo(true), "", "", "")
	objects := []client.Object{run, failed, queued, active, completed, otherGroup, otherRun, disabledFailure, disabledQueued, cancelledFailure, cancelledQueued}
	clusterClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&actionsv1alpha1.WorkflowJob{}).WithObjects(objects...).Build()

	reconciler := &WorkflowRunReconciler{Client: clusterClient, APIReader: clusterClient}
	jobs := &actionsv1alpha1.WorkflowJobList{}
	if err := clusterClient.List(context.Background(), jobs); err != nil {
		t.Fatal(err)
	}
	pending, err := reconciler.reconcileMatrixFailFast(context.Background(), jobs)
	if err != nil {
		t.Fatal(err)
	}
	if !pending {
		t.Fatal("active matrix sibling did not keep fail-fast reconciliation pending")
	}

	jobs = &actionsv1alpha1.WorkflowJobList{}
	if err := clusterClient.List(context.Background(), jobs); err != nil {
		t.Fatal(err)
	}
	restarted := &WorkflowRunReconciler{Client: clusterClient, APIReader: clusterClient}
	if pending, err := restarted.reconcileMatrixFailFast(context.Background(), jobs); err != nil || !pending {
		t.Fatalf("restart reconciliation = pending %v, error %v", pending, err)
	}

	stored := func(name string) *actionsv1alpha1.WorkflowJob {
		workflowJob := &actionsv1alpha1.WorkflowJob{}
		if err := clusterClient.Get(context.Background(), client.ObjectKey{Namespace: run.Namespace, Name: name}, workflowJob); err != nil {
			t.Fatal(err)
		}
		return workflowJob
	}
	queuedJob := stored(queued.Name)
	if queuedJob.Status.Result != actionsv1alpha1.WorkflowJobResultCancelled || queuedJob.Status.CompletionTime == nil {
		t.Fatalf("queued status = %#v", queuedJob.Status)
	}
	queuedScheduled := meta.FindStatusCondition(queuedJob.Status.Conditions, actionsv1alpha1.WorkflowJobConditionScheduled)
	if queuedScheduled == nil || queuedScheduled.Status != metav1.ConditionFalse || queuedScheduled.Reason != matrixFailFastReason {
		t.Fatalf("queued scheduled condition = %#v", queuedScheduled)
	}
	activeCancellation := meta.FindStatusCondition(stored(active.Name).Status.Conditions, actionsv1alpha1.WorkflowJobConditionCancellationRequested)
	if activeCancellation == nil || activeCancellation.Status != metav1.ConditionTrue || activeCancellation.Reason != matrixFailFastReason {
		t.Fatalf("active cancellation = %#v", activeCancellation)
	}
	if result := meta.FindStatusCondition(stored(completed.Name).Status.Conditions, actionsv1alpha1.WorkflowJobConditionSucceeded); result == nil || result.Status != metav1.ConditionTrue {
		t.Fatalf("concurrently completed result = %#v", result)
	}
	for _, unaffected := range []*actionsv1alpha1.WorkflowJob{otherGroup, otherRun, disabledQueued, cancelledQueued} {
		if result := meta.FindStatusCondition(stored(unaffected.Name).Status.Conditions, actionsv1alpha1.WorkflowJobConditionSucceeded); result != nil {
			t.Errorf("unrelated WorkflowJob %q result = %#v", unaffected.Name, result)
		}
	}
	if result := meta.FindStatusCondition(stored(failed.Name).Status.Conditions, actionsv1alpha1.WorkflowJobConditionSucceeded); result == nil || result.Reason != "JobFailed" {
		t.Fatalf("triggering failure = %#v", result)
	}
}
