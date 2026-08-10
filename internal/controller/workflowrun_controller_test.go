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
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	actionsv1alpha1 "github.com/kelos-dev/open-actions/api/v1alpha1"
	githubclient "github.com/kelos-dev/open-actions/internal/github"
	"github.com/kelos-dev/open-actions/internal/runner"
	"github.com/kelos-dev/open-actions/internal/workflow"
	batchv1 "k8s.io/api/batch/v1"
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

func TestJobPlanCoversSupportedSteps(t *testing.T) {
	reconciler := &WorkflowRunReconciler{GitHubServerURL: "https://github.com", GitHubAPIBase: "https://api.github.com", ActionCloneBaseURL: "https://github.com/git"}
	run := &actionsv1alpha1.WorkflowRun{Spec: actionsv1alpha1.WorkflowRunSpec{
		Source: actionsv1alpha1.WorkflowRunSource{
			Type: actionsv1alpha1.SourceTypeGitHub,
			GitHub: &actionsv1alpha1.GitHubWorkflowRunSource{
				Repository: actionsv1alpha1.GitHubRepository{ID: 1, Owner: "example", Name: "project"},
				Event:      actionsv1alpha1.GitHubEvent{Name: "push", DeliveryID: "delivery"},
				Revision:   actionsv1alpha1.GitRevision{SHA: strings.Repeat("a", 40), Ref: "refs/heads/main", BaseRef: "target"},
			},
		},
	}}
	job := workflow.Job{RunsOn: workflow.StringList{"ubuntu-latest"}, Steps: []workflow.Step{
		{Uses: "actions/checkout@v4"},
		{Uses: "actions/setup-go@v5", With: map[string]any{"go-version-file": "go.mod"}},
		{Name: "Build", Run: "make build"},
	}}
	plan, err := reconciler.jobPlan(run, "CI", "build", job)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Repository.ServerURL != "https://github.com" || plan.Repository.APIURL != "https://api.github.com" || plan.Repository.ActionCloneBaseURL != "https://github.com/git" {
		t.Errorf("repository endpoints = %#v", plan.Repository)
	}
	if plan.Version != runner.PlanVersion || plan.Repository.ID != 1 || plan.Event.DeliveryID != "delivery" || plan.Revision.BaseRef != "target" {
		t.Errorf("plan identity = %#v", plan)
	}
	if len(plan.Steps) != 3 {
		t.Errorf("steps = %d", len(plan.Steps))
	}
}

func TestPlanWorkflowJobsSetsDisplayNames(t *testing.T) {
	reconciler := &WorkflowRunReconciler{}
	run := &actionsv1alpha1.WorkflowRun{Spec: actionsv1alpha1.WorkflowRunSpec{Source: actionsv1alpha1.WorkflowRunSource{
		Type: actionsv1alpha1.SourceTypeGitHub,
		GitHub: &actionsv1alpha1.GitHubWorkflowRunSource{
			Repository: actionsv1alpha1.GitHubRepository{ID: 1},
			Event:      actionsv1alpha1.GitHubEvent{Name: "pull_request", Action: "synchronize", DeliveryID: "delivery"},
			Revision:   actionsv1alpha1.GitRevision{SHA: strings.Repeat("a", 40), Ref: "refs/pull/1/merge", HeadRef: "feature", BaseRef: "main"},
		},
	}}}
	definition := &workflow.Definition{Name: "CI", Jobs: map[string]workflow.Job{
		"build": {Name: "Build ${{ github.base_ref }}", RunsOn: workflow.StringList{"ubuntu-latest"}},
		"lint":  {RunsOn: workflow.StringList{"ubuntu-latest"}},
	}}

	planned, err := reconciler.planWorkflowJobs(run, definition)
	if err != nil {
		t.Fatal(err)
	}
	if len(planned) != 2 {
		t.Fatalf("planned jobs = %d, want 2", len(planned))
	}
	if planned[0].id != "build" || planned[0].displayName != "Build main" {
		t.Errorf("build job = %#v", planned[0])
	}
	if planned[1].id != "lint" || planned[1].displayName != "lint" {
		t.Errorf("lint job = %#v", planned[1])
	}
}

func TestWorkflowEventIncludesRevisionValues(t *testing.T) {
	event := workflowEvent(&actionsv1alpha1.GitHubWorkflowRunSource{
		Event:    actionsv1alpha1.GitHubEvent{Name: "push", Action: "created"},
		Revision: actionsv1alpha1.GitRevision{Ref: "refs/heads/main", HeadRef: "feature", BaseRef: "target"},
	})
	if event.Name != "push" || event.Action != "created" || event.Ref != "refs/heads/main" || event.RefName != "main" || event.HeadRef != "feature" || event.BaseRef != "target" {
		t.Fatalf("workflow event = %#v", event)
	}
}

func TestGitHubCheckRunIsCreatedAndRecorded(t *testing.T) {
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
		case request.Method == http.MethodGet && strings.HasSuffix(request.URL.Path, "/check-runs"):
			fmt.Fprint(writer, `{"total_count":0,"check_runs":[]}`)
		case request.Method == http.MethodPost && request.URL.Path == "/repos/acme/example/check-runs":
			body := githubclient.CreateCheckRunRequest{}
			if err := json.NewDecoder(request.Body).Decode(&body); err != nil || body.ExternalID != "run-uid" || body.Status != "queued" || body.HeadSHA != strings.Repeat("a", 40) {
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
		ObjectMeta: metav1.ObjectMeta{Name: "ci", Namespace: "default", UID: "run-uid"},
		Spec: actionsv1alpha1.WorkflowRunSpec{
			ProjectRef:   corev1.LocalObjectReference{Name: project.Name},
			WorkflowPath: ".open-actions/workflows/ci.yaml",
			Source: actionsv1alpha1.WorkflowRunSource{Type: actionsv1alpha1.SourceTypeGitHub, GitHub: &actionsv1alpha1.GitHubWorkflowRunSource{
				Repository: actionsv1alpha1.GitHubRepository{ID: 3, Owner: "acme", Name: "example"},
				Revision:   actionsv1alpha1.GitRevision{SHA: strings.Repeat("a", 40)},
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
}

func TestWorkflowRunCheckReportMapsLifecycle(t *testing.T) {
	run := &actionsv1alpha1.WorkflowRun{Spec: actionsv1alpha1.WorkflowRunSpec{WorkflowPath: ".open-actions/workflows/ci.yaml"}}
	if report := workflowRunCheckReport(run); report.Status != "queued" || report.Conclusion != "" {
		t.Fatalf("queued report = %#v", report)
	}
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
	deletionTime := metav1.NewTime(time.Unix(1_700_000_000, 0))
	canceled := &actionsv1alpha1.WorkflowRun{ObjectMeta: metav1.ObjectMeta{DeletionTimestamp: &deletionTime}}
	if report := workflowRunCheckReport(canceled); report.Status != "completed" || report.Conclusion != "cancelled" || report.CompletedAt != deletionTime.UTC().Format(time.RFC3339) {
		t.Fatalf("canceled report = %#v", report)
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

func (c *recordingDeleteClient) Delete(ctx context.Context, object client.Object, options ...client.DeleteOption) error {
	c.deleteOptions = (&client.DeleteOptions{}).ApplyOptions(options)
	return c.Client.Delete(ctx, object, options...)
}

func TestConcurrencyWaitsForOlderUnevaluatedRun(t *testing.T) {
	older := concurrencyRun("older", "older", 1, time.Unix(1, 0), "", nil)
	current := concurrencyRun("current", "current", 1, time.Unix(2, 0), "", nil)
	reconciler, _ := concurrencyReconciler(t, older, current)

	waiting, err := reconciler.handleConcurrency(context.Background(), current, "deploy", false)
	if err != nil {
		t.Fatal(err)
	}
	if !waiting {
		t.Fatal("newer run did not wait for an older unevaluated run")
	}
}

func TestConcurrencyIsRepositoryScoped(t *testing.T) {
	condition := plannedCondition(metav1.ConditionTrue, "JobsPlanned")
	otherRepository := concurrencyRun("other", "other", 2, time.Unix(1, 0), "deploy", &condition)
	current := concurrencyRun("current", "current", 1, time.Unix(2, 0), "", nil)
	reconciler, clusterClient := concurrencyReconciler(t, otherRepository, current)

	waiting, err := reconciler.handleConcurrency(context.Background(), current, "deploy", true)
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
	current := concurrencyRun("current", "current", 1, time.Unix(3, 0), "", nil)
	reconciler, clusterClient := concurrencyReconciler(t, running, pending, current)

	waiting, err := reconciler.handleConcurrency(context.Background(), current, "deploy", false)
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
	if storedPending.DeletionTimestamp.IsZero() || !controllerutil.ContainsFinalizer(storedPending, workflowRunCancellationFinalizer) {
		t.Fatalf("superseded pending run is not held for foreground cleanup: %#v", storedPending.ObjectMeta)
	}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(running), &actionsv1alpha1.WorkflowRun{}); err != nil {
		t.Fatalf("running member was deleted: %v", err)
	}
}

func TestConcurrencyCancelInProgressDeletesRunningMember(t *testing.T) {
	condition := plannedCondition(metav1.ConditionTrue, "JobsPlanned")
	running := concurrencyRun("running", "running", 1, time.Unix(1, 0), "deploy", &condition)
	current := concurrencyRun("current", "current", 1, time.Unix(2, 0), "", nil)
	reconciler, clusterClient := concurrencyReconciler(t, running, current)

	waiting, err := reconciler.handleConcurrency(context.Background(), current, "deploy", true)
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
	if storedRunning.DeletionTimestamp.IsZero() || !controllerutil.ContainsFinalizer(storedRunning, workflowRunCancellationFinalizer) {
		t.Fatalf("running member is not held for foreground cleanup: %#v", storedRunning.ObjectMeta)
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
		Status: actionsv1alpha1.WorkflowRunStatus{ConcurrencyGroup: group},
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
	clusterClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(runs...).Build()
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
		id: "build", displayName: "Build and test", runsOn: []string{"linux"}, plan: "{}",
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
		WithStatusSubresource(&actionsv1alpha1.WorkflowRun{}).
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
			Jobs:         &actionsv1alpha1.WorkflowRunJobStatus{Total: 1},
			Conditions:   []metav1.Condition{plannedCondition(metav1.ConditionTrue, "JobsPlanned")},
		},
	}
	clusterClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&actionsv1alpha1.WorkflowRun{}).WithObjects(run).Build()
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
	olderCondition := plannedCondition(metav1.ConditionTrue, "JobsPlanned")
	waitingCondition := plannedCondition(metav1.ConditionUnknown, "WaitingForConcurrency")
	older := concurrencyRun("older", "run-older", 1, time.Now().Add(-time.Minute), "deploy", &olderCondition)
	current := concurrencyRun("current", "run-current", 1, time.Now(), "deploy", &waitingCondition)
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
}

func TestGitHubCheckFailurePreservesWorkflowRequeue(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := actionsv1alpha1.AddToScheme(scheme); err != nil {
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
	run := concurrencyRun("current", "run-current", 1, time.Now(), "", nil)
	clusterClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&actionsv1alpha1.WorkflowRun{}).WithObjects(run).Build()
	reconciler := &WorkflowRunReconciler{Client: clusterClient, APIReader: clusterClient}
	if _, err := reconciler.waitingForConcurrency(context.Background(), run, "CI", "deploy", 1, true); err != nil {
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

func TestWorkflowRunCountsAssignedJobAsActiveBeforeExecution(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := actionsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	run := &actionsv1alpha1.WorkflowRun{
		ObjectMeta: metav1.ObjectMeta{Name: "ci", Namespace: "default", UID: types.UID("run-uid")},
	}
	job := &actionsv1alpha1.WorkflowJob{
		TypeMeta: metav1.TypeMeta{APIVersion: actionsv1alpha1.GroupVersion.String(), Kind: "WorkflowJob"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "build",
			Namespace: "default",
			UID:       types.UID("job-uid"),
			Labels:    map[string]string{actionsv1alpha1.LabelWorkflowRunUID: string(run.UID)},
		},
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
	if _, err := reconciler.observeWorkflowJobs(context.Background(), run, "CI", "", 1); err != nil {
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
