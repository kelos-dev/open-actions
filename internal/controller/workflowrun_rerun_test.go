package controller

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"maps"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	actionsv1alpha1 "github.com/kelos-dev/open-actions/api/v1alpha1"
	githubclient "github.com/kelos-dev/open-actions/internal/github"
	"github.com/kelos-dev/open-actions/internal/runner"
	"github.com/kelos-dev/open-actions/internal/workflowrun"
	batchv1 "k8s.io/api/batch/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

const deferredRerunWorkflow = `name: Deferred rerun
on: push
jobs:
  prepare:
    runs-on: ubuntu-latest
    steps:
      - run: prepare
  build:
    needs: prepare
    name: Build ${{ needs.prepare.outputs.marker }}
    runs-on: ubuntu-latest
    strategy:
      fail-fast: false
      matrix:
        project: ${{ fromJSON(needs.prepare.outputs.projects) }}
        number: [9007199254740993]
        enabled: [true]
    steps:
      - run: build
  report:
    needs: build
    runs-on: ubuntu-latest
    steps:
      - run: report
`

type deferredRerunFixture struct {
	t          *testing.T
	reconciler *WorkflowRunReconciler
	root       *actionsv1alpha1.WorkflowRun
}

func newDeferredRerunFixture(t *testing.T, data string) *deferredRerunFixture {
	t.Helper()
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch req.URL.Path {
		case "/app/installations/2/access_tokens":
			fmt.Fprint(w, `{"token":"contents-token"}`)
		case "/repos/acme/example/contents/.open-actions/workflows/ci.yaml":
			if req.URL.Query().Get("ref") != strings.Repeat("a", 40) {
				t.Errorf("workflow ref = %q", req.URL.Query().Get("ref"))
			}
			_ = json.NewEncoder(w).Encode(map[string]string{"encoding": "base64", "content": base64.StdEncoding.EncodeToString([]byte(data))})
		default:
			http.NotFound(w, req)
		}
	}))
	t.Cleanup(server.Close)
	github, err := githubclient.NewClient(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	project := &actionsv1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: "default", UID: "project-uid"},
		Spec: actionsv1alpha1.ProjectSpec{Source: actionsv1alpha1.ProjectSource{Type: actionsv1alpha1.SourceTypeGitHub, GitHub: &actionsv1alpha1.GitHubAppConfiguration{
			AppID: 1, InstallationID: 2, PrivateKeySecretRef: corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "github"}, Key: "key"},
		}}},
	}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "github", Namespace: "default"}, Data: map[string][]byte{"key": pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(privateKey)})}}
	root := &actionsv1alpha1.WorkflowRun{
		ObjectMeta: metav1.ObjectMeta{Name: "ci", Namespace: "default", UID: "root-uid"},
		Spec: actionsv1alpha1.WorkflowRunSpec{
			ProjectRef: corev1.LocalObjectReference{Name: project.Name}, WorkflowPath: ".open-actions/workflows/ci.yaml",
			Source: actionsv1alpha1.WorkflowRunSource{Type: actionsv1alpha1.SourceTypeGitHub, GitHub: &actionsv1alpha1.GitHubWorkflowRunSource{
				Repository: actionsv1alpha1.GitHubRepository{ID: 1, Owner: "acme", Name: "example"},
				Event:      actionsv1alpha1.GitHubEvent{Name: "push", DeliveryID: "delivery"},
				Revision:   actionsv1alpha1.GitRevision{SHA: strings.Repeat("a", 40), Ref: "refs/heads/main"},
			}},
		},
	}
	setTestWorkflowRunIdentity(root)
	scheme := runnerTestScheme(t)
	if err := coordinationv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&actionsv1alpha1.WorkflowRun{}, &actionsv1alpha1.WorkflowJob{}).WithObjects(root, project, secret).Build()
	f := &deferredRerunFixture{t: t, root: root, reconciler: &WorkflowRunReconciler{Client: c, APIReader: c, GitHub: github, GitHubAPIBase: server.URL, GitHubServerURL: "https://github.example", ActionCloneBaseURL: "https://github.example"}}
	f.reconcile(root)
	return f
}

func (f *deferredRerunFixture) reconcile(run *actionsv1alpha1.WorkflowRun) {
	f.t.Helper()
	for range 6 {
		if err := f.reconciler.Get(context.Background(), client.ObjectKeyFromObject(run), run); err != nil {
			f.t.Fatal(err)
		}
		result, err := f.reconciler.reconcileWorkflowRun(context.Background(), run)
		if err != nil {
			f.t.Fatal(err)
		}
		if !result.Requeue {
			break
		}
	}
	if err := f.reconciler.Get(context.Background(), client.ObjectKeyFromObject(run), run); err != nil {
		f.t.Fatal(err)
	}
}

func (f *deferredRerunFixture) jobs(run *actionsv1alpha1.WorkflowRun) map[string]actionsv1alpha1.WorkflowJob {
	f.t.Helper()
	jobs := &actionsv1alpha1.WorkflowJobList{}
	if err := f.reconciler.List(context.Background(), jobs, client.InNamespace(run.Namespace), client.MatchingLabels{actionsv1alpha1.LabelWorkflowRunUID: string(run.UID)}); err != nil {
		f.t.Fatal(err)
	}
	result := make(map[string]actionsv1alpha1.WorkflowJob)
	for _, job := range jobs.Items {
		result[job.Spec.JobID] = job
	}
	return result
}

func (f *deferredRerunFixture) complete(run *actionsv1alpha1.WorkflowRun, id string, result actionsv1alpha1.WorkflowJobResult, outputs map[string]string) {
	f.t.Helper()
	job, ok := f.jobs(run)[id]
	if !ok {
		f.t.Fatalf("WorkflowRun %q has no job %q; conditions = %#v", run.Name, id, run.Status.Conditions)
	}
	job.Status.Result = result
	job.Status.Outputs = outputs
	if err := f.reconciler.Status().Update(context.Background(), &job); err != nil {
		f.t.Fatal(err)
	}
	f.reconcile(run)
}

func (f *deferredRerunFixture) rerun(previous *actionsv1alpha1.WorkflowRun, attempt int32, selectedIDs ...string) *actionsv1alpha1.WorkflowRun {
	f.t.Helper()
	jobs := f.jobs(previous)
	ids := selectedIDs
	if len(ids) == 0 {
		var err error
		ids, err = workflowrun.FailedJobIDs(previous, slices.Collect(maps.Values(jobs)))
		if err != nil {
			f.t.Fatal(err)
		}
	}
	if len(ids) == 0 {
		f.t.Fatal("no failed jobs selected")
	}
	run := workflowrun.NewRerun(f.root, previous, attempt, ids)
	run.UID = types.UID(fmt.Sprintf("attempt-%d-uid", attempt))
	setTestWorkflowRunIdentity(run)
	run.Status.Identity.Attempt = attempt
	if err := f.reconciler.Create(context.Background(), run); err != nil {
		f.t.Fatal(err)
	}
	f.reconcile(run)
	return run
}

func assertJobReady(t *testing.T, job actionsv1alpha1.WorkflowJob, status metav1.ConditionStatus) {
	t.Helper()
	ready := meta.FindStatusCondition(job.Status.Conditions, actionsv1alpha1.WorkflowJobConditionReady)
	if ready == nil || ready.Status != status {
		t.Fatalf("WorkflowJob %q readiness = %#v, want %s", job.Name, ready, status)
	}
}

// GitHub documents output-derived matrices and rerunning failed jobs with their dependents:
// https://docs.github.com/en/actions/how-tos/write-workflows/choose-what-workflows-do/run-job-variations#using-an-output-to-define-two-matrices
// https://docs.github.com/en/rest/actions/workflow-runs#re-run-failed-jobs-from-a-workflow-run
func TestDeferredSelectiveRerunReusesMatrixAndPrerequisites(t *testing.T) {
	f := newDeferredRerunFixture(t, deferredRerunWorkflow)
	f.complete(f.root, "prepare", actionsv1alpha1.WorkflowJobResultSuccess, map[string]string{"projects": `["one","two"]`, "marker": "original"})
	f.complete(f.root, "build-matrix-1", actionsv1alpha1.WorkflowJobResultSuccess, map[string]string{"one": "ready"})
	f.complete(f.root, "build-matrix-2", actionsv1alpha1.WorkflowJobResultFailure, nil)
	original := f.jobs(f.root)["build-matrix-2"]
	previous := f.root
	for _, attempt := range []int32{2, 3} {
		run := f.rerun(previous, attempt)
		jobs := f.jobs(run)
		if len(jobs) != 2 {
			t.Fatalf("attempt %d jobs = %v; conditions = %#v", attempt, slices.Collect(maps.Keys(jobs)), run.Status.Conditions)
		}
		build := jobs["build-matrix-2"]
		if !maps.Equal(build.Spec.Matrix.Values, original.Spec.Matrix.Values) || build.Spec.Matrix.JobIndex != 1 || build.Spec.Matrix.JobTotal != 2 {
			t.Fatalf("rerun matrix = %#v", build.Spec.Matrix)
		}
		if build.Spec.DisplayName != original.Spec.DisplayName {
			t.Fatalf("display name = %q, want %q", build.Spec.DisplayName, original.Spec.DisplayName)
		}
		assertJobReady(t, build, metav1.ConditionTrue)
		assertJobReady(t, jobs["report"], metav1.ConditionUnknown)
		cm := &corev1.ConfigMap{}
		if err := f.reconciler.Get(context.Background(), client.ObjectKey{Namespace: run.Namespace, Name: childName(build.Name, "plan")}, cm); err != nil {
			t.Fatal(err)
		}
		plan := &runner.Plan{}
		decoder := json.NewDecoder(strings.NewReader(cm.Data[jobPlanKey]))
		decoder.UseNumber()
		if err := decoder.Decode(plan); err != nil {
			t.Fatal(err)
		}
		if plan.Run.Attempt != attempt || plan.Matrix["enabled"] != true || plan.Matrix["number"] != json.Number("9007199254740993") {
			t.Fatalf("rerun plan = %#v", plan)
		}
		needsMap := &corev1.ConfigMap{}
		if err := f.reconciler.Get(context.Background(), client.ObjectKey{Namespace: run.Namespace, Name: childName(build.Name, "needs")}, needsMap); err != nil {
			t.Fatal(err)
		}
		needs, err := runner.DecodeNeedsContext([]byte(needsMap.Data[jobNeedsKey]))
		if err != nil {
			t.Fatal(err)
		}
		if needs["prepare"].Outputs["marker"] != "original" {
			t.Fatalf("inherited needs = %#v", needs)
		}
		result := actionsv1alpha1.WorkflowJobResultFailure
		if attempt == 3 {
			result = actionsv1alpha1.WorkflowJobResultSuccess
		}
		f.complete(run, "build-matrix-2", result, map[string]string{"two": "ready"})
		if attempt == 3 {
			assertJobReady(t, f.jobs(run)["report"], metav1.ConditionTrue)
			f.complete(run, "report", actionsv1alpha1.WorkflowJobResultSuccess, nil)
			if condition := meta.FindStatusCondition(run.Status.Conditions, actionsv1alpha1.WorkflowRunConditionSucceeded); condition == nil || condition.Status != metav1.ConditionTrue || run.Status.Jobs.Total != 2 || run.Status.Jobs.Succeeded != 2 {
				t.Fatalf("completed rerun = %#v", run.Status)
			}
		}
		// A fresh reconciler must recover the selection from the persisted plans.
		f.reconciler = &WorkflowRunReconciler{Client: f.reconciler.Client, APIReader: f.reconciler.APIReader, GitHub: f.reconciler.GitHub, GitHubAPIBase: f.reconciler.GitHubAPIBase, GitHubServerURL: f.reconciler.GitHubServerURL, ActionCloneBaseURL: f.reconciler.ActionCloneBaseURL}
		previous = run
	}
}

func TestDeferredSelectiveRerunWaitsForProducerAndReexpands(t *testing.T) {
	for _, initial := range []string{"failed producer", "expanded matrix"} {
		t.Run(initial, func(t *testing.T) {
			f := newDeferredRerunFixture(t, deferredRerunWorkflow)
			var selectedIDs []string
			if initial == "failed producer" {
				f.complete(f.root, "prepare", actionsv1alpha1.WorkflowJobResultFailure, nil)
			} else {
				f.complete(f.root, "prepare", actionsv1alpha1.WorkflowJobResultSuccess, map[string]string{"projects": `["one","two","three"]`, "marker": "original"})
				for _, id := range []string{"build-matrix-1", "build-matrix-2", "build-matrix-3"} {
					f.complete(f.root, id, actionsv1alpha1.WorkflowJobResultFailure, nil)
				}
				selectedIDs = []string{"prepare", "build-matrix-1", "build-matrix-2", "build-matrix-3", "report"}
			}
			run := f.rerun(f.root, 2, selectedIDs...)
			jobs := f.jobs(run)
			if len(jobs) != 2 {
				t.Fatalf("jobs before producer completion = %v; conditions = %#v", slices.Collect(maps.Keys(jobs)), run.Status.Conditions)
			}
			assertJobReady(t, jobs["prepare"], metav1.ConditionTrue)
			assertJobReady(t, jobs["report"], metav1.ConditionUnknown)
			f.complete(run, "prepare", actionsv1alpha1.WorkflowJobResultSuccess, map[string]string{"projects": `["three","one"]`, "marker": "current"})
			jobs = f.jobs(run)
			if len(jobs) != 4 || jobs["build-matrix-1"].Spec.Matrix.Values["project"] != "three" || jobs["build-matrix-2"].Spec.Matrix.Values["project"] != "one" {
				t.Fatalf("reexpanded jobs = %#v", jobs)
			}
			for _, id := range []string{"build-matrix-1", "build-matrix-2"} {
				assertJobReady(t, jobs[id], metav1.ConditionTrue)
				if !strings.HasPrefix(jobs[id].Spec.DisplayName, "Build current") {
					t.Fatalf("display name = %q", jobs[id].Spec.DisplayName)
				}
				f.complete(run, id, actionsv1alpha1.WorkflowJobResultSuccess, nil)
			}
			assertJobReady(t, f.jobs(run)["report"], metav1.ConditionTrue)
			f.complete(run, "report", actionsv1alpha1.WorkflowJobResultFailure, nil)
			third := f.rerun(run, 3)
			jobs = f.jobs(third)
			if len(jobs) != 1 {
				t.Fatalf("third attempt jobs = %#v", jobs)
			}
			assertJobReady(t, jobs["report"], metav1.ConditionTrue)
		})
	}
}

func TestDeferredSelectiveRerunSupportsNeedsBasedConfiguration(t *testing.T) {
	data := `name: Deferred configuration
on: push
jobs:
  prepare:
    runs-on: ubuntu-latest
    steps:
      - run: prepare
  build:
    needs: prepare
    name: Build ${{ needs.prepare.outputs.marker }}
    runs-on: ${{ needs.prepare.outputs.runner }}
    timeout-minutes: ${{ fromJSON(needs.prepare.outputs.timeout) }}
    continue-on-error: ${{ fromJSON(needs.prepare.outputs.tolerate) }}
    steps:
      - run: build
`
	f := newDeferredRerunFixture(t, data)
	f.complete(f.root, "prepare", actionsv1alpha1.WorkflowJobResultSuccess, map[string]string{"marker": "retained", "runner": "linux", "timeout": "7", "tolerate": "false"})
	f.complete(f.root, "build", actionsv1alpha1.WorkflowJobResultFailure, nil)
	run := f.rerun(f.root, 2)
	jobs := f.jobs(run)
	if len(jobs) != 1 || jobs["build"].Spec.TimeoutSeconds != 420 || !slices.Equal(jobs["build"].Spec.RunsOn, []string{"linux"}) || jobs["build"].Spec.DisplayName != "Build retained" {
		t.Fatalf("rerun jobs = %#v", jobs)
	}
	assertJobReady(t, jobs["build"], metav1.ConditionTrue)
}

func TestDeferredSelectiveRerunRejectsMissingHistory(t *testing.T) {
	for _, missing := range []string{"prerequisite", "matrix plan"} {
		t.Run(missing, func(t *testing.T) {
			f := newDeferredRerunFixture(t, deferredRerunWorkflow)
			f.complete(f.root, "prepare", actionsv1alpha1.WorkflowJobResultSuccess, map[string]string{"projects": `["one"]`, "marker": "original"})
			f.complete(f.root, "build-matrix-1", actionsv1alpha1.WorkflowJobResultFailure, nil)
			ids, err := workflowrun.FailedJobIDs(f.root, slices.Collect(maps.Values(f.jobs(f.root))))
			if err != nil {
				t.Fatal(err)
			}
			var object client.Object
			jobs := f.jobs(f.root)
			if missing == "prerequisite" {
				job := jobs["prepare"]
				object = &job
			} else {
				job := jobs["build-matrix-1"]
				object = &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: job.Namespace, Name: childName(job.Name, "plan")}}
			}
			if err := f.reconciler.Delete(context.Background(), object); err != nil {
				t.Fatal(err)
			}
			run := f.rerun(f.root, 2, ids...)
			planned := meta.FindStatusCondition(run.Status.Conditions, actionsv1alpha1.WorkflowRunConditionPlanned)
			if planned == nil || planned.Status != metav1.ConditionFalse || planned.Reason != "RerunInvalid" || !strings.Contains(planned.Message, run.Name) || len(f.jobs(run)) != 0 {
				t.Fatalf("missing history outcome = %#v", run.Status)
			}
		})
	}
}

func TestDeferredSelectiveRerunUsesCurrentProducerFailure(t *testing.T) {
	f := newDeferredRerunFixture(t, deferredRerunWorkflow)
	f.complete(f.root, "prepare", actionsv1alpha1.WorkflowJobResultSuccess, map[string]string{"projects": `["one"]`, "marker": "original"})
	f.complete(f.root, "build-matrix-1", actionsv1alpha1.WorkflowJobResultFailure, nil)
	run := f.rerun(f.root, 2, "prepare", "build-matrix-1", "report")
	f.complete(run, "prepare", actionsv1alpha1.WorkflowJobResultFailure, nil)
	jobs := f.jobs(run)
	if len(jobs) != 3 || jobs["build"].Status.Result != actionsv1alpha1.WorkflowJobResultSkipped || jobs["report"].Status.Result != actionsv1alpha1.WorkflowJobResultSkipped {
		t.Fatalf("jobs after producer failure = %#v", jobs)
	}
	condition := meta.FindStatusCondition(run.Status.Conditions, actionsv1alpha1.WorkflowRunConditionSucceeded)
	if condition == nil || condition.Status != metav1.ConditionFalse || condition.Reason != "JobFailed" {
		t.Fatalf("run condition = %#v", condition)
	}
}

func TestDeferredSelectiveRerunKeepsPartialSelectionWhileProducerRuns(t *testing.T) {
	f := newDeferredRerunFixture(t, deferredRerunWorkflow)
	f.complete(f.root, "prepare", actionsv1alpha1.WorkflowJobResultSuccess, map[string]string{"projects": `["one","two"]`, "marker": "original"})
	f.complete(f.root, "build-matrix-1", actionsv1alpha1.WorkflowJobResultSuccess, nil)
	f.complete(f.root, "build-matrix-2", actionsv1alpha1.WorkflowJobResultFailure, nil)
	run := f.rerun(f.root, 2, "prepare", "build-matrix-2", "report")
	jobs := f.jobs(run)
	if len(jobs) != 2 {
		t.Fatalf("pending jobs = %#v", jobs)
	}
	assertJobReady(t, jobs["report"], metav1.ConditionUnknown)
	f.complete(run, "prepare", actionsv1alpha1.WorkflowJobResultSuccess, map[string]string{"projects": `["three","one"]`, "marker": "current"})
	jobs = f.jobs(run)
	if len(jobs) != 3 || jobs["build-matrix-2"].Spec.Matrix.Values["project"] != "two" {
		t.Fatalf("partial rerun jobs = %#v", jobs)
	}
	assertJobReady(t, jobs["build-matrix-2"], metav1.ConditionTrue)
	assertJobReady(t, jobs["report"], metav1.ConditionUnknown)
	f.complete(run, "build-matrix-2", actionsv1alpha1.WorkflowJobResultSuccess, nil)
	assertJobReady(t, f.jobs(run)["report"], metav1.ConditionTrue)
}

func TestDeferredSelectiveRerunPreservesChildIdentityWhenSkipped(t *testing.T) {
	data := strings.Replace(deferredRerunWorkflow, "  build:\n", "  build:\n    if: github.run_attempt == 1\n", 1)
	f := newDeferredRerunFixture(t, data)
	f.complete(f.root, "prepare", actionsv1alpha1.WorkflowJobResultSuccess, map[string]string{"projects": `["one","two"]`, "marker": "original"})
	f.complete(f.root, "build-matrix-1", actionsv1alpha1.WorkflowJobResultSuccess, nil)
	f.complete(f.root, "build-matrix-2", actionsv1alpha1.WorkflowJobResultFailure, nil)
	run := f.rerun(f.root, 2)
	jobs := f.jobs(run)
	skipped := jobs["build-matrix-2"]
	if len(jobs) != 2 || skipped.Spec.Matrix == nil || skipped.Spec.Matrix.Values["project"] != "two" || skipped.Status.Result != actionsv1alpha1.WorkflowJobResultSkipped {
		t.Fatalf("skipped rerun jobs = %#v", jobs)
	}
	third := f.rerun(run, 3, "build-matrix-2", "report")
	skipped = f.jobs(third)["build-matrix-2"]
	if skipped.Status.Result != actionsv1alpha1.WorkflowJobResultSkipped || skipped.Spec.Matrix == nil {
		t.Fatalf("repeated skipped child = %#v", skipped)
	}
}

func TestDeferredSelectiveRerunPreservesNumericMatrixValues(t *testing.T) {
	for _, value := range []string{"1000000", "20260908", "0.00001", "-1000000"} {
		t.Run(value, func(t *testing.T) {
			f := newDeferredRerunFixture(t, deferredRerunWorkflow)
			f.complete(f.root, "prepare", actionsv1alpha1.WorkflowJobResultSuccess, map[string]string{"projects": "[" + value + "]", "marker": "original"})
			f.complete(f.root, "build-matrix-1", actionsv1alpha1.WorkflowJobResultFailure, nil)
			original := f.jobs(f.root)["build-matrix-1"]
			run := f.rerun(f.root, 2)
			build, exists := f.jobs(run)["build-matrix-1"]
			if !exists || build.Spec.Matrix == nil || !maps.Equal(build.Spec.Matrix.Values, original.Spec.Matrix.Values) {
				t.Fatalf("numeric rerun matrix = %#v; run conditions = %#v", build.Spec.Matrix, run.Status.Conditions)
			}
			assertJobReady(t, build, metav1.ConditionTrue)
			planConfigMap := &corev1.ConfigMap{}
			if err := f.reconciler.Get(context.Background(), client.ObjectKey{Namespace: run.Namespace, Name: childName(build.Name, "plan")}, planConfigMap); err != nil {
				t.Fatal(err)
			}
			var plan runner.Plan
			decoder := json.NewDecoder(strings.NewReader(planConfigMap.Data[jobPlanKey]))
			decoder.UseNumber()
			if err := decoder.Decode(&plan); err != nil {
				t.Fatal(err)
			}
			if plan.Matrix["project"] != json.Number(value) || plan.Matrix["number"] != json.Number("9007199254740993") {
				t.Fatalf("rerun matrix snapshot = %#v", plan.Matrix)
			}
		})
	}
}

func TestDeferredRerunRetainsAttemptAfterExecutionStateLoss(t *testing.T) {
	for _, tc := range []struct {
		name       string
		full       bool
		finishJobs bool
	}{
		{name: "selective rerun loses prerequisite"},
		{name: "full rerun loses deferred plan", full: true},
		{name: "selective rerun loses deferred plan after jobs finish", finishJobs: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newDeferredRerunFixture(t, deferredRerunWorkflow)
			f.complete(f.root, "prepare", actionsv1alpha1.WorkflowJobResultSuccess, map[string]string{"projects": `["one"]`, "marker": "original"})
			f.complete(f.root, "build-matrix-1", actionsv1alpha1.WorkflowJobResultFailure, nil)
			var run *actionsv1alpha1.WorkflowRun
			var missing client.Object
			var missingID string
			if !tc.full {
				run = f.rerun(f.root, 2)
				prepare := f.jobs(f.root)["prepare"]
				missing = &prepare
				missingID = prepare.Spec.JobID
			} else {
				run = workflowrun.NewRerun(f.root, f.root, 2, nil)
				run.UID = "attempt-2-uid"
				setTestWorkflowRunIdentity(run)
				run.Status.Identity.Attempt = 2
				if err := f.reconciler.Create(context.Background(), run); err != nil {
					t.Fatal(err)
				}
				f.reconcile(run)
				missing = &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: run.Namespace, Name: deferredJobPlanConfigMapName(run.Name, "build")}}
				missingID = missing.GetName()
			}
			if tc.finishJobs {
				for _, job := range f.jobs(run) {
					job.Status.Result = actionsv1alpha1.WorkflowJobResultSuccess
					if err := f.reconciler.Status().Update(context.Background(), &job); err != nil {
						t.Fatal(err)
					}
				}
				missing = &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: run.Namespace, Name: deferredJobPlanConfigMapName(run.Name, "build")}}
				missingID = missing.GetName()
			}
			nativeJob := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
				Name: "active-rerun-job", Namespace: run.Namespace,
				Labels: map[string]string{actionsv1alpha1.LabelWorkflowRunUID: string(run.UID)},
			}}
			if err := f.reconciler.Create(context.Background(), nativeJob); err != nil {
				t.Fatal(err)
			}
			if err := f.reconciler.Delete(context.Background(), missing); err != nil {
				t.Fatal(err)
			}
			f.reconcile(run)
			if workflowrun.Terminal(run) || run.Status.CompletionTime != nil {
				t.Fatalf("rerun completed while a native Job was active: %#v", run.Status)
			}
			if err := f.reconciler.Delete(context.Background(), nativeJob); err != nil {
				t.Fatal(err)
			}
			f.reconcile(run)
			for _, conditionType := range []string{actionsv1alpha1.WorkflowRunConditionPlanned, actionsv1alpha1.WorkflowRunConditionSucceeded} {
				condition := meta.FindStatusCondition(run.Status.Conditions, conditionType)
				if condition == nil || condition.Status != metav1.ConditionFalse || condition.Reason != "ExecutionStateLost" || !strings.Contains(condition.Message, missingID) || !strings.Contains(condition.Message, run.Name) {
					t.Fatalf("rerun condition %q = %#v", conditionType, condition)
				}
			}
			latest, err := workflowrun.LatestAttempt(f.root, []actionsv1alpha1.WorkflowRun{*run})
			if err != nil || latest.UID != run.UID {
				t.Fatalf("latest attempt = %#v, error = %v", latest, err)
			}
			next := workflowrun.NewRerun(f.root, latest, latest.Spec.Rerun.Attempt+1, nil)
			next.UID = "attempt-3-uid"
			setTestWorkflowRunIdentity(next)
			next.Status.Identity.Attempt = 3
			if err := f.reconciler.Create(context.Background(), next); err != nil {
				t.Fatal(err)
			}
			f.reconcile(next)
			if next.Spec.Rerun.Attempt != 3 || len(f.jobs(next)) != 2 {
				t.Fatalf("next rerun did not create its jobs: %#v", next)
			}
			assertJobReady(t, f.jobs(next)["prepare"], metav1.ConditionTrue)
		})
	}
}
