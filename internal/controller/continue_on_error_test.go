package controller

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	actionsv1alpha1 "github.com/kelos-dev/open-actions/api/v1alpha1"
	githubclient "github.com/kelos-dev/open-actions/internal/github"
	"github.com/kelos-dev/open-actions/internal/runner"
	"github.com/kelos-dev/open-actions/internal/workflow"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// https://docs.github.com/en/actions/how-tos/write-workflows/choose-what-workflows-do/run-job-variations#handling-failures
const experimentalMatrixStrategy = `
    strategy:
      fail-fast: true
      matrix:
        version: [6, 7, 8]
        experimental: [false]
        include:
          - version: 9
            experimental: true
`

func continueOnErrorTestRun() *actionsv1alpha1.WorkflowRun {
	run := &actionsv1alpha1.WorkflowRun{
		ObjectMeta: metav1.ObjectMeta{Name: "ci", Namespace: "default", UID: "run-uid"},
		Spec: actionsv1alpha1.WorkflowRunSpec{
			ProjectRef: corev1.LocalObjectReference{Name: "project"}, WorkflowPath: ".github/workflows/ci.yml",
			Source: actionsv1alpha1.WorkflowRunSource{Type: actionsv1alpha1.SourceTypeGitHub, GitHub: &actionsv1alpha1.GitHubWorkflowRunSource{
				Repository: actionsv1alpha1.GitHubRepository{ID: 1, Owner: "acme", Name: "example"},
				Event:      actionsv1alpha1.GitHubEvent{Name: actionsv1alpha1.GitHubEventNamePush},
				Revision:   actionsv1alpha1.GitRevision{SHA: strings.Repeat("a", 40), Ref: "refs/heads/main"},
			}},
		},
	}
	setTestWorkflowRunIdentity(run)
	return run
}

func TestJobContinueOnErrorGitHubReporting(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	keyData := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	for _, event := range []actionsv1alpha1.GitHubEventName{
		actionsv1alpha1.GitHubEventNamePush, actionsv1alpha1.GitHubEventNamePullRequest, actionsv1alpha1.GitHubEventNameMergeGroup,
	} {
		for _, matrix := range []bool{false, true} {
			for _, untoleratedFailure := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/matrix=%t/untolerated-failure=%t", event, matrix, untoleratedFailure), func(t *testing.T) {
					ctx := context.Background()
					run := continueOnErrorTestRun()
					run.Spec.Source.GitHub.Event.Name = event
					revision := run.Spec.Source.GitHub.Revision.SHA
					if event == actionsv1alpha1.GitHubEventNamePullRequest {
						revision = strings.Repeat("b", 40)
						run.Spec.Source.GitHub.Revision.HeadSHA = revision
					}
					var reports []githubclient.CreateCommitStatusRequest
					server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
						switch {
						case req.URL.Path == "/app":
							fmt.Fprint(w, "{\"id\":1,\"slug\":\"open-actions\"}")
						case req.URL.Path == "/app/installations/2/access_tokens":
							fmt.Fprint(w, "{\"token\":\"statuses-token\"}")
						case req.Method == http.MethodGet && req.URL.Path == "/repos/acme/example/commits/"+revision+"/statuses":
							statuses := []githubclient.CommitStatus{}
							for index := len(reports) - 1; index >= 0; index-- {
								report := reports[index]
								status := githubclient.CommitStatus{ID: int64(index + 1), State: report.State, Context: report.Context, Description: report.Description, TargetURL: report.TargetURL}
								status.Creator.Login = "open-actions[bot]"
								statuses = append(statuses, status)
							}
							if err := json.NewEncoder(w).Encode(statuses); err != nil {
								t.Error(err)
							}
						case req.Method == http.MethodPost && req.URL.Path == "/repos/acme/example/statuses/"+revision:
							var report githubclient.CreateCommitStatusRequest
							if err := json.NewDecoder(req.Body).Decode(&report); err != nil {
								http.Error(w, err.Error(), http.StatusBadRequest)
								return
							}
							reports = append(reports, report)
							fmt.Fprintf(w, "{\"id\":%d,\"state\":%q}", len(reports), report.State)
						default:
							http.NotFound(w, req)
						}
					}))
					defer server.Close()
					github, err := githubclient.NewClient(server.URL, server.Client())
					if err != nil {
						t.Fatal(err)
					}
					project := &actionsv1alpha1.Project{
						ObjectMeta: metav1.ObjectMeta{Name: "project", Namespace: run.Namespace, UID: "project-uid"},
						Spec: actionsv1alpha1.ProjectSpec{Source: actionsv1alpha1.ProjectSource{Type: actionsv1alpha1.SourceTypeGitHub, GitHub: &actionsv1alpha1.GitHubAppConfiguration{
							AppID: 1, InstallationID: 2,
							PrivateKeySecretRef: corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "github"}, Key: "private-key"},
						}}},
					}
					secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "github", Namespace: run.Namespace}, Data: map[string][]byte{"private-key": keyData}}
					objects := []client.Object{project, secret, run}
					for index := 0; index < 2; index++ {
						id := fmt.Sprintf("test-%d", index)
						result := actionsv1alpha1.WorkflowJobResultSuccess
						if index == 0 || untoleratedFailure {
							result = actionsv1alpha1.WorkflowJobResultFailure
						}
						job := &actionsv1alpha1.WorkflowJob{
							ObjectMeta: metav1.ObjectMeta{Name: id, Namespace: run.Namespace, Labels: map[string]string{actionsv1alpha1.LabelWorkflowRunUID: string(run.UID)}},
							Spec:       actionsv1alpha1.WorkflowJobSpec{WorkflowRunRef: corev1.LocalObjectReference{Name: run.Name}, JobID: id, ContinueOnError: index == 0},
							Status:     actionsv1alpha1.WorkflowJobStatus{Result: result},
						}
						if matrix {
							job.Spec.Matrix = &actionsv1alpha1.WorkflowJobMatrix{LogicalJobID: "test", FailFast: pointerTo(true)}
						}
						objects = append(objects, job)
					}
					clusterClient := fake.NewClientBuilder().WithScheme(runnerTestScheme(t)).
						WithStatusSubresource(&actionsv1alpha1.WorkflowRun{}, &actionsv1alpha1.WorkflowJob{}).WithObjects(objects...).Build()
					wantState := "success"
					wantSucceeded := metav1.ConditionTrue
					wantFailed := int32(1)
					if untoleratedFailure {
						wantState = "failure"
						wantSucceeded = metav1.ConditionFalse
						wantFailed = 2
					}
					for iteration := 0; iteration < 2; iteration++ {
						reconciler := &WorkflowRunReconciler{Client: clusterClient, APIReader: clusterClient, GitHub: github, ConsoleURL: "https://actions.example"}
						if err := clusterClient.Get(ctx, client.ObjectKeyFromObject(run), run); err != nil {
							t.Fatal(err)
						}
						if _, err := reconciler.observeWorkflowJobs(ctx, run, "CI", 2); err != nil {
							t.Fatal(err)
						}
						condition := meta.FindStatusCondition(run.Status.Conditions, actionsv1alpha1.WorkflowRunConditionSucceeded)
						if condition == nil || condition.Status != wantSucceeded || run.Status.CompletionTime == nil || run.Status.Jobs.Failed != wantFailed || run.Status.Jobs.Succeeded != 2-wantFailed {
							t.Fatalf("run status = %#v", run.Status)
						}
						reporter := &GitHubStatusReconciler{Client: clusterClient, APIReader: clusterClient, GitHub: github, ConsoleURL: reconciler.ConsoleURL}
						if err := reporter.reconcileGitHubStatuses(ctx, run); err != nil {
							t.Fatal(err)
						}
						if len(reports) != 2 {
							t.Fatalf("reports = %#v, want two job reports", reports)
						}
						got := map[string]string{}
						for _, report := range reports {
							got[report.Context] = report.State
						}
						suffix := ""
						if matrix {
							suffix = " / test"
						}
						want := map[string]string{
							"Open Actions / CI / test-0" + suffix: "failure",
							"Open Actions / CI / test-1" + suffix: wantState,
						}
						if !reflect.DeepEqual(got, want) {
							t.Fatalf("reported states = %#v, want %#v", got, want)
						}
					}
				})
			}
		}
	}
}

func TestJobContinueOnErrorEffectiveResults(t *testing.T) {
	tolerated := &actionsv1alpha1.WorkflowJob{
		Spec:   actionsv1alpha1.WorkflowJobSpec{ContinueOnError: true},
		Status: actionsv1alpha1.WorkflowJobStatus{Result: actionsv1alpha1.WorkflowJobResultFailure},
	}
	for _, test := range []struct {
		name     string
		result   actionsv1alpha1.WorkflowJobResult
		tolerate bool
		timedOut bool
		want     actionsv1alpha1.WorkflowJobResult
	}{
		{name: "pending", tolerate: true},
		{name: "success", result: actionsv1alpha1.WorkflowJobResultSuccess, tolerate: true, want: actionsv1alpha1.WorkflowJobResultSuccess},
		{name: "tolerated failure", result: actionsv1alpha1.WorkflowJobResultFailure, tolerate: true, want: actionsv1alpha1.WorkflowJobResultSuccess},
		{name: "untolerated failure", result: actionsv1alpha1.WorkflowJobResultFailure, want: actionsv1alpha1.WorkflowJobResultFailure},
		{name: "skipped", result: actionsv1alpha1.WorkflowJobResultSkipped, tolerate: true, want: actionsv1alpha1.WorkflowJobResultSkipped},
		{name: "cancelled", result: actionsv1alpha1.WorkflowJobResultCancelled, tolerate: true, want: actionsv1alpha1.WorkflowJobResultCancelled},
		{name: "timeout", result: actionsv1alpha1.WorkflowJobResultFailure, tolerate: true, timedOut: true, want: actionsv1alpha1.WorkflowJobResultFailure},
	} {
		t.Run(test.name, func(t *testing.T) {
			job := &actionsv1alpha1.WorkflowJob{
				Spec:   actionsv1alpha1.WorkflowJobSpec{JobID: "build", ContinueOnError: test.tolerate, Matrix: &actionsv1alpha1.WorkflowJobMatrix{LogicalJobID: "build"}},
				Status: actionsv1alpha1.WorkflowJobStatus{Result: test.result},
			}
			if test.timedOut {
				job.Status.Conditions = []metav1.Condition{{Type: actionsv1alpha1.WorkflowJobConditionSucceeded, Status: metav1.ConditionFalse, Reason: "JobTimedOut"}}
			}
			if got := workflowJobEffectiveResult(job); got != test.want {
				t.Fatalf("effective result = %q, want %q", got, test.want)
			}
			if got := workflowJobGroupResult([]*actionsv1alpha1.WorkflowJob{tolerated, job}); got != test.want {
				t.Fatalf("group result = %q, want %q", got, test.want)
			}
			if got := workflowJobResult(job); got != test.result {
				t.Fatalf("raw result = %q, want %q", got, test.result)
			}
			if got := workflowJobFailureTriggersMatrixFailFast(job); got != (test.want == actionsv1alpha1.WorkflowJobResultFailure) {
				t.Fatalf("triggers fail-fast = %t", got)
			}
		})
	}
	build := &actionsv1alpha1.WorkflowJob{
		Spec:   actionsv1alpha1.WorkflowJobSpec{Needs: []string{"prepare"}},
		Status: actionsv1alpha1.WorkflowJobStatus{Result: actionsv1alpha1.WorkflowJobResultSuccess},
	}
	report := &actionsv1alpha1.WorkflowJob{Spec: actionsv1alpha1.WorkflowJobSpec{Needs: []string{"build"}}}
	status := workflowJobAncestorStatus(report, map[string][]*actionsv1alpha1.WorkflowJob{"build": {build}, "prepare": {tolerated}}, false)
	if !status.Success || status.Failure || status.Cancelled {
		t.Fatalf("transitive tolerated failure status = %#v", status)
	}
}

func TestJobContinueOnErrorPlanning(t *testing.T) {
	for _, test := range []struct {
		name     string
		field    string
		matrix   bool
		deferred bool
		invalid  bool
		want     bool
	}{
		{name: "omitted"},
		{name: "false", field: "false"},
		{name: "true", field: "true", want: true},
		{name: "experimental matrix", field: "${{ matrix.experimental }}", matrix: true, want: true},
		{name: "matrix strategy metadata", field: "${{ strategy.job-index == 3 && strategy.job-total == 4 && strategy.fail-fast }}", matrix: true, want: true},
		{name: "deferred matrix", field: "${{ fromJSON(needs.prepare.outputs.tolerate) && matrix.experimental }}", matrix: true, deferred: true, want: true},
		{name: "deferred non-matrix", field: "${{ fromJSON(needs.prepare.outputs.tolerate) }}", deferred: true, want: true},
		{name: "invalid immediate", field: "${{ 'true' }}", invalid: true},
		{name: "invalid matrix child", field: "${{ matrix.experimental || 'false' }}", matrix: true, invalid: true},
		{name: "invalid deferred", field: "${{ needs.prepare.outputs.tolerate }}", deferred: true, invalid: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			run := continueOnErrorTestRun()
			project := &actionsv1alpha1.Project{ObjectMeta: metav1.ObjectMeta{Name: "project", Namespace: run.Namespace, UID: "project-uid"}}
			source := "name: CI\non: push\njobs:\n"
			if test.deferred {
				source += "  prepare:\n    runs-on: ubuntu-latest\n    continue-on-error: true\n    steps:\n      - run: prepare\n"
			}
			source += "  test:\n    runs-on: ubuntu-latest\n"
			if test.deferred {
				source += "    needs: prepare\n"
			}
			if test.field != "" {
				source += "    continue-on-error: " + test.field + "\n"
			}
			if test.matrix {
				source += experimentalMatrixStrategy
			}
			source += "    steps:\n      - run: test\n"
			definition, err := workflow.Parse([]byte(source))
			if err != nil {
				t.Fatal(err)
			}
			clusterClient := fake.NewClientBuilder().WithScheme(runnerTestScheme(t)).
				WithStatusSubresource(&actionsv1alpha1.WorkflowJob{}, &actionsv1alpha1.WorkflowRun{}).WithObjects(run).Build()
			reconciler := &WorkflowRunReconciler{Client: clusterClient, APIReader: clusterClient}
			planned, deferred, err := reconciler.planWorkflowJobs(run, definition, nil, nil, nil)
			if test.invalid && !test.deferred {
				if err == nil || !strings.Contains(err.Error(), "job \"test\" continue-on-error") {
					t.Fatalf("planning error = %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if test.deferred && (len(planned) != 1 || len(deferred) != 1 || deferred[0].JobID != "test") {
				t.Fatalf("planned = %#v, deferred = %#v", planned, deferred)
			}
			if err := reconciler.ensureWorkflowPlan(ctx, run, project, planned, deferred, definition); err != nil {
				t.Fatal(err)
			}
			if err := reconciler.ensureWorkflowJobs(ctx, run, project, planned); err != nil {
				t.Fatal(err)
			}
			listJobs := func() *actionsv1alpha1.WorkflowJobList {
				t.Helper()
				jobs := &actionsv1alpha1.WorkflowJobList{}
				if err := clusterClient.List(ctx, jobs); err != nil {
					t.Fatal(err)
				}
				return jobs
			}
			if test.deferred {
				jobs := listJobs()
				state, err := reconciler.reconcileDeferredJobs(ctx, run, jobs)
				if err != nil || state.changed || len(state.pending) != 1 {
					t.Fatalf("pending planning state = %#v, error = %v", state, err)
				}
				prepare := &jobs.Items[0]
				prepare.Status.Result = actionsv1alpha1.WorkflowJobResultFailure
				prepare.Status.Outputs = map[string]string{"tolerate": "true"}
				if err := clusterClient.Status().Update(ctx, prepare); err != nil {
					t.Fatal(err)
				}
				reconciler = &WorkflowRunReconciler{Client: clusterClient, APIReader: clusterClient}
				state, err = reconciler.reconcileDeferredJobs(ctx, run, listJobs())
				if err != nil || !state.changed {
					t.Fatalf("deferred planning state = %#v, error = %v", state, err)
				}
			}
			jobs := listJobs()
			children := 0
			for _, job := range jobs.Items {
				if job.Spec.JobID == "prepare" {
					continue
				}
				children++
				if test.invalid {
					condition := meta.FindStatusCondition(job.Status.Conditions, actionsv1alpha1.WorkflowJobConditionSucceeded)
					if job.Spec.ContinueOnError || job.Status.Result != actionsv1alpha1.WorkflowJobResultFailure || condition == nil || condition.Reason != "JobPlanningFailed" || !strings.Contains(condition.Message, "continue-on-error") {
						t.Fatalf("invalid evaluation job = %#v", job)
					}
					continue
				}
				want := test.want
				if test.matrix {
					want = want && job.Spec.Matrix.Values["experimental"] == "true"
				}
				if job.Spec.ContinueOnError != want {
					t.Fatalf("job %q continueOnError = %t, want %t", job.Spec.JobID, job.Spec.ContinueOnError, want)
				}
			}
			wantChildren := 1
			if test.matrix {
				wantChildren = 4
			}
			if children != wantChildren {
				t.Fatalf("children = %d, want %d", children, wantChildren)
			}
			if test.deferred {
				for index := range jobs.Items {
					job := &jobs.Items[index]
					if job.Spec.JobID == "prepare" {
						job.Status.Outputs["tolerate"] = "false"
						if err := clusterClient.Status().Update(ctx, job); err != nil {
							t.Fatal(err)
						}
					}
				}
			}
			restarted := &WorkflowRunReconciler{Client: clusterClient, APIReader: clusterClient}
			state, err := restarted.reconcileDeferredJobs(ctx, run, listJobs())
			if err != nil || state.changed || len(state.pending) != 0 {
				t.Fatalf("restart planning state = %#v, error = %v", state, err)
			}
			stored := listJobs()
			for index := range jobs.Items {
				if !reflect.DeepEqual(stored.Items[index].Spec, jobs.Items[index].Spec) {
					t.Fatalf("restart changed job spec: %#v", stored.Items[index].Spec)
				}
			}
		})
	}
}

func TestJobContinueOnErrorDependencies(t *testing.T) {
	for _, matrix := range []bool{false, true} {
		for _, tolerated := range []bool{false, true} {
			t.Run(fmt.Sprintf("matrix=%t/tolerated=%t", matrix, tolerated), func(t *testing.T) {
				ctx := context.Background()
				run := continueOnErrorTestRun()
				failed := &actionsv1alpha1.WorkflowJob{
					ObjectMeta: metav1.ObjectMeta{Name: "build", Namespace: run.Namespace},
					Spec:       actionsv1alpha1.WorkflowJobSpec{JobID: "build", ContinueOnError: tolerated},
					Status:     actionsv1alpha1.WorkflowJobStatus{RunnerRef: &corev1.LocalObjectReference{Name: "runner"}},
				}
				if matrix {
					failed.Spec.JobID = "build-matrix-1"
					failed.Spec.Matrix = &actionsv1alpha1.WorkflowJobMatrix{LogicalJobID: "build"}
				}
				objects := []client.Object{failed}
				if matrix {
					objects = append(objects, &actionsv1alpha1.WorkflowJob{
						ObjectMeta: metav1.ObjectMeta{Name: "build-matrix-2", Namespace: run.Namespace},
						Spec:       actionsv1alpha1.WorkflowJobSpec{JobID: "build-matrix-2", Matrix: &actionsv1alpha1.WorkflowJobMatrix{LogicalJobID: "build"}},
						Status:     actionsv1alpha1.WorkflowJobStatus{Result: actionsv1alpha1.WorkflowJobResultSuccess},
					})
				}
				for id, condition := range map[string]string{
					"implicit": "", "context": "needs.build.result == 'success' && needs.build.outputs.artifact == 'ready'",
					"failure": "failure()", "success": "success()",
				} {
					objects = append(objects, &actionsv1alpha1.WorkflowJob{
						ObjectMeta: metav1.ObjectMeta{Name: id, Namespace: run.Namespace},
						Spec:       actionsv1alpha1.WorkflowJobSpec{JobID: id, Needs: []string{"build"}, If: condition},
					})
				}
				clusterClient := fake.NewClientBuilder().WithScheme(runnerTestScheme(t)).
					WithStatusSubresource(&actionsv1alpha1.WorkflowJob{}).WithObjects(objects...).Build()
				finished := metav1.Now()
				nativeJob := &batchv1.Job{Status: batchv1.JobStatus{CompletionTime: &finished}}
				runnerReconciler := &RunnerReconciler{Client: clusterClient, APIReader: clusterClient}
				if err := runnerReconciler.updateWorkflowJobStatus(ctx, failed, nativeJob, &finished, &runner.Result{
					Conclusion: runner.ResultConclusionFailure, Outputs: map[string]string{"artifact": "ready"},
				}, false); err != nil {
					t.Fatal(err)
				}
				before := failed.Status.DeepCopy()
				for iteration := 0; iteration < 2; iteration++ {
					jobs := &actionsv1alpha1.WorkflowJobList{}
					if err := clusterClient.List(ctx, jobs); err != nil {
						t.Fatal(err)
					}
					reconciler := &WorkflowRunReconciler{Client: clusterClient, APIReader: clusterClient}
					if err := reconciler.reconcileWorkflowJobGraph(ctx, run, "CI", nil, nil, nil, jobs.Items); err != nil {
						t.Fatal(err)
					}
					for _, id := range []string{"implicit", "context", "failure", "success"} {
						job := &actionsv1alpha1.WorkflowJob{}
						if err := clusterClient.Get(ctx, client.ObjectKey{Namespace: run.Namespace, Name: id}, job); err != nil {
							t.Fatal(err)
						}
						wantReady := tolerated
						if id == "failure" {
							wantReady = !tolerated
						}
						if workflowJobReady(job) != wantReady || !wantReady && job.Status.Result != actionsv1alpha1.WorkflowJobResultSkipped {
							t.Fatalf("dependent %q status = %#v, want ready %t", id, job.Status, wantReady)
						}
						if wantReady {
							configMap := &corev1.ConfigMap{}
							if err := clusterClient.Get(ctx, client.ObjectKey{Namespace: run.Namespace, Name: childName(id, "needs")}, configMap); err != nil {
								t.Fatal(err)
							}
							needs, err := runner.DecodeNeedsContext([]byte(configMap.Data[jobNeedsKey]))
							if err != nil {
								t.Fatal(err)
							}
							wantResult := "failure"
							if tolerated {
								wantResult = "success"
							}
							if needs["build"].Result != wantResult || needs["build"].Outputs["artifact"] != "ready" {
								t.Fatalf("needs = %#v", needs)
							}
						}
					}
				}
				stored := &actionsv1alpha1.WorkflowJob{}
				if err := clusterClient.Get(ctx, client.ObjectKeyFromObject(failed), stored); err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(before, &stored.Status) || stored.Status.Result != actionsv1alpha1.WorkflowJobResultFailure || stored.Status.CompletionTime == nil {
					t.Fatalf("failed job execution status = %#v", stored.Status)
				}
			})
		}
	}
}
