package main

import (
	"bytes"
	"context"
	"io"
	"regexp"
	"strings"
	"testing"
	"time"

	actionsv1alpha1 "github.com/kelos-dev/open-actions/api/v1alpha1"
	"github.com/kelos-dev/open-actions/internal/workflowrun"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

type testRunLogSource struct {
	pods        []corev1.Pod
	data        string
	streamedPod string
	follow      bool
}

func (s *testRunLogSource) ListPods(context.Context, string, string) (*corev1.PodList, error) {
	return &corev1.PodList{Items: append([]corev1.Pod(nil), s.pods...)}, nil
}

func (s *testRunLogSource) Stream(_ context.Context, _, name string, follow bool) (io.ReadCloser, error) {
	s.streamedPod = name
	s.follow = follow
	return io.NopCloser(strings.NewReader(s.data)), nil
}

func TestRunListShowsNewestRunsFirst(t *testing.T) {
	older := testWorkflowRun("older", "team-ci", "Older", metav1.ConditionTrue)
	older.CreationTimestamp = metav1.NewTime(time.Now().Add(-time.Hour))
	newer := testWorkflowRun("newer", "team-ci", "Newer", metav1.ConditionUnknown)
	newer.CreationTimestamp = metav1.NewTime(time.Now().Add(-time.Minute))

	dependencies := testRunDependencies(t, &testRunLogSource{}, older, newer)
	var stdout bytes.Buffer
	if err := runWithDependencies(context.Background(), []string{"run", "list", "--namespace", "team-ci"}, &stdout, &bytes.Buffer{}, dependencies); err != nil {
		t.Fatalf("run list error = %v", err)
	}
	output := stdout.String()
	if !strings.Contains(output, "✓ Succeeded  older") || !strings.Contains(output, "* Queued     newer") {
		t.Fatalf("run list output = %q", output)
	}
	if strings.Index(output, "newer") > strings.Index(output, "older") {
		t.Fatalf("run list was not newest first: %q", output)
	}
}

func TestRunListSupportsAllNamespaces(t *testing.T) {
	run := testWorkflowRun("ci", "team-ci", "CI", metav1.ConditionUnknown)
	dependencies := testRunDependencies(t, &testRunLogSource{}, run)
	var stdout bytes.Buffer
	if err := runWithDependencies(context.Background(), []string{"run", "list", "--all-namespaces"}, &stdout, &bytes.Buffer{}, dependencies); err != nil {
		t.Fatalf("run list error = %v", err)
	}
	if output := stdout.String(); !strings.Contains(output, "NAMESPACE") || !strings.Contains(output, "team-ci") {
		t.Fatalf("run list output = %q", output)
	}
}

func TestRunListUsesKubeconfigContextNamespace(t *testing.T) {
	run := testWorkflowRun("ci", "team-ci", "CI", metav1.ConditionUnknown)
	scheme := runtime.NewScheme()
	if err := actionsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add actions API to scheme: %v", err)
	}
	kube := fake.NewClientBuilder().WithScheme(scheme).WithObjects(run).Build()
	var received runKubeOptions
	dependencies := commandDependencies{
		defaultKubeconfig: "default-kubeconfig",
		newRunClients: func(options runKubeOptions) (*runClients, error) {
			received = options
			return &runClients{kube: kube, logs: &testRunLogSource{}, defaultNamespace: "team-ci"}, nil
		},
	}
	var stdout bytes.Buffer
	if err := runWithDependencies(context.Background(), []string{"run", "list", "--kubeconfig", "selected-kubeconfig", "--context", "development"}, &stdout, &bytes.Buffer{}, dependencies); err != nil {
		t.Fatalf("run list error = %v", err)
	}
	if received.kubeconfig != "selected-kubeconfig" || received.defaultKubeconfig != "default-kubeconfig" || received.context != "development" {
		t.Fatalf("run options = %#v", received)
	}
	if !strings.Contains(stdout.String(), "ci") {
		t.Fatalf("run list output = %q", stdout.String())
	}
}

func TestRunListRejectsConflictingNamespaceFlags(t *testing.T) {
	dependencies := testRunDependencies(t, &testRunLogSource{})
	err := runWithDependencies(context.Background(), []string{"run", "list", "--namespace", "team-ci", "--all-namespaces"}, &bytes.Buffer{}, &bytes.Buffer{}, dependencies)
	if err == nil || !strings.Contains(err.Error(), "cannot be used together") {
		t.Fatalf("run list error = %v", err)
	}
}

func TestRunViewShowsJobs(t *testing.T) {
	run := testWorkflowRun("ci", "team-ci", "CI", metav1.ConditionUnknown)
	run.Status.StartTime = &metav1.Time{Time: time.Now().Add(-time.Minute)}
	job := testWorkflowJob(t, run, "ci-build", "build", "Build")
	job.Status.RunnerRef = &corev1.LocalObjectReference{Name: "linux-1"}
	job.Status.StartTime = &metav1.Time{Time: time.Now().Add(-30 * time.Second)}

	dependencies := testRunDependencies(t, &testRunLogSource{}, run, job)
	var stdout bytes.Buffer
	if err := runWithDependencies(context.Background(), []string{"run", "view", "ci", "-n", "team-ci"}, &stdout, &bytes.Buffer{}, dependencies); err != nil {
		t.Fatalf("run view error = %v", err)
	}
	lines := outputLines(stdout.String())
	if lines[0] != "* CI · ci" {
		t.Fatalf("run view headline = %q", lines[0])
	}
	jobLine := regexp.MustCompile(`^\* Running · Build \(build\) in \d+s · WorkflowJob ci-build · runner linux-1$`)
	if !matchesLine(lines, jobLine) {
		t.Fatalf("run view output =\n%s", stdout.String())
	}
	hint := "To read runner logs, try: open-actions run logs ci --job build --namespace team-ci"
	if !containsLine(lines, hint) {
		t.Fatalf("run view output =\n%s", stdout.String())
	}
	for _, expected := range []string{"Repository:  acme/example", "Status:      Running"} {
		if !containsLine(lines, expected) {
			t.Fatalf("run view output does not contain %q:\n%s", expected, stdout.String())
		}
	}
}

func TestRunListKeepsWorkflowNamesOnOneLine(t *testing.T) {
	run := testWorkflowRun("ci", "team-ci", "CI\twith\nspaces", metav1.ConditionUnknown)
	dependencies := testRunDependencies(t, &testRunLogSource{}, run)
	var stdout bytes.Buffer
	if err := runWithDependencies(context.Background(), []string{"run", "list", "-n", "team-ci"}, &stdout, &bytes.Buffer{}, dependencies); err != nil {
		t.Fatalf("run list error = %v", err)
	}
	if output := stdout.String(); !strings.Contains(output, "CI with spaces") || strings.Contains(output, "CI\twith") {
		t.Fatalf("run list output = %q", output)
	}
}

func TestRunLogsStreamsSelectedJob(t *testing.T) {
	run := testWorkflowRun("ci", "team-ci", "CI", metav1.ConditionUnknown)
	job := testWorkflowJob(t, run, "ci-build", "build", "Build")
	logs := &testRunLogSource{
		pods: []corev1.Pod{{ObjectMeta: metav1.ObjectMeta{Name: "ci-build-pod", Namespace: "team-ci"}}},
		data: "build output\n",
	}
	dependencies := testRunDependencies(t, logs, run, job)
	var stdout bytes.Buffer
	if err := runWithDependencies(context.Background(), []string{"run", "logs", "ci", "--job", "build", "--follow", "-n", "team-ci"}, &stdout, &bytes.Buffer{}, dependencies); err != nil {
		t.Fatalf("run logs error = %v", err)
	}
	if stdout.String() != "build output\n" || logs.streamedPod != "ci-build-pod" || !logs.follow {
		t.Fatalf("run logs output = %q, pod = %q, follow = %t", stdout.String(), logs.streamedPod, logs.follow)
	}
}

func TestRunLogsRequiresJobForMultiJobRun(t *testing.T) {
	run := testWorkflowRun("ci", "team-ci", "CI", metav1.ConditionUnknown)
	build := testWorkflowJob(t, run, "ci-build", "build", "Build")
	testJob := testWorkflowJob(t, run, "ci-test", "test", "Test")
	dependencies := testRunDependencies(t, &testRunLogSource{}, run, build, testJob)
	err := runWithDependencies(context.Background(), []string{"run", "logs", "ci", "-n", "team-ci"}, &bytes.Buffer{}, &bytes.Buffer{}, dependencies)
	if err == nil || !strings.Contains(err.Error(), "--job is required") {
		t.Fatalf("run logs error = %v", err)
	}
}

func TestRunCommandHelpDoesNotLoadKubernetesConfiguration(t *testing.T) {
	dependencies := commandDependencies{newRunClients: func(runKubeOptions) (*runClients, error) {
		t.Fatal("run help loaded Kubernetes configuration")
		return nil, nil
	}}
	var stdout bytes.Buffer
	if err := runWithDependencies(context.Background(), []string{"run", "--help"}, &stdout, &bytes.Buffer{}, dependencies); err != nil {
		t.Fatalf("run help error = %v", err)
	}
	for _, command := range []string{"list", "view", "watch", "logs", "cancel", "rerun"} {
		if !strings.Contains(stdout.String(), command) {
			t.Fatalf("run help output does not contain %q: %q", command, stdout.String())
		}
	}
}

func testRunDependencies(t *testing.T, logs runLogSource, objects ...client.Object) commandDependencies {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := actionsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add actions API to scheme: %v", err)
	}
	kube := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()
	return commandDependencies{newRunClients: func(runKubeOptions) (*runClients, error) {
		return &runClients{kube: kube, logs: logs, defaultNamespace: "default"}, nil
	}}
}

// watchedRunClient reports every read object so tests can advance a watched
// workflow run between refreshes.
type watchedRunClient struct {
	client.Client
	observe func(client.Object)
}

func (c *watchedRunClient) Get(ctx context.Context, key client.ObjectKey, object client.Object, options ...client.GetOption) error {
	if err := c.Client.Get(ctx, key, object, options...); err != nil {
		return err
	}
	c.observe(object)
	return nil
}

func withRunClientInterceptor(dependencies commandDependencies, observe func(client.Object)) commandDependencies {
	factory := dependencies.newRunClients
	return commandDependencies{
		defaultKubeconfig: dependencies.defaultKubeconfig,
		newRunClients: func(options runKubeOptions) (*runClients, error) {
			clients, err := factory(options)
			if err != nil {
				return nil, err
			}
			clients.kube = &watchedRunClient{Client: clients.kube, observe: observe}
			return clients, nil
		},
	}
}

func testWorkflowRun(name, namespace, workflowName string, status metav1.ConditionStatus) *actionsv1alpha1.WorkflowRun {
	run := &actionsv1alpha1.WorkflowRun{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace, UID: typesUID(name)},
		Spec: actionsv1alpha1.WorkflowRunSpec{
			WorkflowPath: ".open-actions/workflows/ci.yaml",
			Source: actionsv1alpha1.WorkflowRunSource{Type: actionsv1alpha1.SourceTypeGitHub, GitHub: &actionsv1alpha1.GitHubWorkflowRunSource{
				Repository: actionsv1alpha1.GitHubRepository{Owner: "acme", Name: "example"},
				Event:      actionsv1alpha1.GitHubEvent{Name: actionsv1alpha1.GitHubEventNamePush},
				Revision:   actionsv1alpha1.GitRevision{SHA: strings.Repeat("a", 40)},
			}},
		},
		Status: actionsv1alpha1.WorkflowRunStatus{WorkflowName: workflowName},
	}
	if status != "" {
		run.Status.Conditions = []metav1.Condition{{
			Type: actionsv1alpha1.WorkflowRunConditionSucceeded, Status: status, Reason: "Test",
		}}
	}
	return run
}

func testWorkflowJob(t *testing.T, run *actionsv1alpha1.WorkflowRun, name, jobID, displayName string) *actionsv1alpha1.WorkflowJob {
	t.Helper()
	job := &actionsv1alpha1.WorkflowJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: run.Namespace,
			UID:       typesUID(name),
			Labels:    map[string]string{actionsv1alpha1.LabelWorkflowRunUID: string(run.UID)},
		},
		Spec: actionsv1alpha1.WorkflowJobSpec{JobID: jobID, DisplayName: displayName},
	}
	scheme := runtime.NewScheme()
	if err := actionsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add actions API to scheme: %v", err)
	}
	if err := controllerutil.SetControllerReference(run, job, scheme); err != nil {
		t.Fatalf("set WorkflowRun owner: %v", err)
	}
	return job
}

func typesUID(value string) types.UID {
	return types.UID("uid-" + value)
}

func TestRunWatchRefreshesUntilCompletion(t *testing.T) {
	run := testWorkflowRun("ci", "team-ci", "CI", metav1.ConditionUnknown)
	run.Status.StartTime = &metav1.Time{Time: time.Now().Add(-time.Minute)}
	job := testWorkflowJob(t, run, "ci-build", "build", "Build")
	reads := 0
	complete := func(object client.Object) {
		reads++
		watched, isRun := object.(*actionsv1alpha1.WorkflowRun)
		if !isRun || reads < 2 {
			return
		}
		watched.Status.Conditions = []metav1.Condition{{
			Type: actionsv1alpha1.WorkflowRunConditionSucceeded, Status: metav1.ConditionTrue, Reason: "JobsSucceeded",
		}}
	}
	dependencies := testRunDependencies(t, &testRunLogSource{}, run, job)
	dependencies = withRunClientInterceptor(dependencies, complete)

	var stdout, stderr bytes.Buffer
	arguments := []string{"run", "watch", "ci", "-n", "team-ci", "--interval", "1ms"}
	if err := runWithDependencies(context.Background(), arguments, &stdout, &stderr, dependencies); err != nil {
		t.Fatalf("run watch error = %v", err)
	}
	output := stdout.String()
	if strings.Count(output, "JOBS") != 2 {
		t.Fatalf("run watch did not refresh the workflow run: %q", output)
	}
	if !strings.Contains(output, `✓ WorkflowRun "ci" completed with status Succeeded`) {
		t.Fatalf("run watch output = %q", output)
	}
	if !strings.Contains(stderr.String(), `Refreshing WorkflowRun "ci" every 1ms`) {
		t.Fatalf("run watch stderr = %q", stderr.String())
	}
}

func TestRunWatchReportsCompletedRunWithoutRefreshing(t *testing.T) {
	run := testWorkflowRun("ci", "team-ci", "CI", metav1.ConditionFalse)
	run.Status.Conditions[0].Reason = "JobFailed"
	dependencies := testRunDependencies(t, &testRunLogSource{}, run)

	var stdout bytes.Buffer
	if err := runWithDependencies(context.Background(), []string{"run", "watch", "ci", "-n", "team-ci"}, &stdout, &bytes.Buffer{}, dependencies); err != nil {
		t.Fatalf("run watch error = %v", err)
	}
	if !strings.Contains(stdout.String(), `X WorkflowRun "ci" completed with status Failed`) {
		t.Fatalf("run watch output = %q", stdout.String())
	}
}

func TestRunWatchFailsOnRequestedExitStatus(t *testing.T) {
	run := testWorkflowRun("ci", "team-ci", "CI", metav1.ConditionFalse)
	run.Status.Conditions[0].Reason = "JobFailed"
	dependencies := testRunDependencies(t, &testRunLogSource{}, run)

	var stdout bytes.Buffer
	arguments := []string{"run", "watch", "ci", "-n", "team-ci", "--exit-status"}
	err := runWithDependencies(context.Background(), arguments, &stdout, &bytes.Buffer{}, dependencies)
	if err == nil || err.Error() != `WorkflowRun "ci" completed with status Failed` {
		t.Fatalf("run watch error = %v", err)
	}
	if strings.Contains(stdout.String(), "completed with status") {
		t.Fatalf("run watch reported the failure twice: %q", stdout.String())
	}
}

func TestRunWatchRejectsNonPositiveInterval(t *testing.T) {
	dependencies := commandDependencies{newRunClients: func(runKubeOptions) (*runClients, error) {
		t.Fatal("run watch loaded Kubernetes configuration for an invalid interval")
		return nil, nil
	}}
	err := runWithDependencies(context.Background(), []string{"run", "watch", "ci", "--interval", "0s"}, &bytes.Buffer{}, &bytes.Buffer{}, dependencies)
	if err == nil || !strings.Contains(err.Error(), "--interval must be positive") {
		t.Fatalf("run watch error = %v", err)
	}
}

func TestRunCancelRequestsCancellation(t *testing.T) {
	run := testWorkflowRun("ci", "team-ci", "CI", metav1.ConditionUnknown)
	dependencies := testRunDependencies(t, &testRunLogSource{}, run)
	clients, _, err := loadRunClients(dependencies, runCommandOptions{})
	if err != nil {
		t.Fatalf("load run clients error = %v", err)
	}

	var stdout bytes.Buffer
	if err := runWithDependencies(context.Background(), []string{"run", "cancel", "ci", "-n", "team-ci"}, &stdout, &bytes.Buffer{}, dependencies); err != nil {
		t.Fatalf("run cancel error = %v", err)
	}
	if !strings.Contains(stdout.String(), `✓ Requested cancellation of WorkflowRun "ci"`) {
		t.Fatalf("run cancel output = %q", stdout.String())
	}
	cancelled := &actionsv1alpha1.WorkflowRun{}
	if err := clients.kube.Get(context.Background(), client.ObjectKeyFromObject(run), cancelled); err != nil {
		t.Fatalf("get WorkflowRun error = %v", err)
	}
	if !cancelled.Spec.CancelRequested {
		t.Fatal("run cancel did not request cancellation")
	}
}

func TestRunCancelReportsRepeatedRequest(t *testing.T) {
	run := testWorkflowRun("ci", "team-ci", "CI", metav1.ConditionUnknown)
	run.Spec.CancelRequested = true
	dependencies := testRunDependencies(t, &testRunLogSource{}, run)

	var stdout bytes.Buffer
	if err := runWithDependencies(context.Background(), []string{"run", "cancel", "ci", "-n", "team-ci"}, &stdout, &bytes.Buffer{}, dependencies); err != nil {
		t.Fatalf("run cancel error = %v", err)
	}
	if !strings.Contains(stdout.String(), `Cancellation of WorkflowRun "ci" was already requested`) {
		t.Fatalf("run cancel output = %q", stdout.String())
	}
}

func TestRunCancelRejectsCompletedRun(t *testing.T) {
	run := testWorkflowRun("ci", "team-ci", "CI", metav1.ConditionTrue)
	dependencies := testRunDependencies(t, &testRunLogSource{}, run)

	err := runWithDependencies(context.Background(), []string{"run", "cancel", "ci", "-n", "team-ci"}, &bytes.Buffer{}, &bytes.Buffer{}, dependencies)
	if err == nil || err.Error() != `WorkflowRun "ci" is already complete` {
		t.Fatalf("run cancel error = %v", err)
	}
}

func TestRunRerunCreatesNextAttempt(t *testing.T) {
	run := testWorkflowRun("ci", "team-ci", "CI", metav1.ConditionTrue)
	dependencies := testRunDependencies(t, &testRunLogSource{}, run)
	clients, _, err := loadRunClients(dependencies, runCommandOptions{})
	if err != nil {
		t.Fatalf("load run clients error = %v", err)
	}

	var stdout bytes.Buffer
	if err := runWithDependencies(context.Background(), []string{"run", "rerun", "ci", "-n", "team-ci"}, &stdout, &bytes.Buffer{}, dependencies); err != nil {
		t.Fatalf("run rerun error = %v", err)
	}
	attemptName := workflowrun.RerunName(run, 2)
	output := stdout.String()
	if !strings.Contains(output, `✓ Requested rerun of WorkflowRun "ci"`) || !strings.Contains(output, `Attempt 2 is WorkflowRun "`+attemptName+`"`) {
		t.Fatalf("run rerun output = %q", output)
	}
	attempt := &actionsv1alpha1.WorkflowRun{}
	if err := clients.kube.Get(context.Background(), types.NamespacedName{Namespace: "team-ci", Name: attemptName}, attempt); err != nil {
		t.Fatalf("get rerun WorkflowRun error = %v", err)
	}
	if attempt.Spec.Rerun == nil || attempt.Spec.Rerun.Attempt != 2 || attempt.Spec.Rerun.OriginalRunRef.Name != run.Name || len(attempt.Spec.Rerun.JobIDs) != 0 {
		t.Fatalf("rerun spec = %#v", attempt.Spec.Rerun)
	}
}

func TestRunRerunSelectsFailedJobs(t *testing.T) {
	run := testWorkflowRun("ci", "team-ci", "CI", metav1.ConditionFalse)
	run.Status.Conditions[0].Reason = "JobFailed"
	run.Status.Jobs = &actionsv1alpha1.WorkflowRunJobStatus{Total: 2}
	build := testWorkflowJob(t, run, "ci-build", "build", "Build")
	build.Status.Result = actionsv1alpha1.WorkflowJobResultFailure
	unit := testWorkflowJob(t, run, "ci-test", "test", "Test")
	unit.Status.Result = actionsv1alpha1.WorkflowJobResultSuccess
	dependencies := testRunDependencies(t, &testRunLogSource{}, run, build, unit)
	clients, _, err := loadRunClients(dependencies, runCommandOptions{})
	if err != nil {
		t.Fatalf("load run clients error = %v", err)
	}

	var stdout bytes.Buffer
	arguments := []string{"run", "rerun", "ci", "-n", "team-ci", "--failed"}
	if err := runWithDependencies(context.Background(), arguments, &stdout, &bytes.Buffer{}, dependencies); err != nil {
		t.Fatalf("run rerun error = %v", err)
	}
	output := stdout.String()
	if !strings.Contains(output, `✓ Requested rerun (failed jobs) of WorkflowRun "ci"`) || !strings.Contains(output, "Selected jobs: build\n") {
		t.Fatalf("run rerun output = %q", output)
	}
	attempt := &actionsv1alpha1.WorkflowRun{}
	if err := clients.kube.Get(context.Background(), types.NamespacedName{Namespace: "team-ci", Name: workflowrun.RerunName(run, 2)}, attempt); err != nil {
		t.Fatalf("get rerun WorkflowRun error = %v", err)
	}
	if attempt.Spec.Rerun == nil || len(attempt.Spec.Rerun.JobIDs) != 1 || attempt.Spec.Rerun.JobIDs[0] != "build" {
		t.Fatalf("rerun spec = %#v", attempt.Spec.Rerun)
	}
}

func TestRunRerunRejectsActiveRun(t *testing.T) {
	run := testWorkflowRun("ci", "team-ci", "CI", metav1.ConditionUnknown)
	dependencies := testRunDependencies(t, &testRunLogSource{}, run)

	err := runWithDependencies(context.Background(), []string{"run", "rerun", "ci", "-n", "team-ci"}, &bytes.Buffer{}, &bytes.Buffer{}, dependencies)
	if err == nil || err.Error() != `rerun WorkflowRun "ci": the latest workflow attempt is not complete` {
		t.Fatalf("run rerun error = %v", err)
	}
}
