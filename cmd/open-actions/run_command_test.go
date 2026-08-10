package main

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"
	"time"

	actionsv1alpha1 "github.com/kelos-dev/open-actions/api/v1alpha1"
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
	if !strings.Contains(output, "NAME") || !strings.Contains(output, "REPOSITORY") || !strings.Contains(output, "Succeeded") || !strings.Contains(output, "Queued") {
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
	reader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(run).Build()
	var received runKubeOptions
	dependencies := commandDependencies{
		defaultKubeconfig: "default-kubeconfig",
		newRunClients: func(options runKubeOptions) (*runClients, error) {
			received = options
			return &runClients{reader: reader, logs: &testRunLogSource{}, defaultNamespace: "team-ci"}, nil
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
	job := testWorkflowJob(t, run, "ci-build", "build", "Build")
	job.Status.RunnerRef = &corev1.LocalObjectReference{Name: "linux-1"}
	job.Status.StartTime = &metav1.Time{Time: time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)}

	dependencies := testRunDependencies(t, &testRunLogSource{}, run, job)
	var stdout bytes.Buffer
	if err := runWithDependencies(context.Background(), []string{"run", "view", "ci", "-n", "team-ci"}, &stdout, &bytes.Buffer{}, dependencies); err != nil {
		t.Fatalf("run view error = %v", err)
	}
	output := stdout.String()
	for _, expected := range []string{"Name:", "CI", "acme/example", "build", "ci-build", "Build", "linux-1", "Running"} {
		if !strings.Contains(output, expected) {
			t.Fatalf("run view output does not contain %q: %q", expected, output)
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
	for _, command := range []string{"list", "view", "logs"} {
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
	reader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()
	return commandDependencies{newRunClients: func(runKubeOptions) (*runClients, error) {
		return &runClients{reader: reader, logs: logs, defaultNamespace: "default"}, nil
	}}
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
