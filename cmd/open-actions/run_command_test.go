package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	actionsv1alpha1 "github.com/kelos-dev/open-actions/api/v1alpha1"
	"github.com/kelos-dev/open-actions/internal/joblog"
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

func TestRunLogsUsesConsoleArchiveAndReportsReadErrors(t *testing.T) {
	for _, failure := range []string{"", "archive is incomplete"} {
		t.Run(failure, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/runs/team-ci/ci/jobs/ci-build/logs" || r.URL.Query().Get("follow") != "true" {
					t.Errorf("request = %s", r.URL)
				}
				w.Header().Set("Content-Type", joblog.DownloadContentType)
				encoder := json.NewEncoder(w)
				_ = encoder.Encode(joblog.DownloadRecord{Data: []byte("first\nlast\n")})
				if failure != "" {
					_ = encoder.Encode(joblog.DownloadRecord{Error: failure})
				} else {
					_ = encoder.Encode(joblog.DownloadRecord{End: true})
				}
			}))
			defer server.Close()
			run := testWorkflowRun("ci", "team-ci", "CI", metav1.ConditionUnknown)
			run.Status.Identity = &actionsv1alpha1.WorkflowRunIdentityStatus{URL: server.URL + "/runs/team-ci/ci"}
			job := testWorkflowJob(t, run, "ci-build", "build", "Build")
			job.Annotations = map[string]string{joblog.ArchiveAnnotation: "true"}
			logs := &testRunLogSource{}
			dependencies := testRunDependencies(t, logs, run, job)
			var stdout bytes.Buffer
			err := runWithDependencies(context.Background(), []string{"run", "logs", "ci", "-n", "team-ci", "--follow"}, &stdout, &bytes.Buffer{}, dependencies)
			if (failure == "") != (err == nil) {
				t.Fatalf("error = %v", err)
			}
			if err != nil && !strings.Contains(err.Error(), failure) {
				t.Fatal(err)
			}
			if stdout.String() != "first\nlast\n" || logs.streamedPod != "" {
				t.Fatalf("logs = %q, Pod = %q", stdout.String(), logs.streamedPod)
			}
		})
	}
}

func TestRunLogsWithoutConsoleUsesRetainedPodLogs(t *testing.T) {
	run := testWorkflowRun("ci", "team-ci", "CI", metav1.ConditionUnknown)
	job := testWorkflowJob(t, run, "ci-build", "build", "Build")
	job.Annotations = map[string]string{joblog.ArchiveAnnotation: "true"}
	logs := &testRunLogSource{pods: []corev1.Pod{{ObjectMeta: metav1.ObjectMeta{Name: "runner", Namespace: "team-ci"}}}, data: "bare output\n"}
	dependencies := testRunDependencies(t, logs, run, job)
	var stdout, stderr bytes.Buffer
	if err := runWithDependencies(context.Background(), []string{"run", "logs", "ci", "-n", "team-ci"}, &stdout, &stderr, dependencies); err != nil {
		t.Fatal(err)
	}
	if stdout.String() != "bare output\n" || logs.streamedPod != "runner" {
		t.Fatalf("output = %q, Pod = %q", stdout.String(), logs.streamedPod)
	}
	if !strings.Contains(stderr.String(), "showing retained Pod logs") {
		t.Fatalf("missing fallback notice: %q", stderr.String())
	}
}

func TestRunLogsConsoleSnapshotExplainsQueuedJob(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "Runner logs are not available yet; use --follow", http.StatusConflict)
	}))
	defer server.Close()
	run := testWorkflowRun("ci", "team-ci", "CI", metav1.ConditionUnknown)
	job := testWorkflowJob(t, run, "ci-build", "build", "Build")
	dependencies := testRunDependencies(t, &testRunLogSource{}, run, job)
	err := runWithDependencies(context.Background(), []string{"run", "logs", "ci", "-n", "team-ci", "--console-url", server.URL}, &bytes.Buffer{}, &bytes.Buffer{}, dependencies)
	if err == nil || !strings.Contains(err.Error(), "use --follow") {
		t.Fatalf("queued snapshot error = %v", err)
	}
}

func TestRunLogsRejectsUnconfirmedDownload(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", joblog.DownloadContentType)
		_ = json.NewEncoder(w).Encode(joblog.DownloadRecord{Data: []byte("partial output\n")})
	}))
	defer server.Close()
	var output bytes.Buffer
	err := printConsoleLogs(context.Background(), &output, server.URL, "job", true)
	if err == nil || !strings.Contains(err.Error(), "without a completion record") || output.String() != "partial output\n" {
		t.Fatalf("download = %q, %v", output.String(), err)
	}
}

func TestRunLogsRejectsAuthRedirectAndHTML(t *testing.T) {
	for _, redirect := range []bool{false, true} {
		t.Run(fmt.Sprint(redirect), func(t *testing.T) {
			var loginRequests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/login" {
					loginRequests.Add(1)
				}
				if redirect && r.URL.Path != "/login" {
					http.Redirect(w, r, "/login", http.StatusFound)
					return
				}
				w.Header().Set("Content-Type", "text/html")
				io.WriteString(w, "<html>Sign in</html>")
			}))
			defer server.Close()
			var output bytes.Buffer
			err := printConsoleLogs(context.Background(), &output, server.URL, "job", false)
			if err == nil || !strings.Contains(err.Error(), "--console-url") || output.Len() != 0 || loginRequests.Load() != 0 {
				t.Fatalf("auth response output = %q, error %v, redirects %d", output.String(), err, loginRequests.Load())
			}
		})
	}
}

func TestRunLogsHonorsDefaultTransportProxy(t *testing.T) {
	var proxyRequests atomic.Int32
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Host != "console.invalid" {
			t.Errorf("proxy target = %s", r.URL)
		}
		proxyRequests.Add(1)
		w.Header().Set("Content-Type", joblog.DownloadContentType)
		_ = json.NewEncoder(w).Encode(joblog.DownloadRecord{Data: []byte("proxy output\n"), End: true})
	}))
	defer proxy.Close()
	original := http.DefaultTransport
	transport := original.(*http.Transport).Clone()
	proxyURL, err := url.Parse(proxy.URL)
	if err != nil {
		t.Fatal(err)
	}
	transport.Proxy = http.ProxyURL(proxyURL)
	http.DefaultTransport = transport
	defer func() { http.DefaultTransport = original; transport.CloseIdleConnections() }()
	var output bytes.Buffer
	if err := printConsoleLogs(context.Background(), &output, "http://console.invalid/runs/ci", "job", false); err != nil {
		t.Fatal(err)
	}
	if proxyRequests.Load() != 1 || output.String() != "proxy output\n" {
		t.Fatalf("proxy calls = %d, output = %q", proxyRequests.Load(), output.String())
	}
}
