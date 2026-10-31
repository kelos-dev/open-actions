package console

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	actionsv1alpha1 "github.com/kelos-dev/open-actions/api/v1alpha1"
	"github.com/kelos-dev/open-actions/internal/artifact"
	"github.com/kelos-dev/open-actions/internal/joblog"
	"github.com/kelos-dev/open-actions/internal/runner"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	k8stesting "k8s.io/client-go/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestArchivedLogsReplayWithoutRunnerPod(t *testing.T) {
	handler := newTestHandler(t, false)
	job := &actionsv1alpha1.WorkflowJob{}
	if err := handler.client.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "build"}, job); err != nil {
		t.Fatal(err)
	}
	job.Annotations = map[string]string{joblog.ArchiveAnnotation: "true"}
	if err := handler.client.Update(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	store, err := joblog.NewStore(t.TempDir(), time.Hour, 1<<20, joblog.DefaultMaxChunks)
	if err != nil {
		t.Fatal(err)
	}
	chunks := []string{"2026-08-10T12:34:56Z ::group::Build\n2026-08-10T12:34:57Z first\n", "2026-08-10T12:34:58Z second\n2026-08-10T12:34:59Z ::endgroup::\n"}
	for index, data := range chunks {
		if err := store.Append(string(job.UID), joblog.Chunk{Sequence: uint64(index), Data: []byte(data), Complete: index == 1}); err != nil {
			t.Fatal(err)
		}
	}
	archive := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var sequence uint64
		if strings.HasSuffix(r.URL.Path, "/1") {
			sequence = 1
		}
		chunk, err := store.Read(string(job.UID), sequence)
		if err != nil {
			t.Error(err)
			w.WriteHeader(500)
			return
		}
		_ = json.NewEncoder(w).Encode(chunk)
	}))
	defer archive.Close()
	archiveClient, err := joblog.NewClient(archive.URL, archive.Client())
	if err != nil {
		t.Fatal(err)
	}
	tokens, err := joblog.NewReadTokenCodec(bytes.Repeat([]byte("r"), 32))
	if err != nil {
		t.Fatal(err)
	}
	kubernetes := fake.NewClientset()
	handler.logs, err = NewArchivedLogSource(kubernetes, archiveClient, tokens)
	if err != nil {
		t.Fatal(err)
	}
	for attempt := 0; attempt < 2; attempt++ {
		response := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodGet, "/runs/default/ci/jobs/build/stream", nil)
		if attempt == 1 {
			request.Header.Set("Last-Event-ID", "2")
		}
		handler.ServeHTTP(response, request)
		body := response.Body.String()
		if !strings.Contains(body, "second") || !strings.Contains(body, "event: end") || strings.Contains(body, "event: error") {
			t.Fatalf("stream = %s", body)
		}
		if attempt == 1 && strings.Contains(body, "first") {
			t.Fatalf("reconnect duplicated output: %s", body)
		}
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/runs/default/ci/jobs/build/logs", nil))
	download, failure, ended := parseTestDownload(t, response)
	if download != "::group::Build\nfirst\nsecond\n::endgroup::\n" || failure != "" || !ended {
		t.Fatalf("download = %q, error %q, ended %v", download, failure, ended)
	}
	if len(kubernetes.Actions()) != 0 {
		t.Fatalf("archive accessed Pods: %#v", kubernetes.Actions())
	}

	stale := job.DeepCopy()
	stale.Annotations = nil
	_, err = kubernetes.CoreV1().Pods(job.Namespace).Create(context.Background(), &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "runner", Namespace: job.Namespace, Labels: map[string]string{actionsv1alpha1.LabelWorkflowJobUID: string(job.UID)}}}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	stream, err := handler.openJobLogs(context.Background(), stale, true, func() {})
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	output, err := io.ReadAll(stream)
	if err != nil || string(output) != strings.Join(chunks, "") {
		t.Fatalf("queued job archive = %q, %v", output, err)
	}
}

func TestArchivedLogReaderReportsIncompleteOutputAndCancellation(t *testing.T) {
	archive := httptest.NewServer(http.NotFoundHandler())
	defer archive.Close()
	archiveClient, _ := joblog.NewClient(archive.URL, archive.Client())
	tokens, _ := joblog.NewReadTokenCodec(bytes.Repeat([]byte("r"), 32))
	source := &kubernetesLogSource{archive: archiveClient, tokens: tokens}
	stream, err := source.StreamArchive(context.Background(), "job", true, func(context.Context) (bool, error) { return true, nil })
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	if _, err := io.ReadAll(stream); err == nil || !strings.Contains(err.Error(), "incomplete or expired") {
		t.Fatalf("missing archive = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	stream, err = source.StreamArchive(ctx, "running", true, func(context.Context) (bool, error) { return false, nil })
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	defer stream.Close()
	if _, err := io.ReadAll(stream); err == nil {
		t.Fatal("cancelled stream succeeded")
	}
}

func TestCancelledArchiveFollowsCleanupOutput(t *testing.T) {
	handler, job, store, kubernetes := testCancelledArchive(t)
	listed := make(chan struct{})
	notify := sync.OnceFunc(func() { close(listed) })
	kubernetes.PrependReactor("list", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		notify()
		return false, nil, nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	stream, err := handler.openJobLogs(ctx, job, true, func() {})
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	type result struct {
		data []byte
		err  error
	}
	done := make(chan result, 1)
	go func() { data, err := io.ReadAll(stream); done <- result{data: data, err: err} }()
	select {
	case <-listed:
	case result := <-done:
		t.Fatalf("stream ended before cleanup: %q, %v", result.data, result.err)
	case <-ctx.Done():
		t.Fatal("cancelled runner was not checked")
	}
	if err := store.Append(string(job.UID), joblog.Chunk{Sequence: 1, Data: []byte("cleanup output\n"), Complete: true}); err != nil {
		t.Fatal(err)
	}
	select {
	case result := <-done:
		if string(result.data) != "before cancellation\ncleanup output\n" || result.err != nil {
			t.Fatalf("cleanup archive = %q, %v", result.data, result.err)
		}
	case <-ctx.Done():
		t.Fatal("cleanup output was not followed")
	}
}

func TestCancelledArchiveSnapshotWhileRunnerActive(t *testing.T) {
	handler, job, _, _ := testCancelledArchive(t)
	stream, err := handler.openJobLogs(context.Background(), job, false, func() {})
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	data, err := io.ReadAll(stream)
	if string(data) != "before cancellation\n" || err != nil {
		t.Fatalf("snapshot = %q, %v", data, err)
	}
}

func TestCancelledArchiveReportsIncompleteAfterUploadsStop(t *testing.T) {
	for _, state := range []string{"Pod gone", "runner terminated", "cleanup deadline elapsed"} {
		t.Run(state, func(t *testing.T) {
			handler, job, _, kubernetes := testCancelledArchive(t)
			switch state {
			case "Pod gone":
				if err := kubernetes.CoreV1().Pods(job.Namespace).Delete(context.Background(), "runner", metav1.DeleteOptions{}); err != nil {
					t.Fatal(err)
				}
			case "runner terminated":
				pod, err := kubernetes.CoreV1().Pods(job.Namespace).Get(context.Background(), "runner", metav1.GetOptions{})
				if err != nil {
					t.Fatal(err)
				}
				pod.Status.ContainerStatuses[0].State = corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{}}
				if _, err := kubernetes.CoreV1().Pods(job.Namespace).UpdateStatus(context.Background(), pod, metav1.UpdateOptions{}); err != nil {
					t.Fatal(err)
				}
			case "cleanup deadline elapsed":
				completed := metav1.NewTime(time.Now().Add(-cancelledArchiveWait - time.Minute))
				job.Status.CompletionTime = &completed
				if err := handler.client.Update(context.Background(), job); err != nil {
					t.Fatal(err)
				}
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			stream, err := handler.openJobLogs(ctx, job, true, func() {})
			if err != nil {
				t.Fatal(err)
			}
			defer stream.Close()
			data, err := io.ReadAll(stream)
			if string(data) != "before cancellation\n" || !errors.Is(err, errArchiveIncomplete) {
				t.Fatalf("incomplete archive = %q, %v", data, err)
			}
		})
	}
}

func testCancelledArchive(t *testing.T) (*Handler, *actionsv1alpha1.WorkflowJob, *joblog.Store, *fake.Clientset) {
	t.Helper()
	handler := newTestHandler(t, false)
	job := &actionsv1alpha1.WorkflowJob{}
	if err := handler.client.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "build"}, job); err != nil {
		t.Fatal(err)
	}
	job.Annotations = map[string]string{joblog.ArchiveAnnotation: "true"}
	job.Status.Result = actionsv1alpha1.WorkflowJobResultCancelled
	completed := metav1.Now()
	job.Status.CompletionTime = &completed
	job.Status.Conditions = []metav1.Condition{{Type: actionsv1alpha1.WorkflowJobConditionSucceeded, Status: metav1.ConditionFalse, Reason: "JobCancelled"}}
	if err := handler.client.Update(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	store, err := joblog.NewStore(t.TempDir(), time.Hour, 1<<20, joblog.DefaultMaxChunks)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Append(string(job.UID), joblog.Chunk{Data: []byte("before cancellation\n")}); err != nil {
		t.Fatal(err)
	}
	source, kubernetes := testArchiveLogSource(t, store)
	handler.logs = source
	_, err = kubernetes.CoreV1().Pods(job.Namespace).Create(context.Background(), &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "runner", Namespace: job.Namespace, Labels: map[string]string{actionsv1alpha1.LabelWorkflowJobUID: string(job.UID)}},
		Status: corev1.PodStatus{Phase: corev1.PodRunning, ContainerStatuses: []corev1.ContainerStatus{{
			Name: runner.ContainerName, State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
		}}},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return handler, job, store, kubernetes
}

func TestConsoleReadsTimedOutJobLogsWithoutPod(t *testing.T) {
	for _, test := range []struct {
		name     string
		complete bool
		output   bool
	}{
		{name: "runner finalized after timeout", complete: true, output: true},
		{name: "Pod terminated before final upload", output: true},
		{name: "Pod terminated before first upload"},
	} {
		t.Run(test.name, func(t *testing.T) {
			handler := newTestHandler(t, false)
			job := &actionsv1alpha1.WorkflowJob{}
			if err := handler.client.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "build"}, job); err != nil {
				t.Fatal(err)
			}
			job.Annotations = map[string]string{joblog.ArchiveAnnotation: "true"}
			job.Status.Result = actionsv1alpha1.WorkflowJobResultFailure
			job.Status.Conditions = []metav1.Condition{{Type: actionsv1alpha1.WorkflowJobConditionSucceeded, Status: metav1.ConditionFalse, Reason: "JobTimedOut"}}
			if err := handler.client.Update(context.Background(), job); err != nil {
				t.Fatal(err)
			}
			store, err := joblog.NewStore(t.TempDir(), time.Hour, 1<<20, joblog.DefaultMaxChunks)
			if err != nil {
				t.Fatal(err)
			}
			var output string
			if test.output {
				output = "2026-08-10T12:34:56Z before timeout\n2026-08-10T12:34:57Z last buffered line"
				if err := store.Append(string(job.UID), joblog.Chunk{Data: []byte(output), Complete: test.complete}); err != nil {
					t.Fatal(err)
				}
			}
			source, kubernetes := testArchiveLogSource(t, store)
			handler.logs = source

			page := httptest.NewRecorder()
			handler.ServeHTTP(page, httptest.NewRequest(http.MethodGet, "/runs/default/ci/jobs/build", nil))
			if page.Code != http.StatusOK || !strings.Contains(page.Body.String(), ">Timed out<") {
				t.Fatalf("job page = %d, %s", page.Code, page.Body.String())
			}
			for _, resume := range []bool{false, true} {
				request := httptest.NewRequest(http.MethodGet, "/runs/default/ci/jobs/build/stream", nil)
				ctx, cancel := context.WithTimeout(request.Context(), 5*time.Second)
				defer cancel()
				request = request.WithContext(ctx)
				if resume {
					request.Header.Set("Last-Event-ID", "1")
				}
				response := httptest.NewRecorder()
				handler.ServeHTTP(response, request)
				if ctx.Err() != nil {
					t.Fatal("timed-out job log stream did not terminate")
				}
				var texts []string
				for _, batch := range parseTestLogBatches(t, response.Body.String()) {
					for _, entry := range batch.entries {
						texts = append(texts, entry.Text)
					}
				}
				want := ""
				if test.output {
					want = "last buffered line"
					if !resume {
						want = "before timeout\n" + want
					}
				}
				if strings.Join(texts, "\n") != want {
					t.Fatalf("displayed logs = %q, want %q", texts, want)
				}
				body := response.Body.String()
				if test.complete {
					if !strings.Contains(body, "event: end\n") || strings.Contains(body, "event: error\n") {
						t.Fatalf("finalized timeout stream = %s", body)
					}
				} else if !strings.Contains(body, "incomplete or expired") || strings.Contains(body, "event: end\n") {
					t.Fatalf("unfinished timeout stream = %s", body)
				}
			}
			wantDownload := ""
			if test.output {
				wantDownload = "before timeout\nlast buffered line"
			}
			for _, follow := range []string{"false", "true"} {
				response := httptest.NewRecorder()
				handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/runs/default/ci/jobs/build/logs?follow="+follow, nil))
				download, failure, ended := parseTestDownload(t, response)
				if response.Code != http.StatusOK || download != wantDownload {
					t.Fatalf("download = %d, %q", response.Code, download)
				}
				if test.complete && (failure != "" || !ended) || !test.complete && (!strings.Contains(failure, "incomplete or expired") || ended) {
					t.Fatalf("download error = %q, ended %v", failure, ended)
				}
			}
			if len(kubernetes.Actions()) != 0 {
				t.Fatalf("timed-out job logs accessed Pods: %#v", kubernetes.Actions())
			}
		})
	}
}

func TestArchivedLogReaderDrainsUploadBeforeTerminalStatus(t *testing.T) {
	for _, complete := range []bool{false, true} {
		t.Run(fmt.Sprint(complete), func(t *testing.T) {
			store, err := joblog.NewStore(t.TempDir(), time.Hour, 1<<20, joblog.DefaultMaxChunks)
			if err != nil {
				t.Fatal(err)
			}
			if err := store.Append("job", joblog.Chunk{Data: []byte("before timeout\n")}); err != nil {
				t.Fatal(err)
			}
			source, _ := testArchiveLogSource(t, store)
			published := false
			stream, err := source.StreamArchive(context.Background(), "job", true, func(context.Context) (bool, error) {
				if !published {
					published = true
					if err := store.Append("job", joblog.Chunk{Sequence: 1, Data: []byte("last upload"), Complete: complete}); err != nil {
						return false, err
					}
				}
				return true, nil
			})
			if err != nil {
				t.Fatal(err)
			}
			defer stream.Close()
			data, err := io.ReadAll(stream)
			if string(data) != "before timeout\nlast upload" {
				t.Fatalf("archived output = %q", data)
			}
			if complete && err != nil || !complete && (err == nil || !strings.Contains(err.Error(), "incomplete or expired")) {
				t.Fatalf("archive read error = %v", err)
			}
		})
	}
}

func testArchiveLogSource(t *testing.T, store *joblog.Store) (*kubernetesLogSource, *fake.Clientset) {
	t.Helper()
	artifacts, err := artifact.NewStore(t.TempDir(), artifact.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	tokens, err := joblog.NewReadTokenCodec(bytes.Repeat([]byte("r"), 32))
	if err != nil {
		t.Fatal(err)
	}
	service, err := artifact.NewServer(artifact.ServerConfig{
		Address: ":0", PublicURL: "http://artifacts.example", Store: artifacts, Logs: store,
		Tokens: testArchiveTokens(t), LogReads: tokens, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(service.Handler())
	t.Cleanup(server.Close)
	archive, err := joblog.NewClient(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	kubernetes := fake.NewClientset()
	return &kubernetesLogSource{client: kubernetes, archive: archive, tokens: tokens}, kubernetes
}

func TestConsoleQueuedLogDownload(t *testing.T) {
	handler := newTestHandler(t, false)
	handler.logs = NewKubernetesLogSource(fake.NewClientset())
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/runs/default/ci/jobs/build/logs", nil))
	if response.Code != http.StatusConflict || !strings.Contains(response.Body.String(), "use --follow") {
		t.Fatalf("queued snapshot = %d, %q", response.Code, response.Body.String())
	}

	server := httptest.NewServer(handler)
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+"/runs/default/ci/jobs/build/logs?follow=true", nil)
	if err != nil {
		t.Fatal(err)
	}
	transport := &http.Transport{ResponseHeaderTimeout: 5 * time.Second}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport}
	followed, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer followed.Body.Close()
	if followed.StatusCode != http.StatusOK {
		t.Fatalf("queued follow = %d", followed.StatusCode)
	}
	cancel()
}

func TestConsoleHidesInternalArchiveErrors(t *testing.T) {
	handler := newTestHandler(t, false)
	job := &actionsv1alpha1.WorkflowJob{}
	if err := handler.client.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "build"}, job); err != nil {
		t.Fatal(err)
	}
	job.Annotations = map[string]string{joblog.ArchiveAnnotation: "true"}
	if err := handler.client.Update(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	archive := httptest.NewServer(http.NotFoundHandler())
	archive.Close()
	archiveClient, err := joblog.NewClient(archive.URL, archive.Client())
	if err != nil {
		t.Fatal(err)
	}
	tokens, err := joblog.NewReadTokenCodec(bytes.Repeat([]byte("r"), 32))
	if err != nil {
		t.Fatal(err)
	}
	handler.logs = &kubernetesLogSource{archive: archiveClient, tokens: tokens}
	var logged bytes.Buffer
	handler.logger = slog.New(slog.NewJSONHandler(&logged, nil))
	for _, path := range []string{"stream", "logs?follow=true"} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/runs/default/ci/jobs/build/"+path, nil))
		message := response.Body.String()
		if strings.Contains(message, archive.URL) || strings.Contains(message, "127.0.0.1") || !strings.Contains(message, "Runner logs are unavailable") {
			t.Fatalf("public error = %q", message)
		}
	}
	if !strings.Contains(logged.String(), archive.URL) {
		t.Fatalf("missing server diagnostics: %s", logged.String())
	}
}

func testArchiveTokens(t *testing.T) *artifact.TokenCodec {
	t.Helper()
	tokens, err := artifact.NewTokenCodec(bytes.Repeat([]byte("k"), 32))
	if err != nil {
		t.Fatal(err)
	}
	return tokens
}

func parseTestDownload(t *testing.T, response *httptest.ResponseRecorder) (string, string, bool) {
	t.Helper()
	decoder := json.NewDecoder(response.Body)
	var output strings.Builder
	var failure string
	ended := false
	for {
		var record joblog.DownloadRecord
		if err := decoder.Decode(&record); err != nil {
			if err != io.EOF {
				t.Fatal(err)
			}
			break
		}
		output.Write(record.Data)
		if record.Error != "" {
			failure = record.Error
		}
		ended = ended || record.End
	}
	return output.String(), failure, ended
}

func TestConsoleDownloadsPodLogsWithoutArchiving(t *testing.T) {
	for _, follow := range []bool{false, true} {
		t.Run(fmt.Sprint(follow), func(t *testing.T) {
			var requests atomic.Int32
			backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch r.URL.Path {
				case "/api/v1/namespaces/default/pods":
					if r.URL.Query().Get("labelSelector") != "actions.kelos.dev/workflow-job-uid=job-uid" {
						t.Errorf("selector = %s", r.URL)
					}
					_ = json.NewEncoder(w).Encode(corev1.PodList{Items: []corev1.Pod{{ObjectMeta: metav1.ObjectMeta{Name: "runner", Namespace: "default"}}}})
				case "/api/v1/namespaces/default/pods/runner/log":
					requests.Add(1)
					if (r.URL.Query().Get("follow") == "true") != follow || r.URL.Query().Get("timestamps") != "true" || r.URL.Query().Get("container") != "runner" {
						t.Errorf("Pod log options = %s", r.URL)
					}
					io.WriteString(w, "2026-08-10T12:34:56Z retained Pod output\n")
				default:
					t.Errorf("unexpected Kubernetes request: %s", r.URL)
					http.NotFound(w, r)
				}
			}))
			defer backend.Close()
			kubernetes, err := kubernetes.NewForConfig(&rest.Config{Host: backend.URL})
			if err != nil {
				t.Fatal(err)
			}
			handler := newTestHandler(t, false)
			handler.logs = NewKubernetesLogSource(kubernetes)
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/runs/default/ci/jobs/build/logs?follow="+fmt.Sprint(follow), nil))
			output, failure, ended := parseTestDownload(t, response)
			if response.Code != http.StatusOK || output != "retained Pod output\n" || failure != "" || !ended || requests.Load() != 1 {
				t.Fatalf("Pod download = %d, %q, error %q, ended %v, requests %d", response.Code, output, failure, ended, requests.Load())
			}
		})
	}
}

func TestConsoleDownloadHeartbeatsDuringQuietPeriods(t *testing.T) {
	for _, archived := range []bool{false, true} {
		t.Run(fmt.Sprint(archived), func(t *testing.T) {
			t.Parallel()
			handler := newTestHandler(t, false)
			handler.logs = NewKubernetesLogSource(fake.NewClientset())
			if archived {
				job := &actionsv1alpha1.WorkflowJob{}
				if err := handler.client.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "build"}, job); err != nil {
					t.Fatal(err)
				}
				job.Annotations = map[string]string{joblog.ArchiveAnnotation: "true"}
				if err := handler.client.Update(context.Background(), job); err != nil {
					t.Fatal(err)
				}
				store, err := joblog.NewStore(t.TempDir(), time.Hour, 1<<20, joblog.DefaultMaxChunks)
				if err != nil {
					t.Fatal(err)
				}
				source, _ := testArchiveLogSource(t, store)
				handler.logs = source
			}
			server := httptest.NewServer(handler)
			defer server.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
			defer cancel()
			request, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+"/runs/default/ci/jobs/build/logs?follow=true", nil)
			if err != nil {
				t.Fatal(err)
			}
			response, err := server.Client().Do(request)
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			decoder := json.NewDecoder(response.Body)
			for count := 0; count < 2; count++ {
				var record joblog.DownloadRecord
				if err := decoder.Decode(&record); err != nil {
					t.Fatalf("missing quiet-period heartbeat: %v", err)
				}
				if len(record.Data) != 0 || record.End || record.Error != "" {
					t.Fatalf("heartbeat = %#v", record)
				}
			}
			cancel()
		})
	}
}
