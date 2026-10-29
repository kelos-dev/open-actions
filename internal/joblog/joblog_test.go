package joblog

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	actionsv1alpha1 "github.com/kelos-dev/open-actions/api/v1alpha1"
	"github.com/kelos-dev/open-actions/internal/runner"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

const first = "2026-10-29T01:00:00.123456789Z first\n"
const second = "2026-10-29T01:00:00.123456789Z second\n"
const third = "2026-10-29T01:00:01.123456789Z third\n"

func testStore(t *testing.T, maxBytes int64) *Store {
	t.Helper()
	store, err := NewStore(t.TempDir(), maxBytes, 24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func testRecord(t *testing.T, store *Store) Record {
	t.Helper()
	record, err := store.Begin(Record{Namespace: "ci", JobUID: "job-uid", PodUID: "pod-uid", CreatedAt: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	return record
}

func stored(t *testing.T, store *Store) (string, Record) {
	t.Helper()
	stream, record, err := store.Open("ci", "job-uid")
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	data, err := io.ReadAll(stream)
	if err != nil {
		t.Fatal(err)
	}
	return string(data), record
}

func TestArchiveSurvivesRestartAndResumesIdenticalTimestamps(t *testing.T) {
	store := testStore(t, 1<<20)
	record := testRecord(t, store)
	collector, err := NewCollector(&testPods{}, store, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	cursor := Cursor{}
	if err := collector.copy(strings.NewReader(first+second), record, &cursor); err != nil {
		t.Fatal(err)
	}
	store, err = NewStore(store.root, 1<<20, 24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	collector.store = store
	cursor, err = store.Cursor(record)
	if err != nil || cursor.Count != 2 {
		t.Fatalf("cursor = %+v, %v", cursor, err)
	}
	if err := collector.copy(strings.NewReader(first+second+third), record, &cursor); err != nil {
		t.Fatal(err)
	}
	data, _ := stored(t, store)
	if data != first+second+third {
		t.Fatalf("resumed archive = %q", data)
	}
}

func TestArchiveRepairsInterruptedRecord(t *testing.T) {
	store := testStore(t, 1<<20)
	record := testRecord(t, store)
	if err := store.Append(record, []byte(first+"2026-10-29T01:00:01Z part")); err != nil {
		t.Fatal(err)
	}
	cursor, err := store.Cursor(record)
	if err != nil || cursor.Count != 1 {
		t.Fatalf("cursor = %+v, %v", cursor, err)
	}
	data, _ := stored(t, store)
	if data != first {
		t.Fatalf("archive contains interrupted record: %q", data)
	}
}

func TestArchiveByteLimitAndExpiry(t *testing.T) {
	store := testStore(t, int64(len(first)))
	now := time.Now()
	store.now = func() time.Time { return now }
	record := testRecord(t, store)
	if err := store.Append(record, []byte(first)); err != nil {
		t.Fatal(err)
	}
	if err := store.Append(record, []byte(second)); !errors.Is(err, ErrLimit) {
		t.Fatalf("append error = %v", err)
	}
	if err := store.Finish(record, false); err != nil {
		t.Fatal(err)
	}
	data, status := stored(t, store)
	if data != first || !status.Finished || status.Complete {
		t.Fatalf("limited archive = %q, %+v", data, status)
	}
	now = now.Add(25 * time.Hour)
	if _, _, err := store.Open("ci", "job-uid"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expired archive error = %v", err)
	}
	if err := store.Cleanup(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(store.path(record) + ".log"); !os.IsNotExist(err) {
		t.Fatalf("expired log remains: %v", err)
	}
	if _, err := os.Stat(filepath.Dir(store.path(record))); !os.IsNotExist(err) {
		t.Fatalf("expired job directory remains: %v", err)
	}
}

func TestArchiveScopesByNamespaceAndJobUID(t *testing.T) {
	store := testStore(t, 1<<20)
	record := testRecord(t, store)
	if err := store.Append(record, []byte(first)); err != nil {
		t.Fatal(err)
	}
	for _, identity := range [][2]string{{"other", "job-uid"}, {"ci", "rerun-job-uid"}, {"..", "job-uid"}, {"ci", "../../job-uid"}} {
		if _, _, err := store.Open(identity[0], identity[1]); !errors.Is(err, ErrNotFound) {
			t.Fatalf("Open(%q) = %v", identity, err)
		}
	}
}

type testPods struct {
	pod     *corev1.Pod
	stream  io.ReadCloser
	gets    int
	since   *metav1.Time
	deleted bool
}

func (s *testPods) List(context.Context) (*corev1.PodList, error) {
	return &corev1.PodList{Items: []corev1.Pod{*s.pod}}, nil
}
func (s *testPods) Get(context.Context, string, string) (*corev1.Pod, error) {
	s.gets++
	if s.deleted {
		return nil, apierrors.NewNotFound(schema.GroupResource{Resource: "pods"}, "runner")
	}
	return s.pod, nil
}
func (s *testPods) Logs(_ context.Context, _ *corev1.Pod, since *metav1.Time) (io.ReadCloser, error) {
	s.since = since
	return s.stream, nil
}

type deleteAfterRead struct {
	reader io.Reader
	source *testPods
}

func (s *deleteAfterRead) Read(data []byte) (int, error) {
	n, err := s.reader.Read(data)
	if err == io.EOF {
		s.source.deleted = true
	}
	return n, err
}
func (*deleteAfterRead) Close() error { return nil }

func runnerPod() *corev1.Pod {
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "runner", Namespace: "ci", UID: "pod-uid", Labels: map[string]string{actionsv1alpha1.LabelWorkflowJobUID: "job-uid"}}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: runner.ContainerName}}}, Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{Name: runner.ContainerName, State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}}}}
}

func TestCollectorPreservesLogsWhenPodIsDeletedDuringStream(t *testing.T) {
	store := testStore(t, 1<<20)
	source := &testPods{pod: runnerPod()}
	source.stream = &deleteAfterRead{reader: strings.NewReader(first + second), source: source}
	collector, err := NewCollector(source, store, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	if err := collector.collect(context.Background(), source.pod); err != nil {
		t.Fatal(err)
	}
	data, record := stored(t, store)
	if data != first+second || !record.Finished || record.Complete {
		t.Fatalf("deleted Pod archive = %q, %+v", data, record)
	}
}

func TestCollectorCapturesCompletedRunnerLogs(t *testing.T) {
	store := testStore(t, 1<<20)
	pod := runnerPod()
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: runner.ContainerName, State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{}}}}
	source := &testPods{pod: pod, stream: io.NopCloser(strings.NewReader(first + second))}
	collector, err := NewCollector(source, store, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	if err := collector.collect(context.Background(), pod); err != nil {
		t.Fatal(err)
	}
	data, record := stored(t, store)
	if data != first+second || !record.Complete {
		t.Fatalf("completed archive = %q, %+v", data, record)
	}
	if err := collector.collect(context.Background(), pod); err != nil || source.gets != 2 {
		t.Fatalf("finished Pod recollected: gets=%d, err=%v", source.gets, err)
	}
}

func TestCollectorStopsAtSizeLimit(t *testing.T) {
	store := testStore(t, 1)
	source := &testPods{pod: runnerPod(), stream: io.NopCloser(strings.NewReader(first))}
	collector, _ := NewCollector(source, store, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err := collector.collect(context.Background(), source.pod); err != nil {
		t.Fatal(err)
	}
	_, record := stored(t, store)
	if !record.Finished || record.Complete {
		t.Fatalf("size-limited record = %+v", record)
	}
}

func TestArchiveHTTPAuthenticationAndFormatting(t *testing.T) {
	store := testStore(t, 1<<20)
	record := testRecord(t, store)
	if err := store.Append(record, []byte(first)); err != nil {
		t.Fatal(err)
	}
	handler, err := NewHandler(store, "read-token")
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	defer server.Close()
	for _, test := range []struct {
		name, token, namespace, job string
		timestamps                  bool
		want                        string
		wantErr                     bool
	}{
		{name: "plain", token: "read-token", namespace: "ci", job: "job-uid", want: "first\n" + IncompleteWarning},
		{name: "timestamped", token: "read-token", namespace: "ci", job: "job-uid", timestamps: true, want: first + IncompleteWarning},
		{name: "unauthenticated", token: "wrong-token", namespace: "ci", job: "job-uid", wantErr: true},
		{name: "different namespace", token: "read-token", namespace: "other", job: "job-uid", wantErr: true},
		{name: "different job", token: "read-token", namespace: "ci", job: "different-job", wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			client, err := NewClient(server.URL, test.token, server.Client())
			if err != nil {
				t.Fatal(err)
			}
			stream, err := client.Open(context.Background(), test.namespace, test.job, test.timestamps)
			if test.wantErr {
				if err == nil {
					stream.Close()
					t.Fatal("unauthorized or missing archive was returned")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			defer stream.Close()
			body, err := io.ReadAll(stream)
			if err != nil || string(body) != test.want {
				t.Fatalf("body = %q, %v", body, err)
			}
		})
	}
	if err := store.Finish(record, true); err != nil {
		t.Fatal(err)
	}
	client, _ := NewClient(server.URL, "read-token", server.Client())
	stream, err := client.Open(context.Background(), "ci", "job-uid", false)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	body, _ := io.ReadAll(stream)
	if string(body) != "first\n" {
		t.Fatalf("complete archive = %q", body)
	}
}

func TestArchiveClientRejectsRedirect(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("archive client followed redirect") }))
	defer target.Close()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, target.URL, http.StatusFound) }))
	defer server.Close()
	client, _ := NewClient(server.URL, "read-token", server.Client())
	if _, err := client.Open(context.Background(), "ci", "job-uid", false); err == nil {
		t.Fatal("redirect accepted")
	}
}

func TestArchiveRetainsCompleteRecordsAtByteLimit(t *testing.T) {
	store := testStore(t, int64(len(first)+len(second)-1))
	record := testRecord(t, store)
	if err := store.Append(record, []byte(first+second)); !errors.Is(err, ErrLimit) {
		t.Fatalf("append error = %v", err)
	}
	data, _ := stored(t, store)
	if data != first {
		t.Fatalf("limited archive = %q", data)
	}
}

func TestCollectorRunDiscoversPodsAndStops(t *testing.T) {
	store := testStore(t, 1<<20)
	pod := runnerPod()
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: runner.ContainerName, State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{}}}}
	source := &testPods{pod: pod, stream: io.NopCloser(strings.NewReader(first))}
	collector, err := NewCollector(source, store, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- collector.Run(ctx) }()
	defer func() {
		cancel()
		if err := <-done; err != nil {
			t.Error(err)
		}
	}()
	timeout := time.NewTimer(5 * time.Second)
	defer timeout.Stop()
	poll := time.NewTicker(5 * time.Millisecond)
	defer poll.Stop()
	for {
		select {
		case <-timeout.C:
			t.Fatal("collector did not discover runner Pod")
		case <-poll.C:
			stream, record, err := store.Open("ci", "job-uid")
			if err == nil {
				data, readErr := io.ReadAll(stream)
				stream.Close()
				if readErr != nil {
					t.Fatal(readErr)
				}
				if record.Complete {
					if string(data) != first {
						t.Fatalf("collected logs = %q", data)
					}
					return
				}
			}
		}
	}
}
