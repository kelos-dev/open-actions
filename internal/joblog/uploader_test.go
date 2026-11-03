package joblog

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestUploaderRetriesAcknowledgementsAndBoundsSpool(t *testing.T) {
	var output bytes.Buffer
	var mu sync.Mutex
	var next uint64
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		if r.Header.Get("Authorization") != "Bearer job-token" {
			t.Error("missing upload credential")
		}
		var chunk Chunk
		if err := json.NewDecoder(r.Body).Decode(&chunk); err != nil {
			t.Error(err)
			w.WriteHeader(400)
			return
		}
		if chunk.Sequence == next {
			output.Write(chunk.Data)
			next++
		} else if chunk.Sequence != next-1 {
			t.Errorf("sequence = %d, next %d", chunk.Sequence, next)
		}
		if requests.Add(1) == 1 {
			w.WriteHeader(500)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	client, err := NewClient(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	uploader, err := NewUploader(context.Background(), context.Background(), client, "job-token", t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	want := strings.Repeat("ordered output\n", (2*maxPendingBytes)/15)
	if _, err := io.WriteString(uploader, want); err != nil {
		t.Fatal(err)
	}
	info, err := uploader.file.Stat()
	if err != nil || info.Size() > maxPendingBytes {
		t.Fatalf("spool size = %v, %v", info, err)
	}
	if err := uploader.Close(); err != nil {
		t.Fatal(err)
	}
	if err := uploader.Close(); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if output.String() != want {
		t.Fatalf("uploaded bytes = %d, want %d", output.Len(), len(want))
	}
	if _, err := os.Stat(uploader.file.Name()); !os.IsNotExist(err) {
		t.Fatalf("spool remains: %v", err)
	}
}

func TestUploaderFlushesBeforeCompletionAndReportsFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	received := make(chan Chunk, 5)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var chunk Chunk
		if err := json.NewDecoder(r.Body).Decode(&chunk); err != nil {
			t.Error(err)
		}
		received <- chunk
		if chunk.Complete {
			cancel()
			w.WriteHeader(503)
		} else {
			w.WriteHeader(http.StatusNoContent)
		}
	}))
	defer server.Close()
	client, _ := NewClient(server.URL, server.Client())
	uploader, err := NewUploader(ctx, ctx, client, "token", t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(uploader, "live output\n"); err != nil {
		t.Fatal(err)
	}
	select {
	case chunk := <-received:
		if string(chunk.Data) != "live output\n" || chunk.Complete {
			t.Fatalf("live chunk = %#v", chunk)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("output was not flushed while running")
	}
	if err := uploader.Close(); err == nil {
		t.Fatal("missing final upload error")
	}
	if n, err := uploader.Write([]byte("container output")); err != nil || n != len("container output") {
		t.Fatalf("container output after archive failure = %d, %v", n, err)
	}
}

func TestTimestampWriterPreservesFragmentedLines(t *testing.T) {
	var output bytes.Buffer
	writer := NewTimestampWriter(&output)
	for _, data := range []string{"hello", " world\nnext\n", "last"} {
		if _, err := io.WriteString(writer, data); err != nil {
			t.Fatal(err)
		}
	}
	lines := strings.Split(output.String(), "\n")
	for index, want := range []string{"hello world", "next", "last"} {
		stamp, content, _ := strings.Cut(lines[index], " ")
		if _, err := time.Parse(time.RFC3339Nano, stamp); err != nil || content != want {
			t.Fatalf("line = %q, %v", lines[index], err)
		}
	}
}

func TestUploaderRecoversAfterServiceOutage(t *testing.T) {
	var mu sync.Mutex
	var attempts atomic.Int32
	var output bytes.Buffer
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if attempts.Add(1) <= 3 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		var chunk Chunk
		if err := json.NewDecoder(r.Body).Decode(&chunk); err != nil {
			t.Error(err)
			w.WriteHeader(400)
			return
		}
		mu.Lock()
		output.Write(chunk.Data)
		mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	client, err := NewClient(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	uploader, err := NewUploader(ctx, ctx, client, "token", t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(uploader, "output during restart\n"); err != nil {
		t.Fatal(err)
	}
	if err := uploader.Close(); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if attempts.Load() < 4 || output.String() != "output during restart\n" {
		t.Fatalf("attempts = %d, output = %q", attempts.Load(), output.String())
	}
}

func TestUploaderBoundsChunkRateDuringContinuousOutput(t *testing.T) {
	var uploaded bytes.Buffer
	var mu sync.Mutex
	var chunks uint64
	var complete bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var chunk Chunk
		if err := json.NewDecoder(r.Body).Decode(&chunk); err != nil {
			t.Error(err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		mu.Lock()
		if chunk.Sequence == chunks {
			chunks++
			uploaded.Write(chunk.Data)
		} else if chunk.Sequence != chunks-1 {
			t.Errorf("sequence = %d, next %d", chunk.Sequence, chunks)
		}
		complete = complete || chunk.Complete
		mu.Unlock()
		time.Sleep(10 * time.Millisecond)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	client, err := NewClient(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	started := time.Now()
	uploader, err := NewUploader(ctx, ctx, client, "token", t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer uploader.Close()
	producerContext, stopProducer := context.WithCancel(ctx)
	defer stopProducer()
	const line = "continuous output\n"
	produced := 0
	done := make(chan error, 1)
	go func() {
		ticker := time.NewTicker(2 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-producerContext.Done():
				done <- nil
				return
			case <-ticker.C:
				if _, err := io.WriteString(uploader, line); err != nil {
					done <- err
					return
				}
				produced++
			}
		}
	}()
	time.Sleep(3200 * time.Millisecond)
	stopProducer()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if err := uploader.Close(); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if !complete || uploaded.String() != strings.Repeat(line, produced) {
		t.Fatalf("uploaded bytes = %d, produced lines = %d, complete = %v", uploaded.Len(), produced, complete)
	}
	bound := uint64(time.Since(started)/time.Second) + uint64(uploaded.Len()/MaxChunkBytes) + 2
	if chunks > bound {
		t.Fatalf("chunk count = %d, want at most %d for %d bytes", chunks, bound, uploaded.Len())
	}
}

func TestUploaderExecutionCancellationUnblocksFullSpoolWithoutBreakingOutput(t *testing.T) {
	attempted := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case attempted <- struct{}{}:
		default:
		}
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()
	client, err := NewClient(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	retryContext, stopRetry := context.WithCancel(context.Background())
	defer stopRetry()
	uploader, err := NewUploader(retryContext, ctx, client, "token", t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	data := strings.Repeat("x", 2*maxPendingBytes)
	var container bytes.Buffer
	go func() { _, err := io.WriteString(io.MultiWriter(&container, uploader), data); done <- err }()
	select {
	case <-attempted:
	case <-time.After(5 * time.Second):
		t.Fatal("no upload attempt")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("spool writer did not unblock")
	}
	stopRetry()
	if err := uploader.Close(); !errors.Is(err, context.Canceled) {
		t.Fatalf("close = %v", err)
	}
	if container.String() != data {
		t.Fatalf("container bytes = %d, want %d", container.Len(), len(data))
	}
}

func TestUploaderDrainsAcceptedOutputAfterExecutionCancellation(t *testing.T) {
	attempted := make(chan struct{})
	notify := sync.OnceFunc(func() { close(attempted) })
	resume := make(chan struct{})
	release := sync.OnceFunc(func() { close(resume) })
	var uploaded bytes.Buffer
	var mu sync.Mutex
	var next uint64
	var complete bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var chunk Chunk
		if err := json.NewDecoder(r.Body).Decode(&chunk); err != nil {
			t.Error(err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if chunk.Sequence == 0 {
			notify()
			<-resume
		}
		mu.Lock()
		if chunk.Sequence == next {
			next++
			uploaded.Write(chunk.Data)
		} else if chunk.Sequence != next-1 {
			t.Errorf("sequence = %d, want %d", chunk.Sequence, next)
		}
		complete = complete || chunk.Complete
		mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	defer release()
	client, err := NewClient(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	retryContext, stopRetry := context.WithTimeout(context.Background(), 20*time.Second)
	defer stopRetry()
	outputContext, stopOutput := context.WithCancel(context.Background())
	defer stopOutput()
	uploader, err := NewUploader(retryContext, outputContext, client, "token", t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	var container bytes.Buffer
	writer := io.MultiWriter(&container, uploader)
	data := bytes.Repeat([]byte("x"), 2*maxPendingBytes)
	done := make(chan error, 1)
	go func() { _, err := writer.Write(data); done <- err }()
	select {
	case <-attempted:
	case <-time.After(5 * time.Second):
		t.Fatal("no upload attempt")
	}
	stopOutput()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("spool writer did not unblock")
	}
	if _, err := io.WriteString(writer, "cleanup output\n"); err != nil {
		t.Fatal(err)
	}
	release()
	if err := uploader.Close(); !errors.Is(err, context.Canceled) {
		t.Fatalf("close = %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if !bytes.Equal(uploaded.Bytes(), data[:maxPendingBytes]) || complete {
		t.Fatalf("uploaded bytes = %d, complete = %v", uploaded.Len(), complete)
	}
	if !bytes.Equal(container.Bytes(), append(data, []byte("cleanup output\n")...)) {
		t.Fatalf("container bytes = %d", container.Len())
	}
}

func TestUploaderBoundsBackpressureDuringOutage(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusServiceUnavailable) }))
	defer server.Close()
	client, err := NewClient(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	uploader, err := NewUploader(context.Background(), context.Background(), client, "token", t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	uploader.maxBlocked = 100 * time.Millisecond
	done := make(chan error, 1)
	go func() { _, err := io.WriteString(uploader, strings.Repeat("x", 2*maxPendingBytes)); done <- err }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("full spool blocked output indefinitely")
	}
	if err := uploader.Close(); err == nil || !strings.Contains(err.Error(), "wait limit") {
		t.Fatalf("close error = %v", err)
	}
}
