package joblog

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestStorePersistsOrderedChunksAcrossRestart(t *testing.T) {
	root := t.TempDir()
	store, err := NewStore(root, time.Hour, 20<<20, DefaultMaxChunks)
	if err != nil {
		t.Fatal(err)
	}
	data := bytes.Repeat([]byte("runner output\n"), MaxChunkBytes/14)
	// This exceeds the default kubelet rotation threshold without using Pod logs.
	for sequence := uint64(0); sequence < 50; sequence++ {
		chunk := Chunk{Sequence: sequence, Data: data, Complete: sequence == 49}
		if err := store.Append("job", chunk); err != nil {
			t.Fatal(err)
		}
		if err := store.Append("job", chunk); err != nil {
			t.Fatalf("retry: %v", err)
		}
	}
	restarted, err := NewStore(root, time.Hour, 20<<20, DefaultMaxChunks)
	if err != nil {
		t.Fatal(err)
	}
	for sequence := uint64(0); sequence < 50; sequence++ {
		chunk, err := restarted.Read("job", sequence)
		if err != nil || !bytes.Equal(chunk.Data, data) || chunk.Complete != (sequence == 49) {
			t.Fatalf("chunk %d = %#v, %v", sequence, chunk, err)
		}
	}
	if _, err := restarted.Read("other-job", 0); !errors.Is(err, ErrNotFound) {
		t.Fatalf("other job = %v", err)
	}
	if err := restarted.Append("job", Chunk{Sequence: 50, Data: []byte("late")}); !errors.Is(err, ErrConflict) {
		t.Fatalf("append after completion = %v", err)
	}
	if err := restarted.Append("job", Chunk{Sequence: 0, Data: []byte("different")}); !errors.Is(err, ErrConflict) {
		t.Fatalf("conflicting retry = %v", err)
	}
}

func TestStoreLimitsOrderingAndExpiry(t *testing.T) {
	store, err := NewStore(t.TempDir(), time.Hour, 5, DefaultMaxChunks)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	store.now = func() time.Time { return now }
	if err := store.Append("job", Chunk{Sequence: 1, Data: []byte("gap")}); !errors.Is(err, ErrConflict) {
		t.Fatalf("out of order = %v", err)
	}
	if err := store.Append("job", Chunk{Data: []byte("hello")}); err != nil {
		t.Fatal(err)
	}
	if err := store.Append("job", Chunk{Sequence: 1, Data: []byte("!")}); !errors.Is(err, ErrLimit) {
		t.Fatalf("quota = %v", err)
	}
	if err := store.Append("job", Chunk{Sequence: 1, Complete: true}); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(store.directory("job"), ".chunk-interrupted"), []byte("partial"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := store.Cleanup(); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Read("job", 0); err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Hour)
	if _, err := store.Read("job", 0); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expired read = %v", err)
	}
	if err := store.Cleanup(); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(store.root)
	if err != nil || len(entries) != 0 {
		t.Fatalf("cleanup = %v, %v", entries, err)
	}
}

func TestStoreBoundsChunkFilesAndPreservesRetries(t *testing.T) {
	root := t.TempDir()
	store, err := NewStore(root, time.Hour, 1<<20, 2)
	if err != nil {
		t.Fatal(err)
	}
	for sequence := uint64(0); sequence < 2; sequence++ {
		if err := store.Append("job", Chunk{Sequence: sequence, Data: []byte("x")}); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.Append("job", Chunk{Sequence: 2, Data: []byte("x")}); !errors.Is(err, ErrLimit) {
		t.Fatalf("chunk quota = %v", err)
	}
	files, err := os.ReadDir(store.directory("job"))
	if err != nil || len(files) != 2 {
		t.Fatalf("chunk files = %v, %v", files, err)
	}
	restarted, err := NewStore(root, time.Hour, 1<<20, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := restarted.Append("job", Chunk{Sequence: 1, Data: []byte("x")}); err != nil {
		t.Fatalf("retry after reducing limit = %v", err)
	}
	chunk, err := restarted.Read("job", 1)
	if err != nil || string(chunk.Data) != "x" {
		t.Fatalf("retained chunk = %#v, %v", chunk, err)
	}
	if err := restarted.Append("job", Chunk{Sequence: 2, Complete: true}); !errors.Is(err, ErrLimit) {
		t.Fatalf("completion chunk quota = %v", err)
	}
	if err := restarted.Append("other-job", Chunk{Data: []byte("independent"), Complete: true}); err != nil {
		t.Fatalf("other job quota = %v", err)
	}
}

func TestStoreRequiresPositiveChunkLimit(t *testing.T) {
	for _, limit := range []int64{0, -1} {
		if _, err := NewStore(t.TempDir(), time.Hour, 1<<20, limit); err == nil {
			t.Fatalf("accepted chunk limit %d", limit)
		}
	}
}

func TestStoreKeepsJobsIndependentDuringCleanup(t *testing.T) {
	store, err := NewStore(t.TempDir(), time.Hour, 1<<20, DefaultMaxChunks)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Append("blocked", Chunk{Data: []byte("busy")}); err != nil {
		t.Fatal(err)
	}
	_, unlock := store.lockJob(store.directory("blocked"))
	var release sync.Once
	defer release.Do(unlock)
	cleanup := make(chan error, 1)
	go func() { cleanup <- store.Cleanup() }()
	operation := make(chan error, 1)
	go func() {
		if err := store.Append("independent", Chunk{Data: []byte("available"), Complete: true}); err != nil {
			operation <- err
			return
		}
		chunk, err := store.Read("independent", 0)
		if err == nil && string(chunk.Data) != "available" {
			err = fmt.Errorf("unexpected output %q", chunk.Data)
		}
		operation <- err
	}()
	select {
	case err := <-operation:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("another job's lock blocked uploads and reads")
	}
	release.Do(unlock)
	select {
	case err := <-cleanup:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cleanup did not finish")
	}
}
