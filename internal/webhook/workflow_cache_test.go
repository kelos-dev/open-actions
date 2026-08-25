package webhook

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

func TestWorkflowRevisionCacheCoalescesConcurrentLoads(t *testing.T) {
	cache := newWorkflowRevisionCache(16, 1024)
	key := workflowRevisionKey{ProjectUID: "project-uid", RepositoryID: 1, WorkflowDirectory: ".open-actions/workflows", Revision: "revision"}
	want := workflowRevision{Paths: []string{".open-actions/workflows/ci.yaml"}, Size: 32}
	var loads atomic.Int32
	const workers = 16
	var ready sync.WaitGroup
	var complete sync.WaitGroup
	ready.Add(workers)
	complete.Add(workers)
	start := make(chan struct{})
	errors := make(chan error, workers)
	for range workers {
		go func() {
			defer complete.Done()
			ready.Done()
			<-start
			got, err := cache.load(context.Background(), key, func(context.Context) (workflowRevision, error) {
				loads.Add(1)
				return want, nil
			})
			if err != nil {
				errors <- err
				return
			}
			if len(got.Paths) != 1 || got.Paths[0] != want.Paths[0] {
				errors <- fmt.Errorf("workflow revision paths = %v", got.Paths)
			}
		}()
	}
	ready.Wait()
	close(start)
	complete.Wait()
	close(errors)
	for err := range errors {
		t.Fatal(err)
	}
	if loads.Load() != 1 {
		t.Fatalf("workflow revision loads = %d, want 1", loads.Load())
	}
}

func TestWorkflowRevisionCacheBoundsEntriesAndBytes(t *testing.T) {
	cache := newWorkflowRevisionCache(2, 10)
	loads := map[string]int{}
	load := func(revision string, size int) {
		t.Helper()
		key := workflowRevisionKey{ProjectUID: "project-uid", RepositoryID: 1, WorkflowDirectory: ".open-actions/workflows", Revision: revision}
		if _, err := cache.load(context.Background(), key, func(context.Context) (workflowRevision, error) {
			loads[revision]++
			return workflowRevision{Size: size}, nil
		}); err != nil {
			t.Fatal(err)
		}
	}

	load("a", 6)
	load("b", 6)
	load("a", 6)
	load("oversized", 11)
	load("oversized", 11)

	if loads["a"] != 2 {
		t.Fatalf("evicted revision loads = %d, want 2", loads["a"])
	}
	if loads["oversized"] != 2 {
		t.Fatalf("oversized revision loads = %d, want 2", loads["oversized"])
	}
	cache.mutex.Lock()
	defer cache.mutex.Unlock()
	if len(cache.entries) > 2 || cache.bytes > 10 {
		t.Fatalf("workflow revision cache contains %d entries and %d bytes", len(cache.entries), cache.bytes)
	}
}

func TestWorkflowRevisionCacheDoesNotCacheLoadFailures(t *testing.T) {
	cache := newWorkflowRevisionCache(2, 1024)
	key := workflowRevisionKey{ProjectUID: "project-uid", RepositoryID: 1, WorkflowDirectory: ".open-actions/workflows", Revision: "revision"}
	loads := 0
	loader := func(context.Context) (workflowRevision, error) {
		loads++
		if loads == 1 {
			return workflowRevision{}, errors.New("temporary failure")
		}
		return workflowRevision{Size: 1}, nil
	}
	if _, err := cache.load(context.Background(), key, loader); err == nil {
		t.Fatal("workflow revision load succeeded during a temporary failure")
	}
	if _, err := cache.load(context.Background(), key, loader); err != nil {
		t.Fatal(err)
	}
	if loads != 2 {
		t.Fatalf("workflow revision loads = %d, want 2", loads)
	}
}
