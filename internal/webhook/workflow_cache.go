package webhook

import (
	"container/list"
	"context"
	"sync"

	"github.com/kelos-dev/open-actions/internal/workflow"
)

const (
	maxCachedWorkflowRevisions = 256
	maxCachedWorkflowBytes     = 16 << 20
)

type workflowRevisionKey struct {
	ProjectUID        string
	RepositoryID      int64
	WorkflowDirectory string
	Revision          string
}

type revisionWorkflow struct {
	Path       string
	Definition *workflow.Definition
}

type workflowRevision struct {
	Paths        []string
	Workflows    []revisionWorkflow
	InvalidPath  string
	InvalidError string
	Size         int
}

type workflowRevisionCache struct {
	mutex      sync.Mutex
	maxEntries int
	maxBytes   int
	bytes      int
	entries    map[workflowRevisionKey]*list.Element
	pending    map[workflowRevisionKey]*workflowRevisionRequest
	recent     list.List
}

type workflowRevisionCacheEntry struct {
	key      workflowRevisionKey
	revision workflowRevision
}

type workflowRevisionRequest struct {
	done     chan struct{}
	revision workflowRevision
	err      error
}

func newWorkflowRevisionCache(maxEntries, maxBytes int) *workflowRevisionCache {
	return &workflowRevisionCache{
		maxEntries: maxEntries,
		maxBytes:   maxBytes,
		entries:    make(map[workflowRevisionKey]*list.Element),
		pending:    make(map[workflowRevisionKey]*workflowRevisionRequest),
	}
}

func (c *workflowRevisionCache) load(ctx context.Context, key workflowRevisionKey, loader func(context.Context) (workflowRevision, error)) (workflowRevision, error) {
	c.mutex.Lock()
	if element := c.entries[key]; element != nil {
		c.recent.MoveToFront(element)
		revision := element.Value.(*workflowRevisionCacheEntry).revision
		c.mutex.Unlock()
		return revision, nil
	}
	if request := c.pending[key]; request != nil {
		c.mutex.Unlock()
		select {
		case <-ctx.Done():
			return workflowRevision{}, ctx.Err()
		case <-request.done:
			return request.revision, request.err
		}
	}
	request := &workflowRevisionRequest{done: make(chan struct{})}
	c.pending[key] = request
	c.mutex.Unlock()

	revision, err := loader(ctx)

	c.mutex.Lock()
	request.revision = revision
	request.err = err
	delete(c.pending, key)
	if err == nil {
		c.store(key, revision)
	}
	close(request.done)
	c.mutex.Unlock()
	return revision, err
}

func (c *workflowRevisionCache) store(key workflowRevisionKey, revision workflowRevision) {
	if c.maxEntries < 1 || c.maxBytes < 1 || revision.Size > c.maxBytes {
		return
	}
	entry := &workflowRevisionCacheEntry{key: key, revision: revision}
	c.entries[key] = c.recent.PushFront(entry)
	c.bytes += revision.Size
	for len(c.entries) > c.maxEntries || c.bytes > c.maxBytes {
		element := c.recent.Back()
		if element == nil {
			return
		}
		cached := element.Value.(*workflowRevisionCacheEntry)
		delete(c.entries, cached.key)
		c.bytes -= cached.revision.Size
		c.recent.Remove(element)
	}
}
