package github

import (
	"bytes"
	"container/list"
	"context"
	"encoding/hex"
	"sync"
)

const (
	maxCachedFiles     = 512
	maxCachedFileBytes = 16 << 20
)

type fileCacheKey struct {
	installation *InstallationClient
	path         string
	revision     string
}

type fileCacheEntry struct {
	key  fileCacheKey
	data []byte
}

type fileRequest struct {
	done chan struct{}
	data []byte
	err  error
}

type fileCache struct {
	mutex      sync.Mutex
	maxEntries int
	maxBytes   int
	bytes      int
	entries    map[fileCacheKey]*list.Element
	pending    map[fileCacheKey]*fileRequest
	recent     list.List
}

func newFileCache(maxEntries, maxBytes int) *fileCache {
	return &fileCache{
		maxEntries: maxEntries, maxBytes: maxBytes,
		entries: map[fileCacheKey]*list.Element{}, pending: map[fileCacheKey]*fileRequest{},
	}
}

func (c *fileCache) load(ctx context.Context, key fileCacheKey, loader func(context.Context) ([]byte, error)) ([]byte, error) {
	c.mutex.Lock()
	if element := c.entries[key]; element != nil {
		c.recent.MoveToFront(element)
		data := bytes.Clone(element.Value.(*fileCacheEntry).data)
		c.mutex.Unlock()
		return data, nil
	}
	if request := c.pending[key]; request != nil {
		c.mutex.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-request.done:
			return bytes.Clone(request.data), request.err
		}
	}
	request := &fileRequest{done: make(chan struct{})}
	c.pending[key] = request
	c.mutex.Unlock()

	data, err := loader(ctx)

	c.mutex.Lock()
	request.data, request.err = bytes.Clone(data), err
	delete(c.pending, key)
	if err == nil && c.maxEntries > 0 && len(data) <= c.maxBytes {
		entry := &fileCacheEntry{key: key, data: request.data}
		c.entries[key] = c.recent.PushFront(entry)
		c.bytes += len(data)
		for len(c.entries) > c.maxEntries || c.bytes > c.maxBytes {
			element := c.recent.Back()
			entry := element.Value.(*fileCacheEntry)
			delete(c.entries, entry.key)
			c.bytes -= len(entry.data)
			c.recent.Remove(element)
		}
	}
	close(request.done)
	c.mutex.Unlock()
	return data, err
}

func fullCommitSHA(revision string) bool {
	if len(revision) != 2*gitSHA1Bytes {
		return false
	}
	decoded, err := hex.DecodeString(revision)
	return err == nil && len(decoded) == gitSHA1Bytes
}

func (c *InstallationClient) cachedTokenActive() bool {
	if c.cacheKey == nil {
		return false
	}
	c.client.installationMutex.Lock()
	defer c.client.installationMutex.Unlock()
	cached, found := c.client.installationCache[*c.cacheKey]
	return found && cached.client == c && cached.expiresAt.After(c.client.now())
}
