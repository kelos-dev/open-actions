// Package joblog persists ordered runner output independently of container logs.
package joblog

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const (
	Path              = "/logs/"
	URLEnvVar         = "OPEN_ACTIONS_LOG_URL"
	ArchiveAnnotation = "actions.kelos.dev/log-archive"
	MaxChunkBytes     = 256 << 10
	DefaultMaxChunks  = 32768
)

var (
	ErrNotFound = errors.New("log chunk is unavailable")
	ErrConflict = errors.New("log chunk conflicts with persisted output")
	ErrLimit    = errors.New("job log storage limit exceeded")
)

// Chunk is an immutable segment of a job attempt's output.
type Chunk struct {
	Sequence uint64 `json:"sequence"`
	Data     []byte `json:"data,omitempty"`
	Complete bool   `json:"complete,omitempty"`
}

type record struct {
	Chunk
	Bytes     int64     `json:"bytes"`
	ExpiresAt time.Time `json:"expiresAt"`
}

type jobState struct {
	sync.Mutex
	users  int
	latest *record
}

type Store struct {
	root      string
	retention time.Duration
	maxBytes  int64
	maxChunks int64
	mu        sync.Mutex
	now       func() time.Time
	jobs      map[string]*jobState
}

func NewStore(root string, retention time.Duration, maxBytes, maxChunks int64) (*Store, error) {
	if root == "" || retention <= 0 || maxBytes < 1 || maxChunks < 1 {
		return nil, errors.New("log storage directory, retention, byte limit, and chunk limit are required")
	}
	if err := os.MkdirAll(root, 0o700); err != nil {
		return nil, err
	}
	return &Store{root: root, retention: retention, maxBytes: maxBytes, maxChunks: maxChunks, now: time.Now, jobs: map[string]*jobState{}}, nil
}

func (s *Store) directory(job string) string {
	digest := sha256.Sum256([]byte(job))
	return filepath.Join(s.root, hex.EncodeToString(digest[:]))
}

// lockJob keeps disk operations for different jobs independent. References
// include waiters so cleanup cannot replace a lock that another request uses.
func (s *Store) lockJob(directory string) (*jobState, func()) {
	s.mu.Lock()
	state := s.jobs[directory]
	if state == nil {
		state = &jobState{}
		s.jobs[directory] = state
	}
	state.users++
	s.mu.Unlock()
	state.Lock()
	return state, func() {
		state.Unlock()
		s.mu.Lock()
		state.users--
		if state.users == 0 && state.latest == nil {
			delete(s.jobs, directory)
		}
		s.mu.Unlock()
	}
}

func chunkPath(directory string, sequence uint64) string {
	return filepath.Join(directory, fmt.Sprintf("%020d.json", sequence))
}

func load(path string) (record, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return record{}, ErrNotFound
	}
	if err != nil {
		return record{}, err
	}
	var value record
	err = json.Unmarshal(data, &value)
	return value, err
}

func (s *Store) Append(job string, chunk Chunk) error {
	if job == "" || len(chunk.Data) > MaxChunkBytes || len(chunk.Data) == 0 && !chunk.Complete {
		return errors.New("log chunk is invalid")
	}
	directory := s.directory(job)
	state, unlock := s.lockJob(directory)
	defer unlock()
	last, found, err := state.last(directory)
	if err != nil {
		return err
	}
	expected := uint64(0)
	if found {
		if !s.now().Before(last.ExpiresAt) {
			return ErrNotFound
		}
		expected = last.Sequence + 1
	}
	if chunk.Sequence < expected {
		stored, err := load(chunkPath(directory, chunk.Sequence))
		if err != nil {
			return err
		}
		if stored.Complete != chunk.Complete || !bytes.Equal(stored.Data, chunk.Data) {
			return ErrConflict
		}
		if err := syncDirectory(directory); err != nil {
			return err
		}
		if chunk.Sequence == 0 {
			return syncDirectory(s.root)
		}
		return nil
	}
	if chunk.Sequence != expected || last.Complete {
		return ErrConflict
	}
	if chunk.Sequence >= uint64(s.maxChunks) || int64(len(chunk.Data)) > s.maxBytes-last.Bytes {
		return ErrLimit
	}
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return err
	}
	value := record{Chunk: chunk, Bytes: last.Bytes + int64(len(chunk.Data)), ExpiresAt: s.now().Add(s.retention)}
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	file, err := os.CreateTemp(directory, ".chunk-")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	_, writeErr := file.Write(data)
	syncErr := file.Sync()
	closeErr := file.Close()
	if err := errors.Join(writeErr, syncErr, closeErr); err != nil {
		return err
	}
	if err := os.Rename(file.Name(), chunkPath(directory, chunk.Sequence)); err != nil {
		return err
	}
	if err := syncDirectory(directory); err != nil {
		state.latest = nil
		return err
	}
	if !found {
		if err := syncDirectory(s.root); err != nil {
			return err
		}
	}
	value.Data = nil
	state.latest = &value
	return nil
}

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	return errors.Join(directory.Sync(), directory.Close())
}

func (s *Store) Read(job string, sequence uint64) (Chunk, error) {
	directory := s.directory(job)
	state, unlock := s.lockJob(directory)
	defer unlock()
	value, err := load(chunkPath(directory, sequence))
	if err != nil {
		return Chunk{}, err
	}
	last, _, err := state.last(directory)
	if err != nil {
		return Chunk{}, err
	}
	if !s.now().Before(last.ExpiresAt) {
		return Chunk{}, ErrNotFound
	}
	return value.Chunk, nil
}

// last loads the final committed record once after a service restart.
func (state *jobState) last(directory string) (record, bool, error) {
	if state.latest != nil {
		return *state.latest, true, nil
	}
	entries, err := os.ReadDir(directory)
	if errors.Is(err, os.ErrNotExist) {
		return record{}, false, nil
	}
	if err != nil {
		return record{}, false, err
	}
	for index := len(entries) - 1; index >= 0; index-- {
		if !strings.HasSuffix(entries[index].Name(), ".json") {
			continue
		}
		value, err := load(filepath.Join(directory, entries[index].Name()))
		if err != nil {
			return record{}, false, err
		}
		value.Data = nil
		state.latest = &value
		return value, true, nil
	}
	return record{}, false, nil
}

func (s *Store) Cleanup() error {
	jobs, err := os.ReadDir(s.root)
	if err != nil {
		return err
	}
	for _, job := range jobs {
		if !job.IsDir() {
			continue
		}
		if err := s.cleanupJob(filepath.Join(s.root, job.Name())); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) cleanupJob(directory string) error {
	state, unlock := s.lockJob(directory)
	defer unlock()
	entries, err := os.ReadDir(directory)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".chunk-") {
			if err := os.Remove(filepath.Join(directory, entry.Name())); err != nil {
				return err
			}
		}
	}
	latest, found, err := state.last(directory)
	if err != nil {
		return err
	}
	if !found || !s.now().Before(latest.ExpiresAt) {
		if err := os.RemoveAll(directory); err != nil {
			return err
		}
		state.latest = nil
	}
	return nil
}
