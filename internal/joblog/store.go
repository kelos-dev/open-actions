package joblog

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
)

var ErrNotFound = errors.New("archived job logs are unavailable")
var ErrLimit = errors.New("archived job log size limit reached")
var identityPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]*$`)

type Record struct {
	Namespace string    `json:"namespace"`
	JobUID    string    `json:"jobUID"`
	PodUID    string    `json:"podUID"`
	CreatedAt time.Time `json:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt"`
	Finished  bool      `json:"finished"`
	Complete  bool      `json:"complete"`
}

type Store struct {
	root      string
	maxBytes  int64
	retention time.Duration
	mu        sync.Mutex
	now       func() time.Time
}

func NewStore(root string, maxBytes int64, retention time.Duration) (*Store, error) {
	if root == "" || maxBytes <= 0 || retention <= 0 {
		return nil, errors.New("job log storage directory, positive byte limit, and positive retention are required")
	}
	if err := os.MkdirAll(root, 0o700); err != nil {
		return nil, fmt.Errorf("create job log storage: %w", err)
	}
	return &Store{root: root, maxBytes: maxBytes, retention: retention, now: time.Now}, nil
}

func validIdentity(values ...string) bool {
	for _, value := range values {
		if !identityPattern.MatchString(value) {
			return false
		}
	}
	return true
}

func (s *Store) path(record Record) string {
	return filepath.Join(s.root, record.Namespace, record.JobUID, record.PodUID)
}

func (s *Store) Begin(record Record) (Record, error) {
	if !validIdentity(record.Namespace, record.JobUID, record.PodUID) {
		return Record{}, errors.New("job log identity is invalid")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	existing, err := readRecord(s.path(record) + ".json")
	if err == nil {
		return existing, nil
	}
	if !errors.Is(err, fs.ErrNotExist) {
		return Record{}, err
	}
	record.UpdatedAt = s.now().UTC()
	if err := os.MkdirAll(filepath.Dir(s.path(record)), 0o700); err != nil {
		return Record{}, err
	}
	if err := s.save(record); err != nil {
		return Record{}, err
	}
	return record, nil
}

func (s *Store) save(record Record) error {
	data, err := json.Marshal(record)
	if err != nil {
		return err
	}
	path := s.path(record) + ".json"
	temporary, err := os.OpenFile(path+".tmp", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	defer os.Remove(path + ".tmp")
	if _, err := temporary.Write(data); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := os.Rename(path+".tmp", path); err != nil {
		return err
	}
	directory, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}

func readRecord(path string) (Record, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Record{}, err
	}
	var record Record
	if err := json.Unmarshal(data, &record); err != nil {
		return Record{}, err
	}
	return record, nil
}

func (s *Store) Append(record Record, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	file, err := os.OpenFile(s.path(record)+".log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return err
	}
	limited := int64(len(data)) > s.maxBytes-info.Size()
	if limited {
		remaining := s.maxBytes - info.Size()
		if remaining <= 0 {
			return ErrLimit
		}
		data = data[:remaining]
		end := bytes.LastIndexByte(data, '\n')
		if end < 0 {
			return ErrLimit
		}
		data = data[:end+1]
	}
	if _, err := file.Write(data); err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return err
	}
	record.UpdatedAt = s.now().UTC()
	if err := s.save(record); err != nil {
		return err
	}
	if limited {
		return ErrLimit
	}
	return nil
}

func (s *Store) Finish(record Record, complete bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	record.Finished = true
	record.Complete = complete
	record.UpdatedAt = s.now().UTC()
	return s.save(record)
}

// Cursor includes the number of records at the last timestamp because Kubernetes
// can return multiple log records with an identical timestamp on reconnect.
type Cursor struct {
	Time  time.Time
	Count int
}

func (s *Store) Cursor(record Record) (Cursor, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	file, err := os.OpenFile(s.path(record)+".log", os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return Cursor{}, err
	}
	defer file.Close()
	reader := bufio.NewReader(file)
	var cursor Cursor
	var offset int64
	for {
		line, err := reader.ReadString('\n')
		if err == io.EOF {
			return cursor, file.Truncate(offset)
		}
		if err != nil {
			return Cursor{}, err
		}
		timestamp, _, _ := strings.Cut(line, " ")
		parsed, err := time.Parse(time.RFC3339Nano, timestamp)
		if err != nil {
			return Cursor{}, fmt.Errorf("invalid archived log timestamp: %w", err)
		}
		if parsed.Equal(cursor.Time) {
			cursor.Count++
		} else {
			cursor = Cursor{Time: parsed, Count: 1}
		}
		offset += int64(len(line))
	}
}

func (s *Store) Open(namespace, jobUID string) (io.ReadCloser, Record, error) {
	if !validIdentity(namespace, jobUID) {
		return nil, Record{}, ErrNotFound
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	paths, err := filepath.Glob(filepath.Join(s.root, namespace, jobUID, "*.json"))
	if err != nil {
		return nil, Record{}, err
	}
	var records []Record
	for _, path := range paths {
		record, err := readRecord(path)
		if err != nil {
			return nil, Record{}, err
		}
		if record.UpdatedAt.Add(s.retention).After(s.now()) {
			records = append(records, record)
		}
	}
	if len(records) == 0 {
		return nil, Record{}, ErrNotFound
	}
	sort.Slice(records, func(i, j int) bool { return records[i].CreatedAt.Before(records[j].CreatedAt) })
	record := records[0]
	file, err := os.Open(s.path(record) + ".log")
	if errors.Is(err, fs.ErrNotExist) {
		return io.NopCloser(strings.NewReader("")), record, nil
	}
	return file, record, err
}

func (s *Store) Cleanup() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var directories []string
	err := filepath.WalkDir(s.root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if path != s.root {
				directories = append(directories, path)
			}
			return nil
		}
		if filepath.Ext(path) != ".json" {
			return nil
		}
		record, err := readRecord(path)
		if err != nil {
			return err
		}
		if record.UpdatedAt.Add(s.retention).After(s.now()) {
			return nil
		}
		for _, suffix := range []string{".log", ".json"} {
			if err := os.Remove(strings.TrimSuffix(path, ".json") + suffix); err != nil && !errors.Is(err, fs.ErrNotExist) {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	for index := len(directories) - 1; index >= 0; index-- {
		entries, err := os.ReadDir(directories[index])
		if err != nil {
			return err
		}
		if len(entries) == 0 {
			if err := os.Remove(directories[index]); err != nil {
				return err
			}
		}
	}
	return nil
}
