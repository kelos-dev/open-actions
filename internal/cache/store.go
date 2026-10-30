package cache

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

var (
	ErrExists   = errors.New("cache entry already exists")
	ErrNotFound = errors.New("cache entry not found")
	ErrInvalid  = errors.New("invalid cache entry")
	ErrLimit    = errors.New("cache storage limit exceeded")
)

type Limits struct {
	MaxEntryBytes      int64
	MaxRepositoryBytes int64
	MaxTotalBytes      int64
	Retention          time.Duration
	UploadRetention    time.Duration
}

func DefaultLimits() Limits {
	return Limits{MaxEntryBytes: 10 << 30, MaxRepositoryBytes: 10 << 30, MaxTotalBytes: 10 << 30, Retention: 7 * 24 * time.Hour, UploadRetention: time.Hour}
}

func (l Limits) Validate() error {
	if l.MaxEntryBytes < 1 || l.MaxRepositoryBytes < l.MaxEntryBytes || l.MaxTotalBytes < l.MaxRepositoryBytes || l.Retention <= 0 || l.UploadRetention <= 0 {
		return errors.New("cache limits must be positive and ordered by entry, repository, and total")
	}
	return nil
}

type Entry struct {
	ID         int64            `json:"id"`
	Tenant     Tenant           `json:"tenant"`
	Ref        string           `json:"ref"`
	Key        string           `json:"key"`
	Version    string           `json:"version"`
	Owner      string           `json:"owner"`
	CreatedAt  time.Time        `json:"created_at"`
	AccessedAt time.Time        `json:"accessed_at"`
	Size       int64            `json:"size"`
	Uploaded   bool             `json:"uploaded"`
	Digest     string           `json:"digest,omitempty"`
	Finalized  bool             `json:"finalized"`
	Blocks     map[string]int64 `json:"blocks,omitempty"`
}

type Store struct {
	root            string
	limits          Limits
	now             func() time.Time
	mu              sync.Mutex
	entries         map[int64]Entry
	active          map[int64]int
	reserved        map[int64]int64
	committing      map[int64]bool
	copyBlock       func(io.Writer, io.Reader) (int64, error)
	removeDirectory func(string) error
}

func NewStore(root string, limits Limits) (*Store, error) {
	if root == "" {
		return nil, errors.New("cache storage directory must be specified")
	}
	if err := limits.Validate(); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(root, 0700); err != nil {
		return nil, err
	}
	s := &Store{root: root, limits: limits, now: time.Now, entries: map[int64]Entry{}, active: map[int64]int{}, reserved: map[int64]int64{}, committing: map[int64]bool{}, copyBlock: io.Copy, removeDirectory: os.RemoveAll}
	dirs, err := os.ReadDir(root)
	if err != nil {
		return nil, err
	}
	for _, dir := range dirs {
		if !dir.IsDir() {
			continue
		}
		id, err := strconv.ParseInt(dir.Name(), 10, 64)
		if err != nil || id < 1 {
			return nil, fmt.Errorf("invalid cache storage directory %q", dir.Name())
		}
		data, err := os.ReadFile(filepath.Join(root, dir.Name(), "entry.json"))
		if errors.Is(err, fs.ErrNotExist) {
			if err := os.RemoveAll(filepath.Join(root, dir.Name())); err != nil {
				return nil, err
			}
			continue
		}
		if err != nil {
			return nil, err
		}
		var e Entry
		if err := json.Unmarshal(data, &e); err != nil {
			return nil, err
		}
		if e.ID != id || e.Tenant.ProjectUID == "" || e.Tenant.RepositoryID < 1 {
			return nil, fmt.Errorf("invalid cache entry %d metadata", id)
		}
		if e.Uploaded {
			info, err := os.Stat(filepath.Join(s.directory(id), "blob"))
			if err != nil || info.Size() != e.Size {
				return nil, fmt.Errorf("cache entry %d blob is missing or has an invalid size", id)
			}
		} else {
			interrupted := false
			for block, size := range e.Blocks {
				info, err := os.Stat(s.blockPath(id, block))
				if err != nil && !errors.Is(err, fs.ErrNotExist) {
					return nil, err
				}
				if err != nil || info.Size() != size {
					interrupted = true
					break
				}
			}
			if interrupted {
				if err := os.RemoveAll(s.directory(id)); err != nil {
					return nil, err
				}
				continue
			}
		}
		s.entries[id] = e
		// Temporary files are never published and may be left by an interrupted upload.
		files, err := os.ReadDir(s.directory(id))
		if err != nil {
			return nil, err
		}
		referenced := map[string]bool{}
		if !e.Uploaded {
			for block := range e.Blocks {
				referenced[filepath.Base(s.blockPath(id, block))] = true
			}
		}
		for _, file := range files {
			remove := strings.HasPrefix(file.Name(), ".incoming-") || strings.HasPrefix(file.Name(), "block-") && !referenced[file.Name()] || file.Name() == "blob" && !e.Uploaded
			if remove {
				if err := os.Remove(filepath.Join(s.directory(id), file.Name())); err != nil {
					return nil, err
				}
			}
		}
	}
	if err := s.Cleanup(); err != nil {
		return nil, err
	}
	return s, nil
}

func ValidateKey(key, version string) error {
	if key == "" || !utf8.ValidString(key) || utf8.RuneCountInString(key) > 512 || strings.Contains(key, ",") || version == "" || len(version) > 1024 {
		return ErrInvalid
	}
	return nil
}

func (s *Store) Create(tenant Tenant, ref, owner, key, version string) (Entry, error) {
	if tenant.ProjectUID == "" || tenant.RepositoryID < 1 || !strings.HasPrefix(ref, "refs/") || owner == "" {
		return Entry{}, ErrInvalid
	}
	if err := ValidateKey(key, version); err != nil {
		return Entry{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.cleanupLocked(); err != nil {
		return Entry{}, err
	}
	for _, e := range s.entries {
		if e.Tenant == tenant && e.Ref == ref && e.Key == key && e.Version == version {
			return Entry{}, ErrExists
		}
	}
	var random [8]byte
	if _, err := rand.Read(random[:]); err != nil {
		return Entry{}, err
	}
	// The Actions toolkit returns cache IDs as JavaScript numbers.
	id := int64(binary.BigEndian.Uint64(random[:]) & ((1 << 53) - 1))
	if id == 0 {
		id = 1
	}
	if _, exists := s.entries[id]; exists {
		return Entry{}, ErrExists
	}
	e := Entry{ID: id, Tenant: tenant, Ref: ref, Owner: owner, Key: key, Version: version, CreatedAt: s.now().UTC(), AccessedAt: s.now().UTC(), Blocks: map[string]int64{}}
	if err := os.Mkdir(s.directory(id), 0700); err != nil {
		return Entry{}, err
	}
	if err := s.save(e); err != nil {
		return Entry{}, err
	}
	s.entries[id] = e
	return e, nil
}

func (s *Store) Get(tenant Tenant, id int64) (Entry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.getLocked(tenant, id)
}

func (s *Store) getLocked(tenant Tenant, id int64) (Entry, error) {
	e, ok := s.entries[id]
	if !ok || e.Tenant != tenant {
		return Entry{}, ErrNotFound
	}
	return e, nil
}

func (s *Store) Stage(tenant Tenant, id int64, block string, size int64, body io.Reader) error {
	if len(block) > 1024 || size < 0 || size > s.limits.MaxEntryBytes {
		return ErrInvalid
	}
	s.mu.Lock()
	e, err := s.getLocked(tenant, id)
	if err == nil && s.committing[id] {
		err = ErrExists
	}
	if err == nil && e.Uploaded {
		if !e.Finalized && block == "" && size == e.Size && e.Digest != "" {
			s.mu.Unlock()
			hash := sha256.New()
			n, err := io.Copy(hash, io.LimitReader(body, size+1))
			if err != nil {
				return err
			}
			if n == size && hex.EncodeToString(hash.Sum(nil)) == e.Digest {
				return nil
			}
			return ErrExists
		}
		err = ErrExists
	}
	if err == nil && block != "" && len(e.Blocks) >= 50000 {
		if _, exists := e.Blocks[block]; !exists {
			err = ErrLimit
		}
	}
	if err == nil {
		used := stagedSize(e) + s.reserved[id]
		if block != "" {
			used -= e.Blocks[block]
		}
		if size > s.limits.MaxEntryBytes-used {
			err = ErrLimit
		} else {
			err = s.makeRoomLocked(tenant, size-e.Blocks[block], id)
		}
	}
	if err != nil {
		s.mu.Unlock()
		return err
	}
	s.active[id]++
	s.reserved[id] += size
	s.mu.Unlock()
	temporary, err := os.CreateTemp(s.directory(id), ".incoming-")
	hash := sha256.New()
	if err == nil {
		defer os.Remove(temporary.Name())
		var n int64
		n, err = io.Copy(io.MultiWriter(temporary, hash), io.LimitReader(body, size+1))
		if err == nil && n != size {
			err = ErrInvalid
		}
		if err == nil {
			err = temporary.Sync()
		}
		err = errors.Join(err, temporary.Close())
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.active[id]--
	s.reserved[id] -= size
	if err != nil {
		return err
	}
	e = s.entries[id]
	if e.Uploaded || e.Finalized {
		return ErrExists
	}
	if block == "" {
		if len(e.Blocks) > 0 {
			return ErrInvalid
		}
		err = os.Rename(temporary.Name(), filepath.Join(s.directory(id), "blob"))
		e.Uploaded = true
		e.Digest = hex.EncodeToString(hash.Sum(nil))
		e.Size = size
	} else {
		err = os.Rename(temporary.Name(), s.blockPath(id, block))
		if e.Blocks == nil {
			e.Blocks = map[string]int64{}
		}
		e.Blocks[block] = size
	}
	if err != nil {
		return err
	}
	if err = s.save(e); err != nil {
		return err
	}
	s.entries[id] = e
	return nil
}

func (s *Store) Commit(tenant Tenant, id int64, blocks []string) error {
	e, err := s.beginCommit(tenant, id, blocks)
	if err != nil {
		return err
	}
	if e.Uploaded {
		return nil
	}
	defer s.finishCommit(id)
	temporary, err := os.CreateTemp(s.directory(id), ".incoming-")
	if err != nil {
		return err
	}
	defer os.Remove(temporary.Name())
	defer temporary.Close()
	for _, block := range blocks {
		f, err := os.Open(s.blockPath(id, block))
		if err != nil {
			return err
		}
		_, copyErr := s.copyBlock(temporary, f)
		if err := errors.Join(copyErr, f.Close()); err != nil {
			return err
		}
	}
	if err := temporary.Sync(); err != nil {
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := s.publishCommit(e, temporary.Name()); err != nil {
		return err
	}
	for block := range e.Blocks {
		if err := os.Remove(s.blockPath(id, block)); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return err
		}
	}
	return nil
}

func (s *Store) beginCommit(tenant Tenant, id int64, blocks []string) (Entry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, err := s.getLocked(tenant, id)
	if err != nil {
		return Entry{}, err
	}
	if e.Uploaded {
		return e, nil
	}
	if s.active[id] > 0 || s.committing[id] || len(blocks) == 0 || len(blocks) > 50000 {
		return Entry{}, ErrInvalid
	}
	seen := map[string]bool{}
	var size int64
	for _, block := range blocks {
		n, ok := e.Blocks[block]
		if !ok || seen[block] {
			return Entry{}, ErrInvalid
		}
		seen[block] = true
		size += n
	}
	if size > s.limits.MaxEntryBytes {
		return Entry{}, ErrLimit
	}
	e.Size = size
	s.committing[id] = true
	return e, nil
}

func (s *Store) publishCommit(e Entry, path string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := os.Rename(path, filepath.Join(s.directory(e.ID), "blob")); err != nil {
		return err
	}
	e.Uploaded = true
	if err := s.save(e); err != nil {
		return err
	}
	s.entries[e.ID] = e
	return nil
}

func (s *Store) finishCommit(id int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.committing, id)
}

func (s *Store) Finalize(tenant Tenant, ref, owner, key, version string, size int64) (Entry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, e := range s.entries {
		if e.Tenant != tenant || e.Ref != ref || e.Key != key || e.Version != version {
			continue
		}
		if e.Owner != owner {
			return Entry{}, ErrExists
		}
		if !e.Uploaded || size < 0 || e.Size != size {
			return Entry{}, ErrInvalid
		}
		if e.Finalized {
			return e, nil
		}
		e.Finalized = true
		e.AccessedAt = s.now().UTC()
		if err := s.save(e); err != nil {
			return Entry{}, err
		}
		s.entries[e.ID] = e
		return e, nil
	}
	return Entry{}, ErrNotFound
}

func (s *Store) Lookup(tenant Tenant, refs []string, key, version string, restore []string) (Entry, error) {
	if err := ValidateKey(key, version); err != nil {
		return Entry{}, err
	}
	if len(restore) > 10 {
		return Entry{}, ErrInvalid
	}
	for _, prefix := range restore {
		if err := ValidateKey(prefix, version); err != nil {
			return Entry{}, err
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.cleanupLocked(); err != nil {
		return Entry{}, err
	}
	for _, ref := range refs {
		candidates := []Entry{}
		for _, e := range s.entries {
			if e.Tenant == tenant && e.Ref == ref && e.Version == version && e.Finalized {
				candidates = append(candidates, e)
			}
		}
		sort.Slice(candidates, func(i, j int) bool { return candidates[i].CreatedAt.After(candidates[j].CreatedAt) })
		keys := append([]string{key}, restore...)
		for _, prefix := range keys {
			var match *Entry
			for _, e := range candidates {
				if e.Key == prefix {
					v := e
					match = &v
					break
				}
			}
			if match == nil {
				for _, e := range candidates {
					if strings.HasPrefix(e.Key, prefix) {
						v := e
						match = &v
						break
					}
				}
			}
			if match != nil {
				match.AccessedAt = s.now().UTC()
				if err := s.save(*match); err != nil {
					return Entry{}, err
				}
				s.entries[match.ID] = *match
				return *match, nil
			}
		}
	}
	return Entry{}, ErrNotFound
}

func (s *Store) Open(tenant Tenant, id int64) (*os.File, Entry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, err := s.getLocked(tenant, id)
	if err != nil {
		return nil, Entry{}, err
	}
	if !e.Finalized {
		return nil, Entry{}, ErrNotFound
	}
	e.AccessedAt = s.now().UTC()
	if err := s.save(e); err != nil {
		return nil, Entry{}, err
	}
	s.entries[id] = e
	f, err := os.Open(filepath.Join(s.directory(id), "blob"))
	return f, e, err
}

func (s *Store) Cleanup() error { s.mu.Lock(); defer s.mu.Unlock(); return s.cleanupLocked() }
func (s *Store) cleanupLocked() error {
	for id, e := range s.entries {
		expired := !e.Finalized && s.now().Sub(e.CreatedAt) >= s.limits.UploadRetention || e.Finalized && s.now().Sub(e.AccessedAt) >= s.limits.Retention
		if expired && s.active[id] == 0 && !s.committing[id] {
			if err := s.remove(id); err != nil {
				return err
			}
		}
	}
	return nil
}

func stagedSize(e Entry) int64 {
	if e.Uploaded {
		return e.Size
	}
	var n int64
	for _, size := range e.Blocks {
		n += size
	}
	return n
}

func (s *Store) makeRoomLocked(tenant Tenant, incoming, except int64) error {
	var total, repository int64
	candidates := []Entry{}
	for id, e := range s.entries {
		size := stagedSize(e) + s.reserved[id]
		total += size
		if e.Tenant == tenant {
			repository += size
		}
		if e.Finalized && id != except && s.active[id] == 0 && !s.committing[id] {
			candidates = append(candidates, e)
		}
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].AccessedAt.Before(candidates[j].AccessedAt) })
	victims := []int64{}
	selected := map[int64]bool{}
	selectEntry := func(e Entry) {
		victims = append(victims, e.ID)
		selected[e.ID] = true
		total -= e.Size
		if e.Tenant == tenant {
			repository -= e.Size
		}
	}
	for _, e := range candidates {
		if repository+incoming <= s.limits.MaxRepositoryBytes {
			break
		}
		if e.Tenant == tenant {
			selectEntry(e)
		}
	}
	if repository+incoming > s.limits.MaxRepositoryBytes {
		return ErrLimit
	}
	for _, e := range candidates {
		if total+incoming <= s.limits.MaxTotalBytes {
			break
		}
		if !selected[e.ID] {
			selectEntry(e)
		}
	}
	if total+incoming > s.limits.MaxTotalBytes {
		return ErrLimit
	}
	for _, id := range victims {
		if err := s.remove(id); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) remove(id int64) error {
	// Persist logical deletion before removing blobs so interrupted cleanup is recoverable at startup.
	path := s.directory(id)
	if err := os.Remove(filepath.Join(path, "entry.json")); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	if err := errors.Join(directory.Sync(), directory.Close()); err != nil {
		return err
	}
	delete(s.entries, id)
	delete(s.active, id)
	delete(s.reserved, id)
	delete(s.committing, id)
	return s.removeDirectory(path)
}

func (s *Store) directory(id int64) string { return filepath.Join(s.root, strconv.FormatInt(id, 10)) }
func (s *Store) blockPath(id int64, block string) string {
	hash := sha256.Sum256([]byte(block))
	return filepath.Join(s.directory(id), "block-"+hex.EncodeToString(hash[:]))
}
func (s *Store) save(e Entry) error {
	data, err := json.Marshal(e)
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(s.directory(e.ID), ".incoming-")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(f.Name(), filepath.Join(s.directory(e.ID), "entry.json")); err != nil {
		return err
	}
	dir, err := os.Open(s.directory(e.ID))
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
