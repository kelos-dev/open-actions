package artifact

import (
	"archive/zip"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
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
)

var (
	ErrArtifactExists   = errors.New("artifact already exists")
	ErrArtifactNotFound = errors.New("artifact not found")
	ErrInvalidArtifact  = errors.New("artifact is invalid")
	ErrLimitExceeded    = errors.New("artifact storage limit exceeded")
)

type Limits struct {
	MaxFileBytes              int64
	MaxArtifactBytes          int64
	MaxRunBytes               int64
	MaxArtifactsPerJob        int
	DefaultRetention          time.Duration
	MaxRetention              time.Duration
	IncompleteUploadRetention time.Duration
}

func DefaultLimits() Limits {
	return Limits{
		MaxFileBytes:              1 << 30,
		MaxArtifactBytes:          2 << 30,
		MaxRunBytes:               10 << 30,
		MaxArtifactsPerJob:        500,
		DefaultRetention:          7 * 24 * time.Hour,
		MaxRetention:              30 * 24 * time.Hour,
		IncompleteUploadRetention: time.Hour,
	}
}

type Artifact struct {
	ID           int64     `json:"id"`
	Scope        Scope     `json:"scope"`
	JobBackendID string    `json:"job_backend_id"`
	Name         string    `json:"name"`
	MIMEType     string    `json:"mime_type"`
	Version      int32     `json:"version"`
	State        string    `json:"state"`
	CreatedAt    time.Time `json:"created_at"`
	ExpiresAt    time.Time `json:"expires_at"`
	FinalizedAt  time.Time `json:"finalized_at,omitempty"`
	Size         int64     `json:"size,omitempty"`
	Digest       string    `json:"digest,omitempty"`
}

const (
	artifactStatePending   = "pending"
	artifactStateUploaded  = "uploaded"
	artifactStateFinalized = "finalized"
)

type Store struct {
	root                 string
	limits               Limits
	now                  func() time.Time
	validateBlob         func(string, string, Limits) error
	mu                   sync.Mutex
	operationChanged     *sync.Cond
	scopeReservations    map[string]int64
	artifactReservations map[string]int64
	incomingReservations map[string]int64
	activeUploads        map[string]int
	activeCommits        map[string]bool
}

func NewStore(root string, limits Limits) (*Store, error) {
	if root == "" {
		return nil, errors.New("artifact storage directory must be specified")
	}
	if limits.MaxFileBytes < 1 || limits.MaxArtifactBytes < limits.MaxFileBytes || limits.MaxRunBytes < limits.MaxArtifactBytes ||
		limits.MaxArtifactsPerJob < 1 || limits.DefaultRetention <= 0 || limits.MaxRetention < limits.DefaultRetention || limits.IncompleteUploadRetention <= 0 {
		return nil, errors.New("artifact storage limits are invalid")
	}
	if err := os.MkdirAll(filepath.Join(root, "scopes"), 0o700); err != nil {
		return nil, fmt.Errorf("create artifact storage directory: %w", err)
	}
	if err := os.MkdirAll(filepath.Join(root, "incoming"), 0o700); err != nil {
		return nil, fmt.Errorf("create artifact incoming directory: %w", err)
	}
	store := &Store{
		root: root, limits: limits, now: time.Now, validateBlob: validateBlob,
		scopeReservations: map[string]int64{}, artifactReservations: map[string]int64{}, incomingReservations: map[string]int64{},
		activeUploads: map[string]int{}, activeCommits: map[string]bool{},
	}
	store.operationChanged = sync.NewCond(&store.mu)
	return store, nil
}

func (s *Store) Create(scope Scope, jobBackendID, name, mimeType string, version int32, requestedExpiration *time.Time) (Artifact, error) {
	if err := validateArtifactName(name); err != nil {
		return Artifact{}, err
	}
	if jobBackendID == "" || version < 4 || version > 7 || len(mimeType) > 255 || strings.ContainsAny(mimeType, "\r\n") {
		return Artifact{}, fmt.Errorf("%w: artifact metadata is invalid", ErrInvalidArtifact)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.cleanupScopeLocked(scope); err != nil {
		return Artifact{}, err
	}
	records, err := s.loadRecords(scope)
	if err != nil {
		return Artifact{}, err
	}
	jobCount := 0
	for _, record := range records {
		if record.Name == name {
			return Artifact{}, fmt.Errorf("%w: artifact %q", ErrArtifactExists, name)
		}
		if record.JobBackendID == jobBackendID {
			jobCount++
		}
	}
	if jobCount >= s.limits.MaxArtifactsPerJob {
		return Artifact{}, fmt.Errorf("%w: a job may create at most %d artifacts", ErrLimitExceeded, s.limits.MaxArtifactsPerJob)
	}
	now := s.now().UTC()
	expiresAt := now.Add(s.limits.DefaultRetention)
	if requestedExpiration != nil && requestedExpiration.After(now) {
		expiresAt = requestedExpiration.UTC()
	}
	maximumExpiration := now.Add(s.limits.MaxRetention)
	if expiresAt.After(maximumExpiration) {
		expiresAt = maximumExpiration
	}
	id, err := s.newArtifactID(scope, records)
	if err != nil {
		return Artifact{}, err
	}
	record := Artifact{
		ID: id, Scope: scope, JobBackendID: jobBackendID, Name: name, MIMEType: mimeType, Version: version,
		State: artifactStatePending, CreatedAt: now, ExpiresAt: expiresAt,
	}
	if err := s.writeRecord(record); err != nil {
		return Artifact{}, err
	}
	return record, nil
}

func (s *Store) StageBlock(scope Scope, jobBackendID string, artifactID int64, blockID string, contentLength int64, body io.Reader) error {
	if blockID == "" || len(blockID) > 256 {
		return fmt.Errorf("%w: block ID is invalid", ErrInvalidArtifact)
	}
	if contentLength < 0 || contentLength > s.limits.MaxArtifactBytes {
		return fmt.Errorf("%w: artifact block content length is invalid", ErrLimitExceeded)
	}
	record, blockDirectory, release, err := s.reserveBlock(scope, jobBackendID, artifactID, blockID, contentLength)
	if err != nil {
		return err
	}
	defer release()
	temporary, err := os.CreateTemp(filepath.Join(s.root, "incoming"), ".block-")
	if err != nil {
		return fmt.Errorf("create artifact block: %w", err)
	}
	temporaryName := temporary.Name()
	defer os.Remove(temporaryName)
	written, copyErr := io.Copy(temporary, io.LimitReader(body, contentLength+1))
	closeErr := temporary.Close()
	if copyErr != nil {
		return fmt.Errorf("write artifact block: %w", copyErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close artifact block: %w", closeErr)
	}
	if written != contentLength {
		return fmt.Errorf("%w: artifact block content length does not match the request body", ErrInvalidArtifact)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	record, err = s.loadRecord(scope, artifactID)
	if err != nil {
		return err
	}
	if record.JobBackendID != jobBackendID || record.State != artifactStatePending {
		return fmt.Errorf("%w: artifact upload is not active for this job", ErrArtifactNotFound)
	}
	destination := filepath.Join(blockDirectory, blockFileName(blockID))
	if err := os.Rename(temporaryName, destination); err != nil {
		return fmt.Errorf("commit artifact block: %w", err)
	}
	return syncDirectory(blockDirectory)
}

func (s *Store) reserveBlock(scope Scope, jobBackendID string, artifactID int64, blockID string, contentLength int64) (Artifact, string, func(), error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	record, err := s.loadRecord(scope, artifactID)
	if err != nil {
		return Artifact{}, "", nil, err
	}
	artifactKey := s.reservationKey(scope, artifactID)
	if s.activeCommits[artifactKey] {
		return Artifact{}, "", nil, fmt.Errorf("%w: artifact upload is not active for this job", ErrArtifactNotFound)
	}
	if s.expired(record) {
		if err := s.deleteRecord(record); err != nil {
			return Artifact{}, "", nil, err
		}
		return Artifact{}, "", nil, ErrArtifactNotFound
	}
	if record.JobBackendID != jobBackendID || record.State != artifactStatePending {
		return Artifact{}, "", nil, fmt.Errorf("%w: artifact upload is not active for this job", ErrArtifactNotFound)
	}
	blockDirectory := s.blockDirectory(record)
	if err := os.MkdirAll(blockDirectory, 0o700); err != nil {
		return Artifact{}, "", nil, fmt.Errorf("create artifact block directory: %w", err)
	}
	artifactBytes, err := directoryFileBytes(blockDirectory)
	if err != nil {
		return Artifact{}, "", nil, err
	}
	oldSize := int64(0)
	if info, err := os.Stat(filepath.Join(blockDirectory, blockFileName(blockID))); err == nil {
		oldSize = info.Size()
	} else if !errors.Is(err, fs.ErrNotExist) {
		return Artifact{}, "", nil, err
	}
	growth := max(contentLength-oldSize, 0)
	if growth > s.limits.MaxArtifactBytes-artifactBytes-s.artifactReservations[artifactKey] {
		return Artifact{}, "", nil, fmt.Errorf("%w: artifact exceeds %d bytes", ErrLimitExceeded, s.limits.MaxArtifactBytes)
	}
	runBytes, err := s.scopeStorageBytes(scope)
	if err != nil {
		return Artifact{}, "", nil, err
	}
	scopeKey := s.scopeDirectory(scope)
	if growth > s.limits.MaxRunBytes-runBytes-s.scopeReservations[scopeKey] {
		return Artifact{}, "", nil, fmt.Errorf("%w: workflow run exceeds %d bytes", ErrLimitExceeded, s.limits.MaxRunBytes)
	}
	if contentLength > s.limits.MaxRunBytes-s.incomingReservations[scopeKey] {
		return Artifact{}, "", nil, fmt.Errorf("%w: concurrent uploads exceed %d bytes", ErrLimitExceeded, s.limits.MaxRunBytes)
	}
	s.artifactReservations[artifactKey] += growth
	s.scopeReservations[scopeKey] += growth
	s.incomingReservations[scopeKey] += contentLength
	s.activeUploads[artifactKey]++
	return record, blockDirectory, func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.artifactReservations[artifactKey] -= growth
		s.scopeReservations[scopeKey] -= growth
		s.incomingReservations[scopeKey] -= contentLength
		if s.artifactReservations[artifactKey] == 0 {
			delete(s.artifactReservations, artifactKey)
		}
		if s.scopeReservations[scopeKey] == 0 {
			delete(s.scopeReservations, scopeKey)
		}
		if s.incomingReservations[scopeKey] == 0 {
			delete(s.incomingReservations, scopeKey)
		}
		s.activeUploads[artifactKey]--
		if s.activeUploads[artifactKey] == 0 {
			delete(s.activeUploads, artifactKey)
		}
		s.operationChanged.Broadcast()
	}, nil
}

type blockList struct {
	Latest      []string `xml:"Latest"`
	Uncommitted []string `xml:"Uncommitted"`
	Committed   []string `xml:"Committed"`
}

func DecodeBlockList(reader io.Reader) ([]string, error) {
	list := blockList{}
	decoder := xml.NewDecoder(io.LimitReader(reader, 1<<20))
	if err := decoder.Decode(&list); err != nil {
		return nil, fmt.Errorf("%w: decode block list: %v", ErrInvalidArtifact, err)
	}
	blocks := append(append(list.Latest, list.Uncommitted...), list.Committed...)
	if len(blocks) == 0 || len(blocks) > 50000 {
		return nil, fmt.Errorf("%w: block list is empty or too large", ErrInvalidArtifact)
	}
	for _, blockID := range blocks {
		if blockID == "" || len(blockID) > 256 {
			return nil, fmt.Errorf("%w: block ID is invalid", ErrInvalidArtifact)
		}
	}
	return blocks, nil
}

func (s *Store) Commit(scope Scope, jobBackendID string, artifactID int64, blocks []string) (Artifact, error) {
	record, active, err := s.beginCommit(scope, jobBackendID, artifactID)
	if err != nil {
		return Artifact{}, err
	}
	if !active {
		return record, nil
	}
	artifactKey := s.reservationKey(scope, artifactID)
	defer s.finishCommit(artifactKey)

	temporary, err := os.CreateTemp(s.scopeDirectory(scope), ".blob-")
	if err != nil {
		return Artifact{}, fmt.Errorf("create artifact blob: %w", err)
	}
	temporaryName := temporary.Name()
	defer os.Remove(temporaryName)
	hash := sha256.New()
	written := int64(0)
	for _, blockID := range blocks {
		block, openErr := os.Open(filepath.Join(s.blockDirectory(record), blockFileName(blockID)))
		if openErr != nil {
			temporary.Close()
			return Artifact{}, fmt.Errorf("%w: artifact block is missing", ErrInvalidArtifact)
		}
		copied, copyErr := io.Copy(io.MultiWriter(temporary, hash), block)
		closeErr := block.Close()
		written += copied
		if copyErr != nil || closeErr != nil {
			temporary.Close()
			return Artifact{}, fmt.Errorf("assemble artifact blob: %w", errors.Join(copyErr, closeErr))
		}
		if written > s.limits.MaxArtifactBytes {
			temporary.Close()
			return Artifact{}, fmt.Errorf("%w: artifact exceeds %d bytes", ErrLimitExceeded, s.limits.MaxArtifactBytes)
		}
	}
	if err := temporary.Sync(); err != nil {
		temporary.Close()
		return Artifact{}, fmt.Errorf("sync artifact blob: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return Artifact{}, fmt.Errorf("close artifact blob: %w", err)
	}
	if err := s.validateBlob(temporaryName, record.MIMEType, s.limits); err != nil {
		return Artifact{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	record, err = s.loadRecord(scope, artifactID)
	if err != nil {
		return Artifact{}, err
	}
	if record.JobBackendID != jobBackendID {
		return Artifact{}, fmt.Errorf("%w: artifact upload is not active for this job", ErrArtifactNotFound)
	}
	if record.State == artifactStateUploaded || record.State == artifactStateFinalized {
		return record, nil
	}
	if record.State != artifactStatePending {
		return Artifact{}, fmt.Errorf("%w: artifact state is invalid", ErrInvalidArtifact)
	}
	runBytes, err := s.finalizedBytes(scope)
	if err != nil {
		return Artifact{}, err
	}
	if runBytes+written > s.limits.MaxRunBytes {
		return Artifact{}, fmt.Errorf("%w: workflow run exceeds %d bytes", ErrLimitExceeded, s.limits.MaxRunBytes)
	}
	if err := os.MkdirAll(filepath.Dir(s.blobPath(record)), 0o700); err != nil {
		return Artifact{}, fmt.Errorf("create artifact blob directory: %w", err)
	}
	if err := os.Rename(temporaryName, s.blobPath(record)); err != nil {
		return Artifact{}, fmt.Errorf("commit artifact blob: %w", err)
	}
	if err := syncDirectory(filepath.Dir(s.blobPath(record))); err != nil {
		return Artifact{}, fmt.Errorf("sync artifact blob directory: %w", err)
	}
	record.State = artifactStateUploaded
	record.Size = written
	record.Digest = "sha256:" + hex.EncodeToString(hash.Sum(nil))
	if err := s.writeRecord(record); err != nil {
		return Artifact{}, err
	}
	if err := os.RemoveAll(s.uploadDirectory(record)); err != nil {
		return Artifact{}, fmt.Errorf("remove artifact blocks: %w", err)
	}
	if err := syncDirectoryIfExists(filepath.Dir(s.uploadDirectory(record))); err != nil {
		return Artifact{}, fmt.Errorf("sync artifact upload directory: %w", err)
	}
	return record, nil
}

func (s *Store) beginCommit(scope Scope, jobBackendID string, artifactID int64) (Artifact, bool, error) {
	artifactKey := s.reservationKey(scope, artifactID)
	s.mu.Lock()
	defer s.mu.Unlock()
	for s.activeUploads[artifactKey] > 0 || s.activeCommits[artifactKey] {
		s.operationChanged.Wait()
	}
	record, err := s.loadRecord(scope, artifactID)
	if err != nil {
		return Artifact{}, false, err
	}
	if record.JobBackendID != jobBackendID {
		return Artifact{}, false, fmt.Errorf("%w: artifact upload is not active for this job", ErrArtifactNotFound)
	}
	if record.State == artifactStateUploaded || record.State == artifactStateFinalized {
		return record, false, nil
	}
	if record.State != artifactStatePending {
		return Artifact{}, false, fmt.Errorf("%w: artifact state is invalid", ErrInvalidArtifact)
	}
	s.activeCommits[artifactKey] = true
	return record, true, nil
}

func (s *Store) finishCommit(artifactKey string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.activeCommits, artifactKey)
	s.operationChanged.Broadcast()
}

func (s *Store) Finalize(scope Scope, jobBackendID, name string, size int64, digest string) (Artifact, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	records, err := s.loadRecords(scope)
	if err != nil {
		return Artifact{}, err
	}
	for _, record := range records {
		if record.Name != name || record.JobBackendID != jobBackendID {
			continue
		}
		if record.State == artifactStateFinalized && record.Size == size && record.Digest == digest {
			return record, nil
		}
		if record.State != artifactStateUploaded {
			return Artifact{}, fmt.Errorf("%w: artifact upload is incomplete", ErrInvalidArtifact)
		}
		if record.Size != size || record.Digest != digest {
			return Artifact{}, fmt.Errorf("%w: artifact size or digest does not match uploaded content", ErrInvalidArtifact)
		}
		record.State = artifactStateFinalized
		record.FinalizedAt = s.now().UTC()
		if err := s.writeRecord(record); err != nil {
			return Artifact{}, err
		}
		return record, nil
	}
	return Artifact{}, fmt.Errorf("%w: artifact %q", ErrArtifactNotFound, name)
}

func (s *Store) List(scope Scope, name string, id int64) ([]Artifact, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.cleanupScopeLocked(scope); err != nil {
		return nil, err
	}
	records, err := s.loadRecords(scope)
	if err != nil {
		return nil, err
	}
	result := make([]Artifact, 0, len(records))
	for _, record := range records {
		if record.State != artifactStateFinalized || name != "" && record.Name != name || id != 0 && record.ID != id {
			continue
		}
		result = append(result, record)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result, nil
}

func (s *Store) Get(scope Scope, artifactID int64) (Artifact, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	record, err := s.loadRecord(scope, artifactID)
	if err != nil {
		return Artifact{}, err
	}
	if s.expired(record) && !s.artifactActiveLocked(record) {
		if err := s.deleteRecord(record); err != nil {
			return Artifact{}, err
		}
		return Artifact{}, ErrArtifactNotFound
	}
	return record, nil
}

func (s *Store) Delete(scope Scope, jobBackendID, name string) (Artifact, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	records, err := s.loadRecords(scope)
	if err != nil {
		return Artifact{}, err
	}
	for _, record := range records {
		if record.Name == name && record.JobBackendID == jobBackendID && record.State == artifactStateFinalized {
			if err := s.deleteRecord(record); err != nil {
				return Artifact{}, err
			}
			return record, nil
		}
	}
	return Artifact{}, fmt.Errorf("%w: artifact %q", ErrArtifactNotFound, name)
}

func (s *Store) OpenBlob(record Artifact) (*os.File, error) {
	if record.State != artifactStateFinalized {
		return nil, ErrArtifactNotFound
	}
	file, err := os.Open(s.blobPath(record))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, ErrArtifactNotFound
	}
	return file, err
}

func (s *Store) Cleanup() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.cleanupIncomingLocked(); err != nil {
		return err
	}
	entries, err := os.ReadDir(filepath.Join(s.root, "scopes"))
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		recordDirectory := filepath.Join(s.root, "scopes", entry.Name(), "records")
		recordEntries, readErr := os.ReadDir(recordDirectory)
		if errors.Is(readErr, fs.ErrNotExist) {
			if err := s.cleanupOrphansLocked(filepath.Join(s.root, "scopes", entry.Name())); err != nil {
				return err
			}
			continue
		}
		if readErr != nil {
			return readErr
		}
		for _, recordEntry := range recordEntries {
			if recordEntry.IsDir() || !strings.HasSuffix(recordEntry.Name(), ".json") {
				continue
			}
			record, readErr := readRecordFile(filepath.Join(recordDirectory, recordEntry.Name()))
			if readErr != nil {
				return readErr
			}
			if s.expired(record) && !s.artifactActiveLocked(record) {
				if err := s.deleteRecord(record); err != nil {
					return err
				}
			}
		}
		if err := s.cleanupOrphansLocked(filepath.Join(s.root, "scopes", entry.Name())); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) cleanupScopeLocked(scope Scope) error {
	records, err := s.loadRecords(scope)
	if err != nil {
		return err
	}
	for _, record := range records {
		if s.expired(record) && !s.artifactActiveLocked(record) {
			if err := s.deleteRecord(record); err != nil {
				return err
			}
		}
	}
	return s.cleanupOrphansLocked(s.scopeDirectory(scope))
}

func (s *Store) cleanupIncomingLocked() error {
	entries, err := os.ReadDir(filepath.Join(s.root, "incoming"))
	if err != nil {
		return err
	}
	cutoff := s.now().Add(-s.limits.IncompleteUploadRetention)
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if info.ModTime().Before(cutoff) {
			if err := os.RemoveAll(filepath.Join(s.root, "incoming", entry.Name())); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *Store) cleanupOrphansLocked(scopeDirectory string) error {
	recordDirectory := filepath.Join(scopeDirectory, "records")
	entries, err := os.ReadDir(recordDirectory)
	if errors.Is(err, fs.ErrNotExist) {
		entries = nil
	} else if err != nil {
		return err
	}
	records := map[string]Artifact{}
	removedRecordTemporary := false
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".json") {
			record, err := readRecordFile(filepath.Join(recordDirectory, entry.Name()))
			if err != nil {
				return err
			}
			records[strings.TrimSuffix(entry.Name(), ".json")] = record
		} else if !entry.IsDir() && strings.HasPrefix(entry.Name(), ".record-") {
			if err := os.Remove(filepath.Join(recordDirectory, entry.Name())); err != nil && !errors.Is(err, fs.ErrNotExist) {
				return err
			}
			removedRecordTemporary = true
		}
	}
	if removedRecordTemporary {
		if err := syncDirectory(recordDirectory); err != nil {
			return err
		}
	}
	for _, directory := range []string{"blobs", "uploads"} {
		path := filepath.Join(scopeDirectory, directory)
		children, err := os.ReadDir(path)
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			return err
		}
		removed := false
		for _, child := range children {
			identifier := strings.TrimSuffix(child.Name(), ".blob")
			record, found := records[identifier]
			remove := !found || directory == "blobs" && record.State == artifactStatePending || directory == "uploads" && record.State != artifactStatePending
			if remove {
				if err := os.RemoveAll(filepath.Join(path, child.Name())); err != nil {
					return err
				}
				removed = true
			}
		}
		if removed {
			if err := syncDirectory(path); err != nil {
				return err
			}
		}
	}
	if s.scopeHasActiveCommitLocked(scopeDirectory) {
		return nil
	}
	scopeEntries, err := os.ReadDir(scopeDirectory)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	removedBlobTemporary := false
	for _, entry := range scopeEntries {
		if !entry.IsDir() && strings.HasPrefix(entry.Name(), ".blob-") {
			if err := os.Remove(filepath.Join(scopeDirectory, entry.Name())); err != nil && !errors.Is(err, fs.ErrNotExist) {
				return err
			}
			removedBlobTemporary = true
		}
	}
	if removedBlobTemporary {
		return syncDirectory(scopeDirectory)
	}
	return nil
}

func (s *Store) artifactActiveLocked(record Artifact) bool {
	artifactKey := s.reservationKey(record.Scope, record.ID)
	return s.activeUploads[artifactKey] > 0 || s.activeCommits[artifactKey]
}

func (s *Store) scopeHasActiveCommitLocked(scopeDirectory string) bool {
	prefix := scopeDirectory + string(filepath.Separator)
	for artifactKey := range s.activeCommits {
		if strings.HasPrefix(artifactKey, prefix) {
			return true
		}
	}
	return false
}

func (s *Store) expired(record Artifact) bool {
	now := s.now()
	if record.State == artifactStateFinalized {
		return !record.ExpiresAt.After(now)
	}
	return !record.CreatedAt.Add(s.limits.IncompleteUploadRetention).After(now)
}

func (s *Store) newArtifactID(scope Scope, records []Artifact) (int64, error) {
	existing := make(map[int64]struct{}, len(records))
	for _, record := range records {
		existing[record.ID] = struct{}{}
	}
	for attempt := 0; attempt < 100; attempt++ {
		bytes := make([]byte, 8)
		if _, err := rand.Read(bytes); err != nil {
			return 0, fmt.Errorf("generate artifact ID: %w", err)
		}
		id := int64(0)
		for _, value := range bytes {
			id = (id << 8) | int64(value)
		}
		id &= 1<<53 - 1
		if id > 0 {
			if _, found := existing[id]; !found {
				return id, nil
			}
		}
	}
	return 0, errors.New("generate unique artifact ID")
}

func (s *Store) loadRecords(scope Scope) ([]Artifact, error) {
	directory := filepath.Join(s.scopeDirectory(scope), "records")
	entries, err := os.ReadDir(directory)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	records := make([]Artifact, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		record, err := readRecordFile(filepath.Join(directory, entry.Name()))
		if err != nil {
			return nil, err
		}
		if record.Scope != scope {
			return nil, errors.New("artifact record scope does not match its storage directory")
		}
		records = append(records, record)
	}
	return records, nil
}

func (s *Store) loadRecord(scope Scope, artifactID int64) (Artifact, error) {
	record, err := readRecordFile(s.recordPath(scope, artifactID))
	if errors.Is(err, fs.ErrNotExist) {
		return Artifact{}, ErrArtifactNotFound
	}
	if err != nil {
		return Artifact{}, err
	}
	if record.Scope != scope || record.ID != artifactID {
		return Artifact{}, ErrArtifactNotFound
	}
	return record, nil
}

func readRecordFile(path string) (Artifact, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Artifact{}, err
	}
	record := Artifact{}
	if err := json.Unmarshal(data, &record); err != nil {
		return Artifact{}, fmt.Errorf("decode artifact record: %w", err)
	}
	return record, nil
}

func (s *Store) writeRecord(record Artifact) error {
	directory := filepath.Join(s.scopeDirectory(record.Scope), "records")
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return err
	}
	data, err := json.Marshal(record)
	if err != nil {
		return err
	}
	temporary, err := os.CreateTemp(directory, ".record-")
	if err != nil {
		return err
	}
	temporaryName := temporary.Name()
	defer os.Remove(temporaryName)
	if err := temporary.Chmod(0o600); err != nil {
		temporary.Close()
		return err
	}
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
	if err := os.Rename(temporaryName, s.recordPath(record.Scope, record.ID)); err != nil {
		return err
	}
	return syncDirectory(directory)
}

func (s *Store) deleteRecord(record Artifact) error {
	recordPath := s.recordPath(record.Scope, record.ID)
	if err := os.Remove(recordPath); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	if err := syncDirectoryIfExists(filepath.Dir(recordPath)); err != nil {
		return err
	}
	blobPath := s.blobPath(record)
	if err := os.Remove(blobPath); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	if err := syncDirectoryIfExists(filepath.Dir(blobPath)); err != nil {
		return err
	}
	if err := os.RemoveAll(s.uploadDirectory(record)); err != nil {
		return err
	}
	return syncDirectoryIfExists(filepath.Dir(s.uploadDirectory(record)))
}

func (s *Store) finalizedBytes(scope Scope) (int64, error) {
	records, err := s.loadRecords(scope)
	if err != nil {
		return 0, err
	}
	total := int64(0)
	for _, record := range records {
		if record.State == artifactStateUploaded || record.State == artifactStateFinalized {
			total += record.Size
		}
	}
	return total, nil
}

func (s *Store) scopeStorageBytes(scope Scope) (int64, error) {
	total, err := directoryFileBytes(filepath.Join(s.scopeDirectory(scope), "uploads"))
	if err != nil {
		return 0, err
	}
	finalized, err := s.finalizedBytes(scope)
	if err != nil {
		return 0, err
	}
	return total + finalized, nil
}

func directoryFileBytes(root string) (int64, error) {
	total := int64(0)
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		if entry.Type().IsRegular() {
			info, err := entry.Info()
			if err != nil {
				return err
			}
			total += info.Size()
		}
		return nil
	})
	return total, err
}

func (s *Store) scopeDirectory(scope Scope) string {
	data, _ := json.Marshal(scope)
	digest := sha256.Sum256(data)
	return filepath.Join(s.root, "scopes", hex.EncodeToString(digest[:]))
}

func (s *Store) recordPath(scope Scope, artifactID int64) string {
	return filepath.Join(s.scopeDirectory(scope), "records", fmt.Sprintf("%d.json", artifactID))
}

func (s *Store) uploadDirectory(record Artifact) string {
	return filepath.Join(s.scopeDirectory(record.Scope), "uploads", fmt.Sprintf("%d", record.ID))
}

func (s *Store) blockDirectory(record Artifact) string {
	return filepath.Join(s.uploadDirectory(record), "blocks")
}

func (s *Store) blobPath(record Artifact) string {
	return filepath.Join(s.scopeDirectory(record.Scope), "blobs", fmt.Sprintf("%d.blob", record.ID))
}

func (s *Store) reservationKey(scope Scope, artifactID int64) string {
	return s.scopeDirectory(scope) + "/" + strconv.FormatInt(artifactID, 10)
}

func blockFileName(blockID string) string {
	digest := sha256.Sum256([]byte(blockID))
	return hex.EncodeToString(digest[:])
}

func validateArtifactName(name string) error {
	if name == "" || len(name) > 255 || strings.ContainsAny(name, "\"\\/:<>|*?\r\n\x00") {
		return fmt.Errorf("%w: artifact name is invalid", ErrInvalidArtifact)
	}
	for _, character := range name {
		if character < ' ' || character == '\x7f' {
			return fmt.Errorf("%w: artifact name is invalid", ErrInvalidArtifact)
		}
	}
	return nil
}

func validateBlob(path, mimeType string, limits Limits) error {
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	if info.Size() > limits.MaxArtifactBytes {
		return fmt.Errorf("%w: artifact exceeds %d bytes", ErrLimitExceeded, limits.MaxArtifactBytes)
	}
	if !isZipMIMEType(mimeType) {
		if info.Size() > limits.MaxFileBytes {
			return fmt.Errorf("%w: file exceeds %d bytes", ErrLimitExceeded, limits.MaxFileBytes)
		}
		return nil
	}
	reader, err := zip.OpenReader(path)
	if err != nil {
		return fmt.Errorf("%w: artifact is not a valid zip archive", ErrInvalidArtifact)
	}
	defer reader.Close()
	seen := map[string]bool{}
	total := int64(0)
	for _, file := range reader.File {
		name := file.Name
		clean := filepath.ToSlash(filepath.Clean(name))
		if name == "" || strings.HasPrefix(name, "/") || strings.ContainsAny(name, "\\:") || clean == "." || clean == ".." || strings.HasPrefix(clean, "../") ||
			clean != strings.TrimSuffix(name, "/") || strings.ContainsRune(name, '\x00') {
			return fmt.Errorf("%w: zip entry path %q is unsafe", ErrInvalidArtifact, name)
		}
		for _, character := range name {
			if character < ' ' || character == '\x7f' {
				return fmt.Errorf("%w: zip entry path %q is unsafe", ErrInvalidArtifact, name)
			}
		}
		if file.Mode()&os.ModeSymlink != 0 || !file.FileInfo().IsDir() && !file.Mode().IsRegular() {
			return fmt.Errorf("%w: zip entry %q is not a regular file or directory", ErrInvalidArtifact, name)
		}
		for parent := clean; parent != "." && parent != "/"; parent = filepath.ToSlash(filepath.Dir(parent)) {
			if seen[parent] && parent != clean {
				return fmt.Errorf("%w: zip entry %q descends from a file", ErrInvalidArtifact, name)
			}
		}
		if _, found := seen[clean]; found {
			return fmt.Errorf("%w: zip entry path %q is duplicated", ErrInvalidArtifact, name)
		}
		if !file.FileInfo().IsDir() {
			for existing := range seen {
				if strings.HasPrefix(existing, clean+"/") {
					return fmt.Errorf("%w: zip entry %q replaces a parent directory", ErrInvalidArtifact, name)
				}
			}
		}
		seen[clean] = !file.FileInfo().IsDir()
		if file.FileInfo().IsDir() {
			continue
		}
		if file.UncompressedSize64 > uint64(limits.MaxFileBytes) {
			return fmt.Errorf("%w: file %q exceeds %d bytes", ErrLimitExceeded, name, limits.MaxFileBytes)
		}
		entry, err := file.Open()
		if err != nil {
			return fmt.Errorf("%w: open zip entry %q: %v", ErrInvalidArtifact, name, err)
		}
		read, copyErr := io.Copy(io.Discard, io.LimitReader(entry, limits.MaxFileBytes+1))
		closeErr := entry.Close()
		if copyErr != nil || closeErr != nil {
			return fmt.Errorf("%w: read zip entry %q: %v", ErrInvalidArtifact, name, errors.Join(copyErr, closeErr))
		}
		if read > limits.MaxFileBytes {
			return fmt.Errorf("%w: file %q exceeds %d bytes", ErrLimitExceeded, name, limits.MaxFileBytes)
		}
		total += read
		if total > limits.MaxArtifactBytes {
			return fmt.Errorf("%w: uncompressed artifact exceeds %d bytes", ErrLimitExceeded, limits.MaxArtifactBytes)
		}
	}
	return nil
}

func isZipMIMEType(value string) bool {
	value = strings.ToLower(strings.TrimSpace(strings.Split(value, ";")[0]))
	return value == "application/zip" || value == "application/x-zip-compressed" || value == "application/zip-compressed"
}

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}

func syncDirectoryIfExists(path string) error {
	err := syncDirectory(path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	return err
}
