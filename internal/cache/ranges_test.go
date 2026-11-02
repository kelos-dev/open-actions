package cache

import (
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
)

func TestCacheRangeUpload(t *testing.T) {
	s := newTestStore(t)
	e, err := s.Create(tenant, "refs/heads/main", "job", "key", "v")
	if err != nil {
		t.Fatal(err)
	}
	var uploads sync.WaitGroup
	for offset, data := range map[int64]string{0: "abcd", 4: "efgh", 8: "ijkl"} {
		uploads.Go(func() {
			if err := s.StageRange(tenant, e.ID, offset, int64(len(data)), strings.NewReader(data)); err != nil {
				t.Error(err)
			}
		})
	}
	uploads.Wait()
	if err := s.StageRange(tenant, e.ID, 4, 4, strings.NewReader("efgh")); err != nil {
		t.Fatal(err)
	}
	s, err = NewStore(s.root, s.limits)
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := s.CommitRanges(tenant, e.ID, 12); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.Lookup(tenant, []string{e.Ref}, e.Key, e.Version, nil); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unfinalized upload is visible: %v", err)
	}
	if _, err := s.Finalize(tenant, e.Ref, e.Owner, e.Key, e.Version, 12); err != nil {
		t.Fatal(err)
	}
	f, _, err := s.Open(tenant, e.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	data, err := io.ReadAll(f)
	if err != nil || string(data) != "abcdefghijkl" {
		t.Fatalf("data=%q error=%v", data, err)
	}
	if err := s.StageRange(tenant, e.ID, 0, 4, strings.NewReader("evil")); !errors.Is(err, ErrExists) {
		t.Fatalf("overwrite error=%v", err)
	}
	if err := s.CommitRanges(tenant, e.ID, 11); !errors.Is(err, ErrInvalid) {
		t.Fatalf("commit with incorrect size: %v", err)
	}
}

func TestCacheRangeCommitRequiresCompleteCoverage(t *testing.T) {
	for _, tc := range []struct {
		name   string
		ranges map[int64]string
		size   int64
		valid  bool
	}{
		{name: "empty", size: 0, valid: true},
		{name: "no upload", size: 4},
		{name: "missing start", ranges: map[int64]string{4: "data"}, size: 8},
		{name: "gap", ranges: map[int64]string{0: "ab", 4: "ef"}, size: 6},
		{name: "overlap", ranges: map[int64]string{0: "abcd", 2: "cdef"}, size: 6},
		{name: "short", ranges: map[int64]string{0: "abc"}, size: 4},
		{name: "excess", ranges: map[int64]string{0: "abcde"}, size: 4},
		{name: "negative size", size: -1},
		{name: "too large", size: 17},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newTestStore(t)
			e, err := s.Create(tenant, "refs/heads/main", "job", "key", "v")
			if err != nil {
				t.Fatal(err)
			}
			for offset, data := range tc.ranges {
				if err := s.StageRange(tenant, e.ID, offset, int64(len(data)), strings.NewReader(data)); err != nil {
					t.Fatal(err)
				}
			}
			err = s.CommitRanges(tenant, e.ID, tc.size)
			if tc.valid {
				if err != nil {
					t.Fatal(err)
				}
			} else if !errors.Is(err, ErrInvalid) {
				t.Fatalf("commit error=%v", err)
			}
		})
	}
}

func TestCacheRangeUploadValidationAndQuota(t *testing.T) {
	s := newTestStore(t)
	e, err := s.Create(tenant, "refs/heads/main", "job", "key", "v")
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ offset, size int64 }{{-1, 1}, {0, 0}, {0, -1}, {16, 1}, {1<<63 - 1, 1}, {0, 17}} {
		if err := s.StageRange(tenant, e.ID, tc.offset, tc.size, strings.NewReader("x")); !errors.Is(err, ErrInvalid) {
			t.Fatalf("range=%#v error=%v", tc, err)
		}
	}
	for _, body := range []string{"a", "abc"} {
		if err := s.StageRange(tenant, e.ID, 0, 2, strings.NewReader(body)); !errors.Is(err, ErrInvalid) {
			t.Fatalf("body=%q error=%v", body, err)
		}
	}
	if err := s.StageRange(tenant, e.ID, 0, 16, strings.NewReader(strings.Repeat("a", 16))); err != nil {
		t.Fatal(err)
	}
	if err := s.StageRange(tenant, e.ID, 1, 1, strings.NewReader("b")); !errors.Is(err, ErrLimit) {
		t.Fatalf("quota error=%v", err)
	}
}

func TestCacheRangeCommitRejectsActiveUploads(t *testing.T) {
	s := newTestStore(t)
	e, err := s.Create(tenant, "refs/heads/main", "job", "key", "v")
	if err != nil {
		t.Fatal(err)
	}
	entered, release, finished := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	var once sync.Once
	source := strings.NewReader("data")
	reader := cacheReaderFunc(func(p []byte) (int, error) {
		once.Do(func() { close(entered); <-release })
		return source.Read(p)
	})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	go func() { finished <- s.StageRange(tenant, e.ID, 0, 4, reader) }()
	<-entered
	if err := s.CommitRanges(tenant, e.ID, 0); !errors.Is(err, ErrInvalid) {
		t.Fatalf("empty commit while uploading: %v", err)
	}
	if err := s.CommitRanges(tenant, e.ID, 4); !errors.Is(err, ErrInvalid) {
		t.Fatalf("commit while uploading: %v", err)
	}
	releaseOnce.Do(func() { close(release) })
	if err := <-finished; err != nil {
		t.Fatal(err)
	}
	if err := s.CommitRanges(tenant, e.ID, 4); err != nil {
		t.Fatal(err)
	}
}
