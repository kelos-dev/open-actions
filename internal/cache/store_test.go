package cache

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

var tenant = Tenant{ProjectUID: "project", RepositoryID: 1}

func newTestStore(t *testing.T) *Store {
	t.Helper()
	s, err := NewStore(t.TempDir(), Limits{MaxEntryBytes: 16, MaxRepositoryBytes: 32, MaxTotalBytes: 48, Retention: time.Hour, UploadRetention: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func saveTestEntry(t *testing.T, s *Store, tenant Tenant, ref, key, version, body string) Entry {
	t.Helper()
	e, err := s.Create(tenant, ref, "job", key, version)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Stage(tenant, e.ID, "", int64(len(body)), strings.NewReader(body)); err != nil {
		t.Fatal(err)
	}
	e, err = s.Finalize(tenant, ref, "job", key, version, int64(len(body)))
	if err != nil {
		t.Fatal(err)
	}
	return e
}

// Search priority and branch isolation follow GitHub's dependency caching reference.
func TestCacheLookupScopeAndKeyPriority(t *testing.T) {
	s := newTestStore(t)
	now := time.Now()
	s.now = func() time.Time { return now }
	for _, e := range []struct {
		tenant            Tenant
		ref, key, version string
	}{
		{tenant, "refs/heads/main", "npm-lock", "v1"},
		{tenant, "refs/heads/base", "npm-base", "v1"},
		{tenant, "refs/pull/1/merge", "npm-older", "v1"},
		{tenant, "refs/pull/1/merge", "npm-latest", "v1"},
		{tenant, "refs/heads/sibling", "npm-lock", "v1"},
		{Tenant{ProjectUID: "other", RepositoryID: 1}, "refs/pull/1/merge", "npm-lock", "v1"},
		{Tenant{ProjectUID: "project", RepositoryID: 2}, "refs/pull/1/merge", "npm-lock", "v1"},
		{tenant, "refs/pull/1/merge", "npm-lock", "v2"},
	} {
		saveTestEntry(t, s, e.tenant, e.ref, e.key, e.version, "x")
		now = now.Add(time.Second)
	}
	for _, tc := range []struct {
		name         string
		refs         []string
		key, version string
		restore      []string
		want         string
	}{
		{name: "current prefix precedes default exact", refs: []string{"refs/pull/1/merge", "refs/heads/base", "refs/heads/main"}, key: "npm-lock", version: "v1", restore: []string{"npm-"}, want: "npm-latest"},
		{name: "exact key precedes prefix", refs: []string{"refs/pull/1/merge"}, key: "npm-older", version: "v1", restore: []string{"npm-"}, want: "npm-older"},
		{name: "primary prefix", refs: []string{"refs/pull/1/merge"}, key: "npm-", version: "v1", want: "npm-latest"},
		{name: "ordered restore keys", refs: []string{"refs/pull/1/merge"}, key: "miss", version: "v1", restore: []string{"npm-older", "npm-"}, want: "npm-older"},
		{name: "base branch fallback", refs: []string{"refs/pull/2/merge", "refs/heads/base", "refs/heads/main"}, key: "miss", version: "v1", restore: []string{"npm-"}, want: "npm-base"},
		{name: "default fallback", refs: []string{"refs/heads/feature", "refs/heads/main"}, key: "npm-lock", version: "v1", want: "npm-lock"},
		{name: "version partition", refs: []string{"refs/pull/1/merge"}, key: "npm-lock", version: "v2", want: "npm-lock"},
		{name: "version miss", refs: []string{"refs/pull/1/merge"}, key: "npm-lock", version: "v3"},
		{name: "sibling is not visible", refs: []string{"refs/heads/feature"}, key: "npm-lock", version: "v1"},
		{name: "PR cache is not visible to main", refs: []string{"refs/heads/main"}, key: "npm-latest", version: "v1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e, err := s.Lookup(tenant, tc.refs, tc.key, tc.version, tc.restore)
			if tc.want == "" {
				if !errors.Is(err, ErrNotFound) {
					t.Fatalf("lookup = %#v, %v", e, err)
				}
				return
			}
			if err != nil || e.Key != tc.want {
				t.Fatalf("lookup = %#v, %v; want %q", e, err, tc.want)
			}
		})
	}
}

func TestCacheUploadPublicationAndOwnership(t *testing.T) {
	s := newTestStore(t)
	e, err := s.Create(tenant, "refs/heads/main", "owner", "key", "v")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Lookup(tenant, []string{e.Ref}, e.Key, e.Version, nil); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	if err := s.Stage(tenant, e.ID, "a", 3, strings.NewReader("abc")); err != nil {
		t.Fatal(err)
	}
	if err := s.Stage(tenant, e.ID, "b", 3, strings.NewReader("def")); err != nil {
		t.Fatal(err)
	}
	if err := s.Commit(tenant, e.ID, []string{"b", "a"}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Finalize(tenant, e.Ref, "other", e.Key, e.Version, 6); !errors.Is(err, ErrExists) {
		t.Fatal(err)
	}
	if _, err := s.Finalize(tenant, e.Ref, e.Owner, e.Key, e.Version, 5); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := s.Finalize(tenant, e.Ref, e.Owner, e.Key, e.Version, 6); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Finalize(tenant, e.Ref, e.Owner, e.Key, e.Version, 6); err != nil {
		t.Fatal(err)
	}
	if err := s.Stage(tenant, e.ID, "", 3, strings.NewReader("bad")); !errors.Is(err, ErrExists) {
		t.Fatal(err)
	}
	if _, err := s.Create(tenant, e.Ref, "other", e.Key, e.Version); !errors.Is(err, ErrExists) {
		t.Fatal(err)
	}
	if _, err := s.Get(Tenant{ProjectUID: "another", RepositoryID: 1}, e.ID); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	f, _, err := s.Open(tenant, e.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	body, err := io.ReadAll(f)
	if err != nil || string(body) != "defabc" {
		t.Fatalf("body=%q err=%v", body, err)
	}
}

func TestCacheCapacityEvictionRetentionAndRestart(t *testing.T) {
	s := newTestStore(t)
	now := time.Now()
	s.now = func() time.Time { return now }
	first := saveTestEntry(t, s, tenant, "refs/heads/main", "first", "v", strings.Repeat("a", 16))
	now = now.Add(time.Second)
	second := saveTestEntry(t, s, tenant, "refs/heads/main", "second", "v", strings.Repeat("b", 16))
	now = now.Add(time.Second)
	if _, err := s.Lookup(tenant, []string{first.Ref}, first.Key, first.Version, nil); err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Second)
	third := saveTestEntry(t, s, tenant, "refs/heads/main", "third", "v", strings.Repeat("c", 16))
	if _, err := s.Get(tenant, second.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("least recently used entry: %v", err)
	}
	restarted, err := NewStore(s.root, s.limits)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := restarted.Lookup(tenant, []string{first.Ref}, first.Key, first.Version, nil); err != nil {
		t.Fatal(err)
	}
	pending, err := s.Create(tenant, "refs/heads/main", "job", "pending", "v")
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Minute)
	if err := s.Cleanup(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Get(tenant, pending.ID); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	now = now.Add(time.Hour)
	if err := s.Cleanup(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Get(tenant, third.ID); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	files, err := os.ReadDir(s.root)
	if err != nil || len(files) != 0 {
		t.Fatalf("files=%v err=%v", files, err)
	}
}

func TestCacheGlobalCapacityAndUploadLimits(t *testing.T) {
	s := newTestStore(t)
	for i, project := range []string{"a", "b", "c", "d"} {
		s.now = func() time.Time { return time.Now().Add(time.Duration(i) * time.Second) }
		saveTestEntry(t, s, Tenant{ProjectUID: project, RepositoryID: 1}, "refs/heads/main", "key", "v", strings.Repeat(project, 16))
	}
	if len(s.entries) != 3 {
		t.Fatalf("entries=%d", len(s.entries))
	}
	e, err := s.Create(tenant, "refs/heads/main", "job", "key", "v")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Stage(tenant, e.ID, "", 17, strings.NewReader(strings.Repeat("x", 17))); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if err := s.Stage(tenant, e.ID, "", 2, strings.NewReader("short")); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if err := s.Stage(tenant, e.ID, "", 0, strings.NewReader("")); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Finalize(tenant, e.Ref, e.Owner, e.Key, e.Version, 0); err != nil {
		t.Fatal(err)
	}
}

func TestCacheConcurrentReservations(t *testing.T) {
	s := newTestStore(t)
	var wg sync.WaitGroup
	var mu sync.Mutex
	successes := 0
	for range 20 {
		wg.Go(func() {
			_, err := s.Create(tenant, "refs/heads/main", "job", "key", "v")
			if err == nil {
				mu.Lock()
				successes++
				mu.Unlock()
			} else if !errors.Is(err, ErrExists) {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	if successes != 1 {
		t.Fatalf("successful reservations=%d", successes)
	}
}

func TestCacheConfigurationAndMetadataValidation(t *testing.T) {
	for _, l := range []Limits{{}, {MaxEntryBytes: 2, MaxRepositoryBytes: 1, MaxTotalBytes: 3, Retention: time.Hour, UploadRetention: time.Hour}} {
		if _, err := NewStore(t.TempDir(), l); err == nil {
			t.Fatal("invalid limits accepted")
		}
	}
	for _, key := range []string{"", strings.Repeat("k", 513), "a,b"} {
		if err := ValidateKey(key, "v"); err == nil {
			t.Fatalf("invalid key %q accepted", key)
		}
	}
	if err := ValidateKey(strings.Repeat("é", 512), "v"); err != nil {
		t.Fatal(err)
	}
	s := newTestStore(t)
	if err := os.Mkdir(filepath.Join(s.root, "invalid"), 0700); err != nil {
		t.Fatal(err)
	}
	if _, err := NewStore(s.root, s.limits); err == nil {
		t.Fatal("invalid storage accepted")
	}
}

func TestCacheUploadRetriesAndInterruptedWrites(t *testing.T) {
	s := newTestStore(t)
	e, err := s.Create(tenant, "refs/heads/main", "job", "key", "v")
	if err != nil {
		t.Fatal(err)
	}
	body := strings.Repeat("a", 16)
	for range 2 {
		if err := s.Stage(tenant, e.ID, "block", 16, strings.NewReader(body)); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Commit(tenant, e.ID, []string{"block"}); err != nil {
		t.Fatal(err)
	}
	if err := s.Commit(tenant, e.ID, []string{"block"}); err != nil {
		t.Fatal(err)
	}
	single, err := s.Create(tenant, "refs/heads/feature", "job", "single", "v")
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := s.Stage(tenant, single.ID, "", 4, strings.NewReader("data")); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Stage(tenant, single.ID, "", 4, strings.NewReader("evil")); !errors.Is(err, ErrExists) {
		t.Fatal(err)
	}
	pending, err := s.Create(tenant, "refs/heads/main", "job", "pending", "v")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Stage(tenant, pending.ID, "block", 4, strings.NewReader("data")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(s.blockPath(pending.ID, "block"), []byte("interrupted block replacement"), 0600); err != nil {
		t.Fatal(err)
	}
	orphan := filepath.Join(s.directory(e.ID), ".incoming-interrupted")
	if err := os.WriteFile(orphan, []byte("orphan"), 0600); err != nil {
		t.Fatal(err)
	}
	restarted, err := NewStore(s.root, s.limits)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(orphan); !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	if _, err := restarted.Get(tenant, pending.ID); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	if _, err := restarted.Get(tenant, e.ID); err != nil {
		t.Fatal(err)
	}
}

type cacheReaderFunc func([]byte) (int, error)

func (f cacheReaderFunc) Read(p []byte) (int, error) { return f(p) }

func TestCacheActiveUploadsHoldCapacityReservations(t *testing.T) {
	s := newTestStore(t)
	e, err := s.Create(tenant, "refs/heads/main", "job", "key", "v")
	if err != nil {
		t.Fatal(err)
	}
	entered, release, finished := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	var once sync.Once
	reader := cacheReaderFunc(func(p []byte) (int, error) { once.Do(func() { close(entered); <-release }); return 0, io.EOF })
	go func() { finished <- s.Stage(tenant, e.ID, "a", 16, reader) }()
	<-entered
	if err := s.Stage(tenant, e.ID, "b", 1, strings.NewReader("x")); !errors.Is(err, ErrLimit) {
		t.Fatalf("reserved bytes not enforced: %v", err)
	}
	close(release)
	if err := <-finished; !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if err := s.Stage(tenant, e.ID, "a", 16, strings.NewReader(strings.Repeat("a", 16))); err != nil {
		t.Fatal(err)
	}
}

func TestCacheEvictionPlansRepositoryAndTotalCapacity(t *testing.T) {
	other := Tenant{ProjectUID: tenant.ProjectUID, RepositoryID: 2}
	type fixtureEntry struct {
		tenant    Tenant
		key       string
		size      int64
		finalized bool
	}
	for _, test := range []struct {
		name    string
		total   int64
		entries []fixtureEntry
		evicted []string
		limited bool
	}{
		{name: "both quotas require eviction", total: 20, entries: []fixtureEntry{
			{other, "other", 8, true}, {tenant, "own", 8, true},
		}, evicted: []string{"own", "other"}},
		{name: "global eviction selects oldest other entry", total: 28, entries: []fixtureEntry{
			{other, "other-old", 8, true}, {other, "other-new", 8, true}, {tenant, "own", 8, true},
		}, evicted: []string{"own", "other-old"}},
		{name: "repository eviction also satisfies total", total: 28, entries: []fixtureEntry{
			{other, "other", 8, true}, {tenant, "own", 8, true},
		}, evicted: []string{"own"}},
		{name: "pending uploads prevent total capacity", total: 20, entries: []fixtureEntry{
			{other, "pending", 8, false}, {tenant, "own", 8, true},
		}, limited: true},
		{name: "pending uploads prevent repository capacity", total: 32, entries: []fixtureEntry{
			{other, "other", 4, true}, {tenant, "pending", 8, false}, {tenant, "own", 4, true},
		}, limited: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			limits := Limits{MaxEntryBytes: 16, Retention: time.Hour, UploadRetention: time.Minute}
			limits.MaxRepositoryBytes = 20
			limits.MaxTotalBytes = test.total
			s, err := NewStore(t.TempDir(), limits)
			if err != nil {
				t.Fatal(err)
			}
			now := time.Now()
			s.now = func() time.Time { return now }
			entries := []Entry{}
			for _, fixture := range test.entries {
				e, err := s.Create(fixture.tenant, "refs/heads/main", "job", fixture.key, "v")
				if err != nil {
					t.Fatal(err)
				}
				if err := s.Stage(e.Tenant, e.ID, "", fixture.size, strings.NewReader(strings.Repeat("x", int(fixture.size)))); err != nil {
					t.Fatal(err)
				}
				if fixture.finalized {
					if _, err := s.Finalize(e.Tenant, e.Ref, e.Owner, e.Key, e.Version, fixture.size); err != nil {
						t.Fatal(err)
					}
				}
				entries = append(entries, e)
				now = now.Add(time.Second)
			}
			incoming, err := s.Create(tenant, "refs/heads/main", "job", "incoming", "v")
			if err != nil {
				t.Fatal(err)
			}
			err = s.Stage(tenant, incoming.ID, "", 16, strings.NewReader(strings.Repeat("x", 16)))
			if test.limited {
				if !errors.Is(err, ErrLimit) {
					t.Fatalf("upload error = %v, want capacity limit", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			for _, e := range entries {
				evicted := slices.Contains(test.evicted, e.Key)
				_, err := s.Get(e.Tenant, e.ID)
				if evicted {
					if !errors.Is(err, ErrNotFound) {
						t.Fatalf("cache %q was not evicted: %v", e.Key, err)
					}
				} else {
					if err != nil {
						t.Fatalf("cache %q should be retained: %v", e.Key, err)
					}
					if _, err := os.Stat(filepath.Join(s.directory(e.ID), "blob")); err != nil {
						t.Fatalf("cache %q blob should be retained: %v", e.Key, err)
					}
				}
			}
			if !test.limited {
				if _, err := s.Finalize(tenant, incoming.Ref, incoming.Owner, incoming.Key, incoming.Version, 16); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestCacheCommitAllowsConcurrentOperations(t *testing.T) {
	s := newTestStore(t)
	other := saveTestEntry(t, s, tenant, "refs/heads/main", "other", "v", "data")
	e, err := s.Create(tenant, "refs/heads/main", "job", "committing", "v")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Stage(tenant, e.ID, "block", 4, strings.NewReader("blob")); err != nil {
		t.Fatal(err)
	}
	started, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	result := make(chan error, 1)
	probeDone := make(chan struct{})
	probeLaunched := false
	var releaseOnce sync.Once
	s.copyBlock = func(destination io.Writer, source io.Reader) (int64, error) {
		if source.(*os.File).Name() == s.blockPath(e.ID, "block") {
			close(started)
			<-release
		}
		return io.Copy(destination, source)
	}
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(release) })
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("cache commit did not finish")
		}
		if probeLaunched {
			select {
			case <-probeDone:
			case <-time.After(10 * time.Second):
				t.Error("cache probe did not finish")
			}
		}
	})
	go func() { defer close(done); result <- s.Commit(tenant, e.ID, []string{"block"}) }()
	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("cache assembly did not start")
	}
	probe := make(chan error, 1)
	probeLaunched = true
	go func() {
		defer close(probeDone)
		probe <- func() error {
			s.mu.Lock()
			expired := time.Now().Add(2 * time.Minute)
			s.now = func() time.Time { return expired }
			s.mu.Unlock()
			if err := s.Cleanup(); err != nil {
				return err
			}
			if _, err := s.Get(tenant, e.ID); err != nil {
				return fmt.Errorf("cleanup removed active commit: %w", err)
			}
			if err := s.Stage(tenant, e.ID, "block", 4, strings.NewReader("evil")); !errors.Is(err, ErrExists) {
				return fmt.Errorf("stage during commit: %v", err)
			}
			if err := s.Commit(tenant, e.ID, []string{"block"}); !errors.Is(err, ErrInvalid) {
				return fmt.Errorf("concurrent commit: %v", err)
			}
			if _, err := s.Finalize(tenant, e.Ref, e.Owner, e.Key, e.Version, 4); !errors.Is(err, ErrInvalid) {
				return fmt.Errorf("finalize during assembly: %v", err)
			}
			if _, err := s.Lookup(tenant, []string{e.Ref}, e.Key, e.Version, nil); !errors.Is(err, ErrNotFound) {
				return fmt.Errorf("active commit is visible: %v", err)
			}
			if _, err := s.Lookup(tenant, []string{other.Ref}, other.Key, other.Version, nil); err != nil {
				return err
			}
			file, _, err := s.Open(tenant, other.ID)
			if err != nil {
				return err
			}
			body, readErr := io.ReadAll(file)
			closeErr := file.Close()
			if err := errors.Join(readErr, closeErr); err != nil {
				return err
			}
			if string(body) != "data" {
				return fmt.Errorf("other cache body = %q", body)
			}
			parallel, err := s.Create(tenant, "refs/heads/main", "job", "parallel", "v")
			if err != nil {
				return err
			}
			if err := s.Stage(tenant, parallel.ID, "block", 4, strings.NewReader("next")); err != nil {
				return err
			}
			if err := s.Commit(tenant, parallel.ID, []string{"block"}); err != nil {
				return err
			}
			_, err = s.Finalize(tenant, parallel.Ref, parallel.Owner, parallel.Key, parallel.Version, 4)
			return err
		}()
	}()
	select {
	case err := <-probe:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("cache operations blocked on another entry's assembly")
	}
	releaseOnce.Do(func() { close(release) })
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("cache commit did not finish")
	}
	if _, err := s.Finalize(tenant, e.Ref, e.Owner, e.Key, e.Version, 4); err != nil {
		t.Fatal(err)
	}
	file, _, err := s.Open(tenant, e.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	body, err := io.ReadAll(file)
	if err != nil || string(body) != "blob" {
		t.Fatalf("committed blob = %q, error = %v", body, err)
	}
}

func TestCacheFailedAssemblyReleasesCommit(t *testing.T) {
	s := newTestStore(t)
	e, err := s.Create(tenant, "refs/heads/main", "job", "key", "v")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Stage(tenant, e.ID, "block", 4, strings.NewReader("data")); err != nil {
		t.Fatal(err)
	}
	failure := errors.New("copy failed")
	s.copyBlock = func(io.Writer, io.Reader) (int64, error) { return 0, failure }
	if err := s.Commit(tenant, e.ID, []string{"block"}); !errors.Is(err, failure) {
		t.Fatal(err)
	}
	if err := s.Stage(tenant, e.ID, "block", 4, strings.NewReader("next")); err != nil {
		t.Fatal(err)
	}
	s.copyBlock = io.Copy
	if err := s.Commit(tenant, e.ID, []string{"block"}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Finalize(tenant, e.Ref, e.Owner, e.Key, e.Version, 4); err != nil {
		t.Fatal(err)
	}
	files, err := os.ReadDir(s.directory(e.ID))
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range files {
		if strings.HasPrefix(file.Name(), ".incoming-") {
			t.Fatalf("temporary cache file %q was retained", file.Name())
		}
	}
}

func TestCacheRestartRecoversInterruptedEviction(t *testing.T) {
	s := newTestStore(t)
	now := time.Now()
	s.now = func() time.Time { return now }
	evicted := saveTestEntry(t, s, tenant, "refs/heads/main", "old", "v", strings.Repeat("a", 16))
	now = now.Add(time.Second)
	retained := saveTestEntry(t, s, tenant, "refs/heads/main", "retained", "v", strings.Repeat("b", 16))
	now = now.Add(time.Second)
	incoming, err := s.Create(tenant, "refs/heads/main", "job", "incoming", "v")
	if err != nil {
		t.Fatal(err)
	}
	interrupted := errors.New("directory removal interrupted")
	s.removeDirectory = func(path string) error {
		if path != s.directory(evicted.ID) {
			return fmt.Errorf("unexpected removal of %q", path)
		}
		if _, err := os.Stat(filepath.Join(path, "entry.json")); !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("metadata must be removed before blob deletion: %v", err)
		}
		if err := os.Remove(filepath.Join(path, "blob")); err != nil {
			return err
		}
		return interrupted
	}
	if err := s.Stage(tenant, incoming.ID, "", 16, strings.NewReader(strings.Repeat("c", 16))); !errors.Is(err, interrupted) {
		t.Fatalf("eviction error = %v", err)
	}
	if _, err := s.Get(tenant, evicted.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("partially removed cache is still visible: %v", err)
	}
	restarted, err := NewStore(s.root, s.limits)
	if err != nil {
		t.Fatalf("restart after interrupted eviction: %v", err)
	}
	if _, err := os.Stat(s.directory(evicted.ID)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("partially removed directory remains: %v", err)
	}
	restored, err := restarted.Lookup(tenant, []string{retained.Ref}, retained.Key, retained.Version, nil)
	if err != nil || restored.ID != retained.ID {
		t.Fatalf("retained cache lookup = %#v, %v", restored, err)
	}
	file, _, err := restarted.Open(tenant, retained.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	body, err := io.ReadAll(file)
	if err != nil || string(body) != strings.Repeat("b", 16) {
		t.Fatalf("retained cache contents = %q, error = %v", body, err)
	}
}
