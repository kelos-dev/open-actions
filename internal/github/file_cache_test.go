package github

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fileCacheFixture struct {
	client        *Client
	privateKey    []byte
	now           atomic.Int64
	tokenRequests atomic.Int32
	fileRequests  atomic.Int32
	statusCode    atomic.Int32
}

func newFileCacheFixture(t *testing.T) *fileCacheFixture {
	t.Helper()
	f := &fileCacheFixture{privateKey: testPrivateKey(t)}
	f.now.Store(time.Now().UnixNano())
	f.statusCode.Store(http.StatusOK)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch {
		case request.Method == http.MethodPost:
			id := f.tokenRequests.Add(1)
			fmt.Fprintf(writer, `{"token":"token-%d","expires_at":%q}`, id, time.Unix(0, f.now.Load()).Add(time.Hour).UTC().Format(time.RFC3339))
		case request.Method == http.MethodDelete:
			writer.WriteHeader(http.StatusNoContent)
		default:
			id := f.fileRequests.Add(1)
			if code := int(f.statusCode.Load()); code != http.StatusOK {
				http.Error(writer, "file unavailable", code)
				return
			}
			fmt.Fprintf(writer, `{"encoding":"base64","content":%q}`, base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("file-%d", id))))
		}
	}))
	t.Cleanup(server.Close)
	var err error
	f.client, err = NewClient(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	f.client.now = func() time.Time { return time.Unix(0, f.now.Load()) }
	return f
}

func (f *fileCacheFixture) installation(t *testing.T) *InstallationClient {
	t.Helper()
	installation, err := f.client.CachedInstallation(t.Context(), 1, 2, f.privateKey, "example", InstallationPermissions{"contents": "read"})
	if err != nil {
		t.Fatal(err)
	}
	return installation
}

func TestGetFileCachesOnlyPinnedControlPlaneReads(t *testing.T) {
	for _, test := range []struct {
		name, revision string
		jobToken       bool
		wantRequests   int32
	}{
		{name: "full commit", revision: strings.Repeat("a", 40), wantRequests: 1},
		{name: "branch", revision: "main", wantRequests: 2},
		{name: "tag", revision: "v1", wantRequests: 2},
		{name: "short commit", revision: "aaaaaaa", wantRequests: 2},
		{name: "commit expression", revision: strings.Repeat("a", 40) + "~1", wantRequests: 2},
		{name: "default branch", wantRequests: 2},
		{name: "non-hex revision", revision: strings.Repeat("z", 40), wantRequests: 2},
		{name: "job token", revision: strings.Repeat("a", 40), jobToken: true, wantRequests: 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newFileCacheFixture(t)
			installation := f.installation(t)
			if test.jobToken {
				var err error
				installation, err = f.client.Installation(t.Context(), 1, 2, f.privateKey, "example", InstallationPermissions{"contents": "read"})
				if err != nil {
					t.Fatal(err)
				}
			}
			for range 2 {
				if _, err := installation.GetFile(t.Context(), "acme", "example", "ci.yaml", test.revision); err != nil {
					t.Fatal(err)
				}
			}
			if got := f.fileRequests.Load(); got != test.wantRequests {
				t.Fatalf("file requests = %d, want %d", got, test.wantRequests)
			}
		})
	}
}

func TestGetFileCacheKeys(t *testing.T) {
	f := newFileCacheFixture(t)
	installation := f.installation(t)
	sha := strings.Repeat("a", 40)
	read := func(installation *InstallationClient, owner, repository, path, revision string, wantRequests int32) {
		t.Helper()
		for range 2 {
			data, err := installation.GetFile(t.Context(), owner, repository, path, revision)
			if err != nil || string(data) != fmt.Sprintf("file-%d", wantRequests) || f.fileRequests.Load() != wantRequests {
				t.Fatalf("GetFile() = %q, %v; requests = %d, want %d", data, err, f.fileRequests.Load(), wantRequests)
			}
		}
	}
	read(installation, "acme", "example", "ci.yaml", sha, 1)
	read(f.installation(t), "acme", "example", "ci.yaml", sha, 1)
	read(installation, "other", "example", "ci.yaml", sha, 2)
	read(installation, "acme", "other", "ci.yaml", sha, 3)
	read(installation, "acme", "example", "other.yaml", sha, 4)
	read(installation, "acme", "example", "ci.yaml", strings.Repeat("b", 40), 5)
	otherPermissions, err := f.client.CachedInstallation(t.Context(), 1, 2, f.privateKey, "example", InstallationPermissions{"contents": "write"})
	if err != nil {
		t.Fatal(err)
	}
	read(otherPermissions, "acme", "example", "ci.yaml", sha, 6)
	otherInstallation, err := f.client.CachedInstallation(t.Context(), 1, 3, f.privateKey, "example", InstallationPermissions{"contents": "read"})
	if err != nil {
		t.Fatal(err)
	}
	read(otherInstallation, "acme", "example", "ci.yaml", sha, 7)
}

func TestGetFileCacheStopsReusingInactiveTokens(t *testing.T) {
	for _, reason := range []string{"expiration", "revocation", "unauthorized response"} {
		t.Run(reason, func(t *testing.T) {
			f := newFileCacheFixture(t)
			installation := f.installation(t)
			sha := strings.Repeat("a", 40)
			if _, err := installation.GetFile(t.Context(), "acme", "example", "ci.yaml", sha); err != nil {
				t.Fatal(err)
			}
			f.statusCode.Store(http.StatusUnauthorized)
			switch reason {
			case "expiration":
				f.now.Add(int64(time.Hour))
			case "revocation":
				if err := installation.Revoke(t.Context()); err != nil {
					t.Fatal(err)
				}
			case "unauthorized response":
				if _, err := installation.GetRepository(t.Context(), "acme", "example"); err == nil {
					t.Fatal("unauthorized repository request succeeded")
				}
			}
			before := f.fileRequests.Load()
			if _, err := installation.GetFile(t.Context(), "acme", "example", "ci.yaml", sha); err == nil || f.fileRequests.Load() != before+1 {
				t.Fatalf("inactive token read cached content: error = %v, requests = %d", err, f.fileRequests.Load())
			}
			f.statusCode.Store(http.StatusOK)
			refreshed := f.installation(t)
			if _, err := refreshed.GetFile(t.Context(), "acme", "example", "ci.yaml", sha); err != nil {
				t.Fatal(err)
			}
			if f.tokenRequests.Load() != 2 || f.fileRequests.Load() != before+2 {
				t.Fatalf("refreshed token reused cached content: token requests = %d, file requests = %d", f.tokenRequests.Load(), f.fileRequests.Load())
			}
		})
	}
}

func TestGetFileDoesNotCacheFailures(t *testing.T) {
	f := newFileCacheFixture(t)
	installation := f.installation(t)
	sha := strings.Repeat("a", 40)
	f.statusCode.Store(http.StatusServiceUnavailable)
	if _, err := installation.GetFile(t.Context(), "acme", "example", "ci.yaml", sha); err == nil {
		t.Fatal("unavailable file request succeeded")
	}
	f.statusCode.Store(http.StatusOK)
	for range 2 {
		if _, err := installation.GetFile(t.Context(), "acme", "example", "ci.yaml", sha); err != nil {
			t.Fatal(err)
		}
	}
	if f.fileRequests.Load() != 2 {
		t.Fatalf("file requests = %d, want 2", f.fileRequests.Load())
	}
}

func TestFileCacheBoundsAndCopies(t *testing.T) {
	for _, test := range []struct {
		name              string
		maxEntries, bytes int
	}{
		{name: "entry limit", maxEntries: 2, bytes: 100},
		{name: "byte limit", maxEntries: 10, bytes: 6},
	} {
		t.Run(test.name, func(t *testing.T) {
			cache := newFileCache(test.maxEntries, test.bytes)
			loads := 0
			read := func(path string) []byte {
				t.Helper()
				data, err := cache.load(t.Context(), fileCacheKey{path: path}, func(context.Context) ([]byte, error) {
					loads++
					return []byte("abc"), nil
				})
				if err != nil || string(data) != "abc" {
					t.Fatalf("cached data = %q, %v", data, err)
				}
				return data
			}
			data := read("a")
			data[0] = 'x'
			read("b")
			data = read("a")
			data[0] = 'y'
			read("c")
			read("a")
			if loads != 3 {
				t.Fatalf("recent file was evicted or mutated: loads = %d", loads)
			}
			read("b")
			if loads != 4 {
				t.Fatalf("least recently used file was retained: loads = %d", loads)
			}
		})
	}
	cache := newFileCache(2, 2)
	loads := 0
	for range 2 {
		if _, err := cache.load(t.Context(), fileCacheKey{path: "large"}, func(context.Context) ([]byte, error) {
			loads++
			return []byte("abc"), nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	if loads != 2 {
		t.Fatal("oversized file was cached")
	}
}

func TestFileCacheSharesConcurrentRequests(t *testing.T) {
	cache := newFileCache(2, 10)
	started, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	var loads atomic.Int32
	read := func(ctx context.Context) ([]byte, error) {
		return cache.load(ctx, fileCacheKey{path: "ci.yaml"}, func(context.Context) ([]byte, error) {
			if loads.Add(1) == 1 {
				close(started)
			}
			<-release
			return []byte("abc"), nil
		})
	}
	results := make(chan []byte, 11)
	go func() { data, _ := read(t.Context()); results <- data }()
	<-started
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := read(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled waiter: %v", err)
	}
	for range 10 {
		go func() { data, _ := read(t.Context()); results <- data }()
	}
	unblock()
	for range 11 {
		data := <-results
		if string(data) != "abc" {
			t.Fatalf("shared result = %q", data)
		}
		data[0] = 'x'
	}
	if loads.Load() != 1 {
		t.Fatalf("shared request loads = %d, want 1", loads.Load())
	}
}
