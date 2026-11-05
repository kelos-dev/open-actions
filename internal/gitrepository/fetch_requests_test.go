package gitrepository

import (
	"net/http"
	"net/http/cgi"
	"net/http/httptest"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
)

func gitHTTPServer(t *testing.T, remote string) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	gitPath, err := exec.LookPath("git")
	if err != nil {
		t.Fatal(err)
	}
	runGit(t, remote, "config", "uploadpack.allowReachableSHA1InWant", "true")
	backend := &cgi.Handler{Path: gitPath, Args: []string{"http-backend"}, Env: []string{
		"GIT_PROJECT_ROOT=" + filepath.Dir(filepath.Dir(remote)), "GIT_HTTP_EXPORT_ALL=1",
	}}
	requests := &atomic.Int32{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		backend.ServeHTTP(w, r)
	}))
	t.Cleanup(server.Close)
	return server, requests
}

func TestMergeFetchRequestCountAndRevision(t *testing.T) {
	remote, revision := testRepository(t, false)
	server, requests := gitHTTPServer(t, remote)
	c, err := NewClient(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	reference := t.TempDir()
	runGit(t, "", "clone", "--quiet", remote, reference)
	expectedSHA, err := c.createMergeCommit(t.Context(), reference, nil, revision)
	if err != nil {
		t.Fatal(err)
	}
	merged, err := c.Merge(t.Context(), "acme", "example", "token", revision)
	if err != nil {
		t.Fatal(err)
	}
	defer merged.Close()
	if merged.SHA != expectedSHA {
		t.Fatalf("merge revision = %s, want %s", merged.SHA, expectedSHA)
	}
	t.Logf("merge Git HTTP requests = %d", requests.Load())
	if requests.Load() != 2 {
		t.Errorf("merge Git HTTP requests = %d, want 2", requests.Load())
	}
	packs, err := filepath.Glob(filepath.Join(merged.directory, ".git", "objects", "pack", "*.pack"))
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("merge fetched pack files = %d", len(packs))
	if len(packs) != 1 {
		t.Errorf("merge fetched pack files = %d, want 1", len(packs))
	}
	data, err := merged.ReadFile(t.Context(), ".open-actions/workflows/head.yaml")
	if err != nil || string(data) != "name: Head\n" {
		t.Fatalf("workflow bytes = %q, %v", data, err)
	}
}

func TestChangedFilesFetchRequestCounts(t *testing.T) {
	remote, revision := testRepository(t, false)
	server, requests := gitHTTPServer(t, remote)
	c, err := NewClient(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name     string
		revision DiffRevision
		want     []string
	}{
		{name: "two trees", revision: DiffRevision{BaseSHA: revision.BaseSHA, HeadSHA: revision.HeadSHA}, want: []string{".open-actions/workflows/base.yaml", ".open-actions/workflows/head.yaml", ".open-actions/workflows/link.yaml"}},
		{name: "merge base", revision: DiffRevision{BaseSHA: revision.MergeBaseSHA, HeadSHA: revision.HeadSHA}, want: []string{".open-actions/workflows/head.yaml", ".open-actions/workflows/link.yaml"}},
		{name: "first parent", revision: DiffRevision{BaseSHA: revision.HeadSHA, HeadSHA: revision.HeadSHA, BaseParent: true}, want: []string{".open-actions/workflows/head.yaml", ".open-actions/workflows/link.yaml"}},
		{name: "root commit", revision: DiffRevision{BaseSHA: revision.MergeBaseSHA, HeadSHA: revision.MergeBaseSHA, BaseParent: true}, want: []string{"shared.txt"}},
		{name: "same tree", revision: DiffRevision{BaseSHA: revision.HeadSHA, HeadSHA: revision.HeadSHA}, want: []string{}},
	} {
		t.Run(test.name, func(t *testing.T) {
			requests.Store(0)
			paths, err := c.ChangedFiles(t.Context(), "acme", "example", "token", test.revision, 3000)
			if err != nil || !slices.Equal(paths, test.want) {
				t.Fatalf("changed paths = %v, %v; want %v", paths, err, test.want)
			}
			t.Logf("diff Git HTTP requests = %d", requests.Load())
			if requests.Load() != 2 {
				t.Errorf("diff Git HTTP requests = %d, want 2", requests.Load())
			}
		})
	}
}

func TestMergeRejectsUnavailablePinnedCommit(t *testing.T) {
	remote, revision := testRepository(t, false)
	server, _ := gitHTTPServer(t, remote)
	c, err := NewClient(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	revision.HeadSHA = strings.Repeat("f", 40)
	merged, err := c.Merge(t.Context(), "acme", "example", "token", revision)
	if err == nil || merged != nil || !strings.Contains(err.Error(), revision.HeadSHA) {
		t.Fatalf("missing pinned commit: repository = %v, error = %v", merged, err)
	}
}
