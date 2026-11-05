package gitrepository

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestPrefetchFilesSelectsCurrentBlobs(t *testing.T) {
	for _, filtered := range []bool{true, false} {
		name := "present blobs"
		if filtered {
			name = "missing blobs"
		}
		t.Run(name, func(t *testing.T) {
			remote, revision := testRepository(t, false)
			work := filepath.Join(filepath.Dir(filepath.Dir(remote)), "work")
			historical := strings.TrimSpace(runGit(t, work, "rev-parse", "HEAD:.open-actions/workflows/head.yaml"))
			writeFile(t, work, ".open-actions/workflows/head.yaml", "name: Selected\n")
			writeFile(t, work, ".open-actions/workflows/[selected] file.yaml", "name: Literal\n")
			writeFile(t, work, ".open-actions/workflows/ignored.yaml", "name: Ignored\n")
			runGit(t, work, "add", ".")
			runGit(t, work, "commit", "--quiet", "-m", "workflow contents")
			revision.HeadSHA = strings.TrimSpace(runGit(t, work, "rev-parse", "HEAD"))
			ignored := strings.TrimSpace(runGit(t, work, "rev-parse", "HEAD:.open-actions/workflows/ignored.yaml"))
			runGit(t, work, "push", "--quiet", remote, "head")
			if !filtered {
				runGit(t, remote, "config", "uploadpack.allowFilter", "false")
			}
			server, requests := gitHTTPServer(t, remote)
			c, err := NewClient(server.URL)
			if err != nil {
				t.Fatal(err)
			}
			r, err := c.Merge(t.Context(), "acme", "example", "token", revision)
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			paths := []string{".open-actions/workflows/head.yaml", ".open-actions/workflows/[selected] file.yaml"}
			if err := r.PrefetchFiles(t.Context(), nil); err != nil {
				t.Fatal(err)
			}
			for range 2 {
				if err := r.PrefetchFiles(t.Context(), paths); err != nil {
					t.Fatal(err)
				}
			}
			for index, want := range []string{"name: Selected\n", "name: Literal\n"} {
				data, err := r.ReadFile(t.Context(), paths[index])
				if err != nil || string(data) != want {
					t.Fatalf("file %s = %q, %v; want %q", paths[index], data, err, want)
				}
			}
			wantRequests := int32(2)
			if filtered {
				wantRequests = 4
				objects := runGit(t, r.directory, "cat-file", "--batch-all-objects", "--batch-check=%(objectname)")
				for _, unwanted := range []string{historical, ignored} {
					if strings.Contains(objects, unwanted) {
						t.Fatalf("prefetch downloaded unrelated blob %s", unwanted)
					}
				}
			}
			if requests.Load() != wantRequests {
				t.Fatalf("Git HTTP requests = %d, want %d", requests.Load(), wantRequests)
			}
		})
	}
}
