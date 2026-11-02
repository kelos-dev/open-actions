package gitrepository

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func TestChangedFilesPinnedTrees(t *testing.T) {
	remote, revision := testRepository(t, false)
	client, err := NewClient(filepath.Dir(filepath.Dir(remote)))
	if err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		name     string
		revision DiffRevision
		want     []string
	}{
		{"two dot includes base deletions", DiffRevision{BaseSHA: revision.BaseSHA, HeadSHA: revision.HeadSHA}, []string{".open-actions/workflows/base.yaml", ".open-actions/workflows/head.yaml", ".open-actions/workflows/link.yaml"}},
		{"three dot excludes base changes", DiffRevision{BaseSHA: revision.MergeBaseSHA, HeadSHA: revision.HeadSHA}, []string{".open-actions/workflows/head.yaml", ".open-actions/workflows/link.yaml"}},
		{"new branch parent", DiffRevision{BaseSHA: revision.HeadSHA, HeadSHA: revision.HeadSHA, BaseParent: true}, []string{".open-actions/workflows/head.yaml", ".open-actions/workflows/link.yaml"}},
		{"new repository root", DiffRevision{BaseSHA: revision.MergeBaseSHA, HeadSHA: revision.MergeBaseSHA, BaseParent: true}, []string{"shared.txt"}},
		{"identical trees", DiffRevision{BaseSHA: revision.HeadSHA, HeadSHA: revision.HeadSHA}, []string{}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			paths, err := client.ChangedFiles(context.Background(), "acme", "example", "token", tt.revision, 3000)
			if err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(paths, tt.want) {
				t.Fatalf("ChangedFiles() = %q, want %q", paths, tt.want)
			}
		})
	}
	merged, err := client.Merge(context.Background(), "acme", "example", "token", revision)
	if err != nil {
		t.Fatal(err)
	}
	defer merged.Close()
	paths, err := merged.ChangedFiles(context.Background(), revision.MergeBaseSHA, revision.HeadSHA, 1)
	if err != nil || !slices.Equal(paths, []string{".open-actions/workflows/head.yaml"}) {
		t.Fatalf("limited merged diff = %q, error = %v", paths, err)
	}
}

func TestChangedFilesPreservesNamesAndRenames(t *testing.T) {
	root := t.TempDir()
	remote := filepath.Join(root, "acme", "example")
	work := filepath.Join(root, "work")
	if err := os.MkdirAll(filepath.Dir(remote), 0o755); err != nil {
		t.Fatal(err)
	}
	runGit(t, "", "init", "--quiet", work)
	runGit(t, work, "config", "user.name", "Test")
	runGit(t, work, "config", "user.email", "test@example.com")
	writeFile(t, work, "removed.txt", "removed\n")
	writeFile(t, work, "source.txt", "renamed\n")
	writeFile(t, work, "modified.txt", "original\n")
	runGit(t, work, "add", ".")
	runGit(t, work, "commit", "--quiet", "-m", "base")
	baseSHA := strings.TrimSpace(runGit(t, work, "rev-parse", "HEAD"))
	runGit(t, work, "rm", "removed.txt")
	runGit(t, work, "mv", "source.txt", "renamed.txt")
	writeFile(t, work, "modified.txt", "modified\n")
	writeFile(t, work, "added\nfile.txt", "added\n")
	runGit(t, work, "add", ".")
	runGit(t, work, "commit", "--quiet", "-m", "changes")
	headSHA := strings.TrimSpace(runGit(t, work, "rev-parse", "HEAD"))
	runGit(t, "", "clone", "--quiet", "--bare", work, remote)
	client, err := NewClient(root)
	if err != nil {
		t.Fatal(err)
	}
	paths, err := client.ChangedFiles(context.Background(), "acme", "example", "token", DiffRevision{BaseSHA: baseSHA, HeadSHA: headSHA}, 3000)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"added\nfile.txt", "modified.txt", "removed.txt", "renamed.txt", "source.txt"}
	if !slices.Equal(paths, want) {
		t.Fatalf("ChangedFiles() = %q, want %q", paths, want)
	}
}

func TestChangedFilesNewBranchWithMultipleCommits(t *testing.T) {
	root := t.TempDir()
	remote := filepath.Join(root, "acme", "example")
	work := filepath.Join(root, "work")
	if err := os.MkdirAll(filepath.Dir(remote), 0o755); err != nil {
		t.Fatal(err)
	}
	runGit(t, "", "init", "--quiet", work)
	runGit(t, work, "config", "user.name", "Test")
	runGit(t, work, "config", "user.email", "test@example.com")
	var firstSHA, headSHA string
	for _, name := range []string{"root.txt", "parent.txt", "first.txt", "last.txt"} {
		writeFile(t, work, name, name+"\n")
		runGit(t, work, "add", ".")
		runGit(t, work, "commit", "--quiet", "-m", name)
		headSHA = strings.TrimSpace(runGit(t, work, "rev-parse", "HEAD"))
		if name == "first.txt" {
			firstSHA = headSHA
		}
	}
	runGit(t, "", "clone", "--quiet", "--bare", work, remote)
	runGit(t, remote, "config", "uploadpack.allowFilter", "true")
	client, err := NewClient(root)
	if err != nil {
		t.Fatal(err)
	}
	paths, err := client.ChangedFiles(context.Background(), "acme", "example", "token", DiffRevision{BaseSHA: firstSHA, HeadSHA: headSHA, BaseParent: true}, 3000)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"first.txt", "last.txt"}
	if !slices.Equal(paths, want) {
		t.Fatalf("ChangedFiles() = %q, want %q", paths, want)
	}
}
