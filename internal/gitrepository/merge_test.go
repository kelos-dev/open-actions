package gitrepository

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestMergeConstructsDeterministicRevisionAndFiles(t *testing.T) {
	remote, revision := testRepository(t, false)
	client, err := NewClient(filepath.Dir(filepath.Dir(remote)))
	if err != nil {
		t.Fatal(err)
	}
	repository, err := client.Merge(context.Background(), "acme", "example", "token", revision)
	if err != nil {
		t.Fatal(err)
	}
	defer repository.Close()
	files, err := repository.ListFiles(context.Background(), ".open-actions/workflows")
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 2 || files[0] != ".open-actions/workflows/base.yaml" || files[1] != ".open-actions/workflows/head.yaml" {
		t.Fatalf("merged workflow files = %#v", files)
	}
	data, err := repository.ReadFile(context.Background(), files[1])
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "name: Head\n" {
		t.Fatalf("head workflow = %q", data)
	}
	if _, err := repository.ReadFile(context.Background(), ".open-actions/workflows/missing.yaml"); !errors.Is(err, ErrPathNotFound) {
		t.Fatalf("ReadFile() error = %v, want ErrPathNotFound", err)
	}

	checkout := t.TempDir()
	runGit(t, "", "clone", "--quiet", remote, checkout)
	runGit(t, checkout, "checkout", "--quiet", "--detach", revision.HeadSHA)
	if err := client.IntegrateCheckout(context.Background(), checkout, "acme", "example", "token", repository.SHA, revision); err != nil {
		t.Fatal(err)
	}
	if got := strings.TrimSpace(runGit(t, checkout, "rev-parse", "HEAD")); got != repository.SHA {
		t.Fatalf("checkout revision = %q, want %q", got, repository.SHA)
	}
	if got := strings.TrimSpace(runGit(t, checkout, "rev-parse", "--is-shallow-repository")); got != "false" {
		t.Fatalf("checkout shallow = %q, want false", got)
	}
}

func TestMergeReportsConflicts(t *testing.T) {
	remote, revision := testRepository(t, true)
	client, err := NewClient(filepath.Dir(filepath.Dir(remote)))
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.Merge(context.Background(), "acme", "example", "token", revision)
	conflict := &ConflictError{}
	if !errors.As(err, &conflict) {
		t.Fatalf("Merge() error = %v, want conflict", err)
	}
}

func TestIntegrateCheckoutPreservesShallowHistory(t *testing.T) {
	remote, revision := testRepository(t, false)
	client, err := NewClient("file://" + filepath.Dir(filepath.Dir(remote)))
	if err != nil {
		t.Fatal(err)
	}
	repository, err := client.Merge(context.Background(), "acme", "example", "token", revision)
	if err != nil {
		t.Fatal(err)
	}
	defer repository.Close()

	checkout := t.TempDir()
	runGit(t, "", "clone", "--quiet", "--depth=1", "file://"+remote, checkout)
	if got := strings.TrimSpace(runGit(t, checkout, "rev-parse", "--is-shallow-repository")); got != "true" {
		t.Fatalf("checkout shallow before integration = %q, want true", got)
	}
	if err := client.IntegrateCheckout(context.Background(), checkout, "acme", "example", "token", repository.SHA, revision); err != nil {
		t.Fatal(err)
	}
	if got := strings.TrimSpace(runGit(t, checkout, "rev-parse", "--is-shallow-repository")); got != "true" {
		t.Fatalf("checkout shallow after integration = %q, want true", got)
	}
	if got := strings.TrimSpace(runGit(t, checkout, "rev-list", "--count", revision.BaseSHA)); got != "1" {
		t.Fatalf("base history count = %q, want 1", got)
	}
}

func testRepository(t *testing.T, conflict bool) (string, Revision) {
	t.Helper()
	root := t.TempDir()
	work := filepath.Join(root, "work")
	remote := filepath.Join(root, "acme", "example")
	if err := os.MkdirAll(filepath.Dir(remote), 0o755); err != nil {
		t.Fatal(err)
	}
	runGit(t, "", "init", "--quiet", work)
	runGit(t, work, "config", "user.name", "Test")
	runGit(t, work, "config", "user.email", "test@example.com")
	writeFile(t, work, "shared.txt", "common\n")
	runGit(t, work, "add", ".")
	runGit(t, work, "commit", "--quiet", "-m", "common")
	mergeBaseSHA := strings.TrimSpace(runGit(t, work, "rev-parse", "HEAD"))

	runGit(t, work, "switch", "--quiet", "-c", "base")
	writeFile(t, work, ".open-actions/workflows/base.yaml", "name: Base\n")
	if conflict {
		writeFile(t, work, "shared.txt", "base\n")
	}
	runGit(t, work, "add", ".")
	runGit(t, work, "commit", "--quiet", "-m", "base")
	baseSHA := strings.TrimSpace(runGit(t, work, "rev-parse", "HEAD"))

	runGit(t, work, "switch", "--quiet", "-c", "head", mergeBaseSHA)
	writeFile(t, work, ".open-actions/workflows/head.yaml", "name: Head\n")
	if err := os.Symlink("head.yaml", filepath.Join(work, ".open-actions/workflows/link.yaml")); err != nil {
		t.Fatal(err)
	}
	if conflict {
		writeFile(t, work, "shared.txt", "head\n")
	}
	runGit(t, work, "add", ".")
	runGit(t, work, "commit", "--quiet", "-m", "head")
	headSHA := strings.TrimSpace(runGit(t, work, "rev-parse", "HEAD"))
	runGit(t, "", "clone", "--quiet", "--bare", work, remote)
	runGit(t, remote, "config", "uploadpack.allowFilter", "true")
	return remote, Revision{BaseSHA: baseSHA, HeadSHA: headSHA, MergeBaseSHA: mergeBaseSHA}
}

func writeFile(t *testing.T, root, path, data string) {
	t.Helper()
	fullPath := filepath.Join(root, path)
	if err := os.MkdirAll(filepath.Dir(fullPath), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(fullPath, []byte(data), 0o644); err != nil {
		t.Fatal(err)
	}
}

func runGit(t *testing.T, directory string, arguments ...string) string {
	t.Helper()
	command := exec.Command("git", arguments...)
	if directory != "" {
		command.Dir = directory
	}
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("git %s: %v: %s", strings.Join(arguments, " "), err, output)
	}
	return string(output)
}
