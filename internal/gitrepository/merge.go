package gitrepository

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

const maxCommandOutput = 4 << 20

var ErrPathNotFound = errors.New("Git path does not exist")

type Revision struct {
	BaseSHA      string
	HeadSHA      string
	MergeBaseSHA string
}

type ConflictError struct {
	BaseSHA string
	HeadSHA string
}

func (e *ConflictError) Error() string {
	return fmt.Sprintf("commits %s and %s conflict", e.BaseSHA, e.HeadSHA)
}

type Client struct {
	serverURL string
}

type Repository struct {
	client      *Client
	directory   string
	environment []string
	SHA         string
}

func NewClient(serverURL string) (*Client, error) {
	if strings.TrimSpace(serverURL) == "" {
		return nil, errors.New("Git server URL must be specified")
	}
	return &Client{serverURL: strings.TrimSuffix(serverURL, "/")}, nil
}

func (c *Client) Merge(ctx context.Context, owner, repository, token string, revision Revision) (*Repository, error) {
	if owner == "" || repository == "" || token == "" || !validRevision(revision) {
		return nil, errors.New("Git merge configuration is incomplete")
	}
	directory, err := os.MkdirTemp("", "open-actions-merge-")
	if err != nil {
		return nil, fmt.Errorf("create Git merge directory: %w", err)
	}
	result := &Repository{client: c, directory: directory}
	if err := c.run(ctx, directory, nil, nil, "git", "init", "--quiet"); err != nil {
		return nil, errors.Join(err, result.Close())
	}
	remoteURL := c.repositoryURL(owner, repository)
	if err := c.run(ctx, directory, nil, nil, "git", "remote", "add", "origin", remoteURL); err != nil {
		return nil, errors.Join(err, result.Close())
	}
	environment := gitEnvironment(remoteURL, token)
	result.environment = environment
	if err := c.fetch(ctx, directory, environment, "origin", true, revision); err != nil {
		return nil, errors.Join(err, result.Close())
	}
	result.SHA, err = c.createMergeCommit(ctx, directory, environment, revision)
	if err != nil {
		return nil, errors.Join(err, result.Close())
	}
	return result, nil
}

func (c *Client) IntegrateCheckout(ctx context.Context, directory, owner, repository, token, expectedSHA string, revision Revision) error {
	if directory == "" || owner == "" || repository == "" || token == "" || !validSHA(expectedSHA) || !validRevision(revision) {
		return errors.New("Git checkout integration configuration is incomplete")
	}
	remoteURL := c.repositoryURL(owner, repository)
	environment := gitEnvironment(remoteURL, token)
	depthOne, err := c.isShallowRepository(ctx, directory, environment)
	if err != nil {
		return err
	}
	if err := c.fetch(ctx, directory, environment, remoteURL, depthOne, revision); err != nil {
		return err
	}
	sha, err := c.createMergeCommit(ctx, directory, environment, revision)
	if err != nil {
		return err
	}
	if sha != expectedSHA {
		return fmt.Errorf("constructed merge commit %s does not match expected revision %s", sha, expectedSHA)
	}
	return c.run(ctx, directory, environment, nil, "git", "reset", "--hard", "--quiet", sha)
}

func (r *Repository) ListFiles(ctx context.Context, directory string) ([]string, error) {
	if r == nil || r.client == nil || r.directory == "" || r.SHA == "" || directory == "" {
		return nil, errors.New("Git repository file listing is incomplete")
	}
	path := r.SHA + ":" + strings.Trim(directory, "/")
	output, err := r.client.output(ctx, r.directory, r.environment, nil, "git", "ls-tree", "-z", path)
	if err != nil {
		if commandExitCode(err) == 128 {
			return nil, nil
		}
		return nil, err
	}
	names := bytes.Split(output, []byte{0})
	files := make([]string, 0, len(names))
	for _, entry := range names {
		metadata, name, found := bytes.Cut(entry, []byte{'\t'})
		fields := bytes.Fields(metadata)
		regularFile := len(fields) == 3 && (bytes.Equal(fields[0], []byte("100644")) || bytes.Equal(fields[0], []byte("100755"))) && bytes.Equal(fields[1], []byte("blob"))
		if found && regularFile && len(name) > 0 {
			files = append(files, strings.TrimSuffix(directory, "/")+"/"+string(name))
		}
	}
	return files, nil
}

func (r *Repository) ReadFile(ctx context.Context, path string) ([]byte, error) {
	if r == nil || r.client == nil || r.directory == "" || r.SHA == "" || path == "" {
		return nil, errors.New("Git repository file read is incomplete")
	}
	path = strings.TrimPrefix(path, "/")
	data, err := r.client.output(ctx, r.directory, r.environment, nil, "git", "show", r.SHA+":"+path)
	if gitPathMissing(err, path) {
		return nil, fmt.Errorf("%w: %s", ErrPathNotFound, path)
	}
	return data, err
}

func (r *Repository) Close() error {
	if r == nil || r.directory == "" {
		return nil
	}
	directory := r.directory
	r.directory = ""
	r.environment = nil
	if err := os.RemoveAll(directory); err != nil {
		return fmt.Errorf("remove Git merge directory: %w", err)
	}
	return nil
}

func (c *Client) fetch(ctx context.Context, directory string, environment []string, remote string, depthOne bool, revision Revision) error {
	seen := map[string]struct{}{}
	for _, sha := range []string{revision.BaseSHA, revision.HeadSHA, revision.MergeBaseSHA} {
		if _, found := seen[sha]; found {
			continue
		}
		seen[sha] = struct{}{}
		arguments := []string{"-c", "protocol.version=2", "fetch", "--quiet", "--no-tags"}
		if depthOne {
			arguments = append(arguments, "--depth=1")
		}
		arguments = append(arguments, "--filter=blob:none", remote, sha)
		if err := c.run(ctx, directory, environment, nil, "git", arguments...); err != nil {
			return fmt.Errorf("fetch commit %s: %w", sha, err)
		}
	}
	return nil
}

func (c *Client) isShallowRepository(ctx context.Context, directory string, environment []string) (bool, error) {
	output, err := c.output(ctx, directory, environment, nil, "git", "rev-parse", "--is-shallow-repository")
	if err != nil {
		return false, fmt.Errorf("inspect Git checkout history: %w", err)
	}
	switch strings.TrimSpace(string(output)) {
	case "true":
		return true, nil
	case "false":
		return false, nil
	default:
		return false, fmt.Errorf("Git returned invalid shallow repository state %q", strings.TrimSpace(string(output)))
	}
}

// createMergeCommit runs independently in the controller and runner. Its Git
// behavior and commit metadata are part of the job-plan compatibility contract.
func (c *Client) createMergeCommit(ctx context.Context, directory string, environment []string, revision Revision) (string, error) {
	commitEnvironment := deterministicCommitEnvironment(environment)
	baseSHA, err := c.createMergeInput(ctx, directory, commitEnvironment, revision.BaseSHA, revision.MergeBaseSHA)
	if err != nil {
		return "", fmt.Errorf("create merge input for base commit %s: %w", revision.BaseSHA, err)
	}
	headSHA, err := c.createMergeInput(ctx, directory, commitEnvironment, revision.HeadSHA, revision.MergeBaseSHA)
	if err != nil {
		return "", fmt.Errorf("create merge input for head commit %s: %w", revision.HeadSHA, err)
	}
	output, err := c.output(ctx, directory, environment, nil, "git", "merge-tree", "--write-tree", "--no-messages", baseSHA, headSHA)
	if err != nil {
		if commandExitCode(err) == 1 {
			return "", &ConflictError{BaseSHA: revision.BaseSHA, HeadSHA: revision.HeadSHA}
		}
		return "", fmt.Errorf("construct merge tree: %w", err)
	}
	treeSHA := strings.TrimSpace(string(output))
	if !validSHA(treeSHA) {
		return "", fmt.Errorf("Git returned invalid merge tree SHA %q", treeSHA)
	}
	message := []byte(fmt.Sprintf("Merge %s into %s\n", revision.HeadSHA, revision.BaseSHA))
	output, err = c.output(ctx, directory, commitEnvironment, message, "git", "commit-tree", treeSHA, "-p", revision.BaseSHA, "-p", revision.HeadSHA)
	if err != nil {
		return "", fmt.Errorf("create merge commit: %w", err)
	}
	sha := strings.TrimSpace(string(output))
	if !validSHA(sha) {
		return "", fmt.Errorf("Git returned invalid merge commit SHA %q", sha)
	}
	return sha, nil
}

func (c *Client) createMergeInput(ctx context.Context, directory string, environment []string, revisionSHA, mergeBaseSHA string) (string, error) {
	output, err := c.output(ctx, directory, environment, nil, "git", "rev-parse", "--verify", revisionSHA+"^{tree}")
	if err != nil {
		return "", fmt.Errorf("resolve commit tree: %w", err)
	}
	treeSHA := strings.TrimSpace(string(output))
	if !validSHA(treeSHA) {
		return "", fmt.Errorf("Git returned invalid commit tree SHA %q", treeSHA)
	}
	output, err = c.output(ctx, directory, environment, []byte("Open Actions merge input\n"), "git", "commit-tree", treeSHA, "-p", mergeBaseSHA)
	if err != nil {
		return "", fmt.Errorf("create commit: %w", err)
	}
	sha := strings.TrimSpace(string(output))
	if !validSHA(sha) {
		return "", fmt.Errorf("Git returned invalid merge input SHA %q", sha)
	}
	return sha, nil
}

func deterministicCommitEnvironment(environment []string) []string {
	result := append([]string(nil), environment...)
	for name, value := range map[string]string{
		"GIT_AUTHOR_NAME":     "Open Actions",
		"GIT_AUTHOR_EMAIL":    "open-actions@localhost",
		"GIT_AUTHOR_DATE":     "2000-01-01T00:00:00Z",
		"GIT_COMMITTER_NAME":  "Open Actions",
		"GIT_COMMITTER_EMAIL": "open-actions@localhost",
		"GIT_COMMITTER_DATE":  "2000-01-01T00:00:00Z",
	} {
		result = setEnvironment(result, name, value)
	}
	return result
}

func (c *Client) repositoryURL(owner, repository string) string {
	if parsed, err := url.Parse(c.serverURL); err == nil && parsed.Scheme != "" {
		return c.serverURL + "/" + url.PathEscape(owner) + "/" + url.PathEscape(repository)
	}
	return filepath.Join(c.serverURL, owner, repository)
}

func (c *Client) run(ctx context.Context, directory string, environment []string, input []byte, name string, arguments ...string) error {
	_, err := c.output(ctx, directory, environment, input, name, arguments...)
	return err
}

func (c *Client) output(ctx context.Context, directory string, environment []string, input []byte, name string, arguments ...string) ([]byte, error) {
	command := exec.CommandContext(ctx, name, arguments...)
	command.Dir = directory
	command.Env = append([]string(nil), os.Environ()...)
	for _, entry := range environment {
		name, value, found := strings.Cut(entry, "=")
		if found {
			command.Env = setEnvironment(command.Env, name, value)
		}
	}
	if input != nil {
		command.Stdin = bytes.NewReader(input)
	}
	stdout := &limitedBuffer{remaining: maxCommandOutput}
	stderr := &limitedBuffer{remaining: maxCommandOutput}
	command.Stdout = stdout
	command.Stderr = stderr
	err := command.Run()
	if err != nil {
		message := strings.TrimSpace(string(stderr.Bytes()))
		if message == "" {
			message = err.Error()
		}
		return nil, &commandError{name: name, arguments: arguments, message: message, cause: err}
	}
	return stdout.Bytes(), nil
}

type commandError struct {
	name      string
	arguments []string
	message   string
	cause     error
}

func (e *commandError) Error() string {
	return fmt.Sprintf("run %s: %s", strings.Join(append([]string{e.name}, e.arguments...), " "), e.message)
}

func (e *commandError) Unwrap() error {
	return e.cause
}

func commandExitCode(err error) int {
	exitError := &exec.ExitError{}
	if errors.As(err, &exitError) {
		return exitError.ExitCode()
	}
	return -1
}

func gitPathMissing(err error, path string) bool {
	commandErr := &commandError{}
	return commandExitCode(err) == 128 && errors.As(err, &commandErr) && strings.Contains(commandErr.message, "path '"+path+"' does not exist in ")
}

type limitedBuffer struct {
	bytes.Buffer
	remaining int
}

func (b *limitedBuffer) Write(data []byte) (int, error) {
	length := len(data)
	if length > b.remaining {
		data = data[:b.remaining]
	}
	b.remaining -= len(data)
	_, _ = b.Buffer.Write(data)
	return length, nil
}

func gitEnvironment(remoteURL, token string) []string {
	credential := base64.StdEncoding.EncodeToString([]byte("x-access-token:" + token))
	return []string{
		"GIT_CONFIG_COUNT=1",
		"GIT_CONFIG_KEY_0=http." + remoteURL + ".extraHeader",
		"GIT_CONFIG_VALUE_0=AUTHORIZATION: basic " + credential,
		"GIT_TERMINAL_PROMPT=0",
	}
}

func setEnvironment(environment []string, name, value string) []string {
	prefix := name + "="
	for index := range environment {
		if strings.HasPrefix(environment[index], prefix) {
			environment[index] = prefix + value
			return environment
		}
	}
	return append(environment, prefix+value)
}

func validRevision(revision Revision) bool {
	return validSHA(revision.BaseSHA) && validSHA(revision.HeadSHA) && validSHA(revision.MergeBaseSHA)
}

func validSHA(value string) bool {
	if len(value) != 40 {
		return false
	}
	for _, character := range value {
		if character < '0' || character > '9' && character < 'a' || character > 'f' {
			return false
		}
	}
	return true
}
