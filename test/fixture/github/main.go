package main

import (
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/http/cgi"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

const workflowPath = ".open-actions/workflows/ci.yaml"

const workflowData = `name: Fixture CI
on:
  push:
    branches: [main]
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with:
          go-version-file: go.mod
      - uses: actions/composite@v1
        with:
          message: from composite
      - name: Verify runner context
        run: |
          test "$GITHUB_REPOSITORY" = "acme/example"
          test "$GITHUB_REF_NAME" = "main"
          test "$EXTERNAL_SETUP_GO" = "ready"
          test "$COMPOSITE_VALUE" = "from composite"
          go test ./...
          printf 'open actions e2e works\n'
`

const unsupportedTriggerWorkflowData = `name: Unsupported trigger
on: schedule
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - run: echo test
`

const unsupportedFieldWorkflowData = `name: Unsupported field
on: push
jobs:
  test:
    runs-on: ubuntu-latest
    timeout-minutes: 10
    steps:
      - run: echo test
`

const checkoutMetadata = `name: Checkout fixture
inputs:
  repository:
    default: ${{ github.repository }}
  token:
    default: ${{ github.token }}
runs:
  using: node20
  main: dist/index.js
  post: dist/index.js
`

const checkoutScript = `const childProcess = require('child_process');
const fs = require('fs');

if (process.env.STATE_checked_out === 'true') {
  console.log('external checkout post ran');
  process.exit(0);
}

const workspace = process.env.GITHUB_WORKSPACE;
const repository = process.env['INPUT_REPOSITORY'];
const remote = process.env.GITHUB_SERVER_URL + '/' + repository;
const run = (args) => childProcess.execFileSync('git', args, {stdio: 'inherit'});
fs.mkdirSync(workspace, {recursive: true});
run(['config', '--global', '--add', 'safe.directory', workspace]);
run(['init', '--quiet', workspace]);
run(['-C', workspace, 'remote', 'add', 'origin', remote]);
run(['-C', workspace, 'fetch', '--quiet', '--depth=1', 'origin', process.env.GITHUB_SHA]);
run(['-C', workspace, 'checkout', '--quiet', '--detach', 'FETCH_HEAD']);
fs.appendFileSync(process.env.GITHUB_STATE, 'checked_out=true\n');
console.log('external checkout main ran');
`

const setupGoMetadata = `name: Setup Go fixture
inputs:
  go-version-file:
    required: true
runs:
  using: node20
  main: dist/index.js
  post: dist/index.js
  post-if: success()
`

const setupGoScript = `const childProcess = require('child_process');
const fs = require('fs');
const path = require('path');

if (process.env.STATE_setup === 'true') {
  console.log('external setup-go post ran');
  process.exit(0);
}

const versionFile = path.join(process.env.GITHUB_WORKSPACE, process.env['INPUT_GO-VERSION-FILE']);
if (!fs.existsSync(versionFile)) {
  throw new Error('Go version file was not checked out');
}
childProcess.execFileSync('go', ['version'], {stdio: 'inherit'});
fs.appendFileSync(process.env.GITHUB_ENV, 'EXTERNAL_SETUP_GO=ready\n');
fs.appendFileSync(process.env.GITHUB_PATH, '/usr/local/go/bin\n');
fs.appendFileSync(process.env.GITHUB_STATE, 'setup=true\n');
console.log('external setup-go main ran');
`

const markerMetadata = `name: Marker fixture
inputs:
  message:
    required: true
runs:
  using: node20
  main: dist/index.js
  post: dist/index.js
`

const markerScript = `const fs = require('fs');

if (process.env.STATE_marker === 'true') {
  console.log('external marker post ran');
  process.exit(0);
}

fs.appendFileSync(process.env.GITHUB_OUTPUT, 'value=' + process.env['INPUT_MESSAGE'] + '\n');
fs.appendFileSync(process.env.GITHUB_STATE, 'marker=true\n');
`

const compositeMetadata = `name: Composite fixture
inputs:
  message:
    required: true
outputs:
  result:
    value: ${{ steps.marker.outputs.value }}
runs:
  using: composite
  steps:
    - id: marker
      uses: actions/marker@v1
      with:
        message: ${{ inputs.message }}
    - name: Export composite result
      run: |
        test -f "${{ github.action_path }}/action.yml"
        printf 'COMPOSITE_VALUE=%s\n' "${{ steps.marker.outputs.value }}" >> "$GITHUB_ENV"
        printf 'external composite run\n'
      shell: bash
`

func main() {
	dataDirectory := flag.String("data-dir", "/data", "Directory used for fixture Git repositories")
	listenAddress := flag.String("listen-address", ":8080", "HTTP listen address")
	flag.Parse()
	sha, err := createRepositories(*dataDirectory)
	if err != nil {
		log.Fatal(err)
	}
	gitPath, err := exec.LookPath("git")
	if err != nil {
		log.Fatal(err)
	}
	mux := http.NewServeMux()
	gitBackend := &cgi.Handler{
		Path: gitPath,
		Args: []string{"http-backend"},
		Env: []string{
			"GIT_HTTP_EXPORT_ALL=1",
			"GIT_PROJECT_ROOT=" + filepath.Join(*dataDirectory, "git"),
		},
	}
	mux.Handle("/acme/", gitBackend)
	mux.Handle("/actions/", gitBackend)
	mux.HandleFunc("/app/installations/", func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost {
			http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if !strings.HasSuffix(request.URL.Path, "/access_tokens") {
			http.NotFound(writer, request)
			return
		}
		writeJSON(writer, map[string]string{"token": "fixture-installation-token"})
	})
	mux.HandleFunc("/repos/acme/example/contents/.open-actions/workflows", func(writer http.ResponseWriter, _ *http.Request) {
		writeJSON(writer, []map[string]string{{"name": "ci.yaml", "path": workflowPath, "type": "file"}})
	})
	mux.HandleFunc("/repos/acme/example/contents/"+workflowPath, func(writer http.ResponseWriter, _ *http.Request) {
		writeJSON(writer, map[string]string{"encoding": "base64", "content": base64.StdEncoding.EncodeToString([]byte(workflowData))})
	})
	for repository, data := range map[string]string{
		"invalid-trigger": unsupportedTriggerWorkflowData,
		"invalid-field":   unsupportedFieldWorkflowData,
	} {
		repository := repository
		data := data
		mux.HandleFunc("/repos/acme/"+repository+"/contents/.open-actions/workflows", func(writer http.ResponseWriter, _ *http.Request) {
			writeJSON(writer, []map[string]string{{"name": "ci.yaml", "path": workflowPath, "type": "file"}})
		})
		mux.HandleFunc("/repos/acme/"+repository+"/contents/"+workflowPath, func(writer http.ResponseWriter, _ *http.Request) {
			writeJSON(writer, map[string]string{"encoding": "base64", "content": base64.StdEncoding.EncodeToString([]byte(data))})
		})
	}
	mux.HandleFunc("/fixture/sha", func(writer http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(writer, sha)
	})
	mux.HandleFunc("/healthz", func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusOK)
	})
	server := &http.Server{Addr: *listenAddress, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	log.Fatal(server.ListenAndServe())
}

func createRepositories(dataDirectory string) (string, error) {
	if err := os.RemoveAll(filepath.Join(dataDirectory, "git")); err != nil {
		return "", err
	}
	if err := os.RemoveAll(filepath.Join(dataDirectory, "work")); err != nil {
		return "", err
	}
	repositorySHA, err := createRepository(dataDirectory, "acme", "example", "", map[string]string{
		"go.mod":          fmt.Sprintf("module example.com/fixture\n\ngo %s\n", strings.TrimPrefix(runtime.Version(), "go")),
		"fixture.go":      "package fixture\n\nfunc Message() string { return \"open actions e2e works\" }\n",
		"fixture_test.go": "package fixture\n\nimport \"testing\"\n\nfunc TestMessage(t *testing.T) {\n\tif Message() != \"open actions e2e works\" { t.Fatal(\"unexpected message\") }\n}\n",
	})
	if err != nil {
		return "", err
	}
	if _, err := createRepository(dataDirectory, "actions", "checkout", "v4", map[string]string{
		"action.yml":    checkoutMetadata,
		"dist/index.js": checkoutScript,
	}); err != nil {
		return "", err
	}
	if _, err := createRepository(dataDirectory, "actions", "setup-go", "v5", map[string]string{
		"action.yml":    setupGoMetadata,
		"dist/index.js": setupGoScript,
	}); err != nil {
		return "", err
	}
	if _, err := createRepository(dataDirectory, "actions", "marker", "v1", map[string]string{
		"action.yml":    markerMetadata,
		"dist/index.js": markerScript,
	}); err != nil {
		return "", err
	}
	if _, err := createRepository(dataDirectory, "actions", "composite", "v1", map[string]string{
		"action.yml": compositeMetadata,
	}); err != nil {
		return "", err
	}
	return repositorySHA, nil
}

func createRepository(dataDirectory, owner, name, tag string, files map[string]string) (string, error) {
	workDirectory := filepath.Join(dataDirectory, "work", owner, name)
	bareDirectory := filepath.Join(dataDirectory, "git", owner, name)
	if err := os.MkdirAll(workDirectory, 0o755); err != nil {
		return "", err
	}
	if err := os.MkdirAll(filepath.Dir(bareDirectory), 0o755); err != nil {
		return "", err
	}
	for name, content := range files {
		filePath := filepath.Join(workDirectory, name)
		if err := os.MkdirAll(filepath.Dir(filePath), 0o755); err != nil {
			return "", err
		}
		if err := os.WriteFile(filePath, []byte(content), 0o644); err != nil {
			return "", err
		}
	}
	commands := [][]string{
		{"git", "init", "--quiet", "--initial-branch=main", workDirectory},
		{"git", "-C", workDirectory, "config", "user.name", "Open Actions Fixture"},
		{"git", "-C", workDirectory, "config", "user.email", "fixture@example.invalid"},
		{"git", "-C", workDirectory, "add", "."},
		{"git", "-C", workDirectory, "commit", "--quiet", "-m", "fixture"},
	}
	if tag != "" {
		commands = append(commands, []string{"git", "-C", workDirectory, "tag", tag})
	}
	commands = append(commands, []string{"git", "clone", "--quiet", "--bare", workDirectory, bareDirectory})
	for _, command := range commands {
		if output, err := exec.Command(command[0], command[1:]...).CombinedOutput(); err != nil {
			return "", fmt.Errorf("run %s: %w: %s", strings.Join(command, " "), err, output)
		}
	}
	output, err := exec.Command("git", "-C", workDirectory, "rev-parse", "HEAD").Output()
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(output)), nil
}

func writeJSON(writer http.ResponseWriter, value any) {
	writer.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(writer).Encode(value); err != nil {
		log.Printf("encode response: %v", err)
	}
}
