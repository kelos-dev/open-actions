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
	"sync"
	"time"
)

const workflowPath = ".open-actions/workflows/ci.yaml"
const preparationWorkflowPath = ".open-actions/workflows/preparation.yaml"
const dynamicMatrixWorkflowPath = ".open-actions/workflows/dynamic-matrix.yaml"
const artifactWorkflowPath = ".open-actions/workflows/artifacts.yaml"
const tokenPermissionsWorkflowPath = ".open-actions/workflows/token-permissions.yaml"
const jobConcurrencyWorkflowPath = ".open-actions/workflows/job-concurrency.yaml"
const concurrencyConflictWorkflowPath = ".open-actions/workflows/concurrency-conflict.yaml"
const fixtureJobToken = "fixture-job-token"
const fixtureActionToken = "fixture-action-token"

type installationTokenRequest struct {
	Repositories []string          `json:"repositories,omitempty"`
	Permissions  map[string]string `json:"permissions"`
}

const workflowData = `name: Fixture CI
on:
  push:
    branches: [main]
jobs:
  test:
    runs-on: [ubuntu-latest, docker]
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with:
          go-version-file: go.mod
      - uses: helm/kind-action@v1
        with:
          cluster_name: kind
      - uses: actions/composite@v1
        with:
          message: from composite
      - name: Verify runner context
        run: |
          test "$GITHUB_REPOSITORY" = "acme/example"
          test "$GITHUB_REF_NAME" = "main"
          test "$EXTERNAL_SETUP_GO" = "ready"
          test "$KIND_ACTION_RUNTIME" = "24"
          test "$KIND_ACTION_DOCKER" = "ready"
          test "$COMPOSITE_VALUE" = "from composite"
          git status --short
          printf 'runner workspace git works\n'
          go test ./...
          printf 'open actions e2e works\n'
      - name: Verify Docker execution
        run: |
          CGO_ENABLED=0 go build -o docker-e2e ./cmd/docker-e2e
          docker build --tag open-actions-e2e --file Dockerfile.docker-e2e .
          test "$(docker run --rm open-actions-e2e)" = "open actions docker e2e works"
          printf 'Docker execution works\n'
  report:
    needs: test
    if: always() && needs.test.result == 'success'
    runs-on: [ubuntu-latest, docker]
    steps:
      - run: test "${{ needs.test.result }}" = success && printf 'dependency graph e2e works\n'
`

const preparationSteps = `      - name: Block action downloads
        run: |
          curl --silent --show-error --fail --request POST "$GITHUB_SERVER_URL/fixture/block-preparation-actions"
          printf 'action downloads blocked after preparation\n'
      - uses: preparation-actions/composite@v1
        with:
          message: prepared before steps
      - name: Verify prepared actions
        run: |
          test "$PREPARATION_VALUE" = "prepared before steps"
          printf 'prepared action graph e2e works\n'
`

var preparationWorkflowData = strings.Replace(workflowData, "    steps:\n", "    steps:\n"+preparationSteps, 1)

const dynamicMatrixWorkflowData = `name: Dynamic matrix
on: push
jobs:
  prepare:
    runs-on: [ubuntu-latest, docker]
    outputs:
      matrix: ${{ steps.matrix.outputs.value }}
    steps:
      - id: matrix
        run: |
          printf 'value={"target":["first","second"]}\n' >> "$GITHUB_OUTPUT"
  execute:
    needs: prepare
    strategy:
      max-parallel: 1
      matrix: ${{ fromJSON(needs.prepare.outputs.matrix) }}
    name: Execute ${{ matrix.target }}
    runs-on: [ubuntu-latest, docker]
    steps:
      - run: test "${{ matrix.target }}" = first || test "${{ matrix.target }}" = second
`

const artifactWorkflowData = `name: Artifact exchange
on: push
jobs:
  generate:
    strategy:
      matrix:
        shard: [one, two]
    runs-on: ubuntu-latest
    steps:
      - name: Create shard result
        run: |
          sleep 5
          printf '%s\n' '${{ matrix.shard }}' > 'result-${{ matrix.shard }}.txt'
      - uses: actions/upload-artifact@v7
        with:
          name: result-${{ matrix.shard }}
          path: result-${{ matrix.shard }}.txt
  aggregate:
    needs: generate
    runs-on: ubuntu-latest
    steps:
      - uses: actions/download-artifact@v8
        with:
          pattern: result-*
          merge-multiple: true
          path: artifacts
      - name: Verify aggregation
        run: |
          test "$(cat artifacts/result-one.txt)" = one
          test "$(cat artifacts/result-two.txt)" = two
          printf 'artifact aggregation e2e works\n'
`

const tokenPermissionsWorkflowData = `name: Token permissions
on: push
permissions:
  issues: write
jobs:
  inherited:
    runs-on: ubuntu-latest
    steps:
      - name: Verify inherited token
        run: test -n "$CONTEXT_TOKEN" && test "$CONTEXT_TOKEN" = "$SECRET_TOKEN"
        env:
          CONTEXT_TOKEN: ${{ github.token }}
          SECRET_TOKEN: ${{ secrets.GITHUB_TOKEN }}
  overridden:
    permissions:
      statuses: read
    runs-on: ubuntu-latest
    steps:
      - name: Verify overridden token
        run: test -n "$CONTEXT_TOKEN" && test "$CONTEXT_TOKEN" = "$SECRET_TOKEN"
        env:
          CONTEXT_TOKEN: ${{ github.token }}
          SECRET_TOKEN: ${{ secrets.GITHUB_TOKEN }}
  disabled:
    permissions: {}
    runs-on: ubuntu-latest
    steps:
      - name: Verify metadata-only token
        run: test -n "$CONTEXT_TOKEN" && test "$CONTEXT_TOKEN" = "$SECRET_TOKEN"
        env:
          CONTEXT_TOKEN: ${{ github.token }}
          SECRET_TOKEN: ${{ secrets.GITHUB_TOKEN }}
`

const jobConcurrencyWorkflowData = `name: Job concurrency
on: push
jobs:
  first:
    runs-on: ubuntu-latest
    concurrency:
      group: deploy-${{ github.repository }}
      cancel-in-progress: ${{ false }}
    steps:
      - run: sleep 10
  second:
    runs-on: ubuntu-latest
    concurrency:
      group: deploy-${{ github.repository }}
      cancel-in-progress: ${{ false }}
    steps:
      - run: sleep 10
`

const concurrencyConflictWorkflowData = `name: Concurrency conflict
on: push
concurrency:
  group: parent
jobs:
  conflict:
    runs-on: ubuntu-latest
    concurrency:
      group: PARENT
      cancel-in-progress: ${{ true }}
    steps:
      - run: echo unreachable
`

const pullRequestWorkflowPath = ".open-actions/workflows/pull-request.yaml"

const pullRequestWorkflowData = `name: Pull request checkout
on: pull_request
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Verify pull request checkout
        run: |
          test "$GITHUB_REPOSITORY" = "acme/example"
          test "$GITHUB_REF_NAME" = "42/merge"
          test "$(git rev-parse HEAD)" = "$GITHUB_SHA"
          test -f base.txt
          test -f head.txt
          credential_key="http.${GITHUB_SERVER_URL}/.extraheader"
          test "$(git config --local --get-all "$credential_key" | wc -l)" -eq 1
          printf 'pull request checkout integration works\n'
`

const unsupportedTriggerWorkflowData = `name: Unsupported trigger
on: repository_dispatch
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
    unsupported-field: true
    steps:
      - run: echo test
`

const checkoutMetadata = `name: Checkout fixture
inputs:
  repository:
    default: ${{ github.repository }}
  ref:
    description: The branch, tag or SHA to checkout
  token:
    default: ${{ github.token }}
  persist-credentials:
    default: true
runs:
  using: node20
  main: dist/index.js
  post: dist/index.js
`

const checkoutScript = `const childProcess = require('child_process');
const fs = require('fs');

const workspace = process.env.GITHUB_WORKSPACE;
const repository = process.env['INPUT_REPOSITORY'];
const remote = process.env.GITHUB_SERVER_URL + '/' + repository;
const credentialKey = 'http.' + process.env.GITHUB_SERVER_URL + '/.extraheader';
const gitEnvironment = {
  ...process.env,
  GIT_CONFIG_COUNT: '1',
  GIT_CONFIG_KEY_0: 'safe.directory',
  GIT_CONFIG_VALUE_0: workspace,
};
const run = (args) => childProcess.execFileSync('git', args, {
  env: gitEnvironment,
  stdio: 'inherit',
});
const removeCredential = () => {
  try {
    run(['-C', workspace, 'config', '--local', '--unset-all', credentialKey]);
  } catch (error) {
    if (error.status !== 5) throw error;
  }
};

if (process.env.STATE_checked_out === 'true') {
  removeCredential();
  console.log('external checkout post ran');
  process.exit(0);
}

fs.mkdirSync(workspace, {recursive: true});
run(['init', '--quiet', workspace]);
run(['-C', workspace, 'remote', 'add', 'origin', remote]);
const credential = Buffer.from('x-access-token:' + process.env['INPUT_TOKEN']).toString('base64');
run(['-C', workspace, 'config', '--local', credentialKey, 'AUTHORIZATION: basic ' + credential]);
const revision = process.env['INPUT_REF'] || process.env.GITHUB_SHA;
run(['-C', workspace, 'fetch', '--quiet', '--depth=1', 'origin', revision]);
run(['-C', workspace, 'checkout', '--quiet', '--detach', 'FETCH_HEAD']);
if (process.env['INPUT_PERSIST-CREDENTIALS'] === 'false') {
  removeCredential();
}
fs.appendFileSync(process.env.GITHUB_STATE, 'checked_out=true\n');
console.log('external checkout main ran');
`

type fixtureRevisions struct {
	PushSHA        string `json:"pushSHA"`
	IntegrationSHA string `json:"integrationSHA"`
	BaseSHA        string `json:"baseSHA"`
	HeadSHA        string `json:"headSHA"`
	MergeBaseSHA   string `json:"mergeBaseSHA"`
}

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

if (process.versions.node.split('.')[0] !== '20') {
  throw new Error('setup-go fixture did not run with Node 20');
}

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

const kindActionMetadata = `name: "Kind Cluster"
description: "Create a kind (Kubernetes IN Docker) cluster"
author: "The Helm authors"
branding:
  color: blue
  icon: box
inputs:
  version:
    description: "The kind version to use (default: v0.31.0)"
    required: false
    default: "v0.31.0"
  config:
    description: "The path to the kind config file"
    required: false
  kubeconfig:
    description: "The path to the kubeconfig config file"
    required: false
  node_image:
    description: "The Docker image for the cluster nodes"
    required: false
  cluster_name:
    description: "The name of the cluster to create (default: chart-testing)"
    required: false
    default: "chart-testing"
  wait:
    description: "The duration to wait for the control plane to become ready (default: 60s)"
    required: false
    default: "60s"
  verbosity:
    description: "The verbosity level for kind (default: 0)"
    default: "0"
    required: false
  kubectl_version:
    description: "The kubectl version to use (default: v1.35.0)"
    required: false
    default: "v1.35.0"
  registry:
    description: "Whether to configure an insecure local registry (default: false)"
    required: false
    default: "false"
  registry_image:
    description: "The registry image to use (default: registry:2)"
    required: false
    default: "registry:2"
  registry_name:
    description: "The registry name to use (default: kind-registry)"
    required: false
    default: "kind-registry"
  registry_port:
    description: "The local port used to bind the registry (default: 5000)"
    required: false
    default: "5000"
  registry_enable_delete:
    description: "Enable delete operations (default: false)"
    required: false
    default: "false"
  install_only:
    description: "Skips cluster creation, only install kind (default: false)"
    default: "false"
    required: false
  ignore_failed_clean:
    description: "Whether to ignore the post-delete the cluster (default: false)"
    default: "false"
    required: false
  cloud_provider:
    description: "Whether to use cloud provider loadbalancer (default: false)"
    required: false
    default: "false"
runs:
  using: "node24"
  main: "main.js"
  post: "cleanup.js"
`

const kindActionScript = `const childProcess = require('child_process');
const fs = require('fs');

if (process.versions.node.split('.')[0] !== '24') {
  throw new Error('kind-action fixture did not run with Node 24');
}
if (process.env['INPUT_CLUSTER_NAME'] !== 'kind') {
  throw new Error('kind-action fixture did not receive its input');
}
const dockerVersion = childProcess.execFileSync('docker', ['version', '--format', '{{.Server.Version}}'], {encoding: 'utf8'}).trim();
if (!dockerVersion) {
  throw new Error('kind-action fixture could not reach Docker');
}
fs.appendFileSync(process.env.GITHUB_ENV, 'KIND_ACTION_RUNTIME=24\n');
fs.appendFileSync(process.env.GITHUB_ENV, 'KIND_ACTION_DOCKER=ready\n');
fs.appendFileSync(process.env.GITHUB_STATE, 'kind_cluster=created\n');
console.log('external kind-action Docker ready');
console.log('external kind-action main ran');
`

const kindActionCleanupScript = `if (process.versions.node.split('.')[0] !== '24') {
  throw new Error('kind-action cleanup did not run with Node 24');
}
if (process.env.STATE_kind_cluster !== 'created') {
  throw new Error('kind-action cleanup did not receive action state');
}
console.log('external kind-action post ran');
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

const uploadArtifactMetadata = `name: Upload artifact fixture
inputs:
  name:
    default: artifact
  path:
    required: true
runs:
  using: node24
  main: dist/index.js
`

const uploadArtifactScript = `const crypto = require('crypto');
const fs = require('fs');
const path = require('path');

function backendIDs() {
  const payload = JSON.parse(Buffer.from(process.env.ACTIONS_RUNTIME_TOKEN.split('.')[1], 'base64url').toString());
  const scope = payload.scp.split(' ').find(value => value.startsWith('Actions.Results:')).split(':');
  return {run: scope[1], job: scope[2]};
}

async function rpc(method, body) {
  const response = await fetch(new URL('/twirp/github.actions.results.api.v1.ArtifactService/' + method, process.env.ACTIONS_RESULTS_URL), {
    method: 'POST', headers: {'Authorization': 'Bearer ' + process.env.ACTIONS_RUNTIME_TOKEN, 'Content-Type': 'application/json'}, body: JSON.stringify(body)
  });
  const result = await response.json();
  if (!response.ok) throw new Error(method + ': ' + result.msg);
  return result;
}

function crc32(data) {
  let crc = 0xffffffff;
  for (const byte of data) {
    crc ^= byte;
    for (let bit = 0; bit < 8; bit++) crc = (crc >>> 1) ^ (crc & 1 ? 0xedb88320 : 0);
  }
  return (crc ^ 0xffffffff) >>> 0;
}

function zip(name, data) {
  const fileName = Buffer.from(name);
  const crc = crc32(data);
  const local = Buffer.alloc(30);
  local.writeUInt32LE(0x04034b50, 0); local.writeUInt16LE(20, 4); local.writeUInt16LE(0, 6); local.writeUInt16LE(0, 8);
  local.writeUInt32LE(crc, 14); local.writeUInt32LE(data.length, 18); local.writeUInt32LE(data.length, 22); local.writeUInt16LE(fileName.length, 26);
  const central = Buffer.alloc(46);
  central.writeUInt32LE(0x02014b50, 0); central.writeUInt16LE(20, 4); central.writeUInt16LE(20, 6); central.writeUInt16LE(0, 8); central.writeUInt16LE(0, 10);
  central.writeUInt32LE(crc, 16); central.writeUInt32LE(data.length, 20); central.writeUInt32LE(data.length, 24); central.writeUInt16LE(fileName.length, 28);
  const end = Buffer.alloc(22);
  end.writeUInt32LE(0x06054b50, 0); end.writeUInt16LE(1, 8); end.writeUInt16LE(1, 10);
  end.writeUInt32LE(central.length + fileName.length, 12); end.writeUInt32LE(local.length + fileName.length + data.length, 16);
  return Buffer.concat([local, fileName, data, central, fileName, end]);
}

async function run() {
  const name = process.env.INPUT_NAME || 'artifact';
  const source = path.resolve(process.env.GITHUB_WORKSPACE, process.env.INPUT_PATH);
  const content = zip(path.basename(source), fs.readFileSync(source));
  const ids = backendIDs();
  const created = await rpc('CreateArtifact', {workflow_run_backend_id: ids.run, workflow_job_run_backend_id: ids.job, name, version: 7, mime_type: 'application/zip'});
  const blockID = Buffer.from('block-0001').toString('base64');
  const upload = new URL(created.signed_upload_url);
  upload.searchParams.set('comp', 'block'); upload.searchParams.set('blockid', blockID);
  let response = await fetch(upload, {method: 'PUT', body: content, headers: {'Content-Type': 'application/octet-stream', 'x-ms-version': '2021-12-02'}});
  if (!response.ok) throw new Error('stage block: ' + response.status);
  upload.searchParams.set('comp', 'blocklist'); upload.searchParams.delete('blockid');
  response = await fetch(upload, {method: 'PUT', body: '<BlockList><Latest>' + blockID + '</Latest></BlockList>', headers: {'Content-Type': 'application/xml', 'x-ms-version': '2021-12-02'}});
  if (!response.ok) throw new Error('commit block list: ' + response.status);
  const digest = 'sha256:' + crypto.createHash('sha256').update(content).digest('hex');
  const finalized = await rpc('FinalizeArtifact', {workflow_run_backend_id: ids.run, workflow_job_run_backend_id: ids.job, name, size: String(content.length), hash: digest});
  fs.appendFileSync(process.env.GITHUB_OUTPUT, 'artifact-id=' + finalized.artifact_id + '\nartifact-digest=' + digest + '\n');
  console.log('artifact ' + name + ' uploaded');
}

run().catch(error => { console.error(error); process.exitCode = 1; });
`

const downloadArtifactMetadata = `name: Download artifact fixture
inputs:
  name:
    required: false
  pattern:
    required: false
  path:
    required: false
  merge-multiple:
    default: "false"
runs:
  using: node24
  main: dist/index.js
`

const downloadArtifactScript = `const fs = require('fs');
const path = require('path');

function backendIDs() {
  const payload = JSON.parse(Buffer.from(process.env.ACTIONS_RUNTIME_TOKEN.split('.')[1], 'base64url').toString());
  const scope = payload.scp.split(' ').find(value => value.startsWith('Actions.Results:')).split(':');
  return {run: scope[1], job: scope[2]};
}

async function rpc(method, body) {
  const response = await fetch(new URL('/twirp/github.actions.results.api.v1.ArtifactService/' + method, process.env.ACTIONS_RESULTS_URL), {
    method: 'POST', headers: {'Authorization': 'Bearer ' + process.env.ACTIONS_RUNTIME_TOKEN, 'Content-Type': 'application/json'}, body: JSON.stringify(body)
  });
  const result = await response.json();
  if (!response.ok) throw new Error(method + ': ' + result.msg);
  return result;
}

function extract(archive, destination) {
  let offset = 0;
  while (archive.readUInt32LE(offset) === 0x04034b50) {
    const size = archive.readUInt32LE(offset + 18);
    const nameLength = archive.readUInt16LE(offset + 26);
    const extraLength = archive.readUInt16LE(offset + 28);
    const name = archive.subarray(offset + 30, offset + 30 + nameLength).toString();
    if (path.basename(name) !== name) throw new Error('unsafe artifact path');
    const start = offset + 30 + nameLength + extraLength;
    fs.mkdirSync(destination, {recursive: true});
    fs.writeFileSync(path.join(destination, name), archive.subarray(start, start + size));
    offset = start + size;
  }
}

async function run() {
  const ids = backendIDs();
  const listed = await rpc('ListArtifacts', {workflow_run_backend_id: ids.run, workflow_job_run_backend_id: ids.job});
  const name = process.env.INPUT_NAME || '';
  const pattern = process.env.INPUT_PATTERN || '';
  const expression = pattern ? new RegExp('^' + pattern.replace(/[.+?^${}()|[\]\\]/g, '\\$&').replaceAll('*', '.*') + '$') : null;
  const artifacts = listed.artifacts.filter(artifact => name ? artifact.name === name : !expression || expression.test(artifact.name));
  const root = path.resolve(process.env.GITHUB_WORKSPACE, process.env.INPUT_PATH || '.');
  for (const artifact of artifacts) {
    const signed = await rpc('GetSignedArtifactURL', {workflow_run_backend_id: ids.run, workflow_job_run_backend_id: artifact.workflow_job_run_backend_id, name: artifact.name});
    const response = await fetch(signed.signed_url);
    if (!response.ok) throw new Error('download artifact: ' + response.status);
    const destination = process.env['INPUT_MERGE-MULTIPLE'] === 'true' || artifacts.length === 1 ? root : path.join(root, artifact.name);
    extract(Buffer.from(await response.arrayBuffer()), destination);
  }
  fs.appendFileSync(process.env.GITHUB_OUTPUT, 'download-path=' + root + '\n');
  console.log('downloaded ' + artifacts.length + ' artifacts');
}

run().catch(error => { console.error(error); process.exitCode = 1; });
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

const preparationCompositeMetadata = `name: Preparation composite fixture
inputs:
  message:
    required: true
runs:
  using: composite
  steps:
    - id: marker
      uses: preparation-actions/marker@v1
      with:
        message: ${{ inputs.message }}
    - name: Export preparation result
      run: |
        printf 'PREPARATION_VALUE=%s\n' "${{ steps.marker.outputs.value }}" >> "$GITHUB_ENV"
        printf 'external preparation composite run\n'
      shell: bash
`

func main() {
	dataDirectory := flag.String("data-dir", "/data", "Directory used for fixture Git repositories")
	listenAddress := flag.String("listen-address", ":8080", "HTTP listen address")
	flag.Parse()
	revisions, err := createRepositories(*dataDirectory)
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
	authenticatedGitBackend := func(token string) http.Handler {
		return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			authorizations := request.Header.Values("Authorization")
			expected := "basic " + base64.StdEncoding.EncodeToString([]byte("x-access-token:"+token))
			if len(authorizations) != 1 || authorizations[0] != expected {
				http.Error(writer, fmt.Sprintf("unexpected Authorization headers: %d", len(authorizations)), http.StatusUnauthorized)
				return
			}
			gitBackend.ServeHTTP(writer, request)
		})
	}
	jobGitBackend := authenticatedGitBackend(fixtureJobToken)
	actionGitBackend := authenticatedGitBackend(fixtureActionToken)
	mux.Handle("/acme/", jobGitBackend)
	mux.Handle("/actions/", actionGitBackend)
	mux.Handle("/helm/", actionGitBackend)
	preparationActionsMutex := sync.RWMutex{}
	preparationActionsBlocked := false
	installationTokenRequestsMutex := sync.RWMutex{}
	installationTokenRequests := map[string][]installationTokenRequest{}
	mux.Handle("/preparation-actions/", http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		preparationActionsMutex.RLock()
		blocked := preparationActionsBlocked
		preparationActionsMutex.RUnlock()
		if blocked {
			http.Error(writer, "preparation action downloads are blocked", http.StatusServiceUnavailable)
			return
		}
		actionGitBackend.ServeHTTP(writer, request)
	}))
	mux.HandleFunc("/fixture/block-preparation-actions", func(writer http.ResponseWriter, request *http.Request) {
		blocked := false
		switch request.Method {
		case http.MethodPost:
			blocked = true
		case http.MethodDelete:
		default:
			http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		preparationActionsMutex.Lock()
		preparationActionsBlocked = blocked
		preparationActionsMutex.Unlock()
		writer.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("/app/installations/", func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost {
			http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if !strings.HasSuffix(request.URL.Path, "/access_tokens") {
			http.NotFound(writer, request)
			return
		}
		installationID := strings.TrimSuffix(strings.TrimPrefix(request.URL.Path, "/app/installations/"), "/access_tokens")
		if installationID == "" || strings.Contains(installationID, "/") {
			http.NotFound(writer, request)
			return
		}
		body := installationTokenRequest{}
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			http.Error(writer, "invalid token request", http.StatusBadRequest)
			return
		}
		var token string
		switch {
		case len(body.Repositories) == 0:
			token = fixtureActionToken
		case len(body.Repositories) == 1 && body.Repositories[0] != "":
			token = fixtureJobToken
		default:
			http.Error(writer, "unexpected repository scope", http.StatusBadRequest)
			return
		}
		installationTokenRequestsMutex.Lock()
		installationTokenRequests[installationID] = append(installationTokenRequests[installationID], body)
		installationTokenRequestsMutex.Unlock()
		writeJSON(writer, map[string]string{"token": token})
	})
	mux.HandleFunc("/installation/repositories", func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet {
			http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		writeJSON(writer, map[string]any{"total_count": 0, "repositories": []any{}})
	})
	mux.HandleFunc("/installation/token", func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodDelete {
			http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		authorization := request.Header.Get("Authorization")
		if authorization != "Bearer "+fixtureJobToken && authorization != "Bearer "+fixtureActionToken {
			http.Error(writer, "unauthorized", http.StatusUnauthorized)
			return
		}
		writer.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("/repos/acme/example/contents/.open-actions/workflows", func(writer http.ResponseWriter, _ *http.Request) {
		writeJSON(writer, []map[string]string{{"name": "ci.yaml", "path": workflowPath, "type": "file"}})
	})
	mux.HandleFunc("/repos/acme/example/contents/"+workflowPath, func(writer http.ResponseWriter, _ *http.Request) {
		writeJSON(writer, map[string]string{"encoding": "base64", "content": base64.StdEncoding.EncodeToString([]byte(workflowData))})
	})
	mux.HandleFunc("/repos/acme/example/contents/"+preparationWorkflowPath, func(writer http.ResponseWriter, _ *http.Request) {
		writeJSON(writer, map[string]string{"encoding": "base64", "content": base64.StdEncoding.EncodeToString([]byte(preparationWorkflowData))})
	})
	mux.HandleFunc("/repos/acme/example/contents/"+dynamicMatrixWorkflowPath, func(writer http.ResponseWriter, _ *http.Request) {
		writeJSON(writer, map[string]string{"encoding": "base64", "content": base64.StdEncoding.EncodeToString([]byte(dynamicMatrixWorkflowData))})
	})
	mux.HandleFunc("/repos/acme/example/contents/"+artifactWorkflowPath, func(writer http.ResponseWriter, _ *http.Request) {
		writeJSON(writer, map[string]string{"encoding": "base64", "content": base64.StdEncoding.EncodeToString([]byte(artifactWorkflowData))})
	})
	mux.HandleFunc("/repos/acme/example/contents/"+tokenPermissionsWorkflowPath, func(writer http.ResponseWriter, _ *http.Request) {
		writeJSON(writer, map[string]string{"encoding": "base64", "content": base64.StdEncoding.EncodeToString([]byte(tokenPermissionsWorkflowData))})
	})
	mux.HandleFunc("/repos/acme/example/contents/"+jobConcurrencyWorkflowPath, func(writer http.ResponseWriter, _ *http.Request) {
		writeJSON(writer, map[string]string{"encoding": "base64", "content": base64.StdEncoding.EncodeToString([]byte(jobConcurrencyWorkflowData))})
	})
	mux.HandleFunc("/repos/acme/example/contents/"+concurrencyConflictWorkflowPath, func(writer http.ResponseWriter, _ *http.Request) {
		writeJSON(writer, map[string]string{"encoding": "base64", "content": base64.StdEncoding.EncodeToString([]byte(concurrencyConflictWorkflowData))})
	})
	checkRunMutex := sync.RWMutex{}
	checkRuns := map[string]map[string]any{}
	recordCheckRun := func(writer http.ResponseWriter, request *http.Request) {
		body := struct {
			DetailsURL string `json:"details_url"`
			ExternalID string `json:"external_id"`
			Status     string `json:"status"`
			Conclusion string `json:"conclusion"`
		}{}
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil || body.ExternalID == "" {
			http.Error(writer, "invalid check run", http.StatusBadRequest)
			return
		}
		result := map[string]any{
			"id": 1, "details_url": body.DetailsURL, "external_id": body.ExternalID,
			"status": body.Status, "conclusion": body.Conclusion,
		}
		checkRunMutex.Lock()
		checkRuns[body.ExternalID] = result
		checkRunMutex.Unlock()
		writeJSON(writer, result)
	}
	mux.HandleFunc("/repos/acme/example/check-runs", func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost {
			http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		recordCheckRun(writer, request)
	})
	mux.HandleFunc("/repos/acme/example/check-runs/1", func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPatch {
			http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		recordCheckRun(writer, request)
	})
	mux.HandleFunc("/repos/acme/example/commits/", func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet || !strings.HasSuffix(request.URL.Path, "/check-runs") {
			http.NotFound(writer, request)
			return
		}
		writeJSON(writer, map[string]any{"total_count": 0, "check_runs": []any{}})
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
	mux.HandleFunc("/fixture/revisions", func(writer http.ResponseWriter, _ *http.Request) {
		writeJSON(writer, revisions)
	})
	mux.HandleFunc("/fixture/installation-token-requests/", func(writer http.ResponseWriter, request *http.Request) {
		installationID := strings.TrimPrefix(request.URL.Path, "/fixture/installation-token-requests/")
		if installationID == "" || strings.Contains(installationID, "/") {
			http.NotFound(writer, request)
			return
		}
		switch request.Method {
		case http.MethodGet:
			installationTokenRequestsMutex.RLock()
			requests := append([]installationTokenRequest{}, installationTokenRequests[installationID]...)
			installationTokenRequestsMutex.RUnlock()
			writeJSON(writer, requests)
		case http.MethodDelete:
			installationTokenRequestsMutex.Lock()
			delete(installationTokenRequests, installationID)
			installationTokenRequestsMutex.Unlock()
			writer.WriteHeader(http.StatusNoContent)
		default:
			http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		}
	})
	mux.HandleFunc("/fixture/check-runs/", func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet {
			http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		externalID := strings.TrimPrefix(request.URL.Path, "/fixture/check-runs/")
		checkRunMutex.RLock()
		result, found := checkRuns[externalID]
		checkRunMutex.RUnlock()
		if !found {
			http.NotFound(writer, request)
			return
		}
		writeJSON(writer, result)
	})
	mux.HandleFunc("/healthz", func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusOK)
	})
	server := &http.Server{Addr: *listenAddress, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	log.Fatal(server.ListenAndServe())
}

func createRepositories(dataDirectory string) (fixtureRevisions, error) {
	if err := os.RemoveAll(filepath.Join(dataDirectory, "git")); err != nil {
		return fixtureRevisions{}, err
	}
	if err := os.RemoveAll(filepath.Join(dataDirectory, "work")); err != nil {
		return fixtureRevisions{}, err
	}
	mergeBaseSHA, err := createRepository(dataDirectory, "acme", "example", "", map[string]string{
		pullRequestWorkflowPath:  pullRequestWorkflowData,
		"Dockerfile.docker-e2e":  "FROM scratch\nCOPY docker-e2e /docker-e2e\nENTRYPOINT [\"/docker-e2e\"]\n",
		"cmd/docker-e2e/main.go": "package main\n\nimport \"fmt\"\n\nfunc main() { fmt.Println(\"open actions docker e2e works\") }\n",
		"go.mod":                 fmt.Sprintf("module example.com/fixture\n\ngo %s\n", strings.TrimPrefix(runtime.Version(), "go")),
		"fixture.go":             "package fixture\n\nfunc Message() string { return \"open actions e2e works\" }\n",
		"fixture_test.go":        "package fixture\n\nimport \"testing\"\n\nfunc TestMessage(t *testing.T) {\n\tif Message() != \"open actions e2e works\" { t.Fatal(\"unexpected message\") }\n}\n",
	})
	if err != nil {
		return fixtureRevisions{}, err
	}
	revisions, err := createPullRequestRevisions(dataDirectory, mergeBaseSHA)
	if err != nil {
		return fixtureRevisions{}, err
	}
	if _, err := createRepository(dataDirectory, "actions", "checkout", "v4", map[string]string{
		"action.yml":    checkoutMetadata,
		"dist/index.js": checkoutScript,
	}); err != nil {
		return fixtureRevisions{}, err
	}
	if _, err := createRepository(dataDirectory, "actions", "setup-go", "v5", map[string]string{
		"action.yml":    setupGoMetadata,
		"dist/index.js": setupGoScript,
	}); err != nil {
		return fixtureRevisions{}, err
	}
	if _, err := createRepository(dataDirectory, "helm", "kind-action", "v1", map[string]string{
		"action.yml": kindActionMetadata,
		"main.js":    kindActionScript,
		"cleanup.js": kindActionCleanupScript,
	}); err != nil {
		return fixtureRevisions{}, err
	}
	if _, err := createRepository(dataDirectory, "actions", "marker", "v1", map[string]string{
		"action.yml":    markerMetadata,
		"dist/index.js": markerScript,
	}); err != nil {
		return fixtureRevisions{}, err
	}
	if _, err := createRepository(dataDirectory, "actions", "composite", "v1", map[string]string{
		"action.yml": compositeMetadata,
	}); err != nil {
		return fixtureRevisions{}, err
	}
	if _, err := createRepository(dataDirectory, "actions", "upload-artifact", "v7", map[string]string{
		"action.yml": uploadArtifactMetadata, "dist/index.js": uploadArtifactScript,
	}); err != nil {
		return fixtureRevisions{}, err
	}
	if _, err := createRepository(dataDirectory, "actions", "download-artifact", "v8", map[string]string{
		"action.yml": downloadArtifactMetadata, "dist/index.js": downloadArtifactScript,
	}); err != nil {
		return fixtureRevisions{}, err
	}
	if _, err := createRepository(dataDirectory, "preparation-actions", "marker", "v1", map[string]string{
		"action.yml":    markerMetadata,
		"dist/index.js": markerScript,
	}); err != nil {
		return fixtureRevisions{}, err
	}
	if _, err := createRepository(dataDirectory, "preparation-actions", "composite", "v1", map[string]string{
		"action.yml": preparationCompositeMetadata,
	}); err != nil {
		return fixtureRevisions{}, err
	}
	return revisions, nil
}

func createPullRequestRevisions(dataDirectory, mergeBaseSHA string) (fixtureRevisions, error) {
	workDirectory := filepath.Join(dataDirectory, "work", "acme", "example")
	bareDirectory := filepath.Join(dataDirectory, "git", "acme", "example")
	run := func(description string, arguments ...string) error {
		output, err := exec.Command("git", append([]string{"-C", workDirectory}, arguments...)...).CombinedOutput()
		if err != nil {
			return fmt.Errorf("%s: %w: %s", description, err, output)
		}
		return nil
	}
	if err := run("create feature branch", "switch", "--quiet", "-c", "feature"); err != nil {
		return fixtureRevisions{}, err
	}
	if err := os.WriteFile(filepath.Join(workDirectory, "head.txt"), []byte("head\n"), 0o644); err != nil {
		return fixtureRevisions{}, err
	}
	if err := run("stage head revision", "add", "head.txt"); err != nil {
		return fixtureRevisions{}, err
	}
	if err := run("commit head revision", "commit", "--quiet", "-m", "head"); err != nil {
		return fixtureRevisions{}, err
	}
	headSHA, err := repositoryRevision(workDirectory)
	if err != nil {
		return fixtureRevisions{}, err
	}
	if err := run("switch to main branch", "switch", "--quiet", "main"); err != nil {
		return fixtureRevisions{}, err
	}
	if err := os.WriteFile(filepath.Join(workDirectory, "base.txt"), []byte("base\n"), 0o644); err != nil {
		return fixtureRevisions{}, err
	}
	if err := run("stage base revision", "add", "base.txt"); err != nil {
		return fixtureRevisions{}, err
	}
	if err := run("commit base revision", "commit", "--quiet", "-m", "base"); err != nil {
		return fixtureRevisions{}, err
	}
	baseSHA, err := repositoryRevision(workDirectory)
	if err != nil {
		return fixtureRevisions{}, err
	}
	if err := run("publish pull request revisions", "push", "--quiet", bareDirectory, "main", "feature"); err != nil {
		return fixtureRevisions{}, err
	}
	integrationSHA, err := integrationRevision(workDirectory, baseSHA, headSHA)
	if err != nil {
		return fixtureRevisions{}, err
	}
	return fixtureRevisions{
		PushSHA: baseSHA, IntegrationSHA: integrationSHA, BaseSHA: baseSHA, HeadSHA: headSHA, MergeBaseSHA: mergeBaseSHA,
	}, nil
}

func repositoryRevision(directory string) (string, error) {
	output, err := exec.Command("git", "-C", directory, "rev-parse", "HEAD").CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("resolve repository revision: %w: %s", err, output)
	}
	return strings.TrimSpace(string(output)), nil
}

func integrationRevision(directory, baseSHA, headSHA string) (string, error) {
	output, err := exec.Command("git", "-C", directory, "merge-tree", "--write-tree", "--no-messages", baseSHA, headSHA).CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("construct integration tree: %w: %s", err, output)
	}
	treeSHA := strings.TrimSpace(string(output))
	command := exec.Command("git", "-C", directory, "commit-tree", treeSHA, "-p", baseSHA, "-p", headSHA)
	command.Env = append(os.Environ(),
		"GIT_AUTHOR_NAME=Open Actions",
		"GIT_AUTHOR_EMAIL=open-actions@localhost",
		"GIT_AUTHOR_DATE=2000-01-01T00:00:00Z",
		"GIT_COMMITTER_NAME=Open Actions",
		"GIT_COMMITTER_EMAIL=open-actions@localhost",
		"GIT_COMMITTER_DATE=2000-01-01T00:00:00Z",
	)
	command.Stdin = strings.NewReader(fmt.Sprintf("Merge %s into %s\n", headSHA, baseSHA))
	output, err = command.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("commit integration revision: %w: %s", err, output)
	}
	return strings.TrimSpace(string(output)), nil
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
