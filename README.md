# Open Actions

Open Actions is an open, Kubernetes-native alternative to GitHub Actions. It is
a self-hosted control plane for high-volume automation workloads, built for
teams whose CI demand is growing faster than a hosted scheduler can comfortably
handle. Source events trigger supported workflows, while Open Actions schedules
their jobs in the team's own Kubernetes cluster. GitHub webhooks are the first
source integration. Workflow planning, scheduling, and execution belong to Open
Actions and run on self-hosted infrastructure.

![Open Actions Console showing live workflow runner logs](docs/open-actions-console.png)

Kubernetes 1.29 or newer is required.

The supported execution subset includes:

- webhook, manual, scheduled, chained, and reusable-workflow trigger declarations used by kelos
- workflow concurrency groups and `cancel-in-progress`
- runner-label scheduling for independent jobs using Kubernetes `Runner` and
  `WorkflowJob` resources
- external `owner/repository[/path]@ref` Node 20, Node 24, and composite
  actions, including nested external actions
- action inputs, pre/main/post hooks, and environment, PATH, and state file commands
- composite run and uses steps, step outputs, and composite outputs
- `::command::` and `##[command]` workflow commands, including problem matcher annotations
- Bash `run` steps
- optional job-scoped Docker daemons for Docker-dependent steps and Node actions,
  including the default `helm/kind-action` cluster workflow
- job and step environment variables and working directories
- typed expressions and string interpolation in concurrency, job planning,
  workflow steps, and composite actions
- GitHub Check Runs linked to authenticated Console run and job pages
- GitHub-style live runner logs with collapsible groups, annotations, debug
  filtering, and timestamps while the native Kubernetes Job is retained

Unsupported workflow fields and action reference forms are rejected during
planning. Unsupported action runtimes fail explicitly in the runner so a
workflow is never silently executed with different semantics.

## Architecture

![Open Actions controller, Console, and execution flow](docs/architecture.drawio.svg)

The `open-actions-controller` accepts GitHub webhooks, discovers workflows, and
creates `WorkflowRun` and `WorkflowJob` resources. It assigns queued jobs to
matching `Runner` resources, which execute the workflow steps in Kubernetes
Jobs. The controller also reports WorkflowRun state through GitHub Check Runs.
The `open-actions-console` serves an authenticated cluster-wide run overview,
run and job pages, and runner Pod logs. The runner interprets GitHub Actions
workflow commands and problem matchers, while the Console presents groups,
commands, annotations, masked action inputs and outputs, debug messages, and
post actions. A `Project` defines the execution domain and its GitHub App
integration.

## Deploy Open Actions

Before starting, you need:

- a Kubernetes 1.29 or newer cluster selected by the current `kubectl` context
- Helm on `PATH`
- a public HTTPS endpoint that can route GitHub webhooks to the cluster
- Go, only for installing the CLI with `go install`

Install the CLI:

```console
go install github.com/kelos-dev/open-actions/cmd/open-actions@latest
```

For a publicly exposed Console, save a `values.yaml` containing the URL users
and GitHub Check Runs can reach:

```yaml
console:
  publicURL: https://actions.example.com
```

Install the CRDs, RBAC, controller, Console, and Services. The command below
uses the public Console values; run `open-actions install` instead for the
local-only defaults:

```console
open-actions install --values values.yaml
kubectl rollout status deployment/open-actions-controller \
  --namespace open-actions-system
```

The install command installs or upgrades the embedded Helm chart as the
`open-actions` release in the `open-actions-system` namespace. It uses Helm's
current Kubernetes context. All chart settings are documented in
[`internal/manifests/charts/open-actions`](internal/manifests/charts/open-actions).

The chart does not create an Ingress or TLS certificate. Use an Ingress,
Gateway, or external load balancer to expose these Services:

- route the GitHub App webhook URL, such as
  `https://open-actions-webhook.example.com`, to
  `open-actions-webhook.open-actions-system:80`
- route `console.publicURL` to
  `open-actions-console.open-actions-system:80`

The webhook endpoint must be reachable by GitHub over HTTPS. Keep the Console
private if cluster-wide run and log access should not be public. For local-only
Console access, keep its default URL and run:

```console
kubectl port-forward --namespace open-actions-system \
  service/open-actions-console 8080:80
```

The chart creates a random Console administrator token and preserves it across
upgrades. Retrieve it and enter it on the Console login page:

```console
kubectl get secret open-actions-console-auth \
  --namespace open-actions-system \
  --output jsonpath='{.data.token}' | base64 --decode; echo
```

The token grants read access to every run, job, and runner Pod log visible to
the Console. Keep it secret and use HTTPS whenever the Console is exposed
outside a trusted local connection. To manage the token outside Helm, create a
Secret in the release namespace and set `console.secretName` to its name.
`console.tokenKey` selects the key, which defaults to `token`.

The Console uses a separate ServiceAccount limited to reading workflow runs,
workflow jobs, Pods, and Pod logs.

## Create and install the GitHub App

Open Actions uses a GitHub App for webhook delivery, repository access, runner
job tokens, and Check Runs. Create a dedicated App for each Open Actions
deployment. It may be private when the App owner is also the installation
target. Runner job tokens remain restricted to the selected repository with
Contents read access.

First generate a high-entropy webhook secret. Keep this shell open until the
Kubernetes Secret is created below:

```console
OA_WEBHOOK_SECRET="$(openssl rand -hex 32)"
printf '%s\n' "$OA_WEBHOOK_SECRET"
```

Then [register a GitHub App](https://docs.github.com/en/apps/creating-github-apps/registering-a-github-app/registering-a-github-app)
under the organization or account that will own it:

1. Choose a unique App name and use the public Console URL or this repository's
   URL as the Homepage URL.
2. Enable the webhook, use the public Open Actions webhook URL, paste
   `OA_WEBHOOK_SECRET` as the webhook secret, and keep SSL verification enabled.
   OAuth user authorization and a callback URL are not required.
3. Grant these repository permissions:

   - **Contents: Read-only** for workflow discovery, action checkout, and job
     execution.
   - **Actions: Read-only** for `workflow_run` webhooks.
   - **Issues: Read-only** for `issues` and `issue_comment` webhooks.
   - **Pull requests: Read-only** for pull request and review webhooks.
   - **Merge queues: Read-only** for `merge_group` webhooks.
   - **Checks: Read and write** for WorkflowRun reporting.

4. Subscribe to **Push**, **Pull request**, **Merge group**, **Workflow run**,
   **Issues**, **Issue comment**, **Pull request review comment**, **Pull request
   review**, and **Release** events.
5. Choose **Only on this account** when the App will be installed on its owner;
   otherwise choose **Any account**.
6. Create the App and record its numeric **App ID**, not its Client ID.
7. On the App settings page, generate and download a private key. GitHub
   provides it once as a PEM file; store it as a secret.
8. Select **Install App**, install it on the target account, and grant it access
   to every repository that Open Actions should run. The numeric final segment
   of the installation's configuration-page URL is the installation ID.

GitHub's current UI steps are also documented in
[Installing your own GitHub App](https://docs.github.com/en/apps/using-github-apps/installing-your-own-github-app)
and
[Managing private keys for GitHub Apps](https://docs.github.com/en/apps/creating-github-apps/authenticating-with-a-github-app/managing-private-keys-for-github-apps).

### Connect the App installation

Create a workload namespace and a Secret containing the downloaded private key
and the same webhook secret entered in GitHub:

```console
kubectl create namespace open-actions
kubectl create secret generic open-actions-github-app \
  --namespace open-actions \
  --from-file=private-key.pem=/absolute/path/to/open-actions.private-key.pem \
  --from-literal=webhook-secret="$OA_WEBHOOK_SECRET"
```

Save the following as `open-actions-resources.yaml`, replacing `APP_ID` and
`INSTALLATION_ID` with the numeric values from GitHub:

```yaml
apiVersion: actions.kelos.dev/v1alpha1
kind: Project
metadata:
  name: default
  namespace: open-actions
spec:
  source:
    type: GitHub
    github:
      appID: APP_ID
      installationID: INSTALLATION_ID
      privateKeySecretRef:
        name: open-actions-github-app
        key: private-key.pem
      webhookSecretRef:
        name: open-actions-github-app
        key: webhook-secret
  workflowDirectory: .open-actions/workflows
---
apiVersion: actions.kelos.dev/v1alpha1
kind: Runner
metadata:
  name: runner-1
  namespace: open-actions
spec:
  projectRef:
    name: default
  execution:
    image: ghcr.io/kelos-dev/open-actions-runner:latest
    resources:
      requests:
        cpu: "1"
        memory: 1Gi
      limits:
        cpu: "2"
        memory: 2Gi
  labels:
    - self-hosted
    - linux
    - x64
    - ubuntu-latest
```

Apply the resources and wait for their local configuration checks to pass:

```console
kubectl apply -f open-actions-resources.yaml
kubectl wait project/default --for=condition=Configured \
  --namespace open-actions --timeout=60s
kubectl wait runner/runner-1 --for=condition=Ready \
  --namespace open-actions --timeout=60s
```

A Project represents one GitHub App installation and can discover every
repository selected during installation. Only one Project in the cluster may
use a given installation ID. Each Runner is one reusable execution slot; add
more Runner resources to increase concurrency. Every label requested by a
job's `runs-on` value must appear in the selected Runner's labels.

The readiness checks above validate the Kubernetes configuration and local
credentials. The first webhook and workflow run also verify the public route,
App installation, repository permissions, and GitHub API access.

## Migrate a GitHub Actions workflow

Open Actions deliberately supports a subset of GitHub Actions and rejects
unsupported workflow fields instead of silently changing their behavior. Check
the complete [Workflow API](docs/reference.md#workflow-api) before migrating a
workflow. The most common migration constraints are:

| Existing workflow feature | Open Actions migration |
| --- | --- |
| `runs-on` | Make every requested label match a Runner. The example Runner accepts `ubuntu-latest` jobs. |
| `run` steps | Use Bash; other shells are not supported. |
| `uses` steps | Use external `owner/repository[/path]@ref` Node 20, Node 24, or composite actions. Local and Docker actions are not supported. |
| `needs`, matrices, job-level `if`, job containers, and services | Rewrite or split the workflow; these job features are not supported. |
| Repository secrets and variables, caches, and artifacts | Replace the dependency before migrating; these sources and services are not supported. |
| Docker commands used by a Bash, Node, or composite step | Assign the job to a [Docker-enabled Runner](config/samples/actions_v1alpha1_docker_runner.yaml). This uses a privileged sidecar and does not enable Docker actions or job containers. |
| `workflow_dispatch` | Create a Kubernetes `WorkflowRun`; it is not started from GitHub's **Run workflow** button. See the [manual-run sample](config/samples/actions_v1alpha1_workflowrun-dispatch.yaml). |
| `schedule` | Open Actions evaluates five-field POSIX cron expressions from the default branch in UTC, with a minimum interval of five minutes. |
| `workflow_run` | It can consume native GitHub Actions completion webhooks, but an Open Actions completion does not emit another `workflow_run` event. |

Migrate one workflow at a time with a reversible cutover:

1. Copy, rather than move, the workflow into the Open Actions directory:

   ```console
   mkdir -p .open-actions/workflows
   cp .github/workflows/ci.yaml .open-actions/workflows/ci.yaml
   ```

2. Adjust the copied file for the supported subset and Runner labels. A minimal
   compatible workflow looks like this:

   ```yaml
   name: CI

   on:
     push:
       branches: [main]
     pull_request:

   jobs:
     test:
       runs-on: ubuntu-latest
       steps:
         - uses: actions/checkout@v4
         - uses: actions/setup-go@v5
           with:
             go-version-file: go.mod
         - run: make test
   ```

3. Commit the copied workflow on a branch in the same repository and open a
   pull request. Keeping `.github/workflows/ci.yaml` in place makes GitHub
   Actions and Open Actions run in parallel during validation. Ordinary
   `pull_request` runs from forks are skipped, so use a branch in the installed
   repository for this test.
4. In the GitHub App's **Advanced** settings, confirm the event delivery
   received a `202` response. Then confirm the `Open Actions /
   .open-actions/workflows/ci.yaml` Check Run and inspect it with:

   ```console
   open-actions run list --namespace open-actions
   open-actions run view RUN --namespace open-actions
   open-actions run logs RUN --job JOB --namespace open-actions
   ```

5. After equivalent runs pass, update any branch-protection rules to require
   the Open Actions check. Delete the original `.github/workflows/ci.yaml` in a
   follow-up change to complete the cutover. Restore that file to roll back.

A webhook delivery then follows the path shown in
[Architecture](#architecture). The `WorkflowJob` example under
[`config/samples`](config/samples) illustrates a controller-owned child shape
and is not applied independently.

## Inspecting workflow runs

The CLI reads workflow runs and runner logs through the Kubernetes API using the
active kubeconfig context. It defaults to that context's namespace; use
`--namespace` to select another namespace or `--all-namespaces` when listing
runs.

```console
open-actions run list --namespace team-ci
open-actions run view RUN --namespace team-ci
open-actions run logs RUN --job JOB --namespace team-ci
open-actions run logs RUN --job JOB --follow --namespace team-ci
```

`JOB` may be the workflow-local job ID shown by `run view` or the WorkflowJob
resource name. An exact resource-name match takes precedence over a job ID.
`--job` may be omitted when the run contains exactly one job.
Completed runner logs remain available until the native Kubernetes Job is
deleted by its retention policy.

## API reference

See the [API reference](docs/reference.md) for Kubernetes resources, command-line
configuration, supported workflow syntax, and the webhook contract.

## Development

```console
make update     # Format, generate Go code and CRDs, and tidy modules.
make verify     # Check generated files, formatting, modules, and go vet.
make test       # Run unit and schema tests.
make build      # Build the CLI, controller, Console, and runner binaries under bin/.
make image      # Build the controller, Console, and runner images.
```

Install the control plane before running the end-to-end suite against the
cluster selected by the current Kubernetes context:

```console
make build WHAT=cmd/open-actions
bin/open-actions install --values config/e2e/values.yaml
kubectl apply -f config/e2e/fixture.yaml
kubectl wait --for=condition=Available --timeout=180s \
  --namespace open-actions-system \
  deployment/open-actions-controller \
  deployment/open-actions-console \
  deployment/github-fixture
make test-e2e
```

The suite expects the control plane and GitHub fixture to be ready before it
starts.
