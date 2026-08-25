# Open Actions

Open Actions is an open, Kubernetes-native alternative to the GitHub Actions
control plane. It is designed for high-volume automation workloads whose
workflow requests can saturate that control plane even when runner capacity is
available. GitHub webhooks trigger supported workflows, which Open Actions
plans, schedules, and executes in the team's own Kubernetes cluster. GitHub
webhooks are the first source integration.

![Open Actions Console showing live workflow runner logs](docs/open-actions-console.png)

Open Actions implements a documented subset of GitHub Actions. See the
[Workflow API](docs/reference.md#workflow-api) for supported syntax and
constraints. Unsupported workflows fail explicitly.

## Architecture

![Open Actions controller, artifact service, Console, and execution flow](docs/architecture.drawio.svg)

The controller receives GitHub webhooks, creates `WorkflowRun` and `WorkflowJob`
resources, schedules jobs on matching `Runner` resources, and reports status
through GitHub Check Runs. Runners execute steps in Kubernetes Jobs and use the
standalone artifact service for workflow artifact uploads and downloads. The
Console shows runs, jobs, and live logs. Each `Project` defines an execution
domain and its GitHub App integration.

## Migrate from GitHub Actions

### 1. Deploy Open Actions

Kubernetes 1.29 or newer, Helm, and a public HTTPS endpoint for GitHub webhooks
are required.

```console
curl -fsSL https://raw.githubusercontent.com/kelos-dev/open-actions/main/hack/install.sh | bash
```

Each tagged release also publishes the chart at
`oci://ghcr.io/kelos-dev/charts/open-actions`. The chart version omits the tag's
`v` prefix; for example, release `v0.1.0` is installed as chart version `0.1.0`
and uses `v0.1.0` for its controller, artifact server, and Console images:

```console
helm upgrade --install open-actions \
  oci://ghcr.io/kelos-dev/charts/open-actions \
  --version 0.1.0 \
  --namespace open-actions-system \
  --create-namespace
```

Expose `open-actions-webhook.open-actions-system:80` through HTTPS. The Console
is available locally with:

```console
kubectl port-forward --namespace open-actions-system \
  service/open-actions-console 8080:80
```

See the [chart values](internal/manifests/charts/open-actions) to expose the
Console or customize the installation.

### 2. Install the GitHub App

[Create a GitHub App](https://docs.github.com/en/apps/creating-github-apps/registering-a-github-app/registering-a-github-app)
with the public webhook URL and a webhook secret. Configure it with:

- repository permissions: Actions, Checks, Contents, Issues, Packages, Pull
  requests, and Commit statuses read and write, plus Merge queues read
- events: Push, Pull request, Merge group, Workflow run, Issues, Issue comment,
  Pull request review comment, Pull request review, Release, and Check run

Trusted fork workflows load definitions and `refs/pull/<number>/merge` from the
base repository with Contents read permission. The App does not need to be
installed on a public fork for that path. Checks read and write reports run
state; no GitHub App permission by itself makes fork code safe to execute.
Each job token is narrowed to the permissions requested by its workflow and
cannot exceed these App permissions.

Generate a private key, [install the App](https://docs.github.com/en/apps/using-github-apps/installing-your-own-github-app)
on every repository whose workflows or actions Open Actions runs, and record
the App ID and installation ID. The job token is limited to its Project
repository, while action downloads use a separate short-lived token for the
repositories granted to the installation. See
[External actions](docs/reference.md#external-actions).

Create the Kubernetes Secret, replacing `WEBHOOK_SECRET` with the secret from
GitHub:

```console
kubectl create namespace open-actions
kubectl create secret generic open-actions-github-app \
  --namespace open-actions \
  --from-file=private-key.pem=/path/to/private-key.pem \
  --from-literal=webhook-secret='<WEBHOOK_SECRET>'
```

Put the App and installation IDs in the [Project
sample](config/samples/actions_v1alpha1_project.yaml), then create the Project
and a RunnerSet:

```console
kubectl apply -f config/samples/actions_v1alpha1_project.yaml
kubectl apply -f config/samples/actions_v1alpha1_runnerset.yaml
```

Each RunnerSet maintains the number of Runner execution slots configured by
`spec.replicas`. Each Runner executes one job at a time, and its labels must
include every label in the job's `runs-on` value. The standard runner image is
based on Ubuntu 24.04. To add tools, build a [custom runner
image](examples/runner/Dockerfile) from the standard image and set
`spec.template.spec.execution.image` to its address. Use the [Docker Runner
sample](config/samples/actions_v1alpha1_docker_runner.yaml) for jobs that need
Docker by copying its `spec` into the RunnerSet's `spec.template.spec`.

### 3. Migrate a workflow

Open Actions supports a [subset of the GitHub Actions workflow
API](docs/reference.md#workflow-api). Check compatibility first, then copy one
workflow without deleting the original:

```console
mkdir -p .open-actions/workflows
cp .github/workflows/ci.yaml .open-actions/workflows/ci.yaml
```

Push the copy from a branch in the installed repository and confirm its
`Open Actions / .open-actions/workflows/ci.yaml` check passes. GitHub Actions
and Open Actions will run in parallel while both files exist. Then update any
required checks and delete `.github/workflows/ci.yaml`. Restore that file to
roll back.

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
Completed native Jobs and runner Pods are retained with their WorkflowRun, so
runner logs remain available until that run is deleted.

## API reference

See the [API reference](docs/reference.md) for Kubernetes resources, command-line
configuration, supported workflow syntax, and the webhook contract.

## Development

```console
make update     # Format, generate Go code and CRDs, and tidy modules.
make verify     # Check generated files, formatting, modules, and go vet.
make test       # Run unit and schema tests.
make build      # Build the CLI, controller, artifact service, Console, and runner binaries under bin/.
make image      # Build the controller, artifact service, Console, and runner images.
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
kubectl rollout status statefulset/open-actions-artifacts \
  --namespace open-actions-system \
  --timeout=180s
make test-e2e
```

The suite expects the control plane and GitHub fixture to be ready before it
starts.
