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

![Open Actions controller, Console, and execution flow](docs/architecture.drawio.svg)

The controller receives GitHub webhooks, creates `WorkflowRun` and `WorkflowJob`
resources, schedules jobs on matching `Runner` resources, and reports status
through GitHub Check Runs. Runners execute steps in Kubernetes Jobs. The Console
shows runs, jobs, and live logs. Each `Project` defines an execution domain and
its GitHub App integration.

## Migrate from GitHub Actions

### 1. Deploy Open Actions

Kubernetes 1.29 or newer, Helm, and a public HTTPS endpoint for GitHub webhooks
are required.

```console
go install github.com/kelos-dev/open-actions/cmd/open-actions@latest
open-actions install
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

- repository permissions: Contents read, Actions read, Issues read, Pull
  requests read, Merge queues read, and Checks read and write
- events: Push, Pull request, Merge group, Workflow run, Issues, Issue comment,
  Pull request review comment, Pull request review, and Release

Generate a private key, [install the App](https://docs.github.com/en/apps/using-github-apps/installing-your-own-github-app)
on the repositories to run, and record the App ID and installation ID. Then
create the Kubernetes Secret, replacing `WEBHOOK_SECRET` with the secret from
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
and a Runner:

```console
kubectl apply -f config/samples/actions_v1alpha1_project.yaml
kubectl apply -f config/samples/actions_v1alpha1_runner.yaml
```

Each Runner executes one job at a time. Its labels must include every label in
the job's `runs-on` value. Use the [Docker Runner
sample](config/samples/actions_v1alpha1_docker_runner.yaml) for jobs that need
Docker.

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
