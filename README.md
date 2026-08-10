# Open Actions

Open Actions is a Kubernetes-native, self-hosted control plane for high-volume
automation workloads. It is built for teams whose CI demand is growing faster
than a hosted scheduler can comfortably handle: source events trigger supported
workflows, while Open Actions schedules their jobs in the team's own Kubernetes
cluster. GitHub webhooks are the first source integration. Workflow planning,
scheduling, and execution belong to Open Actions and run on self-hosted
infrastructure.

Kubernetes 1.29 or newer is required.

The supported execution subset includes:

- `push`, `pull_request`, and `merge_group` triggers with branch and action filters
- workflow concurrency groups and `cancel-in-progress`
- runner-label scheduling for independent jobs using Kubernetes `Runner` and
  `WorkflowJob` resources
- external `owner/repository[/path]@ref` JavaScript and composite actions,
  including nested external actions
- action inputs, pre/main/post hooks, and environment, PATH, and state file commands
- composite run and uses steps, step outputs, and composite outputs
- Bash `run` steps
- job and step environment variables and working directories

Unsupported workflow fields and action reference forms are rejected during
planning. Unsupported action runtimes fail explicitly in the runner so a
workflow is never silently executed with different semantics.

## Architecture

![Open Actions architecture and event flow](docs/architecture.drawio.svg)

The `open-actions-controller` accepts GitHub webhooks, discovers workflows, and
creates `WorkflowRun` and `WorkflowJob` resources. It assigns queued jobs to
matching `Runner` resources, which execute the workflow steps in Kubernetes
Jobs. A `Project` defines the execution domain and its GitHub App integration.

## Installation

Install the CLI:

```console
go install github.com/kelos-dev/open-actions/cmd/open-actions@latest
```

Then install the CRDs, RBAC, controller, and webhook Service in the cluster
selected by the current Kubernetes context:

```console
open-actions install
```

The command installs or upgrades the embedded Helm chart as the `open-actions`
release in the `open-actions-system` namespace. It requires Helm on `PATH` and
uses Helm's current Kubernetes context. Pass custom chart configuration with
`open-actions install --values values.yaml`. The chart and its values are
documented in
[`internal/manifests/charts/open-actions`](internal/manifests/charts/open-actions).

Create a Secret containing a GitHub App RSA private key and webhook secret, a
`Project`, and one or more `Runner` resources. Each `Runner` is one reusable
execution slot; create more Runners to increase concurrency. Examples are under
[`config/samples`](config/samples). The `WorkflowJob` manifest illustrates the
controller-owned child shape and is not applied independently. Expose the
`open-actions-webhook.open-actions-system` Service through HTTPS and set
that URL as the GitHub App's webhook URL. Subscribe the App to push, pull
request, and merge group events. Grant the App read access to these repository
permissions:

- Contents, for workflow discovery and job execution
- Pull requests, for `pull_request` webhooks
- Merge queues, for `merge_group` webhooks

Runner job tokens remain restricted to the selected repository with Contents
read access.

Move repository workflows from `.github/workflows` to
`.open-actions/workflows`. A webhook delivery then follows the path shown in
[Architecture](#architecture).

## API reference

See the [API reference](docs/reference.md) for Kubernetes resources, command-line
configuration, supported workflow syntax, and the webhook contract.

## Development

```console
make update     # Format, generate Go code and CRDs, and tidy modules.
make verify     # Check generated files, formatting, modules, and go vet.
make test       # Run unit and schema tests.
make build      # Build the CLI, controller, and runner binaries under bin/.
make image      # Build the controller and runner images.
```

Run `make test-e2e` against the cluster selected by the current Kubernetes
context. The end-to-end suite requires Helm.
