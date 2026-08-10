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
- GitHub Check Runs linked to authenticated Console run and job pages
- live runner logs in the Console while the native Kubernetes Job is retained

Unsupported workflow fields and action reference forms are rejected during
planning. Unsupported action runtimes fail explicitly in the runner so a
workflow is never silently executed with different semantics.

## Architecture

![Open Actions controller, Console, and execution flow](docs/architecture.drawio.svg)

The `open-actions-controller` accepts GitHub webhooks, discovers workflows, and
creates `WorkflowRun` and `WorkflowJob` resources. It assigns queued jobs to
matching `Runner` resources, which execute the workflow steps in Kubernetes
Jobs. The controller also reports WorkflowRun state through GitHub Check Runs.
The `open-actions-console` serves authenticated run and job pages and
streams runner Pod logs. A `Project` defines the execution domain and its GitHub
App integration.

## Installation

Install the CLI:

```console
go install github.com/kelos-dev/open-actions/cmd/open-actions@latest
```

Then install the CRDs, RBAC, controller, Console, and Services in the
cluster selected by the current Kubernetes context:

```console
open-actions install
```

The command installs or upgrades the embedded Helm chart as the `open-actions`
release in the `open-actions-system` namespace. It requires Helm on `PATH` and
uses Helm's current Kubernetes context. Pass custom chart configuration with
`open-actions install --values values.yaml`. The chart and its values are
documented in
[`internal/manifests/charts/open-actions`](internal/manifests/charts/open-actions).

The Console is installed by default for access through a local port-forward:

```console
kubectl port-forward --namespace open-actions-system service/open-actions-console 8080:80
```

When exposing the Console, set its public HTTPS URL and route that origin to
its Service:

```yaml
console:
  publicURL: https://actions.example
```

The chart creates a random administrator token for the Console and preserves it
across upgrades. Retrieve it after installation, then enter it on the Console
login page:

```console
kubectl get secret open-actions-console-auth \
  --namespace open-actions-system \
  --output jsonpath='{.data.token}' | base64 --decode; echo
```

To manage the token outside Helm, create a Secret in the release namespace and
set `console.secretName` to its name. `console.tokenKey` selects the key, which
defaults to `token`.

The token grants read access to every run, job, and runner Pod log visible to
the Console. Keep it secret, and use HTTPS whenever the Console is exposed
outside a trusted local connection.

Create a Secret containing a GitHub App RSA private key and webhook secret, a
`Project`, and one or more `Runner` resources. Each `Runner` is one reusable
execution slot; create more Runners to increase concurrency. Examples are under
[`config/samples`](config/samples). The `WorkflowJob` manifest illustrates the
controller-owned child shape and is not applied independently. Expose the
`open-actions-webhook.open-actions-system` Service through HTTPS and set that
URL as the GitHub App's webhook URL. Expose
`open-actions-console.open-actions-system` through HTTPS at its configured
public URL. Subscribe the App to push, pull request, and merge group events.
Grant the App these repository permissions:

- Contents: read, for workflow discovery and job execution
- Pull requests: read, for `pull_request` webhooks
- Merge queues: read, for `merge_group` webhooks
- Checks: read and write, for WorkflowRun reporting

Runner job tokens remain restricted to the selected repository with Contents
read access. The Console runs with a separate ServiceAccount limited to reading
workflow runs, workflow jobs, Pods, and Pod logs. Its administrator token is
mounted directly into the Console pod from the configured Secret.

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
