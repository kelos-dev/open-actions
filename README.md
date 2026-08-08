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
workflow is never silently executed with different semantics. Workflow files
must define a non-empty `name` of at most 256 characters.
`GITHUB_EVENT_PATH` contains a bounded normalized document with repository
identity and the selected push, pull-request, or merge-group revision fields;
actions that require other fields from GitHub's raw webhook payload are outside
the supported execution subset.

## Development

```console
make update     # Format, generate Go code and CRDs, and tidy modules.
make verify     # Check generated files, formatting, modules, and go vet.
make test       # Run unit and schema tests.
make build      # Build the controller and runner binaries under bin/.
```

The Ginkgo/Gomega end-to-end suite expects a current Kubernetes context. CI
creates a Kind cluster, builds and loads the images, and installs the controller
and CRDs. The `ActionsGateway` tests verify webhook authentication, typed
`WorkflowRun` creation, and queued delivery failures for invalid workflows.
The `Runner` tests create a typed `WorkflowRun` and verify job assignment,
native Job execution, status updates, and cleanup. The GitHub fixture serves
separate tagged action repositories so the Runner tests cover JavaScript and
composite actions, nested action outputs, file commands, and post hooks without
public network access.

```console
make image-e2e VERSION=e2e
kind load docker-image ghcr.io/kelos-dev/open-actions-controller:e2e
kind load docker-image ghcr.io/kelos-dev/open-actions-runner:e2e
kind load docker-image ghcr.io/kelos-dev/open-actions-fixture:e2e
RUNNER_IMAGE=ghcr.io/kelos-dev/open-actions-runner:e2e make test-e2e
```

## Installation

Install the CRDs, RBAC, controller, and webhook Service:

```console
make install
```

The controller deployment uses `open-actions-controller`. Each Runner's
`spec.execution.image` selects the `open-actions-runner` image used by its
Workflow Jobs. Controller and Runner images may use different image tags only
while both support the same job-plan schema; the current schema version is 1,
and Runners reject missing, unknown, or unsupported plan fields and versions.
Future plan changes must deploy reader support before a controller begins
emitting a new version. The
`open-actions-fixture` image is only used by the end-to-end test environment.

`--github-api-url` defaults to `https://api.github.com/` and may include a
GitHub Enterprise API path such as `/api/v3`. `--github-server-url` defaults to
`https://github.com` and supplies `github.server_url`; it requires `http` or
`https`. `--action-clone-base-url` defaults to `https://github.com`, is used
only to fetch external action repositories, and also requires `http` or `https`.
Each flag requires an absolute URL with a host and an optional clean path prefix.
User information, queries, fragments, escaped paths, and `.` or `..` path
segments are rejected. The API URL is normalized with a trailing slash, while
the server and clone bases are normalized without one. For example:

```console
open-actions-controller \
  --github-api-url=https://github.example/api/v3 \
  --github-server-url=https://github.example \
  --action-clone-base-url=https://github.example
```

Create a Secret containing a GitHub App RSA private key and webhook secret, an
`ActionsGateway`, and one or more `Runner` resources. Each `Runner` is one
reusable execution slot: it accepts one queued
`WorkflowJob` from its `spec.gatewayRef` whose `runs-on` labels are all present
in `spec.labels`. Native Jobs have a 50-minute execution deadline so their
GitHub installation token remains valid and a broken image pull or
unschedulable Pod cannot hold a Runner indefinitely. An assigned job that
cannot create its native Job within five minutes fails with `JobStartFailed`,
releases the Runner, and removes any job credential. Create more
`Runner` resources to provide more concurrent slots. Examples are under
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
`.open-actions/workflows`. A webhook delivery then follows this path:

```text
GitHub webhook
  -> ConfigMap delivery queue
    -> WorkflowRun
      -> WorkflowJob
        -> matching Runner
          -> ConfigMap job plan + batch/v1 Job
            -> Go runner
```

## API behavior

All references resolve within the resource's namespace. An `ActionsGateway`
selects its integration through the discriminated `spec.source` union. The
supported variant is `type: GitHub` with GitHub App configuration under
`source.github`. The gateway defaults `spec.workflowDirectory` to
`.open-actions/workflows`; its source type and GitHub App and installation IDs
are immutable, and only one gateway in the cluster may claim an installation.
The earliest-created gateway retains that claim; later duplicates remain
unconfigured until the owner is deleted.
A `WorkflowRun` records provider-specific event data under its own immutable
`spec.source` union. A Runner's
`spec.gatewayRef` is immutable; changes to `spec.execution` apply only to native
Jobs created afterward. The controller owns the Pod shape, including its
authentication, workspace, retry, and security configuration. A `WorkflowJob`
spec is immutable. The scheduler records its one-time Runner assignment in
`status.runnerRef`.

Concurrency groups are case-insensitive and scoped by gateway and GitHub's
stable repository ID. One run may execute while one newer run waits. A newer
waiting run supersedes the existing waiting run. With `cancel-in-progress: true`,
it also cancels the executing run. A run waits conservatively for an
older run in the same repository until that older run has evaluated its
workflow concurrency configuration. Concurrency expressions reject unavailable
context and empty evaluated groups; use
`github.head_ref || github.ref_name` for workflows that need a ref fallback.

The controllers publish these condition contracts:

| Resource | Condition | Status | Reasons |
| --- | --- | --- | --- |
| `ActionsGateway` | `Configured` | `True` | `ConfigurationValid` |
| `ActionsGateway` | `Configured` | `False` | `DuplicateInstallation`, `CredentialsUnavailable`, `InvalidCredentials` |
| `Runner` | `Ready` | `True` | `Ready` |
| `Runner` | `Ready` | `False` | `GatewayUnavailable`, `GatewayNotConfigured` |
| `Runner` | `Busy` | `False` | `Idle` |
| `Runner` | `Busy` | `True` | `JobAssigned` |
| `WorkflowRun` | `Planned` | `True` | `JobsPlanned` |
| `WorkflowRun` | `Planned` | `Unknown` | `WaitingForConcurrency`, `WaitingForConcurrencyCancellation`, `GatewayUnavailable`, `CredentialsUnavailable`, `GitHubAuthenticationFailed`, `WorkflowFetchFailed`, `ChildCreationFailed`, `ConcurrencyCheckFailed` |
| `WorkflowRun` | `Planned` | `False` | `GatewayUnavailable`, `WorkflowFetchFailed`, `WorkflowInvalid`, `ChildCreationFailed`, `ExecutionStateLost` |
| `WorkflowRun` | `Succeeded` | `Unknown` | `JobsQueued`, `JobsRunning` |
| `WorkflowRun` | `Succeeded` | `True` | `JobsSucceeded` |
| `WorkflowRun` | `Succeeded` | `False` | `GatewayUnavailable`, `WorkflowFetchFailed`, `WorkflowInvalid`, `ChildCreationFailed`, `JobFailed`, `ExecutionStateLost` |
| `WorkflowJob` | `Scheduled` | `True` | `RunnerAssigned` |
| `WorkflowJob` | `Scheduled` | `False` | `GatewayRecreated` |
| `WorkflowJob` | `Succeeded` | `Unknown` | `JobRunning` |
| `WorkflowJob` | `Succeeded` | `True` | `JobSucceeded` |
| `WorkflowJob` | `Succeeded` | `False` | `JobFailed`, `PlanUnavailable`, `JobStartFailed`, `ExecutionStateLost`, `GatewayRecreated` |

`ActionsGateway/Configured` covers local Secret availability, private-key
parsing, and installation uniqueness. It does not assert remote GitHub App or
installation availability. `Runner/Ready` reports operational health
independently of capacity; clients use `Runner/Busy` to determine whether a
Runner already has an assignment.

Child resources carry `actions.kelos.dev/gateway-uid`, `runner-uid`,
`workflow-run-uid`, and `workflow-job-uid` labels where applicable. The
`actions.kelos.dev/workflow-job` label contains the workflow job ID when it is a
valid Kubernetes label value, or the full SHA-256 digest encoded as lowercase
unpadded base32 otherwise. The original job ID and assigned runner name remain
available through
`actions.kelos.dev/workflow-job-id` and `actions.kelos.dev/runner-name`
annotations. Queued jobs record their gateway name in the
`actions.kelos.dev/gateway-name` annotation so a recreated gateway can be
distinguished from the original object.

The webhook endpoint accepts only signed GitHub `POST` deliveries up to 10 MiB
and requires exactly one configured gateway for the installation. Supported
deliveries return HTTP 202 with `{"accepted":true,"queued":true}`. Unsupported event names,
conflicted pull requests, and pull requests from fork repositories return HTTP
202 with `{"accepted":true,"queued":false}`. Pull request deliveries whose merge
ref is still being prepared are queued and resolved asynchronously. The
controller derives replay identity from
the signed body, persists a normalized delivery as a ConfigMap, and performs
workflow discovery asynchronously. The ConfigMap's `state` is `Completed` or
`Failed`, with `workflowRuns` and an optional validation `message`. Invalid or
unsupported workflow definitions fail the whole delivery before any runs are
created. Terminal delivery ConfigMaps are retained for 24 hours to deduplicate
replays and then deleted. Installation tokens are limited to read-only
repository contents, mounted only into the assigned job, and their Secrets are
deleted after execution reaches a terminal result.

A repository without the configured workflow directory is accepted with zero
runs. For a deleted branch or tag, trigger matching uses the deleted ref while
workflow discovery and `github.sha` use the current commit of the repository's
default branch.

Workflow discovery also enforces explicit configuration limits before creating
a `WorkflowRun`: a delivery may contain at most 100 workflow files and 1,000
jobs across matching workflows; workflow files are at most 1,000,000 bytes;
workflows have at most 1,000 jobs; jobs have at most 100 steps and 100,000 bytes
of aggregate planned content; run scripts are at most 65,536 bytes; and each
`env` or `with` map has at most 100 entries. Names, branch patterns, action
references, paths, map keys, and values are bounded by the
[`internal/workflow` parser](internal/workflow/workflow.go). The
[`api/v1alpha1` Go types and markers](api/v1alpha1) define the Kubernetes API
schema and generate the checked-in CRDs. Runner labels are canonical lowercase
ASCII in Kubernetes resources; workflow `runs-on` labels are normalized to
that representation during discovery.

Push branch filters apply only to branch refs, not tags with the same short
name. An omitted `pull_request.types` filter matches GitHub's default `opened`,
`synchronize`, and `reopened` activities. Explicit empty `branches` and `types`
lists are invalid. Configured concurrency requires a non-empty group.

## Limitations

Open Actions provides a focused workflow subset with GitHub as its source and
repository integration. External actions must use the `node20`
JavaScript runtime and be available from the configured GitHub server, or use
the composite runtime with Bash run steps and external action references.
Composite expressions cover inputs, step outputs, selected GitHub and runner
values, and environment variables. Docker and local actions; private
cross-repository action authentication; job dependencies; matrices; service
containers; general expression evaluation; caches; artifacts; and GitHub check
reporting are outside the supported subset. WorkflowJobs are not retried or
reassigned when a Runner is removed.
General expressions outside the supported concurrency and composite-action
contexts are rejected during planning or execution and are never interpreted as
literal values.
