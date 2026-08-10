# Open Actions API reference

This document describes the externally visible Open Actions interfaces:
Kubernetes resources, command-line configuration, supported workflow syntax,
and the webhook endpoint. See the [project README](../README.md) for an overview
and installation instructions.

## Command-line configuration

The `open-actions` CLI uses the active kubeconfig context for Kubernetes-backed
commands. `--kubeconfig` selects another kubeconfig file, `--context` selects a
context, and `--namespace` overrides the context's namespace.

`open-actions run list` lists WorkflowRuns in one namespace. Pass
`--all-namespaces` to list them cluster-wide. `open-actions run view RUN` shows
the selected run and its WorkflowJobs. `open-actions run logs RUN` prints the
runner logs for the job selected by `--job`; the selector accepts either a
workflow-local job ID or a WorkflowJob resource name, with an exact resource
name taking precedence. The selector is optional for single-job runs. Pass
`--follow` to wait for the runner Pod when necessary and continue streaming its
logs.

```console
open-actions run list --namespace team-ci
open-actions run view ci-abc123 --namespace team-ci
open-actions run logs ci-abc123 --job build --follow --namespace team-ci
```

The CLI accesses these resources with the permissions of the selected
kubeconfig user. It does not use the Console administrator token.

### Controller and Console

`--github-api-url` defaults to `https://api.github.com/` and may include a
GitHub Enterprise API path such as `/api/v3`. `--github-server-url` defaults to
`https://github.com` and supplies `github.server_url`. `--action-clone-base-url`
defaults to `https://github.com` and is used only to fetch external action
repositories. The controller's optional `--console-url` adds Console links to
GitHub Check Runs. The Console serves HTTP on `--bind-address` (default
`:8080`) and requires `--token-file`. Set `--secure-cookie` when it is served
through HTTPS. The Helm chart configures both authentication flags from its
configured Secret and `console.publicURL`.

Each URL requires an absolute `http` or `https` URL with a host and an optional
clean path prefix. User information, queries, fragments, escaped paths, and `.`
or `..` path segments are rejected. The API URL is normalized with a trailing
slash, while the server and clone base URLs are normalized without one.

```console
open-actions-controller \
  --github-api-url=https://github.example/api/v3 \
  --github-server-url=https://github.example \
  --action-clone-base-url=https://github.example \
  --console-url=https://actions.example

open-actions-console \
  --token-file=/var/run/secrets/open-actions-console/token \
  --secure-cookie
```

## Kubernetes API

Open Actions exposes the namespaced `Project`, `Runner`, `WorkflowRun`, and
`WorkflowJob` resources in `actions.kelos.dev/v1alpha1`. The installed CRD
schemas define their fields and validation. Example manifests are available
under [`config/samples`](../config/samples).

All references resolve within the resource's namespace. A `Project` selects its
integration through the discriminated `spec.source` union. The supported
variant is `type: GitHub`, with GitHub App configuration under `source.github`.
The project defaults `spec.workflowDirectory` to `.open-actions/workflows`. Its
source type and GitHub App and installation IDs are immutable. Only one project
in the cluster may claim an installation; the earliest-created project retains
the claim, and later duplicates remain unconfigured until the owner is deleted.
A `WorkflowRun` records provider-specific event data under its own immutable
`spec.source` union. `status.source.github.checkRun` records the GitHub Check Run
ID and the last report accepted by GitHub. A Runner's `spec.projectRef` is
immutable, and changes to `spec.execution` apply only to Kubernetes Jobs created
afterward. A `WorkflowJob` spec is immutable, and `status.runnerRef` identifies
its one-time Runner assignment.

Runner labels are canonical lowercase ASCII in Kubernetes resources. Workflow
`runs-on` labels use the same representation. Each Runner is one reusable
execution slot and accepts one queued `WorkflowJob` from its `spec.projectRef`
whose `runs-on` labels are all present in `spec.labels`.

### Conditions

The resources expose these condition contracts:

| Resource | Condition | Status | Reasons |
| --- | --- | --- | --- |
| `Project` | `Configured` | `True` | `ConfigurationValid` |
| `Project` | `Configured` | `False` | `DuplicateInstallation`, `CredentialsUnavailable`, `InvalidCredentials` |
| `Runner` | `Ready` | `True` | `Ready` |
| `Runner` | `Ready` | `False` | `ProjectUnavailable`, `ProjectNotConfigured` |
| `Runner` | `Busy` | `False` | `Idle` |
| `Runner` | `Busy` | `True` | `JobAssigned` |
| `WorkflowRun` | `Planned` | `True` | `JobsPlanned` |
| `WorkflowRun` | `Planned` | `Unknown` | `WaitingForConcurrency`, `WaitingForConcurrencyCancellation`, `ProjectUnavailable`, `CredentialsUnavailable`, `GitHubAuthenticationFailed`, `WorkflowFetchFailed`, `ChildCreationFailed`, `ConcurrencyCheckFailed` |
| `WorkflowRun` | `Planned` | `False` | `ProjectUnavailable`, `WorkflowFetchFailed`, `WorkflowInvalid`, `ChildCreationFailed`, `ExecutionStateLost` |
| `WorkflowRun` | `Succeeded` | `Unknown` | `JobsQueued`, `JobsRunning` |
| `WorkflowRun` | `Succeeded` | `True` | `JobsSucceeded` |
| `WorkflowRun` | `Succeeded` | `False` | `ProjectUnavailable`, `WorkflowFetchFailed`, `WorkflowInvalid`, `ChildCreationFailed`, `JobFailed`, `ExecutionStateLost` |
| `WorkflowJob` | `Scheduled` | `True` | `RunnerAssigned` |
| `WorkflowJob` | `Scheduled` | `False` | `ProjectRecreated` |
| `WorkflowJob` | `Succeeded` | `Unknown` | `JobRunning` |
| `WorkflowJob` | `Succeeded` | `True` | `JobSucceeded` |
| `WorkflowJob` | `Succeeded` | `False` | `JobFailed`, `PlanUnavailable`, `JobStartFailed`, `ExecutionStateLost`, `ProjectRecreated` |

`Project/Configured` covers local Secret availability, private-key parsing, and
installation uniqueness. It does not assert remote GitHub App or installation
availability. `Runner/Ready` reports operational health independently of
capacity; clients use `Runner/Busy` to determine whether a Runner already has
an assignment.

### Resource metadata

Child resources carry `actions.kelos.dev/project-uid`, `runner-uid`,
`workflow-run-uid`, and `workflow-job-uid` labels where applicable. The
`actions.kelos.dev/workflow-job` label contains the workflow job ID when it is a
valid Kubernetes label value, or the full SHA-256 digest encoded as lowercase
unpadded base32 otherwise. The original job ID, user-facing job display name,
and assigned runner name remain available through
`actions.kelos.dev/workflow-job-id`,
`actions.kelos.dev/workflow-job-display-name`, and
`actions.kelos.dev/runner-name` annotations. The workflow job display name is
the user-facing `jobs.<id>.name` value, or the job ID when no name is
configured. Queued jobs record their project name in the
`actions.kelos.dev/project-name` annotation so a recreated project can be
distinguished from the original object.

Webhook-created WorkflowRuns use the workflow filename followed by a stable
20-character digest of the project, delivery replay, and workflow path.
WorkflowJobs and their native Jobs append the workflow job ID and a stable
16-character digest. Readable portions are truncated as needed to keep names
within the 63-character Kubernetes limit; retries of the same delivery reuse
the same names, while different signed payloads create distinct runs. The full
replay-and-path digest form is also recognized as an idempotency alias.

## Workflow API

Workflow files must define a non-empty `name` of at most 256 characters.
Unsupported workflow fields and action reference forms are rejected during
planning. Unsupported action runtimes fail explicitly during execution.

### Concurrency

Concurrency groups are case-insensitive and scoped by Project and repository.
One run may execute while one newer run waits. A newer waiting run supersedes
the existing waiting run. When `cancel-in-progress` is `true`, it also cancels
the executing run. A run waits for an older run in the same repository until
that run has evaluated its workflow concurrency configuration.

Concurrency expressions reject unavailable context and empty evaluated groups.
Use `github.head_ref || github.ref_name` for workflows that need a ref fallback.

### Trigger matching

Push branch filters apply only to branch refs, not tags with the same short
name. An omitted `pull_request.types` filter matches GitHub's default `opened`,
`synchronize`, and `reopened` activities. Explicit empty `branches` and `types`
lists are invalid. Configured concurrency requires a non-empty group.

A repository without the configured workflow directory is accepted with zero
runs. For a deleted branch or tag, trigger matching uses the deleted ref, while
`github.sha` contains the current commit of the repository's default branch.

### Validation limits

Workflow definitions must satisfy these limits:

- A delivery may contain at most 100 workflow files and 1,000 jobs across
  matching workflows.
- A workflow file may contain at most 1,000,000 bytes.
- A workflow may contain at most 1,000 jobs.
- A job may contain at most 100 steps and 100,000 bytes of aggregate planned
  content.
- A run script may contain at most 65,536 bytes.
- Each `env` or `with` map may contain at most 100 entries.

Names, branch patterns, action references, paths, map keys, and values are also
bounded during workflow validation.

### Execution constraints

External actions must use the `node20`, `node24`, or composite runtime and be
available from the configured GitHub server. The default runner image pins Node
20.20.2 for `node20` actions and Node 24.19.0 for `node24` actions. Custom runner
images must provide the Node 20 executable as `node` and the Node 24 executable
as `node24` on `PATH`; execution fails before the first lifecycle hook when the
declared runtime is unavailable. Composite actions support Bash run steps and
external action references. Composite expressions cover inputs, step outputs,
selected GitHub and runner values, and environment variables.

`GITHUB_EVENT_PATH` contains a bounded normalized document with repository
identity and the selected push, pull-request, or merge-group revision fields.
Actions that require other fields from GitHub's raw webhook payload are not
supported.

Docker and local actions, private cross-repository action authentication, job
dependencies, matrices, service containers, general expression evaluation,
caches, and artifacts are not supported. General expressions outside the
supported concurrency and composite-action contexts are rejected during
planning or execution and are never interpreted as literal values.
`WorkflowJob` resources are not retried or reassigned when a Runner is removed.
Native Jobs and their Pod logs are deleted one hour after completion. Completed
WorkflowRuns are retained indefinitely unless `spec.ttlSecondsAfterFinished` is
set. When that TTL expires, the WorkflowRun and any remaining owned resources
are deleted. Open Actions does not archive logs.

## Webhook API

The webhook endpoint accepts only signed GitHub `POST` deliveries up to 10 MiB
and requires exactly one configured project for the installation. Supported
deliveries return HTTP 202 with `{"accepted":true,"queued":true}`. Unsupported
event names, conflicted pull requests, and pull requests from fork repositories
return HTTP 202 with `{"accepted":true,"queued":false}`. Open pull request
workflows use GitHub's test merge revision for the webhook head. Deliveries wait
up to two minutes for that revision; unavailable or superseded revisions
produce a `Failed` delivery.

Queued deliveries are processed asynchronously. Invalid or unsupported workflow
definitions fail the whole delivery before any `WorkflowRun` resources are
created. Repeated deliveries with the same signed body are deduplicated for 24
hours.
