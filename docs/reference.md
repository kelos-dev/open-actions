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

The Console landing page lists up to 100 WorkflowRuns across all namespaces,
newest first, and links to each run's details and jobs. The Console presents
runner output as line-oriented GitHub Actions logs. It
supports `group` and `endgroup`, debug and annotation commands, command lines,
escaped command data and properties, and `stop-commands` markers. It also shows
action input and output names without persisting their values and groups post
actions separately. Workflow step headings show running, succeeded, failed, and
cancelled states; succeeded steps collapse automatically, while failed and
cancelled steps remain expanded. ANSI SGR foreground and background colors,
including standard, bright, 256-color, and RGB values, are rendered along with
bold, dim, italic, underline, and strike-through text. Other CSI control
sequences are discarded. Log lines larger than 256 KiB are truncated so a
workflow cannot retain unbounded Console memory.

The runner accepts `::command::` and bracket-form `##[command]` syntax, with
the property delimiters and escape rules defined for each form. It consumes
mask, output, state, and problem matcher commands instead of exposing their raw
protocol lines. Stdout `set-env` and `add-path` commands are ignored because
step output is not a trusted environment update channel; actions must write to
`GITHUB_ENV` and `GITHUB_PATH` instead. Problem matcher files may contain
single-line or ordered multiline regular-expression patterns; matching output
is emitted as a Console annotation. Matchers remain active until another
matcher with the same owner replaces them or `remove-matcher` removes them. A
matcher that encounters a regular-expression runtime error is disabled with a
warning and does not fail the workflow step.

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
ID and the last report accepted by GitHub. For open pull requests,
`spec.source.github.revision.sha` identifies the test merge commit used for
execution, while `headSHA` identifies the pull request commit used for check
reporting. A Runner's `spec.projectRef` is
immutable, and changes to `spec.execution` apply only to Kubernetes Jobs created
afterward. A `WorkflowJob` spec is immutable, and `status.runnerRef` identifies
its one-time Runner assignment. After execution, `status.outputs` contains the
non-secret outputs declared by that workflow job. The controller copies these
values from the completed runner Pod before allowing native Job cleanup, so
they remain available after controller restarts and Pod deletion.

Runner labels are canonical lowercase ASCII in Kubernetes resources. Workflow
`runs-on` labels use the same representation. Each Runner is one reusable
execution slot and accepts one queued `WorkflowJob` from its `spec.projectRef`
whose `runs-on` labels are all present in `spec.labels`.

### Docker execution

`spec.execution.docker` enables a job-scoped Docker daemon. Its required
`image` field identifies a Docker-in-Docker image whose entrypoint accepts
`dockerd` as its first argument and that provides the `docker` CLI used by the
startup probe. `resources` uses the same requests and limits schema as the
runner container. When an `ephemeral-storage` limit is present, the controller
also applies it as the Docker data volume's size limit.

The controller runs the daemon as a privileged Kubernetes native sidecar and
connects the runner through a private Unix socket exposed as `DOCKER_HOST`.
The daemon, socket, and image data exist only for one WorkflowJob. The runner
and daemon share the workspace and Pod network, allowing kind's Docker node
containers and loopback API-server endpoint to work from workflow steps. The
sidecar does not receive the job plan or authentication Secret volume, and the
Pod does not mount the node's Docker socket or a Kubernetes service-account
token.

The standard runner image includes the Docker CLI, Bash, curl, and the other
tools needed by the default `helm/kind-action` workflow. A custom runner image
used with `spec.execution.docker` must provide a compatible `docker` executable
on `PATH`. The runner remains non-root; action options that invoke `sudo`, such
as `helm/kind-action`'s local-registry and cloud-provider setup, are not
supported by the standard image.

Docker execution is disabled when `spec.execution.docker` is omitted. Enabling
it changes the WorkflowJob Pod's security posture because the daemon sidecar is
privileged. Use dedicated or sandboxed nodes when workflows are not fully
trusted. See
[`config/samples/actions_v1alpha1_docker_runner.yaml`](../config/samples/actions_v1alpha1_docker_runner.yaml)
for a Docker-enabled Runner.

A job strategy may define scalar matrix axes, `include` and `exclude`
transformations, and an optional positive `max-parallel`. `exclude` mappings
remove every Cartesian-product combination that partially matches their values.
The controller then applies `include` mappings in declaration order. An include
augments every compatible original combination and may overwrite values added by
an earlier include, but it does not overwrite original axis values. An include
that matches no original combination is appended as a standalone combination.
A matrix may consist only of `include` mappings.

```yaml
strategy:
  max-parallel: 2
  matrix:
    os: [linux, darwin]
    version: [1, 2]
    exclude:
      - os: darwin
        version: 1
    include:
      - os: linux
        coverage: true
      - os: windows
        version: 2
```

The controller creates one `WorkflowJob` per transformed combination. Axis and
value declaration order determines the order of Cartesian-product combinations,
followed by standalone includes in declaration order. Each child has a unique
`spec.jobID`, while `spec.matrix.logicalJobID`, `values`, and `maxParallel`
preserve its logical identity and scheduling group. Every scalar axis or include
value is available through the `matrix` expression context and persisted in
`spec.matrix.values`. `max-parallel` limits active children in that group
independently of the number of matching Runners. A failed matrix child makes the
completed WorkflowRun fail. Strategy `fail-fast` is not supported; remaining
combinations continue to completion after a child fails.

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
| `WorkflowRun` | `Planned` | `False` | `ProjectUnavailable`, `WorkflowFetchFailed`, `WorkflowInvalid`, `TriggerInvalid`, `ChildCreationFailed`, `ExecutionStateLost` |
| `WorkflowRun` | `Succeeded` | `Unknown` | `JobsQueued`, `JobsRunning` |
| `WorkflowRun` | `Succeeded` | `True` | `JobsSucceeded` |
| `WorkflowRun` | `Succeeded` | `False` | `ProjectUnavailable`, `WorkflowFetchFailed`, `WorkflowInvalid`, `TriggerInvalid`, `ChildCreationFailed`, `JobFailed`, `ExecutionStateLost` |
| `WorkflowJob` | `Scheduled` | `True` | `RunnerAssigned` |
| `WorkflowJob` | `Scheduled` | `False` | `ProjectRecreated` |
| `WorkflowJob` | `Succeeded` | `Unknown` | `JobRunning` |
| `WorkflowJob` | `Succeeded` | `True` | `JobSucceeded` |
| `WorkflowJob` | `Succeeded` | `False` | `JobFailed`, `JobResultInvalid`, `PlanUnavailable`, `JobStartFailed`, `ExecutionStateLost`, `ProjectRecreated` |

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
distinguished from the original object. Workflow jobs that declare outputs,
and their native Jobs and Pods, carry the
`actions.kelos.dev/runner-result-version` annotation. Its value identifies the
runner result format required to complete that job.

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

### Expressions

Open Actions parses expression templates independently from YAML. A value made
up of one `${{ ... }}` expression keeps its boolean, number, null, or string
type. Expressions embedded in other text are converted to strings without
discarding the surrounding literal text. Missing object properties evaluate to
an empty string. Malformed delimiters, unknown functions, unavailable contexts,
and unsupported syntax fail explicitly.

The supported grammar includes null, boolean, number, and single-quoted string
literals; property and index access; parentheses; `!`, comparisons, `&&`, and
`||`; and the `contains`, `format`, and `startsWith` functions. Comparisons and
string conversions follow GitHub Actions coercion rules. `&&` and `||` return a
selected operand and short-circuit, which supports fallback expressions and
the conventional `condition && value || fallback` form. Conditions also
support `success`, `always`, `failure`, and `cancelled` where status is
available. Each expression is limited to 256 syntax nodes and 64 nesting
levels. Format expansion and interpolated template output are limited to 100,000
bytes during evaluation.

Contexts are restricted by evaluation phase. An allowed context still fails at
evaluation when the corresponding execution feature has not supplied it.

| Phase | Allowed contexts and functions | Currently supplied |
| --- | --- | --- |
| Workflow concurrency | `github`, `inputs`, `vars` | `github`, `inputs` |
| Job name and runner labels | `github`, `needs`, `strategy`, `matrix`, `vars`, `inputs` | `github`, `inputs`, and `matrix` for matrix jobs |
| Job environment | `github`, `needs`, `strategy`, `matrix`, `vars`, `secrets`, `inputs` | `github`, `inputs`, and `matrix` for matrix jobs |
| Workflow step name, run script, working directory, environment, and inputs | `github`, `needs`, `strategy`, `matrix`, `job`, `runner`, `env`, `vars`, `secrets`, `steps`, `inputs` | `github`, `matrix`, `runner`, `env`, `inputs`, `steps` |
| Workflow step condition | Step contexts except `secrets`, plus status functions | `github`, `matrix`, `runner`, `env`, `inputs`, `steps`, and status functions |
| Job outputs | Workflow step contexts | `github`, `matrix`, `runner`, `env`, `inputs`, `steps` |
| Composite step fields and outputs | `github`, `runner`, `env`, `inputs`, `steps` | All listed contexts |
| Composite step condition | Composite contexts and status functions | All listed contexts and functions |
| Action input default | `github` | `github` |

Dependency scheduling and its `needs` context, and repository secret and
variable sources remain separate execution features. Values derived
from `github.token` or the `secrets` context are marked sensitive through
interpolation and function calls, and evaluation diagnostics do not include
resolved values. The runner maps interrupt and termination signals to cancelled
status, separately from failure, so eligible workflow and composite cleanup
steps can run during pod termination.

### Step and job outputs

A workflow step `id` must start with a letter or `_`, may contain letters,
digits, `-`, and `_`, and is limited to 256 characters. IDs must be unique
within a job without regard to ASCII case. Run steps and external actions may
write single-line or heredoc-style multiline values to `GITHUB_OUTPUT`.
Each step's output command file is limited to 1 MiB. Repeated names use the last
value. Once the step finishes, later steps can read those values through
`steps.<id>.outputs.<name>`; missing and skipped-step outputs evaluate to an
empty string.

`jobs.<id>.outputs` is evaluated after workflow steps and post actions finish,
including when a step failed. Output names use the same identifier syntax.
Values that are derived from an expression secret or contain a registered mask
are omitted from runner logs, the result document, and `WorkflowJob.status`.
The complete versioned job result is limited to 4 KiB and 100 outputs. Exceeding
that bound fails the job without persisting a partial result. A successful job
that declares outputs finishes with reason `JobResultInvalid` when its runner
result is missing or malformed. Jobs without declared outputs do not require
runner result metadata. Dependency jobs cannot consume these values until
dependency graph scheduling supplies the `needs` context.

### Concurrency

Concurrency groups are case-insensitive and scoped by Project and repository.
One run may execute while one newer run waits. A newer waiting run supersedes
the existing waiting run. When `cancel-in-progress` is `true`, it also cancels
the executing run. A run waits for an older run in the same repository until
that run has evaluated its workflow concurrency configuration.

Concurrency expressions reject unavailable contexts and empty evaluated
groups. Event properties that do not exist evaluate to an empty string. Use
`github.head_ref || github.ref_name` for workflows that need a ref fallback.
Pull request and merge group target branches are available as
`github.base_ref`.

### Trigger matching

Push branch filters apply only to branch refs, and push tag filters apply only
to tag refs. Defining only one filter kind excludes the other ref kind. An
omitted `pull_request.types` filter matches GitHub's default `opened`,
`synchronize`, and `reopened` activities. Explicit empty `branches` and `types`
lists are invalid. Configured concurrency requires a non-empty group.

The supported trigger declarations are `push`, `pull_request`, `merge_group`,
`workflow_run`, `workflow_dispatch`, `issues`, `pull_request_target`,
`issue_comment`, `pull_request_review_comment`, `pull_request_review`,
`schedule`, `release`, and `workflow_call`. Activity `types` are validated for
each webhook-backed event. Webhook deliveries with unrecognized activity types
are accepted without being queued. `pull_request_target` uses the pull request's
trusted base-branch workflow and revision, including for fork pull requests;
ordinary `pull_request` workflows continue to use the test merge revision. This
differs from GitHub Actions, which loads native `pull_request_target` workflows
from the repository's default branch. Review and review-comment events discover
and execute workflows only from the trusted default branch, so a maintainer
action cannot execute a fork-controlled workflow definition. Checks for review
events are reported on the trusted default-branch revision,
`pull_request_target` checks are reported on the trusted base-branch revision,
and ordinary `pull_request` checks are reported on the pull request head
revision.

For `pull_request_target`, `spec.source.github.revision` always identifies the
trusted base-branch workflow and execution commit, and `revision.ref` must equal
`refs/heads/` followed by `event.pullRequest.baseRef`. The execution SHA is
pinned to the base commit from the signed webhook instead of resolving the
mutable branch during asynchronous delivery processing. Bounded untrusted
metadata is recorded separately under `event.pullRequest`: the pull request
number and body, HTML URL, head repository, head branch and SHA, and base branch.
The normalized `github.event.pull_request` object exposes the same values and
derives `merge_ref` from the pull request number. Open Actions does not
automatically check out the head or merge ref, expand the runner token beyond
repository-scoped Contents read, or grant approval-gated secrets based on this
metadata.

`workflow_run` accepts `workflows`, `types`, and `branches` filters and consumes
GitHub App `workflow_run` webhooks. It does not synthesize a GitHub
`workflow_run` delivery when an Open Actions run completes.

Webhook-backed runs persist only the bounded event fields exposed by Open
Actions. `github.event.workflow_run` contains `conclusion` and `head_sha`;
`github.event.issue` contains `number` and `body`; `github.event.comment` and
`github.event.review` contain `body`; and `github.event.release` contains
`tag_name`, derived from the canonical revision tag ref. Draft release
activities are ignored. Pull-request-backed events expose the pull request
number, body, HTML URL, head repository, head branch and SHA, and base branch.
These objects have the same shape in planning expressions and
`GITHUB_EVENT_PATH`; the raw webhook payload is not persisted.

Issue, comment, review, and pull request bodies contain at most 48,000
characters. Pull request HTML URLs contain at most 2,048 characters.
`workflow_run.conclusion` contains at most 64 lowercase letters or underscores,
and `workflow_run.head_sha` is a 40-character lowercase hexadecimal SHA. Release
tag names contain at most 1,014 characters so the `refs/tags/` revision ref
remains within its 1,024-character limit.

`workflow_dispatch` accepts up to 25 typed, bounded inputs. Initiate a manual
run by creating a `WorkflowRun` through the Kubernetes API with the selected
workflow path, pinned commit and branch or tag ref, and any supplied inputs.
Use a deterministic `metadata.name` as the invocation's idempotency key; a
retry of the same request must reuse that name. `deliveryID` is reserved for
the `X-GitHub-Delivery` value on webhook-backed events. The controller verifies
that the workflow declares `workflow_dispatch`, validates the supplied inputs,
and applies defaults before planning jobs. See
[`config/samples/actions_v1alpha1_workflowrun-dispatch.yaml`](../config/samples/actions_v1alpha1_workflowrun-dispatch.yaml).
Supported input types are `string`, `boolean`, `number`, `choice`, and
`environment`; `choice` inputs require options. `workflow_call` declarations
accept `string`, `boolean`, and `number` inputs with required types and use the
same direct `WorkflowRun` initiation and validation path. Optional
`workflow_call` inputs without an explicit default resolve to `false`, `0`, or
an empty string according to their type. Input names must be unique ignoring
ASCII case, and input names and resolved values are limited to 65,535 characters
in total. The `inputs.<name>` expression preserves declared boolean and number
types, while `github.event.inputs.<name>` is always string-valued; number inputs
use the canonical parsed number representation in both planning and runner
expressions. Local job-level calls are described separately from trigger
parsing.

Schedules use five-field POSIX cron expressions in UTC with a minimum interval
of five minutes. The controller enumerates repositories available to each
configured GitHub App installation, reads workflows and the latest revision
from each default branch, and creates one deterministic `WorkflowRun` for each
matching workflow, cron expression, and minute. Reconciliation is idempotent,
and transient repository failures are retried during the current minute, but
missed minutes are not backfilled. Parsed schedules are cached while a
repository's push metadata and resolved revision remain unchanged. Planning
verifies that a scheduled `WorkflowRun` names a valid cron expression declared
by the selected workflow.

Scheduled WorkflowRuns retain an idempotency finalizer until their creation
minute ends. A TTL of zero or a manual deletion may therefore leave the run
terminating until that boundary, preventing the same scheduled invocation from
being created twice during its due minute.

Scheduled discovery supports at most 1,000 repositories per installation. An
installation above that limit fails the schedule reconciliation before any
repository is processed. A repository with more than 100 workflow files is
skipped for scheduled discovery while other repositories continue.

A repository without the configured workflow directory is accepted with zero
runs. For a deleted branch or tag, trigger matching uses the deleted ref, while
`github.sha` contains the current commit of the repository's default branch.

### Validation limits

Workflow definitions must satisfy these limits:

- A delivery may contain at most 100 workflow files and 1,000 jobs across
  matching workflows.
- A workflow file may contain at most 1,000,000 bytes.
- A workflow may contain at most 1,000 jobs.
- A branch or tag filter may contain at most 256 patterns of at most 256
  characters each.
- A `workflow_run` trigger may contain at most 100 workflow names of at most
  256 characters each.
- A workflow trigger may define at most 25 inputs. Input names contain at most
  100 characters, descriptions at most 1,024 characters, and choice inputs at
  most 100 options. Each option, default, or supplied value contains at most
  65,535 characters, and all input names and values together contain at most
  65,535 characters.
- A `schedule` trigger may contain at most 20 cron expressions, each at most
  256 characters.
- A matrix may define at most 100 axes. Its `include` and `exclude` lists may
  each contain at most 256 mappings, with at most 100 values per mapping. The
  final transformed matrix must contain 1 to 256 jobs, and a workflow may expand
  to at most 1,000 jobs in total.
- Matrix keys contain at most 256 characters, scalar matrix values contain at
  most 1,024 characters, and each transformed combination contains at most 100
  values.
- A job may contain at most 100 steps and 100,000 bytes of aggregate planned
  content.
- A completed job result may contain at most 100 outputs and 4 KiB of encoded
  output metadata.
- A run script may contain at most 65,536 bytes.
- A step condition may contain at most 65,536 bytes.
- Each `env` or `with` map may contain at most 100 entries.

Names, branch patterns, action references, paths, map keys, and values are also
bounded during workflow validation. Field and aggregate content limits are
reapplied after expression evaluation before a step executes.

### Execution constraints

External actions must use the `node20`, `node24`, or composite runtime and be
available from the configured GitHub server. The default runner image pins Node
20.20.2 for `node20` actions and Node 24.19.0 for `node24` actions. Custom runner
images must provide the Node 20 executable as `node` and the Node 24 executable
as `node24` on `PATH`; execution fails before the first lifecycle hook when the
declared runtime is unavailable. Composite actions support Bash run steps and
external action references. Composite expressions cover inputs, step outputs,
selected GitHub and runner values, and environment variables.

Node, composite, and Bash steps can use Docker when assigned to a Runner with
`spec.execution.docker`. This capability supports tools such as kind but does
not add support for action metadata declaring `runs.using: docker`.

`GITHUB_EVENT_PATH` contains a bounded normalized document with repository
identity, trigger action, event-specific metadata described above, manual or
reusable inputs, the selected cron expression, and revision fields used by the
supported event. Actions that require other fields from GitHub's raw webhook
payload are not supported.

The controller emits job-plan version 4, and the runner accepts versions 1
through 4. When a release changes the job-plan version, update every Runner
`spec.execution.image` to an image that accepts both the installed and target
controller versions before upgrading the controller. The received job-plan
version also determines the runner result version. A runner that accepts more
than one plan version must emit the result version assigned to that plan, not
always the latest result version supported by the runner binary.

Docker and local actions, private cross-repository action authentication, job
dependencies, strategy `fail-fast`, service containers, repository secret and
variable sources, caches, and artifacts are not supported. Expressions outside
the documented fields and runtime contexts are rejected during planning or
execution and are never interpreted as literal values.
`WorkflowJob` resources are not retried or reassigned when a Runner is removed.
Native Jobs and their Pod logs are deleted one hour after completion. Completed
WorkflowRuns are retained indefinitely unless `spec.ttlSecondsAfterFinished` is
set. When that TTL expires, the WorkflowRun and any remaining owned resources
are deleted. Open Actions does not archive logs.

## Webhook API

The webhook endpoint accepts only signed GitHub `POST` deliveries up to 10 MiB
and requires exactly one configured project for the installation. Supported
deliveries return HTTP 202 with `{"accepted":true,"queued":true}`. Unsupported
event names return HTTP 202 with `{"accepted":true,"queued":false}`. Ordinary
open pull request workflows use GitHub's test merge revision and are skipped
for fork or conflicted pull requests. `pull_request_target` workflows remain
eligible and load only from the pull request's base branch in the base
repository. Deliveries wait up to two minutes for a test merge revision while
eligible `pull_request_target` workflows are discovered independently.
Unavailable or superseded test merge revisions produce a `Failed` delivery
after that wait.

Queued deliveries are processed asynchronously. Invalid or unsupported workflow
definitions fail the whole delivery before any `WorkflowRun` resources are
created. Repeated deliveries with the same signed body are deduplicated for 24
hours.
