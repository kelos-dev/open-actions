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
defaults to the GitHub server URL and is used only to fetch external action
repositories. Set it explicitly when actions are hosted on another server. The
controller's optional `--console-url` adds Console links to GitHub Check Runs
and supplies workflow run and stale-query URLs to job contexts.
`--max-job-timeout` is the cluster-wide upper bound for workflow job execution
and defaults to `6h`. It must be a positive whole number of minutes. The Helm
chart configures it through `controller.maxJobTimeout`.
The Console serves HTTP on `--bind-address` (default
`:8080`) and serves its read-only views without authentication. Anyone who can
reach the Console can read Project and workflow metadata and runner logs. The
required `--token-file` authenticates workflow dispatches, cancellation,
reruns, and Project Secret management. Set
`--secure-cookie` when the Console is served through HTTPS. The Helm chart
configures the administrator token and cookie security from its configured
Secret and `console.publicURL`.

The controller and Console both accept
`--workflow-run-ttl-seconds-after-finished` as the default
`spec.ttlSecondsAfterFinished` for the WorkflowRuns they create. Omit the flag
to retain those runs indefinitely. The Helm chart passes
`controller.workflowRunTTLSecondsAfterFinished` to both components.

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

After signing in with the Console administrator token, an administrator can
gracefully cancel an active workflow attempt. The Console sets that
WorkflowRun's `spec.cancelRequested` field, after which ordinary jobs stop while
cancellation-aware reporting and cleanup jobs may finish. A requested run is
shown as `Cancelling` until it reaches a terminal result. Cancellation requests
are idempotent and are unavailable for completed attempts.

An administrator can also rerun all jobs from the latest completed attempt in
a workflow lineage. When
the attempt failed because one or more jobs failed, the administrator can
instead rerun the failed expanded job IDs and their transitive dependents; the
controller also includes the prerequisite jobs needed by that selected graph.
The Console creates a new immutable WorkflowRun attempt with the same project,
source, workflow path, and retention setting, clears any prior cancellation
request, and redirects to the new run. Rerun actions are unavailable while the
latest attempt is still active. The Helm chart grants the Console `create` and
`update` access to WorkflowRuns across the namespaces it displays.

An administrator can use **Run workflow** to create a `workflow_dispatch`
WorkflowRun in any configured Project namespace. The form accepts a repository,
workflow path, branch or tag, pinned commit SHA, and declared workflow inputs.
Starting from an existing branch- or tag-backed run prepopulates its Project,
repository, workflow, and revision. Each form instance carries a request ID, so
resubmitting the same dispatch is idempotent and redirects to the existing run.

The Projects page lists Project configuration across all namespaces. A Project
detail page lists the names, but never the values, of keys in its referenced
workflow Secret. An administrator can sign in with the Console token to add,
replace, and delete those keys only when the Project is in the Console's
`--secret-management-namespace`. The Helm chart sets that namespace to its
release namespace and grants the Console `get`, `create`, and `update` access to
Secrets only there. Direct Kubernetes clients and external secret controllers
can manage the same Secret.

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
The controller image must provide Git with support for
`merge-tree --write-tree` so it can construct pull request integration
revisions.

```console
open-actions-controller \
  --github-api-url=https://github.example/api/v3 \
  --github-server-url=https://github.example \
  --action-clone-base-url=https://github.example \
  --max-job-timeout=6h \
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
ID and the last report accepted by GitHub. For locally integrated pull requests,
`spec.source.github.revision.sha` identifies the deterministic integration
commit used for execution. `baseSHA`, `headSHA`, and `mergeBaseSHA` pin its two
parents and merge base; `headSHA` is also used for check reporting. Pull request
runs without these integration inputs use the remote revision identified by
`sha`. `status.identity` records the stable numeric run ID, per-workflow run
number, attempt, and configured Console URL before jobs are planned. The
immutable `spec.source.github.actor` records the source actor. Set
`spec.cancelRequested` to gracefully cancel ordinary jobs while
allowing cancellation-aware reporting and cleanup jobs to finish. Deleting a
WorkflowRun force-cancels and removes its child resources. A Runner's
`spec.projectRef` is
immutable, and changes to `spec.execution` apply only to Kubernetes Jobs created
afterward. A `WorkflowJob` spec is immutable, and `status.runnerRef` identifies
its one-time Runner assignment. `spec.needs` records direct workflow
dependencies, `spec.if` records its scheduling condition,
`spec.timeoutSeconds` records its effective execution timeout, and `status.result`
contains `success`, `failure`, `skipped`, or `cancelled` after completion. After
execution, `status.outputs` contains the non-secret outputs declared by that
workflow job. The controller copies these values from the completed runner Pod
before allowing native Job cleanup, so they remain available after controller
restarts and Pod deletion. `WorkflowRun.status.jobs.timedOut` counts jobs whose
`Succeeded` condition has reason `JobTimedOut`; those jobs retain `failure` as
their `status.result` for dependency evaluation.

### Project secrets and variables

A Project selects one namespace-local Secret through
`spec.secrets.secretRef` and one namespace-local ConfigMap through
`spec.variables.configMapRef`. Each key becomes a name in the corresponding
`secrets` or `vars` workflow context. Keys must use the canonical uppercase
GitHub representation: letters, digits, and `_`, without a leading digit or
the reserved `GITHUB_` prefix. A Project supports up to 100 secrets and 500
variables. Individual values are limited to 48 KiB, Secret values must be valid
UTF-8, and the variable ConfigMap must not contain `binaryData`.

```yaml
spec:
  secrets:
    secretRef:
      name: project-secrets
  variables:
    configMapRef:
      name: project-variables
```

The Project remains unconfigured while a referenced object is missing or its
contents violate these constraints. Variables needed for workflow concurrency,
job names, or runner labels are read during planning; job conditions read them
when their dependencies settle. Job and step expressions remain unresolved in
the immutable plan. Kubernetes mounts the Secret and ConfigMap into runner-only,
read-only volumes when a job Pod starts, and the runner reads a consistent
snapshot at startup. Rotations apply to new jobs. The Docker sidecar does not
mount these volumes, and workflow commands do not receive the internal files as
ambient environment variables.

See the [Project value source sample](../config/samples/actions_v1alpha1_project-values.yaml)
for a complete Project manifest.

Secret values never enter job-plan ConfigMaps, custom-resource specs or status,
controller logs, or Console records. The runner marks values derived from the
`secrets` context as sensitive and masks configured secrets in raw, standard
and unpadded Base64, JSON-string, percent-encoded, XML-escaped, and common
shell-escaped forms. Project variables are non-sensitive and are not
masked. Missing names in either context evaluate to an empty string.

The job-scoped GitHub App installation token is available as both
`github.token` and `secrets.GITHUB_TOKEN`. It is not added to the step
environment unless the workflow assigns one of those expressions to an
environment variable or action input. Token permission selection is described
separately from Project value sources.

### External actions

External action repositories on the configured GitHub server are downloaded
with a short-lived token granting Contents read access to repositories
granted to the Project's GitHub App installation. Install the App on each
action repository used by a workflow. The token is used only for the exact
action repository URL and is not placed in workflow expressions, action inputs,
or workflow step environments. Runner and workflow processes share a container
security boundary, so every workflow using an installation must be trusted to
read every repository granted to it. Limit the installation to repositories
within that trust boundary. No GitHub credential is sent when
`--action-clone-base-url` has a different scheme or host from
`--github-server-url`.

Before the first workflow step runs, the runner recursively resolves and
downloads every external action referenced by the job or by a nested composite
action, including actions on steps whose `if` condition evaluates to false.
An unavailable action therefore fails preparation even when its step would be
skipped. This keeps action download authentication out of workflow inputs and
the step environment.

For pull request events, Open Actions applies its merge checkout integration to
`actions/checkout` and compatible forks whose repository is named `checkout`.
An action with that repository name must implement the `actions/checkout`
contract; other actions named `checkout` are unsupported.

`spec.rerun` identifies a repeated attempt. `originalRunRef` anchors the
attempt lineage, `previousRunRef` names the immediately preceding completed
attempt, and `attempt` starts at 2. Both references include the WorkflowRun UID
to reject names that were deleted and recreated. `requestID` is an optional
idempotency identity and contains the webhook delivery ID for GitHub
rerequests. `jobIDs` is an optional set of expanded WorkflowJob IDs; the
controller also includes their prerequisite jobs so the dependency graph is
complete. Omitting `jobIDs` reruns every job. The rerun fields are immutable.
The controller also requires the project, source, workflow path, lineage, and
attempt number to match the previous run before it executes a rerun.

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

A job strategy may define scalar matrix axes, an optional positive
`max-parallel`, and an optional Boolean `fail-fast`. The controller creates one
`WorkflowJob` per Cartesian-product combination in deterministic order. Each
child has a unique `spec.jobID`, while `spec.matrix.logicalJobID`, `values`,
`maxParallel`, and `failFast` preserve its logical identity and strategy.
`max-parallel` limits active children in that group independently of the number
of matching Runners.

`fail-fast` defaults to `true`. After a matrix child fails, queued combinations
in the same WorkflowRun and logical matrix job finish with `MatrixFailFast`, and
active combinations receive a cancellation request. Cancellation stops the
active command while still allowing eligible `cancelled()` and `always()` steps
and action post hooks to run. A fail-fast cancellation is terminal and does not
start another queued combination, including when `max-parallel` is set.
Independent jobs and other logical matrix jobs continue normally. Set
`fail-fast: false` to let every combination reach its normal terminal result.

### Job timeouts

`jobs.<id>.timeout-minutes` accepts a positive integer or an expression that
resolves to a positive whole number. It defaults to 360 minutes. The controller
caps both explicit and default values at `--max-job-timeout` and persists the
effective value in `WorkflowJob.spec.timeoutSeconds`.

The effective timeout is applied to both the runner execution context and the
native Kubernetes Job's `activeDeadlineSeconds`. When it expires, the active
command is cancelled and eligible `cancelled()` and `always()` workflow and
composite steps run before action post hooks. All timeout and cancellation
cleanup shares one five-minute window. The Pod's
`terminationGracePeriodSeconds` enforces the same cleanup bound if the native
Job deadline expires or a cancellation deletes the Job.

A timed-out WorkflowJob has `status.result: failure` and a false `Succeeded`
condition with reason `JobTimedOut`, while user cancellation has
`status.result: cancelled`. A WorkflowRun containing a timed-out job uses
reason `JobTimedOut` and reports the GitHub Check Run conclusion `timed_out`;
ordinary failures use `JobFailed` and `failure`, and cancellations use
`JobCancelled` and `cancelled`. When a run contains both timed-out and ordinarily
failed jobs, `JobTimedOut` and `timed_out` take precedence in its summary.

### Conditions

The resources expose these condition contracts:

| Resource | Condition | Status | Reasons |
| --- | --- | --- | --- |
| `Project` | `Configured` | `True` | `ConfigurationValid` |
| `Project` | `Configured` | `False` | `DuplicateInstallation`, `CredentialsUnavailable`, `InvalidCredentials`, `ProjectValuesUnavailable` |
| `Runner` | `Ready` | `True` | `Ready` |
| `Runner` | `Ready` | `False` | `ProjectUnavailable`, `ProjectNotConfigured` |
| `Runner` | `Busy` | `False` | `Idle` |
| `Runner` | `Busy` | `True` | `JobAssigned` |
| `WorkflowRun` | `Planned` | `True` | `JobsPlanned` |
| `WorkflowRun` | `Planned` | `Unknown` | `WaitingForConcurrency`, `WaitingForConcurrencyCancellation`, `ProjectUnavailable`, `CredentialsUnavailable`, `ProjectValuesUnavailable`, `GitHubAuthenticationFailed`, `WorkflowFetchFailed`, `ChildCreationFailed`, `ConcurrencyCheckFailed` |
| `WorkflowRun` | `Planned` | `False` | `ProjectUnavailable`, `WorkflowFetchFailed`, `WorkflowInvalid`, `TriggerInvalid`, `RerunInvalid`, `ChildCreationFailed`, `ExecutionStateLost` |
| `WorkflowRun` | `Succeeded` | `Unknown` | `JobsWaiting`, `JobsQueued`, `JobsRunning` |
| `WorkflowRun` | `Succeeded` | `True` | `JobsSucceeded` |
| `WorkflowRun` | `Succeeded` | `False` | `ProjectUnavailable`, `WorkflowFetchFailed`, `WorkflowInvalid`, `TriggerInvalid`, `RerunInvalid`, `ChildCreationFailed`, `JobFailed`, `JobTimedOut`, `JobCancelled`, `ExecutionStateLost` |
| `WorkflowJob` | `Ready` | `Unknown` | `DependenciesPending` |
| `WorkflowJob` | `Ready` | `True` | `ConditionPassed` |
| `WorkflowJob` | `Ready` | `False` | `ConditionFalse`, `ConditionEvaluationFailed`, `CancellationRequested`, `MatrixFailFast` |
| `WorkflowJob` | `Scheduled` | `True` | `RunnerAssigned` |
| `WorkflowJob` | `Scheduled` | `False` | `ConditionFalse`, `ConditionEvaluationFailed`, `CancellationRequested`, `MatrixFailFast`, `ProjectRecreated` |
| `WorkflowJob` | `Succeeded` | `Unknown` | `JobRunning` |
| `WorkflowJob` | `Succeeded` | `True` | `JobSucceeded` |
| `WorkflowJob` | `Succeeded` | `False` | `JobFailed`, `JobTimedOut`, `JobCancelled`, `JobResultInvalid`, `ConditionEvaluationFailed`, `PlanUnavailable`, `JobStartFailed`, `ExecutionStateLost`, `CancellationRequested`, `MatrixFailFast`, `ProjectRecreated` |
| `WorkflowJob` | `CancellationRequested` | `True` | `CancellationRequested`, `ConditionEvaluationFailed`, `MatrixFailFast` |
| `WorkflowJob` | `CancellationRequested` | `False` | `ConditionPassed` |

`Project/Configured` covers local Secret and ConfigMap availability,
private-key parsing, Project value constraints, and installation uniqueness. It
does not assert remote GitHub App or installation availability. `Runner/Ready`
reports operational health independently of capacity; clients use
`Runner/Busy` to determine whether a Runner already has an assignment.

When the controller reports a Project, Runner, WorkflowRun, or WorkflowJob
failure, it emits a Kubernetes Warning Event with the condition's reason and
message. Reconciliation of an unchanged condition does not emit another Event.

### Resource metadata

Child resources carry `actions.kelos.dev/project-uid`, `runner-uid`,
`workflow-run-uid`, and `workflow-job-uid` labels where applicable. WorkflowRun
objects carry `actions.kelos.dev/workflow-run-root-uid`, which groups attempts
in the same rerun lineage. The
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
distinguished from the original object. Workflow jobs, their native Jobs, and
their Pods carry the
`actions.kelos.dev/runner-result-version` annotation. Its value identifies the
runner result format required to complete that job.

Webhook-created WorkflowRuns use the workflow filename followed by a stable
20-character digest of the project, delivery replay, and workflow path.
WorkflowJobs and their native Jobs append the workflow job ID and a stable
16-character digest. Readable portions are truncated as needed to keep names
within the 63-character Kubernetes limit; retries of the same delivery reuse
the same names, while different signed payloads create distinct runs. The full
replay-and-path digest form is also recognized as an idempotency alias. Rerun
names contain their attempt number and a stable digest of the original
WorkflowRun UID.

Webhook-created WorkflowRuns carry an
`actions.kelos.dev/github-event-snapshot` annotation naming the immutable Secret
that holds the authenticated provider payload. The annotation is a bounded
reference; the payload is not embedded in the WorkflowRun or WorkflowJob API.
One snapshot is shared by all workflows selected from the delivery and by their
reruns. Kubernetes garbage collection removes it after the delivery record and
all referencing WorkflowRuns have been deleted.

## Workflow API

Workflow files must define a non-empty `name` of at most 256 characters.
Unsupported workflow fields and action reference forms are rejected during
planning. Unsupported action runtimes fail explicitly during execution.

### Workflow, job, and step environments

A top-level `env` map supplies defaults to every job in a workflow. A job-level
entry overrides a workflow entry with the same name, and a step-level entry
overrides both while that step executes. The effective environment is available
to run steps, external actions, and composite actions. Step fields and action
inputs that use the `env` context see the same effective values.

Entries in an `env` map cannot depend on other entries in that map. In a step
environment, an `env` expression sees only values inherited from the workflow
and job, not entries being defined for that step. Each workflow, job, or step
map may contain at most 100 entries. Names contain 1 to 256 characters and
values are scalar values of at most 65,536 bytes. Names beginning with
`GITHUB_` or `RUNNER_`, without regard to ASCII case, are reserved at every
scope and are rejected.

Environment expressions remain unresolved in controller-visible job plans.
The runner resolves them when the job starts, so values derived from
`github.token` or `secrets` do not enter custom resources, plan ConfigMaps,
controller logs, or diagnostics and receive the masking behavior described
under Project secrets and variables.

### Expressions

Open Actions parses expression templates independently from YAML. A value made
up of one `${{ ... }}` expression keeps its boolean, number, null, or string
type. Expressions embedded in other text are converted to strings without
discarding the surrounding literal text. Missing object properties evaluate to
an empty string. Malformed delimiters, unknown functions, unavailable contexts,
and unsupported syntax fail explicitly.

The supported grammar includes null, boolean, number, and single-quoted string
literals; property and index access; object-filter wildcards; parentheses; `!`,
comparisons, `&&`, and `||`; and the `contains`, `startsWith`, `endsWith`,
`format`, `join`, `toJSON`, and `fromJSON` functions. Comparisons and string
conversions follow GitHub Actions coercion rules. `&&` and `||` return a selected
operand and short-circuit, which supports fallback expressions and the
conventional `condition && value || fallback` form. Conditions also support
`success`, `always`, `failure`, and `cancelled` where status is available. Each
expression is limited to 256 syntax nodes and 64 nesting levels. Function and
interpolated template output are limited to 100,000 bytes during evaluation.

Step expressions also support `hashFiles` with one or more glob patterns.
Patterns are evaluated against files in `github.workspace`; `*`, `**`, `?`,
character classes, multiline patterns, and ordered `!` exclusions are
supported. Directories and symlink targets outside the workspace are not
hashed. The function returns the SHA-256 digest of the matched file digests in
path order, or an empty string when no files match.

Contexts are restricted by evaluation phase. An allowed context still fails at
evaluation when the corresponding execution feature has not supplied it.

| Phase | Allowed contexts and functions | Currently supplied |
| --- | --- | --- |
| Workflow concurrency | `github`, `inputs`, `vars` | All listed contexts |
| Workflow environment | `github`, `open_actions`, `secrets`, `inputs`, `vars` | All listed contexts |
| Workflow job condition | `github`, `open_actions`, `needs`, `vars`, `inputs`, and status functions | `github`, `open_actions`, direct dependency results and outputs, `vars`, `inputs`, and status functions |
| Job name, timeout, and runner labels | `github`, `open_actions`, `needs`, `strategy`, `matrix`, `vars`, `inputs` | `github`, `open_actions`, `inputs`, `vars`, and `matrix` for matrix jobs |
| Job environment | `github`, `open_actions`, `needs`, `strategy`, `matrix`, `vars`, `secrets`, `inputs` | `github`, `open_actions`, `inputs`, `vars`, `secrets`, and `matrix` for matrix jobs |
| Workflow step name, run script, working directory, environment, and inputs | `github`, `open_actions`, `needs`, `strategy`, `matrix`, `job`, `runner`, `env`, `vars`, `secrets`, `steps`, `inputs`, and `hashFiles` | `github`, `open_actions`, `matrix`, `runner`, `env`, `vars`, `secrets`, `inputs`, `steps`, and `hashFiles` |
| Workflow step condition | Step contexts except `secrets`, plus status functions and `hashFiles` | `github`, `open_actions`, `matrix`, `runner`, `env`, `vars`, `inputs`, `steps`, status functions, and `hashFiles` |
| Job outputs | Workflow step contexts without `hashFiles` | `github`, `open_actions`, `matrix`, `runner`, `env`, `vars`, `secrets`, `inputs`, `steps` |
| Composite step fields and outputs | `github`, `open_actions`, `runner`, `env`, `inputs`, `steps`, and `hashFiles` | All listed contexts and functions |
| Composite step condition | Composite contexts, status functions, and `hashFiles` | All listed contexts and functions |
| Action input default | `github`, `open_actions` | All listed contexts |

Values derived from `github.token` or the `secrets` context are marked sensitive
through interpolation and function calls, and evaluation diagnostics do not
include resolved values. The runner maps interrupt and termination signals to
cancelled status, separately from failure and timeout, so eligible workflow and
composite cleanup steps can run during Pod termination. A single five-minute
cleanup deadline is shared by those steps and action post hooks.

### Run identity and stale-run queries

Each first attempt receives a numeric `run_id` that is unique within the
namespace and Project name, and a one-based `run_number` for its repository and
workflow path. The counters persist independently of retained WorkflowRuns and
Project recreation. They are stored in namespace-scoped ConfigMaps whose names
start with `open-actions-run-sequence-`; preserve those ConfigMaps in backups
and while retaining WorkflowRuns to prevent reused IDs and numbers. A GitHub
Check Run rerequest creates a new immutable WorkflowRun resource in the same
lineage, reuses both values, and increments only `run_attempt`. Ordinary
webhook deliveries, manual dispatches, schedules,
reusable-workflow calls, and concurrency replacements create new lineages and
therefore receive new IDs and numbers. Controller retries and restarts do not
increment an identity already recorded in `status.identity`.

Webhook runs use the signed payload's `sender.login` as `actor`. Direct
`workflow_dispatch` and `workflow_call` resources use
`spec.source.github.actor`; it defaults to `open-actions` when the caller does
not supply an identity. Scheduled runs use `open-actions`. Reruns preserve the
first attempt's actor, matching GitHub's `github.actor` behavior.

The values are available during planning and runner execution:

| Expression | Default runner environment |
| --- | --- |
| `github.run_id` | `GITHUB_RUN_ID` |
| `github.run_number` | `GITHUB_RUN_NUMBER` |
| `github.run_attempt` | `GITHUB_RUN_ATTEMPT` |
| `github.actor` | `GITHUB_ACTOR` |
| `open_actions.run_url` | `OPEN_ACTIONS_RUN_URL` |

`open_actions.run_url` links to the current attempt in the Open Actions Console. It
is empty when the controller has no `--console-url`. Open Actions does not
construct a `github.server_url/<owner>/<repository>/actions/runs/<run_id>` URL,
because that page identifies a GitHub Actions workflow run that does not exist.

When a Console URL is configured, `open_actions.run_query_url` and
`OPEN_ACTIONS_RUN_QUERY_URL` identify a read-only JSON endpoint for detecting a
newer Open Actions run of the same Project, repository, workflow path, and
revision SHA. The endpoint has the same read access as Console workflow pages.
Query it immediately before writing a shared external status:

```bash
response=$(curl --fail --silent --show-error "$OPEN_ACTIONS_RUN_QUERY_URL")
if [ "$(jq -r '.newer' <<<"$response")" = true ]; then
  echo "Skipping status update from a stale Open Actions run"
  exit 0
fi
```

The response contains `current` and, when `newer` is `true`, `superseding`
objects with string-valued `id`, `number`, and `attempt` fields plus `actor`,
`namespace`, `name`, and the Console `url`. A higher run ID supersedes the
current lineage; for the same ID, only a higher attempt supersedes it. The query
is live rather than captured in the job plan, so it detects concurrency
replacements and reruns that begin after the job starts.
The endpoint returns HTTP 503 until the current run identity is available.

GitHub's Actions REST endpoints under
`/repos/{owner}/{repo}/actions/runs`, including workflow-specific listings,
jobs, logs, cancellation, and rerun endpoints, contain only GitHub Actions
workflow-run records. They never contain Open Actions WorkflowRuns. GitHub's
Checks endpoints can return the Check Runs reported by Open Actions, but the
Console query above is the supported stale-run contract.

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
The complete versioned job result, including its conclusion, is limited to 4
KiB and 100 outputs. Exceeding that bound fails the job without persisting a
partial result. A successful job finishes with reason `JobResultInvalid` when
its runner result is missing or malformed.

### Job dependencies and conditions

`jobs.<id>.needs` accepts one job ID or a list of IDs. Planning rejects missing
jobs, repeated dependencies, self-dependencies, and cycles. A WorkflowJob waits
until every direct dependency has a terminal result before its condition is
evaluated. Jobs without `needs` may run in parallel.

An omitted job `if` condition uses the default `success()` gate. Conditions
without an explicit status function also require successful ancestors.
`failure()` includes failures anywhere in the transitive dependency chain,
while the `needs` context contains only direct dependencies. Each direct entry
supplies `needs.<job>.result` as `success`, `failure`, `skipped`, or `cancelled`
and exposes persisted values through `needs.<job>.outputs`. A matrix dependency
becomes terminal only after every expanded job finishes, and its child results
are aggregated under the logical job ID. A false condition records the job as
skipped without assigning a Runner. `always()`, `failure()`, and `cancelled()`
can allow report or cleanup jobs to run after unsuccessful dependencies.

Graceful cancellation sets `spec.cancelRequested`. Assigned jobs are cancelled
unless their job condition still evaluates to true, and unassigned jobs are
recorded as cancelled unless their condition permits cancellation-time work.
Deleting the WorkflowRun remains the force-cancellation path and does not wait
for graph cleanup jobs.

### Concurrency

Concurrency groups are case-insensitive and scoped by Project and repository.
One run may execute while one newer run waits. A newer waiting run gracefully
cancels the existing waiting run. When `cancel-in-progress` is `true`, it also
requests graceful cancellation of the executing run. A replacement waits for
cancellation-aware reporting and cleanup jobs before taking the group. A run
waits for an older run in the same repository until that run has evaluated its
workflow concurrency configuration.

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
ordinary `pull_request` workflows use a deterministic integration revision. This
differs from GitHub Actions, which loads native `pull_request_target` workflows
from the repository's default branch. Review and review-comment events discover
and execute workflows only from the trusted default branch, so a maintainer
action cannot execute a fork-controlled workflow definition. Checks for review
events are reported on the trusted default-branch revision,
`pull_request_target` checks are reported on the trusted base-branch revision,
and ordinary `pull_request` checks are reported on the pull request head
revision.

For an ordinary open `pull_request`, `github.sha` identifies the locally
constructed integration commit. `github.event.pull_request.merge_commit_sha`
retains the value received from GitHub, while
`github.event.pull_request.base.sha` and `github.event.pull_request.head.sha`
identify its pinned parents. The default
checkout of the current repository with `actions/checkout` fetches the pinned
commits and reconstructs that integration commit in the workspace. Setting an
explicit `ref` other than the integration SHA, or checking out another
repository, leaves checkout behavior unchanged. The integration SHA is local to
Open Actions and cannot be fetched from the GitHub remote by SHA; workflows
that need it should use `actions/checkout` rather than fetching `github.sha`
directly. Steps that send `github.sha` to a GitHub API or another service that
requires a GitHub-hosted commit must use the appropriate
`github.event.pull_request.head.sha` or `github.event.pull_request.base.sha`
instead.

For `pull_request_target`, `spec.source.github.revision` always identifies the
trusted base-branch workflow and execution commit, and `revision.ref` must equal
`refs/heads/` followed by `event.pullRequest.baseRef`. The execution SHA is
pinned to the base commit from the signed webhook instead of resolving the
mutable branch during asynchronous delivery processing. Bounded untrusted
metadata is recorded separately under `event.pullRequest`: the pull request
number and body, HTML URL, head repository, head branch and SHA, and base branch.
The complete authenticated webhook payload is exposed through
`github.event.pull_request` without rewriting its base, head, repository,
number, URL, or merge fields. For this event,
`github.sha` and `github.event.pull_request.base.sha` are both the pinned trusted
base SHA; `github.event.pull_request.head.sha` remains the untrusted fork head.
Open Actions does not
automatically check out the head or merge ref, expand the runner token beyond
repository-scoped Contents read, or grant approval-gated secrets based on this
metadata.

#### Trusted fork checkout

Treat every value under `github.event.pull_request.head` and all content fetched
from `refs/pull/<number>/merge` as untrusted. Each `synchronize`
delivery records its own immutable head SHA and creates a distinct WorkflowRun,
so an approval system must bind approval to that SHA or to the exact
WorkflowJob. An approval attached to an earlier delivery must not authorize a
later fork update. Open Actions does not infer approval from labels, comments,
or the existence of a pull request.

Current releases of `actions/checkout` refuse an explicit fork head or merge-ref
checkout from `pull_request_target` unless the trusted workflow sets
`allow-unsafe-pr-checkout: true`. Older commit-pinned releases may not contain
this guard. The opt-in only makes the checkout explicit; it does not make the
checked-out code trusted. Follow the
[GitHub `pull_request_target` security guidance](https://docs.github.com/en/actions/reference/security/securely-using-pull_request_target),
use an isolated runner, and do not expose secrets until an approval tied to the
current head SHA has passed.

When an approved workflow must test the base repository's merge ref, disable
persisted credentials and verify the checked-out merge commit before executing
any pull-request-controlled command:

```yaml
- uses: actions/checkout@v4
  with:
    ref: refs/pull/${{ github.event.pull_request.number }}/merge
    fetch-depth: 2
    persist-credentials: false
    allow-unsafe-pr-checkout: true
- name: Verify approved pull request head
  env:
    EXPECTED_BASE_SHA: ${{ github.event.pull_request.base.sha }}
    EXPECTED_HEAD_SHA: ${{ github.event.pull_request.head.sha }}
  run: |
    set -euo pipefail
    read -r base_parent head_parent extra < <(git show --no-patch --format=%P HEAD)
    if [ "$base_parent" != "$EXPECTED_BASE_SHA" ] || [ "$head_parent" != "$EXPECTED_HEAD_SHA" ] || [ -n "${extra:-}" ]; then
      echo "::error::merge ref does not match the approved base and pull request head"
      exit 1
    fi
```

The verification step must precede builds, dependency installation, local
actions, or any other operation that can execute repository content. A missing
or superseded merge ref therefore fails before untrusted code runs. Prefer the
base repository's `refs/pull/<number>/merge` ref over cloning the fork directly.
The job token is scoped to the base repository and is not used to authenticate
downloads of actions from the fork repository.

`workflow_run` accepts `workflows`, `types`, and `branches` filters and consumes
GitHub App `workflow_run` webhooks. It does not synthesize a GitHub
`workflow_run` delivery when an Open Actions run completes.

Webhook-backed runs expose the complete authenticated GitHub payload through
`github.event`. The exact signed bytes are stored in an immutable Secret and
mounted as `GITHUB_EVENT_PATH`, while expressions decode that same snapshot.
The payload remains outside CRD spec and status fields and job-plan ConfigMaps.
Controller logs and diagnostics identify the snapshot Secret without including
provider payload fields. Draft release activities are ignored.

Issue, comment, review, and pull request bodies contain at most 48,000
characters. Pull request HTML URLs contain at most 2,048 characters.
`workflow_run.conclusion` contains at most 64 lowercase letters or underscores,
and `workflow_run.head_sha` is a 40-character lowercase hexadecimal SHA. Release
tag names contain at most 1,014 characters so the `refs/tags/` revision ref
remains within its 1,024-character limit.

`workflow_dispatch` accepts up to 25 typed, bounded inputs. Initiate a manual
run by creating a `WorkflowRun` through the Kubernetes API with the selected
workflow path, pinned commit and branch or tag ref, initiating `actor`, and any
supplied inputs.
Use a deterministic `metadata.name` as the invocation's idempotency key; a
retry of the same request must reuse that name. `deliveryID` is reserved for
the `X-GitHub-Delivery` value on webhook-backed events. The controller verifies
that the workflow declares `workflow_dispatch`, validates the supplied inputs,
and applies defaults before planning jobs. See
[`config/samples/actions_v1alpha1_workflowrun-dispatch.yaml`](../config/samples/actions_v1alpha1_workflowrun-dispatch.yaml).
The authenticated Console **Run workflow** form creates the same resource and
can be prepopulated from a previous branch- or tag-backed run.

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
- A matrix may define at most 100 axes and expand one logical job into at most
  256 jobs. A workflow may expand to at most 1,000 jobs in total.
- Matrix axis names contain at most 256 characters, and scalar matrix values
  contain at most 1,024 characters.
- A job may contain at most 100 steps and 100,000 bytes of aggregate planned
  content.
- A completed job result may contain at most 100 outputs and 4 KiB of encoded
  output metadata.
- A run script may contain at most 65,536 bytes.
- A step condition may contain at most 65,536 bytes.
- Each workflow, job, or step `env` map and each `with` map may contain at most
  100 entries.

Names, branch patterns, action references, paths, map keys, and values are also
bounded during workflow validation. Field and aggregate content limits are
reapplied after expression evaluation before a step executes.

### Execution constraints

External actions must use the `node20`, `node24`, or composite runtime and be
available from the configured GitHub server. The default runner image pins Node
20.20.2 for `node20` actions and Node 24.19.0 for `node24` actions. Custom runner
images must provide the Node 20 executable as `node` and the Node 24 executable
as `node24` on `PATH`; execution fails before the first lifecycle hook when the
declared runtime is unavailable. They must also provide Git with support for
`merge-tree --write-tree` to run the default current-repository checkout for
ordinary pull requests. The controller and every runner that can execute its
plans must use Git versions that produce identical merge results; a mismatch
causes the runner's integration revision verification to fail. Composite
actions support Bash run steps and external action references. Composite
expressions cover inputs, step
outputs, selected GitHub and runner values, and environment variables.

Node, composite, and Bash steps can use Docker when assigned to a Runner with
`spec.execution.docker`. This capability supports tools such as kind but does
not add support for action metadata declaring `runs.using: docker`.

For webhook-backed events, `GITHUB_EVENT_PATH` contains the exact authenticated
GitHub payload, limited to 900,000 bytes, and `github.event` is decoded from the
same immutable snapshot. Synthetic `workflow_dispatch`, `schedule`, and
`workflow_call` runs use a bounded generated document containing their inputs,
schedule, repository identity, and supported event metadata.

The controller emits job-plan version 7, and the runner accepts versions 1
through 7. When a release changes the job-plan version, update every Runner
`spec.execution.image` to an image that accepts both the installed and target
controller versions before upgrading the controller. The received job-plan
version also determines the runner result version: plan versions 1 through 5
use result version 1, and plan versions 6 and 7 use result version 2. A runner that accepts more
than one plan version must emit the result version assigned to that plan, not
always the latest result version supported by the runner binary. Integration
commit construction is part of this versioned contract; changing its merge
behavior or commit metadata requires a job-plan version transition.

Docker and local actions, matrix `include` and `exclude`, service containers,
caches, and artifacts are not supported. Expressions outside the documented
fields and runtime contexts are rejected during planning or execution and are
never interpreted as literal values.
`WorkflowJob` resources are not retried or reassigned when a Runner is removed.
Native Jobs and their Pod logs are deleted one hour after completion. Completed
WorkflowRuns are retained indefinitely unless `spec.ttlSecondsAfterFinished` is
set. When that TTL expires, the WorkflowRun and any remaining owned resources
are deleted. GitHub reruns require the original and latest WorkflowRuns, and
failed-job reruns also require the latest run's WorkflowJobs, to remain
available. Open Actions does not archive logs.

## Webhook API

The webhook endpoint accepts only signed GitHub `POST` deliveries up to 900,000
bytes and requires exactly one configured project for the installation.
Supported deliveries return HTTP 202 with `{"accepted":true,"queued":true}`. Unsupported
event names return HTTP 202 with `{"accepted":true,"queued":false}`. For an
ordinary open pull request, the controller resolves the merge base and
constructs a deterministic integration commit from the base and head SHAs in
the signed webhook. It uses that commit to discover and plan workflows without
waiting for GitHub's mutable pull request merge ref. Fork and conflicting pull
requests are skipped. Eligible `pull_request_target` workflows are discovered
independently and load only from the pull request's base branch in the base
repository.

For a Check Run created by Open Actions, GitHub's **Re-run** action sends a
`check_run.rerequested` delivery. Open Actions authenticates the delivery,
verifies the App, repository, check ID, external ID, and reported commit, and
creates a new immutable WorkflowRun attempt. A run that failed because one or
more jobs failed reruns the failed expanded job IDs, their transitive
dependents, and the prerequisite jobs needed to make that selected dependency
graph complete. Matrix failures select the failed combinations until a
dependent needs the logical matrix job, in which case every combination is a
required prerequisite. A successful or cancelled run, or a run that failed
before job results were available, reruns every job. The new attempt clears any
prior cancellation request, updates the original GitHub Check Run instead of
creating another check, and points its details URL to the new attempt. Open
Actions does not call GitHub's Actions rerun API because these jobs are executed
as Open Actions resources rather than GitHub Actions jobs.

Queued deliveries are processed asynchronously. Invalid or unsupported workflow
definitions fail the whole delivery before any `WorkflowRun` resources are
created. Repeated deliveries with the same signed body are deduplicated for 24
hours. Check rerun deliveries are deduplicated by `X-GitHub-Delivery`, so two
separate user rerequests remain distinct intents.
