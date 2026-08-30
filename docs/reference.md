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
`:8080`) and uses `--github-api-url` to resolve repositories for manual
dispatches. It serves its read-only views without authentication. Anyone who can
reach the Console can read Project and workflow metadata, the exact workflow
file retained after it is fetched and validated for a run, and runner logs. The
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
newest first, and links to each run's details and jobs. A run page renders the
immutable workflow file snapshot retained by the controller with that
WorkflowRun. Runs created before snapshot support or whose workflow has not
yet been fetched and validated report that the file is unavailable. The
Console presents
runner output as line-oriented GitHub Actions logs. It
supports `group` and `endgroup`, debug and annotation commands, command lines,
escaped command data and properties, and `stop-commands` markers. It also shows
action input and output names without persisting their values and groups post
actions separately. Workflow step headings show running, succeeded, failed,
cancelled, and skipped states; succeeded and skipped steps collapse
automatically, while failed and cancelled steps remain expanded. ANSI SGR
foreground and background colors,
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

An approval-required fork pull request run also presents an **Approve
workflow** action. Approval updates its policy snapshot only from
`approved: false` to `approved: true`; the controller validates the current
pull request head before creating jobs. Approval is unavailable after
cancellation or completion.

An administrator can also rerun all jobs from the latest completed attempt in
a workflow lineage. When
the attempt failed because one or more jobs failed, the administrator can
instead rerun the failed expanded job IDs, matrix combinations cancelled by
fail-fast, and their transitive dependents.
Jobs in the new attempt reuse the latest results and outputs of prerequisites
that completed in earlier attempts instead of executing those prerequisites
again.
The Console creates a new immutable WorkflowRun attempt with the same project,
source, workflow path, and retention setting, clears any prior cancellation
request, and redirects to the new run. Rerun actions are unavailable while the
latest attempt is still active. The Helm chart grants the Console `create` and
`update` access to WorkflowRuns across the namespaces it displays.

An administrator can use **Run workflow** to create a `workflow_dispatch`
WorkflowRun in any configured Project namespace. The form accepts a repository,
workflow path, branch or tag, pinned commit SHA, and declared workflow inputs.
The Console authenticates through the selected Project's GitHub App installation
and records the repository ID and canonical owner and name returned by GitHub.
Starting from an existing branch- or tag-backed run prepopulates its Project,
repository, workflow, revision, and the typed inputs declared by the run's
immutable workflow file snapshot. Each form instance carries a request ID, so
resubmitting the same dispatch is idempotent and redirects to the existing run.

The Projects page lists Project configuration across all namespaces. A Project
detail page lists the names, but never the values, of keys in its referenced
workflow Secret. An administrator can sign in with the Console token to add,
replace, and delete those keys only when the Project is in the Console's
`--secret-management-namespace`. The Helm chart sets that namespace to its
release namespace and grants the Console `create` and `update` access to Secrets
only there. The Console has cluster-wide `get` access so it can read each
Project's GitHub App private key when resolving repositories for manual
dispatches. Direct Kubernetes clients and external secret controllers can manage
the same Secret.

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

The controller reuses installation tokens for workflow discovery, planning,
and GitHub Check reporting until five minutes before their GitHub expiration.
Job tokens remain unique to each job. When GitHub returns a rate-limit response,
the controller pauses requests for the affected installation until
`X-RateLimit-Reset` permits another attempt. A secondary limit with
`Retry-After` pauses GitHub requests across installations so work from another
Project cannot continue the same burst.

```console
open-actions-controller \
  --github-api-url=https://github.example/api/v3 \
  --github-server-url=https://github.example \
  --action-clone-base-url=https://github.example \
  --max-job-timeout=6h \
  --console-url=https://actions.example \
  --artifact-service-url=http://open-actions-artifacts.open-actions-system.svc \
  --artifact-signing-key-file=/var/run/secrets/open-actions-artifacts/signing-key

open-actions-artifact-server \
  --public-url=http://open-actions-artifacts.open-actions-system.svc \
  --storage-directory=/var/lib/open-actions/artifacts \
  --signing-key-file=/var/run/secrets/open-actions-artifacts/signing-key

open-actions-console \
  --github-api-url=https://github.example/api/v3 \
  --token-file=/var/run/secrets/open-actions-console/token \
  --secure-cookie
```

### Metrics

The controller serves Prometheus metrics from `--metrics-bind-address`, which
defaults to `:8082`. In addition to controller-runtime metrics, it exposes these
duration histograms:

| Metric | Interval | Labels |
| --- | --- | --- |
| `open_actions_workflow_run_duration_seconds` | First child Job start to WorkflowRun completion | `namespace`, `project`, `conclusion` |
| `open_actions_workflow_job_queue_duration_seconds` | WorkflowJob readiness to Runner assignment | `namespace`, `project` |
| `open_actions_workflow_job_startup_duration_seconds` | Runner assignment to native Job start | `namespace`, `project` |
| `open_actions_workflow_job_execution_duration_seconds` | Native Job start to WorkflowJob completion | `namespace`, `project`, `conclusion` |
| `open_actions_webhook_request_duration_seconds` | GitHub webhook HTTP request handling | `event`, `result` |
| `open_actions_webhook_delivery_duration_seconds` | Queued delivery creation to asynchronous workflow discovery completion | `namespace`, `project`, `event`, `result` |

Run and job conclusions are `success`, `failure`, `cancelled`, or `timed_out`.
Webhook request results are `accepted`, `ignored`, `rejected`, or `error`, and
delivery results are `completed` or `failed`. Unknown or unsupported event names
use the bounded `unknown` label value.

A WorkflowJob becomes ready after its dependencies, condition, and concurrency
group permit Runner assignment. Dependency-free jobs without those gates begin
queuing after both the WorkflowJob exists and its WorkflowRun is planned. Queue
duration excludes time waiting on the dependency graph, but includes
`strategy.max-parallel` throttling and time waiting for a matching Runner.

Run, startup, and execution duration observations require their corresponding
start and completion timestamps. Runs and jobs that finish without starting are
omitted from those histograms. Webhook delivery duration covers the asynchronous
work that can create zero or more WorkflowRuns; it does not include the execution
time of those runs.

## Kubernetes API

Open Actions exposes the namespaced `Project`, `Runner`, `RunnerSet`,
`WorkflowRun`, and `WorkflowJob` resources in `actions.kelos.dev/v1alpha1`. The installed CRD
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
`spec.projectRef` is immutable, and changes to `spec.execution` apply only to
Kubernetes Jobs created afterward. `spec.execution.imagePullPolicy` controls
when Kubernetes pulls the runner image and accepts `Always`, `IfNotPresent`, or
`Never`; omitting it uses Kubernetes defaulting. `spec.execution.imagePullSecrets`
accepts up to 32 unique Secret references in the Runner's namespace that
Kubernetes uses when pulling the runner image and the optional Docker image.
`spec.execution.terminationGracePeriodSeconds` sets how long Kubernetes waits
for a workflow Pod's containers to stop after termination is requested. Omitting
it leaves the generated Pod field unset so Kubernetes applies its 30-second
default; zero requests immediate shutdown. A RunnerSet maintains homogeneous
Runners from `spec.template.spec`; its template project reference is immutable.
A `WorkflowJob` spec is immutable, and `status.runnerRef` identifies
its one-time Runner assignment. `spec.needs` records direct workflow
dependencies, `spec.if` records its scheduling condition, and
`spec.concurrency` records the unevaluated job concurrency policy.
`status.concurrency` contains the evaluated group and cancellation decision
after dependencies and the job condition settle. WorkflowRun uses the same
`status.concurrency` shape for its evaluated workflow-level policy.
`WorkflowRun.status.concurrencyGroup` remains available as a deprecated group
key fallback. For matrix jobs, `spec.matrix.jobIndex` and
`spec.matrix.jobTotal` expose the values used by the `strategy` context.
`spec.timeoutSeconds` records the effective execution timeout, and
`status.result` contains `success`, `failure`, `skipped`, or `cancelled` after
completion. After execution, `status.outputs` contains the non-secret outputs
declared by that workflow job. The controller copies these values from the
completed runner Pod before removing job credentials and releasing the Runner,
so they do not depend on Pod log retention.
`WorkflowRun.status.jobs.timedOut` counts jobs whose `Succeeded` condition has
reason `JobTimedOut`; those jobs retain `failure` as their `status.result` for
dependency evaluation.

### Fork pull request policy

`spec.source.github.forkPullRequests` controls ordinary `pull_request`
workflows whose code comes from a fork. Dependabot pull requests use this
policy even when their head repository is the base repository. The policy does
not apply to `pull_request_target`, which continues to load and execute the
trusted base-branch workflow.

```yaml
spec:
  source:
    type: GitHub
    github:
      forkPullRequests:
        enabled: true
        requireApproval: true
        sendWriteTokens: false
        sendSecrets: false
```

Omitting `forkPullRequests`, `enabled`, or `requireApproval` uses the secure
defaults shown above. The fields correspond to GitHub's fork workflow settings
as follows:

| Project field | GitHub repository setting | Behavior |
| --- | --- | --- |
| `enabled` | **Run workflows from fork pull requests** | Creates ordinary fork `pull_request` runs when true. |
| `requireApproval` | **Require approval for fork pull request workflows** | Creates the run but no WorkflowJobs until an authenticated Console user approves it. Open Actions applies this requirement to every fork run rather than evaluating contributor history. |
| `sendWriteTokens` | **Send write tokens to workflows from pull requests** | Preserves requested write permissions when true; otherwise requested writes are reduced to reads. |
| `sendSecrets` | **Send secrets and variables to workflows from pull requests** | Makes the Project Secret available when true. Project variables are non-sensitive and remain available independently of this setting. |

GitHub exposes the four settings for private repositories and exposes
contributor-based approval policies for public repositories. Because a Project
may cover both and Open Actions does not query contributor trust, the Project
policy is explicit and applies consistently to every repository in the
installation. Keep the defaults for GitHub-compatible public-fork credential
boundaries. For a private repository, copy the equivalent repository settings
into this policy; enable secrets or write tokens only when fork code is within
the installation's trust boundary. See GitHub's
[repository Actions settings](https://docs.github.com/en/repositories/managing-your-repositorys-settings-and-features/enabling-features-for-your-repository/managing-github-actions-settings-for-a-repository)
for the corresponding controls.

Dependabot runs honor `enabled` and `requireApproval`, but always use read-only
tokens and withhold Project secrets. Setting `sendWriteTokens` or `sendSecrets`
does not relax GitHub's Dependabot credential boundary.

The effective policy is copied to the immutable
`WorkflowRun.spec.forkPullRequest` snapshot when the webhook is processed, so a
later Project update cannot grant more access to an existing run. An approval
authorizes only that run's signed head SHA. Immediately before planning jobs,
the controller resolves `refs/pull/<number>/head`; if it no longer matches, the
run finishes as cancelled with reason `RevisionSuperseded`. A newer
`synchronize` delivery creates its own run and approval decision, and completes
older approval-pending runs for the same pull request as `RevisionSuperseded`.

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
job names, or runner labels are read during planning; job conditions and job
concurrency read them when dependencies settle. Job and step expressions remain
unresolved in the immutable plan. Kubernetes mounts the Secret and ConfigMap
into runner-only, read-only volumes when a job Pod starts, and the runner reads
a consistent snapshot at startup. Rotations apply to new jobs. The Docker
sidecar does not mount these volumes, and workflow commands do not receive the
internal files as ambient environment variables.

See the [Project value source sample](../config/samples/actions_v1alpha1_project-values.yaml)
for a complete Project manifest.

Secret values never enter job-plan ConfigMaps, custom-resource specs or status,
controller logs, or Console records. The runner marks values derived from the
`secrets` context as sensitive and masks configured secrets in raw, standard
and unpadded Base64, JSON-string, percent-encoded, XML-escaped, and common
shell-escaped forms. Secrets named `ACTIONS_STEP_DEBUG` or
`ACTIONS_RUNNER_DEBUG` are exempt from masking, matched without regard to case,
to match GitHub's debug-secret whitelist; their values can therefore appear in
logs and outputs. Project variables are non-sensitive and are not masked.
Missing names in either context evaluate to an empty string.

For fork pull request runs, the Project Secret is mounted only when the run's
policy snapshot has `sendSecrets: true`. Project variables remain available.
The job-scoped GitHub App installation token is available as both
`github.token` and `secrets.GITHUB_TOKEN`. It is not added to the step
environment unless the workflow assigns one of those expressions to an
environment variable or action input. Token permission selection is described
separately from Project value sources.

`ACTIONS_STEP_DEBUG` is a special Project secret or variable. A value of
`true`, after trimming surrounding whitespace and matched without regard to
case, enables the runner debug indicator for new jobs. The Secret value takes
precedence when the name exists in both sources. Other values leave the
indicator disabled. The runner retains `debug` workflow-command output only
when this setting is enabled, and the Console displays retained debug output
with the other step logs.

### Job token permissions

Open Actions accepts `permissions` at the workflow and job levels for
`actions`, `checks`, `contents`, `issues`, `packages`, `pull-requests`, and
`statuses`. Each value may be `read`, `write`, or `none`; `write` includes
read access. `read-all`, `write-all`, and `{}` are also supported. A permission
map sets every omitted permission to `none`. For `{}`, Open Actions explicitly
requests only GitHub's repository Metadata read permission instead of omitting
the token permission restriction.

When a job omits `permissions`, it inherits the workflow-level value. When
both levels omit it, the default is `contents: read`. A job-level value replaces
the workflow-level value rather than merging with it:

```yaml
permissions:
  contents: read

jobs:
  label:
    permissions:
      issues: write
    runs-on: ubuntu-latest
    steps:
      - run: gh issue edit 42 --add-label triage
        env:
          GH_TOKEN: ${{ github.token }}
```

The `label` job receives only `issues: write`; it does not inherit
`contents: read`. Release API operations use the `contents` permission, so
there is no separate `releases` permission. Open Actions rejects unsupported
permission names, including `id-token`, because it does not provide GitHub's
Actions OIDC token service.

Immediately before creating a job Pod, the controller requests a GitHub App
installation token selected to the Project repository and narrowed to the
effective permissions. A request that exceeds the permissions granted to the
configured App immediately fails the WorkflowJob before the Pod starts and
reports the requested permission set. For fork pull request runs whose policy
snapshot has `sendWriteTokens: false`, requested writes are reduced to reads;
`pull_request_target` keeps the permissions declared by its trusted base
workflow.

The controller stores the job token in an owned Kubernetes Secret. Jobs that
reference external actions on the configured GitHub server also receive a
separate action-download token; script-only jobs do not request one. The
controller revokes the tokens after the job finishes and then deletes the
Secret. GitHub App installation tokens also expire one hour after creation, so
a token used by a job running longer than one hour can expire before the job
completes.

Because this token belongs to the Project's GitHub App, GitHub treats events it
creates as ordinary App events. It does not receive the special recursive-run
suppression that GitHub applies to its native Actions `GITHUB_TOKEN`.

### External actions

External action repositories on the configured GitHub server are downloaded
with a short-lived token granting Contents read access. Ordinarily the token
covers repositories granted to the Project's GitHub App installation, so the
App must be installed on each private action repository used by a workflow.
When a fork pull request policy withholds secrets, the token is restricted to
the base repository; private actions from other repositories are unavailable
to that run. The token is used only for the exact action repository URL and is
not placed in workflow expressions, action inputs, or workflow step
environments. Runner and workflow processes share a container security
boundary, so every workflow receiving an installation-wide action token must
be trusted to read every repository granted to it. Limit the installation to
repositories within that trust boundary. No GitHub credential is sent when
`--action-clone-base-url` has a different scheme or host from
`--github-server-url`, and jobs using such a clone endpoint do not request an
action-download token.

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
rerequests. `triggeringActor` is the optional GitHub login that requested this
attempt; GitHub Check Run rerequests populate it. `jobIDs` is an optional set of
expanded WorkflowJob IDs; the selected jobs reuse the latest available results
and outputs of prerequisites from earlier attempts. Omitting `jobIDs` reruns
every job. The rerun fields are immutable.
The controller also requires the project, source, workflow path, lineage, and
attempt number to match the previous run before it executes a rerun. Workflows
with jobs whose planning depends on `needs` require a full rerun with `jobIDs`
omitted because their final configuration and expanded IDs do not exist until
dependencies finish.

### Runner images

Runner labels are canonical lowercase ASCII in Kubernetes resources. Workflow
`runs-on` labels use the same representation. Each Runner is one reusable
execution slot and accepts one queued `WorkflowJob` from its `spec.projectRef`
whose `runs-on` labels are all present in `spec.labels`.

The standard `ghcr.io/kelos-dev/open-actions-runner` image is currently based
on Ubuntu 24.04. Administrators identify this maintained profile by adding
`ubuntu-latest` to the Runner or RunnerSet `spec.labels`; workflows select it
by setting `runs-on` to `ubuntu-latest`. Open Actions publishes one standard
Ubuntu runner image per release rather than separate images for Ubuntu
versions. The image provides this supported command and native-build baseline:

| Capability | Commands or files |
| --- | --- |
| Shell and source operations | `bash`, `curl`, `git`, and `make` |
| Go builds | `go` and the compiler and headers required by cgo |
| Action runtimes | `node`, `node24`, `npm`, and `npx` |
| Docker client | `docker` |
| C and C++ builds | `cc`, `gcc`, `g++`, `ld`, and the Ubuntu libc development headers provided by `build-essential` |
| JSON processing | `jq` |
| GitHub API and releases | `gh` 2.98.0 |
| Kubernetes API | `kubectl` 1.35.8 |
| Cryptography and random data | `openssl` |
| TCP and UDP probes | `nc` from `netcat-openbsd` |

The native compiler and headers support cgo, including `go test -race` after
`actions/setup-go`. The image contract guarantees the listed command names and
capabilities, not exact Ubuntu package patch versions.

The runner image is versioned and published with Open Actions rather than on an
independent schedule. Patch releases may update tool patch versions and Ubuntu
packages for fixes, but keep the commands in the table. Removing a command,
changing the Ubuntu release, or upgrading a tool across a compatibility
boundary is a user-facing release-note item. Pin the image to the controller's
Open Actions release tag, or to an OCI digest when the exact filesystem must be
immutable. `main` and `latest` are moving tags. Workflows that require another
tool version should install it in user-writable storage or use a custom runner
image. In particular, select a `kubectl` version compatible with the target
cluster according to the Kubernetes
[version-skew policy](https://kubernetes.io/releases/version-skew-policy/).

The image runs as numeric user and group `65532:65532` under the restricted
workflow Pod security context. It remains intentionally different from a
GitHub-hosted Ubuntu virtual machine, where GitHub documents
[passwordless `sudo`](https://docs.github.com/en/actions/reference/runners/github-hosted-runners#administrative-privileges).
The workflow container has no `sudo` access and cannot install Debian packages
or modify system paths at runtime. It does not provide the full GitHub-hosted
software inventory, a system service manager, or VM-level kernel access. The
Pod does not receive a Kubernetes service-account token, so the included
`kubectl` requires credentials supplied explicitly to the workflow. Docker
commands require `spec.execution.docker`; that option adds a privileged sidecar
as described below but does not grant root privileges to workflow steps.

A custom runner image can add other tools by extending the standard image:

```dockerfile
ARG OPEN_ACTIONS_RUNNER_IMAGE=ghcr.io/kelos-dev/open-actions-runner:latest
FROM ${OPEN_ACTIONS_RUNNER_IMAGE}

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends rsync \
    && rm -rf /var/lib/apt/lists/*
USER 65532:65532
```

Build and publish the example with the base image pinned to the controller's
release:

```console
docker build \
  --file examples/runner/Dockerfile \
  --build-arg 'OPEN_ACTIONS_RUNNER_IMAGE=ghcr.io/kelos-dev/open-actions-runner:<release-tag>' \
  --tag registry.example/custom-runner:latest \
  .
docker push registry.example/custom-runner:latest
```

Set `spec.execution.image` on a Runner or
`spec.template.spec.execution.image` on a RunnerSet to the published address.
Using the same Open Actions release for the controller and base runner keeps
their job-plan versions compatible. Preserve the inherited entrypoint and
numeric non-root user. The complete example Dockerfile is available at
[`examples/runner/Dockerfile`](../examples/runner/Dockerfile).

A RunnerSet creates and owns homogeneous Runner resources from
`spec.template.spec`. `spec.replicas` defaults to one and may be zero. Template
changes update non-terminating managed Runners and affect only WorkflowJobs
created or assigned afterward according to the corresponding Runner field's
contract. Scaling down removes idle Runners before busy Runners. A busy Runner
selected for removal stops accepting assignments, finishes its current
WorkflowJob, and then terminates. `status.replicas`, `readyReplicas`,
`busyReplicas`, `idleReplicas`, and `terminatingReplicas` summarize the managed
Runners. The scale subresource supports `kubectl scale` and compatible scaling
controllers. See the [RunnerSet sample](../config/samples/actions_v1alpha1_runnerset.yaml)
for a complete manifest.

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

A custom runner image used with `spec.execution.docker` must provide a
compatible `docker` executable on `PATH`. The runner remains non-root; action
options that invoke `sudo`, such as `helm/kind-action`'s local-registry and
cloud-provider setup, are not supported by the standard runner image.

Docker execution is disabled when `spec.execution.docker` is omitted. Enabling
it changes the WorkflowJob Pod's security posture because the daemon sidecar is
privileged. Use dedicated or sandboxed nodes when workflows are not fully
trusted. See
[`config/samples/actions_v1alpha1_docker_runner.yaml`](../config/samples/actions_v1alpha1_docker_runner.yaml)
for a Docker-enabled Runner.

A job strategy may define scalar matrix axes, `include` and `exclude`
transformations, an optional positive `max-parallel`, and an optional Boolean
`fail-fast`. An axis may be an array or an expression that evaluates to an
array. The complete `matrix` value may also be an expression that evaluates to
a mapping of axes and transformations. `include` and `exclude` may be literal
arrays of scalar mappings or expressions that produce those arrays. Exclusions
partially match and remove Cartesian-product combinations. Includes are applied
in declaration order: compatible entries augment base combinations and
incompatible entries add standalone combinations. Include-only matrices are
supported.

Matrix expressions may use `github`, `open_actions`, `needs`, `vars`, and
`inputs`. Job names, runner labels, and timeout expressions may also use
`needs`. When one of these planning expressions reads `needs`, planning waits
until every direct dependency is terminal and its outputs are persisted. The
controller then evaluates the job condition before the deferred fields. A
failed, skipped, or cancelled dependency therefore skips the job under the
default success condition; an explicit status function such as `always()` can
permit evaluation. Missing outputs, invalid JSON, non-array axes, non-mapping
complete matrices, non-scalar final values, empty axes, and oversized results
finish the logical job with `JobPlanningFailed` rather than leaving it pending.

The controller creates one `WorkflowJob` per final combination in deterministic
order. Each child has a unique `spec.jobID`, while
`spec.matrix.logicalJobID`, `values`, `maxParallel`, and `failFast` preserve its
logical identity and strategy. Deferred job plans are immutable resources
owned by the WorkflowRun, so the same configuration and children are recovered
across controller restarts. `max-parallel` limits active children in that group
independently of the number of matching Runners.

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
cleanup has an internal five-minute deadline. The Runner's
`spec.execution.terminationGracePeriodSeconds` controls the workflow Pod's
termination grace period when set; omitting it uses Kubernetes' 30-second
default. If the native Job deadline expires or a cancellation deletes the Job,
the time available for cleanup is bounded by both the Pod's termination grace
period and the runner's five-minute cleanup deadline. A longer Pod grace period
does not extend the runner's cleanup deadline.

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
| `RunnerSet` | `Ready` | `True` | `RunnersReady` |
| `RunnerSet` | `Ready` | `False` | `ReplicaCountMismatch`, `RunnersTerminating`, `RunnersNotReady` |
| `WorkflowRun` | `Approved` | `True` | `ApprovalGranted`, `ApprovalNotRequired` |
| `WorkflowRun` | `Approved` | `False` | `ApprovalRequired`, `RevisionSuperseded`, `JobCancelled` |
| `WorkflowRun` | `Planned` | `True` | `JobsPlanned` |
| `WorkflowRun` | `Planned` | `Unknown` | `ApprovalRequired`, `ApprovalValidationFailed`, `WaitingForPriorRunPlanning`, `WaitingForConcurrency`, `WaitingForConcurrencyCancellation`, `ProjectUnavailable`, `CredentialsUnavailable`, `ProjectValuesUnavailable`, `GitHubAuthenticationFailed`, `WorkflowFetchFailed`, `ChildCreationFailed`, `ConcurrencyCheckFailed` |
| `WorkflowRun` | `Planned` | `False` | `ProjectUnavailable`, `WorkflowFetchFailed`, `WorkflowInvalid`, `TriggerInvalid`, `RerunInvalid`, `ChildCreationFailed`, `RevisionSuperseded`, `JobCancelled`, `ExecutionStateLost` |
| `WorkflowRun` | `Succeeded` | `Unknown` | `JobsWaiting`, `JobsQueued`, `JobsRunning` |
| `WorkflowRun` | `Succeeded` | `True` | `JobsSucceeded` |
| `WorkflowRun` | `Succeeded` | `False` | `ProjectUnavailable`, `WorkflowFetchFailed`, `WorkflowInvalid`, `TriggerInvalid`, `RerunInvalid`, `ChildCreationFailed`, `JobFailed`, `JobTimedOut`, `JobCancelled`, `RevisionSuperseded`, `ExecutionStateLost` |
| `WorkflowJob` | `Ready` | `Unknown` | `DependenciesPending`, `WaitingForConcurrency` |
| `WorkflowJob` | `Ready` | `True` | `ConditionPassed`, `ConcurrencyAcquired` |
| `WorkflowJob` | `Ready` | `False` | `ConditionFalse`, `ConditionEvaluationFailed`, `JobPlanningFailed`, `ConcurrencyEvaluationFailed`, `ConcurrencySuperseded`, `ConcurrencyCancelled`, `CancellationRequested`, `MatrixFailFast` |
| `WorkflowJob` | `ConcurrencyAcquired` | `Unknown` | `WaitingForConcurrency` |
| `WorkflowJob` | `ConcurrencyAcquired` | `True` | `ConcurrencyAcquired` |
| `WorkflowJob` | `ConcurrencyAcquired` | `False` | `ConcurrencySuperseded` |
| `WorkflowJob` | `Scheduled` | `True` | `RunnerAssigned` |
| `WorkflowJob` | `Scheduled` | `False` | `ConditionFalse`, `ConditionEvaluationFailed`, `JobPlanningFailed`, `ConcurrencyEvaluationFailed`, `ConcurrencySuperseded`, `ConcurrencyCancelled`, `CancellationRequested`, `MatrixFailFast`, `ProjectRecreated` |
| `WorkflowJob` | `Succeeded` | `Unknown` | `JobRunning` |
| `WorkflowJob` | `Succeeded` | `True` | `JobSucceeded` |
| `WorkflowJob` | `Succeeded` | `False` | `JobFailed`, `JobTimedOut`, `JobCancelled`, `JobResultInvalid`, `ConditionEvaluationFailed`, `JobPlanningFailed`, `ConcurrencyEvaluationFailed`, `PlanUnavailable`, `JobStartFailed`, `GitHubTokenPermissionsRejected`, `ExecutionStateLost`, `CancellationRequested`, `MatrixFailFast`, `ProjectRecreated` |
| `WorkflowJob` | `CancellationRequested` | `True` | `CancellationRequested`, `ConditionEvaluationFailed`, `ConcurrencyCancelled`, `MatrixFailFast` |
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
`runner-set-uid`, `workflow-run-uid`, and `workflow-job-uid` labels where
applicable. WorkflowRun
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

After a workflow file is fetched and validated, its WorkflowRun carries an
`actions.kelos.dev/workflow-file` annotation naming the immutable ConfigMap that
holds the exact file in its `workflow.yaml` key. The ConfigMap is owned by the
WorkflowRun and is garbage-collected with it. The Console reports the workflow
file as unavailable when the annotation or ConfigMap is missing.

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
values are scalar values of at most 65,536 bytes.

Open Actions supplies the following runner-owned names:

- Action identity: `GITHUB_ACTION_PATH`, `GITHUB_ACTION_REPOSITORY`.
- Workflow and repository identity: `GITHUB_ACTIONS`, `GITHUB_API_URL`,
  `GITHUB_ACTOR`, `GITHUB_BASE_REF`, `GITHUB_EVENT_ACTION`,
  `GITHUB_EVENT_NAME`, `GITHUB_EVENT_PATH`, `GITHUB_GRAPHQL_URL`,
  `GITHUB_HEAD_REF`, `GITHUB_JOB`, `GITHUB_REF`, `GITHUB_REF_NAME`,
  `GITHUB_REF_TYPE`, `GITHUB_REPOSITORY`, `GITHUB_REPOSITORY_ID`,
  `GITHUB_REPOSITORY_OWNER`, `GITHUB_RETENTION_DAYS`, `GITHUB_RUN_ATTEMPT`,
  `GITHUB_RUN_ID`, `GITHUB_RUN_NUMBER`, `GITHUB_SERVER_URL`, `GITHUB_SHA`,
  `GITHUB_TRIGGERING_ACTOR`, `GITHUB_WORKFLOW`, `GITHUB_WORKFLOW_REF`,
  `GITHUB_WORKFLOW_SHA`, `GITHUB_WORKSPACE`.
- Command files: `GITHUB_ENV`, `GITHUB_OUTPUT`, `GITHUB_PATH`, `GITHUB_STATE`,
  `GITHUB_STEP_SUMMARY`.
- Runner identity and paths: `RUNNER_ARCH`, `RUNNER_ENVIRONMENT`, `RUNNER_NAME`,
  `RUNNER_OS`, `RUNNER_TEMP`, `RUNNER_TOOL_CACHE`, and the conditional
  `RUNNER_DEBUG`.

Environment names are case-sensitive, matching GitHub Actions on Linux.
Workflow, job, step, and composite-action maps may assign runner-owned names,
but the runner-supplied value takes precedence in the process environment.
Assignments to the exact names through `GITHUB_ENV` are also ignored. A name
with different casing is a separate variable. `GITHUB_ENV` additionally cannot
set `NODE_OPTIONS` under any casing; a workflow or step `env` map may set it.

Other names with a `GITHUB_` or `RUNNER_` prefix are allowed. To pass the
job-scoped installation token under the conventional name, assign
`github.token` or `secrets.GITHUB_TOKEN` explicitly; the value remains secret
and is masked in output:

```yaml
env:
  GITHUB_TOKEN: ${{ github.token }}
```

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

Contexts are restricted by expression site. A context that is not listed below
is rejected when the workflow or action metadata is loaded.

| Expression site | Contexts and functions |
| --- | --- |
| Workflow concurrency | `github`, `inputs`, `vars` |
| Workflow environment | `github`, `open_actions`, `secrets`, `inputs`, `vars` |
| Job condition | `github`, `open_actions`, `needs`, `vars`, `inputs`; status functions |
| Job matrix | `github`, `open_actions`, `needs`, `vars`, `inputs` |
| Job name, runner labels, timeout, and concurrency | `github`, `open_actions`, `needs`, `strategy`, `matrix`, `vars`, `inputs` |
| Job environment | `github`, `open_actions`, `needs`, `strategy`, `matrix`, `vars`, `secrets`, `inputs` |
| Job outputs | `github`, `open_actions`, `needs`, `strategy`, `matrix`, `job`, `runner`, `env`, `vars`, `secrets`, `steps`, `inputs` |
| Workflow step name, run script, working directory, environment, inputs, and `continue-on-error` | `github`, `open_actions`, `needs`, `strategy`, `matrix`, `job`, `runner`, `env`, `vars`, `secrets`, `steps`, `inputs`; `hashFiles` |
| Workflow step condition | Step contexts except `secrets`; status functions and `hashFiles` |
| Action input default | `github`, `open_actions`, `strategy`, `matrix`, `job`, `runner`; `hashFiles` |
| Composite step name, run script, working directory, environment, inputs, and `continue-on-error` | `github`, `open_actions`, `inputs`, `strategy`, `matrix`, `steps`, `job`, `runner`, `env`; `hashFiles` |
| Composite step condition | Composite step contexts; status functions and `hashFiles` |
| Composite output | Composite step contexts without `hashFiles` |

`open_actions` is an Open Actions extension. It supplies `run_url` and
`run_query_url` at every site where the table lists it.

The `github` context supplies the following properties when their source is
available:

- Repository and revision: `repository`, `repository_id`, `repository_owner`,
  `repositoryUrl`, `sha`, `ref`, `ref_name`, `ref_type`, `head_ref`, and
  `base_ref`.
- Workflow and run: `workflow`, `workflow_ref`, `workflow_sha`, `run_id`,
  `run_number`, `run_attempt`, `actor`, `triggering_actor`, `job`, and
  `retention_days`.
- Event and endpoints: `event`, `event_name`, `event_path`, `server_url`,
  `api_url`, and `graphql_url`.
- Execution: `workspace`, `token`, and `secret_source`.
- Action metadata: `action_path`, `action_ref`, `action_repository`, and
  `action_status` for composite actions, with repository and ref also available
  while action input defaults are evaluated.

IDs and run counters in `github` are strings, `ref_protected` is Boolean when
available, and all other scalar properties are strings. `event` preserves the
JSON payload's Boolean, number, string, array, object, and null types. Webhook
payloads also supply `actor_id` and `repository_owner_id` when the corresponding
numeric ID is present. Synthetic events do not invent those IDs. Open Actions
does not currently supply `github.action`, `github.env`, `github.path`, or
`github.ref_protected` to expressions. The `GITHUB_ENV` and `GITHUB_PATH`
process variables still identify the current command files. Missing and
inapplicable properties evaluate to an empty string.

The `job` context supplies `status`, `workflow_ref`, `workflow_sha`,
`workflow_repository`, and `workflow_file_path`. `status` reflects the job's
current `success`, `failure`, or `cancelled` state. Job containers and services
are unsupported, so `job.container` and `job.services` are unavailable. Open
Actions reports one Check Run for the workflow rather than one per job and does
not supply `job.check_run_id`.

For each expanded matrix job, `strategy` supplies numeric `job-index`,
`job-total`, and `max-parallel` values and Boolean `fail-fast`; `matrix`
contains the typed scalar values for that combination. Both contexts are empty
for a non-matrix job. `runner` supplies `name`, `os`, `arch`, `temp`,
`tool_cache`, and `environment` during job execution. When
`ACTIONS_STEP_DEBUG` enables the runner debug indicator, `runner.debug` has the
string value `1`; otherwise that property is absent.

Each completed or skipped identified step appears in `steps` with an `outputs`
mapping plus `outcome` and `conclusion`. The latter values are `success`,
`failure`, `cancelled`, or `skipped`; `continue-on-error` can make a failed
outcome have a successful conclusion. Composite actions receive the same shape
for their own identified steps. `needs` contains only direct dependencies and
supplies their `result` and persisted `outputs` mappings.

`inputs` preserves the declared input types: strings remain strings, Booleans
remain Boolean, and numeric values remain numbers. `vars`, `secrets`, and `env`
contain string values. The `env` context includes only values declared at the
workflow, job, current step, or current composite scope and values written to
`GITHUB_ENV` by completed commands. It does not expose inherited process
variables or runner default variables. Context and property lookup is
case-insensitive; environment variable names remain case-sensitive on Linux.

`runner.name` and `RUNNER_NAME` identify the assigned Runner resource.
`runner.environment` and `RUNNER_ENVIRONMENT` are `self-hosted`, because Open
Actions runners execute on user-managed infrastructure. The `RUNNER_DEBUG`
process variable follows `runner.debug`. Action metadata input defaults receive
the same runner context, including the expression used by
`actions/github-script`: `${{ runner.debug == '1' }}`.
Every action input default is syntax- and context-validated when the action
metadata loads, including defaults for supplied inputs and skipped steps.

Values derived from `github.token` or the `secrets` context are marked sensitive
through interpolation and function calls, and evaluation diagnostics do not
include resolved values. The runner maps interrupt and termination signals to
cancelled status, separately from failure and timeout, so eligible workflow and
composite cleanup steps can run during Pod termination. Those steps and action
post hooks share an internal five-minute cleanup deadline, while Kubernetes may
stop the Pod sooner when its termination grace period expires.

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
first attempt's actor, matching GitHub's `github.actor` behavior. For GitHub
Check Run rerequests, `github.triggering_actor` identifies the user who
requested the current attempt; otherwise it matches `github.actor`.

The values are available during planning and runner execution:

| Expression | Default runner environment |
| --- | --- |
| `github.run_id` | `GITHUB_RUN_ID` |
| `github.run_number` | `GITHUB_RUN_NUMBER` |
| `github.run_attempt` | `GITHUB_RUN_ATTEMPT` |
| `github.actor` | `GITHUB_ACTOR` |
| `github.triggering_actor` | `GITHUB_TRIGGERING_ACTOR` |
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
empty string. After an identified step runs or is skipped,
`steps.<id>.outcome` and `steps.<id>.conclusion` are each `success`, `failure`,
`cancelled`, or `skipped`. `outcome` records the execution result before
`continue-on-error` is applied, while `conclusion` records the result afterward.

A run step or external action may set `continue-on-error` to a Boolean or to a
single expression that evaluates to a Boolean. When such a step exits
unsuccessfully and the resolved value is `true`, later steps continue under a
successful job status. The step remains observable as
`steps.<id>.outcome == 'failure'`, while
`steps.<id>.conclusion == 'success'`; outputs emitted before the failure remain
available. A false value preserves ordinary failure behavior. Cancellation
remains terminal and is never converted to success. Runner logs retain the
underlying error, and the Console shows a successful step conclusion with the
failure warning inside the step group. Action post hooks still run and can fail
the job independently.

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
and exposes persisted values through `needs.<job>.outputs`. Missing output
properties evaluate to an empty string. Outputs omitted as sensitive are not
present in this context.

The controller records one immutable, versioned dependency snapshot after all
direct dependencies finish and before the dependent job becomes runnable. The
same snapshot is used for the job environment, every workflow step expression,
and job outputs, and remains available across controller restarts. The encoded
snapshot is limited to 900,000 bytes; a snapshot that exceeds the limit fails
the dependent job with reason `PlanUnavailable`. Jobs cannot access entries
they did not name in `needs`.

A matrix dependency becomes terminal only after every expanded job finishes,
and its child results and outputs are aggregated under the logical job ID. A
false condition records the job as skipped without assigning a Runner.
`always()`, `failure()`, and `cancelled()` can allow report or cleanup jobs to
run after unsuccessful dependencies.

Outputs used by a dynamic matrix are read only from direct `needs` entries and
use persisted `WorkflowJob.status.outputs` values. A producer that omits a
referenced output supplies the normal empty missing-property value; functions
such as `fromJSON` then report that empty value as an evaluation error.
Output-derived matrices are deterministically re-evaluated from immutable
terminal outputs and the Project variable values captured during initial
planning. Changes to Project variables while a run is active do not alter its
matrix children during controller recovery.

Graceful cancellation sets `spec.cancelRequested`. Assigned jobs are cancelled
unless their job condition still evaluates to true, and unassigned jobs are
recorded as cancelled unless their condition permits cancellation-time work.
Deleting the WorkflowRun remains the force-cancellation path and does not wait
for graph cleanup jobs.

### Concurrency

Workflow and `jobs.<id>.concurrency` accept either a scalar group or a mapping
with `group` and `cancel-in-progress`. Groups are
case-insensitive, scoped by Project and repository, and shared by workflow runs
and jobs. At most one member executes while one newer member waits. A newer
waiting member cancels and replaces the existing waiting member. When
`cancel-in-progress` is `true`, it also requests graceful cancellation of the
executing member. Cancelling an executing job does not cancel its WorkflowRun or
unrelated jobs. A replacement acquires the group only after the executing
member's active workload and cancellation-aware cleanup have stopped.

Workflow concurrency is acquired before any jobs in the run become ready. Job
concurrency is evaluated after direct dependencies reach terminal results and
the job condition passes, when `needs`, `strategy`, and `matrix` values are
known. The job concurrency gate precedes Runner assignment, including matrix
`max-parallel`. Matrix `fail-fast` and WorkflowRun cancellation can still cancel
a job while it waits. A job whose evaluated group matches its parent
WorkflowRun's group fails with reason `ConcurrencyEvaluationFailed` because the
run holds that group until all of its jobs finish. WorkflowRuns and WorkflowJobs
store their evaluated group and cancellation decision under `status.concurrency`
before entering the gate, so controller restarts resume the same acquisition or
replacement decision. A run waits for an older run in the same repository until
that run has evaluated its workflow concurrency configuration.

At workflow scope, the `group` and `cancel-in-progress` expressions are
evaluated together during planning. Both may use `github`, `inputs`, and `vars`;
`secrets` and job-only contexts are unavailable. At job scope, both expressions
are evaluated after dependencies and the job condition settle. They may use
`github`, `needs`, `strategy`, `matrix`, `inputs`, and `vars`; `strategy` and
`matrix` are populated for matrix jobs. Secrets are unavailable at both scopes.

`cancel-in-progress` accepts a Boolean literal or one whole expression that
evaluates to a Boolean. A true result requests graceful cancellation of the
executing member. A false result leaves that member executing and makes the new
member wait while retaining the normal one-pending-member replacement behavior.
For example:

```yaml
concurrency:
  group: deploy-${{ github.ref_name }}
  cancel-in-progress: ${{ github.event_name == 'pull_request' && github.ref != 'refs/heads/main' }}

jobs:
  deploy:
    concurrency:
      group: deploy-${{ matrix.environment }}
      cancel-in-progress: ${{ needs.build.result == 'success' && matrix.environment != 'production' }}
```

Concurrency expressions reject unavailable contexts, non-Boolean
`cancel-in-progress` results, and empty evaluated groups. Missing event
properties evaluate to an empty string, which is invalid as the whole result of
`cancel-in-progress` but may be compared explicitly. Use
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

Ordinary fork and Dependabot pull requests follow the Project's fork pull
request policy. Their workflow definition comes from the pull request head and
their execution revision is the same deterministic base/head integration used
for same-repository pull requests. Approval, when required, gates job planning
and is tied to the signed head SHA.

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
from `refs/pull/<number>/merge` as untrusted. Ordinary `pull_request` fork runs
use the integration checkout described above and the built-in approval gate.
Each `synchronize` delivery records its own immutable head SHA and creates a
distinct WorkflowRun. Open Actions does not infer approval from labels,
comments, or the existence of a pull request.

Current releases of `actions/checkout` refuse an explicit fork head or merge-ref
checkout from `pull_request_target` unless the trusted workflow sets
`allow-unsafe-pr-checkout: true`. Older commit-pinned releases may not contain
this guard. The opt-in only makes the checkout explicit; it does not make the
checked-out code trusted. Follow the
[GitHub `pull_request_target` security guidance](https://docs.github.com/en/actions/reference/security/securely-using-pull_request_target),
use an isolated runner, and do not expose secrets until an approval tied to the
current head SHA has passed. The ordinary fork policy does not authorize an
unsafe checkout in a `pull_request_target` workflow.

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
- A matrix may define at most 100 axes. `include` and `exclude` may each contain
  at most 256 mappings. The resolved, transformed matrix may expand one logical
  job into at most 256 jobs, and a workflow may expand to at most 1,000 jobs in
  total. Limits are checked before children for that dynamic matrix are created.
- Matrix axis names contain at most 256 characters, and scalar matrix values
  contain at most 1,024 characters.
- A job may contain at most 100 steps and 100,000 bytes of aggregate planned
  content.
- A completed job result may contain at most 100 outputs and 4 KiB of encoded
  output metadata.
- A run script may contain at most 65,536 bytes.
- A step condition may contain at most 65,536 bytes.
- A step `continue-on-error` expression may contain at most 65,536 bytes.
- Each workflow, job, or step `env` map and each `with` map may contain at most
  100 entries.

Names, branch patterns, action references, paths, map keys, and values are also
bounded during workflow validation. Field and aggregate content limits are
reapplied after expression evaluation before a step executes.

### Workflow artifacts

Open Actions supports the Results Service protocol used by pinned
`actions/upload-artifact` majors 4 through 7 and
`actions/download-artifact` majors 4 through 8. Artifact operations without a
`github-token` are scoped to the current WorkflowRun attempt. Cross-repository
and cross-run downloads through GitHub's artifact REST API are not supported.

Each job receives `ACTIONS_RESULTS_URL` and an `ACTIONS_RUNTIME_TOKEN`. The
controller mints the token immediately before creating the native Job and sets
its lifetime to the job's effective, capped execution timeout plus five-minute
startup and cleanup allowances. The token is held in the job's owned
authentication Secret and is removed after the job finishes. The signed token
remains valid until its expiry, including if the authentication Secret is
deleted.
Its scope includes the Project UID, GitHub repository ID, WorkflowRun lineage
and attempt, WorkflowRun UID, and WorkflowJob UID.
Jobs in one attempt may list and download artifacts finalized by other jobs in
that attempt; requests cannot use it to cross Projects, repositories, lineages,
or attempts. Anyone who can read a job authentication Secret can impersonate
that job until the token expires. Signed upload and
download URLs grant access only to one artifact and operation, expire with the
job credential, and do not contain the runtime token. Artifact bytes and
metadata are stored on the artifact volume, not in Kubernetes API objects.
The chart exposes the service only through a cluster-internal `ClusterIP` and
uses HTTP inside the cluster. Treat the job network as trusted or apply network
policies that restrict access to the artifact service; do not expose that
Service externally without a TLS-terminating, access-controlled ingress.

Artifact names are immutable and unique within an attempt. A second upload
with the same name receives a conflict. `overwrite: true` uses the action's
delete-then-create behavior and assigns a new artifact ID. Concurrent matrix
jobs should therefore include a matrix value in each artifact name. Downloads
by name select that unique finalized artifact; omitted names and `pattern`
inputs can aggregate multiple finalized artifacts in a dependent job. Missing
names fail through the action's normal `ArtifactNotFound` behavior.
The `artifact-id` and `artifact-digest` outputs are supported. The
GitHub-shaped `artifact-url` output is informational only because Open Actions
does not provide GitHub's web or REST artifact-download route.

File globbing, hidden-file selection, and `if-no-files-found` behavior are
implemented by the pinned upload action. Archive creation and extraction are
implemented by the pinned upload and download actions. Before accepting a ZIP,
the service fully reads every entry and rejects absolute or parent-relative
paths, backslashes, duplicate or conflicting paths, links, non-regular entries,
invalid archives, and content over the configured limits. Raw uploads are
stored under the validated artifact name. The official uploader dereferences a
selected symbolic link before creating its archive; workflows must not select
a link whose target should not be published.

The default limits are 1 GiB for one uncompressed file, 2 GiB for one stored or
uncompressed artifact, 10 GiB of stored artifacts for one run attempt, and 500
artifacts created by one job. Limits are enforced while blocks are staged and
again when an archive is committed. Artifact IDs, sizes, and SHA-256 digests
are verified during idempotent finalization. The defaults retain artifacts for
7 days and cap `retention-days` at 30 days. Expired artifacts and unfinished
uploads older than one hour are removed on access and by an hourly idempotent
cleanup pass; cleanup resumes after artifact service restarts. Deleting a
WorkflowRun does not delete its artifacts before their artifact retention
expires.

The Helm chart enables a standalone artifact service as a one-replica
StatefulSet. Its volume claim template provisions a 20 GiB `ReadWriteOnce`
PersistentVolumeClaim by default and retains that claim when the StatefulSet is
deleted or scaled down. The StatefulSet replaces its only Pod sequentially;
its headless Service governs Pod identity, while workflow jobs use the separate
`open-actions-artifacts` ClusterIP Service. Controller rollouts do not interrupt
uploads or cleanup. The artifact service does not use the Kubernetes API and
its service account token is not mounted.

The controller and artifact service share a credential signing key. By default,
the chart generates that key in the
`open-actions-artifact-auth` Secret. Set `artifacts.signingKeySecretName` and
`artifacts.signingKeyKey` to use an externally managed key of at least 32 bytes.
Anyone who can read the key can mint artifact credentials. After rotating an
external key, restart the `open-actions-controller` Deployment and
`open-actions-artifacts` StatefulSet; tokens and signed URLs issued with the
previous key stop working.

Set
`artifacts.persistence.existingClaim` to use a provisioned claim, or configure
its storage class, access modes, and size through the chart. The PVC must have
capacity for all retained attempts, not just the per-attempt limit. Disabling
artifact persistence uses an `emptyDir` and is intended only for disposable
test clusters because Pod replacement loses stored artifacts. Set
`artifacts.enabled=false` to omit the service and runtime credentials.

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

The controller emits job-plan version 9, and the runner accepts versions 1
through 9. When a release changes the job-plan version, update every Runner
`spec.execution.image` to an image that accepts both the installed and target
controller versions before upgrading the controller. The received job-plan
version also determines the runner result version: plan versions 1 through 5
use result version 1, and plan versions 6 through 9 use result version 2. A
runner that accepts more than one plan version must emit the result version
assigned to that plan, not always the latest result version supported by the
runner binary. Integration
commit construction is part of this versioned contract; changing its merge
behavior or commit metadata requires a job-plan version transition.

Docker actions, local actions, service containers, and caches are not
supported. Expressions outside the documented
fields and runtime contexts are rejected during planning or execution and are
never interpreted as literal values.
`WorkflowJob` resources are not retried or reassigned when a Runner is removed.
Completed native Jobs and runner Pods are retained with their WorkflowRun.
WorkflowRuns are retained indefinitely unless `spec.ttlSecondsAfterFinished` is
set. When that TTL expires, the WorkflowRun, its Jobs and Pods, and their logs
are deleted. GitHub reruns require the original and latest WorkflowRuns to
remain available. Failed-job reruns also require the WorkflowJobs that provide
the selected jobs' latest prerequisite results and outputs. Open Actions does
not archive logs outside Kubernetes, so cluster-level log rotation and node
retention policies still apply.

## Webhook API

The webhook endpoint accepts only signed GitHub `POST` deliveries up to 900,000
bytes and requires exactly one configured project for the installation.
Supported deliveries return HTTP 202 with `{"accepted":true,"queued":true}`. Unsupported
event names return HTTP 202 with `{"accepted":true,"queued":false}`. For an
ordinary open pull request, the controller resolves the merge base and
constructs a deterministic integration commit from the base and head SHAs in
the signed webhook. It uses that commit to discover and plan workflows without
waiting for GitHub's mutable pull request merge ref. Ordinary fork pull
requests are accepted according to the Project's fork pull request policy;
conflicting pull requests without an integration revision are skipped.
Eligible `pull_request_target` workflows are discovered independently and load
only from the pull request's base branch in the base repository.
Invalid fork-controlled workflow definitions and fork candidate fan-out limit
violations skip the ordinary `pull_request` candidate without suppressing
independently discovered `pull_request_target` runs.

For a Check Run created by Open Actions, GitHub's **Re-run** action sends a
`check_run.rerequested` delivery. Open Actions authenticates the delivery,
verifies the App, repository, check ID, external ID, and reported commit, and
creates a new immutable WorkflowRun attempt. A run that failed because one or
more jobs failed reruns the failed expanded job IDs, matrix combinations
cancelled by fail-fast, and their transitive dependents. The new attempt reuses
the latest results and outputs of prerequisite jobs from earlier attempts.
Static matrix failures rerun failed and fail-fast-cancelled combinations; a
dependent of the logical matrix job evaluates the combined results of rerun and
retained combinations. A successful or cancelled run, or a run that failed
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
