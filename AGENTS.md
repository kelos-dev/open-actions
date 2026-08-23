# Project Conventions for AI Assistants

## General rules

- **Use Makefile targets.** Use the repository's documented targets instead of reimplementing build, generation, verification, or test commands.
- **Keep changes minimal.** Do not refactor, reorganize, or improve code beyond what the task requires.
- **Reuse build logic in automation.** CI and release workflows must call existing Makefile targets instead of duplicating their commands in YAML.
- **Test changed behavior.** Add or improve unit, integration, or end-to-end coverage when behavior changes, choosing the narrowest level that proves the contract.
- **Use structured logging.** Keep log messages concise and free of terminal punctuation. Put variable values and machine-readable categories in key-value fields instead of interpolating them into the message.
- **Keep commit messages self-contained.** Do not include pull request links in commit messages.
- **Compare Kubernetes quantities semantically.** Use `.Equal()` or `.Cmp()` for `resource.Quantity` values, not `reflect.DeepEqual`; structurally different quantities such as `1000m` and `1` CPU can be semantically identical.
- **Never expose secrets as flag defaults.** Do not use `os.Getenv()` for a secret's Go `flag` default because usage output includes default values. Use an empty default and read the environment after `flag.Parse()`.
- **Fail fast on invalid configuration.** Return an error or exit when required configuration or credentials are invalid or missing. Do not silently fall back to unauthenticated or degraded behavior.
- **Document implemented behavior only.** Documentation, READMEs, and comments must describe what the code enforces. Do not overstate guarantees or document planned behavior as current behavior.
- **Keep `docs/reference.md` user-facing.** Document API fields, configuration, validation, defaults, and observable operational behavior. Include implementation details only when users need them to configure, operate, or troubleshoot Open Actions.
- **Name resources in errors.** Errors for operations on a named Project, Runner, WorkflowRun, WorkflowJob, or other resource must include that resource's name.

## GitHub Actions compatibility

Use the current [GitHub Actions documentation](https://docs.github.com/en/actions)
as the source of truth. Open Actions must match its documented workflow syntax,
behavior, and security rules.

- Verify changes against the relevant GitHub documentation and add conformance
  tests.
- Treat differences as bugs. Reject unsupported behavior explicitly, document
  it, and link a tracking issue.
- Kubernetes-specific features must not change the meaning of a valid GitHub
  Actions workflow.

## Tests

- Do not use Gomega's global `Expect()` inside `Eventually` polling blocks. Use `Eventually(func(g Gomega) { ... })` when assertions should be retried, or return a value or error for `Eventually` to evaluate.
- Test primary actions as well as early-return guards. Controllers and handlers must have a positive case proving that reconciliation, resource creation, GitHub reporting, or another main action runs with the expected arguments.
- Avoid vacuous substring assertions in printer and formatter tests. When checking a `label: value` line, match the complete line or a suitable regular expression rather than a bare value that may occur elsewhere in the fixture.

## Key Makefile targets

- `make verify` — check formatting, module metadata, generated Go code and CRDs, Helm rendering, and `go vet`.
- `make update` — format Go code, regenerate API code and CRDs, and tidy modules.
- `make test` — run unit tests; pass additional Go test options through `TEST_FLAGS`.
- `make test-e2e` — run end-to-end tests against an installed control plane.
- `make build` — build the commands under `cmd/` into `bin/`.
- `make image` — build component images; limit the build with `WHAT` when needed.

## Pull requests

- Follow `.github/PULL_REQUEST_TEMPLATE.md` and complete every section. Use `N/A` when there is no associated issue or a section does not apply.
- Choose exactly one `/kind` label from `api`, `bug`, `docs`, or `feature`.
- Include a `release-note` block. Write `NONE` when there is no user-facing change; otherwise describe the user-visible change.
- Use `/kind api` for new API fields, CRD changes, or changes to user-facing API behavior.
