# AGENTS.md

## Kubernetes API changes

- Design and review every Kubernetes-style API, including CRDs, according to the Kubernetes [API conventions](https://github.com/kubernetes/community/blob/main/contributors/devel/sig-architecture/api-conventions.md).
- Apply the Kubernetes [API review process](https://github.com/kubernetes/community/blob/main/sig-architecture/api-review-process.md) to this repository: review the API design before implementation, and require an explicit API-focused review before merging new resources, new versions, or changes to field meaning, defaulting, validation, or behavior.
- Treat Go API types, generated CRDs, validation, defaulting, labels, annotations, command-line flags, configuration files, and webhook request or response formats as user-facing API surfaces.
- Keep API changes level-based and declarative. Separate desired state in `spec` from observations in `status`, enable the status subresource, and use standard `metav1.Condition` fields instead of adding `phase` fields to new APIs.
- Declare every field required or optional, bound strings, numbers, lists, and maps, and document defaults and immutability. Prefer schema validation and CEL rules over controller-only validation when the rule can be expressed by the CRD schema.
- Use same-namespace references whose field names end in `Ref` or `Refs`. Reuse Kubernetes reference types when the target kind is fixed.
- Keep large, rapidly changing, or independently secured data in separate resources. Do not put logs, artifacts, raw execution history, or unbounded child-resource lists in status.
- For every API change, update generated CRDs, examples, and schema tests in the same change.
