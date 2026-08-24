# Open Actions Helm Chart

Install or upgrade Open Actions with custom values:

```console
open-actions install --values values.yaml
```

The chart installs the controller, artifact service, Console, and the Open Actions CRDs.
CRDs carry the `helm.sh/resource-policy: keep` annotation so uninstalling the
release does not delete Open Actions resources.

The default Console URL supports access through `kubectl port-forward
service/open-actions-console 8080:80 --namespace open-actions-system`. Set
`console.publicURL` to the public HTTPS origin when exposing the Service. The
Console serves Project and workflow metadata, workflow file snapshots, and
runner Pod logs without authentication, so expose it only to the intended
audience. The chart creates
and preserves an administrator token in the `open-actions-console-auth` Secret.
Retrieve the `token` key and enter it on the Console login page to manage
Project Secrets in the release namespace and initiate manual workflow runs.
The Console uses its own Deployment and ServiceAccount with read access to
workflow resources and create access to WorkflowRuns. Set
`console.enabled=false` to omit the Console. Set `console.secretName` to mount
an externally managed Secret from the release namespace instead;
`console.tokenKey` selects its token key.

Generated WorkflowRuns omit `spec.ttlSecondsAfterFinished` by default and are
retained indefinitely. Set `controller.workflowRunTTLSecondsAfterFinished` to
populate that field on new runs. For example, `604800` retains each new run for
seven days after completion; `0` makes a run eligible for deletion immediately
after completion. Changing the chart value does not alter existing runs.
Deleting an expired WorkflowRun also deletes its owned WorkflowJobs and execution
resources.

The chart enables the internal artifact service as a standalone, one-replica
StatefulSet. By default, its volume claim template provisions persistent
storage for content and metadata and retains the claim when the StatefulSet is
deleted or scaled down. The StatefulSet replaces its only Pod sequentially;
its headless Service governs Pod identity, while workflow jobs use the separate
`open-actions-artifacts` ClusterIP Service. Controller rollouts do not interrupt
artifact traffic. The artifact service has no Kubernetes API access and does
not mount a service account token. Set
`artifacts.persistence.existingClaim` to mount a claim managed outside the
StatefulSet; that claim must be writable by the artifact Pod's supplemental
group `65532`. Size the volume for all artifacts retained across Projects and
attempts; `artifacts.maxRunBytes` is a per-attempt quota, not a volume-wide
quota. Storage administrators and principals that can mount or snapshot the
claim can read artifact content. Use storage-class encryption and access
controls appropriate for workflow output.

The controller signs job credentials for each job's effective execution timeout
plus startup and cleanup allowances, and the artifact service validates them
with a shared key. The chart generates the key in
`open-actions-artifact-auth` by default and reuses it across upgrades. Set
`artifacts.signingKeySecretName` to mount an externally managed Secret and
`artifacts.signingKeyKey` to select its key, which must contain at least 32
bytes. Readers of that key can mint artifact credentials. Restart the
controller Deployment and artifact StatefulSet after rotating an external key;
credentials signed with the previous key stop working.

`artifacts.persistence.enabled=false` replaces the claim with an `emptyDir`.
That setting is suitable for disposable test clusters only because an artifact
Pod replacement loses every artifact. `artifacts.enabled=false` omits the
artifact Service and does not issue artifact runtime credentials to jobs.

Set each component's `resources.requests` and `resources.limits` in the values
file passed to `open-actions install`. Resource lists accept Kubernetes resource
names, including extended resources. Resource quantities must be strings; quote
integer values such as `cpu: "2"`:

```yaml
controller:
  resources:
    requests:
      cpu: 100m
      memory: 128Mi
    limits:
      cpu: "2"
      memory: 512Mi
```

## Values

| Value | Default | Description |
| --- | --- | --- |
| `controller.image.repository` | `ghcr.io/kelos-dev/open-actions-controller` | Controller image repository |
| `controller.image.tag` | `latest` | Controller image tag |
| `controller.image.pullPolicy` | `IfNotPresent` | Kubernetes image pull policy |
| `controller.githubAPIURL` | `https://api.github.com/` | Base URL for the GitHub API |
| `controller.githubServerURL` | `https://github.com` | GitHub web-server URL exposed to workflows |
| `controller.actionCloneBaseURL` | `""` | Base URL used to clone external action repositories; defaults to `controller.githubServerURL` when empty |
| `controller.maxJobTimeout` | `6h` | Maximum execution timeout available to workflow jobs, expressed in whole hours and minutes such as `1h30m`; longer `timeout-minutes` values are capped |
| `controller.workflowRunTTLSecondsAfterFinished` | `null` | Default `spec.ttlSecondsAfterFinished` for generated WorkflowRuns; `null` retains them indefinitely |
| `controller.resources` | CPU `50m`/`1`, memory `64Mi`/`256Mi` | Kubernetes resource requests and limits for the controller container |
| `artifacts.enabled` | `true` | Deploy the internal artifact service and issue job-scoped runtime credentials |
| `artifacts.image.repository` | `ghcr.io/kelos-dev/open-actions-artifact-server` | Artifact service image repository |
| `artifacts.image.tag` | `latest` | Artifact service image tag |
| `artifacts.image.pullPolicy` | `IfNotPresent` | Artifact service image pull policy |
| `artifacts.signingKeySecretName` | `""` | Existing credential signing Secret; the chart creates `open-actions-artifact-auth` when empty |
| `artifacts.signingKeyKey` | `signing-key` | Secret key containing at least 32 bytes of signing material |
| `artifacts.defaultRetentionDays` | `7` | Retention used when an upload omits `retention-days` |
| `artifacts.maxRetentionDays` | `30` | Maximum accepted artifact retention and value exposed to upload actions |
| `artifacts.maxFileBytes` | `1073741824` | Maximum uncompressed size of one file |
| `artifacts.maxArtifactBytes` | `2147483648` | Maximum stored and total uncompressed size of one artifact |
| `artifacts.maxRunBytes` | `10737418240` | Maximum stored artifact bytes in one WorkflowRun attempt |
| `artifacts.resources` | CPU `50m`/`1`, memory `64Mi`/`256Mi` | Kubernetes resource requests and limits for the artifact server container |
| `artifacts.persistence.enabled` | `true` | Use StatefulSet-managed persistent storage instead of an ephemeral `emptyDir` |
| `artifacts.persistence.existingClaim` | `""` | Existing claim to mount instead of creating one from the StatefulSet volume claim template |
| `artifacts.persistence.storageClass` | `null` | Storage class for the StatefulSet-managed claim; `null` uses the cluster default |
| `artifacts.persistence.accessModes` | `[ReadWriteOnce]` | Access modes for the StatefulSet-managed claim |
| `artifacts.persistence.size` | `20Gi` | Requested capacity for the StatefulSet-managed claim |
| `console.enabled` | `true` | Deploy the Open Actions Console |
| `console.replicas` | `1` | Console replica count |
| `console.publicURL` | `http://localhost:8080` | Public Console URL used by GitHub Check Run links |
| `console.secretName` | `""` | Existing Console administrator Secret; the chart creates `open-actions-console-auth` when empty |
| `console.tokenKey` | `token` | Secret key containing the Console administrator token |
| `console.image.repository` | `ghcr.io/kelos-dev/open-actions-console` | Console image repository |
| `console.image.tag` | `latest` | Console image tag |
| `console.image.pullPolicy` | `IfNotPresent` | Console image pull policy |
| `console.resources` | CPU `50m`/`1`, memory `64Mi`/`256Mi` | Kubernetes resource requests and limits for the Console container |
| `console.service.type` | `ClusterIP` | Console Service type (`ClusterIP`, `NodePort`, or `LoadBalancer`) |
| `console.service.nodePort` | `null` | Fixed Console node port for a `NodePort` or `LoadBalancer` Service |
| `service.type` | `ClusterIP` | Webhook Service type (`ClusterIP`, `NodePort`, or `LoadBalancer`) |
| `service.nodePort` | `null` | Fixed webhook node port for a `NodePort` or `LoadBalancer` Service |
