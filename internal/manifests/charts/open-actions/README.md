# Open Actions Helm Chart

Install or upgrade Open Actions with custom values:

```console
open-actions install --values values.yaml
```

The chart installs the controller, Console, and the Open Actions CRDs.
CRDs carry the `helm.sh/resource-policy: keep` annotation so uninstalling the
release does not delete Open Actions resources.

The default Console URL supports access through `kubectl port-forward
service/open-actions-console 8080:80 --namespace open-actions-system`. Set
`console.publicURL` to the public HTTPS origin when exposing the Service. The
chart creates and preserves an administrator token in the
`open-actions-console-auth` Secret. Retrieve the `token` key and enter it on the
Console login page. This token grants read access to every run, job, and runner
Pod log visible to the Console, so protect it like any other administrator
credential. The Console uses its own Deployment and ServiceAccount with
read-only access to those resources. Set `console.enabled=false` to omit the
Console. Set `console.secretName` to mount an externally managed Secret from the
release namespace instead; `console.tokenKey` selects its token key.

Generated WorkflowRuns omit `spec.ttlSecondsAfterFinished` by default and are
retained indefinitely. Set `controller.workflowRunTTLSecondsAfterFinished` to
populate that field on new runs. For example, `604800` retains each new run for
seven days after completion; `0` makes a run eligible for deletion immediately
after completion. Changing the chart value does not alter existing runs.
Deleting an expired WorkflowRun also deletes its owned WorkflowJobs and execution
resources.

## Values

| Value | Default | Description |
| --- | --- | --- |
| `controller.image.repository` | `ghcr.io/kelos-dev/open-actions-controller` | Controller image repository |
| `controller.image.tag` | `latest` | Controller image tag |
| `controller.image.pullPolicy` | `IfNotPresent` | Kubernetes image pull policy |
| `controller.githubAPIURL` | `https://api.github.com/` | Base URL for the GitHub API |
| `controller.githubServerURL` | `https://github.com` | GitHub web-server URL exposed to workflows |
| `controller.actionCloneBaseURL` | `https://github.com` | Base URL used to clone external action repositories |
| `controller.workflowRunTTLSecondsAfterFinished` | `null` | Default `spec.ttlSecondsAfterFinished` for generated WorkflowRuns; `null` retains them indefinitely |
| `console.enabled` | `true` | Deploy the Open Actions Console |
| `console.replicas` | `1` | Console replica count |
| `console.publicURL` | `http://localhost:8080` | Public Console URL used by GitHub Check Run links |
| `console.secretName` | `""` | Existing Console authentication Secret; the chart creates `open-actions-console-auth` when empty |
| `console.tokenKey` | `token` | Secret key containing the Console administrator token |
| `console.image.repository` | `ghcr.io/kelos-dev/open-actions-console` | Console image repository |
| `console.image.tag` | `latest` | Console image tag |
| `console.image.pullPolicy` | `IfNotPresent` | Console image pull policy |
| `console.service.type` | `ClusterIP` | Console Service type (`ClusterIP`, `NodePort`, or `LoadBalancer`) |
| `console.service.nodePort` | `null` | Fixed Console node port for a `NodePort` or `LoadBalancer` Service |
| `service.type` | `ClusterIP` | Webhook Service type (`ClusterIP`, `NodePort`, or `LoadBalancer`) |
| `service.nodePort` | `null` | Fixed webhook node port for a `NodePort` or `LoadBalancer` Service |
