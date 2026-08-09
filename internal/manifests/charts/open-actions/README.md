# Open Actions Helm Chart

Install or upgrade Open Actions with custom values:

```console
open-actions install --values values.yaml
```

The chart installs the controller and the Open Actions CRDs. CRDs carry the
`helm.sh/resource-policy: keep` annotation so uninstalling the release does not
delete Open Actions resources.

## Values

| Value | Default | Description |
| --- | --- | --- |
| `controller.image.repository` | `ghcr.io/kelos-dev/open-actions-controller` | Controller image repository |
| `controller.image.tag` | `latest` | Controller image tag |
| `controller.image.pullPolicy` | `IfNotPresent` | Kubernetes image pull policy |
| `controller.githubAPIURL` | `https://api.github.com/` | Base URL for the GitHub API |
| `controller.githubServerURL` | `https://github.com` | GitHub web-server URL exposed to workflows |
| `controller.actionCloneBaseURL` | `https://github.com` | Base URL used to clone external action repositories |
| `service.type` | `ClusterIP` | Webhook Service type (`ClusterIP`, `NodePort`, or `LoadBalancer`) |
| `service.nodePort` | `null` | Fixed webhook node port for a `NodePort` or `LoadBalancer` Service |
