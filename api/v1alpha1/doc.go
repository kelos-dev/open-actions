// Package v1alpha1 defines the v1alpha1 Kubernetes API for Open Actions. Its Go
// types and kubebuilder markers are the source of truth for the generated CRDs.
// Users own Project, Runner, and RunnerSet resources. The RunnerSet controller
// creates managed Runners, the webhook delivery controller creates WorkflowRuns,
// the WorkflowRun controller creates WorkflowJobs, and each resource controller
// is the sole writer of that resource's status.
// +kubebuilder:object:generate=true
// +groupName=actions.kelos.dev
package v1alpha1
