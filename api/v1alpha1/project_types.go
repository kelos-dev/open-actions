package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const ProjectConditionConfigured = "Configured"

const SourceTypeGitHub SourceType = "GitHub"

// SourceType identifies an external workflow source provider.
type SourceType string

// ProjectSpec describes the workflow source for an Open Actions Project.
type ProjectSpec struct {
	// Source selects and configures the external workflow source.
	// +required
	Source ProjectSource `json:"source"`

	// Secrets selects the Secret whose data entries become available to
	// workflows through the secrets context. The Secret must be in the Project
	// namespace, and its keys use GitHub's canonical uppercase representation.
	// +optional
	Secrets *ProjectSecretSource `json:"secrets,omitempty"`

	// Variables selects the ConfigMap whose data entries become available to
	// workflows through the vars context. The ConfigMap must be in the Project
	// namespace, and its keys use GitHub's canonical uppercase representation.
	// +optional
	Variables *ProjectVariableSource `json:"variables,omitempty"`

	// WorkflowDirectory is the repository-relative directory containing workflow
	// files. The default keeps the files outside GitHub's native workflow path.
	// +kubebuilder:default=".open-actions/workflows"
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=256
	// +kubebuilder:validation:XValidation:rule="!self.startsWith('/') && !self.endsWith('/') && !self.contains('//') && self != '.' && !self.startsWith('./') && !self.contains('/./') && !self.endsWith('/.')",message="must be a repository-relative directory without empty or '.' path segments"
	// +kubebuilder:validation:XValidation:rule="self != '..' && !self.startsWith('../') && !self.contains('/../') && !self.endsWith('/..')",message="must not contain '..' path segments"
	// +optional
	WorkflowDirectory string `json:"workflowDirectory,omitempty"`
}

// +kubebuilder:validation:XValidation:rule="size(self.secretRef.name) > 0",message="`secretRef.name` must be specified"
// +kubebuilder:validation:XValidation:rule="size(self.secretRef.name) <= 253",message="`secretRef.name` must be no more than 253 characters"
// +kubebuilder:validation:XValidation:rule="self.secretRef.name.matches('^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?([.][a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?)*$')",message="`secretRef.name` must be a DNS subdomain"
// ProjectSecretSource selects one Kubernetes Secret.
type ProjectSecretSource struct {
	// SecretRef identifies the Secret in the Project namespace.
	// +required
	SecretRef corev1.LocalObjectReference `json:"secretRef"`
}

// +kubebuilder:validation:XValidation:rule="size(self.configMapRef.name) > 0",message="`configMapRef.name` must be specified"
// +kubebuilder:validation:XValidation:rule="size(self.configMapRef.name) <= 253",message="`configMapRef.name` must be no more than 253 characters"
// +kubebuilder:validation:XValidation:rule="self.configMapRef.name.matches('^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?([.][a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?)*$')",message="`configMapRef.name` must be a DNS subdomain"
// ProjectVariableSource selects one Kubernetes ConfigMap.
type ProjectVariableSource struct {
	// ConfigMapRef identifies the ConfigMap in the Project namespace.
	// +required
	ConfigMapRef corev1.LocalObjectReference `json:"configMapRef"`
}

// ProjectSource is a discriminated union of supported workflow sources.
// +kubebuilder:validation:XValidation:rule="self.type == 'GitHub' ? has(self.github) : !has(self.github)",message="github must be specified exactly when type is GitHub"
// +kubebuilder:validation:XValidation:rule="self.type == oldSelf.type",message="source type is immutable"
type ProjectSource struct {
	// Type selects the source configuration variant.
	// +kubebuilder:validation:Enum=GitHub
	// +required
	Type SourceType `json:"type"`

	// GitHub configures a GitHub App source. Referenced Secrets must be in the
	// Project namespace.
	// +optional
	GitHub *GitHubAppConfiguration `json:"github,omitempty"`
}

// GitHubAppConfiguration identifies one GitHub App installation.
// +kubebuilder:validation:XValidation:rule="self.appID == oldSelf.appID",message="`appID` is immutable"
// +kubebuilder:validation:XValidation:rule="self.installationID == oldSelf.installationID",message="`installationID` is immutable"
// +kubebuilder:validation:XValidation:rule="size(self.privateKeySecretRef.name) > 0",message="`privateKeySecretRef.name` must be specified"
// +kubebuilder:validation:XValidation:rule="size(self.privateKeySecretRef.name) <= 253",message="`privateKeySecretRef.name` must be no more than 253 characters"
// +kubebuilder:validation:XValidation:rule="self.privateKeySecretRef.name.matches('^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?([.][a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?)*$')",message="`privateKeySecretRef.name` must be a DNS subdomain"
// +kubebuilder:validation:XValidation:rule="size(self.privateKeySecretRef.key) > 0",message="`privateKeySecretRef.key` must be specified"
// +kubebuilder:validation:XValidation:rule="size(self.privateKeySecretRef.key) <= 253",message="`privateKeySecretRef.key` must be no more than 253 characters"
// +kubebuilder:validation:XValidation:rule="self.privateKeySecretRef.key.matches('^[-._A-Za-z0-9]+$')",message="`privateKeySecretRef.key` must be a valid Secret data key"
// +kubebuilder:validation:XValidation:rule="!has(self.privateKeySecretRef.optional) || !self.privateKeySecretRef.optional",message="`privateKeySecretRef.optional` may not be true"
// +kubebuilder:validation:XValidation:rule="size(self.webhookSecretRef.name) > 0",message="`webhookSecretRef.name` must be specified"
// +kubebuilder:validation:XValidation:rule="size(self.webhookSecretRef.name) <= 253",message="`webhookSecretRef.name` must be no more than 253 characters"
// +kubebuilder:validation:XValidation:rule="self.webhookSecretRef.name.matches('^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?([.][a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?)*$')",message="`webhookSecretRef.name` must be a DNS subdomain"
// +kubebuilder:validation:XValidation:rule="size(self.webhookSecretRef.key) > 0",message="`webhookSecretRef.key` must be specified"
// +kubebuilder:validation:XValidation:rule="size(self.webhookSecretRef.key) <= 253",message="`webhookSecretRef.key` must be no more than 253 characters"
// +kubebuilder:validation:XValidation:rule="self.webhookSecretRef.key.matches('^[-._A-Za-z0-9]+$')",message="`webhookSecretRef.key` must be a valid Secret data key"
// +kubebuilder:validation:XValidation:rule="!has(self.webhookSecretRef.optional) || !self.webhookSecretRef.optional",message="`webhookSecretRef.optional` may not be true"
type GitHubAppConfiguration struct {
	// AppID is the numeric identifier of the GitHub App.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=9007199254740991
	// +required
	AppID int64 `json:"appID"`

	// InstallationID is the GitHub App installation associated with this Project.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=9007199254740991
	// +required
	InstallationID int64 `json:"installationID"`

	// PrivateKeySecretRef selects the PEM-encoded GitHub App private key.
	// +required
	PrivateKeySecretRef corev1.SecretKeySelector `json:"privateKeySecretRef"`

	// WebhookSecretRef selects the secret used to authenticate GitHub webhook
	// deliveries.
	// +required
	WebhookSecretRef corev1.SecretKeySelector `json:"webhookSecretRef"`

	// ForkPullRequests controls ordinary pull_request workflows whose code comes
	// from a fork or Dependabot. Omit it to enable workflows with approval
	// required, read-only token permissions, and Project secrets withheld.
	// +optional
	ForkPullRequests *GitHubForkPullRequestPolicy `json:"forkPullRequests,omitempty"`
}

// GitHubForkPullRequestPolicy controls execution of untrusted ordinary pull
// request workflows. These settings do not apply to pull_request_target.
type GitHubForkPullRequestPolicy struct {
	// Enabled controls whether ordinary fork pull request workflows are created.
	// +kubebuilder:default=true
	// +optional
	Enabled *bool `json:"enabled,omitempty"`

	// RequireApproval prevents job creation until an administrator approves the
	// WorkflowRun for its pinned pull request head revision.
	// +kubebuilder:default=true
	// +optional
	RequireApproval *bool `json:"requireApproval,omitempty"`

	// SendWriteTokens preserves write permissions requested by the workflow.
	// When false, requested write permissions are reduced to read.
	// +optional
	SendWriteTokens bool `json:"sendWriteTokens,omitempty"`

	// SendSecrets makes the Project Secret available to fork pull request jobs.
	// +optional
	SendSecrets bool `json:"sendSecrets,omitempty"`
}

// ProjectStatus contains observations made by the project controller.
type ProjectStatus struct {
	// ObservedGeneration is the most recent generation observed by the
	// controller.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Conditions describe whether the project's local configuration is valid.
	// Configured does not assert that the GitHub App or installation is remotely
	// reachable. The known condition type is Configured.
	// +listType=map
	// +listMapKey=type
	// +kubebuilder:validation:MaxItems=16
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:metadata:annotations=helm.sh/resource-policy=keep
// +kubebuilder:printcolumn:name="Configured",type=string,JSONPath=`.status.conditions[?(@.type=="Configured")].status`
// +kubebuilder:printcolumn:name="Installation",type=integer,JSONPath=`.spec.source.github.installationID`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// Project defines an isolated Open Actions execution domain backed by an
// external workflow source.
type Project struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// +required
	Spec ProjectSpec `json:"spec"`

	// +optional
	Status ProjectStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// ProjectList contains Project objects.
type ProjectList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Project `json:"items"`
}
