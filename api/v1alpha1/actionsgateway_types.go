package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const ActionsGatewayConditionConfigured = "Configured"

const SourceTypeGitHub SourceType = "GitHub"

// SourceType identifies an external workflow source provider.
type SourceType string

// ActionsGatewaySpec describes a source gateway for Open Actions.
type ActionsGatewaySpec struct {
	// Source selects and configures the external workflow source.
	// +required
	Source ActionsGatewaySource `json:"source"`

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

// ActionsGatewaySource is a discriminated union of supported workflow sources.
// +kubebuilder:validation:XValidation:rule="self.type == 'GitHub' ? has(self.github) : !has(self.github)",message="github must be specified exactly when type is GitHub"
// +kubebuilder:validation:XValidation:rule="self.type == oldSelf.type",message="source type is immutable"
type ActionsGatewaySource struct {
	// Type selects the source configuration variant.
	// +kubebuilder:validation:Enum=GitHub
	// +required
	Type SourceType `json:"type"`

	// GitHub configures a GitHub App source. Referenced Secrets must be in the
	// ActionsGateway namespace.
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

	// InstallationID is the GitHub App installation served by this gateway.
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
}

// ActionsGatewayStatus contains observations made by the gateway controller.
type ActionsGatewayStatus struct {
	// ObservedGeneration is the most recent generation observed by the
	// controller.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Conditions describe whether the gateway's local configuration is valid.
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
// +kubebuilder:printcolumn:name="Configured",type=string,JSONPath=`.status.conditions[?(@.type=="Configured")].status`
// +kubebuilder:printcolumn:name="Installation",type=integer,JSONPath=`.spec.source.github.installationID`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// ActionsGateway configures GitHub webhook authentication and asynchronous
// workflow discovery.
type ActionsGateway struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// +required
	Spec ActionsGatewaySpec `json:"spec"`

	// +optional
	Status ActionsGatewayStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// ActionsGatewayList contains ActionsGateway objects.
type ActionsGatewayList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ActionsGateway `json:"items"`
}
