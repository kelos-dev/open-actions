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
// +kubebuilder:validation:XValidation:rule="!has(self.environments) || self.environments.all(e, self.environments.exists_one(other, e.name.lowerAscii() == other.name.lowerAscii()))",message="environment names must be unique ignoring ASCII case"
type ProjectSpec struct {
	// Source selects and configures the external workflow source.
	// +required
	Source ProjectSource `json:"source"`

	// WorkflowDirectory is the repository-relative directory containing workflow
	// files. The default keeps the files outside GitHub's native workflow path.
	// +kubebuilder:default=".open-actions/workflows"
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=256
	// +kubebuilder:validation:XValidation:rule="!self.startsWith('/') && !self.endsWith('/') && !self.contains('//') && self != '.' && !self.startsWith('./') && !self.contains('/./') && !self.endsWith('/.')",message="must be a repository-relative directory without empty or '.' path segments"
	// +kubebuilder:validation:XValidation:rule="self != '..' && !self.startsWith('../') && !self.contains('/../') && !self.endsWith('/..')",message="must not contain '..' path segments"
	// +optional
	WorkflowDirectory string `json:"workflowDirectory,omitempty"`

	// Environments defines the environment names workflows may select. Each
	// environment may expose one Secret and require approval before its jobs can
	// be assigned to a Runner.
	// +listType=map
	// +listMapKey=name
	// +kubebuilder:validation:MaxItems=100
	// +optional
	Environments []ProjectEnvironment `json:"environments,omitempty"`
}

// ProjectEnvironment configures one workflow environment. Environment names
// are matched without regard to ASCII case.
type ProjectEnvironment struct {
	// Name is the GitHub-compatible environment name selected by a workflow job.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=255
	// +kubebuilder:validation:Pattern=`^[^\x00-\x1f\x7f]+$`
	// +required
	Name string `json:"name"`

	// SecretRef identifies a Secret in the Project namespace whose data keys
	// populate the secrets expression context for jobs in this environment.
	// +optional
	SecretRef *EnvironmentSecretReference `json:"secretRef,omitempty"`

	// Protection configures the gate enforced before a job can be assigned.
	// +optional
	Protection *EnvironmentProtection `json:"protection,omitempty"`
}

// EnvironmentSecretReference identifies a Secret in the same namespace.
type EnvironmentSecretReference struct {
	// Name is the Secret resource name.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?([.][a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?)*$`
	// +required
	Name string `json:"name"`
}

// EnvironmentProtection configures the Open Actions environment gate.
type EnvironmentProtection struct {
	// RequiredApproval requires an authorized user to approve each WorkflowJob
	// before a Runner can claim it.
	// +optional
	RequiredApproval bool `json:"requiredApproval,omitempty"`
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
