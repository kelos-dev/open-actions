package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	RunnerConditionReady = "Ready"
	RunnerConditionBusy  = "Busy"
)

// RunnerSpec describes one execution agent that accepts one matching
// WorkflowJob at a time.
// +kubebuilder:validation:XValidation:rule="self.gatewayRef == oldSelf.gatewayRef",message="gatewayRef is immutable"
// +kubebuilder:validation:XValidation:rule="size(self.gatewayRef.name) > 0",message="`gatewayRef.name` must be specified"
// +kubebuilder:validation:XValidation:rule="size(self.gatewayRef.name) <= 253",message="`gatewayRef.name` must be no more than 253 characters"
// +kubebuilder:validation:XValidation:rule="self.gatewayRef.name.matches('^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?([.][a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?)*$')",message="`gatewayRef.name` must be a DNS subdomain"
type RunnerSpec struct {
	// GatewayRef identifies the ActionsGateway whose WorkflowJobs this Runner
	// accepts. The gateway must be in the same namespace.
	// +required
	GatewayRef corev1.LocalObjectReference `json:"gatewayRef"`

	// Execution describes the supported configuration of the environment used
	// to execute assigned WorkflowJobs. The controller owns the underlying Pod
	// and Job configuration. Changes apply only to Jobs created afterward.
	// +required
	Execution RunnerExecutionSpec `json:"execution"`

	// Labels identify the jobs this Runner can accept. Labels use lowercase ASCII
	// letters, digits, '-', '_' and '.', and start with a letter or digit. A
	// WorkflowJob matches only when every requested runs-on label is present.
	// +listType=set
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=64
	// +kubebuilder:validation:items:MinLength=1
	// +kubebuilder:validation:items:MaxLength=128
	// +kubebuilder:validation:items:Pattern=`^[a-z0-9][a-z0-9._-]*$`
	// +required
	Labels []string `json:"labels"`
}

// RunnerExecutionSpec describes the user-configurable execution environment
// for a Runner.
type RunnerExecutionSpec struct {
	// Image is a container image whose entrypoint launches the Open Actions
	// runner.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=2048
	// +kubebuilder:validation:Pattern=`^[^[:space:]]+$`
	// +required
	Image string `json:"image"`

	// Resources describes the compute resources available to the runner.
	// +optional
	Resources *RunnerResources `json:"resources,omitempty"`
}

// RunnerResources describes compute resource requests and limits for runner
// execution.
// +kubebuilder:validation:XValidation:rule="!has(self.requests) || !has(self.limits) || self.requests.all(k, !(k in self.limits) || !quantity(string(self.requests[k])).isGreaterThan(quantity(string(self.limits[k]))))",message="resource requests must not exceed limits"
// +kubebuilder:validation:XValidation:rule="!has(self.requests) || self.requests.all(k, k == 'cpu' || k == 'memory' || k == 'ephemeral-storage' || (has(self.limits) && k in self.limits && quantity(string(self.requests[k])).compareTo(quantity(string(self.limits[k]))) == 0))",message="extended and huge-page resource requests require an equal limit"
type RunnerResources struct {
	// Limits describes the maximum compute resources available to the runner.
	// +optional
	Limits RunnerResourceList `json:"limits,omitempty"`

	// Requests describes the minimum compute resources reserved for the runner.
	// +optional
	Requests RunnerResourceList `json:"requests,omitempty"`
}

// RunnerResourceList maps up to seven resource names to quantities. The bound
// keeps cross-map quantity validation within the Kubernetes CEL cost budget.
// +kubebuilder:validation:MaxProperties=7
// +kubebuilder:validation:XValidation:rule="self.all(k, size(k.split('/')) <= 2 && size(k.split('/')[size(k.split('/')) - 1]) <= 63 && k.split('/')[size(k.split('/')) - 1].matches('^[A-Za-z0-9]([-A-Za-z0-9_.]*[A-Za-z0-9])?$') && (size(k.split('/')) == 1 || (size(k.split('/')[0]) <= 253 && k.split('/')[0].split('.').all(p, size(p) <= 63 && p.matches('^[a-z0-9]([-a-z0-9]*[a-z0-9])?$')))))",message="resource names must be qualified names"
// +kubebuilder:validation:XValidation:rule="self.all(k, k == 'cpu' || k == 'memory' || k == 'ephemeral-storage' || k.startsWith('hugepages-') || k.contains('/'))",message="unprefixed resource names must be standard container resources"
// +kubebuilder:validation:XValidation:rule="self.all(k, sign(quantity(string(self[k]))) >= 0)",message="resource quantities must not be negative"
// +kubebuilder:validation:XValidation:rule="self.all(k, !k.contains('/') || quantity(string(self[k])).isInteger())",message="extended resource quantities must be whole numbers"
type RunnerResourceList map[corev1.ResourceName]resource.Quantity

// RunnerStatus contains observations about one execution agent.
// +kubebuilder:validation:XValidation:rule="!has(self.workflowJobRef) || (size(self.workflowJobRef.name) > 0 && size(self.workflowJobRef.name) <= 253 && self.workflowJobRef.name.matches('^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?([.][a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?)*$'))",message="`workflowJobRef.name` must be a DNS subdomain"
type RunnerStatus struct {
	// ObservedGeneration is the most recent generation observed by the
	// controller.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// WorkflowJobRef identifies the currently assigned WorkflowJob in the same
	// namespace. It is unset while the Runner is idle.
	// +optional
	WorkflowJobRef *corev1.LocalObjectReference `json:"workflowJobRef,omitempty"`

	// Conditions describe whether the Runner is operational and whether it has
	// an assignment. Ready reports operational health independently of capacity;
	// Busy is the capacity signal. Known condition types are Ready and Busy.
	// +listType=map
	// +listMapKey=type
	// +kubebuilder:validation:MaxItems=16
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="Busy",type=string,JSONPath=`.status.conditions[?(@.type=="Busy")].status`
// +kubebuilder:printcolumn:name="WorkflowJob",type=string,JSONPath=`.status.workflowJobRef.name`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// Runner represents one execution agent that accepts one matching WorkflowJob
// at a time.
type Runner struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// +required
	Spec RunnerSpec `json:"spec"`

	// +optional
	Status RunnerStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// RunnerList contains Runner objects.
type RunnerList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Runner `json:"items"`
}
