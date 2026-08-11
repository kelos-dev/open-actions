package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	WorkflowJobConditionScheduled = "Scheduled"
	WorkflowJobConditionSucceeded = "Succeeded"
)

// WorkflowJobSpec describes one immutable job expanded from a WorkflowRun.
// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="spec is immutable"
// +kubebuilder:validation:XValidation:rule="size(self.workflowRunRef.name) > 0",message="`workflowRunRef.name` must be specified"
// +kubebuilder:validation:XValidation:rule="size(self.workflowRunRef.name) <= 253",message="`workflowRunRef.name` must be no more than 253 characters"
// +kubebuilder:validation:XValidation:rule="self.workflowRunRef.name.matches('^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?([.][a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?)*$')",message="`workflowRunRef.name` must be a DNS subdomain"
type WorkflowJobSpec struct {
	// WorkflowRunRef identifies the owning WorkflowRun in the same namespace.
	// +required
	WorkflowRunRef corev1.LocalObjectReference `json:"workflowRunRef"`

	// JobID is the stable workflow-local identifier of this expanded job. For a
	// matrix job, each combination has a distinct JobID and Matrix.LogicalJobID
	// identifies the source workflow job.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=256
	// +kubebuilder:validation:Pattern=`^[A-Za-z_][A-Za-z0-9_-]*$`
	// +required
	JobID string `json:"jobID"`

	// DisplayName is the user-facing name of the workflow job. The controller
	// uses JobID when the workflow does not specify a name.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=256
	// +optional
	DisplayName string `json:"displayName,omitempty"`

	// RunsOn contains the canonical lowercase labels requested by the workflow
	// job. Labels use lowercase ASCII letters, digits, '-', '_' and '.', and
	// start with a letter or digit. A Runner must contain every label to accept
	// this job.
	// +listType=set
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=16
	// +kubebuilder:validation:items:MinLength=1
	// +kubebuilder:validation:items:MaxLength=128
	// +kubebuilder:validation:items:Pattern=`^[a-z0-9][a-z0-9._-]*$`
	// +required
	RunsOn []string `json:"runsOn"`

	// Matrix describes the matrix combination represented by this job.
	// +optional
	Matrix *WorkflowJobMatrix `json:"matrix,omitempty"`
}

// WorkflowJobMatrix identifies one expanded combination of a logical workflow
// job. The controller writes the same MaxParallel value to every combination
// of a logical job.
type WorkflowJobMatrix struct {
	// LogicalJobID is the workflow-local identifier before matrix expansion.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=256
	// +kubebuilder:validation:Pattern=`^[A-Za-z_][A-Za-z0-9_-]*$`
	// +required
	LogicalJobID string `json:"logicalJobID"`

	// Values contains the scalar matrix axis values for this combination.
	// Values are represented using their workflow string form.
	// +kubebuilder:validation:MinProperties=1
	// +kubebuilder:validation:MaxProperties=100
	// +kubebuilder:validation:XValidation:rule="self.all(k, size(k) > 0 && size(k) <= 256 && size(self[k]) <= 1024)",message="matrix keys must contain 1 to 256 characters and values must contain at most 1024 characters"
	// +required
	Values map[string]string `json:"values"`

	// MaxParallel limits concurrently scheduled combinations in this logical
	// job. Omit MaxParallel to apply no matrix-specific limit.
	// +kubebuilder:validation:Minimum=1
	// +optional
	MaxParallel int32 `json:"maxParallel,omitempty"`
}

// WorkflowJobStatus contains observations made while executing a workflow job.
// +kubebuilder:validation:XValidation:rule="!has(oldSelf.runnerRef) || self.runnerRef == oldSelf.runnerRef",message="runnerRef is immutable after assignment"
// +kubebuilder:validation:XValidation:rule="!has(self.runnerRef) || (size(self.runnerRef.name) > 0 && size(self.runnerRef.name) <= 253 && self.runnerRef.name.matches('^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?([.][a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?)*$'))",message="`runnerRef.name` must be a DNS subdomain"
type WorkflowJobStatus struct {
	// ObservedGeneration is the most recent generation observed by the
	// controller.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// RunnerRef identifies the Runner assigned by the scheduler in the same
	// namespace. It is set once when the job is scheduled.
	// +optional
	RunnerRef *corev1.LocalObjectReference `json:"runnerRef,omitempty"`

	// StartTime is when the native Job started.
	// +optional
	StartTime *metav1.Time `json:"startTime,omitempty"`

	// CompletionTime is when execution reached a terminal result.
	// +optional
	CompletionTime *metav1.Time `json:"completionTime,omitempty"`

	// Conditions describe Runner assignment and the terminal result. Scheduled
	// is true after the scheduler assigns status.runnerRef. Known condition types
	// are Scheduled and Succeeded.
	// +listType=map
	// +listMapKey=type
	// +kubebuilder:validation:MaxItems=16
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:metadata:annotations=helm.sh/resource-policy=keep
// +kubebuilder:printcolumn:name="Job",type=string,JSONPath=`.spec.displayName`
// +kubebuilder:printcolumn:name="WorkflowRun",type=string,JSONPath=`.spec.workflowRunRef.name`
// +kubebuilder:printcolumn:name="Runner",type=string,JSONPath=`.status.runnerRef.name`
// +kubebuilder:printcolumn:name="Scheduled",type=string,JSONPath=`.status.conditions[?(@.type=="Scheduled")].status`
// +kubebuilder:printcolumn:name="Succeeded",type=string,JSONPath=`.status.conditions[?(@.type=="Succeeded")].status`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
// +kubebuilder:printcolumn:name="JobID",type=string,JSONPath=`.spec.jobID`,priority=1

// WorkflowJob represents one schedulable job expanded from a WorkflowRun.
type WorkflowJob struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// +required
	Spec WorkflowJobSpec `json:"spec"`

	// +optional
	Status WorkflowJobStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// WorkflowJobList contains WorkflowJob objects.
type WorkflowJobList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []WorkflowJob `json:"items"`
}
