package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	WorkflowJobConditionReady                 = "Ready"
	WorkflowJobConditionConcurrencyAcquired   = "ConcurrencyAcquired"
	WorkflowJobConditionScheduled             = "Scheduled"
	WorkflowJobConditionSucceeded             = "Succeeded"
	WorkflowJobConditionCancellationRequested = "CancellationRequested"

	WorkflowJobResultSuccess   WorkflowJobResult = "success"
	WorkflowJobResultFailure   WorkflowJobResult = "failure"
	WorkflowJobResultSkipped   WorkflowJobResult = "skipped"
	WorkflowJobResultCancelled WorkflowJobResult = "cancelled"
)

// WorkflowJobResult is the terminal result exposed to dependent workflow jobs.
type WorkflowJobResult string

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

	// Needs contains the workflow-local IDs of jobs that must reach a terminal
	// result before this job's condition can be evaluated.
	// +listType=set
	// +kubebuilder:validation:MaxItems=1000
	// +kubebuilder:validation:items:MinLength=1
	// +kubebuilder:validation:items:MaxLength=256
	// +kubebuilder:validation:items:Pattern=`^[A-Za-z_][A-Za-z0-9_-]*$`
	// +optional
	Needs []string `json:"needs,omitempty"`

	// If is the GitHub Actions condition that gates Runner assignment after all
	// jobs in Needs have reached terminal results. An omitted condition uses the
	// default success gate.
	// +kubebuilder:validation:MaxLength=65536
	// +optional
	If string `json:"if,omitempty"`

	// Concurrency delays Runner assignment until this job owns the evaluated
	// group. The group expression is evaluated after Needs reaches terminal
	// results and matrix values are available.
	// +optional
	Concurrency *WorkflowJobConcurrency `json:"concurrency,omitempty"`

	// Matrix describes the matrix combination represented by this job.
	// +optional
	Matrix *WorkflowJobMatrix `json:"matrix,omitempty"`

	// TimeoutSeconds is the effective execution timeout after applying the
	// controller maximum. Cleanup may continue during the Pod termination grace
	// period after this timeout expires. Omit it to apply the default 360-minute
	// timeout, capped by the controller maximum.
	// +kubebuilder:validation:Minimum=60
	// The maximum is 9223372036 seconds, the largest timeout the controller can
	// represent.
	// +kubebuilder:validation:Maximum=9223372036
	// +optional
	TimeoutSeconds int64 `json:"timeoutSeconds,omitempty"`
}

// WorkflowJobConcurrency is the concurrency policy copied from the workflow.
type WorkflowJobConcurrency struct {
	// Group is a literal or expression template evaluated by the WorkflowRun
	// controller. Group names are case-insensitive.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=256
	// +required
	Group string `json:"group"`

	// CancelInProgress is evaluated after dependencies reach terminal results
	// and matrix values are available. Omit it to leave the executing member
	// running when this job becomes pending.
	// +optional
	CancelInProgress *WorkflowJobConcurrencyCancellation `json:"cancelInProgress,omitempty"`
}

// WorkflowJobConcurrencyCancellation preserves a cancellation literal or
// expression until job concurrency is evaluated.
// +kubebuilder:validation:XValidation:rule="has(self.value) != has(self.expression)",message="exactly one of value or expression must be specified"
type WorkflowJobConcurrencyCancellation struct {
	// Value is a literal cancellation decision.
	// +optional
	Value *bool `json:"value,omitempty"`

	// Expression is a whole expression that evaluates to a Boolean.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=65536
	// +optional
	Expression string `json:"expression,omitempty"`
}

// WorkflowJobMatrix identifies one expanded combination of a logical workflow
// job. The controller writes the same strategy settings to every combination
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

	// JobIndex is the zero-based index exposed through strategy.job-index.
	// +kubebuilder:validation:Minimum=0
	// +optional
	JobIndex int32 `json:"jobIndex,omitempty"`

	// JobTotal is the number of combinations exposed through strategy.job-total.
	// +kubebuilder:validation:Minimum=1
	// +optional
	JobTotal int32 `json:"jobTotal,omitempty"`

	// MaxParallel limits concurrently scheduled combinations in this logical
	// job. Omit MaxParallel to apply no matrix-specific limit.
	// +kubebuilder:validation:Minimum=1
	// +optional
	MaxParallel int32 `json:"maxParallel,omitempty"`

	// FailFast requests cancellation of unfinished combinations in this logical
	// job after one combination fails. It defaults to true.
	// +kubebuilder:default=true
	// +optional
	FailFast *bool `json:"failFast,omitempty"`
}

// WorkflowJobStatus contains observations made while executing a workflow job.
// +kubebuilder:validation:XValidation:rule="!has(oldSelf.runnerRef) || self.runnerRef == oldSelf.runnerRef",message="runnerRef is immutable after assignment"
// +kubebuilder:validation:XValidation:rule="!has(oldSelf.result) || self.result == oldSelf.result",message="result is immutable after completion"
// +kubebuilder:validation:XValidation:rule="!has(self.runnerRef) || (size(self.runnerRef.name) > 0 && size(self.runnerRef.name) <= 253 && self.runnerRef.name.matches('^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?([.][a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?)*$'))",message="`runnerRef.name` must be a DNS subdomain"
type WorkflowJobStatus struct {
	// ObservedGeneration is the most recent generation observed by the
	// controller.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Result is the immutable terminal result exposed through the needs context.
	// +kubebuilder:validation:Enum=success;failure;skipped;cancelled
	// +optional
	Result WorkflowJobResult `json:"result,omitempty"`

	// RunnerRef identifies the Runner assigned by the scheduler in the same
	// namespace. It is set once when the job is scheduled.
	// +optional
	RunnerRef *corev1.LocalObjectReference `json:"runnerRef,omitempty"`

	// StartTime is when the runner container started.
	// +optional
	StartTime *metav1.Time `json:"startTime,omitempty"`

	// CompletionTime is when execution reached a terminal result.
	// +optional
	CompletionTime *metav1.Time `json:"completionTime,omitempty"`

	// Outputs contains the non-secret outputs declared by the workflow job.
	// The controller records them only after execution reaches a terminal result.
	// +kubebuilder:validation:MaxProperties=100
	// +kubebuilder:validation:XValidation:rule="self.all(k, size(k) <= 256 && k.matches('^[A-Za-z_][A-Za-z0-9_-]*$'))",message="output names must be workflow identifiers no more than 256 characters"
	// +kubebuilder:validation:XValidation:rule="self.all(k, size(self[k]) <= 4096)",message="output values must be no more than 4096 characters"
	// +optional
	Outputs map[string]string `json:"outputs,omitempty"`

	// Concurrency records the evaluated job concurrency policy. It is set before
	// the job enters the concurrency gate and remains stable for the job's
	// lifetime.
	// +optional
	Concurrency *ConcurrencyStatus `json:"concurrency,omitempty"`

	// Conditions describe dependency readiness, concurrency acquisition, Runner
	// assignment, cancellation, and the terminal result. When present, Ready is
	// true when a Runner may claim the job. Dependency-free unconditional jobs
	// without concurrency are also claimable when Ready is absent. Scheduled is
	// true after the scheduler assigns status.runnerRef. Known condition types are
	// Ready, ConcurrencyAcquired, Scheduled, Succeeded, and CancellationRequested.
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
