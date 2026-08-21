package v1alpha1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

const RunnerSetConditionReady = "Ready"

// RunnerSetSpec describes a homogeneous set of Runner execution slots.
type RunnerSetSpec struct {
	// Replicas is the desired number of Runner execution slots. It defaults to
	// one and may be zero to drain all managed Runners.
	// +kubebuilder:default=1
	// +kubebuilder:validation:Minimum=0
	// +optional
	Replicas *int32 `json:"replicas,omitempty"`

	// Template describes the Runners managed by this RunnerSet.
	// +required
	Template RunnerTemplateSpec `json:"template"`
}

// RunnerTemplateSpec describes the desired state of each managed Runner.
type RunnerTemplateSpec struct {
	// Spec is applied to each managed Runner.
	// +required
	Spec RunnerSpec `json:"spec"`
}

// RunnerSetStatus contains observations about the managed Runners.
type RunnerSetStatus struct {
	// ObservedGeneration is the most recent generation observed by the
	// controller.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Replicas is the number of non-terminating managed Runners.
	// +optional
	Replicas int32 `json:"replicas,omitempty"`

	// ReadyReplicas is the number of non-terminating managed Runners whose Ready
	// condition is true for their current generation.
	// +optional
	ReadyReplicas int32 `json:"readyReplicas,omitempty"`

	// BusyReplicas is the number of non-terminating managed Runners assigned a
	// WorkflowJob.
	// +optional
	BusyReplicas int32 `json:"busyReplicas,omitempty"`

	// IdleReplicas is the number of ready, non-terminating managed Runners that
	// are not assigned a WorkflowJob.
	// +optional
	IdleReplicas int32 `json:"idleReplicas,omitempty"`

	// TerminatingReplicas is the number of managed Runners draining before
	// deletion.
	// +optional
	TerminatingReplicas int32 `json:"terminatingReplicas,omitempty"`

	// Conditions describe whether the RunnerSet has reached its desired size and
	// whether its managed Runners are operational. The known condition type is
	// Ready.
	// +listType=map
	// +listMapKey=type
	// +kubebuilder:validation:MaxItems=16
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:subresource:scale:specpath=.spec.replicas,statuspath=.status.replicas
// +kubebuilder:metadata:annotations=helm.sh/resource-policy=keep
// +kubebuilder:printcolumn:name="Desired",type=integer,JSONPath=`.spec.replicas`
// +kubebuilder:printcolumn:name="Current",type=integer,JSONPath=`.status.replicas`
// +kubebuilder:printcolumn:name="Ready",type=integer,JSONPath=`.status.readyReplicas`
// +kubebuilder:printcolumn:name="Busy",type=integer,JSONPath=`.status.busyReplicas`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// RunnerSet maintains a desired number of homogeneous Runner execution slots.
type RunnerSet struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// +required
	Spec RunnerSetSpec `json:"spec"`

	// +optional
	Status RunnerSetStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// RunnerSetList contains RunnerSet objects.
type RunnerSetList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []RunnerSet `json:"items"`
}
