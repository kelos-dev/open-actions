package v1alpha1

// ConcurrencyStatus records an evaluated concurrency policy.
type ConcurrencyStatus struct {
	// Group is the evaluated, case-insensitive concurrency key.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=256
	// +required
	Group string `json:"group"`

	// CancelInProgress is the evaluated cancellation decision.
	// +optional
	CancelInProgress bool `json:"cancelInProgress,omitempty"`
}
