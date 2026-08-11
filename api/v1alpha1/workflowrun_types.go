package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	WorkflowRunConditionPlanned   = "Planned"
	WorkflowRunConditionSucceeded = "Succeeded"

	GitHubEventNamePush        GitHubEventName = "push"
	GitHubEventNamePullRequest GitHubEventName = "pull_request"
	GitHubEventNameMergeGroup  GitHubEventName = "merge_group"
)

// GitHubEventName identifies a supported GitHub webhook event.
type GitHubEventName string

// WorkflowRunSpec describes one workflow execution. ProjectRef, Source, and
// WorkflowPath are immutable.
// Deleting a WorkflowRun requests cancellation of its child resources.
// +kubebuilder:validation:XValidation:rule="self.projectRef == oldSelf.projectRef && self.source == oldSelf.source && self.workflowPath == oldSelf.workflowPath",message="projectRef, source, and workflowPath are immutable"
// +kubebuilder:validation:XValidation:rule="size(self.projectRef.name) > 0",message="`projectRef.name` must be specified"
// +kubebuilder:validation:XValidation:rule="size(self.projectRef.name) <= 253",message="`projectRef.name` must be no more than 253 characters"
// +kubebuilder:validation:XValidation:rule="self.projectRef.name.matches('^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?([.][a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?)*$')",message="`projectRef.name` must be a DNS subdomain"
type WorkflowRunSpec struct {
	// ProjectRef identifies a Project in the same namespace.
	// +required
	ProjectRef corev1.LocalObjectReference `json:"projectRef"`

	// Source identifies the provider-specific event and repository revision.
	// +required
	Source WorkflowRunSource `json:"source"`

	// WorkflowPath is the repository-relative path of the selected workflow.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=512
	// +kubebuilder:validation:XValidation:rule="self.endsWith('.yaml') || self.endsWith('.yml')",message="must end in .yaml or .yml"
	// +kubebuilder:validation:XValidation:rule="!self.startsWith('/') && !self.contains('//') && !self.startsWith('./') && !self.contains('/./')",message="must be a repository-relative path without empty or '.' path segments"
	// +kubebuilder:validation:XValidation:rule="!self.startsWith('../') && !self.contains('/../')",message="must not contain '..' path segments"
	// +required
	WorkflowPath string `json:"workflowPath"`

	// TTLSecondsAfterFinished limits the lifetime of a WorkflowRun that has
	// reached a terminal result. The timer starts at status.completionTime, or
	// the terminal Succeeded condition's lastTransitionTime when completionTime
	// is absent. Set this field to zero to make the run eligible for deletion
	// immediately after completion. Omit it to retain the run indefinitely. This
	// field may be changed while the run exists, but updates are not guaranteed
	// to retain a run that is already eligible for deletion.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=2147483647
	// +optional
	TTLSecondsAfterFinished *int32 `json:"ttlSecondsAfterFinished,omitempty"`
}

// WorkflowRunSource is a discriminated union of supported workflow event
// sources.
// +kubebuilder:validation:XValidation:rule="self.type == 'GitHub' ? has(self.github) : !has(self.github)",message="github must be specified exactly when type is GitHub"
type WorkflowRunSource struct {
	// Type selects the source data variant.
	// +kubebuilder:validation:Enum=GitHub
	// +required
	Type SourceType `json:"type"`

	// GitHub records the GitHub delivery and repository revision for this run.
	// +optional
	GitHub *GitHubWorkflowRunSource `json:"github,omitempty"`
}

// GitHubWorkflowRunSource identifies the GitHub content and delivery that
// requested a WorkflowRun.
// +kubebuilder:validation:XValidation:rule="self.event.name == 'pull_request' ? has(self.revision.headRef) : !has(self.revision.headRef)",message="revision.headRef must be specified exactly for pull_request events"
// +kubebuilder:validation:XValidation:rule="!has(self.revision.baseRef) || self.event.name in ['pull_request', 'merge_group']",message="revision.baseRef may be specified only for pull_request and merge_group events"
// +kubebuilder:validation:XValidation:rule="!has(self.revision.headSHA) || self.event.name == 'pull_request'",message="revision.headSHA may be specified only for pull_request events"
// +kubebuilder:validation:XValidation:rule="self.event.name != 'push' || self.revision.ref.startsWith('refs/heads/') || self.revision.ref.startsWith('refs/tags/')",message="push revision.ref must identify a branch or tag"
// +kubebuilder:validation:XValidation:rule="self.event.name != 'pull_request' || self.revision.ref.matches('^refs/pull/[1-9][0-9]*/merge$') || (self.event.action == 'closed' && self.revision.ref.startsWith('refs/heads/'))",message="pull_request revision.ref must identify its merge ref, or its base branch when merged and closed"
// +kubebuilder:validation:XValidation:rule="self.event.name != 'merge_group' || self.revision.ref.startsWith('refs/heads/gh-readonly-queue/')",message="merge_group revision.ref must identify a merge queue branch"
type GitHubWorkflowRunSource struct {
	// Repository identifies the GitHub repository at the time of the event.
	// +required
	Repository GitHubRepository `json:"repository"`

	// Event identifies the GitHub delivery that requested this run.
	// +required
	Event GitHubEvent `json:"event"`

	// Revision identifies the repository content executed by the run.
	// +required
	Revision GitRevision `json:"revision"`
}

// GitHubRepository identifies a GitHub repository independently of its clone
// URL.
// +kubebuilder:validation:XValidation:rule="self.name != '.' && self.name != '..'",message="`name` must be a GitHub repository name"
type GitHubRepository struct {
	// ID is GitHub's stable numeric repository identifier.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=9007199254740991
	// +required
	ID int64 `json:"id"`

	// Owner is the repository owner's login at the time of the event.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=100
	// +kubebuilder:validation:Pattern=`^[A-Za-z0-9][A-Za-z0-9_-]*$`
	// +required
	Owner string `json:"owner"`

	// Name is the repository name at the time of the event.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=100
	// +kubebuilder:validation:Pattern=`^[A-Za-z0-9._-]+$`
	// +required
	Name string `json:"name"`
}

// GitHubEvent identifies a supported webhook delivery. Name and Action retain
// GitHub's lowercase external protocol values.
// +kubebuilder:validation:XValidation:rule="self.name != 'push' || !has(self.action)",message="push events must not specify action"
// +kubebuilder:validation:XValidation:rule="self.name != 'pull_request' || (has(self.action) && self.action in ['assigned', 'unassigned', 'labeled', 'unlabeled', 'opened', 'edited', 'closed', 'reopened', 'synchronize', 'converted_to_draft', 'locked', 'unlocked', 'enqueued', 'dequeued', 'milestoned', 'demilestoned', 'ready_for_review', 'review_requested', 'review_request_removed', 'auto_merge_enabled', 'auto_merge_disabled'])",message="pull_request action must be a supported activity type"
// +kubebuilder:validation:XValidation:rule="self.name != 'merge_group' || (has(self.action) && self.action == 'checks_requested')",message="merge_group action must be checks_requested"
type GitHubEvent struct {
	// Name is the value of the X-GitHub-Event delivery header.
	// +kubebuilder:validation:Enum=push;pull_request;merge_group
	// +required
	Name GitHubEventName `json:"name"`

	// Action is the payload action, when the event defines one.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=64
	// +kubebuilder:validation:Pattern=`^[a-z][a-z0-9_]*$`
	// +optional
	Action string `json:"action,omitempty"`

	// DeliveryID is the value of the X-GitHub-Delivery header.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=128
	// +kubebuilder:validation:Pattern=`^[A-Za-z0-9-]+$`
	// +required
	DeliveryID string `json:"deliveryID"`
}

// GitRevision records the selected execution commit and the ref associated
// with the event.
type GitRevision struct {
	// SHA is the full 40-character hexadecimal Git SHA-1 object ID used to
	// discover and execute the workflow. For a deleted push ref, it is resolved
	// from the repository's default branch and does not belong to Ref.
	// +kubebuilder:validation:Pattern=`^[0-9a-f]{40}$`
	// +required
	SHA string `json:"sha"`

	// HeadSHA is the pull request head commit used for GitHub check reporting.
	// It may differ from SHA when the workflow executes a test merge commit.
	// When absent, GitHub checks are reported on SHA.
	// +kubebuilder:validation:Pattern=`^[0-9a-f]{40}$`
	// +optional
	HeadSHA string `json:"headSHA,omitempty"`

	// Ref is the fully qualified Git ref associated with the event. For a
	// deleted push, it records the deleted ref rather than identifying SHA.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=1024
	// +kubebuilder:validation:Pattern=`^[^\x00-\x20\x7f~^:?*\[\\]+$`
	// +kubebuilder:validation:XValidation:rule="self.startsWith('refs/') && size(self) > 5 && !self.contains('//') && !self.contains('..') && !self.contains('/.') && !self.contains('.lock/') && !self.endsWith('/') && !self.endsWith('.') && !self.endsWith('.lock') && !self.contains('@{')",message="must be a well-formed fully qualified Git ref"
	// +required
	Ref string `json:"ref"`

	// HeadRef is the pull request source branch name, when applicable.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=255
	// +kubebuilder:validation:Pattern=`^[^\x00-\x20\x7f~^:?*\[\\]+$`
	// +kubebuilder:validation:XValidation:rule="!self.startsWith('/') && !self.startsWith('.') && !self.startsWith('refs/') && !self.endsWith('/') && !self.contains('//') && !self.contains('..') && !self.contains('/.') && !self.endsWith('.') && !self.contains('.lock/') && !self.endsWith('.lock') && !self.contains('@{') && !self.matches('^[0-9A-Fa-f]{40}$')",message="must be a well-formed GitHub branch name"
	// +optional
	HeadRef string `json:"headRef,omitempty"`

	// BaseRef is the target branch name for pull request and merge group events,
	// when applicable.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=255
	// +kubebuilder:validation:Pattern=`^[^\x00-\x20\x7f~^:?*\[\\]+$`
	// +kubebuilder:validation:XValidation:rule="!self.startsWith('/') && !self.startsWith('.') && !self.startsWith('refs/') && !self.endsWith('/') && !self.contains('//') && !self.contains('..') && !self.contains('/.') && !self.endsWith('.') && !self.contains('.lock/') && !self.endsWith('.lock') && !self.contains('@{') && !self.matches('^[0-9A-Fa-f]{40}$')",message="must be a well-formed GitHub branch name"
	// +optional
	BaseRef string `json:"baseRef,omitempty"`
}

// WorkflowRunJobStatus summarizes native child Jobs without embedding a list
// whose size grows with the workflow.
type WorkflowRunJobStatus struct {
	// Total is the number of jobs defined by the workflow.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=100000
	// +optional
	Total int32 `json:"total,omitempty"`

	// Queued is the number of jobs waiting for a matching Runner.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=100000
	// +optional
	Queued int32 `json:"queued,omitempty"`

	// Active is the number of assigned jobs that have not reached a terminal condition.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=100000
	// +optional
	Active int32 `json:"active,omitempty"`

	// Succeeded is the number of jobs with a successful terminal condition.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=100000
	// +optional
	Succeeded int32 `json:"succeeded,omitempty"`

	// Failed is the number of jobs with a failed terminal condition.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=100000
	// +optional
	Failed int32 `json:"failed,omitempty"`
}

// GitHubCheckRunStatus records the GitHub Check Run that reports this
// WorkflowRun.
// +kubebuilder:validation:XValidation:rule="self.status == 'completed' ? has(self.conclusion) : !has(self.conclusion)",message="conclusion must be specified exactly when status is completed"
type GitHubCheckRunStatus struct {
	// ID is GitHub's check-run identifier.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=9007199254740991
	// +required
	ID int64 `json:"id"`

	// Status is the last check-run status accepted by GitHub.
	// +kubebuilder:validation:Enum=queued;in_progress;completed
	// +required
	Status string `json:"status"`

	// Conclusion is the terminal result accepted by GitHub.
	// +kubebuilder:validation:Enum=success;failure;cancelled
	// +optional
	Conclusion string `json:"conclusion,omitempty"`

	// ReportDigest is the SHA-256 digest of the check-run fields last accepted
	// by GitHub.
	// +kubebuilder:validation:Pattern=`^[0-9a-f]{64}$`
	// +optional
	ReportDigest string `json:"reportDigest,omitempty"`
}

// GitHubWorkflowRunStatus contains GitHub observations for a WorkflowRun.
type GitHubWorkflowRunStatus struct {
	// CheckRun is the GitHub check that reports this WorkflowRun.
	// +optional
	CheckRun *GitHubCheckRunStatus `json:"checkRun,omitempty"`
}

// WorkflowRunSourceStatus contains provider-specific observations.
type WorkflowRunSourceStatus struct {
	// GitHub contains observations for a GitHub workflow source.
	// +optional
	GitHub *GitHubWorkflowRunStatus `json:"github,omitempty"`
}

// WorkflowRunStatus contains observations made while executing a workflow.
type WorkflowRunStatus struct {
	// ObservedGeneration is the most recent generation observed by the
	// controller.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// WorkflowName is the display name read from the selected workflow file.
	// +kubebuilder:validation:MaxLength=256
	// +optional
	WorkflowName string `json:"workflowName,omitempty"`

	// ConcurrencyGroup is the evaluated workflow concurrency key, when present.
	// +kubebuilder:validation:MaxLength=256
	// +optional
	ConcurrencyGroup string `json:"concurrencyGroup,omitempty"`

	// Jobs summarizes WorkflowJobs owned by this run.
	// +optional
	Jobs *WorkflowRunJobStatus `json:"jobs,omitempty"`

	// StartTime is when the first child Job started.
	// +optional
	StartTime *metav1.Time `json:"startTime,omitempty"`

	// CompletionTime is when the run reached a terminal result.
	// +optional
	CompletionTime *metav1.Time `json:"completionTime,omitempty"`

	// Source contains provider-specific reporting state.
	// +optional
	Source *WorkflowRunSourceStatus `json:"source,omitempty"`

	// Conditions describe planning and the terminal result.
	// Known condition types are Planned and Succeeded.
	// +listType=map
	// +listMapKey=type
	// +kubebuilder:validation:MaxItems=16
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:metadata:annotations=helm.sh/resource-policy=keep
// +kubebuilder:printcolumn:name="Workflow",type=string,JSONPath=`.status.workflowName`
// +kubebuilder:printcolumn:name="Repository",type=string,JSONPath=`.spec.source.github.repository.name`
// +kubebuilder:printcolumn:name="Event",type=string,JSONPath=`.spec.source.github.event.name`
// +kubebuilder:printcolumn:name="Succeeded",type=string,JSONPath=`.status.conditions[?(@.type=="Succeeded")].status`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
// +kubebuilder:printcolumn:name="SHA",type=string,JSONPath=`.spec.source.github.revision.sha`,priority=1

// WorkflowRun represents one immutable execution of one workflow at one Git
// revision.
type WorkflowRun struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// +required
	Spec WorkflowRunSpec `json:"spec"`

	// +optional
	Status WorkflowRunStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// WorkflowRunList contains WorkflowRun objects.
type WorkflowRunList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []WorkflowRun `json:"items"`
}
