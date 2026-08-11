package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	WorkflowRunConditionPlanned   = "Planned"
	WorkflowRunConditionSucceeded = "Succeeded"

	GitHubEventNamePush                     GitHubEventName = "push"
	GitHubEventNamePullRequest              GitHubEventName = "pull_request"
	GitHubEventNameMergeGroup               GitHubEventName = "merge_group"
	GitHubEventNameWorkflowRun              GitHubEventName = "workflow_run"
	GitHubEventNameWorkflowDispatch         GitHubEventName = "workflow_dispatch"
	GitHubEventNameIssues                   GitHubEventName = "issues"
	GitHubEventNamePullRequestTarget        GitHubEventName = "pull_request_target"
	GitHubEventNameIssueComment             GitHubEventName = "issue_comment"
	GitHubEventNamePullRequestReviewComment GitHubEventName = "pull_request_review_comment"
	GitHubEventNamePullRequestReview        GitHubEventName = "pull_request_review"
	GitHubEventNameSchedule                 GitHubEventName = "schedule"
	GitHubEventNameRelease                  GitHubEventName = "release"
	GitHubEventNameWorkflowCall             GitHubEventName = "workflow_call"
)

// GitHubEventName identifies a supported GitHub-compatible workflow trigger.
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

	// GitHub records the GitHub trigger, repository, and revision for this run.
	// +optional
	GitHub *GitHubWorkflowRunSource `json:"github,omitempty"`
}

// GitHubWorkflowRunSource identifies the GitHub content and trigger that
// requested a WorkflowRun.
// +kubebuilder:validation:XValidation:rule="self.event.name == 'pull_request' ? has(self.revision.headRef) : !has(self.revision.headRef)",message="revision.headRef must be specified exactly for pull_request events"
// +kubebuilder:validation:XValidation:rule="!has(self.revision.baseRef) || self.event.name in ['pull_request', 'merge_group']",message="revision.baseRef may be specified only for pull_request and merge_group events"
// +kubebuilder:validation:XValidation:rule="!has(self.revision.headSHA) || self.event.name == 'pull_request'",message="revision.headSHA may be specified only for pull_request events"
// +kubebuilder:validation:XValidation:rule="self.event.name != 'push' || self.revision.ref.startsWith('refs/heads/') || self.revision.ref.startsWith('refs/tags/')",message="push revision.ref must identify a branch or tag"
// +kubebuilder:validation:XValidation:rule="self.event.name != 'pull_request' || self.revision.ref.matches('^refs/pull/[1-9][0-9]*/merge$') || (self.event.action == 'closed' && self.revision.ref.startsWith('refs/heads/'))",message="pull_request revision.ref must identify its merge ref, or its base branch when merged and closed"
// +kubebuilder:validation:XValidation:rule="self.event.name != 'merge_group' || self.revision.ref.startsWith('refs/heads/gh-readonly-queue/')",message="merge_group revision.ref must identify a merge queue branch"
// +kubebuilder:validation:XValidation:rule="self.event.name != 'pull_request_target' || (has(self.event.pullRequest) && self.revision.ref == 'refs/heads/' + self.event.pullRequest.baseRef)",message="pull_request_target revision.ref must identify event.pullRequest.baseRef"
// +kubebuilder:validation:XValidation:rule="!(self.event.name in ['workflow_run', 'issues', 'issue_comment', 'pull_request_review', 'pull_request_review_comment', 'schedule']) || self.revision.ref.startsWith('refs/heads/')",message="default-branch events must identify a branch ref"
// +kubebuilder:validation:XValidation:rule="!(self.event.name in ['workflow_dispatch', 'workflow_call']) || self.revision.ref.startsWith('refs/heads/') || self.revision.ref.startsWith('refs/tags/')",message="workflow_dispatch and workflow_call revision.ref must identify a branch or tag"
// +kubebuilder:validation:XValidation:rule="self.event.name != 'release' || self.revision.ref.startsWith('refs/tags/')",message="release revision.ref must identify a tag"
// +kubebuilder:validation:XValidation:rule="self.event.name != 'pull_request' || !has(self.event.pullRequest) || (self.event.pullRequest.headRef == self.revision.headRef && self.event.pullRequest.baseRef == self.revision.baseRef && has(self.revision.headSHA) && self.event.pullRequest.headSHA == self.revision.headSHA)",message="pull request event metadata must match duplicated revision fields"
// +kubebuilder:validation:XValidation:rule="self.event.name != 'pull_request' || !has(self.event.pullRequest) || !self.revision.ref.startsWith('refs/pull/') || int(self.revision.ref.split('/')[2]) == self.event.pullRequest.number",message="pull request revision.ref must identify event.pullRequest.number"
type GitHubWorkflowRunSource struct {
	// Repository identifies the GitHub repository at the time of the event.
	// +required
	Repository GitHubRepository `json:"repository"`

	// Event identifies the GitHub-compatible trigger that requested this run.
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

// GitHubEvent identifies a supported GitHub-compatible trigger. Name and Action
// retain GitHub's lowercase external protocol values.
// +kubebuilder:validation:XValidation:rule="self.name != 'push' || !has(self.action)",message="push events must not specify action"
// +kubebuilder:validation:XValidation:rule="!(self.name in ['workflow_dispatch', 'schedule', 'workflow_call']) || !has(self.action)",message="workflow_dispatch, schedule, and workflow_call events must not specify action"
// +kubebuilder:validation:XValidation:rule="self.name != 'pull_request' || (has(self.action) && self.action in ['assigned', 'unassigned', 'labeled', 'unlabeled', 'opened', 'edited', 'closed', 'reopened', 'synchronize', 'converted_to_draft', 'locked', 'unlocked', 'enqueued', 'dequeued', 'milestoned', 'demilestoned', 'ready_for_review', 'review_requested', 'review_request_removed', 'auto_merge_enabled', 'auto_merge_disabled'])",message="pull_request action must be a supported activity type"
// +kubebuilder:validation:XValidation:rule="self.name != 'pull_request_target' || (has(self.action) && self.action in ['assigned', 'unassigned', 'labeled', 'unlabeled', 'opened', 'edited', 'closed', 'reopened', 'synchronize', 'converted_to_draft', 'locked', 'unlocked', 'enqueued', 'dequeued', 'milestoned', 'demilestoned', 'ready_for_review', 'review_requested', 'review_request_removed', 'auto_merge_enabled', 'auto_merge_disabled'])",message="pull_request_target action must be a supported activity type"
// +kubebuilder:validation:XValidation:rule="self.name != 'merge_group' || (has(self.action) && self.action == 'checks_requested')",message="merge_group action must be checks_requested"
// +kubebuilder:validation:XValidation:rule="self.name != 'workflow_run' || (has(self.action) && self.action in ['completed', 'requested', 'in_progress'])",message="workflow_run action must be a supported activity type"
// +kubebuilder:validation:XValidation:rule="self.name != 'issues' || (has(self.action) && self.action in ['opened', 'edited', 'deleted', 'transferred', 'pinned', 'unpinned', 'closed', 'reopened', 'assigned', 'unassigned', 'labeled', 'unlabeled', 'locked', 'unlocked', 'milestoned', 'demilestoned', 'typed', 'untyped', 'field_added', 'field_removed'])",message="issues action must be a supported activity type"
// +kubebuilder:validation:XValidation:rule="self.name != 'issue_comment' || (has(self.action) && self.action in ['created', 'edited', 'deleted'])",message="issue_comment action must be a supported activity type"
// +kubebuilder:validation:XValidation:rule="self.name != 'pull_request_review_comment' || (has(self.action) && self.action in ['created', 'edited', 'deleted'])",message="pull_request_review_comment action must be a supported activity type"
// +kubebuilder:validation:XValidation:rule="self.name != 'pull_request_review' || (has(self.action) && self.action in ['submitted', 'edited', 'dismissed'])",message="pull_request_review action must be a supported activity type"
// +kubebuilder:validation:XValidation:rule="self.name != 'release' || (has(self.action) && self.action in ['published', 'unpublished', 'created', 'edited', 'deleted', 'prereleased', 'released'])",message="release action must be a supported activity type"
// +kubebuilder:validation:XValidation:rule="self.name in ['pull_request_target', 'pull_request_review', 'pull_request_review_comment'] ? has(self.pullRequest) : true",message="pullRequest must be specified for pull_request_target and pull request review events"
// +kubebuilder:validation:XValidation:rule="!has(self.pullRequest) || self.name in ['pull_request', 'pull_request_target', 'pull_request_review', 'pull_request_review_comment']",message="pullRequest may be specified only for pull request events"
// +kubebuilder:validation:XValidation:rule="self.name == 'workflow_run' ? has(self.workflowRun) : !has(self.workflowRun)",message="workflowRun must be specified exactly for workflow_run events"
// +kubebuilder:validation:XValidation:rule="self.name != 'workflow_run' || (has(self.workflowRun) && (self.action != 'completed' || has(self.workflowRun.conclusion)))",message="workflowRun.conclusion must be specified for completed workflow_run events"
// +kubebuilder:validation:XValidation:rule="self.name in ['issues', 'issue_comment'] ? has(self.issue) : !has(self.issue)",message="issue must be specified exactly for issues and issue_comment events"
// +kubebuilder:validation:XValidation:rule="self.name in ['issue_comment', 'pull_request_review_comment'] ? has(self.comment) : !has(self.comment)",message="comment must be specified exactly for comment events"
// +kubebuilder:validation:XValidation:rule="self.name == 'pull_request_review' ? has(self.review) : !has(self.review)",message="review must be specified exactly for pull_request_review events"
// +kubebuilder:validation:XValidation:rule="self.name in ['workflow_dispatch', 'workflow_call'] || !has(self.inputs)",message="inputs may be specified only for workflow_dispatch and workflow_call events"
// +kubebuilder:validation:XValidation:rule="self.name == 'schedule' ? has(self.schedule) : !has(self.schedule)",message="schedule must be specified exactly for schedule events"
// +kubebuilder:validation:XValidation:rule="self.name in ['workflow_dispatch', 'schedule', 'workflow_call'] ? !has(self.deliveryID) : has(self.deliveryID)",message="deliveryID must be specified exactly for webhook-backed events"
type GitHubEvent struct {
	// Name identifies the GitHub-compatible event that initiated the run.
	// +kubebuilder:validation:Enum=push;pull_request;merge_group;workflow_run;workflow_dispatch;issues;pull_request_target;issue_comment;pull_request_review_comment;pull_request_review;schedule;release;workflow_call
	// +required
	Name GitHubEventName `json:"name"`

	// Action is the payload action, when the event defines one.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=64
	// +kubebuilder:validation:Pattern=`^[a-z][a-z0-9_]*$`
	// +optional
	Action string `json:"action,omitempty"`

	// DeliveryID is the value of the X-GitHub-Delivery header for webhook-backed
	// runs. It must be omitted for manual, scheduled, and reusable-workflow runs.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=128
	// +kubebuilder:validation:Pattern=`^[A-Za-z0-9-]+$`
	// +optional
	DeliveryID string `json:"deliveryID,omitempty"`

	// PullRequest records bounded pull request metadata separately from the
	// trusted revision selected for workflow execution.
	// +optional
	PullRequest *GitHubPullRequest `json:"pullRequest,omitempty"`

	// WorkflowRun records bounded metadata about the GitHub Actions workflow run
	// that initiated a workflow_run event.
	// +optional
	WorkflowRun *GitHubWorkflowRunEvent `json:"workflowRun,omitempty"`

	// Issue records bounded issue metadata for issues and issue_comment events.
	// +optional
	Issue *GitHubIssueEvent `json:"issue,omitempty"`

	// Comment records the comment body for issue and pull request comment events.
	// +optional
	Comment *GitHubCommentEvent `json:"comment,omitempty"`

	// Review records the review body for pull_request_review events.
	// +optional
	Review *GitHubReviewEvent `json:"review,omitempty"`

	// Inputs contains validated values supplied to a manual or reusable workflow.
	// +kubebuilder:validation:MaxProperties=25
	// +kubebuilder:validation:XValidation:rule="self.all(key, key.matches('^[A-Za-z_][A-Za-z0-9_-]{0,99}$'))",message="input names must start with a letter or '_' and contain at most 100 letters, digits, '-' or '_'"
	// +kubebuilder:validation:XValidation:rule="self.all(key, size(self[key]) <= 65535)",message="input values must be no more than 65535 characters"
	// +kubebuilder:validation:XValidation:rule="self.map(key, size(key) + size(self[key])).sum() <= 65535",message="input names and values must contain no more than 65535 characters in total"
	// +optional
	Inputs map[string]string `json:"inputs,omitempty"`

	// Schedule is the validated cron expression that initiated a scheduled run.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=256
	// +kubebuilder:validation:Pattern=`^[^[:space:]]+([[:space:]]+[^[:space:]]+){4}$`
	// +optional
	Schedule string `json:"schedule,omitempty"`
}

// GitHubPullRequest records the untrusted revision metadata associated with a
// pull request event.
type GitHubPullRequest struct {
	// Number is the repository-scoped pull request number.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=9007199254740991
	// +required
	Number int64 `json:"number"`

	// Body is the pull request body observed in the webhook payload.
	// +kubebuilder:validation:MaxLength=48000
	// +required
	Body string `json:"body"`

	// HTMLURL is the browser URL for the pull request.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=2048
	// +kubebuilder:validation:Pattern=`^https?://[^[:space:]]+$`
	// +required
	HTMLURL string `json:"htmlURL"`

	// HeadRepository identifies the repository containing the pull request head.
	// +required
	HeadRepository GitHubRepository `json:"headRepository"`

	// HeadRef is the pull request source branch name.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=255
	// +kubebuilder:validation:Pattern=`^[^\x00-\x20\x7f~^:?*\[\\]+$`
	// +kubebuilder:validation:XValidation:rule="!self.startsWith('/') && !self.startsWith('.') && !self.startsWith('refs/') && !self.endsWith('/') && !self.contains('//') && !self.contains('..') && !self.contains('/.') && !self.endsWith('.') && !self.contains('.lock/') && !self.endsWith('.lock') && !self.contains('@{') && !self.matches('^[0-9A-Fa-f]{40}$')",message="must be a well-formed GitHub branch name"
	// +required
	HeadRef string `json:"headRef"`

	// HeadSHA is the pull request head commit observed in the webhook payload.
	// +kubebuilder:validation:Pattern=`^[0-9a-f]{40}$`
	// +required
	HeadSHA string `json:"headSHA"`

	// BaseRef is the pull request target branch name.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=255
	// +kubebuilder:validation:Pattern=`^[^\x00-\x20\x7f~^:?*\[\\]+$`
	// +kubebuilder:validation:XValidation:rule="!self.startsWith('/') && !self.startsWith('.') && !self.startsWith('refs/') && !self.endsWith('/') && !self.contains('//') && !self.contains('..') && !self.contains('/.') && !self.endsWith('.') && !self.contains('.lock/') && !self.endsWith('.lock') && !self.contains('@{') && !self.matches('^[0-9A-Fa-f]{40}$')",message="must be a well-formed GitHub branch name"
	// +required
	BaseRef string `json:"baseRef"`
}

// GitHubWorkflowRunEvent records fields exposed for a workflow_run trigger.
type GitHubWorkflowRunEvent struct {
	// Conclusion is the completed workflow run conclusion. It is absent while
	// the triggering run is requested or in progress.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=64
	// +kubebuilder:validation:Pattern=`^[a-z][a-z_]*$`
	// +optional
	Conclusion string `json:"conclusion,omitempty"`

	// HeadSHA is the triggering workflow run's head commit.
	// +kubebuilder:validation:Pattern=`^[0-9a-f]{40}$`
	// +required
	HeadSHA string `json:"headSHA"`
}

// GitHubIssueEvent records fields exposed for an issue-backed trigger.
type GitHubIssueEvent struct {
	// Number is the repository-scoped issue or pull request number.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=9007199254740991
	// +required
	Number int64 `json:"number"`

	// Body is the issue or pull request body observed in the webhook payload.
	// +kubebuilder:validation:MaxLength=48000
	// +required
	Body string `json:"body"`
}

// GitHubCommentEvent records fields exposed for a comment-backed trigger.
type GitHubCommentEvent struct {
	// Body is the comment body observed in the webhook payload.
	// +kubebuilder:validation:MaxLength=48000
	// +required
	Body string `json:"body"`
}

// GitHubReviewEvent records fields exposed for a pull request review trigger.
type GitHubReviewEvent struct {
	// Body is the review body observed in the webhook payload.
	// +kubebuilder:validation:MaxLength=48000
	// +required
	Body string `json:"body"`
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

	// HeadRef is the pull request source branch name, when applicable to the
	// execution revision.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=255
	// +kubebuilder:validation:Pattern=`^[^\x00-\x20\x7f~^:?*\[\\]+$`
	// +kubebuilder:validation:XValidation:rule="!self.startsWith('/') && !self.startsWith('.') && !self.startsWith('refs/') && !self.endsWith('/') && !self.contains('//') && !self.contains('..') && !self.contains('/.') && !self.endsWith('.') && !self.contains('.lock/') && !self.endsWith('.lock') && !self.contains('@{') && !self.matches('^[0-9A-Fa-f]{40}$')",message="must be a well-formed GitHub branch name"
	// +optional
	HeadRef string `json:"headRef,omitempty"`

	// BaseRef is the target branch name for pull_request and merge_group execution
	// revisions, when applicable.
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
