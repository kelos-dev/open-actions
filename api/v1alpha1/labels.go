package v1alpha1

const (
	LabelProjectUID     = "actions.kelos.dev/project-uid"
	LabelRunnerUID      = "actions.kelos.dev/runner-uid"
	LabelWorkflowRunUID = "actions.kelos.dev/workflow-run-uid"
	LabelWorkflowJobUID = "actions.kelos.dev/workflow-job-uid"
	LabelWorkflowJob    = "actions.kelos.dev/workflow-job"

	AnnotationWorkflowJobID          = "actions.kelos.dev/workflow-job-id"
	AnnotationWorkflowJobDisplayName = "actions.kelos.dev/workflow-job-display-name"
	AnnotationRunnerName             = "actions.kelos.dev/runner-name"
	AnnotationRunnerResultVersion    = "actions.kelos.dev/runner-result-version"
	AnnotationProjectName            = "actions.kelos.dev/project-name"
)
