package webhook

import (
	"context"
	"fmt"
	"strings"

	actionsv1alpha1 "github.com/kelos-dev/open-actions/api/v1alpha1"
)

func (r *DeliveryReconciler) jobTokenEvent(ctx context.Context, project *actionsv1alpha1.Project, event normalizedEvent, privateKey []byte) (bool, error) {
	if event.Name != "pull_request" && allowJobTokenEvent(event) {
		return false, nil
	}
	if !strings.HasSuffix(strings.ToLower(event.Actor), "[bot]") {
		return false, nil
	}
	// Webhooks identify the App actor, not the individual installation token.
	// The Project App must be reserved for Open Actions.
	appLogin, err := r.GitHub.AppBotLogin(ctx, project.Spec.Source.GitHub.AppID, privateKey)
	if err != nil {
		return false, fmt.Errorf("resolve GitHub App identity for Project %q: %w", project.Name, err)
	}
	return strings.EqualFold(event.Actor, appLogin), nil
}

func allowJobTokenEvent(event normalizedEvent) bool {
	switch event.Name {
	case "workflow_dispatch", "repository_dispatch", "schedule", "workflow_call":
		return true
	case "workflow_run":
		// The job token can start native GitHub workflows through repository
		// mutations. Only explicit dispatches may feed back into Open Actions.
		return event.WorkflowRun != nil && (event.WorkflowRun.Event == "workflow_dispatch" || event.WorkflowRun.Event == "repository_dispatch")
	case "pull_request":
		return event.Action == "opened" || event.Action == "synchronize" || event.Action == "reopened"
	default:
		return false
	}
}
