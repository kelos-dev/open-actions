package runner

import (
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"

	workflowexpression "github.com/kelos-dev/open-actions/internal/expression"
	"github.com/kelos-dev/open-actions/internal/workflow"
)

var (
	runnerJobAvailability          = workflowexpression.NewAvailability("github", "open_actions", "matrix", "needs", "vars", "secrets", "inputs")
	actionDefaultAvailability      = workflowexpression.NewAvailability("github", "open_actions")
	runnerStepAvailability         = workflowexpression.NewAvailability("github", "open_actions", "matrix", "needs", "runner", "env", "vars", "secrets", "inputs", "steps").WithHashFiles()
	runnerConditionAvailability    = workflowexpression.NewAvailability("github", "open_actions", "matrix", "needs", "runner", "env", "vars", "inputs", "steps").WithStatusFunctions().WithHashFiles()
	compositeAvailability          = workflowexpression.NewAvailability("github", "open_actions", "runner", "env", "inputs", "steps").WithHashFiles()
	compositeConditionAvailability = workflowexpression.NewAvailability("github", "open_actions", "runner", "env", "inputs", "steps").WithStatusFunctions().WithHashFiles()
)

func resolveJobEnvironment(values map[string]string, plan *Plan, environment []string, token string, secrets, variables map[string]string) (map[string]string, error) {
	return resolveExpressionMap(values, expressionContext(plan, environment, "", nil, runnerJobAvailability, nil, token, secrets, variables))
}

func resolveActionDefaultExpression(input string, plan *Plan, environment []string, token string) (string, error) {
	context := expressionContext(plan, environment, "", nil, actionDefaultAvailability, nil, token, nil, nil)
	return resolveExpressionString(input, context)
}

func resolveWorkflowStepEnvironment(step Step, state *executionState) (map[string]string, error) {
	context := workflowExpressionContext(state, state.environment, runnerStepAvailability, nil)
	return resolveExpressionMap(step.Env, context)
}

func resolveWorkflowStep(step Step, environment map[string]string, state *executionState) (Step, error) {
	stepEnvironment := appendEnvironment(append([]string(nil), state.environment...), environment)
	context := workflowExpressionContext(state, stepEnvironment, runnerStepAvailability, nil)
	resolved := step
	var err error
	for _, field := range []struct {
		name   string
		target *string
	}{
		{name: "name", target: &resolved.Name},
		{name: "run", target: &resolved.Run},
		{name: "working-directory", target: &resolved.WorkingDirectory},
	} {
		*field.target, err = resolveExpressionString(*field.target, context)
		if err != nil {
			return Step{}, fmt.Errorf("resolve %s: %w", field.name, err)
		}
	}
	resolved.With, err = resolveExpressionMap(step.With, context)
	if err != nil {
		return Step{}, fmt.Errorf("resolve with: %w", err)
	}
	resolved.Env = environment
	return resolved, nil
}

func workflowStepCondition(input string, environment map[string]string, status workflowexpression.Status, state *executionState) (bool, error) {
	if strings.TrimSpace(input) == "" {
		return status.Success, nil
	}
	context := workflowExpressionContext(state, state.environment, runnerConditionAvailability, &status)
	environmentContext := workflowExpressionContext(state, state.environment, runnerStepAvailability, nil)
	baseEnvironment := context.Values["env"].(map[string]any)
	resolveEnvironment := func(name string) (any, bool, error) {
		if input, found := stringMapValue(environment, name); found {
			resolved, err := resolveMapExpression(input, environmentContext)
			return resolved, true, err
		}
		value, found := anyMapValue(baseEnvironment, name)
		return value, found, nil
	}
	allEnvironment := func() (map[string]any, error) {
		result := make(map[string]any, len(baseEnvironment)+len(environment))
		for name, value := range baseEnvironment {
			result[name] = value
		}
		for name, input := range environment {
			resolved, err := resolveMapExpression(input, environmentContext)
			if err != nil {
				return nil, err
			}
			result[name] = resolved
		}
		return result, nil
	}
	context.Values["env"] = workflowexpression.DeferredObjectMap{Resolve: resolveEnvironment, Values: allEnvironment}
	return evaluateCondition(input, context, status.Success)
}

func workflowExpressionContext(state *executionState, environment []string, availability workflowexpression.Availability, status *workflowexpression.Status) workflowexpression.Context {
	values := map[string]any{"steps": state.stepOutputs}
	return expressionContext(state.plan, environment, "", values, availability, status, state.githubToken, state.secrets, state.variables)
}

func compositeExpressionContext(compositeContext *compositeContext, availability workflowexpression.Availability, status *workflowexpression.Status) workflowexpression.Context {
	environment := appendEnvironment(append([]string(nil), compositeContext.state.environment...), compositeContext.scopedEnvironment)
	steps := make(map[string]any, len(compositeContext.stepOutput))
	for id, outputs := range compositeContext.stepOutput {
		steps[id] = map[string]any{"outputs": outputs}
	}
	values := map[string]any{
		"inputs": compositeContext.inputs,
		"steps":  steps,
	}
	return expressionContext(compositeContext.state.plan, environment, compositeContext.actionPath, values, availability, status, compositeContext.state.githubToken, nil, nil)
}

func expressionContext(plan *Plan, environment []string, actionPath string, extra map[string]any, availability workflowexpression.Availability, status *workflowexpression.Status, token string, secrets, variables map[string]string) workflowexpression.Context {
	pullRequestRefs := planPullRequestRefs(plan)
	github := map[string]any{
		"actor":       plan.Run.Actor,
		"workflow":    plan.WorkflowName,
		"event_name":  plan.Event.Name,
		"event":       githubExpressionEvent(plan),
		"repository":  plan.Repository.Owner + "/" + plan.Repository.Name,
		"sha":         plan.Revision.SHA,
		"ref":         plan.Revision.Ref,
		"ref_name":    plan.Revision.RefName,
		"head_ref":    pullRequestRefs.head,
		"base_ref":    pullRequestRefs.base,
		"workspace":   environmentValue(environment, "GITHUB_WORKSPACE"),
		"server_url":  strings.TrimSuffix(plan.Repository.ServerURL, "/"),
		"api_url":     strings.TrimSuffix(plan.Repository.APIURL, "/"),
		"run_id":      strconv.FormatInt(plan.Run.ID, 10),
		"run_number":  strconv.FormatInt(plan.Run.Number, 10),
		"run_attempt": strconv.FormatInt(int64(plan.Run.Attempt), 10),
		"token":       workflowexpression.Secret(token),
	}
	if actionPath != "" {
		github["action_path"] = actionPath
	}
	values := map[string]any{
		"github": github,
		"open_actions": map[string]any{
			"run_url":       plan.Run.URL,
			"run_query_url": plan.Run.QueryURL,
		},
		"inputs":  plan.Inputs,
		"matrix":  plan.Matrix,
		"needs":   plan.Needs.ExpressionValues(),
		"secrets": secretContext(token, secrets),
		"vars":    variables,
		"runner": map[string]any{
			"os":         environmentValue(environment, "RUNNER_OS"),
			"arch":       environmentValue(environment, "RUNNER_ARCH"),
			"temp":       environmentValue(environment, "RUNNER_TEMP"),
			"tool_cache": environmentValue(environment, "RUNNER_TOOL_CACHE"),
		},
		"env": environmentContext(environment),
	}
	for name, value := range extra {
		values[name] = value
	}
	return workflowexpression.Context{Availability: availability, Values: values, Status: status}
}

func secretContext(token string, secrets map[string]string) map[string]any {
	values := make(map[string]any, len(secrets)+1)
	for name, value := range secrets {
		values[name] = workflowexpression.Secret(value)
	}
	values["GITHUB_TOKEN"] = workflowexpression.Secret(token)
	return values
}

func githubExpressionEvent(plan *Plan) map[string]any {
	if plan.eventPayload != nil {
		return plan.eventPayload
	}
	result := map[string]any{"action": plan.Event.Action}
	if len(plan.Inputs) > 0 {
		result["inputs"] = eventInputValues(plan.Inputs)
	}
	if plan.Event.Schedule != "" {
		result["schedule"] = plan.Event.Schedule
	}
	switch plan.Event.Name {
	case "push":
		result["after"] = plan.Revision.SHA
		result["ref"] = plan.Revision.Ref
	case "pull_request", "pull_request_target", "pull_request_review", "pull_request_review_comment":
		if pullRequest := githubPullRequestEvent(plan); pullRequest != nil {
			result["pull_request"] = pullRequest
		}
	case "merge_group":
		result["merge_group"] = map[string]any{
			"head_sha": plan.Revision.SHA,
			"head_ref": plan.Revision.Ref,
		}
	}
	if plan.Event.WorkflowRun != nil {
		result["workflow_run"] = map[string]any{"conclusion": plan.Event.WorkflowRun.Conclusion, "head_sha": plan.Event.WorkflowRun.HeadSHA}
	}
	if plan.Event.Issue != nil {
		result["issue"] = map[string]any{"number": plan.Event.Issue.Number, "body": plan.Event.Issue.Body}
	}
	if plan.Event.Comment != nil {
		result["comment"] = map[string]any{"body": plan.Event.Comment.Body}
	}
	if plan.Event.Review != nil {
		result["review"] = map[string]any{"body": plan.Event.Review.Body}
	}
	if plan.Event.Name == "release" {
		result["release"] = map[string]any{"tag_name": plan.Revision.RefName}
	}
	return result
}

func environmentContext(environment []string) map[string]any {
	result := make(map[string]any, len(environment))
	for _, entry := range environment {
		name, value, found := strings.Cut(entry, "=")
		if found {
			result[name] = value
		}
	}
	return result
}

func stringMapValue(values map[string]string, name string) (string, bool) {
	if value, found := values[name]; found {
		return value, true
	}
	for candidate, value := range values {
		if strings.EqualFold(candidate, name) {
			return value, true
		}
	}
	return "", false
}

func anyMapValue(values map[string]any, name string) (any, bool) {
	if value, found := values[name]; found {
		return value, true
	}
	for candidate, value := range values {
		if strings.EqualFold(candidate, name) {
			return value, true
		}
	}
	return nil, false
}

func resolveExpressionMap(values map[string]string, context workflowexpression.Context) (map[string]string, error) {
	if len(values) == 0 {
		return nil, nil
	}
	result := make(map[string]string, len(values))
	for name, input := range values {
		resolved, err := resolveMapExpression(input, context)
		if err != nil {
			return nil, fmt.Errorf("value %q: %w", name, err)
		}
		result[name] = resolved
	}
	return result, nil
}

func resolveMapExpression(input string, context workflowexpression.Context) (string, error) {
	resolved, err := resolveExpressionString(input, context)
	if err != nil {
		return "", err
	}
	if len(resolved) > workflow.MaxMapValueBytes {
		return "", fmt.Errorf("evaluated value exceeds %d bytes", workflow.MaxMapValueBytes)
	}
	return resolved, nil
}

func resolvedMapBytes(field string, values map[string]string) (int, error) {
	total := 0
	for name, value := range values {
		if len(value) > workflow.MaxMapValueBytes {
			return 0, fmt.Errorf("%s value %q exceeds %d bytes", field, name, workflow.MaxMapValueBytes)
		}
		total += len(name) + len(value)
	}
	return total, nil
}

func resolvedStepBytes(step Step) (int, error) {
	if utf8.RuneCountInString(step.Name) > workflow.MaxStepNameLength {
		return 0, fmt.Errorf("evaluated name exceeds %d characters", workflow.MaxStepNameLength)
	}
	if utf8.RuneCountInString(step.Uses) > workflow.MaxActionReferenceLength {
		return 0, fmt.Errorf("evaluated action reference exceeds %d characters", workflow.MaxActionReferenceLength)
	}
	if len(step.Run) > workflow.MaxRunScriptBytes {
		return 0, fmt.Errorf("evaluated run script exceeds %d bytes", workflow.MaxRunScriptBytes)
	}
	if utf8.RuneCountInString(step.WorkingDirectory) > workflow.MaxWorkingDirectoryLength {
		return 0, fmt.Errorf("evaluated working-directory exceeds %d characters", workflow.MaxWorkingDirectoryLength)
	}
	withBytes, err := resolvedMapBytes("evaluated with", step.With)
	if err != nil {
		return 0, err
	}
	environmentBytes, err := resolvedMapBytes("evaluated environment", step.Env)
	if err != nil {
		return 0, err
	}
	return len(step.Name) + len(step.Uses) + len(step.Run) + len(step.WorkingDirectory) + len(step.If) + withBytes + environmentBytes, nil
}

func resolveExpressionString(input string, context workflowexpression.Context) (string, error) {
	program, err := workflowexpression.Parse(input)
	if err != nil {
		return "", err
	}
	result, err := program.Evaluate(context)
	if err != nil {
		return "", err
	}
	return result.String()
}

func evaluateCondition(input string, context workflowexpression.Context, successful bool) (bool, error) {
	program, err := workflowexpression.ParseCondition(input)
	if err != nil {
		return false, err
	}
	if !program.UsesStatusFunction() && !successful {
		return false, nil
	}
	result, err := program.Evaluate(context)
	if err != nil {
		return false, err
	}
	return result.Bool(), nil
}
