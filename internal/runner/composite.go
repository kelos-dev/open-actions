package runner

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"github.com/kelos-dev/open-actions/internal/actionref"
)

const (
	maxCompositeDepth = 10
	maxCompositeSteps = 1000
)

type compositeStep struct {
	ID               string         `yaml:"id"`
	Name             string         `yaml:"name"`
	Uses             string         `yaml:"uses"`
	Run              string         `yaml:"run"`
	Shell            string         `yaml:"shell"`
	WorkingDirectory string         `yaml:"working-directory"`
	With             map[string]any `yaml:"with"`
	Env              map[string]any `yaml:"env"`
	If               string         `yaml:"if"`
	ContinueOnError  any            `yaml:"continue-on-error"`
}

type compositeContext struct {
	inputs            map[string]string
	stepOutput        map[string]map[string]string
	actionPath        string
	scopedEnvironment map[string]string
	state             *executionState
}

func validateComposite(definition actionDefinition) error {
	if definition.Runs.Main != "" || definition.Runs.Pre != "" || definition.Runs.Post != "" {
		return errors.New("composite action must define runs.steps instead of JavaScript entrypoints")
	}
	if len(definition.Runs.Steps) == 0 {
		return errors.New("composite action must define at least one step")
	}
	if len(definition.Runs.Steps) > maxCompositeSteps {
		return fmt.Errorf("composite action defines %d steps; maximum is %d", len(definition.Runs.Steps), maxCompositeSteps)
	}
	ids := map[string]bool{}
	for index, step := range definition.Runs.Steps {
		if (step.Uses == "") == (step.Run == "") {
			return fmt.Errorf("composite step %d must define exactly one of uses or run", index+1)
		}
		if step.ID != "" {
			if !compositeStepIDPattern.MatchString(step.ID) {
				return fmt.Errorf("composite step %d has invalid id %q", index+1, step.ID)
			}
			if ids[step.ID] {
				return fmt.Errorf("composite step id %q is duplicated", step.ID)
			}
			ids[step.ID] = true
		}
		if step.Run != "" {
			if step.Shell != "bash" {
				return fmt.Errorf("composite run step %d uses unsupported shell %q", index+1, step.Shell)
			}
		} else {
			if _, err := actionref.Parse(step.Uses); err != nil {
				return fmt.Errorf("composite action step %d: %w", index+1, err)
			}
			if step.Shell != "" || step.WorkingDirectory != "" {
				return fmt.Errorf("composite action step %d configures run-only fields", index+1)
			}
		}
	}
	return nil
}

func (e *Executor) runComposite(ctx context.Context, state *executionState, invocation *actionInvocation, depth int) (map[string]string, error) {
	if depth > maxCompositeDepth {
		return nil, fmt.Errorf("composite action nesting exceeds %d levels", maxCompositeDepth)
	}
	key := compositeKey(invocation)
	if state.compositeStack[key] {
		return nil, fmt.Errorf("composite action cycle detected at %s", invocation.step.Uses)
	}
	state.compositeStack[key] = true
	defer delete(state.compositeStack, key)

	compositeContext := &compositeContext{
		inputs:            invocation.inputs,
		stepOutput:        map[string]map[string]string{},
		actionPath:        invocation.directory,
		scopedEnvironment: invocation.step.Env,
		state:             state,
	}
	compositeFailed := false
	var compositeError error
	for index, rawStep := range invocation.definition.Runs.Steps {
		runStep, err := compositeCondition(rawStep.If, !compositeFailed, compositeContext)
		if err != nil {
			return nil, fmt.Errorf("composite step %d condition: %w", index+1, err)
		}
		if !runStep {
			continue
		}
		step, continueOnError, err := resolveCompositeStep(rawStep, compositeContext)
		if err != nil {
			return nil, fmt.Errorf("composite step %d: %w", index+1, err)
		}
		step.Env = mergeEnvironment(invocation.step.Env, step.Env)
		name := step.Name
		if name == "" {
			name = step.Uses
		}
		if name == "" {
			name = fmt.Sprintf("run step %d", index+1)
		}
		e.logger.Info("starting composite step", "action", invocation.step.Uses, "step", index+1, "name", name)
		var outputs map[string]string
		if step.Uses != "" {
			outputs, err = e.executeAction(ctx, state, step, depth)
		} else {
			outputs, err = e.runCompositeScript(ctx, state, invocation, step)
		}
		if rawStep.ID != "" && outputs != nil {
			compositeContext.stepOutput[rawStep.ID] = outputs
		}
		if err != nil {
			if continueOnError {
				e.logger.Warn("composite step failed with continue-on-error", "action", invocation.step.Uses, "step", index+1, "name", name, "error", e.masker.mask(err.Error()))
				continue
			}
			compositeFailed = true
			compositeError = errors.Join(compositeError, fmt.Errorf("composite step %d (%s): %w", index+1, name, err))
			continue
		}
		e.logger.Info("completed composite step", "action", invocation.step.Uses, "step", index+1, "name", name)
	}
	if compositeError != nil {
		return nil, compositeError
	}
	outputs := map[string]string{}
	for name, definition := range invocation.definition.Outputs {
		if definition.Value == nil {
			outputs[name] = ""
			continue
		}
		value, err := inputString(definition.Value)
		if err != nil {
			return nil, fmt.Errorf("composite output %q: %w", name, err)
		}
		value, err = resolveCompositeExpressions(value, compositeContext)
		if err != nil {
			return nil, fmt.Errorf("composite output %q: %w", name, err)
		}
		outputs[name] = value
	}
	return outputs, nil
}

func (e *Executor) runCompositeScript(ctx context.Context, state *executionState, invocation *actionInvocation, step Step) (map[string]string, error) {
	directory, err := withinDirectory(state.workspace, step.WorkingDirectory)
	if err != nil {
		return nil, err
	}
	files, err := newCommandFiles(filepath.Join(state.temporaryDirectory, "commands"), fmt.Sprintf("%d-composite", e.commandID.Add(1)))
	if err != nil {
		return nil, err
	}
	environment := appendEnvironment(append([]string(nil), state.environment...), invocation.step.Env)
	environment = appendEnvironment(environment, step.Env)
	environment = setEnvironment(environment, "GITHUB_ACTION_PATH", invocation.directory)
	environment = setEnvironment(environment, "GITHUB_ACTION_REPOSITORY", invocation.reference.Owner+"/"+invocation.reference.Repository)
	environment = files.environmentVariables(environment)
	executionError := e.executeCommand(ctx, "/usr/bin/env", []string{"bash", "-e", "-o", "pipefail", "-c", step.Run}, directory, environment)
	updates, commandError := files.read()
	if commandError != nil {
		return nil, errors.Join(executionError, commandError)
	}
	applyEnvironmentUpdates(&state.environment, updates)
	return updates.outputs, executionError
}

func resolveCompositeStep(step compositeStep, compositeContext *compositeContext) (Step, bool, error) {
	resolved := Step{Name: step.Name, Uses: step.Uses, Run: step.Run, WorkingDirectory: step.WorkingDirectory}
	var err error
	for field, value := range map[string]*string{
		"name": &resolved.Name, "uses": &resolved.Uses, "run": &resolved.Run, "working-directory": &resolved.WorkingDirectory,
	} {
		*value, err = resolveCompositeExpressions(*value, compositeContext)
		if err != nil {
			return Step{}, false, fmt.Errorf("resolve %s: %w", field, err)
		}
	}
	resolved.With, err = resolveCompositeMap(step.With, compositeContext)
	if err != nil {
		return Step{}, false, fmt.Errorf("resolve with: %w", err)
	}
	resolved.Env, err = resolveCompositeMap(step.Env, compositeContext)
	if err != nil {
		return Step{}, false, fmt.Errorf("resolve env: %w", err)
	}
	continueOnError, err := resolveCompositeBoolean(step.ContinueOnError, compositeContext)
	if err != nil {
		return Step{}, false, fmt.Errorf("resolve continue-on-error: %w", err)
	}
	return resolved, continueOnError, nil
}

func resolveCompositeMap(values map[string]any, compositeContext *compositeContext) (map[string]string, error) {
	if len(values) == 0 {
		return nil, nil
	}
	result := make(map[string]string, len(values))
	for name, rawValue := range values {
		value, err := inputString(rawValue)
		if err != nil {
			return nil, fmt.Errorf("value %q: %w", name, err)
		}
		value, err = resolveCompositeExpressions(value, compositeContext)
		if err != nil {
			return nil, fmt.Errorf("value %q: %w", name, err)
		}
		result[name] = value
	}
	return result, nil
}

func resolveCompositeBoolean(value any, compositeContext *compositeContext) (bool, error) {
	if value == nil {
		return false, nil
	}
	if boolean, ok := value.(bool); ok {
		return boolean, nil
	}
	text, err := inputString(value)
	if err != nil {
		return false, err
	}
	text, err = resolveCompositeExpressions(text, compositeContext)
	if err != nil {
		return false, err
	}
	boolean, err := strconv.ParseBool(strings.TrimSpace(text))
	if err != nil {
		return false, fmt.Errorf("value %q must evaluate to a boolean", text)
	}
	return boolean, nil
}

func compositeCondition(value string, successful bool, compositeContext *compositeContext) (bool, error) {
	condition := strings.TrimSpace(value)
	if strings.HasPrefix(condition, "${{") && strings.HasSuffix(condition, "}}") {
		condition = strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(condition, "${{"), "}}"))
	}
	switch condition {
	case "", "success()":
		return successful, nil
	case "always()":
		return true, nil
	case "failure()":
		return !successful, nil
	case "true":
		return true, nil
	case "false":
		return false, nil
	}
	resolved, err := resolveCompositeExpression(condition, compositeContext)
	if err != nil {
		return false, err
	}
	boolean, err := strconv.ParseBool(strings.TrimSpace(resolved))
	if err != nil {
		return false, fmt.Errorf("condition %q must evaluate to a boolean", value)
	}
	return boolean, nil
}

func resolveCompositeExpressions(value string, compositeContext *compositeContext) (string, error) {
	var expressionError error
	result := compositeExpressionPattern.ReplaceAllStringFunc(value, func(match string) string {
		expression := strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(match, "${{"), "}}"))
		resolved, err := resolveCompositeExpression(expression, compositeContext)
		if err != nil {
			expressionError = err
			return ""
		}
		return resolved
	})
	if expressionError != nil {
		return "", expressionError
	}
	if strings.Contains(result, "${{") {
		return "", fmt.Errorf("invalid composite expression in %q", value)
	}
	return result, nil
}

func resolveCompositeExpression(expression string, compositeContext *compositeContext) (string, error) {
	if name, found := strings.CutPrefix(expression, "inputs."); found {
		return compositeContext.inputs[strings.ToLower(name)], nil
	}
	if rest, found := strings.CutPrefix(expression, "steps."); found {
		id, output, found := strings.Cut(rest, ".outputs.")
		if !found || id == "" || output == "" {
			return "", fmt.Errorf("invalid steps expression %q", expression)
		}
		return compositeContext.stepOutput[id][output], nil
	}
	switch expression {
	case "github.action_path":
		return compositeContext.actionPath, nil
	case "github.workspace":
		return compositeContext.state.workspace, nil
	case "github.repository":
		return compositeContext.state.plan.Repository.Owner + "/" + compositeContext.state.plan.Repository.Name, nil
	case "github.sha":
		return compositeContext.state.plan.Revision.SHA, nil
	case "github.ref":
		return compositeContext.state.plan.Revision.Ref, nil
	case "github.ref_name":
		return compositeContext.state.plan.Revision.RefName, nil
	case "github.head_ref":
		return compositeContext.state.plan.Revision.HeadRef, nil
	case "github.event_name":
		return compositeContext.state.plan.Event.Name, nil
	case "github.workflow":
		return compositeContext.state.plan.WorkflowName, nil
	case "runner.os":
		return environmentValue(compositeContext.state.environment, "RUNNER_OS"), nil
	case "runner.arch":
		return environmentValue(compositeContext.state.environment, "RUNNER_ARCH"), nil
	case "runner.temp":
		return environmentValue(compositeContext.state.environment, "RUNNER_TEMP"), nil
	}
	if name, found := strings.CutPrefix(expression, "env."); found {
		environment := appendEnvironment(append([]string(nil), compositeContext.state.environment...), compositeContext.scopedEnvironment)
		return environmentValue(environment, name), nil
	}
	return "", fmt.Errorf("unsupported composite expression %q", expression)
}

func applyEnvironmentUpdates(environment *[]string, updates commandUpdates) {
	for name, value := range updates.environment {
		if reservedEnvironmentName(name) {
			continue
		}
		*environment = setEnvironment(*environment, name, value)
	}
	if len(updates.paths) == 0 {
		return
	}
	pathValue := environmentValue(*environment, "PATH")
	for _, value := range updates.paths {
		pathValue = value + string(os.PathListSeparator) + pathValue
	}
	*environment = setEnvironment(*environment, "PATH", pathValue)
}

func mergeEnvironment(base, override map[string]string) map[string]string {
	if len(base) == 0 && len(override) == 0 {
		return nil
	}
	result := make(map[string]string, len(base)+len(override))
	for name, value := range base {
		result[name] = value
	}
	for name, value := range override {
		result[name] = value
	}
	return result
}

func compositeKey(invocation *actionInvocation) string {
	return invocation.reference.Owner + "/" + invocation.reference.Repository + "/" + invocation.reference.Path + "@" + invocation.reference.Ref
}

var (
	compositeExpressionPattern = regexp.MustCompile(`\$\{\{[^{}]+}}`)
	compositeStepIDPattern     = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_-]*$`)
)
