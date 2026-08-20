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
	workflowexpression "github.com/kelos-dev/open-actions/internal/expression"
	"github.com/kelos-dev/open-actions/internal/workflow"
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

func (e *Executor) runComposite(ctx context.Context, state *executionState, invocation *actionInvocation, cancelled bool) (map[string]string, error) {
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
		cancelled = cancelled || ctx.Err() != nil
		runStep, err := compositeCondition(rawStep.If, rawStep.Env, compositeFailed, cancelled, compositeContext)
		if err != nil {
			return nil, fmt.Errorf("composite step %d condition: %w", index+1, err)
		}
		if !runStep {
			continue
		}
		stepEnvironment, err := resolveCompositeMap(rawStep.Env, compositeContext)
		if err != nil {
			return nil, fmt.Errorf("composite step %d environment: %w", index+1, err)
		}
		stepContext := compositeContextWithEnvironment(compositeContext, stepEnvironment)
		step, continueOnError, err := resolveCompositeStep(rawStep, stepEnvironment, stepContext)
		if err != nil {
			return nil, fmt.Errorf("composite step %d: %w", index+1, err)
		}
		stepBytes, err := resolvedStepBytes(step)
		if err != nil {
			return nil, fmt.Errorf("composite step %d: %w", index+1, err)
		}
		if state.resolvedContent+stepBytes > workflow.MaxJobContentBytes {
			return nil, fmt.Errorf("composite step %d: evaluated job configuration exceeds %d bytes", index+1, workflow.MaxJobContentBytes)
		}
		state.resolvedContent += stepBytes
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
		cancelledBeforeCommand := ctx.Err() != nil
		commandContext := state.commandContext(ctx)
		if step.Uses != "" {
			outputs, err = e.executeAction(commandContext, state, step, cancelled)
		} else {
			outputs, err = e.runCompositeScript(commandContext, state, invocation, step)
		}
		if rawStep.ID != "" && outputs != nil {
			compositeContext.stepOutput[rawStep.ID] = outputs
		}
		if err != nil {
			if continueOnError {
				e.logger.Warn("composite step failed with continue-on-error", "action", invocation.step.Uses, "step", index+1, "name", name, "error", e.masker.mask(err.Error()))
			} else {
				cancelledDuringCommand := !cancelledBeforeCommand && ctx.Err() != nil
				if !cancelledDuringCommand {
					compositeFailed = true
				}
				compositeError = errors.Join(compositeError, fmt.Errorf("composite step %d (%s): %w", index+1, name, err))
			}
		}
		e.logger.Info("completed composite step", "action", invocation.step.Uses, "step", index+1, "name", name)
	}
	outputs := map[string]string{}
	for name, definition := range invocation.definition.Outputs {
		if definition.Value == nil {
			outputs[name] = ""
			continue
		}
		value, err := inputString(definition.Value)
		if err != nil {
			return outputs, errors.Join(compositeError, fmt.Errorf("composite output %q: %w", name, err))
		}
		value, err = resolveCompositeExpressions(value, compositeContext)
		if err != nil {
			return outputs, errors.Join(compositeError, fmt.Errorf("composite output %q: %w", name, err))
		}
		outputs[name] = value
	}
	return outputs, compositeError
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
	executionError := e.executeCommandWithFiles(ctx, "/usr/bin/env", []string{"bash", "-e", "-o", "pipefail", "-c", step.Run}, directory, environment, &files, state.workspace)
	updates, commandError := files.read()
	if commandError != nil {
		return nil, errors.Join(executionError, commandError)
	}
	applyEnvironmentUpdates(&state.environment, updates)
	e.logCommandNames("workflow step output", updates.outputs)
	return updates.outputs, executionError
}

func resolveCompositeStep(step compositeStep, environment map[string]string, compositeContext *compositeContext) (Step, bool, error) {
	resolved := Step{Name: step.Name, Uses: step.Uses, Run: step.Run, WorkingDirectory: step.WorkingDirectory}
	var err error
	for _, field := range []struct {
		name   string
		target *string
	}{
		{name: "name", target: &resolved.Name},
		{name: "run", target: &resolved.Run},
		{name: "working-directory", target: &resolved.WorkingDirectory},
	} {
		*field.target, err = resolveCompositeExpressions(*field.target, compositeContext)
		if err != nil {
			return Step{}, false, fmt.Errorf("resolve %s: %w", field.name, err)
		}
	}
	resolved.With, err = resolveCompositeMap(step.With, compositeContext)
	if err != nil {
		return Step{}, false, fmt.Errorf("resolve with: %w", err)
	}
	resolved.Env = environment
	continueOnError, err := resolveCompositeBoolean(step.ContinueOnError, compositeContext)
	if err != nil {
		return Step{}, false, fmt.Errorf("resolve continue-on-error: %w", err)
	}
	return resolved, continueOnError, nil
}

func compositeContextWithEnvironment(base *compositeContext, environment map[string]string) *compositeContext {
	result := *base
	result.scopedEnvironment = mergeEnvironment(base.scopedEnvironment, environment)
	return &result
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
		if len(value) > workflow.MaxMapValueBytes {
			return nil, fmt.Errorf("value %q exceeds %d evaluated bytes", name, workflow.MaxMapValueBytes)
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
		return false, errors.New("value must evaluate to a boolean")
	}
	return boolean, nil
}

func compositeCondition(value string, environment map[string]any, failed, cancelled bool, compositeContext *compositeContext) (bool, error) {
	if strings.TrimSpace(value) == "" {
		return !failed && !cancelled, nil
	}
	status := workflowStepStatus(failed, cancelled)
	context := compositeExpressionContext(compositeContext, compositeConditionAvailability, &status)
	baseEnvironment := context.Values["env"].(map[string]any)
	resolveEnvironment := func(name string) (any, bool, error) {
		if rawValue, found := anyMapValue(environment, name); found {
			input, err := inputString(rawValue)
			if err != nil {
				return nil, true, err
			}
			resolved, err := resolveCompositeExpressions(input, compositeContext)
			return resolved, true, err
		}
		resolved, found := anyMapValue(baseEnvironment, name)
		return resolved, found, nil
	}
	allEnvironment := func() (map[string]any, error) {
		result := make(map[string]any, len(baseEnvironment)+len(environment))
		for name, value := range baseEnvironment {
			result[name] = value
		}
		for name, rawValue := range environment {
			input, err := inputString(rawValue)
			if err != nil {
				return nil, err
			}
			resolved, err := resolveCompositeExpressions(input, compositeContext)
			if err != nil {
				return nil, err
			}
			result[name] = resolved
		}
		return result, nil
	}
	context.Values["env"] = workflowexpression.DeferredObjectMap{Resolve: resolveEnvironment, Values: allEnvironment}
	return evaluateCondition(value, context, status.Success)
}

func resolveCompositeExpressions(value string, compositeContext *compositeContext) (string, error) {
	context := compositeExpressionContext(compositeContext, compositeAvailability, nil)
	return resolveExpressionString(value, context)
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

func actionKey(reference actionref.Reference) string {
	return reference.Owner + "/" + reference.Repository + "/" + reference.Path + "@" + reference.Ref
}

var (
	compositeStepIDPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_-]*$`)
)
