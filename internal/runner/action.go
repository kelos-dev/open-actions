package runner

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/kelos-dev/open-actions/internal/actionref"
	"gopkg.in/yaml.v3"
)

type actionDefinition struct {
	Inputs  map[string]actionInput  `yaml:"inputs"`
	Outputs map[string]actionOutput `yaml:"outputs"`
	Runs    actionRuns              `yaml:"runs"`
}

type actionInput struct {
	Default any `yaml:"default"`
}

type actionOutput struct {
	Value any `yaml:"value"`
}

type actionRuns struct {
	Using  string          `yaml:"using"`
	Pre    string          `yaml:"pre"`
	PreIf  string          `yaml:"pre-if"`
	Main   string          `yaml:"main"`
	Post   string          `yaml:"post"`
	PostIf string          `yaml:"post-if"`
	Steps  []compositeStep `yaml:"steps"`
}

type actionResolver struct {
	cloneBaseURL  string
	directory     string
	environment   []string
	execute       func(context.Context, string, []string, string, []string) error
	cache         map[string]string
	nextDirectory int
}

type actionInvocation struct {
	step       Step
	reference  actionref.Reference
	directory  string
	definition actionDefinition
	executable string
	inputs     map[string]string
	state      map[string]string
	outputs    map[string]string
}

func newActionResolver(cloneBaseURL, directory string, environment []string, executeCommand func(context.Context, string, []string, string, []string) error) *actionResolver {
	return &actionResolver{
		cloneBaseURL: strings.TrimSuffix(cloneBaseURL, "/"),
		directory:    directory,
		environment:  environment,
		execute:      executeCommand,
		cache:        map[string]string{},
	}
}

func (r *actionResolver) prepare(ctx context.Context, step Step, plan *Plan, token string) (*actionInvocation, error) {
	reference, err := actionref.Parse(step.Uses)
	if err != nil {
		return nil, err
	}
	repositoryKey := reference.Owner + "/" + reference.Repository + "@" + reference.Ref
	repositoryDirectory, found := r.cache[repositoryKey]
	if !found {
		r.nextDirectory++
		repositoryDirectory = filepath.Join(r.directory, strconv.Itoa(r.nextDirectory))
		cloneToken := actionCloneToken(reference, plan, token)
		if err := r.download(ctx, reference, repositoryDirectory, cloneToken); err != nil {
			if cleanupErr := os.RemoveAll(repositoryDirectory); cleanupErr != nil {
				return nil, errors.Join(err, fmt.Errorf("clean failed action download: %w", cleanupErr))
			}
			return nil, err
		}
		r.cache[repositoryKey] = repositoryDirectory
	}
	actionDirectory, err := withinDirectory(repositoryDirectory, reference.Path)
	if err != nil {
		return nil, err
	}
	definition, err := loadActionDefinition(actionDirectory)
	if err != nil {
		return nil, fmt.Errorf("load action %s: %w", step.Uses, err)
	}
	switch definition.Runs.Using {
	case "node20", "node24":
		if definition.Runs.Main == "" {
			return nil, fmt.Errorf("action %s does not define runs.main", step.Uses)
		}
		if !supportedActionCondition(definition.Runs.PreIf) || !supportedActionCondition(definition.Runs.PostIf) {
			return nil, fmt.Errorf("action %s uses an unsupported lifecycle condition", step.Uses)
		}
	case "composite":
		if err := validateComposite(definition); err != nil {
			return nil, fmt.Errorf("action %s: %w", step.Uses, err)
		}
	default:
		return nil, fmt.Errorf("action %s uses unsupported runtime %q", step.Uses, definition.Runs.Using)
	}
	inputs, err := actionInputs(definition.Inputs, step.With, plan, token)
	if err != nil {
		return nil, fmt.Errorf("configure action %s: %w", step.Uses, err)
	}
	return &actionInvocation{
		step:       step,
		reference:  reference,
		directory:  actionDirectory,
		definition: definition,
		inputs:     inputs,
		state:      map[string]string{},
		outputs:    map[string]string{},
	}, nil
}

func actionCloneToken(reference actionref.Reference, plan *Plan, token string) string {
	if strings.EqualFold(reference.Owner, plan.Repository.Owner) && strings.EqualFold(reference.Repository, plan.Repository.Name) {
		return token
	}
	return ""
}

func (r *actionResolver) download(ctx context.Context, reference actionref.Reference, directory, token string) error {
	remoteURL := r.cloneBaseURL + "/" + url.PathEscape(reference.Owner) + "/" + url.PathEscape(reference.Repository)
	environment := actionDownloadEnvironment(r.environment, remoteURL, token)
	commands := [][]string{
		{"git", "init", "--quiet", directory},
		{"git", "-C", directory, "remote", "add", "origin", remoteURL},
		{"git", "-C", directory, "fetch", "--quiet", "--depth=1", "origin", reference.Ref},
		{"git", "-C", directory, "checkout", "--quiet", "--detach", "FETCH_HEAD"},
	}
	for _, command := range commands {
		if err := r.execute(ctx, command[0], command[1:], r.directory, environment); err != nil {
			return fmt.Errorf("download action %s/%s@%s: %w", reference.Owner, reference.Repository, reference.Ref, err)
		}
	}
	return nil
}

func actionDownloadEnvironment(environment []string, remoteURL, token string) []string {
	if token == "" {
		return environment
	}
	credential := base64.StdEncoding.EncodeToString([]byte("x-access-token:" + token))
	environment = setEnvironment(environment, "GIT_CONFIG_COUNT", "1")
	environment = setEnvironment(environment, "GIT_CONFIG_KEY_0", "http."+remoteURL+".extraHeader")
	return setEnvironment(environment, "GIT_CONFIG_VALUE_0", "AUTHORIZATION: basic "+credential)
}

func loadActionDefinition(directory string) (actionDefinition, error) {
	var data []byte
	var err error
	for _, name := range []string{"action.yml", "action.yaml"} {
		data, err = os.ReadFile(filepath.Join(directory, name))
		if err == nil {
			break
		}
		if !os.IsNotExist(err) {
			return actionDefinition{}, err
		}
	}
	if err != nil {
		return actionDefinition{}, fmt.Errorf("find action.yml or action.yaml: %w", err)
	}
	definition := actionDefinition{}
	if err := yaml.Unmarshal(data, &definition); err != nil {
		return actionDefinition{}, fmt.Errorf("decode action metadata: %w", err)
	}
	return definition, nil
}

func actionInputs(definitions map[string]actionInput, supplied map[string]string, plan *Plan, token string) (map[string]string, error) {
	result := make(map[string]string, len(definitions)+len(supplied))
	definitionNames := make(map[string]string, len(definitions))
	for name, definition := range definitions {
		canonicalName := strings.ToLower(name)
		if existing, found := definitionNames[canonicalName]; found && existing != name {
			return nil, fmt.Errorf("action defines input names %q and %q that differ only by case", existing, name)
		}
		definitionNames[canonicalName] = name
		if definition.Default != nil {
			value, err := inputString(definition.Default)
			if err != nil {
				return nil, fmt.Errorf("input %q default: %w", name, err)
			}
			resolved, err := resolveActionDefault(value, plan, token)
			if err != nil {
				return nil, fmt.Errorf("input %q default: %w", name, err)
			}
			result[canonicalName] = resolved
		}
	}
	suppliedNames := make(map[string]string, len(supplied))
	for name, value := range supplied {
		canonicalName := strings.ToLower(name)
		if existing, found := suppliedNames[canonicalName]; found && existing != name {
			return nil, fmt.Errorf("workflow supplies input names %q and %q that differ only by case", existing, name)
		}
		suppliedNames[canonicalName] = name
		result[canonicalName] = value
	}
	return result, nil
}

func inputString(value any) (string, error) {
	switch typed := value.(type) {
	case string:
		return typed, nil
	case bool, int, int64, uint64, float64:
		return fmt.Sprint(typed), nil
	default:
		return "", fmt.Errorf("value must be a scalar")
	}
}

func resolveActionDefault(value string, plan *Plan, token string) (string, error) {
	switch strings.TrimSpace(value) {
	case "${{ github.repository }}":
		return plan.Repository.Owner + "/" + plan.Repository.Name, nil
	case "${{ github.token }}":
		return token, nil
	case "${{ github.server_url }}":
		return strings.TrimSuffix(plan.Repository.ServerURL, "/"), nil
	case "${{ github.server_url == 'https://github.com' && github.token || '' }}":
		if strings.TrimSuffix(plan.Repository.ServerURL, "/") == "https://github.com" {
			return token, nil
		}
		return "", nil
	default:
		if strings.Contains(value, "${{") {
			return "", fmt.Errorf("unsupported expression %q", value)
		}
		return value, nil
	}
}

func (e *Executor) runJavaScriptHook(ctx context.Context, invocation *actionInvocation, hook, entrypoint, temporaryDirectory, workspace string, environment *[]string) error {
	if entrypoint == "" {
		return nil
	}
	if hook == "pre" && !matchesPreCondition(invocation.definition.Runs.PreIf) {
		return nil
	}
	entrypointPath, err := withinDirectory(invocation.directory, entrypoint)
	if err != nil {
		return err
	}
	if _, err := os.Stat(entrypointPath); err != nil {
		return fmt.Errorf("find %s entrypoint: %w", hook, err)
	}
	files, err := newCommandFiles(filepath.Join(temporaryDirectory, "commands"), fmt.Sprintf("%d-%s", e.commandID.Add(1), hook))
	if err != nil {
		return err
	}
	actionEnvironment := append([]string(nil), (*environment)...)
	actionEnvironment = appendEnvironment(actionEnvironment, invocation.step.Env)
	actionEnvironment = setEnvironment(actionEnvironment, "GITHUB_ACTION_PATH", invocation.directory)
	actionEnvironment = setEnvironment(actionEnvironment, "GITHUB_ACTION_REPOSITORY", invocation.reference.Owner+"/"+invocation.reference.Repository)
	actionEnvironment = files.environmentVariables(actionEnvironment)
	for name, value := range invocation.inputs {
		actionEnvironment = setEnvironment(actionEnvironment, "INPUT_"+strings.ToUpper(strings.ReplaceAll(name, " ", "_")), value)
	}
	if hook == "post" {
		for name, value := range invocation.state {
			actionEnvironment = setEnvironment(actionEnvironment, "STATE_"+name, value)
		}
	}
	executionError := e.executeCommand(ctx, invocation.executable, []string{entrypointPath}, workspace, actionEnvironment)
	updates, commandError := files.read()
	if commandError != nil {
		return errors.Join(executionError, commandError)
	}
	applyEnvironmentUpdates(environment, updates)
	for name, value := range updates.state {
		invocation.state[name] = value
	}
	if hook == "main" {
		for name, value := range updates.outputs {
			invocation.outputs[name] = value
		}
	}
	return executionError
}

func (e *Executor) executeAction(ctx context.Context, state *executionState, step Step, depth int) (map[string]string, error) {
	invocation, err := state.resolver.prepare(ctx, step, state.plan, e.githubToken)
	if err != nil {
		return nil, err
	}
	e.logger.Info("prepared external action", "action", step.Uses, "runtime", invocation.definition.Runs.Using)
	switch invocation.definition.Runs.Using {
	case "node20", "node24":
		executable, err := actionRuntimeExecutable(invocation.definition.Runs.Using, e.environment)
		if err != nil {
			return nil, fmt.Errorf("action %s: %w", step.Uses, err)
		}
		invocation.executable = executable
		if invocation.definition.Runs.Post != "" {
			state.posts = append(state.posts, invocation)
		}
		if err := e.runJavaScriptHook(ctx, invocation, "pre", invocation.definition.Runs.Pre, state.temporaryDirectory, state.workspace, &state.environment); err != nil {
			return nil, err
		}
		if err := e.runJavaScriptHook(ctx, invocation, "main", invocation.definition.Runs.Main, state.temporaryDirectory, state.workspace, &state.environment); err != nil {
			return nil, err
		}
		return invocation.outputs, nil
	case "composite":
		return e.runComposite(ctx, state, invocation, depth+1)
	default:
		return nil, fmt.Errorf("action %s uses unsupported runtime %q", step.Uses, invocation.definition.Runs.Using)
	}
}

func actionRuntimeExecutable(runtime string, environment []string) (string, error) {
	executable := "node"
	if runtime == "node24" {
		executable = "node24"
	}
	for _, directory := range filepath.SplitList(environmentValue(environment, "PATH")) {
		if directory == "" {
			directory = "."
		}
		path := filepath.Join(directory, executable)
		info, err := os.Stat(path)
		if err == nil && !info.IsDir() && info.Mode()&0o111 != 0 {
			absolutePath, err := filepath.Abs(path)
			if err != nil {
				return "", fmt.Errorf("resolve %s action runtime executable %q: %w", runtime, executable, err)
			}
			return absolutePath, nil
		}
	}
	return "", fmt.Errorf("%s action runtime is unavailable: executable %q was not found in PATH", runtime, executable)
}

func matchesPreCondition(condition string) bool {
	switch strings.TrimSpace(condition) {
	case "", "always()", "success()":
		return true
	default:
		return false
	}
}

func supportedActionCondition(condition string) bool {
	switch strings.TrimSpace(condition) {
	case "", "always()", "success()", "failure()":
		return true
	default:
		return false
	}
}
