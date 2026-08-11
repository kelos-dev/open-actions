package runner

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync/atomic"

	"github.com/kelos-dev/open-actions/internal/expression"
	"github.com/kelos-dev/open-actions/internal/workflow"
)

const (
	minimumPlanVersion = 1
	PlanVersion        = 3
	ContainerName      = "runner"
)

const runnerLogMarker = "open_actions_runner"

type Plan struct {
	Version      int               `json:"version"`
	Repository   Repository        `json:"repository"`
	Event        Event             `json:"event"`
	Revision     Revision          `json:"revision"`
	Inputs       map[string]any    `json:"inputs,omitempty"`
	WorkflowName string            `json:"workflowName"`
	JobID        string            `json:"jobID"`
	Env          map[string]string `json:"env,omitempty"`
	Steps        []Step            `json:"steps"`
}

type Repository struct {
	ID                 int64  `json:"id"`
	Owner              string `json:"owner"`
	Name               string `json:"name"`
	ServerURL          string `json:"serverURL"`
	APIURL             string `json:"apiURL"`
	ActionCloneBaseURL string `json:"actionCloneBaseURL"`
}

type Event struct {
	Name        string            `json:"name"`
	Action      string            `json:"action,omitempty"`
	DeliveryID  string            `json:"deliveryID,omitempty"`
	Schedule    string            `json:"schedule,omitempty"`
	PullRequest *PullRequest      `json:"pullRequest,omitempty"`
	WorkflowRun *WorkflowRunEvent `json:"workflowRun,omitempty"`
	Issue       *IssueEvent       `json:"issue,omitempty"`
	Comment     *CommentEvent     `json:"comment,omitempty"`
	Review      *ReviewEvent      `json:"review,omitempty"`
}

type PullRequest struct {
	Number         int64           `json:"number"`
	Body           string          `json:"body"`
	HTMLURL        string          `json:"htmlURL"`
	HeadRepository EventRepository `json:"headRepository"`
	HeadRef        string          `json:"headRef"`
	HeadSHA        string          `json:"headSHA"`
	BaseRef        string          `json:"baseRef"`
}

type WorkflowRunEvent struct {
	Conclusion string `json:"conclusion,omitempty"`
	HeadSHA    string `json:"headSHA"`
}

type IssueEvent struct {
	Number int64  `json:"number"`
	Body   string `json:"body"`
}

type CommentEvent struct {
	Body string `json:"body"`
}

type ReviewEvent struct {
	Body string `json:"body"`
}

type EventRepository struct {
	ID    int64  `json:"id"`
	Owner string `json:"owner"`
	Name  string `json:"name"`
}

type Revision struct {
	SHA     string `json:"sha"`
	Ref     string `json:"ref"`
	RefName string `json:"refName"`
	HeadRef string `json:"headRef,omitempty"`
	BaseRef string `json:"baseRef,omitempty"`
}

type Step struct {
	Name             string            `json:"name,omitempty"`
	Uses             string            `json:"uses,omitempty"`
	Run              string            `json:"run,omitempty"`
	WorkingDirectory string            `json:"workingDirectory,omitempty"`
	With             map[string]string `json:"with,omitempty"`
	Env              map[string]string `json:"env,omitempty"`
	If               string            `json:"if,omitempty"`
}

type ExecutorConfig struct {
	Logger      *slog.Logger
	GitHubToken string
	Environment []string
	Stdout      io.Writer
	Stderr      io.Writer
}

type Executor struct {
	logger      *slog.Logger
	githubToken string
	environment []string
	stdout      io.Writer
	stderr      io.Writer
	masker      *outputMasker
	commands    *workflowCommandState
	commandID   atomic.Uint64
}

type executionState struct {
	plan               *Plan
	workspace          string
	temporaryDirectory string
	environment        []string
	githubToken        string
	resolver           *actionResolver
	posts              []*actionInvocation
	compositeStack     map[string]bool
	resolvedContent    int
}

func NewExecutor(config ExecutorConfig) (*Executor, error) {
	if config.Logger == nil || config.GitHubToken == "" || config.Environment == nil || config.Stdout == nil || config.Stderr == nil {
		return nil, errors.New("runner executor configuration is incomplete")
	}
	masker := newOutputMasker(config.GitHubToken)
	masker.add(base64.StdEncoding.EncodeToString([]byte("x-access-token:" + config.GitHubToken)))
	return &Executor{
		logger:      config.Logger.With(runnerLogMarker, true),
		githubToken: config.GitHubToken,
		environment: append([]string(nil), config.Environment...),
		stdout:      config.Stdout,
		stderr:      config.Stderr,
		masker:      masker,
		commands:    newWorkflowCommandState(),
	}, nil
}

func LoadPlan(path string) (*Plan, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read job plan: %w", err)
	}
	plan := &Plan{}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(plan); err != nil {
		return nil, fmt.Errorf("decode job plan: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return nil, errors.New("decode job plan: trailing JSON value")
	}
	if plan.Version < minimumPlanVersion || plan.Version > PlanVersion {
		return nil, fmt.Errorf("unsupported job plan version %d", plan.Version)
	}
	if plan.Repository.ID < 1 || plan.Repository.Owner == "" || plan.Repository.Name == "" || plan.Repository.ServerURL == "" || plan.Repository.APIURL == "" || plan.Repository.ActionCloneBaseURL == "" || plan.Event.Name == "" || !validEventDeliveryID(plan.Event) || !validInputValues(plan.Inputs) || plan.Revision.SHA == "" || plan.Revision.Ref == "" || plan.Revision.RefName == "" || plan.WorkflowName == "" || plan.JobID == "" || len(plan.Steps) == 0 {
		return nil, errors.New("job plan is incomplete")
	}
	return plan, nil
}

func validInputValues(inputs map[string]any) bool {
	for _, value := range inputs {
		switch value.(type) {
		case string, bool, float64:
		default:
			return false
		}
	}
	return true
}

func validEventDeliveryID(event Event) bool {
	synthetic := event.Name == "workflow_dispatch" || event.Name == "schedule" || event.Name == "workflow_call"
	return synthetic == (event.DeliveryID == "")
}

func (e *Executor) Execute(ctx context.Context, plan *Plan, workspace string) error {
	err := e.executePlan(ctx, plan, workspace)
	if err == nil {
		return nil
	}
	return errors.New(e.masker.mask(err.Error()))
}

func (e *Executor) executePlan(ctx context.Context, plan *Plan, workspace string) error {
	if err := os.MkdirAll(workspace, 0o755); err != nil {
		return fmt.Errorf("create workspace: %w", err)
	}
	temporaryDirectory, err := os.MkdirTemp("", "open-actions-runner-")
	if err != nil {
		return fmt.Errorf("create runner temporary directory: %w", err)
	}
	defer os.RemoveAll(temporaryDirectory)
	for _, directory := range []string{"actions", "commands", "tool-cache", "temp"} {
		if err := os.MkdirAll(filepath.Join(temporaryDirectory, directory), 0o755); err != nil {
			return fmt.Errorf("create runner directory: %w", err)
		}
	}
	eventPath := filepath.Join(temporaryDirectory, "event.json")
	eventDocument, err := githubEventDocument(plan)
	if err != nil {
		return err
	}
	if err := os.WriteFile(eventPath, eventDocument, 0o600); err != nil {
		return fmt.Errorf("create event file: %w", err)
	}

	environment := append(append([]string(nil), e.environment...),
		"CI=true",
		"GITHUB_ACTIONS=true",
		"GITHUB_API_URL="+strings.TrimSuffix(plan.Repository.APIURL, "/"),
		"GITHUB_BASE_REF="+planPullRequestRefs(plan).base,
		"GITHUB_EVENT_ACTION="+plan.Event.Action,
		"GITHUB_EVENT_NAME="+plan.Event.Name,
		"GITHUB_EVENT_PATH="+eventPath,
		"GITHUB_HEAD_REF="+planPullRequestRefs(plan).head,
		"GITHUB_JOB="+plan.JobID,
		"GITHUB_REF="+plan.Revision.Ref,
		"GITHUB_REF_NAME="+plan.Revision.RefName,
		"GITHUB_REPOSITORY="+plan.Repository.Owner+"/"+plan.Repository.Name,
		"GITHUB_SERVER_URL="+strings.TrimSuffix(plan.Repository.ServerURL, "/"),
		"GITHUB_SHA="+plan.Revision.SHA,
		"GITHUB_WORKFLOW="+plan.WorkflowName,
		"GITHUB_WORKSPACE="+workspace,
		"RUNNER_ARCH="+runnerArchitecture(),
		"RUNNER_OS=Linux",
		"RUNNER_TEMP="+filepath.Join(temporaryDirectory, "temp"),
		"RUNNER_TOOL_CACHE="+filepath.Join(temporaryDirectory, "tool-cache"),
	)
	jobEnvironment, err := resolveJobEnvironment(plan.Env, plan, environment, e.githubToken)
	if err != nil {
		return fmt.Errorf("resolve job environment: %w", err)
	}
	jobContentBytes, err := resolvedMapBytes("evaluated job environment", jobEnvironment)
	if err != nil {
		return err
	}
	if jobContentBytes > workflow.MaxJobContentBytes {
		return fmt.Errorf("evaluated job configuration exceeds %d bytes", workflow.MaxJobContentBytes)
	}
	environment = appendEnvironment(environment, jobEnvironment)
	state := &executionState{
		plan:               plan,
		workspace:          workspace,
		temporaryDirectory: temporaryDirectory,
		environment:        environment,
		githubToken:        e.githubToken,
		resolver:           newActionResolver(plan.Repository.ActionCloneBaseURL, filepath.Join(temporaryDirectory, "actions"), environment, e.executeCommand),
		compositeStack:     map[string]bool{},
		resolvedContent:    jobContentBytes,
	}

	failed := false
	var executionErrors error
	for index, rawStep := range plan.Steps {
		status := workflowStepStatus(failed, ctx.Err() != nil)
		runStep, err := workflowStepCondition(rawStep.If, rawStep.Env, status, state)
		if err != nil {
			executionErrors = errors.Join(executionErrors, fmt.Errorf("step %d condition: %w", index+1, err))
			failed = true
			continue
		}
		if !runStep {
			e.logger.Info("skipping workflow step", "job", plan.JobID, "step", index+1, "name", rawStep.Name)
			continue
		}
		stepEnvironment, err := resolveWorkflowStepEnvironment(rawStep, state)
		if err != nil {
			executionErrors = errors.Join(executionErrors, fmt.Errorf("step %d resolve env: %w", index+1, err))
			failed = true
			continue
		}
		step, err := resolveWorkflowStep(rawStep, stepEnvironment, state)
		if err != nil {
			executionErrors = errors.Join(executionErrors, fmt.Errorf("step %d: %w", index+1, err))
			failed = true
			continue
		}
		stepBytes, err := resolvedStepBytes(step)
		if err != nil {
			executionErrors = errors.Join(executionErrors, fmt.Errorf("step %d: %w", index+1, err))
			failed = true
			continue
		}
		if state.resolvedContent+stepBytes > workflow.MaxJobContentBytes {
			executionErrors = errors.Join(executionErrors, fmt.Errorf("step %d: evaluated job configuration exceeds %d bytes", index+1, workflow.MaxJobContentBytes))
			failed = true
			continue
		}
		state.resolvedContent += stepBytes
		name := step.Name
		if name == "" {
			name = step.Uses
		}
		e.logger.Info("starting workflow step", "job", plan.JobID, "step", index+1, "name", name)
		var stepError error
		cancelledBeforeCommand := ctx.Err() != nil
		stepContext := executionContext(ctx)
		if step.Uses == "" {
			stepError = e.runScript(stepContext, state, step)
		} else {
			_, stepError = e.executeAction(stepContext, state, step, 0, cancelledBeforeCommand)
		}
		if stepError != nil {
			stepError = fmt.Errorf("step %d (%s): %w", index+1, name, stepError)
			executionErrors = errors.Join(executionErrors, stepError)
			cancelledDuringCommand := !cancelledBeforeCommand && ctx.Err() != nil
			if !cancelledDuringCommand {
				failed = true
			}
			continue
		}
		e.logger.Info("completed workflow step", "job", plan.JobID, "step", index+1, "name", name)
	}
	if ctx.Err() != nil {
		executionErrors = errors.Join(executionErrors, ctx.Err())
	}
	status := workflowStepStatus(failed, ctx.Err() != nil)
	return errors.Join(executionErrors, e.runPostActions(executionContext(ctx), state, status))
}

func executionContext(ctx context.Context) context.Context {
	if ctx.Err() == nil {
		return ctx
	}
	return context.WithoutCancel(ctx)
}

func workflowStepStatus(failed, cancelled bool) expression.Status {
	return expression.Status{Success: !failed && !cancelled, Failure: failed, Cancelled: cancelled}
}

func githubEventDocument(plan *Plan) ([]byte, error) {
	repository := map[string]any{
		"id":        plan.Repository.ID,
		"name":      plan.Repository.Name,
		"full_name": plan.Repository.Owner + "/" + plan.Repository.Name,
		"owner":     map[string]string{"login": plan.Repository.Owner},
	}
	document := map[string]any{"repository": repository}
	if plan.Event.Action != "" {
		document["action"] = plan.Event.Action
	}
	if len(plan.Inputs) > 0 {
		document["inputs"] = eventInputValues(plan.Inputs)
	}
	if plan.Event.Schedule != "" {
		document["schedule"] = plan.Event.Schedule
	}
	switch plan.Event.Name {
	case "push":
		document["after"] = plan.Revision.SHA
		document["ref"] = plan.Revision.Ref
	case "pull_request", "pull_request_target":
		if pullRequest := githubPullRequestEvent(plan); pullRequest != nil {
			document["pull_request"] = pullRequest
		}
	case "merge_group":
		document["merge_group"] = map[string]string{
			"head_sha": plan.Revision.SHA,
			"head_ref": plan.Revision.Ref,
		}
	case "workflow_run":
		if plan.Event.WorkflowRun != nil {
			document["workflow_run"] = map[string]any{"conclusion": plan.Event.WorkflowRun.Conclusion, "head_sha": plan.Event.WorkflowRun.HeadSHA}
		}
	case "issues":
		addIssueEvent(document, plan.Event)
	case "issue_comment":
		addIssueEvent(document, plan.Event)
		addCommentEvent(document, plan.Event)
	case "pull_request_review_comment":
		if pullRequest := githubPullRequestEvent(plan); pullRequest != nil {
			document["pull_request"] = pullRequest
		}
		addCommentEvent(document, plan.Event)
	case "pull_request_review":
		if pullRequest := githubPullRequestEvent(plan); pullRequest != nil {
			document["pull_request"] = pullRequest
		}
		if plan.Event.Review != nil {
			document["review"] = map[string]any{"body": plan.Event.Review.Body}
		}
	case "release":
		document["release"] = map[string]any{"tag_name": plan.Revision.RefName}
	case "workflow_dispatch", "schedule", "workflow_call":
	default:
		return nil, fmt.Errorf("create event file: unsupported event %q", plan.Event.Name)
	}
	data, err := json.Marshal(document)
	if err != nil {
		return nil, fmt.Errorf("create event file: %w", err)
	}
	return append(data, '\n'), nil
}

func addIssueEvent(document map[string]any, event Event) {
	if event.Issue != nil {
		document["issue"] = map[string]any{"number": event.Issue.Number, "body": event.Issue.Body}
	}
}

func addCommentEvent(document map[string]any, event Event) {
	if event.Comment != nil {
		document["comment"] = map[string]any{"body": event.Comment.Body}
	}
}

func githubPullRequestEvent(plan *Plan) map[string]any {
	if plan.Event.PullRequest == nil {
		if plan.Event.Name == "pull_request" {
			return map[string]any{
				"merge_commit_sha": plan.Revision.SHA,
				"head":             map[string]any{"ref": plan.Revision.HeadRef},
				"base":             map[string]any{"ref": plan.Revision.BaseRef},
			}
		}
		return nil
	}
	pullRequest := map[string]any{
		"number": plan.Event.PullRequest.Number, "body": plan.Event.PullRequest.Body, "html_url": plan.Event.PullRequest.HTMLURL,
		"merge_ref": fmt.Sprintf("refs/pull/%d/merge", plan.Event.PullRequest.Number),
		"head": map[string]any{
			"ref": plan.Event.PullRequest.HeadRef,
			"sha": plan.Event.PullRequest.HeadSHA,
			"repo": map[string]any{
				"id":        plan.Event.PullRequest.HeadRepository.ID,
				"name":      plan.Event.PullRequest.HeadRepository.Name,
				"full_name": plan.Event.PullRequest.HeadRepository.Owner + "/" + plan.Event.PullRequest.HeadRepository.Name,
				"owner":     map[string]any{"login": plan.Event.PullRequest.HeadRepository.Owner},
			},
		},
		"base": map[string]any{"ref": plan.Event.PullRequest.BaseRef},
	}
	if plan.Event.Name == "pull_request" {
		pullRequest["merge_commit_sha"] = plan.Revision.SHA
	}
	return pullRequest
}

func eventInputValues(inputs map[string]any) map[string]string {
	values := make(map[string]string, len(inputs))
	for name, value := range inputs {
		values[name] = fmt.Sprint(value)
	}
	return values
}

type pullRequestRefs struct {
	head string
	base string
}

func planPullRequestRefs(plan *Plan) pullRequestRefs {
	if plan.Event.Name == "pull_request_target" && plan.Event.PullRequest != nil {
		return pullRequestRefs{head: plan.Event.PullRequest.HeadRef, base: plan.Event.PullRequest.BaseRef}
	}
	return pullRequestRefs{head: plan.Revision.HeadRef, base: plan.Revision.BaseRef}
}

func (e *Executor) runPostActions(ctx context.Context, state *executionState, status expression.Status) error {
	var result error
	for index := len(state.posts) - 1; index >= 0; index-- {
		invocation := state.posts[index]
		if !matchesPostCondition(invocation.definition.Runs.PostIf, status) {
			continue
		}
		e.logger.Info("starting post action", "action", invocation.step.Uses)
		err := e.runJavaScriptHook(ctx, invocation, "post", invocation.definition.Runs.Post, state.temporaryDirectory, state.workspace, &state.environment)
		e.logger.Info("completed post action", "action", invocation.step.Uses)
		if err != nil {
			result = errors.Join(result, fmt.Errorf("post action %s: %w", invocation.step.Uses, err))
			status.Success = false
			status.Failure = true
		}
	}
	return result
}

func (e *Executor) runScript(ctx context.Context, state *executionState, step Step) error {
	directory, err := withinDirectory(state.workspace, step.WorkingDirectory)
	if err != nil {
		return err
	}
	files, err := newCommandFiles(filepath.Join(state.temporaryDirectory, "commands"), fmt.Sprintf("%d-step", e.commandID.Add(1)))
	if err != nil {
		return err
	}
	environment := appendEnvironment(append([]string(nil), state.environment...), step.Env)
	environment = files.environmentVariables(environment)
	executionError := e.executeCommandWithFiles(ctx, "/usr/bin/env", []string{"bash", "-e", "-o", "pipefail", "-c", step.Run}, directory, environment, &files, state.workspace)
	updates, commandError := files.read()
	if commandError != nil {
		return errors.Join(executionError, commandError)
	}
	applyEnvironmentUpdates(&state.environment, updates)
	e.logCommandNames("workflow step output", updates.outputs)
	return executionError
}

func (e *Executor) executeCommand(ctx context.Context, name string, args []string, directory string, environment []string) error {
	return e.executeCommandWithFiles(ctx, name, args, directory, environment, nil, directory)
}

func (e *Executor) executeCommandWithFiles(ctx context.Context, name string, args []string, directory string, environment []string, files *commandFiles, workspace string) error {
	stdout := e.commands.writer(e.masker, e.masker.writer(e.stdout), files, workspace)
	stderr := e.commands.writer(e.masker, e.masker.writer(e.stderr), files, workspace)
	err := execute(ctx, name, args, directory, environment, stdout, stderr)
	return errors.Join(err, stdout.flush(), stderr.flush())
}

func (e *Executor) logCommandNames(message string, values map[string]string) {
	names := make([]string, 0, len(values))
	for name := range values {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		e.logger.Info(message, "name", name)
	}
}

func execute(ctx context.Context, name string, args []string, directory string, environment []string, stdout, stderr io.Writer) error {
	command := exec.CommandContext(ctx, name, args...)
	command.Dir = directory
	command.Env = environment
	command.Stdout = stdout
	command.Stderr = stderr
	if err := command.Run(); err != nil {
		return fmt.Errorf("execute %s: %w", name, err)
	}
	return nil
}

func withinDirectory(root, relative string) (string, error) {
	if relative == "" {
		return root, nil
	}
	if filepath.IsAbs(relative) {
		return "", fmt.Errorf("path %q must be relative", relative)
	}
	clean := filepath.Clean(relative)
	if clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("path %q leaves its root directory", relative)
	}
	return filepath.Join(root, clean), nil
}

func appendEnvironment(environment []string, values map[string]string) []string {
	for name, value := range values {
		if reservedEnvironmentName(name) {
			continue
		}
		environment = setEnvironment(environment, name, value)
	}
	return environment
}

func reservedEnvironmentName(name string) bool {
	name = strings.ToUpper(name)
	return strings.HasPrefix(name, "GITHUB_") || strings.HasPrefix(name, "RUNNER_")
}

func setEnvironment(environment []string, name, value string) []string {
	prefix := name + "="
	result := make([]string, 0, len(environment)+1)
	for _, entry := range environment {
		if !strings.HasPrefix(entry, prefix) {
			result = append(result, entry)
		}
	}
	return append(result, prefix+value)
}

func environmentValue(environment []string, name string) string {
	prefix := name + "="
	for index := len(environment) - 1; index >= 0; index-- {
		if strings.HasPrefix(environment[index], prefix) {
			return strings.TrimPrefix(environment[index], prefix)
		}
	}
	return ""
}

func runnerArchitecture() string {
	switch runtime.GOARCH {
	case "amd64":
		return "X64"
	case "arm64":
		return "ARM64"
	case "386":
		return "X86"
	case "arm":
		return "ARM"
	default:
		return runtime.GOARCH
	}
}

func matchesPostCondition(condition string, status expression.Status) bool {
	switch strings.TrimSpace(condition) {
	case "", "always()":
		return true
	case "success()":
		return status.Success
	case "failure()":
		return status.Failure
	default:
		return false
	}
}
