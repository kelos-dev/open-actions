package workflow

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/kelos-dev/open-actions/internal/actionref"
	"github.com/kelos-dev/open-actions/internal/expression"
	"github.com/robfig/cron/v3"
	"gopkg.in/yaml.v3"
)

const (
	maxWorkflowBytes          = 1_000_000
	maxWorkflowNameLength     = 256
	maxConcurrencyGroupLength = 256
	maxJobs                   = 1000
	maxMatrixJobs             = 256
	maxMatrixValueLength      = 1024
	maxJobIDLength            = 256
	maxJobNameLength          = 256
	maxRunnerLabels           = 16
	maxSteps                  = 100
	MaxStepIDLength           = 256
	MaxStepNameLength         = 256
	MaxActionReferenceLength  = 512
	MaxRunScriptBytes         = 65_536
	MaxWorkingDirectoryLength = 512
	MaxConditionBytes         = 65_536
	maxMapEntries             = 100
	maxMapKeyLength           = 256
	MaxMapValueBytes          = 65_536
	MaxJobContentBytes        = 100_000
	maxBranchPatterns         = 256
	maxBranchPatternLength    = 256
	maxActivityTypes          = 64
	maxActivityTypeLength     = 64
	maxWorkflowFilters        = 100
	maxWorkflowFilterLength   = 256
	maxTriggerInputs          = 25
	maxInputNameLength        = 100
	maxInputDescriptionLength = 1024
	maxInputOptions           = 100
	maxInputValueLength       = 65_535
	maxInputPayloadLength     = 65_535
	maxSchedules              = 20
	maxCronLength             = 256
)

type Definition struct {
	Name        string         `yaml:"name"`
	On          Trigger        `yaml:"on"`
	Concurrency Concurrency    `yaml:"concurrency"`
	Jobs        map[string]Job `yaml:"jobs"`
}

type Concurrency struct {
	Group            string `yaml:"group"`
	CancelInProgress bool   `yaml:"cancel-in-progress"`
	configured       bool
}

type Trigger struct {
	Events map[string]EventFilter
}

type EventFilter struct {
	Branches     []string                 `yaml:"branches"`
	Tags         []string                 `yaml:"tags"`
	Types        []string                 `yaml:"types"`
	Workflows    []string                 `yaml:"workflows"`
	Inputs       map[string]WorkflowInput `yaml:"inputs"`
	Schedules    []Schedule               `yaml:"-"`
	branchesSet  bool
	tagsSet      bool
	typesSet     bool
	workflowsSet bool
	inputsSet    bool
}

type WorkflowInput struct {
	Description string   `yaml:"description"`
	Required    bool     `yaml:"required"`
	Default     any      `yaml:"default"`
	Type        string   `yaml:"type"`
	Options     []string `yaml:"options"`
	defaultSet  bool
}

type Schedule struct {
	Cron string `yaml:"cron"`
}

func (schedule *Schedule) UnmarshalYAML(node *yaml.Node) error {
	if node.Kind != yaml.MappingNode {
		return fmt.Errorf("schedule must be a mapping")
	}
	if err := rejectDuplicateMappingKeys(node, "schedule"); err != nil {
		return err
	}
	for index := 0; index < len(node.Content); index += 2 {
		name := node.Content[index].Value
		switch name {
		case "cron":
			if err := node.Content[index+1].Decode(&schedule.Cron); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unsupported schedule field %q", name)
		}
	}
	return nil
}

type Job struct {
	Name      string         `yaml:"name"`
	RunsOn    StringList     `yaml:"runs-on"`
	Needs     StringList     `yaml:"needs"`
	Outputs   map[string]any `yaml:"outputs"`
	Steps     []Step         `yaml:"steps"`
	Strategy  Strategy       `yaml:"strategy"`
	Container yaml.Node      `yaml:"container"`
	Services  yaml.Node      `yaml:"services"`
	If        string         `yaml:"if"`
	Env       map[string]any `yaml:"env"`
}

type Strategy struct {
	Matrix         map[string][]any
	MaxParallel    int32
	configured     bool
	maxParallelSet bool
}

type Step struct {
	ID               string         `yaml:"id"`
	Name             string         `yaml:"name"`
	Uses             string         `yaml:"uses"`
	Run              string         `yaml:"run"`
	Shell            string         `yaml:"shell"`
	WorkingDirectory string         `yaml:"working-directory"`
	With             map[string]any `yaml:"with"`
	Env              map[string]any `yaml:"env"`
	If               string         `yaml:"if"`
}

type Event struct {
	Name         string
	Action       string
	SHA          string
	Ref          string
	RefName      string
	HeadRef      string
	BaseRef      string
	WorkflowName string
	Inputs       map[string]string
	InputValues  map[string]any
	Schedule     string
	PullRequest  *PullRequest
	WorkflowRun  *WorkflowRunEvent
	Issue        *IssueEvent
	Comment      *CommentEvent
	Review       *ReviewEvent
}

type PullRequest struct {
	Number         int64
	Body           string
	HTMLURL        string
	HeadRepository Repository
	HeadRef        string
	HeadSHA        string
	BaseRef        string
}

type WorkflowRunEvent struct {
	Conclusion string
	HeadSHA    string
}

type IssueEvent struct {
	Number int64
	Body   string
}

type CommentEvent struct {
	Body string
}

type ReviewEvent struct {
	Body string
}

type Repository struct {
	ID    int64
	Owner string
	Name  string
}

var (
	workflowConcurrencyAvailability = expression.NewAvailability("github", "inputs", "vars")
	jobNameAvailability             = expression.NewAvailability("github", "needs", "strategy", "matrix", "vars", "inputs")
	jobEnvironmentAvailability      = expression.NewAvailability("github", "needs", "strategy", "matrix", "vars", "secrets", "inputs")
	jobConditionAvailability        = expression.NewAvailability("github", "needs", "vars", "inputs").WithStatusFunctions()
	stepAvailability                = expression.NewAvailability("github", "needs", "strategy", "matrix", "job", "runner", "env", "vars", "secrets", "steps", "inputs")
	stepConditionAvailability       = expression.NewAvailability("github", "needs", "strategy", "matrix", "job", "runner", "env", "vars", "steps", "inputs").WithStatusFunctions()
)

func Parse(data []byte) (*Definition, error) {
	if len(data) > maxWorkflowBytes {
		return nil, fmt.Errorf("workflow exceeds %d bytes", maxWorkflowBytes)
	}
	definition := &Definition{}
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	if err := decoder.Decode(definition); err != nil {
		return nil, fmt.Errorf("decode workflow: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		if err != nil {
			return nil, fmt.Errorf("decode trailing workflow document: %w", err)
		}
		return nil, fmt.Errorf("workflow must contain exactly one YAML document")
	}
	if definition.Name == "" {
		return nil, fmt.Errorf("workflow must define a name")
	}
	if utf8.RuneCountInString(definition.Name) > maxWorkflowNameLength {
		return nil, fmt.Errorf("workflow name exceeds %d characters", maxWorkflowNameLength)
	}
	if utf8.RuneCountInString(definition.Concurrency.Group) > maxConcurrencyGroupLength {
		return nil, fmt.Errorf("workflow concurrency group exceeds %d characters", maxConcurrencyGroupLength)
	}
	if definition.Concurrency.configured && definition.Concurrency.Group == "" {
		return nil, fmt.Errorf("workflow concurrency group must not be empty")
	}
	if definition.Concurrency.Group != "" {
		if err := validateTemplate("workflow concurrency group", definition.Concurrency.Group, workflowConcurrencyAvailability); err != nil {
			return nil, err
		}
	}
	if err := validateTrigger(definition.On); err != nil {
		return nil, err
	}
	if len(definition.Jobs) == 0 {
		return nil, fmt.Errorf("workflow must define at least one job")
	}
	if len(definition.Jobs) > maxJobs {
		return nil, fmt.Errorf("workflow defines %d jobs; maximum is %d", len(definition.Jobs), maxJobs)
	}
	expandedJobs := 0
	for id, job := range definition.Jobs {
		if err := validateJob(id, &job); err != nil {
			return nil, err
		}
		combinations := MatrixCombinations(job.Strategy)
		if len(combinations) == 0 {
			expandedJobs++
		} else {
			expandedJobs += len(combinations)
		}
		if expandedJobs > maxJobs {
			return nil, fmt.Errorf("workflow expands to more than %d jobs", maxJobs)
		}
		definition.Jobs[id] = job
	}
	if err := validateJobGraph(definition.Jobs); err != nil {
		return nil, err
	}
	return definition, nil
}

func validateJob(id string, job *Job) error {
	if len(id) > maxJobIDLength {
		return fmt.Errorf("workflow job ID %q exceeds %d characters", id, maxJobIDLength)
	}
	if !jobIDPattern.MatchString(id) {
		return fmt.Errorf("workflow job ID %q must start with a letter or '_' and contain only letters, digits, '-' or '_'", id)
	}
	if utf8.RuneCountInString(job.Name) > maxJobNameLength {
		return fmt.Errorf("job %q name exceeds %d characters", id, maxJobNameLength)
	}
	if err := validateTemplate(fmt.Sprintf("job %q name", id), job.Name, jobNameAvailability); err != nil {
		return err
	}
	if len(job.RunsOn) == 0 {
		return fmt.Errorf("job %q must define runs-on", id)
	}
	if len(job.RunsOn) > maxRunnerLabels {
		return fmt.Errorf("job %q requests %d runs-on labels; maximum is %d", id, len(job.RunsOn), maxRunnerLabels)
	}
	labels := map[string]struct{}{}
	for index, label := range job.RunsOn {
		if label == "" {
			return fmt.Errorf("job %q contains an empty runs-on label", id)
		}
		if len(label) > 128 {
			return fmt.Errorf("job %q contains a runs-on label longer than 128 characters", id)
		}
		if containsExpression(label) {
			if err := validateTemplate(fmt.Sprintf("job %q runs-on label", id), label, jobNameAvailability); err != nil {
				return err
			}
			continue
		}
		if !runnerLabelPattern.MatchString(label) {
			return fmt.Errorf("job %q contains invalid runs-on label %q", id, label)
		}
		key := lowerASCII(label)
		if _, found := labels[key]; found {
			return fmt.Errorf("job %q repeats runs-on label %q", id, label)
		}
		labels[key] = struct{}{}
		job.RunsOn[index] = key
	}
	needs := make(map[string]struct{}, len(job.Needs))
	for _, dependency := range job.Needs {
		if !jobIDPattern.MatchString(dependency) || len(dependency) > maxJobIDLength {
			return fmt.Errorf("job %q needs invalid job ID %q", id, dependency)
		}
		if dependency == id {
			return fmt.Errorf("job %q cannot need itself", id)
		}
		if _, found := needs[dependency]; found {
			return fmt.Errorf("job %q repeats needed job %q", id, dependency)
		}
		needs[dependency] = struct{}{}
	}
	if err := validateStrategy(id, job.Strategy); err != nil {
		return err
	}
	if job.If != "" {
		if len(job.If) > MaxConditionBytes {
			return fmt.Errorf("job %q if exceeds %d bytes", id, MaxConditionBytes)
		}
		condition, err := expression.ParseCondition(job.If)
		if err != nil {
			return fmt.Errorf("job %q if: %w", id, err)
		}
		if err := condition.Validate(jobConditionAvailability); err != nil {
			return fmt.Errorf("job %q if: %w", id, err)
		}
	}
	if job.Container.Kind != 0 || job.Services.Kind != 0 {
		return fmt.Errorf("job %q uses an unsupported job feature", id)
	}
	if len(job.Steps) == 0 {
		return fmt.Errorf("job %q must define at least one step", id)
	}
	if len(job.Steps) > maxSteps {
		return fmt.Errorf("job %q defines %d steps; maximum is %d", id, len(job.Steps), maxSteps)
	}
	contentBytes := len(job.Name) + len(job.If)
	for _, dependency := range job.Needs {
		contentBytes += len(dependency)
	}
	envBytes, err := validateEnvironmentMap(fmt.Sprintf("job %q env", id), job.Env, jobEnvironmentAvailability)
	if err != nil {
		return err
	}
	contentBytes += envBytes
	outputBytes, err := validateJobOutputs(id, job.Outputs)
	if err != nil {
		return err
	}
	contentBytes += outputBytes
	stepIDs := map[string]struct{}{}
	for i, step := range job.Steps {
		if utf8.RuneCountInString(step.ID) > MaxStepIDLength || (step.ID != "" && !jobIDPattern.MatchString(step.ID)) {
			return fmt.Errorf("job %q step %d has invalid id %q", id, i+1, step.ID)
		}
		if step.ID != "" {
			canonicalID := lowerASCII(step.ID)
			if _, found := stepIDs[canonicalID]; found {
				return fmt.Errorf("job %q step id %q is duplicated", id, step.ID)
			}
			stepIDs[canonicalID] = struct{}{}
		}
		if utf8.RuneCountInString(step.Name) > MaxStepNameLength {
			return fmt.Errorf("job %q step %d name exceeds %d characters", id, i+1, MaxStepNameLength)
		}
		if err := validateTemplate(fmt.Sprintf("job %q step %d name", id, i+1), step.Name, stepAvailability); err != nil {
			return err
		}
		if (step.Uses == "") == (step.Run == "") {
			return fmt.Errorf("job %q step %d must define exactly one of uses or run", id, i+1)
		}
		if step.Uses != "" {
			if utf8.RuneCountInString(step.Uses) > MaxActionReferenceLength {
				return fmt.Errorf("job %q step %d action reference exceeds %d characters", id, i+1, MaxActionReferenceLength)
			}
			if _, err := actionref.Parse(step.Uses); err != nil {
				return fmt.Errorf("job %q step %d: %w", id, i+1, err)
			}
		}
		if len(step.Run) > MaxRunScriptBytes {
			return fmt.Errorf("job %q step %d run script exceeds %d bytes", id, i+1, MaxRunScriptBytes)
		}
		if err := validateTemplate(fmt.Sprintf("job %q step %d run script", id, i+1), step.Run, stepAvailability); err != nil {
			return err
		}
		if utf8.RuneCountInString(step.WorkingDirectory) > MaxWorkingDirectoryLength {
			return fmt.Errorf("job %q step %d working-directory exceeds %d characters", id, i+1, MaxWorkingDirectoryLength)
		}
		if err := validateTemplate(fmt.Sprintf("job %q step %d working-directory", id, i+1), step.WorkingDirectory, stepAvailability); err != nil {
			return err
		}
		if step.Shell != "" && step.Shell != "bash" {
			return fmt.Errorf("job %q step %d uses unsupported shell %q", id, i+1, step.Shell)
		}
		if step.Uses != "" && (step.Shell != "" || step.WorkingDirectory != "") {
			return fmt.Errorf("job %q step %d configures shell fields on an action step", id, i+1)
		}
		if step.Run != "" && len(step.With) > 0 {
			return fmt.Errorf("job %q step %d configures with on a run step", id, i+1)
		}
		if step.If != "" {
			if len(step.If) > MaxConditionBytes {
				return fmt.Errorf("job %q step %d if exceeds %d bytes", id, i+1, MaxConditionBytes)
			}
			condition, err := expression.ParseCondition(step.If)
			if err != nil {
				return fmt.Errorf("job %q step %d if: %w", id, i+1, err)
			}
			if err := condition.Validate(stepConditionAvailability); err != nil {
				return fmt.Errorf("job %q step %d if: %w", id, i+1, err)
			}
		}
		withBytes, err := validateScalarMap(fmt.Sprintf("job %q step %d with", id, i+1), step.With, stepAvailability)
		if err != nil {
			return err
		}
		stepEnvBytes, err := validateEnvironmentMap(fmt.Sprintf("job %q step %d env", id, i+1), step.Env, stepAvailability)
		if err != nil {
			return err
		}
		contentBytes += len(step.ID) + len(step.Name) + len(step.Uses) + len(step.Run) + len(step.WorkingDirectory) + len(step.If) + withBytes + stepEnvBytes
	}
	if contentBytes > MaxJobContentBytes {
		return fmt.Errorf("job %q configuration exceeds %d bytes", id, MaxJobContentBytes)
	}
	return nil
}

func validateStrategy(id string, strategy Strategy) error {
	if !strategy.configured {
		return nil
	}
	if len(strategy.Matrix) == 0 {
		return fmt.Errorf("job %q strategy must define a matrix", id)
	}
	if len(strategy.Matrix) > maxMapEntries {
		return fmt.Errorf("job %q matrix defines %d axes; maximum is %d", id, len(strategy.Matrix), maxMapEntries)
	}
	combinations := 1
	for name, values := range strategy.Matrix {
		if name == "" || utf8.RuneCountInString(name) > maxMapKeyLength {
			return fmt.Errorf("job %q matrix axis %q must contain 1 to %d characters", id, name, maxMapKeyLength)
		}
		if len(values) == 0 {
			return fmt.Errorf("job %q matrix axis %q must define at least one value", id, name)
		}
		if len(values) > maxMatrixJobs || combinations > maxMatrixJobs/len(values) {
			return fmt.Errorf("job %q matrix expands to more than %d jobs", id, maxMatrixJobs)
		}
		combinations *= len(values)
		for _, value := range values {
			scalar, ok := scalarString(value)
			if !ok {
				return fmt.Errorf("job %q matrix axis %q values must be scalars", id, name)
			}
			if utf8.RuneCountInString(scalar) > maxMatrixValueLength {
				return fmt.Errorf("job %q matrix axis %q value exceeds %d characters", id, name, maxMatrixValueLength)
			}
		}
	}
	if strategy.maxParallelSet && strategy.MaxParallel < 1 {
		return fmt.Errorf("job %q strategy max-parallel must be greater than zero", id)
	}
	return nil
}

func validateJobGraph(jobs map[string]Job) error {
	jobIDs := make([]string, 0, len(jobs))
	for id := range jobs {
		jobIDs = append(jobIDs, id)
	}
	sort.Strings(jobIDs)
	for _, id := range jobIDs {
		job := jobs[id]
		for _, dependency := range job.Needs {
			if _, found := jobs[dependency]; !found {
				return fmt.Errorf("job %q needs missing job %q", id, dependency)
			}
		}
	}

	state := make(map[string]uint8, len(jobs))
	stack := make([]string, 0, len(jobs))
	var visit func(string) error
	visit = func(id string) error {
		switch state[id] {
		case 1:
			start := 0
			for stack[start] != id {
				start++
			}
			cycle := append(append([]string(nil), stack[start:]...), id)
			return fmt.Errorf("workflow job dependency cycle: %s", strings.Join(cycle, " -> "))
		case 2:
			return nil
		}
		state[id] = 1
		stack = append(stack, id)
		for _, dependency := range jobs[id].Needs {
			if err := visit(dependency); err != nil {
				return err
			}
		}
		stack = stack[:len(stack)-1]
		state[id] = 2
		return nil
	}
	for _, id := range jobIDs {
		if err := visit(id); err != nil {
			return err
		}
	}
	return nil
}

// MatrixCombinations returns matrix values in stable axis and value order.
func MatrixCombinations(strategy Strategy) []map[string]any {
	if len(strategy.Matrix) == 0 {
		return nil
	}
	axisNames := make([]string, 0, len(strategy.Matrix))
	for name := range strategy.Matrix {
		axisNames = append(axisNames, name)
	}
	sort.Strings(axisNames)
	combinations := []map[string]any{{}}
	for _, name := range axisNames {
		values := strategy.Matrix[name]
		next := make([]map[string]any, 0, len(combinations)*len(values))
		for _, combination := range combinations {
			for _, value := range values {
				item := make(map[string]any, len(combination)+1)
				for existingName, existingValue := range combination {
					item[existingName] = existingValue
				}
				item[name] = value
				next = append(next, item)
			}
		}
		combinations = next
	}
	return combinations
}

func validateJobOutputs(jobID string, outputs map[string]any) (int, error) {
	field := fmt.Sprintf("job %q outputs", jobID)
	bytes, err := validateScalarMap(field, outputs, stepAvailability)
	if err != nil {
		return 0, err
	}
	for name := range outputs {
		if !jobIDPattern.MatchString(name) {
			return 0, fmt.Errorf("%s contains invalid output name %q", field, name)
		}
	}
	return bytes, nil
}

func validateEnvironmentMap(field string, values map[string]any, availability expression.Availability) (int, error) {
	for name := range values {
		if reservedEnvironmentName(name) {
			return 0, fmt.Errorf("%s contains reserved variable %q", field, name)
		}
	}
	return validateScalarMap(field, values, availability)
}

func reservedEnvironmentName(name string) bool {
	name = strings.ToUpper(name)
	return strings.HasPrefix(name, "GITHUB_") || strings.HasPrefix(name, "RUNNER_")
}

func validateScalarMap(field string, values map[string]any, availability expression.Availability) (int, error) {
	if len(values) > maxMapEntries {
		return 0, fmt.Errorf("%s defines %d entries; maximum is %d", field, len(values), maxMapEntries)
	}
	total := 0
	for key, value := range values {
		if key == "" || utf8.RuneCountInString(key) > maxMapKeyLength {
			return 0, fmt.Errorf("%s key %q must contain 1 to %d characters", field, key, maxMapKeyLength)
		}
		scalar, ok := scalarString(value)
		if !ok {
			return 0, fmt.Errorf("%s value %q must be a scalar", field, key)
		}
		if err := validateTemplate(fmt.Sprintf("%s value %q", field, key), scalar, availability); err != nil {
			return 0, err
		}
		if len(scalar) > MaxMapValueBytes {
			return 0, fmt.Errorf("%s value %q exceeds %d bytes", field, key, MaxMapValueBytes)
		}
		total += len(key) + len(scalar)
	}
	return total, nil
}

func containsExpression(value string) bool {
	return strings.Contains(value, "${{")
}

func validateTemplate(field, input string, availability expression.Availability) error {
	program, err := expression.Parse(input)
	if err != nil {
		return fmt.Errorf("%s: %w", field, err)
	}
	if err := program.Validate(availability); err != nil {
		return fmt.Errorf("%s: %w", field, err)
	}
	return nil
}

func scalarString(value any) (string, bool) {
	switch typed := value.(type) {
	case string:
		return typed, true
	case bool, int, int64, uint64, float64:
		return fmt.Sprint(typed), true
	default:
		return "", false
	}
}

// EvaluateJob resolves fields needed before a WorkflowJob can be created.
func EvaluateJob(id string, job Job, context expression.Context) (Job, error) {
	name, err := expression.Parse(job.Name)
	if err != nil {
		return Job{}, fmt.Errorf("job %q name: %w", id, err)
	}
	resolvedName, err := name.Evaluate(context)
	if err != nil {
		return Job{}, fmt.Errorf("job %q name: %w", id, err)
	}
	job.Name, err = resolvedName.String()
	if err != nil {
		return Job{}, fmt.Errorf("job %q name: %w", id, err)
	}
	if utf8.RuneCountInString(job.Name) > maxJobNameLength {
		return Job{}, fmt.Errorf("job %q evaluated name exceeds %d characters", id, maxJobNameLength)
	}

	job.RunsOn = append(StringList(nil), job.RunsOn...)
	labels := make(map[string]struct{}, len(job.RunsOn))
	for index, input := range job.RunsOn {
		program, err := expression.Parse(input)
		if err != nil {
			return Job{}, fmt.Errorf("job %q runs-on label: %w", id, err)
		}
		result, err := program.Evaluate(context)
		if err != nil {
			return Job{}, fmt.Errorf("job %q runs-on label: %w", id, err)
		}
		label, err := result.String()
		if err != nil {
			return Job{}, fmt.Errorf("job %q runs-on label: %w", id, err)
		}
		if label == "" || len(label) > 128 || !runnerLabelPattern.MatchString(label) {
			return Job{}, fmt.Errorf("job %q evaluated an invalid runs-on label", id)
		}
		label = lowerASCII(label)
		if _, found := labels[label]; found {
			return Job{}, fmt.Errorf("job %q repeats evaluated runs-on label %q", id, label)
		}
		labels[label] = struct{}{}
		job.RunsOn[index] = label
	}
	return job, nil
}

// EvaluateJobCondition evaluates a job condition with GitHub's implicit
// success gate for conditions that do not call a status function.
func EvaluateJobCondition(id, input string, context expression.Context) (bool, error) {
	status := context.Status
	if status == nil {
		status = &expression.Status{Success: true}
		context.Status = status
	}
	if strings.TrimSpace(input) == "" {
		return status.Success, nil
	}
	program, err := expression.ParseCondition(input)
	if err != nil {
		return false, fmt.Errorf("job %q if: %w", id, err)
	}
	if !program.UsesStatusFunction() && !status.Success {
		return false, nil
	}
	context.Availability = jobConditionAvailability
	result, err := program.Evaluate(context)
	if err != nil {
		return false, fmt.Errorf("job %q if: %w", id, err)
	}
	return result.Bool(), nil
}

func EvaluateConcurrency(definition *Definition, event Event) (string, bool, error) {
	if definition.Concurrency.Group == "" {
		return "", false, nil
	}
	program, err := expression.Parse(definition.Concurrency.Group)
	if err != nil {
		return "", false, fmt.Errorf("parse concurrency group: %w", err)
	}
	eventValues := eventExpressionValue(event)
	result, err := program.Evaluate(expression.Context{
		Availability: expression.NewAvailability("github", "inputs"),
		Values: map[string]any{
			"inputs": event.InputValues,
			"github": map[string]any{
				"workflow":   definition.Name,
				"event_name": event.Name,
				"event":      eventValues,
				"ref":        event.Ref,
				"ref_name":   event.RefName,
				"head_ref":   event.HeadRef,
				"base_ref":   event.BaseRef,
			},
		},
	})
	if err != nil {
		return "", false, fmt.Errorf("evaluate concurrency group: %w", err)
	}
	group, err := result.String()
	if err != nil {
		return "", false, fmt.Errorf("evaluate concurrency group: %w", err)
	}
	if group == "" {
		return "", false, fmt.Errorf("evaluated concurrency group must not be empty")
	}
	if utf8.RuneCountInString(group) > maxConcurrencyGroupLength {
		return "", false, fmt.Errorf("evaluated concurrency group exceeds %d characters", maxConcurrencyGroupLength)
	}
	return group, definition.Concurrency.CancelInProgress, nil
}

func eventExpressionValue(event Event) map[string]any {
	eventValues := map[string]any{"action": event.Action}
	if len(event.Inputs) > 0 {
		eventValues["inputs"] = event.Inputs
	}
	if event.Schedule != "" {
		eventValues["schedule"] = event.Schedule
	}
	if event.PullRequest != nil {
		pullRequest := pullRequestExpressionValue(event.PullRequest)
		if event.Name == "pull_request" {
			pullRequest["merge_commit_sha"] = event.SHA
		}
		eventValues["pull_request"] = pullRequest
	} else if event.Name == "pull_request" {
		eventValues["pull_request"] = map[string]any{
			"merge_commit_sha": event.SHA,
			"head":             map[string]any{"ref": event.HeadRef},
			"base":             map[string]any{"ref": event.BaseRef},
		}
	}
	if event.WorkflowRun != nil {
		eventValues["workflow_run"] = map[string]any{"conclusion": event.WorkflowRun.Conclusion, "head_sha": event.WorkflowRun.HeadSHA}
	}
	if event.Issue != nil {
		eventValues["issue"] = map[string]any{"number": event.Issue.Number, "body": event.Issue.Body}
	}
	if event.Comment != nil {
		eventValues["comment"] = map[string]any{"body": event.Comment.Body}
	}
	if event.Review != nil {
		eventValues["review"] = map[string]any{"body": event.Review.Body}
	}
	if event.Name == "release" {
		eventValues["release"] = map[string]any{"tag_name": event.RefName}
	}
	return eventValues
}

func pullRequestExpressionValue(pullRequest *PullRequest) map[string]any {
	return map[string]any{
		"number": pullRequest.Number, "body": pullRequest.Body, "html_url": pullRequest.HTMLURL,
		"merge_ref": fmt.Sprintf("refs/pull/%d/merge", pullRequest.Number),
		"head": map[string]any{
			"ref": pullRequest.HeadRef,
			"sha": pullRequest.HeadSHA,
			"repo": map[string]any{
				"id":        pullRequest.HeadRepository.ID,
				"name":      pullRequest.HeadRepository.Name,
				"full_name": pullRequest.HeadRepository.Owner + "/" + pullRequest.HeadRepository.Name,
				"owner":     map[string]any{"login": pullRequest.HeadRepository.Owner},
			},
		},
		"base": map[string]any{"ref": pullRequest.BaseRef},
	}
}

var jobIDPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_-]*$`)
var runnerLabelPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*$`)

var supportedEventTypes = map[string][]string{
	"push": nil,
	"pull_request": {
		"assigned", "unassigned", "labeled", "unlabeled", "opened", "edited",
		"closed", "reopened", "synchronize", "converted_to_draft", "locked",
		"unlocked", "enqueued", "dequeued", "milestoned", "demilestoned",
		"ready_for_review", "review_requested", "review_request_removed",
		"auto_merge_enabled", "auto_merge_disabled",
	},
	"merge_group":  {"checks_requested"},
	"workflow_run": {"completed", "requested", "in_progress"},
	"issues": {
		"opened", "edited", "deleted", "transferred", "pinned", "unpinned",
		"closed", "reopened", "assigned", "unassigned", "labeled", "unlabeled",
		"locked", "unlocked", "milestoned", "demilestoned", "typed", "untyped",
		"field_added", "field_removed",
	},
	"pull_request_target": {
		"assigned", "unassigned", "labeled", "unlabeled", "opened", "edited",
		"closed", "reopened", "synchronize", "converted_to_draft", "locked",
		"unlocked", "enqueued", "dequeued", "milestoned", "demilestoned",
		"ready_for_review", "review_requested", "review_request_removed",
		"auto_merge_enabled", "auto_merge_disabled",
	},
	"issue_comment":               {"created", "edited", "deleted"},
	"pull_request_review_comment": {"created", "edited", "deleted"},
	"pull_request_review":         {"submitted", "edited", "dismissed"},
	"release":                     {"published", "unpublished", "created", "edited", "deleted", "prereleased", "released"},
	"workflow_dispatch":           nil,
	"schedule":                    nil,
	"workflow_call":               nil,
}

var defaultPullRequestTypes = []string{"opened", "synchronize", "reopened"}
var defaultMergeGroupTypes = []string{"checks_requested"}

func SupportsEventAction(eventName, action string) bool {
	allowedTypes, supported := supportedEventTypes[eventName]
	if !supported {
		return false
	}
	if len(allowedTypes) == 0 {
		return action == ""
	}
	return contains(allowedTypes, action)
}

func validateTrigger(trigger Trigger) error {
	if len(trigger.Events) == 0 {
		return fmt.Errorf("workflow must define at least one trigger")
	}
	for eventName, filter := range trigger.Events {
		allowedTypes, supported := supportedEventTypes[eventName]
		if !supported {
			return fmt.Errorf("workflow uses unsupported trigger %q", eventName)
		}
		if len(filter.Types) > maxActivityTypes {
			return fmt.Errorf("trigger %q defines %d activity types; maximum is %d", eventName, len(filter.Types), maxActivityTypes)
		}
		if filter.typesSet && len(filter.Types) == 0 {
			return fmt.Errorf("trigger %q activity types must not be empty", eventName)
		}
		for _, eventType := range filter.Types {
			if utf8.RuneCountInString(eventType) > maxActivityTypeLength {
				return fmt.Errorf("trigger %q activity type exceeds %d characters", eventName, maxActivityTypeLength)
			}
			if !contains(allowedTypes, eventType) {
				return fmt.Errorf("trigger %q uses unsupported activity type %q", eventName, eventType)
			}
		}
		if filter.typesSet && !eventSupportsTypes(eventName) {
			return fmt.Errorf("trigger %q does not support activity types", eventName)
		}
		if err := validateRefPatterns(eventName, "branch", filter.Branches, filter.branchesSet, eventSupportsBranches(eventName)); err != nil {
			return err
		}
		if err := validateRefPatterns(eventName, "tag", filter.Tags, filter.tagsSet, eventName == "push"); err != nil {
			return err
		}
		if len(filter.Workflows) > maxWorkflowFilters {
			return fmt.Errorf("trigger %q defines %d workflow filters; maximum is %d", eventName, len(filter.Workflows), maxWorkflowFilters)
		}
		if filter.workflowsSet && eventName != "workflow_run" {
			return fmt.Errorf("trigger %q does not support workflow filters", eventName)
		}
		if filter.workflowsSet && len(filter.Workflows) == 0 {
			return fmt.Errorf("trigger %q workflow filters must not be empty", eventName)
		}
		for _, name := range filter.Workflows {
			if name == "" || utf8.RuneCountInString(name) > maxWorkflowFilterLength {
				return fmt.Errorf("trigger %q workflow filters must contain 1 to %d characters", eventName, maxWorkflowFilterLength)
			}
		}
		if filter.inputsSet && eventName != "workflow_dispatch" && eventName != "workflow_call" {
			return fmt.Errorf("trigger %q does not support inputs", eventName)
		}
		if err := validateWorkflowInputs(eventName, filter.Inputs); err != nil {
			return err
		}
		if eventName == "schedule" {
			if len(filter.Schedules) == 0 {
				return fmt.Errorf("trigger %q must define at least one cron schedule", eventName)
			}
			if len(filter.Schedules) > maxSchedules {
				return fmt.Errorf("trigger %q defines %d schedules; maximum is %d", eventName, len(filter.Schedules), maxSchedules)
			}
			for _, schedule := range filter.Schedules {
				if _, err := ParseCron(schedule.Cron); err != nil {
					return fmt.Errorf("trigger %q has invalid cron schedule %q: %w", eventName, schedule.Cron, err)
				}
			}
		} else if len(filter.Schedules) != 0 {
			return fmt.Errorf("trigger %q does not support schedules", eventName)
		}
	}
	return nil
}

func eventSupportsTypes(name string) bool {
	return name != "push" && name != "workflow_dispatch" && name != "schedule" && name != "workflow_call"
}

func eventSupportsBranches(name string) bool {
	return name == "push" || name == "pull_request" || name == "pull_request_target" || name == "merge_group" || name == "workflow_run"
}

func validateRefPatterns(eventName, kind string, patterns []string, configured, supported bool) error {
	if len(patterns) > maxBranchPatterns {
		return fmt.Errorf("trigger %q defines %d %s patterns; maximum is %d", eventName, len(patterns), kind, maxBranchPatterns)
	}
	if configured && len(patterns) == 0 {
		return fmt.Errorf("trigger %q %s filters must not be empty", eventName, kind)
	}
	hasPositivePattern := false
	for _, pattern := range patterns {
		if utf8.RuneCountInString(pattern) > maxBranchPatternLength {
			return fmt.Errorf("trigger %q %s pattern exceeds %d characters", eventName, kind, maxBranchPatternLength)
		}
		if !strings.HasPrefix(pattern, "!") {
			hasPositivePattern = true
		}
		candidate := strings.TrimPrefix(pattern, "!")
		if candidate == "" {
			return fmt.Errorf("trigger %q contains an empty %s pattern", eventName, kind)
		}
		if _, err := compileGitHubPattern(candidate); err != nil {
			return fmt.Errorf("trigger %q contains invalid %s pattern %q: %w", eventName, kind, pattern, err)
		}
	}
	if len(patterns) > 0 && !hasPositivePattern {
		return fmt.Errorf("trigger %q %s filters must include a positive pattern", eventName, kind)
	}
	if configured && !supported {
		return fmt.Errorf("trigger %q does not support %s filters", eventName, kind)
	}
	return nil
}

var inputNamePattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_-]*$`)

func validateWorkflowInputs(eventName string, inputs map[string]WorkflowInput) error {
	if len(inputs) > maxTriggerInputs {
		return fmt.Errorf("trigger %q defines %d inputs; maximum is %d", eventName, len(inputs), maxTriggerInputs)
	}
	defaults := make(map[string]string, len(inputs))
	inputNames := make(map[string]struct{}, len(inputs))
	for name, input := range inputs {
		if utf8.RuneCountInString(name) > maxInputNameLength || !inputNamePattern.MatchString(name) {
			return fmt.Errorf("trigger %q input %q must start with a letter or '_' and contain at most %d letters, digits, '-' or '_'", eventName, name, maxInputNameLength)
		}
		canonicalName := lowerASCII(name)
		if _, found := inputNames[canonicalName]; found {
			return fmt.Errorf("trigger %q input names must be unique ignoring case", eventName)
		}
		inputNames[canonicalName] = struct{}{}
		if utf8.RuneCountInString(input.Description) > maxInputDescriptionLength {
			return fmt.Errorf("trigger %q input %q description exceeds %d characters", eventName, name, maxInputDescriptionLength)
		}
		inputType := input.Type
		if inputType == "" && eventName == "workflow_dispatch" {
			inputType = "string"
		}
		if eventName == "workflow_call" && inputType == "" {
			return fmt.Errorf("trigger %q input %q must define type", eventName, name)
		}
		allowed := inputType == "string" || inputType == "boolean" || inputType == "number"
		if eventName == "workflow_dispatch" {
			allowed = allowed || inputType == "choice" || inputType == "environment"
		}
		if !allowed {
			return fmt.Errorf("trigger %q input %q uses unsupported type %q", eventName, name, input.Type)
		}
		if len(input.Options) > maxInputOptions {
			return fmt.Errorf("trigger %q input %q defines %d options; maximum is %d", eventName, name, len(input.Options), maxInputOptions)
		}
		if inputType == "choice" && len(input.Options) == 0 {
			return fmt.Errorf("trigger %q input %q choice options must not be empty", eventName, name)
		}
		if inputType != "choice" && len(input.Options) != 0 {
			return fmt.Errorf("trigger %q input %q options require type choice", eventName, name)
		}
		for _, option := range input.Options {
			if option == "" || utf8.RuneCountInString(option) > maxInputValueLength {
				return fmt.Errorf("trigger %q input %q options must contain 1 to %d characters", eventName, name, maxInputValueLength)
			}
		}
		if input.defaultSet {
			value, ok := scalarString(input.Default)
			if !ok || utf8.RuneCountInString(value) > maxInputValueLength || !validInputValue(inputType, value, input.Options) {
				return fmt.Errorf("trigger %q input %q has an invalid default for type %s", eventName, name, inputType)
			}
			defaults[name] = value
		}
	}
	return validateInputPayloadLength(fmt.Sprintf("trigger %q defaults", eventName), defaults)
}

var numberInputPattern = regexp.MustCompile(`^-?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?$`)

func validInputValue(inputType, value string, options []string) bool {
	switch inputType {
	case "boolean":
		return value == "true" || value == "false"
	case "number":
		parsed, err := strconv.ParseFloat(value, 64)
		return err == nil && !math.IsInf(parsed, 0) && !math.IsNaN(parsed) && numberInputPattern.MatchString(value)
	case "choice":
		return contains(options, value)
	default:
		return true
	}
}

var cronParser = cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)

func ParseCron(expression string) (cron.Schedule, error) {
	if expression == "" || utf8.RuneCountInString(expression) > maxCronLength {
		return nil, fmt.Errorf("cron expression must contain 1 to %d characters", maxCronLength)
	}
	if len(strings.Fields(expression)) != 5 {
		return nil, fmt.Errorf("cron expression must contain five fields")
	}
	parsed, err := cronParser.Parse(expression)
	if err != nil {
		return nil, err
	}
	cursor := time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)
	for range 128 {
		next := parsed.Next(cursor)
		following := parsed.Next(next)
		if following.Sub(next) < 5*time.Minute {
			return nil, fmt.Errorf("cron schedule interval must be at least five minutes")
		}
		cursor = next
	}
	return parsed, nil
}

func ScheduleMatches(schedule cron.Schedule, instant time.Time) bool {
	minute := instant.UTC().Truncate(time.Minute)
	return schedule.Next(minute.Add(-time.Minute)).Equal(minute)
}

func lowerASCII(value string) string {
	buffer := []byte(value)
	for index, character := range buffer {
		if character >= 'A' && character <= 'Z' {
			buffer[index] = character + ('a' - 'A')
		}
	}
	return string(buffer)
}

func (s *Strategy) UnmarshalYAML(node *yaml.Node) error {
	s.configured = true
	if node.Kind != yaml.MappingNode {
		return fmt.Errorf("job strategy must be a mapping")
	}
	if err := rejectDuplicateMappingKeys(node, "job strategy"); err != nil {
		return err
	}
	for index := 0; index < len(node.Content); index += 2 {
		name := node.Content[index].Value
		value := node.Content[index+1]
		switch name {
		case "matrix":
			if value.Kind != yaml.MappingNode {
				return fmt.Errorf("job strategy matrix must be a mapping")
			}
			if err := rejectDuplicateMappingKeys(value, "job strategy matrix"); err != nil {
				return err
			}
			if err := value.Decode(&s.Matrix); err != nil {
				return err
			}
		case "max-parallel":
			s.maxParallelSet = true
			if err := value.Decode(&s.MaxParallel); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unsupported job strategy field %q", name)
		}
	}
	return nil
}

func (c *Concurrency) UnmarshalYAML(node *yaml.Node) error {
	c.configured = true
	if node.Kind == yaml.ScalarNode {
		if node.Tag == "!!null" {
			return nil
		}
		c.Group = node.Value
		return nil
	}
	if node.Kind != yaml.MappingNode {
		return fmt.Errorf("workflow concurrency must be a string or mapping")
	}
	if err := rejectDuplicateMappingKeys(node, "workflow concurrency"); err != nil {
		return err
	}
	for index := 0; index < len(node.Content); index += 2 {
		name := node.Content[index].Value
		value := node.Content[index+1]
		switch name {
		case "group":
			if err := value.Decode(&c.Group); err != nil {
				return err
			}
		case "cancel-in-progress":
			if err := value.Decode(&c.CancelInProgress); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unsupported concurrency field %q", name)
		}
	}
	return nil
}

func (f *EventFilter) UnmarshalYAML(node *yaml.Node) error {
	if node.Kind == yaml.ScalarNode && node.Tag == "!!null" {
		return nil
	}
	if node.Kind != yaml.MappingNode {
		return fmt.Errorf("event filter must be a mapping")
	}
	if err := rejectDuplicateMappingKeys(node, "event filter"); err != nil {
		return err
	}
	for index := 0; index < len(node.Content); index += 2 {
		name := node.Content[index].Value
		value := node.Content[index+1]
		switch name {
		case "branches":
			f.branchesSet = true
			if err := value.Decode(&f.Branches); err != nil {
				return err
			}
		case "tags":
			f.tagsSet = true
			if err := value.Decode(&f.Tags); err != nil {
				return err
			}
		case "types":
			f.typesSet = true
			if err := value.Decode(&f.Types); err != nil {
				return err
			}
		case "workflows":
			f.workflowsSet = true
			if err := value.Decode(&f.Workflows); err != nil {
				return err
			}
		case "inputs":
			f.inputsSet = true
			inputs, err := decodeWorkflowInputs(value)
			if err != nil {
				return err
			}
			f.Inputs = inputs
		default:
			return fmt.Errorf("unsupported event filter %q", name)
		}
	}
	return nil
}

func decodeWorkflowInputs(node *yaml.Node) (map[string]WorkflowInput, error) {
	if node.Kind != yaml.MappingNode {
		return nil, fmt.Errorf("workflow inputs must be a mapping")
	}
	if err := rejectDuplicateMappingKeys(node, "workflow inputs"); err != nil {
		return nil, err
	}
	inputs := make(map[string]WorkflowInput, len(node.Content)/2)
	for index := 0; index < len(node.Content); index += 2 {
		name := node.Content[index].Value
		input := WorkflowInput{}
		if err := node.Content[index+1].Decode(&input); err != nil {
			return nil, fmt.Errorf("decode workflow input %q: %w", name, err)
		}
		inputs[name] = input
	}
	return inputs, nil
}

func (input *WorkflowInput) UnmarshalYAML(node *yaml.Node) error {
	if node.Kind != yaml.MappingNode {
		return fmt.Errorf("workflow input must be a mapping")
	}
	if err := rejectDuplicateMappingKeys(node, "workflow input"); err != nil {
		return err
	}
	for index := 0; index < len(node.Content); index += 2 {
		name := node.Content[index].Value
		value := node.Content[index+1]
		switch name {
		case "description":
			if err := value.Decode(&input.Description); err != nil {
				return err
			}
		case "required":
			if err := value.Decode(&input.Required); err != nil {
				return err
			}
		case "default":
			input.defaultSet = true
			if err := value.Decode(&input.Default); err != nil {
				return err
			}
		case "type":
			if err := value.Decode(&input.Type); err != nil {
				return err
			}
		case "options":
			if err := value.Decode(&input.Options); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unsupported workflow input field %q", name)
		}
	}
	return nil
}

func Matches(trigger Trigger, event Event) bool {
	_, matched, err := Match(trigger, event)
	return err == nil && matched
}

func Match(trigger Trigger, event Event) (Event, bool, error) {
	filter, found := trigger.Events[event.Name]
	if !found {
		return event, false, nil
	}
	types := filter.Types
	if (event.Name == "pull_request" || event.Name == "pull_request_target") && !filter.typesSet && len(types) == 0 {
		types = defaultPullRequestTypes
	}
	if event.Name == "merge_group" && !filter.typesSet && len(types) == 0 {
		types = defaultMergeGroupTypes
	}
	if len(types) > 0 && !contains(types, event.Action) {
		return event, false, nil
	}
	if event.Name == "workflow_run" && len(filter.Workflows) > 0 && !contains(filter.Workflows, event.WorkflowName) {
		return event, false, nil
	}
	if event.Name == "schedule" {
		if _, err := ParseCron(event.Schedule); err != nil {
			return event, false, fmt.Errorf("schedule event contains an invalid cron expression: %w", err)
		}
		for _, schedule := range filter.Schedules {
			if schedule.Cron == event.Schedule {
				return event, true, nil
			}
		}
		return event, false, nil
	}
	if event.Name == "push" {
		switch {
		case strings.HasPrefix(event.Ref, "refs/heads/"):
			if (filter.tagsSet && !filter.branchesSet) || (filter.branchesSet && !matchesBranch(filter.Branches, event.RefName)) {
				return event, false, nil
			}
		case strings.HasPrefix(event.Ref, "refs/tags/"):
			if (filter.branchesSet && !filter.tagsSet) || (filter.tagsSet && !matchesBranch(filter.Tags, event.RefName)) {
				return event, false, nil
			}
		default:
			return event, false, nil
		}
	}
	branch := event.RefName
	if event.Name == "pull_request" || event.Name == "pull_request_target" || event.Name == "merge_group" || event.Name == "workflow_run" {
		branch = event.BaseRef
	}
	if event.Name != "push" && len(filter.Branches) > 0 && !matchesBranch(filter.Branches, branch) {
		return event, false, nil
	}
	if event.Name == "workflow_dispatch" || event.Name == "workflow_call" {
		inputs, inputValues, err := resolveInputs(event.Name, filter.Inputs, event.Inputs)
		if err != nil {
			return event, false, err
		}
		event.Inputs = inputs
		event.InputValues = inputValues
	}
	return event, true, nil
}

func resolveInputs(eventName string, definitions map[string]WorkflowInput, provided map[string]string) (map[string]string, map[string]any, error) {
	if len(provided) > maxTriggerInputs {
		return nil, nil, fmt.Errorf("%s event provides %d inputs; maximum is %d", eventName, len(provided), maxTriggerInputs)
	}
	for name := range provided {
		if _, found := definitions[name]; !found {
			return nil, nil, fmt.Errorf("%s event provides unknown input %q", eventName, name)
		}
	}
	resolved := make(map[string]string, len(definitions))
	inputValues := make(map[string]any, len(definitions))
	for name, definition := range definitions {
		inputType := definition.Type
		if inputType == "" {
			inputType = "string"
		}
		value, found := provided[name]
		if !found && definition.defaultSet {
			value, _ = scalarString(definition.Default)
			found = true
		}
		if !found && definition.Required {
			return nil, nil, fmt.Errorf("%s event is missing required input %q", eventName, name)
		}
		if !found && eventName == "workflow_call" {
			value = implicitWorkflowCallInputDefault(inputType)
			found = true
		}
		if !found {
			continue
		}
		if utf8.RuneCountInString(value) > maxInputValueLength {
			return nil, nil, fmt.Errorf("%s event input %q exceeds %d characters", eventName, name, maxInputValueLength)
		}
		if !validInputValue(inputType, value, definition.Options) {
			return nil, nil, fmt.Errorf("%s event input %q is invalid for type %s", eventName, name, inputType)
		}
		resolved[name] = value
		inputValues[name] = inputExpressionValue(inputType, value)
	}
	if err := validateInputPayloadLength(eventName+" event", resolved); err != nil {
		return nil, nil, err
	}
	return resolved, inputValues, nil
}

func inputExpressionValue(inputType, value string) any {
	switch inputType {
	case "boolean":
		return value == "true"
	case "number":
		parsed, _ := strconv.ParseFloat(value, 64)
		return parsed
	default:
		return value
	}
}

func implicitWorkflowCallInputDefault(inputType string) string {
	if inputType == "boolean" {
		return "false"
	}
	if inputType == "number" {
		return "0"
	}
	return ""
}

func validateInputPayloadLength(field string, inputs map[string]string) error {
	characters := 0
	for name, value := range inputs {
		characters += utf8.RuneCountInString(name) + utf8.RuneCountInString(value)
	}
	if characters > maxInputPayloadLength {
		return fmt.Errorf("%s input names and values contain %d characters; maximum is %d", field, characters, maxInputPayloadLength)
	}
	return nil
}

func (t *Trigger) UnmarshalYAML(node *yaml.Node) error {
	t.Events = map[string]EventFilter{}
	switch node.Kind {
	case yaml.ScalarNode:
		t.Events[node.Value] = EventFilter{}
	case yaml.SequenceNode:
		for _, child := range node.Content {
			if child.Kind != yaml.ScalarNode {
				return fmt.Errorf("workflow trigger list entries must be event names")
			}
			t.Events[child.Value] = EventFilter{}
		}
	case yaml.MappingNode:
		if err := rejectDuplicateMappingKeys(node, "workflow trigger"); err != nil {
			return err
		}
		for i := 0; i < len(node.Content); i += 2 {
			name := node.Content[i].Value
			value := node.Content[i+1]
			filter := EventFilter{}
			if name == "schedule" {
				if err := value.Decode(&filter.Schedules); err != nil {
					return fmt.Errorf("decode %s trigger: %w", name, err)
				}
			} else if value.Kind != yaml.ScalarNode || value.Tag != "!!null" {
				if err := value.Decode(&filter); err != nil {
					return fmt.Errorf("decode %s trigger: %w", name, err)
				}
			}
			t.Events[name] = filter
		}
	default:
		return fmt.Errorf("workflow on must be an event, list, or mapping")
	}
	return nil
}

func rejectDuplicateMappingKeys(node *yaml.Node, field string) error {
	keys := make(map[string]struct{}, len(node.Content)/2)
	for index := 0; index < len(node.Content); index += 2 {
		key := node.Content[index]
		if key.Kind != yaml.ScalarNode || key.Tag != "!!str" {
			return fmt.Errorf("%s keys must be strings", field)
		}
		if _, found := keys[key.Value]; found {
			return fmt.Errorf("%s field %q is duplicated", field, key.Value)
		}
		keys[key.Value] = struct{}{}
	}
	return nil
}

func (s *StringList) UnmarshalYAML(node *yaml.Node) error {
	switch node.Kind {
	case 0:
		return nil
	case yaml.ScalarNode:
		if node.Tag != "!!str" {
			return fmt.Errorf("value must be a string or list")
		}
		*s = []string{node.Value}
	case yaml.SequenceNode:
		for _, child := range node.Content {
			if child.Kind != yaml.ScalarNode || child.Tag != "!!str" {
				return fmt.Errorf("values must be strings")
			}
			*s = append(*s, child.Value)
		}
	default:
		return fmt.Errorf("value must be a string or list")
	}
	return nil
}

// StringList accepts either a scalar string or a list of strings in workflow YAML.
type StringList []string

func contains(values []string, candidate string) bool {
	for _, value := range values {
		if value == candidate {
			return true
		}
	}
	return false
}

func matchesBranch(patterns []string, branch string) bool {
	matched := false
	for _, pattern := range patterns {
		negated := strings.HasPrefix(pattern, "!")
		pattern = strings.TrimPrefix(pattern, "!")
		matcher, err := compileGitHubPattern(pattern)
		if err != nil {
			continue
		}
		if matcher.MatchString(branch) {
			matched = !negated
		}
	}
	return matched
}

type globAtom struct {
	expression   string
	quantifiable bool
}

func compileGitHubPattern(pattern string) (*regexp.Regexp, error) {
	runes := []rune(pattern)
	atoms := make([]globAtom, 0, len(runes))
	for index := 0; index < len(runes); index++ {
		character := runes[index]
		switch character {
		case '\\':
			index++
			if index == len(runes) {
				return nil, fmt.Errorf("trailing escape")
			}
			atoms = append(atoms, globAtom{expression: regexp.QuoteMeta(string(runes[index])), quantifiable: true})
		case '*':
			if index+1 < len(runes) && runes[index+1] == '*' {
				if (index == 0 || runes[index-1] == '/') && index+2 < len(runes) && runes[index+2] == '/' {
					atoms = append(atoms, globAtom{expression: "(?:.*/)?"})
					index += 2
				} else {
					atoms = append(atoms, globAtom{expression: ".*"})
					index++
				}
			} else {
				atoms = append(atoms, globAtom{expression: "[^/]*"})
			}
		case '?', '+':
			if len(atoms) == 0 || !atoms[len(atoms)-1].quantifiable {
				return nil, fmt.Errorf("%q must follow a literal or character class", character)
			}
			quantifier := string(character)
			atoms[len(atoms)-1].expression = "(" + atoms[len(atoms)-1].expression + ")" + quantifier
			atoms[len(atoms)-1].quantifiable = false
		case '[':
			end := index + 1
			for end < len(runes) && runes[end] != ']' {
				end++
			}
			if end == len(runes) {
				return nil, fmt.Errorf("unterminated character class")
			}
			class, err := githubCharacterClass(runes[index+1 : end])
			if err != nil {
				return nil, err
			}
			atoms = append(atoms, globAtom{expression: class, quantifiable: true})
			index = end
		default:
			atoms = append(atoms, globAtom{expression: regexp.QuoteMeta(string(character)), quantifiable: true})
		}
	}
	var expression strings.Builder
	expression.WriteByte('^')
	for _, atom := range atoms {
		expression.WriteString(atom.expression)
	}
	expression.WriteByte('$')
	return regexp.Compile(expression.String())
}

func githubCharacterClass(characters []rune) (string, error) {
	if len(characters) == 0 {
		return "", fmt.Errorf("empty character class")
	}
	for index, character := range characters {
		if isASCIIAlphanumeric(character) {
			continue
		}
		if character != '-' || index == 0 || index == len(characters)-1 || !sameCharacterClassRange(characters[index-1], characters[index+1]) {
			return "", fmt.Errorf("character classes may contain ASCII alphanumeric values and ranges")
		}
	}
	return "[" + string(characters) + "]", nil
}

func isASCIIAlphanumeric(character rune) bool {
	return character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' || character >= '0' && character <= '9'
}

func sameCharacterClassRange(left, right rune) bool {
	return left <= right && (left >= 'a' && right <= 'z' || left >= 'A' && right <= 'Z' || left >= '0' && right <= '9')
}
