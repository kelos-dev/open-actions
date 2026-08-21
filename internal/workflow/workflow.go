package workflow

import (
	"bytes"
	"encoding/json"
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
	DefaultJobTimeoutMinutes  = int64(360)
	maxWorkflowBytes          = 1_000_000
	maxWorkflowNameLength     = 256
	maxConcurrencyGroupLength = 256
	MaxJobs                   = 1000
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
	Env         map[string]any `yaml:"env"`
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
	Name           string         `yaml:"name"`
	RunsOn         StringList     `yaml:"runs-on"`
	Needs          StringList     `yaml:"needs"`
	Outputs        map[string]any `yaml:"outputs"`
	Steps          []Step         `yaml:"steps"`
	Strategy       Strategy       `yaml:"strategy"`
	Container      yaml.Node      `yaml:"container"`
	Services       yaml.Node      `yaml:"services"`
	If             string         `yaml:"if"`
	Env            map[string]any `yaml:"env"`
	TimeoutMinutes JobTimeout     `yaml:"timeout-minutes"`
}

// JobTimeout is a positive whole-minute timeout or an expression that resolves
// to one during job planning.
type JobTimeout struct {
	minutes    int64
	expression string
	configured bool
}

// Minutes returns the resolved timeout, including GitHub's default when the
// workflow omits timeout-minutes.
func (t JobTimeout) Minutes() int64 {
	if !t.configured {
		return DefaultJobTimeoutMinutes
	}
	return t.minutes
}

type Strategy struct {
	Matrix         MatrixDefinition
	MaxParallel    int32
	FailFast       bool
	configured     bool
	maxParallelSet bool
}

// MatrixDefinition is an unevaluated matrix strategy. Expressions may define
// the complete matrix, an axis, or individual scalar values.
type MatrixDefinition struct {
	Expression string
	Axes       map[string]MatrixAxis
	Include    MatrixEntries
	Exclude    MatrixEntries
}

type MatrixAxis struct {
	Expression string
	Values     []any
}

type MatrixEntries struct {
	Expression string
	Values     []map[string]any
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
	ContinueOnError  any            `yaml:"continue-on-error"`
}

type Event struct {
	Name         string
	Action       string
	SHA          string
	BaseSHA      string
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
	Payload      map[string]any
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
	workflowEnvironmentAvailability = expression.NewAvailability("github", "open_actions", "secrets", "inputs", "vars")
	jobNameAvailability             = expression.NewAvailability("github", "open_actions", "needs", "strategy", "matrix", "vars", "inputs")
	jobEnvironmentAvailability      = expression.NewAvailability("github", "open_actions", "needs", "strategy", "matrix", "vars", "secrets", "inputs")
	jobConditionAvailability        = expression.NewAvailability("github", "open_actions", "needs", "vars", "inputs").WithStatusFunctions()
	matrixAvailability              = expression.NewAvailability("github", "open_actions", "needs", "vars", "inputs")
	stepAvailability                = expression.NewAvailability("github", "open_actions", "needs", "strategy", "matrix", "job", "runner", "env", "vars", "secrets", "steps", "inputs").WithHashFiles()
	stepConditionAvailability       = expression.NewAvailability("github", "open_actions", "needs", "strategy", "matrix", "job", "runner", "env", "vars", "steps", "inputs").WithStatusFunctions().WithHashFiles()
	jobOutputAvailability           = expression.NewAvailability("github", "open_actions", "needs", "strategy", "matrix", "job", "runner", "env", "vars", "secrets", "steps", "inputs")
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
	if _, err := validateScalarMap("workflow env", definition.Env, workflowEnvironmentAvailability); err != nil {
		return nil, err
	}
	if err := validateTrigger(definition.On); err != nil {
		return nil, err
	}
	if len(definition.Jobs) == 0 {
		return nil, fmt.Errorf("workflow must define at least one job")
	}
	if len(definition.Jobs) > MaxJobs {
		return nil, fmt.Errorf("workflow defines %d jobs; maximum is %d", len(definition.Jobs), MaxJobs)
	}
	expandedJobs := 0
	for id, job := range definition.Jobs {
		if err := validateJob(id, &job, definition.Env); err != nil {
			return nil, err
		}
		combinations := MatrixCombinations(job.Strategy)
		if len(combinations) == 0 || MatrixRequiresEvaluation(job.Strategy) {
			expandedJobs++
		} else {
			expandedJobs += len(combinations)
		}
		if expandedJobs > MaxJobs {
			return nil, fmt.Errorf("workflow expands to more than %d jobs", MaxJobs)
		}
		definition.Jobs[id] = job
	}
	if err := validateJobGraph(definition.Jobs); err != nil {
		return nil, err
	}
	return definition, nil
}

func validateJob(id string, job *Job, workflowEnv map[string]any) error {
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
	if err := job.TimeoutMinutes.validate(id); err != nil {
		return err
	}
	if MatrixUsesNeeds(job.Strategy) && len(job.Needs) == 0 {
		return fmt.Errorf("job %q matrix uses needs but the job declares no dependencies", id)
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
	contentBytes := len(job.Name) + len(job.If) + len(job.TimeoutMinutes.expression)
	for _, dependency := range job.Needs {
		contentBytes += len(dependency)
	}
	if _, err := validateScalarMap(fmt.Sprintf("job %q env", id), job.Env, jobEnvironmentAvailability); err != nil {
		return err
	}
	contentBytes += mergedScalarMapBytes(workflowEnv, job.Env)
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
		continueOnErrorBytes, err := validateStepContinueOnError(fmt.Sprintf("job %q step %d continue-on-error", id, i+1), step.ContinueOnError)
		if err != nil {
			return err
		}
		withBytes, err := validateScalarMap(fmt.Sprintf("job %q step %d with", id, i+1), step.With, stepAvailability)
		if err != nil {
			return err
		}
		stepEnvBytes, err := validateScalarMap(fmt.Sprintf("job %q step %d env", id, i+1), step.Env, stepAvailability)
		if err != nil {
			return err
		}
		contentBytes += len(step.ID) + len(step.Name) + len(step.Uses) + len(step.Run) + len(step.WorkingDirectory) + len(step.If) + continueOnErrorBytes + withBytes + stepEnvBytes
	}
	if contentBytes > MaxJobContentBytes {
		return fmt.Errorf("job %q configuration exceeds %d bytes", id, MaxJobContentBytes)
	}
	return nil
}

func validateStepContinueOnError(field string, value any) (int, error) {
	switch typed := value.(type) {
	case nil:
		return 0, nil
	case bool:
		return len(strconv.FormatBool(typed)), nil
	case string:
		if len(typed) > MaxConditionBytes {
			return 0, fmt.Errorf("%s exceeds %d bytes", field, MaxConditionBytes)
		}
		trimmed := strings.TrimSpace(typed)
		if !strings.HasPrefix(trimmed, "${{") {
			return 0, fmt.Errorf("%s must be a boolean or expression", field)
		}
		program, err := expression.ParseCondition(trimmed)
		if err != nil {
			return 0, fmt.Errorf("%s: %w", field, err)
		}
		if err := program.Validate(stepAvailability); err != nil {
			return 0, fmt.Errorf("%s: %w", field, err)
		}
		return len(typed), nil
	default:
		return 0, fmt.Errorf("%s must be a boolean or expression", field)
	}
}

func validateStrategy(id string, strategy Strategy) error {
	if !strategy.configured {
		return nil
	}
	if strategy.Matrix.Expression == "" && len(strategy.Matrix.Axes) == 0 && len(strategy.Matrix.Include.Values) == 0 && strategy.Matrix.Include.Expression == "" {
		return fmt.Errorf("job %q strategy must define a matrix", id)
	}
	if strategy.Matrix.Expression != "" {
		if len(strategy.Matrix.Axes) > 0 || len(strategy.Matrix.Include.Values) > 0 || strategy.Matrix.Include.Expression != "" || len(strategy.Matrix.Exclude.Values) > 0 || strategy.Matrix.Exclude.Expression != "" {
			return fmt.Errorf("job %q matrix expression cannot be combined with matrix fields", id)
		}
		if err := validateMatrixExpression(fmt.Sprintf("job %q matrix", id), strategy.Matrix.Expression); err != nil {
			return err
		}
	} else if len(strategy.Matrix.Axes) > maxMapEntries {
		return fmt.Errorf("job %q matrix defines %d axes; maximum is %d", id, len(strategy.Matrix.Axes), maxMapEntries)
	}
	for name, axis := range strategy.Matrix.Axes {
		if name == "" || utf8.RuneCountInString(name) > maxMapKeyLength {
			return fmt.Errorf("job %q matrix axis %q must contain 1 to %d characters", id, name, maxMapKeyLength)
		}
		if axis.Expression == "" && len(axis.Values) == 0 {
			return fmt.Errorf("job %q matrix axis %q must define at least one value", id, name)
		}
		if axis.Expression != "" {
			if len(axis.Values) > 0 {
				return fmt.Errorf("job %q matrix axis %q expression cannot be combined with literal values", id, name)
			}
			if err := validateMatrixExpression(fmt.Sprintf("job %q matrix axis %q", id, name), axis.Expression); err != nil {
				return err
			}
		}
		if len(axis.Values) > maxMatrixJobs {
			return fmt.Errorf("job %q matrix axis %q defines more than %d values", id, name, maxMatrixJobs)
		}
		for _, value := range axis.Values {
			if err := validateMatrixValue(fmt.Sprintf("job %q matrix axis %q", id, name), value); err != nil {
				return err
			}
		}
	}
	if err := validateMatrixEntries(id, "include", strategy.Matrix.Include); err != nil {
		return err
	}
	if err := validateMatrixEntries(id, "exclude", strategy.Matrix.Exclude); err != nil {
		return err
	}
	if strategy.maxParallelSet && strategy.MaxParallel < 1 {
		return fmt.Errorf("job %q strategy max-parallel must be greater than zero", id)
	}
	if !MatrixRequiresEvaluation(strategy) {
		if _, err := EvaluateMatrix(id, strategy, expression.Context{}); err != nil {
			return err
		}
	}
	return nil
}

func validateMatrixExpression(field, input string) error {
	if len(input) > MaxMapValueBytes {
		return fmt.Errorf("%s expression exceeds %d bytes", field, MaxMapValueBytes)
	}
	if !containsExpression(input) {
		return fmt.Errorf("%s must be an expression", field)
	}
	return validateTemplate(field, input, matrixAvailability)
}

func validateMatrixValue(field string, value any) error {
	scalar, ok := scalarString(value)
	if !ok {
		return fmt.Errorf("%s values must be scalars", field)
	}
	if utf8.RuneCountInString(scalar) > maxMatrixValueLength {
		return fmt.Errorf("%s value exceeds %d characters", field, maxMatrixValueLength)
	}
	if containsExpression(scalar) {
		return validateTemplate(field+" value", scalar, matrixAvailability)
	}
	return nil
}

func validateMatrixEntries(jobID, name string, entries MatrixEntries) error {
	field := fmt.Sprintf("job %q matrix %s", jobID, name)
	if entries.Expression != "" {
		if len(entries.Values) > 0 {
			return fmt.Errorf("%s expression cannot be combined with literal entries", field)
		}
		return validateMatrixExpression(field, entries.Expression)
	}
	if len(entries.Values) > maxMatrixJobs {
		return fmt.Errorf("%s defines more than %d entries", field, maxMatrixJobs)
	}
	for index, entry := range entries.Values {
		entryField := fmt.Sprintf("%s entry %d", field, index+1)
		if len(entry) == 0 {
			return fmt.Errorf("%s must not be empty", entryField)
		}
		if len(entry) > maxMapEntries {
			return fmt.Errorf("%s defines %d values; maximum is %d", entryField, len(entry), maxMapEntries)
		}
		for key, value := range entry {
			if key == "" || utf8.RuneCountInString(key) > maxMapKeyLength {
				return fmt.Errorf("%s key %q must contain 1 to %d characters", entryField, key, maxMapKeyLength)
			}
			if err := validateMatrixValue(entryField, value); err != nil {
				return err
			}
		}
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

// MatrixRequiresEvaluation reports whether expansion depends on expressions.
func MatrixRequiresEvaluation(strategy Strategy) bool {
	matrix := strategy.Matrix
	if matrix.Expression != "" || matrix.Include.Expression != "" || matrix.Exclude.Expression != "" {
		return true
	}
	for _, axis := range matrix.Axes {
		if axis.Expression != "" || matrixValuesContainExpressions(axis.Values) {
			return true
		}
	}
	return matrixEntriesContainExpressions(matrix.Include.Values) || matrixEntriesContainExpressions(matrix.Exclude.Values)
}

// MatrixUsesNeeds reports whether matrix expansion reads the needs context.
func MatrixUsesNeeds(strategy Strategy) bool {
	for _, input := range matrixExpressions(strategy.Matrix) {
		program, err := expression.Parse(input)
		if err == nil && program.UsesContext("needs") {
			return true
		}
	}
	return false
}

func matrixExpressions(matrix MatrixDefinition) []string {
	inputs := []string{matrix.Expression, matrix.Include.Expression, matrix.Exclude.Expression}
	for _, axis := range matrix.Axes {
		inputs = append(inputs, axis.Expression)
		for _, value := range axis.Values {
			if scalar, ok := scalarString(value); ok && containsExpression(scalar) {
				inputs = append(inputs, scalar)
			}
		}
	}
	for _, entries := range [][]map[string]any{matrix.Include.Values, matrix.Exclude.Values} {
		for _, entry := range entries {
			for _, value := range entry {
				if scalar, ok := scalarString(value); ok && containsExpression(scalar) {
					inputs = append(inputs, scalar)
				}
			}
		}
	}
	return inputs
}

func matrixValuesContainExpressions(values []any) bool {
	for _, value := range values {
		if scalar, ok := scalarString(value); ok && containsExpression(scalar) {
			return true
		}
	}
	return false
}

func matrixEntriesContainExpressions(entries []map[string]any) bool {
	for _, entry := range entries {
		for _, value := range entry {
			if scalar, ok := scalarString(value); ok && containsExpression(scalar) {
				return true
			}
		}
	}
	return false
}

// MatrixCombinations returns literal matrix values in stable axis and value
// order. Matrices containing expressions are expanded by EvaluateMatrix.
func MatrixCombinations(strategy Strategy) []map[string]any {
	if MatrixRequiresEvaluation(strategy) {
		return nil
	}
	combinations, _ := EvaluateMatrix("matrix", strategy, expression.Context{})
	return combinations
}

// EvaluateMatrix evaluates and expands a matrix definition.
func EvaluateMatrix(jobID string, strategy Strategy, context expression.Context) ([]map[string]any, error) {
	if !strategy.configured && strategy.Matrix.Expression == "" && len(strategy.Matrix.Axes) == 0 && len(strategy.Matrix.Include.Values) == 0 && strategy.Matrix.Include.Expression == "" {
		return nil, nil
	}
	context.Availability = matrixAvailability
	matrix, err := resolveMatrixDefinition(jobID, strategy.Matrix, context)
	if err != nil {
		return nil, err
	}
	return expandMatrix(jobID, matrix)
}

type resolvedMatrix struct {
	axes    map[string][]any
	include []map[string]any
	exclude []map[string]any
}

func resolveMatrixDefinition(jobID string, definition MatrixDefinition, context expression.Context) (resolvedMatrix, error) {
	if definition.Expression != "" {
		value, err := evaluateMatrixExpression(fmt.Sprintf("job %q matrix", jobID), definition.Expression, context)
		if err != nil {
			return resolvedMatrix{}, err
		}
		mapping, ok := value.(map[string]any)
		if !ok {
			return resolvedMatrix{}, fmt.Errorf("job %q matrix expression must evaluate to a mapping", jobID)
		}
		return resolvedMatrixFromMapping(jobID, mapping)
	}

	resolved := resolvedMatrix{axes: make(map[string][]any, len(definition.Axes))}
	for name, axis := range definition.Axes {
		values := axis.Values
		evaluateValues := true
		if axis.Expression != "" {
			value, err := evaluateMatrixExpression(fmt.Sprintf("job %q matrix axis %q", jobID, name), axis.Expression, context)
			if err != nil {
				return resolvedMatrix{}, err
			}
			var ok bool
			values, ok = value.([]any)
			if !ok {
				return resolvedMatrix{}, fmt.Errorf("job %q matrix axis %q expression must evaluate to an array", jobID, name)
			}
			evaluateValues = false
		}
		resolvedValues, err := resolveMatrixValues(fmt.Sprintf("job %q matrix axis %q", jobID, name), values, context, evaluateValues)
		if err != nil {
			return resolvedMatrix{}, err
		}
		resolved.axes[name] = resolvedValues
	}
	var err error
	resolved.include, err = resolveMatrixEntries(jobID, "include", definition.Include, context)
	if err != nil {
		return resolvedMatrix{}, err
	}
	resolved.exclude, err = resolveMatrixEntries(jobID, "exclude", definition.Exclude, context)
	if err != nil {
		return resolvedMatrix{}, err
	}
	return resolved, nil
}

func resolvedMatrixFromMapping(jobID string, mapping map[string]any) (resolvedMatrix, error) {
	if len(mapping) > maxMapEntries+2 {
		return resolvedMatrix{}, fmt.Errorf("job %q matrix defines too many fields", jobID)
	}
	resolved := resolvedMatrix{axes: map[string][]any{}}
	for name, value := range mapping {
		switch name {
		case "include", "exclude":
			entries, err := matrixEntryList(fmt.Sprintf("job %q matrix %s", jobID, name), value)
			if err != nil {
				return resolvedMatrix{}, err
			}
			if name == "include" {
				resolved.include = entries
			} else {
				resolved.exclude = entries
			}
		default:
			if name == "" || utf8.RuneCountInString(name) > maxMapKeyLength {
				return resolvedMatrix{}, fmt.Errorf("job %q matrix axis %q must contain 1 to %d characters", jobID, name, maxMapKeyLength)
			}
			values, ok := value.([]any)
			if !ok {
				return resolvedMatrix{}, fmt.Errorf("job %q matrix axis %q must be an array", jobID, name)
			}
			resolved.axes[name] = values
		}
	}
	if len(resolved.axes) > maxMapEntries {
		return resolvedMatrix{}, fmt.Errorf("job %q matrix defines %d axes; maximum is %d", jobID, len(resolved.axes), maxMapEntries)
	}
	for name, values := range resolved.axes {
		if _, err := resolveMatrixValues(fmt.Sprintf("job %q matrix axis %q", jobID, name), values, expression.Context{}, false); err != nil {
			return resolvedMatrix{}, err
		}
	}
	return resolved, nil
}

func resolveMatrixEntries(jobID, name string, entries MatrixEntries, context expression.Context) ([]map[string]any, error) {
	if entries.Expression == "" {
		resolved := make([]map[string]any, 0, len(entries.Values))
		for index, entry := range entries.Values {
			values, err := resolveMatrixEntry(fmt.Sprintf("job %q matrix %s entry %d", jobID, name, index+1), entry, context, true)
			if err != nil {
				return nil, err
			}
			resolved = append(resolved, values)
		}
		return resolved, nil
	}
	value, err := evaluateMatrixExpression(fmt.Sprintf("job %q matrix %s", jobID, name), entries.Expression, context)
	if err != nil {
		return nil, err
	}
	return matrixEntryList(fmt.Sprintf("job %q matrix %s", jobID, name), value)
}

func matrixEntryList(field string, value any) ([]map[string]any, error) {
	items, ok := value.([]any)
	if !ok {
		return nil, fmt.Errorf("%s must be an array of mappings", field)
	}
	if len(items) > maxMatrixJobs {
		return nil, fmt.Errorf("%s defines more than %d entries", field, maxMatrixJobs)
	}
	entries := make([]map[string]any, 0, len(items))
	for index, item := range items {
		entry, ok := item.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("%s entry %d must be a mapping", field, index+1)
		}
		resolved, err := resolveMatrixEntry(fmt.Sprintf("%s entry %d", field, index+1), entry, expression.Context{}, false)
		if err != nil {
			return nil, err
		}
		entries = append(entries, resolved)
	}
	return entries, nil
}

func resolveMatrixEntry(field string, entry map[string]any, context expression.Context, evaluateExpressions bool) (map[string]any, error) {
	if len(entry) == 0 {
		return nil, fmt.Errorf("%s must not be empty", field)
	}
	if len(entry) > maxMapEntries {
		return nil, fmt.Errorf("%s defines %d values; maximum is %d", field, len(entry), maxMapEntries)
	}
	result := make(map[string]any, len(entry))
	for name, value := range entry {
		if name == "" || utf8.RuneCountInString(name) > maxMapKeyLength {
			return nil, fmt.Errorf("%s key %q must contain 1 to %d characters", field, name, maxMapKeyLength)
		}
		resolved, err := resolveMatrixValue(field, value, context, evaluateExpressions)
		if err != nil {
			return nil, err
		}
		result[name] = resolved
	}
	return result, nil
}

func resolveMatrixValues(field string, values []any, context expression.Context, evaluateExpressions bool) ([]any, error) {
	if len(values) == 0 {
		return nil, fmt.Errorf("%s must define at least one value", field)
	}
	if len(values) > maxMatrixJobs {
		return nil, fmt.Errorf("%s defines more than %d values", field, maxMatrixJobs)
	}
	result := make([]any, 0, len(values))
	for _, value := range values {
		resolved, err := resolveMatrixValue(field, value, context, evaluateExpressions)
		if err != nil {
			return nil, err
		}
		result = append(result, resolved)
	}
	return result, nil
}

func resolveMatrixValue(field string, value any, context expression.Context, evaluateExpressions bool) (any, error) {
	if input, ok := value.(string); evaluateExpressions && ok && containsExpression(input) {
		var err error
		value, err = evaluateMatrixExpression(field+" value", input, context)
		if err != nil {
			return nil, err
		}
	}
	scalar, ok := scalarString(value)
	if !ok {
		return nil, fmt.Errorf("%s values must be scalars", field)
	}
	if utf8.RuneCountInString(scalar) > maxMatrixValueLength {
		return nil, fmt.Errorf("%s value exceeds %d characters", field, maxMatrixValueLength)
	}
	return value, nil
}

func evaluateMatrixExpression(field, input string, context expression.Context) (any, error) {
	program, err := expression.Parse(input)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", field, err)
	}
	result, err := program.Evaluate(context)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", field, err)
	}
	return result.Value, nil
}

func expandMatrix(jobID string, matrix resolvedMatrix) ([]map[string]any, error) {
	if len(matrix.axes) == 0 {
		if len(matrix.include) == 0 {
			return nil, fmt.Errorf("job %q matrix must define at least one axis or include entry", jobID)
		}
		if len(matrix.include) > maxMatrixJobs {
			return nil, fmt.Errorf("job %q matrix expands to more than %d jobs", jobID, maxMatrixJobs)
		}
		return cloneMatrixEntries(matrix.include), nil
	}

	axisNames := make([]string, 0, len(matrix.axes))
	for name := range matrix.axes {
		axisNames = append(axisNames, name)
	}
	sort.Strings(axisNames)
	combinationCount := 1
	for _, name := range axisNames {
		values := matrix.axes[name]
		if combinationCount > maxMatrixJobs/len(values) {
			return nil, fmt.Errorf("job %q matrix expands to more than %d jobs", jobID, maxMatrixJobs)
		}
		combinationCount *= len(values)
	}
	combinations := make([]map[string]any, 0, combinationCount)
	var visit func(int, map[string]any) error
	visit = func(axis int, values map[string]any) error {
		if axis == len(axisNames) {
			if matrixEntryMatchesAny(values, matrix.exclude) {
				return nil
			}
			combinations = append(combinations, cloneMatrixEntry(values))
			if len(combinations) > maxMatrixJobs {
				return fmt.Errorf("job %q matrix expands to more than %d jobs", jobID, maxMatrixJobs)
			}
			return nil
		}
		name := axisNames[axis]
		for _, value := range matrix.axes[name] {
			values[name] = value
			if !matrixPartialMatchAny(values, axisNames[:axis+1], matrix.exclude) {
				if err := visit(axis+1, values); err != nil {
					return err
				}
			}
		}
		delete(values, name)
		return nil
	}
	if err := visit(0, map[string]any{}); err != nil {
		return nil, err
	}

	originals := cloneMatrixEntries(combinations)
	for _, include := range matrix.include {
		applied := false
		for index, original := range originals {
			if !matrixEntriesCompatible(original, include) {
				continue
			}
			for name, value := range include {
				combinations[index][name] = value
			}
			if len(combinations[index]) > maxMapEntries {
				return nil, fmt.Errorf("job %q matrix combination defines more than %d values", jobID, maxMapEntries)
			}
			applied = true
		}
		if !applied {
			combinations = append(combinations, cloneMatrixEntry(include))
		}
		if len(combinations) > maxMatrixJobs {
			return nil, fmt.Errorf("job %q matrix expands to more than %d jobs", jobID, maxMatrixJobs)
		}
	}
	if len(combinations) == 0 {
		return nil, fmt.Errorf("job %q matrix expands to no jobs", jobID)
	}
	return combinations, nil
}

func matrixEntryMatchesAny(values map[string]any, entries []map[string]any) bool {
	for _, entry := range entries {
		matched := true
		for name, value := range entry {
			actual, found := values[name]
			if !found || !matrixValuesEqual(actual, value) {
				matched = false
				break
			}
		}
		if matched {
			return true
		}
	}
	return false
}

func matrixPartialMatchAny(values map[string]any, assigned []string, entries []map[string]any) bool {
	assignedNames := make(map[string]struct{}, len(assigned))
	for _, name := range assigned {
		assignedNames[name] = struct{}{}
	}
	for _, entry := range entries {
		if len(entry) > len(assignedNames) {
			continue
		}
		matched := true
		for name, value := range entry {
			if _, assigned := assignedNames[name]; !assigned || !matrixValuesEqual(values[name], value) {
				matched = false
				break
			}
		}
		if matched {
			return true
		}
	}
	return false
}

func matrixEntriesCompatible(original, include map[string]any) bool {
	for name, value := range include {
		if existing, found := original[name]; found && !matrixValuesEqual(existing, value) {
			return false
		}
	}
	return true
}

func matrixValuesEqual(left, right any) bool {
	leftString, leftOK := scalarString(left)
	rightString, rightOK := scalarString(right)
	return leftOK && rightOK && leftString == rightString
}

func cloneMatrixEntries(entries []map[string]any) []map[string]any {
	result := make([]map[string]any, 0, len(entries))
	for _, entry := range entries {
		result = append(result, cloneMatrixEntry(entry))
	}
	return result
}

func cloneMatrixEntry(entry map[string]any) map[string]any {
	result := make(map[string]any, len(entry))
	for name, value := range entry {
		result[name] = value
	}
	return result
}

func validateJobOutputs(jobID string, outputs map[string]any) (int, error) {
	field := fmt.Sprintf("job %q outputs", jobID)
	bytes, err := validateScalarMap(field, outputs, jobOutputAvailability)
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

func mergedScalarMapBytes(base, override map[string]any) int {
	total := 0
	for key, value := range base {
		if _, found := override[key]; found {
			continue
		}
		scalar, _ := scalarString(value)
		total += len(key) + len(scalar)
	}
	for key, value := range override {
		scalar, _ := scalarString(value)
		total += len(key) + len(scalar)
	}
	return total
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
	case json.Number:
		return typed.String(), true
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
	job.TimeoutMinutes, err = job.TimeoutMinutes.evaluate(id, context)
	if err != nil {
		return Job{}, err
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

func EvaluateConcurrency(definition *Definition, event Event, variables any) (string, bool, error) {
	if definition.Concurrency.Group == "" {
		return "", false, nil
	}
	program, err := expression.Parse(definition.Concurrency.Group)
	if err != nil {
		return "", false, fmt.Errorf("parse concurrency group: %w", err)
	}
	eventValues := eventExpressionValue(event)
	result, err := program.Evaluate(expression.Context{
		Availability: expression.NewAvailability("github", "inputs", "vars"),
		Values: map[string]any{
			"inputs": event.InputValues,
			"vars":   variables,
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
	if event.Payload != nil {
		return event.Payload
	}
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
			if event.BaseSHA != "" {
				pullRequest["base"].(map[string]any)["sha"] = event.BaseSHA
			}
		} else if event.Name == "pull_request_target" {
			pullRequest["base"].(map[string]any)["sha"] = event.SHA
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

func (t *JobTimeout) UnmarshalYAML(node *yaml.Node) error {
	t.configured = true
	if node.Kind != yaml.ScalarNode {
		return fmt.Errorf("timeout-minutes must be a positive integer or expression")
	}
	if node.Tag == "!!int" {
		if err := node.Decode(&t.minutes); err != nil {
			return fmt.Errorf("timeout-minutes must be a positive integer: %w", err)
		}
		return nil
	}
	if node.Tag == "!!str" && containsExpression(node.Value) {
		t.expression = node.Value
		return nil
	}
	return fmt.Errorf("timeout-minutes must be a positive integer or expression")
}

func (t JobTimeout) validate(jobID string) error {
	if !t.configured {
		return nil
	}
	if t.expression != "" {
		if len(t.expression) > MaxConditionBytes {
			return fmt.Errorf("job %q timeout-minutes exceeds %d bytes", jobID, MaxConditionBytes)
		}
		return validateTemplate(fmt.Sprintf("job %q timeout-minutes", jobID), t.expression, jobNameAvailability)
	}
	if t.minutes < 1 {
		return fmt.Errorf("job %q timeout-minutes must be a positive integer", jobID)
	}
	return nil
}

func (t JobTimeout) evaluate(jobID string, context expression.Context) (JobTimeout, error) {
	if !t.configured {
		return JobTimeout{minutes: DefaultJobTimeoutMinutes, configured: true}, nil
	}
	if t.expression == "" {
		return t, nil
	}
	program, err := expression.Parse(t.expression)
	if err != nil {
		return JobTimeout{}, fmt.Errorf("job %q timeout-minutes: %w", jobID, err)
	}
	result, err := program.Evaluate(context)
	if err != nil {
		return JobTimeout{}, fmt.Errorf("job %q timeout-minutes: %w", jobID, err)
	}
	minutes, valid := positiveWholeMinutes(result.Value)
	if !valid {
		return JobTimeout{}, fmt.Errorf("job %q timeout-minutes must evaluate to a positive integer", jobID)
	}
	return JobTimeout{minutes: minutes, configured: true}, nil
}

func positiveWholeMinutes(value any) (int64, bool) {
	switch typed := value.(type) {
	case int:
		return int64(typed), typed > 0
	case int32:
		return int64(typed), typed > 0
	case int64:
		return typed, typed > 0
	case uint:
		if uint64(typed) > math.MaxInt64 {
			return 0, false
		}
		return int64(typed), typed > 0
	case uint32:
		return int64(typed), typed > 0
	case uint64:
		if typed > math.MaxInt64 {
			return 0, false
		}
		return int64(typed), typed > 0
	case float64:
		if math.IsNaN(typed) || math.IsInf(typed, 0) || typed < 1 || typed >= float64(math.MaxInt64) || math.Trunc(typed) != typed {
			return 0, false
		}
		return int64(typed), true
	case string:
		minutes, err := strconv.ParseInt(typed, 10, 64)
		return minutes, err == nil && minutes > 0
	default:
		return 0, false
	}
}

func (s *Strategy) UnmarshalYAML(node *yaml.Node) error {
	s.configured = true
	s.FailFast = true
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
			if err := decodeMatrixDefinition(value, &s.Matrix); err != nil {
				return err
			}
		case "max-parallel":
			s.maxParallelSet = true
			if err := value.Decode(&s.MaxParallel); err != nil {
				return err
			}
		case "fail-fast":
			if err := value.Decode(&s.FailFast); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unsupported job strategy field %q", name)
		}
	}
	return nil
}

func decodeMatrixDefinition(node *yaml.Node, matrix *MatrixDefinition) error {
	if node.Kind == yaml.ScalarNode {
		if err := node.Decode(&matrix.Expression); err != nil {
			return err
		}
		return nil
	}
	if node.Kind != yaml.MappingNode {
		return fmt.Errorf("job strategy matrix must be a mapping or expression")
	}
	if err := rejectDuplicateMappingKeys(node, "job strategy matrix"); err != nil {
		return err
	}
	matrix.Axes = map[string]MatrixAxis{}
	for index := 0; index < len(node.Content); index += 2 {
		name := node.Content[index].Value
		value := node.Content[index+1]
		switch name {
		case "include", "exclude":
			entries := &matrix.Include
			if name == "exclude" {
				entries = &matrix.Exclude
			}
			if err := decodeMatrixEntries(value, "job strategy matrix "+name, entries); err != nil {
				return err
			}
		default:
			axis := MatrixAxis{}
			switch value.Kind {
			case yaml.SequenceNode:
				if err := value.Decode(&axis.Values); err != nil {
					return err
				}
			case yaml.ScalarNode:
				if err := value.Decode(&axis.Expression); err != nil {
					return err
				}
			default:
				return fmt.Errorf("job strategy matrix axis %q must be an array or expression", name)
			}
			matrix.Axes[name] = axis
		}
	}
	return nil
}

func decodeMatrixEntries(node *yaml.Node, field string, entries *MatrixEntries) error {
	if node.Kind == yaml.ScalarNode {
		return node.Decode(&entries.Expression)
	}
	if node.Kind != yaml.SequenceNode {
		return fmt.Errorf("%s must be an array or expression", field)
	}
	entries.Values = make([]map[string]any, 0, len(node.Content))
	for index, item := range node.Content {
		if item.Kind != yaml.MappingNode {
			return fmt.Errorf("%s entry %d must be a mapping", field, index+1)
		}
		if err := rejectDuplicateMappingKeys(item, fmt.Sprintf("%s entry %d", field, index+1)); err != nil {
			return err
		}
		entry := map[string]any{}
		if err := item.Decode(&entry); err != nil {
			return err
		}
		entries.Values = append(entries.Values, entry)
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
