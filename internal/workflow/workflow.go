package workflow

import (
	"bytes"
	"fmt"
	"io"
	"regexp"
	"strings"
	"unicode/utf8"

	"github.com/kelos-dev/open-actions/internal/actionref"
	"gopkg.in/yaml.v3"
)

const (
	maxWorkflowBytes          = 1_000_000
	maxWorkflowNameLength     = 256
	maxConcurrencyGroupLength = 256
	maxJobs                   = 1000
	maxJobIDLength            = 256
	maxJobNameLength          = 256
	maxRunnerLabels           = 16
	maxSteps                  = 100
	maxStepNameLength         = 256
	maxActionReferenceLength  = 512
	maxRunScriptBytes         = 65_536
	maxWorkingDirectoryLength = 512
	maxMapEntries             = 100
	maxMapKeyLength           = 256
	maxMapValueBytes          = 65_536
	maxJobContentBytes        = 100_000
	maxBranchPatterns         = 256
	maxBranchPatternLength    = 256
	maxActivityTypes          = 64
	maxActivityTypeLength     = 64
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
	Branches    []string `yaml:"branches"`
	Types       []string `yaml:"types"`
	branchesSet bool
	typesSet    bool
}

type Job struct {
	Name      string         `yaml:"name"`
	RunsOn    StringList     `yaml:"runs-on"`
	Needs     StringList     `yaml:"needs"`
	Steps     []Step         `yaml:"steps"`
	Strategy  yaml.Node      `yaml:"strategy"`
	Container yaml.Node      `yaml:"container"`
	Services  yaml.Node      `yaml:"services"`
	If        string         `yaml:"if"`
	Env       map[string]any `yaml:"env"`
}

type Step struct {
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
	Name    string
	Action  string
	Ref     string
	RefName string
	HeadRef string
	BaseRef string
}

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
	if err := validateTrigger(definition.On); err != nil {
		return nil, err
	}
	if len(definition.Jobs) == 0 {
		return nil, fmt.Errorf("workflow must define at least one job")
	}
	if len(definition.Jobs) > maxJobs {
		return nil, fmt.Errorf("workflow defines %d jobs; maximum is %d", len(definition.Jobs), maxJobs)
	}
	for id, job := range definition.Jobs {
		if err := validateJob(id, &job); err != nil {
			return nil, err
		}
		definition.Jobs[id] = job
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
	if containsExpression(job.Name) {
		return fmt.Errorf("job %q name uses an unsupported expression", id)
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
	if len(job.Needs) > 0 {
		return fmt.Errorf("job %q uses unsupported needs", id)
	}
	if job.Strategy.Kind != 0 || job.Container.Kind != 0 || job.Services.Kind != 0 || job.If != "" {
		return fmt.Errorf("job %q uses an unsupported job feature", id)
	}
	if len(job.Steps) == 0 {
		return fmt.Errorf("job %q must define at least one step", id)
	}
	if len(job.Steps) > maxSteps {
		return fmt.Errorf("job %q defines %d steps; maximum is %d", id, len(job.Steps), maxSteps)
	}
	contentBytes := len(job.Name)
	envBytes, err := validateEnvironmentMap(fmt.Sprintf("job %q env", id), job.Env)
	if err != nil {
		return err
	}
	contentBytes += envBytes
	for i, step := range job.Steps {
		if utf8.RuneCountInString(step.Name) > maxStepNameLength {
			return fmt.Errorf("job %q step %d name exceeds %d characters", id, i+1, maxStepNameLength)
		}
		if containsExpression(step.Name) {
			return fmt.Errorf("job %q step %d name uses an unsupported expression", id, i+1)
		}
		if (step.Uses == "") == (step.Run == "") {
			return fmt.Errorf("job %q step %d must define exactly one of uses or run", id, i+1)
		}
		if step.Uses != "" {
			if utf8.RuneCountInString(step.Uses) > maxActionReferenceLength {
				return fmt.Errorf("job %q step %d action reference exceeds %d characters", id, i+1, maxActionReferenceLength)
			}
			if _, err := actionref.Parse(step.Uses); err != nil {
				return fmt.Errorf("job %q step %d: %w", id, i+1, err)
			}
		}
		if len(step.Run) > maxRunScriptBytes {
			return fmt.Errorf("job %q step %d run script exceeds %d bytes", id, i+1, maxRunScriptBytes)
		}
		if containsExpression(step.Run) {
			return fmt.Errorf("job %q step %d run script uses an unsupported expression", id, i+1)
		}
		if utf8.RuneCountInString(step.WorkingDirectory) > maxWorkingDirectoryLength {
			return fmt.Errorf("job %q step %d working-directory exceeds %d characters", id, i+1, maxWorkingDirectoryLength)
		}
		if containsExpression(step.WorkingDirectory) {
			return fmt.Errorf("job %q step %d working-directory uses an unsupported expression", id, i+1)
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
			return fmt.Errorf("job %q step %d uses unsupported if", id, i+1)
		}
		withBytes, err := validateScalarMap(fmt.Sprintf("job %q step %d with", id, i+1), step.With)
		if err != nil {
			return err
		}
		stepEnvBytes, err := validateEnvironmentMap(fmt.Sprintf("job %q step %d env", id, i+1), step.Env)
		if err != nil {
			return err
		}
		contentBytes += len(step.Name) + len(step.Uses) + len(step.Run) + len(step.WorkingDirectory) + withBytes + stepEnvBytes
	}
	if contentBytes > maxJobContentBytes {
		return fmt.Errorf("job %q configuration exceeds %d bytes", id, maxJobContentBytes)
	}
	return nil
}

func validateEnvironmentMap(field string, values map[string]any) (int, error) {
	for name := range values {
		if reservedEnvironmentName(name) {
			return 0, fmt.Errorf("%s contains reserved variable %q", field, name)
		}
	}
	return validateScalarMap(field, values)
}

func reservedEnvironmentName(name string) bool {
	name = strings.ToUpper(name)
	return strings.HasPrefix(name, "GITHUB_") || strings.HasPrefix(name, "RUNNER_")
}

func validateScalarMap(field string, values map[string]any) (int, error) {
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
		if containsExpression(scalar) {
			return 0, fmt.Errorf("%s value %q uses an unsupported expression", field, key)
		}
		if len(scalar) > maxMapValueBytes {
			return 0, fmt.Errorf("%s value %q exceeds %d bytes", field, key, maxMapValueBytes)
		}
		total += len(key) + len(scalar)
	}
	return total, nil
}

func containsExpression(value string) bool {
	return strings.Contains(value, "${{")
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

func EvaluateConcurrency(definition *Definition, event Event) (string, bool, error) {
	if definition.Concurrency.Group == "" {
		return "", false, nil
	}
	var evaluationError error
	unavailable := func(name string) string {
		if evaluationError == nil {
			evaluationError = fmt.Errorf("concurrency context %q is unavailable for this event", name)
		}
		return ""
	}
	group := expressionPattern.ReplaceAllStringFunc(definition.Concurrency.Group, func(match string) string {
		expression := strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(match, "${{"), "}}"))
		switch expression {
		case "github.workflow":
			if definition.Name == "" {
				return unavailable("github.workflow")
			}
			return definition.Name
		case "github.head_ref || github.ref_name":
			if event.HeadRef != "" {
				return event.HeadRef
			}
			if event.RefName == "" {
				return unavailable("github.head_ref || github.ref_name")
			}
			return event.RefName
		case "github.head_ref":
			if event.HeadRef == "" {
				return unavailable("github.head_ref")
			}
			return event.HeadRef
		case "github.ref_name":
			if event.RefName == "" {
				return unavailable("github.ref_name")
			}
			return event.RefName
		default:
			evaluationError = fmt.Errorf("unsupported concurrency expression %q", expression)
			return ""
		}
	})
	if evaluationError != nil {
		return "", false, evaluationError
	}
	if strings.Contains(group, "${{") {
		return "", false, fmt.Errorf("invalid concurrency expression %q", definition.Concurrency.Group)
	}
	if group == "" {
		return "", false, fmt.Errorf("evaluated concurrency group must not be empty")
	}
	if utf8.RuneCountInString(group) > maxConcurrencyGroupLength {
		return "", false, fmt.Errorf("evaluated concurrency group exceeds %d characters", maxConcurrencyGroupLength)
	}
	return group, definition.Concurrency.CancelInProgress, nil
}

var expressionPattern = regexp.MustCompile(`\$\{\{[^{}]+}}`)
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
	"merge_group": {"checks_requested"},
}

var defaultPullRequestTypes = []string{"opened", "synchronize", "reopened"}
var defaultMergeGroupTypes = []string{"checks_requested"}

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
		if len(filter.Branches) > maxBranchPatterns {
			return fmt.Errorf("trigger %q defines %d branch patterns; maximum is %d", eventName, len(filter.Branches), maxBranchPatterns)
		}
		if filter.branchesSet && len(filter.Branches) == 0 {
			return fmt.Errorf("trigger %q branch filters must not be empty", eventName)
		}
		hasPositivePattern := false
		for _, pattern := range filter.Branches {
			if utf8.RuneCountInString(pattern) > maxBranchPatternLength {
				return fmt.Errorf("trigger %q branch pattern exceeds %d characters", eventName, maxBranchPatternLength)
			}
			if !strings.HasPrefix(pattern, "!") {
				hasPositivePattern = true
			}
			candidate := strings.TrimPrefix(pattern, "!")
			if candidate == "" {
				return fmt.Errorf("trigger %q contains an empty branch pattern", eventName)
			}
			if _, err := compileGitHubPattern(candidate); err != nil {
				return fmt.Errorf("trigger %q contains invalid branch pattern %q: %w", eventName, pattern, err)
			}
		}
		if len(filter.Branches) > 0 && !hasPositivePattern {
			return fmt.Errorf("trigger %q branch filters must include a positive pattern", eventName)
		}
	}
	return nil
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
		case "types":
			f.typesSet = true
			if err := value.Decode(&f.Types); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unsupported event filter %q", name)
		}
	}
	return nil
}

func Matches(trigger Trigger, event Event) bool {
	filter, found := trigger.Events[event.Name]
	if !found {
		return false
	}
	types := filter.Types
	if event.Name == "pull_request" && !filter.typesSet && len(types) == 0 {
		types = defaultPullRequestTypes
	}
	if event.Name == "merge_group" && !filter.typesSet && len(types) == 0 {
		types = defaultMergeGroupTypes
	}
	if len(types) > 0 && !contains(types, event.Action) {
		return false
	}
	if event.Name == "push" && len(filter.Branches) > 0 && !strings.HasPrefix(event.Ref, "refs/heads/") {
		return false
	}
	branch := event.RefName
	if event.Name == "pull_request" || event.Name == "merge_group" {
		branch = event.BaseRef
	}
	return len(filter.Branches) == 0 || matchesBranch(filter.Branches, branch)
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
			if value.Kind != yaml.ScalarNode || value.Tag != "!!null" {
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
