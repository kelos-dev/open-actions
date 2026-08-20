package expression

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
)

const maxEvaluationStringBytes = 100_000

// Availability identifies the contexts and status functions allowed at a
// workflow evaluation phase.
type Availability struct {
	contexts        map[string]struct{}
	statusFunctions bool
	hashFiles       bool
}

// NewAvailability returns an availability set for the named contexts.
func NewAvailability(contexts ...string) Availability {
	result := Availability{contexts: make(map[string]struct{}, len(contexts))}
	for _, name := range contexts {
		result.contexts[strings.ToLower(name)] = struct{}{}
	}
	return result
}

// WithStatusFunctions permits success, always, failure, and cancelled.
func (a Availability) WithStatusFunctions() Availability {
	result := NewAvailability()
	for name := range a.contexts {
		result.contexts[name] = struct{}{}
	}
	result.statusFunctions = true
	result.hashFiles = a.hashFiles
	return result
}

// WithHashFiles permits hashFiles in runner-backed step expressions.
func (a Availability) WithHashFiles() Availability {
	result := NewAvailability()
	for name := range a.contexts {
		result.contexts[name] = struct{}{}
	}
	result.statusFunctions = a.statusFunctions
	result.hashFiles = true
	return result
}

func (a Availability) allowsContext(name string) bool {
	_, found := a.contexts[strings.ToLower(name)]
	return found
}

// Status supplies the state used by status check functions.
type Status struct {
	Success   bool
	Failure   bool
	Cancelled bool
}

// Context supplies values and availability for one evaluation.
type Context struct {
	Availability Availability
	Values       map[string]any
	Status       *Status
}

// Program is a parsed expression template or condition.
type Program struct {
	segments []segment
	whole    bool
}

type segment struct {
	literal    string
	expression node
}

// Parse parses an interpolation template. A template consisting of exactly
// one expression preserves that expression's result type.
func Parse(input string) (*Program, error) {
	var segments []segment
	offset := 0
	for offset < len(input) {
		relative := strings.Index(input[offset:], "${{")
		if relative < 0 {
			segments = append(segments, segment{literal: input[offset:]})
			offset = len(input)
			break
		}
		start := offset + relative
		if start > offset {
			segments = append(segments, segment{literal: input[offset:start]})
		}
		end, err := expressionEnd(input, start+3)
		if err != nil {
			return nil, err
		}
		source := strings.TrimSpace(input[start+3 : end])
		if source == "" {
			return nil, fmt.Errorf("expression at column %d is empty", start+1)
		}
		parsed, err := parseExpression(source)
		if err != nil {
			return nil, fmt.Errorf("parse expression at column %d: %w", start+1, err)
		}
		segments = append(segments, segment{expression: parsed})
		offset = end + 2
	}
	if len(input) == 0 {
		segments = append(segments, segment{})
	}
	whole := len(segments) == 1 && segments[0].expression != nil
	return &Program{segments: segments, whole: whole}, nil
}

// ParseCondition parses a condition with optional expression delimiters.
func ParseCondition(input string) (*Program, error) {
	trimmed := strings.TrimSpace(input)
	if strings.HasPrefix(trimmed, "${{") {
		program, err := Parse(trimmed)
		if err != nil {
			return nil, err
		}
		if !program.whole {
			return nil, fmt.Errorf("condition must contain exactly one expression")
		}
		return program, nil
	}
	if trimmed == "" {
		return nil, fmt.Errorf("condition is empty")
	}
	parsed, err := parseExpression(trimmed)
	if err != nil {
		return nil, err
	}
	return &Program{segments: []segment{{expression: parsed}}, whole: true}, nil
}

// IsWholeExpression reports whether the template consists of exactly one
// expression and therefore preserves the expression's result type.
func (p *Program) IsWholeExpression() bool {
	return p.whole
}

func expressionEnd(input string, offset int) (int, error) {
	quoted := false
	for index := offset; index < len(input); index++ {
		if input[index] == '\'' {
			if quoted && index+1 < len(input) && input[index+1] == '\'' {
				index++
				continue
			}
			quoted = !quoted
			continue
		}
		if quoted {
			continue
		}
		if index+2 < len(input) && input[index:index+3] == "${{" {
			return 0, fmt.Errorf("nested expression at column %d", index+1)
		}
		if index+1 < len(input) && input[index:index+2] == "}}" {
			return index, nil
		}
	}
	return 0, fmt.Errorf("expression at column %d is not closed", offset-2)
}

// Validate checks syntax-dependent context and function availability without
// evaluating either side of an operator.
func (p *Program) Validate(availability Availability) error {
	for _, item := range p.segments {
		if item.expression != nil {
			if err := item.expression.validate(availability); err != nil {
				return err
			}
		}
	}
	return nil
}

// UsesStatusFunction reports whether the program explicitly calls a status
// check function. Conditions without one receive GitHub's default success
// check from their caller.
func (p *Program) UsesStatusFunction() bool {
	for _, item := range p.segments {
		if usesStatusFunction(item.expression) {
			return true
		}
	}
	return false
}

func usesStatusFunction(input node) bool {
	switch typed := input.(type) {
	case nil, literalNode, contextNode, wildcardNode:
		return false
	case propertyNode:
		return usesStatusFunction(typed.target)
	case indexNode:
		return usesStatusFunction(typed.target) || usesStatusFunction(typed.index)
	case unaryNode:
		return usesStatusFunction(typed.operand)
	case binaryNode:
		return usesStatusFunction(typed.left) || usesStatusFunction(typed.right)
	case callNode:
		switch strings.ToLower(typed.name) {
		case "success", "always", "failure", "cancelled":
			return true
		}
		for _, argument := range typed.arguments {
			if usesStatusFunction(argument) {
				return true
			}
		}
	}
	return false
}

// Evaluate evaluates a parsed template with the supplied context.
func (p *Program) Evaluate(context Context) (Result, error) {
	if err := p.Validate(context.Availability); err != nil {
		return Result{}, err
	}
	evaluation := newEvaluation(context)
	if p.whole {
		result, err := p.segments[0].expression.evaluate(evaluation)
		if err != nil {
			return Result{}, err
		}
		if result.kind == stringKind && len(result.text) > maxEvaluationStringBytes {
			return Result{}, evaluationSizeError()
		}
		resolved, err := result.interfaceValue()
		if err != nil {
			return Result{}, err
		}
		return Result{Value: resolved, Secret: result.containsSensitive()}, nil
	}
	var result strings.Builder
	sensitive := false
	for _, item := range p.segments {
		if item.expression == nil {
			if err := appendEvaluationString(&result, item.literal); err != nil {
				return Result{}, err
			}
			continue
		}
		resolved, err := item.expression.evaluate(evaluation)
		if err != nil {
			return Result{}, err
		}
		text, err := resolved.stringValue()
		if err != nil {
			return Result{}, err
		}
		if err := appendEvaluationString(&result, text); err != nil {
			return Result{}, err
		}
		sensitive = sensitive || resolved.sensitive
	}
	return Result{Value: result.String(), Secret: sensitive}, nil
}

func appendEvaluationString(result *strings.Builder, text string) error {
	if len(text) > maxEvaluationStringBytes-result.Len() {
		return evaluationSizeError()
	}
	result.WriteString(text)
	return nil
}

func evaluationSizeError() error {
	return fmt.Errorf("evaluated expression exceeds %d bytes", maxEvaluationStringBytes)
}

type evaluation struct {
	roots  map[string]value
	status *Status
}

func newEvaluation(context Context) *evaluation {
	roots := make(map[string]value, len(context.Values))
	for name, input := range context.Values {
		roots[strings.ToLower(name)] = normalize(input, strings.EqualFold(name, "secrets"))
	}
	return &evaluation{roots: roots, status: context.Status}
}

func (e *evaluation) context(name string) (value, error) {
	if result, found := e.roots[strings.ToLower(name)]; found {
		return result, nil
	}
	return value{}, fmt.Errorf("context %q is unavailable", name)
}

type node interface {
	validate(Availability) error
	evaluate(*evaluation) (value, error)
}

type literalNode struct {
	value value
}

func (n literalNode) validate(Availability) error { return nil }
func (n literalNode) evaluate(*evaluation) (value, error) {
	return n.value, nil
}

type contextNode struct {
	name string
}

type wildcardNode struct{}

func (wildcardNode) validate(Availability) error { return nil }
func (wildcardNode) evaluate(*evaluation) (value, error) {
	return value{}, fmt.Errorf("wildcard is only valid in property access")
}

func (n contextNode) validate(availability Availability) error {
	if !availability.allowsContext(n.name) {
		return fmt.Errorf("context %q is unavailable", n.name)
	}
	return nil
}

func (n contextNode) evaluate(evaluation *evaluation) (value, error) {
	return evaluation.context(n.name)
}

type propertyNode struct {
	target node
	name   string
}

func (n propertyNode) validate(availability Availability) error {
	return n.target.validate(availability)
}

func (n propertyNode) evaluate(evaluation *evaluation) (value, error) {
	target, err := n.target.evaluate(evaluation)
	if err != nil {
		return value{}, err
	}
	return target.property(n.name)
}

type indexNode struct {
	target node
	index  node
}

func (n indexNode) validate(availability Availability) error {
	if err := n.target.validate(availability); err != nil {
		return err
	}
	return n.index.validate(availability)
}

func (n indexNode) evaluate(evaluation *evaluation) (value, error) {
	target, err := n.target.evaluate(evaluation)
	if err != nil {
		return value{}, err
	}
	if _, wildcard := n.index.(wildcardNode); wildcard {
		return target.wildcard()
	}
	index, err := n.index.evaluate(evaluation)
	if err != nil {
		return value{}, err
	}
	if target.kind == arrayKind && target.array.filtered {
		return target.filteredIndex(index)
	}
	if target.kind == arrayKind && index.kind == numberKind && index.number == math.Trunc(index.number) {
		position := int(index.number)
		if position >= 0 && position < len(target.array.items) {
			result := target.array.items[position]
			result.sensitive = result.sensitive || target.sensitive || index.sensitive
			return result, nil
		}
		return value{kind: stringKind, sensitive: target.sensitive || index.sensitive}, nil
	}
	name, err := index.stringValue()
	if err != nil {
		return value{}, fmt.Errorf("property index must be a scalar")
	}
	result, err := target.property(name)
	if err != nil {
		return value{}, err
	}
	result.sensitive = result.sensitive || index.sensitive
	return result, nil
}

type unaryNode struct {
	operand node
}

func (n unaryNode) validate(availability Availability) error {
	return n.operand.validate(availability)
}

func (n unaryNode) evaluate(evaluation *evaluation) (value, error) {
	operand, err := n.operand.evaluate(evaluation)
	if err != nil {
		return value{}, err
	}
	return value{kind: boolKind, boolean: !operand.truthy(), sensitive: operand.sensitive}, nil
}

type binaryNode struct {
	operator    tokenKind
	left, right node
}

func (n binaryNode) validate(availability Availability) error {
	if err := n.left.validate(availability); err != nil {
		return err
	}
	return n.right.validate(availability)
}

func (n binaryNode) evaluate(evaluation *evaluation) (value, error) {
	left, err := n.left.evaluate(evaluation)
	if err != nil {
		return value{}, err
	}
	if n.operator == andToken {
		if !left.truthy() {
			return left, nil
		}
		right, err := n.right.evaluate(evaluation)
		if err != nil {
			return value{}, err
		}
		right.sensitive = right.sensitive || left.sensitive
		return right, nil
	}
	if n.operator == orToken {
		if left.truthy() {
			return left, nil
		}
		right, err := n.right.evaluate(evaluation)
		if err != nil {
			return value{}, err
		}
		right.sensitive = right.sensitive || left.sensitive
		return right, nil
	}
	right, err := n.right.evaluate(evaluation)
	if err != nil {
		return value{}, err
	}
	sensitive := left.sensitive || right.sensitive
	switch n.operator {
	case equalToken:
		return value{kind: boolKind, boolean: equal(left, right), sensitive: sensitive}, nil
	case notEqualToken:
		return value{kind: boolKind, boolean: !equal(left, right), sensitive: sensitive}, nil
	case lessToken, lessEqualToken, greaterToken, greaterEqualToken:
		comparison, valid := compare(left, right)
		matched := false
		if valid {
			switch n.operator {
			case lessToken:
				matched = comparison < 0
			case lessEqualToken:
				matched = comparison <= 0
			case greaterToken:
				matched = comparison > 0
			case greaterEqualToken:
				matched = comparison >= 0
			}
		}
		return value{kind: boolKind, boolean: matched, sensitive: sensitive}, nil
	default:
		return value{}, fmt.Errorf("unsupported operator")
	}
}

type callNode struct {
	name      string
	arguments []node
}

func (n callNode) validate(availability Availability) error {
	name := strings.ToLower(n.name)
	minimum, maximum := 0, 0
	switch name {
	case "contains", "startswith", "endswith":
		minimum, maximum = 2, 2
	case "format":
		minimum, maximum = 2, int(^uint(0)>>1)
	case "join":
		minimum, maximum = 1, 2
	case "tojson", "fromjson":
		minimum, maximum = 1, 1
	case "hashfiles":
		if !availability.hashFiles {
			return fmt.Errorf("function %q is unavailable", n.name)
		}
		minimum, maximum = 1, int(^uint(0)>>1)
	case "success", "always", "failure", "cancelled":
		if !availability.statusFunctions {
			return fmt.Errorf("function %q is unavailable", n.name)
		}
	default:
		return fmt.Errorf("unsupported function %q", n.name)
	}
	if len(n.arguments) < minimum || len(n.arguments) > maximum {
		if minimum == maximum {
			return fmt.Errorf("function %q requires %d arguments", n.name, minimum)
		}
		return fmt.Errorf("function %q requires at least %d arguments", n.name, minimum)
	}
	for _, argument := range n.arguments {
		if err := argument.validate(availability); err != nil {
			return err
		}
	}
	return nil
}

func (n callNode) evaluate(evaluation *evaluation) (value, error) {
	name := strings.ToLower(n.name)
	switch name {
	case "success", "failure", "cancelled":
		if evaluation.status == nil {
			return value{}, fmt.Errorf("status is unavailable for function %q", n.name)
		}
		switch name {
		case "success":
			return value{kind: boolKind, boolean: evaluation.status.Success}, nil
		case "failure":
			return value{kind: boolKind, boolean: evaluation.status.Failure}, nil
		default:
			return value{kind: boolKind, boolean: evaluation.status.Cancelled}, nil
		}
	case "always":
		return value{kind: boolKind, boolean: true}, nil
	}
	arguments := make([]value, len(n.arguments))
	for index, argument := range n.arguments {
		resolved, err := argument.evaluate(evaluation)
		if err != nil {
			return value{}, err
		}
		arguments[index] = resolved
	}
	switch name {
	case "contains":
		return evaluateContains(arguments)
	case "startswith":
		return evaluateStartsWith(arguments)
	case "endswith":
		return evaluateEndsWith(arguments)
	case "format":
		return evaluateFormat(arguments)
	case "join":
		return evaluateJoin(arguments)
	case "tojson":
		return evaluateToJSON(arguments)
	case "fromjson":
		return evaluateFromJSON(arguments)
	case "hashfiles":
		return evaluateHashFiles(arguments, evaluation)
	default:
		return value{}, fmt.Errorf("unsupported function %q", n.name)
	}
}

func evaluateContains(arguments []value) (value, error) {
	search, item := arguments[0], arguments[1]
	sensitive := search.sensitive || item.sensitive
	if search.kind == arrayKind {
		candidateSensitive := false
		for _, candidate := range search.array.items {
			candidateSensitive = candidateSensitive || candidate.sensitive
			if equal(candidate, item) {
				return value{kind: boolKind, boolean: true, sensitive: sensitive || candidate.sensitive}, nil
			}
		}
		return value{kind: boolKind, sensitive: sensitive || candidateSensitive}, nil
	}
	searchText, err := search.stringValue()
	if err != nil {
		return value{}, fmt.Errorf("contains search value must be a scalar or array")
	}
	itemText, err := item.stringValue()
	if err != nil {
		return value{}, fmt.Errorf("contains item must be a scalar")
	}
	return value{kind: boolKind, boolean: strings.Contains(strings.ToLower(searchText), strings.ToLower(itemText)), sensitive: sensitive}, nil
}

func evaluateStartsWith(arguments []value) (value, error) {
	search, err := arguments[0].stringValue()
	if err != nil {
		return value{}, fmt.Errorf("startsWith search value must be a scalar")
	}
	prefix, err := arguments[1].stringValue()
	if err != nil {
		return value{}, fmt.Errorf("startsWith prefix must be a scalar")
	}
	return value{
		kind:      boolKind,
		boolean:   strings.HasPrefix(strings.ToLower(search), strings.ToLower(prefix)),
		sensitive: arguments[0].sensitive || arguments[1].sensitive,
	}, nil
}

func evaluateEndsWith(arguments []value) (value, error) {
	search, err := arguments[0].stringValue()
	if err != nil {
		return value{kind: boolKind, sensitive: arguments[0].sensitive || arguments[1].sensitive}, nil
	}
	suffix, err := arguments[1].stringValue()
	if err != nil {
		return value{kind: boolKind, sensitive: arguments[0].sensitive || arguments[1].sensitive}, nil
	}
	return value{
		kind:      boolKind,
		boolean:   strings.HasSuffix(strings.ToLower(search), strings.ToLower(suffix)),
		sensitive: arguments[0].sensitive || arguments[1].sensitive,
	}, nil
}

func evaluateJoin(arguments []value) (value, error) {
	items := arguments[0]
	if items.kind != arrayKind {
		text, err := items.stringValue()
		if err != nil {
			return value{kind: stringKind, sensitive: items.sensitive}, nil
		}
		return value{kind: stringKind, text: text, sensitive: items.sensitive}, nil
	}
	separator := ","
	sensitive := items.sensitive
	if len(items.array.items) > 1 && len(arguments) == 2 {
		var err error
		separator, err = arguments[1].stringValue()
		if err != nil {
			separator = ","
		}
		sensitive = sensitive || arguments[1].sensitive
	}
	var result strings.Builder
	for index, item := range items.array.items {
		if index > 0 {
			if err := appendEvaluationString(&result, separator); err != nil {
				return value{}, err
			}
		}
		text := item.joinStringValue()
		if err := appendEvaluationString(&result, text); err != nil {
			return value{}, err
		}
		sensitive = sensitive || item.sensitive
	}
	return value{kind: stringKind, text: result.String(), sensitive: sensitive}, nil
}

func evaluateToJSON(arguments []value) (value, error) {
	var result bytes.Buffer
	encoder := json.NewEncoder(&result)
	encoder.SetEscapeHTML(false)
	encoder.SetIndent("", "  ")
	input, err := arguments[0].interfaceValue()
	if err != nil {
		return value{}, err
	}
	if err := encoder.Encode(input); err != nil {
		return value{}, fmt.Errorf("serialize value as JSON: %w", err)
	}
	text := strings.TrimSuffix(result.String(), "\n")
	if len(text) > maxEvaluationStringBytes {
		return value{}, evaluationSizeError()
	}
	return value{kind: stringKind, text: text, sensitive: arguments[0].containsSensitive()}, nil
}

func evaluateFromJSON(arguments []value) (value, error) {
	input, err := arguments[0].stringValue()
	if err != nil {
		return value{}, fmt.Errorf("fromJSON value must be a scalar")
	}
	if len(input) > maxEvaluationStringBytes {
		return value{}, evaluationSizeError()
	}
	decoder := json.NewDecoder(strings.NewReader(input))
	var decoded any
	if err := decoder.Decode(&decoded); err != nil {
		return value{}, fmt.Errorf("parse JSON: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return value{}, fmt.Errorf("parse JSON: multiple values are not allowed")
		}
		return value{}, fmt.Errorf("parse JSON: %w", err)
	}
	return normalize(decoded, arguments[0].sensitive), nil
}

func evaluateFormat(arguments []value) (value, error) {
	format, err := arguments[0].stringValue()
	if err != nil {
		return value{}, fmt.Errorf("format template must be a scalar")
	}
	var result strings.Builder
	sensitive := arguments[0].sensitive
	for index := 0; index < len(format); {
		switch {
		case strings.HasPrefix(format[index:], "{{"):
			if err := appendEvaluationString(&result, "{"); err != nil {
				return value{}, err
			}
			index += 2
		case strings.HasPrefix(format[index:], "}}"):
			if err := appendEvaluationString(&result, "}"); err != nil {
				return value{}, err
			}
			index += 2
		case format[index] == '{':
			end := strings.IndexByte(format[index+1:], '}')
			if end < 0 {
				return value{}, fmt.Errorf("format template contains an unclosed placeholder")
			}
			end += index + 1
			position, err := strconv.Atoi(format[index+1 : end])
			if err != nil || position < 0 || position+1 >= len(arguments) {
				return value{}, fmt.Errorf("format template contains an invalid placeholder")
			}
			replacement, err := arguments[position+1].stringValue()
			if err != nil {
				return value{}, fmt.Errorf("format replacement must be a scalar")
			}
			if err := appendEvaluationString(&result, replacement); err != nil {
				return value{}, err
			}
			sensitive = sensitive || arguments[position+1].sensitive
			index = end + 1
		case format[index] == '}':
			return value{}, fmt.Errorf("format template contains an unmatched closing brace")
		default:
			if err := appendEvaluationString(&result, format[index:index+1]); err != nil {
				return value{}, err
			}
			index++
		}
	}
	return value{kind: stringKind, text: result.String(), sensitive: sensitive}, nil
}
