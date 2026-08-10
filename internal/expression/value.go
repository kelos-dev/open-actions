package expression

import (
	"fmt"
	"math"
	"reflect"
	"regexp"
	"strconv"
	"strings"
)

type valueKind uint8

const (
	nullKind valueKind = iota
	boolKind
	numberKind
	stringKind
	arrayKind
	objectKind
)

type value struct {
	kind      valueKind
	boolean   bool
	number    float64
	text      string
	array     *arrayValue
	object    *objectValue
	sensitive bool
}

type arrayValue struct {
	items []value
}

type objectValue struct {
	fields  map[string]value
	resolve func(string) (value, bool, error)
}

var jsonNumberPattern = regexp.MustCompile(`^-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?$`)

// SensitiveValue marks a value and every result derived from it as secret.
type SensitiveValue struct {
	Value any
}

// DeferredObject resolves an object property only when an expression reads it.
type DeferredObject func(name string) (value any, found bool, err error)

// Secret marks a value and every result derived from it as secret.
func Secret(input any) SensitiveValue {
	return SensitiveValue{Value: input}
}

// Result is a typed expression result. Secret reports whether the result was
// derived from a sensitive value.
type Result struct {
	Value  any
	Secret bool
}

// String converts a scalar result to GitHub Actions interpolation text.
func (r Result) String() (string, error) {
	return normalize(r.Value, r.Secret).stringValue()
}

// Bool applies GitHub Actions conditional truthiness to a result.
func (r Result) Bool() bool {
	return normalize(r.Value, r.Secret).truthy()
}

// Redacted returns a display-safe form of the result.
func (r Result) Redacted() any {
	if r.Secret {
		return "***"
	}
	return r.Value
}

func normalize(input any, sensitive bool) value {
	if marked, ok := input.(SensitiveValue); ok {
		return normalize(marked.Value, true)
	}
	if input == nil {
		return value{kind: nullKind, sensitive: sensitive}
	}
	switch typed := input.(type) {
	case value:
		typed.sensitive = typed.sensitive || sensitive
		return typed
	case bool:
		return value{kind: boolKind, boolean: typed, sensitive: sensitive}
	case string:
		return value{kind: stringKind, text: typed, sensitive: sensitive}
	case float64:
		return value{kind: numberKind, number: typed, sensitive: sensitive}
	case float32:
		return value{kind: numberKind, number: float64(typed), sensitive: sensitive}
	case int:
		return value{kind: numberKind, number: float64(typed), sensitive: sensitive}
	case int8:
		return value{kind: numberKind, number: float64(typed), sensitive: sensitive}
	case int16:
		return value{kind: numberKind, number: float64(typed), sensitive: sensitive}
	case int32:
		return value{kind: numberKind, number: float64(typed), sensitive: sensitive}
	case int64:
		return value{kind: numberKind, number: float64(typed), sensitive: sensitive}
	case uint:
		return value{kind: numberKind, number: float64(typed), sensitive: sensitive}
	case uint8:
		return value{kind: numberKind, number: float64(typed), sensitive: sensitive}
	case uint16:
		return value{kind: numberKind, number: float64(typed), sensitive: sensitive}
	case uint32:
		return value{kind: numberKind, number: float64(typed), sensitive: sensitive}
	case uint64:
		return value{kind: numberKind, number: float64(typed), sensitive: sensitive}
	case map[string]any:
		fields := make(map[string]value, len(typed))
		for name, item := range typed {
			fields[name] = normalize(item, sensitive)
		}
		return value{kind: objectKind, object: &objectValue{fields: fields}, sensitive: sensitive}
	case map[string]string:
		fields := make(map[string]value, len(typed))
		for name, item := range typed {
			fields[name] = normalize(item, sensitive)
		}
		return value{kind: objectKind, object: &objectValue{fields: fields}, sensitive: sensitive}
	case DeferredObject:
		resolver := func(name string) (value, bool, error) {
			resolved, found, err := typed(name)
			if err != nil || !found {
				return value{}, found, err
			}
			return normalize(resolved, sensitive), true, nil
		}
		return value{kind: objectKind, object: &objectValue{fields: map[string]value{}, resolve: resolver}, sensitive: sensitive}
	case []any:
		items := make([]value, len(typed))
		for index, item := range typed {
			items[index] = normalize(item, sensitive)
		}
		return value{kind: arrayKind, array: &arrayValue{items: items}, sensitive: sensitive}
	case []string:
		items := make([]value, len(typed))
		for index, item := range typed {
			items[index] = normalize(item, sensitive)
		}
		return value{kind: arrayKind, array: &arrayValue{items: items}, sensitive: sensitive}
	}

	reflected := reflect.ValueOf(input)
	switch reflected.Kind() {
	case reflect.Map:
		if reflected.Type().Key().Kind() != reflect.String {
			return value{kind: nullKind, sensitive: sensitive}
		}
		fields := make(map[string]value, reflected.Len())
		iterator := reflected.MapRange()
		for iterator.Next() {
			fields[iterator.Key().String()] = normalize(iterator.Value().Interface(), sensitive)
		}
		return value{kind: objectKind, object: &objectValue{fields: fields}, sensitive: sensitive}
	case reflect.Array, reflect.Slice:
		items := make([]value, reflected.Len())
		for index := range items {
			items[index] = normalize(reflected.Index(index).Interface(), sensitive)
		}
		return value{kind: arrayKind, array: &arrayValue{items: items}, sensitive: sensitive}
	}
	return value{kind: stringKind, text: fmt.Sprint(input), sensitive: sensitive}
}

func (v value) interfaceValue() any {
	switch v.kind {
	case nullKind:
		return nil
	case boolKind:
		return v.boolean
	case numberKind:
		return v.number
	case stringKind:
		return v.text
	case arrayKind:
		result := make([]any, len(v.array.items))
		for index, item := range v.array.items {
			result[index] = item.interfaceValue()
		}
		return result
	case objectKind:
		result := make(map[string]any, len(v.object.fields))
		for name, item := range v.object.fields {
			result[name] = item.interfaceValue()
		}
		return result
	default:
		return nil
	}
}

func (v value) truthy() bool {
	switch v.kind {
	case nullKind:
		return false
	case boolKind:
		return v.boolean
	case numberKind:
		return v.number != 0 && !math.IsNaN(v.number)
	case stringKind:
		return v.text != ""
	default:
		return true
	}
}

func (v value) stringValue() (string, error) {
	switch v.kind {
	case nullKind:
		return "", nil
	case boolKind:
		return strconv.FormatBool(v.boolean), nil
	case numberKind:
		return strconv.FormatFloat(v.number, 'g', -1, 64), nil
	case stringKind:
		return v.text, nil
	default:
		return "", fmt.Errorf("arrays and objects cannot be interpolated as strings")
	}
}

func (v value) numberValue() float64 {
	switch v.kind {
	case nullKind:
		return 0
	case boolKind:
		if v.boolean {
			return 1
		}
		return 0
	case numberKind:
		return v.number
	case stringKind:
		text := strings.TrimSpace(v.text)
		if text == "" {
			return 0
		}
		if !jsonNumberPattern.MatchString(text) {
			return math.NaN()
		}
		number, err := strconv.ParseFloat(text, 64)
		if err != nil {
			return math.NaN()
		}
		return number
	default:
		return math.NaN()
	}
}

func (v value) property(name string) (value, error) {
	if v.kind != objectKind || v.object == nil {
		return value{kind: stringKind, sensitive: v.sensitive}, nil
	}
	if item, found := v.object.fields[name]; found {
		item.sensitive = item.sensitive || v.sensitive
		return item, nil
	}
	for candidate, item := range v.object.fields {
		if strings.EqualFold(candidate, name) {
			item.sensitive = item.sensitive || v.sensitive
			return item, nil
		}
	}
	if v.object.resolve != nil {
		item, found, err := v.object.resolve(name)
		if err != nil {
			return value{}, err
		}
		if found {
			v.object.fields[name] = item
			item.sensitive = item.sensitive || v.sensitive
			return item, nil
		}
	}
	return value{kind: stringKind, sensitive: v.sensitive}, nil
}

func equal(left, right value) bool {
	if left.kind == right.kind {
		switch left.kind {
		case nullKind:
			return true
		case boolKind:
			return left.boolean == right.boolean
		case numberKind:
			return !math.IsNaN(left.number) && !math.IsNaN(right.number) && left.number == right.number
		case stringKind:
			return strings.EqualFold(left.text, right.text)
		case arrayKind:
			return left.array == right.array
		case objectKind:
			return left.object == right.object
		}
	}
	leftNumber := left.numberValue()
	rightNumber := right.numberValue()
	return !math.IsNaN(leftNumber) && !math.IsNaN(rightNumber) && leftNumber == rightNumber
}

func compare(left, right value) (int, bool) {
	if left.kind == stringKind && right.kind == stringKind {
		return strings.Compare(strings.ToLower(left.text), strings.ToLower(right.text)), true
	}
	leftNumber := left.numberValue()
	rightNumber := right.numberValue()
	if math.IsNaN(leftNumber) || math.IsNaN(rightNumber) {
		return 0, false
	}
	switch {
	case leftNumber < rightNumber:
		return -1, true
	case leftNumber > rightNumber:
		return 1, true
	default:
		return 0, true
	}
}
