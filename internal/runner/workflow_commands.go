package runner

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/dlclark/regexp2/v2"
	"github.com/kelos-dev/open-actions/internal/workflowcommand"
)

const (
	maxWorkflowCommandBytes = 64 << 10
	maxProblemMatcherBytes  = 1 << 20
	maxProblemMatchers      = 100
	maxProblemPatterns      = 10
	problemMatcherTimeout   = 100 * time.Millisecond
)

type workflowCommandState struct {
	mutex          sync.Mutex
	matchers       map[string]*compiledProblemMatcher
	stoppedCommand string
}

type workflowCommandWriter struct {
	state         *workflowCommandState
	masker        *outputMasker
	target        *maskingWriter
	files         *commandFiles
	workspace     string
	buffer        []byte
	streamingLine bool
	mutex         sync.Mutex
}

type problemMatcherDocument struct {
	Matchers []problemMatcher `json:"problemMatcher"`
}

type problemMatcher struct {
	Owner    string           `json:"owner"`
	Severity string           `json:"severity"`
	Patterns []problemPattern `json:"pattern"`
}

type problemPattern struct {
	Regexp    string `json:"regexp"`
	File      int    `json:"file"`
	Line      int    `json:"line"`
	Column    int    `json:"column"`
	EndLine   int    `json:"endLine"`
	EndColumn int    `json:"endColumn"`
	Message   int    `json:"message"`
	Code      int    `json:"code"`
	Severity  int    `json:"severity"`
	Loop      bool   `json:"loop"`
}

type compiledProblemMatcher struct {
	definition problemMatcher
	patterns   []*regexp2.Regexp
	next       int
	values     problemMatch
}

type problemMatch struct {
	file      string
	line      string
	column    string
	endLine   string
	endColumn string
	message   string
	code      string
	severity  string
}

func newWorkflowCommandState() *workflowCommandState {
	return &workflowCommandState{matchers: map[string]*compiledProblemMatcher{}}
}

func (s *workflowCommandState) writer(masker *outputMasker, target *maskingWriter, files *commandFiles, workspace string) *workflowCommandWriter {
	return &workflowCommandWriter{state: s, masker: masker, target: target, files: files, workspace: workspace}
}

func (w *workflowCommandWriter) Write(data []byte) (int, error) {
	w.mutex.Lock()
	defer w.mutex.Unlock()
	written := 0
	for len(data) > 0 {
		if w.streamingLine {
			newline := bytes.IndexByte(data, '\n')
			if newline < 0 {
				if err := w.writeUnmatched(data); err != nil {
					return written, err
				}
				return written + len(data), nil
			}
			if err := w.writeUnmatched(data[:newline+1]); err != nil {
				return written, err
			}
			written += newline + 1
			data = data[newline+1:]
			w.streamingLine = false
			continue
		}
		newline := bytes.IndexByte(data, '\n')
		if newline < 0 {
			w.buffer = append(w.buffer, data...)
			written += len(data)
			if len(w.buffer) > maxWorkflowCommandBytes {
				if supportedWorkflowCommandLine(w.buffer) {
					return written, fmt.Errorf("workflow command exceeds %d bytes", maxWorkflowCommandBytes)
				}
				if err := w.writeUnmatched(w.buffer); err != nil {
					return written, err
				}
				w.buffer = nil
				w.streamingLine = true
			}
			return written, nil
		}
		w.buffer = append(w.buffer, data[:newline]...)
		written += newline + 1
		if len(w.buffer) > maxWorkflowCommandBytes {
			if supportedWorkflowCommandLine(w.buffer) {
				return written, fmt.Errorf("workflow command exceeds %d bytes", maxWorkflowCommandBytes)
			}
			if err := w.writeUnmatched(w.buffer); err != nil {
				return written, err
			}
			if err := w.writeUnmatched([]byte{'\n'}); err != nil {
				return written, err
			}
			w.buffer = nil
			data = data[newline+1:]
			continue
		}
		if err := w.processLine(w.buffer, true); err != nil {
			return written, err
		}
		w.buffer = nil
		data = data[newline+1:]
	}
	return written, nil
}

func supportedWorkflowCommandLine(line []byte) bool {
	command, found := workflowcommand.Parse(strings.TrimSuffix(string(line), "\r"))
	return found && supportedWorkflowCommand(command.Name)
}

func (w *workflowCommandWriter) flush() error {
	w.mutex.Lock()
	defer w.mutex.Unlock()
	var err error
	if len(w.buffer) > 0 {
		err = w.processLine(w.buffer, true)
		w.buffer = nil
	} else if w.streamingLine {
		err = w.writeUnmatched([]byte{'\n'})
		w.streamingLine = false
	}
	return errors.Join(err, w.target.flush())
}

func (w *workflowCommandWriter) processLine(line []byte, newline bool) error {
	text := strings.TrimSuffix(string(line), "\r")
	commandsStopped := w.state.commandsStopped(text)
	command, commandFound := workflowcommand.Parse(text)
	if commandFound && !commandsStopped {
		if renderedWorkflowCommand(command.Name) {
			return w.writeRenderedCommand(command, newline)
		}
		handled, err := w.handleCommand(command)
		if err != nil {
			return err
		}
		if handled {
			return nil
		}
	}
	if isRunnerLogRecord(text) {
		if err := w.writeUnmatched([]byte{' '}); err != nil {
			return err
		}
	}
	if err := w.writeUnmatched(line); err != nil {
		return err
	}
	if newline {
		if err := w.writeUnmatched([]byte{'\n'}); err != nil {
			return err
		}
	}
	annotations, failedMatchers := w.state.match(text)
	for _, annotation := range annotations {
		annotation = w.maskProblemMatch(annotation)
		if err := w.writeUnmatched([]byte(formatProblemAnnotation(annotation) + "\n")); err != nil {
			return err
		}
	}
	for _, owner := range failedMatchers {
		message := fmt.Sprintf("Problem matcher %q was disabled after a regular expression error", w.masker.mask(owner))
		if err := w.writeUnmatched([]byte("::warning::" + escapeCommandData(message) + "\n")); err != nil {
			return err
		}
	}
	return nil
}

func isRunnerLogRecord(line string) bool {
	if timestamp, content, found := strings.Cut(line, " "); found {
		if _, err := time.Parse(time.RFC3339Nano, timestamp); err == nil {
			line = content
		}
	}
	if !strings.HasPrefix(line, "{") {
		return false
	}
	record := struct {
		Runner bool `json:"open_actions_runner"`
	}{}
	return json.Unmarshal([]byte(line), &record) == nil && record.Runner
}

func renderedWorkflowCommand(name string) bool {
	switch name {
	case "debug", "notice", "warning", "error", "group", "endgroup", "command", "section":
		return true
	default:
		return false
	}
}

func (w *workflowCommandWriter) writeRenderedCommand(command workflowcommand.Command, newline bool) error {
	properties := make([]string, 0, len(command.Properties))
	for name, value := range command.Properties {
		properties = append(properties, name+"="+escapeCommandProperty(w.masker.mask(value)))
	}
	sort.Strings(properties)
	header := command.Name
	if len(properties) > 0 {
		header += " " + strings.Join(properties, ",")
	}
	line := "::" + header + "::" + escapeCommandData(w.masker.mask(command.Value))
	if newline {
		line += "\n"
	}
	return w.writeUnmatched([]byte(line))
}

func (w *workflowCommandWriter) maskProblemMatch(match problemMatch) problemMatch {
	match.file = w.masker.mask(match.file)
	match.line = w.masker.mask(match.line)
	match.column = w.masker.mask(match.column)
	match.endLine = w.masker.mask(match.endLine)
	match.endColumn = w.masker.mask(match.endColumn)
	match.message = w.masker.mask(match.message)
	match.code = w.masker.mask(match.code)
	return match
}

func (w *workflowCommandWriter) writeUnmatched(data []byte) error {
	return writeAll(w.target, data)
}

func (w *workflowCommandWriter) handleCommand(command workflowcommand.Command) (bool, error) {
	switch command.Name {
	case "add-mask":
		w.masker.add(command.Value)
		for _, line := range strings.FieldsFunc(command.Value, func(character rune) bool { return character == '\r' || character == '\n' }) {
			w.masker.add(line)
		}
		return true, nil
	case "add-matcher":
		path := command.Value
		if !filepath.IsAbs(path) {
			path = filepath.Join(w.workspace, path)
		}
		return true, w.state.addMatchers(path)
	case "remove-matcher":
		w.state.removeMatcher(command.Properties["owner"])
		return true, nil
	case "set-output":
		return true, w.appendKeyValue(w.files, func(files *commandFiles) string { return files.output }, command.Properties["name"], command.Value)
	case "save-state":
		return true, w.appendKeyValue(w.files, func(files *commandFiles) string { return files.state }, command.Properties["name"], command.Value)
	case "set-env", "add-path":
		// Stdout is untrusted; environment changes must use the command files.
		return true, nil
	case "stop-commands":
		if command.Value != "" {
			w.state.stopCommands(command.Value)
		}
		return false, nil
	}
	return false, nil
}

func (s *workflowCommandState) commandsStopped(line string) bool {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if s.stoppedCommand == "" {
		return false
	}
	if isResumeCommand(line, s.stoppedCommand) {
		s.stoppedCommand = ""
	}
	return true
}

func isResumeCommand(line, token string) bool {
	return workflowcommand.IsResume(line, token)
}

func (s *workflowCommandState) stopCommands(token string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.stoppedCommand = token
}

func (w *workflowCommandWriter) appendKeyValue(files *commandFiles, path func(*commandFiles) string, name, value string) error {
	if files == nil || name == "" {
		return nil
	}
	return appendCommandKeyValue(path(files), name, value)
}

func appendCommandKeyValue(path, name, value string) error {
	if name == "" || strings.ContainsAny(name, "\r\n=") {
		return fmt.Errorf("invalid workflow command name %q", name)
	}
	if !strings.ContainsAny(value, "\r\n") {
		return appendCommandValue(path, name+"="+value)
	}
	delimiter := "OPEN_ACTIONS_EOF"
	for strings.Contains("\n"+value+"\n", "\n"+delimiter+"\n") {
		delimiter += "_"
	}
	return appendCommandValue(path, name+"<<"+delimiter+"\n"+value+"\n"+delimiter)
}

func appendCommandValue(path, value string) error {
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		return err
	}
	defer file.Close()
	_, err = io.WriteString(file, value+"\n")
	return err
}

func supportedWorkflowCommand(name string) bool {
	switch name {
	case "debug", "notice", "warning", "error", "group", "endgroup", "command", "section",
		"add-mask", "add-matcher", "remove-matcher", "set-output", "save-state", "set-env", "add-path", "stop-commands":
		return true
	default:
		return false
	}
}

func (m *problemMatcher) UnmarshalJSON(data []byte) error {
	raw := struct {
		Owner    string          `json:"owner"`
		Severity string          `json:"severity"`
		Pattern  json.RawMessage `json:"pattern"`
	}{}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	*m = problemMatcher{Owner: raw.Owner, Severity: raw.Severity}
	if len(raw.Pattern) == 0 {
		return errors.New("problem matcher has no pattern")
	}
	if raw.Pattern[0] == '[' {
		return json.Unmarshal(raw.Pattern, &m.Patterns)
	}
	var pattern problemPattern
	if err := json.Unmarshal(raw.Pattern, &pattern); err != nil {
		return err
	}
	m.Patterns = []problemPattern{pattern}
	return nil
}

func (s *workflowCommandState) addMatchers(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("inspect problem matcher %q: %w", path, err)
	}
	if info.Size() > maxProblemMatcherBytes {
		return fmt.Errorf("problem matcher %q exceeds %d bytes", path, maxProblemMatcherBytes)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read problem matcher %q: %w", path, err)
	}
	if len(data) > maxProblemMatcherBytes {
		return fmt.Errorf("problem matcher %q exceeds %d bytes", path, maxProblemMatcherBytes)
	}
	document := problemMatcherDocument{}
	if err := json.Unmarshal(data, &document); err != nil {
		return fmt.Errorf("decode problem matcher %q: %w", path, err)
	}
	if len(document.Matchers) == 0 || len(document.Matchers) > maxProblemMatchers {
		return fmt.Errorf("problem matcher %q must contain between 1 and %d matchers", path, maxProblemMatchers)
	}
	compiled := make([]*compiledProblemMatcher, 0, len(document.Matchers))
	for _, definition := range document.Matchers {
		matcher, err := compileProblemMatcher(definition)
		if err != nil {
			return fmt.Errorf("decode problem matcher %q: %w", path, err)
		}
		compiled = append(compiled, matcher)
	}
	s.mutex.Lock()
	defer s.mutex.Unlock()
	owners := make(map[string]struct{}, len(s.matchers)+len(compiled))
	for owner := range s.matchers {
		owners[owner] = struct{}{}
	}
	for _, matcher := range compiled {
		owners[matcher.definition.Owner] = struct{}{}
	}
	if len(owners) > maxProblemMatchers {
		return fmt.Errorf("registered problem matchers exceed %d owners", maxProblemMatchers)
	}
	for _, matcher := range compiled {
		s.matchers[matcher.definition.Owner] = matcher
	}
	return nil
}

func compileProblemMatcher(definition problemMatcher) (*compiledProblemMatcher, error) {
	if definition.Owner == "" {
		return nil, errors.New("problem matcher owner is required")
	}
	if len(definition.Patterns) == 0 || len(definition.Patterns) > maxProblemPatterns {
		return nil, fmt.Errorf("problem matcher %q must contain between 1 and %d patterns", definition.Owner, maxProblemPatterns)
	}
	matcher := &compiledProblemMatcher{definition: definition}
	for _, pattern := range definition.Patterns {
		compiled, err := regexp2.Compile(pattern.Regexp)
		if err != nil {
			return nil, fmt.Errorf("problem matcher %q pattern: %w", definition.Owner, err)
		}
		compiled.MatchTimeout = problemMatcherTimeout
		groups := make(map[int]struct{})
		for _, group := range compiled.GetGroupNumbers() {
			groups[group] = struct{}{}
		}
		for _, group := range []int{pattern.File, pattern.Line, pattern.Column, pattern.EndLine, pattern.EndColumn, pattern.Message, pattern.Code, pattern.Severity} {
			if group < 0 {
				return nil, fmt.Errorf("problem matcher %q references invalid capture group %d", definition.Owner, group)
			}
			if group > 0 {
				if _, found := groups[group]; !found {
					return nil, fmt.Errorf("problem matcher %q does not define capture group %d", definition.Owner, group)
				}
			}
		}
		matcher.patterns = append(matcher.patterns, compiled)
	}
	return matcher, nil
}

func (s *workflowCommandState) removeMatcher(owner string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	delete(s.matchers, owner)
}

func (s *workflowCommandState) match(line string) ([]problemMatch, []string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	annotations := []problemMatch{}
	failedMatchers := []string{}
	owners := make([]string, 0, len(s.matchers))
	for owner := range s.matchers {
		owners = append(owners, owner)
	}
	sort.Strings(owners)
	for _, owner := range owners {
		matcher := s.matchers[owner]
		annotation, found, err := matcher.match(line)
		if err != nil {
			delete(s.matchers, owner)
			failedMatchers = append(failedMatchers, owner)
			continue
		}
		if found {
			annotations = append(annotations, annotation)
		}
	}
	return annotations, failedMatchers
}

func (m *compiledProblemMatcher) match(line string) (problemMatch, bool, error) {
	if m.next > 0 {
		captures, found, err := problemMatcherCaptures(m.patterns[m.next], line)
		if err != nil {
			m.next = 0
			m.values = problemMatch{}
			return problemMatch{}, false, err
		}
		if found {
			patternIndex := m.next
			base := m.values
			m.values.apply(m.definition.Patterns[patternIndex], captures)
			m.next++
			if m.next == len(m.patterns) {
				if m.definition.Patterns[patternIndex].Loop && patternIndex > 0 {
					result := m.result()
					m.next = patternIndex
					m.values = base
					return result, true, nil
				}
				result := m.finish()
				return result, true, nil
			}
			return problemMatch{}, false, nil
		}
		m.next = 0
		m.values = problemMatch{}
	}
	captures, found, err := problemMatcherCaptures(m.patterns[0], line)
	if err != nil {
		return problemMatch{}, false, err
	}
	if !found {
		return problemMatch{}, false, nil
	}
	m.values.apply(m.definition.Patterns[0], captures)
	m.next = 1
	if len(m.patterns) == 1 {
		result := m.finish()
		return result, true, nil
	}
	return problemMatch{}, false, nil
}

func problemMatcherCaptures(pattern *regexp2.Regexp, line string) ([]string, bool, error) {
	match, err := pattern.FindStringMatch(line)
	if err != nil {
		return nil, false, err
	}
	if match == nil {
		return nil, false, nil
	}
	groupNumbers := pattern.GetGroupNumbers()
	maxGroup := 0
	for _, group := range groupNumbers {
		if group > maxGroup {
			maxGroup = group
		}
	}
	captures := make([]string, maxGroup+1)
	for _, group := range groupNumbers {
		capture := match.GroupByNumber(group)
		if capture != nil && len(capture.Captures) > 0 {
			captures[group] = capture.String()
		}
	}
	return captures, true, nil
}

func (m *compiledProblemMatcher) finish() problemMatch {
	result := m.result()
	m.next = 0
	m.values = problemMatch{}
	return result
}

func (m *compiledProblemMatcher) result() problemMatch {
	result := m.values
	if result.severity == "" {
		result.severity = m.definition.Severity
	}
	if result.severity == "" {
		result.severity = "error"
	}
	return result
}

func (m *problemMatch) apply(pattern problemPattern, captures []string) {
	assignCapture(&m.file, pattern.File, captures)
	assignCapture(&m.line, pattern.Line, captures)
	assignCapture(&m.column, pattern.Column, captures)
	assignCapture(&m.endLine, pattern.EndLine, captures)
	assignCapture(&m.endColumn, pattern.EndColumn, captures)
	assignCapture(&m.message, pattern.Message, captures)
	assignCapture(&m.code, pattern.Code, captures)
	assignCapture(&m.severity, pattern.Severity, captures)
}

func assignCapture(target *string, group int, captures []string) {
	if group > 0 && group < len(captures) && captures[group] != "" {
		*target = captures[group]
	}
}

func formatProblemAnnotation(match problemMatch) string {
	severity := strings.ToLower(match.severity)
	switch severity {
	case "warning", "warn":
		severity = "warning"
	case "notice", "info", "information":
		severity = "notice"
	default:
		severity = "error"
	}
	properties := []string{}
	for _, property := range []struct{ name, value string }{
		{name: "file", value: match.file}, {name: "line", value: match.line}, {name: "col", value: match.column},
		{name: "endLine", value: match.endLine}, {name: "endColumn", value: match.endColumn}, {name: "title", value: match.code},
	} {
		if property.value != "" {
			properties = append(properties, property.name+"="+escapeCommandProperty(property.value))
		}
	}
	header := severity
	if len(properties) > 0 {
		header += " " + strings.Join(properties, ",")
	}
	return "::" + header + "::" + escapeCommandData(match.message)
}

func escapeCommandData(value string) string {
	value = strings.ReplaceAll(value, "%", "%25")
	value = strings.ReplaceAll(value, "\r", "%0D")
	return strings.ReplaceAll(value, "\n", "%0A")
}

func escapeCommandProperty(value string) string {
	value = escapeCommandData(value)
	value = strings.ReplaceAll(value, ":", "%3A")
	return strings.ReplaceAll(value, ",", "%2C")
}
