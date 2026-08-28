package console

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/kelos-dev/open-actions/internal/workflowcommand"
)

type logEntry struct {
	Kind       string            `json:"kind"`
	Text       string            `json:"text,omitempty"`
	Parts      []logTextPart     `json:"parts,omitempty"`
	Time       string            `json:"time,omitempty"`
	Scope      string            `json:"scope,omitempty"`
	Conclusion string            `json:"conclusion,omitempty"`
	Properties map[string]string `json:"properties,omitempty"`
}

type logTextPart struct {
	Text       string `json:"text"`
	Foreground string `json:"foreground,omitempty"`
	Background string `json:"background,omitempty"`
	Bold       bool   `json:"bold,omitempty"`
	Dim        bool   `json:"dim,omitempty"`
	Italic     bool   `json:"italic,omitempty"`
	Underline  bool   `json:"underline,omitempty"`
	Strike     bool   `json:"strike,omitempty"`
}

type actionLogParser struct {
	stoppedCommand string
	ansi           ansiTextParser
}

type runnerLogRecord struct {
	Runner bool   `json:"open_actions_runner"`
	Time   string `json:"time"`
	Level  string `json:"level"`
	Msg    string `json:"msg"`
	Step   int    `json:"step"`
	Name   string `json:"name"`
	Action string `json:"action"`
	Error  string `json:"error"`
}

func (p *actionLogParser) parse(line string) (logEntry, bool) {
	entry, visible := p.parseLine(line)
	if !visible {
		return logEntry{}, false
	}
	entry.Text, entry.Parts = p.ansi.format(entry.Text)
	return entry, true
}

func (p *actionLogParser) parseLine(line string) (logEntry, bool) {
	timestamp, content := splitLogTimestamp(strings.TrimSuffix(line, "\r"))
	if record, ok := parseRunnerLog(content); ok {
		p.ansi.reset()
		if timestamp == "" {
			timestamp = record.Time
		}
		return runnerLogEntry(record, timestamp)
	}

	if p.stoppedCommand != "" {
		if workflowcommand.IsResume(content, p.stoppedCommand) {
			p.stoppedCommand = ""
			return logEntry{}, false
		}
		return logEntry{Kind: "output", Text: content, Time: timestamp}, true
	}

	if value, found := strings.CutPrefix(content, "[command]"); found {
		return logEntry{Kind: "command", Text: value, Time: timestamp}, true
	}
	command, ok := workflowcommand.Parse(content)
	if !ok {
		return logEntry{Kind: "output", Text: content, Time: timestamp}, true
	}
	switch command.Name {
	case "debug", "notice", "warning", "error":
		return logEntry{Kind: command.Name, Text: command.Value, Time: timestamp, Properties: command.Properties}, true
	case "group":
		return logEntry{Kind: "group", Text: command.Value, Time: timestamp, Scope: "command"}, true
	case "endgroup":
		return logEntry{Kind: "endgroup", Time: timestamp, Scope: "command"}, true
	case "set-output":
		return internalCommandEntry("Set output", command.Properties["name"], timestamp), true
	case "save-state":
		return internalCommandEntry("Save state", command.Properties["name"], timestamp), true
	case "set-env":
		return internalCommandEntry("Set environment variable", command.Properties["name"], timestamp), true
	case "add-path":
		return logEntry{Kind: "command", Text: "Add system path", Time: timestamp}, true
	case "command", "section":
		return logEntry{Kind: "command", Text: command.Value, Time: timestamp}, true
	case "add-mask", "add-matcher", "remove-matcher", "echo":
		return logEntry{}, false
	case "stop-commands":
		if command.Value != "" {
			p.stoppedCommand = command.Value
			return logEntry{}, false
		}
	}
	return logEntry{Kind: "output", Text: content, Time: timestamp}, true
}

func splitLogTimestamp(line string) (string, string) {
	timestamp, content, found := strings.Cut(line, " ")
	if !found {
		return "", line
	}
	if _, err := time.Parse(time.RFC3339Nano, timestamp); err != nil {
		return "", line
	}
	return timestamp, content
}

func parseRunnerLog(line string) (runnerLogRecord, bool) {
	if !strings.HasPrefix(line, "{") {
		return runnerLogRecord{}, false
	}
	record := runnerLogRecord{}
	if err := json.Unmarshal([]byte(line), &record); err != nil || !record.Runner || record.Msg == "" || record.Level == "" {
		return runnerLogRecord{}, false
	}
	return record, true
}

func runnerLogEntry(record runnerLogRecord, timestamp string) (logEntry, bool) {
	switch record.Msg {
	case "starting workflow step":
		title := record.Name
		if record.Step > 0 {
			title = strconv.Itoa(record.Step) + ". " + title
		}
		return logEntry{Kind: "group", Text: title, Time: timestamp, Scope: "workflow"}, true
	case "completed workflow step":
		return logEntry{Kind: "endgroup", Time: timestamp, Scope: "workflow", Conclusion: "success"}, true
	case "failed workflow step":
		return logEntry{Kind: "endgroup", Time: timestamp, Scope: "workflow", Conclusion: "failure"}, true
	case "cancelled workflow step":
		return logEntry{Kind: "endgroup", Time: timestamp, Scope: "workflow", Conclusion: "cancelled"}, true
	case "skipping workflow step":
		title := record.Name
		if record.Step > 0 {
			if title == "" {
				title = "step " + strconv.Itoa(record.Step)
			} else {
				title = strconv.Itoa(record.Step) + ". " + title
			}
		}
		if title == "" {
			title = "workflow step"
		}
		return logEntry{Kind: "group", Text: title, Time: timestamp, Scope: "workflow", Conclusion: "skipped"}, true
	case "starting composite step":
		title := record.Name
		if title == "" {
			title = record.Action
		}
		if record.Step > 0 {
			title = strconv.Itoa(record.Step) + ". " + title
		}
		return logEntry{Kind: "group", Text: title, Time: timestamp, Scope: "composite"}, true
	case "completed composite step":
		return logEntry{Kind: "endgroup", Time: timestamp, Scope: "composite"}, true
	case "starting post action":
		return logEntry{Kind: "group", Text: "Post " + record.Action, Time: timestamp, Scope: "post"}, true
	case "completed post action":
		return logEntry{Kind: "endgroup", Time: timestamp, Scope: "post"}, true
	case "prepared external action":
		return logEntry{Kind: "debug", Text: "Prepared " + record.Action, Time: timestamp}, true
	case "workflow step input":
		return logEntry{Kind: "input", Text: record.Name, Time: timestamp}, true
	case "workflow step output":
		return logEntry{Kind: "step-output", Text: record.Name, Time: timestamp}, true
	}

	text := record.Msg
	if record.Error != "" {
		text += ": " + record.Error
	}
	switch strings.ToUpper(record.Level) {
	case "DEBUG":
		return logEntry{Kind: "debug", Text: text, Time: timestamp}, true
	case "WARN":
		return logEntry{Kind: "warning", Text: text, Time: timestamp}, true
	case "ERROR":
		return logEntry{Kind: "error", Text: text, Time: timestamp}, true
	default:
		return logEntry{Kind: "runner", Text: text, Time: timestamp}, true
	}
}

func internalCommandEntry(action, name, timestamp string) logEntry {
	if name != "" {
		action += " " + name
	}
	return logEntry{Kind: "command", Text: action, Time: timestamp}
}

type ansiTextParser struct {
	style ansiStyle
}

type ansiStyle struct {
	foreground string
	background string
	bold       bool
	dim        bool
	italic     bool
	underline  bool
	strike     bool
}

func (p *ansiTextParser) reset() {
	p.style = ansiStyle{}
}

func (p *ansiTextParser) format(value string) (string, []logTextPart) {
	var plain strings.Builder
	var parts []logTextPart
	hasANSI := false
	for position := 0; position < len(value); {
		escape := strings.IndexByte(value[position:], '\x1b')
		if escape < 0 {
			p.appendText(&plain, &parts, value[position:])
			break
		}
		escape += position
		p.appendText(&plain, &parts, value[position:escape])
		hasANSI = true
		position = p.consumeEscape(value, escape)
	}
	if !hasANSI && len(parts) == 1 && parts[0] == (logTextPart{Text: value}) {
		return value, nil
	}
	return plain.String(), parts
}

func (p *ansiTextParser) appendText(plain *strings.Builder, parts *[]logTextPart, value string) {
	if value == "" {
		return
	}
	plain.WriteString(value)
	part := logTextPart{
		Text:       value,
		Foreground: p.style.foreground,
		Background: p.style.background,
		Bold:       p.style.bold,
		Dim:        p.style.dim,
		Italic:     p.style.italic,
		Underline:  p.style.underline,
		Strike:     p.style.strike,
	}
	if len(*parts) > 0 {
		last := &(*parts)[len(*parts)-1]
		if sameLogTextStyle(*last, part) {
			last.Text += value
			return
		}
	}
	*parts = append(*parts, part)
}

func sameLogTextStyle(left, right logTextPart) bool {
	left.Text = ""
	right.Text = ""
	return left == right
}

func (p *ansiTextParser) consumeEscape(value string, escape int) int {
	if escape+1 >= len(value) {
		return escape + 1
	}
	if value[escape+1] != '[' {
		return escape + 2
	}
	for end := escape + 2; end < len(value); end++ {
		if value[end] < 0x40 || value[end] > 0x7e {
			continue
		}
		if value[end] == 'm' {
			p.applySGR(value[escape+2 : end])
		}
		return end + 1
	}
	return len(value)
}

func (p *ansiTextParser) applySGR(parameters string) {
	if parameters == "" {
		p.reset()
		return
	}
	values := strings.Split(parameters, ";")
	for index := 0; index < len(values); index++ {
		code, ok := ansiParameter(values[index])
		if !ok {
			continue
		}
		switch {
		case code == 0:
			p.reset()
		case code == 1:
			p.style.bold = true
		case code == 2:
			p.style.dim = true
		case code == 3:
			p.style.italic = true
		case code == 4:
			p.style.underline = true
		case code == 9:
			p.style.strike = true
		case code == 22:
			p.style.bold = false
			p.style.dim = false
		case code == 23:
			p.style.italic = false
		case code == 24:
			p.style.underline = false
		case code == 29:
			p.style.strike = false
		case code >= 30 && code <= 37:
			p.style.foreground = ansiColor(code - 30)
		case code == 38:
			if color, consumed := extendedANSIColor(values[index+1:]); consumed > 0 {
				p.style.foreground = color
				index += consumed
			}
		case code == 39:
			p.style.foreground = ""
		case code >= 40 && code <= 47:
			p.style.background = ansiColor(code - 40)
		case code == 48:
			if color, consumed := extendedANSIColor(values[index+1:]); consumed > 0 {
				p.style.background = color
				index += consumed
			}
		case code == 49:
			p.style.background = ""
		case code >= 90 && code <= 97:
			p.style.foreground = ansiColor(code - 90 + 8)
		case code >= 100 && code <= 107:
			p.style.background = ansiColor(code - 100 + 8)
		}
	}
}

func ansiParameter(value string) (int, bool) {
	if value == "" {
		return 0, true
	}
	parameter, err := strconv.Atoi(value)
	return parameter, err == nil
}

func extendedANSIColor(parameters []string) (string, int) {
	if len(parameters) < 2 {
		return "", 0
	}
	mode, ok := ansiParameter(parameters[0])
	if !ok {
		return "", 0
	}
	switch mode {
	case 5:
		color, ok := ansiParameter(parameters[1])
		if !ok || color < 0 || color > 255 {
			return "", 0
		}
		return ansiColor(color), 2
	case 2:
		if len(parameters) < 4 {
			return "", 0
		}
		red, redOK := ansiParameter(parameters[1])
		green, greenOK := ansiParameter(parameters[2])
		blue, blueOK := ansiParameter(parameters[3])
		if !redOK || !greenOK || !blueOK || red < 0 || red > 255 || green < 0 || green > 255 || blue < 0 || blue > 255 {
			return "", 0
		}
		return ansiRGB(red, green, blue), 4
	default:
		return "", 0
	}
}

func ansiColor(color int) string {
	base := [...]string{
		"#000000", "#800000", "#008000", "#808000", "#000080", "#800080", "#008080", "#c0c0c0",
		"#808080", "#ff0000", "#00ff00", "#ffff00", "#0000ff", "#ff00ff", "#00ffff", "#ffffff",
	}
	if color < len(base) {
		return base[color]
	}
	if color < 232 {
		color -= 16
		levels := [...]int{0, 95, 135, 175, 215, 255}
		return ansiRGB(levels[color/36], levels[(color/6)%6], levels[color%6])
	}
	level := 8 + (color-232)*10
	return ansiRGB(level, level, level)
}

func ansiRGB(red, green, blue int) string {
	return fmt.Sprintf("#%02x%02x%02x", red, green, blue)
}
