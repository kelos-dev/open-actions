package console

import (
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"github.com/kelos-dev/open-actions/internal/workflowcommand"
)

type logEntry struct {
	Kind       string            `json:"kind"`
	Text       string            `json:"text,omitempty"`
	Time       string            `json:"time,omitempty"`
	Scope      string            `json:"scope,omitempty"`
	Properties map[string]string `json:"properties,omitempty"`
}

type actionLogParser struct {
	stoppedCommand string
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
	timestamp, content := splitLogTimestamp(strings.TrimSuffix(line, "\r"))
	if record, ok := parseRunnerLog(content); ok {
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
		return logEntry{Kind: "endgroup", Time: timestamp, Scope: "workflow"}, true
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
		return logEntry{Kind: "runner", Text: "Skipped " + title, Time: timestamp}, true
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
