package console

import (
	"context"
	"io"
	"reflect"
	"strings"
	"testing"
)

func TestActionLogParserHandlesWorkflowCommands(t *testing.T) {
	parser := &actionLogParser{}
	tests := []struct {
		line           string
		wantVisible    bool
		wantKind       string
		wantText       string
		wantTime       string
		wantProperties map[string]string
	}{
		{
			line:        "2026-08-10T12:34:56.123456789Z ::warning file=main.go,line=12,col=4,title=Lint%3A result::bad%25value%0Atry again",
			wantVisible: true, wantKind: "warning", wantText: "bad%value\ntry again", wantTime: "2026-08-10T12:34:56.123456789Z",
			wantProperties: map[string]string{"file": "main.go", "line": "12", "col": "4", "title": "Lint: result"},
		},
		{line: "::debug::request%20body", wantVisible: true, wantKind: "debug", wantText: "request%20body"},
		{line: "  ::debug::indented", wantVisible: true, wantKind: "debug", wantText: "indented"},
		{line: "::group::Install dependencies", wantVisible: true, wantKind: "group", wantText: "Install dependencies"},
		{line: "::endgroup::", wantVisible: true, wantKind: "endgroup"},
		{line: "::set-output name=image::registry.example/private", wantVisible: true, wantKind: "command", wantText: "Set output image"},
		{line: "::save-state name=token::sensitive", wantVisible: true, wantKind: "command", wantText: "Save state token"},
		{line: "::add-mask::sensitive", wantVisible: false},
		{line: "::add-matcher::/tmp/matcher.json", wantVisible: false},
		{line: "##[add-matcher]/tmp/matcher.json", wantVisible: false},
		{line: "prefix ##[add-mask]sensitive", wantVisible: false},
		{line: "::remove-matcher owner=go::", wantVisible: false},
		{line: "[command]/usr/bin/git version", wantVisible: true, wantKind: "command", wantText: "/usr/bin/git version"},
		{line: "##[command]/usr/bin/git status", wantVisible: true, wantKind: "command", wantText: "/usr/bin/git status"},
		{
			line: "##[warning file=main.go;line=9;title=Lint%3Bcheck%5D]bracket warning", wantVisible: true,
			wantKind: "warning", wantText: "bracket warning", wantProperties: map[string]string{"file": "main.go", "line": "9", "title": "Lint;check]"},
		},
		{line: "prefix ##[warning]embedded", wantVisible: true, wantKind: "warning", wantText: "embedded"},
		{line: "ordinary ::debug:: text", wantVisible: true, wantKind: "output", wantText: "ordinary ::debug:: text"},
	}
	for _, test := range tests {
		t.Run(test.line, func(t *testing.T) {
			entry, visible := parser.parse(test.line)
			if visible != test.wantVisible {
				t.Fatalf("visible = %t, want %t", visible, test.wantVisible)
			}
			if !visible {
				return
			}
			if entry.Kind != test.wantKind || entry.Text != test.wantText || entry.Time != test.wantTime {
				t.Fatalf("entry = %#v", entry)
			}
			for name, want := range test.wantProperties {
				if entry.Properties[name] != want {
					t.Errorf("property %q = %q, want %q", name, entry.Properties[name], want)
				}
			}
		})
	}
}

func TestActionLogParserHonorsStopCommands(t *testing.T) {
	parser := &actionLogParser{}
	if _, visible := parser.parse("::stop-commands::marker"); visible {
		t.Fatal("stop-commands was visible")
	}
	entry, visible := parser.parse(`{"time":"2026-08-10T12:34:56Z","level":"INFO","msg":"starting workflow step","open_actions_runner":true,"job":"build","step":2,"name":"Test"}`)
	if !visible || entry.Kind != "group" || entry.Text != "2. Test" {
		t.Fatalf("runner entry while commands were stopped = %#v, %t", entry, visible)
	}
	entry, visible = parser.parse("::debug::shown as ordinary output")
	if !visible || entry.Kind != "output" || entry.Text != "::debug::shown as ordinary output" {
		t.Fatalf("stopped entry = %#v, %t", entry, visible)
	}
	if _, visible := parser.parse("::marker::"); visible {
		t.Fatal("command marker was visible")
	}
	entry, visible = parser.parse("::debug::debug output")
	if !visible || entry.Kind != "debug" || entry.Text != "debug output" {
		t.Fatalf("resumed entry = %#v, %t", entry, visible)
	}
}

func TestActionLogParserHonorsBracketStopCommands(t *testing.T) {
	parser := &actionLogParser{}
	if _, visible := parser.parse("##[stop-commands]marker"); visible {
		t.Fatal("stop-commands was visible")
	}
	entry, visible := parser.parse("##[debug]shown as ordinary output")
	if !visible || entry.Kind != "output" || entry.Text != "##[debug]shown as ordinary output" {
		t.Fatalf("stopped entry = %#v, %t", entry, visible)
	}
	if _, visible := parser.parse("##[marker]"); visible {
		t.Fatal("command marker was visible")
	}
	entry, visible = parser.parse("##[debug]debug output")
	if !visible || entry.Kind != "debug" || entry.Text != "debug output" {
		t.Fatalf("resumed entry = %#v, %t", entry, visible)
	}
}

func TestActionLogParserStructuresRunnerMessages(t *testing.T) {
	parser := &actionLogParser{}
	applicationLog := `{"time":"2026-08-10T12:34:55Z","level":"INFO","msg":"completed workflow step"}`
	entry, visible := parser.parse(applicationLog)
	if !visible || entry.Kind != "output" || entry.Text != applicationLog {
		t.Fatalf("application log = %#v, %t", entry, visible)
	}
	entry, visible = parser.parse(`{"time":"2026-08-10T12:34:56Z","level":"INFO","msg":"starting workflow step","open_actions_runner":true,"job":"build","step":2,"name":"Test"}`)
	if !visible || entry.Kind != "group" || entry.Text != "2. Test" || entry.Time != "2026-08-10T12:34:56Z" {
		t.Fatalf("start entry = %#v, %t", entry, visible)
	}
	entry, visible = parser.parse(`{"time":"2026-08-10T12:34:57Z","level":"INFO","msg":"completed workflow step","open_actions_runner":true,"job":"build","step":2,"name":"Test"}`)
	if !visible || entry.Kind != "endgroup" || entry.Conclusion != "success" {
		t.Fatalf("completion entry = %#v, %t", entry, visible)
	}
	entry, visible = parser.parse(`{"time":"2026-08-10T12:34:57Z","level":"INFO","msg":"failed workflow step","open_actions_runner":true,"job":"build","step":2,"name":"Test"}`)
	if !visible || entry.Kind != "endgroup" || entry.Conclusion != "failure" {
		t.Fatalf("failure entry = %#v, %t", entry, visible)
	}
	entry, visible = parser.parse(`{"time":"2026-08-10T12:34:57Z","level":"INFO","msg":"cancelled workflow step","open_actions_runner":true,"job":"build","step":2,"name":"Test"}`)
	if !visible || entry.Kind != "endgroup" || entry.Conclusion != "cancelled" {
		t.Fatalf("cancellation entry = %#v, %t", entry, visible)
	}
	entry, visible = parser.parse(`{"time":"2026-08-10T12:34:58Z","level":"INFO","msg":"skipping workflow step","open_actions_runner":true,"job":"build","step":3,"name":"Deploy"}`)
	if !visible || entry.Kind != "runner" || entry.Text != "Skipped 3. Deploy" {
		t.Fatalf("skipped entry = %#v, %t", entry, visible)
	}
	entry, visible = parser.parse(`{"time":"2026-08-10T12:35:00Z","level":"INFO","msg":"workflow step input","open_actions_runner":true,"name":"token","value":"sensitive-value"}`)
	if !visible || entry.Kind != "input" || entry.Text != "token" {
		t.Fatalf("input entry = %#v, %t", entry, visible)
	}
	entry, visible = parser.parse(`{"time":"2026-08-10T12:35:01Z","level":"INFO","msg":"workflow step output","open_actions_runner":true,"name":"artifact","value":"sensitive-value"}`)
	if !visible || entry.Kind != "step-output" || entry.Text != "artifact" {
		t.Fatalf("output entry = %#v, %t", entry, visible)
	}
	entry, visible = parser.parse(`{"time":"2026-08-10T12:35:02Z","level":"INFO","msg":"starting post action","open_actions_runner":true,"action":"actions/checkout@v4"}`)
	if !visible || entry.Kind != "group" || entry.Scope != "post" || entry.Text != "Post actions/checkout@v4" {
		t.Fatalf("post entry = %#v, %t", entry, visible)
	}
	entry, visible = parser.parse(`{"time":"2026-08-10T12:35:03Z","level":"INFO","msg":"completed post action","open_actions_runner":true,"action":"actions/checkout@v4"}`)
	if !visible || entry.Kind != "endgroup" || entry.Scope != "post" {
		t.Fatalf("post completion entry = %#v, %t", entry, visible)
	}
}

func TestActionLogParserFormatsANSIOutput(t *testing.T) {
	parser := &actionLogParser{}
	entry, visible := parser.parse("plain \x1b[38;5;243mgray \x1b[1mbold\x1b[0m end")
	if !visible || entry.Text != "plain gray bold end" {
		t.Fatalf("entry = %#v, %t", entry, visible)
	}
	want := []logTextPart{
		{Text: "plain "},
		{Text: "gray ", Foreground: "#767676"},
		{Text: "bold", Foreground: "#767676", Bold: true},
		{Text: " end"},
	}
	if !reflect.DeepEqual(entry.Parts, want) {
		t.Fatalf("parts = %#v, want %#v", entry.Parts, want)
	}

	entry, visible = parser.parse("\x1b[38;2;12;34;56;48;5;196;3;4;9mstyled\x1b[22;23;24;29;39;49m plain")
	if !visible || entry.Text != "styled plain" {
		t.Fatalf("entry = %#v, %t", entry, visible)
	}
	want = []logTextPart{
		{Text: "styled", Foreground: "#0c2238", Background: "#ff0000", Italic: true, Underline: true, Strike: true},
		{Text: " plain"},
	}
	if !reflect.DeepEqual(entry.Parts, want) {
		t.Fatalf("parts = %#v, want %#v", entry.Parts, want)
	}
}

func TestActionLogParserCarriesANSIStyleAcrossLines(t *testing.T) {
	parser := &actionLogParser{}
	first, visible := parser.parse("\x1b[31mred")
	if !visible || first.Text != "red" || !reflect.DeepEqual(first.Parts, []logTextPart{{Text: "red", Foreground: "#800000"}}) {
		t.Fatalf("first entry = %#v, %t", first, visible)
	}
	second, visible := parser.parse("continued\x1b[0m plain")
	want := []logTextPart{{Text: "continued", Foreground: "#800000"}, {Text: " plain"}}
	if !visible || second.Text != "continued plain" || !reflect.DeepEqual(second.Parts, want) {
		t.Fatalf("second entry = %#v, %t", second, visible)
	}
	third, visible := parser.parse("unstyled")
	if !visible || third.Text != "unstyled" || third.Parts != nil {
		t.Fatalf("third entry = %#v, %t", third, visible)
	}
}

func TestReadLogStreamTruncatesOversizedLinesAndContinues(t *testing.T) {
	prefix := "\x1b[31msome output "
	incompleteEscape := "\x1b[3"
	line := prefix + strings.Repeat("x", maxLogLineBytes-len(prefix)-len(incompleteEscape)) + incompleteEscape + "1mhidden\n"
	stream := strings.NewReader(line + "::debug::after\n")
	reads := readLogStream(context.Background(), stream)
	var entries []logEntry
	var finalError error
	for result := range reads {
		if result.entry != nil {
			entries = append(entries, *result.entry)
		}
		if result.err != nil {
			finalError = result.err
		}
	}
	if finalError != io.EOF {
		t.Fatalf("final error = %v", finalError)
	}
	if len(entries) != 2 || !strings.HasSuffix(entries[0].Text, truncatedLogMarker) || entries[1].Kind != "debug" || entries[1].Text != "after" {
		t.Fatalf("entries = %#v", entries)
	}
	if len(entries[0].Text) > maxLogLineBytes+64 {
		t.Fatalf("truncated line contains %d bytes", len(entries[0].Text))
	}
	if len(entries[0].Parts) != 2 {
		t.Fatalf("ANSI parts = %#v", entries[0].Parts)
	}
	lastPart := entries[0].Parts[len(entries[0].Parts)-1]
	if entries[0].Parts[0].Foreground != "#800000" || lastPart != (logTextPart{Text: truncatedLogMarker}) || entries[1].Parts != nil {
		t.Fatalf("ANSI parts = %#v, %#v", entries[0].Parts, entries[1].Parts)
	}
}

func TestReadLogStreamPreservesLinesAcrossReads(t *testing.T) {
	stream := &oneByteReader{reader: strings.NewReader("first 😄 line\n::debug::details%0Acontinued\nlast line")}
	reads := readLogStream(context.Background(), stream)
	var entries []logEntry
	var finalError error
	for result := range reads {
		if result.entry != nil {
			entries = append(entries, *result.entry)
		}
		if result.err != nil {
			finalError = result.err
		}
	}
	if finalError != io.EOF {
		t.Fatalf("final error = %v", finalError)
	}
	if len(entries) != 3 {
		t.Fatalf("entries = %#v", entries)
	}
	if entries[0].Text != "first 😄 line" || entries[1].Kind != "debug" || entries[1].Text != "details\ncontinued" || entries[2].Text != "last line" {
		t.Fatalf("entries = %#v", entries)
	}
}

type oneByteReader struct {
	reader io.Reader
}

func (r *oneByteReader) Read(data []byte) (int, error) {
	return r.reader.Read(data[:1])
}
