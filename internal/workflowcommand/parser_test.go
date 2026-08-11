package workflowcommand

import "testing"

func TestParseRecognizesGitHubCommandLocationsAndEscapes(t *testing.T) {
	tests := []struct {
		name       string
		line       string
		wantName   string
		wantValue  string
		properties map[string]string
	}{
		{
			name: "indented double-colon", line: "  \t::warning file=main.go,line=9,title=Lint%3Acheck::bad%25value",
			wantName: "warning", wantValue: "bad%value", properties: map[string]string{"file": "main.go", "line": "9", "title": "Lint:check"},
		},
		{
			name: "embedded bracket", line: "prefix ##[warning file=main.go;line=9;title=Lint%3Bcheck%5D]message%3Btail%5D",
			wantName: "warning", wantValue: "message;tail]", properties: map[string]string{"file": "main.go", "line": "9", "title": "Lint;check]"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			command, found := Parse(test.line)
			if !found || command.Name != test.wantName || command.Value != test.wantValue {
				t.Fatalf("Parse() = %#v, %t", command, found)
			}
			for name, want := range test.properties {
				if command.Properties[name] != want {
					t.Errorf("property %q = %q, want %q", name, command.Properties[name], want)
				}
			}
		})
	}
}

func TestParseRejectsOrdinaryOutput(t *testing.T) {
	for _, line := range []string{"ordinary output", "prefix ::warning::message", "##[unfinished"} {
		if command, found := Parse(line); found {
			t.Errorf("Parse(%q) = %#v", line, command)
		}
	}
}

func TestIsResumeUsesCommandDiscovery(t *testing.T) {
	for _, line := range []string{"  ::marker::", "prefix ##[marker]"} {
		if !IsResume(line, "marker") {
			t.Errorf("IsResume(%q) = false", line)
		}
	}
	if IsResume("::other::", "marker") {
		t.Fatal("unrelated command resumed processing")
	}
}
