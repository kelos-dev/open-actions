package workflow

import (
	"strings"
	"testing"
)

// Examples follow GitHub's workflow syntax and Git diff comparison rules:
// https://docs.github.com/en/actions/reference/workflows-and-actions/workflow-syntax#onpushpull_requestpull_request_targetpathspaths-ignore
func TestGitHubPathFilters(t *testing.T) {
	tests := []struct {
		name, filters string
		paths         []string
		want          bool
	}{
		{"numeric scalar", "paths: 2024", []string{"2024"}, true},
		{"numeric list", "paths: [2024]", []string{"2024"}, true},
		{"boolean scalar", "paths: true", []string{"true"}, true},
		{"boolean list", "paths: [true]", []string{"true"}, true},
		{"boolean casing", "paths: TRUE", []string{"true"}, true},
		{"quoted boolean casing", "paths: 'TRUE'", []string{"TRUE"}, true},
		{"hex number", "paths: 0x10", []string{"16"}, true},
		{"float number", "paths: 2.0", []string{"2"}, true},
		{"numeric ignore scalar", "paths-ignore: 123", []string{"123"}, false},
		{"boolean ignore scalar", "paths-ignore: false", []string{"false"}, false},
		{"boolean ignore list", "paths-ignore: [false]", []string{"false", "src/main.go"}, true},
		{"scalar paths match", "paths: 'src/**'", []string{"src/main.go"}, true},
		{"scalar paths do not match", "paths: 'src/**'", []string{"docs/readme.md"}, false},
		{"scalar ignore skips", "paths-ignore: 'docs/**'", []string{"docs/readme.md"}, false},
		{"scalar ignore runs", "paths-ignore: 'docs/**'", []string{"docs/readme.md", "src/main.go"}, true},
		{"javascript at root", "paths: ['**.js']", []string{"index.js"}, true},
		{"javascript nested", "paths: ['**.js']", []string{"src/index.js"}, true},
		{"nonmatching paths", "paths: ['**.js']", []string{"README.md"}, false},
		{"any changed file", "paths: ['**.js']", []string{"README.md", "src/index.js"}, true},
		{"full path match", "paths: ['*.js']", []string{"src/index.js"}, false},
		{"exclude docs", "paths-ignore: ['docs/**']", []string{"docs/index.md"}, false},
		{"any nonignored file", "paths-ignore: ['docs/**']", []string{"docs/index.md", "src/main.go"}, true},
		{"ordered exclusion", "paths: ['sub-project/**', '!sub-project/docs/**']", []string{"sub-project/docs/readme.md"}, false},
		{"ordered inclusion", "paths: ['sub-project/**', '!sub-project/docs/**', 'sub-project/docs/required.md']", []string{"sub-project/docs/required.md"}, true},
		{"exclusion per file", "paths: ['sub-project/**', '!sub-project/docs/**']", []string{"sub-project/docs/readme.md", "sub-project/index.js"}, true},
		{"root globstar", "paths: ['**/*.js']", []string{"index.js"}, true},
		{"dotfile", "paths: ['**']", []string{".github/workflows/ci.yaml"}, true},
		{"newline filename", "paths: ['**']", []string{"src/line\nbreak.go"}, true},
		{"escaped operator", `paths: ['src/\!main.go']`, []string{"src/!main.go"}, true},
		{"case sensitive", "paths: ['src/**']", []string{"Src/main.go"}, false},
		{"branch and paths match", "branches: [main]\n    paths: ['src/**']", []string{"src/main.go"}, true},
		{"branch mismatch", "branches: [release]\n    paths: ['src/**']", []string{"src/main.go"}, false},
		{"empty paths diff", "paths: ['**']", nil, false},
		{"empty ignore diff", "paths-ignore: ['docs/**']", nil, false},
	}
	for _, tt := range tests {
		for _, eventName := range []string{"push", "pull_request", "pull_request_target"} {
			t.Run(tt.name+"/"+eventName, func(t *testing.T) {
				definition, err := Parse([]byte("on:\n  " + eventName + ":\n    " + tt.filters + "\n" + minimalJob))
				if err != nil {
					t.Fatal(err)
				}
				event := Event{Name: eventName, Action: "opened", Ref: "refs/heads/main", RefName: "main", BaseRef: "main", ChangedFiles: &ChangedFiles{Paths: tt.paths}}
				if got := Matches(definition.On, event); got != tt.want {
					t.Fatalf("Matches() = %v, want %v", got, tt.want)
				}
			})
		}
	}
}

func TestPathFilterDiffRequirements(t *testing.T) {
	definition, err := Parse([]byte("on:\n  push:\n    paths: ['src/**']\n" + minimalJob))
	if err != nil {
		t.Fatal(err)
	}
	event := Event{Name: "push", Ref: "refs/tags/v1", RefName: "v1"}
	if !Matches(definition.On, event) {
		t.Fatal("tag push should bypass path filters without fetching a diff")
	}
	event.Ref, event.RefName = "refs/heads/main", "main"
	if _, _, err := Match(definition.On, event); err == nil {
		t.Fatal("path filters accepted an unresolved diff")
	}
	event.ChangedFiles = &ChangedFiles{AlwaysRun: true}
	if !Matches(definition.On, event) {
		t.Fatal("large push or diff timeout should bypass path filters")
	}
	filter := definition.On.Events["push"]
	filter.Branches, filter.branchesSet = []string{"release"}, true
	definition.On.Events["push"] = filter
	if Matches(definition.On, event) {
		t.Fatal("diff bypass must still enforce branch filters")
	}
}

func TestPathFilterFileLimit(t *testing.T) {
	definition, err := Parse([]byte("on:\n  push:\n    paths: ['src/**']\n" + minimalJob))
	if err != nil {
		t.Fatal(err)
	}
	paths := make([]string, MaxChangedFiles+1)
	for i := range paths {
		paths[i] = "docs/readme.md"
	}
	paths[MaxChangedFiles] = "src/main.go"
	event := Event{Name: "push", Ref: "refs/heads/main", RefName: "main", ChangedFiles: &ChangedFiles{Paths: paths}}
	if Matches(definition.On, event) {
		t.Fatal("path beyond the diff limit matched")
	}
	paths[MaxChangedFiles-1] = "src/main.go"
	if !Matches(definition.On, event) {
		t.Fatal("last path within the diff limit did not match")
	}
}

func TestParsePathFilterValidation(t *testing.T) {
	for _, eventName := range []string{"push", "pull_request", "pull_request_target"} {
		for _, filters := range []string{
			"paths: []", "paths: null", "paths: ''", "paths: [{}]", "paths: [null]", "paths-ignore: [null]",
			"paths-ignore: []", "paths-ignore: null", "paths-ignore: ''",
			"paths: ['']", "paths: ['!']", "paths: ['!docs/**']", "paths: ['[']",
			"paths: ['**']\n    paths-ignore: ['docs/**']", "paths: ['**']\n    paths: ['src/**']",
			"paths: ['" + strings.Repeat("x", maxPathPatternLength+1) + "']",
			"paths: [" + strings.Repeat("'src/**',", maxPathPatterns+1) + "]",
		} {
			t.Run(eventName+"/"+filters, func(t *testing.T) {
				if _, err := Parse([]byte("on:\n  " + eventName + ":\n    " + filters + "\n" + minimalJob)); err == nil {
					t.Fatal("accepted invalid path filter")
				}
			})
		}
	}
	for eventName := range supportedEventTypes {
		if eventName == "push" || eventName == "pull_request" || eventName == "pull_request_target" || eventName == "schedule" {
			continue
		}
		if _, err := Parse([]byte("on:\n  " + eventName + ":\n    paths: ['**']\n" + minimalJob)); err == nil {
			t.Fatalf("accepted path filter for %s", eventName)
		}
	}
}

func TestPathFiltersYAMLAliases(t *testing.T) {
	definition, err := Parse([]byte(`on:
  push:
    paths: &paths
      - &source 'src/**'
      - '!src/docs/**'
  pull_request:
    paths: *paths
  pull_request_target:
    paths:
      - *source
` + minimalJob))
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"push", "pull_request", "pull_request_target"} {
		event := Event{Name: name, Action: "opened", Ref: "refs/heads/main", RefName: "main", ChangedFiles: &ChangedFiles{Paths: []string{"src/main.go"}}}
		if !Matches(definition.On, event) {
			t.Fatalf("%s did not resolve path aliases", name)
		}
	}
}
