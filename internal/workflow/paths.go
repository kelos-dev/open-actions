package workflow

import (
	"fmt"
	"strings"
	"unicode/utf8"

	"gopkg.in/yaml.v3"
)

const (
	maxPathPatterns      = 256
	maxPathPatternLength = 1024
	MaxChangedFiles      = 3000
)

// ChangedFiles contains the diff used for trigger filtering. AlwaysRun bypasses
// path filters for pushes over 1,000 commits or a diff that timed out.
type ChangedFiles struct {
	Paths     []string
	AlwaysRun bool
}

// NeedsChangedFiles reports whether matching this event requires a diff.
func (f EventFilter) NeedsChangedFiles(event Event) bool {
	return (f.pathsSet || f.pathsIgnoreSet || len(f.Paths) > 0 || len(f.PathsIgnore) > 0) &&
		!(event.Name == "push" && strings.HasPrefix(event.Ref, "refs/tags/"))
}

func (f *EventFilter) decodePaths(name string, node *yaml.Node) error {
	for node.Kind == yaml.AliasNode && node.Alias != nil {
		node = node.Alias
	}
	nodes := node.Content
	if node.Kind == yaml.ScalarNode {
		nodes = []*yaml.Node{node}
	} else if node.Kind != yaml.SequenceNode {
		return fmt.Errorf("event filter %q must be a pattern or list of patterns", name)
	}
	patterns := make([]string, 0, len(nodes))
	for _, child := range nodes {
		pattern, err := decodePathPattern(name, child)
		if err != nil {
			return err
		}
		patterns = append(patterns, pattern)
	}
	if name == "paths" {
		f.Paths, f.pathsSet = patterns, true
	} else {
		f.PathsIgnore, f.pathsIgnoreSet = patterns, true
	}
	return nil
}

func decodePathPattern(name string, node *yaml.Node) (string, error) {
	for node.Kind == yaml.AliasNode && node.Alias != nil {
		node = node.Alias
	}
	if node.Kind != yaml.ScalarNode || node.Tag == "!!null" {
		return "", fmt.Errorf("event filter %q patterns must be non-null scalars", name)
	}
	if node.Tag == "!!bool" || node.Tag == "!!int" || node.Tag == "!!float" {
		var value any
		if err := node.Decode(&value); err != nil {
			return "", err
		}
		pattern, valid := scalarString(value)
		if !valid {
			return "", fmt.Errorf("event filter %q contains an invalid pattern", name)
		}
		return pattern, nil
	}
	return node.Value, nil
}

func validatePathFilters(event string, filter EventFilter) error {
	if filter.pathsSet && filter.pathsIgnoreSet {
		return fmt.Errorf("trigger %q cannot define both paths and paths-ignore", event)
	}
	for _, field := range []struct {
		name       string
		patterns   []string
		configured bool
	}{
		{"paths", filter.Paths, filter.pathsSet},
		{"paths-ignore", filter.PathsIgnore, filter.pathsIgnoreSet},
	} {
		if !field.configured {
			continue
		}
		if event != "push" && event != "pull_request" && event != "pull_request_target" {
			return fmt.Errorf("trigger %q does not support %s filters", event, field.name)
		}
		if len(field.patterns) == 0 || len(field.patterns) > maxPathPatterns {
			return fmt.Errorf("trigger %q %s must contain 1 to %d patterns", event, field.name, maxPathPatterns)
		}
		positive := false
		for _, pattern := range field.patterns {
			candidate := strings.TrimPrefix(pattern, "!")
			if candidate == "" || utf8.RuneCountInString(pattern) > maxPathPatternLength {
				return fmt.Errorf("trigger %q %s patterns must contain 1 to %d characters", event, field.name, maxPathPatternLength)
			}
			if _, err := compileGitHubPattern(candidate); err != nil {
				return fmt.Errorf("trigger %q contains invalid %s pattern %q: %w", event, field.name, pattern, err)
			}
			positive = positive || !strings.HasPrefix(pattern, "!")
		}
		if !positive {
			return fmt.Errorf("trigger %q %s must include a positive pattern", event, field.name)
		}
	}
	return nil
}

func (f EventFilter) matchesPaths(files ChangedFiles) bool {
	if files.AlwaysRun {
		return true
	}
	patterns := f.Paths
	ignore := len(f.PathsIgnore) > 0
	if ignore {
		patterns = f.PathsIgnore
	}
	matchers := compilePatterns(patterns)
	for _, path := range files.Paths[:min(len(files.Paths), MaxChangedFiles)] {
		if matchers.matches(path) != ignore {
			return true
		}
	}
	return false
}
