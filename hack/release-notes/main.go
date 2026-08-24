/*
Copyright 2026 Gunju Kim

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// release-notes generates categorized release notes from merged pull request
// descriptions up to a release tag.
//
// Usage: release-notes <version-tag>
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"strings"
)

type category struct {
	Label   string
	Heading string
}

var categories = []category{
	{Label: "kind/api", Heading: "API Changes"},
	{Label: "kind/feature", Heading: "Features"},
	{Label: "kind/bug", Heading: "Bug Fixes"},
	{Label: "kind/docs", Heading: "Documentation"},
}

type prData struct {
	Number string    `json:"-"`
	Author prAuthor  `json:"author"`
	Body   string    `json:"body"`
	Labels []prLabel `json:"labels"`
}

type prAuthor struct {
	Login string `json:"login"`
}

type prLabel struct {
	Name string `json:"name"`
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "Usage: release-notes <version-tag>")
		os.Exit(1)
	}
	version := os.Args[1]

	previousTag, err := findPreviousTag(version)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
		os.Exit(1)
	}
	if previousTag == "" {
		fmt.Fprintf(os.Stderr, "Generating release notes for %s from repository start\n", version)
	} else {
		fmt.Fprintf(os.Stderr, "Generating release notes for %s since %s\n", version, previousTag)
	}

	prNumbers, err := collectPRs(previousTag, version)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
		os.Exit(1)
	}
	if len(prNumbers) == 0 {
		fmt.Fprintln(os.Stderr, "No merged pull requests found")
	}

	pullRequests := make([]prData, 0, len(prNumbers))
	for _, number := range prNumbers {
		data, err := fetchPR(number)
		if err != nil {
			fmt.Fprintf(os.Stderr, "WARNING: Could not fetch PR #%s: %v\n", number, err)
			continue
		}
		data.Number = number
		pullRequests = append(pullRequests, *data)
	}
	fmt.Print(renderReleaseNotes(pullRequests))
}

func renderReleaseNotes(pullRequests []prData) string {
	categoryNotes := make(map[string][]string)
	var otherNotes []string
	for _, pullRequest := range pullRequests {
		note := extractNote(pullRequest.Body)
		if note == "" || isNone(note) {
			continue
		}

		labelSet := make(map[string]bool)
		for _, label := range pullRequest.Labels {
			labelSet[label.Name] = true
		}

		formatted := formatNote(note, pullRequest.Number, pullRequest.Author.Login)
		matched := false
		for _, category := range categories {
			if labelSet[category.Label] {
				categoryNotes[category.Label] = append(categoryNotes[category.Label], formatted...)
				matched = true
				break
			}
		}
		if !matched {
			otherNotes = append(otherNotes, formatted...)
		}
	}

	var output strings.Builder
	for _, category := range categories {
		lines := categoryNotes[category.Label]
		if len(lines) == 0 {
			continue
		}
		fmt.Fprintf(&output, "## %s\n\n", category.Heading)
		for _, line := range lines {
			fmt.Fprintln(&output, line)
		}
		fmt.Fprintln(&output)
	}

	if len(otherNotes) > 0 {
		fmt.Fprintln(&output, "## Other Changes")
		fmt.Fprintln(&output)
		for _, line := range otherNotes {
			fmt.Fprintln(&output, line)
		}
		fmt.Fprintln(&output)
	}

	if output.Len() == 0 {
		return "No notable changes.\n"
	}
	return output.String()
}

func findPreviousTag(version string) (string, error) {
	out, err := gitOutput("tag", "--list", "v*", "--sort=-version:refname")
	if err != nil {
		return "", fmt.Errorf("listing tags: %w", err)
	}
	previous, found := selectPreviousTag(out, version)
	if !found {
		return "", fmt.Errorf("release tag %q was not found", version)
	}
	return previous, nil
}

func selectPreviousTag(tagList, version string) (string, bool) {
	found := false
	for _, tag := range strings.Split(strings.TrimSpace(tagList), "\n") {
		tag = strings.TrimSpace(tag)
		if !releaseTagRe.MatchString(tag) {
			continue
		}
		if tag == version {
			found = true
			continue
		}
		if found {
			return tag, true
		}
	}
	return "", found
}

var releaseTagRe = regexp.MustCompile(`^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$`)

func collectPRs(previousTag, version string) ([]string, error) {
	revisionRange := version
	if previousTag != "" {
		revisionRange = previousTag + ".." + version
	}
	out, err := gitOutput("log", revisionRange, "--merges", "--oneline")
	if err != nil {
		return nil, fmt.Errorf("listing merge commits: %w", err)
	}
	return parsePRNumbers(out), nil
}

var mergePRRe = regexp.MustCompile(`Merge pull request #(\d+)`)

func parsePRNumbers(gitLogOutput string) []string {
	var numbers []string
	for _, line := range strings.Split(gitLogOutput, "\n") {
		if match := mergePRRe.FindStringSubmatch(line); match != nil {
			numbers = append(numbers, match[1])
		}
	}
	return numbers
}

func fetchPR(number string) (*prData, error) {
	out, err := runCommand("gh", "pr", "view", number, "--json", "author,body,labels")
	if err != nil {
		return nil, err
	}
	var data prData
	if err := json.Unmarshal([]byte(out), &data); err != nil {
		return nil, fmt.Errorf("parsing pull request JSON: %w", err)
	}
	return &data, nil
}

func extractNote(body string) string {
	lines := strings.Split(body, "\n")
	var inBlock bool
	var result []string
	for _, line := range lines {
		if strings.TrimSpace(line) == "```release-note" {
			inBlock = true
			continue
		}
		if inBlock && strings.TrimSpace(line) == "```" {
			break
		}
		if inBlock && strings.TrimSpace(line) != "" {
			result = append(result, line)
		}
	}
	return strings.Join(result, "\n")
}

func isNone(note string) bool {
	return strings.EqualFold(strings.TrimSpace(note), "none")
}

func formatNote(note, pr, author string) []string {
	suffix := fmt.Sprintf("(#%s", pr)
	if author != "" {
		suffix += ", @" + author
	}
	suffix += ")"
	var lines []string
	for _, line := range strings.Split(note, "\n") {
		if line != "" {
			lines = append(lines, fmt.Sprintf("- %s %s", line, suffix))
		}
	}
	return lines
}

func gitOutput(args ...string) (string, error) {
	return runCommand("git", args...)
}

func runCommand(name string, args ...string) (string, error) {
	command := exec.Command(name, args...)
	out, err := command.Output()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			return "", fmt.Errorf("%s %v: %w: %s", name, args, err, string(exitErr.Stderr))
		}
		return "", fmt.Errorf("%s %v: %w", name, args, err)
	}
	return string(out), nil
}
