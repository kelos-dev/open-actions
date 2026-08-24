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

package main

import (
	"reflect"
	"testing"
)

func TestSelectPreviousTag(t *testing.T) {
	tests := []struct {
		name      string
		tags      string
		version   string
		want      string
		wantFound bool
	}{
		{name: "previous release", tags: "v1.2.0\nv1.1.0\nv1.0.0\n", version: "v1.2.0", want: "v1.1.0", wantFound: true},
		{name: "ignore prerelease tags", tags: "v1.2.0\nv1.2.0-rc.1\nv1.1.0\n", version: "v1.2.0", want: "v1.1.0", wantFound: true},
		{name: "first release", tags: "v0.1.0\n", version: "v0.1.0", wantFound: true},
		{name: "missing release", tags: "v1.1.0\nv1.0.0\n", version: "v1.2.0"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, found := selectPreviousTag(test.tags, test.version)
			if got != test.want || found != test.wantFound {
				t.Fatalf("selectPreviousTag() = %q, %v, want %q, %v", got, found, test.want, test.wantFound)
			}
		})
	}
}

func TestParsePRNumbers(t *testing.T) {
	tests := []struct {
		name string
		log  string
		want []string
	}{
		{
			name: "merge commits",
			log:  "abc1234 Merge pull request #523 from org/feature\ndef5678 Merge pull request #525 from org/fix",
			want: []string{"523", "525"},
		},
		{
			name: "mixed commits",
			log:  "abc1234 Merge pull request #100 from org/branch\ndef5678 Regular commit\nghi9012 Merge pull request #200 from org/other",
			want: []string{"100", "200"},
		},
		{name: "no merge commits", log: "abc1234 Regular commit"},
		{name: "empty output"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := parsePRNumbers(test.log); !reflect.DeepEqual(got, test.want) {
				t.Fatalf("parsePRNumbers() = %#v, want %#v", got, test.want)
			}
		})
	}
}

func TestExtractNote(t *testing.T) {
	tests := []struct {
		name string
		body string
		want string
	}{
		{name: "release note", body: "## Description\nText\n\n```release-note\nAdded a feature\n```", want: "Added a feature"},
		{name: "none", body: "```release-note\nNONE\n```", want: "NONE"},
		{name: "multiple lines", body: "```release-note\nFixed one issue\nFixed another issue\n```", want: "Fixed one issue\nFixed another issue"},
		{name: "missing", body: "No release note"},
		{name: "empty", body: "```release-note\n\n```"},
		{name: "blank lines", body: "```release-note\n\nAdded a feature\n\n```", want: "Added a feature"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := extractNote(test.body); got != test.want {
				t.Fatalf("extractNote() = %q, want %q", got, test.want)
			}
		})
	}
}

func TestIsNone(t *testing.T) {
	tests := []struct {
		note string
		want bool
	}{
		{note: "NONE", want: true},
		{note: "none", want: true},
		{note: " None ", want: true},
		{note: "Added a feature"},
		{note: ""},
	}

	for _, test := range tests {
		if got := isNone(test.note); got != test.want {
			t.Errorf("isNone(%q) = %v, want %v", test.note, got, test.want)
		}
	}
}

func TestFormatNote(t *testing.T) {
	tests := []struct {
		name   string
		note   string
		pr     string
		author string
		want   []string
	}{
		{name: "with author", note: "Added a feature", pr: "42", author: "octocat", want: []string{"- Added a feature (#42, @octocat)"}},
		{name: "multiple lines", note: "Fixed one\nFixed two", pr: "99", author: "octocat", want: []string{"- Fixed one (#99, @octocat)", "- Fixed two (#99, @octocat)"}},
		{name: "without author", note: "Added a feature", pr: "42", want: []string{"- Added a feature (#42)"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := formatNote(test.note, test.pr, test.author); !reflect.DeepEqual(got, test.want) {
				t.Fatalf("formatNote() = %#v, want %#v", got, test.want)
			}
		})
	}
}

func TestRenderReleaseNotes(t *testing.T) {
	pullRequests := []prData{
		{
			Number: "10",
			Author: prAuthor{Login: "api-author"},
			Body:   "```release-note\nAdded an API field\n```",
			Labels: []prLabel{{Name: "kind/feature"}, {Name: "kind/api"}},
		},
		{
			Number: "11",
			Author: prAuthor{Login: "feature-author"},
			Body:   "```release-note\nAdded a feature\n```",
			Labels: []prLabel{{Name: "kind/feature"}},
		},
		{
			Number: "12",
			Body:   "```release-note\nChanged release automation\n```",
		},
		{
			Number: "13",
			Body:   "```release-note\nNONE\n```",
			Labels: []prLabel{{Name: "kind/bug"}},
		},
	}
	want := "## API Changes\n\n" +
		"- Added an API field (#10, @api-author)\n\n" +
		"## Features\n\n" +
		"- Added a feature (#11, @feature-author)\n\n" +
		"## Other Changes\n\n" +
		"- Changed release automation (#12)\n\n"
	if got := renderReleaseNotes(pullRequests); got != want {
		t.Fatalf("renderReleaseNotes() = %q, want %q", got, want)
	}
}

func TestRenderReleaseNotesWithoutNotableChanges(t *testing.T) {
	pullRequests := []prData{
		{Number: "10", Body: "```release-note\nNONE\n```", Labels: []prLabel{{Name: "kind/feature"}}},
		{Number: "11", Body: "No release note"},
	}
	if got := renderReleaseNotes(pullRequests); got != "No notable changes.\n" {
		t.Fatalf("renderReleaseNotes() = %q", got)
	}
}
