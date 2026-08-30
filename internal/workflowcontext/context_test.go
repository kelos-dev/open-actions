package workflowcontext

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func TestGitHubContextIncludesDocumentedProperties(t *testing.T) {
	protected := true
	values := GitHub(GitHubValues{
		Action:            "__run",
		ActionPath:        "/actions/example",
		ActionRef:         "v1",
		ActionRepository:  "actions/example",
		ActionStatus:      "success",
		Actor:             "octocat",
		ActorID:           "1",
		APIURL:            "https://github.example/api/v3/",
		BaseRef:           "main",
		EnvironmentFile:   "/commands/env",
		Event:             map[string]any{"action": "opened"},
		EventName:         "pull_request",
		EventPath:         "/events/payload.json",
		HeadRef:           "feature",
		JobID:             "build",
		PathFile:          "/commands/path",
		Ref:               "refs/pull/42/merge",
		RefName:           "42/merge",
		RefProtected:      &protected,
		RepositoryID:      2,
		RepositoryName:    "example",
		RepositoryOwner:   "acme",
		RepositoryOwnerID: "3",
		RetentionDays:     "30",
		RunAttempt:        2,
		RunID:             101,
		RunNumber:         7,
		ServerURL:         "https://github.example/",
		SHA:               strings.Repeat("a", 40),
		Token:             "token",
		TriggeringActor:   "hubot",
		WorkflowName:      "CI",
		WorkflowPath:      ".github/workflows/ci.yml",
		Workspace:         "/workspace",
	})
	properties := []string{
		"action", "action_path", "action_ref", "action_repository", "action_status",
		"actor", "actor_id", "api_url", "base_ref", "env", "event", "event_name",
		"event_path", "graphql_url", "head_ref", "job", "path", "ref", "ref_name",
		"ref_protected", "ref_type", "repository", "repository_id", "repository_owner",
		"repository_owner_id", "repositoryUrl", "retention_days", "run_id", "run_number",
		"run_attempt", "secret_source", "server_url", "sha", "token", "triggering_actor",
		"workflow", "workflow_ref", "workflow_sha", "workspace",
	}
	for _, property := range properties {
		if _, found := values[property]; !found {
			t.Errorf("github.%s is absent", property)
		}
	}
	if len(values) != len(properties) {
		t.Errorf("github context has %d properties, want %d: %#v", len(values), len(properties), values)
	}
	for _, property := range properties {
		if property == "event" || property == "ref_protected" {
			continue
		}
		if _, ok := values[property].(string); !ok {
			t.Errorf("github.%s type = %T, want string", property, values[property])
		}
	}
	if values["repository_owner"] != "acme" || values["repository_id"] != "2" ||
		values["ref_type"] != "branch" || values["graphql_url"] != "https://github.example/api/graphql" ||
		values["repositoryUrl"] != "git://github.example/acme/example.git" ||
		values["workflow_ref"] != "acme/example/.github/workflows/ci.yml@refs/pull/42/merge" ||
		values["workflow_sha"] != strings.Repeat("a", 40) {
		t.Fatalf("github context = %#v", values)
	}
	if _, ok := values["ref_protected"].(bool); !ok {
		t.Errorf("github.ref_protected type = %T", values["ref_protected"])
	}

	available := GitHub(GitHubValues{})
	for _, property := range []string{"api_url", "graphql_url", "ref", "ref_type", "repository", "repositoryUrl", "workflow_ref"} {
		if available[property] != "" {
			t.Errorf("github.%s = %#v without a source value, want empty string", property, available[property])
		}
	}
	for _, property := range []string{"action_path", "actor_id", "repository_owner_id", "ref_protected"} {
		if _, found := available[property]; found {
			t.Errorf("github.%s is present without a source value", property)
		}
	}
}

func TestStrategyContextPreservesDocumentedTypes(t *testing.T) {
	values := Strategy(1, 4, 0, false)
	want := map[string]any{
		"job-index": int32(1), "job-total": int32(4), "max-parallel": int32(4), "fail-fast": false,
	}
	if !reflect.DeepEqual(values, want) {
		t.Fatalf("strategy = %#v, want %#v", values, want)
	}
}

func TestJobContextIncludesAvailableMetadata(t *testing.T) {
	values := Job("failure", "acme/example/.github/workflows/ci.yml@refs/heads/main", "sha", "acme/example", ".github/workflows/ci.yml")
	if len(values) != 5 {
		t.Fatalf("job context has %d properties: %#v", len(values), values)
	}
	if values["status"] != "failure" ||
		values["workflow_repository"] != "acme/example" || values["workflow_file_path"] != ".github/workflows/ci.yml" {
		t.Fatalf("job context = %#v", values)
	}
}

func TestRefType(t *testing.T) {
	if RefType("refs/tags/v1") != "tag" || RefType("refs/heads/main") != "branch" || RefType("refs/pull/42/merge") != "branch" {
		t.Fatal("ref type does not match GitHub Actions")
	}
}

func TestRepositoryURLUsesGitProtocol(t *testing.T) {
	if got := RepositoryURL("https://github.example/", "acme/example"); got != "git://github.example/acme/example.git" {
		t.Fatalf("RepositoryURL() = %q", got)
	}
	if got := RepositoryURL("github.example", "acme/example"); got != "" {
		t.Fatalf("RepositoryURL() with invalid server URL = %q, want empty string", got)
	}
}

func TestEventIDPreservesDecodedNumbers(t *testing.T) {
	event := map[string]any{"sender": map[string]any{"id": json.Number("9007199254740993")}}
	if got := EventID(event, "sender", "id"); got != "9007199254740993" {
		t.Fatalf("EventID() = %q", got)
	}
}
