package workflowenv

import "testing"

func TestIsRunnerOwned(t *testing.T) {
	for _, name := range []string{
		"GITHUB_ACTION_PATH",
		"GITHUB_ACTION_REPOSITORY",
		"GITHUB_ACTIONS",
		"GITHUB_API_URL",
		"GITHUB_ACTOR",
		"GITHUB_BASE_REF",
		"GITHUB_ENV",
		"GITHUB_EVENT_ACTION",
		"GITHUB_EVENT_NAME",
		"GITHUB_EVENT_PATH",
		"GITHUB_GRAPHQL_URL",
		"GITHUB_HEAD_REF",
		"GITHUB_JOB",
		"GITHUB_OUTPUT",
		"GITHUB_PATH",
		"GITHUB_REF",
		"GITHUB_REF_NAME",
		"GITHUB_REF_TYPE",
		"GITHUB_REPOSITORY",
		"GITHUB_REPOSITORY_ID",
		"GITHUB_REPOSITORY_OWNER",
		"GITHUB_RETENTION_DAYS",
		"GITHUB_RUN_ATTEMPT",
		"GITHUB_RUN_ID",
		"GITHUB_RUN_NUMBER",
		"GITHUB_SERVER_URL",
		"GITHUB_SHA",
		"GITHUB_STATE",
		"GITHUB_STEP_SUMMARY",
		"GITHUB_TRIGGERING_ACTOR",
		"GITHUB_WORKFLOW",
		"GITHUB_WORKFLOW_REF",
		"GITHUB_WORKFLOW_SHA",
		"GITHUB_WORKSPACE",
		"RUNNER_ARCH",
		"RUNNER_DEBUG",
		"RUNNER_ENVIRONMENT",
		"RUNNER_NAME",
		"RUNNER_OS",
		"RUNNER_TEMP",
		"RUNNER_TOOL_CACHE",
	} {
		if !IsRunnerOwned(name) {
			t.Errorf("IsRunnerOwned(%q) = false", name)
		}
	}
	for _, name := range []string{"GITHUB_TOKEN", "GITHUB_CUSTOM", "RUNNER_CUSTOM", "github_sha", "runner_os", "CI"} {
		if IsRunnerOwned(name) {
			t.Errorf("IsRunnerOwned(%q) = true", name)
		}
	}
}

func TestIsEnvironmentFileBlocked(t *testing.T) {
	for _, name := range []string{"NODE_OPTIONS", "node_options"} {
		if !IsEnvironmentFileBlocked(name) {
			t.Errorf("IsEnvironmentFileBlocked(%q) = false", name)
		}
	}
	for _, name := range []string{"GITHUB_TOKEN", "NODE_PATH"} {
		if IsEnvironmentFileBlocked(name) {
			t.Errorf("IsEnvironmentFileBlocked(%q) = true", name)
		}
	}
}
