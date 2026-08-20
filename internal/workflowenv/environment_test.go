package workflowenv

import "testing"

func TestIsRunnerOwned(t *testing.T) {
	for _, name := range []string{
		"GITHUB_ACTION_PATH",
		"GITHUB_ACTION_REPOSITORY",
		"GITHUB_ACTIONS",
		"GITHUB_API_URL",
		"GITHUB_BASE_REF",
		"GITHUB_ENV",
		"GITHUB_EVENT_ACTION",
		"GITHUB_EVENT_NAME",
		"GITHUB_EVENT_PATH",
		"GITHUB_HEAD_REF",
		"GITHUB_JOB",
		"GITHUB_OUTPUT",
		"GITHUB_PATH",
		"GITHUB_REF",
		"GITHUB_REF_NAME",
		"GITHUB_REPOSITORY",
		"GITHUB_SERVER_URL",
		"GITHUB_SHA",
		"GITHUB_STATE",
		"GITHUB_STEP_SUMMARY",
		"GITHUB_WORKFLOW",
		"GITHUB_WORKSPACE",
		"RUNNER_ARCH",
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
