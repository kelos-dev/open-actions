package workflowenv

import "strings"

var runnerOwnedNames = map[string]struct{}{
	"GITHUB_ACTION_PATH":       {},
	"GITHUB_ACTION_REPOSITORY": {},
	"GITHUB_ACTIONS":           {},
	"GITHUB_API_URL":           {},
	"GITHUB_BASE_REF":          {},
	"GITHUB_ENV":               {},
	"GITHUB_EVENT_ACTION":      {},
	"GITHUB_EVENT_NAME":        {},
	"GITHUB_EVENT_PATH":        {},
	"GITHUB_HEAD_REF":          {},
	"GITHUB_JOB":               {},
	"GITHUB_OUTPUT":            {},
	"GITHUB_PATH":              {},
	"GITHUB_REF":               {},
	"GITHUB_REF_NAME":          {},
	"GITHUB_REPOSITORY":        {},
	"GITHUB_SERVER_URL":        {},
	"GITHUB_SHA":               {},
	"GITHUB_STATE":             {},
	"GITHUB_STEP_SUMMARY":      {},
	"GITHUB_WORKFLOW":          {},
	"GITHUB_WORKSPACE":         {},
	"RUNNER_ARCH":              {},
	"RUNNER_OS":                {},
	"RUNNER_TEMP":              {},
	"RUNNER_TOOL_CACHE":        {},
}

// IsRunnerOwned reports whether Open Actions supplies a variable at execution.
func IsRunnerOwned(name string) bool {
	_, found := runnerOwnedNames[name]
	return found
}

// IsEnvironmentFileBlocked reports whether GITHUB_ENV cannot set a variable.
func IsEnvironmentFileBlocked(name string) bool {
	return strings.EqualFold(name, "NODE_OPTIONS")
}
