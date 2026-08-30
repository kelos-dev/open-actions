package workflowcontext

import (
	"encoding/json"
	"net/url"
	"strconv"
	"strings"
)

// GitHubValues contains the data used to construct the github context at a
// planning or execution phase.
type GitHubValues struct {
	Action            string
	ActionPath        string
	ActionRef         string
	ActionRepository  string
	ActionStatus      string
	Actor             string
	ActorID           string
	APIURL            string
	BaseRef           string
	EnvironmentFile   string
	Event             map[string]any
	EventName         string
	EventPath         string
	HeadRef           string
	JobID             string
	PathFile          string
	Ref               string
	RefName           string
	RefProtected      *bool
	RepositoryID      int64
	RepositoryName    string
	RepositoryOwner   string
	RepositoryOwnerID string
	RepositoryURL     string
	RetentionDays     string
	RunAttempt        int32
	RunID             int64
	RunNumber         int64
	SecretSource      string
	ServerURL         string
	SHA               string
	Token             any
	TriggeringActor   string
	WorkflowName      string
	WorkflowPath      string
	WorkflowSHA       string
	Workspace         string
}

// GitHub returns the documented github context properties whose values Open
// Actions can determine at the current phase.
func GitHub(input GitHubValues) map[string]any {
	serverURL := strings.TrimSuffix(input.ServerURL, "/")
	apiURL := strings.TrimSuffix(input.APIURL, "/")
	repository := ""
	if input.RepositoryOwner != "" && input.RepositoryName != "" {
		repository = input.RepositoryOwner + "/" + input.RepositoryName
	}
	workflowSHA := input.WorkflowSHA
	if workflowSHA == "" {
		workflowSHA = input.SHA
	}
	triggeringActor := input.TriggeringActor
	if triggeringActor == "" {
		triggeringActor = input.Actor
	}
	secretSource := input.SecretSource
	if secretSource == "" {
		secretSource = "Actions"
		if strings.EqualFold(input.Actor, "dependabot[bot]") {
			secretSource = "Dependabot"
		}
	}
	repositoryURL := input.RepositoryURL
	if repositoryURL == "" {
		repositoryURL = RepositoryURL(serverURL, repository)
	}
	values := map[string]any{
		"action":            input.Action,
		"action_ref":        input.ActionRef,
		"action_repository": input.ActionRepository,
		"action_status":     input.ActionStatus,
		"actor":             input.Actor,
		"api_url":           apiURL,
		"base_ref":          input.BaseRef,
		"env":               input.EnvironmentFile,
		"event":             input.Event,
		"event_name":        input.EventName,
		"event_path":        input.EventPath,
		"graphql_url":       GraphQLURL(apiURL),
		"head_ref":          input.HeadRef,
		"job":               input.JobID,
		"path":              input.PathFile,
		"ref":               input.Ref,
		"ref_name":          input.RefName,
		"ref_type":          RefType(input.Ref),
		"repository":        repository,
		"repository_id":     positiveNumber(input.RepositoryID),
		"repository_owner":  input.RepositoryOwner,
		"repositoryUrl":     repositoryURL,
		"retention_days":    input.RetentionDays,
		"run_attempt":       positiveNumber(int64(input.RunAttempt)),
		"run_id":            positiveNumber(input.RunID),
		"run_number":        positiveNumber(input.RunNumber),
		"secret_source":     secretSource,
		"server_url":        serverURL,
		"sha":               input.SHA,
		"token":             input.Token,
		"triggering_actor":  triggeringActor,
		"workflow":          input.WorkflowName,
		"workflow_ref":      WorkflowRef(repository, input.WorkflowPath, input.Ref),
		"workflow_sha":      workflowSHA,
		"workspace":         input.Workspace,
	}
	if input.ActionPath != "" {
		values["action_path"] = input.ActionPath
	}
	if input.ActorID != "" {
		values["actor_id"] = input.ActorID
	}
	if input.RepositoryOwnerID != "" {
		values["repository_owner_id"] = input.RepositoryOwnerID
	}
	if input.RefProtected != nil {
		values["ref_protected"] = *input.RefProtected
	}
	return values
}

// EventID returns a numeric webhook property using GitHub's documented string
// representation for IDs.
func EventID(event map[string]any, path ...string) string {
	value, found := eventValue(event, path...)
	if !found {
		return ""
	}
	switch typed := value.(type) {
	case json.Number:
		if parsed, err := typed.Int64(); err == nil {
			return positiveNumber(parsed)
		}
	case float64:
		if typed > 0 && typed == float64(int64(typed)) {
			return strconv.FormatInt(int64(typed), 10)
		}
	case int64:
		return positiveNumber(typed)
	case int:
		return positiveNumber(int64(typed))
	case string:
		if parsed, err := strconv.ParseInt(typed, 10, 64); err == nil {
			return positiveNumber(parsed)
		}
	}
	return ""
}

// EventString returns a string webhook property.
func EventString(event map[string]any, path ...string) string {
	value, found := eventValue(event, path...)
	if !found {
		return ""
	}
	result, _ := value.(string)
	return result
}

func eventValue(event map[string]any, path ...string) (any, bool) {
	var current any = event
	for _, name := range path {
		values, ok := current.(map[string]any)
		if !ok {
			return nil, false
		}
		current, ok = values[name]
		if !ok {
			return nil, false
		}
	}
	return current, true
}

func positiveNumber(value int64) string {
	if value < 1 {
		return ""
	}
	return strconv.FormatInt(value, 10)
}

// GraphQLURL derives GitHub's GraphQL endpoint from the REST API endpoint.
func GraphQLURL(apiURL string) string {
	apiURL = strings.TrimSuffix(apiURL, "/")
	if apiURL == "" {
		return ""
	}
	if strings.HasSuffix(apiURL, "/api/v3") {
		return strings.TrimSuffix(apiURL, "/api/v3") + "/api/graphql"
	}
	return apiURL + "/graphql"
}

// RefType returns the GitHub Actions ref_type value for a workflow revision.
func RefType(ref string) string {
	if ref == "" {
		return ""
	}
	if strings.HasPrefix(ref, "refs/tags/") {
		return "tag"
	}
	return "branch"
}

// RepositoryURL returns the clone URL exposed as github.repositoryUrl.
func RepositoryURL(serverURL, repository string) string {
	if serverURL == "" || repository == "" {
		return ""
	}
	server, err := url.Parse(serverURL)
	if err != nil || server.Host == "" {
		return ""
	}
	return "git://" + server.Host + "/" + repository + ".git"
}

// WorkflowRef returns the fully qualified workflow file reference.
func WorkflowRef(repository, path, ref string) string {
	if repository == "" || path == "" || ref == "" {
		return ""
	}
	return repository + "/" + path + "@" + ref
}

// Job returns the job context for a directly defined workflow job.
func Job(status, workflowRef, workflowSHA, workflowRepository, workflowPath string) map[string]any {
	return map[string]any{
		"status":              status,
		"workflow_ref":        workflowRef,
		"workflow_sha":        workflowSHA,
		"workflow_repository": workflowRepository,
		"workflow_file_path":  workflowPath,
	}
}

// Strategy returns the strategy context for one expanded matrix job.
func Strategy(jobIndex, jobTotal, maxParallel int32, failFast bool) map[string]any {
	if maxParallel == 0 {
		maxParallel = jobTotal
	}
	return map[string]any{
		"job-index":    jobIndex,
		"job-total":    jobTotal,
		"max-parallel": maxParallel,
		"fail-fast":    failFast,
	}
}
