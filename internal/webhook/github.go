package webhook

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"unicode/utf8"

	actionsv1alpha1 "github.com/kelos-dev/open-actions/api/v1alpha1"
	"github.com/kelos-dev/open-actions/internal/eventsnapshot"
	githubclient "github.com/kelos-dev/open-actions/internal/github"
	"github.com/kelos-dev/open-actions/internal/workflow"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	maxPayloadBytes       = eventsnapshot.MaxBytes
	maxEventBodyLength    = 48_000
	maxEventURLLength     = 2_048
	maxEventTagNameLength = 1_014
	maxConclusionLength   = 64
	zeroGitSHA            = "0000000000000000000000000000000000000000"
)

type GitHubHandler struct {
	Client    client.Client
	APIReader client.Reader
	Logger    *slog.Logger
}

type payloadRepository struct {
	ID            int64  `json:"id"`
	Name          string `json:"name"`
	DefaultBranch string `json:"default_branch"`
	Owner         struct {
		Login string `json:"login"`
	} `json:"owner"`
}

type payload struct {
	Action       string `json:"action"`
	Ref          string `json:"ref"`
	Installation struct {
		ID int64 `json:"id"`
	} `json:"installation"`
	Repository  payloadRepository `json:"repository"`
	After       string            `json:"after"`
	Deleted     bool              `json:"deleted"`
	PullRequest struct {
		Number         int64  `json:"number"`
		Body           string `json:"body"`
		HTMLURL        string `json:"html_url"`
		MergeCommitSHA string `json:"merge_commit_sha"`
		State          string `json:"state"`
		Merged         bool   `json:"merged"`
		Mergeable      *bool  `json:"mergeable"`
		Head           struct {
			Ref        string            `json:"ref"`
			SHA        string            `json:"sha"`
			Repository payloadRepository `json:"repo"`
		} `json:"head"`
		Base struct {
			Ref string `json:"ref"`
			SHA string `json:"sha"`
		} `json:"base"`
	} `json:"pull_request"`
	MergeGroup struct {
		HeadSHA string `json:"head_sha"`
		HeadRef string `json:"head_ref"`
		BaseRef string `json:"base_ref"`
	} `json:"merge_group"`
	WorkflowRun struct {
		Name       string `json:"name"`
		HeadBranch string `json:"head_branch"`
		HeadSHA    string `json:"head_sha"`
		Conclusion string `json:"conclusion"`
	} `json:"workflow_run"`
	Issue struct {
		Number int64  `json:"number"`
		Body   string `json:"body"`
	} `json:"issue"`
	Comment struct {
		Body string `json:"body"`
	} `json:"comment"`
	Review struct {
		Body string `json:"body"`
	} `json:"review"`
	Release struct {
		TagName string `json:"tag_name"`
		Draft   bool   `json:"draft"`
	} `json:"release"`
	CheckRun struct {
		ID         int64  `json:"id"`
		HeadSHA    string `json:"head_sha"`
		ExternalID string `json:"external_id"`
		App        struct {
			ID int64 `json:"id"`
		} `json:"app"`
	} `json:"check_run"`
}

type normalizedRepository struct {
	ID    int64  `json:"id"`
	Owner string `json:"owner"`
	Name  string `json:"name"`
}

type normalizedPullRequest struct {
	Number         int64                `json:"number"`
	Body           string               `json:"body"`
	HTMLURL        string               `json:"htmlURL"`
	HeadRepository normalizedRepository `json:"headRepository"`
	HeadRef        string               `json:"headRef"`
	HeadSHA        string               `json:"headSHA"`
	BaseRef        string               `json:"baseRef"`
	BaseSHA        string               `json:"baseSHA,omitempty"`
}

type normalizedWorkflowRun struct {
	Conclusion string `json:"conclusion,omitempty"`
	HeadSHA    string `json:"headSHA"`
}

type normalizedIssue struct {
	Number int64  `json:"number"`
	Body   string `json:"body"`
}

type normalizedComment struct {
	Body string `json:"body"`
}

type normalizedReview struct {
	Body string `json:"body"`
}

type normalizedEvent struct {
	Name          string                 `json:"name"`
	Action        string                 `json:"action,omitempty"`
	SHA           string                 `json:"sha,omitempty"`
	Ref           string                 `json:"ref"`
	HeadRef       string                 `json:"headRef,omitempty"`
	BaseRef       string                 `json:"baseRef,omitempty"`
	ResolveRef    string                 `json:"resolveRef,omitempty"`
	HeadSHA       string                 `json:"headSHA,omitempty"`
	MergeBaseSHA  string                 `json:"mergeBaseSHA,omitempty"`
	Fork          bool                   `json:"fork,omitempty"`
	MergeRevision bool                   `json:"mergeRevision,omitempty"`
	WorkflowName  string                 `json:"workflowName,omitempty"`
	PullRequest   *normalizedPullRequest `json:"pullRequest,omitempty"`
	WorkflowRun   *normalizedWorkflowRun `json:"workflowRun,omitempty"`
	Issue         *normalizedIssue       `json:"issue,omitempty"`
	Comment       *normalizedComment     `json:"comment,omitempty"`
	Review        *normalizedReview      `json:"review,omitempty"`
}

type normalizedRerun struct {
	CheckRunID int64  `json:"checkRunID"`
	RootRunUID string `json:"rootRunUID"`
	HeadSHA    string `json:"headSHA"`
}

func (h *GitHubHandler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		writer.Header().Set("Allow", http.MethodPost)
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	body, err := io.ReadAll(io.LimitReader(request.Body, maxPayloadBytes+1))
	if err != nil {
		http.Error(writer, "read request body", http.StatusBadRequest)
		return
	}
	if len(body) > maxPayloadBytes {
		http.Error(writer, "request body is too large", http.StatusRequestEntityTooLarge)
		return
	}
	eventName := request.Header.Get("X-GitHub-Event")
	deliveryID := request.Header.Get("X-GitHub-Delivery")
	parsed := &payload{}
	if err := json.Unmarshal(body, parsed); err != nil {
		http.Error(writer, "invalid JSON payload", http.StatusBadRequest)
		return
	}
	if parsed.Installation.ID == 0 || deliveryID == "" || eventName == "" {
		http.Error(writer, "missing GitHub delivery metadata", http.StatusBadRequest)
		return
	}

	project, webhookSecret, err := h.projectForInstallation(request.Context(), parsed.Installation.ID)
	if err != nil {
		h.Logger.Error("failed to resolve project for webhook", "installation_id", parsed.Installation.ID, "error", err)
		http.Error(writer, "project unavailable", http.StatusServiceUnavailable)
		return
	}
	if !validSignature(body, webhookSecret, request.Header.Get("X-Hub-Signature-256")) {
		http.Error(writer, "invalid webhook signature", http.StatusUnauthorized)
		return
	}
	if eventName == "check_run" {
		rerun, supported, err := normalizeRerun(project, parsed)
		if err != nil {
			http.Error(writer, err.Error(), http.StatusBadRequest)
			return
		}
		if !supported {
			writeJSON(writer, http.StatusAccepted, map[string]any{"accepted": true, "queued": false})
			return
		}
		if err := h.enqueueRerunDelivery(request.Context(), project, parsed, rerun, deliveryID); err != nil {
			h.Logger.Error("failed to enqueue GitHub check rerun", "delivery_id", deliveryID, "check_run_id", rerun.CheckRunID, "error", err)
			if apierrors.IsConflict(err) {
				http.Error(writer, "webhook replay conflict", http.StatusConflict)
				return
			}
			http.Error(writer, "enqueue webhook delivery failed", http.StatusInternalServerError)
			return
		}
		h.Logger.Info("accepted GitHub check rerun", "delivery_id", deliveryID, "check_run_id", rerun.CheckRunID)
		writeJSON(writer, http.StatusAccepted, map[string]any{"accepted": true, "queued": true})
		return
	}
	normalized, supported, err := normalize(eventName, parsed)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusBadRequest)
		return
	}
	if !supported {
		writeJSON(writer, http.StatusAccepted, map[string]any{"accepted": true, "queued": false})
		return
	}
	if err := h.enqueueDelivery(request.Context(), project, parsed, normalized, deliveryID, body); err != nil {
		h.Logger.Error("failed to enqueue GitHub webhook", "delivery_id", deliveryID, "event", eventName, "error", err)
		if apierrors.IsConflict(err) {
			http.Error(writer, "webhook replay conflict", http.StatusConflict)
			return
		}
		http.Error(writer, "enqueue webhook delivery failed", http.StatusInternalServerError)
		return
	}
	h.Logger.Info("accepted GitHub webhook", "delivery_id", deliveryID, "event", eventName)
	writeJSON(writer, http.StatusAccepted, map[string]any{"accepted": true, "queued": true})
}

func normalizeRerun(project *actionsv1alpha1.Project, event *payload) (*normalizedRerun, bool, error) {
	if event.Action != "rerequested" {
		return nil, false, nil
	}
	github := project.Spec.Source.GitHub
	if github == nil || event.CheckRun.App.ID != github.AppID {
		return nil, false, errors.New("GitHub check run was not created by this Project's app")
	}
	if event.CheckRun.ID < 1 || event.CheckRun.ExternalID == "" || len(validation.IsValidLabelValue(event.CheckRun.ExternalID)) > 0 || !validGitSHA(event.CheckRun.HeadSHA) {
		return nil, false, errors.New("GitHub check run event is incomplete")
	}
	return &normalizedRerun{CheckRunID: event.CheckRun.ID, RootRunUID: event.CheckRun.ExternalID, HeadSHA: event.CheckRun.HeadSHA}, true, nil
}

func (h *GitHubHandler) projectForInstallation(ctx context.Context, installationID int64) (*actionsv1alpha1.Project, []byte, error) {
	projects := &actionsv1alpha1.ProjectList{}
	if err := h.APIReader.List(ctx, projects); err != nil {
		return nil, nil, err
	}
	matches := []*actionsv1alpha1.Project{}
	for index := range projects.Items {
		project := &projects.Items[index]
		configured := meta.FindStatusCondition(project.Status.Conditions, actionsv1alpha1.ProjectConditionConfigured)
		if project.Spec.Source.GitHub.InstallationID == installationID && configured != nil && configured.Status == metav1.ConditionTrue && configured.ObservedGeneration == project.Generation {
			matches = append(matches, project)
		}
	}
	if len(matches) != 1 {
		return nil, nil, fmt.Errorf("installation %d matched %d projects", installationID, len(matches))
	}
	project := matches[0]
	github := project.Spec.Source.GitHub
	webhookSecret, err := readSecretValue(ctx, h.APIReader, project.Namespace, github.WebhookSecretRef)
	if err != nil {
		return nil, nil, err
	}
	return project, webhookSecret, nil
}

func normalize(eventName string, event *payload) (normalizedEvent, bool, error) {
	result := normalizedEvent{Name: eventName, Action: event.Action}
	if !workflow.SupportsEventAction(eventName, event.Action) {
		return result, false, nil
	}
	switch eventName {
	case "push":
		result.Ref = event.Ref
		if event.Deleted {
			result.ResolveRef = event.Repository.DefaultBranch
		} else {
			result.SHA = event.After
		}
	case "pull_request":
		pullRequest := event.PullRequest
		if pullRequest.State != "open" && pullRequest.State != "closed" {
			return normalizedEvent{}, false, errors.New("GitHub pull request event has an invalid state")
		}
		metadata, err := pullRequestMetadata(event)
		if err != nil {
			return normalizedEvent{}, false, err
		}
		if metadata == nil {
			return result, false, nil
		}
		if pullRequest.Merged && (pullRequest.State != "closed" || event.Action != "closed") {
			return normalizedEvent{}, false, errors.New("GitHub pull request event has an invalid merged state")
		}
		mergeRef := "refs/pull/" + strconv.FormatInt(pullRequest.Number, 10) + "/merge"
		result.PullRequest = metadata
		result.Fork = pullRequest.Head.Repository.ID != event.Repository.ID
		if pullRequest.Merged {
			if pullRequest.MergeCommitSHA == "" {
				return normalizedEvent{}, false, errors.New("GitHub merged pull request event does not identify a revision")
			}
			result.SHA = pullRequest.MergeCommitSHA
			result.Ref = "refs/heads/" + pullRequest.Base.Ref
			result.MergeRevision = true
		} else {
			result.Ref = mergeRef
			if pullRequest.State == "open" && !result.Fork && (pullRequest.Mergeable == nil || *pullRequest.Mergeable) {
				result.HeadSHA = pullRequest.Head.SHA
				result.MergeRevision = true
			} else if pullRequest.State == "closed" && pullRequest.MergeCommitSHA != "" {
				result.SHA = pullRequest.MergeCommitSHA
				result.MergeRevision = true
			}
		}
		result.HeadRef = pullRequest.Head.Ref
		result.BaseRef = pullRequest.Base.Ref
	case "pull_request_review", "pull_request_review_comment":
		pullRequest, err := pullRequestMetadata(event)
		if err != nil {
			return normalizedEvent{}, false, err
		}
		if pullRequest == nil {
			return result, false, nil
		}
		result.PullRequest = pullRequest
		if eventName == "pull_request_review" {
			if !validEventBody(event.Review.Body) {
				return normalizedEvent{}, false, errors.New("GitHub pull request review body is too large")
			}
			result.Review = &normalizedReview{Body: event.Review.Body}
		} else {
			if !validEventBody(event.Comment.Body) {
				return normalizedEvent{}, false, errors.New("GitHub pull request review comment body is too large")
			}
			result.Comment = &normalizedComment{Body: event.Comment.Body}
		}
		setDefaultBranchRevision(&result, event)
	case "merge_group":
		result.SHA = event.MergeGroup.HeadSHA
		result.Ref = event.MergeGroup.HeadRef
		result.BaseRef = githubclient.RefName(event.MergeGroup.BaseRef)
	case "workflow_run":
		if event.WorkflowRun.Name == "" || !validGitSHA(event.WorkflowRun.HeadSHA) || !validConclusion(event.WorkflowRun.Conclusion, event.Action == "completed") {
			return normalizedEvent{}, false, errors.New("GitHub workflow run event is incomplete")
		}
		result.WorkflowName = event.WorkflowRun.Name
		result.BaseRef = event.WorkflowRun.HeadBranch
		result.WorkflowRun = &normalizedWorkflowRun{Conclusion: event.WorkflowRun.Conclusion, HeadSHA: event.WorkflowRun.HeadSHA}
		setDefaultBranchRevision(&result, event)
	case "issues", "issue_comment":
		if event.Issue.Number < 1 || !validEventBody(event.Issue.Body) {
			return normalizedEvent{}, false, errors.New("GitHub issue event is incomplete")
		}
		result.Issue = &normalizedIssue{Number: event.Issue.Number, Body: event.Issue.Body}
		if eventName == "issue_comment" {
			if !validEventBody(event.Comment.Body) {
				return normalizedEvent{}, false, errors.New("GitHub issue comment body is too large")
			}
			result.Comment = &normalizedComment{Body: event.Comment.Body}
		}
		setDefaultBranchRevision(&result, event)
	case "release":
		if event.Release.Draft {
			return result, false, nil
		}
		if event.Release.TagName == "" || utf8.RuneCountInString(event.Release.TagName) > maxEventTagNameLength {
			return normalizedEvent{}, false, errors.New("GitHub release event has no tag")
		}
		result.Ref = "refs/tags/" + event.Release.TagName
		result.ResolveRef = result.Ref
	default:
		return result, false, nil
	}
	missingRevision := result.SHA == "" && result.ResolveRef == ""
	pullRequestRevision := eventName == "pull_request" && result.PullRequest != nil
	if (missingRevision && !pullRequestRevision) || result.Ref == "" || githubclient.RefName(result.Ref) == "" {
		return normalizedEvent{}, false, errors.New("GitHub event does not identify a revision")
	}
	if result.SHA != "" && !validGitSHA(result.SHA) {
		return normalizedEvent{}, false, errors.New("GitHub event contains an invalid revision")
	}
	return result, true, nil
}

func pullRequestMetadata(event *payload) (*normalizedPullRequest, error) {
	pullRequest := event.PullRequest
	if pullRequest.Number < 1 || pullRequest.Head.Ref == "" || pullRequest.Base.Ref == "" || !validGitHubHTMLURL(pullRequest.HTMLURL) || !validEventBody(pullRequest.Body) {
		return nil, errors.New("GitHub pull request event is incomplete")
	}
	if pullRequest.Head.Repository.ID == 0 || pullRequest.Head.Repository.Owner.Login == "" || pullRequest.Head.Repository.Name == "" {
		return nil, nil
	}
	if !validGitSHA(pullRequest.Head.SHA) {
		return nil, errors.New("GitHub pull request event contains an invalid head revision")
	}
	if !validGitSHA(pullRequest.Base.SHA) {
		return nil, errors.New("GitHub pull request event contains an invalid base revision")
	}
	return &normalizedPullRequest{
		Number: pullRequest.Number, Body: pullRequest.Body, HTMLURL: pullRequest.HTMLURL,
		HeadRepository: normalizedRepository{
			ID: pullRequest.Head.Repository.ID, Owner: pullRequest.Head.Repository.Owner.Login, Name: pullRequest.Head.Repository.Name,
		},
		HeadRef: pullRequest.Head.Ref, HeadSHA: pullRequest.Head.SHA,
		BaseRef: pullRequest.Base.Ref, BaseSHA: pullRequest.Base.SHA,
	}, nil
}

func validEventBody(body string) bool {
	return utf8.RuneCountInString(body) <= maxEventBodyLength
}

func validGitHubHTMLURL(value string) bool {
	return value != "" && utf8.RuneCountInString(value) <= maxEventURLLength && (strings.HasPrefix(value, "https://") || strings.HasPrefix(value, "http://")) && !strings.ContainsAny(value, " \t\r\n")
}

func validConclusion(value string, required bool) bool {
	if value == "" {
		return !required
	}
	if len(value) > maxConclusionLength || value[0] < 'a' || value[0] > 'z' {
		return false
	}
	for _, character := range value[1:] {
		if (character < 'a' || character > 'z') && character != '_' {
			return false
		}
	}
	return true
}

func setDefaultBranchRevision(result *normalizedEvent, event *payload) {
	result.Ref = "refs/heads/" + event.Repository.DefaultBranch
	result.ResolveRef = event.Repository.DefaultBranch
}

func validSignature(body, secret []byte, signature string) bool {
	const prefix = "sha256="
	if !strings.HasPrefix(signature, prefix) {
		return false
	}
	received, err := hex.DecodeString(strings.TrimPrefix(signature, prefix))
	if err != nil {
		return false
	}
	digest := hmac.New(sha256.New, secret)
	digest.Write(body)
	return hmac.Equal(received, digest.Sum(nil))
}

func validGitSHA(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == 20 && value == strings.ToLower(value) && value != zeroGitSHA
}

func writeJSON(writer http.ResponseWriter, status int, value any) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(value)
}
