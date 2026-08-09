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

	actionsv1alpha1 "github.com/kelos-dev/open-actions/api/v1alpha1"
	githubclient "github.com/kelos-dev/open-actions/internal/github"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	maxPayloadBytes = 10 << 20
	zeroGitSHA      = "0000000000000000000000000000000000000000"
)

type GitHubHandler struct {
	Client    client.Client
	APIReader client.Reader
	Logger    *slog.Logger
}

type payload struct {
	Action       string `json:"action"`
	Installation struct {
		ID int64 `json:"id"`
	} `json:"installation"`
	Repository struct {
		ID            int64  `json:"id"`
		Name          string `json:"name"`
		DefaultBranch string `json:"default_branch"`
		Owner         struct {
			Login string `json:"login"`
		} `json:"owner"`
	} `json:"repository"`
	After       string `json:"after"`
	Deleted     bool   `json:"deleted"`
	Ref         string `json:"ref"`
	PullRequest struct {
		Number         int64  `json:"number"`
		MergeCommitSHA string `json:"merge_commit_sha"`
		State          string `json:"state"`
		Merged         bool   `json:"merged"`
		Mergeable      *bool  `json:"mergeable"`
		Head           struct {
			Ref        string `json:"ref"`
			Repository struct {
				ID int64 `json:"id"`
			} `json:"repo"`
		} `json:"head"`
		Base struct {
			Ref string `json:"ref"`
		} `json:"base"`
	} `json:"pull_request"`
	MergeGroup struct {
		HeadSHA string `json:"head_sha"`
		HeadRef string `json:"head_ref"`
		BaseRef string `json:"base_ref"`
	} `json:"merge_group"`
}

type normalizedEvent struct {
	Name       string `json:"name"`
	Action     string `json:"action,omitempty"`
	SHA        string `json:"sha,omitempty"`
	Ref        string `json:"ref"`
	HeadRef    string `json:"headRef,omitempty"`
	BaseRef    string `json:"baseRef,omitempty"`
	ResolveRef string `json:"resolveRef,omitempty"`
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
		if pullRequest.Number < 1 || pullRequest.Head.Ref == "" || pullRequest.Base.Ref == "" {
			return normalizedEvent{}, false, errors.New("GitHub pull request event is incomplete")
		}
		if pullRequest.Head.Repository.ID == 0 || pullRequest.Head.Repository.ID != event.Repository.ID {
			return normalizedEvent{}, false, nil
		}
		if pullRequest.Merged && (pullRequest.State != "closed" || event.Action != "closed") {
			return normalizedEvent{}, false, errors.New("GitHub pull request event has an invalid merged state")
		}
		if !pullRequest.Merged && pullRequest.Mergeable != nil && !*pullRequest.Mergeable {
			return result, false, nil
		}
		if pullRequest.Merged {
			if pullRequest.MergeCommitSHA == "" {
				return normalizedEvent{}, false, errors.New("GitHub merged pull request event does not identify a revision")
			}
			result.SHA = pullRequest.MergeCommitSHA
			result.Ref = "refs/heads/" + pullRequest.Base.Ref
		} else {
			result.Ref = "refs/pull/" + strconv.FormatInt(pullRequest.Number, 10) + "/merge"
			if pullRequest.MergeCommitSHA == "" {
				if pullRequest.State != "open" {
					return result, false, nil
				}
				result.ResolveRef = result.Ref
			} else {
				result.SHA = pullRequest.MergeCommitSHA
			}
		}
		result.HeadRef = pullRequest.Head.Ref
		result.BaseRef = pullRequest.Base.Ref
	case "merge_group":
		if event.Action != "checks_requested" {
			return result, false, nil
		}
		result.SHA = event.MergeGroup.HeadSHA
		result.Ref = event.MergeGroup.HeadRef
		result.BaseRef = githubclient.RefName(event.MergeGroup.BaseRef)
	default:
		return result, false, nil
	}
	if (result.SHA == "" && result.ResolveRef == "") || result.Ref == "" || githubclient.RefName(result.Ref) == "" {
		return normalizedEvent{}, false, errors.New("GitHub event does not identify a revision")
	}
	if result.SHA != "" && !validGitSHA(result.SHA) {
		return normalizedEvent{}, false, errors.New("GitHub event contains an invalid revision")
	}
	return result, true, nil
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
