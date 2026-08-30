package webhook

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	actionsv1alpha1 "github.com/kelos-dev/open-actions/api/v1alpha1"
	"github.com/kelos-dev/open-actions/internal/eventsnapshot"
	githubclient "github.com/kelos-dev/open-actions/internal/github"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestValidSignature(t *testing.T) {
	body := []byte(`{"zen":"Keep it logically awesome."}`)
	secret := []byte("secret")
	digest := hmac.New(sha256.New, secret)
	digest.Write(body)
	signature := "sha256=" + hex.EncodeToString(digest.Sum(nil))
	if !validSignature(body, secret, signature) {
		t.Fatal("valid signature was rejected")
	}
	if validSignature(body, secret, signature[:len(signature)-1]+"0") {
		t.Fatal("invalid signature was accepted")
	}
}

func TestGitHubHandlerRejectsOversizedSnapshot(t *testing.T) {
	handler := &GitHubHandler{}
	request := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(make([]byte, maxPayloadBytes+1)))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("response status = %d, want %d", response.Code, http.StatusRequestEntityTooLarge)
	}
}

func TestNormalizePreservesWebhookActor(t *testing.T) {
	event := &payload{Ref: "refs/heads/main", After: strings.Repeat("a", 40)}
	event.Sender.Login = "octocat"
	normalized, supported, err := normalize("push", event)
	if err != nil || !supported {
		t.Fatalf("normalize() = %#v, supported %t, error %v", normalized, supported, err)
	}
	if normalized.Actor != "octocat" {
		t.Fatalf("actor = %q, want octocat", normalized.Actor)
	}
}

func TestProjectForInstallationUsesConfiguredOwner(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := actionsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	githubConfiguration := &actionsv1alpha1.GitHubAppConfiguration{
		InstallationID: 42,
		WebhookSecretRef: corev1.SecretKeySelector{
			LocalObjectReference: corev1.LocalObjectReference{Name: "github"},
			Key:                  "webhook-secret",
		},
	}
	owner := &actionsv1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: "owner", Namespace: "trusted", Generation: 1},
		Spec: actionsv1alpha1.ProjectSpec{Source: actionsv1alpha1.ProjectSource{
			Type: actionsv1alpha1.SourceTypeGitHub, GitHub: githubConfiguration.DeepCopy(),
		}},
		Status: actionsv1alpha1.ProjectStatus{Conditions: []metav1.Condition{{
			Type: actionsv1alpha1.ProjectConditionConfigured, Status: metav1.ConditionTrue, ObservedGeneration: 1,
		}}},
	}
	duplicate := owner.DeepCopy()
	duplicate.Name = "duplicate"
	duplicate.Namespace = "untrusted"
	duplicate.Status.Conditions[0].Status = metav1.ConditionFalse
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "github", Namespace: owner.Namespace}, Data: map[string][]byte{"webhook-secret": []byte("secret")}}
	clusterClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(owner, duplicate, secret).Build()
	handler := &GitHubHandler{Client: clusterClient, APIReader: clusterClient}

	selected, webhookSecret, err := handler.projectForInstallation(context.Background(), 42)
	if err != nil {
		t.Fatal(err)
	}
	if selected.Namespace != owner.Namespace || selected.Name != owner.Name || string(webhookSecret) != "secret" {
		t.Fatalf("selected project = %s/%s", selected.Namespace, selected.Name)
	}
}

func TestNormalizePullRequestRevisions(t *testing.T) {
	mergeable := true
	conflicted := false
	headSHA := strings.Repeat("f", 40)
	for _, tt := range []struct {
		name        string
		action      string
		state       string
		merged      bool
		mergeable   *bool
		mergeSHA    string
		wantSupport bool
		wantRef     string
		wantRefName string
		wantSHA     string
		wantResolve string
		wantHeadSHA string
		wantError   bool
	}{
		{name: "open mergeable pins integration inputs", action: "synchronize", state: "open", mergeable: &mergeable, mergeSHA: strings.Repeat("a", 40), wantSupport: true, wantRef: "refs/pull/42/merge", wantRefName: "42/merge", wantHeadSHA: headSHA},
		{name: "open conflicted runs trusted workflow", action: "synchronize", state: "open", mergeable: &conflicted, mergeSHA: strings.Repeat("b", 40), wantSupport: true, wantRef: "refs/pull/42/merge", wantRefName: "42/merge"},
		{name: "open merge result unavailable", action: "opened", state: "open", mergeable: &mergeable, wantSupport: true, wantRef: "refs/pull/42/merge", wantRefName: "42/merge", wantHeadSHA: headSHA},
		{name: "closed unmerged", action: "closed", state: "closed", mergeable: &mergeable, mergeSHA: strings.Repeat("c", 40), wantSupport: true, wantRef: "refs/pull/42/merge", wantRefName: "42/merge", wantSHA: strings.Repeat("c", 40)},
		{name: "closed unmerged non-closed activity", action: "labeled", state: "closed", mergeable: &mergeable, mergeSHA: strings.Repeat("c", 40), wantSupport: true, wantRef: "refs/pull/42/merge", wantRefName: "42/merge", wantSHA: strings.Repeat("c", 40)},
		{name: "closed unmerged without revision runs trusted workflow", action: "closed", state: "closed", mergeable: &mergeable, wantSupport: true, wantRef: "refs/pull/42/merge", wantRefName: "42/merge"},
		{name: "closed merged", action: "closed", state: "closed", merged: true, mergeable: &conflicted, mergeSHA: strings.Repeat("d", 40), wantSupport: true, wantRef: "refs/heads/main", wantRefName: "main", wantSHA: strings.Repeat("d", 40)},
		{name: "merged payload with non-closed activity", action: "synchronize", state: "closed", merged: true, mergeable: &conflicted, mergeSHA: strings.Repeat("e", 40), wantError: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			event := &payload{Action: tt.action}
			event.PullRequest.Number = 42
			event.PullRequest.Body = "Pull request body"
			event.PullRequest.HTMLURL = "https://github.com/acme/example/pull/42"
			event.PullRequest.State = tt.state
			event.PullRequest.Merged = tt.merged
			event.PullRequest.Mergeable = tt.mergeable
			event.PullRequest.MergeCommitSHA = tt.mergeSHA
			event.Repository.ID = 1
			event.PullRequest.Head.Ref = "feature"
			event.PullRequest.Head.SHA = headSHA
			event.PullRequest.Head.Repository.ID = event.Repository.ID
			event.PullRequest.Head.Repository.Owner.Login = "acme"
			event.PullRequest.Head.Repository.Name = "example"
			event.PullRequest.Base.Ref = "main"
			event.PullRequest.Base.SHA = strings.Repeat("9", 40)
			normalized, supported, err := normalize("pull_request", event)
			if tt.wantError {
				if err == nil {
					t.Fatal("normalize() accepted a merged pull request with a non-closed activity")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if supported != tt.wantSupport {
				t.Fatalf("supported = %v, want %v", supported, tt.wantSupport)
			}
			if !supported {
				return
			}
			if normalized.Ref != tt.wantRef || githubclient.RefName(normalized.Ref) != tt.wantRefName || normalized.HeadRef != "feature" || normalized.BaseRef != "main" || normalized.SHA != tt.wantSHA || normalized.ResolveRef != tt.wantResolve || normalized.HeadSHA != tt.wantHeadSHA {
				t.Errorf("normalized event = %#v", normalized)
			}
			if normalized.PullRequest == nil || normalized.PullRequest.Number != 42 || normalized.PullRequest.Body != "Pull request body" || normalized.PullRequest.HTMLURL != "https://github.com/acme/example/pull/42" || normalized.PullRequest.HeadRef != "feature" || normalized.PullRequest.HeadSHA != headSHA || normalized.PullRequest.BaseRef != "main" || normalized.PullRequest.BaseSHA != strings.Repeat("9", 40) || normalized.PullRequest.HeadRepository.Owner != "acme" || normalized.PullRequest.HeadRepository.Name != "example" {
				t.Errorf("normalized pull request = %#v", normalized.PullRequest)
			}
		})
	}
}

func TestNormalizePullRequestRequiresValidRevisions(t *testing.T) {
	mergeable := true
	validSHA := strings.Repeat("9", 40)
	for _, field := range []struct {
		name string
		set  func(*payload, string)
	}{
		{name: "head", set: func(event *payload, value string) { event.PullRequest.Head.SHA = value }},
		{name: "base", set: func(event *payload, value string) { event.PullRequest.Base.SHA = value }},
	} {
		for _, revision := range []string{"", "not-a-sha", strings.Repeat("A", 40), zeroGitSHA} {
			event := &payload{Action: "synchronize"}
			event.Repository.ID = 1
			event.PullRequest.Number = 42
			event.PullRequest.HTMLURL = "https://github.com/acme/example/pull/42"
			event.PullRequest.State = "open"
			event.PullRequest.Mergeable = &mergeable
			event.PullRequest.Head.Ref = "feature"
			event.PullRequest.Head.SHA = validSHA
			event.PullRequest.Head.Repository.ID = event.Repository.ID
			event.PullRequest.Head.Repository.Owner.Login = "acme"
			event.PullRequest.Head.Repository.Name = "example"
			event.PullRequest.Base.Ref = "main"
			event.PullRequest.Base.SHA = validSHA
			field.set(event, revision)

			if _, _, err := normalize("pull_request", event); err == nil {
				t.Fatalf("normalize() accepted %s revision %q", field.name, revision)
			}
		}
	}
}

func TestNormalizeSkipsPullRequestEventsWithoutHeadRepository(t *testing.T) {
	for _, eventName := range []string{"pull_request", "pull_request_review", "pull_request_review_comment"} {
		t.Run(eventName, func(t *testing.T) {
			event := &payload{Action: "synchronize"}
			event.Repository.ID = 1
			event.Repository.DefaultBranch = "main"
			event.PullRequest.Number = 42
			event.PullRequest.HTMLURL = "https://github.com/acme/example/pull/42"
			event.PullRequest.State = "open"
			event.PullRequest.Head.Ref = "feature"
			event.PullRequest.Head.SHA = strings.Repeat("a", 40)
			event.PullRequest.Base.Ref = "main"
			_, supported, err := normalize(eventName, event)
			if err != nil || supported {
				t.Fatalf("normalize(%q) supported = %v, error = %v", eventName, supported, err)
			}
		})
	}
}

func TestNormalizeMergeGroupActions(t *testing.T) {
	for _, tt := range []struct {
		action        string
		wantSupported bool
	}{
		{action: "checks_requested", wantSupported: true},
		{action: "destroyed"},
	} {
		event := &payload{Action: tt.action}
		event.MergeGroup.HeadSHA = strings.Repeat("a", 40)
		event.MergeGroup.HeadRef = "refs/heads/gh-readonly-queue/main/pr-1"
		event.MergeGroup.BaseRef = "refs/heads/main"
		_, supported, err := normalize("merge_group", event)
		if err != nil {
			t.Fatal(err)
		}
		if supported != tt.wantSupported {
			t.Errorf("action %q supported = %v, want %v", tt.action, supported, tt.wantSupported)
		}
	}
}

func TestNormalizeRemainingWebhookEvents(t *testing.T) {
	base := func() *payload {
		event := &payload{}
		event.Repository.DefaultBranch = "main"
		return event
	}
	for _, eventName := range []string{"issues", "issue_comment"} {
		event := base()
		event.Action = "opened"
		event.Issue.Number = 17
		event.Issue.Body = "Issue body"
		if eventName == "issue_comment" {
			event.Action = "created"
			event.Comment.Body = "/kind bug"
		}
		normalized, supported, err := normalize(eventName, event)
		if err != nil || !supported || normalized.Ref != "refs/heads/main" || normalized.ResolveRef != "main" || normalized.Issue == nil || normalized.Issue.Number != 17 || normalized.Issue.Body != "Issue body" || (eventName == "issue_comment" && (normalized.Comment == nil || normalized.Comment.Body != "/kind bug")) {
			t.Fatalf("normalize(%q) = %#v, supported %v, error %v", eventName, normalized, supported, err)
		}
	}

	workflowRun := base()
	workflowRun.Action = "completed"
	workflowRun.WorkflowRun.Name = "Release"
	workflowRun.WorkflowRun.HeadBranch = "main"
	workflowRun.WorkflowRun.HeadSHA = strings.Repeat("a", 40)
	workflowRun.WorkflowRun.Conclusion = "success"
	normalized, supported, err := normalize("workflow_run", workflowRun)
	if err != nil || !supported || normalized.WorkflowName != "Release" || normalized.BaseRef != "main" || normalized.ResolveRef != "main" || normalized.WorkflowRun == nil || normalized.WorkflowRun.Conclusion != "success" || normalized.WorkflowRun.HeadSHA != strings.Repeat("a", 40) {
		t.Fatalf("workflow_run normalized = %#v, supported %v, error %v", normalized, supported, err)
	}

	release := base()
	release.Action = "published"
	release.Release.TagName = "v1.2.3"
	normalized, supported, err = normalize("release", release)
	if err != nil || !supported || normalized.Ref != "refs/tags/v1.2.3" || normalized.ResolveRef != "refs/tags/v1.2.3" {
		t.Fatalf("release normalized = %#v, supported %v, error %v", normalized, supported, err)
	}
	release.Release.Draft = true
	if _, supported, err := normalize("release", release); err != nil || supported {
		t.Fatalf("draft release supported = %v, error = %v", supported, err)
	}
}

func TestNormalizeSkipsUnsupportedActivityTypes(t *testing.T) {
	event := &payload{Action: "future_activity"}
	event.Repository.DefaultBranch = "main"
	event.Issue.Number = 17
	_, supported, err := normalize("issues", event)
	if err != nil || supported {
		t.Fatalf("unsupported activity supported = %v, error = %v", supported, err)
	}
}

func TestNormalizeRerunAcceptsOnlyProjectCheckRuns(t *testing.T) {
	project := &actionsv1alpha1.Project{Spec: actionsv1alpha1.ProjectSpec{Source: actionsv1alpha1.ProjectSource{
		Type:   actionsv1alpha1.SourceTypeGitHub,
		GitHub: &actionsv1alpha1.GitHubAppConfiguration{AppID: 17},
	}}}
	event := &payload{Action: "rerequested"}
	event.CheckRun.ID = 42
	event.CheckRun.App.ID = 17
	event.CheckRun.ExternalID = "workflow-run-uid"
	event.CheckRun.HeadSHA = strings.Repeat("a", 40)
	event.Sender.Login = "octocat"

	rerun, supported, err := normalizeRerun(project, event)
	if err != nil || !supported || rerun.CheckRunID != 42 || rerun.RootRunUID != "workflow-run-uid" || rerun.HeadSHA != event.CheckRun.HeadSHA || rerun.TriggeringActor != "octocat" {
		t.Fatalf("normalized rerun = %#v, supported = %v, error = %v", rerun, supported, err)
	}
	event.Action = "completed"
	if _, supported, err := normalizeRerun(project, event); err != nil || supported {
		t.Fatalf("completed check run supported = %v, error = %v", supported, err)
	}
	event.Action = "rerequested"
	event.CheckRun.App.ID = 18
	if _, _, err := normalizeRerun(project, event); err == nil {
		t.Fatal("check run from another app was accepted")
	}
}

func TestGitHubHandlerQueuesCheckRerequestByDeliveryID(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := actionsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	project := &actionsv1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: "project", Namespace: "default", UID: "project-uid", Generation: 1},
		Spec: actionsv1alpha1.ProjectSpec{Source: actionsv1alpha1.ProjectSource{Type: actionsv1alpha1.SourceTypeGitHub, GitHub: &actionsv1alpha1.GitHubAppConfiguration{
			AppID: 17, InstallationID: 23,
			WebhookSecretRef: corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "github"}, Key: "webhook-secret"},
		}}},
		Status: actionsv1alpha1.ProjectStatus{Conditions: []metav1.Condition{{Type: actionsv1alpha1.ProjectConditionConfigured, Status: metav1.ConditionTrue, ObservedGeneration: 1}}},
	}
	secretValue := []byte("secret")
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "github", Namespace: project.Namespace}, Data: map[string][]byte{"webhook-secret": secretValue}}
	clusterClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(project, secret).Build()
	handler := &GitHubHandler{Client: clusterClient, APIReader: clusterClient, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	body := []byte(`{"action":"rerequested","installation":{"id":23},"repository":{"id":1,"name":"example","owner":{"login":"acme"}},"check_run":{"id":42,"head_sha":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","external_id":"root-uid","app":{"id":17}}}`)
	digest := hmac.New(sha256.New, secretValue)
	digest.Write(body)
	request := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
	request.Header.Set("X-GitHub-Event", "check_run")
	request.Header.Set("X-GitHub-Delivery", "delivery-123")
	request.Header.Set("X-Hub-Signature-256", "sha256="+hex.EncodeToString(digest.Sum(nil)))
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)
	if response.Code != http.StatusAccepted {
		t.Fatalf("response status = %d, body = %s", response.Code, response.Body.String())
	}
	queued := &corev1.ConfigMap{}
	key := client.ObjectKey{Namespace: project.Namespace, Name: webhookDeliveryName([]byte("delivery-123"))}
	if err := clusterClient.Get(context.Background(), key, queued); err != nil {
		t.Fatal(err)
	}
	delivery := &queuedDelivery{}
	if err := json.Unmarshal([]byte(queued.Data[deliveryDataKey]), delivery); err != nil {
		t.Fatal(err)
	}
	if delivery.Rerun == nil || delivery.Rerun.CheckRunID != 42 || delivery.Rerun.RootRunUID != "root-uid" {
		t.Fatalf("queued rerun = %#v", delivery.Rerun)
	}
}

func TestGitHubHandlerPreservesSupportedEventFixtures(t *testing.T) {
	for _, eventName := range []string{
		"push", "pull_request", "merge_group", "workflow_run", "issues", "issue_comment",
		"pull_request_review_comment", "pull_request_review", "release",
	} {
		t.Run(eventName, func(t *testing.T) {
			body, err := os.ReadFile(filepath.Join("testdata", "github", eventName+".json"))
			if err != nil {
				t.Fatal(err)
			}
			if eventName == "pull_request" {
				parsed := &payload{}
				if err := json.Unmarshal(body, parsed); err != nil {
					t.Fatal(err)
				}
				normalized, supported, err := normalize(eventName, parsed)
				if err != nil || !supported {
					t.Fatalf("normalize pull request fixture: supported %v, error %v", supported, err)
				}
				events := deliveryEvents(normalized)
				if len(events) != 2 || events[0].Name != "pull_request" || events[1].Name != "pull_request_target" {
					t.Fatalf("pull request fixture events = %#v", events)
				}
			}
			scheme := runtime.NewScheme()
			if err := actionsv1alpha1.AddToScheme(scheme); err != nil {
				t.Fatal(err)
			}
			if err := corev1.AddToScheme(scheme); err != nil {
				t.Fatal(err)
			}
			project := &actionsv1alpha1.Project{
				ObjectMeta: metav1.ObjectMeta{Name: "project", Namespace: "default", UID: "project-uid", Generation: 1},
				Spec: actionsv1alpha1.ProjectSpec{Source: actionsv1alpha1.ProjectSource{Type: actionsv1alpha1.SourceTypeGitHub, GitHub: &actionsv1alpha1.GitHubAppConfiguration{
					InstallationID:   2002,
					WebhookSecretRef: corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "github"}, Key: "webhook-secret"},
				}}},
				Status: actionsv1alpha1.ProjectStatus{Conditions: []metav1.Condition{{Type: actionsv1alpha1.ProjectConditionConfigured, Status: metav1.ConditionTrue, ObservedGeneration: 1}}},
			}
			webhookSecret := []byte("secret")
			credential := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "github", Namespace: project.Namespace}, Data: map[string][]byte{"webhook-secret": webhookSecret}}
			clusterClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(project, credential).Build()
			logs := &bytes.Buffer{}
			handler := &GitHubHandler{Client: clusterClient, APIReader: clusterClient, Logger: slog.New(slog.NewTextHandler(logs, nil))}
			digest := hmac.New(sha256.New, webhookSecret)
			digest.Write(body)
			request := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
			request.Header.Set("X-GitHub-Event", eventName)
			request.Header.Set("X-GitHub-Delivery", "delivery-"+eventName)
			request.Header.Set("X-Hub-Signature-256", "sha256="+hex.EncodeToString(digest.Sum(nil)))
			response := httptest.NewRecorder()

			handler.ServeHTTP(response, request)
			if response.Code != http.StatusAccepted {
				t.Fatalf("response status = %d, body = %s", response.Code, response.Body.String())
			}
			replayID := webhookReplayID(body)
			delivery := &corev1.ConfigMap{}
			if err := clusterClient.Get(context.Background(), client.ObjectKey{Namespace: project.Namespace, Name: webhookDeliveryName(body)}, delivery); err != nil {
				t.Fatal(err)
			}
			snapshot := &corev1.Secret{}
			if err := clusterClient.Get(context.Background(), client.ObjectKey{Namespace: project.Namespace, Name: eventSnapshotName(replayID)}, snapshot); err != nil {
				t.Fatal(err)
			}
			if snapshot.Immutable == nil || !*snapshot.Immutable || !metav1.IsControlledBy(snapshot, delivery) || !bytes.Equal(snapshot.Data[eventsnapshot.DataKey], body) {
				t.Fatalf("stored event snapshot = %#v", snapshot)
			}
			if strings.Contains(delivery.Data[deliveryDataKey], "private@example.com") || strings.Contains(logs.String(), "private@example.com") {
				t.Fatal("sensitive provider field appeared outside the event snapshot")
			}
		})
	}
}

func TestNormalizeRejectsOversizedEventMetadata(t *testing.T) {
	base := func() *payload {
		event := &payload{}
		event.Repository.DefaultBranch = "main"
		return event
	}
	for _, tt := range []struct {
		name      string
		eventName string
		mutate    func(*payload)
	}{
		{
			name: "issue body", eventName: "issues",
			mutate: func(event *payload) {
				event.Action = "opened"
				event.Issue.Number = 17
				event.Issue.Body = strings.Repeat("x", maxEventBodyLength+1)
			},
		},
		{
			name: "release tag", eventName: "release",
			mutate: func(event *payload) {
				event.Action = "published"
				event.Release.TagName = strings.Repeat("x", maxEventTagNameLength+1)
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			event := base()
			tt.mutate(event)
			if _, _, err := normalize(tt.eventName, event); err == nil {
				t.Fatal("normalize() accepted oversized event metadata")
			}
		})
	}
}

func TestNormalizePullRequestReviewEventsUseDefaultBranch(t *testing.T) {
	for _, tt := range []struct {
		name   string
		action string
	}{
		{name: "pull_request_review", action: "submitted"},
		{name: "pull_request_review_comment", action: "created"},
	} {
		event := &payload{Action: tt.action}
		event.Repository.ID = 1
		event.Repository.DefaultBranch = "main"
		event.PullRequest.Number = 42
		event.PullRequest.Body = "Pull request body"
		event.PullRequest.HTMLURL = "https://github.com/acme/example/pull/42"
		event.PullRequest.Head.Ref = "fork-feature"
		event.PullRequest.Head.SHA = strings.Repeat("a", 40)
		event.PullRequest.Head.Repository.ID = 2
		event.PullRequest.Head.Repository.Owner.Login = "contributor"
		event.PullRequest.Head.Repository.Name = "example"
		event.PullRequest.Base.Ref = "main"
		event.PullRequest.Base.SHA = strings.Repeat("b", 40)
		event.Comment.Body = "/kind bug"
		event.Review.Body = "/priority important-soon"
		normalized, supported, err := normalize(tt.name, event)
		if err != nil || !supported || normalized.Ref != "refs/heads/main" || normalized.ResolveRef != "main" || normalized.HeadSHA != "" || normalized.HeadRef != "" || normalized.SHA != "" || normalized.PullRequest == nil || normalized.PullRequest.Number != 42 || normalized.PullRequest.Body != "Pull request body" || normalized.PullRequest.HTMLURL != "https://github.com/acme/example/pull/42" {
			t.Fatalf("normalize(%q) = %#v, supported %v, error %v", tt.name, normalized, supported, err)
		}
		if tt.name == "pull_request_review" && (normalized.Review == nil || normalized.Review.Body != "/priority important-soon") {
			t.Fatalf("normalized review = %#v", normalized.Review)
		}
		if tt.name == "pull_request_review_comment" && (normalized.Comment == nil || normalized.Comment.Body != "/kind bug") {
			t.Fatalf("normalized comment = %#v", normalized.Comment)
		}
	}
}

func TestDeliveryEventsUsePullRequestBaseBranch(t *testing.T) {
	baseSHA := strings.Repeat("b", 40)
	headSHA := strings.Repeat("a", 40)
	event := normalizedEvent{
		Name: "pull_request", Action: "synchronize", Fork: true, MergeRevision: true, HeadSHA: headSHA,
		Ref: "refs/pull/42/merge", HeadRef: "feature", BaseRef: "release",
		PullRequest: &normalizedPullRequest{
			Number: 42, Body: "Pull request body", HTMLURL: "https://github.com/contributor/example/pull/42",
			HeadRef: "feature", HeadSHA: headSHA, BaseRef: "release", BaseSHA: baseSHA,
			HeadRepository: normalizedRepository{ID: 2, Owner: "contributor", Name: "example"},
		},
	}
	events := deliveryEvents(event)
	if len(events) != 2 || events[0].Name != "pull_request_target" || events[0].SHA != baseSHA || events[0].ResolveRef != "" || events[0].Ref != "refs/heads/release" || events[0].HeadRef != "feature" || events[0].PullRequest != event.PullRequest {
		t.Fatalf("pull request target event = %#v", events)
	}
	if events[1].Name != "pull_request" || events[1].HeadSHA != headSHA || events[1].Ref != "refs/pull/42/merge" {
		t.Fatalf("ordinary pull request event = %#v", events)
	}
}

func TestNormalizeRetainsUntrustedPullRequestIntegrationRevisions(t *testing.T) {
	mergeable := true
	for _, tt := range []struct {
		name             string
		headRepositoryID int64
		author           string
		wantSupported    bool
		wantFork         bool
		wantDependabot   bool
	}{
		{name: "deleted head repository"},
		{name: "fork", headRepositoryID: 2, wantSupported: true, wantFork: true},
		{name: "Dependabot", headRepositoryID: 1, author: "dependabot[bot]", wantSupported: true, wantFork: true, wantDependabot: true},
		{name: "same repository", headRepositoryID: 1, author: "contributor", wantSupported: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			event := &payload{Action: "synchronize"}
			event.Repository.ID = 1
			event.PullRequest.Number = 42
			event.PullRequest.HTMLURL = "https://github.com/acme/example/pull/42"
			event.PullRequest.State = "open"
			event.PullRequest.Mergeable = &mergeable
			event.PullRequest.MergeCommitSHA = strings.Repeat("a", 40)
			event.PullRequest.User.Login = tt.author
			event.PullRequest.Head.Ref = "feature"
			event.PullRequest.Head.SHA = strings.Repeat("b", 40)
			event.PullRequest.Head.Repository.ID = tt.headRepositoryID
			event.PullRequest.Head.Repository.Owner.Login = "contributor"
			event.PullRequest.Head.Repository.Name = "example"
			event.PullRequest.Base.Ref = "main"
			event.PullRequest.Base.SHA = strings.Repeat("9", 40)

			normalized, supported, err := normalize("pull_request", event)
			if err != nil {
				t.Fatal(err)
			}
			if supported != tt.wantSupported || normalized.Fork != tt.wantFork || normalized.Dependabot != tt.wantDependabot {
				t.Fatalf("normalized event = %#v, supported = %v", normalized, supported)
			}
			if supported && (!normalized.MergeRevision || normalized.HeadSHA != event.PullRequest.Head.SHA) {
				t.Fatalf("integration revision = %#v", normalized)
			}
		})
	}
}

func TestNormalizePushRefs(t *testing.T) {
	for _, tt := range []struct {
		name        string
		after       string
		deleted     bool
		wantSHA     string
		wantResolve string
	}{
		{name: "created", after: strings.Repeat("a", 40), wantSHA: strings.Repeat("a", 40)},
		{name: "updated", after: strings.Repeat("b", 40), wantSHA: strings.Repeat("b", 40)},
		{name: "deleted", after: zeroGitSHA, deleted: true, wantResolve: "main"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			event := &payload{After: tt.after, Ref: "refs/heads/feature", Deleted: tt.deleted}
			event.Repository.DefaultBranch = "main"
			normalized, supported, err := normalize("push", event)
			if err != nil {
				t.Fatal(err)
			}
			if !supported || normalized.Ref != event.Ref || githubclient.RefName(normalized.Ref) != "feature" || normalized.SHA != tt.wantSHA || normalized.ResolveRef != tt.wantResolve {
				t.Errorf("normalized event = %#v, supported = %v", normalized, supported)
			}
		})
	}
}

func TestNormalizeRejectsZeroPushRevisionWithoutDeletion(t *testing.T) {
	event := &payload{After: zeroGitSHA, Ref: "refs/heads/feature"}
	if _, _, err := normalize("push", event); err == nil {
		t.Fatal("normalize() accepted a zero revision for a non-deletion push")
	}
}
