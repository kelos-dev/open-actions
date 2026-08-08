package webhook

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"

	actionsv1alpha1 "github.com/kelos-dev/open-actions/api/v1alpha1"
	githubclient "github.com/kelos-dev/open-actions/internal/github"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
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

func TestGatewayForInstallationUsesConfiguredOwner(t *testing.T) {
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
	owner := &actionsv1alpha1.ActionsGateway{
		ObjectMeta: metav1.ObjectMeta{Name: "owner", Namespace: "trusted", Generation: 1},
		Spec: actionsv1alpha1.ActionsGatewaySpec{Source: actionsv1alpha1.ActionsGatewaySource{
			Type: actionsv1alpha1.SourceTypeGitHub, GitHub: githubConfiguration.DeepCopy(),
		}},
		Status: actionsv1alpha1.ActionsGatewayStatus{Conditions: []metav1.Condition{{
			Type: actionsv1alpha1.ActionsGatewayConditionConfigured, Status: metav1.ConditionTrue, ObservedGeneration: 1,
		}}},
	}
	duplicate := owner.DeepCopy()
	duplicate.Name = "duplicate"
	duplicate.Namespace = "untrusted"
	duplicate.Status.Conditions[0].Status = metav1.ConditionFalse
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "github", Namespace: owner.Namespace}, Data: map[string][]byte{"webhook-secret": []byte("secret")}}
	clusterClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(owner, duplicate, secret).Build()
	handler := &GitHubHandler{Client: clusterClient, APIReader: clusterClient}

	selected, webhookSecret, err := handler.gatewayForInstallation(context.Background(), 42)
	if err != nil {
		t.Fatal(err)
	}
	if selected.Namespace != owner.Namespace || selected.Name != owner.Name || string(webhookSecret) != "secret" {
		t.Fatalf("selected gateway = %s/%s", selected.Namespace, selected.Name)
	}
}

func TestNormalizePullRequestRevisions(t *testing.T) {
	mergeable := true
	conflicted := false
	for _, tt := range []struct {
		name        string
		action      string
		state       string
		merged      bool
		mergeable   *bool
		sha         string
		wantSupport bool
		wantRef     string
		wantRefName string
		wantResolve string
		wantError   bool
	}{
		{name: "open mergeable", action: "synchronize", state: "open", mergeable: &mergeable, sha: strings.Repeat("a", 40), wantSupport: true, wantRef: "refs/pull/42/merge", wantRefName: "42/merge"},
		{name: "open conflicted", action: "synchronize", state: "open", mergeable: &conflicted, sha: strings.Repeat("b", 40)},
		{name: "open merge result unavailable", action: "opened", state: "open", mergeable: &mergeable, wantSupport: true, wantRef: "refs/pull/42/merge", wantRefName: "42/merge", wantResolve: "refs/pull/42/merge"},
		{name: "closed unmerged", action: "closed", state: "closed", mergeable: &mergeable, sha: strings.Repeat("c", 40), wantSupport: true, wantRef: "refs/pull/42/merge", wantRefName: "42/merge"},
		{name: "closed unmerged without revision", action: "closed", state: "closed", mergeable: &mergeable},
		{name: "closed merged", action: "closed", state: "closed", merged: true, mergeable: &conflicted, sha: strings.Repeat("d", 40), wantSupport: true, wantRef: "refs/heads/main", wantRefName: "main"},
		{name: "merged payload with non-closed activity", action: "synchronize", state: "closed", merged: true, mergeable: &conflicted, sha: strings.Repeat("e", 40), wantError: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			event := &payload{Action: tt.action}
			event.PullRequest.Number = 42
			event.PullRequest.State = tt.state
			event.PullRequest.Merged = tt.merged
			event.PullRequest.Mergeable = tt.mergeable
			event.PullRequest.MergeCommitSHA = tt.sha
			event.Repository.ID = 1
			event.PullRequest.Head.Ref = "feature"
			event.PullRequest.Head.Repository.ID = event.Repository.ID
			event.PullRequest.Base.Ref = "main"
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
			if normalized.Ref != tt.wantRef || githubclient.RefName(normalized.Ref) != tt.wantRefName || normalized.HeadRef != "feature" || normalized.BaseRef != "main" || normalized.SHA != tt.sha || normalized.ResolveRef != tt.wantResolve {
				t.Errorf("normalized event = %#v", normalized)
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

func TestNormalizeRejectsUntrustedPullRequestHeads(t *testing.T) {
	mergeable := true
	for _, headRepositoryID := range []int64{0, 2} {
		event := &payload{Action: "synchronize"}
		event.Repository.ID = 1
		event.PullRequest.Number = 42
		event.PullRequest.State = "open"
		event.PullRequest.Mergeable = &mergeable
		event.PullRequest.MergeCommitSHA = strings.Repeat("a", 40)
		event.PullRequest.Head.Ref = "feature"
		event.PullRequest.Head.Repository.ID = headRepositoryID
		event.PullRequest.Base.Ref = "main"

		_, supported, err := normalize("pull_request", event)
		if err != nil {
			t.Fatal(err)
		}
		if supported {
			t.Fatalf("normalize() accepted pull request head repository %d", headRepositoryID)
		}
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
