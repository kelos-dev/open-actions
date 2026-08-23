package controller

import (
	"errors"
	"net/http"
	"testing"
	"time"

	actionsv1alpha1 "github.com/kelos-dev/open-actions/api/v1alpha1"
	githubclient "github.com/kelos-dev/open-actions/internal/github"
	ctrl "sigs.k8s.io/controller-runtime"
)

func TestRequeueAfterGitHubRateLimit(t *testing.T) {
	now := time.Date(2026, time.August, 23, 12, 0, 0, 0, time.UTC)
	result, err := requeueAfterGitHubRateLimit(t.Context(), ctrl.Result{}, &githubclient.APIError{
		StatusCode: http.StatusTooManyRequests,
		Status:     "429 Too Many Requests",
		RetryAfter: 30 * time.Second,
	}, now)
	if err != nil || result.Requeue || result.RequeueAfter != 30*time.Second {
		t.Fatalf("rate-limit result = %#v, %v", result, err)
	}

	wantResult := ctrl.Result{RequeueAfter: 5 * time.Second}
	wantError := errors.New("GitHub unavailable")
	result, err = requeueAfterGitHubRateLimit(t.Context(), wantResult, wantError, now)
	if result != wantResult || !errors.Is(err, wantError) {
		t.Fatalf("ordinary result = %#v, %v", result, err)
	}

	rateLimitError := &githubclient.APIError{StatusCode: http.StatusTooManyRequests, RetryAfter: 30 * time.Second}
	joinedError := errors.Join(rateLimitError, wantError)
	result, err = requeueAfterGitHubRateLimit(t.Context(), wantResult, joinedError, now)
	if result != wantResult || !errors.Is(err, rateLimitError) || !errors.Is(err, wantError) {
		t.Fatalf("mixed joined result = %#v, %v", result, err)
	}
}

func TestGitHubCheckRateLimitIsRetryable(t *testing.T) {
	now := time.Date(2026, time.August, 23, 12, 0, 0, 0, time.UTC)
	reconciler := &WorkflowRunReconciler{Now: func() time.Time { return now }}
	err := &githubclient.APIError{
		StatusCode: http.StatusForbidden,
		Status:     "403 Forbidden",
		RetryAfter: 30 * time.Second,
	}
	if reconciler.githubCheckReportPermanentlyUnavailable(t.Context(), &actionsv1alpha1.WorkflowRun{}, err) {
		t.Fatal("rate-limited GitHub Check report was treated as permanently unavailable")
	}
}
