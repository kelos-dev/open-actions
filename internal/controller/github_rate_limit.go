package controller

import (
	"context"
	"time"

	githubclient "github.com/kelos-dev/open-actions/internal/github"
	ctrl "sigs.k8s.io/controller-runtime"
)

func requeueAfterGitHubRateLimit(ctx context.Context, result ctrl.Result, err error, now time.Time) (ctrl.Result, error) {
	delay, limited := githubclient.RetryDelay(err, now)
	if !limited {
		return result, err
	}
	ctrl.LoggerFrom(ctx).Info("Deferring reconciliation for GitHub API rate limit", "retry_after", delay, "error", err)
	return ctrl.Result{RequeueAfter: delay}, nil
}
