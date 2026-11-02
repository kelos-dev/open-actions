package webhook

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/kelos-dev/open-actions/internal/eventsnapshot"
	githubclient "github.com/kelos-dev/open-actions/internal/github"
	"github.com/kelos-dev/open-actions/internal/gitrepository"
	"github.com/kelos-dev/open-actions/internal/workflow"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const pathDiffTimeout = 30 * time.Second

func (r *DeliveryReconciler) changedFiles(ctx context.Context, installation *githubclient.InstallationClient, merged *gitrepository.Repository, object *corev1.ConfigMap, delivery *queuedDelivery, event normalizedEvent) (*workflow.ChangedFiles, error) {
	diffCtx, cancel := context.WithTimeout(ctx, pathDiffTimeout)
	defer cancel()
	files, err := r.diffFiles(diffCtx, installation, merged, object, delivery, event)
	if ctx.Err() == nil && errors.Is(diffCtx.Err(), context.DeadlineExceeded) {
		r.Logger.Info("path filter diff timed out", "delivery_id", delivery.DeliveryID)
		return &workflow.ChangedFiles{AlwaysRun: true}, nil
	}
	return files, err
}

func (r *DeliveryReconciler) diffFiles(ctx context.Context, installation *githubclient.InstallationClient, merged *gitrepository.Repository, object *corev1.ConfigMap, delivery *queuedDelivery, event normalizedEvent) (*workflow.ChangedFiles, error) {
	revision := gitrepository.DiffRevision{HeadSHA: event.SHA}
	if event.Name == "push" {
		if delivery.EventSnapshot == "" {
			return nil, errors.New("push path filtering requires a signed event snapshot")
		}
		snapshot := &corev1.Secret{}
		if err := r.APIReader.Get(ctx, client.ObjectKey{Namespace: object.Namespace, Name: delivery.EventSnapshot}, snapshot); err != nil {
			return nil, err
		}
		if !metav1.IsControlledBy(snapshot, object) {
			return nil, fmt.Errorf("GitHub event snapshot %q is not owned by delivery %q", snapshot.Name, object.Name)
		}
		if snapshot.Immutable == nil || !*snapshot.Immutable {
			return nil, fmt.Errorf("GitHub event snapshot %q for delivery %q must be immutable", snapshot.Name, object.Name)
		}
		push := struct {
			Before  string `json:"before"`
			Deleted bool   `json:"deleted"`
			Commits []struct {
				ID string `json:"id"`
			} `json:"commits"`
		}{}
		if err := json.Unmarshal(snapshot.Data[eventsnapshot.DataKey], &push); err != nil {
			return nil, fmt.Errorf("decode push event snapshot %q: %w", snapshot.Name, err)
		}
		if push.Deleted {
			return &workflow.ChangedFiles{}, nil
		}
		if len(push.Commits) > 1000 {
			return &workflow.ChangedFiles{AlwaysRun: true}, nil
		}
		revision.BaseSHA = push.Before
		if push.Before == zeroGitSHA {
			if len(push.Commits) == 0 {
				return &workflow.ChangedFiles{}, nil
			}
			revision.BaseSHA = push.Commits[0].ID
			revision.BaseParent = true
		}
	} else {
		if event.PullRequest == nil {
			return nil, errors.New("pull request path filtering requires pinned revision metadata")
		}
		revision.BaseSHA = event.MergeBaseSHA
		revision.HeadSHA = event.PullRequest.HeadSHA
		if revision.BaseSHA == "" {
			var err error
			revision.BaseSHA, err = installation.ResolveMergeBase(ctx, delivery.Repository.Owner, delivery.Repository.Name, event.PullRequest.BaseSHA, revision.HeadSHA)
			if err != nil {
				return nil, err
			}
		}
	}
	var paths []string
	var err error
	if merged != nil {
		paths, err = merged.ChangedFiles(ctx, revision.BaseSHA, revision.HeadSHA, workflow.MaxChangedFiles)
	} else {
		paths, err = r.GitRepository.ChangedFiles(ctx, delivery.Repository.Owner, delivery.Repository.Name, installation.Token(), revision, workflow.MaxChangedFiles)
	}
	if err != nil {
		return nil, err
	}
	return &workflow.ChangedFiles{Paths: paths}, nil
}
