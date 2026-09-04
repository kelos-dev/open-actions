package webhook

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base32"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	actionsv1alpha1 "github.com/kelos-dev/open-actions/api/v1alpha1"
	"github.com/kelos-dev/open-actions/internal/eventsnapshot"
	githubclient "github.com/kelos-dev/open-actions/internal/github"
	"github.com/kelos-dev/open-actions/internal/gitrepository"
	actionmetrics "github.com/kelos-dev/open-actions/internal/metrics"
	"github.com/kelos-dev/open-actions/internal/workflow"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

var digestEncoding = base32.StdEncoding.WithPadding(base32.NoPadding)

const (
	deliveryLabel             = "actions.kelos.dev/webhook-delivery"
	deliveryDataKey           = "delivery.json"
	deliveryRevisionKey       = "resolvedRevision"
	deliveryMergeBaseKey      = "resolvedMergeBase"
	deliveryTargetRevisionKey = "resolvedTargetRevision"
	deliveryStateKey          = "state"
	deliveryMessageKey        = "message"
	deliveryRunCountKey       = "workflowRuns"
	deliveryFinishedKey       = "finishedAt"
	deliveryStateCompleted    = "Completed"
	deliveryStateFailed       = "Failed"
	maxDeliveryBytes          = 900_000
	maxWorkflowFiles          = 100
	maxWorkflowJobs           = 1000
	resourceNameMaxLength     = 63
	workflowRunDigestLength   = 20
	deliveryRetention         = 24 * time.Hour
	maxConcurrentDeliveries   = 4
)

type queuedDelivery struct {
	ProjectName   string             `json:"projectName"`
	ProjectUID    string             `json:"projectUID"`
	Repository    deliveryRepository `json:"repository"`
	Event         normalizedEvent    `json:"event"`
	EventSnapshot string             `json:"eventSnapshot,omitempty"`
	ReplayID      string             `json:"replayID"`
	DeliveryID    string             `json:"deliveryID"`
}

type deliveryRepository struct {
	ID    int64  `json:"id"`
	Owner string `json:"owner"`
	Name  string `json:"name"`
}

func (delivery *queuedDelivery) UnmarshalJSON(data []byte) error {
	document := struct {
		ProjectName   string             `json:"projectName"`
		ProjectUID    string             `json:"projectUID"`
		Repository    deliveryRepository `json:"repository"`
		Payload       *payload           `json:"payload"`
		Event         normalizedEvent    `json:"event"`
		EventSnapshot string             `json:"eventSnapshot,omitempty"`
		ReplayID      string             `json:"replayID"`
		DeliveryID    string             `json:"deliveryID"`
	}{}
	if err := json.Unmarshal(data, &document); err != nil {
		return err
	}
	repositoryFromPayload := document.Repository.ID == 0 && document.Payload != nil
	if repositoryFromPayload {
		document.Repository = deliveryRepository{
			ID: document.Payload.Repository.ID, Owner: document.Payload.Repository.Owner.Login,
			Name: document.Payload.Repository.Name,
		}
	}
	if document.Repository.ID < 1 || document.Repository.Owner == "" || document.Repository.Name == "" {
		return errors.New("delivery repository is incomplete")
	}
	if repositoryFromPayload && document.Event.Name == "pull_request" && (document.Event.ResolveRef != "" || document.Event.SHA != "") {
		document.Event.MergeRevision = true
	}
	*delivery = queuedDelivery{
		ProjectName: document.ProjectName, ProjectUID: document.ProjectUID, Repository: document.Repository,
		Event: document.Event, EventSnapshot: document.EventSnapshot, ReplayID: document.ReplayID, DeliveryID: document.DeliveryID,
	}
	return nil
}

type DeliveryReconciler struct {
	client.Client
	APIReader                          client.Reader
	GitHub                             *githubclient.Client
	GitRepository                      *gitrepository.Client
	Logger                             *slog.Logger
	WorkflowRunTTLSecondsAfterFinished *int32
	Now                                func() time.Time
	Metrics                            actionmetrics.DurationRecorder
	workflowCacheOnce                  sync.Once
	workflowCache                      *workflowRevisionCache
}

type workflowSelection struct {
	Path  string
	Event normalizedEvent
}

func (h *GitHubHandler) enqueueDelivery(ctx context.Context, project *actionsv1alpha1.Project, event *payload, normalized normalizedEvent, deliveryID string, signedBody []byte) error {
	replayID := webhookReplayID(signedBody)
	delivery := queuedDelivery{
		ProjectName: project.Name,
		ProjectUID:  string(project.UID),
		Repository: deliveryRepository{
			ID: event.Repository.ID, Owner: event.Repository.Owner.Login, Name: event.Repository.Name,
		},
		Event:         normalized,
		EventSnapshot: eventSnapshotName(replayID),
		ReplayID:      replayID,
		DeliveryID:    deliveryID,
	}
	return h.enqueueQueuedDelivery(ctx, project, delivery, signedBody, signedBody)
}

func (h *GitHubHandler) enqueueQueuedDelivery(ctx context.Context, project *actionsv1alpha1.Project, delivery queuedDelivery, identity, eventSnapshot []byte) error {
	data, err := json.Marshal(delivery)
	if err != nil {
		return err
	}
	if len(data) > maxDeliveryBytes {
		return fmt.Errorf("normalized webhook delivery exceeds %d bytes", maxDeliveryBytes)
	}
	object := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{
		Name:      webhookDeliveryName(identity),
		Namespace: project.Namespace,
		Labels:    map[string]string{deliveryLabel: "true"},
	}, Data: map[string]string{deliveryDataKey: string(data)}}
	if err := controllerutil.SetControllerReference(project, object, h.Client.Scheme()); err != nil {
		return err
	}
	stored := object
	if err := h.Client.Create(ctx, object); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return err
		}
		existing := &corev1.ConfigMap{}
		if err := h.APIReader.Get(ctx, client.ObjectKeyFromObject(object), existing); err != nil {
			return err
		}
		existingDelivery := queuedDelivery{}
		decodeErr := json.Unmarshal([]byte(existing.Data[deliveryDataKey]), &existingDelivery)
		existingDelivery.DeliveryID = delivery.DeliveryID
		if !metav1.IsControlledBy(existing, project) || decodeErr != nil || !apiequality.Semantic.DeepEqual(existingDelivery, delivery) {
			return apierrors.NewConflict(corev1.Resource("configmaps"), object.Name, errors.New("existing webhook delivery does not match the signed payload"))
		}
		stored = existing
	}
	if eventSnapshot != nil {
		return h.ensureEventSnapshot(ctx, stored, delivery.EventSnapshot, eventSnapshot)
	}
	return nil
}

func (h *GitHubHandler) ensureEventSnapshot(ctx context.Context, delivery *corev1.ConfigMap, name string, data []byte) error {
	if _, err := eventsnapshot.Decode(data); err != nil {
		return err
	}
	immutable := true
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: delivery.Namespace},
		Immutable:  &immutable,
		Data:       map[string][]byte{eventsnapshot.DataKey: data},
	}
	if err := controllerutil.SetControllerReference(delivery, secret, h.Client.Scheme()); err != nil {
		return err
	}
	if err := h.Client.Create(ctx, secret); err == nil {
		return nil
	} else if !apierrors.IsAlreadyExists(err) {
		return err
	}
	existing := &corev1.Secret{}
	if err := h.APIReader.Get(ctx, client.ObjectKeyFromObject(secret), existing); err != nil {
		return err
	}
	if !metav1.IsControlledBy(existing, delivery) || existing.Immutable == nil || !*existing.Immutable || !bytes.Equal(existing.Data[eventsnapshot.DataKey], data) {
		return apierrors.NewConflict(corev1.Resource("secrets"), secret.Name, errors.New("existing GitHub event snapshot does not match the signed payload"))
	}
	return nil
}

func eventSnapshotName(replayID string) string {
	return "event-" + replayID
}

func webhookDeliveryName(body []byte) string {
	return "delivery-" + webhookReplayID(body)
}

func webhookReplayID(body []byte) string {
	digest := sha256.Sum256(body)
	return strings.ToLower(digestEncoding.EncodeToString(digest[:]))
}

func workflowFile(path string) bool {
	extension := filepath.Ext(path)
	return extension == ".yaml" || extension == ".yml"
}

func validateDeliveryFanOut(workflowFiles, workflowJobs int) error {
	if workflowFiles > maxWorkflowFiles {
		return fmt.Errorf("delivery contains %d workflow files; maximum is %d", workflowFiles, maxWorkflowFiles)
	}
	if workflowJobs > maxWorkflowJobs {
		return fmt.Errorf("matching workflows define %d jobs; maximum is %d", workflowJobs, maxWorkflowJobs)
	}
	return nil
}

func workflowRunName(workflowPath, projectUID, replayID string) string {
	base := strings.TrimSuffix(filepath.Base(workflowPath), filepath.Ext(workflowPath))
	base = sanitizeName(base)
	if base == "" {
		base = "workflow"
	}
	digest := sha256.Sum256([]byte(projectUID + "\x00" + replayID + "\x00" + workflowPath))
	suffix := strings.ToLower(digestEncoding.EncodeToString(digest[:]))[:workflowRunDigestLength]
	if len(base) > resourceNameMaxLength-len(suffix)-1 {
		base = strings.Trim(base[:resourceNameMaxLength-len(suffix)-1], "-")
	}
	return base + "-" + suffix
}

func fullDigestWorkflowRunName(workflowPath, replayID string) string {
	base := strings.TrimSuffix(filepath.Base(workflowPath), filepath.Ext(workflowPath))
	base = sanitizeName(base)
	if base == "" {
		base = "workflow"
	}
	digest := sha256.Sum256([]byte(replayID + "|" + workflowPath))
	suffix := strings.ToLower(digestEncoding.EncodeToString(digest[:]))
	if len(base) > resourceNameMaxLength-len(suffix)-1 {
		base = strings.Trim(base[:resourceNameMaxLength-len(suffix)-1], "-")
	}
	return base + "-" + suffix
}

func sanitizeName(value string) string {
	value = strings.ToLower(value)
	var builder strings.Builder
	lastDash := false
	for _, character := range value {
		valid := character >= 'a' && character <= 'z' || character >= '0' && character <= '9'
		if valid {
			builder.WriteRune(character)
			lastDash = false
		} else if !lastDash {
			builder.WriteByte('-')
			lastDash = true
		}
	}
	return strings.Trim(builder.String(), "-")
}

func missingWorkflowDirectory(err error) bool {
	apiError := &githubclient.APIError{}
	return errors.As(err, &apiError) && apiError.StatusCode == http.StatusNotFound && apiError.Message == "Not Found"
}

func unavailableDeliveryRevision(err error) bool {
	apiError := &githubclient.APIError{}
	if errors.As(err, &apiError) {
		return apiError.StatusCode == http.StatusNotFound ||
			apiError.StatusCode == http.StatusConflict && strings.EqualFold(strings.TrimSpace(apiError.Message), "Git Repository is empty.")
	}
	return strings.Contains(err.Error(), "GitHub returned no commits")
}

func (r *DeliveryReconciler) Reconcile(ctx context.Context, request ctrl.Request) (result ctrl.Result, reconcileErr error) {
	defer func() {
		if delay, limited := githubclient.RetryDelay(reconcileErr, r.now()); limited {
			ctrl.LoggerFrom(ctx).Info("Deferring reconciliation for GitHub API rate limit", "retry_after", delay, "error", reconcileErr)
			result, reconcileErr = ctrl.Result{RequeueAfter: delay}, nil
		}
	}()
	object := &corev1.ConfigMap{}
	if err := r.APIReader.Get(ctx, request.NamespacedName, object); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if object.Labels[deliveryLabel] != "true" {
		return ctrl.Result{}, nil
	}
	if object.Data[deliveryStateKey] != "" {
		return r.retain(ctx, object)
	}
	delivery := queuedDelivery{}
	if err := json.Unmarshal([]byte(object.Data[deliveryDataKey]), &delivery); err != nil {
		return ctrl.Result{}, r.finish(ctx, object, deliveryStateFailed, 0, fmt.Sprintf("decode delivery: %v", err))
	}
	if delivery.EventSnapshot != "" && delivery.EventSnapshot != eventSnapshotName(delivery.ReplayID) {
		return ctrl.Result{}, r.finish(ctx, object, deliveryStateFailed, 0, "delivery contains an invalid event snapshot reference")
	}
	if revision := object.Data[deliveryRevisionKey]; revision != "" {
		if !validGitSHA(revision) {
			return ctrl.Result{}, r.finish(ctx, object, deliveryStateFailed, 0, "delivery contains an invalid resolved revision")
		}
		delivery.Event.SHA = revision
	}
	if mergeBase := object.Data[deliveryMergeBaseKey]; mergeBase != "" {
		if !validGitSHA(mergeBase) {
			return ctrl.Result{}, r.finish(ctx, object, deliveryStateFailed, 0, "delivery contains an invalid resolved merge base")
		}
		delivery.Event.MergeBaseSHA = mergeBase
	}
	targetRevision := object.Data[deliveryTargetRevisionKey]
	if targetRevision != "" && !validGitSHA(targetRevision) {
		return ctrl.Result{}, r.finish(ctx, object, deliveryStateFailed, 0, "delivery contains an invalid resolved target revision")
	}
	reader := r.APIReader
	project := &actionsv1alpha1.Project{}
	if err := reader.Get(ctx, client.ObjectKey{Namespace: object.Namespace, Name: delivery.ProjectName}, project); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, r.finish(ctx, object, deliveryStateFailed, 0, "Project was deleted")
		}
		return ctrl.Result{}, err
	}
	if string(project.UID) != delivery.ProjectUID || !metav1.IsControlledBy(object, project) {
		return ctrl.Result{}, r.finish(ctx, object, deliveryStateFailed, 0, "Project was recreated")
	}
	githubConfig := project.Spec.Source.GitHub
	privateKey, err := readSecretValue(ctx, reader, project.Namespace, githubConfig.PrivateKeySecretRef)
	if err != nil {
		return ctrl.Result{}, err
	}
	installation, err := r.GitHub.CachedInstallation(ctx, githubConfig.AppID, githubConfig.InstallationID, privateKey, delivery.Repository.Name, githubclient.InstallationPermissions{"contents": "read"})
	if err != nil {
		return ctrl.Result{}, err
	}
	events := deliveryEvents(delivery.Event)
	for index := range events {
		if events[index].Name == "pull_request_target" && targetRevision != "" {
			events[index].SHA = targetRevision
		}
	}
	selections := make([]workflowSelection, 0)
	seenFiles := map[string]struct{}{}
	workflowJobs := 0
candidateLoop:
	for _, candidate := range events {
		if candidate.Name == "pull_request" && candidate.Fork {
			enabled, _ := forkPullRequestPolicy(project, candidate.Dependabot)
			if !enabled {
				continue
			}
		}
		var mergedRepository *gitrepository.Repository
		if candidate.Name == "pull_request" && candidate.HeadSHA != "" {
			if candidate.PullRequest == nil {
				return ctrl.Result{}, r.finish(ctx, object, deliveryStateFailed, 0, "pull request delivery does not contain revision metadata")
			}
			if candidate.MergeBaseSHA == "" {
				candidate.MergeBaseSHA, err = installation.ResolveMergeBase(ctx, delivery.Repository.Owner, delivery.Repository.Name, candidate.PullRequest.BaseSHA, candidate.PullRequest.HeadSHA)
				if err != nil {
					return ctrl.Result{}, err
				}
			}
			mergedRepository, err = r.GitRepository.Merge(ctx, delivery.Repository.Owner, delivery.Repository.Name, installation.Token(), gitrepository.Revision{
				BaseSHA: candidate.PullRequest.BaseSHA, HeadSHA: candidate.PullRequest.HeadSHA, MergeBaseSHA: candidate.MergeBaseSHA,
			})
			if err != nil {
				conflict := &gitrepository.ConflictError{}
				if errors.As(err, &conflict) {
					r.Logger.Info("skipped conflicting pull request revision", "delivery_id", delivery.DeliveryID, "head_sha", candidate.PullRequest.HeadSHA, "base_sha", candidate.PullRequest.BaseSHA)
					continue
				}
				return ctrl.Result{}, err
			}
			repositoryToClose := mergedRepository
			defer func() {
				if err := repositoryToClose.Close(); err != nil {
					r.Logger.Warn("failed to clean Git merge", "delivery_id", delivery.DeliveryID, "error", err)
				}
			}()
			if candidate.SHA != "" && candidate.SHA != mergedRepository.SHA {
				return ctrl.Result{}, r.finish(ctx, object, deliveryStateFailed, 0, "delivery contains an inconsistent resolved revision")
			}
			candidate.SHA = mergedRepository.SHA
			if err := r.persistResolvedRevision(ctx, object, candidate.Name, candidate.SHA, candidate.MergeBaseSHA); err != nil {
				return ctrl.Result{}, err
			}
		}
		if candidate.ResolveRef != "" && candidate.SHA == "" {
			revision, err := installation.ResolveRevision(ctx, delivery.Repository.Owner, delivery.Repository.Name, candidate.ResolveRef)
			if err != nil {
				if unavailableDeliveryRevision(err) {
					message := fmt.Sprintf("GitHub event revision %q is unavailable: %v", candidate.ResolveRef, err)
					return ctrl.Result{}, r.finish(ctx, object, deliveryStateFailed, 0, message)
				}
				return ctrl.Result{}, err
			}
			candidate.SHA = revision
			if err := r.persistResolvedRevision(ctx, object, candidate.Name, revision, ""); err != nil {
				return ctrl.Result{}, err
			}
		}
		cachedRevision, err := r.cachedWorkflowRevision(ctx, project, installation, mergedRepository, &delivery, candidate.SHA)
		if err != nil {
			return ctrl.Result{}, err
		}
		candidateSeenFiles := make(map[string]struct{}, len(seenFiles)+len(cachedRevision.Paths))
		for path := range seenFiles {
			candidateSeenFiles[path] = struct{}{}
		}
		workflowFileCount := recordWorkflowFiles(candidateSeenFiles, cachedRevision.Paths)
		candidateWorkflowJobs := workflowJobs
		candidateSelections := make([]workflowSelection, 0)
		if err := validateDeliveryFanOut(workflowFileCount, candidateWorkflowJobs); err != nil {
			if r.skipForkPullRequestDiscovery(candidate, &delivery, err.Error()) {
				continue
			}
			return ctrl.Result{}, r.finish(ctx, object, deliveryStateFailed, 0, err.Error())
		}
		for _, cachedWorkflow := range cachedRevision.Workflows {
			workflowPath := cachedWorkflow.Path
			definition := cachedWorkflow.Definition
			_, matched, err := workflow.Match(definition.On, workflow.Event{
				Name: candidate.Name, Action: candidate.Action, Ref: candidate.Ref,
				RefName: githubclient.RefName(candidate.Ref), HeadRef: candidate.HeadRef,
				BaseRef: candidate.BaseRef, WorkflowName: candidate.WorkflowName,
			})
			if err != nil {
				message := fmt.Sprintf("workflow %q does not accept the event: %v", workflowPath, err)
				if r.skipForkPullRequestDiscovery(candidate, &delivery, message) {
					continue candidateLoop
				}
				return ctrl.Result{}, r.finish(ctx, object, deliveryStateFailed, 0, message)
			}
			if matched {
				candidateWorkflowJobs += len(definition.Jobs)
				if err := validateDeliveryFanOut(workflowFileCount, candidateWorkflowJobs); err != nil {
					if r.skipForkPullRequestDiscovery(candidate, &delivery, err.Error()) {
						continue candidateLoop
					}
					return ctrl.Result{}, r.finish(ctx, object, deliveryStateFailed, 0, err.Error())
				}
				candidateSelections = append(candidateSelections, workflowSelection{Path: workflowPath, Event: candidate})
			}
		}
		if cachedRevision.InvalidPath != "" {
			message := fmt.Sprintf("invalid workflow %q: %s", cachedRevision.InvalidPath, cachedRevision.InvalidError)
			if r.skipForkPullRequestDiscovery(candidate, &delivery, message) {
				continue candidateLoop
			}
			r.Logger.Warn("rejected invalid workflow", "path", cachedRevision.InvalidPath, "error", cachedRevision.InvalidError)
			return ctrl.Result{}, r.finish(ctx, object, deliveryStateFailed, 0, message)
		}
		seenFiles = candidateSeenFiles
		workflowJobs = candidateWorkflowJobs
		selections = append(selections, candidateSelections...)
	}
	for _, selection := range selections {
		if err := r.createWorkflowRun(ctx, project, &delivery, selection); err != nil {
			if terminalWorkflowRunCreationError(err) {
				return ctrl.Result{}, r.finish(ctx, object, deliveryStateFailed, 0, err.Error())
			}
			return ctrl.Result{}, err
		}
	}
	workflowRuns := len(selections)
	r.Logger.Info("completed GitHub webhook discovery", "delivery_id", delivery.DeliveryID, "workflow_runs", workflowRuns)
	return ctrl.Result{}, r.finish(ctx, object, deliveryStateCompleted, workflowRuns, "")
}

func recordWorkflowFiles(seen map[string]struct{}, paths []string) int {
	for _, path := range paths {
		seen[path] = struct{}{}
	}
	return len(seen)
}

func (r *DeliveryReconciler) skipForkPullRequestDiscovery(candidate normalizedEvent, delivery *queuedDelivery, message string) bool {
	if candidate.Name != "pull_request" || !candidate.Fork {
		return false
	}
	r.Logger.Warn("skipped fork pull request workflow discovery", "delivery_id", delivery.DeliveryID, "reason", message)
	return true
}

func deliveryEvents(event normalizedEvent) []normalizedEvent {
	if event.Name != "pull_request" {
		return []normalizedEvent{event}
	}
	events := make([]normalizedEvent, 0, 2)
	if event.PullRequest == nil {
		if event.MergeRevision {
			events = append(events, event)
		}
		return events
	}
	target := event
	target.Name = "pull_request_target"
	target.SHA = event.PullRequest.BaseSHA
	target.Ref = "refs/heads/" + event.PullRequest.BaseRef
	target.ResolveRef = ""
	if target.SHA == "" {
		target.ResolveRef = event.PullRequest.BaseRef
	}
	target.HeadSHA = ""
	target.MergeBaseSHA = ""
	if event.Fork {
		events = append(events, target)
	}
	if event.MergeRevision {
		events = append(events, event)
	}
	if !event.Fork {
		events = append(events, target)
	}
	return events
}

func workflowFilesAtRevision(ctx context.Context, installation *githubclient.InstallationClient, mergedRepository *gitrepository.Repository, delivery *queuedDelivery, directory, revision string) ([]string, error) {
	if mergedRepository != nil {
		paths, err := mergedRepository.ListFiles(ctx, directory)
		if err != nil {
			return nil, err
		}
		workflowFiles := make([]string, 0, len(paths))
		for _, path := range paths {
			if workflowFile(path) {
				workflowFiles = append(workflowFiles, path)
			}
		}
		return workflowFiles, nil
	}
	contents, err := installation.ListDirectory(ctx, delivery.Repository.Owner, delivery.Repository.Name, directory, revision)
	if err != nil {
		if missingWorkflowDirectory(err) {
			return nil, nil
		}
		return nil, err
	}
	workflowFiles := make([]string, 0, len(contents))
	for _, content := range contents {
		if content.Type == "file" && workflowFile(content.Path) {
			workflowFiles = append(workflowFiles, content.Path)
		}
	}
	return workflowFiles, nil
}

func (r *DeliveryReconciler) cachedWorkflowRevision(ctx context.Context, project *actionsv1alpha1.Project, installation *githubclient.InstallationClient, mergedRepository *gitrepository.Repository, delivery *queuedDelivery, revision string) (workflowRevision, error) {
	r.workflowCacheOnce.Do(func() {
		if r.workflowCache == nil {
			r.workflowCache = newWorkflowRevisionCache(maxCachedWorkflowRevisions, maxCachedWorkflowBytes)
		}
	})
	key := workflowRevisionKey{
		ProjectUID: string(project.UID), RepositoryID: delivery.Repository.ID,
		WorkflowDirectory: project.Spec.WorkflowDirectory, Revision: revision,
	}
	return r.workflowCache.load(ctx, key, func(ctx context.Context) (workflowRevision, error) {
		paths, err := workflowFilesAtRevision(ctx, installation, mergedRepository, delivery, project.Spec.WorkflowDirectory, revision)
		if err != nil {
			return workflowRevision{}, err
		}
		result := workflowRevision{Paths: paths, Size: 1}
		for _, path := range paths {
			result.Size += len(path)
		}
		if len(paths) > maxWorkflowFiles {
			return result, nil
		}
		result.Workflows = make([]revisionWorkflow, 0, len(paths))
		for _, workflowPath := range paths {
			var data []byte
			if mergedRepository != nil {
				data, err = mergedRepository.ReadFile(ctx, workflowPath)
			} else {
				data, err = installation.GetFile(ctx, delivery.Repository.Owner, delivery.Repository.Name, workflowPath, revision)
			}
			if err != nil {
				return workflowRevision{}, err
			}
			result.Size += len(data)
			definition, err := workflow.Parse(data)
			if err != nil {
				result.InvalidPath = workflowPath
				result.InvalidError = err.Error()
				return result, nil
			}
			result.Workflows = append(result.Workflows, revisionWorkflow{Path: workflowPath, Definition: definition})
		}
		return result, nil
	})
}

func terminalWorkflowRunCreationError(err error) bool {
	return apierrors.IsConflict(err) || apierrors.IsInvalid(err)
}

func (r *DeliveryReconciler) createWorkflowRun(ctx context.Context, project *actionsv1alpha1.Project, delivery *queuedDelivery, selection workflowSelection) error {
	replayID := delivery.ReplayID
	if selection.Event.Name == "pull_request_target" {
		replayID += ":pull_request_target"
	}
	name := workflowRunName(selection.Path, string(project.UID), replayID)
	annotations := map[string]string{}
	if delivery.EventSnapshot != "" {
		annotations[eventsnapshot.Annotation] = delivery.EventSnapshot
	}
	var forkPullRequest *actionsv1alpha1.WorkflowRunForkPullRequest
	if selection.Event.Name == "pull_request" && selection.Event.Fork {
		_, forkPullRequest = forkPullRequestPolicy(project, selection.Event.Dependabot)
	}
	desired := &actionsv1alpha1.WorkflowRun{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: project.Namespace,
			Annotations: annotations,
		},
		Spec: actionsv1alpha1.WorkflowRunSpec{
			ProjectRef:              corev1.LocalObjectReference{Name: project.Name},
			TTLSecondsAfterFinished: r.WorkflowRunTTLSecondsAfterFinished,
			ForkPullRequest:         forkPullRequest,
			Source: actionsv1alpha1.WorkflowRunSource{
				Type: actionsv1alpha1.SourceTypeGitHub,
				GitHub: &actionsv1alpha1.GitHubWorkflowRunSource{
					Actor:      selection.Event.Actor,
					Repository: actionsv1alpha1.GitHubRepository{ID: delivery.Repository.ID, Owner: delivery.Repository.Owner, Name: delivery.Repository.Name},
					Event:      workflowRunEvent(selection.Event, delivery.DeliveryID),
					Revision:   workflowRunRevision(selection.Event),
				},
			},
			WorkflowPath: selection.Path,
		},
	}
	alias := &actionsv1alpha1.WorkflowRun{}
	aliasKey := client.ObjectKey{Namespace: desired.Namespace, Name: fullDigestWorkflowRunName(selection.Path, replayID)}
	if err := r.APIReader.Get(ctx, aliasKey, alias); err == nil {
		if err := matchingWorkflowRun(alias, desired); err != nil {
			return err
		}
		return r.ensureEventSnapshotOwner(ctx, alias)
	} else if !apierrors.IsNotFound(err) {
		return err
	}
	if err := r.Create(ctx, desired); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return err
		}
		existing := &actionsv1alpha1.WorkflowRun{}
		if err := r.APIReader.Get(ctx, client.ObjectKeyFromObject(desired), existing); err != nil {
			return err
		}
		if err := matchingWorkflowRun(existing, desired); err != nil {
			return err
		}
		return r.ensureEventSnapshotOwner(ctx, existing)
	}
	return r.ensureEventSnapshotOwner(ctx, desired)
}

func forkPullRequestPolicy(project *actionsv1alpha1.Project, dependabot bool) (bool, *actionsv1alpha1.WorkflowRunForkPullRequest) {
	enabled := true
	requireApproval := true
	policy := project.Spec.Source.GitHub.ForkPullRequests
	if policy != nil {
		if policy.Enabled != nil {
			enabled = *policy.Enabled
		}
		if policy.RequireApproval != nil {
			requireApproval = *policy.RequireApproval
		}
	}
	result := &actionsv1alpha1.WorkflowRunForkPullRequest{RequireApproval: requireApproval}
	if policy != nil && !dependabot {
		result.SendWriteTokens = policy.SendWriteTokens
		result.SendSecrets = policy.SendSecrets
	}
	return enabled, result
}

func (r *DeliveryReconciler) ensureEventSnapshotOwner(ctx context.Context, run *actionsv1alpha1.WorkflowRun) error {
	name := run.Annotations[eventsnapshot.Annotation]
	if name == "" {
		return nil
	}
	secret := &corev1.Secret{}
	if err := r.APIReader.Get(ctx, client.ObjectKey{Namespace: run.Namespace, Name: name}, secret); err != nil {
		return err
	}
	for _, owner := range secret.OwnerReferences {
		if owner.APIVersion == actionsv1alpha1.GroupVersion.String() && owner.Kind == "WorkflowRun" && owner.Name == run.Name && owner.UID == run.UID {
			return nil
		}
	}
	before := secret.DeepCopy()
	secret.OwnerReferences = append(secret.OwnerReferences, metav1.OwnerReference{
		APIVersion: actionsv1alpha1.GroupVersion.String(), Kind: "WorkflowRun", Name: run.Name, UID: run.UID,
	})
	return r.Patch(ctx, secret, client.MergeFrom(before))
}

func workflowRunEvent(event normalizedEvent, deliveryID string) actionsv1alpha1.GitHubEvent {
	result := actionsv1alpha1.GitHubEvent{
		Name: actionsv1alpha1.GitHubEventName(event.Name), Action: event.Action, DeliveryID: deliveryID,
		PullRequest: workflowRunPullRequest(event.PullRequest),
	}
	if event.WorkflowRun != nil {
		result.WorkflowRun = &actionsv1alpha1.GitHubWorkflowRunEvent{Conclusion: event.WorkflowRun.Conclusion, HeadSHA: event.WorkflowRun.HeadSHA}
	}
	if event.Issue != nil {
		result.Issue = &actionsv1alpha1.GitHubIssueEvent{Number: event.Issue.Number, Body: event.Issue.Body}
	}
	if event.Comment != nil {
		result.Comment = &actionsv1alpha1.GitHubCommentEvent{Body: event.Comment.Body}
	}
	if event.Review != nil {
		result.Review = &actionsv1alpha1.GitHubReviewEvent{Body: event.Review.Body}
	}
	return result
}

func workflowRunPullRequest(pullRequest *normalizedPullRequest) *actionsv1alpha1.GitHubPullRequest {
	if pullRequest == nil {
		return nil
	}
	return &actionsv1alpha1.GitHubPullRequest{
		Number: pullRequest.Number, Body: pullRequest.Body, HTMLURL: pullRequest.HTMLURL,
		HeadRepository: actionsv1alpha1.GitHubRepository{
			ID: pullRequest.HeadRepository.ID, Owner: pullRequest.HeadRepository.Owner, Name: pullRequest.HeadRepository.Name,
		},
		HeadRef: pullRequest.HeadRef,
		HeadSHA: pullRequest.HeadSHA,
		BaseRef: pullRequest.BaseRef,
	}
}

func workflowRunRevision(event normalizedEvent) actionsv1alpha1.GitRevision {
	integrationRevision := event.HeadSHA != ""
	revision := actionsv1alpha1.GitRevision{SHA: event.SHA, HeadSHA: event.HeadSHA, MergeBaseSHA: event.MergeBaseSHA, Ref: event.Ref}
	if event.Name == "pull_request" {
		revision.HeadRef = event.HeadRef
		if event.PullRequest != nil {
			revision.HeadSHA = event.PullRequest.HeadSHA
			if integrationRevision {
				revision.BaseSHA = event.PullRequest.BaseSHA
			}
		}
	}
	if event.Name == "pull_request" || event.Name == "merge_group" {
		revision.BaseRef = event.BaseRef
	}
	return revision
}

func matchingWorkflowRun(existing, desired *actionsv1alpha1.WorkflowRun) error {
	if existing.Annotations[eventsnapshot.Annotation] != desired.Annotations[eventsnapshot.Annotation] {
		return apierrors.NewConflict(
			actionsv1alpha1.GroupVersion.WithResource("workflowruns").GroupResource(),
			existing.Name,
			errors.New("existing WorkflowRun does not match the webhook delivery"),
		)
	}
	existingSpec := existing.Spec.DeepCopy()
	desiredSpec := desired.Spec.DeepCopy()
	existingSpec.TTLSecondsAfterFinished = nil
	desiredSpec.TTLSecondsAfterFinished = nil
	if existingSpec.ForkPullRequest != nil && desiredSpec.ForkPullRequest != nil {
		existingSpec.ForkPullRequest.Approved = desiredSpec.ForkPullRequest.Approved
	}
	if existingGitHub, desiredGitHub := existingSpec.Source.GitHub, desiredSpec.Source.GitHub; existingGitHub != nil && desiredGitHub != nil && existingGitHub.Revision.HeadSHA == "" {
		// Missing HeadSHA is compatible because the API defines SHA as its reporting fallback.
		existingGitHub.Revision.HeadSHA = desiredGitHub.Revision.HeadSHA
	}
	if existingGitHub, desiredGitHub := existingSpec.Source.GitHub, desiredSpec.Source.GitHub; existingGitHub != nil && desiredGitHub != nil && existingGitHub.Event.Name == actionsv1alpha1.GitHubEventNamePullRequest && existingGitHub.Event.PullRequest == nil {
		desiredGitHub.Event.PullRequest = nil
	}
	if apiequality.Semantic.DeepEqual(existingSpec, desiredSpec) {
		return nil
	}
	return apierrors.NewConflict(
		actionsv1alpha1.GroupVersion.WithResource("workflowruns").GroupResource(),
		existing.Name,
		errors.New("existing WorkflowRun does not match the webhook delivery"),
	)
}

func (r *DeliveryReconciler) finish(ctx context.Context, object *corev1.ConfigMap, state string, workflowRuns int, message string) error {
	before := object.DeepCopy()
	finishedAt := r.now()
	if object.Data == nil {
		object.Data = map[string]string{}
	}
	object.Data[deliveryStateKey] = state
	object.Data[deliveryRunCountKey] = strconv.Itoa(workflowRuns)
	if object.Data[deliveryFinishedKey] == "" {
		object.Data[deliveryFinishedKey] = finishedAt.UTC().Format(time.RFC3339)
	} else if recorded, err := time.Parse(time.RFC3339, object.Data[deliveryFinishedKey]); err == nil {
		finishedAt = recorded
	}
	if message == "" {
		delete(object.Data, deliveryMessageKey)
	} else {
		object.Data[deliveryMessageKey] = message
	}
	if apiequality.Semantic.DeepEqual(before.Data, object.Data) {
		return nil
	}
	if err := r.Patch(ctx, object, client.MergeFrom(before)); err != nil {
		return err
	}
	if r.Metrics != nil && before.Data[deliveryStateKey] == "" && !object.CreationTimestamp.IsZero() {
		delivery := queuedDelivery{}
		_ = json.Unmarshal([]byte(object.Data[deliveryDataKey]), &delivery)
		r.Metrics.WebhookDelivery(
			object.Namespace, delivery.ProjectName, delivery.Event.Name, strings.ToLower(state), finishedAt.Sub(object.CreationTimestamp.Time),
		)
	}
	return nil
}

func (r *DeliveryReconciler) persistResolvedRevision(ctx context.Context, object *corev1.ConfigMap, eventName, revision, mergeBase string) error {
	before := object.DeepCopy()
	if object.Data == nil {
		object.Data = map[string]string{}
	}
	key := deliveryRevisionKey
	if eventName == "pull_request_target" {
		key = deliveryTargetRevisionKey
	}
	object.Data[key] = revision
	if mergeBase != "" {
		object.Data[deliveryMergeBaseKey] = mergeBase
	}
	return r.Patch(ctx, object, client.MergeFrom(before))
}

func (r *DeliveryReconciler) retain(ctx context.Context, object *corev1.ConfigMap) (ctrl.Result, error) {
	finishedAt, err := time.Parse(time.RFC3339, object.Data[deliveryFinishedKey])
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("parse webhook delivery completion time: %w", err)
	}
	remaining := deliveryRetention - r.now().Sub(finishedAt)
	if remaining <= 0 {
		return ctrl.Result{}, client.IgnoreNotFound(r.Delete(ctx, object))
	}
	if remaining > deliveryRetention {
		remaining = deliveryRetention
	}
	return ctrl.Result{RequeueAfter: remaining}, nil
}

func (r *DeliveryReconciler) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

func (r *DeliveryReconciler) deliveryAge(object *corev1.ConfigMap) time.Duration {
	if object.CreationTimestamp.IsZero() {
		return 0
	}
	age := r.now().Sub(object.CreationTimestamp.Time)
	if age < 0 {
		return 0
	}
	return age
}

func (r *DeliveryReconciler) SetupWithManager(manager ctrl.Manager) error {
	if r.GitRepository == nil {
		return errors.New("Git repository client must be specified")
	}
	return ctrl.NewControllerManagedBy(manager).
		For(&corev1.ConfigMap{}, builder.OnlyMetadata, builder.WithPredicates(predicate.NewPredicateFuncs(isWebhookDelivery))).
		WithOptions(controller.Options{MaxConcurrentReconciles: maxConcurrentDeliveries}).
		Complete(r)
}

func isWebhookDelivery(object client.Object) bool {
	return object.GetLabels()[deliveryLabel] == "true"
}
