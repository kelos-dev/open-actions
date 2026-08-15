package webhook

import (
	"context"
	"crypto/sha256"
	"encoding/base32"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	actionsv1alpha1 "github.com/kelos-dev/open-actions/api/v1alpha1"
	githubclient "github.com/kelos-dev/open-actions/internal/github"
	"github.com/kelos-dev/open-actions/internal/gitrepository"
	"github.com/kelos-dev/open-actions/internal/workflow"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
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
	rerunRootWaitTimeout      = 2 * time.Minute
	resourceNameMaxLength     = 63
	workflowRunDigestLength   = 20
	deliveryRetention         = 24 * time.Hour
)

type queuedDelivery struct {
	ProjectName string             `json:"projectName"`
	ProjectUID  string             `json:"projectUID"`
	Repository  deliveryRepository `json:"repository"`
	Event       normalizedEvent    `json:"event"`
	Rerun       *normalizedRerun   `json:"rerun,omitempty"`
	ReplayID    string             `json:"replayID"`
	DeliveryID  string             `json:"deliveryID"`
}

type deliveryRepository struct {
	ID    int64  `json:"id"`
	Owner string `json:"owner"`
	Name  string `json:"name"`
}

func (delivery *queuedDelivery) UnmarshalJSON(data []byte) error {
	document := struct {
		ProjectName string             `json:"projectName"`
		ProjectUID  string             `json:"projectUID"`
		Repository  deliveryRepository `json:"repository"`
		Payload     *payload           `json:"payload"`
		Event       normalizedEvent    `json:"event"`
		Rerun       *normalizedRerun   `json:"rerun,omitempty"`
		ReplayID    string             `json:"replayID"`
		DeliveryID  string             `json:"deliveryID"`
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
		Event: document.Event, Rerun: document.Rerun, ReplayID: document.ReplayID, DeliveryID: document.DeliveryID,
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
}

type workflowSelection struct {
	Path  string
	Event normalizedEvent
}

func (h *GitHubHandler) enqueueDelivery(ctx context.Context, project *actionsv1alpha1.Project, event *payload, normalized normalizedEvent, deliveryID string, signedBody []byte) error {
	delivery := queuedDelivery{
		ProjectName: project.Name,
		ProjectUID:  string(project.UID),
		Repository: deliveryRepository{
			ID: event.Repository.ID, Owner: event.Repository.Owner.Login, Name: event.Repository.Name,
		},
		Event:      normalized,
		ReplayID:   webhookReplayID(signedBody),
		DeliveryID: deliveryID,
	}
	return h.enqueueQueuedDelivery(ctx, project, delivery, signedBody)
}

func (h *GitHubHandler) enqueueRerunDelivery(ctx context.Context, project *actionsv1alpha1.Project, event *payload, rerun *normalizedRerun, deliveryID string) error {
	identity := []byte(deliveryID)
	delivery := queuedDelivery{
		ProjectName: project.Name,
		ProjectUID:  string(project.UID),
		Repository: deliveryRepository{
			ID: event.Repository.ID, Owner: event.Repository.Owner.Login, Name: event.Repository.Name,
		},
		Rerun:      rerun,
		ReplayID:   webhookReplayID(identity),
		DeliveryID: deliveryID,
	}
	return h.enqueueQueuedDelivery(ctx, project, delivery, identity)
}

func (h *GitHubHandler) enqueueQueuedDelivery(ctx context.Context, project *actionsv1alpha1.Project, delivery queuedDelivery, identity []byte) error {
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
	}
	return nil
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

func (r *DeliveryReconciler) Reconcile(ctx context.Context, request ctrl.Request) (ctrl.Result, error) {
	object := &corev1.ConfigMap{}
	if err := r.Get(ctx, request.NamespacedName, object); err != nil {
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
	if delivery.Rerun != nil {
		return r.reconcileRerun(ctx, object, project, &delivery)
	}
	githubConfig := project.Spec.Source.GitHub
	privateKey, err := readSecretValue(ctx, reader, project.Namespace, githubConfig.PrivateKeySecretRef)
	if err != nil {
		return ctrl.Result{}, err
	}
	installation, err := r.GitHub.Installation(ctx, githubConfig.AppID, githubConfig.InstallationID, privateKey, delivery.Repository.Name, githubclient.InstallationPermissions{ContentsRead: true})
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
	for _, candidate := range events {
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
		workflowFiles, err := workflowFilesAtRevision(ctx, installation, mergedRepository, &delivery, project.Spec.WorkflowDirectory, candidate.SHA)
		if err != nil {
			return ctrl.Result{}, err
		}
		workflowFileCount := recordWorkflowFiles(seenFiles, workflowFiles)
		if err := validateDeliveryFanOut(workflowFileCount, workflowJobs); err != nil {
			return ctrl.Result{}, r.finish(ctx, object, deliveryStateFailed, 0, err.Error())
		}
		for _, workflowPath := range workflowFiles {
			var data []byte
			if mergedRepository != nil {
				data, err = mergedRepository.ReadFile(ctx, workflowPath)
			} else {
				data, err = installation.GetFile(ctx, delivery.Repository.Owner, delivery.Repository.Name, workflowPath, candidate.SHA)
			}
			if err != nil {
				return ctrl.Result{}, err
			}
			definition, err := workflow.Parse(data)
			if err != nil {
				r.Logger.Warn("rejected invalid workflow", "path", workflowPath, "error", err)
				return ctrl.Result{}, r.finish(ctx, object, deliveryStateFailed, 0, fmt.Sprintf("invalid workflow %q: %v", workflowPath, err))
			}
			_, matched, err := workflow.Match(definition.On, workflow.Event{
				Name: candidate.Name, Action: candidate.Action, Ref: candidate.Ref,
				RefName: githubclient.RefName(candidate.Ref), HeadRef: candidate.HeadRef,
				BaseRef: candidate.BaseRef, WorkflowName: candidate.WorkflowName,
			})
			if err != nil {
				return ctrl.Result{}, r.finish(ctx, object, deliveryStateFailed, 0, fmt.Sprintf("workflow %q does not accept the event: %v", workflowPath, err))
			}
			if matched {
				workflowJobs += len(definition.Jobs)
				if err := validateDeliveryFanOut(workflowFileCount, workflowJobs); err != nil {
					return ctrl.Result{}, r.finish(ctx, object, deliveryStateFailed, 0, err.Error())
				}
				selections = append(selections, workflowSelection{Path: workflowPath, Event: candidate})
			}
		}
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

func deliveryEvents(event normalizedEvent) []normalizedEvent {
	if event.Name != "pull_request" {
		return []normalizedEvent{event}
	}
	events := make([]normalizedEvent, 0, 2)
	if !event.Fork && event.MergeRevision {
		events = append(events, event)
	}
	if event.PullRequest == nil {
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
	events = append(events, target)
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

func terminalWorkflowRunCreationError(err error) bool {
	return apierrors.IsConflict(err) || apierrors.IsInvalid(err)
}

func (r *DeliveryReconciler) reconcileRerun(ctx context.Context, object *corev1.ConfigMap, project *actionsv1alpha1.Project, delivery *queuedDelivery) (ctrl.Result, error) {
	runs := &actionsv1alpha1.WorkflowRunList{}
	if err := r.APIReader.List(ctx, runs, client.InNamespace(project.Namespace), client.MatchingLabels{actionsv1alpha1.LabelWorkflowRunRootUID: delivery.Rerun.RootRunUID}); err != nil {
		return ctrl.Result{}, err
	}
	root := workflowRunByUID(runs.Items, delivery.Rerun.RootRunUID)
	if root == nil {
		age := r.deliveryAge(object)
		if age < rerunRootWaitTimeout {
			retryAfter := 2 * time.Second
			if remaining := rerunRootWaitTimeout - age; remaining < retryAfter {
				retryAfter = remaining
			}
			return ctrl.Result{RequeueAfter: retryAfter}, nil
		}
		return ctrl.Result{}, r.finish(ctx, object, deliveryStateFailed, 0, fmt.Sprintf("original WorkflowRun with UID %q is unavailable", delivery.Rerun.RootRunUID))
	}
	if err := validateRerunCheck(project, delivery, root); err != nil {
		return ctrl.Result{}, r.finish(ctx, object, deliveryStateFailed, 0, err.Error())
	}
	latest, err := latestWorkflowRunAttempt(root, runs.Items)
	if err != nil {
		return ctrl.Result{}, r.finish(ctx, object, deliveryStateFailed, 0, err.Error())
	}
	if workflowRunAttemptForRequest(root, runs.Items, delivery.DeliveryID) != nil {
		return ctrl.Result{}, r.finish(ctx, object, deliveryStateCompleted, 1, "")
	}
	if !workflowRunTerminal(latest) {
		return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
	}
	if latest.Spec.Rerun != nil && latest.Spec.Rerun.Attempt == 2147483647 {
		return ctrl.Result{}, r.finish(ctx, object, deliveryStateFailed, 0, "WorkflowRun rerun attempt limit reached")
	}

	jobIDs, err := r.rerunWorkflowJobIDs(ctx, latest)
	if err != nil {
		return ctrl.Result{}, r.finish(ctx, object, deliveryStateFailed, 0, err.Error())
	}
	attempt := int32(2)
	if latest.Spec.Rerun != nil {
		attempt = latest.Spec.Rerun.Attempt + 1
	}
	if err := r.createRerunWorkflowRun(ctx, root, latest, attempt, delivery.DeliveryID, jobIDs); err != nil {
		if errors.Is(err, errRerunAttemptClaimed) {
			return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
		}
		if terminalWorkflowRunCreationError(err) {
			return ctrl.Result{}, r.finish(ctx, object, deliveryStateFailed, 0, err.Error())
		}
		return ctrl.Result{}, err
	}
	r.Logger.Info("created WorkflowRun rerun", "delivery_id", delivery.DeliveryID, "original_run", root.Name, "previous_run", latest.Name, "attempt", attempt, "jobs", len(jobIDs))
	return ctrl.Result{}, r.finish(ctx, object, deliveryStateCompleted, 1, "")
}

var errRerunAttemptClaimed = errors.New("rerun attempt was claimed by another request")

func workflowRunByUID(runs []actionsv1alpha1.WorkflowRun, uid string) *actionsv1alpha1.WorkflowRun {
	for index := range runs {
		if string(runs[index].UID) == uid {
			return &runs[index]
		}
	}
	return nil
}

func workflowRunAttemptForRequest(root *actionsv1alpha1.WorkflowRun, runs []actionsv1alpha1.WorkflowRun, requestID string) *actionsv1alpha1.WorkflowRun {
	for index := range runs {
		candidate := &runs[index]
		if candidate.Spec.Rerun != nil && candidate.Spec.Rerun.RequestID == requestID &&
			candidate.Spec.Rerun.OriginalRunRef.Name == root.Name && candidate.Spec.Rerun.OriginalRunRef.UID == root.UID {
			return candidate
		}
	}
	return nil
}

func validateRerunCheck(project *actionsv1alpha1.Project, delivery *queuedDelivery, root *actionsv1alpha1.WorkflowRun) error {
	if root.Spec.Rerun != nil {
		return fmt.Errorf("WorkflowRun %q is not an original attempt", root.Name)
	}
	if root.Spec.ProjectRef.Name != project.Name || root.Spec.Source.Type != actionsv1alpha1.SourceTypeGitHub || root.Spec.Source.GitHub == nil {
		return fmt.Errorf("WorkflowRun %q does not belong to Project %q", root.Name, project.Name)
	}
	source := root.Spec.Source.GitHub
	repository := delivery.Repository
	if source.Repository.ID != repository.ID || source.Repository.Owner != repository.Owner || source.Repository.Name != repository.Name {
		return fmt.Errorf("WorkflowRun %q does not match the check-run repository", root.Name)
	}
	headSHA := source.Revision.SHA
	if source.Event.Name == actionsv1alpha1.GitHubEventNamePullRequest && source.Revision.HeadSHA != "" {
		headSHA = source.Revision.HeadSHA
	}
	if headSHA != delivery.Rerun.HeadSHA {
		return fmt.Errorf("WorkflowRun %q does not match the check-run revision", root.Name)
	}
	if root.Status.Source == nil || root.Status.Source.GitHub == nil || root.Status.Source.GitHub.CheckRun == nil || root.Status.Source.GitHub.CheckRun.ID != delivery.Rerun.CheckRunID {
		return fmt.Errorf("WorkflowRun %q does not identify GitHub check run %d", root.Name, delivery.Rerun.CheckRunID)
	}
	return nil
}

func latestWorkflowRunAttempt(root *actionsv1alpha1.WorkflowRun, runs []actionsv1alpha1.WorkflowRun) (*actionsv1alpha1.WorkflowRun, error) {
	latest := root
	attempts := map[int32]struct{}{1: {}}
	for index := range runs {
		candidate := &runs[index]
		rerun := candidate.Spec.Rerun
		if rerun == nil || rerun.OriginalRunRef.Name != root.Name || rerun.OriginalRunRef.UID != root.UID {
			continue
		}
		if candidate.Spec.ProjectRef != root.Spec.ProjectRef || candidate.Spec.WorkflowPath != root.Spec.WorkflowPath || !apiequality.Semantic.DeepEqual(candidate.Spec.Source, root.Spec.Source) {
			continue
		}
		planned := meta.FindStatusCondition(candidate.Status.Conditions, actionsv1alpha1.WorkflowRunConditionPlanned)
		if planned != nil && planned.Status == metav1.ConditionFalse && planned.Reason == "RerunInvalid" {
			continue
		}
		if _, found := attempts[rerun.Attempt]; found {
			return nil, fmt.Errorf("WorkflowRun rerun lineage has multiple attempt %d objects", rerun.Attempt)
		}
		attempts[rerun.Attempt] = struct{}{}
		if latest.Spec.Rerun == nil || rerun.Attempt > latest.Spec.Rerun.Attempt {
			latest = candidate
		}
	}
	return latest, nil
}

func workflowRunTerminal(run *actionsv1alpha1.WorkflowRun) bool {
	condition := meta.FindStatusCondition(run.Status.Conditions, actionsv1alpha1.WorkflowRunConditionSucceeded)
	return condition != nil && (condition.Status == metav1.ConditionTrue || condition.Status == metav1.ConditionFalse)
}

func (r *DeliveryReconciler) rerunWorkflowJobIDs(ctx context.Context, run *actionsv1alpha1.WorkflowRun) ([]string, error) {
	condition := meta.FindStatusCondition(run.Status.Conditions, actionsv1alpha1.WorkflowRunConditionSucceeded)
	if condition == nil || condition.Status != metav1.ConditionFalse || condition.Reason != "JobFailed" {
		return nil, nil
	}
	jobs := &actionsv1alpha1.WorkflowJobList{}
	if err := r.APIReader.List(ctx, jobs, client.InNamespace(run.Namespace), client.MatchingLabels{actionsv1alpha1.LabelWorkflowRunUID: string(run.UID)}); err != nil {
		return nil, err
	}
	if run.Status.Jobs == nil || int32(len(jobs.Items)) != run.Status.Jobs.Total {
		return nil, fmt.Errorf("WorkflowRun %q does not have its complete WorkflowJob history", run.Name)
	}
	selected := make(map[string]struct{})
	selectedLogicalIDs := make(map[string]struct{})
	for index := range jobs.Items {
		job := &jobs.Items[index]
		succeeded := meta.FindStatusCondition(job.Status.Conditions, actionsv1alpha1.WorkflowJobConditionSucceeded)
		if job.Status.Result == actionsv1alpha1.WorkflowJobResultFailure || (job.Status.Result == "" && succeeded != nil && succeeded.Status == metav1.ConditionFalse) {
			selected[job.Spec.JobID] = struct{}{}
			selectedLogicalIDs[workflowJobLogicalID(job)] = struct{}{}
		}
	}
	if len(selected) == 0 {
		return nil, fmt.Errorf("WorkflowRun %q reports failed jobs but no failed WorkflowJobs are available", run.Name)
	}
	for changed := true; changed; {
		changed = false
		for index := range jobs.Items {
			job := &jobs.Items[index]
			if _, found := selected[job.Spec.JobID]; found || !needsSelectedJob(job.Spec.Needs, selectedLogicalIDs) {
				continue
			}
			selected[job.Spec.JobID] = struct{}{}
			selectedLogicalIDs[workflowJobLogicalID(job)] = struct{}{}
			changed = true
		}
	}
	jobIDs := make([]string, 0, len(selected))
	for id := range selected {
		jobIDs = append(jobIDs, id)
	}
	sort.Strings(jobIDs)
	return jobIDs, nil
}

func workflowJobLogicalID(job *actionsv1alpha1.WorkflowJob) string {
	if job.Spec.Matrix != nil {
		return job.Spec.Matrix.LogicalJobID
	}
	return job.Spec.JobID
}

func needsSelectedJob(needs []string, selectedLogicalIDs map[string]struct{}) bool {
	for _, dependency := range needs {
		if _, found := selectedLogicalIDs[dependency]; found {
			return true
		}
	}
	return false
}

func (r *DeliveryReconciler) createRerunWorkflowRun(ctx context.Context, root, previous *actionsv1alpha1.WorkflowRun, attempt int32, requestID string, jobIDs []string) error {
	desired := &actionsv1alpha1.WorkflowRun{
		ObjectMeta: metav1.ObjectMeta{
			Name: rerunWorkflowRunName(root, attempt), Namespace: root.Namespace,
			Labels: map[string]string{actionsv1alpha1.LabelWorkflowRunRootUID: string(root.UID)},
		},
		Spec: *previous.Spec.DeepCopy(),
	}
	desired.Spec.CancelRequested = false
	desired.Spec.Rerun = &actionsv1alpha1.WorkflowRunRerun{
		OriginalRunRef: actionsv1alpha1.WorkflowRunReference{Name: root.Name, UID: root.UID},
		PreviousRunRef: actionsv1alpha1.WorkflowRunReference{Name: previous.Name, UID: previous.UID},
		Attempt:        attempt,
		RequestID:      requestID,
		JobIDs:         append([]string(nil), jobIDs...),
	}
	if err := r.Create(ctx, desired); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return err
		}
		existing := &actionsv1alpha1.WorkflowRun{}
		if err := r.APIReader.Get(ctx, client.ObjectKeyFromObject(desired), existing); err != nil {
			return err
		}
		if matchingRerunRequest(existing, desired) {
			return errRerunAttemptClaimed
		}
		return matchingWorkflowRun(existing, desired)
	}
	return nil
}

func matchingRerunRequest(existing, desired *actionsv1alpha1.WorkflowRun) bool {
	if existing.Spec.Rerun == nil || desired.Spec.Rerun == nil || existing.Spec.Rerun.RequestID == desired.Spec.Rerun.RequestID {
		return false
	}
	existingCopy := existing.DeepCopy()
	desiredCopy := desired.DeepCopy()
	existingCopy.Spec.Rerun.RequestID = desiredCopy.Spec.Rerun.RequestID
	return matchingWorkflowRun(existingCopy, desiredCopy) == nil
}

func rerunWorkflowRunName(root *actionsv1alpha1.WorkflowRun, attempt int32) string {
	digest := sha256.Sum256([]byte(root.UID))
	suffix := fmt.Sprintf("-attempt-%d-%s", attempt, strings.ToLower(digestEncoding.EncodeToString(digest[:]))[:8])
	base := sanitizeName(root.Name)
	if len(base) > resourceNameMaxLength-len(suffix) {
		base = strings.Trim(base[:resourceNameMaxLength-len(suffix)], "-")
	}
	return base + suffix
}

func (r *DeliveryReconciler) createWorkflowRun(ctx context.Context, project *actionsv1alpha1.Project, delivery *queuedDelivery, selection workflowSelection) error {
	replayID := delivery.ReplayID
	if selection.Event.Name == "pull_request_target" {
		replayID += ":pull_request_target"
	}
	name := workflowRunName(selection.Path, string(project.UID), replayID)
	desired := &actionsv1alpha1.WorkflowRun{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: project.Namespace},
		Spec: actionsv1alpha1.WorkflowRunSpec{
			ProjectRef:              corev1.LocalObjectReference{Name: project.Name},
			TTLSecondsAfterFinished: r.WorkflowRunTTLSecondsAfterFinished,
			Source: actionsv1alpha1.WorkflowRunSource{
				Type: actionsv1alpha1.SourceTypeGitHub,
				GitHub: &actionsv1alpha1.GitHubWorkflowRunSource{
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
		return matchingWorkflowRun(alias, desired)
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
		return matchingWorkflowRun(existing, desired)
	}
	return nil
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
	existingSpec := existing.Spec.DeepCopy()
	desiredSpec := desired.Spec.DeepCopy()
	existingSpec.TTLSecondsAfterFinished = nil
	desiredSpec.TTLSecondsAfterFinished = nil
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
	if object.Data == nil {
		object.Data = map[string]string{}
	}
	object.Data[deliveryStateKey] = state
	object.Data[deliveryRunCountKey] = strconv.Itoa(workflowRuns)
	if object.Data[deliveryFinishedKey] == "" {
		object.Data[deliveryFinishedKey] = r.now().UTC().Format(time.RFC3339)
	}
	if message == "" {
		delete(object.Data, deliveryMessageKey)
	} else {
		object.Data[deliveryMessageKey] = message
	}
	if apiequality.Semantic.DeepEqual(before.Data, object.Data) {
		return nil
	}
	return r.Patch(ctx, object, client.MergeFrom(before))
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
		For(&corev1.ConfigMap{}, builder.WithPredicates(predicate.NewPredicateFuncs(isWebhookDelivery))).
		Complete(r)
}

func isWebhookDelivery(object client.Object) bool {
	return object.GetLabels()[deliveryLabel] == "true"
}
