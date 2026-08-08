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
	"strconv"
	"strings"
	"time"

	actionsv1alpha1 "github.com/kelos-dev/open-actions/api/v1alpha1"
	githubclient "github.com/kelos-dev/open-actions/internal/github"
	"github.com/kelos-dev/open-actions/internal/workflow"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

var digestEncoding = base32.StdEncoding.WithPadding(base32.NoPadding)

const (
	deliveryLabel          = "actions.kelos.dev/webhook-delivery"
	deliveryDataKey        = "delivery.json"
	deliveryStateKey       = "state"
	deliveryMessageKey     = "message"
	deliveryRunCountKey    = "workflowRuns"
	deliveryFinishedKey    = "finishedAt"
	deliveryStateCompleted = "Completed"
	deliveryStateFailed    = "Failed"
	maxDeliveryBytes       = 900_000
	maxWorkflowFiles       = 100
	maxWorkflowJobs        = 1000
	deliveryRetention      = 24 * time.Hour
)

type queuedDelivery struct {
	GatewayName string          `json:"gatewayName"`
	GatewayUID  string          `json:"gatewayUID"`
	Payload     payload         `json:"payload"`
	Event       normalizedEvent `json:"event"`
	ReplayID    string          `json:"replayID"`
	DeliveryID  string          `json:"deliveryID"`
}

type DeliveryReconciler struct {
	client.Client
	APIReader client.Reader
	GitHub    *githubclient.Client
	Logger    *slog.Logger
	Now       func() time.Time
}

func (h *GitHubHandler) enqueueDelivery(ctx context.Context, gateway *actionsv1alpha1.ActionsGateway, event *payload, normalized normalizedEvent, deliveryID string, signedBody []byte) error {
	delivery := queuedDelivery{
		GatewayName: gateway.Name,
		GatewayUID:  string(gateway.UID),
		Payload:     *event,
		Event:       normalized,
		ReplayID:    webhookReplayID(signedBody),
		DeliveryID:  deliveryID,
	}
	data, err := json.Marshal(delivery)
	if err != nil {
		return err
	}
	if len(data) > maxDeliveryBytes {
		return fmt.Errorf("normalized webhook delivery exceeds %d bytes", maxDeliveryBytes)
	}
	object := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{
		Name:      webhookDeliveryName(signedBody),
		Namespace: gateway.Namespace,
		Labels:    map[string]string{deliveryLabel: "true"},
	}, Data: map[string]string{deliveryDataKey: string(data)}}
	if err := controllerutil.SetControllerReference(gateway, object, h.Client.Scheme()); err != nil {
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
		if !metav1.IsControlledBy(existing, gateway) || decodeErr != nil || !apiequality.Semantic.DeepEqual(existingDelivery, delivery) {
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

func workflowRunName(workflowPath, replayID string) string {
	base := strings.TrimSuffix(filepath.Base(workflowPath), filepath.Ext(workflowPath))
	base = sanitizeName(base)
	if base == "" {
		base = "workflow"
	}
	digest := sha256.Sum256([]byte(replayID + "|" + workflowPath))
	suffix := strings.ToLower(digestEncoding.EncodeToString(digest[:]))
	if len(base) > 63-len(suffix)-1 {
		base = strings.Trim(base[:63-len(suffix)-1], "-")
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
	reader := r.APIReader
	gateway := &actionsv1alpha1.ActionsGateway{}
	if err := reader.Get(ctx, client.ObjectKey{Namespace: object.Namespace, Name: delivery.GatewayName}, gateway); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, r.finish(ctx, object, deliveryStateFailed, 0, "ActionsGateway was deleted")
		}
		return ctrl.Result{}, err
	}
	if string(gateway.UID) != delivery.GatewayUID || !metav1.IsControlledBy(object, gateway) {
		return ctrl.Result{}, r.finish(ctx, object, deliveryStateFailed, 0, "ActionsGateway was recreated")
	}
	githubConfig := gateway.Spec.Source.GitHub
	privateKey, err := readSecretValue(ctx, reader, gateway.Namespace, githubConfig.PrivateKeySecretRef)
	if err != nil {
		return ctrl.Result{}, err
	}
	installation, err := r.GitHub.Installation(ctx, githubConfig.AppID, githubConfig.InstallationID, privateKey, delivery.Payload.Repository.Name)
	if err != nil {
		return ctrl.Result{}, err
	}
	if delivery.Event.ResolveRef != "" {
		delivery.Event.SHA, err = installation.ResolveRevision(ctx, delivery.Payload.Repository.Owner.Login, delivery.Payload.Repository.Name, delivery.Event.ResolveRef)
		if err != nil {
			return ctrl.Result{}, err
		}
	}
	contents, err := installation.ListDirectory(ctx, delivery.Payload.Repository.Owner.Login, delivery.Payload.Repository.Name, gateway.Spec.WorkflowDirectory, delivery.Event.SHA)
	if err != nil {
		if missingWorkflowDirectory(err) {
			contents = nil
		} else {
			return ctrl.Result{}, err
		}
	}
	workflowFiles := make([]string, 0, len(contents))
	for _, content := range contents {
		if content.Type == "file" && workflowFile(content.Path) {
			workflowFiles = append(workflowFiles, content.Path)
		}
	}
	if err := validateDeliveryFanOut(len(workflowFiles), 0); err != nil {
		return ctrl.Result{}, r.finish(ctx, object, deliveryStateFailed, 0, err.Error())
	}
	workflowPaths := make([]string, 0, len(workflowFiles))
	workflowJobs := 0
	for _, workflowPath := range workflowFiles {
		data, err := installation.GetFile(ctx, delivery.Payload.Repository.Owner.Login, delivery.Payload.Repository.Name, workflowPath, delivery.Event.SHA)
		if err != nil {
			return ctrl.Result{}, err
		}
		definition, err := workflow.Parse(data)
		if err != nil {
			r.Logger.Warn("rejected invalid workflow", "path", workflowPath, "error", err)
			return ctrl.Result{}, r.finish(ctx, object, deliveryStateFailed, 0, fmt.Sprintf("invalid workflow %q: %v", workflowPath, err))
		}
		if workflow.Matches(definition.On, workflow.Event{
			Name: delivery.Event.Name, Action: delivery.Event.Action, Ref: delivery.Event.Ref,
			RefName: githubclient.RefName(delivery.Event.Ref), BaseRef: delivery.Event.BaseRef,
		}) {
			workflowJobs += len(definition.Jobs)
			if err := validateDeliveryFanOut(len(workflowFiles), workflowJobs); err != nil {
				return ctrl.Result{}, r.finish(ctx, object, deliveryStateFailed, 0, err.Error())
			}
			workflowPaths = append(workflowPaths, workflowPath)
		}
	}
	for _, workflowPath := range workflowPaths {
		if err := r.createWorkflowRun(ctx, gateway, &delivery, workflowPath); err != nil {
			if terminalWorkflowRunCreationError(err) {
				return ctrl.Result{}, r.finish(ctx, object, deliveryStateFailed, 0, err.Error())
			}
			return ctrl.Result{}, err
		}
	}
	r.Logger.Info("completed GitHub webhook discovery", "delivery_id", delivery.DeliveryID, "workflow_runs", len(workflowPaths))
	return ctrl.Result{}, r.finish(ctx, object, deliveryStateCompleted, len(workflowPaths), "")
}

func terminalWorkflowRunCreationError(err error) bool {
	return apierrors.IsConflict(err) || apierrors.IsInvalid(err)
}

func (r *DeliveryReconciler) createWorkflowRun(ctx context.Context, gateway *actionsv1alpha1.ActionsGateway, delivery *queuedDelivery, workflowPath string) error {
	name := workflowRunName(workflowPath, delivery.ReplayID)
	desired := &actionsv1alpha1.WorkflowRun{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: gateway.Namespace},
		Spec: actionsv1alpha1.WorkflowRunSpec{
			GatewayRef: corev1.LocalObjectReference{Name: gateway.Name},
			Source: actionsv1alpha1.WorkflowRunSource{
				Type: actionsv1alpha1.SourceTypeGitHub,
				GitHub: &actionsv1alpha1.GitHubWorkflowRunSource{
					Repository: actionsv1alpha1.GitHubRepository{ID: delivery.Payload.Repository.ID, Owner: delivery.Payload.Repository.Owner.Login, Name: delivery.Payload.Repository.Name},
					Event: actionsv1alpha1.GitHubEvent{
						Name:       actionsv1alpha1.GitHubEventName(delivery.Event.Name),
						Action:     delivery.Event.Action,
						DeliveryID: delivery.DeliveryID,
					},
					Revision: actionsv1alpha1.GitRevision{SHA: delivery.Event.SHA, Ref: delivery.Event.Ref, HeadRef: delivery.Event.HeadRef},
				},
			},
			WorkflowPath: workflowPath,
		},
	}
	if err := r.Create(ctx, desired); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return err
		}
		existing := &actionsv1alpha1.WorkflowRun{}
		if err := r.APIReader.Get(ctx, client.ObjectKeyFromObject(desired), existing); err != nil {
			return err
		}
		if !apiequality.Semantic.DeepEqual(existing.Spec, desired.Spec) {
			return apierrors.NewConflict(
				actionsv1alpha1.GroupVersion.WithResource("workflowruns").GroupResource(),
				name,
				errors.New("existing WorkflowRun does not match the webhook delivery"),
			)
		}
	}
	return nil
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

func (r *DeliveryReconciler) SetupWithManager(manager ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(manager).
		For(&corev1.ConfigMap{}, builder.WithPredicates(predicate.NewPredicateFuncs(isWebhookDelivery))).
		Complete(r)
}

func isWebhookDelivery(object client.Object) bool {
	return object.GetLabels()[deliveryLabel] == "true"
}
