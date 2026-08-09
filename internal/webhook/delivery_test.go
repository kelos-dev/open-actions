package webhook

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	actionsv1alpha1 "github.com/kelos-dev/open-actions/api/v1alpha1"
	githubclient "github.com/kelos-dev/open-actions/internal/github"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestWebhookReplayIDUsesSignedBody(t *testing.T) {
	scheme := deliveryTestScheme(t)
	project := &actionsv1alpha1.Project{
		TypeMeta:   metav1.TypeMeta{APIVersion: actionsv1alpha1.GroupVersion.String(), Kind: "Project"},
		ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: "default", UID: "project-uid"},
	}
	clusterClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(project).Build()
	handler := &GitHubHandler{Client: clusterClient, APIReader: clusterClient}
	event := &payload{}
	event.Repository.ID = 1
	event.Repository.Name = "example"
	event.Repository.Owner.Login = "acme"
	body := []byte(`{"after":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","repository":{"id":1,"name":"example"}}`)
	normalized := normalizedEvent{Name: "push", SHA: strings.Repeat("a", 40), Ref: "refs/heads/main"}

	if err := handler.enqueueDelivery(context.Background(), project, event, normalized, "original-delivery", body); err != nil {
		t.Fatal(err)
	}
	if err := handler.enqueueDelivery(context.Background(), project, event, normalized, "replay-delivery", body); err != nil {
		t.Fatalf("signed-body replay was not idempotent: %v", err)
	}
	deliveries := &corev1.ConfigMapList{}
	if err := clusterClient.List(context.Background(), deliveries); err != nil {
		t.Fatal(err)
	}
	if len(deliveries.Items) != 1 {
		t.Fatalf("delivery ConfigMaps = %d, want 1", len(deliveries.Items))
	}
	queued := queuedDelivery{}
	if err := json.Unmarshal([]byte(deliveries.Items[0].Data[deliveryDataKey]), &queued); err != nil {
		t.Fatal(err)
	}
	if queued.ReplayID != webhookReplayID(body) || queued.DeliveryID != "original-delivery" {
		t.Fatalf("queued delivery = %#v", queued)
	}
}

func TestCreateWorkflowRunIsIdempotent(t *testing.T) {
	scheme := deliveryTestScheme(t)
	project := &actionsv1alpha1.Project{ObjectMeta: metav1.ObjectMeta{Name: "project", Namespace: "default"}}
	event := &payload{}
	event.Repository.ID = 1
	event.Repository.Owner.Login = "acme"
	event.Repository.Name = "example"
	normalized := normalizedEvent{Name: "push", SHA: strings.Repeat("a", 40), Ref: "refs/heads/main"}
	delivery := &queuedDelivery{Payload: *event, Event: normalized, ReplayID: "replay", DeliveryID: "delivery"}

	clusterClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	reconciler := &DeliveryReconciler{Client: clusterClient, APIReader: clusterClient}
	if err := reconciler.createWorkflowRun(context.Background(), project, delivery, ".open-actions/workflows/ci.yaml"); err != nil {
		t.Fatal(err)
	}
	if err := reconciler.createWorkflowRun(context.Background(), project, delivery, ".open-actions/workflows/ci.yaml"); err != nil {
		t.Fatalf("matching replay failed: %v", err)
	}

	workflowPath := ".open-actions/workflows/deploy.yaml"
	conflicting := &actionsv1alpha1.WorkflowRun{
		ObjectMeta: metav1.ObjectMeta{Name: workflowRunName(workflowPath, "conflict"), Namespace: project.Namespace},
		Spec: actionsv1alpha1.WorkflowRunSpec{
			ProjectRef:   corev1.LocalObjectReference{Name: "other-project"},
			Source:       actionsv1alpha1.WorkflowRunSource{Type: actionsv1alpha1.SourceTypeGitHub, GitHub: &actionsv1alpha1.GitHubWorkflowRunSource{Repository: actionsv1alpha1.GitHubRepository{ID: 2}}},
			WorkflowPath: workflowPath,
		},
	}
	if err := clusterClient.Create(context.Background(), conflicting); err != nil {
		t.Fatal(err)
	}
	conflictingDelivery := *delivery
	conflictingDelivery.ReplayID = "conflict"
	err := reconciler.createWorkflowRun(context.Background(), project, &conflictingDelivery, workflowPath)
	if !apierrors.IsConflict(err) {
		t.Fatalf("conflicting WorkflowRun error = %v, want conflict", err)
	}
}

func TestInvalidWorkflowRunCreationIsTerminal(t *testing.T) {
	err := apierrors.NewInvalid(actionsv1alpha1.GroupVersion.WithKind("WorkflowRun").GroupKind(), "ci", nil)
	if !terminalWorkflowRunCreationError(err) {
		t.Fatal("invalid WorkflowRun creation was not terminal")
	}
	if terminalWorkflowRunCreationError(errors.New("admission unavailable")) {
		t.Fatal("transient WorkflowRun creation failure was terminal")
	}
}

func TestCreateWorkflowRunReplayUsesLiveReader(t *testing.T) {
	scheme := deliveryTestScheme(t)
	project := &actionsv1alpha1.Project{ObjectMeta: metav1.ObjectMeta{Name: "project", Namespace: "default"}}
	event := &payload{}
	event.Repository.ID = 1
	event.Repository.Owner.Login = "acme"
	event.Repository.Name = "example"
	normalized := normalizedEvent{Name: "push", SHA: strings.Repeat("a", 40), Ref: "refs/heads/main"}
	workflowPath := ".open-actions/workflows/ci.yaml"
	delivery := &queuedDelivery{Payload: *event, Event: normalized, ReplayID: "replay", DeliveryID: "delivery"}
	existing := &actionsv1alpha1.WorkflowRun{
		ObjectMeta: metav1.ObjectMeta{Name: workflowRunName(workflowPath, delivery.ReplayID), Namespace: project.Namespace},
		Spec: actionsv1alpha1.WorkflowRunSpec{
			ProjectRef: corev1.LocalObjectReference{Name: project.Name},
			Source: actionsv1alpha1.WorkflowRunSource{
				Type: actionsv1alpha1.SourceTypeGitHub,
				GitHub: &actionsv1alpha1.GitHubWorkflowRunSource{
					Repository: actionsv1alpha1.GitHubRepository{ID: 1, Owner: "acme", Name: "example"},
					Event:      actionsv1alpha1.GitHubEvent{Name: "push", DeliveryID: delivery.DeliveryID},
					Revision:   actionsv1alpha1.GitRevision{SHA: normalized.SHA, Ref: normalized.Ref},
				},
			},
			WorkflowPath: workflowPath,
		},
	}
	cachedClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	liveReader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(existing).Build()
	reconciler := &DeliveryReconciler{
		Client:    &workflowRunAlreadyExistsClient{Client: cachedClient},
		APIReader: liveReader,
	}
	if err := reconciler.createWorkflowRun(context.Background(), project, delivery, workflowPath); err != nil {
		t.Fatalf("matching replay failed against stale cache: %v", err)
	}
}

type workflowRunAlreadyExistsClient struct {
	client.Client
}

func (c *workflowRunAlreadyExistsClient) Create(_ context.Context, object client.Object, _ ...client.CreateOption) error {
	return apierrors.NewAlreadyExists(actionsv1alpha1.GroupVersion.WithResource("workflowruns").GroupResource(), object.GetName())
}

func TestWorkflowRunNameIsStableAndBounded(t *testing.T) {
	name := workflowRunName(strings.Repeat("workflow", 20)+".yaml", "replay")
	if len(name) > 63 {
		t.Errorf("name has %d characters", len(name))
	}
	if name != workflowRunName(strings.Repeat("workflow", 20)+".yaml", "replay") {
		t.Error("name is not stable")
	}
	if suffix := name[len(name)-52:]; suffix != strings.ToLower(digestEncoding.EncodeToString(sha256Digest("replay|"+strings.Repeat("workflow", 20)+".yaml"))) {
		t.Errorf("name does not contain the full replay and path digest")
	}
}

func TestMissingWorkflowDirectory(t *testing.T) {
	missing := fmt.Errorf("list directory: %w", &githubclient.APIError{StatusCode: 404, Status: "404 Not Found", Message: "Not Found"})
	if !missingWorkflowDirectory(missing) {
		t.Fatal("missing directory was not recognized")
	}
	invalidRevision := fmt.Errorf("list directory: %w", &githubclient.APIError{StatusCode: 404, Status: "404 Not Found", Message: "No commit found for the ref deadbeef"})
	if missingWorkflowDirectory(invalidRevision) {
		t.Fatal("invalid revision was treated as a missing directory")
	}
	transient := fmt.Errorf("list directory: %w", &githubclient.APIError{StatusCode: 503, Status: "503 Service Unavailable"})
	if missingWorkflowDirectory(transient) {
		t.Fatal("transient error was treated as a missing directory")
	}
}

func sha256Digest(value string) []byte {
	digest := sha256.Sum256([]byte(value))
	return digest[:]
}

func TestWorkflowFileExtensionIsCaseSensitive(t *testing.T) {
	for _, path := range []string{"ci.yaml", "ci.yml"} {
		if !workflowFile(path) {
			t.Errorf("workflowFile(%q) = false", path)
		}
	}
	for _, path := range []string{"ci.YAML", "ci.YML", "ci.txt"} {
		if workflowFile(path) {
			t.Errorf("workflowFile(%q) = true", path)
		}
	}
}

func TestDeliveryFanOutLimits(t *testing.T) {
	for _, tt := range []struct {
		name          string
		workflowFiles int
		workflowJobs  int
		wantError     bool
	}{
		{name: "maximum", workflowFiles: maxWorkflowFiles, workflowJobs: maxWorkflowJobs},
		{name: "too many workflow files", workflowFiles: maxWorkflowFiles + 1, wantError: true},
		{name: "too many workflow jobs", workflowFiles: 1, workflowJobs: maxWorkflowJobs + 1, wantError: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			err := validateDeliveryFanOut(tt.workflowFiles, tt.workflowJobs)
			if (err != nil) != tt.wantError {
				t.Fatalf("validateDeliveryFanOut() error = %v, wantError = %v", err, tt.wantError)
			}
		})
	}
}

func TestTerminalDeliveryRetention(t *testing.T) {
	now := time.Date(2026, 8, 8, 12, 0, 0, 0, time.UTC)
	for _, tt := range []struct {
		name        string
		finishedAt  time.Time
		wantDeleted bool
		wantRequeue time.Duration
	}{
		{name: "recent", finishedAt: now.Add(-12 * time.Hour), wantRequeue: 12 * time.Hour},
		{name: "expired", finishedAt: now.Add(-25 * time.Hour), wantDeleted: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			scheme := deliveryTestScheme(t)
			delivery := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{
				Name: "delivery-test", Namespace: "default", Labels: map[string]string{deliveryLabel: "true"},
			}, Data: map[string]string{
				deliveryStateKey: deliveryStateCompleted, deliveryFinishedKey: tt.finishedAt.Format(time.RFC3339),
			}}
			clusterClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(delivery).Build()
			reconciler := &DeliveryReconciler{Client: clusterClient, Now: func() time.Time { return now }}
			result, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(delivery)})
			if err != nil {
				t.Fatal(err)
			}
			if result.RequeueAfter != tt.wantRequeue {
				t.Fatalf("requeue after = %v, want %v", result.RequeueAfter, tt.wantRequeue)
			}
			err = clusterClient.Get(context.Background(), client.ObjectKeyFromObject(delivery), &corev1.ConfigMap{})
			if tt.wantDeleted && !apierrors.IsNotFound(err) {
				t.Fatalf("expired delivery still exists: %v", err)
			}
			if !tt.wantDeleted && err != nil {
				t.Fatalf("recent delivery was deleted: %v", err)
			}
		})
	}
}

func deliveryTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := actionsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	return scheme
}
