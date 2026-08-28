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
	"testing"
	"time"

	actionsv1alpha1 "github.com/kelos-dev/open-actions/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestGitHubHandlerRecordsAcceptedRequest(t *testing.T) {
	body, err := os.ReadFile(filepath.Join("testdata", "github", "push.json"))
	if err != nil {
		t.Fatal(err)
	}
	payload := &payload{}
	if err := json.Unmarshal(body, payload); err != nil {
		t.Fatal(err)
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
		Spec: actionsv1alpha1.ProjectSpec{Source: actionsv1alpha1.ProjectSource{
			Type: actionsv1alpha1.SourceTypeGitHub,
			GitHub: &actionsv1alpha1.GitHubAppConfiguration{
				InstallationID: payload.Installation.ID,
				WebhookSecretRef: corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: "github"}, Key: "webhook-secret",
				},
			},
		}},
		Status: actionsv1alpha1.ProjectStatus{Conditions: []metav1.Condition{{
			Type: actionsv1alpha1.ProjectConditionConfigured, Status: metav1.ConditionTrue, ObservedGeneration: 1,
		}}},
	}
	secretValue := []byte("secret")
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "github", Namespace: project.Namespace},
		Data:       map[string][]byte{"webhook-secret": secretValue},
	}
	clusterClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(project, secret).Build()
	metrics := &recordingWebhookMetrics{}
	base := time.Date(2026, 8, 28, 10, 0, 0, 0, time.UTC)
	nowCalls := 0
	handler := &GitHubHandler{
		Client: clusterClient, APIReader: clusterClient,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Metrics: metrics,
		Now: func() time.Time {
			nowCalls++
			return base.Add(time.Duration(nowCalls-1) * 2 * time.Second)
		},
	}
	digest := hmac.New(sha256.New, secretValue)
	digest.Write(body)
	request := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
	request.Header.Set("X-GitHub-Event", "push")
	request.Header.Set("X-GitHub-Delivery", "delivery-push")
	request.Header.Set("X-Hub-Signature-256", "sha256="+hex.EncodeToString(digest.Sum(nil)))
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)
	if response.Code != http.StatusAccepted {
		t.Fatalf("response status = %d, body = %s", response.Code, response.Body.String())
	}
	if metrics.requests != 1 || metrics.requestEvent != "push" || metrics.requestResult != "accepted" || metrics.requestDuration != 2*time.Second {
		t.Fatalf("webhook request metric = count %d, event %q, result %q, duration %s", metrics.requests, metrics.requestEvent, metrics.requestResult, metrics.requestDuration)
	}
}

func TestDeliveryFinishRecordsDurationOnce(t *testing.T) {
	base := time.Date(2026, 8, 28, 10, 0, 0, 0, time.UTC)
	deliveryData, err := json.Marshal(queuedDelivery{
		ProjectName: "project", ProjectUID: "project-uid",
		Repository: deliveryRepository{ID: 1, Owner: "acme", Name: "example"},
		Event:      normalizedEvent{Name: "push"}, ReplayID: "replay", DeliveryID: "delivery",
	})
	if err != nil {
		t.Fatal(err)
	}
	delivery := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name: "delivery", Namespace: "default", CreationTimestamp: metav1.NewTime(base), Labels: map[string]string{deliveryLabel: "true"},
		},
		Data: map[string]string{deliveryDataKey: string(deliveryData)},
	}
	scheme := deliveryTestScheme(t)
	clusterClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(delivery).Build()
	metrics := &recordingWebhookMetrics{}
	reconciler := &DeliveryReconciler{
		Client: clusterClient, APIReader: clusterClient, Metrics: metrics,
		Now: func() time.Time { return base.Add(12 * time.Second) },
	}
	if err := reconciler.finish(context.Background(), delivery, deliveryStateCompleted, 1, ""); err != nil {
		t.Fatal(err)
	}
	stored := &corev1.ConfigMap{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(delivery), stored); err != nil {
		t.Fatal(err)
	}
	if err := reconciler.finish(context.Background(), stored, deliveryStateCompleted, 1, ""); err != nil {
		t.Fatal(err)
	}
	if metrics.deliveries != 1 || metrics.deliveryEvent != "push" || metrics.deliveryResult != "completed" || metrics.deliveryDuration != 12*time.Second {
		t.Fatalf("webhook delivery metric = count %d, event %q, result %q, duration %s", metrics.deliveries, metrics.deliveryEvent, metrics.deliveryResult, metrics.deliveryDuration)
	}
}

type recordingWebhookMetrics struct {
	requests         int
	requestEvent     string
	requestResult    string
	requestDuration  time.Duration
	deliveries       int
	deliveryEvent    string
	deliveryResult   string
	deliveryDuration time.Duration
}

func (m *recordingWebhookMetrics) WorkflowRunCompleted(_ *actionsv1alpha1.WorkflowRunStatus, _ *actionsv1alpha1.WorkflowRun) {
}

func (m *recordingWebhookMetrics) WorkflowJobScheduled(_ *actionsv1alpha1.WorkflowJob, _ time.Time) {
}

func (m *recordingWebhookMetrics) WorkflowJobUpdated(_ *actionsv1alpha1.WorkflowJobStatus, _ *actionsv1alpha1.WorkflowJob) {
}

func (m *recordingWebhookMetrics) WebhookRequest(event, result string, duration time.Duration) {
	m.requests++
	m.requestEvent = event
	m.requestResult = result
	m.requestDuration = duration
}

func (m *recordingWebhookMetrics) WebhookDelivery(_, _, event, result string, duration time.Duration) {
	m.deliveries++
	m.deliveryEvent = event
	m.deliveryResult = result
	m.deliveryDuration = duration
}
