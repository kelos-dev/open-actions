package webhook

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	actionsv1alpha1 "github.com/kelos-dev/open-actions/api/v1alpha1"
	"github.com/kelos-dev/open-actions/internal/eventsnapshot"
	githubclient "github.com/kelos-dev/open-actions/internal/github"
	"github.com/kelos-dev/open-actions/internal/gitrepository"
	"github.com/kelos-dev/open-actions/internal/workflowrun"
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
	stored := &corev1.ConfigMap{}
	key := client.ObjectKey{Namespace: project.Namespace, Name: webhookDeliveryName(body)}
	if err := clusterClient.Get(context.Background(), key, stored); err != nil {
		t.Fatal(err)
	}
	stored.Data[deliveryRevisionKey] = strings.Repeat("b", 40)
	if err := clusterClient.Update(context.Background(), stored); err != nil {
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

func TestEnqueueDeliveryStoresBoundedMetadataOnce(t *testing.T) {
	scheme := deliveryTestScheme(t)
	project := &actionsv1alpha1.Project{ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: "default", UID: "project-uid"}}
	clusterClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(project).Build()
	handler := &GitHubHandler{Client: clusterClient, APIReader: clusterClient}
	body := strings.Repeat("😀", maxEventBodyLength)
	event := &payload{}
	event.Repository.ID = 1
	event.Repository.Owner.Login = "acme"
	event.Repository.Name = "example"
	event.Repository.DefaultBranch = "main"
	event.Issue.Number = 17
	event.Issue.Body = body
	event.Comment.Body = body
	normalized := normalizedEvent{
		Name: "issue_comment", Action: "created", Ref: "refs/heads/main", ResolveRef: "main",
		Issue: &normalizedIssue{Number: 17, Body: body}, Comment: &normalizedComment{Body: body},
	}
	signedBody := []byte(`{"delivery":"bounded"}`)
	if err := handler.enqueueDelivery(context.Background(), project, event, normalized, "delivery", signedBody); err != nil {
		t.Fatal(err)
	}
	stored := &corev1.ConfigMap{}
	key := client.ObjectKey{Namespace: project.Namespace, Name: webhookDeliveryName(signedBody)}
	if err := clusterClient.Get(context.Background(), key, stored); err != nil {
		t.Fatal(err)
	}
	data := stored.Data[deliveryDataKey]
	if len(data) > maxDeliveryBytes {
		t.Fatalf("delivery size = %d, maximum = %d", len(data), maxDeliveryBytes)
	}
	document := map[string]any{}
	if err := json.Unmarshal([]byte(data), &document); err != nil {
		t.Fatal(err)
	}
	if _, found := document["payload"]; found {
		t.Fatal("queued delivery persisted the parsed webhook payload")
	}
	if document["repository"].(map[string]any)["name"] != "example" {
		t.Fatalf("queued repository = %#v", document["repository"])
	}
}

func TestQueuedDeliveryReadsRepositoryFromPayload(t *testing.T) {
	data := []byte(`{"projectName":"default","projectUID":"project-uid","payload":{"repository":{"id":1,"owner":{"login":"acme"},"name":"example","default_branch":"main"}},"event":{"name":"push","sha":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","ref":"refs/heads/main"},"replayID":"replay","deliveryID":"delivery"}`)
	delivery := queuedDelivery{}
	if err := json.Unmarshal(data, &delivery); err != nil {
		t.Fatal(err)
	}
	if delivery.Repository.ID != 1 || delivery.Repository.Owner != "acme" || delivery.Repository.Name != "example" {
		t.Fatalf("delivery repository = %#v", delivery.Repository)
	}
	if delivery.EventSnapshot != "" {
		t.Fatalf("legacy delivery event snapshot = %q", delivery.EventSnapshot)
	}
	pullRequestData := []byte(`{"projectName":"default","projectUID":"project-uid","payload":{"repository":{"id":1,"owner":{"login":"acme"},"name":"example","default_branch":"main"}},"event":{"name":"pull_request","ref":"refs/pull/7/merge","resolveRef":"refs/pull/7/merge","headSHA":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"},"replayID":"replay","deliveryID":"delivery"}`)
	if err := json.Unmarshal(pullRequestData, &delivery); err != nil {
		t.Fatal(err)
	}
	events := deliveryEvents(delivery.Event)
	if !delivery.Event.MergeRevision || len(events) != 1 || events[0].Name != "pull_request" {
		t.Fatalf("pull request delivery = %#v, events = %#v", delivery, events)
	}
}

func TestDeliveryEventsKeepIntegrationMetadataOnPullRequest(t *testing.T) {
	mergeBaseSHA := strings.Repeat("c", 40)
	events := deliveryEvents(normalizedEvent{
		Name: "pull_request", Action: "synchronize", Ref: "refs/pull/42/merge", MergeRevision: true, MergeBaseSHA: mergeBaseSHA,
		PullRequest: &normalizedPullRequest{BaseSHA: strings.Repeat("a", 40), HeadSHA: strings.Repeat("b", 40), BaseRef: "main"},
	})
	if len(events) != 2 {
		t.Fatalf("delivery events = %#v", events)
	}
	if events[0].Name != "pull_request" || events[0].MergeBaseSHA != mergeBaseSHA {
		t.Fatalf("pull request event = %#v", events[0])
	}
	if events[1].Name != "pull_request_target" || events[1].MergeBaseSHA != "" {
		t.Fatalf("pull request target event = %#v", events[1])
	}
}

func TestRerequestedCheckCreatesFailedJobRerun(t *testing.T) {
	scheme := deliveryTestScheme(t)
	project := &actionsv1alpha1.Project{ObjectMeta: metav1.ObjectMeta{Name: "project", Namespace: "default", UID: "project-uid"}}
	root := &actionsv1alpha1.WorkflowRun{
		ObjectMeta: metav1.ObjectMeta{Name: "ci", Namespace: project.Namespace, UID: "root-uid", Labels: map[string]string{actionsv1alpha1.LabelWorkflowRunRootUID: "root-uid"}},
		Spec: actionsv1alpha1.WorkflowRunSpec{
			ProjectRef:   corev1.LocalObjectReference{Name: project.Name},
			WorkflowPath: ".open-actions/workflows/ci.yaml",
			Source: actionsv1alpha1.WorkflowRunSource{Type: actionsv1alpha1.SourceTypeGitHub, GitHub: &actionsv1alpha1.GitHubWorkflowRunSource{
				Repository: actionsv1alpha1.GitHubRepository{ID: 1, Owner: "acme", Name: "example"},
				Event:      actionsv1alpha1.GitHubEvent{Name: actionsv1alpha1.GitHubEventNamePush, DeliveryID: "original-delivery"},
				Revision:   actionsv1alpha1.GitRevision{SHA: strings.Repeat("a", 40), Ref: "refs/heads/main"},
			}},
		},
		Status: actionsv1alpha1.WorkflowRunStatus{
			Source:     &actionsv1alpha1.WorkflowRunSourceStatus{GitHub: &actionsv1alpha1.GitHubWorkflowRunStatus{CheckRun: &actionsv1alpha1.GitHubCheckRunStatus{ID: 42}}},
			Jobs:       &actionsv1alpha1.WorkflowRunJobStatus{Total: 3},
			Conditions: []metav1.Condition{{Type: actionsv1alpha1.WorkflowRunConditionSucceeded, Status: metav1.ConditionFalse, Reason: "JobFailed"}},
		},
	}
	failed := &actionsv1alpha1.WorkflowJob{
		ObjectMeta: metav1.ObjectMeta{Name: "failed", Namespace: project.Namespace, Labels: map[string]string{actionsv1alpha1.LabelWorkflowRunUID: string(root.UID)}},
		Spec:       actionsv1alpha1.WorkflowJobSpec{JobID: "unit-matrix-2", Matrix: &actionsv1alpha1.WorkflowJobMatrix{LogicalJobID: "unit", Values: map[string]string{"os": "linux"}}},
		Status:     actionsv1alpha1.WorkflowJobStatus{Result: actionsv1alpha1.WorkflowJobResultFailure, Conditions: []metav1.Condition{{Type: actionsv1alpha1.WorkflowJobConditionSucceeded, Status: metav1.ConditionFalse}}},
	}
	succeeded := &actionsv1alpha1.WorkflowJob{
		ObjectMeta: metav1.ObjectMeta{Name: "succeeded", Namespace: project.Namespace, Labels: map[string]string{actionsv1alpha1.LabelWorkflowRunUID: string(root.UID)}},
		Spec:       actionsv1alpha1.WorkflowJobSpec{JobID: "lint"},
		Status:     actionsv1alpha1.WorkflowJobStatus{Result: actionsv1alpha1.WorkflowJobResultSuccess, Conditions: []metav1.Condition{{Type: actionsv1alpha1.WorkflowJobConditionSucceeded, Status: metav1.ConditionTrue}}},
	}
	dependent := &actionsv1alpha1.WorkflowJob{
		ObjectMeta: metav1.ObjectMeta{Name: "report", Namespace: project.Namespace, Labels: map[string]string{actionsv1alpha1.LabelWorkflowRunUID: string(root.UID)}},
		Spec:       actionsv1alpha1.WorkflowJobSpec{JobID: "report", Needs: []string{"unit"}},
		Status:     actionsv1alpha1.WorkflowJobStatus{Result: actionsv1alpha1.WorkflowJobResultSkipped},
	}
	delivery := queuedDelivery{
		ProjectName: project.Name,
		ProjectUID:  string(project.UID),
		Repository:  deliveryRepository{ID: 1, Owner: "acme", Name: "example"},
		Rerun:       &normalizedRerun{CheckRunID: 42, RootRunUID: string(root.UID), HeadSHA: strings.Repeat("a", 40)},
		ReplayID:    "rerun-replay",
		DeliveryID:  "rerun-delivery",
	}
	data, err := json.Marshal(delivery)
	if err != nil {
		t.Fatal(err)
	}
	controller := true
	queued := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{
		Name: "delivery-rerun", Namespace: project.Namespace, Labels: map[string]string{deliveryLabel: "true"},
		OwnerReferences: []metav1.OwnerReference{{APIVersion: actionsv1alpha1.GroupVersion.String(), Kind: "Project", Name: project.Name, UID: project.UID, Controller: &controller}},
	}, Data: map[string]string{deliveryDataKey: string(data)}}
	clusterClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(project, root, failed, succeeded, dependent, queued).Build()
	reconciler := &DeliveryReconciler{Client: clusterClient, APIReader: clusterClient, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}

	if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(queued)}); err != nil {
		t.Fatal(err)
	}
	retry := &actionsv1alpha1.WorkflowRun{}
	retryKey := client.ObjectKey{Namespace: project.Namespace, Name: rerunWorkflowRunName(root, 2)}
	if err := clusterClient.Get(context.Background(), retryKey, retry); err != nil {
		t.Fatal(err)
	}
	if retry.Spec.Rerun == nil || retry.Spec.Rerun.Attempt != 2 || retry.Spec.Rerun.RequestID != delivery.DeliveryID || retry.Spec.Rerun.OriginalRunRef.UID != root.UID || retry.Spec.Rerun.PreviousRunRef.UID != root.UID || len(retry.Spec.Rerun.JobIDs) != 2 || retry.Spec.Rerun.JobIDs[0] != "report" || retry.Spec.Rerun.JobIDs[1] != "unit-matrix-2" {
		t.Fatalf("rerun WorkflowRun = %#v", retry.Spec.Rerun)
	}
	storedDelivery := &corev1.ConfigMap{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(queued), storedDelivery); err != nil {
		t.Fatal(err)
	}
	if storedDelivery.Data[deliveryStateKey] != deliveryStateCompleted || storedDelivery.Data[deliveryRunCountKey] != "1" {
		t.Fatalf("delivery state = %#v", storedDelivery.Data)
	}
}

func TestRerunWorkflowJobIDsIncludesTimedOutJobs(t *testing.T) {
	scheme := deliveryTestScheme(t)
	run := &actionsv1alpha1.WorkflowRun{
		ObjectMeta: metav1.ObjectMeta{Name: "ci", Namespace: "default", UID: "run-uid"},
		Status: actionsv1alpha1.WorkflowRunStatus{
			Jobs:       &actionsv1alpha1.WorkflowRunJobStatus{Total: 1, TimedOut: 1},
			Conditions: []metav1.Condition{{Type: actionsv1alpha1.WorkflowRunConditionSucceeded, Status: metav1.ConditionFalse, Reason: "JobTimedOut"}},
		},
	}
	job := &actionsv1alpha1.WorkflowJob{
		ObjectMeta: metav1.ObjectMeta{Name: "build", Namespace: run.Namespace, Labels: map[string]string{actionsv1alpha1.LabelWorkflowRunUID: string(run.UID)}},
		Spec:       actionsv1alpha1.WorkflowJobSpec{JobID: "build"},
		Status: actionsv1alpha1.WorkflowJobStatus{
			Result:     actionsv1alpha1.WorkflowJobResultFailure,
			Conditions: []metav1.Condition{{Type: actionsv1alpha1.WorkflowJobConditionSucceeded, Status: metav1.ConditionFalse, Reason: "JobTimedOut"}},
		},
	}
	clusterClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(run, job).Build()
	reconciler := &DeliveryReconciler{APIReader: clusterClient}
	jobIDs, err := reconciler.rerunWorkflowJobIDs(context.Background(), run)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(jobIDs, []string{"build"}) {
		t.Fatalf("rerun job IDs = %v", jobIDs)
	}
}

func TestConcurrentRerunRequestsDoNotShareAnAttempt(t *testing.T) {
	scheme := deliveryTestScheme(t)
	root := &actionsv1alpha1.WorkflowRun{
		ObjectMeta: metav1.ObjectMeta{
			Name: "ci", Namespace: "default", UID: "root-uid",
			Annotations: map[string]string{eventsnapshot.Annotation: "event-snapshot"},
		},
		Spec: actionsv1alpha1.WorkflowRunSpec{CancelRequested: true},
	}
	immutable := true
	snapshot := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name: "event-snapshot", Namespace: root.Namespace,
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: actionsv1alpha1.GroupVersion.String(), Kind: "WorkflowRun", Name: root.Name, UID: root.UID,
			}},
		},
		Immutable: &immutable,
		Data:      map[string][]byte{eventsnapshot.DataKey: []byte(`{"repository":{"full_name":"acme/example"}}`)},
	}
	clusterClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(snapshot).Build()
	reconciler := &DeliveryReconciler{Client: clusterClient, APIReader: clusterClient}

	if err := reconciler.createRerunWorkflowRun(context.Background(), root, root, 2, "delivery-a", []string{"unit"}); err != nil {
		t.Fatal(err)
	}
	retry := &actionsv1alpha1.WorkflowRun{}
	if err := clusterClient.Get(context.Background(), client.ObjectKey{Namespace: root.Namespace, Name: rerunWorkflowRunName(root, 2)}, retry); err != nil {
		t.Fatal(err)
	}
	if retry.Spec.CancelRequested {
		t.Fatal("rerun retained the previous cancellation request")
	}
	if retry.Annotations[eventsnapshot.Annotation] != snapshot.Name {
		t.Fatalf("rerun event snapshot = %q", retry.Annotations[eventsnapshot.Annotation])
	}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(snapshot), snapshot); err != nil {
		t.Fatal(err)
	}
	foundOwner := false
	for _, owner := range snapshot.OwnerReferences {
		foundOwner = foundOwner || owner.Kind == "WorkflowRun" && owner.Name == retry.Name
	}
	if !foundOwner {
		t.Fatalf("snapshot owners = %#v", snapshot.OwnerReferences)
	}
	if err := reconciler.createRerunWorkflowRun(context.Background(), root, root, 2, "delivery-b", []string{"unit"}); !errors.Is(err, errRerunAttemptClaimed) {
		t.Fatalf("second rerun error = %v", err)
	}
}

func TestRerunDeliveryRepairsSnapshotOwnerBeforeCompleting(t *testing.T) {
	scheme := deliveryTestScheme(t)
	project := &actionsv1alpha1.Project{ObjectMeta: metav1.ObjectMeta{Name: "project", Namespace: "default", UID: "project-uid"}}
	root := &actionsv1alpha1.WorkflowRun{
		ObjectMeta: metav1.ObjectMeta{
			Name: "ci", Namespace: project.Namespace, UID: "root-uid",
			Labels:      map[string]string{actionsv1alpha1.LabelWorkflowRunRootUID: "root-uid"},
			Annotations: map[string]string{eventsnapshot.Annotation: "event-snapshot"},
		},
		Spec: actionsv1alpha1.WorkflowRunSpec{
			ProjectRef: corev1.LocalObjectReference{Name: project.Name},
			Source: actionsv1alpha1.WorkflowRunSource{Type: actionsv1alpha1.SourceTypeGitHub, GitHub: &actionsv1alpha1.GitHubWorkflowRunSource{
				Repository: actionsv1alpha1.GitHubRepository{ID: 1, Owner: "acme", Name: "example"},
				Event:      actionsv1alpha1.GitHubEvent{Name: actionsv1alpha1.GitHubEventNamePush, DeliveryID: "original-delivery"},
				Revision:   actionsv1alpha1.GitRevision{SHA: strings.Repeat("a", 40), Ref: "refs/heads/main"},
			}},
		},
		Status: actionsv1alpha1.WorkflowRunStatus{
			Source:     &actionsv1alpha1.WorkflowRunSourceStatus{GitHub: &actionsv1alpha1.GitHubWorkflowRunStatus{CheckRun: &actionsv1alpha1.GitHubCheckRunStatus{ID: 42}}},
			Conditions: []metav1.Condition{{Type: actionsv1alpha1.WorkflowRunConditionSucceeded, Status: metav1.ConditionTrue}},
		},
	}
	delivery := queuedDelivery{
		ProjectName: project.Name,
		ProjectUID:  string(project.UID),
		Repository:  deliveryRepository{ID: 1, Owner: "acme", Name: "example"},
		Rerun:       &normalizedRerun{CheckRunID: 42, RootRunUID: string(root.UID), HeadSHA: strings.Repeat("a", 40)},
		ReplayID:    "rerun-replay",
		DeliveryID:  "rerun-delivery",
	}
	retry := workflowrun.NewRerun(root, root, 2, delivery.DeliveryID, nil)
	retry.UID = "retry-uid"
	retry.Annotations = map[string]string{eventsnapshot.Annotation: "event-snapshot"}
	immutable := true
	snapshot := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name: "event-snapshot", Namespace: root.Namespace,
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: actionsv1alpha1.GroupVersion.String(), Kind: "WorkflowRun", Name: root.Name, UID: root.UID,
			}},
		},
		Immutable: &immutable,
		Data:      map[string][]byte{eventsnapshot.DataKey: []byte(`{"repository":{"full_name":"acme/example"}}`)},
	}
	data, err := json.Marshal(delivery)
	if err != nil {
		t.Fatal(err)
	}
	controller := true
	queued := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{
		Name: "delivery-rerun", Namespace: project.Namespace, Labels: map[string]string{deliveryLabel: "true"},
		OwnerReferences: []metav1.OwnerReference{{APIVersion: actionsv1alpha1.GroupVersion.String(), Kind: "Project", Name: project.Name, UID: project.UID, Controller: &controller}},
	}, Data: map[string]string{deliveryDataKey: string(data)}}
	clusterClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(project, root, retry, snapshot, queued).Build()
	reconciler := &DeliveryReconciler{Client: clusterClient, APIReader: clusterClient, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}

	if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(queued)}); err != nil {
		t.Fatal(err)
	}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(snapshot), snapshot); err != nil {
		t.Fatal(err)
	}
	foundOwner := false
	for _, owner := range snapshot.OwnerReferences {
		foundOwner = foundOwner || owner.Kind == "WorkflowRun" && owner.Name == retry.Name && owner.UID == retry.UID
	}
	if !foundOwner {
		t.Fatalf("snapshot owners = %#v", snapshot.OwnerReferences)
	}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(queued), queued); err != nil {
		t.Fatal(err)
	}
	if queued.Data[deliveryStateKey] != deliveryStateCompleted || queued.Data[deliveryRunCountKey] != "1" {
		t.Fatalf("delivery state = %#v", queued.Data)
	}
}

func TestLatestRerunAttemptDoesNotRequireIntermediateAttempt(t *testing.T) {
	root := &actionsv1alpha1.WorkflowRun{
		ObjectMeta: metav1.ObjectMeta{Name: "ci", Namespace: "default", UID: "root-uid"},
		Spec: actionsv1alpha1.WorkflowRunSpec{
			ProjectRef: corev1.LocalObjectReference{Name: "project"}, WorkflowPath: ".open-actions/workflows/ci.yaml",
		},
	}
	attempt3 := &actionsv1alpha1.WorkflowRun{
		ObjectMeta: metav1.ObjectMeta{Name: "ci-attempt-3", Namespace: root.Namespace, UID: "attempt-3-uid"},
		Spec:       *root.Spec.DeepCopy(),
	}
	attempt3.Spec.Rerun = &actionsv1alpha1.WorkflowRunRerun{
		OriginalRunRef: actionsv1alpha1.WorkflowRunReference{Name: root.Name, UID: root.UID},
		PreviousRunRef: actionsv1alpha1.WorkflowRunReference{Name: "ci-attempt-2", UID: "attempt-2-uid"},
		Attempt:        3,
	}

	latest, err := latestWorkflowRunAttempt(root, []actionsv1alpha1.WorkflowRun{*root, *attempt3})
	if err != nil {
		t.Fatal(err)
	}
	if latest.Name != attempt3.Name {
		t.Fatalf("latest attempt = %q, want %q", latest.Name, attempt3.Name)
	}
}

func TestRerunWaitsForPreUpgradeLineageLabel(t *testing.T) {
	scheme := deliveryTestScheme(t)
	now := time.Date(2026, 8, 12, 12, 0, 0, 0, time.UTC)
	project := &actionsv1alpha1.Project{ObjectMeta: metav1.ObjectMeta{Name: "project", Namespace: "default", UID: "project-uid"}}
	delivery := queuedDelivery{
		ProjectName: project.Name,
		ProjectUID:  string(project.UID),
		Repository:  deliveryRepository{ID: 1, Owner: "acme", Name: "example"},
		Rerun:       &normalizedRerun{CheckRunID: 42, RootRunUID: "root-uid", HeadSHA: strings.Repeat("a", 40)},
		ReplayID:    "rerun-replay",
		DeliveryID:  "rerun-delivery",
	}
	data, err := json.Marshal(delivery)
	if err != nil {
		t.Fatal(err)
	}
	controller := true
	queued := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{
		Name: "delivery-rerun", Namespace: project.Namespace, CreationTimestamp: metav1.NewTime(now), Labels: map[string]string{deliveryLabel: "true"},
		OwnerReferences: []metav1.OwnerReference{{APIVersion: actionsv1alpha1.GroupVersion.String(), Kind: "Project", Name: project.Name, UID: project.UID, Controller: &controller}},
	}, Data: map[string]string{deliveryDataKey: string(data)}}
	clusterClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(project, queued).Build()
	currentTime := now
	reconciler := &DeliveryReconciler{Client: clusterClient, APIReader: clusterClient, Now: func() time.Time { return currentTime }}

	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(queued)})
	if err != nil {
		t.Fatal(err)
	}
	if result.RequeueAfter != 2*time.Second {
		t.Fatalf("initial requeue after = %v, want 2s", result.RequeueAfter)
	}
	stored := &corev1.ConfigMap{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(queued), stored); err != nil {
		t.Fatal(err)
	}
	if stored.Data[deliveryStateKey] != "" {
		t.Fatalf("waiting delivery state = %q", stored.Data[deliveryStateKey])
	}

	currentTime = now.Add(rerunRootWaitTimeout)
	if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(queued)}); err != nil {
		t.Fatal(err)
	}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(queued), stored); err != nil {
		t.Fatal(err)
	}
	if stored.Data[deliveryStateKey] != deliveryStateFailed || !strings.Contains(stored.Data[deliveryMessageKey], "root-uid") {
		t.Fatalf("expired delivery state = %#v", stored.Data)
	}
}

func TestCreateWorkflowRunIsIdempotent(t *testing.T) {
	scheme := deliveryTestScheme(t)
	project := &actionsv1alpha1.Project{ObjectMeta: metav1.ObjectMeta{Name: "project", Namespace: "default", UID: "project-uid"}}
	event := &payload{}
	event.Repository.ID = 1
	event.Repository.Owner.Login = "acme"
	event.Repository.Name = "example"
	normalized := normalizedEvent{Name: "push", SHA: strings.Repeat("a", 40), Ref: "refs/heads/main"}
	delivery := &queuedDelivery{
		Repository: deliveryRepository{ID: 1, Owner: "acme", Name: "example"}, Event: normalized,
		EventSnapshot: eventSnapshotName("replay"), ReplayID: "replay", DeliveryID: "delivery",
	}
	ttl := int32(604800)
	immutable := true
	snapshot := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: eventSnapshotName(delivery.ReplayID), Namespace: project.Namespace},
		Immutable:  &immutable,
		Data:       map[string][]byte{eventsnapshot.DataKey: []byte(`{"repository":{"full_name":"acme/example"}}`)},
	}

	clusterClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(snapshot).Build()
	reconciler := &DeliveryReconciler{Client: clusterClient, APIReader: clusterClient, WorkflowRunTTLSecondsAfterFinished: &ttl}
	selection := workflowSelection{Path: ".open-actions/workflows/ci.yaml", Event: delivery.Event}
	if err := reconciler.createWorkflowRun(context.Background(), project, delivery, selection); err != nil {
		t.Fatal(err)
	}
	run := &actionsv1alpha1.WorkflowRun{}
	runKey := client.ObjectKey{Namespace: project.Namespace, Name: workflowRunName(".open-actions/workflows/ci.yaml", string(project.UID), delivery.ReplayID)}
	if err := clusterClient.Get(context.Background(), runKey, run); err != nil {
		t.Fatal(err)
	}
	if run.Spec.TTLSecondsAfterFinished == nil || *run.Spec.TTLSecondsAfterFinished != ttl {
		t.Fatalf("WorkflowRun TTL = %v, want %d", run.Spec.TTLSecondsAfterFinished, ttl)
	}
	if run.Annotations[eventsnapshot.Annotation] != snapshot.Name {
		t.Fatalf("WorkflowRun event snapshot = %q, want %q", run.Annotations[eventsnapshot.Annotation], snapshot.Name)
	}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(snapshot), snapshot); err != nil {
		t.Fatal(err)
	}
	foundSnapshotOwner := false
	for _, owner := range snapshot.OwnerReferences {
		foundSnapshotOwner = foundSnapshotOwner || owner.Kind == "WorkflowRun" && owner.Name == run.Name
	}
	if !foundSnapshotOwner {
		t.Fatalf("snapshot owners = %#v", snapshot.OwnerReferences)
	}
	updatedTTL := int32(3600)
	run.Spec.TTLSecondsAfterFinished = &updatedTTL
	if err := clusterClient.Update(context.Background(), run); err != nil {
		t.Fatal(err)
	}
	if err := reconciler.createWorkflowRun(context.Background(), project, delivery, selection); err != nil {
		t.Fatalf("matching replay failed: %v", err)
	}
	if err := clusterClient.Get(context.Background(), runKey, run); err != nil {
		t.Fatal(err)
	}
	if run.Spec.TTLSecondsAfterFinished == nil || *run.Spec.TTLSecondsAfterFinished != updatedTTL {
		t.Fatalf("replayed WorkflowRun TTL = %v, want %d", run.Spec.TTLSecondsAfterFinished, updatedTTL)
	}
	forgedSnapshot := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "forged-snapshot", Namespace: project.Namespace},
		Immutable:  &immutable,
		Data:       map[string][]byte{eventsnapshot.DataKey: []byte(`{"sender":{"login":"attacker"}}`)},
	}
	if err := clusterClient.Create(context.Background(), forgedSnapshot); err != nil {
		t.Fatal(err)
	}
	run.Annotations[eventsnapshot.Annotation] = forgedSnapshot.Name
	if err := clusterClient.Update(context.Background(), run); err != nil {
		t.Fatal(err)
	}
	if err := reconciler.createWorkflowRun(context.Background(), project, delivery, selection); !apierrors.IsConflict(err) {
		t.Fatalf("replay with a different event snapshot error = %v, want conflict", err)
	}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(forgedSnapshot), forgedSnapshot); err != nil {
		t.Fatal(err)
	}
	for _, owner := range forgedSnapshot.OwnerReferences {
		if owner.Kind == "WorkflowRun" && owner.Name == run.Name {
			t.Fatalf("forged snapshot owners = %#v", forgedSnapshot.OwnerReferences)
		}
	}

	workflowPath := ".open-actions/workflows/deploy.yaml"
	conflicting := &actionsv1alpha1.WorkflowRun{
		ObjectMeta: metav1.ObjectMeta{Name: workflowRunName(workflowPath, string(project.UID), "conflict"), Namespace: project.Namespace},
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
	err := reconciler.createWorkflowRun(context.Background(), project, &conflictingDelivery, workflowSelection{Path: workflowPath, Event: conflictingDelivery.Event})
	if !apierrors.IsConflict(err) {
		t.Fatalf("conflicting WorkflowRun error = %v, want conflict", err)
	}
}

func TestMatchingWorkflowRunAcceptsMissingHeadSHA(t *testing.T) {
	desired := &actionsv1alpha1.WorkflowRun{
		ObjectMeta: metav1.ObjectMeta{Name: "ci", Namespace: "default"},
		Spec: actionsv1alpha1.WorkflowRunSpec{
			Source: actionsv1alpha1.WorkflowRunSource{
				Type: actionsv1alpha1.SourceTypeGitHub,
				GitHub: &actionsv1alpha1.GitHubWorkflowRunSource{
					Event: actionsv1alpha1.GitHubEvent{
						Name: actionsv1alpha1.GitHubEventNamePullRequest,
						PullRequest: &actionsv1alpha1.GitHubPullRequest{
							Number: 9, HeadRef: "feature", HeadSHA: strings.Repeat("b", 40), BaseRef: "main",
							HeadRepository: actionsv1alpha1.GitHubRepository{ID: 1, Owner: "acme", Name: "example"},
						},
					},
					Revision: actionsv1alpha1.GitRevision{SHA: strings.Repeat("a", 40), HeadSHA: strings.Repeat("b", 40)},
				},
			},
		},
	}
	existing := desired.DeepCopy()
	existing.Spec.Source.GitHub.Revision.HeadSHA = ""
	existing.Spec.Source.GitHub.Event.PullRequest = nil

	if err := matchingWorkflowRun(existing, desired); err != nil {
		t.Fatalf("compatible WorkflowRun source did not match its delivery replay: %v", err)
	}

	existing.Spec.Source.GitHub.Revision.HeadSHA = strings.Repeat("c", 40)
	if err := matchingWorkflowRun(existing, desired); !apierrors.IsConflict(err) {
		t.Fatalf("WorkflowRun with a different head SHA error = %v, want conflict", err)
	}
}

func TestMatchingWorkflowRunIgnoresForkApprovalState(t *testing.T) {
	desired := &actionsv1alpha1.WorkflowRun{
		ObjectMeta: metav1.ObjectMeta{Name: "ci", Namespace: "default"},
		Spec: actionsv1alpha1.WorkflowRunSpec{
			ForkPullRequest: &actionsv1alpha1.WorkflowRunForkPullRequest{RequireApproval: true},
		},
	}
	existing := desired.DeepCopy()
	existing.Spec.ForkPullRequest.Approved = true
	if err := matchingWorkflowRun(existing, desired); err != nil {
		t.Fatalf("approved WorkflowRun did not match its delivery replay: %v", err)
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
	project := &actionsv1alpha1.Project{ObjectMeta: metav1.ObjectMeta{Name: "project", Namespace: "default", UID: "project-uid"}}
	event := &payload{}
	event.Repository.ID = 1
	event.Repository.Owner.Login = "acme"
	event.Repository.Name = "example"
	normalized := normalizedEvent{Name: "push", SHA: strings.Repeat("a", 40), Ref: "refs/heads/main"}
	workflowPath := ".open-actions/workflows/ci.yaml"
	delivery := &queuedDelivery{Repository: deliveryRepository{ID: 1, Owner: "acme", Name: "example"}, Event: normalized, ReplayID: "replay", DeliveryID: "delivery"}
	existing := &actionsv1alpha1.WorkflowRun{
		ObjectMeta: metav1.ObjectMeta{Name: fullDigestWorkflowRunName(workflowPath, delivery.ReplayID), Namespace: project.Namespace},
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
	if err := reconciler.createWorkflowRun(context.Background(), project, delivery, workflowSelection{Path: workflowPath, Event: delivery.Event}); err != nil {
		t.Fatalf("matching replay failed against stale cache: %v", err)
	}
}

func TestForkPullRequestPolicySnapshotsSecureDefaultsAndPrivateRepositorySettings(t *testing.T) {
	falseValue := false
	for _, test := range []struct {
		name          string
		configuration *actionsv1alpha1.GitHubForkPullRequestPolicy
		dependabot    bool
		wantEnabled   bool
		want          actionsv1alpha1.WorkflowRunForkPullRequest
	}{
		{
			name: "public repository secure defaults", wantEnabled: true,
			want: actionsv1alpha1.WorkflowRunForkPullRequest{RequireApproval: true},
		},
		{
			name: "private repository trusted forks",
			configuration: &actionsv1alpha1.GitHubForkPullRequestPolicy{
				RequireApproval: &falseValue, SendWriteTokens: true, SendSecrets: true,
			},
			wantEnabled: true,
			want:        actionsv1alpha1.WorkflowRunForkPullRequest{SendWriteTokens: true, SendSecrets: true},
		},
		{
			name:          "private repository forks disabled",
			configuration: &actionsv1alpha1.GitHubForkPullRequestPolicy{Enabled: &falseValue},
			want:          actionsv1alpha1.WorkflowRunForkPullRequest{RequireApproval: true},
		},
		{
			name: "Dependabot keeps restricted credentials",
			configuration: &actionsv1alpha1.GitHubForkPullRequestPolicy{
				RequireApproval: &falseValue, SendWriteTokens: true, SendSecrets: true,
			},
			dependabot: true, wantEnabled: true,
			want: actionsv1alpha1.WorkflowRunForkPullRequest{},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			project := &actionsv1alpha1.Project{Spec: actionsv1alpha1.ProjectSpec{Source: actionsv1alpha1.ProjectSource{
				GitHub: &actionsv1alpha1.GitHubAppConfiguration{ForkPullRequests: test.configuration},
			}}}
			enabled, policy := forkPullRequestPolicy(project, test.dependabot)
			if enabled != test.wantEnabled || policy == nil || *policy != test.want {
				t.Fatalf("policy = enabled %t, %#v", enabled, policy)
			}
		})
	}
}

func TestDeliveryBuildsPullRequestRevisionFromPinnedCommits(t *testing.T) {
	now := time.Date(2026, 8, 9, 23, 0, 0, 0, time.UTC)
	workflowData := []byte("name: CI\non: pull_request\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: make test\n")
	gitRoot, revision := testPullRequestGitRepository(t, workflowData, nil, false)
	compareCalls := 0
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/app/installations/2/access_tokens":
			fmt.Fprint(writer, `{"token":"installation-token"}`)
		case "/repos/acme/example/compare/" + revision.BaseSHA + "..." + revision.HeadSHA:
			compareCalls++
			fmt.Fprintf(writer, `{"merge_base_commit":{"sha":%q}}`, revision.MergeBaseSHA)
		case "/repos/acme/example/contents/.open-actions/workflows":
			fmt.Fprint(writer, `[{"name":"ci.yaml","path":".open-actions/workflows/ci.yaml","type":"file"}]`)
		case "/repos/acme/example/contents/.open-actions/workflows/ci.yaml":
			fmt.Fprintf(writer, `{"encoding":"base64","content":%q}`, base64.StdEncoding.EncodeToString(workflowData))
		default:
			http.NotFound(writer, request)
		}
	}))
	defer server.Close()
	clusterClient, reconciler, handler, project := newPullRequestDeliveryTest(t, server, now)
	reconciler.GitRepository, _ = gitrepository.NewClient(gitRoot)
	deliveryKey := enqueuePullRequestDelivery(t, handler, clusterClient, project, now, revision.BaseSHA, revision.HeadSHA, []byte(`{"delivery":"local-merge"}`))

	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: deliveryKey})
	if err != nil {
		t.Fatal(err)
	}
	if result.RequeueAfter != 0 || compareCalls != 1 {
		t.Fatalf("result = %#v, compare calls = %d", result, compareCalls)
	}
	stored := &corev1.ConfigMap{}
	if err := clusterClient.Get(context.Background(), deliveryKey, stored); err != nil {
		t.Fatal(err)
	}
	if stored.Data[deliveryStateKey] != deliveryStateCompleted || !validGitSHA(stored.Data[deliveryRevisionKey]) || stored.Data[deliveryMergeBaseKey] != revision.MergeBaseSHA {
		t.Fatalf("delivery data = %#v", stored.Data)
	}
	runs := &actionsv1alpha1.WorkflowRunList{}
	if err := clusterClient.List(context.Background(), runs); err != nil {
		t.Fatal(err)
	}
	if len(runs.Items) != 1 {
		t.Fatalf("WorkflowRuns = %d, want 1", len(runs.Items))
	}
	got := runs.Items[0].Spec.Source.GitHub.Revision
	if got.SHA != stored.Data[deliveryRevisionKey] || got.HeadSHA != revision.HeadSHA || got.BaseSHA != revision.BaseSHA || got.MergeBaseSHA != revision.MergeBaseSHA || got.BaseRef != "main" {
		t.Fatalf("WorkflowRun revision = %#v", got)
	}
	policy := runs.Items[0].Spec.ForkPullRequest
	if policy == nil || !policy.RequireApproval || policy.Approved || policy.SendWriteTokens || policy.SendSecrets {
		t.Fatalf("fork pull request policy = %#v", policy)
	}
}

func TestDeliveryCachesPullRequestMergeRevision(t *testing.T) {
	now := time.Date(2026, 8, 25, 13, 0, 0, 0, time.UTC)
	workflowData := []byte("name: CI\non: pull_request\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: make test\n")
	gitRoot, revision := testPullRequestGitRepository(t, workflowData, nil, false)
	gitPath, err := exec.LookPath("git")
	if err != nil {
		t.Fatal(err)
	}
	commandDirectory := t.TempDir()
	commandLog := filepath.Join(commandDirectory, "commands.log")
	gitWrapper := filepath.Join(commandDirectory, "git")
	wrapperData := fmt.Sprintf("#!/bin/sh\nif [ \"$1\" = \"ls-tree\" ] || [ \"$1\" = \"show\" ]; then\n  printf '%%s\\n' \"$1\" >> \"$OPEN_ACTIONS_TEST_GIT_LOG\"\nfi\nexec %q \"$@\"\n", gitPath)
	if err := os.WriteFile(gitWrapper, []byte(wrapperData), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("OPEN_ACTIONS_TEST_GIT_LOG", commandLog)
	t.Setenv("PATH", commandDirectory+string(os.PathListSeparator)+os.Getenv("PATH"))

	compareCalls := 0
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/app/installations/2/access_tokens":
			fmt.Fprint(writer, `{"token":"installation-token"}`)
		case "/repos/acme/example/compare/" + revision.BaseSHA + "..." + revision.HeadSHA:
			compareCalls++
			fmt.Fprintf(writer, `{"merge_base_commit":{"sha":%q}}`, revision.MergeBaseSHA)
		case "/repos/acme/example/contents/.open-actions/workflows":
			fmt.Fprint(writer, `[{"name":"ci.yaml","path":".open-actions/workflows/ci.yaml","type":"file"}]`)
		case "/repos/acme/example/contents/.open-actions/workflows/ci.yaml":
			fmt.Fprintf(writer, `{"encoding":"base64","content":%q}`, base64.StdEncoding.EncodeToString(workflowData))
		default:
			http.NotFound(writer, request)
		}
	}))
	defer server.Close()
	clusterClient, reconciler, handler, project := newPullRequestDeliveryTest(t, server, now)
	reconciler.GitRepository, _ = gitrepository.NewClient(gitRoot)
	reconcile := func(body []byte) {
		t.Helper()
		key := enqueuePullRequestDelivery(t, handler, clusterClient, project, now, revision.BaseSHA, revision.HeadSHA, body)
		if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: key}); err != nil {
			t.Fatal(err)
		}
	}

	reconcile([]byte(`{"delivery":"first-merge"}`))
	firstCommands, err := os.ReadFile(commandLog)
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Fields(string(firstCommands)); !slices.Equal(got, []string{"ls-tree", "show"}) {
		t.Fatalf("first merge workflow discovery commands = %v", got)
	}
	reconcile([]byte(`{"delivery":"second-merge"}`))
	secondCommands, err := os.ReadFile(commandLog)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(firstCommands, secondCommands) {
		t.Fatalf("cached merge workflow discovery commands = %q, want %q", secondCommands, firstCommands)
	}
	if compareCalls != 2 {
		t.Fatalf("merge base calls = %d, want 2", compareCalls)
	}
	runs := &actionsv1alpha1.WorkflowRunList{}
	if err := clusterClient.List(context.Background(), runs); err != nil {
		t.Fatal(err)
	}
	if len(runs.Items) != 2 {
		t.Fatalf("WorkflowRuns = %d, want 2", len(runs.Items))
	}
}

func TestDeliveryCachesWorkflowsAcrossEventsAtAnImmutableRevision(t *testing.T) {
	revisionA := strings.Repeat("a", 40)
	revisionB := strings.Repeat("b", 40)
	workflowData := []byte("name: Deploy\non:\n  push:\n    branches: [main]\n  workflow_run:\n    workflows: [Release]\n    types: [completed]\n    branches: [main]\njobs:\n  deploy:\n    runs-on: ubuntu-latest\n    steps:\n      - run: make deploy\n")
	revisionCalls := 0
	directoryCalls := 0
	fileCalls := 0
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/app/installations/2/access_tokens":
			expiresAt := time.Now().Add(time.Hour).UTC().Format(time.RFC3339)
			fmt.Fprintf(writer, `{"token":"installation-token","expires_at":%q}`, expiresAt)
		case "/repos/acme/example/commits":
			revisionCalls++
			revision := revisionA
			if revisionCalls > 2 {
				revision = revisionB
			}
			fmt.Fprintf(writer, `[{"sha":%q}]`, revision)
		case "/repos/acme/example/contents/.open-actions/workflows":
			directoryCalls++
			fmt.Fprint(writer, `[{"name":"deploy.yaml","path":".open-actions/workflows/deploy.yaml","type":"file"}]`)
		case "/repos/acme/example/contents/.open-actions/workflows/deploy.yaml":
			fileCalls++
			fmt.Fprintf(writer, `{"encoding":"base64","content":%q}`, base64.StdEncoding.EncodeToString(workflowData))
		default:
			http.NotFound(writer, request)
		}
	}))
	defer server.Close()

	now := time.Date(2026, 8, 25, 12, 0, 0, 0, time.UTC)
	clusterClient, reconciler, handler, project := newPullRequestDeliveryTest(t, server, now)
	enqueue := func(name string, normalized normalizedEvent) client.ObjectKey {
		t.Helper()
		event := &payload{}
		event.Repository.ID = 1
		event.Repository.Name = "example"
		event.Repository.DefaultBranch = "main"
		event.Repository.Owner.Login = "acme"
		body := []byte(fmt.Sprintf(`{"delivery":%q}`, name))
		if err := handler.enqueueDelivery(context.Background(), project, event, normalized, name, body); err != nil {
			t.Fatal(err)
		}
		return client.ObjectKey{Namespace: project.Namespace, Name: webhookDeliveryName(body)}
	}
	reconcile := func(key client.ObjectKey) {
		t.Helper()
		if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: key}); err != nil {
			t.Fatal(err)
		}
	}

	requested := normalizedEvent{
		Name: "workflow_run", Action: "requested", Ref: "refs/heads/main", ResolveRef: "main", BaseRef: "main",
		WorkflowName: "Release", WorkflowRun: &normalizedWorkflowRun{HeadSHA: strings.Repeat("c", 40)},
	}
	reconcile(enqueue("requested-workflow-run-a", requested))
	runs := &actionsv1alpha1.WorkflowRunList{}
	if err := clusterClient.List(context.Background(), runs); err != nil {
		t.Fatal(err)
	}
	if len(runs.Items) != 0 {
		t.Fatalf("requested event WorkflowRuns = %d, want 0", len(runs.Items))
	}
	push := normalizedEvent{Name: "push", Ref: "refs/heads/main", ResolveRef: "main"}
	reconcile(enqueue("push-a", push))
	completed := requested
	completed.Action = "completed"
	completed.WorkflowRun = &normalizedWorkflowRun{Conclusion: "success", HeadSHA: strings.Repeat("c", 40)}
	reconcile(enqueue("completed-workflow-run-b", completed))

	if err := clusterClient.List(context.Background(), runs); err != nil {
		t.Fatal(err)
	}
	if len(runs.Items) != 2 {
		t.Fatalf("matching event WorkflowRuns = %d, want 2", len(runs.Items))
	}
	if revisionCalls != 3 {
		t.Fatalf("default branch resolution calls = %d, want 3", revisionCalls)
	}
	if directoryCalls != 2 || fileCalls != 2 {
		t.Fatalf("workflow discovery calls = directory %d, file %d; want 2 each", directoryCalls, fileCalls)
	}
}

func TestInvalidForkWorkflowDoesNotSuppressPullRequestTarget(t *testing.T) {
	now := time.Date(2026, 8, 23, 14, 0, 0, 0, time.UTC)
	baseWorkflowData := []byte("name: Trusted\non: pull_request_target\njobs:\n  check:\n    runs-on: ubuntu-latest\n    steps:\n      - run: make check\n")
	gitRoot, revision := testPullRequestGitRepository(t, baseWorkflowData, []byte("not a workflow\n"), false)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/app/installations/2/access_tokens":
			fmt.Fprint(writer, `{"token":"installation-token"}`)
		case "/repos/acme/example/compare/" + revision.BaseSHA + "..." + revision.HeadSHA:
			fmt.Fprintf(writer, `{"merge_base_commit":{"sha":%q}}`, revision.MergeBaseSHA)
		case "/repos/acme/example/contents/.open-actions/workflows":
			fmt.Fprint(writer, `[{"name":"ci.yaml","path":".open-actions/workflows/ci.yaml","type":"file"}]`)
		case "/repos/acme/example/contents/.open-actions/workflows/ci.yaml":
			fmt.Fprintf(writer, `{"encoding":"base64","content":%q}`, base64.StdEncoding.EncodeToString(baseWorkflowData))
		default:
			http.NotFound(writer, request)
		}
	}))
	defer server.Close()
	clusterClient, reconciler, handler, project := newPullRequestDeliveryTest(t, server, now)
	reconciler.GitRepository, _ = gitrepository.NewClient(gitRoot)
	deliveryKey := enqueuePullRequestDelivery(t, handler, clusterClient, project, now, revision.BaseSHA, revision.HeadSHA, []byte(`{"delivery":"invalid-fork-workflow"}`))

	if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: deliveryKey}); err != nil {
		t.Fatal(err)
	}
	stored := &corev1.ConfigMap{}
	if err := clusterClient.Get(context.Background(), deliveryKey, stored); err != nil {
		t.Fatal(err)
	}
	if stored.Data[deliveryStateKey] != deliveryStateCompleted || stored.Data[deliveryRunCountKey] != "1" {
		t.Fatalf("delivery data = %#v", stored.Data)
	}
	runs := &actionsv1alpha1.WorkflowRunList{}
	if err := clusterClient.List(context.Background(), runs); err != nil {
		t.Fatal(err)
	}
	if len(runs.Items) != 1 || runs.Items[0].Spec.Source.GitHub.Event.Name != actionsv1alpha1.GitHubEventNamePullRequestTarget {
		t.Fatalf("WorkflowRuns = %#v", runs.Items)
	}
}

func TestDisabledForkPolicyCreatesOnlyPullRequestTargetRun(t *testing.T) {
	now := time.Date(2026, 8, 23, 14, 30, 0, 0, time.UTC)
	workflowData := []byte("name: CI\non:\n  pull_request: {}\n  pull_request_target: {}\njobs:\n  check:\n    runs-on: ubuntu-latest\n    steps:\n      - run: make check\n")
	gitRoot, revision := testPullRequestGitRepository(t, workflowData, nil, false)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/app/installations/2/access_tokens":
			fmt.Fprint(writer, `{"token":"installation-token"}`)
		case "/repos/acme/example/compare/" + revision.BaseSHA + "..." + revision.HeadSHA:
			fmt.Fprintf(writer, `{"merge_base_commit":{"sha":%q}}`, revision.MergeBaseSHA)
		case "/repos/acme/example/contents/.open-actions/workflows":
			fmt.Fprint(writer, `[{"name":"ci.yaml","path":".open-actions/workflows/ci.yaml","type":"file"}]`)
		case "/repos/acme/example/contents/.open-actions/workflows/ci.yaml":
			fmt.Fprintf(writer, `{"encoding":"base64","content":%q}`, base64.StdEncoding.EncodeToString(workflowData))
		default:
			http.NotFound(writer, request)
		}
	}))
	defer server.Close()
	clusterClient, reconciler, handler, project := newPullRequestDeliveryTest(t, server, now)
	reconciler.GitRepository, _ = gitrepository.NewClient(gitRoot)
	storedProject := &actionsv1alpha1.Project{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(project), storedProject); err != nil {
		t.Fatal(err)
	}
	falseValue := false
	storedProject.Spec.Source.GitHub.ForkPullRequests = &actionsv1alpha1.GitHubForkPullRequestPolicy{Enabled: &falseValue}
	if err := clusterClient.Update(context.Background(), storedProject); err != nil {
		t.Fatal(err)
	}
	deliveryKey := enqueuePullRequestDelivery(t, handler, clusterClient, storedProject, now, revision.BaseSHA, revision.HeadSHA, []byte(`{"delivery":"forks-disabled"}`))

	if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: deliveryKey}); err != nil {
		t.Fatal(err)
	}
	runs := &actionsv1alpha1.WorkflowRunList{}
	if err := clusterClient.List(context.Background(), runs); err != nil {
		t.Fatal(err)
	}
	if len(runs.Items) != 1 || runs.Items[0].Spec.Source.GitHub.Event.Name != actionsv1alpha1.GitHubEventNamePullRequestTarget || runs.Items[0].Spec.ForkPullRequest != nil {
		t.Fatalf("WorkflowRuns = %#v", runs.Items)
	}
}

func TestDeliverySkipsConflictingPullRequestRevision(t *testing.T) {
	now := time.Date(2026, 8, 9, 23, 0, 0, 0, time.UTC)
	workflowData := []byte("name: Trusted\non: pull_request_target\njobs:\n  check:\n    runs-on: ubuntu-latest\n    steps:\n      - run: make check\n")
	gitRoot, revision := testPullRequestGitRepository(t, workflowData, nil, true)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/app/installations/2/access_tokens":
			fmt.Fprint(writer, `{"token":"installation-token"}`)
		case "/repos/acme/example/compare/" + revision.BaseSHA + "..." + revision.HeadSHA:
			fmt.Fprintf(writer, `{"merge_base_commit":{"sha":%q}}`, revision.MergeBaseSHA)
		case "/repos/acme/example/contents/.open-actions/workflows":
			fmt.Fprint(writer, `[{"name":"trusted.yaml","path":".open-actions/workflows/trusted.yaml","type":"file"}]`)
		case "/repos/acme/example/contents/.open-actions/workflows/trusted.yaml":
			fmt.Fprintf(writer, `{"encoding":"base64","content":%q}`, base64.StdEncoding.EncodeToString(workflowData))
		default:
			http.NotFound(writer, request)
		}
	}))
	defer server.Close()
	clusterClient, reconciler, handler, project := newPullRequestDeliveryTest(t, server, now)
	reconciler.GitRepository, _ = gitrepository.NewClient(gitRoot)
	deliveryKey := enqueuePullRequestDelivery(t, handler, clusterClient, project, now, revision.BaseSHA, revision.HeadSHA, []byte(`{"delivery":"conflict"}`))

	if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: deliveryKey}); err != nil {
		t.Fatal(err)
	}
	stored := &corev1.ConfigMap{}
	if err := clusterClient.Get(context.Background(), deliveryKey, stored); err != nil {
		t.Fatal(err)
	}
	if stored.Data[deliveryStateKey] != deliveryStateCompleted || stored.Data[deliveryRunCountKey] != "1" || stored.Data[deliveryRevisionKey] != "" {
		t.Fatalf("delivery data = %#v", stored.Data)
	}
	runs := &actionsv1alpha1.WorkflowRunList{}
	if err := clusterClient.List(context.Background(), runs); err != nil {
		t.Fatal(err)
	}
	if len(runs.Items) != 1 || runs.Items[0].Spec.Source.GitHub.Event.Name != actionsv1alpha1.GitHubEventNamePullRequestTarget || runs.Items[0].Spec.Source.GitHub.Revision.SHA != revision.BaseSHA || runs.Items[0].Spec.ForkPullRequest != nil {
		t.Fatalf("trusted target runs = %#v", runs.Items)
	}
}

func TestForkUpdatesCreateIndependentTrustedTargetRuns(t *testing.T) {
	baseSHA := strings.Repeat("a", 40)
	firstHeadSHA := strings.Repeat("b", 40)
	secondHeadSHA := strings.Repeat("c", 40)
	workflowData := []byte("name: Trusted\non: pull_request_target\njobs:\n  check:\n    runs-on: ubuntu-latest\n    steps:\n      - run: make check\n")
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/app/installations/2/access_tokens":
			fmt.Fprint(writer, `{"token":"installation-token"}`)
		case "/repos/acme/example/contents/.open-actions/workflows":
			if request.URL.Query().Get("ref") != baseSHA {
				http.Error(writer, "workflow discovery did not use the trusted base SHA", http.StatusBadRequest)
				return
			}
			fmt.Fprint(writer, `[{"name":"trusted.yaml","path":".open-actions/workflows/trusted.yaml","type":"file"}]`)
		case "/repos/acme/example/contents/.open-actions/workflows/trusted.yaml":
			if request.URL.Query().Get("ref") != baseSHA {
				http.Error(writer, "workflow fetch did not use the trusted base SHA", http.StatusBadRequest)
				return
			}
			fmt.Fprintf(writer, `{"encoding":"base64","content":%q}`, base64.StdEncoding.EncodeToString(workflowData))
		default:
			http.NotFound(writer, request)
		}
	}))
	defer server.Close()
	now := time.Date(2026, 8, 12, 22, 0, 0, 0, time.UTC)
	clusterClient, reconciler, handler, project := newPullRequestDeliveryTest(t, server, now)

	enqueue := func(headSHA string, body []byte) client.ObjectKey {
		event := &payload{}
		event.Repository.ID = 1
		event.Repository.Name = "example"
		event.Repository.Owner.Login = "acme"
		normalized := normalizedEvent{
			Name: "pull_request", Action: "synchronize", Ref: "refs/pull/42/merge", Fork: true,
			PullRequest: &normalizedPullRequest{
				Number: 42, HTMLURL: "https://github.com/acme/example/pull/42",
				HeadRepository: normalizedRepository{ID: 2, Owner: "contributor", Name: "example"},
				HeadRef:        "feature", HeadSHA: headSHA, BaseRef: "main", BaseSHA: baseSHA,
			},
		}
		if err := handler.enqueueDelivery(context.Background(), project, event, normalized, "delivery-"+headSHA[:8], body); err != nil {
			t.Fatal(err)
		}
		return client.ObjectKey{Namespace: project.Namespace, Name: webhookDeliveryName(body)}
	}

	for _, update := range []struct {
		headSHA string
		body    []byte
	}{
		{headSHA: firstHeadSHA, body: []byte(`{"head":"first"}`)},
		{headSHA: secondHeadSHA, body: []byte(`{"head":"second"}`)},
	} {
		key := enqueue(update.headSHA, update.body)
		if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: key}); err != nil {
			t.Fatal(err)
		}
	}

	runs := &actionsv1alpha1.WorkflowRunList{}
	if err := clusterClient.List(context.Background(), runs); err != nil {
		t.Fatal(err)
	}
	if len(runs.Items) != 2 {
		t.Fatalf("WorkflowRuns = %d, want 2", len(runs.Items))
	}
	wantHeads := map[string]bool{firstHeadSHA: true, secondHeadSHA: true}
	for index := range runs.Items {
		source := runs.Items[index].Spec.Source.GitHub
		if source.Event.Name != actionsv1alpha1.GitHubEventNamePullRequestTarget || source.Revision.SHA != baseSHA || source.Revision.Ref != "refs/heads/main" || source.Event.PullRequest == nil || !wantHeads[source.Event.PullRequest.HeadSHA] {
			t.Fatalf("trusted target source = %#v", source)
		}
		delete(wantHeads, source.Event.PullRequest.HeadSHA)
	}
	if len(wantHeads) != 0 {
		t.Fatalf("missing fork head snapshots = %#v", wantHeads)
	}
}

func TestDeliveryFinishesWhenEventRevisionIsUnavailable(t *testing.T) {
	now := time.Date(2026, 8, 10, 0, 0, 0, 0, time.UTC)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/app/installations/2/access_tokens":
			fmt.Fprint(writer, `{"token":"installation-token"}`)
		case "/repos/acme/example/commits":
			writer.WriteHeader(http.StatusConflict)
			fmt.Fprint(writer, `{"message":"Git Repository is empty."}`)
		default:
			http.NotFound(writer, request)
		}
	}))
	defer server.Close()
	clusterClient, reconciler, handler, project := newPullRequestDeliveryTest(t, server, now)
	event := &payload{}
	event.Repository.ID = 1
	event.Repository.Owner.Login = "acme"
	event.Repository.Name = "example"
	event.Repository.DefaultBranch = "main"
	normalized := normalizedEvent{Name: "issues", Action: "opened", Ref: "refs/heads/main", ResolveRef: "main", Issue: &normalizedIssue{Number: 1}}
	body := []byte(`{"delivery":"empty-repository"}`)
	if err := handler.enqueueDelivery(context.Background(), project, event, normalized, "delivery", body); err != nil {
		t.Fatal(err)
	}
	key := client.ObjectKey{Namespace: project.Namespace, Name: webhookDeliveryName(body)}
	if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: key}); err != nil {
		t.Fatal(err)
	}
	stored := &corev1.ConfigMap{}
	if err := clusterClient.Get(context.Background(), key, stored); err != nil {
		t.Fatal(err)
	}
	if stored.Data[deliveryStateKey] != deliveryStateFailed || stored.Data[deliveryFinishedKey] == "" || !strings.Contains(stored.Data[deliveryMessageKey], "unavailable") {
		t.Fatalf("terminal delivery data = %#v", stored.Data)
	}
}

func TestDeliveryDoesNotCountRunsBeforeTargetRevisionFailure(t *testing.T) {
	now := time.Date(2026, 8, 10, 0, 0, 0, 0, time.UTC)
	workflowData := []byte("name: CI\non:\n  pull_request:\n    types: [closed]\njobs:\n  check:\n    runs-on: ubuntu-latest\n    steps:\n      - run: make check\n")
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/app/installations/2/access_tokens":
			fmt.Fprint(writer, `{"token":"installation-token"}`)
		case "/repos/acme/example/commits":
			http.NotFound(writer, request)
		case "/repos/acme/example/contents/.open-actions/workflows":
			fmt.Fprint(writer, `[{"name":"ci.yaml","path":".open-actions/workflows/ci.yaml","type":"file"}]`)
		case "/repos/acme/example/contents/.open-actions/workflows/ci.yaml":
			fmt.Fprintf(writer, `{"encoding":"base64","content":%q}`, base64.StdEncoding.EncodeToString(workflowData))
		default:
			http.NotFound(writer, request)
		}
	}))
	defer server.Close()
	clusterClient, reconciler, handler, project := newPullRequestDeliveryTest(t, server, now)
	event := &payload{}
	event.Repository.ID = 1
	event.Repository.Owner.Login = "acme"
	event.Repository.Name = "example"
	event.Repository.DefaultBranch = "main"
	normalized := normalizedEvent{
		Name: "pull_request", Action: "closed", SHA: strings.Repeat("a", 40), Ref: "refs/pull/9/merge", MergeRevision: true,
		PullRequest: &normalizedPullRequest{
			Number: 9, HeadRef: "feature", HeadSHA: strings.Repeat("b", 40), BaseRef: "main",
			HeadRepository: normalizedRepository{ID: 1, Owner: "acme", Name: "example"},
		},
	}
	body := []byte(`{"delivery":"missing-target"}`)
	if err := handler.enqueueDelivery(context.Background(), project, event, normalized, "delivery", body); err != nil {
		t.Fatal(err)
	}
	key := client.ObjectKey{Namespace: project.Namespace, Name: webhookDeliveryName(body)}
	if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: key}); err != nil {
		t.Fatal(err)
	}
	stored := &corev1.ConfigMap{}
	if err := clusterClient.Get(context.Background(), key, stored); err != nil {
		t.Fatal(err)
	}
	if stored.Data[deliveryStateKey] != deliveryStateFailed || stored.Data[deliveryRunCountKey] != "0" {
		t.Fatalf("terminal delivery data = %#v", stored.Data)
	}
	runs := &actionsv1alpha1.WorkflowRunList{}
	if err := clusterClient.List(context.Background(), runs); err != nil {
		t.Fatal(err)
	}
	if len(runs.Items) != 0 {
		t.Fatalf("WorkflowRuns = %#v, want none before terminal discovery", runs.Items)
	}
}

func TestWorkflowRunRevisionOmitsFilterOnlyBaseRef(t *testing.T) {
	revision := workflowRunRevision(normalizedEvent{
		Name: "workflow_run", SHA: strings.Repeat("a", 40), Ref: "refs/heads/main", BaseRef: "release",
	})
	if revision.BaseRef != "" {
		t.Fatalf("workflow_run revision baseRef = %q, want empty", revision.BaseRef)
	}
	revision = workflowRunRevision(normalizedEvent{
		Name: "pull_request_target", SHA: strings.Repeat("b", 40), Ref: "refs/heads/main", HeadRef: "feature", BaseRef: "main",
	})
	if revision.HeadRef != "" || revision.BaseRef != "" {
		t.Fatalf("pull_request_target revision = %#v", revision)
	}
}

func TestWorkflowRunRevisionIncludesIntegrationInputsTogether(t *testing.T) {
	headSHA := strings.Repeat("b", 40)
	baseSHA := strings.Repeat("c", 40)
	mergeBaseSHA := strings.Repeat("d", 40)
	pullRequest := &normalizedPullRequest{HeadSHA: headSHA, BaseSHA: baseSHA}

	revision := workflowRunRevision(normalizedEvent{
		Name: "pull_request", Action: "labeled", SHA: strings.Repeat("a", 40), PullRequest: pullRequest,
	})
	if revision.HeadSHA != headSHA || revision.BaseSHA != "" || revision.MergeBaseSHA != "" {
		t.Fatalf("non-integration revision = %#v", revision)
	}

	revision = workflowRunRevision(normalizedEvent{
		Name: "pull_request", Action: "synchronize", SHA: strings.Repeat("a", 40), HeadSHA: headSHA, MergeBaseSHA: mergeBaseSHA, PullRequest: pullRequest,
	})
	if revision.HeadSHA != headSHA || revision.BaseSHA != baseSHA || revision.MergeBaseSHA != mergeBaseSHA {
		t.Fatalf("integration revision = %#v", revision)
	}
}

func TestWorkflowRunEventCopiesBoundedMetadata(t *testing.T) {
	event := workflowRunEvent(normalizedEvent{
		Name: "issue_comment", Action: "created",
		PullRequest: &normalizedPullRequest{
			Number: 42, Body: "Pull request body", HTMLURL: "https://github.com/acme/example/pull/42",
			HeadRepository: normalizedRepository{ID: 2, Owner: "contributor", Name: "example"}, HeadRef: "feature", HeadSHA: strings.Repeat("a", 40), BaseRef: "main",
		},
		WorkflowRun: &normalizedWorkflowRun{Conclusion: "success", HeadSHA: strings.Repeat("b", 40)},
		Issue:       &normalizedIssue{Number: 17, Body: "Issue body"},
		Comment:     &normalizedComment{Body: "Comment body"},
		Review:      &normalizedReview{Body: "Review body"},
	}, "delivery")
	if event.DeliveryID != "delivery" || event.PullRequest == nil || event.PullRequest.Body != "Pull request body" || event.PullRequest.HTMLURL != "https://github.com/acme/example/pull/42" ||
		event.WorkflowRun == nil || event.WorkflowRun.Conclusion != "success" || event.Issue == nil || event.Issue.Number != 17 ||
		event.Comment == nil || event.Comment.Body != "Comment body" || event.Review == nil || event.Review.Body != "Review body" {
		t.Fatalf("WorkflowRun event = %#v", event)
	}
}

func newPullRequestDeliveryTest(t *testing.T, server *httptest.Server, now time.Time) (client.Client, *DeliveryReconciler, *GitHubHandler, *actionsv1alpha1.Project) {
	t.Helper()
	githubAPI, err := githubclient.NewClient(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	privateKeyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(privateKey)})
	project := &actionsv1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: "default", UID: "project-uid"},
		Spec: actionsv1alpha1.ProjectSpec{
			WorkflowDirectory: ".open-actions/workflows",
			Source: actionsv1alpha1.ProjectSource{
				Type: actionsv1alpha1.SourceTypeGitHub,
				GitHub: &actionsv1alpha1.GitHubAppConfiguration{
					AppID: 1, InstallationID: 2,
					PrivateKeySecretRef: corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "github"}, Key: "private-key"},
					WebhookSecretRef:    corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "github"}, Key: "webhook-secret"},
				},
			},
		},
	}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "github", Namespace: project.Namespace},
		Data:       map[string][]byte{"private-key": privateKeyPEM, "webhook-secret": []byte("secret")},
	}
	scheme := deliveryTestScheme(t)
	clusterClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(project, secret).Build()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	gitRepository, err := gitrepository.NewClient(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	reconciler := &DeliveryReconciler{
		Client: clusterClient, APIReader: clusterClient, GitHub: githubAPI, GitRepository: gitRepository, Logger: logger,
		Now: func() time.Time { return now },
	}
	handler := &GitHubHandler{Client: clusterClient, APIReader: clusterClient}
	return clusterClient, reconciler, handler, project
}

func enqueuePullRequestDelivery(t *testing.T, handler *GitHubHandler, clusterClient client.Client, project *actionsv1alpha1.Project, createdAt time.Time, baseSHA, headSHA string, body []byte) client.ObjectKey {
	t.Helper()
	event := &payload{}
	event.Repository.ID = 1
	event.Repository.Name = "example"
	event.Repository.DefaultBranch = "main"
	event.Repository.Owner.Login = "acme"
	normalized := normalizedEvent{
		Name: "pull_request", Action: "synchronize", Ref: "refs/pull/9/merge", Fork: true,
		HeadRef: "feature", BaseRef: "main", HeadSHA: headSHA, MergeRevision: true,
		PullRequest: &normalizedPullRequest{
			Number: 9, Body: "Pull request body", HTMLURL: "https://github.com/acme/example/pull/9",
			HeadRef: "feature", HeadSHA: headSHA, BaseRef: "main", BaseSHA: baseSHA,
			HeadRepository: normalizedRepository{ID: 2, Owner: "contributor", Name: "example"},
		},
	}
	if err := handler.enqueueDelivery(context.Background(), project, event, normalized, "delivery", body); err != nil {
		t.Fatal(err)
	}
	key := client.ObjectKey{Namespace: project.Namespace, Name: webhookDeliveryName(body)}
	object := &corev1.ConfigMap{}
	if err := clusterClient.Get(context.Background(), key, object); err != nil {
		t.Fatal(err)
	}
	object.CreationTimestamp = metav1.NewTime(createdAt)
	if err := clusterClient.Update(context.Background(), object); err != nil {
		t.Fatal(err)
	}
	return key
}

func testPullRequestGitRepository(t *testing.T, baseWorkflowData, headWorkflowData []byte, conflict bool) (string, gitrepository.Revision) {
	t.Helper()
	root := t.TempDir()
	work := filepath.Join(root, "work")
	remote := filepath.Join(root, "acme", "example")
	if err := os.MkdirAll(filepath.Dir(remote), 0o755); err != nil {
		t.Fatal(err)
	}
	runTestGit(t, "", "init", "--quiet", work)
	runTestGit(t, work, "config", "user.name", "Test")
	runTestGit(t, work, "config", "user.email", "test@example.com")
	writeTestFile(t, work, "shared.txt", "common\n")
	runTestGit(t, work, "add", ".")
	runTestGit(t, work, "commit", "--quiet", "-m", "common")
	mergeBaseSHA := strings.TrimSpace(runTestGit(t, work, "rev-parse", "HEAD"))

	runTestGit(t, work, "switch", "--quiet", "-c", "base")
	writeTestFile(t, work, ".open-actions/workflows/ci.yaml", string(baseWorkflowData))
	if conflict {
		writeTestFile(t, work, "shared.txt", "base\n")
	}
	runTestGit(t, work, "add", ".")
	runTestGit(t, work, "commit", "--quiet", "-m", "base")
	baseSHA := strings.TrimSpace(runTestGit(t, work, "rev-parse", "HEAD"))

	runTestGit(t, work, "switch", "--quiet", "-c", "head", mergeBaseSHA)
	if headWorkflowData != nil {
		writeTestFile(t, work, ".open-actions/workflows/ci.yaml", string(headWorkflowData))
	}
	if conflict {
		writeTestFile(t, work, "shared.txt", "head\n")
	} else {
		writeTestFile(t, work, "head.txt", "head\n")
	}
	runTestGit(t, work, "add", ".")
	runTestGit(t, work, "commit", "--quiet", "-m", "head")
	headSHA := strings.TrimSpace(runTestGit(t, work, "rev-parse", "HEAD"))
	runTestGit(t, "", "clone", "--quiet", "--bare", work, remote)
	return root, gitrepository.Revision{BaseSHA: baseSHA, HeadSHA: headSHA, MergeBaseSHA: mergeBaseSHA}
}

func writeTestFile(t *testing.T, root, path, data string) {
	t.Helper()
	fullPath := filepath.Join(root, path)
	if err := os.MkdirAll(filepath.Dir(fullPath), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(fullPath, []byte(data), 0o644); err != nil {
		t.Fatal(err)
	}
}

func runTestGit(t *testing.T, directory string, arguments ...string) string {
	t.Helper()
	command := exec.Command("git", arguments...)
	command.Dir = directory
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("git %s: %v: %s", strings.Join(arguments, " "), err, output)
	}
	return string(output)
}

type workflowRunAlreadyExistsClient struct {
	client.Client
}

func (c *workflowRunAlreadyExistsClient) Create(_ context.Context, object client.Object, _ ...client.CreateOption) error {
	return apierrors.NewAlreadyExists(actionsv1alpha1.GroupVersion.WithResource("workflowruns").GroupResource(), object.GetName())
}

func TestWorkflowRunNameIsStableAndBounded(t *testing.T) {
	workflowPath := strings.Repeat("workflow", 20) + ".yaml"
	name := workflowRunName(workflowPath, "project-uid", "replay")
	if len(name) > 63 {
		t.Errorf("name has %d characters", len(name))
	}
	if name != workflowRunName(workflowPath, "project-uid", "replay") {
		t.Error("name is not stable")
	}
	wantSuffix := strings.ToLower(digestEncoding.EncodeToString(sha256Digest("project-uid\x00replay\x00" + workflowPath)))[:workflowRunDigestLength]
	if suffix := name[len(name)-workflowRunDigestLength:]; suffix != wantSuffix {
		t.Errorf("name suffix = %q, want %q", suffix, wantSuffix)
	}
	if other := workflowRunName(workflowPath, "other-project-uid", "replay"); other == name {
		t.Error("projects share a WorkflowRun name")
	}
	fullDigestName := fullDigestWorkflowRunName(workflowPath, "replay")
	fullDigestSuffix := strings.ToLower(digestEncoding.EncodeToString(sha256Digest("replay|" + workflowPath)))
	if suffix := fullDigestName[len(fullDigestName)-len(fullDigestSuffix):]; suffix != fullDigestSuffix {
		t.Errorf("full digest name suffix = %q, want %q", suffix, fullDigestSuffix)
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

func TestWorkflowFileCountIsSharedAcrossRevisions(t *testing.T) {
	paths := make([]string, 0, maxWorkflowFiles)
	for index := range maxWorkflowFiles {
		paths = append(paths, fmt.Sprintf(".open-actions/workflows/workflow-%03d.yaml", index))
	}
	seen := map[string]struct{}{}
	if count := recordWorkflowFiles(seen, paths); count != maxWorkflowFiles {
		t.Fatalf("first revision workflow files = %d, want %d", count, maxWorkflowFiles)
	}
	if count := recordWorkflowFiles(seen, paths); count != maxWorkflowFiles {
		t.Fatalf("second revision workflow files = %d, want %d", count, maxWorkflowFiles)
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
			reconciler := &DeliveryReconciler{Client: clusterClient, APIReader: clusterClient, Now: func() time.Time { return now }}
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

func TestDeliveryReconcileUsesLiveReader(t *testing.T) {
	now := time.Date(2026, 8, 8, 12, 0, 0, 0, time.UTC)
	delivery := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{
		Name: "delivery-test", Namespace: "default", Labels: map[string]string{deliveryLabel: "true"},
	}, Data: map[string]string{
		deliveryStateKey: deliveryStateCompleted, deliveryFinishedKey: now.Add(-12 * time.Hour).Format(time.RFC3339),
	}}
	scheme := deliveryTestScheme(t)
	cachedClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	liveReader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(delivery).Build()
	reconciler := &DeliveryReconciler{Client: cachedClient, APIReader: liveReader, Now: func() time.Time { return now }}

	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(delivery)})
	if err != nil {
		t.Fatal(err)
	}
	if result.RequeueAfter != 12*time.Hour {
		t.Fatalf("requeue after = %v, want %v", result.RequeueAfter, 12*time.Hour)
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
