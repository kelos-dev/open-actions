package controller

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	actionsv1alpha1 "github.com/kelos-dev/open-actions/api/v1alpha1"
	githubclient "github.com/kelos-dev/open-actions/internal/github"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

func TestScheduleReconcilerCreatesOneRunPerDueMinute(t *testing.T) {
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	privateKeyData := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(privateKey)})
	revision := strings.Repeat("a", 40)
	workflowPath := ".open-actions/workflows/refresh.yaml"
	invalidWorkflowPath := ".open-actions/workflows/invalid.yaml"
	workflowData := "name: Refresh\non:\n  schedule:\n    - cron: '0 6 * * *'\njobs:\n  refresh:\n    runs-on: ubuntu-latest\n    steps:\n      - run: make refresh\n"
	var revisionCalls atomic.Int32
	var emptyRevisionCalls atomic.Int32
	var brokenRevisionCalls atomic.Int32
	var brokenTransient atomic.Bool
	var directoryCalls atomic.Int32
	var oversizedDirectoryCalls atomic.Int32
	var fileCalls atomic.Int32
	brokenTransient.Store(true)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/app/installations/42/access_tokens":
			body := map[string]any{}
			if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
				t.Error(err)
			}
			if _, found := body["repositories"]; found {
				t.Error("installation-wide schedule token unexpectedly selected repositories")
			}
			_ = json.NewEncoder(writer).Encode(map[string]string{"token": "installation-token"})
		case "/installation/repositories":
			_ = json.NewEncoder(writer).Encode(map[string]any{"total_count": 4, "repositories": []any{
				map[string]any{"id": 1, "name": "empty", "default_branch": "main", "owner": map[string]string{"login": "acme"}},
				map[string]any{"id": 2, "name": "broken", "default_branch": "main", "owner": map[string]string{"login": "acme"}},
				map[string]any{"id": 3, "name": "example", "default_branch": "main", "pushed_at": "2026-08-10T05:59:00Z", "owner": map[string]string{"login": "acme"}},
				map[string]any{"id": 4, "name": "oversized", "default_branch": "main", "pushed_at": "2026-08-10T05:59:00Z", "owner": map[string]string{"login": "acme"}},
			}})
		case "/repos/acme/empty/commits":
			emptyRevisionCalls.Add(1)
			writer.WriteHeader(http.StatusConflict)
			_ = json.NewEncoder(writer).Encode(map[string]string{"message": "Git Repository is empty."})
		case "/repos/acme/broken/commits":
			brokenRevisionCalls.Add(1)
			if brokenTransient.Load() {
				http.Error(writer, "temporarily unavailable", http.StatusServiceUnavailable)
			} else {
				writer.WriteHeader(http.StatusConflict)
				_ = json.NewEncoder(writer).Encode(map[string]string{"message": "Git Repository is empty."})
			}
		case "/repos/acme/example/commits":
			revisionCalls.Add(1)
			_ = json.NewEncoder(writer).Encode([]map[string]any{{"sha": revision}})
		case "/repos/acme/oversized/commits":
			_ = json.NewEncoder(writer).Encode([]map[string]any{{"sha": revision}})
		case "/repos/acme/example/contents/.open-actions/workflows":
			directoryCalls.Add(1)
			_ = json.NewEncoder(writer).Encode([]map[string]string{
				{"name": "invalid.yaml", "path": invalidWorkflowPath, "type": "file"},
				{"name": "refresh.yaml", "path": workflowPath, "type": "file"},
			})
		case "/repos/acme/oversized/contents/.open-actions/workflows":
			oversizedDirectoryCalls.Add(1)
			contents := make([]map[string]string, maxScheduledWorkflows+1)
			for index := range contents {
				path := fmt.Sprintf(".open-actions/workflows/workflow-%03d.yaml", index)
				contents[index] = map[string]string{"name": filepath.Base(path), "path": path, "type": "file"}
			}
			_ = json.NewEncoder(writer).Encode(contents)
		case "/repos/acme/example/contents/.open-actions/workflows/invalid.yaml":
			fileCalls.Add(1)
			_ = json.NewEncoder(writer).Encode(map[string]string{"encoding": "base64", "content": base64.StdEncoding.EncodeToString([]byte("not a workflow"))})
		case "/repos/acme/example/contents/.open-actions/workflows/refresh.yaml":
			fileCalls.Add(1)
			_ = json.NewEncoder(writer).Encode(map[string]string{"encoding": "base64", "content": base64.StdEncoding.EncodeToString([]byte(workflowData))})
		default:
			http.NotFound(writer, request)
		}
	}))
	defer server.Close()

	scheme := runtime.NewScheme()
	if err := actionsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	project := &actionsv1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: "project", Namespace: "default", UID: "project-uid", Generation: 1},
		Spec: actionsv1alpha1.ProjectSpec{
			WorkflowDirectory: ".open-actions/workflows",
			Source: actionsv1alpha1.ProjectSource{Type: actionsv1alpha1.SourceTypeGitHub, GitHub: &actionsv1alpha1.GitHubAppConfiguration{
				AppID: 7, InstallationID: 42,
				PrivateKeySecretRef: corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "github"}, Key: "private-key"},
			}},
		},
		Status: actionsv1alpha1.ProjectStatus{Conditions: []metav1.Condition{{
			Type: actionsv1alpha1.ProjectConditionConfigured, Status: metav1.ConditionTrue, ObservedGeneration: 1,
		}}},
	}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "github", Namespace: "default"}, Data: map[string][]byte{"private-key": privateKeyData}}
	clusterClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(project, secret).Build()
	github, err := githubclient.NewClient(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	reconciler := &ScheduleReconciler{
		Client: clusterClient, APIReader: clusterClient, GitHub: github, Logger: slog.Default(),
		WorkflowRunTTLSecondsAfterFinished: pointerTo(int32(604800)),
		Now:                                func() time.Time { return time.Date(2026, 8, 10, 6, 0, 30, 0, time.UTC) },
	}
	request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(project)}
	for range 2 {
		result, err := reconciler.Reconcile(context.Background(), request)
		if err != nil {
			t.Fatal(err)
		}
		if result.RequeueAfter != scheduleFailureRetry {
			t.Fatalf("requeue after = %v, want %v", result.RequeueAfter, scheduleFailureRetry)
		}
	}
	runs := &actionsv1alpha1.WorkflowRunList{}
	if err := clusterClient.List(context.Background(), runs); err != nil {
		t.Fatal(err)
	}
	if len(runs.Items) != 1 {
		t.Fatalf("WorkflowRuns = %d, want 1", len(runs.Items))
	}
	if revisionCalls.Load() != 1 || directoryCalls.Load() != 1 || fileCalls.Load() != 2 {
		t.Fatalf("GitHub discovery calls = revision %d, directory %d, files %d; cached reconcile repeated content discovery", revisionCalls.Load(), directoryCalls.Load(), fileCalls.Load())
	}
	if emptyRevisionCalls.Load() != 1 {
		t.Fatalf("empty repository revision calls = %d, want 1 cached discovery", emptyRevisionCalls.Load())
	}
	if brokenRevisionCalls.Load() != 2 {
		t.Fatalf("broken repository revision calls = %d, want 2 isolated attempts", brokenRevisionCalls.Load())
	}
	brokenTransient.Store(false)
	result, err := reconciler.Reconcile(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if result.RequeueAfter != 30*time.Second {
		t.Fatalf("permanent failure requeue after = %v, want 30s minute boundary", result.RequeueAfter)
	}
	if oversizedDirectoryCalls.Load() != 1 {
		t.Fatalf("oversized repository directory calls = %d, want 1 cached discovery", oversizedDirectoryCalls.Load())
	}
	if runs.Items[0].Spec.TTLSecondsAfterFinished == nil || *runs.Items[0].Spec.TTLSecondsAfterFinished != 604800 {
		t.Fatalf("scheduled WorkflowRun TTL = %v", runs.Items[0].Spec.TTLSecondsAfterFinished)
	}
	if !controllerutil.ContainsFinalizer(&runs.Items[0], workflowRunScheduleFinalizer) {
		t.Fatalf("scheduled WorkflowRun finalizers = %v", runs.Items[0].Finalizers)
	}
	githubSource := runs.Items[0].Spec.Source.GitHub
	if githubSource.Event.Name != actionsv1alpha1.GitHubEventNameSchedule || githubSource.Event.Schedule != "0 6 * * *" || githubSource.Revision.SHA != revision || githubSource.Revision.Ref != "refs/heads/main" {
		t.Fatalf("scheduled source = %#v", githubSource)
	}
}

func TestScheduleReconcilerImmediatelyRequeuesAfterMinuteBoundary(t *testing.T) {
	reconciler := &ScheduleReconciler{
		Now: func() time.Time { return time.Date(2026, 8, 10, 6, 1, 10, 0, time.UTC) },
	}
	result := reconciler.scheduleRequeue(time.Date(2026, 8, 10, 6, 0, 0, 0, time.UTC))
	if !result.Requeue || result.RequeueAfter != 0 {
		t.Fatalf("requeue result = %#v, want immediate requeue", result)
	}
}

func TestScheduleReconcilerRetriesFailuresBeforeMinuteBoundary(t *testing.T) {
	reconciler := &ScheduleReconciler{
		Now: func() time.Time { return time.Date(2026, 8, 10, 6, 0, 58, 0, time.UTC) },
	}
	result := reconciler.scheduleFailureRequeue(time.Date(2026, 8, 10, 6, 0, 0, 0, time.UTC))
	if result.Requeue || result.RequeueAfter != time.Second {
		t.Fatalf("requeue result = %#v, want one-second retry", result)
	}
}

func TestScheduleRepositoryRetryClassification(t *testing.T) {
	if !retryScheduleRepository(errors.New("temporarily unavailable")) {
		t.Fatal("transient repository error was not retried")
	}
	if retryScheduleRepository(permanentScheduleRepositoryError(errors.New("invalid repository"))) {
		t.Fatal("permanent repository error was retried")
	}
}
