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

func TestCreateWorkflowRunIsIdempotent(t *testing.T) {
	scheme := deliveryTestScheme(t)
	project := &actionsv1alpha1.Project{ObjectMeta: metav1.ObjectMeta{Name: "project", Namespace: "default", UID: "project-uid"}}
	event := &payload{}
	event.Repository.ID = 1
	event.Repository.Owner.Login = "acme"
	event.Repository.Name = "example"
	normalized := normalizedEvent{Name: "push", SHA: strings.Repeat("a", 40), Ref: "refs/heads/main"}
	delivery := &queuedDelivery{Payload: *event, Event: normalized, ReplayID: "replay", DeliveryID: "delivery"}
	ttl := int32(604800)

	clusterClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	reconciler := &DeliveryReconciler{Client: clusterClient, APIReader: clusterClient, WorkflowRunTTLSecondsAfterFinished: &ttl}
	if err := reconciler.createWorkflowRun(context.Background(), project, delivery, ".open-actions/workflows/ci.yaml"); err != nil {
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
	updatedTTL := int32(3600)
	run.Spec.TTLSecondsAfterFinished = &updatedTTL
	if err := clusterClient.Update(context.Background(), run); err != nil {
		t.Fatal(err)
	}
	if err := reconciler.createWorkflowRun(context.Background(), project, delivery, ".open-actions/workflows/ci.yaml"); err != nil {
		t.Fatalf("matching replay failed: %v", err)
	}
	if err := clusterClient.Get(context.Background(), runKey, run); err != nil {
		t.Fatal(err)
	}
	if run.Spec.TTLSecondsAfterFinished == nil || *run.Spec.TTLSecondsAfterFinished != updatedTTL {
		t.Fatalf("replayed WorkflowRun TTL = %v, want %d", run.Spec.TTLSecondsAfterFinished, updatedTTL)
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
	err := reconciler.createWorkflowRun(context.Background(), project, &conflictingDelivery, workflowPath)
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
					Revision: actionsv1alpha1.GitRevision{SHA: strings.Repeat("a", 40), HeadSHA: strings.Repeat("b", 40)},
				},
			},
		},
	}
	existing := desired.DeepCopy()
	existing.Spec.Source.GitHub.Revision.HeadSHA = ""

	if err := matchingWorkflowRun(existing, desired); err != nil {
		t.Fatalf("WorkflowRun using the SHA fallback did not match its delivery replay: %v", err)
	}

	existing.Spec.Source.GitHub.Revision.HeadSHA = strings.Repeat("c", 40)
	if err := matchingWorkflowRun(existing, desired); !apierrors.IsConflict(err) {
		t.Fatalf("WorkflowRun with a different head SHA error = %v, want conflict", err)
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
	delivery := &queuedDelivery{Payload: *event, Event: normalized, ReplayID: "replay", DeliveryID: "delivery"}
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
	if err := reconciler.createWorkflowRun(context.Background(), project, delivery, workflowPath); err != nil {
		t.Fatalf("matching replay failed against stale cache: %v", err)
	}
}

func TestDeliveryPinsCurrentPullRequestMergeRevision(t *testing.T) {
	now := time.Date(2026, 8, 9, 23, 0, 0, 0, time.UTC)
	headSHA := strings.Repeat("b", 40)
	mergeSHA := strings.Repeat("c", 40)
	movedMergeSHA := strings.Repeat("d", 40)
	parentSHA := strings.Repeat("a", 40)
	resolvedSHA := mergeSHA
	resolveCalls := 0
	failDiscovery := false
	workflowData := []byte("name: CI\non: pull_request\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: make test\n")
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/app/installations/2/access_tokens":
			fmt.Fprint(writer, `{"token":"installation-token"}`)
		case "/repos/acme/example/commits":
			resolveCalls++
			fmt.Fprintf(writer, `[{"sha":%q,"parents":[{"sha":%q}]}]`, resolvedSHA, parentSHA)
		case "/repos/acme/example/contents/.open-actions/workflows":
			if failDiscovery {
				failDiscovery = false
				http.Error(writer, "temporarily unavailable", http.StatusServiceUnavailable)
				return
			}
			fmt.Fprint(writer, `[{"name":"ci.yaml","path":".open-actions/workflows/ci.yaml","type":"file"}]`)
		case "/repos/acme/example/contents/.open-actions/workflows/ci.yaml":
			fmt.Fprintf(writer, `{"encoding":"base64","content":%q}`, base64.StdEncoding.EncodeToString(workflowData))
		default:
			http.NotFound(writer, request)
		}
	}))
	defer server.Close()
	clusterClient, reconciler, handler, project := newPullRequestDeliveryTest(t, server, now)
	deliveryKey := enqueuePullRequestDelivery(t, handler, clusterClient, project, now, headSHA, []byte(`{"delivery":"current-merge-ref"}`))

	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: deliveryKey})
	if err != nil {
		t.Fatal(err)
	}
	if result.RequeueAfter != mergeRefRetryInterval(0) {
		t.Fatalf("requeue after = %v, want %v", result.RequeueAfter, mergeRefRetryInterval(0))
	}

	parentSHA = headSHA
	failDiscovery = true
	if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: deliveryKey}); err == nil {
		t.Fatal("reconcile succeeded during a transient discovery failure")
	}
	stored := &corev1.ConfigMap{}
	if err := clusterClient.Get(context.Background(), deliveryKey, stored); err != nil {
		t.Fatal(err)
	}
	if stored.Data[deliveryRevisionKey] != mergeSHA {
		t.Fatalf("resolved revision = %q, want %q", stored.Data[deliveryRevisionKey], mergeSHA)
	}

	resolvedSHA = movedMergeSHA
	result, err = reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: deliveryKey})
	if err != nil {
		t.Fatal(err)
	}
	if result.RequeueAfter != 0 {
		t.Fatalf("requeue after = %v, want 0", result.RequeueAfter)
	}
	if resolveCalls != 2 {
		t.Fatalf("merge ref resolutions = %d, want 2", resolveCalls)
	}
	if err := clusterClient.Get(context.Background(), deliveryKey, stored); err != nil {
		t.Fatal(err)
	}
	if stored.Data[deliveryStateKey] != deliveryStateCompleted {
		t.Fatalf("delivery state = %q, want %q", stored.Data[deliveryStateKey], deliveryStateCompleted)
	}
	runs := &actionsv1alpha1.WorkflowRunList{}
	if err := clusterClient.List(context.Background(), runs); err != nil {
		t.Fatal(err)
	}
	if len(runs.Items) != 1 {
		t.Fatalf("WorkflowRuns = %d, want 1", len(runs.Items))
	}
	revision := runs.Items[0].Spec.Source.GitHub.Revision
	if revision.SHA != mergeSHA || revision.HeadSHA != headSHA {
		t.Fatalf("WorkflowRun revision = %#v, want execution SHA %q and head SHA %q", revision, mergeSHA, headSHA)
	}
	if got := runs.Items[0].Spec.Source.GitHub.Revision.BaseRef; got != "main" {
		t.Fatalf("WorkflowRun base ref = %q, want main", got)
	}
}

func TestDeliveryTimesOutWhenPullRequestMergeRefIsUnavailable(t *testing.T) {
	now := time.Date(2026, 8, 9, 23, 0, 0, 0, time.UTC)
	headSHA := strings.Repeat("b", 40)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/app/installations/2/access_tokens":
			fmt.Fprint(writer, `{"token":"installation-token"}`)
		case "/repos/acme/example/commits":
			http.NotFound(writer, request)
		default:
			http.NotFound(writer, request)
		}
	}))
	defer server.Close()
	clusterClient, reconciler, handler, project := newPullRequestDeliveryTest(t, server, now)
	deliveryKey := enqueuePullRequestDelivery(t, handler, clusterClient, project, now, headSHA, []byte(`{"delivery":"unavailable"}`))

	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: deliveryKey})
	if err != nil {
		t.Fatal(err)
	}
	if result.RequeueAfter != mergeRefRetryInterval(0) {
		t.Fatalf("requeue after = %v, want %v", result.RequeueAfter, mergeRefRetryInterval(0))
	}
	stored := &corev1.ConfigMap{}
	if err := clusterClient.Get(context.Background(), deliveryKey, stored); err != nil {
		t.Fatal(err)
	}
	if stored.Data[deliveryStateKey] != "" {
		t.Fatalf("delivery state = %q, want pending", stored.Data[deliveryStateKey])
	}

	reconciler.Now = func() time.Time { return now.Add(mergeRefWaitTimeout) }
	result, err = reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: deliveryKey})
	if err != nil {
		t.Fatal(err)
	}
	if result.RequeueAfter != 0 {
		t.Fatalf("requeue after = %v, want 0", result.RequeueAfter)
	}
	if err := clusterClient.Get(context.Background(), deliveryKey, stored); err != nil {
		t.Fatal(err)
	}
	if stored.Data[deliveryStateKey] != deliveryStateFailed || !strings.Contains(stored.Data[deliveryMessageKey], headSHA) {
		t.Fatalf("terminal delivery data = %#v", stored.Data)
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
	reconciler := &DeliveryReconciler{Client: clusterClient, APIReader: clusterClient, GitHub: githubAPI, Logger: logger, Now: func() time.Time { return now }}
	handler := &GitHubHandler{Client: clusterClient, APIReader: clusterClient}
	return clusterClient, reconciler, handler, project
}

func enqueuePullRequestDelivery(t *testing.T, handler *GitHubHandler, clusterClient client.Client, project *actionsv1alpha1.Project, createdAt time.Time, headSHA string, body []byte) client.ObjectKey {
	t.Helper()
	event := &payload{}
	event.Repository.ID = 1
	event.Repository.Name = "example"
	event.Repository.Owner.Login = "acme"
	normalized := normalizedEvent{
		Name: "pull_request", Action: "synchronize", Ref: "refs/pull/9/merge",
		ResolveRef: "refs/pull/9/merge", HeadRef: "feature", BaseRef: "main", HeadSHA: headSHA,
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

func TestMergeRefRetryIntervalGrowsWithDeliveryAge(t *testing.T) {
	for _, tt := range []struct {
		age  time.Duration
		want time.Duration
	}{
		{age: 0, want: 2 * time.Second},
		{age: 10 * time.Second, want: 5 * time.Second},
		{age: 30 * time.Second, want: 15 * time.Second},
	} {
		if got := mergeRefRetryInterval(tt.age); got != tt.want {
			t.Errorf("mergeRefRetryInterval(%v) = %v, want %v", tt.age, got, tt.want)
		}
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
