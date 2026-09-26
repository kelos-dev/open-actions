package console

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	actionsv1alpha1 "github.com/kelos-dev/open-actions/api/v1alpha1"
	githubclient "github.com/kelos-dev/open-actions/internal/github"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestGitHubRepositoryResolverUsesProjectInstallation(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requests++
		switch {
		case request.Method == http.MethodPost && request.URL.Path == "/app/installations/2/access_tokens":
			var scope struct {
				Repositories []string          `json:"repositories"`
				Permissions  map[string]string `json:"permissions"`
			}
			if err := json.NewDecoder(request.Body).Decode(&scope); err != nil {
				t.Error(err)
			}
			wantRepository, wantPermission := "example", "metadata"
			if requests == 3 {
				wantRepository, wantPermission = "Example", "contents"
			}
			if !reflect.DeepEqual(scope.Repositories, []string{wantRepository}) || !reflect.DeepEqual(scope.Permissions, map[string]string{wantPermission: "read"}) {
				t.Errorf("installation token scope = %#v", scope)
			}
			fmt.Fprint(writer, `{"token":"installation-token","expires_at":"2099-01-01T00:00:00Z"}`)
		case request.Method == http.MethodGet && request.URL.Path == "/repos/acme/example":
			if request.Header.Get("Authorization") != "Bearer installation-token" {
				http.Error(writer, "missing installation token", http.StatusUnauthorized)
				return
			}
			fmt.Fprint(writer, `{"id":123,"name":"Example","default_branch":"trunk","owner":{"login":"Acme"}}`)
		case request.Method == http.MethodGet && request.URL.Path == "/repos/Acme/Example/commits":
			if request.Header.Get("Authorization") != "Bearer installation-token" || request.URL.Query().Get("sha") != "refs/heads/trunk" {
				t.Errorf("revision request = %#v", request)
			}
			fmt.Fprintf(writer, `[{"sha":%q}]`, strings.Repeat("b", 40))
		case request.Method == http.MethodGet && request.URL.Path == "/repos/Acme/Example/contents/.open-actions/workflows/deploy.yaml":
			if request.Header.Get("Authorization") != "Bearer installation-token" || request.URL.Query().Get("ref") != strings.Repeat("b", 40) {
				t.Errorf("workflow request = %#v", request)
			}
			fmt.Fprintf(writer, `{"content":%q,"encoding":"base64"}`, base64.StdEncoding.EncodeToString([]byte(testDispatchWorkflow)))
		default:
			http.NotFound(writer, request)
		}
	}))
	defer server.Close()

	github, err := githubclient.NewClient(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	encodedPrivateKey := testGitHubPrivateKey(t)
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "github", Namespace: "default"}, Data: map[string][]byte{"private-key": encodedPrivateKey}}
	reader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(secret).Build()
	resolver, err := NewGitHubRepositoryResolver(reader, github)
	if err != nil {
		t.Fatal(err)
	}
	project := testGitHubProject()
	repository, defaultBranch, err := resolver.Resolve(context.Background(), project, "acme", "example")
	if err != nil {
		t.Fatal(err)
	}
	if repository.ID != 123 || repository.Owner != "Acme" || repository.Name != "Example" || defaultBranch != "trunk" || requests != 2 {
		t.Fatalf("Resolve() = %#v, %q after %d requests", repository, defaultBranch, requests)
	}
	revision, err := resolver.ResolveRevision(context.Background(), project, repository.Owner, repository.Name, "refs/heads/"+defaultBranch)
	if err != nil || revision != strings.Repeat("b", 40) || requests != 4 {
		t.Fatalf("ResolveRevision() = %q, %v after %d requests", revision, err, requests)
	}
	workflowFile, err := resolver.GetWorkflowFile(context.Background(), project, repository.Owner, repository.Name, ".open-actions/workflows/deploy.yaml", revision)
	if err != nil || string(workflowFile) != testDispatchWorkflow || requests != 5 {
		t.Fatalf("GetWorkflowFile() = %q, %v after %d requests", workflowFile, err, requests)
	}
}

func TestGitHubRepositoryResolverRejectsInvalidConfigurationAndIdentity(t *testing.T) {
	privateKey := testGitHubPrivateKey(t)
	tests := []struct {
		name               string
		project            *actionsv1alpha1.Project
		secret             *corev1.Secret
		repositoryResponse string
		want               string
	}{
		{name: "missing GitHub source", project: &actionsv1alpha1.Project{ObjectMeta: metav1.ObjectMeta{Name: "project"}}, want: `Project "project" has no GitHub source`},
		{name: "missing private key Secret", project: testGitHubProject(), want: `get Project "project" private key Secret "github"`},
		{
			name: "empty private key", project: testGitHubProject(),
			secret: &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "github", Namespace: "default"}},
			want:   `Project "project" private key Secret "github" does not contain non-empty key "private-key"`,
		},
		{
			name: "invalid repository identity", project: testGitHubProject(),
			secret:             &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "github", Namespace: "default"}, Data: map[string][]byte{"private-key": privateKey}},
			repositoryResponse: `{"id":0,"name":"Example","owner":{"login":"Acme"}}`,
			want:               "GitHub returned invalid identity for repository acme/example",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				switch {
				case request.Method == http.MethodPost && request.URL.Path == "/app/installations/2/access_tokens":
					fmt.Fprint(writer, `{"token":"installation-token","expires_at":"2099-01-01T00:00:00Z"}`)
				case request.Method == http.MethodGet && request.URL.Path == "/repos/acme/example":
					fmt.Fprint(writer, test.repositoryResponse)
				default:
					http.NotFound(writer, request)
				}
			}))
			defer server.Close()
			github, err := githubclient.NewClient(server.URL, server.Client())
			if err != nil {
				t.Fatal(err)
			}
			scheme := runtime.NewScheme()
			if err := corev1.AddToScheme(scheme); err != nil {
				t.Fatal(err)
			}
			objects := []client.Object{}
			if test.secret != nil {
				objects = append(objects, test.secret)
			}
			reader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()
			resolver, err := NewGitHubRepositoryResolver(reader, github)
			if err != nil {
				t.Fatal(err)
			}
			_, _, err = resolver.Resolve(context.Background(), test.project, "acme", "example")
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Resolve() error = %v, want %q", err, test.want)
			}
		})
	}
}

func testGitHubPrivateKey(t *testing.T) []byte {
	t.Helper()
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(privateKey)})
}

func testGitHubProject() *actionsv1alpha1.Project {
	return &actionsv1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: "project", Namespace: "default"},
		Spec: actionsv1alpha1.ProjectSpec{Source: actionsv1alpha1.ProjectSource{Type: actionsv1alpha1.SourceTypeGitHub, GitHub: &actionsv1alpha1.GitHubAppConfiguration{
			AppID: 1, InstallationID: 2,
			PrivateKeySecretRef: corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "github"}, Key: "private-key"},
		}}},
	}
}
