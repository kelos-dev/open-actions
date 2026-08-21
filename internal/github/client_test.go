package github

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
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestInstallationAndRepositoryContents(t *testing.T) {
	const workflowPath = ".open-actions/workflow files/CI + checks?.yaml"
	workflow := []byte("name: CI\non: push\njobs: {}\n")
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Authorization") == "" {
			http.Error(writer, "missing authorization", http.StatusUnauthorized)
			return
		}
		switch request.URL.Path {
		case "/app/installations/2/access_tokens":
			body := struct {
				Repositories []string          `json:"repositories"`
				Permissions  map[string]string `json:"permissions"`
			}{}
			if err := json.NewDecoder(request.Body).Decode(&body); err != nil || len(body.Repositories) != 1 || body.Repositories[0] != "example" || body.Permissions["contents"] != "read" {
				http.Error(writer, "token request is not repository scoped", http.StatusBadRequest)
				return
			}
			fmt.Fprint(writer, `{"token":"installation-token"}`)
		case "/repos/acme/example/contents/.open-actions/workflow files":
			fmt.Fprint(writer, `[{"name":"CI + checks?.yaml","path":".open-actions/workflow files/CI + checks?.yaml","type":"file"}]`)
		case "/repos/acme/example/contents/" + workflowPath:
			fmt.Fprintf(writer, `{"encoding":"base64","content":%q}`, base64.StdEncoding.EncodeToString(workflow))
		case "/repos/acme/example/commits":
			if request.URL.Query().Get("sha") != "main" || request.URL.Query().Get("per_page") != "1" {
				http.Error(writer, "unexpected revision query", http.StatusBadRequest)
				return
			}
			fmt.Fprintf(writer, `[{"sha":%q}]`, strings.Repeat("b", 40))
		default:
			http.NotFound(writer, request)
		}
	}))
	defer server.Close()

	client, err := NewClient(server.URL+"/", server.Client())
	if err != nil {
		t.Fatal(err)
	}
	installation, err := client.Installation(context.Background(), 1, 2, testPrivateKey(t), "example", InstallationPermissions{"contents": "read"})
	if err != nil {
		t.Fatalf("installation client: %v", err)
	}
	contents, err := installation.ListDirectory(context.Background(), "acme", "example", ".open-actions/workflow files", strings.Repeat("a", 40))
	if err != nil {
		t.Fatalf("list directory: %v", err)
	}
	if len(contents) != 1 || contents[0].Name != "CI + checks?.yaml" {
		t.Fatalf("contents = %#v", contents)
	}
	data, err := installation.GetFile(context.Background(), "acme", "example", contents[0].Path, strings.Repeat("a", 40))
	if err != nil {
		t.Fatalf("get file: %v", err)
	}
	if string(data) != string(workflow) {
		t.Errorf("workflow = %q", data)
	}
	resolved, err := installation.ResolveRevision(context.Background(), "acme", "example", "main")
	if err != nil {
		t.Fatalf("resolve revision: %v", err)
	}
	if resolved != strings.Repeat("b", 40) {
		t.Errorf("resolved revision = %q", resolved)
	}
}

func TestInstallationRequestsOnlySelectedPermissions(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		body := struct {
			Permissions map[string]string `json:"permissions"`
		}{}
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			http.Error(writer, "invalid request", http.StatusBadRequest)
			return
		}
		if len(body.Permissions) != 1 || body.Permissions["checks"] != "write" {
			http.Error(writer, "unexpected permissions", http.StatusBadRequest)
			return
		}
		fmt.Fprint(writer, `{"token":"checks-token"}`)
	}))
	defer server.Close()
	client, err := NewClient(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	installation, err := client.Installation(context.Background(), 1, 2, testPrivateKey(t), "example", InstallationPermissions{"checks": "write"})
	if err != nil {
		t.Fatal(err)
	}
	if installation.Token() != "checks-token" {
		t.Fatalf("token = %q", installation.Token())
	}
	if _, err := client.Installation(context.Background(), 1, 2, testPrivateKey(t), "example", nil); err == nil {
		t.Fatal("Installation() accepted unspecified permissions")
	}
}

func TestInstallationRequestsMetadataForEmptyPermissions(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		body := struct {
			Permissions map[string]string `json:"permissions"`
		}{}
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil || len(body.Permissions) != 1 || body.Permissions["metadata"] != "read" {
			http.Error(writer, "unexpected permissions", http.StatusBadRequest)
			return
		}
		fmt.Fprint(writer, `{"token":"metadata-token"}`)
	}))
	defer server.Close()
	client, err := NewClient(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	installation, err := client.Installation(context.Background(), 1, 2, testPrivateKey(t), "example", InstallationPermissions{})
	if err != nil {
		t.Fatal(err)
	}
	if installation.Token() != "metadata-token" {
		t.Fatalf("token = %q", installation.Token())
	}
}

func TestRevokeInstallationToken(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requests++
		if request.Method != http.MethodDelete || request.URL.Path != "/installation/token" || request.Header.Get("Authorization") != "Bearer installation-token" {
			http.Error(writer, "unexpected revoke request", http.StatusBadRequest)
			return
		}
		if requests == 1 {
			writer.WriteHeader(http.StatusNoContent)
			return
		}
		writer.WriteHeader(http.StatusUnauthorized)
	}))
	defer server.Close()
	client, err := NewClient(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := client.RevokeInstallationToken(context.Background(), "installation-token"); err != nil {
			t.Fatal(err)
		}
	}
	if requests != 2 {
		t.Fatalf("revoke requests = %d", requests)
	}
}

func TestInstallationForAllRepositoriesOmitsRepositorySelection(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		body := map[string]any{}
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			http.Error(writer, "invalid request", http.StatusBadRequest)
			return
		}
		if _, found := body["repositories"]; found {
			http.Error(writer, "repository selection was included", http.StatusBadRequest)
			return
		}
		permissions, ok := body["permissions"].(map[string]any)
		if !ok || permissions["contents"] != "read" {
			http.Error(writer, "unexpected permissions", http.StatusBadRequest)
			return
		}
		fmt.Fprint(writer, `{"token":"action-installation-token"}`)
	}))
	defer server.Close()
	client, err := NewClient(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	installation, err := client.InstallationForAllRepositories(context.Background(), 1, 2, testPrivateKey(t), InstallationPermissions{"contents": "read"})
	if err != nil {
		t.Fatal(err)
	}
	if installation.Token() != "action-installation-token" {
		t.Fatalf("token = %q", installation.Token())
	}
}

func TestListRepositoriesStopsAtLimit(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requests++
		page := request.URL.Query().Get("page")
		pageSize := request.URL.Query().Get("per_page")
		var repositories []Repository
		switch page {
		case "1":
			if pageSize != "100" {
				t.Errorf("first page size = %q", pageSize)
			}
			for index := range 100 {
				repositories = append(repositories, Repository{ID: int64(index + 1)})
			}
		case "2":
			if pageSize != "100" {
				t.Errorf("second page size = %q", pageSize)
			}
			for index := range 100 {
				repositories = append(repositories, Repository{ID: int64(index + 101)})
			}
		default:
			t.Fatalf("unexpected repository page %q", page)
		}
		_ = json.NewEncoder(writer).Encode(map[string]any{"total_count": 10_000, "repositories": repositories})
	}))
	defer server.Close()
	client, err := NewClient(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	installation := &InstallationClient{client: client, token: "token"}
	repositories, err := installation.ListRepositories(context.Background(), 101)
	if err != nil {
		t.Fatal(err)
	}
	if len(repositories) != 101 || repositories[100].ID != 101 || requests != 2 {
		t.Fatalf("repositories = %#v, requests = %d", repositories, requests)
	}
}

func TestCheckRunLifecycleAndRepositoryAccess(t *testing.T) {
	revision := strings.Repeat("a", 40)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch {
		case request.Method == http.MethodGet && request.URL.Path == "/repos/acme/example/commits/"+revision+"/check-runs":
			if request.URL.Query().Get("app_id") != "7" || request.URL.Query().Get("filter") != "all" {
				http.Error(writer, "unexpected query", http.StatusBadRequest)
				return
			}
			fmt.Fprint(writer, `{"total_count":1,"check_runs":[{"id":11,"external_id":"run-uid","status":"queued"}]}`)
		case request.Method == http.MethodPost && request.URL.Path == "/repos/acme/example/check-runs":
			requestBody := CreateCheckRunRequest{}
			if err := json.NewDecoder(request.Body).Decode(&requestBody); err != nil || requestBody.ExternalID != "other-run" || requestBody.Status != "queued" {
				http.Error(writer, "unexpected create request", http.StatusBadRequest)
				return
			}
			fmt.Fprint(writer, `{"id":12,"external_id":"other-run","status":"queued"}`)
		case request.Method == http.MethodPatch && request.URL.Path == "/repos/acme/example/check-runs/12":
			requestBody := UpdateCheckRunRequest{}
			if err := json.NewDecoder(request.Body).Decode(&requestBody); err != nil || requestBody.Conclusion != "success" {
				http.Error(writer, "unexpected update request", http.StatusBadRequest)
				return
			}
			fmt.Fprint(writer, `{"id":12,"external_id":"other-run","status":"completed","conclusion":"success"}`)
		default:
			http.NotFound(writer, request)
		}
	}))
	defer server.Close()
	client, err := NewClient(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	installation := &InstallationClient{client: client, token: "token"}
	found, err := installation.FindCheckRun(context.Background(), "acme", "example", revision, 7, "run-uid")
	if err != nil || found == nil || found.ID != 11 {
		t.Fatalf("FindCheckRun() = %#v, %v", found, err)
	}
	created, err := installation.CreateCheckRun(context.Background(), "acme", "example", CreateCheckRunRequest{Name: "CI", HeadSHA: revision, ExternalID: "other-run", Status: "queued"})
	if err != nil || created.ID != 12 {
		t.Fatalf("CreateCheckRun() = %#v, %v", created, err)
	}
	updated, err := installation.UpdateCheckRun(context.Background(), "acme", "example", created.ID, UpdateCheckRunRequest{Status: "completed", Conclusion: "success"})
	if err != nil || updated.Conclusion != "success" {
		t.Fatalf("UpdateCheckRun() = %#v, %v", updated, err)
	}
}

func TestGetFileErrorIncludesRepositoryIdentity(t *testing.T) {
	server := httptest.NewServer(http.NotFoundHandler())
	defer server.Close()
	client, err := NewClient(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	installation := &InstallationClient{client: client, token: "token"}
	revision := strings.Repeat("a", 40)
	_, err = installation.GetFile(context.Background(), "acme", "example", ".open-actions/workflows/ci.yaml", revision)
	if err == nil || !strings.Contains(err.Error(), "acme/example") || !strings.Contains(err.Error(), ".open-actions/workflows/ci.yaml") || !strings.Contains(err.Error(), revision) {
		t.Fatalf("GetFile() error = %v", err)
	}
}

func TestListDirectoryErrorIncludesRepositoryIdentity(t *testing.T) {
	server := httptest.NewServer(http.NotFoundHandler())
	defer server.Close()
	client, err := NewClient(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	installation := &InstallationClient{client: client, token: "token"}
	revision := strings.Repeat("a", 40)
	_, err = installation.ListDirectory(context.Background(), "acme", "example", ".open-actions/workflows", revision)
	if err == nil || !strings.Contains(err.Error(), "acme/example") || !strings.Contains(err.Error(), ".open-actions/workflows") || !strings.Contains(err.Error(), revision) {
		t.Fatalf("ListDirectory() error = %v", err)
	}
}

func TestResolveRevisionErrorIncludesRepositoryIdentity(t *testing.T) {
	server := httptest.NewServer(http.NotFoundHandler())
	defer server.Close()
	client, err := NewClient(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	installation := &InstallationClient{client: client, token: "token"}
	_, err = installation.ResolveRevision(context.Background(), "acme", "example", "main")
	if err == nil || !strings.Contains(err.Error(), "acme/example") || !strings.Contains(err.Error(), "main") {
		t.Fatalf("ResolveRevision() error = %v", err)
	}
}

func TestResolveMergeBase(t *testing.T) {
	baseSHA := strings.Repeat("a", 40)
	headSHA := strings.Repeat("b", 40)
	mergeBaseSHA := strings.Repeat("c", 40)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/repos/acme/example/compare/"+baseSHA+"..."+headSHA || request.URL.Query().Get("per_page") != "1" {
			http.NotFound(writer, request)
			return
		}
		fmt.Fprintf(writer, `{"merge_base_commit":{"sha":%q}}`, mergeBaseSHA)
	}))
	defer server.Close()
	client, err := NewClient(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	installation := &InstallationClient{client: client, token: "token"}
	got, err := installation.ResolveMergeBase(context.Background(), "acme", "example", baseSHA, headSHA)
	if err != nil {
		t.Fatal(err)
	}
	if got != mergeBaseSHA {
		t.Fatalf("merge base = %q, want %q", got, mergeBaseSHA)
	}
}

func TestAPIErrorPreservesStatusAndMessage(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusNotFound)
		fmt.Fprint(writer, `{"message":"Not Found"}`)
	}))
	defer server.Close()
	client, err := NewClient(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	installation := &InstallationClient{client: client, token: "token"}
	_, err = installation.ListDirectory(context.Background(), "acme", "example", ".open-actions/workflows", strings.Repeat("a", 40))
	apiError := &APIError{}
	if !errors.As(err, &apiError) {
		t.Fatalf("error type = %T, want *APIError", err)
	}
	if apiError.StatusCode != http.StatusNotFound || apiError.Message != "Not Found" {
		t.Errorf("API error = %#v", apiError)
	}
}

func TestNormalizeEndpointURLs(t *testing.T) {
	for _, tt := range []struct {
		name      string
		normalize func(string) (string, error)
		value     string
		want      string
	}{
		{name: "GitHub API root", normalize: NormalizeAPIURL, value: "https://api.github.com", want: "https://api.github.com/"},
		{name: "GitHub Enterprise API path", normalize: NormalizeAPIURL, value: "https://github.example/api/v3", want: "https://github.example/api/v3/"},
		{name: "GitHub server root", normalize: NormalizeServerURL, value: "https://github.com/", want: "https://github.com"},
		{name: "GitHub server path", normalize: NormalizeServerURL, value: "https://github.example/git/", want: "https://github.example/git"},
		{name: "action clone path", normalize: NormalizeActionCloneBaseURL, value: "https://github.example/git/", want: "https://github.example/git"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.normalize(tt.value)
			if err != nil {
				t.Fatal(err)
			}
			if got != tt.want {
				t.Errorf("normalized URL = %q, want %q", got, tt.want)
			}
		})
	}

	for _, value := range []string{
		"ssh://github.example",
		"https:///api/v3",
		"https://user:password@github.example",
		"https://github.example/api/v3?token=secret",
		"https://github.example/api/v3#fragment",
		"https://github.example/api/../v3",
		"https://github.example/api//v3",
		"https://github.example/api path",
		"https://github.example/api%2Fv3",
	} {
		t.Run(value, func(t *testing.T) {
			if _, err := NormalizeAPIURL(value); err == nil {
				t.Fatal("NormalizeAPIURL() accepted an invalid URL")
			}
			if _, err := NormalizeServerURL(value); err == nil {
				t.Fatal("NormalizeServerURL() accepted an invalid URL")
			}
			if _, err := NormalizeActionCloneBaseURL(value); err == nil {
				t.Fatal("NormalizeActionCloneBaseURL() accepted an invalid URL")
			}
		})
	}
	if _, err := NormalizeServerURL("git://github.example:9418"); err == nil {
		t.Fatal("NormalizeServerURL() accepted the git protocol")
	}
	if _, err := NormalizeActionCloneBaseURL("git://github.example:9418"); err == nil {
		t.Fatal("NormalizeActionCloneBaseURL() accepted the git protocol")
	}
}

func TestRefName(t *testing.T) {
	for _, tt := range []struct {
		ref  string
		want string
	}{
		{ref: "refs/heads/main", want: "main"},
		{ref: "refs/tags/v1.0.0", want: "v1.0.0"},
		{ref: "refs/pull/42/merge", want: "42/merge"},
		{ref: "refs/heads/gh-readonly-queue/main/pr-42", want: "gh-readonly-queue/main/pr-42"},
	} {
		if got := RefName(tt.ref); got != tt.want {
			t.Errorf("RefName(%q) = %q, want %q", tt.ref, got, tt.want)
		}
	}
}

func testPrivateKey(t *testing.T) []byte {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
}
