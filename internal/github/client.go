package github

import (
	"bytes"
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/kelos-dev/open-actions/internal/endpointurl"
)

const (
	maxResponseBytes = 4 << 20
	gitSHA1Bytes     = 20
)

type Client struct {
	baseURL    *url.URL
	httpClient *http.Client
	now        func() time.Time
}

type InstallationClient struct {
	client *Client
	token  string
}

type Content struct {
	Name string `json:"name"`
	Path string `json:"path"`
	Type string `json:"type"`
}

type Repository struct {
	ID            int64     `json:"id"`
	Name          string    `json:"name"`
	DefaultBranch string    `json:"default_branch"`
	PushedAt      time.Time `json:"pushed_at"`
	Owner         struct {
		Login string `json:"login"`
	} `json:"owner"`
}

type repositoryCommit struct {
	SHA string `json:"sha"`
}

// InstallationPermissions selects the repository permissions for a scoped
// installation token.
type InstallationPermissions map[string]string

var installationPermissionLevels = map[string]map[string]struct{}{
	"actions":       {"read": {}, "write": {}},
	"checks":        {"read": {}, "write": {}},
	"contents":      {"read": {}, "write": {}},
	"issues":        {"read": {}, "write": {}},
	"packages":      {"read": {}, "write": {}},
	"pull_requests": {"read": {}, "write": {}},
	"statuses":      {"read": {}, "write": {}},
}

func (p InstallationPermissions) String() string {
	if len(p) == 0 {
		return "none"
	}
	values := make([]string, 0, len(p))
	for name, level := range p {
		values = append(values, name+":"+level)
	}
	sort.Strings(values)
	return strings.Join(values, ", ")
}

func (p InstallationPermissions) validate() error {
	if p == nil {
		return errors.New("installation token permissions must be specified")
	}
	for name, level := range p {
		levels := installationPermissionLevels[name]
		if _, found := levels[level]; !found {
			return fmt.Errorf("unsupported installation token permission %s:%s", name, level)
		}
	}
	return nil
}

// CheckRun is the GitHub representation needed to reconcile a workflow check.
type CheckRun struct {
	ID         int64  `json:"id"`
	ExternalID string `json:"external_id"`
	HTMLURL    string `json:"html_url"`
	Status     string `json:"status"`
	Conclusion string `json:"conclusion"`
}

// CheckRunOutput is the user-visible content of a check run.
type CheckRunOutput struct {
	Title   string `json:"title"`
	Summary string `json:"summary"`
	Text    string `json:"text,omitempty"`
}

// CreateCheckRunRequest describes a new check run.
type CreateCheckRunRequest struct {
	Name        string          `json:"name"`
	HeadSHA     string          `json:"head_sha"`
	DetailsURL  string          `json:"details_url,omitempty"`
	ExternalID  string          `json:"external_id"`
	Status      string          `json:"status"`
	Conclusion  string          `json:"conclusion,omitempty"`
	StartedAt   string          `json:"started_at,omitempty"`
	CompletedAt string          `json:"completed_at,omitempty"`
	Output      *CheckRunOutput `json:"output,omitempty"`
}

// UpdateCheckRunRequest describes mutable check-run fields.
type UpdateCheckRunRequest struct {
	Name        string          `json:"name,omitempty"`
	DetailsURL  string          `json:"details_url,omitempty"`
	ExternalID  string          `json:"external_id,omitempty"`
	Status      string          `json:"status,omitempty"`
	Conclusion  string          `json:"conclusion,omitempty"`
	StartedAt   string          `json:"started_at,omitempty"`
	CompletedAt string          `json:"completed_at,omitempty"`
	Output      *CheckRunOutput `json:"output,omitempty"`
}

// APIError describes a non-success response from the GitHub API.
type APIError struct {
	StatusCode int
	Status     string
	Message    string
}

func (e *APIError) Error() string {
	if e.Message == "" {
		return fmt.Sprintf("GitHub API returned %s", e.Status)
	}
	return fmt.Sprintf("GitHub API returned %s: %s", e.Status, e.Message)
}

// NormalizeAPIURL validates and canonicalizes a GitHub REST API base URL.
func NormalizeAPIURL(value string) (string, error) {
	return endpointurl.Normalize(value, "GitHub API URL", true)
}

// NormalizeServerURL validates and canonicalizes a GitHub web-server URL.
func NormalizeServerURL(value string) (string, error) {
	return endpointurl.Normalize(value, "GitHub server URL", false)
}

// NormalizeActionCloneBaseURL validates and canonicalizes the base URL used to
// clone external action repositories.
func NormalizeActionCloneBaseURL(value string) (string, error) {
	return endpointurl.Normalize(value, "action clone base URL", false)
}

// RefName returns the short GitHub Actions name for a fully qualified ref.
func RefName(ref string) string {
	for _, prefix := range []string{"refs/heads/", "refs/tags/", "refs/pull/"} {
		if strings.HasPrefix(ref, prefix) {
			return strings.TrimPrefix(ref, prefix)
		}
	}
	return strings.TrimPrefix(ref, "refs/")
}

func NewClient(baseURL string, httpClient *http.Client) (*Client, error) {
	normalized, err := NormalizeAPIURL(baseURL)
	if err != nil {
		return nil, err
	}
	parsed, err := url.Parse(normalized)
	if err != nil {
		return nil, fmt.Errorf("parse normalized GitHub API URL: %w", err)
	}
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 30 * time.Second}
	}
	return &Client{baseURL: parsed, httpClient: httpClient, now: time.Now}, nil
}

func ValidatePrivateKey(data []byte) error {
	_, err := parsePrivateKey(data)
	return err
}

func (c *Client) Installation(ctx context.Context, appID, installationID int64, privateKey []byte, repository string, permissions InstallationPermissions) (*InstallationClient, error) {
	if repository == "" {
		return nil, errors.New("repository must be specified for an installation token")
	}
	return c.installation(ctx, appID, installationID, privateKey, []string{repository}, permissions)
}

func (c *Client) InstallationForAllRepositories(ctx context.Context, appID, installationID int64, privateKey []byte, permissions InstallationPermissions) (*InstallationClient, error) {
	return c.installation(ctx, appID, installationID, privateKey, nil, permissions)
}

func (c *Client) installation(ctx context.Context, appID, installationID int64, privateKey []byte, repositories []string, permissions InstallationPermissions) (*InstallationClient, error) {
	if err := permissions.validate(); err != nil {
		return nil, err
	}
	key, err := parsePrivateKey(privateKey)
	if err != nil {
		return nil, err
	}
	jwt, err := signJWT(key, appID, c.now())
	if err != nil {
		return nil, err
	}
	requestPath := fmt.Sprintf("app/installations/%d/access_tokens", installationID)
	response := struct {
		Token string `json:"token"`
	}{}
	tokenPermissions := permissions
	if len(tokenPermissions) == 0 {
		tokenPermissions = InstallationPermissions{"metadata": "read"}
	}
	tokenRequest := map[string]any{"permissions": tokenPermissions}
	if len(repositories) > 0 {
		tokenRequest["repositories"] = repositories
	}
	if err := c.doJSONWithBody(ctx, http.MethodPost, requestPath, jwt, tokenRequest, &response); err != nil {
		return nil, fmt.Errorf("create installation access token: %w", err)
	}
	if response.Token == "" {
		return nil, errors.New("GitHub returned an empty installation access token")
	}
	return &InstallationClient{client: c, token: response.Token}, nil
}

func (c *InstallationClient) Token() string {
	return c.token
}

// Revoke invalidates this installation access token.
func (c *InstallationClient) Revoke(ctx context.Context) error {
	return c.client.RevokeInstallationToken(ctx, c.token)
}

// RevokeInstallationToken invalidates an installation access token. An already
// expired or revoked token is considered successfully revoked.
func (c *Client) RevokeInstallationToken(ctx context.Context, token string) error {
	if token == "" {
		return errors.New("installation token is required")
	}
	err := c.doJSONWithBody(ctx, http.MethodDelete, "installation/token", token, nil, nil)
	apiError := &APIError{}
	if errors.As(err, &apiError) && apiError.StatusCode == http.StatusUnauthorized {
		return nil
	}
	return err
}

func (c *InstallationClient) ListRepositories(ctx context.Context, limit int) ([]Repository, error) {
	if limit < 1 {
		return nil, errors.New("repository list limit must be positive")
	}
	const perPage = 100
	repositories := make([]Repository, 0, min(limit, perPage))
	for page := 1; ; page++ {
		response := struct {
			TotalCount   int          `json:"total_count"`
			Repositories []Repository `json:"repositories"`
		}{}
		query := url.Values{"page": []string{strconv.Itoa(page)}, "per_page": []string{strconv.Itoa(perPage)}}
		if err := c.client.doJSONWithQuery(ctx, http.MethodGet, "installation/repositories", query, c.token, &response); err != nil {
			return nil, fmt.Errorf("list installation repositories: %w", err)
		}
		repositories = append(repositories, response.Repositories...)
		if len(repositories) >= limit {
			return repositories[:limit], nil
		}
		if len(response.Repositories) == 0 || len(repositories) >= response.TotalCount {
			return repositories, nil
		}
	}
}

func (c *InstallationClient) ListDirectory(ctx context.Context, owner, repository, directory, revision string) ([]Content, error) {
	requestPath := repositoryContentPath(owner, repository, directory)
	contents := []Content{}
	if err := c.client.doJSONWithQuery(ctx, http.MethodGet, requestPath, url.Values{"ref": []string{revision}}, c.token, &contents); err != nil {
		return nil, fmt.Errorf("list repository directory %q from %s/%s at revision %q: %w", directory, owner, repository, revision, err)
	}
	return contents, nil
}

func (c *InstallationClient) GetFile(ctx context.Context, owner, repository, filePath, revision string) ([]byte, error) {
	requestPath := repositoryContentPath(owner, repository, filePath)
	content := struct {
		Content  string `json:"content"`
		Encoding string `json:"encoding"`
	}{}
	if err := c.client.doJSONWithQuery(ctx, http.MethodGet, requestPath, url.Values{"ref": []string{revision}}, c.token, &content); err != nil {
		return nil, fmt.Errorf("get repository file %q from %s/%s at revision %q: %w", filePath, owner, repository, revision, err)
	}
	if content.Encoding != "base64" {
		return nil, fmt.Errorf("unsupported GitHub content encoding %q", content.Encoding)
	}
	decoded, err := base64.StdEncoding.DecodeString(strings.ReplaceAll(content.Content, "\n", ""))
	if err != nil {
		return nil, fmt.Errorf("decode repository file: %w", err)
	}
	return decoded, nil
}

// ResolveRevision resolves a branch, tag, or commit expression to a full commit
// SHA.
func (c *InstallationClient) ResolveRevision(ctx context.Context, owner, repository, revision string) (string, error) {
	commit, err := c.resolveRevision(ctx, owner, repository, revision)
	if err != nil {
		return "", err
	}
	return commit.SHA, nil
}

func (c *InstallationClient) resolveRevision(ctx context.Context, owner, repository, revision string) (repositoryCommit, error) {
	identity := fmt.Sprintf("resolve repository revision %q from %s/%s", revision, owner, repository)
	requestPath := "repos/" + owner + "/" + repository + "/commits"
	commits := []repositoryCommit{}
	if err := c.client.doJSONWithQuery(ctx, http.MethodGet, requestPath, url.Values{"sha": []string{revision}, "per_page": []string{"1"}}, c.token, &commits); err != nil {
		return repositoryCommit{}, fmt.Errorf("%s: %w", identity, err)
	}
	if len(commits) == 0 {
		return repositoryCommit{}, fmt.Errorf("%s: GitHub returned no commits", identity)
	}
	commit := commits[0]
	sha := commit.SHA
	decoded, err := hex.DecodeString(sha)
	if err != nil || len(decoded) != gitSHA1Bytes || sha != strings.ToLower(sha) {
		return repositoryCommit{}, fmt.Errorf("%s: GitHub returned invalid commit SHA %q", identity, sha)
	}
	return commit, nil
}

// ResolveMergeBase returns the common ancestor selected by GitHub for two
// commits in one repository.
func (c *InstallationClient) ResolveMergeBase(ctx context.Context, owner, repository, baseSHA, headSHA string) (string, error) {
	identity := fmt.Sprintf("resolve merge base for %s and %s from %s/%s", baseSHA, headSHA, owner, repository)
	requestPath := "repos/" + owner + "/" + repository + "/compare/" + baseSHA + "..." + headSHA
	response := struct {
		MergeBaseCommit repositoryCommit `json:"merge_base_commit"`
	}{}
	if err := c.client.doJSONWithQuery(ctx, http.MethodGet, requestPath, url.Values{"per_page": []string{"1"}}, c.token, &response); err != nil {
		return "", fmt.Errorf("%s: %w", identity, err)
	}
	sha := response.MergeBaseCommit.SHA
	decoded, err := hex.DecodeString(sha)
	if err != nil || len(decoded) != gitSHA1Bytes || sha != strings.ToLower(sha) {
		return "", fmt.Errorf("%s: GitHub returned invalid commit SHA %q", identity, sha)
	}
	return sha, nil
}

// CreateCheckRun creates a check run for a repository commit.
func (c *InstallationClient) CreateCheckRun(ctx context.Context, owner, repository string, request CreateCheckRunRequest) (*CheckRun, error) {
	result := &CheckRun{}
	requestPath := "repos/" + owner + "/" + repository + "/check-runs"
	if err := c.client.doJSONWithBody(ctx, http.MethodPost, requestPath, c.token, request, result); err != nil {
		return nil, fmt.Errorf("create check run for %s/%s: %w", owner, repository, err)
	}
	return result, nil
}

// UpdateCheckRun updates a check run created by the authenticated GitHub App.
func (c *InstallationClient) UpdateCheckRun(ctx context.Context, owner, repository string, id int64, request UpdateCheckRunRequest) (*CheckRun, error) {
	result := &CheckRun{}
	requestPath := "repos/" + owner + "/" + repository + "/check-runs/" + strconv.FormatInt(id, 10)
	if err := c.client.doJSONWithBody(ctx, http.MethodPatch, requestPath, c.token, request, result); err != nil {
		return nil, fmt.Errorf("update check run %d for %s/%s: %w", id, owner, repository, err)
	}
	return result, nil
}

// FindCheckRun returns the check run with an external ID for an app and commit.
func (c *InstallationClient) FindCheckRun(ctx context.Context, owner, repository, revision string, appID int64, externalID string) (*CheckRun, error) {
	requestPath := "repos/" + owner + "/" + repository + "/commits/" + revision + "/check-runs"
	for page := 1; ; page++ {
		response := struct {
			TotalCount int        `json:"total_count"`
			CheckRuns  []CheckRun `json:"check_runs"`
		}{}
		query := url.Values{
			"app_id":   []string{strconv.FormatInt(appID, 10)},
			"filter":   []string{"all"},
			"page":     []string{strconv.Itoa(page)},
			"per_page": []string{"100"},
		}
		if err := c.client.doJSONWithQuery(ctx, http.MethodGet, requestPath, query, c.token, &response); err != nil {
			return nil, fmt.Errorf("list check runs for %s/%s at revision %q: %w", owner, repository, revision, err)
		}
		for index := range response.CheckRuns {
			if response.CheckRuns[index].ExternalID == externalID {
				return &response.CheckRuns[index], nil
			}
		}
		if len(response.CheckRuns) == 0 || page*100 >= response.TotalCount {
			return nil, nil
		}
	}
}

func (c *Client) doJSON(ctx context.Context, method, requestPath, token string, destination any) error {
	return c.doJSONWithBodyAndQuery(ctx, method, requestPath, nil, token, nil, destination)
}

func (c *Client) doJSONWithQuery(ctx context.Context, method, requestPath string, query url.Values, token string, destination any) error {
	return c.doJSONWithBodyAndQuery(ctx, method, requestPath, query, token, nil, destination)
}

func (c *Client) doJSONWithBody(ctx context.Context, method, requestPath, token string, requestBody, destination any) error {
	return c.doJSONWithBodyAndQuery(ctx, method, requestPath, nil, token, requestBody, destination)
}

func (c *Client) doJSONWithBodyAndQuery(ctx context.Context, method, requestPath string, query url.Values, token string, requestBody, destination any) error {
	reference := &url.URL{Path: requestPath, RawQuery: query.Encode()}
	requestURL := c.baseURL.ResolveReference(reference)
	var requestReader io.Reader
	if requestBody != nil {
		encoded, err := json.Marshal(requestBody)
		if err != nil {
			return fmt.Errorf("encode GitHub request: %w", err)
		}
		requestReader = bytes.NewReader(encoded)
	}
	request, err := http.NewRequestWithContext(ctx, method, requestURL.String(), requestReader)
	if err != nil {
		return err
	}
	request.Header.Set("Accept", "application/vnd.github+json")
	if requestBody != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("User-Agent", "open-actions")
	request.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	response, err := c.httpClient.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	responseBody, err := io.ReadAll(io.LimitReader(response.Body, maxResponseBytes+1))
	if err != nil {
		return err
	}
	if len(responseBody) > maxResponseBytes {
		return errors.New("GitHub response exceeds size limit")
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		apiError := &APIError{StatusCode: response.StatusCode, Status: response.Status}
		message := struct {
			Message string `json:"message"`
		}{}
		if json.Unmarshal(responseBody, &message) == nil {
			apiError.Message = message.Message
		}
		if apiError.Message == "" {
			apiError.Message = strings.TrimSpace(string(responseBody))
		}
		return apiError
	}
	if destination == nil {
		return nil
	}
	if err := json.Unmarshal(responseBody, destination); err != nil {
		return fmt.Errorf("decode GitHub response: %w", err)
	}
	return nil
}

func repositoryContentPath(owner, repository, contentPath string) string {
	segments := strings.Split(path.Clean(contentPath), "/")
	return "repos/" + owner + "/" + repository + "/contents/" + strings.Join(segments, "/")
}

func parsePrivateKey(data []byte) (*rsa.PrivateKey, error) {
	block, _ := pem.Decode(data)
	if block == nil {
		return nil, errors.New("GitHub App private key is not PEM encoded")
	}
	if key, err := x509.ParsePKCS1PrivateKey(block.Bytes); err == nil {
		return key, nil
	}
	parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("parse GitHub App private key: %w", err)
	}
	key, ok := parsed.(*rsa.PrivateKey)
	if !ok {
		return nil, errors.New("GitHub App private key must be RSA")
	}
	return key, nil
}

func signJWT(key *rsa.PrivateKey, appID int64, now time.Time) (string, error) {
	header, err := json.Marshal(map[string]string{"alg": "RS256", "typ": "JWT"})
	if err != nil {
		return "", err
	}
	claims, err := json.Marshal(map[string]any{
		"iat": now.Add(-60 * time.Second).Unix(),
		"exp": now.Add(9 * time.Minute).Unix(),
		"iss": strconv.FormatInt(appID, 10),
	})
	if err != nil {
		return "", err
	}
	unsigned := base64.RawURLEncoding.EncodeToString(header) + "." + base64.RawURLEncoding.EncodeToString(claims)
	digest := sha256.Sum256([]byte(unsigned))
	signature, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, digest[:])
	if err != nil {
		return "", fmt.Errorf("sign GitHub App JWT: %w", err)
	}
	return unsigned + "." + base64.RawURLEncoding.EncodeToString(signature), nil
}
