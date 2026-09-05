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
	"sync"
	"time"

	"github.com/kelos-dev/open-actions/internal/endpointurl"
)

const (
	maxResponseBytes        = 4 << 20
	gitSHA1Bytes            = 20
	installationTokenSkew   = 5 * time.Minute
	defaultRateLimitBackoff = time.Minute
)

type Client struct {
	baseURL             *url.URL
	httpClient          *http.Client
	now                 func() time.Time
	installationMutex   sync.Mutex
	installationCache   map[installationCacheKey]cachedInstallation
	installationPending map[installationCacheKey]*installationRequest
	rateLimitMutex      sync.Mutex
	rateLimitReset      map[int64]time.Time
}

type InstallationClient struct {
	client         *Client
	token          string
	expiresAt      time.Time
	installationID int64
	cacheKey       *installationCacheKey
}

type installationCacheKey struct {
	AppID          int64
	InstallationID int64
	PrivateKeyHash [sha256.Size]byte
	Repository     string
	Permissions    string
}

type cachedInstallation struct {
	client    *InstallationClient
	expiresAt time.Time
}

type installationRequest struct {
	done   chan struct{}
	client *InstallationClient
	err    error
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

// CommitStatus is the GitHub representation needed after reporting a status.
type CommitStatus struct {
	ID          int64  `json:"id"`
	State       string `json:"state"`
	TargetURL   string `json:"target_url"`
	Description string `json:"description"`
	Context     string `json:"context"`
	Creator     struct {
		Login string `json:"login"`
	} `json:"creator"`
}

// CreateCommitStatusRequest describes a commit status report.
type CreateCommitStatusRequest struct {
	State       string `json:"state"`
	TargetURL   string `json:"target_url,omitempty"`
	Description string `json:"description,omitempty"`
	Context     string `json:"context"`
}

// ListCommitStatuses returns commit statuses in reverse chronological order.
func (c *InstallationClient) ListCommitStatuses(ctx context.Context, owner, repository, revision string) ([]CommitStatus, error) {
	const perPage = 100
	statuses := []CommitStatus{}
	requestPath := "repos/" + owner + "/" + repository + "/commits/" + revision + "/statuses"
	for page := 1; ; page++ {
		response := []CommitStatus{}
		query := url.Values{"page": []string{strconv.Itoa(page)}, "per_page": []string{strconv.Itoa(perPage)}}
		if err := c.doJSONWithQuery(ctx, http.MethodGet, requestPath, query, &response); err != nil {
			return nil, fmt.Errorf("list commit statuses for %s/%s at revision %q: %w", owner, repository, revision, err)
		}
		statuses = append(statuses, response...)
		if len(response) < perPage {
			return statuses, nil
		}
	}
}

// APIError describes a non-success response from the GitHub API.
type APIError struct {
	StatusCode         int
	Status             string
	Message            string
	RetryAfter         time.Duration
	RateLimitRemaining *int
	RateLimitReset     time.Time
}

func (e *APIError) Error() string {
	if e.Message == "" {
		return fmt.Sprintf("GitHub API returned %s", e.Status)
	}
	return fmt.Sprintf("GitHub API returned %s: %s", e.Status, e.Message)
}

// RateLimitError reports that GitHub asked this client to defer requests for
// an installation.
type RateLimitError struct {
	RetryAt time.Time
}

func (e *RateLimitError) Error() string {
	return fmt.Sprintf("GitHub API rate limit remains active until %s", e.RetryAt.UTC().Format(time.RFC3339))
}

// RetryDelay returns how long a rate-limited operation should wait before it
// is attempted again.
func RetryDelay(err error, now time.Time) (time.Duration, bool) {
	if err == nil {
		return 0, false
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		var delay time.Duration
		for _, nested := range joined.Unwrap() {
			nestedDelay, limited := RetryDelay(nested, now)
			if !limited {
				return 0, false
			}
			delay = max(delay, nestedDelay)
		}
		return delay, delay > 0
	}
	if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		return RetryDelay(wrapped.Unwrap(), now)
	}
	if rateLimitError, ok := err.(*RateLimitError); ok {
		return max(rateLimitError.RetryAt.Sub(now), time.Second), true
	}
	apiError, ok := err.(*APIError)
	if !ok {
		return 0, false
	}
	if apiError.RetryAfter > 0 {
		return apiError.RetryAfter, true
	}
	if primaryRateLimited(apiError) {
		return max(apiError.RateLimitReset.Sub(now), time.Second), true
	}
	message := strings.ToLower(apiError.Message)
	if apiError.StatusCode == http.StatusTooManyRequests || apiError.StatusCode == http.StatusForbidden && (strings.Contains(message, "rate limit") || strings.Contains(message, "abuse")) {
		return defaultRateLimitBackoff, true
	}
	return 0, false
}

func primaryRateLimited(apiError *APIError) bool {
	return (apiError.StatusCode == http.StatusForbidden || apiError.StatusCode == http.StatusTooManyRequests) &&
		apiError.RateLimitRemaining != nil && *apiError.RateLimitRemaining == 0 && !apiError.RateLimitReset.IsZero()
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
	return &Client{
		baseURL: parsed, httpClient: httpClient, now: time.Now,
		installationCache: map[installationCacheKey]cachedInstallation{}, installationPending: map[installationCacheKey]*installationRequest{},
		rateLimitReset: map[int64]time.Time{},
	}, nil
}

func ValidatePrivateKey(data []byte) error {
	_, err := parsePrivateKey(data)
	return err
}

// AppBotLogin returns the bot login associated with a GitHub App.
func (c *Client) AppBotLogin(ctx context.Context, appID int64, privateKey []byte) (string, error) {
	key, err := parsePrivateKey(privateKey)
	if err != nil {
		return "", err
	}
	token, err := signJWT(key, appID, c.now())
	if err != nil {
		return "", err
	}
	response := struct {
		ID   int64  `json:"id"`
		Slug string `json:"slug"`
	}{}
	if err := c.doJSONWithQueryForInstallation(ctx, 0, http.MethodGet, "app", nil, token, &response); err != nil {
		return "", fmt.Errorf("get authenticated GitHub App: %w", err)
	}
	if response.ID != appID || response.Slug == "" {
		return "", errors.New("GitHub returned an invalid App identity")
	}
	return response.Slug + "[bot]", nil
}

func (c *Client) Installation(ctx context.Context, appID, installationID int64, privateKey []byte, repository string, permissions InstallationPermissions) (*InstallationClient, error) {
	if repository == "" {
		return nil, errors.New("repository must be specified for an installation token")
	}
	return c.installation(ctx, appID, installationID, privateKey, []string{repository}, permissions)
}

// CachedInstallation returns a repository-scoped installation client and
// reuses its token until shortly before GitHub expires it. It is intended for
// control-plane requests; workflow jobs should continue to use Installation so
// every job receives an independently revocable token.
func (c *Client) CachedInstallation(ctx context.Context, appID, installationID int64, privateKey []byte, repository string, permissions InstallationPermissions) (*InstallationClient, error) {
	if repository == "" {
		return nil, errors.New("repository must be specified for an installation token")
	}
	return c.cachedInstallation(ctx, appID, installationID, privateKey, []string{repository}, repository, permissions)
}

func (c *Client) InstallationForAllRepositories(ctx context.Context, appID, installationID int64, privateKey []byte, permissions InstallationPermissions) (*InstallationClient, error) {
	return c.installation(ctx, appID, installationID, privateKey, nil, permissions)
}

// CachedInstallationForAllRepositories returns a cached installation client
// with access to every repository granted to the installation.
func (c *Client) CachedInstallationForAllRepositories(ctx context.Context, appID, installationID int64, privateKey []byte, permissions InstallationPermissions) (*InstallationClient, error) {
	return c.cachedInstallation(ctx, appID, installationID, privateKey, nil, "", permissions)
}

func (c *Client) cachedInstallation(ctx context.Context, appID, installationID int64, privateKey []byte, repositories []string, repository string, permissions InstallationPermissions) (*InstallationClient, error) {
	if err := permissions.validate(); err != nil {
		return nil, err
	}
	key := installationCacheKey{
		AppID: appID, InstallationID: installationID, PrivateKeyHash: sha256.Sum256(privateKey),
		Repository: repository, Permissions: permissions.String(),
	}
	now := c.now()
	c.installationMutex.Lock()
	c.pruneInstallationCache(now)
	if cached, found := c.installationCache[key]; found && cached.expiresAt.After(now.Add(installationTokenSkew)) {
		c.installationMutex.Unlock()
		return cached.client, nil
	}
	if pending, found := c.installationPending[key]; found {
		c.installationMutex.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-pending.done:
			return pending.client, pending.err
		}
	}
	pending := &installationRequest{done: make(chan struct{})}
	c.installationPending[key] = pending
	c.installationMutex.Unlock()

	installation, expiresAt, err := c.createInstallation(ctx, appID, installationID, privateKey, repositories, permissions)
	if installation != nil {
		installation.cacheKey = &key
	}
	c.installationMutex.Lock()
	pending.client, pending.err = installation, err
	delete(c.installationPending, key)
	if err == nil && expiresAt.After(c.now().Add(installationTokenSkew)) {
		c.installationCache[key] = cachedInstallation{client: installation, expiresAt: expiresAt}
	}
	close(pending.done)
	c.installationMutex.Unlock()
	return installation, err
}

func (c *Client) pruneInstallationCache(now time.Time) {
	for key, cached := range c.installationCache {
		if !cached.expiresAt.After(now.Add(installationTokenSkew)) {
			delete(c.installationCache, key)
		}
	}
}

func (c *Client) installation(ctx context.Context, appID, installationID int64, privateKey []byte, repositories []string, permissions InstallationPermissions) (*InstallationClient, error) {
	installation, _, err := c.createInstallation(ctx, appID, installationID, privateKey, repositories, permissions)
	return installation, err
}

func (c *Client) createInstallation(ctx context.Context, appID, installationID int64, privateKey []byte, repositories []string, permissions InstallationPermissions) (*InstallationClient, time.Time, error) {
	if err := permissions.validate(); err != nil {
		return nil, time.Time{}, err
	}
	key, err := parsePrivateKey(privateKey)
	if err != nil {
		return nil, time.Time{}, err
	}
	jwt, err := signJWT(key, appID, c.now())
	if err != nil {
		return nil, time.Time{}, err
	}
	requestPath := fmt.Sprintf("app/installations/%d/access_tokens", installationID)
	response := struct {
		Token     string    `json:"token"`
		ExpiresAt time.Time `json:"expires_at"`
	}{}
	tokenPermissions := permissions
	if len(tokenPermissions) == 0 {
		tokenPermissions = InstallationPermissions{"metadata": "read"}
	}
	tokenRequest := map[string]any{"permissions": tokenPermissions}
	if len(repositories) > 0 {
		tokenRequest["repositories"] = repositories
	}
	if err := c.doJSONWithBodyForInstallation(ctx, installationID, http.MethodPost, requestPath, jwt, tokenRequest, &response); err != nil {
		return nil, time.Time{}, fmt.Errorf("create installation access token: %w", err)
	}
	if response.Token == "" {
		return nil, time.Time{}, errors.New("GitHub returned an empty installation access token")
	}
	return &InstallationClient{client: c, token: response.Token, expiresAt: response.ExpiresAt, installationID: installationID}, response.ExpiresAt, nil
}

func (c *InstallationClient) Token() string {
	return c.token
}

// ExpiresAt returns when GitHub expires the installation token.
func (c *InstallationClient) ExpiresAt() time.Time {
	return c.expiresAt
}

// Revoke invalidates this installation access token.
func (c *InstallationClient) Revoke(ctx context.Context) error {
	c.client.invalidateCachedInstallation(c.cacheKey, c.token)
	return c.client.revokeInstallationToken(ctx, c.installationID, c.token)
}

// RevokeInstallationToken invalidates an installation access token. An already
// expired or revoked token is considered successfully revoked.
func (c *Client) RevokeInstallationToken(ctx context.Context, token string) error {
	return c.revokeInstallationToken(ctx, 0, token)
}

func (c *Client) revokeInstallationToken(ctx context.Context, installationID int64, token string) error {
	if token == "" {
		return errors.New("installation token is required")
	}
	err := c.doJSONWithBodyForInstallation(ctx, installationID, http.MethodDelete, "installation/token", token, nil, nil)
	apiError := &APIError{}
	if errors.As(err, &apiError) && apiError.StatusCode == http.StatusUnauthorized {
		return nil
	}
	return err
}

func (c *Client) invalidateCachedInstallation(key *installationCacheKey, token string) {
	if key == nil {
		return
	}
	c.installationMutex.Lock()
	defer c.installationMutex.Unlock()
	if cached, found := c.installationCache[*key]; found && cached.client.token == token {
		delete(c.installationCache, *key)
	}
}

func (c *InstallationClient) finish(err error) error {
	apiError := &APIError{}
	if errors.As(err, &apiError) && apiError.StatusCode == http.StatusUnauthorized {
		c.client.invalidateCachedInstallation(c.cacheKey, c.token)
	}
	return err
}

func (c *InstallationClient) doJSONWithQuery(ctx context.Context, method, requestPath string, query url.Values, destination any) error {
	return c.finish(c.client.doJSONWithQueryForInstallation(ctx, c.installationID, method, requestPath, query, c.token, destination))
}

func (c *InstallationClient) doJSONWithBody(ctx context.Context, method, requestPath string, requestBody, destination any) error {
	return c.finish(c.client.doJSONWithBodyForInstallation(ctx, c.installationID, method, requestPath, c.token, requestBody, destination))
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
		if err := c.doJSONWithQuery(ctx, http.MethodGet, "installation/repositories", query, &response); err != nil {
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

// GetRepository returns the repository identified by its owner and name.
func (c *InstallationClient) GetRepository(ctx context.Context, owner, repository string) (Repository, error) {
	result := Repository{}
	requestPath := "repos/" + owner + "/" + repository
	if err := c.doJSONWithQuery(ctx, http.MethodGet, requestPath, nil, &result); err != nil {
		return Repository{}, fmt.Errorf("get repository %s/%s: %w", owner, repository, err)
	}
	return result, nil
}

func (c *InstallationClient) ListDirectory(ctx context.Context, owner, repository, directory, revision string) ([]Content, error) {
	requestPath := repositoryContentPath(owner, repository, directory)
	contents := []Content{}
	if err := c.doJSONWithQuery(ctx, http.MethodGet, requestPath, url.Values{"ref": []string{revision}}, &contents); err != nil {
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
	if err := c.doJSONWithQuery(ctx, http.MethodGet, requestPath, url.Values{"ref": []string{revision}}, &content); err != nil {
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
	if err := c.doJSONWithQuery(ctx, http.MethodGet, requestPath, url.Values{"sha": []string{revision}, "per_page": []string{"1"}}, &commits); err != nil {
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
	if err := c.doJSONWithQuery(ctx, http.MethodGet, requestPath, url.Values{"per_page": []string{"1"}}, &response); err != nil {
		return "", fmt.Errorf("%s: %w", identity, err)
	}
	sha := response.MergeBaseCommit.SHA
	decoded, err := hex.DecodeString(sha)
	if err != nil || len(decoded) != gitSHA1Bytes || sha != strings.ToLower(sha) {
		return "", fmt.Errorf("%s: GitHub returned invalid commit SHA %q", identity, sha)
	}
	return sha, nil
}

// CreateCommitStatus reports a status for a repository commit.
func (c *InstallationClient) CreateCommitStatus(ctx context.Context, owner, repository, revision string, request CreateCommitStatusRequest) (*CommitStatus, error) {
	result := &CommitStatus{}
	requestPath := "repos/" + owner + "/" + repository + "/statuses/" + revision
	if err := c.doJSONWithBody(ctx, http.MethodPost, requestPath, request, result); err != nil {
		return nil, fmt.Errorf("create commit status for %s/%s at revision %q: %w", owner, repository, revision, err)
	}
	return result, nil
}

func (c *Client) doJSONWithQueryForInstallation(ctx context.Context, installationID int64, method, requestPath string, query url.Values, token string, destination any) error {
	return c.doJSONWithBodyAndQueryForInstallation(ctx, installationID, method, requestPath, query, token, nil, destination)
}

func (c *Client) doJSONWithBodyForInstallation(ctx context.Context, installationID int64, method, requestPath, token string, requestBody, destination any) error {
	return c.doJSONWithBodyAndQueryForInstallation(ctx, installationID, method, requestPath, nil, token, requestBody, destination)
}

func (c *Client) doJSONWithBodyAndQueryForInstallation(ctx context.Context, installationID int64, method, requestPath string, query url.Values, token string, requestBody, destination any) error {
	if err := c.checkRateLimit(installationID); err != nil {
		return err
	}
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
		apiError.RetryAfter = retryAfter(response.Header.Get("Retry-After"), c.now())
		if remaining, parseErr := strconv.Atoi(response.Header.Get("X-RateLimit-Remaining")); parseErr == nil {
			apiError.RateLimitRemaining = &remaining
		}
		if reset, parseErr := strconv.ParseInt(response.Header.Get("X-RateLimit-Reset"), 10, 64); parseErr == nil {
			apiError.RateLimitReset = time.Unix(reset, 0)
		}
		message := struct {
			Message string `json:"message"`
		}{}
		if json.Unmarshal(responseBody, &message) == nil {
			apiError.Message = message.Message
		}
		if apiError.Message == "" {
			apiError.Message = strings.TrimSpace(string(responseBody))
		}
		if delay, limited := RetryDelay(apiError, c.now()); limited {
			if scope, record := rateLimitScope(apiError, installationID); record {
				c.recordRateLimit(scope, c.now().Add(delay))
			}
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

func rateLimitScope(apiError *APIError, installationID int64) (int64, bool) {
	if primaryRateLimited(apiError) {
		return installationID, installationID != 0
	}
	return 0, true
}

func (c *Client) checkRateLimit(installationID int64) error {
	now := c.now()
	c.rateLimitMutex.Lock()
	defer c.rateLimitMutex.Unlock()
	retryAt := c.rateLimitReset[installationID]
	if installationID != 0 {
		if globalRetryAt := c.rateLimitReset[0]; globalRetryAt.After(retryAt) {
			retryAt = globalRetryAt
		}
	}
	if retryAt.After(now) {
		return &RateLimitError{RetryAt: retryAt}
	}
	delete(c.rateLimitReset, installationID)
	return nil
}

func (c *Client) recordRateLimit(installationID int64, retryAt time.Time) {
	c.rateLimitMutex.Lock()
	defer c.rateLimitMutex.Unlock()
	if retryAt.After(c.rateLimitReset[installationID]) {
		c.rateLimitReset[installationID] = retryAt
	}
}

func retryAfter(value string, now time.Time) time.Duration {
	if seconds, err := strconv.ParseInt(value, 10, 64); err == nil && seconds > 0 {
		return time.Duration(seconds) * time.Second
	}
	if retryAt, err := http.ParseTime(value); err == nil {
		return max(retryAt.Sub(now), time.Second)
	}
	return 0
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
