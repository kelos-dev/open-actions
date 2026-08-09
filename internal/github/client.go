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
	"strconv"
	"strings"
	"time"
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

type repositoryCommit struct {
	SHA     string `json:"sha"`
	Parents []struct {
		SHA string `json:"sha"`
	} `json:"parents"`
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
	return normalizeEndpointURL(value, "GitHub API URL", true)
}

// NormalizeServerURL validates and canonicalizes a GitHub web-server URL.
func NormalizeServerURL(value string) (string, error) {
	return normalizeEndpointURL(value, "GitHub server URL", false)
}

// NormalizeActionCloneBaseURL validates and canonicalizes the base URL used to
// clone external action repositories.
func NormalizeActionCloneBaseURL(value string) (string, error) {
	return normalizeEndpointURL(value, "action clone base URL", false)
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

func (c *Client) Installation(ctx context.Context, appID, installationID int64, privateKey []byte, repository string) (*InstallationClient, error) {
	if repository == "" {
		return nil, errors.New("repository must be specified for an installation token")
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
	tokenRequest := map[string]any{
		"repositories": []string{repository},
		"permissions":  map[string]string{"contents": "read"},
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

// ResolvePullRequestRevision resolves a pull request merge ref and reports
// whether its merge commit includes the expected head commit.
func (c *InstallationClient) ResolvePullRequestRevision(ctx context.Context, owner, repository, revision, headSHA string) (string, bool, error) {
	commit, err := c.resolveRevision(ctx, owner, repository, revision)
	if err != nil {
		return "", false, err
	}
	for _, parent := range commit.Parents {
		if parent.SHA == headSHA {
			return commit.SHA, true, nil
		}
	}
	return commit.SHA, false, nil
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
	if err := json.Unmarshal(responseBody, destination); err != nil {
		return fmt.Errorf("decode GitHub response: %w", err)
	}
	return nil
}

func normalizeEndpointURL(value, label string, trailingSlash bool) (string, error) {
	parsed, err := url.Parse(value)
	if err != nil {
		return "", fmt.Errorf("parse %s: %w", label, err)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return "", fmt.Errorf("%s must use http or https", label)
	}
	if parsed.Host == "" {
		return "", fmt.Errorf("%s must include a host", label)
	}
	if parsed.User != nil {
		return "", fmt.Errorf("%s must not include user information", label)
	}
	if parsed.RawQuery != "" || parsed.ForceQuery {
		return "", fmt.Errorf("%s must not include a query", label)
	}
	if parsed.Fragment != "" {
		return "", fmt.Errorf("%s must not include a fragment", label)
	}
	if parsed.Opaque != "" || parsed.RawPath != "" {
		return "", fmt.Errorf("%s must use an unescaped hierarchical path", label)
	}
	if strings.Contains(parsed.Path, "//") {
		return "", fmt.Errorf("%s path must not contain empty segments", label)
	}
	trimmedPath := strings.TrimRight(parsed.Path, "/")
	if trimmedPath == "" {
		trimmedPath = "/"
	}
	if parsed.Path != "" && path.Clean(parsed.Path) != trimmedPath {
		return "", fmt.Errorf("%s path must not contain empty, '.' or '..' segments", label)
	}
	for _, character := range strings.Trim(parsed.Path, "/") {
		if !isEndpointPathCharacter(character) {
			return "", fmt.Errorf("%s path must contain only URL-safe path characters", label)
		}
	}
	if trailingSlash {
		if trimmedPath == "/" {
			parsed.Path = "/"
		} else {
			parsed.Path = trimmedPath + "/"
		}
	} else if trimmedPath == "/" {
		parsed.Path = ""
	} else {
		parsed.Path = trimmedPath
	}
	return parsed.String(), nil
}

func isEndpointPathCharacter(character rune) bool {
	return character >= 'a' && character <= 'z' ||
		character >= 'A' && character <= 'Z' ||
		character >= '0' && character <= '9' ||
		strings.ContainsRune("-._~/", character)
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
