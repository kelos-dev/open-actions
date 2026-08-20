package artifact

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

const (
	TokenSecretKey        = "artifact-token"
	MinimumSigningKeySize = 32
)

type Scope struct {
	ProjectUID   string `json:"project_uid"`
	RepositoryID int64  `json:"repository_id"`
	RootRunUID   string `json:"root_run_uid"`
	RunUID       string `json:"run_uid"`
	Attempt      int32  `json:"attempt"`
}

type TokenClaims struct {
	Scope
	ScopeClaim           string `json:"scp"`
	WorkflowRunBackendID string `json:"workflow_run_backend_id"`
	WorkflowJobBackendID string `json:"workflow_job_run_backend_id"`
	IssuedAt             int64  `json:"iat"`
	ExpiresAt            int64  `json:"exp"`
	TokenID              string `json:"jti"`
}

type TokenCodec struct {
	key []byte
}

func NewTokenCodec(key []byte) (*TokenCodec, error) {
	if len(key) < MinimumSigningKeySize {
		return nil, fmt.Errorf("artifact signing key must be at least %d bytes", MinimumSigningKeySize)
	}
	return &TokenCodec{key: append([]byte(nil), key...)}, nil
}

func (c *TokenCodec) NewRuntimeToken(now time.Time, lifetime time.Duration, claims TokenClaims) (string, error) {
	if lifetime <= 0 {
		return "", errors.New("artifact token lifetime must be positive")
	}
	if err := validateScope(claims.Scope); err != nil {
		return "", err
	}
	if err := validateIdentity(claims.WorkflowRunBackendID, claims.WorkflowJobBackendID); err != nil {
		return "", err
	}
	claims.IssuedAt = now.Unix()
	claims.ExpiresAt = now.Add(lifetime).Unix()
	claims.ScopeClaim = "Actions.Results:" + claims.WorkflowRunBackendID + ":" + claims.WorkflowJobBackendID
	random := make([]byte, 16)
	if _, err := rand.Read(random); err != nil {
		return "", fmt.Errorf("generate artifact token: %w", err)
	}
	claims.TokenID = base64.RawURLEncoding.EncodeToString(random)
	header, err := json.Marshal(map[string]string{"alg": "HS256", "typ": "JWT"})
	if err != nil {
		return "", err
	}
	payload, err := json.Marshal(claims)
	if err != nil {
		return "", err
	}
	headerPart := base64.RawURLEncoding.EncodeToString(header)
	payloadPart := base64.RawURLEncoding.EncodeToString(payload)
	signed := headerPart + "." + payloadPart
	return signed + "." + base64.RawURLEncoding.EncodeToString(c.sign(signed)), nil
}

func (c *TokenCodec) ParseRuntimeToken(token string, now time.Time) (TokenClaims, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 || parts[0] == "" || parts[1] == "" || parts[2] == "" {
		return TokenClaims{}, errors.New("artifact token is malformed")
	}
	signature, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil || subtle.ConstantTimeCompare(signature, c.sign(parts[0]+"."+parts[1])) != 1 {
		return TokenClaims{}, errors.New("artifact token signature is invalid")
	}
	headerData, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return TokenClaims{}, errors.New("artifact token header is malformed")
	}
	header := struct {
		Algorithm string `json:"alg"`
		Type      string `json:"typ"`
	}{}
	if err := json.Unmarshal(headerData, &header); err != nil || header.Algorithm != "HS256" || header.Type != "JWT" {
		return TokenClaims{}, errors.New("artifact token header is invalid")
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return TokenClaims{}, errors.New("artifact token payload is malformed")
	}
	claims := TokenClaims{}
	if err := json.Unmarshal(payload, &claims); err != nil {
		return TokenClaims{}, errors.New("artifact token payload is invalid")
	}
	if err := validateScope(claims.Scope); err != nil {
		return TokenClaims{}, err
	}
	if err := validateIdentity(claims.WorkflowRunBackendID, claims.WorkflowJobBackendID); err != nil {
		return TokenClaims{}, err
	}
	if claims.ScopeClaim != "Actions.Results:"+claims.WorkflowRunBackendID+":"+claims.WorkflowJobBackendID {
		return TokenClaims{}, errors.New("artifact token scope is invalid")
	}
	if claims.TokenID == "" || claims.IssuedAt <= 0 || claims.ExpiresAt <= claims.IssuedAt || now.Unix() >= claims.ExpiresAt {
		return TokenClaims{}, errors.New("artifact token is expired")
	}
	return claims, nil
}

func (c *TokenCodec) SignBlob(scope Scope, artifactID int64, operation string, expiresAt int64) (string, string, error) {
	if err := validateScope(scope); err != nil {
		return "", "", err
	}
	if artifactID < 1 || expiresAt < 1 || operation != "read" && operation != "write" {
		return "", "", errors.New("artifact blob signature parameters are invalid")
	}
	scopeData, err := json.Marshal(scope)
	if err != nil {
		return "", "", err
	}
	encodedScope := base64.RawURLEncoding.EncodeToString(scopeData)
	signature := base64.RawURLEncoding.EncodeToString(c.sign(blobSignatureInput(encodedScope, artifactID, operation, expiresAt)))
	return encodedScope, signature, nil
}

func (c *TokenCodec) ValidateBlob(encodedScope string, artifactID int64, operation string, expiresAt int64, signature string, now time.Time) (Scope, error) {
	if encodedScope == "" || signature == "" || artifactID < 1 || expiresAt <= now.Unix() || operation != "read" && operation != "write" {
		return Scope{}, errors.New("artifact blob signature is invalid")
	}
	provided, err := base64.RawURLEncoding.DecodeString(signature)
	if err != nil || subtle.ConstantTimeCompare(provided, c.sign(blobSignatureInput(encodedScope, artifactID, operation, expiresAt))) != 1 {
		return Scope{}, errors.New("artifact blob signature is invalid")
	}
	scopeData, err := base64.RawURLEncoding.DecodeString(encodedScope)
	if err != nil {
		return Scope{}, errors.New("artifact blob scope is malformed")
	}
	scope := Scope{}
	if err := json.Unmarshal(scopeData, &scope); err != nil {
		return Scope{}, errors.New("artifact blob scope is invalid")
	}
	if err := validateScope(scope); err != nil {
		return Scope{}, err
	}
	return scope, nil
}

func (c *TokenCodec) sign(value string) []byte {
	mac := hmac.New(sha256.New, c.key)
	_, _ = mac.Write([]byte(value))
	return mac.Sum(nil)
}

func blobSignatureInput(encodedScope string, artifactID int64, operation string, expiresAt int64) string {
	return encodedScope + "\n" + strconv.FormatInt(artifactID, 10) + "\n" + operation + "\n" + strconv.FormatInt(expiresAt, 10)
}

func validateScope(scope Scope) error {
	if scope.ProjectUID == "" || scope.RepositoryID < 1 || scope.RootRunUID == "" || scope.RunUID == "" || scope.Attempt < 1 {
		return errors.New("artifact token claims are incomplete")
	}
	return validateIdentity(scope.ProjectUID, scope.RootRunUID, scope.RunUID)
}

func validateIdentity(values ...string) error {
	for _, value := range values {
		if value == "" || len(value) > 128 || strings.ContainsAny(value, ":\x00") {
			return errors.New("artifact token scope identity is invalid")
		}
	}
	return nil
}
