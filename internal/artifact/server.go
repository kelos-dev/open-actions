package artifact

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

const artifactServicePath = "/twirp/github.actions.results.api.v1.ArtifactService/"

type ServerConfig struct {
	Address   string
	PublicURL string
	Store     *Store
	Tokens    *TokenCodec
	Logger    *slog.Logger
	Now       func() time.Time
}

type Server struct {
	address   string
	publicURL string
	store     *Store
	tokens    *TokenCodec
	logger    *slog.Logger
	now       func() time.Time
	ready     atomic.Bool
	handler   http.Handler
}

func NewServer(config ServerConfig) (*Server, error) {
	if config.Address == "" || config.PublicURL == "" || config.Store == nil || config.Tokens == nil || config.Logger == nil {
		return nil, errors.New("artifact server configuration is incomplete")
	}
	parsed, err := url.Parse(config.PublicURL)
	if err != nil || parsed.Scheme != "http" && parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || parsed.Path != "" && parsed.Path != "/" {
		return nil, errors.New("artifact service URL must be an HTTP or HTTPS origin")
	}
	now := config.Now
	if now == nil {
		now = time.Now
	}
	server := &Server{address: config.Address, publicURL: strings.TrimSuffix(parsed.String(), "/"), store: config.Store, tokens: config.Tokens, logger: config.Logger, now: now}
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", server.health)
	mux.HandleFunc("/readyz", server.readiness)
	mux.HandleFunc(artifactServicePath+"CreateArtifact", server.createArtifact)
	mux.HandleFunc(artifactServicePath+"FinalizeArtifact", server.finalizeArtifact)
	mux.HandleFunc(artifactServicePath+"ListArtifacts", server.listArtifacts)
	mux.HandleFunc(artifactServicePath+"GetSignedArtifactURL", server.getSignedArtifactURL)
	mux.HandleFunc(artifactServicePath+"DeleteArtifact", server.deleteArtifact)
	mux.HandleFunc("/artifacts/blobs/", server.blob)
	server.handler = mux
	return server, nil
}

func (s *Server) Handler() http.Handler {
	return s.handler
}

func (s *Server) Start(ctx context.Context) error {
	if err := s.store.Cleanup(); err != nil {
		return fmt.Errorf("clean artifact storage: %w", err)
	}
	listener, err := net.Listen("tcp", s.address)
	if err != nil {
		return err
	}
	httpServer := &http.Server{
		Handler: s.handler, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Minute,
		WriteTimeout: 10 * time.Minute, IdleTimeout: 60 * time.Second,
	}
	s.ready.Store(true)
	defer s.ready.Store(false)
	go s.cleanup(ctx)
	go func() {
		<-ctx.Done()
		shutdownContext, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = httpServer.Shutdown(shutdownContext)
	}()
	s.logger.Info("starting artifact service", "address", listener.Addr().String())
	if err := httpServer.Serve(listener); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

func (s *Server) cleanup(ctx context.Context) {
	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := s.store.Cleanup(); err != nil {
				s.logger.Error("artifact cleanup failed", "error", err)
			}
		}
	}
}

func (s *Server) health(writer http.ResponseWriter, _ *http.Request) {
	writer.WriteHeader(http.StatusOK)
}

func (s *Server) readiness(writer http.ResponseWriter, _ *http.Request) {
	if !s.ready.Load() {
		http.Error(writer, "artifact service is not ready", http.StatusServiceUnavailable)
		return
	}
	writer.WriteHeader(http.StatusOK)
}

type createArtifactRequest struct {
	WorkflowRunBackendID    string     `json:"workflow_run_backend_id"`
	WorkflowJobRunBackendID string     `json:"workflow_job_run_backend_id"`
	Name                    string     `json:"name"`
	ExpiresAt               *time.Time `json:"expires_at"`
	Version                 int32      `json:"version"`
	MIMEType                *string    `json:"mime_type"`
}

type finalizeArtifactRequest struct {
	WorkflowRunBackendID    string  `json:"workflow_run_backend_id"`
	WorkflowJobRunBackendID string  `json:"workflow_job_run_backend_id"`
	Name                    string  `json:"name"`
	Size                    string  `json:"size"`
	Hash                    *string `json:"hash"`
}

type listArtifactsRequest struct {
	WorkflowRunBackendID    string  `json:"workflow_run_backend_id"`
	WorkflowJobRunBackendID string  `json:"workflow_job_run_backend_id"`
	NameFilter              *string `json:"name_filter"`
	IDFilter                *string `json:"id_filter"`
}

type artifactIdentityRequest struct {
	WorkflowRunBackendID    string `json:"workflow_run_backend_id"`
	WorkflowJobRunBackendID string `json:"workflow_job_run_backend_id"`
	Name                    string `json:"name"`
}

func (s *Server) createArtifact(writer http.ResponseWriter, request *http.Request) {
	claims, ok := s.authenticateTwirp(writer, request)
	if !ok {
		return
	}
	input := createArtifactRequest{}
	if !decodeTwirp(writer, request, &input) {
		return
	}
	if !currentJobRequest(writer, claims, input.WorkflowRunBackendID, input.WorkflowJobRunBackendID) {
		return
	}
	mimeType := "application/zip"
	if input.MIMEType != nil && *input.MIMEType != "" {
		mimeType = *input.MIMEType
	}
	record, err := s.store.Create(claims.Scope, claims.WorkflowJobBackendID, input.Name, mimeType, input.Version, input.ExpiresAt)
	if err != nil {
		s.logStoreError("create", err)
		writeServiceError(writer, err)
		return
	}
	signedURL, err := s.blobURL(record, claims, "write")
	if err != nil {
		s.logger.Error("artifact URL signing failed", "operation", "write", "error", err)
		writeTwirpError(writer, http.StatusInternalServerError, "internal", "artifact URL signing failed")
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"ok": true, "signed_upload_url": signedURL})
}

func (s *Server) finalizeArtifact(writer http.ResponseWriter, request *http.Request) {
	claims, ok := s.authenticateTwirp(writer, request)
	if !ok {
		return
	}
	input := finalizeArtifactRequest{}
	if !decodeTwirp(writer, request, &input) {
		return
	}
	if !currentJobRequest(writer, claims, input.WorkflowRunBackendID, input.WorkflowJobRunBackendID) {
		return
	}
	size, err := strconv.ParseInt(input.Size, 10, 64)
	if err != nil || size < 0 || input.Hash == nil || *input.Hash == "" {
		writeTwirpError(writer, http.StatusBadRequest, "invalid_argument", "artifact size or hash is invalid")
		return
	}
	record, err := s.store.Finalize(claims.Scope, claims.WorkflowJobBackendID, input.Name, size, *input.Hash)
	if err != nil {
		s.logStoreError("finalize", err)
		writeServiceError(writer, err)
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"ok": true, "artifact_id": strconv.FormatInt(record.ID, 10)})
}

func (s *Server) listArtifacts(writer http.ResponseWriter, request *http.Request) {
	claims, ok := s.authenticateTwirp(writer, request)
	if !ok {
		return
	}
	input := listArtifactsRequest{}
	if !decodeTwirp(writer, request, &input) {
		return
	}
	if !currentJobRequest(writer, claims, input.WorkflowRunBackendID, input.WorkflowJobRunBackendID) {
		return
	}
	name := ""
	if input.NameFilter != nil {
		name = *input.NameFilter
	}
	id := int64(0)
	if input.IDFilter != nil {
		var err error
		id, err = strconv.ParseInt(*input.IDFilter, 10, 64)
		if err != nil || id < 1 {
			writeTwirpError(writer, http.StatusBadRequest, "invalid_argument", "artifact ID is invalid")
			return
		}
	}
	records, err := s.store.List(claims.Scope, name, id)
	if err != nil {
		s.logStoreError("list", err)
		writeServiceError(writer, err)
		return
	}
	artifacts := make([]map[string]any, 0, len(records))
	for _, record := range records {
		artifacts = append(artifacts, artifactResponse(record))
	}
	writeJSON(writer, http.StatusOK, map[string]any{"artifacts": artifacts})
}

func (s *Server) getSignedArtifactURL(writer http.ResponseWriter, request *http.Request) {
	claims, ok := s.authenticateTwirp(writer, request)
	if !ok {
		return
	}
	input := artifactIdentityRequest{}
	if !decodeTwirp(writer, request, &input) {
		return
	}
	if input.WorkflowRunBackendID != claims.WorkflowRunBackendID {
		writeTwirpError(writer, http.StatusForbidden, "permission_denied", "artifact belongs to another workflow run")
		return
	}
	record, err := s.findArtifact(claims.Scope, input.WorkflowJobRunBackendID, input.Name)
	if err != nil {
		s.logStoreError("get_signed_url", err)
		writeServiceError(writer, err)
		return
	}
	signedURL, err := s.blobURL(record, claims, "read")
	if err != nil {
		s.logger.Error("artifact URL signing failed", "operation", "read", "error", err)
		writeTwirpError(writer, http.StatusInternalServerError, "internal", "artifact URL signing failed")
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"signed_url": signedURL})
}

func (s *Server) deleteArtifact(writer http.ResponseWriter, request *http.Request) {
	claims, ok := s.authenticateTwirp(writer, request)
	if !ok {
		return
	}
	input := artifactIdentityRequest{}
	if !decodeTwirp(writer, request, &input) {
		return
	}
	if input.WorkflowRunBackendID != claims.WorkflowRunBackendID {
		writeTwirpError(writer, http.StatusForbidden, "permission_denied", "artifact belongs to another workflow run")
		return
	}
	record, err := s.store.Delete(claims.Scope, input.WorkflowJobRunBackendID, input.Name)
	if err != nil {
		s.logStoreError("delete", err)
		writeServiceError(writer, err)
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"ok": true, "artifact_id": strconv.FormatInt(record.ID, 10)})
}

func (s *Server) blob(writer http.ResponseWriter, request *http.Request) {
	path := strings.TrimPrefix(request.URL.Path, "/artifacts/blobs/")
	idText, _, found := strings.Cut(path, "/")
	artifactID, parseErr := strconv.ParseInt(idText, 10, 64)
	if !found || parseErr != nil || artifactID < 1 {
		writeBlobError(writer, http.StatusNotFound, "BlobNotFound", "artifact was not found")
		return
	}
	component := request.URL.Query().Get("comp")
	operation := ""
	switch {
	case request.Method == http.MethodPut && (component == "block" || component == "blocklist"):
		operation = "write"
	case request.Method == http.MethodGet && component == "":
		operation = "read"
	default:
		writer.Header().Set("Allow", "GET, PUT")
		writeBlobError(writer, http.StatusMethodNotAllowed, "UnsupportedHttpVerb", "artifact operation is not supported")
		return
	}
	expiresAt, parseErr := strconv.ParseInt(request.URL.Query().Get("exp"), 10, 64)
	if parseErr != nil {
		writeBlobError(writer, http.StatusUnauthorized, "AuthenticationFailed", "artifact credential is invalid")
		return
	}
	scope, err := s.tokens.ValidateBlob(request.URL.Query().Get("scope"), artifactID, operation, expiresAt, request.URL.Query().Get("sig"), s.now())
	if err != nil {
		writeBlobError(writer, http.StatusUnauthorized, "AuthenticationFailed", "artifact credential is invalid")
		return
	}
	record, err := s.store.Get(scope, artifactID)
	if err != nil {
		s.logStoreError("get_blob", err)
		writeBlobStoreError(writer, err)
		return
	}
	switch {
	case operation == "write" && component == "block":
		if request.ContentLength < 0 {
			writeBlobError(writer, http.StatusLengthRequired, "MissingContentLength", "artifact block content length is required")
			return
		}
		if err := s.store.StageBlock(scope, record.JobBackendID, record.ID, request.URL.Query().Get("blockid"), request.ContentLength, request.Body); err != nil {
			s.logStoreError("stage_block", err)
			writeBlobStoreError(writer, err)
			return
		}
		writeAzureSuccess(writer, http.StatusCreated, record)
	case operation == "write" && component == "blocklist":
		blocks, err := DecodeBlockList(request.Body)
		if err != nil {
			s.logStoreError("decode_block_list", err)
			writeBlobStoreError(writer, err)
			return
		}
		record, err = s.store.Commit(scope, record.JobBackendID, record.ID, blocks)
		if err != nil {
			s.logStoreError("commit_block_list", err)
			writeBlobStoreError(writer, err)
			return
		}
		writeAzureSuccess(writer, http.StatusCreated, record)
	case operation == "read":
		if record.State != artifactStateFinalized {
			writeBlobError(writer, http.StatusNotFound, "BlobNotFound", "artifact was not found")
			return
		}
		file, err := s.store.OpenBlob(record)
		if err != nil {
			s.logStoreError("open_blob", err)
			writeBlobStoreError(writer, err)
			return
		}
		defer file.Close()
		fileName := record.Name
		if isZipMIMEType(record.MIMEType) {
			fileName += ".zip"
		}
		writer.Header().Set("Content-Type", record.MIMEType)
		writer.Header().Set("Content-Disposition", mime.FormatMediaType("attachment", map[string]string{"filename": fileName}))
		http.ServeContent(writer, request, fileName, record.FinalizedAt, file)
	}
}

func (s *Server) authenticateTwirp(writer http.ResponseWriter, request *http.Request) (TokenClaims, bool) {
	if request.Method != http.MethodPost {
		writer.Header().Set("Allow", http.MethodPost)
		writeTwirpError(writer, http.StatusMethodNotAllowed, "bad_route", "method must be POST")
		return TokenClaims{}, false
	}
	authorization := request.Header.Get("Authorization")
	scheme, token, found := strings.Cut(authorization, " ")
	if !found || !strings.EqualFold(scheme, "Bearer") || token == "" {
		writeTwirpError(writer, http.StatusUnauthorized, "unauthenticated", "artifact credential is required")
		return TokenClaims{}, false
	}
	claims, err := s.tokens.ParseRuntimeToken(token, s.now())
	if err != nil {
		writeTwirpError(writer, http.StatusUnauthorized, "unauthenticated", "artifact credential is invalid")
		return TokenClaims{}, false
	}
	return claims, true
}

func (s *Server) findArtifact(scope Scope, jobBackendID, name string) (Artifact, error) {
	records, err := s.store.List(scope, name, 0)
	if err != nil {
		return Artifact{}, err
	}
	for _, record := range records {
		if record.JobBackendID == jobBackendID {
			return record, nil
		}
	}
	return Artifact{}, ErrArtifactNotFound
}

func (s *Server) blobURL(record Artifact, claims TokenClaims, operation string) (string, error) {
	fileName := record.Name
	if isZipMIMEType(record.MIMEType) {
		fileName += ".zip"
	}
	encodedScope, signature, err := s.tokens.SignBlob(record.Scope, record.ID, operation, claims.ExpiresAt)
	if err != nil {
		return "", err
	}
	query := url.Values{
		"exp":   []string{strconv.FormatInt(claims.ExpiresAt, 10)},
		"scope": []string{encodedScope},
		"sig":   []string{signature},
	}
	return s.publicURL + "/artifacts/blobs/" + strconv.FormatInt(record.ID, 10) + "/" + url.PathEscape(fileName) + "?" + query.Encode(), nil
}

func (s *Server) logStoreError(operation string, err error) {
	if errors.Is(err, ErrArtifactExists) || errors.Is(err, ErrArtifactNotFound) || errors.Is(err, ErrInvalidArtifact) || errors.Is(err, ErrLimitExceeded) {
		return
	}
	s.logger.Error("artifact storage operation failed", "operation", operation, "error", err)
}

func artifactResponse(record Artifact) map[string]any {
	return map[string]any{
		"workflow_run_backend_id":     record.Scope.RunUID,
		"workflow_job_run_backend_id": record.JobBackendID,
		"database_id":                 strconv.FormatInt(record.ID, 10),
		"name":                        record.Name,
		"size":                        strconv.FormatInt(record.Size, 10),
		"created_at":                  record.CreatedAt,
		"digest":                      record.Digest,
	}
}

func currentJobRequest(writer http.ResponseWriter, claims TokenClaims, runID, jobID string) bool {
	if runID != claims.WorkflowRunBackendID || jobID != claims.WorkflowJobBackendID {
		writeTwirpError(writer, http.StatusForbidden, "permission_denied", "artifact request is outside this job scope")
		return false
	}
	return true
}

func decodeTwirp(writer http.ResponseWriter, request *http.Request, target any) bool {
	if contentType := strings.Split(request.Header.Get("Content-Type"), ";")[0]; contentType != "application/json" {
		writeTwirpError(writer, http.StatusBadRequest, "malformed", "content type must be application/json")
		return false
	}
	decoder := json.NewDecoder(http.MaxBytesReader(writer, request.Body, 64<<10))
	if err := decoder.Decode(target); err != nil {
		writeTwirpError(writer, http.StatusBadRequest, "malformed", "request body is invalid")
		return false
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		writeTwirpError(writer, http.StatusBadRequest, "malformed", "request body contains trailing data")
		return false
	}
	return true
}

func writeServiceError(writer http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrArtifactExists):
		writeTwirpError(writer, http.StatusConflict, "already_exists", err.Error())
	case errors.Is(err, ErrArtifactNotFound):
		writeTwirpError(writer, http.StatusNotFound, "not_found", err.Error())
	case errors.Is(err, ErrInvalidArtifact):
		writeTwirpError(writer, http.StatusBadRequest, "invalid_argument", err.Error())
	case errors.Is(err, ErrLimitExceeded):
		writeTwirpError(writer, http.StatusRequestEntityTooLarge, "resource_exhausted", err.Error())
	default:
		writeTwirpError(writer, http.StatusInternalServerError, "internal", "artifact storage operation failed")
	}
}

func writeTwirpError(writer http.ResponseWriter, status int, code, message string) {
	writeJSON(writer, status, map[string]string{"code": code, "msg": message})
}

func writeJSON(writer http.ResponseWriter, status int, value any) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(value)
}

type azureError struct {
	XMLName xml.Name `xml:"Error"`
	Code    string   `xml:"Code"`
	Message string   `xml:"Message"`
}

func writeBlobStoreError(writer http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrArtifactNotFound):
		writeBlobError(writer, http.StatusNotFound, "BlobNotFound", err.Error())
	case errors.Is(err, ErrInvalidArtifact):
		writeBlobError(writer, http.StatusBadRequest, "InvalidBlobOrBlock", err.Error())
	case errors.Is(err, ErrLimitExceeded):
		writeBlobError(writer, http.StatusRequestEntityTooLarge, "RequestBodyTooLarge", err.Error())
	default:
		writeBlobError(writer, http.StatusInternalServerError, "InternalError", "artifact storage operation failed")
	}
}

func writeBlobError(writer http.ResponseWriter, status int, code, message string) {
	writer.Header().Set("Content-Type", "application/xml")
	writer.Header().Set("x-ms-error-code", code)
	writer.WriteHeader(status)
	_ = xml.NewEncoder(writer).Encode(azureError{Code: code, Message: message})
}

func writeAzureSuccess(writer http.ResponseWriter, status int, record Artifact) {
	writer.Header().Set("ETag", fmt.Sprintf("\"%x\"", record.ID))
	writer.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
	writer.Header().Set("x-ms-request-id", strconv.FormatInt(record.ID, 16))
	writer.Header().Set("x-ms-version", "2021-12-02")
	writer.Header().Set("x-ms-request-server-encrypted", "true")
	writer.WriteHeader(status)
}
