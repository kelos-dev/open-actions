package artifact

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestArtifactProtocolUploadsAndDownloadsAcrossJobs(t *testing.T) {
	now := time.Unix(1000, 0)
	scope := Scope{ProjectUID: "project-uid", RepositoryID: 123, RootRunUID: "root-uid", RunUID: "run-uid", Attempt: 1}
	uploaderClaims := testClaims("upload-job-uid")
	uploaderClaims.Scope = scope
	downloaderClaims := testClaims("download-job-uid")
	downloaderClaims.Scope = scope
	codec := testTokenCodec(t)
	uploaderToken := testRuntimeToken(t, codec, now, uploaderClaims)
	downloaderToken := testRuntimeToken(t, codec, now, downloaderClaims)
	store, err := NewStore(t.TempDir(), testLimits())
	if err != nil {
		t.Fatal(err)
	}
	store.now = func() time.Time { return now }

	testServer := httptest.NewUnstartedServer(nil)
	publicURL := "http://" + testServer.Listener.Addr().String()
	service, err := NewServer(ServerConfig{
		Address: "127.0.0.1:0", PublicURL: publicURL, Store: store,
		Tokens: codec, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	testServer.Config.Handler = service.Handler()
	testServer.Start()
	defer testServer.Close()

	create := postArtifact(t, publicURL, "CreateArtifact", uploaderToken, map[string]any{
		"workflow_run_backend_id": "run-uid", "workflow_job_run_backend_id": "upload-job-uid",
		"name": "linux-results", "version": 7, "mime_type": "application/octet-stream",
	})
	if create.StatusCode != http.StatusOK {
		t.Fatalf("CreateArtifact status = %d, body = %s", create.StatusCode, readBody(t, create))
	}
	created := struct {
		UploadURL string `json:"signed_upload_url"`
	}{}
	decodeResponse(t, create, &created)
	if strings.Contains(created.UploadURL, uploaderToken) {
		t.Fatal("signed upload URL exposes the runtime token")
	}
	duplicate := postArtifact(t, publicURL, "CreateArtifact", uploaderToken, map[string]any{
		"workflow_run_backend_id": "run-uid", "workflow_job_run_backend_id": "upload-job-uid",
		"name": "linux-results", "version": 7, "mime_type": "application/octet-stream",
	})
	if duplicate.StatusCode != http.StatusConflict {
		t.Fatalf("duplicate CreateArtifact status = %d, body = %s", duplicate.StatusCode, readBody(t, duplicate))
	}
	duplicate.Body.Close()
	uploadURL, err := url.Parse(created.UploadURL)
	if err != nil {
		t.Fatal(err)
	}
	query := uploadURL.Query()
	query.Set("comp", "block")
	query.Set("blockid", "block-1")
	uploadURL.RawQuery = query.Encode()
	response := request(t, http.MethodPut, uploadURL.String(), "", strings.NewReader("artifact content"), "application/octet-stream")
	if response.StatusCode != http.StatusCreated {
		t.Fatalf("stage block status = %d, body = %s", response.StatusCode, readBody(t, response))
	}
	response.Body.Close()
	response = request(t, http.MethodGet, created.UploadURL, "", nil, "")
	if response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("download through upload URL status = %d, body = %s", response.StatusCode, readBody(t, response))
	}
	response.Body.Close()
	query.Set("comp", "blocklist")
	query.Del("blockid")
	uploadURL.RawQuery = query.Encode()
	response = request(t, http.MethodPut, uploadURL.String(), "", strings.NewReader("<BlockList><Latest>block-1</Latest></BlockList>"), "application/xml")
	if response.StatusCode != http.StatusCreated {
		t.Fatalf("commit block list status = %d, body = %s", response.StatusCode, readBody(t, response))
	}
	response.Body.Close()
	digest := sha256.Sum256([]byte("artifact content"))
	finalize := postArtifact(t, publicURL, "FinalizeArtifact", uploaderToken, map[string]any{
		"workflow_run_backend_id": "run-uid", "workflow_job_run_backend_id": "upload-job-uid", "name": "linux-results",
		"size": strconv.Itoa(len("artifact content")), "hash": "sha256:" + hex.EncodeToString(digest[:]),
	})
	if finalize.StatusCode != http.StatusOK {
		t.Fatalf("FinalizeArtifact status = %d, body = %s", finalize.StatusCode, readBody(t, finalize))
	}
	finalized := struct {
		ArtifactID string `json:"artifact_id"`
	}{}
	decodeResponse(t, finalize, &finalized)

	list := postArtifact(t, publicURL, "ListArtifacts", downloaderToken, map[string]any{
		"workflow_run_backend_id": "run-uid", "workflow_job_run_backend_id": "download-job-uid",
	})
	listed := struct {
		Artifacts []struct {
			ID   string `json:"database_id"`
			Name string `json:"name"`
		} `json:"artifacts"`
	}{}
	decodeResponse(t, list, &listed)
	if len(listed.Artifacts) != 1 || listed.Artifacts[0].ID != finalized.ArtifactID || listed.Artifacts[0].Name != "linux-results" {
		t.Fatalf("listed artifacts = %#v", listed.Artifacts)
	}

	signed := postArtifact(t, publicURL, "GetSignedArtifactURL", downloaderToken, map[string]any{
		"workflow_run_backend_id": "run-uid", "workflow_job_run_backend_id": "upload-job-uid", "name": "linux-results",
	})
	download := struct {
		URL string `json:"signed_url"`
	}{}
	decodeResponse(t, signed, &download)
	if strings.Contains(download.URL, downloaderToken) {
		t.Fatal("signed download URL exposes the runtime token")
	}
	response = request(t, http.MethodGet, download.URL, "", nil, "")
	if response.StatusCode != http.StatusOK || readBody(t, response) != "artifact content" {
		t.Fatalf("download status = %d", response.StatusCode)
	}
	downloadURL, err := url.Parse(download.URL)
	if err != nil {
		t.Fatal(err)
	}
	downloadQuery := downloadURL.Query()
	downloadQuery.Set("comp", "block")
	downloadQuery.Set("blockid", "replacement")
	downloadURL.RawQuery = downloadQuery.Encode()
	response = request(t, http.MethodPut, downloadURL.String(), "", strings.NewReader("replacement"), "application/octet-stream")
	if response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("upload through download URL status = %d, body = %s", response.StatusCode, readBody(t, response))
	}
	response.Body.Close()

	deleted := postArtifact(t, publicURL, "DeleteArtifact", downloaderToken, map[string]any{
		"workflow_run_backend_id": "run-uid", "workflow_job_run_backend_id": "upload-job-uid", "name": "linux-results",
	})
	if deleted.StatusCode != http.StatusOK {
		t.Fatalf("DeleteArtifact status = %d, body = %s", deleted.StatusCode, readBody(t, deleted))
	}
	deleted.Body.Close()
	list = postArtifact(t, publicURL, "ListArtifacts", downloaderToken, map[string]any{
		"workflow_run_backend_id": "run-uid", "workflow_job_run_backend_id": "download-job-uid",
	})
	decodeResponse(t, list, &listed)
	if len(listed.Artifacts) != 0 {
		t.Fatalf("artifacts after delete = %#v", listed.Artifacts)
	}
	missing := postArtifact(t, publicURL, "GetSignedArtifactURL", downloaderToken, map[string]any{
		"workflow_run_backend_id": "run-uid", "workflow_job_run_backend_id": "upload-job-uid", "name": "linux-results",
	})
	if missing.StatusCode != http.StatusNotFound {
		t.Fatalf("missing artifact status = %d, body = %s", missing.StatusCode, readBody(t, missing))
	}
	missing.Body.Close()
	recreated := postArtifact(t, publicURL, "CreateArtifact", downloaderToken, map[string]any{
		"workflow_run_backend_id": "run-uid", "workflow_job_run_backend_id": "download-job-uid",
		"name": "linux-results", "version": 7, "mime_type": "application/octet-stream",
	})
	if recreated.StatusCode != http.StatusOK {
		t.Fatalf("CreateArtifact after delete status = %d, body = %s", recreated.StatusCode, readBody(t, recreated))
	}
	recreated.Body.Close()
}

func TestArtifactServerHealthAndReadiness(t *testing.T) {
	store, err := NewStore(t.TempDir(), testLimits())
	if err != nil {
		t.Fatal(err)
	}
	service, err := NewServer(ServerConfig{
		Address: "127.0.0.1:0", PublicURL: "http://artifacts.example", Store: store,
		Tokens: testTokenCodec(t), Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	health := httptest.NewRecorder()
	service.Handler().ServeHTTP(health, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if health.Code != http.StatusOK {
		t.Fatalf("health status = %d", health.Code)
	}
	readiness := httptest.NewRecorder()
	service.Handler().ServeHTTP(readiness, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if readiness.Code != http.StatusServiceUnavailable {
		t.Fatalf("initial readiness status = %d", readiness.Code)
	}
	service.ready.Store(true)
	readiness = httptest.NewRecorder()
	service.Handler().ServeHTTP(readiness, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if readiness.Code != http.StatusOK {
		t.Fatalf("ready status = %d", readiness.Code)
	}
}

func TestStoreRejectsUnsafeArchiveAndSurvivesRestart(t *testing.T) {
	now := time.Unix(1000, 0)
	root := t.TempDir()
	store, err := NewStore(root, testLimits())
	if err != nil {
		t.Fatal(err)
	}
	store.now = func() time.Time { return now }
	scope := Scope{ProjectUID: "project", RepositoryID: 1, RootRunUID: "root", RunUID: "run", Attempt: 1}
	record, err := store.Create(scope, "job", "unsafe", "application/zip", 7, nil)
	if err != nil {
		t.Fatal(err)
	}
	archive := &bytes.Buffer{}
	zipWriter := zip.NewWriter(archive)
	entry, err := zipWriter.Create("../outside")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := entry.Write([]byte("unsafe")); err != nil {
		t.Fatal(err)
	}
	if err := zipWriter.Close(); err != nil {
		t.Fatal(err)
	}
	if err := store.StageBlock(scope, "job", record.ID, "block", int64(archive.Len()), bytes.NewReader(archive.Bytes())); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Commit(scope, "job", record.ID, []string{"block"}); !errors.Is(err, ErrInvalidArtifact) {
		t.Fatalf("unsafe archive commit error = %v", err)
	}
	record, err = store.Create(scope, "job", "link", "application/zip", 7, nil)
	if err != nil {
		t.Fatal(err)
	}
	archive.Reset()
	zipWriter = zip.NewWriter(archive)
	header := &zip.FileHeader{Name: "link"}
	header.SetMode(os.ModeSymlink | 0o777)
	entry, err = zipWriter.CreateHeader(header)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := entry.Write([]byte("outside")); err != nil {
		t.Fatal(err)
	}
	if err := zipWriter.Close(); err != nil {
		t.Fatal(err)
	}
	if err := store.StageBlock(scope, "job", record.ID, "block", int64(archive.Len()), bytes.NewReader(archive.Bytes())); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Commit(scope, "job", record.ID, []string{"block"}); !errors.Is(err, ErrInvalidArtifact) {
		t.Fatalf("archive link commit error = %v", err)
	}

	record, err = store.Create(scope, "job", "raw", "application/octet-stream", 7, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.StageBlock(scope, "job", record.ID, "block", int64(len("restart-safe")), strings.NewReader("restart-safe")); err != nil {
		t.Fatal(err)
	}
	record, err = store.Commit(scope, "job", record.ID, []string{"block"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Finalize(scope, "job", "raw", record.Size, record.Digest); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(store.blockDirectory(record), 0o700); err != nil {
		t.Fatal(err)
	}
	staleUpload := filepath.Join(store.blockDirectory(record), "stale")
	if err := os.WriteFile(staleUpload, []byte("stale"), 0o600); err != nil {
		t.Fatal(err)
	}
	pending, err := store.Create(scope, "job", "pending", "application/octet-stream", 7, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(store.blobPath(pending)), 0o700); err != nil {
		t.Fatal(err)
	}
	staleBlob := store.blobPath(pending)
	if err := os.WriteFile(staleBlob, []byte("stale"), 0o600); err != nil {
		t.Fatal(err)
	}
	staleBlobTemporary := filepath.Join(store.scopeDirectory(scope), ".blob-stale")
	if err := os.WriteFile(staleBlobTemporary, []byte("stale"), 0o600); err != nil {
		t.Fatal(err)
	}
	staleRecordTemporary := filepath.Join(store.scopeDirectory(scope), "records", ".record-stale")
	if err := os.WriteFile(staleRecordTemporary, []byte("stale"), 0o600); err != nil {
		t.Fatal(err)
	}
	restarted, err := NewStore(root, testLimits())
	if err != nil {
		t.Fatal(err)
	}
	restarted.now = func() time.Time { return now }
	if err := restarted.Cleanup(); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{staleUpload, staleBlob, staleBlobTemporary, staleRecordTemporary} {
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("crash temporary %q error = %v", path, err)
		}
	}
	records, err := restarted.List(scope, "raw", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || records[0].Digest != record.Digest {
		t.Fatalf("records after restart = %#v", records)
	}
}

func TestStoreEnforcesFileArtifactRunAndRetentionLimits(t *testing.T) {
	now := time.Unix(1000, 0)
	limits := Limits{
		MaxFileBytes: 8, MaxArtifactBytes: 10, MaxRunBytes: 15, MaxArtifactsPerJob: 10,
		DefaultRetention: time.Hour, MaxRetention: 2 * time.Hour, IncompleteUploadRetention: time.Hour,
	}
	store, err := NewStore(t.TempDir(), limits)
	if err != nil {
		t.Fatal(err)
	}
	store.now = func() time.Time { return now }
	scope := func(run string) Scope {
		return Scope{ProjectUID: "project", RepositoryID: 1, RootRunUID: run, RunUID: run, Attempt: 1}
	}
	retentionScope := scope("retention-run")
	requestedExpiration := now.Add(3 * time.Hour)
	retained, err := store.Create(retentionScope, "job", "retained", "application/octet-stream", 7, &requestedExpiration)
	if err != nil {
		t.Fatal(err)
	}
	if !retained.ExpiresAt.Equal(now.Add(2 * time.Hour)) {
		t.Fatalf("capped retention = %s", retained.ExpiresAt)
	}
	fileScope := scope("file-run")
	record, err := store.Create(fileScope, "job", "file", "application/octet-stream", 7, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.StageBlock(fileScope, "job", record.ID, "block", int64(len("123456789")), strings.NewReader("123456789")); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Commit(fileScope, "job", record.ID, []string{"block"}); !errors.Is(err, ErrLimitExceeded) {
		t.Fatalf("file limit error = %v", err)
	}

	artifactScope := scope("artifact-run")
	record, err = store.Create(artifactScope, "job", "artifact", "application/octet-stream", 7, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.StageBlock(artifactScope, "job", record.ID, "block", int64(len("12345678901")), strings.NewReader("12345678901")); !errors.Is(err, ErrLimitExceeded) {
		t.Fatalf("artifact limit error = %v", err)
	}

	runScope := scope("quota-run")
	first, err := store.Create(runScope, "job", "first", "application/octet-stream", 7, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.StageBlock(runScope, "job", first.ID, "block", int64(len("12345678")), strings.NewReader("12345678")); err != nil {
		t.Fatal(err)
	}
	first, err = store.Commit(runScope, "job", first.ID, []string{"block"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Finalize(runScope, "job", first.Name, first.Size, first.Digest); err != nil {
		t.Fatal(err)
	}
	second, err := store.Create(runScope, "job", "second", "application/octet-stream", 7, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.StageBlock(runScope, "job", second.ID, "block", int64(len("12345678")), strings.NewReader("12345678")); !errors.Is(err, ErrLimitExceeded) {
		t.Fatalf("run limit error = %v", err)
	}

	incompleteScope := scope("incomplete-run")
	incomplete, err := store.Create(incompleteScope, "job", "incomplete", "application/octet-stream", 7, nil)
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Hour)
	if err := store.Cleanup(); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Get(incompleteScope, incomplete.ID); !errors.Is(err, ErrArtifactNotFound) {
		t.Fatalf("expired incomplete upload error = %v", err)
	}
}

func TestStoreReservesQuotaForConcurrentBlockUploads(t *testing.T) {
	limits := Limits{
		MaxFileBytes: 15, MaxArtifactBytes: 15, MaxRunBytes: 15, MaxArtifactsPerJob: 10,
		DefaultRetention: time.Hour, MaxRetention: 2 * time.Hour, IncompleteUploadRetention: time.Hour,
	}
	store, err := NewStore(t.TempDir(), limits)
	if err != nil {
		t.Fatal(err)
	}
	scope := Scope{ProjectUID: "project", RepositoryID: 1, RootRunUID: "root", RunUID: "run", Attempt: 1}
	first, err := store.Create(scope, "job", "first", "application/octet-stream", 7, nil)
	if err != nil {
		t.Fatal(err)
	}
	second, err := store.Create(scope, "job", "second", "application/octet-stream", 7, nil)
	if err != nil {
		t.Fatal(err)
	}
	reader := &blockingReader{started: make(chan struct{}), released: make(chan struct{}), reader: strings.NewReader("12345678")}
	result := make(chan error, 1)
	go func() {
		result <- store.StageBlock(scope, "job", first.ID, "block", 8, reader)
	}()
	<-reader.started
	if err := store.StageBlock(scope, "job", second.ID, "block", 8, strings.NewReader("12345678")); !errors.Is(err, ErrLimitExceeded) {
		t.Fatalf("concurrent run limit error = %v", err)
	}
	close(reader.released)
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	if err := store.StageBlock(scope, "job", first.ID, "block", 8, strings.NewReader("abcdefgh")); err != nil {
		t.Fatalf("idempotent block replacement error = %v", err)
	}
}

func TestStoreCommitsArtifactsConcurrently(t *testing.T) {
	store, err := NewStore(t.TempDir(), testLimits())
	if err != nil {
		t.Fatal(err)
	}
	scope := Scope{ProjectUID: "project", RepositoryID: 1, RootRunUID: "root", RunUID: "run", Attempt: 1}
	records := make([]Artifact, 0, 2)
	for _, name := range []string{"first", "second"} {
		record, err := store.Create(scope, "job-"+name, name, "application/octet-stream", 7, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := store.StageBlock(scope, record.JobBackendID, record.ID, "block", 7, strings.NewReader("content")); err != nil {
			t.Fatal(err)
		}
		records = append(records, record)
	}

	validateBlob := store.validateBlob
	validationStarted := make(chan struct{}, len(records))
	releaseValidation := make(chan struct{})
	store.validateBlob = func(path, mimeType string, limits Limits) error {
		validationStarted <- struct{}{}
		<-releaseValidation
		return validateBlob(path, mimeType, limits)
	}
	results := make(chan error, len(records))
	for _, record := range records {
		go func() {
			_, err := store.Commit(scope, record.JobBackendID, record.ID, []string{"block"})
			results <- err
		}()
	}

	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	for range records {
		select {
		case <-validationStarted:
		case <-timer.C:
			close(releaseValidation)
			for range records {
				<-results
			}
			t.Fatal("artifact commits did not validate concurrently")
		}
	}
	artifacts, err := store.List(scope, "", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(artifacts) != 0 {
		t.Fatalf("artifacts visible before finalization = %#v", artifacts)
	}
	close(releaseValidation)
	for range records {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
}

func TestStoreDoesNotServeExpiredArtifact(t *testing.T) {
	now := time.Unix(1000, 0)
	store, err := NewStore(t.TempDir(), testLimits())
	if err != nil {
		t.Fatal(err)
	}
	store.now = func() time.Time { return now }
	scope := Scope{ProjectUID: "project", RepositoryID: 1, RootRunUID: "root", RunUID: "run", Attempt: 1}
	expiresAt := now.Add(time.Minute)
	record, err := store.Create(scope, "job", "result", "application/octet-stream", 7, &expiresAt)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.StageBlock(scope, "job", record.ID, "block", 6, strings.NewReader("result")); err != nil {
		t.Fatal(err)
	}
	record, err = store.Commit(scope, "job", record.ID, []string{"block"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Finalize(scope, "job", record.Name, record.Size, record.Digest); err != nil {
		t.Fatal(err)
	}
	now = expiresAt
	if _, err := store.Get(scope, record.ID); !errors.Is(err, ErrArtifactNotFound) {
		t.Fatalf("expired artifact error = %v", err)
	}
}

type blockingReader struct {
	started  chan struct{}
	released chan struct{}
	reader   *strings.Reader
}

func (r *blockingReader) Read(data []byte) (int, error) {
	select {
	case <-r.started:
	default:
		close(r.started)
		<-r.released
	}
	return r.reader.Read(data)
}

func testRuntimeToken(t *testing.T, codec *TokenCodec, now time.Time, claims TokenClaims) string {
	t.Helper()
	token, err := codec.NewRuntimeToken(now, time.Hour, claims)
	if err != nil {
		t.Fatal(err)
	}
	return token
}

func testLimits() Limits {
	return Limits{
		MaxFileBytes: 1 << 20, MaxArtifactBytes: 2 << 20, MaxRunBytes: 10 << 20, MaxArtifactsPerJob: 10,
		DefaultRetention: 24 * time.Hour, MaxRetention: 7 * 24 * time.Hour, IncompleteUploadRetention: time.Hour,
	}
}

func postArtifact(t *testing.T, baseURL, method, token string, body any) *http.Response {
	t.Helper()
	data, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	return request(t, http.MethodPost, baseURL+artifactServicePath+method, token, bytes.NewReader(data), "application/json")
}

func request(t *testing.T, method, target, token string, body io.Reader, contentType string) *http.Response {
	t.Helper()
	request, err := http.NewRequestWithContext(context.Background(), method, target, body)
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	if contentType != "" {
		request.Header.Set("Content-Type", contentType)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	return response
}

func decodeResponse(t *testing.T, response *http.Response, target any) {
	t.Helper()
	defer response.Body.Close()
	if err := json.NewDecoder(response.Body).Decode(target); err != nil {
		t.Fatal(err)
	}
}

func readBody(t *testing.T, response *http.Response) string {
	t.Helper()
	defer response.Body.Close()
	data, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}
