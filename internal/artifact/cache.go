package artifact

import (
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/kelos-dev/open-actions/internal/cache"
)

const cacheServicePath = "/twirp/github.actions.results.api.v1.CacheService/"

type cacheRequest struct {
	Key         string          `json:"key"`
	Version     string          `json:"version"`
	RestoreKeys []string        `json:"restore_keys"`
	Size        json.RawMessage `json:"size_bytes"`
}

func (s *Server) cacheRPC(w http.ResponseWriter, r *http.Request) {
	claims, ok := s.authenticateTwirp(w, r)
	if !ok {
		return
	}
	if claims.Cache == nil {
		writeTwirpError(w, http.StatusForbidden, "permission_denied", "cache access is not granted")
		return
	}
	var input cacheRequest
	if !decodeTwirp(w, r, &input) {
		return
	}
	if err := cache.ValidateKey(input.Key, input.Version); err != nil {
		s.cacheError(w, err)
		return
	}
	tenant := cache.Tenant{ProjectUID: claims.ProjectUID, RepositoryID: claims.RepositoryID}
	switch strings.TrimPrefix(r.URL.Path, cacheServicePath) {
	case "CreateCacheEntry":
		if !claims.Cache.Writable() {
			writeTwirpError(w, http.StatusForbidden, "permission_denied", "cache write denied: job has no cache write access")
			return
		}
		entry, err := s.cacheStore.Create(tenant, claims.Cache.WriteRef, claims.WorkflowJobBackendID, input.Key, input.Version)
		if errors.Is(err, cache.ErrExists) {
			writeJSON(w, http.StatusOK, map[string]any{"ok": false, "message": "cache entry already exists"})
			return
		}
		if err != nil {
			s.cacheError(w, err)
			return
		}
		signed, err := s.cacheBlobURL(entry, claims, "write")
		if err != nil {
			s.cacheError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "signed_upload_url": signed})
	case "FinalizeCacheEntryUpload":
		if !claims.Cache.Writable() {
			writeTwirpError(w, http.StatusForbidden, "permission_denied", "cache write denied: job has no cache write access")
			return
		}
		var sizeValue any
		decoder := json.NewDecoder(strings.NewReader(string(input.Size)))
		decoder.UseNumber()
		err := decoder.Decode(&sizeValue)
		sizeText := ""
		switch value := sizeValue.(type) {
		case string:
			sizeText = value
		case json.Number:
			sizeText = string(value)
		}
		size, parseErr := strconv.ParseInt(sizeText, 10, 64)
		if err == nil {
			err = parseErr
		}
		if err != nil || size < 0 {
			s.cacheError(w, cache.ErrInvalid)
			return
		}
		entry, err := s.cacheStore.Finalize(tenant, claims.Cache.WriteRef, claims.WorkflowJobBackendID, input.Key, input.Version, size)
		if err != nil {
			s.cacheError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "entry_id": strconv.FormatInt(entry.ID, 10)})
	case "GetCacheEntryDownloadURL":
		if !claims.Cache.Readable() {
			writeTwirpError(w, http.StatusForbidden, "permission_denied", "cache read denied: job has no cache read access")
			return
		}
		entry, err := s.cacheStore.Lookup(tenant, claims.Cache.ReadRefs, input.Key, input.Version, input.RestoreKeys)
		if errors.Is(err, cache.ErrNotFound) {
			writeJSON(w, http.StatusOK, map[string]any{"ok": false})
			return
		}
		if err != nil {
			s.cacheError(w, err)
			return
		}
		signed, err := s.cacheBlobURL(entry, claims, "read")
		if err != nil {
			s.cacheError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "signed_download_url": signed, "matched_key": entry.Key})
	default:
		writeTwirpError(w, http.StatusNotFound, "bad_route", "cache operation is not supported")
	}
}

type cacheBlobClaims struct {
	Tenant    cache.Tenant `json:"tenant"`
	ID        int64        `json:"id"`
	Operation string       `json:"operation"`
	ExpiresAt int64        `json:"exp"`
}

func (s *Server) cacheBlobURL(entry cache.Entry, claims TokenClaims, operation string) (string, error) {
	data, err := json.Marshal(cacheBlobClaims{Tenant: entry.Tenant, ID: entry.ID, Operation: operation, ExpiresAt: claims.ExpiresAt})
	if err != nil {
		return "", err
	}
	encoded := base64.RawURLEncoding.EncodeToString(data)
	signature := base64.RawURLEncoding.EncodeToString(s.tokens.sign("cache-blob." + encoded))
	return s.publicURL + "/cache/blobs/" + strconv.FormatInt(entry.ID, 10) + "/cache?" + url.Values{"claims": {encoded}, "sig": {signature}}.Encode(), nil
}

func (s *Server) cacheBlob(w http.ResponseWriter, r *http.Request) {
	encoded := r.URL.Query().Get("claims")
	signature, err := base64.RawURLEncoding.DecodeString(r.URL.Query().Get("sig"))
	if err != nil || subtle.ConstantTimeCompare(signature, s.tokens.sign("cache-blob."+encoded)) != 1 {
		writeBlobError(w, http.StatusUnauthorized, "AuthenticationFailed", "cache credential is invalid")
		return
	}
	data, err := base64.RawURLEncoding.DecodeString(encoded)
	var claims cacheBlobClaims
	if err != nil || json.Unmarshal(data, &claims) != nil || claims.ExpiresAt <= s.now().Unix() || r.URL.Path != "/cache/blobs/"+strconv.FormatInt(claims.ID, 10)+"/cache" {
		writeBlobError(w, http.StatusUnauthorized, "AuthenticationFailed", "cache credential is invalid")
		return
	}
	write := r.Method == http.MethodPut
	read := r.Method == http.MethodGet || r.Method == http.MethodHead
	if write && claims.Operation != "write" || read && claims.Operation != "read" {
		writeBlobError(w, http.StatusUnauthorized, "AuthenticationFailed", "cache credential does not allow this operation")
		return
	}
	if !read && !write {
		w.Header().Set("Allow", "GET, HEAD, PUT")
		writeBlobError(w, http.StatusMethodNotAllowed, "UnsupportedHttpVerb", "cache operation is not supported")
		return
	}
	component := r.URL.Query().Get("comp")
	if read {
		if component != "" {
			writeBlobError(w, http.StatusBadRequest, "InvalidQueryParameterValue", "cache operation is not supported")
			return
		}
		file, entry, err := s.cacheStore.Open(claims.Tenant, claims.ID)
		if err != nil {
			s.cacheBlobError(w, err)
			return
		}
		defer file.Close()
		w.Header().Set("Content-Type", "application/octet-stream")
		cacheAzureHeaders(w, entry.ID, entry.CreatedAt)
		http.ServeContent(w, r, "cache", entry.CreatedAt, file)
		return
	}
	switch component {
	case "", "block":
		if r.ContentLength < 0 {
			writeBlobError(w, http.StatusLengthRequired, "MissingContentLength", "cache content length is required")
			return
		}
		block := ""
		if component == "block" {
			block = r.URL.Query().Get("blockid")
			if block == "" {
				s.cacheBlobError(w, cache.ErrInvalid)
				return
			}
		}
		err = s.cacheStore.Stage(claims.Tenant, claims.ID, block, r.ContentLength, r.Body)
	case "blocklist":
		var list struct {
			Blocks []string `xml:"Latest"`
		}
		decoder := xml.NewDecoder(http.MaxBytesReader(w, r.Body, 8<<20))
		if err = decoder.Decode(&list); err == nil {
			if trailing := decoder.Decode(&struct{}{}); trailing != io.EOF {
				err = cache.ErrInvalid
			} else {
				err = s.cacheStore.Commit(claims.Tenant, claims.ID, list.Blocks)
			}
		} else {
			err = cache.ErrInvalid
		}
	default:
		err = cache.ErrInvalid
	}
	if err != nil {
		s.cacheBlobError(w, err)
		return
	}
	cacheAzureHeaders(w, claims.ID, s.now())
	w.WriteHeader(http.StatusCreated)
}

func cacheAzureHeaders(w http.ResponseWriter, id int64, modified time.Time) {
	w.Header().Set("ETag", fmt.Sprintf("\"%d\"", id))
	w.Header().Set("Last-Modified", modified.UTC().Format(http.TimeFormat))
	w.Header().Set("x-ms-request-id", strconv.FormatInt(id, 10))
	w.Header().Set("x-ms-version", "2023-11-03")
	w.Header().Set("x-ms-blob-type", "BlockBlob")
}

func (s *Server) cacheError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, cache.ErrInvalid):
		writeTwirpError(w, http.StatusBadRequest, "invalid_argument", err.Error())
	case errors.Is(err, cache.ErrExists):
		writeTwirpError(w, http.StatusConflict, "already_exists", err.Error())
	case errors.Is(err, cache.ErrNotFound):
		writeTwirpError(w, http.StatusNotFound, "not_found", err.Error())
	case errors.Is(err, cache.ErrLimit):
		writeTwirpError(w, http.StatusRequestEntityTooLarge, "resource_exhausted", err.Error())
	default:
		s.logger.Error("cache storage operation failed", "error", err)
		writeTwirpError(w, http.StatusInternalServerError, "internal", "cache storage operation failed")
	}
}

func (s *Server) cacheBlobError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, cache.ErrInvalid):
		writeBlobError(w, http.StatusBadRequest, "InvalidBlobOrBlock", err.Error())
	case errors.Is(err, cache.ErrExists):
		writeBlobError(w, http.StatusConflict, "BlobAlreadyExists", err.Error())
	case errors.Is(err, cache.ErrNotFound):
		writeBlobError(w, http.StatusNotFound, "BlobNotFound", err.Error())
	case errors.Is(err, cache.ErrLimit):
		writeBlobError(w, http.StatusRequestEntityTooLarge, "ContentLengthExceedsLimit", err.Error())
	default:
		s.logger.Error("cache blob operation failed", "error", err)
		writeBlobError(w, http.StatusInternalServerError, "InternalError", "cache storage operation failed")
	}
}
