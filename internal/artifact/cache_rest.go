package artifact

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"regexp"
	"strconv"
	"strings"

	"github.com/kelos-dev/open-actions/internal/cache"
)

const cacheRESTPath = "/_apis/artifactcache/"

var cacheContentRange = regexp.MustCompile(`^bytes ([0-9]+)-([0-9]+)/(\*|[0-9]+)$`)

func (s *Server) cacheREST(w http.ResponseWriter, r *http.Request) {
	scheme, token, ok := strings.Cut(r.Header.Get("Authorization"), " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") || token == "" {
		writeCacheRESTError(w, http.StatusUnauthorized, "cache credential is required")
		return
	}
	claims, err := s.tokens.ParseRuntimeToken(token, s.now())
	if err != nil {
		writeCacheRESTError(w, http.StatusUnauthorized, "cache credential is invalid")
		return
	}
	if claims.Cache == nil {
		writeCacheRESTError(w, http.StatusForbidden, "cache access is not granted")
		return
	}
	path := strings.TrimPrefix(r.URL.Path, cacheRESTPath)
	switch {
	case path == "cache" && r.Method == http.MethodGet:
		s.lookupCacheREST(w, r, claims)
	case path == "caches" && r.Method == http.MethodGet:
		s.listCachesREST(w, r, claims)
	case path == "caches" && r.Method == http.MethodPost:
		s.reserveCacheREST(w, r, claims)
	case strings.HasPrefix(path, "caches/") && (r.Method == http.MethodPatch || r.Method == http.MethodPost):
		s.uploadCacheREST(w, r, claims, strings.TrimPrefix(path, "caches/"))
	default:
		writeCacheRESTError(w, http.StatusNotImplemented, "cache operation is not supported (https://github.com/kelos-dev/open-actions/issues/41)")
	}
}

func (s *Server) lookupCacheREST(w http.ResponseWriter, r *http.Request, claims TokenClaims) {
	if !claims.Cache.Readable() {
		writeCacheRESTError(w, http.StatusForbidden, "cache read denied: job has no cache read access")
		return
	}
	keys := strings.Split(r.URL.Query().Get("keys"), ",")
	entry, err := s.cacheStore.Lookup(cache.Tenant{ProjectUID: claims.ProjectUID, RepositoryID: claims.RepositoryID}, claims.Cache.ReadRefs, keys[0], r.URL.Query().Get("version"), keys[1:])
	if errors.Is(err, cache.ErrNotFound) {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if err != nil {
		s.cacheRESTError(w, err)
		return
	}
	signed, err := s.cacheBlobURL(entry, claims, "read")
	if err != nil {
		s.cacheRESTError(w, err)
		return
	}
	result := cacheRESTEntry(entry)
	result["archiveLocation"] = signed
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) listCachesREST(w http.ResponseWriter, r *http.Request, claims TokenClaims) {
	if !claims.Cache.Readable() {
		writeCacheRESTError(w, http.StatusForbidden, "cache read denied: job has no cache read access")
		return
	}
	entries, err := s.cacheStore.List(cache.Tenant{ProjectUID: claims.ProjectUID, RepositoryID: claims.RepositoryID}, claims.Cache.ReadRefs, r.URL.Query().Get("key"))
	if err != nil {
		s.cacheRESTError(w, err)
		return
	}
	results := make([]map[string]any, 0, len(entries))
	for _, entry := range entries {
		results = append(results, cacheRESTEntry(entry))
	}
	writeJSON(w, http.StatusOK, map[string]any{"totalCount": len(results), "artifactCaches": results})
}

func cacheRESTEntry(entry cache.Entry) map[string]any {
	return map[string]any{
		"cacheKey": entry.Key, "cacheVersion": entry.Version, "scope": entry.Ref, "creationTime": entry.CreatedAt,
	}
}

func (s *Server) reserveCacheREST(w http.ResponseWriter, r *http.Request, claims TokenClaims) {
	if !claims.Cache.Writable() {
		writeCacheRESTError(w, http.StatusForbidden, "cache write denied: job has no cache write access")
		return
	}
	var input struct {
		Key       string `json:"key"`
		Version   string `json:"version"`
		CacheSize *int64 `json:"cacheSize"`
	}
	if !decodeCacheREST(w, r, &input) {
		return
	}
	if input.CacheSize != nil && *input.CacheSize < 0 {
		s.cacheRESTError(w, cache.ErrInvalid)
		return
	}
	if input.CacheSize != nil && *input.CacheSize > s.cacheStore.MaxEntryBytes() {
		writeCacheRESTError(w, http.StatusBadRequest, fmt.Sprintf("cache size %d exceeds the %d-byte entry limit", *input.CacheSize, s.cacheStore.MaxEntryBytes()))
		return
	}
	entry, err := s.cacheStore.Create(cache.Tenant{ProjectUID: claims.ProjectUID, RepositoryID: claims.RepositoryID}, claims.Cache.WriteRef, claims.WorkflowJobBackendID, input.Key, input.Version)
	if err != nil {
		s.cacheRESTError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"cacheId": entry.ID})
}

func (s *Server) uploadCacheREST(w http.ResponseWriter, r *http.Request, claims TokenClaims, idText string) {
	if !claims.Cache.Writable() {
		writeCacheRESTError(w, http.StatusForbidden, "cache write denied: job has no cache write access")
		return
	}
	id, err := strconv.ParseInt(idText, 10, 64)
	if err != nil || id <= 0 {
		s.cacheRESTError(w, cache.ErrInvalid)
		return
	}
	tenant := cache.Tenant{ProjectUID: claims.ProjectUID, RepositoryID: claims.RepositoryID}
	entry, err := s.cacheStore.Get(tenant, id)
	if err != nil {
		s.cacheRESTError(w, err)
		return
	}
	if entry.Owner != claims.WorkflowJobBackendID || entry.Ref != claims.Cache.WriteRef {
		writeCacheRESTError(w, http.StatusForbidden, "cache upload is outside this job scope")
		return
	}
	if r.Method == http.MethodPatch {
		parts := cacheContentRange.FindStringSubmatch(r.Header.Get("Content-Range"))
		if parts == nil {
			s.cacheRESTError(w, cache.ErrInvalid)
			return
		}
		start, startErr := strconv.ParseInt(parts[1], 10, 64)
		end, endErr := strconv.ParseInt(parts[2], 10, 64)
		if startErr != nil || endErr != nil || end < start || end == math.MaxInt64 {
			s.cacheRESTError(w, cache.ErrInvalid)
			return
		}
		if parts[3] != "*" {
			total, err := strconv.ParseInt(parts[3], 10, 64)
			if err != nil || total <= end {
				s.cacheRESTError(w, cache.ErrInvalid)
				return
			}
		}
		size := end - start + 1
		if r.ContentLength >= 0 && r.ContentLength != size {
			s.cacheRESTError(w, cache.ErrInvalid)
			return
		}
		err = s.cacheStore.StageRange(tenant, id, start, size, r.Body)
	} else {
		var input struct {
			Size *int64 `json:"size"`
		}
		if !decodeCacheREST(w, r, &input) {
			return
		}
		if input.Size == nil {
			s.cacheRESTError(w, cache.ErrInvalid)
			return
		}
		err = s.cacheStore.CommitRanges(tenant, id, *input.Size)
		if err == nil {
			_, err = s.cacheStore.Finalize(tenant, entry.Ref, entry.Owner, entry.Key, entry.Version, *input.Size)
		}
	}
	if err != nil {
		s.cacheRESTError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func decodeCacheREST(w http.ResponseWriter, r *http.Request, target any) bool {
	if strings.Split(r.Header.Get("Content-Type"), ";")[0] != "application/json" {
		writeCacheRESTError(w, http.StatusBadRequest, "content type must be application/json")
		return false
	}
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10))
	if err := decoder.Decode(target); err != nil {
		writeCacheRESTError(w, http.StatusBadRequest, "request body is invalid")
		return false
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		writeCacheRESTError(w, http.StatusBadRequest, "request body contains trailing data")
		return false
	}
	return true
}

func (s *Server) cacheRESTError(w http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	switch {
	case errors.Is(err, cache.ErrInvalid):
		status = http.StatusBadRequest
	case errors.Is(err, cache.ErrExists):
		status = http.StatusConflict
	case errors.Is(err, cache.ErrNotFound):
		status = http.StatusNotFound
	case errors.Is(err, cache.ErrLimit):
		status = http.StatusRequestEntityTooLarge
	default:
		s.logger.Error("cache storage operation failed", "error", err)
		err = errors.New("cache storage operation failed")
	}
	writeCacheRESTError(w, status, err.Error())
}

func writeCacheRESTError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"message": message})
}
