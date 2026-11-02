package artifact

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/kelos-dev/open-actions/internal/cache"
)

func postCacheREST(t *testing.T, server, path, token string, input any) *http.Response {
	t.Helper()
	data, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	return request(t, http.MethodPost, server+cacheRESTPath+path, token, strings.NewReader(string(data)), "application/json")
}

func patchCacheREST(t *testing.T, target, token, contentRange string, body io.Reader) *http.Response {
	t.Helper()
	r, err := http.NewRequest(http.MethodPatch, target, body)
	if err != nil {
		t.Fatal(err)
	}
	r.Header.Set("Authorization", "Bearer "+token)
	r.Header.Set("Content-Type", "application/octet-stream")
	r.Header.Set("Content-Range", contentRange)
	response, err := http.DefaultClient.Do(r)
	if err != nil {
		t.Fatal(err)
	}
	return response
}

func requireCacheStatus(t *testing.T, response *http.Response, status int) {
	t.Helper()
	if response.StatusCode != status {
		t.Fatalf("status=%d want=%d body=%s", response.StatusCode, status, readBody(t, response))
	}
	response.Body.Close()
}

func reserveCacheREST(t *testing.T, server, token, key string) int64 {
	t.Helper()
	response := postCacheREST(t, server, "caches", token, map[string]any{"key": key, "version": "v", "cacheSize": 10})
	if response.StatusCode != http.StatusCreated {
		t.Fatalf("reserve status=%d body=%s", response.StatusCode, readBody(t, response))
	}
	var result struct {
		CacheID int64 `json:"cacheId"`
	}
	decodeResponse(t, response, &result)
	if result.CacheID <= 0 {
		t.Fatalf("invalid cache ID %d", result.CacheID)
	}
	return result.CacheID
}

func TestCacheRESTUploadsAndInteroperatesWithResultsService(t *testing.T) {
	server, codec, store := cacheTestServer(t)
	owner := cacheToken(t, codec, "project", "owner", "refs/heads/main", "write")
	reader := cacheToken(t, codec, "project", "reader", "refs/heads/feature", "read")
	lookup := server.URL + cacheRESTPath + "cache?keys=missing,npm-&version=v"
	requireCacheStatus(t, request(t, http.MethodGet, lookup, reader, nil, ""), http.StatusNoContent)
	id := reserveCacheREST(t, server.URL, owner, "npm-lock")
	upload := fmt.Sprintf("%scaches/%d", cacheRESTPath, id)
	for _, chunk := range []struct{ contentRange, data string }{
		{"bytes 5-9/*", " data"}, {"bytes 0-4/10", "cache"}, {"bytes 0-4/*", "cache"},
	} {
		// Streaming bodies without a Content-Length match the toolkit's PATCH requests.
		requireCacheStatus(t, patchCacheREST(t, server.URL+upload, owner, chunk.contentRange, io.NopCloser(strings.NewReader(chunk.data))), http.StatusNoContent)
	}
	requireCacheStatus(t, request(t, http.MethodGet, lookup, reader, nil, ""), http.StatusNoContent)
	for range 2 {
		requireCacheStatus(t, postCacheREST(t, server.URL, strings.TrimPrefix(upload, cacheRESTPath), owner, map[string]any{"size": 10}), http.StatusNoContent)
	}
	response := request(t, http.MethodGet, lookup, reader, nil, "")
	var result struct{ CacheKey, CacheVersion, Scope, ArchiveLocation string }
	decodeResponse(t, response, &result)
	if result.CacheKey != "npm-lock" || result.CacheVersion != "v" || result.Scope != "refs/heads/main" || strings.Contains(result.ArchiveLocation, owner) {
		t.Fatalf("lookup=%#v", result)
	}
	download := request(t, http.MethodGet, result.ArchiveLocation, "", nil, "")
	if download.StatusCode != http.StatusOK || readBody(t, download) != "cache data" {
		t.Fatal("cache contents did not match")
	}
	response = postCache(t, server.URL, "GetCacheEntryDownloadURL", reader, map[string]any{"key": "npm-lock", "version": "v"})
	var hit struct {
		OK  bool   `json:"ok"`
		URL string `json:"signed_download_url"`
	}
	decodeResponse(t, response, &hit)
	if !hit.OK || hit.URL == "" {
		t.Fatalf("v2 lookup=%#v", hit)
	}
	requireCacheStatus(t, patchCacheREST(t, server.URL+upload, owner, "bytes 0-3/*", strings.NewReader("evil")), http.StatusConflict)
	requireCacheStatus(t, postCacheREST(t, server.URL, "caches", owner, map[string]any{"key": "npm-lock", "version": "v"}), http.StatusConflict)
	response = postCache(t, server.URL, "CreateCacheEntry", owner, map[string]any{"key": "npm-lock", "version": "v"})
	var duplicate struct {
		OK bool `json:"ok"`
	}
	decodeResponse(t, response, &duplicate)
	if duplicate.OK {
		t.Fatal("v2 reserved a duplicate of a v1 cache")
	}

	created := postCache(t, server.URL, "CreateCacheEntry", owner, map[string]any{"key": "node-lock", "version": "v"})
	var v2 struct {
		URL string `json:"signed_upload_url"`
	}
	decodeResponse(t, created, &v2)
	requireCacheStatus(t, request(t, http.MethodPut, v2.URL, "", strings.NewReader("node"), "application/octet-stream"), http.StatusCreated)
	finalized := postCache(t, server.URL, "FinalizeCacheEntryUpload", owner, map[string]any{"key": "node-lock", "version": "v", "size_bytes": "4"})
	requireCacheStatus(t, finalized, http.StatusOK)
	response = request(t, http.MethodGet, server.URL+cacheRESTPath+"cache?keys=node-lock&version=v", reader, nil, "")
	decodeResponse(t, response, &result)
	if result.CacheKey != "node-lock" {
		t.Fatalf("v1 lookup of v2 cache=%#v", result)
	}
	entry, err := store.Get(cache.Tenant{ProjectUID: "project", RepositoryID: testClaims("owner").RepositoryID}, id)
	if err != nil || !entry.Finalized || entry.Size != 10 {
		t.Fatalf("entry=%#v error=%v", entry, err)
	}
}

func TestCacheRESTEnforcesCredentialScopes(t *testing.T) {
	server, codec, _ := cacheTestServer(t)
	owner := cacheToken(t, codec, "project", "owner", "refs/heads/main", "write")
	id := reserveCacheREST(t, server.URL, owner, "key")
	path := fmt.Sprintf("caches/%d", id)
	for _, tc := range []struct {
		name, project, job, ref, mode string
		status                        int
	}{
		{"other job", "project", "other", "refs/heads/main", "write", 403},
		{"other ref", "project", "owner", "refs/heads/feature", "write", 403},
		{"other project", "other", "owner", "refs/heads/main", "write", 404},
		{"read only", "project", "owner", "refs/heads/main", "read", 403},
		{"none", "project", "owner", "refs/heads/main", "none", 403},
	} {
		t.Run(tc.name, func(t *testing.T) {
			token := cacheToken(t, codec, tc.project, tc.job, tc.ref, tc.mode)
			requireCacheStatus(t, patchCacheREST(t, server.URL+cacheRESTPath+path, token, "bytes 0-3/*", strings.NewReader("evil")), tc.status)
			requireCacheStatus(t, postCacheREST(t, server.URL, path, token, map[string]any{"size": 4}), tc.status)
		})
	}
	claims := testClaims("owner")
	claims.RepositoryID++
	access := cache.ResolveAccess("push", "refs/heads/main", "", "main", "write")
	claims.Cache = &access
	claims.ProjectUID = "project"
	otherRepository := testRuntimeToken(t, codec, time.Now(), claims)
	requireCacheStatus(t, patchCacheREST(t, server.URL+cacheRESTPath+path, otherRepository, "bytes 0-3/*", strings.NewReader("evil")), http.StatusNotFound)
	for _, token := range []string{"", "invalid", testRuntimeToken(t, codec, time.Now().Add(-2*time.Hour), testClaims("expired"))} {
		requireCacheStatus(t, request(t, http.MethodGet, server.URL+cacheRESTPath+"cache?keys=key&version=v", token, nil, ""), http.StatusUnauthorized)
	}
	artifactOnly := testRuntimeToken(t, codec, time.Now(), testClaims("artifact-only"))
	requireCacheStatus(t, postCacheREST(t, server.URL, "caches", artifactOnly, map[string]any{"key": "other", "version": "v"}), http.StatusForbidden)
	for _, mode := range []string{"read", "none"} {
		token := cacheToken(t, codec, "project", "owner", "refs/heads/main", mode)
		requireCacheStatus(t, postCacheREST(t, server.URL, "caches", token, map[string]any{"key": "other", "version": "v"}), http.StatusForbidden)
	}
	for _, mode := range []string{"none", "write-only"} {
		token := cacheToken(t, codec, "project", "owner", "refs/heads/main", mode)
		requireCacheStatus(t, request(t, http.MethodGet, server.URL+cacheRESTPath+"cache?keys=key&version=v", token, nil, ""), http.StatusForbidden)
		requireCacheStatus(t, request(t, http.MethodGet, server.URL+cacheRESTPath+"caches?key=key", token, nil, ""), http.StatusForbidden)
	}
}

func TestCacheRESTLookupRespectsRefAndVersion(t *testing.T) {
	server, codec, store := cacheTestServer(t)
	tenant := cache.Tenant{ProjectUID: "project", RepositoryID: testClaims("job").RepositoryID}
	for _, ref := range []string{"refs/heads/main", "refs/heads/feature", "refs/heads/sibling", "refs/pull/1/merge"} {
		entry, err := store.Create(tenant, ref, "job", "npm-"+strings.TrimPrefix(ref, "refs/"), "v")
		if err != nil {
			t.Fatal(err)
		}
		if err := store.Stage(tenant, entry.ID, "", 4, strings.NewReader("data")); err != nil {
			t.Fatal(err)
		}
		if _, err := store.Finalize(tenant, ref, "job", entry.Key, "v", 4); err != nil {
			t.Fatal(err)
		}
	}
	reader := cacheToken(t, codec, "project", "reader", "refs/heads/feature", "read")
	for _, tc := range []struct{ keys, version, want string }{
		{"missing,npm-heads/main,npm-", "v", "npm-heads/feature"},
		{"npm-heads/main", "v", "npm-heads/main"},
		{"npm-heads/sibling", "v", ""},
		{"npm-pull/1/merge", "v", ""},
		{"npm-", "other", ""},
	} {
		t.Run(tc.keys+"/"+tc.version, func(t *testing.T) {
			response := request(t, http.MethodGet, server.URL+cacheRESTPath+"cache?"+url.Values{"keys": {tc.keys}, "version": {tc.version}}.Encode(), reader, nil, "")
			if tc.want == "" {
				requireCacheStatus(t, response, http.StatusNoContent)
				return
			}
			var result struct{ CacheKey string }
			decodeResponse(t, response, &result)
			if result.CacheKey != tc.want {
				t.Fatalf("key=%q want=%q", result.CacheKey, tc.want)
			}
		})
	}
	response := request(t, http.MethodGet, server.URL+cacheRESTPath+"caches?key=npm-", reader, nil, "")
	var listed struct {
		TotalCount     int
		ArtifactCaches []struct{ CacheKey, Scope string }
	}
	decodeResponse(t, response, &listed)
	if listed.TotalCount != 2 || len(listed.ArtifactCaches) != 2 {
		t.Fatalf("list=%#v", listed)
	}
	for _, entry := range listed.ArtifactCaches {
		if entry.Scope != "refs/heads/main" && entry.Scope != "refs/heads/feature" {
			t.Fatalf("list exposed an inaccessible ref: %#v", entry)
		}
	}
	other := cacheToken(t, codec, "other", "reader", "refs/heads/main", "read")
	response = request(t, http.MethodGet, server.URL+cacheRESTPath+"caches?key=npm-", other, nil, "")
	if response.StatusCode != http.StatusOK {
		t.Fatalf("list status=%d body=%s", response.StatusCode, readBody(t, response))
	}
	decodeResponse(t, response, &listed)
	if listed.TotalCount != 0 || len(listed.ArtifactCaches) != 0 {
		t.Fatalf("list exposed another Project's entries: %#v", listed)
	}
}

func TestCacheRESTRejectsInvalidUploads(t *testing.T) {
	server, codec, _ := cacheTestServer(t)
	owner := cacheToken(t, codec, "project", "owner", "refs/heads/main", "write")
	id := reserveCacheREST(t, server.URL, owner, "key")
	target := fmt.Sprintf("%s%scaches/%d", server.URL, cacheRESTPath, id)
	for _, contentRange := range []string{"", "bytes */4", "bytes -1-2/*", "bytes 3-2/*", "bytes 0-3/3", "bytes 0-9223372036854775807/*", "bytes 0-99999999999999999999/*", "bytes 0-4/*"} {
		t.Run(contentRange, func(t *testing.T) {
			requireCacheStatus(t, patchCacheREST(t, target, owner, contentRange, strings.NewReader("data")), http.StatusBadRequest)
		})
	}
	for _, body := range []string{"abc", "abcde"} {
		requireCacheStatus(t, patchCacheREST(t, target, owner, "bytes 0-3/*", io.NopCloser(strings.NewReader(body))), http.StatusBadRequest)
	}
	requireCacheStatus(t, patchCacheREST(t, target, owner, "bytes 1-4/*", strings.NewReader("data")), http.StatusNoContent)
	for _, input := range []any{map[string]any{}, map[string]any{"size": -1}, map[string]any{"size": 5}, map[string]any{"size": "4"}} {
		requireCacheStatus(t, postCacheREST(t, server.URL, fmt.Sprintf("caches/%d", id), owner, input), http.StatusBadRequest)
	}
	requireCacheStatus(t, request(t, http.MethodGet, server.URL+cacheRESTPath+"cache?keys=key&version=v", owner, nil, ""), http.StatusNoContent)
	for _, input := range []any{map[string]any{"key": "", "version": "v"}, map[string]any{"key": "a,b", "version": "v"}, map[string]any{"key": "key"}, map[string]any{"key": "size", "version": "v", "cacheSize": -1}} {
		requireCacheStatus(t, postCacheREST(t, server.URL, "caches", owner, input), http.StatusBadRequest)
	}
	for _, query := range []string{"keys=&version=v", "keys=key", "keys=key,&version=v"} {
		requireCacheStatus(t, request(t, http.MethodGet, server.URL+cacheRESTPath+"cache?"+query, owner, nil, ""), http.StatusBadRequest)
	}
}

func TestCacheRESTReservationValidatesSize(t *testing.T) {
	server, codec, store := cacheTestServer(t)
	owner := cacheToken(t, codec, "project", "owner", "refs/heads/main", "write")
	input := map[string]any{"key": "oversized", "version": "v", "cacheSize": store.MaxEntryBytes() + 1}
	response := postCacheREST(t, server.URL, "caches", owner, input)
	if response.StatusCode != http.StatusBadRequest {
		t.Fatalf("reserve status=%d body=%s", response.StatusCode, readBody(t, response))
	}
	var result struct {
		Message string `json:"message"`
	}
	decodeResponse(t, response, &result)
	wantMessage := fmt.Sprintf("cache size %d exceeds the %d-byte entry limit", store.MaxEntryBytes()+1, store.MaxEntryBytes())
	if result.Message != wantMessage {
		t.Fatalf("message=%q want=%q", result.Message, wantMessage)
	}
	input["cacheSize"] = store.MaxEntryBytes()
	requireCacheStatus(t, postCacheREST(t, server.URL, "caches", owner, input), http.StatusCreated)
	input["key"] = "empty"
	input["cacheSize"] = 0
	requireCacheStatus(t, postCacheREST(t, server.URL, "caches", owner, input), http.StatusCreated)
	input["key"] = "unknown-size"
	delete(input, "cacheSize")
	requireCacheStatus(t, postCacheREST(t, server.URL, "caches", owner, input), http.StatusCreated)
}

func TestCacheRESTErrorMessages(t *testing.T) {
	server, codec, _ := cacheTestServer(t)
	for _, tc := range []struct {
		name, method, path, mode, body, contentType, message string
		status                                               int
	}{
		{"write denied", http.MethodPost, "caches", "read", `{"key":"key","version":"v"}`, "application/json", "cache write denied:", http.StatusForbidden},
		{"read denied", http.MethodGet, "cache?keys=key&version=v", "write-only", "", "", "cache read denied:", http.StatusForbidden},
		{"invalid entry", http.MethodPost, "caches", "write", `{"key":"","version":"v"}`, "application/json", "invalid cache entry", http.StatusBadRequest},
		{"invalid JSON", http.MethodPost, "caches", "write", `{`, "application/json", "request body is invalid", http.StatusBadRequest},
		{"trailing JSON", http.MethodPost, "caches", "write", `{} {}`, "application/json", "request body contains trailing data", http.StatusBadRequest},
		{"invalid content type", http.MethodPost, "caches", "write", `{}`, "text/plain", "content type must be application/json", http.StatusBadRequest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			token := cacheToken(t, codec, "project", "owner", "refs/heads/main", tc.mode)
			response := request(t, tc.method, server.URL+cacheRESTPath+tc.path, token, strings.NewReader(tc.body), tc.contentType)
			if response.StatusCode != tc.status {
				t.Fatalf("status=%d want=%d body=%s", response.StatusCode, tc.status, readBody(t, response))
			}
			var result struct {
				Message string `json:"message"`
			}
			decodeResponse(t, response, &result)
			if !strings.HasPrefix(result.Message, tc.message) {
				t.Fatalf("message=%q want prefix=%q", result.Message, tc.message)
			}
		})
	}
}
