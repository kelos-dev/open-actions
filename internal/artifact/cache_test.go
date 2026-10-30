package artifact

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/kelos-dev/open-actions/internal/cache"
)

func cacheTestServer(t *testing.T) (*httptest.Server, *TokenCodec, *cache.Store) {
	t.Helper()
	store, err := NewStore(t.TempDir(), DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	caches, err := cache.NewStore(t.TempDir(), cache.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewUnstartedServer(nil)
	codec := testTokenCodec(t)
	handler, err := NewServer(ServerConfig{Address: "127.0.0.1:0", PublicURL: "http://" + server.Listener.Addr().String(), Store: store, CacheStore: caches, Tokens: codec, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if err != nil {
		t.Fatal(err)
	}
	server.Config.Handler = handler.Handler()
	server.Start()
	t.Cleanup(server.Close)
	return server, codec, caches
}

func cacheToken(t *testing.T, codec *TokenCodec, project, job, ref, mode string) string {
	t.Helper()
	claims := testClaims(job)
	claims.ProjectUID = project
	claims.RunUID = job + "-run"
	claims.RootRunUID = claims.RunUID
	a := cache.ResolveAccess("push", ref, "", "main", mode)
	claims.Cache = &a
	return testRuntimeToken(t, codec, time.Now(), claims)
}

func postCache(t *testing.T, server, method, token string, input any) *http.Response {
	t.Helper()
	data, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	return request(t, http.MethodPost, server+cacheServicePath+method, token, strings.NewReader(string(data)), "application/json")
}

func TestCacheProtocolAcrossRunsAndBlobOperations(t *testing.T) {
	for _, blocks := range []bool{false, true} {
		t.Run(fmt.Sprintf("blocks=%t", blocks), func(t *testing.T) {
			server, codec, _ := cacheTestServer(t)
			writer := cacheToken(t, codec, "project", "first-job", "refs/heads/main", "write")
			reader := cacheToken(t, codec, "project", "next-job", "refs/heads/feature", "read")
			input := map[string]any{"key": "npm-lock", "version": "version"}
			miss := postCache(t, server.URL, "GetCacheEntryDownloadURL", reader, input)
			var result struct {
				OK       bool   `json:"ok"`
				Upload   string `json:"signed_upload_url"`
				Download string `json:"signed_download_url"`
				Key      string `json:"matched_key"`
				ID       string `json:"entry_id"`
			}
			decodeResponse(t, miss, &result)
			if result.OK {
				t.Fatal("unexpected hit")
			}
			created := postCache(t, server.URL, "CreateCacheEntry", writer, input)
			decodeResponse(t, created, &result)
			if !result.OK || result.Upload == "" {
				t.Fatalf("create=%#v", result)
			}
			upload := result.Upload
			if strings.Contains(upload, writer) {
				t.Fatal("URL contains runtime token")
			}
			bad := request(t, http.MethodGet, upload, "", nil, "")
			if bad.StatusCode != http.StatusUnauthorized {
				t.Fatal(bad.StatusCode)
			}
			bad.Body.Close()
			before := postCache(t, server.URL, "GetCacheEntryDownloadURL", reader, input)
			var pending map[string]any
			decodeResponse(t, before, &pending)
			if pending["ok"] != false {
				t.Fatal(pending)
			}
			if blocks {
				parsed, err := url.Parse(upload)
				if err != nil {
					t.Fatal(err)
				}
				q := parsed.Query()
				q.Set("comp", "block")
				q.Set("blockid", "a")
				parsed.RawQuery = q.Encode()
				staged := request(t, http.MethodPut, parsed.String(), "", strings.NewReader("cache data"), "application/octet-stream")
				if staged.StatusCode != http.StatusCreated {
					t.Fatal(readBody(t, staged))
				}
				staged.Body.Close()
				q.Set("comp", "blocklist")
				q.Del("blockid")
				parsed.RawQuery = q.Encode()
				committed := request(t, http.MethodPut, parsed.String(), "", strings.NewReader("<BlockList><Latest>a</Latest></BlockList>"), "application/xml")
				if committed.StatusCode != http.StatusCreated {
					t.Fatal(readBody(t, committed))
				}
				committed.Body.Close()
			} else {
				uploaded := request(t, http.MethodPut, upload, "", strings.NewReader("cache data"), "application/octet-stream")
				if uploaded.StatusCode != http.StatusCreated {
					t.Fatal(readBody(t, uploaded))
				}
				uploaded.Body.Close()
			}
			input["size_bytes"] = 10
			finalized := postCache(t, server.URL, "FinalizeCacheEntryUpload", writer, input)
			decodeResponse(t, finalized, &result)
			if !result.OK {
				t.Fatal(result)
			}
			if _, err := strconv.ParseInt(result.ID, 10, 64); err != nil {
				t.Fatal(err)
			}
			// The Node toolkit serializes int64 fields as strings; BuildKit uses numbers.
			input["size_bytes"] = "10"
			finalized = postCache(t, server.URL, "FinalizeCacheEntryUpload", writer, input)
			decodeResponse(t, finalized, &result)
			hit := postCache(t, server.URL, "GetCacheEntryDownloadURL", reader, input)
			decodeResponse(t, hit, &result)
			if !result.OK || result.Key != "npm-lock" || result.Download == "" {
				t.Fatalf("lookup=%#v", result)
			}
			download := request(t, http.MethodGet, result.Download, "", nil, "")
			if download.StatusCode != http.StatusOK || readBody(t, download) != "cache data" {
				t.Fatal(download.StatusCode)
			}
			head := request(t, http.MethodHead, result.Download, "", nil, "")
			if head.StatusCode != http.StatusOK || head.ContentLength != 10 {
				t.Fatalf("HEAD=%d %d", head.StatusCode, head.ContentLength)
			}
			head.Body.Close()
			rangeRequest, err := http.NewRequest(http.MethodGet, result.Download, nil)
			if err != nil {
				t.Fatal(err)
			}
			rangeRequest.Header.Set("Range", "bytes=2-5")
			partial, err := http.DefaultClient.Do(rangeRequest)
			if err != nil {
				t.Fatal(err)
			}
			if partial.StatusCode != http.StatusPartialContent || readBody(t, partial) != "che " {
				t.Fatal(partial.StatusCode)
			}
			overwrite := request(t, http.MethodPut, result.Download, "", strings.NewReader("bad"), "application/octet-stream")
			if overwrite.StatusCode != http.StatusUnauthorized {
				t.Fatal(overwrite.StatusCode)
			}
			overwrite.Body.Close()
			overwrite = request(t, http.MethodPut, upload, "", strings.NewReader("bad"), "application/octet-stream")
			if overwrite.StatusCode != http.StatusConflict {
				t.Fatal(overwrite.StatusCode)
			}
			overwrite.Body.Close()
		})
	}
}

func TestCacheProtocolEnforcesCredentialScopes(t *testing.T) {
	server, codec, _ := cacheTestServer(t)
	owner := cacheToken(t, codec, "project", "owner", "refs/heads/main", "write")
	input := map[string]any{"key": "key", "version": "v"}
	created := postCache(t, server.URL, "CreateCacheEntry", owner, input)
	var result struct {
		OK  bool   `json:"ok"`
		URL string `json:"signed_upload_url"`
	}
	decodeResponse(t, created, &result)
	uploaded := request(t, http.MethodPut, result.URL, "", strings.NewReader("data"), "application/octet-stream")
	uploaded.Body.Close()
	input["size_bytes"] = "4"
	finalize := postCache(t, server.URL, "FinalizeCacheEntryUpload", owner, input)
	finalize.Body.Close()
	for _, tc := range []struct {
		name, project, ref, mode, method string
		status                           int
		hit                              bool
	}{
		{"cross project", "other", "refs/heads/main", "write", "GetCacheEntryDownloadURL", 200, false},
		{"read only cannot save", "project", "refs/heads/main", "read", "CreateCacheEntry", 403, false},
		{"none cannot read", "project", "refs/heads/main", "none", "GetCacheEntryDownloadURL", 403, false},
		{"write only cannot read", "project", "refs/heads/main", "write-only", "GetCacheEntryDownloadURL", 403, false},
		{"feature can read default", "project", "refs/heads/feature", "read", "GetCacheEntryDownloadURL", 200, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			token := cacheToken(t, codec, tc.project, tc.name, tc.ref, tc.mode)
			response := postCache(t, server.URL, tc.method, token, input)
			if response.StatusCode != tc.status {
				t.Fatalf("status=%d body=%s", response.StatusCode, readBody(t, response))
			}
			if tc.status == 200 {
				var got struct {
					OK bool `json:"ok"`
				}
				decodeResponse(t, response, &got)
				if got.OK != tc.hit {
					t.Fatal(got)
				}
			} else {
				response.Body.Close()
			}
		})
	}
	token := testRuntimeToken(t, codec, time.Now(), testClaims("artifact-only"))
	denied := postCache(t, server.URL, "GetCacheEntryDownloadURL", token, input)
	if denied.StatusCode != http.StatusForbidden {
		t.Fatal(denied.StatusCode)
	}
	denied.Body.Close()
	for _, signed := range []string{result.URL + "tampered", strings.Replace(result.URL, "/cache?", "/different?", 1)} {
		response := request(t, http.MethodPut, signed, "", strings.NewReader("bad"), "application/octet-stream")
		if response.StatusCode != http.StatusUnauthorized {
			t.Fatal(response.StatusCode)
		}
		response.Body.Close()
	}
}
