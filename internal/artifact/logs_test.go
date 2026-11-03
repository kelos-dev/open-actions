package artifact

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/kelos-dev/open-actions/internal/joblog"
)

func TestJobLogAuthenticationAndPersistence(t *testing.T) {
	root := t.TempDir()
	store, err := NewStore(filepath.Join(root, "artifacts"), DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	logs, err := joblog.NewStore(filepath.Join(root, "logs"), time.Hour, 1<<20, joblog.DefaultMaxChunks)
	if err != nil {
		t.Fatal(err)
	}
	tokens, err := NewTokenCodec(bytes.Repeat([]byte("k"), 32))
	if err != nil {
		t.Fatal(err)
	}
	readTokens, err := joblog.NewReadTokenCodec(bytes.Repeat([]byte("r"), 32))
	if err != nil {
		t.Fatal(err)
	}
	server, err := NewServer(ServerConfig{Address: ":0", PublicURL: "http://artifacts.example", Store: store, Logs: logs, LogReads: readTokens, Tokens: tokens, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(server.Handler())
	defer httpServer.Close()
	client, err := joblog.NewClient(httpServer.URL, httpServer.Client())
	if err != nil {
		t.Fatal(err)
	}
	uploadToken, err := tokens.NewRuntimeToken(time.Now(), time.Hour, TokenClaims{Scope: Scope{ProjectUID: "project", RepositoryID: 1, RootRunUID: "run", RunUID: "attempt", Attempt: 2}, WorkflowRunBackendID: "attempt", WorkflowJobBackendID: "job"})
	if err != nil {
		t.Fatal(err)
	}
	chunk := joblog.Chunk{Data: []byte("masked runner output\n"), Complete: true}
	if err := client.Append(context.Background(), uploadToken, chunk); err != nil {
		t.Fatal(err)
	}
	if err := client.Append(context.Background(), uploadToken, chunk); err != nil {
		t.Fatalf("retry: %v", err)
	}
	readToken := readTokens.Sign("job", time.Now().Add(time.Minute))
	actual, err := client.Read(context.Background(), "job", 0, readToken)
	if err != nil || !bytes.Equal(actual.Data, chunk.Data) || !actual.Complete {
		t.Fatalf("read = %#v, %v", actual, err)
	}
	for _, test := range []struct{ job, token string }{
		{"job", ""}, {"job", uploadToken}, {"other-job", readToken}, {"job", readTokens.Sign("job", time.Now().Add(-time.Second))},
	} {
		if _, err := client.Read(context.Background(), test.job, 0, test.token); err == nil {
			t.Fatalf("read accepted invalid credential for %q", test.job)
		}
	}
	if err := client.Append(context.Background(), readToken, chunk); err == nil {
		t.Fatal("read credential permitted upload")
	}
	response := request(t, http.MethodDelete, httpServer.URL+joblog.Path+"job/0", readToken, nil, "")
	defer response.Body.Close()
	if response.StatusCode != http.StatusNotFound {
		t.Fatalf("DELETE = %d", response.StatusCode)
	}
}

func TestLogReadKeyCannotAuthorizeArtifactOperations(t *testing.T) {
	master, err := NewTokenCodec(bytes.Repeat([]byte("m"), 32))
	if err != nil {
		t.Fatal(err)
	}
	readKey := bytes.Repeat([]byte("r"), 32)
	impersonator, err := NewTokenCodec(readKey)
	if err != nil {
		t.Fatal(err)
	}
	claims := TokenClaims{Scope: Scope{ProjectUID: "project", RepositoryID: 1, RootRunUID: "run", RunUID: "run", Attempt: 1}, WorkflowRunBackendID: "run", WorkflowJobBackendID: "job"}
	forged, err := impersonator.NewRuntimeToken(time.Now(), time.Hour, claims)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := master.ParseRuntimeToken(forged, time.Now()); err == nil {
		t.Fatal("artifact service accepted credentials minted with the log read key")
	}
	reads, err := joblog.NewReadTokenCodec(readKey)
	if err != nil {
		t.Fatal(err)
	}
	other, err := joblog.NewReadTokenCodec(bytes.Repeat([]byte("m"), 32))
	if err != nil {
		t.Fatal(err)
	}
	if !reads.Validate("job", reads.Sign("job", time.Now().Add(time.Minute)), time.Now()) || reads.Validate("job", other.Sign("job", time.Now().Add(time.Minute)), time.Now()) {
		t.Fatal("log read keys are not isolated")
	}
}
