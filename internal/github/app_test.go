package github

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestAppBotLoginCache(t *testing.T) {
	now := time.Date(2026, time.November, 5, 0, 0, 0, 0, time.UTC)
	requests := 0
	appID := int64(1)
	slug := "open-actions"
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requests++
		if request.URL.Path != "/app" || request.Header.Get("Authorization") == "" {
			t.Errorf("unexpected App request: %s", request.URL.Path)
		}
		fmt.Fprintf(writer, `{"id":%d,"slug":%q}`, appID, slug)
	}))
	defer server.Close()
	client, err := NewClient(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	client.now = func() time.Time { return now }
	privateKey := testPrivateKey(t)
	lookup := func(want string, wantRequests int) {
		t.Helper()
		login, err := client.AppBotLogin(t.Context(), appID, privateKey)
		if err != nil || login != want || requests != wantRequests {
			t.Fatalf("AppBotLogin() = %q, %v; requests = %d, want %q and %d requests", login, err, requests, want, wantRequests)
		}
	}
	lookup("open-actions[bot]", 1)
	now = now.Add(appCacheTTL - time.Second)
	lookup("open-actions[bot]", 1)
	slug = "renamed-app"
	now = now.Add(time.Second)
	lookup("renamed-app[bot]", 2)
	appID = 2
	lookup("renamed-app[bot]", 3)
	privateKey = testPrivateKey(t)
	lookup("renamed-app[bot]", 4)
	privateKey = []byte("invalid key")
	if _, err := client.AppBotLogin(t.Context(), appID, privateKey); err == nil || requests != 4 {
		t.Fatalf("invalid private key: error = %v, requests = %d", err, requests)
	}
}

func TestAppBotLoginDoesNotCacheFailures(t *testing.T) {
	for _, failure := range []string{"unauthorized", "wrong App", "empty slug"} {
		t.Run(failure, func(t *testing.T) {
			requests := 0
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
				requests++
				if requests > 1 {
					fmt.Fprint(writer, `{"id":1,"slug":"open-actions"}`)
					return
				}
				switch failure {
				case "unauthorized":
					writer.WriteHeader(http.StatusUnauthorized)
				case "wrong App":
					fmt.Fprint(writer, `{"id":2,"slug":"open-actions"}`)
				case "empty slug":
					fmt.Fprint(writer, `{"id":1,"slug":""}`)
				}
			}))
			defer server.Close()
			client, err := NewClient(server.URL, server.Client())
			if err != nil {
				t.Fatal(err)
			}
			privateKey := testPrivateKey(t)
			if _, err := client.AppBotLogin(t.Context(), 1, privateKey); err == nil {
				t.Fatal("App lookup accepted invalid identity or credentials")
			}
			login, err := client.AppBotLogin(t.Context(), 1, privateKey)
			if err != nil || login != "open-actions[bot]" || requests != 2 {
				t.Fatalf("AppBotLogin() retry = %q, %v; requests = %d", login, err, requests)
			}
		})
	}
}

func TestAppBotLoginConcurrentRequests(t *testing.T) {
	var requests atomic.Int32
	started := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		if requests.Add(1) == 1 {
			close(started)
		}
		<-release
		fmt.Fprint(writer, `{"id":1,"slug":"open-actions"}`)
	}))
	defer server.Close()
	defer unblock()
	client, err := NewClient(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	privateKey := testPrivateKey(t)
	lookup := func() error {
		login, err := client.AppBotLogin(t.Context(), 1, privateKey)
		if err == nil && login != "open-actions[bot]" {
			return fmt.Errorf("unexpected bot login %q", login)
		}
		return err
	}
	results := make(chan error, 11)
	go func() { results <- lookup() }()
	<-started
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := client.AppBotLogin(ctx, 1, privateKey); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled waiter: %v", err)
	}
	for range 10 {
		go func() { results <- lookup() }()
	}
	unblock()
	for range 11 {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	if requests.Load() != 1 {
		t.Fatalf("concurrent App requests = %d, want 1", requests.Load())
	}
}
