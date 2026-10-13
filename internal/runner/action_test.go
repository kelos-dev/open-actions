package runner

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/kelos-dev/open-actions/internal/actionref"
)

func TestActionDownloadRetriesFetch(t *testing.T) {
	for _, test := range []struct {
		name        string
		failCommand string
		failures    int
		wantError   bool
		wantCalls   []string
	}{
		{name: "success", wantCalls: []string{"init", "remote", "fetch", "checkout"}},
		{name: "transient failure", failCommand: "fetch", failures: 1, wantCalls: []string{"init", "remote", "fetch", "fetch", "checkout"}},
		{name: "last attempt succeeds", failCommand: "fetch", failures: 3, wantCalls: []string{"init", "remote", "fetch", "fetch", "fetch", "fetch", "checkout"}},
		{name: "persistent failure", failCommand: "fetch", failures: 4, wantError: true, wantCalls: []string{"init", "remote", "fetch", "fetch", "fetch", "fetch"}},
		{name: "init failure", failCommand: "init", failures: 1, wantError: true, wantCalls: []string{"init"}},
		{name: "remote failure", failCommand: "remote", failures: 1, wantError: true, wantCalls: []string{"init", "remote"}},
		{name: "checkout failure", failCommand: "checkout", failures: 1, wantError: true, wantCalls: []string{"init", "remote", "fetch", "checkout"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var calls []string
				var fetchTimes []time.Time
				var logs bytes.Buffer
				commandError := errors.New("git failed")
				failures := 0
				wantArguments := map[string][]string{
					"init":     {"init", "--quiet", "/actions/1"},
					"remote":   {"-C", "/actions/1", "remote", "add", "origin", "https://github.example/actions/example"},
					"fetch":    {"-C", "/actions/1", "fetch", "--quiet", "--depth=1", "origin", "v1"},
					"checkout": {"-C", "/actions/1", "checkout", "--quiet", "--detach", "FETCH_HEAD"},
				}
				logger := slog.New(slog.NewJSONHandler(&logs, nil))
				resolver := newActionResolver(logger, "https://github.example", "/actions", []string{"PATH=/usr/bin"}, "action-token", actionDownloadRetryDelay, func(_ context.Context, name string, arguments []string, directory string, environment []string) error {
					operation := arguments[0]
					if operation == "-C" {
						operation = arguments[2]
					}
					if name != "git" || directory != "/actions" || !slices.Equal(arguments, wantArguments[operation]) {
						t.Fatalf("command = %s %v in %s", name, arguments, directory)
					}
					if environmentValue(environment, "GIT_CONFIG_VALUE_0") != "AUTHORIZATION: "+basicAuthorization("action-token") ||
						environmentValue(environment, "GIT_CONFIG_KEY_0") != "http.https://github.example/actions/example/.extraHeader" ||
						environmentValue(environment, "GIT_TERMINAL_PROMPT") != "0" || environmentValue(environment, "PATH") != "/usr/bin" {
						t.Fatalf("download environment = %v", environment)
					}
					calls = append(calls, operation)
					if operation == "fetch" {
						fetchTimes = append(fetchTimes, time.Now())
					}
					if operation == test.failCommand {
						failures++
						if failures <= test.failures {
							return commandError
						}
					}
					return nil
				})
				err := resolver.download(context.Background(), actionref.Reference{Owner: "actions", Repository: "example", Ref: "v1"}, "/actions/1")
				if test.wantError {
					if !errors.Is(err, commandError) || !strings.Contains(err.Error(), "download action actions/example@v1") {
						t.Fatalf("download() error = %v", err)
					}
				} else if err != nil {
					t.Fatal(err)
				}
				if !slices.Equal(calls, test.wantCalls) {
					t.Fatalf("commands = %v, want %v", calls, test.wantCalls)
				}
				for index := 1; index < len(fetchTimes); index++ {
					delay := fetchTimes[index].Sub(fetchTimes[index-1])
					minimum := time.Second << (index - 1)
					if delay < minimum || delay >= minimum+time.Second {
						t.Fatalf("retry %d delay = %v, want [%v, %v)", index, delay, minimum, minimum+time.Second)
					}
				}
				if count := strings.Count(logs.String(), `"msg":"Retrying action download"`); count != max(0, len(fetchTimes)-1) {
					t.Fatalf("retry log count = %d, fetch attempts = %d", count, len(fetchTimes))
				}
			})
		})
	}
}

func TestActionDownloadCancellation(t *testing.T) {
	for _, timing := range []string{"before download", "during fetch", "during backoff", "deadline during backoff"} {
		t.Run(timing, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				wantError := context.Canceled
				wantElapsed := time.Duration(0)
				if timing == "before download" {
					cancel()
				} else if timing == "during backoff" {
					wantElapsed = 500 * time.Millisecond
					go func() {
						time.Sleep(wantElapsed)
						cancel()
					}()
				} else if timing == "deadline during backoff" {
					wantElapsed = 500 * time.Millisecond
					var cancelDeadline context.CancelFunc
					ctx, cancelDeadline = context.WithTimeout(ctx, wantElapsed)
					defer cancelDeadline()
					wantError = context.DeadlineExceeded
				}
				fetches := 0
				logger := slog.New(slog.NewTextHandler(io.Discard, nil))
				resolver := newActionResolver(logger, "https://github.example", "/actions", nil, "action-token", actionDownloadRetryDelay, func(_ context.Context, _ string, arguments []string, _ string, _ []string) error {
					if timing == "before download" {
						t.Fatal("command executed after cancellation")
					}
					if arguments[0] == "-C" && arguments[2] == "fetch" {
						fetches++
						if timing == "during fetch" {
							cancel()
						}
						return errors.New("git failed")
					}
					return nil
				})
				start := time.Now()
				err := resolver.download(ctx, actionref.Reference{Owner: "actions", Repository: "example", Ref: "v1"}, "/actions/1")
				if !errors.Is(err, wantError) {
					t.Fatalf("download() error = %v, want %v", err, wantError)
				}
				if elapsed := time.Since(start); elapsed != wantElapsed {
					t.Fatalf("download duration = %v, want %v", elapsed, wantElapsed)
				}
				if timing != "before download" && fetches != 1 {
					t.Fatalf("fetch attempts = %d, want 1", fetches)
				}
			})
		})
	}
}

func TestExecuteRetriesActionTokenPropagationFailures(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not installed")
	}
	for _, status := range []int{http.StatusUnauthorized, http.StatusForbidden} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			repositories := t.TempDir()
			createActionRepository(t, repositories, "installed-actions", "parent", "v1", map[string]string{
				"action.yml": "runs:\n  using: composite\n  steps:\n    - uses: installed-actions/child@v1\n",
			})
			createActionRepository(t, repositories, "installed-actions", "child", "v1", map[string]string{
				"action.yml": "runs:\n  using: composite\n  steps:\n    - run: touch \"$GITHUB_WORKSPACE/action-ran\"\n      shell: bash\n",
			})
			backend := newAuthenticatedGitServer(t, repositories, map[string]string{
				"installed-actions/parent": "action-installation-token",
				"installed-actions/child":  "action-installation-token",
			})
			var childFetches atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				if authorization := request.Header.Get("Authorization"); authorization != basicAuthorization("action-installation-token") {
					t.Errorf("action download authorization = %q", authorization)
				}
				if request.URL.Path == "/installed-actions/child/info/refs" && childFetches.Add(1) == 1 {
					writer.Header().Set("WWW-Authenticate", `Basic realm="GitHub"`)
					http.Error(writer, "token has not propagated", status)
					return
				}
				backend.Config.Handler.ServeHTTP(writer, request)
			}))
			t.Cleanup(server.Close)
			plan := testPlan()
			plan.Repository.ServerURL = server.URL
			plan.Repository.APIURL = server.URL
			plan.Repository.ActionCloneBaseURL = server.URL
			plan.Steps = []Step{{Uses: "installed-actions/parent@v1"}}
			workspace := t.TempDir()
			var stderr, logs bytes.Buffer
			executor, err := NewExecutor(ExecutorConfig{
				Logger:      slog.New(slog.NewJSONHandler(&logs, nil)),
				RunnerName:  "runner-1",
				GitHubToken: "installation-token",
				ActionToken: "action-installation-token",
				Environment: os.Environ(),
				Stdout:      io.Discard,
				Stderr:      &stderr,
			})
			if err != nil {
				t.Fatal(err)
			}
			executor.retryDelay = func(int) time.Duration { return 0 }
			if err := executor.Execute(context.Background(), plan, workspace); err != nil {
				t.Fatalf("Execute() error = %v: %s", err, stderr.String())
			}
			if _, err := os.Stat(filepath.Join(workspace, "action-ran")); err != nil {
				t.Fatalf("action did not run after retry: %v", err)
			}
			if childFetches.Load() != 2 {
				t.Fatalf("child fetch attempts = %d, want 2", childFetches.Load())
			}
			if !strings.Contains(logs.String(), `"msg":"Retrying action download","open_actions_runner":true,"action":"installed-actions/child@v1","attempt":2,"retry_after":0`) {
				t.Fatalf("retry log missing action and attempt: %s", logs.String())
			}
			for _, secret := range []string{"action-installation-token", base64.StdEncoding.EncodeToString([]byte("x-access-token:action-installation-token"))} {
				if strings.Contains(stderr.String()+logs.String(), secret) {
					t.Fatal("action token leaked into download output")
				}
			}
		})
	}
}
