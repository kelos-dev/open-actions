//go:build artifactintegration

package runner

import (
	"bytes"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kelos-dev/open-actions/internal/artifact"
)

type artifactActionRelease struct {
	major string
	sha   string
}

func TestArtifactActions(t *testing.T) {
	for executable, version := range map[string]string{"node": "v20.", "node24": "v24."} {
		output, err := exec.CommandContext(t.Context(), executable, "--version").CombinedOutput()
		if err != nil || !strings.HasPrefix(string(output), version) {
			t.Fatalf("%s must provide Node.js %s: %v\n%s", executable, version, err, output)
		}
	}
	uploads := []artifactActionRelease{
		{"v4", "ea165f8d65b6e75b540449e92b4886f43607fa02"},
		{"v5", "330a01c490aca151604b8cf639adc76d48f6c5d4"},
		{"v6", "b7c566a772e6b6bfb58ed0dc250532a479d7789f"},
		{"v7", "043fb46d1a93c77aae656e7c1c64a875d1fc6a0a"},
	}
	downloads := []artifactActionRelease{
		{"v4", "d3f86a106a0bac45b974a628896c90dbdf5c8093"},
		{"v5", "634f93cb2916e3fdff6788551b99b062d0335ce0"},
		{"v6", "018cc2cf5baa6db3ef3c5f8a56943fffe632ef53"},
		{"v7", "37930b1c2abaa49bbe596cd826c3c89aef350131"},
		{"v8", "3e5f45b2cfb9172054b4087a40e8e0b5a5461e7c"},
	}
	actionsDirectory := t.TempDir()
	fetchArtifactActions(t, actionsDirectory, "upload-artifact", uploads)
	fetchArtifactActions(t, actionsDirectory, "download-artifact", downloads)
	cloneURL := (&url.URL{Scheme: "file", Path: actionsDirectory}).String()

	for _, host := range []string{"github.com", "example.ghe.com", "ghe.example.com"} {
		t.Run(host, func(t *testing.T) {
			codec, err := artifact.NewTokenCodec(bytes.Repeat([]byte("a"), artifact.MinimumSigningKeySize))
			if err != nil {
				t.Fatal(err)
			}
			store, err := artifact.NewStore(t.TempDir(), artifact.DefaultLimits())
			if err != nil {
				t.Fatal(err)
			}
			server := httptest.NewUnstartedServer(nil)
			service, err := artifact.NewServer(artifact.ServerConfig{
				Address: "127.0.0.1:0", PublicURL: "http://" + server.Listener.Addr().String(),
				Store: store, Tokens: codec, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
			})
			if err != nil {
				t.Fatal(err)
			}
			var requests atomic.Int64
			server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				service.Handler().ServeHTTP(w, r)
			})
			server.Start()
			t.Cleanup(server.Close)

			run := func(t *testing.T, job string, step Step, workspace string) Result {
				t.Helper()
				token, err := codec.NewRuntimeToken(time.Now(), time.Hour, artifact.TokenClaims{
					Scope: artifact.Scope{
						ProjectUID: "project", RepositoryID: 1, RootRunUID: "run", RunUID: "run", Attempt: 1,
					},
					WorkflowRunBackendID: "run", WorkflowJobBackendID: job,
				})
				if err != nil {
					t.Fatal(err)
				}
				plan := testPlan()
				plan.Repository.ServerURL = "https://" + host
				plan.Repository.ActionCloneBaseURL = cloneURL
				plan.JobID = job
				plan.TimeoutSeconds = 60
				plan.Steps = []Step{step}
				plan.Outputs = map[string]string{
					"artifact-id":     "${{ steps.artifact.outputs.artifact-id }}",
					"artifact-digest": "${{ steps.artifact.outputs.artifact-digest }}",
				}
				// A workflow cannot bypass the upstream host check with its env map.
				plan.Env = map[string]string{"GITHUB_SERVER_URL": "https://github.com"}
				environment := []string{
					"PATH=" + os.Getenv("PATH"), "HOME=" + t.TempDir(),
					ArtifactResultsURLEnvVar + "=" + server.URL, ArtifactTokenEnvVar + "=" + token,
				}
				var output bytes.Buffer
				executor := testExecutorWithEnvironment(t, environment, &output, &output)
				result, err := executor.ExecuteResult(t.Context(), plan, workspace)
				if host == "ghe.example.com" {
					if err == nil || !strings.Contains(output.String(), "GHESNotSupportedError") {
						t.Fatalf("expected upstream GHES rejection, got %v\n%s", err, &output)
					}
				} else if err != nil {
					t.Fatalf("execute %s: %v\n%s", step.Uses, err, &output)
				}
				return result
			}

			for _, release := range uploads {
				t.Run("upload-"+release.major, func(t *testing.T) {
					workspace := t.TempDir()
					if err := os.WriteFile(filepath.Join(workspace, "result.txt"), []byte(release.major), 0o600); err != nil {
						t.Fatal(err)
					}
					result := run(t, "upload-"+release.major, Step{
						ID: "artifact", Uses: "actions/upload-artifact@" + release.sha,
						With: map[string]string{"name": "upload-" + release.major, "path": "result.txt", "if-no-files-found": "error"},
					}, workspace)
					if host != "ghe.example.com" && (result.Outputs["artifact-id"] == "" || len(result.Outputs["artifact-digest"]) != 64) {
						t.Fatalf("artifact outputs = %v", result.Outputs)
					}
				})
			}
			for _, release := range downloads {
				t.Run("download-"+release.major, func(t *testing.T) {
					workspace := t.TempDir()
					run(t, "download-"+release.major, Step{
						ID: "artifact", Uses: "actions/download-artifact@" + release.sha,
						With: map[string]string{"pattern": "upload-*", "path": "downloads"},
					}, workspace)
					if host == "ghe.example.com" {
						return
					}
					for _, upload := range uploads {
						path := filepath.Join(workspace, "downloads", "upload-"+upload.major, "result.txt")
						data, err := os.ReadFile(path)
						if err != nil || string(data) != upload.major {
							t.Errorf("downloaded %s: %q, %v", upload.major, data, err)
						}
					}
				})
			}
			if host == "ghe.example.com" && requests.Load() != 0 {
				t.Fatalf("GHES actions made %d artifact service requests", requests.Load())
			}
		})
	}
}

func fetchArtifactActions(t *testing.T, root, name string, releases []artifactActionRelease) {
	t.Helper()
	directory := filepath.Join(root, "actions", name)
	commands := [][]string{
		{"init", "--quiet", "--bare", directory},
		{"-C", directory, "fetch", "--quiet", "--no-tags", "--depth=1", "https://github.com/actions/" + name},
	}
	for _, release := range releases {
		commands[1] = append(commands[1], fmt.Sprintf("%s:refs/tags/%s", release.sha, release.major))
	}
	for _, args := range commands {
		if output, err := exec.CommandContext(t.Context(), "git", args...).CombinedOutput(); err != nil {
			t.Fatalf("fetch actions/%s: %v\n%s", name, err, output)
		}
	}
}
