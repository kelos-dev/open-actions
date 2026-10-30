//go:build cacheconformance

package artifact

import (
	"archive/tar"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// Pinned action bundles exercise their shipped toolkit and Azure SDK implementations.
func TestCacheConformanceActions(t *testing.T) {
	server, codec, _ := cacheTestServer(t)
	for _, tc := range []struct{ name, sha string }{
		{"cache", "0057852bfaa89a56745cba8c7296529d2fc39830"},
		{"setup-go", "40f1582b2485089dde7abd97c1529aa768e1baff"},
		{"setup-node", "49933ea5288caeca8642d1e84afbd3f7d6820020"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			action := downloadCacheAction(t, tc.name, tc.sha)
			toolcache := t.TempDir()
			nodePath, err := exec.LookPath("node")
			if err != nil {
				t.Fatal(err)
			}
			nodeOutput, err := exec.Command(nodePath, "--version").Output()
			if err != nil {
				t.Fatal(err)
			}
			nodeVersion := strings.TrimPrefix(strings.TrimSpace(string(nodeOutput)), "v")
			goOutput, err := exec.Command("go", "env", "GOVERSION").Output()
			if err != nil {
				t.Fatal(err)
			}
			goVersion := strings.TrimPrefix(strings.TrimSpace(string(goOutput)), "go")
			arch := runtime.GOARCH
			if arch == "amd64" {
				arch = "x64"
			}
			for _, tool := range []struct{ name, version, source string }{{"go", goVersion, runtime.GOROOT()}, {"node", nodeVersion, ""}} {
				directory := filepath.Join(toolcache, tool.name, tool.version)
				if err := os.MkdirAll(directory, 0700); err != nil {
					t.Fatal(err)
				}
				if tool.name == "go" {
					if err := os.Symlink(tool.source, filepath.Join(directory, arch)); err != nil {
						t.Fatal(err)
					}
				} else {
					bin := filepath.Join(directory, arch, "bin")
					if err := os.MkdirAll(bin, 0700); err != nil {
						t.Fatal(err)
					}
					for _, program := range []string{"node", "npm"} {
						source, err := exec.LookPath(program)
						if err != nil {
							t.Fatal(err)
						}
						source, err = filepath.EvalSymlinks(source)
						if err != nil {
							t.Fatal(err)
						}
						if err := os.Symlink(source, filepath.Join(bin, program)); err != nil {
							t.Fatal(err)
						}
					}
				}
				if err := os.WriteFile(filepath.Join(directory, arch+".complete"), nil, 0600); err != nil {
					t.Fatal(err)
				}
			}
			workspace := filepath.Join(t.TempDir(), "workspace")
			home := filepath.Join(t.TempDir(), "home")
			for attempt := range 2 {
				for _, directory := range []string{workspace, home} {
					if err := os.RemoveAll(directory); err != nil {
						t.Fatal(err)
					}
					if err := os.MkdirAll(directory, 0700); err != nil {
						t.Fatal(err)
					}
				}
				temp := t.TempDir()
				state, output, envPath := filepath.Join(temp, "state"), filepath.Join(temp, "output"), filepath.Join(temp, "path")
				for _, p := range []string{state, output, envPath, filepath.Join(temp, "env")} {
					if err := os.WriteFile(p, nil, 0600); err != nil {
						t.Fatal(err)
					}
				}
				token := cacheToken(t, codec, "project", fmt.Sprintf("%s-job-%d", tc.name, attempt), "refs/heads/main", "write")
				env := map[string]string{
					"HOME": home, "GITHUB_WORKSPACE": workspace, "GITHUB_OUTPUT": output, "GITHUB_STATE": state, "GITHUB_PATH": envPath, "GITHUB_ENV": filepath.Join(temp, "env"),
					"GITHUB_SERVER_URL": "https://github.com", "GITHUB_API_URL": "https://api.github.com", "GITHUB_REPOSITORY": "acme/example", "GITHUB_JOB": "test",
					"GITHUB_REF": "refs/heads/main", "GITHUB_SHA": strings.Repeat("a", 40), "GITHUB_ACTIONS": "true", "RUNNER_OS": "Linux", "RUNNER_ARCH": strings.ToUpper(arch),
					"RUNNER_TEMP": temp, "RUNNER_TOOL_CACHE": toolcache, "ACTIONS_RESULTS_URL": server.URL, "ACTIONS_RUNTIME_TOKEN": token, "ACTIONS_CACHE_SERVICE_V2": "true",
					"ACTIONS_CACHE_MODE": "write", "ACTIONS_CACHE_URL": server.URL + "/", "GITHUB_RUN_ID": fmt.Sprint(attempt + 1), "GITHUB_RUN_ATTEMPT": "1",
					"GOCACHE": filepath.Join(workspace, "go-build"), "GOPATH": filepath.Join(workspace, "gopath"), "NPM_CONFIG_CACHE": filepath.Join(workspace, "npm-cache"),
				}
				marker := ""
				switch tc.name {
				case "cache":
					env["INPUT_PATH"] = filepath.Join(workspace, "cached")
					env["INPUT_KEY"] = "conformance-cache"
					marker = filepath.Join(workspace, "cached", "marker")
				case "setup-go":
					env["INPUT_GO-VERSION"] = goVersion
					env["INPUT_CACHE"] = "true"
					env["INPUT_CACHE-DEPENDENCY-PATH"] = "go.sum"
					if err := os.WriteFile(filepath.Join(workspace, "go.mod"), []byte("module example.com/conformance\n\ngo 1.25\n"), 0600); err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(filepath.Join(workspace, "go.sum"), []byte(""), 0600); err != nil {
						t.Fatal(err)
					}
					marker = filepath.Join(workspace, "gopath", "pkg", "mod", "marker")
				case "setup-node":
					env["INPUT_NODE-VERSION"] = nodeVersion
					env["INPUT_CACHE"] = "npm"
					env["INPUT_CACHE-DEPENDENCY-PATH"] = "package-lock.json"
					if err := os.WriteFile(filepath.Join(workspace, "package-lock.json"), []byte(`{"name":"conformance","lockfileVersion":3,"packages":{}}`), 0600); err != nil {
						t.Fatal(err)
					}
					marker = filepath.Join(workspace, "npm-cache", "marker")
				}
				var metadata struct {
					Inputs map[string]struct{ Default any }
					Runs   struct{ Main, Post string }
				}
				data, err := os.ReadFile(filepath.Join(action, "action.yml"))
				if err != nil {
					t.Fatal(err)
				}
				if err := yaml.Unmarshal(data, &metadata); err != nil {
					t.Fatal(err)
				}
				for name, input := range metadata.Inputs {
					key := "INPUT_" + strings.ToUpper(strings.ReplaceAll(name, " ", "_"))
					if _, set := env[key]; !set && input.Default != nil {
						env[key] = fmt.Sprint(input.Default)
					}
				}
				mainOutput := runCacheAction(t, nodePath, filepath.Join(action, metadata.Runs.Main), workspace, env)
				if attempt == 0 {
					if !strings.Contains(strings.ToLower(mainOutput), "cache not found") {
						t.Fatalf("expected cache miss:\n%s", mainOutput)
					}
					if err := os.MkdirAll(filepath.Dir(marker), 0700); err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(marker, []byte("preserved across runs"), 0600); err != nil {
						t.Fatal(err)
					}
					if tc.name == "setup-go" {
						if err := os.MkdirAll(env["GOCACHE"], 0700); err != nil {
							t.Fatal(err)
						}
						if err := os.WriteFile(filepath.Join(env["GOCACHE"], "marker"), []byte("build cache"), 0600); err != nil {
							t.Fatal(err)
						}
					}
				} else {
					data, err := os.ReadFile(marker)
					if err != nil || string(data) != "preserved across runs" {
						t.Fatalf("cache did not restore: data=%q err=%v\n%s", data, err, mainOutput)
					}
				}
				if attempt == 1 {
					values := readCacheActionValues(t, output)
					if values["cache-hit"] != "true" {
						t.Fatalf("expected exact cache hit: %#v\n%s", values, mainOutput)
					}
				}
				for key, value := range readCacheActionValues(t, state) {
					env["STATE_"+key] = value
				}
				env["STATE_isPost"] = "true"
				postOutput := runCacheAction(t, nodePath, filepath.Join(action, metadata.Runs.Post), workspace, env)
				if attempt == 0 && !strings.Contains(strings.ToLower(postOutput), "cache saved") {
					t.Fatalf("expected post-job save:\n%s", postOutput)
				}
				t.Logf("run %d completed with restore and post-job save hooks", attempt+1)
			}
		})
	}
}

func readCacheActionValues(t *testing.T, path string) map[string]string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	values := map[string]string{}
	lines := strings.Split(string(data), "\n")
	for i := 0; i < len(lines); i++ {
		key, delimiter, multiline := strings.Cut(lines[i], "<<")
		if multiline {
			content := []string{}
			i++
			for i < len(lines) && lines[i] != delimiter {
				content = append(content, lines[i])
				i++
			}
			values[key] = strings.Join(content, "\n")
		} else if key, value, ok := strings.Cut(lines[i], "="); ok {
			values[key] = value
		}
	}
	return values
}

func runCacheAction(t *testing.T, node, script, workspace string, values map[string]string) string {
	t.Helper()
	command := exec.Command(node, script)
	command.Dir = workspace
	for _, entry := range os.Environ() {
		key, _, _ := strings.Cut(entry, "=")
		if _, override := values[key]; !override {
			command.Env = append(command.Env, entry)
		}
	}
	for key, value := range values {
		command.Env = append(command.Env, key+"="+value)
	}
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("action failed: %v\n%s", err, output)
	}
	return string(output)
}

func downloadCacheAction(t *testing.T, name, sha string) string {
	t.Helper()
	command := exec.Command("gh", "api", "repos/actions/"+name+"/tarball/"+sha)
	archive, err := os.CreateTemp(t.TempDir(), "action-*.tar.gz")
	if err != nil {
		t.Fatal(err)
	}
	defer archive.Close()
	command.Stdout = archive
	command.Stderr = os.Stderr
	var downloadErr error
	for range 3 {
		if err := archive.Truncate(0); err != nil {
			t.Fatal(err)
		}
		if _, err := archive.Seek(0, 0); err != nil {
			t.Fatal(err)
		}
		command = exec.Command("gh", "api", "repos/actions/"+name+"/tarball/"+sha)
		command.Stdout = archive
		command.Stderr = os.Stderr
		downloadErr = command.Run()
		if downloadErr == nil {
			break
		}
	}
	if downloadErr != nil {
		t.Fatal(downloadErr)
	}
	if _, err := archive.Seek(0, 0); err != nil {
		t.Fatal(err)
	}
	gz, err := gzip.NewReader(archive)
	if err != nil {
		t.Fatal(err)
	}
	defer gz.Close()
	reader := tar.NewReader(gz)
	root := t.TempDir()
	for {
		header, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		_, name, ok := strings.Cut(header.Name, "/")
		if !ok || header.Typeflag != tar.TypeReg {
			continue
		}
		if name != "action.yml" && !strings.HasPrefix(name, "dist/") {
			continue
		}
		if !filepath.IsLocal(name) {
			t.Fatal("unsafe action archive path")
		}
		destination := filepath.Join(root, name)
		if err := os.MkdirAll(filepath.Dir(destination), 0700); err != nil {
			t.Fatal(err)
		}
		f, err := os.OpenFile(destination, os.O_CREATE|os.O_WRONLY, 0600)
		if err != nil {
			t.Fatal(err)
		}
		_, err = io.Copy(f, reader)
		closeErr := f.Close()
		if err != nil {
			t.Fatal(err)
		}
		if closeErr != nil {
			t.Fatal(closeErr)
		}
	}
	return root
}

func TestCacheConformanceBuildKit(t *testing.T) {
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("Docker with Buildx is required for the BuildKit conformance test")
	}
	if output, err := exec.Command("docker", "info").CombinedOutput(); err != nil {
		t.Fatalf("Docker is unavailable: %v\n%s", err, output)
	}
	server, codec, _ := cacheTestServer(t)
	token := cacheToken(t, codec, "project", "buildkit", "refs/heads/main", "write")
	builder := "open-actions-cache-" + fmt.Sprint(os.Getpid())
	run := func(arguments ...string) string {
		t.Helper()
		command := exec.Command("docker", arguments...)
		output, err := command.CombinedOutput()
		if err != nil {
			t.Fatalf("docker failed: %v\n%s", err, output)
		}
		return string(output)
	}
	run("buildx", "create", "--name", builder, "--driver", "docker-container", "--driver-opt", "network=host")
	t.Cleanup(func() { _ = exec.Command("docker", "buildx", "rm", builder).Run() })
	context := t.TempDir()
	if err := os.WriteFile(filepath.Join(context, "Dockerfile"), []byte("FROM busybox:1.37.0\nRUN echo cached-layer > /marker\n"), 0600); err != nil {
		t.Fatal(err)
	}
	backend := "type=gha,version=2,url_v2=" + server.URL + ",token=" + token + ",scope=conformance"
	run("buildx", "build", "--builder", builder, "--progress=plain", "--cache-to", backend+",mode=max", context)
	run("buildx", "prune", "--builder", builder, "--all", "--force")
	output := run("buildx", "build", "--builder", builder, "--progress=plain", "--cache-from", backend, context)
	step := regexp.MustCompile(`(?m)^#(\d+) \[[^\]]+\] RUN echo cached-layer > /marker$`).FindStringSubmatch(output)
	if len(step) != 2 || !regexp.MustCompile(`(?m)^#`+step[1]+` CACHED$`).MatchString(output) {
		t.Fatalf("BuildKit did not reuse the exported RUN layer:\n%s", output)
	}
}
