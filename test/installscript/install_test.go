package installscript_test

import (
	"crypto/sha256"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestInstallScript(t *testing.T) {
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skipf("install script does not support %s", runtime.GOOS)
	}

	arch := map[string]string{
		"amd64": "amd64",
		"arm64": "arm64",
	}[runtime.GOARCH]
	if arch == "" {
		t.Skipf("install script does not support %s", runtime.GOARCH)
	}

	binaryName := fmt.Sprintf("open-actions-%s-%s", runtime.GOOS, arch)
	binaryData := []byte("#!/bin/sh\necho installed\n")
	checksum := sha256.Sum256(binaryData)

	tests := []struct {
		name      string
		checksums string
		wantError string
	}{
		{
			name:      "installs verified binary",
			checksums: fmt.Sprintf("%x  %s\n", checksum, binaryName),
		},
		{
			name:      "rejects checksum mismatch",
			checksums: fmt.Sprintf("%064d  %s\n", 0, binaryName),
			wantError: "Checksum verification failed for " + binaryName,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				switch request.URL.Path {
				case "/" + binaryName:
					_, _ = writer.Write(binaryData)
				case "/checksums.txt":
					_, _ = writer.Write([]byte(test.checksums))
				default:
					http.NotFound(writer, request)
				}
			}))
			t.Cleanup(server.Close)

			repositoryRoot, err := filepath.Abs(filepath.Join("..", ".."))
			if err != nil {
				t.Fatalf("resolve repository root: %v", err)
			}
			installDirectory := filepath.Join(t.TempDir(), "nested", "bin")
			command := exec.Command("bash", filepath.Join(repositoryRoot, "hack", "install.sh"))
			command.Env = append(os.Environ(),
				"OPEN_ACTIONS_RELEASE_URL="+server.URL,
				"INSTALL_DIR="+installDirectory,
			)
			output, err := command.CombinedOutput()
			if test.wantError != "" {
				if err == nil {
					t.Fatalf("install script succeeded, want error containing %q", test.wantError)
				}
				if !strings.Contains(string(output), test.wantError) {
					t.Fatalf("install output did not contain %q:\n%s", test.wantError, output)
				}
				return
			}
			if err != nil {
				t.Fatalf("run install script: %v\n%s", err, output)
			}

			installedBinary := filepath.Join(installDirectory, "open-actions")
			info, err := os.Stat(installedBinary)
			if err != nil {
				t.Fatalf("stat installed binary: %v", err)
			}
			if info.Mode().Perm()&0o111 == 0 {
				t.Fatalf("installed binary mode %v is not executable", info.Mode().Perm())
			}

			installedOutput, err := exec.Command(installedBinary).CombinedOutput()
			if err != nil {
				t.Fatalf("run installed binary: %v\n%s", err, installedOutput)
			}
			if string(installedOutput) != "installed\n" {
				t.Fatalf("installed binary output = %q, want %q", installedOutput, "installed\n")
			}
			if !strings.Contains(string(output), "open-actions installed to "+installedBinary) {
				t.Fatalf("install output did not identify destination:\n%s", output)
			}
		})
	}
}
