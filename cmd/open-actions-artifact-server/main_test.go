package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRunArtifactServerRejectsInvalidConfiguration(t *testing.T) {
	for _, test := range []struct {
		name      string
		arguments []string
		want      string
	}{
		{name: "required values", want: "are required"},
		{name: "public URL", arguments: []string{"--public-url=git://artifacts.example", "--storage-directory=/tmp/artifacts", "--signing-key-file=/tmp/key"}, want: "must use http or https"},
		{name: "byte limits", arguments: []string{"--public-url=https://artifacts.example", "--storage-directory=/tmp/artifacts", "--signing-key-file=/tmp/key", "--max-file-bytes=2", "--max-artifact-bytes=1"}, want: "byte limits"},
		{name: "retention", arguments: []string{"--public-url=https://artifacts.example", "--storage-directory=/tmp/artifacts", "--signing-key-file=/tmp/key", "--default-retention-days=31", "--max-retention-days=30"}, want: "artifact retention"},
		{name: "cache limits", arguments: []string{"--public-url=https://artifacts.example", "--storage-directory=/tmp/artifacts", "--signing-key-file=/tmp/key", "--cache-max-repository-bytes=1"}, want: "cache limits"},
		{name: "cache retention", arguments: []string{"--public-url=https://artifacts.example", "--storage-directory=/tmp/artifacts", "--signing-key-file=/tmp/key", "--cache-retention-days=0"}, want: "cache retention"},
		{name: "log retention", arguments: []string{"--public-url=https://artifacts.example", "--storage-directory=/tmp/artifacts", "--signing-key-file=/tmp/key", "--log-retention-days=0"}, want: "log retention"},
		{name: "log bytes", arguments: []string{"--public-url=https://artifacts.example", "--storage-directory=/tmp/artifacts", "--signing-key-file=/tmp/key", "--max-log-bytes=0"}, want: "byte limit"},
		{name: "log chunks", arguments: []string{"--public-url=https://artifacts.example", "--storage-directory=/tmp/artifacts", "--signing-key-file=/tmp/key", "--max-log-chunks=0"}, want: "chunk limit"},
		{name: "key file", arguments: []string{"--public-url=https://artifacts.example", "--storage-directory=/tmp/artifacts", "--signing-key-file=/path/that/does/not/exist"}, want: "read artifact signing key"},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := runArtifactServer(context.Background(), append(test.arguments, "--log-read-key-file=/tmp/read-key"))
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("runArtifactServer() error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestRunArtifactServerRejectsShortSigningKey(t *testing.T) {
	path := filepath.Join(t.TempDir(), "signing-key")
	if err := os.WriteFile(path, []byte("short"), 0o600); err != nil {
		t.Fatal(err)
	}
	err := runArtifactServer(context.Background(), []string{
		"--public-url=https://artifacts.example", "--storage-directory=" + t.TempDir(), "--signing-key-file=" + path, "--log-read-key-file=/tmp/read-key",
	})
	if err == nil || !strings.Contains(err.Error(), "at least 32 bytes") {
		t.Fatalf("runArtifactServer() error = %v", err)
	}
}

func TestRunArtifactServerRequiresIndependentLogReadKey(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "key")
	if err := os.WriteFile(path, []byte(strings.Repeat("k", 32)), 0o600); err != nil {
		t.Fatal(err)
	}
	err := runArtifactServer(context.Background(), []string{"--public-url=https://artifacts.example", "--storage-directory=" + directory, "--signing-key-file=" + path, "--log-read-key-file=" + path})
	if err == nil || !strings.Contains(err.Error(), "must differ") {
		t.Fatalf("reused signing key = %v", err)
	}
}
