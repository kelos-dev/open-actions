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
		{name: "key file", arguments: []string{"--public-url=https://artifacts.example", "--storage-directory=/tmp/artifacts", "--signing-key-file=/path/that/does/not/exist"}, want: "read artifact signing key"},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := runArtifactServer(context.Background(), test.arguments)
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
		"--public-url=https://artifacts.example", "--storage-directory=" + t.TempDir(), "--signing-key-file=" + path,
	})
	if err == nil || !strings.Contains(err.Error(), "at least 32 bytes") {
		t.Fatalf("runArtifactServer() error = %v", err)
	}
}
