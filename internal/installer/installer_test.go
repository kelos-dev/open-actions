package installer

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"
)

func TestInstallUpgradesEmbeddedHelmChart(t *testing.T) {
	chart := fstest.MapFS{
		"Chart.yaml":                {Data: []byte("name: open-actions\n")},
		"templates/deployment.yaml": {Data: []byte("kind: Deployment\n")},
	}
	var commandName string
	var commandArguments []string
	installer, err := New(Config{
		Chart:      chart,
		Helm:       "test-helm",
		ValuesFile: "values-test.yaml",
		Stdout:     io.Discard,
		Stderr:     io.Discard,
		RunCommand: func(_ context.Context, name string, arguments []string, _, _ io.Writer) error {
			commandName = name
			commandArguments = append([]string(nil), arguments...)
			if len(arguments) != 9 {
				t.Fatalf("arguments = %v, want Helm upgrade arguments", arguments)
			}
			content, readError := os.ReadFile(filepath.Join(arguments[3], "templates", "deployment.yaml"))
			if readError != nil {
				t.Fatalf("read staged deployment: %v", readError)
			}
			if string(content) != "kind: Deployment\n" {
				t.Fatalf("staged deployment = %q", content)
			}
			return nil
		},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := installer.Install(context.Background()); err != nil {
		t.Fatalf("Install() error = %v", err)
	}
	if commandName != "test-helm" {
		t.Fatalf("command name = %q, want test-helm", commandName)
	}
	if commandArguments[0] != "upgrade" || commandArguments[1] != "--install" || commandArguments[2] != "open-actions" ||
		commandArguments[4] != "--namespace" || commandArguments[5] != "open-actions-system" || commandArguments[6] != "--create-namespace" ||
		commandArguments[7] != "--values" || commandArguments[8] != "values-test.yaml" {
		t.Fatalf("command arguments = %v", commandArguments)
	}
}

func TestInstallReturnsCommandError(t *testing.T) {
	commandError := errors.New("command failed")
	installer, err := New(Config{
		Chart:  fstest.MapFS{"Chart.yaml": {Data: []byte("name: open-actions\n")}},
		Helm:   "helm",
		Stdout: io.Discard,
		Stderr: io.Discard,
		RunCommand: func(context.Context, string, []string, io.Writer, io.Writer) error {
			return commandError
		},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := installer.Install(context.Background()); !errors.Is(err, commandError) || !strings.Contains(err.Error(), "install Open Actions") {
		t.Fatalf("Install() error = %v, want wrapped command error", err)
	}
}

func TestNewRequiresConfiguration(t *testing.T) {
	tests := []struct {
		name   string
		change func(*Config)
		want   string
	}{
		{name: "chart", change: func(config *Config) { config.Chart = nil }, want: "Helm chart is required"},
		{name: "helm", change: func(config *Config) { config.Helm = "" }, want: "Helm command is required"},
		{name: "stdout", change: func(config *Config) { config.Stdout = nil }, want: "stdout is required"},
		{name: "stderr", change: func(config *Config) { config.Stderr = nil }, want: "stderr is required"},
		{name: "runner", change: func(config *Config) { config.RunCommand = nil }, want: "command runner is required"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := Config{
				Chart:      fstest.MapFS{},
				Helm:       "helm",
				Stdout:     &bytes.Buffer{},
				Stderr:     &bytes.Buffer{},
				RunCommand: func(context.Context, string, []string, io.Writer, io.Writer) error { return nil },
			}
			test.change(&config)
			if _, err := New(config); err == nil || err.Error() != test.want {
				t.Fatalf("New() error = %v, want %q", err, test.want)
			}
		})
	}
}
