package installer

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
)

type CommandFunc func(context.Context, string, []string, io.Writer, io.Writer) error

type Config struct {
	Chart      fs.FS
	Helm       string
	ValuesFile string
	Stdout     io.Writer
	Stderr     io.Writer
	RunCommand CommandFunc
}

type Installer struct {
	chart      fs.FS
	helm       string
	valuesFile string
	stdout     io.Writer
	stderr     io.Writer
	runCommand CommandFunc
}

func New(config Config) (*Installer, error) {
	if config.Chart == nil {
		return nil, fmt.Errorf("Helm chart is required")
	}
	if config.Helm == "" {
		return nil, fmt.Errorf("Helm command is required")
	}
	if config.Stdout == nil {
		return nil, fmt.Errorf("stdout is required")
	}
	if config.Stderr == nil {
		return nil, fmt.Errorf("stderr is required")
	}
	if config.RunCommand == nil {
		return nil, fmt.Errorf("command runner is required")
	}
	return &Installer{
		chart:      config.Chart,
		helm:       config.Helm,
		valuesFile: config.ValuesFile,
		stdout:     config.Stdout,
		stderr:     config.Stderr,
		runCommand: config.RunCommand,
	}, nil
}

func (i *Installer) Install(ctx context.Context) error {
	directory, err := os.MkdirTemp("", "open-actions-chart-")
	if err != nil {
		return fmt.Errorf("create temporary chart directory: %w", err)
	}
	defer os.RemoveAll(directory)

	if err := writeFiles(i.chart, directory); err != nil {
		return err
	}
	arguments := []string{
		"upgrade", "--install", "open-actions", directory,
		"--namespace", "open-actions-system", "--create-namespace",
	}
	if i.valuesFile != "" {
		arguments = append(arguments, "--values", i.valuesFile)
	}
	if err := i.runCommand(ctx, i.helm, arguments, i.stdout, i.stderr); err != nil {
		return fmt.Errorf("install Open Actions: %w", err)
	}
	return nil
}

func RunCommand(ctx context.Context, name string, arguments []string, stdout, stderr io.Writer) error {
	command := exec.CommandContext(ctx, name, arguments...)
	command.Stdout = stdout
	command.Stderr = stderr
	return command.Run()
}

func writeFiles(chart fs.FS, destination string) error {
	return fs.WalkDir(chart, ".", func(path string, entry fs.DirEntry, walkError error) error {
		if walkError != nil {
			return fmt.Errorf("read embedded chart file %q: %w", path, walkError)
		}
		if path == "." {
			return nil
		}
		target := filepath.Join(destination, filepath.FromSlash(path))
		if entry.IsDir() {
			if err := os.MkdirAll(target, 0o755); err != nil {
				return fmt.Errorf("create chart directory %q: %w", path, err)
			}
			return nil
		}
		content, err := fs.ReadFile(chart, path)
		if err != nil {
			return fmt.Errorf("read embedded chart file %q: %w", path, err)
		}
		if err := os.WriteFile(target, content, 0o644); err != nil {
			return fmt.Errorf("write embedded chart file %q: %w", path, err)
		}
		return nil
	})
}
