package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/kelos-dev/open-actions/internal/runner"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, arguments []string) error {
	flags := flag.NewFlagSet("open-actions-runner", flag.ContinueOnError)
	jobFile := flags.String("job-file", "/var/run/open-actions/job.json", "Path to the workflow job plan")
	needsFile := flags.String("needs-file", "", "Path to the immutable needs context")
	eventFile := flags.String("event-file", "", "Path to the immutable GitHub event snapshot")
	resultFile := flags.String("result-file", "/dev/termination-log", "Path used to report the workflow job result")
	secretsDirectory := flags.String("secrets-directory", "", "Directory containing Project secret values")
	variablesDirectory := flags.String("variables-directory", "", "Directory containing Project variable values")
	workspace := flags.String("workspace", "/workspace", "Path to the job workspace")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	plan, err := runner.LoadPlan(*jobFile)
	if err != nil {
		return err
	}
	if *needsFile != "" {
		if err := runner.LoadNeedsContext(plan, *needsFile); err != nil {
			return err
		}
	}
	if *eventFile != "" {
		if err := runner.LoadEventSnapshot(plan, *eventFile); err != nil {
			return err
		}
	}
	secrets, err := loadValues(*secretsDirectory)
	if err != nil {
		return fmt.Errorf("load Project secrets: %w", err)
	}
	variables, err := loadValues(*variablesDirectory)
	if err != nil {
		return fmt.Errorf("load Project variables: %w", err)
	}
	githubToken := os.Getenv(runner.GitHubTokenEnvVar)
	actionToken := os.Getenv(runner.ActionTokenEnvVar)
	artifactToken := os.Getenv(runner.ArtifactTokenEnvVar)
	environment := withoutEnvironmentVariables(os.Environ(), runner.GitHubTokenEnvVar, runner.ActionTokenEnvVar)
	executor, err := runner.NewExecutor(runner.ExecutorConfig{
		Logger:        slog.New(slog.NewJSONHandler(os.Stdout, nil)),
		GitHubToken:   githubToken,
		ActionToken:   actionToken,
		ArtifactToken: artifactToken,
		Secrets:       secrets,
		Variables:     variables,
		Environment:   environment,
		Stdout:        os.Stdout,
		Stderr:        os.Stderr,
	})
	if err != nil {
		return err
	}
	result, executionError := executor.ExecuteResult(ctx, plan, *workspace)
	resultData, resultError := runner.EncodeResult(result)
	if resultError == nil {
		if err := os.WriteFile(*resultFile, resultData, 0o600); err != nil {
			resultError = fmt.Errorf("write workflow job result: %w", err)
		}
	}
	return errors.Join(executionError, resultError)
}

func loadValues(directory string) (map[string]string, error) {
	if directory == "" {
		return nil, nil
	}
	entries, err := os.ReadDir(directory)
	if err != nil {
		return nil, err
	}
	values := make(map[string]string, len(entries))
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".") {
			continue
		}
		if entry.IsDir() {
			return nil, fmt.Errorf("value path %q is a directory", entry.Name())
		}
		value, err := os.ReadFile(filepath.Join(directory, entry.Name()))
		if err != nil {
			return nil, fmt.Errorf("read value %q: %w", entry.Name(), err)
		}
		values[entry.Name()] = string(value)
	}
	return values, nil
}

func withoutEnvironmentVariables(environment []string, names ...string) []string {
	prefixes := make([]string, len(names))
	for index, name := range names {
		prefixes[index] = name + "="
	}
	result := make([]string, 0, len(environment))
	for _, entry := range environment {
		include := true
		for _, prefix := range prefixes {
			if strings.HasPrefix(entry, prefix) {
				include = false
				break
			}
		}
		if include {
			result = append(result, entry)
		}
	}
	return result
}
