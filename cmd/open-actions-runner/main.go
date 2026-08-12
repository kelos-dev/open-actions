package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
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
	secretsFile := flags.String("secrets-file", "/var/run/open-actions-credentials/secrets.json", "Path to the environment-scoped workflow job secrets")
	resultFile := flags.String("result-file", "/dev/termination-log", "Path used to report the workflow job result")
	workspace := flags.String("workspace", "/workspace", "Path to the job workspace")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	plan, err := runner.LoadPlan(*jobFile)
	if err != nil {
		return err
	}
	secrets, err := runner.LoadSecrets(*secretsFile)
	if err != nil {
		if plan.Version < runner.PlanVersion && errors.Is(err, os.ErrNotExist) {
			secrets = map[string]string{}
		} else {
			return err
		}
	}
	githubToken := os.Getenv("OPEN_ACTIONS_GITHUB_TOKEN")
	executor, err := runner.NewExecutor(runner.ExecutorConfig{
		Logger:      slog.New(slog.NewJSONHandler(os.Stdout, nil)),
		GitHubToken: githubToken,
		Secrets:     secrets,
		Environment: withoutEnvironmentVariable(os.Environ(), "OPEN_ACTIONS_GITHUB_TOKEN"),
		Stdout:      os.Stdout,
		Stderr:      os.Stderr,
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

func withoutEnvironmentVariable(environment []string, name string) []string {
	prefix := name + "="
	result := make([]string, 0, len(environment))
	for _, entry := range environment {
		if !strings.HasPrefix(entry, prefix) {
			result = append(result, entry)
		}
	}
	return result
}
