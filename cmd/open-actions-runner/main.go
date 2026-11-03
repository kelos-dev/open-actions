package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/kelos-dev/open-actions/internal/joblog"
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

func run(ctx context.Context, arguments []string) (runError error) {
	flags := flag.NewFlagSet("open-actions-runner", flag.ContinueOnError)
	jobFile := flags.String("job-file", "/var/run/open-actions/job.json", "Path to the workflow job plan")
	needsFile := flags.String("needs-file", "", "Path to the immutable needs context")
	eventFile := flags.String("event-file", "", "Path to the immutable GitHub event snapshot")
	resultFile := flags.String("result-file", "/dev/termination-log", "Path used to report the workflow job result")
	secretsDirectory := flags.String("secrets-directory", "", "Directory containing Project secret values")
	variablesDirectory := flags.String("variables-directory", "", "Directory containing Project variable values")
	workspace := flags.String("workspace", "/workspace", "Path to the job workspace")
	githubToken := os.Getenv(runner.GitHubTokenEnvVar)
	actionToken := os.Getenv(runner.ActionTokenEnvVar)
	artifactToken := os.Getenv(runner.ArtifactTokenEnvVar)
	logURL := os.Getenv(joblog.URLEnvVar)
	var secrets map[string]string
	var archive io.Writer
	defer func() {
		if runError == nil || archive != nil || logURL == "" {
			return
		}
		values := []string{githubToken, actionToken, artifactToken}
		for _, value := range secrets {
			values = append(values, value)
		}
		message := runner.MaskMessage(runError.Error(), values...)
		runError = errors.New(message)
		if err := archiveSetupError(ctx, logURL, artifactToken, message); err != nil {
			runError = errors.Join(runError, fmt.Errorf("archive runner setup error: %w", err))
		}
	}()
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
	secrets, err = loadValues(*secretsDirectory)
	if err != nil {
		return fmt.Errorf("load Project secrets: %w", err)
	}
	variables, err := loadValues(*variablesDirectory)
	if err != nil {
		return fmt.Errorf("load Project variables: %w", err)
	}
	runnerName := os.Getenv(runner.RunnerNameEnvVar)
	environment := withoutEnvironmentVariables(os.Environ(), runner.GitHubTokenEnvVar, runner.ActionTokenEnvVar, runner.RunnerNameEnvVar)
	var stdout io.Writer = os.Stdout
	var stderr io.Writer = os.Stderr
	var uploader *joblog.Uploader
	stopOutput := func() {}
	if logURL != "" {
		logClient, err := joblog.NewClient(logURL, &http.Client{})
		if err != nil {
			return err
		}
		retryContext := context.WithoutCancel(ctx)
		stopRetry := func() {}
		if plan.TimeoutSeconds > 0 {
			cleanup := runner.CleanupTimeout
			if plan.CleanupTimeoutSeconds > 0 {
				cleanup = time.Duration(plan.CleanupTimeoutSeconds) * time.Second
			}
			retryContext, stopRetry = context.WithDeadline(retryContext, time.Now().Add(time.Duration(plan.TimeoutSeconds)*time.Second).Add(cleanup))
		}
		defer stopRetry()
		var outputContext context.Context
		if plan.TimeoutSeconds > 0 {
			outputContext, stopOutput = context.WithTimeout(ctx, time.Duration(plan.TimeoutSeconds)*time.Second)
		} else {
			outputContext, stopOutput = context.WithCancel(ctx)
		}
		defer stopOutput()
		uploader, err = joblog.NewUploader(retryContext, outputContext, logClient, artifactToken, os.TempDir())
		if err != nil {
			return err
		}
		defer uploader.Close()
		archive = joblog.NewTimestampWriter(uploader)
		stdout = io.MultiWriter(os.Stdout, archive)
		stderr = io.MultiWriter(os.Stderr, archive)
	}
	executor, err := runner.NewExecutor(runner.ExecutorConfig{
		Logger:        slog.New(slog.NewJSONHandler(stdout, nil)),
		RunnerName:    runnerName,
		GitHubToken:   githubToken,
		ActionToken:   actionToken,
		ArtifactToken: artifactToken,
		Secrets:       secrets,
		Variables:     variables,
		Environment:   environment,
		Stdout:        stdout,
		Stderr:        stderr,
	})
	if err != nil {
		if archive != nil {
			fmt.Fprintln(archive, err)
		}
		return err
	}
	result, executionError := executor.ExecuteResult(ctx, plan, *workspace)
	stopOutput()
	if executionError != nil && archive != nil {
		fmt.Fprintln(archive, executionError)
	}
	if uploader != nil {
		if err := uploader.Close(); err != nil {
			if result.Conclusion == runner.ResultConclusionSuccess {
				result.Conclusion = runner.ResultConclusionFailure
			}
			executionError = errors.Join(executionError, err)
		}
	}
	resultData, resultError := runner.EncodeResult(result)
	if resultError == nil {
		if err := os.WriteFile(*resultFile, resultData, 0o600); err != nil {
			resultError = fmt.Errorf("write workflow job result: %w", err)
		}
	}
	return errors.Join(executionError, resultError)
}

func archiveSetupError(ctx context.Context, origin, credential, message string) error {
	if credential == "" {
		return errors.New("log upload credential is required")
	}
	client, err := joblog.NewClient(origin, &http.Client{})
	if err != nil {
		return err
	}
	var output bytes.Buffer
	if _, err := fmt.Fprintln(joblog.NewTimestampWriter(&output), message); err != nil {
		return err
	}
	data := output.Bytes()
	if len(data) > joblog.MaxChunkBytes {
		const truncated = " [diagnostic truncated]\n"
		data = append(data[:joblog.MaxChunkBytes-len(truncated)], truncated...)
	}
	uploadContext, cancel := context.WithTimeout(context.WithoutCancel(ctx), joblog.FlushTimeout)
	defer cancel()
	return client.Append(uploadContext, credential, joblog.Chunk{Data: data, Complete: true})
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
