package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/kelos-dev/open-actions/internal/artifact"
	"github.com/kelos-dev/open-actions/internal/endpointurl"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := runArtifactServer(ctx, os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func runArtifactServer(ctx context.Context, arguments []string) error {
	flags := flag.NewFlagSet("open-actions-artifact-server", flag.ContinueOnError)
	bindAddress := flags.String("bind-address", ":8080", "Address used by the artifact HTTP server")
	publicURL := flags.String("public-url", "", "Artifact service origin exposed to workflow jobs")
	storageDirectory := flags.String("storage-directory", "", "Directory used for artifact storage")
	signingKeyFile := flags.String("signing-key-file", "", "File containing the artifact credential signing key")
	maxFileBytes := flags.Int64("max-file-bytes", 1<<30, "Maximum uncompressed bytes in one artifact file")
	maxArtifactBytes := flags.Int64("max-artifact-bytes", 2<<30, "Maximum stored and uncompressed bytes in one artifact")
	maxRunBytes := flags.Int64("max-run-bytes", 10<<30, "Maximum stored artifact bytes in one workflow run attempt")
	defaultRetentionDays := flags.Int("default-retention-days", 7, "Default artifact retention in days")
	maxRetentionDays := flags.Int("max-retention-days", 30, "Maximum artifact retention in days")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	if *publicURL == "" || *storageDirectory == "" || *signingKeyFile == "" {
		return errors.New("public-url, storage-directory, and signing-key-file are required")
	}
	normalizedPublicURL, err := endpointurl.NormalizeOrigin(*publicURL, "Artifact service URL")
	if err != nil {
		return err
	}
	if *maxFileBytes < 1 || *maxArtifactBytes < *maxFileBytes || *maxRunBytes < *maxArtifactBytes {
		return errors.New("artifact byte limits must be positive and ordered by file, artifact, and run")
	}
	if *defaultRetentionDays < 1 || *maxRetentionDays < *defaultRetentionDays || *maxRetentionDays > 3650 {
		return errors.New("artifact retention must be between 1 and 3650 days and the default must not exceed the maximum")
	}
	signingKey, err := os.ReadFile(*signingKeyFile)
	if err != nil {
		return fmt.Errorf("read artifact signing key: %w", err)
	}
	tokens, err := artifact.NewTokenCodec(signingKey)
	if err != nil {
		return err
	}
	limits := artifact.DefaultLimits()
	limits.MaxFileBytes = *maxFileBytes
	limits.MaxArtifactBytes = *maxArtifactBytes
	limits.MaxRunBytes = *maxRunBytes
	limits.DefaultRetention = time.Duration(*defaultRetentionDays) * 24 * time.Hour
	limits.MaxRetention = time.Duration(*maxRetentionDays) * 24 * time.Hour
	store, err := artifact.NewStore(*storageDirectory, limits)
	if err != nil {
		return err
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	server, err := artifact.NewServer(artifact.ServerConfig{
		Address: *bindAddress, PublicURL: normalizedPublicURL, Store: store, Tokens: tokens, Logger: logger,
	})
	if err != nil {
		return err
	}
	return server.Start(ctx)
}
