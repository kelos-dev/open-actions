package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/kelos-dev/open-actions/internal/artifact"
	"github.com/kelos-dev/open-actions/internal/cache"
	"github.com/kelos-dev/open-actions/internal/endpointurl"
	"github.com/kelos-dev/open-actions/internal/joblog"
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
	logReadKeyFile := flags.String("log-read-key-file", "", "File containing the dedicated log read signing key")
	maxFileBytes := flags.Int64("max-file-bytes", 1<<30, "Maximum uncompressed bytes in one artifact file")
	maxArtifactBytes := flags.Int64("max-artifact-bytes", 2<<30, "Maximum stored and uncompressed bytes in one artifact")
	maxRunBytes := flags.Int64("max-run-bytes", 10<<30, "Maximum stored artifact bytes in one workflow run attempt")
	defaultRetentionDays := flags.Int("default-retention-days", 7, "Default artifact retention in days")
	maxRetentionDays := flags.Int("max-retention-days", 30, "Maximum artifact retention in days")
	cacheEnabled := flags.Bool("cache-enabled", true, "Enable the workflow cache service")
	cacheMaxEntryBytes := flags.Int64("cache-max-entry-bytes", 10<<30, "Maximum bytes in one cache entry")
	cacheMaxRepositoryBytes := flags.Int64("cache-max-repository-bytes", 10<<30, "Maximum cache bytes per Project and repository")
	cacheMaxTotalBytes := flags.Int64("cache-max-total-bytes", 10<<30, "Maximum cache bytes across all Projects")
	cacheRetentionDays := flags.Int("cache-retention-days", 7, "Expire caches after this many days without access")
	logRetentionDays := flags.Int("log-retention-days", 7, "Job log retention in days after the last upload")
	maxLogBytes := flags.Int64("max-log-bytes", 1<<30, "Maximum archived bytes per job")
	maxLogChunks := flags.Int64("max-log-chunks", joblog.DefaultMaxChunks, "Maximum archived chunk files per job")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	if *publicURL == "" || *storageDirectory == "" || *signingKeyFile == "" || *logReadKeyFile == "" {
		return errors.New("public-url, storage-directory, signing-key-file, and log-read-key-file are required")
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
	cacheLimits := cache.DefaultLimits()
	cacheLimits.MaxEntryBytes = *cacheMaxEntryBytes
	cacheLimits.MaxRepositoryBytes = *cacheMaxRepositoryBytes
	cacheLimits.MaxTotalBytes = *cacheMaxTotalBytes
	if *cacheRetentionDays < 1 || *cacheRetentionDays > 3650 {
		return errors.New("cache retention must be between 1 and 3650 days")
	}
	cacheLimits.Retention = time.Duration(*cacheRetentionDays) * 24 * time.Hour
	if err := cacheLimits.Validate(); err != nil {
		return err
	}
	if *logRetentionDays < 1 || *logRetentionDays > 3650 || *maxLogBytes < 1 {
		return errors.New("log retention must be between 1 and 3650 days and the byte limit must be positive")
	}
	if *maxLogChunks < 1 {
		return errors.New("log chunk limit must be positive")
	}
	signingKey, err := os.ReadFile(*signingKeyFile)
	if err != nil {
		return fmt.Errorf("read artifact signing key: %w", err)
	}
	tokens, err := artifact.NewTokenCodec(signingKey)
	if err != nil {
		return err
	}
	readKey, err := os.ReadFile(*logReadKeyFile)
	if err != nil {
		return fmt.Errorf("read log signing key: %w", err)
	}
	if bytes.Equal(readKey, signingKey) {
		return errors.New("log read signing key must differ from artifact signing key")
	}
	logReads, err := joblog.NewReadTokenCodec(readKey)
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
	var cacheStore *cache.Store
	if *cacheEnabled {
		cacheStore, err = cache.NewStore(filepath.Join(*storageDirectory, "cache"), cacheLimits)
		if err != nil {
			return fmt.Errorf("open cache storage: %w", err)
		}
	}
	logs, err := joblog.NewStore(filepath.Join(*storageDirectory, "logs"), time.Duration(*logRetentionDays)*24*time.Hour, *maxLogBytes, *maxLogChunks)
	if err != nil {
		return err
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	server, err := artifact.NewServer(artifact.ServerConfig{
		Address: *bindAddress, PublicURL: normalizedPublicURL, Store: store, CacheStore: cacheStore, Logs: logs, LogReads: logReads, Tokens: tokens, Logger: logger,
	})
	if err != nil {
		return err
	}
	return server.Start(ctx)
}
