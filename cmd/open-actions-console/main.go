package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	actionsv1alpha1 "github.com/kelos-dev/open-actions/api/v1alpha1"
	"github.com/kelos-dev/open-actions/internal/console"
	githubclient "github.com/kelos-dev/open-actions/internal/github"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func main() {
	if err := runConsole(ctrl.SetupSignalHandler(), os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func runConsole(ctx context.Context, arguments []string) error {
	flags := flag.NewFlagSet("open-actions-console", flag.ContinueOnError)
	bindAddress := flags.String("bind-address", ":8080", "Address used by the Console HTTP server")
	githubAPIURL := flags.String("github-api-url", "https://api.github.com/", "Base URL for the GitHub API")
	tokenFile := flags.String("token-file", "", "File containing the Console administrator token")
	secretManagementNamespace := flags.String("secret-management-namespace", "", "Namespace in which Console administrators may manage Project Secrets")
	var workflowRunTTLSecondsAfterFinished *int32
	flags.Func("workflow-run-ttl-seconds-after-finished", "Default spec.ttlSecondsAfterFinished for Console-generated WorkflowRuns; omit the flag to retain them indefinitely", func(value string) error {
		seconds, err := strconv.ParseInt(value, 10, 32)
		if err != nil || seconds < 0 {
			return fmt.Errorf("must be an integer between 0 and %d", int64(1<<31-1))
		}
		parsed := int32(seconds)
		workflowRunTTLSecondsAfterFinished = &parsed
		return nil
	})
	secureCookie := flags.Bool("secure-cookie", false, "Restrict the Console session cookie to HTTPS")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	normalizedGitHubAPIURL, err := githubclient.NormalizeAPIURL(*githubAPIURL)
	if err != nil {
		return err
	}
	token, err := readToken(*tokenFile)
	if err != nil {
		return err
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		return fmt.Errorf("add Kubernetes types to scheme: %w", err)
	}
	if err := actionsv1alpha1.AddToScheme(scheme); err != nil {
		return fmt.Errorf("add actions API to scheme: %w", err)
	}
	configuration, err := ctrl.GetConfig()
	if err != nil {
		return fmt.Errorf("load Kubernetes configuration: %w", err)
	}
	kubernetesClient, err := client.New(configuration, client.Options{Scheme: scheme})
	if err != nil {
		return fmt.Errorf("create Kubernetes client: %w", err)
	}
	clientset, err := kubernetes.NewForConfig(configuration)
	if err != nil {
		return fmt.Errorf("create Kubernetes clientset: %w", err)
	}
	github, err := githubclient.NewClient(normalizedGitHubAPIURL, nil)
	if err != nil {
		return err
	}
	repositories, err := console.NewGitHubRepositoryResolver(kubernetesClient, github)
	if err != nil {
		return fmt.Errorf("configure GitHub repository resolver: %w", err)
	}
	handler, err := console.New(console.Config{
		Client: kubernetesClient, Logs: console.NewKubernetesLogSource(clientset), Repositories: repositories,
		Token: token, SecretManagementNamespace: *secretManagementNamespace, WorkflowRunTTLSecondsAfterFinished: workflowRunTTLSecondsAfterFinished,
		SecureCookie: *secureCookie, Logger: logger,
	})
	if err != nil {
		return fmt.Errorf("configure Console: %w", err)
	}

	mux := http.NewServeMux()
	mux.Handle("/runs/", handler)
	mux.Handle("/login", handler)
	mux.Handle("/api/login", handler)
	mux.HandleFunc("/healthz", healthy)
	mux.HandleFunc("/readyz", healthy)
	mux.Handle("/", handler)
	server := &http.Server{
		Addr:              *bindAddress,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	listener, err := net.Listen("tcp", server.Addr)
	if err != nil {
		return err
	}
	go func() {
		<-ctx.Done()
		shutdownContext, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownContext)
	}()
	logger.Info("starting Open Actions Console", "address", listener.Addr().String())
	if err := server.Serve(listener); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

func readToken(path string) (string, error) {
	if path == "" {
		return "", fmt.Errorf("Console token file is required")
	}
	value, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read Console token: %w", err)
	}
	token := strings.TrimSpace(string(value))
	if token == "" {
		return "", fmt.Errorf("Console token is empty")
	}
	return token, nil
}

func healthy(writer http.ResponseWriter, _ *http.Request) {
	writer.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = writer.Write([]byte("ok\n"))
}
