package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	actionsv1alpha1 "github.com/kelos-dev/open-actions/api/v1alpha1"
	"github.com/kelos-dev/open-actions/internal/console"
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
	tokenFile := flags.String("token-file", "", "File containing the Console administrator token")
	secureCookie := flags.Bool("secure-cookie", false, "Restrict the Console session cookie to HTTPS")
	if err := flags.Parse(arguments); err != nil {
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
	handler, err := console.New(console.Config{
		Client: kubernetesClient, Logs: console.NewKubernetesLogSource(clientset),
		Token: token, SecureCookie: *secureCookie, Logger: logger,
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
