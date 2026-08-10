package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"sync/atomic"
	"time"

	actionsv1alpha1 "github.com/kelos-dev/open-actions/api/v1alpha1"
	"github.com/kelos-dev/open-actions/internal/controller"
	"github.com/kelos-dev/open-actions/internal/endpointurl"
	githubclient "github.com/kelos-dev/open-actions/internal/github"
	githubwebhook "github.com/kelos-dev/open-actions/internal/webhook"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

type webhookServer struct {
	server *http.Server
	logger *slog.Logger
	ready  atomic.Bool
}

func (s *webhookServer) Start(ctx context.Context) error {
	listener, err := net.Listen("tcp", s.server.Addr)
	if err != nil {
		return err
	}
	s.ready.Store(true)
	defer s.ready.Store(false)
	go func() {
		<-ctx.Done()
		shutdownContext, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = s.server.Shutdown(shutdownContext)
	}()
	s.logger.Info("starting GitHub webhook server", "address", listener.Addr().String())
	if err := s.server.Serve(listener); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

func (s *webhookServer) NeedLeaderElection() bool {
	return false
}

func (s *webhookServer) Readiness(_ *http.Request) error {
	if !s.ready.Load() {
		return fmt.Errorf("GitHub webhook server is not ready")
	}
	return nil
}

func main() {
	if err := runManager(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func runManager(arguments []string) error {
	flags := flag.NewFlagSet("open-actions-controller", flag.ContinueOnError)
	metricsAddress := flags.String("metrics-bind-address", ":8082", "Address used by the metrics endpoint")
	healthAddress := flags.String("health-probe-bind-address", ":8081", "Address used by health probes")
	webhookAddress := flags.String("webhook-bind-address", ":8080", "Address used by the GitHub webhook endpoint")
	githubAPIURL := flags.String("github-api-url", "https://api.github.com/", "Base URL for the GitHub API")
	githubServerURL := flags.String("github-server-url", "https://github.com", "GitHub web-server URL exposed to workflows")
	actionCloneBaseURL := flags.String("action-clone-base-url", "https://github.com", "Base URL used to clone external action repositories")
	consoleURL := flags.String("console-url", "", "Public URL for the Open Actions Console")
	leaderElection := flags.Bool("leader-elect", false, "Use leader election for the controller manager")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	normalizedGitHubAPIURL, err := githubclient.NormalizeAPIURL(*githubAPIURL)
	if err != nil {
		return err
	}
	normalizedGitHubServerURL, err := githubclient.NormalizeServerURL(*githubServerURL)
	if err != nil {
		return err
	}
	normalizedActionCloneBaseURL, err := githubclient.NormalizeActionCloneBaseURL(*actionCloneBaseURL)
	if err != nil {
		return err
	}
	normalizedConsoleURL := ""
	if *consoleURL != "" {
		normalizedConsoleURL, err = endpointurl.NormalizeOrigin(*consoleURL, "Console URL")
		if err != nil {
			return err
		}
	}

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&zap.Options{Development: false})))
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		return fmt.Errorf("add Kubernetes types to scheme: %w", err)
	}
	if err := actionsv1alpha1.AddToScheme(scheme); err != nil {
		return fmt.Errorf("add actions API to scheme: %w", err)
	}
	github, err := githubclient.NewClient(normalizedGitHubAPIURL, nil)
	if err != nil {
		return err
	}
	configuration, err := ctrl.GetConfig()
	if err != nil {
		return fmt.Errorf("load Kubernetes configuration: %w", err)
	}
	controllerManager, err := ctrl.NewManager(configuration, ctrl.Options{
		Scheme: scheme,
		Client: client.Options{Cache: &client.CacheOptions{
			DisableFor: []client.Object{&corev1.Secret{}},
		}},
		Metrics:                metricsserver.Options{BindAddress: *metricsAddress},
		HealthProbeBindAddress: *healthAddress,
		LeaderElection:         *leaderElection,
		LeaderElectionID:       "open-actions.actions.kelos.dev",
	})
	if err != nil {
		return fmt.Errorf("create controller manager: %w", err)
	}
	if err := (&controller.ProjectReconciler{Client: controllerManager.GetClient(), APIReader: controllerManager.GetAPIReader()}).SetupWithManager(controllerManager); err != nil {
		return fmt.Errorf("configure Project controller: %w", err)
	}
	if err := (&controller.WorkflowRunReconciler{
		Client:             controllerManager.GetClient(),
		APIReader:          controllerManager.GetAPIReader(),
		GitHub:             github,
		GitHubAPIBase:      normalizedGitHubAPIURL,
		GitHubServerURL:    normalizedGitHubServerURL,
		ActionCloneBaseURL: normalizedActionCloneBaseURL,
		ConsoleURL:         normalizedConsoleURL,
	}).SetupWithManager(controllerManager); err != nil {
		return fmt.Errorf("configure WorkflowRun controller: %w", err)
	}
	if err := (&controller.RunnerReconciler{
		Client:    controllerManager.GetClient(),
		APIReader: controllerManager.GetAPIReader(),
		GitHub:    github,
	}).SetupWithManager(controllerManager); err != nil {
		return fmt.Errorf("configure Runner controller: %w", err)
	}
	if err := (&githubwebhook.DeliveryReconciler{
		Client:    controllerManager.GetClient(),
		APIReader: controllerManager.GetAPIReader(),
		GitHub:    github,
		Logger:    logger,
	}).SetupWithManager(controllerManager); err != nil {
		return fmt.Errorf("configure webhook delivery controller: %w", err)
	}
	handler := &githubwebhook.GitHubHandler{
		Client:    controllerManager.GetClient(),
		APIReader: controllerManager.GetAPIReader(),
		Logger:    logger,
	}
	webhookRunnable := &webhookServer{server: &http.Server{
		Addr:              *webhookAddress,
		Handler:           http.TimeoutHandler(handler, 25*time.Second, "webhook processing timed out"),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}, logger: logger}
	if err := controllerManager.Add(webhookRunnable); err != nil {
		return fmt.Errorf("register webhook server: %w", err)
	}
	if err := controllerManager.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		return err
	}
	if err := controllerManager.AddReadyzCheck("webhook", webhookRunnable.Readiness); err != nil {
		return err
	}
	logger.Info("starting Open Actions controller")
	return controllerManager.Start(ctrl.SetupSignalHandler())
}
