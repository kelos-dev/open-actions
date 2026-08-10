package main

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/kelos-dev/open-actions/internal/installer"
	"github.com/kelos-dev/open-actions/internal/manifests"
	"github.com/spf13/cobra"
	"k8s.io/client-go/tools/clientcmd"
)

func main() {
	if err := run(context.Background(), os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

type commandDependencies struct {
	defaultKubeconfig string
	newRunClients     runClientFactory
}

func run(ctx context.Context, arguments []string, stdout, stderr io.Writer) error {
	return runWithDependencies(ctx, arguments, stdout, stderr, commandDependencies{
		defaultKubeconfig: os.Getenv(clientcmd.RecommendedConfigPathEnvVar),
		newRunClients:     newKubernetesRunClients,
	})
}

func runWithDependencies(ctx context.Context, arguments []string, stdout, stderr io.Writer, dependencies commandDependencies) error {
	root := &cobra.Command{
		Use:           "open-actions",
		Short:         "Open Actions command-line interface",
		SilenceErrors: true,
		SilenceUsage:  true,
		Args:          cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			return command.Help()
		},
	}
	root.CompletionOptions.DisableDefaultCmd = true
	root.AddCommand(newInstallCommand(), newRunCommand(dependencies))
	root.SetArgs(arguments)
	root.SetOut(stdout)
	root.SetErr(stderr)
	return root.ExecuteContext(ctx)
}

func newInstallCommand() *cobra.Command {
	var valuesFile string
	command := &cobra.Command{
		Use:   "install",
		Short: "Install or upgrade Open Actions in the current Kubernetes cluster with Helm",
		Args: func(_ *cobra.Command, arguments []string) error {
			if len(arguments) != 0 {
				return fmt.Errorf("open-actions install does not accept arguments")
			}
			return nil
		},
		RunE: func(command *cobra.Command, _ []string) error {
			deploymentInstaller, err := installer.New(installer.Config{
				Chart:      manifests.Chart(),
				Helm:       "helm",
				ValuesFile: valuesFile,
				Stdout:     command.OutOrStdout(),
				Stderr:     command.ErrOrStderr(),
				RunCommand: installer.RunCommand,
			})
			if err != nil {
				return err
			}
			return deploymentInstaller.Install(command.Context())
		},
	}
	command.Flags().StringVar(&valuesFile, "values", "", "Path to a Helm values file")
	return command
}
