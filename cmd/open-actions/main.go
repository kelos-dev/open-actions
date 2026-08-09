package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/kelos-dev/open-actions/internal/installer"
	"github.com/kelos-dev/open-actions/internal/manifests"
)

func main() {
	if err := run(context.Background(), os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, arguments []string, stdout, stderr io.Writer) error {
	if len(arguments) == 0 {
		writeUsage(stdout)
		return nil
	}
	switch arguments[0] {
	case "help", "-h", "--help":
		writeUsage(stdout)
		return nil
	case "install":
		return runInstall(ctx, arguments[1:], stdout, stderr)
	default:
		return fmt.Errorf("unknown command %q; run 'open-actions help' for usage", arguments[0])
	}
}

func runInstall(ctx context.Context, arguments []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("open-actions install", flag.ContinueOnError)
	flags.SetOutput(stderr)
	valuesFile := flags.String("values", "", "Path to a Helm values file")
	flags.Usage = func() {
		fmt.Fprintln(stderr, "Usage: open-actions install [--values FILE]")
		flags.PrintDefaults()
	}
	if err := flags.Parse(arguments); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("open-actions install does not accept arguments")
	}
	deploymentInstaller, err := installer.New(installer.Config{
		Chart:      manifests.Chart(),
		Helm:       "helm",
		ValuesFile: *valuesFile,
		Stdout:     stdout,
		Stderr:     stderr,
		RunCommand: installer.RunCommand,
	})
	if err != nil {
		return err
	}
	return deploymentInstaller.Install(ctx)
}

func writeUsage(writer io.Writer) {
	fmt.Fprintln(writer, `Open Actions command-line interface

Usage:
  open-actions <command>

Commands:
  install    Install Open Actions in the current Kubernetes cluster with Helm`)
}
