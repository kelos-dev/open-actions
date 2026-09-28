package main

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"sort"
	"strings"
	"time"

	actionsv1alpha1 "github.com/kelos-dev/open-actions/api/v1alpha1"
	"github.com/kelos-dev/open-actions/internal/runner"
	"github.com/kelos-dev/open-actions/internal/workflowrun"
	"github.com/kelos-dev/open-actions/internal/workflowstatus"
	"github.com/spf13/cobra"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// defaultWatchInterval matches the GitHub CLI's workflow run refresh interval.
const defaultWatchInterval = 3 * time.Second

type runKubeOptions struct {
	kubeconfig        string
	defaultKubeconfig string
	context           string
}

type runCommandOptions struct {
	runKubeOptions
	namespace string
}

type runClientFactory func(runKubeOptions) (*runClients, error)

type runClients struct {
	kube             client.Client
	logs             runLogSource
	defaultNamespace string
}

type runLogSource interface {
	ListPods(context.Context, string, string) (*corev1.PodList, error)
	Stream(context.Context, string, string, bool) (io.ReadCloser, error)
}

type kubernetesRunLogSource struct {
	client kubernetes.Interface
}

func (s *kubernetesRunLogSource) ListPods(ctx context.Context, namespace, selector string) (*corev1.PodList, error) {
	return s.client.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{LabelSelector: selector})
}

func (s *kubernetesRunLogSource) Stream(ctx context.Context, namespace, name string, follow bool) (io.ReadCloser, error) {
	return s.client.CoreV1().Pods(namespace).GetLogs(name, &corev1.PodLogOptions{Container: runner.ContainerName, Follow: follow}).Stream(ctx)
}

func newRunCommand(dependencies commandDependencies) *cobra.Command {
	command := &cobra.Command{
		Use:   "run",
		Short: "Inspect and control workflow runs",
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			return command.Help()
		},
	}
	command.AddCommand(
		newRunListCommand(dependencies),
		newRunViewCommand(dependencies),
		newRunWatchCommand(dependencies),
		newRunLogsCommand(dependencies),
		newRunCancelCommand(dependencies),
		newRunRerunCommand(dependencies),
	)
	return command
}

func newRunListCommand(dependencies commandDependencies) *cobra.Command {
	options := runCommandOptions{}
	allNamespaces := false
	command := &cobra.Command{
		Use:   "list",
		Short: "List workflow runs",
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			if allNamespaces && command.Flags().Changed("namespace") {
				return fmt.Errorf("--all-namespaces and --namespace cannot be used together")
			}
			clients, namespace, err := loadRunClients(dependencies, options)
			if err != nil {
				return err
			}
			if allNamespaces {
				namespace = ""
			}
			runs := &actionsv1alpha1.WorkflowRunList{}
			listOptions := []client.ListOption{}
			if namespace != "" {
				listOptions = append(listOptions, client.InNamespace(namespace))
			}
			if err := clients.kube.List(command.Context(), runs, listOptions...); err != nil {
				return fmt.Errorf("list WorkflowRuns: %w", err)
			}
			sort.SliceStable(runs.Items, func(left, right int) bool {
				return runs.Items[left].CreationTimestamp.After(runs.Items[right].CreationTimestamp.Time)
			})
			return writeWorkflowRunList(command.OutOrStdout(), runs.Items, allNamespaces, time.Now())
		},
	}
	addRunKubeFlags(command, &options, dependencies.defaultKubeconfig)
	command.Flags().BoolVarP(&allNamespaces, "all-namespaces", "A", false, "List workflow runs across all namespaces")
	return command
}

func newRunViewCommand(dependencies commandDependencies) *cobra.Command {
	options := runCommandOptions{}
	command := &cobra.Command{
		Use:   "view RUN",
		Short: "Show a workflow run and its jobs",
		Args:  cobra.ExactArgs(1),
		RunE: func(command *cobra.Command, arguments []string) error {
			clients, namespace, err := loadRunClients(dependencies, options)
			if err != nil {
				return err
			}
			run, jobs, err := getWorkflowRun(command.Context(), clients.kube, namespace, arguments[0])
			if err != nil {
				return err
			}
			if err := writeWorkflowRun(command.OutOrStdout(), run, jobs, time.Now()); err != nil {
				return err
			}
			if len(jobs) > 0 {
				fmt.Fprintf(command.OutOrStdout(), "\nTo read runner logs, try: open-actions run logs %s --job %s --namespace %s\n", run.Name, jobs[0].Spec.JobID, namespace)
			}
			return nil
		},
	}
	addRunKubeFlags(command, &options, dependencies.defaultKubeconfig)
	return command
}

func newRunWatchCommand(dependencies commandDependencies) *cobra.Command {
	options := runCommandOptions{}
	interval := defaultWatchInterval
	exitStatus := false
	command := &cobra.Command{
		Use:   "watch RUN",
		Short: "Watch a workflow run until it completes",
		Args:  cobra.ExactArgs(1),
		RunE: func(command *cobra.Command, arguments []string) error {
			if interval <= 0 {
				return fmt.Errorf("--interval must be positive")
			}
			clients, namespace, err := loadRunClients(dependencies, options)
			if err != nil {
				return err
			}
			return watchWorkflowRun(command, clients, namespace, arguments[0], interval, exitStatus)
		},
	}
	addRunKubeFlags(command, &options, dependencies.defaultKubeconfig)
	command.Flags().DurationVarP(&interval, "interval", "i", defaultWatchInterval, "Time to wait between workflow run refreshes")
	command.Flags().BoolVar(&exitStatus, "exit-status", false, "Fail when the workflow run does not succeed")
	return command
}

func newRunCancelCommand(dependencies commandDependencies) *cobra.Command {
	options := runCommandOptions{}
	command := &cobra.Command{
		Use:   "cancel RUN",
		Short: "Request cancellation of a workflow run",
		Args:  cobra.ExactArgs(1),
		RunE: func(command *cobra.Command, arguments []string) error {
			clients, namespace, err := loadRunClients(dependencies, options)
			if err != nil {
				return err
			}
			name := arguments[0]
			alreadyRequested, completed := false, false
			err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
				alreadyRequested, completed = false, false
				run := &actionsv1alpha1.WorkflowRun{}
				if err := clients.kube.Get(command.Context(), types.NamespacedName{Namespace: namespace, Name: name}, run); err != nil {
					return err
				}
				switch {
				case run.Spec.CancelRequested:
					alreadyRequested = true
					return nil
				case workflowrun.Terminal(run):
					completed = true
					return nil
				}
				run.Spec.CancelRequested = true
				return clients.kube.Update(command.Context(), run)
			})
			if err != nil {
				return fmt.Errorf("request cancellation of WorkflowRun %q: %w", name, err)
			}
			if completed {
				return fmt.Errorf("WorkflowRun %q is already complete", name)
			}
			if alreadyRequested {
				fmt.Fprintf(command.OutOrStdout(), "%s Cancellation of WorkflowRun %q was already requested\n", markerActive, name)
				return nil
			}
			fmt.Fprintf(command.OutOrStdout(), "%s Requested cancellation of WorkflowRun %q\n", markerSucceeded, name)
			return nil
		},
	}
	addRunKubeFlags(command, &options, dependencies.defaultKubeconfig)
	return command
}

func newRunRerunCommand(dependencies commandDependencies) *cobra.Command {
	options := runCommandOptions{}
	failedJobs := false
	command := &cobra.Command{
		Use:   "rerun RUN",
		Short: "Rerun the latest attempt of a workflow run",
		Args:  cobra.ExactArgs(1),
		RunE: func(command *cobra.Command, arguments []string) error {
			clients, namespace, err := loadRunClients(dependencies, options)
			if err != nil {
				return err
			}
			name := arguments[0]
			run := &actionsv1alpha1.WorkflowRun{}
			if err := clients.kube.Get(command.Context(), types.NamespacedName{Namespace: namespace, Name: name}, run); err != nil {
				return fmt.Errorf("get WorkflowRun %q: %w", name, err)
			}
			selection := workflowrun.AllJobs
			scope := ""
			if failedJobs {
				selection = workflowrun.FailedJobs
				scope = " (failed jobs)"
			}
			rerun, err := workflowrun.CreateRerun(command.Context(), clients.kube, run, selection)
			if err != nil {
				return fmt.Errorf("rerun WorkflowRun %q: %w", name, err)
			}
			stdout := command.OutOrStdout()
			fmt.Fprintf(stdout, "%s Requested rerun%s of WorkflowRun %q\n", markerSucceeded, scope, name)
			fmt.Fprintf(stdout, "Attempt %d is WorkflowRun %q\n", rerun.Spec.Rerun.Attempt, rerun.Name)
			if jobIDs := rerun.Spec.Rerun.JobIDs; len(jobIDs) > 0 {
				fmt.Fprintf(stdout, "Selected jobs: %s\n", strings.Join(jobIDs, ", "))
			}
			return nil
		},
	}
	addRunKubeFlags(command, &options, dependencies.defaultKubeconfig)
	command.Flags().BoolVar(&failedJobs, "failed", false, "Rerun only the failed jobs and their dependents")
	return command
}

func newRunLogsCommand(dependencies commandDependencies) *cobra.Command {
	options := runCommandOptions{}
	jobName := ""
	follow := false
	command := &cobra.Command{
		Use:   "logs RUN",
		Short: "Print logs for a workflow job",
		Args:  cobra.ExactArgs(1),
		RunE: func(command *cobra.Command, arguments []string) error {
			clients, namespace, err := loadRunClients(dependencies, options)
			if err != nil {
				return err
			}
			_, jobs, err := getWorkflowRun(command.Context(), clients.kube, namespace, arguments[0])
			if err != nil {
				return err
			}
			job, err := selectWorkflowJob(jobs, jobName)
			if err != nil {
				return err
			}
			pod, err := workflowJobPod(command.Context(), clients, job, follow, command.ErrOrStderr())
			if err != nil {
				return err
			}
			stream, err := clients.logs.Stream(command.Context(), pod.Namespace, pod.Name, follow)
			if err != nil {
				return fmt.Errorf("open logs for WorkflowJob %q: %w", job.Spec.JobID, err)
			}
			defer stream.Close()
			if _, err := io.Copy(command.OutOrStdout(), stream); err != nil && command.Context().Err() == nil {
				return fmt.Errorf("read logs for WorkflowJob %q: %w", job.Spec.JobID, err)
			}
			return nil
		},
	}
	addRunKubeFlags(command, &options, dependencies.defaultKubeconfig)
	command.Flags().StringVar(&jobName, "job", "", "Workflow job ID or resource name (resource name takes precedence); optional when the run has one job")
	command.Flags().BoolVarP(&follow, "follow", "f", false, "Follow the runner log stream")
	return command
}

func addRunKubeFlags(command *cobra.Command, options *runCommandOptions, defaultKubeconfig string) {
	options.defaultKubeconfig = defaultKubeconfig
	command.Flags().StringVar(&options.kubeconfig, "kubeconfig", "", "Path to a kubeconfig file")
	command.Flags().StringVar(&options.context, "context", "", "Kubeconfig context to use")
	command.Flags().StringVarP(&options.namespace, "namespace", "n", "", "Namespace containing Open Actions resources")
}

func loadRunClients(dependencies commandDependencies, options runCommandOptions) (*runClients, string, error) {
	clients, err := dependencies.newRunClients(options.runKubeOptions)
	if err != nil {
		return nil, "", err
	}
	namespace := options.namespace
	if namespace == "" {
		namespace = clients.defaultNamespace
	}
	if namespace == "" {
		namespace = metav1.NamespaceDefault
	}
	return clients, namespace, nil
}

func newKubernetesRunClients(options runKubeOptions) (*runClients, error) {
	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	switch {
	case options.kubeconfig != "":
		loadingRules.ExplicitPath = options.kubeconfig
	case options.defaultKubeconfig != "":
		loadingRules.Precedence = filepath.SplitList(options.defaultKubeconfig)
	}
	overrides := &clientcmd.ConfigOverrides{CurrentContext: options.context}
	configurationLoader := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, overrides)
	namespace, _, err := configurationLoader.Namespace()
	if err != nil {
		return nil, fmt.Errorf("load Kubernetes namespace: %w", err)
	}
	configuration, err := configurationLoader.ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("load Kubernetes configuration: %w", err)
	}
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		return nil, fmt.Errorf("add Kubernetes types to scheme: %w", err)
	}
	if err := actionsv1alpha1.AddToScheme(scheme); err != nil {
		return nil, fmt.Errorf("add actions API to scheme: %w", err)
	}
	kube, err := client.New(configuration, client.Options{Scheme: scheme})
	if err != nil {
		return nil, fmt.Errorf("create Kubernetes client: %w", err)
	}
	clientset, err := kubernetes.NewForConfig(configuration)
	if err != nil {
		return nil, fmt.Errorf("create Kubernetes clientset: %w", err)
	}
	return &runClients{
		kube:             kube,
		logs:             &kubernetesRunLogSource{client: clientset},
		defaultNamespace: namespace,
	}, nil
}

// watchWorkflowRun refreshes a workflow run until it completes. Interactive
// output redraws in place; redirected output appends each refresh.
func watchWorkflowRun(command *cobra.Command, clients *runClients, namespace, name string, interval time.Duration, exitStatus bool) error {
	stdout := command.OutOrStdout()
	redraw := terminalWriter(stdout)
	announced := false
	for refreshes := 0; ; refreshes++ {
		run, jobs, err := getWorkflowRun(command.Context(), clients.kube, namespace, name)
		if err != nil {
			return err
		}
		switch {
		case redraw:
			fmt.Fprint(stdout, clearScreen)
		case refreshes > 0:
			fmt.Fprintln(stdout)
		}
		if err := writeWorkflowRun(stdout, run, jobs, time.Now()); err != nil {
			return err
		}
		if workflowrun.Terminal(run) {
			status := workflowstatus.Run(run)
			if exitStatus && status != workflowstatus.Succeeded {
				return fmt.Errorf("WorkflowRun %q completed with status %s", name, status)
			}
			fmt.Fprintf(stdout, "\n%s WorkflowRun %q completed with status %s\n", statusMarker(status), name, status)
			return nil
		}
		if !announced {
			fmt.Fprintf(command.ErrOrStderr(), "Refreshing WorkflowRun %q every %s; press Ctrl+C to stop\n", name, interval)
			announced = true
		}
		select {
		case <-command.Context().Done():
			return command.Context().Err()
		case <-time.After(interval):
		}
	}
}

func getWorkflowRun(ctx context.Context, reader client.Reader, namespace, name string) (*actionsv1alpha1.WorkflowRun, []actionsv1alpha1.WorkflowJob, error) {
	run := &actionsv1alpha1.WorkflowRun{}
	key := types.NamespacedName{Namespace: namespace, Name: name}
	if err := reader.Get(ctx, key, run); err != nil {
		return nil, nil, fmt.Errorf("get WorkflowRun %q: %w", name, err)
	}
	workflowJobs := &actionsv1alpha1.WorkflowJobList{}
	if err := reader.List(ctx, workflowJobs, client.InNamespace(namespace), client.MatchingLabels{
		actionsv1alpha1.LabelWorkflowRunUID: string(run.UID),
	}); err != nil {
		return nil, nil, fmt.Errorf("list WorkflowJobs for WorkflowRun %q: %w", name, err)
	}
	jobs := make([]actionsv1alpha1.WorkflowJob, 0, len(workflowJobs.Items))
	for index := range workflowJobs.Items {
		job := &workflowJobs.Items[index]
		if metav1.IsControlledBy(job, run) {
			jobs = append(jobs, *job)
		}
	}
	sort.SliceStable(jobs, func(left, right int) bool {
		if jobs[left].Spec.JobID == jobs[right].Spec.JobID {
			return jobs[left].Name < jobs[right].Name
		}
		return jobs[left].Spec.JobID < jobs[right].Spec.JobID
	})
	return run, jobs, nil
}

func selectWorkflowJob(jobs []actionsv1alpha1.WorkflowJob, selector string) (*actionsv1alpha1.WorkflowJob, error) {
	if selector == "" {
		switch len(jobs) {
		case 0:
			return nil, fmt.Errorf("WorkflowRun has no jobs")
		case 1:
			return &jobs[0], nil
		default:
			return nil, fmt.Errorf("--job is required because WorkflowRun has %d jobs", len(jobs))
		}
	}
	for index := range jobs {
		if jobs[index].Name == selector {
			return &jobs[index], nil
		}
	}
	matches := make([]*actionsv1alpha1.WorkflowJob, 0, 1)
	for index := range jobs {
		if jobs[index].Spec.JobID == selector {
			matches = append(matches, &jobs[index])
		}
	}
	switch len(matches) {
	case 0:
		return nil, fmt.Errorf("WorkflowRun has no job %q", selector)
	case 1:
		return matches[0], nil
	default:
		return nil, fmt.Errorf("job ID %q matches multiple WorkflowJobs; use a WorkflowJob resource name", selector)
	}
}

func workflowJobPod(ctx context.Context, clients *runClients, job *actionsv1alpha1.WorkflowJob, follow bool, stderr io.Writer) (*corev1.Pod, error) {
	selector := labels.Set{actionsv1alpha1.LabelWorkflowJobUID: string(job.UID)}.String()
	waitingMessageWritten := false
	for {
		pods, err := clients.logs.ListPods(ctx, job.Namespace, selector)
		if err != nil {
			return nil, fmt.Errorf("list runner Pods for WorkflowJob %q: %w", job.Spec.JobID, err)
		}
		if len(pods.Items) > 0 {
			sort.SliceStable(pods.Items, func(left, right int) bool {
				return pods.Items[left].CreationTimestamp.Before(&pods.Items[right].CreationTimestamp)
			})
			return &pods.Items[0], nil
		}
		if workflowstatus.JobTerminal(job) {
			return nil, fmt.Errorf("logs for WorkflowJob %q are no longer available", job.Spec.JobID)
		}
		if !follow {
			return nil, fmt.Errorf("logs for WorkflowJob %q are not available yet; use --follow to wait", job.Spec.JobID)
		}
		if !waitingMessageWritten {
			fmt.Fprintf(stderr, "Waiting for runner logs for WorkflowJob %q...\n", job.Spec.JobID)
			waitingMessageWritten = true
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Second):
		}
		current := &actionsv1alpha1.WorkflowJob{}
		if err := clients.kube.Get(ctx, client.ObjectKeyFromObject(job), current); err != nil {
			return nil, fmt.Errorf("refresh WorkflowJob %q: %w", job.Name, err)
		}
		job = current
	}
}
