package main

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"path"
	"regexp"
	"strings"
	"unicode/utf8"

	actionsv1alpha1 "github.com/kelos-dev/open-actions/api/v1alpha1"
	githubclient "github.com/kelos-dev/open-actions/internal/github"
	"github.com/kelos-dev/open-actions/internal/workflow"
	"github.com/spf13/cobra"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var (
	runRepositoryPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]{0,99}/[A-Za-z0-9._-]{1,100}$`)
	runInputNamePattern  = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_-]{0,99}$`)
	runCommitPattern     = regexp.MustCompile(`^[0-9a-f]{40}$`)
	runInvalidRefPattern = regexp.MustCompile(`[\x00-\x20\x7f~^:?*\[\\]`)
)

func newRunCreateCommand(dependencies commandDependencies) *cobra.Command {
	options := runCommandOptions{}
	var projectName, repository, ref, name, hostname string
	var fields []string
	command := &cobra.Command{
		Use:   "create WORKFLOW",
		Short: "Create a manual workflow run",
		Long:  "Create a workflow_dispatch run from a repository-relative workflow path. Requires an authenticated GitHub CLI (gh) account with repository write access.",
		Args:  cobra.ExactArgs(1),
		RunE: func(command *cobra.Command, arguments []string) error {
			workflowPath := arguments[0]
			if err := validateRunCreate(projectName, repository, workflowPath, ref, name); err != nil {
				return err
			}
			inputs, err := runCreateInputs(fields)
			if err != nil {
				return err
			}
			github, err := newRunGitHub(hostname, dependencies.runCommand)
			if err != nil {
				return err
			}
			clients, namespace, err := loadRunClients(dependencies, options)
			if err != nil {
				return err
			}
			ctx := command.Context()
			project := &actionsv1alpha1.Project{}
			if err := clients.reader.Get(ctx, client.ObjectKey{Namespace: namespace, Name: projectName}, project); err != nil {
				return fmt.Errorf("get Project %q: %w", projectName, err)
			}
			configured := meta.FindStatusCondition(project.Status.Conditions, actionsv1alpha1.ProjectConditionConfigured)
			if configured == nil || configured.Status != metav1.ConditionTrue || configured.ObservedGeneration != project.Generation {
				return fmt.Errorf("Project %q is not configured", projectName)
			}
			if project.Spec.Source.Type != actionsv1alpha1.SourceTypeGitHub || project.Spec.Source.GitHub == nil {
				return fmt.Errorf("Project %q does not have a GitHub source", projectName)
			}
			if path.Dir(workflowPath) != project.Spec.WorkflowDirectory {
				return fmt.Errorf("workflow %q must be a direct child of Project %q workflow directory %q", workflowPath, projectName, project.Spec.WorkflowDirectory)
			}
			owner, repo, _ := strings.Cut(repository, "/")
			source, defaultRevision, err := github.resolve(ctx, owner, repo, ref)
			if err != nil {
				return err
			}
			content, err := github.workflowFile(ctx, source.Repository, workflowPath, defaultRevision)
			if err != nil {
				return fmt.Errorf("workflow must exist on the default branch: %w", err)
			}
			definition, err := parseDispatchWorkflow(content, workflowPath)
			if err != nil {
				return fmt.Errorf("workflow on the default branch: %w", err)
			}
			if source.Revision.SHA != defaultRevision {
				content, err = github.workflowFile(ctx, source.Repository, workflowPath, source.Revision.SHA)
				if err != nil {
					return err
				}
				definition, err = parseDispatchWorkflow(content, workflowPath)
				if err != nil {
					return err
				}
			}
			if _, _, err := workflow.Match(definition.On, workflow.Event{Name: string(actionsv1alpha1.GitHubEventNameWorkflowDispatch), Inputs: inputs}); err != nil {
				return fmt.Errorf("invalid inputs for workflow %q: %w", workflowPath, err)
			}
			if name == "" {
				data := make([]byte, 10)
				if _, err := rand.Read(data); err != nil {
					return fmt.Errorf("create WorkflowRun name: %w", err)
				}
				name = "dispatch-" + hex.EncodeToString(data)
			}
			source.Event = actionsv1alpha1.GitHubEvent{Name: actionsv1alpha1.GitHubEventNameWorkflowDispatch, Inputs: inputs}
			run := &actionsv1alpha1.WorkflowRun{
				ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
				Spec: actionsv1alpha1.WorkflowRunSpec{
					ProjectRef:   corev1.LocalObjectReference{Name: projectName},
					WorkflowPath: workflowPath,
					Source:       actionsv1alpha1.WorkflowRunSource{Type: actionsv1alpha1.SourceTypeGitHub, GitHub: source},
				},
			}
			if err := clients.writer.Create(ctx, run); err != nil {
				if !apierrors.IsAlreadyExists(err) {
					return fmt.Errorf("create WorkflowRun %q in namespace %q: %w", run.Name, namespace, err)
				}
				existing := &actionsv1alpha1.WorkflowRun{}
				if err := clients.reader.Get(ctx, client.ObjectKeyFromObject(run), existing); err != nil {
					return fmt.Errorf("get existing WorkflowRun %q in namespace %q: %w", run.Name, namespace, err)
				}
				existingSpec := existing.Spec.DeepCopy()
				existingSpec.CancelRequested = false
				existingSpec.TTLSecondsAfterFinished = nil
				if !apiequality.Semantic.DeepEqual(*existingSpec, run.Spec) {
					return fmt.Errorf("WorkflowRun %q already exists with different parameters", run.Name)
				}
			}
			_, err = fmt.Fprintln(command.OutOrStdout(), run.Name)
			return err
		},
	}
	addRunKubeFlags(command, &options, dependencies.defaultKubeconfig)
	command.Flags().StringVar(&projectName, "project", "", "Project containing the workflow run (required)")
	command.Flags().StringVar(&repository, "repo", "", "GitHub repository in OWNER/REPO form (required)")
	command.Flags().StringVar(&ref, "ref", "", "Branch or tag; defaults to the repository's default branch")
	command.Flags().StringArrayVarP(&fields, "raw-field", "f", nil, "Workflow input in key=value form (repeatable)")
	command.Flags().StringVar(&name, "name", "", "WorkflowRun name; defaults to a generated name")
	command.Flags().StringVar(&hostname, "hostname", "github.com", "GitHub hostname used by the control plane")
	return command
}

func parseDispatchWorkflow(content []byte, path string) (*workflow.Definition, error) {
	definition, err := workflow.Parse(content)
	if err != nil {
		return nil, fmt.Errorf("parse workflow %q: %w", path, err)
	}
	if _, found := definition.On.Events[string(actionsv1alpha1.GitHubEventNameWorkflowDispatch)]; !found {
		return nil, fmt.Errorf("workflow %q does not declare workflow_dispatch", path)
	}
	return definition, nil
}

func validateRunCreate(project, repository, path, ref, name string) error {
	if project == "" || len(validation.IsDNS1123Subdomain(project)) > 0 {
		return fmt.Errorf("--project must be a valid Project name")
	}
	if !runRepositoryPattern.MatchString(repository) || strings.HasSuffix(repository, "/.") || strings.HasSuffix(repository, "/..") {
		return fmt.Errorf("--repo must be in OWNER/REPO form")
	}
	if utf8.RuneCountInString(path) > 512 || !(strings.HasSuffix(path, ".yaml") || strings.HasSuffix(path, ".yml")) ||
		strings.HasPrefix(path, "/") || strings.HasPrefix(path, "./") || strings.HasPrefix(path, "../") ||
		strings.Contains(path, "//") || strings.Contains(path, "/./") || strings.Contains(path, "/../") {
		return fmt.Errorf("workflow %q must be a repository-relative .yaml or .yml path without empty, '.' or '..' segments", path)
	}
	if ref != "" {
		fullRef := ref
		if !strings.HasPrefix(ref, "refs/") {
			fullRef = "refs/heads/" + ref
		}
		shortRef := githubclient.RefName(fullRef)
		if !(strings.HasPrefix(fullRef, "refs/heads/") || strings.HasPrefix(fullRef, "refs/tags/")) || shortRef == "" ||
			utf8.RuneCountInString(fullRef) > 1024 || runInvalidRefPattern.MatchString(fullRef) ||
			strings.Contains(fullRef, "//") || strings.Contains(fullRef, "..") || strings.Contains(fullRef, "/.") ||
			strings.Contains(fullRef, ".lock/") || strings.HasSuffix(fullRef, "/") || strings.HasSuffix(fullRef, ".") ||
			strings.HasSuffix(fullRef, ".lock") || strings.Contains(fullRef, "@{") {
			return fmt.Errorf("--ref must identify a branch or tag")
		}
	}
	if name != "" && len(validation.IsDNS1123Subdomain(name)) > 0 {
		return fmt.Errorf("--name must be a valid WorkflowRun name")
	}
	return nil
}

func runCreateInputs(fields []string) (map[string]string, error) {
	if len(fields) == 0 {
		return nil, nil
	}
	if len(fields) > 25 {
		return nil, fmt.Errorf("workflow dispatch accepts at most 25 inputs")
	}
	inputs := make(map[string]string, len(fields))
	names := make(map[string]bool, len(fields))
	total := 0
	for _, field := range fields {
		name, value, found := strings.Cut(field, "=")
		if !found || !runInputNamePattern.MatchString(name) {
			return nil, fmt.Errorf("--raw-field must use key=value with a valid workflow input name")
		}
		if names[strings.ToLower(name)] {
			return nil, fmt.Errorf("workflow input %q is duplicated", name)
		}
		if !utf8.ValidString(value) {
			return nil, fmt.Errorf("workflow input %q must be valid UTF-8", name)
		}
		names[strings.ToLower(name)] = true
		inputs[name] = value
		total += utf8.RuneCountInString(name) + utf8.RuneCountInString(value)
	}
	if total > 65_535 {
		return nil, fmt.Errorf("workflow input names and values exceed 65535 characters")
	}
	return inputs, nil
}
