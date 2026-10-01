package console

import (
	"fmt"
	"net/url"
	"sort"
	"strconv"

	actionsv1alpha1 "github.com/kelos-dev/open-actions/api/v1alpha1"
)

// WorkflowRunFilter selects runs by Project, repository identity, and workflow path.
type WorkflowRunFilter struct {
	Project      string
	RepositoryID int64
	WorkflowPath string
}

// WorkflowRunOption identifies a choice in a run list filter.
type WorkflowRunOption struct {
	Value string
	Label string
}

// WorkflowRunList contains matching runs and filter choices from all retained runs.
type WorkflowRunList struct {
	Runs         []WorkflowRunSummary
	Truncated    bool
	Projects     []WorkflowRunOption
	Repositories []WorkflowRunOption
	Workflows    []WorkflowRunOption
}

func runFilterOptions(values map[string]string) []WorkflowRunOption {
	options := make([]WorkflowRunOption, 0, len(values))
	for value, label := range values {
		options = append(options, WorkflowRunOption{Value: value, Label: label})
	}
	sort.Slice(options, func(i, j int) bool {
		if options[i].Label != options[j].Label {
			return options[i].Label < options[j].Label
		}
		return options[i].Value < options[j].Value
	})
	return options
}

func workflowRunLabel(run *actionsv1alpha1.WorkflowRun) string {
	if run.Status.Identity == nil {
		return run.Name
	}
	label := fmt.Sprintf("#%d", run.Status.Identity.Number)
	if run.Status.Identity.Attempt > 1 {
		label += fmt.Sprintf(" · attempt %d", run.Status.Identity.Attempt)
	}
	return label
}

type runNavigation struct {
	Project       string
	ProjectURL    string
	Repository    string
	RepositoryURL string
	WorkflowName  string
	WorkflowPath  string
	WorkflowURL   string
	RunLabel      string
}

func workflowRunNavigation(run *actionsv1alpha1.WorkflowRun) runNavigation {
	project := namespacedValue(run.Namespace, run.Spec.ProjectRef.Name)
	query := url.Values{"project": {project}}
	navigation := runNavigation{
		Project: project, ProjectURL: "/?" + query.Encode(),
		WorkflowName: run.Status.WorkflowName, WorkflowPath: run.Spec.WorkflowPath,
		RunLabel: workflowRunLabel(run),
	}
	if navigation.WorkflowName == "" {
		navigation.WorkflowName = run.Spec.WorkflowPath
	}
	repository := run.Spec.Source.GitHub.Repository
	navigation.Repository = repository.Owner + "/" + repository.Name
	query.Set("repository", strconv.FormatInt(repository.ID, 10))
	navigation.RepositoryURL = "/?" + query.Encode()
	query.Set("workflow", run.Spec.WorkflowPath)
	navigation.WorkflowURL = "/?" + query.Encode()
	return navigation
}
