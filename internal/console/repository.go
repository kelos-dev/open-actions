package console

import (
	"context"
	"errors"
	"fmt"

	actionsv1alpha1 "github.com/kelos-dev/open-actions/api/v1alpha1"
	githubclient "github.com/kelos-dev/open-actions/internal/github"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// RepositoryResolver reads repository identity, revisions, and workflow files through a Project's source.
type RepositoryResolver interface {
	Resolve(context.Context, *actionsv1alpha1.Project, string, string) (actionsv1alpha1.GitHubRepository, string, error)
	ResolveRevision(context.Context, *actionsv1alpha1.Project, string, string, string) (string, error)
	GetWorkflowFile(context.Context, *actionsv1alpha1.Project, string, string, string, string) ([]byte, error)
}

// GitHubRepositoryResolver resolves repositories through GitHub App installations.
type GitHubRepositoryResolver struct {
	reader client.Reader
	github *githubclient.Client
}

// NewGitHubRepositoryResolver creates a repository resolver.
func NewGitHubRepositoryResolver(reader client.Reader, github *githubclient.Client) (*GitHubRepositoryResolver, error) {
	if reader == nil || github == nil {
		return nil, errors.New("GitHub repository resolver clients are required")
	}
	return &GitHubRepositoryResolver{reader: reader, github: github}, nil
}

// Resolve returns GitHub's canonical identity and default branch for a repository accessible to the Project.
func (r *GitHubRepositoryResolver) Resolve(ctx context.Context, project *actionsv1alpha1.Project, owner, name string) (actionsv1alpha1.GitHubRepository, string, error) {
	requestedRepository := owner + "/" + name
	installation, err := r.installation(ctx, project, name, githubclient.InstallationPermissions{})
	if err != nil {
		return actionsv1alpha1.GitHubRepository{}, "", err
	}
	repository, err := installation.GetRepository(ctx, owner, name)
	if err != nil {
		return actionsv1alpha1.GitHubRepository{}, "", err
	}
	owner = repository.Owner.Login
	name = repository.Name
	if repository.ID < 1 || repository.ID > 9_007_199_254_740_991 || len(owner) > 100 || !repositoryOwnerPattern.MatchString(owner) ||
		len(name) > 100 || name == "." || name == ".." || !repositoryNamePattern.MatchString(name) {
		return actionsv1alpha1.GitHubRepository{}, "", fmt.Errorf("GitHub returned invalid identity for repository %s", requestedRepository)
	}
	return actionsv1alpha1.GitHubRepository{ID: repository.ID, Owner: owner, Name: name}, repository.DefaultBranch, nil
}

// ResolveRevision returns the commit SHA that a branch or tag ref currently identifies.
func (r *GitHubRepositoryResolver) ResolveRevision(ctx context.Context, project *actionsv1alpha1.Project, owner, name, ref string) (string, error) {
	installation, err := r.installation(ctx, project, name, githubclient.InstallationPermissions{"contents": "read"})
	if err != nil {
		return "", err
	}
	return installation.ResolveRevision(ctx, owner, name, ref)
}

// GetWorkflowFile reads a workflow at the selected commit using contents read permission.
func (r *GitHubRepositoryResolver) GetWorkflowFile(ctx context.Context, project *actionsv1alpha1.Project, owner, name, path, revision string) ([]byte, error) {
	installation, err := r.installation(ctx, project, name, githubclient.InstallationPermissions{"contents": "read"})
	if err != nil {
		return nil, err
	}
	return installation.GetFile(ctx, owner, name, path, revision)
}

func (r *GitHubRepositoryResolver) installation(ctx context.Context, project *actionsv1alpha1.Project, name string, permissions githubclient.InstallationPermissions) (*githubclient.InstallationClient, error) {
	githubConfig := project.Spec.Source.GitHub
	if githubConfig == nil {
		return nil, fmt.Errorf("Project %q has no GitHub source", project.Name)
	}
	secret := &corev1.Secret{}
	selector := githubConfig.PrivateKeySecretRef
	if err := r.reader.Get(ctx, client.ObjectKey{Namespace: project.Namespace, Name: selector.Name}, secret); err != nil {
		return nil, fmt.Errorf("get Project %q private key Secret %q: %w", project.Name, selector.Name, err)
	}
	privateKey := secret.Data[selector.Key]
	if len(privateKey) == 0 {
		return nil, fmt.Errorf("Project %q private key Secret %q does not contain non-empty key %q", project.Name, selector.Name, selector.Key)
	}
	installation, err := r.github.CachedInstallation(ctx, githubConfig.AppID, githubConfig.InstallationID, privateKey, name, permissions)
	if err != nil {
		return nil, fmt.Errorf("authenticate Project %q GitHub installation: %w", project.Name, err)
	}
	return installation, nil
}
