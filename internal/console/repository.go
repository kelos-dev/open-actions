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

// RepositoryResolver resolves repository identity through a Project's source.
type RepositoryResolver interface {
	Resolve(context.Context, *actionsv1alpha1.Project, string, string) (actionsv1alpha1.GitHubRepository, error)
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

// Resolve returns GitHub's canonical identity for a repository accessible to the Project.
func (r *GitHubRepositoryResolver) Resolve(ctx context.Context, project *actionsv1alpha1.Project, owner, name string) (actionsv1alpha1.GitHubRepository, error) {
	requestedRepository := owner + "/" + name
	githubConfig := project.Spec.Source.GitHub
	if githubConfig == nil {
		return actionsv1alpha1.GitHubRepository{}, fmt.Errorf("Project %q has no GitHub source", project.Name)
	}
	secret := &corev1.Secret{}
	selector := githubConfig.PrivateKeySecretRef
	if err := r.reader.Get(ctx, client.ObjectKey{Namespace: project.Namespace, Name: selector.Name}, secret); err != nil {
		return actionsv1alpha1.GitHubRepository{}, fmt.Errorf("get Project %q private key Secret %q: %w", project.Name, selector.Name, err)
	}
	privateKey := secret.Data[selector.Key]
	if len(privateKey) == 0 {
		return actionsv1alpha1.GitHubRepository{}, fmt.Errorf("Project %q private key Secret %q does not contain non-empty key %q", project.Name, selector.Name, selector.Key)
	}
	installation, err := r.github.CachedInstallation(ctx, githubConfig.AppID, githubConfig.InstallationID, privateKey, name, githubclient.InstallationPermissions{})
	if err != nil {
		return actionsv1alpha1.GitHubRepository{}, fmt.Errorf("authenticate Project %q GitHub installation: %w", project.Name, err)
	}
	repository, err := installation.GetRepository(ctx, owner, name)
	if err != nil {
		return actionsv1alpha1.GitHubRepository{}, err
	}
	owner = repository.Owner.Login
	name = repository.Name
	if repository.ID < 1 || repository.ID > 9_007_199_254_740_991 || len(owner) > 100 || !repositoryOwnerPattern.MatchString(owner) ||
		len(name) > 100 || name == "." || name == ".." || !repositoryNamePattern.MatchString(name) {
		return actionsv1alpha1.GitHubRepository{}, fmt.Errorf("GitHub returned invalid identity for repository %s", requestedRepository)
	}
	return actionsv1alpha1.GitHubRepository{ID: repository.ID, Owner: owner, Name: name}, nil
}
