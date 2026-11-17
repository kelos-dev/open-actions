package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"

	actionsv1alpha1 "github.com/kelos-dev/open-actions/api/v1alpha1"
	githubclient "github.com/kelos-dev/open-actions/internal/github"
	"github.com/kelos-dev/open-actions/internal/installer"
)

type runGitHub struct {
	hostname   string
	runCommand installer.CommandFunc
}

func newRunGitHub(hostname string, runCommand installer.CommandFunc) (*runGitHub, error) {
	if hostname == "" || strings.ContainsAny(hostname, "/?#@ \t\r\n") {
		return nil, fmt.Errorf("--hostname must be a GitHub hostname")
	}
	if runCommand == nil {
		return nil, fmt.Errorf("GitHub command runner is required")
	}
	return &runGitHub{hostname: hostname, runCommand: runCommand}, nil
}

func (g *runGitHub) api(ctx context.Context, result any, arguments ...string) error {
	var stdout, stderr bytes.Buffer
	args := append([]string{"api", "--hostname", g.hostname}, arguments...)
	if err := g.runCommand(ctx, "gh", args, &stdout, &stderr); err != nil {
		return fmt.Errorf("gh api: %w: %s", err, strings.TrimSpace(stderr.String()))
	}
	if err := json.Unmarshal(stdout.Bytes(), result); err != nil {
		return fmt.Errorf("decode GitHub response: %w", err)
	}
	return nil
}

const runRepositoryQuery = `query($owner: String!, $name: String!, $branch: String!, $tag: String!) {
  viewer { login }
  repository(owner: $owner, name: $name) {
    databaseId name owner { login } viewerPermission
    defaultBranchRef { name target { oid } }
    branch: ref(qualifiedName: $branch) { name }
    tag: ref(qualifiedName: $tag) { name }
  }
}`

func (g *runGitHub) resolve(ctx context.Context, owner, name, ref string) (*actionsv1alpha1.GitHubWorkflowRunSource, string, error) {
	type reference struct {
		Name   string `json:"name"`
		Target struct {
			OID string `json:"oid"`
		} `json:"target"`
	}
	response := struct {
		Data struct {
			Viewer struct {
				Login string `json:"login"`
			} `json:"viewer"`
			Repository *struct {
				ID    int64  `json:"databaseId"`
				Name  string `json:"name"`
				Owner struct {
					Login string `json:"login"`
				} `json:"owner"`
				Permission    string     `json:"viewerPermission"`
				DefaultBranch *reference `json:"defaultBranchRef"`
				Branch        *reference `json:"branch"`
				Tag           *reference `json:"tag"`
			} `json:"repository"`
		} `json:"data"`
		Errors []struct {
			Message string `json:"message"`
		} `json:"errors"`
	}{}
	shortRef := githubclient.RefName(ref)
	if err := g.api(ctx, &response, "graphql", "-f", "query="+runRepositoryQuery,
		"-f", "owner="+owner, "-f", "name="+name,
		"-f", "branch=refs/heads/"+shortRef, "-f", "tag=refs/tags/"+shortRef); err != nil {
		return nil, "", fmt.Errorf("resolve GitHub repository %s/%s: %w", owner, name, err)
	}
	if len(response.Errors) > 0 {
		return nil, "", fmt.Errorf("resolve GitHub repository %s/%s: %s", owner, name, response.Errors[0].Message)
	}
	repository := response.Data.Repository
	if repository == nil || repository.ID <= 0 || repository.Name == "" || repository.Owner.Login == "" || response.Data.Viewer.Login == "" {
		return nil, "", fmt.Errorf("GitHub repository %s/%s or authenticated user is unavailable", owner, name)
	}
	switch repository.Permission {
	case "ADMIN", "MAINTAIN", "WRITE":
	default:
		return nil, "", fmt.Errorf("GitHub user %q requires write access to repository %s/%s", response.Data.Viewer.Login, owner, name)
	}
	if repository.DefaultBranch == nil || repository.DefaultBranch.Target.OID == "" {
		return nil, "", fmt.Errorf("GitHub repository %s/%s has no default branch", owner, name)
	}
	switch {
	case ref == "":
		ref = "refs/heads/" + repository.DefaultBranch.Name
	case strings.HasPrefix(ref, "refs/heads/"):
		if repository.Branch == nil {
			return nil, "", fmt.Errorf("branch %q does not exist in repository %s/%s", ref, owner, name)
		}
	case strings.HasPrefix(ref, "refs/tags/"):
		if repository.Tag == nil {
			return nil, "", fmt.Errorf("tag %q does not exist in repository %s/%s", ref, owner, name)
		}
	case repository.Branch != nil && repository.Tag != nil:
		return nil, "", fmt.Errorf("ref %q is both a branch and tag; use refs/heads/%s or refs/tags/%s", ref, ref, ref)
	case repository.Branch != nil:
		ref = "refs/heads/" + repository.Branch.Name
	case repository.Tag != nil:
		ref = "refs/tags/" + repository.Tag.Name
	default:
		return nil, "", fmt.Errorf("branch or tag %q does not exist in repository %s/%s", ref, owner, name)
	}
	commit := struct {
		SHA string `json:"sha"`
	}{}
	endpoint := "repos/" + repository.Owner.Login + "/" + repository.Name + "/commits/" + url.PathEscape(ref)
	if err := g.api(ctx, &commit, "--method", "GET", endpoint); err != nil {
		return nil, "", fmt.Errorf("resolve ref %q in repository %s/%s: %w", ref, owner, name, err)
	}
	if !runCommitPattern.MatchString(commit.SHA) {
		return nil, "", fmt.Errorf("GitHub returned an invalid commit for ref %q in repository %s/%s", ref, owner, name)
	}
	return &actionsv1alpha1.GitHubWorkflowRunSource{
		Actor:      response.Data.Viewer.Login,
		Repository: actionsv1alpha1.GitHubRepository{ID: repository.ID, Owner: repository.Owner.Login, Name: repository.Name},
		Revision:   actionsv1alpha1.GitRevision{SHA: commit.SHA, Ref: ref},
	}, repository.DefaultBranch.Target.OID, nil
}

func (g *runGitHub) workflowFile(ctx context.Context, repository actionsv1alpha1.GitHubRepository, path, revision string) ([]byte, error) {
	file := struct {
		Type     string `json:"type"`
		Encoding string `json:"encoding"`
		Content  string `json:"content"`
	}{}
	endpoint := "repos/" + repository.Owner + "/" + repository.Name + "/contents/" + url.PathEscape(path)
	if err := g.api(ctx, &file, "--method", "GET", endpoint, "-f", "ref="+revision); err != nil {
		return nil, fmt.Errorf("load workflow %q at %q in repository %s/%s: %w", path, revision, repository.Owner, repository.Name, err)
	}
	if file.Type != "file" || file.Encoding != "base64" {
		return nil, fmt.Errorf("workflow %q at %q must be a file with base64 content", path, revision)
	}
	content, err := base64.StdEncoding.DecodeString(file.Content)
	if err != nil {
		return nil, fmt.Errorf("decode workflow %q at %q: %w", path, revision, err)
	}
	return content, nil
}
