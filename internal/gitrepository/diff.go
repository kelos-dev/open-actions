package gitrepository

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
)

// DiffRevision identifies pinned trees to compare. BaseParent compares against
// BaseSHA's first parent, or the empty tree when BaseSHA is a root commit.
type DiffRevision struct {
	BaseSHA    string
	HeadSHA    string
	BaseParent bool
}

// ChangedFiles fetches only the history needed to compare the pinned trees.
func (c *Client) ChangedFiles(ctx context.Context, owner, repository, token string, revision DiffRevision, limit int) (paths []string, resultErr error) {
	if owner == "" || repository == "" || token == "" || !validSHA(revision.BaseSHA) || !validSHA(revision.HeadSHA) || limit < 1 {
		return nil, errors.New("Git diff configuration is incomplete")
	}
	directory, err := os.MkdirTemp("", "open-actions-diff-")
	if err != nil {
		return nil, fmt.Errorf("create Git diff directory: %w", err)
	}
	remoteURL := c.repositoryURL(owner, repository)
	r := &Repository{client: c, directory: directory, environment: gitEnvironment(remoteURL, token)}
	defer func() { resultErr = errors.Join(resultErr, r.Close()) }()
	if err := c.run(ctx, directory, r.environment, nil, "git", "init", "--quiet"); err != nil {
		return nil, err
	}
	if err := c.run(ctx, directory, r.environment, nil, "git", "remote", "add", "origin", remoteURL); err != nil {
		return nil, err
	}
	depth := "--depth=1"
	if revision.BaseParent {
		depth = "--depth=2"
	}
	// Fetch the base last so its parent remains visible at a shallow boundary.
	for _, sha := range []string{revision.HeadSHA, revision.BaseSHA} {
		if err := c.run(ctx, directory, r.environment, nil, "git", "-c", "protocol.version=2", "fetch", "--quiet", "--no-tags", "--filter=blob:none", depth, "origin", sha); err != nil {
			return nil, fmt.Errorf("fetch diff commit %s from %s/%s: %w", sha, owner, repository, err)
		}
	}
	base := revision.BaseSHA
	if revision.BaseParent {
		parents, err := c.output(ctx, directory, r.environment, nil, "git", "show", "--no-patch", "--format=%P", base)
		if err != nil {
			return nil, err
		}
		if fields := strings.Fields(string(parents)); len(fields) > 0 {
			base = fields[0]
		} else {
			emptyTree, err := c.output(ctx, directory, r.environment, []byte{}, "git", "hash-object", "-t", "tree", "-w", "--stdin")
			if err != nil {
				return nil, err
			}
			base = strings.TrimSpace(string(emptyTree))
		}
	}
	return r.ChangedFiles(ctx, base, revision.HeadSHA, limit)
}

// ChangedFiles compares trees already fetched into this repository. Disabling
// rename detection includes both removed and added paths without loading blobs.
func (r *Repository) ChangedFiles(ctx context.Context, baseSHA, headSHA string, limit int) ([]string, error) {
	if r == nil || r.client == nil || r.directory == "" || !validSHA(baseSHA) || !validSHA(headSHA) || limit < 1 {
		return nil, errors.New("Git repository diff configuration is incomplete")
	}
	output, err := r.client.output(ctx, r.directory, r.environment, nil, "git", "diff", "--no-ext-diff", "--no-renames", "--name-only", "-z", baseSHA, headSHA, "--")
	if err != nil {
		return nil, fmt.Errorf("compare Git trees %s and %s: %w", baseSHA, headSHA, err)
	}
	truncated := len(output) == maxCommandOutput
	paths := make([]string, 0, limit)
	for len(output) > 0 && len(paths) < limit {
		path, remaining, found := bytes.Cut(output, []byte{0})
		if !found {
			return nil, errors.New("Git diff output exceeds the command output limit")
		}
		paths = append(paths, string(path))
		output = remaining
	}
	if truncated && len(paths) < limit {
		return nil, errors.New("Git diff output exceeds the command output limit")
	}
	return paths, nil
}
