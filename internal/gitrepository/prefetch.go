package gitrepository

import (
	"context"
	"errors"
	"strings"
)

// PrefetchFiles downloads missing objects for the selected paths in one fetch.
func (r *Repository) PrefetchFiles(ctx context.Context, paths []string) error {
	if r == nil || r.client == nil || r.directory == "" || r.SHA == "" {
		return errors.New("Git repository file prefetch configuration is incomplete")
	}
	if len(paths) == 0 {
		return nil
	}
	// Starting at the tree excludes historical versions and commit simplification.
	arguments := []string{"rev-list", "--objects", "--no-object-names", "--missing=print", r.SHA + "^{tree}", "--"}
	for _, path := range paths {
		arguments = append(arguments, ":(literal)"+strings.TrimPrefix(path, "/"))
	}
	output, err := r.client.output(ctx, r.directory, r.environment, nil, "git", arguments...)
	if err != nil {
		return err
	}
	if len(output) == maxCommandOutput {
		return errors.New("Git file prefetch output exceeds the command output limit")
	}
	var missing []string
	for _, object := range strings.Fields(string(output)) {
		if sha, found := strings.CutPrefix(object, "?"); found {
			missing = append(missing, sha)
		}
	}
	if len(missing) == 0 {
		return nil
	}
	return r.client.fetch(ctx, r.directory, r.environment, "origin", 0, missing...)
}
