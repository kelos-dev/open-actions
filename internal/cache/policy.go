package cache

import (
	"errors"
	"slices"
	"strings"
)

type Tenant struct {
	ProjectUID   string `json:"project_uid"`
	RepositoryID int64  `json:"repository_id"`
}

type Access struct {
	ReadRefs []string `json:"read_refs"`
	WriteRef string   `json:"write_ref"`
	Mode     string   `json:"mode"`
}

func ValidMode(mode string) bool {
	return slices.Contains([]string{"read", "write", "write-only", "none"}, mode)
}

func (a Access) Validate() error {
	if !ValidMode(a.Mode) || !strings.HasPrefix(a.WriteRef, "refs/") || len(a.ReadRefs) == 0 {
		return errors.New("cache access is incomplete")
	}
	for _, ref := range a.ReadRefs {
		if !strings.HasPrefix(ref, "refs/") {
			return errors.New("cache read ref is invalid")
		}
	}
	return nil
}

func (a Access) Readable() bool { return a.Mode == "read" || a.Mode == "write" }
func (a Access) Writable() bool { return a.Mode == "write" || a.Mode == "write-only" }

// ResolveAccess applies trigger defaults independently of repository token permissions.
func ResolveAccess(event, ref, baseRef, defaultBranch, mode string) Access {
	if mode == "" {
		mode = "write"
		if ref == "refs/heads/"+defaultBranch && !slices.Contains([]string{"push", "workflow_dispatch", "repository_dispatch", "delete", "registry_package", "page_build", "schedule"}, event) {
			mode = "read"
		}
	}
	refs := []string{ref}
	if event == "pull_request" && baseRef != "" {
		refs = append(refs, "refs/heads/"+baseRef)
	}
	if defaultBranch != "" {
		refs = append(refs, "refs/heads/"+defaultBranch)
	}
	unique := []string{}
	for _, r := range refs {
		if !slices.Contains(unique, r) {
			unique = append(unique, r)
		}
	}
	return Access{ReadRefs: unique, WriteRef: ref, Mode: mode}
}
