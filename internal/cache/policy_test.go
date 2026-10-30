package cache

import (
	"reflect"
	"testing"
)

func TestCacheTriggerPolicy(t *testing.T) {
	for _, tc := range []struct {
		event, ref, base, mode, wantMode string
		wantRefs                         []string
	}{
		{"push", "refs/heads/main", "", "", "write", []string{"refs/heads/main"}},
		{"workflow_dispatch", "refs/heads/main", "", "", "write", []string{"refs/heads/main"}},
		{"schedule", "refs/heads/main", "", "", "write", []string{"refs/heads/main"}},
		{"pull_request", "refs/pull/42/merge", "release", "", "write", []string{"refs/pull/42/merge", "refs/heads/release", "refs/heads/main"}},
		{"pull_request_target", "refs/heads/main", "feature", "", "read", []string{"refs/heads/main"}},
		{"issue_comment", "refs/heads/main", "", "", "read", []string{"refs/heads/main"}},
		{"workflow_run", "refs/heads/main", "", "", "read", []string{"refs/heads/main"}},
		{"push", "refs/tags/v1", "", "", "write", []string{"refs/tags/v1", "refs/heads/main"}},
		{"pull_request_target", "refs/heads/main", "", "write", "write", []string{"refs/heads/main"}},
		{"push", "refs/heads/main", "", "read", "read", []string{"refs/heads/main"}},
		{"push", "refs/heads/main", "", "write-only", "write-only", []string{"refs/heads/main"}},
		{"push", "refs/heads/main", "", "none", "none", []string{"refs/heads/main"}},
	} {
		t.Run(tc.event+"/"+tc.ref+"/"+tc.mode, func(t *testing.T) {
			a := ResolveAccess(tc.event, tc.ref, tc.base, "main", tc.mode)
			if err := a.Validate(); err != nil {
				t.Fatal(err)
			}
			if a.Mode != tc.wantMode || a.WriteRef != tc.ref || !reflect.DeepEqual(a.ReadRefs, tc.wantRefs) {
				t.Fatalf("access=%#v", a)
			}
		})
	}
}
