package workflow

import (
	"fmt"
	"testing"
)

func TestCacheModeSyntax(t *testing.T) {
	for _, mode := range []string{"read", "write", "write-only", "none"} {
		definition, err := Parse([]byte(fmt.Sprintf("on: push\ncache-mode: %s\njobs:\n  test:\n    runs-on: ubuntu-latest\n    cache-mode: read\n    steps:\n      - run: true\n", mode)))
		if err != nil {
			t.Fatal(err)
		}
		if definition.CacheMode != CacheMode(mode) || definition.Jobs["test"].CacheMode != "read" {
			t.Fatal(definition)
		}
	}
	for _, mode := range []string{"invalid", "true", "''", "[]", "${{ inputs.mode }}"} {
		if _, err := Parse([]byte(fmt.Sprintf("on: push\ncache-mode: %s\njobs:\n  test:\n    runs-on: ubuntu-latest\n    steps:\n      - run: true\n", mode))); err == nil {
			t.Fatalf("invalid mode %q accepted", mode)
		}
	}
}
