package main

import (
	"strings"
	"testing"

	"github.com/kelos-dev/open-actions/internal/workflow"
)

func TestPreparationWorkflowIsolatesDownloadBlock(t *testing.T) {
	definition, err := workflow.Parse([]byte(preparationWorkflowData))
	if err != nil {
		t.Fatal(err)
	}
	steps := definition.Jobs["test"].Steps
	blockIndex := -1
	actionIndex := -1
	for index, step := range steps {
		if strings.Contains(step.Run, "/fixture/block-preparation-actions") {
			blockIndex = index
		}
		if step.Uses == "preparation-actions/composite@v1" {
			actionIndex = index
		}
	}
	if blockIndex < 0 || actionIndex < 0 || blockIndex >= actionIndex {
		t.Fatalf("preparation steps = %#v", steps)
	}

	standard, err := workflow.Parse([]byte(workflowData))
	if err != nil {
		t.Fatal(err)
	}
	for _, step := range standard.Jobs["test"].Steps {
		if strings.Contains(step.Run, "/fixture/block-preparation-actions") || strings.HasPrefix(step.Uses, "preparation-actions/") {
			t.Fatalf("standard workflow contains preparation probe: %#v", step)
		}
	}
}
