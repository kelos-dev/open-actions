package v1alpha1

import (
	"testing"

	"k8s.io/apimachinery/pkg/runtime"
)

func TestAddToScheme(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := AddToScheme(scheme); err != nil {
		t.Fatalf("add API types to scheme: %v", err)
	}

	for _, kind := range []string{"Project", "ProjectList", "Runner", "RunnerList", "RunnerSet", "RunnerSetList", "WorkflowJob", "WorkflowJobList", "WorkflowRun", "WorkflowRunList"} {
		gvk := GroupVersion.WithKind(kind)
		if _, err := scheme.New(gvk); err != nil {
			t.Errorf("construct %s: %v", gvk, err)
		}
	}
}
