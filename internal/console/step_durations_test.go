package console

import (
	"net/http"
	"net/http/httptest"
	"os/exec"
	"strings"
	"testing"
)

func TestConsoleStepDurations(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("node is not installed")
	}
	handler := newTestHandler(t, false)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/runs/default/ci/jobs/build", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("log page status = %d, want %d", response.Code, http.StatusOK)
	}
	command := exec.Command(node, "testdata/step_durations.cjs")
	command.Stdin = strings.NewReader(response.Body.String())
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("step duration rendering: %v\n%s", err, output)
	}
}
