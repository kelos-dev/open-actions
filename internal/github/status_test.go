package github

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
)

func TestListLatestCommitStatuses(t *testing.T) {
	for _, test := range []struct {
		name      string
		contexts  []string
		wantPages int
		wantIDs   []int64
	}{
		{name: "no contexts"},
		{name: "stop at first page", contexts: []string{"build", "BUILD"}, wantPages: 1, wantIDs: []int64{300}},
		{name: "context on second page", contexts: []string{"build", "test"}, wantPages: 2, wantIDs: []int64{300, 200}},
		{name: "missing context", contexts: []string{"build", "missing"}, wantPages: 3, wantIDs: []int64{300}},
	} {
		t.Run(test.name, func(t *testing.T) {
			requests := 0
			revision := strings.Repeat("a", 40)
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				requests++
				page, err := strconv.Atoi(request.URL.Query().Get("page"))
				if err != nil || page != requests || page > test.wantPages || request.URL.Query().Get("per_page") != "100" || request.URL.Path != "/repos/acme/example/commits/"+revision+"/statuses" {
					t.Errorf("unexpected status request: %s", request.URL)
					http.Error(writer, "unexpected status query", http.StatusBadRequest)
					return
				}
				count := 100
				if page == 3 {
					count = 1
				}
				statuses := make([]CommitStatus, count)
				for index := range statuses {
					statuses[index] = CommitStatus{ID: int64(300 - (page-1)*100 - index), Context: "Build", State: "pending"}
					statuses[index].Creator.Login = "another-app[bot]"
				}
				if page == 2 {
					statuses[0].Context = "Test"
					statuses[0].Creator.Login = "open-actions[bot]"
				}
				if err := json.NewEncoder(writer).Encode(statuses); err != nil {
					t.Error(err)
				}
			}))
			defer server.Close()
			client, err := NewClient(server.URL, server.Client())
			if err != nil {
				t.Fatal(err)
			}
			installation := &InstallationClient{client: client, token: "token"}
			statuses, err := installation.ListLatestCommitStatuses(t.Context(), "acme", "example", revision, test.contexts)
			if err != nil || requests != test.wantPages || len(statuses) != len(test.wantIDs) {
				t.Fatalf("statuses = %#v, error = %v, requests = %d", statuses, err, requests)
			}
			for index, want := range test.wantIDs {
				if statuses[index].ID != want {
					t.Errorf("status %d ID = %d, want %d", index, statuses[index].ID, want)
				}
			}
			if len(statuses) > 0 && statuses[0].Creator.Login != "another-app[bot]" {
				t.Fatalf("latest status creator = %q", statuses[0].Creator.Login)
			}
		})
	}
}

func TestListLatestCommitStatusesPageFailure(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		requests++
		if requests == 1 {
			statuses := make([]CommitStatus, 100)
			statuses[0] = CommitStatus{ID: 1, Context: "build"}
			_ = json.NewEncoder(writer).Encode(statuses)
			return
		}
		http.Error(writer, "unavailable", http.StatusServiceUnavailable)
	}))
	defer server.Close()
	client, err := NewClient(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	installation := &InstallationClient{client: client, token: "token"}
	statuses, err := installation.ListLatestCommitStatuses(t.Context(), "acme", "example", strings.Repeat("a", 40), []string{"build", "test"})
	if err == nil || statuses != nil || requests != 2 {
		t.Fatalf("partial status history was accepted: %#v, %v, requests = %d", statuses, err, requests)
	}
}
