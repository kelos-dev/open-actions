package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	actionsv1alpha1 "github.com/kelos-dev/open-actions/api/v1alpha1"
	githubclient "github.com/kelos-dev/open-actions/internal/github"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

type githubStatusRequests struct {
	history []githubclient.CommitStatus
	reads   int
	writes  int
}

func (r *githubStatusRequests) publish(t *testing.T, writer http.ResponseWriter, request *http.Request) {
	t.Helper()
	r.writes++
	var report githubclient.CreateCommitStatusRequest
	if err := json.NewDecoder(request.Body).Decode(&report); err != nil {
		t.Error(err)
	}
	status := githubclient.CommitStatus{ID: int64(r.writes), State: report.State, Context: report.Context, TargetURL: report.TargetURL, Description: report.Description}
	status.Creator.Login = "open-actions[bot]"
	r.history = append([]githubclient.CommitStatus{status}, r.history...)
	fmt.Fprintf(writer, `{"id":%d}`, status.ID)
}

func (r *githubStatusRequests) list() []githubclient.CommitStatus {
	r.reads++
	return r.history
}

func TestGitHubStatusLifecycleRequestCounts(t *testing.T) {
	for _, count := range []int{1, 10, 100} {
		t.Run(fmt.Sprintf("jobs=%d", count), func(t *testing.T) {
			requests := &githubStatusRequests{}
			reporter, _, clusterClient, run, build, dependent := githubStatusTestControllers(t, func(w http.ResponseWriter, r *http.Request) {
				requests.publish(t, w, r)
			}, requests.list)
			for _, job := range []*actionsv1alpha1.WorkflowJob{build, dependent} {
				if err := clusterClient.Delete(t.Context(), job); err != nil {
					t.Fatal(err)
				}
			}
			jobs := make([]*actionsv1alpha1.WorkflowJob, count)
			for index := range jobs {
				name := fmt.Sprintf("job-%03d", index)
				jobs[index] = &actionsv1alpha1.WorkflowJob{
					ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: run.Namespace, UID: types.UID(name), Labels: map[string]string{actionsv1alpha1.LabelWorkflowRunUID: string(run.UID)}},
					Spec:       actionsv1alpha1.WorkflowJobSpec{WorkflowRunRef: corev1.LocalObjectReference{Name: run.Name}, JobID: name},
				}
				if err := clusterClient.Create(t.Context(), jobs[index]); err != nil {
					t.Fatal(err)
				}
			}
			reconcile := func() {
				t.Helper()
				restarted := &GitHubStatusReconciler{Client: clusterClient, APIReader: clusterClient, GitHub: reporter.GitHub, ConsoleURL: reporter.ConsoleURL}
				if err := restarted.reconcileGitHubStatuses(t.Context(), run); err != nil {
					t.Fatal(err)
				}
			}
			reconcile()
			start := metav1.NewTime(time.Date(2026, time.November, 5, 0, 0, 0, 0, time.UTC))
			for _, result := range []actionsv1alpha1.WorkflowJobResult{"", actionsv1alpha1.WorkflowJobResultSuccess} {
				for _, job := range jobs {
					if err := clusterClient.Get(t.Context(), client.ObjectKeyFromObject(job), job); err != nil {
						t.Fatal(err)
					}
					job.Status.StartTime = &start
					job.Status.Result = result
					if result != "" {
						completion := metav1.NewTime(start.Add(time.Second))
						job.Status.CompletionTime = &completion
					}
					if err := clusterClient.Status().Update(t.Context(), job); err != nil {
						t.Fatal(err)
					}
					reconcile()
				}
			}
			t.Logf("jobs=%d: status GETs=%d, status POSTs=%d", count, requests.reads, requests.writes)
			if requests.reads != 1 || requests.writes != 3*count {
				t.Fatalf("status requests: GETs=%d, POSTs=%d; want 1 and %d", requests.reads, requests.writes, 3*count)
			}
			if err := clusterClient.Get(t.Context(), client.ObjectKeyFromObject(run), run); err != nil {
				t.Fatal(err)
			}
			if workflowRunValidationStatus(run) != nil {
				t.Fatal("workflow without validation history published a validation status")
			}
		})
	}
}

func TestGitHubStatusRecoversUnrecordedWrites(t *testing.T) {
	requests := &githubStatusRequests{}
	reporter, _, clusterClient, run, build, _ := githubStatusTestControllers(t, func(w http.ResponseWriter, r *http.Request) {
		requests.publish(t, w, r)
	}, requests.list)
	if err := reporter.reconcileGitHubStatuses(t.Context(), run); err != nil {
		t.Fatal(err)
	}
	if err := clusterClient.Get(t.Context(), client.ObjectKeyFromObject(build), build); err != nil {
		t.Fatal(err)
	}
	start := metav1.Now()
	build.Status.StartTime = &start
	if err := clusterClient.Status().Update(t.Context(), build); err != nil {
		t.Fatal(err)
	}
	reporter.Client = interceptor.NewClient(clusterClient.(client.WithWatch), interceptor.Funcs{
		SubResourcePatch: func(ctx context.Context, c client.Client, name string, object client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
			return apierrors.NewConflict(actionsv1alpha1.GroupVersion.WithResource("workflowjobs").GroupResource(), object.GetName(), fmt.Errorf("status update unavailable"))
		},
	})
	if err := reporter.reconcileGitHubStatuses(t.Context(), run); err == nil {
		t.Fatal("reporting ignored a failed Kubernetes status update")
	}
	restarted := &GitHubStatusReconciler{Client: clusterClient, APIReader: clusterClient, GitHub: reporter.GitHub, ConsoleURL: reporter.ConsoleURL}
	if err := restarted.reconcileGitHubStatuses(t.Context(), run); err != nil {
		t.Fatal(err)
	}
	if requests.reads != 2 || requests.writes != 3 {
		t.Fatalf("retry duplicated an accepted report: GETs=%d, POSTs=%d", requests.reads, requests.writes)
	}
	if err := clusterClient.Get(t.Context(), client.ObjectKeyFromObject(build), build); err != nil {
		t.Fatal(err)
	}
	request := workflowJobCommitStatusRequest(reporter.ConsoleURL, run, build, workflowJobCommitStatusReport(run, build))
	if status := workflowJobCommitStatus(build); status == nil || status.ReportDigest != commitStatusReportDigest(request) {
		t.Fatalf("recovered status = %#v", status)
	}
}

func TestGitHubStatusRequiresDurableIntentBeforeWriting(t *testing.T) {
	requests := &githubStatusRequests{}
	reporter, _, clusterClient, run, _, _ := githubStatusTestControllers(t, func(w http.ResponseWriter, r *http.Request) {
		requests.publish(t, w, r)
	}, requests.list)
	reporter.Client = interceptor.NewClient(clusterClient.(client.WithWatch), interceptor.Funcs{
		Patch: func(ctx context.Context, c client.WithWatch, object client.Object, patch client.Patch, opts ...client.PatchOption) error {
			if configMap, ok := object.(*corev1.ConfigMap); ok {
				owner := githubStatusOwner{}
				if err := json.Unmarshal([]byte(configMap.Data[githubStatusOwnerDataKey]), &owner); err != nil {
					t.Fatal(err)
				}
				if owner.Reporting != nil {
					return apierrors.NewConflict(corev1.Resource("configmaps"), object.GetName(), fmt.Errorf("report checkpoint unavailable"))
				}
			}
			return c.Patch(ctx, object, patch, opts...)
		},
	})
	if err := reporter.reconcileGitHubStatuses(t.Context(), run); err == nil {
		t.Fatal("reporting ignored a failed intent checkpoint")
	}
	if requests.writes != 0 {
		t.Fatalf("GitHub received %d writes without durable intent", requests.writes)
	}
	reporter.Client = clusterClient
	if err := reporter.reconcileGitHubStatuses(t.Context(), run); err != nil {
		t.Fatal(err)
	}
	if requests.writes != 2 {
		t.Fatalf("reports after checkpoint recovered = %d, want 2", requests.writes)
	}
}

func TestGitHubStatusRecoversUncertainResponse(t *testing.T) {
	requests := &githubStatusRequests{}
	reporter, _, clusterClient, run, _, _ := githubStatusTestControllers(t, func(w http.ResponseWriter, r *http.Request) {
		response := httptest.NewRecorder()
		requests.publish(t, response, r)
		if requests.writes == 1 {
			http.Error(w, "response unavailable", http.StatusServiceUnavailable)
			return
		}
		fmt.Fprint(w, response.Body.String())
	}, requests.list)
	if err := reporter.reconcileGitHubStatuses(t.Context(), run); err == nil {
		t.Fatal("reporting ignored an uncertain GitHub response")
	}
	restarted := &GitHubStatusReconciler{Client: clusterClient, APIReader: clusterClient, GitHub: reporter.GitHub, ConsoleURL: reporter.ConsoleURL}
	for range 2 {
		if err := restarted.reconcileGitHubStatuses(t.Context(), run); err != nil {
			t.Fatal(err)
		}
	}
	if requests.reads != 2 || requests.writes != 2 {
		t.Fatalf("uncertain response recovery: GETs=%d, POSTs=%d; want 2 and 2", requests.reads, requests.writes)
	}
}
