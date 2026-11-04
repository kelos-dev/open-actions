package controller

import (
	"context"
	"testing"
	"time"

	actionsv1alpha1 "github.com/kelos-dev/open-actions/api/v1alpha1"
	coordinationv1 "k8s.io/api/coordination/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// GitHub scopes concurrency to matching groups and does not guarantee dispatch order:
// https://docs.github.com/en/actions/how-tos/write-workflows/choose-when-workflows-run/control-workflow-concurrency
func TestConcurrencyPlanningProceedsWhilePullRequestAwaitsApproval(t *testing.T) {
	for _, origin := range []string{"fork", "job token", "fork and job token"} {
		t.Run(origin, func(t *testing.T) {
			fixture := newDeferredRerunFixture(t, `on: push
concurrency: ${{ github.ref }}
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: true
`)
			pending := pullRequestApprovalTestRun(origin, false)
			pending.Name = "approval-pending"
			pending.Spec.ProjectRef = fixture.root.Spec.ProjectRef
			pending.CreationTimestamp = metav1.NewTime(time.Unix(1, 0))
			if err := fixture.reconciler.Create(context.Background(), pending); err != nil {
				t.Fatal(err)
			}
			fixture.reconcile(pending)

			current := fixture.root.DeepCopy()
			current.ObjectMeta = metav1.ObjectMeta{Name: "current", Namespace: pending.Namespace, UID: "current-uid", CreationTimestamp: metav1.NewTime(time.Unix(2, 0))}
			current.Status = actionsv1alpha1.WorkflowRunStatus{}
			setTestWorkflowRunIdentity(current)
			current.Spec.Source.GitHub.Revision.Ref = "refs/heads/feature"
			if err := fixture.reconciler.Create(context.Background(), current); err != nil {
				t.Fatal(err)
			}
			fixture.reconcile(current)
			if !meta.IsStatusConditionTrue(current.Status.Conditions, actionsv1alpha1.WorkflowRunConditionPlanned) {
				t.Fatalf("current run was not planned: %#v", current.Status.Conditions)
			}
			jobs := fixture.jobs(current)
			job, found := jobs["build"]
			if !found || len(jobs) != 1 {
				t.Fatalf("planned jobs = %#v", jobs)
			}
			assertJobReady(t, job, metav1.ConditionTrue)
			fixture.reconcile(pending)
			approved := meta.FindStatusCondition(pending.Status.Conditions, actionsv1alpha1.WorkflowRunConditionApproved)
			if approved == nil || approved.Status != metav1.ConditionFalse || approved.Reason != "ApprovalRequired" || len(fixture.jobs(pending)) != 0 {
				t.Fatalf("pending approval status = %#v", pending.Status)
			}
		})
	}
}

func TestConcurrencyDoesNotWaitForUnregisteredRuns(t *testing.T) {
	for _, test := range []struct {
		name, group, reason string
	}{
		{name: "unevaluated"},
		{name: "credentials unavailable", reason: "CredentialsUnavailable"},
		{name: "workflow fetch unavailable", reason: "WorkflowFetchFailed"},
		{name: "same group", group: "deploy"},
		{name: "different group", group: "test"},
	} {
		t.Run(test.name, func(t *testing.T) {
			older := concurrencyRun("older", "older", 1, time.Unix(1, 0), test.group, nil)
			if test.reason != "" {
				older.Status.Conditions = []metav1.Condition{plannedCondition(metav1.ConditionUnknown, test.reason)}
			}
			current := concurrencyRun("current", "current", 1, time.Unix(2, 0), "deploy", nil)
			reconciler, clusterClient := concurrencyReconciler(t, older, current)
			if waiting, err := reconciler.handleConcurrency(context.Background(), current); err != nil || waiting {
				t.Fatalf("handleConcurrency() = %t, %v", waiting, err)
			}
			scope, err := workflowRunConcurrencyScope(current)
			if err != nil {
				t.Fatal(err)
			}
			lease := &coordinationv1.Lease{}
			if err := clusterClient.Get(context.Background(), client.ObjectKey{Namespace: current.Namespace, Name: concurrencyLeaseName(scope, "deploy")}, lease); err != nil {
				t.Fatal(err)
			}
			holder, err := concurrencyHolder(lease)
			if err != nil || holder == nil || holder.identity() != workflowRunConcurrencyMember(current).identity() {
				t.Fatalf("group holder = %#v, %v", holder, err)
			}
		})
	}
}

func TestConcurrencyAdmissionFollowsGroupArrival(t *testing.T) {
	for _, cancelInProgress := range []bool{false, true} {
		name := "wait for holder"
		if cancelInProgress {
			name = "cancel holder"
		}
		t.Run(name, func(t *testing.T) {
			holder := concurrencyRun("holder", "holder", 1, time.Unix(3, 0), "deploy", nil)
			pending := concurrencyRun("pending", "pending", 1, time.Unix(2, 0), "deploy", nil)
			delayed := concurrencyRun("delayed", "delayed", 1, time.Unix(1, 0), "deploy", nil)
			delayed.Status.Concurrency.CancelInProgress = cancelInProgress
			reconciler, clusterClient := concurrencyReconciler(t, holder, pending, delayed)
			if waiting, err := reconciler.handleConcurrency(context.Background(), holder); err != nil || waiting {
				t.Fatalf("holder admission = %t, %v", waiting, err)
			}
			for _, run := range []*actionsv1alpha1.WorkflowRun{pending, delayed} {
				if waiting, err := reconciler.handleConcurrency(context.Background(), run); err != nil || !waiting {
					t.Fatalf("WorkflowRun %q admission = %t, %v", run.Name, waiting, err)
				}
				if _, err := reconciler.waitingForConcurrency(context.Background(), run, "CI", 1); err != nil {
					t.Fatal(err)
				}
			}
			if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(pending), pending); err != nil {
				t.Fatal(err)
			}
			if !pending.Spec.CancelRequested {
				t.Fatal("last admitted run did not replace the pending member")
			}
			if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(holder), holder); err != nil {
				t.Fatal(err)
			}
			if holder.Spec.CancelRequested != cancelInProgress {
				t.Fatalf("holder cancellation = %t, want %t", holder.Spec.CancelRequested, cancelInProgress)
			}
			scope, err := workflowRunConcurrencyScope(holder)
			if err != nil {
				t.Fatal(err)
			}
			lease := &coordinationv1.Lease{}
			if err := clusterClient.Get(context.Background(), client.ObjectKey{Namespace: holder.Namespace, Name: concurrencyLeaseName(scope, "deploy")}, lease); err != nil {
				t.Fatal(err)
			}
			waitingMember, err := concurrencyPending(lease)
			if err != nil || waitingMember == nil || waitingMember.identity() != workflowRunConcurrencyMember(delayed).identity() {
				t.Fatalf("pending member = %#v, %v", waitingMember, err)
			}
			reconciler = &WorkflowRunReconciler{Client: clusterClient, APIReader: clusterClient}
			if waiting, err := reconciler.handleConcurrency(context.Background(), delayed); err != nil || !waiting {
				t.Fatalf("resumed admission = %t, %v", waiting, err)
			}
			if err := reconciler.releaseConcurrency(context.Background(), holder.Namespace, scope, "deploy", workflowRunConcurrencyMember(holder)); err != nil {
				t.Fatal(err)
			}
			if waiting, err := reconciler.handleConcurrency(context.Background(), delayed); err != nil || waiting {
				t.Fatalf("promoted admission = %t, %v", waiting, err)
			}
		})
	}
}
