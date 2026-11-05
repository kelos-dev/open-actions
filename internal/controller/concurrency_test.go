package controller

import (
	"context"
	"fmt"
	"testing"
	"time"

	actionsv1alpha1 "github.com/kelos-dev/open-actions/api/v1alpha1"
	coordinationv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// GitHub cancels a replaced pending workflow without admitting it to the group:
// https://docs.github.com/en/actions/reference/workflows-and-actions/workflow-syntax#concurrency
func TestWaitingWorkflowRunCancellationCompletesWithoutAdmission(t *testing.T) {
	for _, cancelInProgress := range []bool{false, true} {
		for _, cancellation := range []string{"manual", "replacement"} {
			for _, releaseHolder := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/cancel-in-progress=%t/release-holder=%t", cancellation, cancelInProgress, releaseHolder), func(t *testing.T) {
					f := newDeferredRerunFixture(t, fmt.Sprintf(`on: push
concurrency:
  group: deploy
  cancel-in-progress: %t
jobs:
  build:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        target: [one, two]
    steps:
      - run: build
  cleanup:
    needs: build
    if: always()
    runs-on: ubuntu-latest
    steps:
      - run: cleanup
  cancellation:
    if: cancelled()
    runs-on: ubuntu-latest
    steps:
      - run: report
  dynamic:
    needs: build
    if: always()
    runs-on: ubuntu-latest
    strategy:
      matrix:
        target: ${{ fromJSON(needs.build.outputs.targets) }}
    steps:
      - run: dynamic
  report:
    needs: dynamic
    if: cancelled()
    runs-on: ubuntu-latest
    steps:
      - run: report
`, cancelInProgress))
					ctx := context.Background()
					createRun := func(name string) *actionsv1alpha1.WorkflowRun {
						t.Helper()
						run := f.root.DeepCopy()
						run.ObjectMeta = metav1.ObjectMeta{Name: name, Namespace: f.root.Namespace, UID: types.UID(name)}
						run.Spec.CancelRequested = false
						run.Status = actionsv1alpha1.WorkflowRunStatus{}
						setTestWorkflowRunIdentity(run)
						if err := f.reconciler.Create(ctx, run); err != nil {
							t.Fatal(err)
						}
						f.reconcile(run)
						if planned := meta.FindStatusCondition(run.Status.Conditions, actionsv1alpha1.WorkflowRunConditionPlanned); !waitingForConcurrencyCondition(planned) {
							t.Fatalf("WorkflowRun %q did not wait for concurrency: %#v", name, run.Status)
						}
						for _, job := range f.jobs(run) {
							if workflowJobReadyCondition(&job) || job.Status.RunnerRef != nil {
								t.Fatalf("waiting job %q became runnable: %#v", job.Name, job.Status)
							}
						}
						return run
					}
					pending := createRun("pending")
					var replacement *actionsv1alpha1.WorkflowRun
					if cancellation == "replacement" {
						replacement = createRun("replacement")
					} else if err := f.reconciler.cancelWorkflowRun(ctx, pending); err != nil {
						t.Fatal(err)
					}
					if err := f.reconciler.Get(ctx, client.ObjectKeyFromObject(pending), pending); err != nil {
						t.Fatal(err)
					}
					if !pending.Spec.CancelRequested {
						t.Fatal("waiting run did not receive a cancellation request")
					}
					if err := f.reconciler.Get(ctx, client.ObjectKeyFromObject(f.root), f.root); err != nil {
						t.Fatal(err)
					}
					if f.root.Spec.CancelRequested != cancelInProgress {
						t.Fatalf("holder cancellation = %t, want %t", f.root.Spec.CancelRequested, cancelInProgress)
					}
					scope, err := workflowRunConcurrencyScope(f.root)
					if err != nil {
						t.Fatal(err)
					}
					if releaseHolder {
						if err := f.reconciler.releaseConcurrency(ctx, f.root.Namespace, scope, "deploy", workflowRunConcurrencyMember(f.root)); err != nil {
							t.Fatal(err)
						}
					}
					lease := &coordinationv1.Lease{}
					leaseKey := client.ObjectKey{Namespace: pending.Namespace, Name: concurrencyLeaseName(scope, "deploy")}
					if err := f.reconciler.Get(ctx, leaseKey, lease); err != nil {
						t.Fatal(err)
					}
					holderBefore, err := concurrencyHolder(lease)
					if err != nil {
						t.Fatal(err)
					}
					f.reconcile(pending)
					condition := meta.FindStatusCondition(pending.Status.Conditions, actionsv1alpha1.WorkflowRunConditionSucceeded)
					if condition == nil || condition.Status != metav1.ConditionFalse || condition.Reason != "JobCancelled" || pending.Status.CompletionTime == nil {
						t.Fatalf("cancelled run did not finish: %#v", pending.Status)
					}
					if pending.Status.StartTime != nil {
						t.Fatal("cancelled pending run has a start time")
					}
					jobs := f.jobs(pending)
					if len(jobs) != 6 || pending.Status.Jobs.Total != 6 || pending.Status.Jobs.Cancelled != 6 || pending.Status.Jobs.Queued != 0 || pending.Status.Jobs.Waiting != 0 || pending.Status.Jobs.Active != 0 {
						t.Fatalf("cancelled job count = %d, summary = %#v", len(jobs), pending.Status.Jobs)
					}
					for _, job := range jobs {
						assertJobReady(t, job, metav1.ConditionFalse)
						if job.Status.Result != actionsv1alpha1.WorkflowJobResultCancelled || job.Status.CompletionTime == nil || job.Status.StartTime != nil || job.Status.RunnerRef != nil {
							t.Fatalf("pending job %q did not cancel without execution: %#v", job.Name, job.Status)
						}
					}
					f.reconcile(pending)
					lease = &coordinationv1.Lease{}
					err = f.reconciler.Get(ctx, leaseKey, lease)
					if releaseHolder && replacement == nil {
						if !apierrors.IsNotFound(err) {
							t.Fatalf("empty concurrency group was not removed: %v", err)
						}
					} else if err != nil {
						t.Fatal(err)
					}
					holderAfter, err := concurrencyHolder(lease)
					if err != nil {
						t.Fatal(err)
					}
					if holderBefore.identity() != workflowRunConcurrencyMember(pending).identity() && (holderAfter == nil || holderAfter.identity() != holderBefore.identity()) {
						t.Fatalf("holder changed from %#v to %#v", holderBefore, holderAfter)
					}
					member, err := concurrencyPending(lease)
					if err != nil {
						t.Fatal(err)
					}
					if member != nil && member.identity() == workflowRunConcurrencyMember(pending).identity() || holderAfter != nil && holderAfter.identity() == workflowRunConcurrencyMember(pending).identity() {
						t.Fatal("cancelled run retained concurrency membership")
					}
					if replacement != nil {
						if err := f.reconciler.Get(ctx, client.ObjectKeyFromObject(replacement), replacement); err != nil {
							t.Fatal(err)
						}
						if replacement.Spec.CancelRequested {
							t.Fatal("cancelled run requested cancellation of its replacement")
						}
						if !releaseHolder && (member == nil || member.identity() != workflowRunConcurrencyMember(replacement).identity()) {
							t.Fatalf("replacement lost its pending slot: %#v", member)
						}
					}
					for _, job := range f.jobs(f.root) {
						if job.Status.Result == actionsv1alpha1.WorkflowJobResultCancelled {
							t.Fatalf("cancelling pending run changed holder job %q", job.Name)
						}
					}
				})
			}
		}
	}
}

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
