package controller

import (
	"context"
	"testing"
	"time"

	actionsv1alpha1 "github.com/kelos-dev/open-actions/api/v1alpha1"
	"github.com/kelos-dev/open-actions/internal/runner"
	"github.com/kelos-dev/open-actions/internal/workflowstatus"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

func TestWorkflowJobSchedulingTracksOwnedPod(t *testing.T) {
	placedAt := metav1.NewTime(time.Now().Add(-time.Minute).Truncate(time.Second))
	startedAt := metav1.NewTime(placedAt.Add(10 * time.Second))
	for _, test := range []struct {
		name          string
		pod           bool
		foreignOwner  bool
		scheduled     corev1.ConditionStatus
		container     string
		wantScheduled metav1.ConditionStatus
		wantReason    string
		wantStarted   bool
	}{
		{name: "no Pod", wantScheduled: metav1.ConditionFalse, wantReason: "PodPending"},
		{name: "Pod without scheduling condition", pod: true, wantScheduled: metav1.ConditionFalse, wantReason: "PodPending"},
		{name: "unschedulable Pod", pod: true, scheduled: corev1.ConditionFalse, wantScheduled: metav1.ConditionFalse, wantReason: "PodPending"},
		{name: "scheduled Pod awaiting container", pod: true, scheduled: corev1.ConditionTrue, wantScheduled: metav1.ConditionTrue, wantReason: "PodScheduled"},
		{name: "running runner", pod: true, scheduled: corev1.ConditionTrue, container: runner.ContainerName, wantScheduled: metav1.ConditionTrue, wantReason: "PodScheduled", wantStarted: true},
		{name: "running sidecar", pod: true, scheduled: corev1.ConditionTrue, container: "docker", wantScheduled: metav1.ConditionTrue, wantReason: "PodScheduled"},
		{name: "unrelated scheduled Pod", pod: true, scheduled: corev1.ConditionTrue, foreignOwner: true, wantScheduled: metav1.ConditionFalse, wantReason: "PodPending"},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			scheme := runnerTestScheme(t)
			job := &actionsv1alpha1.WorkflowJob{
				ObjectMeta: metav1.ObjectMeta{Name: "build", Namespace: "default", UID: types.UID("workflow-job-uid"), Generation: 1},
			}
			clusterClient := fake.NewClientBuilder().WithScheme(scheme).
				WithStatusSubresource(&actionsv1alpha1.WorkflowJob{}, &batchv1.Job{}).
				WithObjects(job).Build()
			reconciler := &RunnerReconciler{Client: clusterClient, APIReader: clusterClient}
			if err := reconciler.assignWorkflowJob(ctx, job, "runner", time.Now()); err != nil {
				t.Fatal(err)
			}
			assigned := meta.FindStatusCondition(job.Status.Conditions, actionsv1alpha1.WorkflowJobConditionAssigned)
			if assigned == nil || assigned.Status != metav1.ConditionTrue || job.Status.RunnerRef == nil || job.Status.RunnerRef.Name != "runner" {
				t.Fatalf("Runner assignment = %#v", job.Status)
			}
			assignedAt := assigned.LastTransitionTime
			if !meta.IsStatusConditionFalse(job.Status.Conditions, actionsv1alpha1.WorkflowJobConditionScheduled) {
				t.Fatalf("assignment marked job scheduled: %#v", job.Status.Conditions)
			}
			nativeJob := &batchv1.Job{
				ObjectMeta: metav1.ObjectMeta{Name: job.Name, Namespace: job.Namespace, UID: types.UID("native-job-uid")},
				Status:     batchv1.JobStatus{Active: 1, StartTime: &placedAt},
			}
			if err := controllerutil.SetControllerReference(job, nativeJob, scheme); err != nil {
				t.Fatal(err)
			}
			if err := clusterClient.Create(ctx, nativeJob); err != nil {
				t.Fatal(err)
			}
			if test.pod {
				pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
					Name: "build-pod", Namespace: job.Namespace,
					Labels: map[string]string{actionsv1alpha1.LabelWorkflowJobUID: string(job.UID)},
				}}
				if test.scheduled != "" {
					pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodScheduled, Status: test.scheduled, LastTransitionTime: placedAt}}
					if test.scheduled == corev1.ConditionFalse {
						pod.Status.Conditions[0].Reason = "Unschedulable"
						pod.Status.Conditions[0].Message = "Insufficient cpu"
					}
				}
				if test.container != "" {
					pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
						Name: test.container, State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: startedAt}},
					}}
				}
				owner := nativeJob.DeepCopy()
				if test.foreignOwner {
					owner.UID = "unrelated-job-uid"
				}
				if err := controllerutil.SetControllerReference(owner, pod, scheme); err != nil {
					t.Fatal(err)
				}
				if err := clusterClient.Create(ctx, pod); err != nil {
					t.Fatal(err)
				}
			}
			for range 2 {
				found, terminal, err := reconciler.observeNativeJob(ctx, job)
				if err != nil || !found || terminal {
					t.Fatalf("observe native Job: found %t, terminal %t, error %v", found, terminal, err)
				}
				if err := clusterClient.Get(ctx, client.ObjectKeyFromObject(job), job); err != nil {
					t.Fatal(err)
				}
				scheduled := meta.FindStatusCondition(job.Status.Conditions, actionsv1alpha1.WorkflowJobConditionScheduled)
				if scheduled == nil || scheduled.Status != test.wantScheduled || scheduled.Reason != test.wantReason || scheduled.ObservedGeneration != job.Generation {
					t.Fatalf("Scheduled condition = %#v", scheduled)
				}
				if test.wantScheduled == metav1.ConditionTrue && !scheduled.LastTransitionTime.Equal(&placedAt) {
					t.Fatalf("scheduling time = %s, want %s", scheduled.LastTransitionTime, placedAt)
				}
				if test.scheduled == corev1.ConditionFalse && scheduled.Message != "Insufficient cpu" {
					t.Fatalf("scheduling failure message = %q", scheduled.Message)
				}
				if got := meta.FindStatusCondition(job.Status.Conditions, actionsv1alpha1.WorkflowJobConditionAssigned); !got.LastTransitionTime.Equal(&assignedAt) {
					t.Fatalf("assignment time = %s, want %s", got.LastTransitionTime, assignedAt)
				}
				wantStatus, wantReason := "Queued", "JobPending"
				if test.wantStarted {
					wantStatus, wantReason = "Running", "JobRunning"
					if job.Status.StartTime == nil || !job.Status.StartTime.Equal(&startedAt) {
						t.Fatalf("start time = %v, want %s", job.Status.StartTime, startedAt)
					}
				} else if job.Status.StartTime != nil {
					t.Fatalf("pending job start time = %s", job.Status.StartTime)
				}
				if got := workflowstatus.Job(job); got != wantStatus {
					t.Fatalf("job status = %q, want %q", got, wantStatus)
				}
				if got := meta.FindStatusCondition(job.Status.Conditions, actionsv1alpha1.WorkflowJobConditionSucceeded); got == nil || got.Status != metav1.ConditionUnknown || got.Reason != wantReason {
					t.Fatalf("Succeeded condition = %#v", got)
				}
			}
		})
	}
}

func TestWorkflowJobAssignmentPreservesRecordedTime(t *testing.T) {
	assignedAt := metav1.NewTime(time.Now().Add(-jobStartTimeout - time.Minute).Truncate(time.Second))
	job := &actionsv1alpha1.WorkflowJob{
		ObjectMeta: metav1.ObjectMeta{Name: "build", Namespace: "default"},
		Status: actionsv1alpha1.WorkflowJobStatus{
			RunnerRef: &corev1.LocalObjectReference{Name: "runner"},
			Conditions: []metav1.Condition{{
				Type: actionsv1alpha1.WorkflowJobConditionScheduled, Status: metav1.ConditionTrue,
				Reason: "RunnerAssigned", LastTransitionTime: assignedAt,
			}},
		},
	}
	scheme := runnerTestScheme(t)
	clusterClient := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&actionsv1alpha1.WorkflowJob{}).WithObjects(job).Build()
	reconciler := &RunnerReconciler{Client: clusterClient}
	for range 2 {
		if err := reconciler.updateWorkflowJobAssigned(context.Background(), job); err != nil {
			t.Fatal(err)
		}
		if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(job), job); err != nil {
			t.Fatal(err)
		}
		assigned := meta.FindStatusCondition(job.Status.Conditions, actionsv1alpha1.WorkflowJobConditionAssigned)
		if assigned == nil || !assigned.LastTransitionTime.Equal(&assignedAt) {
			t.Fatalf("assignment time = %#v, want %s", assigned, assignedAt)
		}
		if !meta.IsStatusConditionFalse(job.Status.Conditions, actionsv1alpha1.WorkflowJobConditionScheduled) {
			t.Fatalf("unobserved Pod scheduling = %#v", job.Status.Conditions)
		}
	}
	expired, err := reconciler.failExpiredPreStart(context.Background(), job, "Pod creation unavailable")
	if err != nil || !expired {
		t.Fatalf("expired assignment: expired %t, error %v", expired, err)
	}
}
