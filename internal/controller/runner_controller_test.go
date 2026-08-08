package controller

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	actionsv1alpha1 "github.com/kelos-dev/open-actions/api/v1alpha1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

func TestRunnerBuildsOwnedJob(t *testing.T) {
	scheme := runnerTestScheme(t)
	reconciler := &RunnerReconciler{Client: fake.NewClientBuilder().WithScheme(scheme).Build()}
	run := &actionsv1alpha1.WorkflowRun{ObjectMeta: metav1.ObjectMeta{Name: "ci", Namespace: "default", UID: types.UID("run-uid")}}
	gateway := &actionsv1alpha1.ActionsGateway{
		ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: "default", UID: types.UID("gateway-uid")},
	}
	runnerObject := &actionsv1alpha1.Runner{
		ObjectMeta: metav1.ObjectMeta{Name: "runner-1", Namespace: "default", UID: types.UID("runner-uid")},
		Spec: actionsv1alpha1.RunnerSpec{Execution: actionsv1alpha1.RunnerExecutionSpec{
			Image: "runner:test",
			Resources: &actionsv1alpha1.RunnerResources{Requests: actionsv1alpha1.RunnerResourceList{
				corev1.ResourceCPU: resource.MustParse("1"),
			}},
		}},
	}
	workflowJob := &actionsv1alpha1.WorkflowJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "ci-build",
			Namespace: "default",
			UID:       types.UID("workflow-job-uid"),
			Labels:    map[string]string{actionsv1alpha1.LabelWorkflowJob: "build"},
		},
		Spec: actionsv1alpha1.WorkflowJobSpec{JobID: "build"},
	}
	job, err := reconciler.buildJob(workflowJob, run, gateway, runnerObject)
	if err != nil {
		t.Fatalf("build job: %v", err)
	}
	container := job.Spec.Template.Spec.Containers[0]
	if container.Image != "runner:test" {
		t.Errorf("image = %q", container.Image)
	}
	if container.Resources.Requests.Cpu().String() != "1" {
		t.Errorf("cpu request = %s", container.Resources.Requests.Cpu().String())
	}
	if strings.Join(container.Args, " ") != "--job-file=/var/run/open-actions/job.json --workspace=/workspace" {
		t.Errorf("args = %v", container.Args)
	}
	if job.Labels[actionsv1alpha1.LabelWorkflowJob] != "build" {
		t.Errorf("workflow job label = %q", job.Labels[actionsv1alpha1.LabelWorkflowJob])
	}
	if len(job.OwnerReferences) != 1 || job.OwnerReferences[0].UID != workflowJob.UID {
		t.Errorf("owner references = %#v", job.OwnerReferences)
	}
	if job.Spec.TTLSecondsAfterFinished != nil {
		t.Error("job TTL must not start until the terminal result is recorded")
	}
	if job.Spec.ActiveDeadlineSeconds == nil || *job.Spec.ActiveDeadlineSeconds != jobTimeoutSeconds {
		t.Errorf("active deadline = %v", job.Spec.ActiveDeadlineSeconds)
	}
	if job.Labels[actionsv1alpha1.LabelRunnerUID] != string(runnerObject.UID) {
		t.Errorf("runner label = %q", job.Labels[actionsv1alpha1.LabelRunnerUID])
	}
	if job.Spec.Template.Spec.RestartPolicy != corev1.RestartPolicyNever {
		t.Errorf("restart policy = %q", job.Spec.Template.Spec.RestartPolicy)
	}
	if job.Spec.Template.Spec.AutomountServiceAccountToken == nil || *job.Spec.Template.Spec.AutomountServiceAccountToken {
		t.Error("service account token must not be mounted")
	}
	if job.Spec.Template.Spec.SecurityContext == nil ||
		job.Spec.Template.Spec.SecurityContext.RunAsNonRoot == nil || !*job.Spec.Template.Spec.SecurityContext.RunAsNonRoot ||
		job.Spec.Template.Spec.SecurityContext.SeccompProfile == nil || job.Spec.Template.Spec.SecurityContext.SeccompProfile.Type != corev1.SeccompProfileTypeRuntimeDefault {
		t.Errorf("pod security context = %#v", job.Spec.Template.Spec.SecurityContext)
	}
	if container.SecurityContext == nil ||
		container.SecurityContext.AllowPrivilegeEscalation == nil || *container.SecurityContext.AllowPrivilegeEscalation ||
		container.SecurityContext.Capabilities == nil || !slices.Equal(container.SecurityContext.Capabilities.Drop, []corev1.Capability{"ALL"}) {
		t.Errorf("container security context = %#v", container.SecurityContext)
	}
	if len(job.Spec.Template.Spec.Volumes) != 2 {
		t.Errorf("volumes = %#v", job.Spec.Template.Spec.Volumes)
	}
}

func TestRunnerClaimsOldestMatchingWorkflowJobInGateway(t *testing.T) {
	scheme := runnerTestScheme(t)
	created := metav1.NewTime(time.Unix(100, 0))
	runnerObject := &actionsv1alpha1.Runner{
		ObjectMeta: metav1.ObjectMeta{Name: "runner-1", Namespace: "default", UID: types.UID("runner-uid")},
		Spec: actionsv1alpha1.RunnerSpec{
			Execution: actionsv1alpha1.RunnerExecutionSpec{Image: "runner:test"},
			Labels:    []string{"self-hosted", "linux", "arm64"},
		},
	}
	gateway := &actionsv1alpha1.ActionsGateway{ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: "default", UID: types.UID("gateway-uid")}}
	run := &actionsv1alpha1.WorkflowRun{
		ObjectMeta: metav1.ObjectMeta{Name: "ci", Namespace: "default", UID: types.UID("run-uid")},
		Status: actionsv1alpha1.WorkflowRunStatus{Conditions: []metav1.Condition{
			plannedCondition(metav1.ConditionTrue, "JobsPlanned"),
		}},
	}
	matching := &actionsv1alpha1.WorkflowJob{
		ObjectMeta: metav1.ObjectMeta{Name: "matching", Namespace: "default", CreationTimestamp: created, Labels: map[string]string{actionsv1alpha1.LabelGatewayUID: string(gateway.UID)}, Annotations: map[string]string{actionsv1alpha1.AnnotationGatewayName: gateway.Name}},
		Spec: actionsv1alpha1.WorkflowJobSpec{
			WorkflowRunRef: corev1.LocalObjectReference{Name: run.Name},
			RunsOn:         []string{"linux", "arm64"},
		},
	}
	newer := matching.DeepCopy()
	newer.Name = "newer"
	newer.CreationTimestamp = metav1.NewTime(created.Add(time.Minute))
	wrongGateway := matching.DeepCopy()
	wrongGateway.Name = "wrong-gateway"
	wrongGateway.Labels = map[string]string{actionsv1alpha1.LabelGatewayUID: "other-gateway"}
	wrongLabels := matching.DeepCopy()
	wrongLabels.Name = "wrong-labels"
	wrongLabels.Spec.RunsOn = []string{"windows"}

	clusterClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithIndex(&actionsv1alpha1.WorkflowJob{}, workflowJobQueuedIndex, indexQueuedWorkflowJob).
		WithIndex(&actionsv1alpha1.WorkflowJob{}, workflowJobGatewayNameIndex, indexWorkflowJobGatewayName).
		WithStatusSubresource(&actionsv1alpha1.Runner{}, &actionsv1alpha1.WorkflowJob{}).
		WithObjects(runnerObject, gateway, run, matching, newer, wrongGateway, wrongLabels).
		Build()
	reconciler := &RunnerReconciler{Client: clusterClient, APIReader: clusterClient}
	claimed, err := reconciler.claimWorkflowJob(context.Background(), runnerObject, gateway)
	if err != nil {
		t.Fatal(err)
	}
	if claimed == nil || claimed.Name != matching.Name {
		t.Fatalf("claimed WorkflowJob = %#v", claimed)
	}

	storedJob := &actionsv1alpha1.WorkflowJob{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(matching), storedJob); err != nil {
		t.Fatal(err)
	}
	if storedJob.Status.RunnerRef == nil || storedJob.Status.RunnerRef.Name != runnerObject.Name {
		t.Errorf("runnerRef = %#v", storedJob.Status.RunnerRef)
	}
	scheduled := meta.FindStatusCondition(storedJob.Status.Conditions, actionsv1alpha1.WorkflowJobConditionScheduled)
	if scheduled == nil || scheduled.Status != metav1.ConditionTrue || scheduled.Reason != "RunnerAssigned" {
		t.Errorf("scheduled condition = %#v", scheduled)
	}
	storedRunner := &actionsv1alpha1.Runner{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(runnerObject), storedRunner); err != nil {
		t.Fatal(err)
	}
	if storedRunner.Status.WorkflowJobRef == nil || storedRunner.Status.WorkflowJobRef.Name != matching.Name {
		t.Errorf("workflowJobRef = %#v", storedRunner.Status.WorkflowJobRef)
	}
	ready := meta.FindStatusCondition(storedRunner.Status.Conditions, actionsv1alpha1.RunnerConditionReady)
	if ready == nil || ready.Status != metav1.ConditionTrue || ready.Reason != "Ready" {
		t.Errorf("ready condition = %#v", ready)
	}
	busy := meta.FindStatusCondition(storedRunner.Status.Conditions, actionsv1alpha1.RunnerConditionBusy)
	if busy == nil || busy.Status != metav1.ConditionTrue || busy.Reason != "JobAssigned" {
		t.Errorf("busy condition = %#v", busy)
	}
	staleJob := &actionsv1alpha1.WorkflowJob{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(wrongGateway), staleJob); err != nil {
		t.Fatal(err)
	}
	staleCondition := meta.FindStatusCondition(staleJob.Status.Conditions, actionsv1alpha1.WorkflowJobConditionSucceeded)
	if staleCondition == nil || staleCondition.Status != metav1.ConditionFalse || staleCondition.Reason != "GatewayRecreated" {
		t.Fatalf("recreated gateway condition = %#v", staleCondition)
	}
}

func TestRunnerDoesNotClaimWorkflowJobBeforePlanningCompletes(t *testing.T) {
	scheme := runnerTestScheme(t)
	runnerObject := &actionsv1alpha1.Runner{
		ObjectMeta: metav1.ObjectMeta{Name: "runner-1", Namespace: "default", UID: types.UID("runner-uid")},
		Spec: actionsv1alpha1.RunnerSpec{
			Execution: actionsv1alpha1.RunnerExecutionSpec{Image: "runner:test"},
			Labels:    []string{"linux"},
		},
	}
	gateway := &actionsv1alpha1.ActionsGateway{ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: "default", UID: types.UID("gateway-uid")}}
	run := &actionsv1alpha1.WorkflowRun{ObjectMeta: metav1.ObjectMeta{Name: "ci", Namespace: "default", UID: types.UID("run-uid")}}
	workflowJob := &actionsv1alpha1.WorkflowJob{
		ObjectMeta: metav1.ObjectMeta{Name: "build", Namespace: "default", Labels: map[string]string{actionsv1alpha1.LabelGatewayUID: string(gateway.UID)}, Annotations: map[string]string{actionsv1alpha1.AnnotationGatewayName: gateway.Name}},
		Spec: actionsv1alpha1.WorkflowJobSpec{
			WorkflowRunRef: corev1.LocalObjectReference{Name: run.Name},
			RunsOn:         []string{"linux"},
		},
	}
	clusterClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithIndex(&actionsv1alpha1.WorkflowJob{}, workflowJobQueuedIndex, indexQueuedWorkflowJob).
		WithIndex(&actionsv1alpha1.WorkflowJob{}, workflowJobGatewayNameIndex, indexWorkflowJobGatewayName).
		WithStatusSubresource(&actionsv1alpha1.Runner{}, &actionsv1alpha1.WorkflowJob{}).
		WithObjects(runnerObject, gateway, run, workflowJob).
		Build()
	reconciler := &RunnerReconciler{Client: clusterClient, APIReader: clusterClient}
	claimed, err := reconciler.claimWorkflowJob(context.Background(), runnerObject, gateway)
	if err != nil {
		t.Fatal(err)
	}
	if claimed != nil {
		t.Fatalf("claimed WorkflowJob before planning completed: %s", claimed.Name)
	}
	stored := &actionsv1alpha1.WorkflowJob{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(workflowJob), stored); err != nil {
		t.Fatal(err)
	}
	if stored.Status.RunnerRef != nil {
		t.Fatalf("runnerRef = %#v", stored.Status.RunnerRef)
	}
}

func TestRunnerDoesNotClaimAnotherJobWhenLiveStatusIsBusy(t *testing.T) {
	scheme := runnerTestScheme(t)
	runnerObject := &actionsv1alpha1.Runner{
		ObjectMeta: metav1.ObjectMeta{Name: "runner-1", Namespace: "default", UID: types.UID("runner-uid")},
		Spec:       actionsv1alpha1.RunnerSpec{Labels: []string{"linux"}},
	}
	liveRunner := runnerObject.DeepCopy()
	liveRunner.Status.WorkflowJobRef = &corev1.LocalObjectReference{Name: "assigned"}
	gateway := &actionsv1alpha1.ActionsGateway{ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: "default", UID: types.UID("gateway-uid")}}
	run := &actionsv1alpha1.WorkflowRun{
		ObjectMeta: metav1.ObjectMeta{Name: "ci", Namespace: "default"},
		Status:     actionsv1alpha1.WorkflowRunStatus{Conditions: []metav1.Condition{plannedCondition(metav1.ConditionTrue, "JobsPlanned")}},
	}
	workflowJob := &actionsv1alpha1.WorkflowJob{
		ObjectMeta: metav1.ObjectMeta{
			Name: "queued", Namespace: "default",
			Labels:      map[string]string{actionsv1alpha1.LabelGatewayUID: string(gateway.UID)},
			Annotations: map[string]string{actionsv1alpha1.AnnotationGatewayName: gateway.Name},
		},
		Spec: actionsv1alpha1.WorkflowJobSpec{WorkflowRunRef: corev1.LocalObjectReference{Name: run.Name}, RunsOn: []string{"linux"}},
	}
	cachedClient := fake.NewClientBuilder().WithScheme(scheme).
		WithIndex(&actionsv1alpha1.WorkflowJob{}, workflowJobQueuedIndex, indexQueuedWorkflowJob).
		WithIndex(&actionsv1alpha1.WorkflowJob{}, workflowJobGatewayNameIndex, indexWorkflowJobGatewayName).
		WithStatusSubresource(&actionsv1alpha1.Runner{}, &actionsv1alpha1.WorkflowJob{}).
		WithObjects(runnerObject, gateway, run, workflowJob).Build()
	liveReader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(liveRunner, run).Build()
	reconciler := &RunnerReconciler{Client: cachedClient, APIReader: liveReader}

	claimed, err := reconciler.claimWorkflowJob(context.Background(), runnerObject, gateway)
	if !errors.Is(err, errRunnerAlreadyAssigned) || claimed != nil {
		t.Fatalf("claimWorkflowJob() = %#v, %v", claimed, err)
	}
	stored := &actionsv1alpha1.WorkflowJob{}
	if err := cachedClient.Get(context.Background(), client.ObjectKeyFromObject(workflowJob), stored); err != nil {
		t.Fatal(err)
	}
	if stored.Status.RunnerRef != nil {
		t.Fatalf("queued WorkflowJob runnerRef = %#v", stored.Status.RunnerRef)
	}
}

func TestAssignedWorkflowJobUsesLiveReader(t *testing.T) {
	scheme := runnerTestScheme(t)
	runnerObject := &actionsv1alpha1.Runner{
		ObjectMeta: metav1.ObjectMeta{Name: "runner", Namespace: "default", UID: types.UID("runner-uid")},
		Status:     actionsv1alpha1.RunnerStatus{WorkflowJobRef: &corev1.LocalObjectReference{Name: "build"}},
	}
	staleJob := &actionsv1alpha1.WorkflowJob{ObjectMeta: metav1.ObjectMeta{Name: "build", Namespace: "default", UID: types.UID("job-uid")}}
	liveJob := staleJob.DeepCopy()
	liveJob.Status.RunnerRef = &corev1.LocalObjectReference{Name: runnerObject.Name}
	cachedClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&actionsv1alpha1.Runner{}).WithObjects(runnerObject, staleJob).Build()
	liveReader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(liveJob).Build()
	reconciler := &RunnerReconciler{Client: cachedClient, APIReader: liveReader}

	assigned, err := reconciler.assignedWorkflowJob(context.Background(), runnerObject)
	if err != nil {
		t.Fatal(err)
	}
	if assigned == nil || assigned.Name != liveJob.Name || assigned.Status.RunnerRef == nil {
		t.Fatalf("assigned WorkflowJob = %#v", assigned)
	}
}

func TestRunnerDoesNotClaimWorkflowJobFromDeletingRun(t *testing.T) {
	scheme := runnerTestScheme(t)
	deletionTime := metav1.Now()
	runnerObject := &actionsv1alpha1.Runner{
		ObjectMeta: metav1.ObjectMeta{Name: "runner-1", Namespace: "default"},
		Spec:       actionsv1alpha1.RunnerSpec{Labels: []string{"linux"}},
	}
	gateway := &actionsv1alpha1.ActionsGateway{ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: "default", UID: types.UID("gateway-uid")}}
	run := &actionsv1alpha1.WorkflowRun{
		ObjectMeta: metav1.ObjectMeta{Name: "ci", Namespace: "default", UID: types.UID("run-uid"), DeletionTimestamp: &deletionTime, Finalizers: []string{"test"}},
		Status: actionsv1alpha1.WorkflowRunStatus{Conditions: []metav1.Condition{
			plannedCondition(metav1.ConditionTrue, "JobsPlanned"),
		}},
	}
	workflowJob := &actionsv1alpha1.WorkflowJob{
		ObjectMeta: metav1.ObjectMeta{
			Name: "build", Namespace: "default",
			Labels:      map[string]string{actionsv1alpha1.LabelGatewayUID: string(gateway.UID)},
			Annotations: map[string]string{actionsv1alpha1.AnnotationGatewayName: gateway.Name},
		},
		Spec: actionsv1alpha1.WorkflowJobSpec{
			WorkflowRunRef: corev1.LocalObjectReference{Name: run.Name},
			RunsOn:         []string{"linux"},
		},
	}
	clusterClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithIndex(&actionsv1alpha1.WorkflowJob{}, workflowJobQueuedIndex, indexQueuedWorkflowJob).
		WithIndex(&actionsv1alpha1.WorkflowJob{}, workflowJobGatewayNameIndex, indexWorkflowJobGatewayName).
		WithStatusSubresource(&actionsv1alpha1.Runner{}, &actionsv1alpha1.WorkflowJob{}).
		WithObjects(runnerObject, gateway, run, workflowJob).
		Build()
	reconciler := &RunnerReconciler{Client: clusterClient, APIReader: clusterClient}

	claimed, err := reconciler.claimWorkflowJob(context.Background(), runnerObject, gateway)
	if err != nil {
		t.Fatal(err)
	}
	if claimed != nil {
		t.Fatalf("claimed WorkflowJob from deleting WorkflowRun: %s", claimed.Name)
	}
}

func TestDeletingWorkflowJobIsNotQueued(t *testing.T) {
	deletionTime := metav1.Now()
	workflowJob := &actionsv1alpha1.WorkflowJob{ObjectMeta: metav1.ObjectMeta{DeletionTimestamp: &deletionTime}}
	if values := indexQueuedWorkflowJob(workflowJob); len(values) != 0 {
		t.Fatalf("queue index = %#v", values)
	}
}

func TestTerminalWorkflowJobStatusIsDurableBeforeCredentialCleanup(t *testing.T) {
	scheme := runnerTestScheme(t)
	workflowJob := &actionsv1alpha1.WorkflowJob{
		TypeMeta:   metav1.TypeMeta{APIVersion: actionsv1alpha1.GroupVersion.String(), Kind: "WorkflowJob"},
		ObjectMeta: metav1.ObjectMeta{Name: "build", Namespace: "default", UID: types.UID("workflow-job-uid")},
		Status:     actionsv1alpha1.WorkflowJobStatus{RunnerRef: &corev1.LocalObjectReference{Name: "runner"}},
	}
	nativeJob := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: workflowJob.Name, Namespace: workflowJob.Namespace},
		Status: batchv1.JobStatus{Conditions: []batchv1.JobCondition{{
			Type: batchv1.JobComplete, Status: corev1.ConditionTrue,
		}}},
	}
	if err := controllerutil.SetControllerReference(workflowJob, nativeJob, scheme); err != nil {
		t.Fatal(err)
	}
	authSecret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: childName(workflowJob.Name, "auth"), Namespace: workflowJob.Namespace}}
	clusterClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&actionsv1alpha1.WorkflowJob{}, &batchv1.Job{}).
		WithObjects(workflowJob, nativeJob, authSecret).
		Build()
	reconciler := &RunnerReconciler{Client: clusterClient, APIReader: clusterClient}
	found, terminal, err := reconciler.observeNativeJob(context.Background(), workflowJob)
	if !found || !terminal || err == nil {
		t.Fatalf("observeNativeJob() = found %t, terminal %t, error %v", found, terminal, err)
	}
	stored := &actionsv1alpha1.WorkflowJob{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(workflowJob), stored); err != nil {
		t.Fatal(err)
	}
	if !terminalWorkflowJob(stored) {
		t.Fatal("WorkflowJob terminal result was not persisted before credential cleanup")
	}
	storedNativeJob := &batchv1.Job{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(nativeJob), storedNativeJob); err != nil {
		t.Fatal(err)
	}
	if storedNativeJob.Spec.TTLSecondsAfterFinished != nil {
		t.Fatal("native Job TTL started before credential cleanup completed")
	}
}

func TestExpiredPreStartFailureTerminatesJobAndCleansCredentials(t *testing.T) {
	scheme := runnerTestScheme(t)
	workflowJob := &actionsv1alpha1.WorkflowJob{
		TypeMeta:   metav1.TypeMeta{APIVersion: actionsv1alpha1.GroupVersion.String(), Kind: "WorkflowJob"},
		ObjectMeta: metav1.ObjectMeta{Name: "build", Namespace: "default", UID: types.UID("workflow-job-uid")},
		Status: actionsv1alpha1.WorkflowJobStatus{
			RunnerRef: &corev1.LocalObjectReference{Name: "runner"},
			Conditions: []metav1.Condition{{
				Type: actionsv1alpha1.WorkflowJobConditionScheduled, Status: metav1.ConditionTrue,
				Reason: "RunnerAssigned", LastTransitionTime: metav1.NewTime(time.Now().Add(-jobStartTimeout - time.Second)),
			}},
		},
	}
	authSecret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: childName(workflowJob.Name, "auth"), Namespace: workflowJob.Namespace}}
	if err := controllerutil.SetControllerReference(workflowJob, authSecret, scheme); err != nil {
		t.Fatal(err)
	}
	clusterClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&actionsv1alpha1.WorkflowJob{}).
		WithObjects(workflowJob, authSecret).
		Build()
	reconciler := &RunnerReconciler{Client: clusterClient, APIReader: clusterClient}
	expired, err := reconciler.failExpiredPreStart(context.Background(), workflowJob, "admission unavailable")
	if err != nil {
		t.Fatal(err)
	}
	if !expired {
		t.Fatal("expired pre-start failure remained retryable")
	}
	stored := &actionsv1alpha1.WorkflowJob{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(workflowJob), stored); err != nil {
		t.Fatal(err)
	}
	condition := meta.FindStatusCondition(stored.Status.Conditions, actionsv1alpha1.WorkflowJobConditionSucceeded)
	if condition == nil || condition.Status != metav1.ConditionFalse || condition.Reason != "JobStartFailed" {
		t.Fatalf("succeeded condition = %#v", condition)
	}
	err = clusterClient.Get(context.Background(), client.ObjectKeyFromObject(authSecret), &corev1.Secret{})
	if !apierrors.IsNotFound(err) {
		t.Fatalf("authentication Secret still exists: %v", err)
	}
}

func TestMissingPlanFailsAssignedWorkflowJob(t *testing.T) {
	scheme := runnerTestScheme(t)
	run := &actionsv1alpha1.WorkflowRun{
		TypeMeta:   metav1.TypeMeta{APIVersion: actionsv1alpha1.GroupVersion.String(), Kind: "WorkflowRun"},
		ObjectMeta: metav1.ObjectMeta{Name: "ci", Namespace: "default", UID: types.UID("run-uid")},
		Spec:       actionsv1alpha1.WorkflowRunSpec{GatewayRef: corev1.LocalObjectReference{Name: "gateway"}},
	}
	workflowJob := &actionsv1alpha1.WorkflowJob{
		TypeMeta: metav1.TypeMeta{APIVersion: actionsv1alpha1.GroupVersion.String(), Kind: "WorkflowJob"},
		ObjectMeta: metav1.ObjectMeta{
			Name: "build", Namespace: "default", UID: types.UID("job-uid"),
		},
		Spec:   actionsv1alpha1.WorkflowJobSpec{WorkflowRunRef: corev1.LocalObjectReference{Name: run.Name}},
		Status: actionsv1alpha1.WorkflowJobStatus{RunnerRef: &corev1.LocalObjectReference{Name: "runner"}},
	}
	if err := controllerutil.SetControllerReference(run, workflowJob, scheme); err != nil {
		t.Fatal(err)
	}
	authSecret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: childName(workflowJob.Name, "auth"), Namespace: workflowJob.Namespace}}
	if err := controllerutil.SetControllerReference(workflowJob, authSecret, scheme); err != nil {
		t.Fatal(err)
	}
	gateway := &actionsv1alpha1.ActionsGateway{ObjectMeta: metav1.ObjectMeta{Name: "gateway", Namespace: "default"}}
	runnerObject := &actionsv1alpha1.Runner{ObjectMeta: metav1.ObjectMeta{Name: "runner", Namespace: "default"}}
	clusterClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&actionsv1alpha1.WorkflowJob{}).
		WithObjects(run, workflowJob, authSecret).
		Build()
	reconciler := &RunnerReconciler{Client: clusterClient, APIReader: clusterClient}
	terminal, err := reconciler.executeWorkflowJob(context.Background(), runnerObject, workflowJob, gateway)
	if err != nil {
		t.Fatal(err)
	}
	if !terminal {
		t.Fatal("missing plan did not terminate the WorkflowJob")
	}
	stored := &actionsv1alpha1.WorkflowJob{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(workflowJob), stored); err != nil {
		t.Fatal(err)
	}
	condition := meta.FindStatusCondition(stored.Status.Conditions, actionsv1alpha1.WorkflowJobConditionSucceeded)
	if condition == nil || condition.Status != metav1.ConditionFalse || condition.Reason != "PlanUnavailable" {
		t.Fatalf("succeeded condition = %#v", condition)
	}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(authSecret), &corev1.Secret{}); !apierrors.IsNotFound(err) {
		t.Fatalf("authentication Secret still exists: %v", err)
	}
}

func TestPlanReadUsesLiveReader(t *testing.T) {
	scheme := runnerTestScheme(t)
	run := &actionsv1alpha1.WorkflowRun{
		TypeMeta:   metav1.TypeMeta{APIVersion: actionsv1alpha1.GroupVersion.String(), Kind: "WorkflowRun"},
		ObjectMeta: metav1.ObjectMeta{Name: "ci", Namespace: "default", UID: types.UID("run-uid")},
		Spec:       actionsv1alpha1.WorkflowRunSpec{GatewayRef: corev1.LocalObjectReference{Name: "gateway"}},
	}
	workflowJob := &actionsv1alpha1.WorkflowJob{
		TypeMeta:   metav1.TypeMeta{APIVersion: actionsv1alpha1.GroupVersion.String(), Kind: "WorkflowJob"},
		ObjectMeta: metav1.ObjectMeta{Name: "build", Namespace: "default", UID: types.UID("job-uid")},
		Spec:       actionsv1alpha1.WorkflowJobSpec{WorkflowRunRef: corev1.LocalObjectReference{Name: run.Name}},
		Status:     actionsv1alpha1.WorkflowJobStatus{RunnerRef: &corev1.LocalObjectReference{Name: "runner"}},
	}
	if err := controllerutil.SetControllerReference(run, workflowJob, scheme); err != nil {
		t.Fatal(err)
	}
	plan := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: childName(workflowJob.Name, "plan"), Namespace: workflowJob.Namespace}}
	if err := controllerutil.SetControllerReference(workflowJob, plan, scheme); err != nil {
		t.Fatal(err)
	}
	cachedClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&actionsv1alpha1.WorkflowJob{}).WithObjects(run, workflowJob).Build()
	liveReader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(run, workflowJob, plan).Build()
	reconciler := &RunnerReconciler{Client: cachedClient, APIReader: liveReader}
	gateway := &actionsv1alpha1.ActionsGateway{
		ObjectMeta: metav1.ObjectMeta{Name: "gateway", Namespace: "default"},
		Spec: actionsv1alpha1.ActionsGatewaySpec{Source: actionsv1alpha1.ActionsGatewaySource{GitHub: &actionsv1alpha1.GitHubAppConfiguration{
			PrivateKeySecretRef: corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "credentials"}, Key: "private-key"},
		}}},
	}
	runnerObject := &actionsv1alpha1.Runner{ObjectMeta: metav1.ObjectMeta{Name: "runner", Namespace: "default"}}

	terminal, err := reconciler.executeWorkflowJob(context.Background(), runnerObject, workflowJob, gateway)
	if err == nil || terminal {
		t.Fatalf("executeWorkflowJob() = terminal %v, error %v", terminal, err)
	}
	stored := &actionsv1alpha1.WorkflowJob{}
	if err := cachedClient.Get(context.Background(), client.ObjectKeyFromObject(workflowJob), stored); err != nil {
		t.Fatal(err)
	}
	condition := meta.FindStatusCondition(stored.Status.Conditions, actionsv1alpha1.WorkflowJobConditionSucceeded)
	if condition != nil && condition.Reason == "PlanUnavailable" {
		t.Fatalf("live plan was treated as unavailable: %#v", condition)
	}
}

func TestMissingPlanDoesNotInterruptAnExistingNativeJob(t *testing.T) {
	scheme := runnerTestScheme(t)
	run := &actionsv1alpha1.WorkflowRun{
		TypeMeta:   metav1.TypeMeta{APIVersion: actionsv1alpha1.GroupVersion.String(), Kind: "WorkflowRun"},
		ObjectMeta: metav1.ObjectMeta{Name: "ci", Namespace: "default", UID: types.UID("run-uid")},
		Spec:       actionsv1alpha1.WorkflowRunSpec{GatewayRef: corev1.LocalObjectReference{Name: "gateway"}},
	}
	workflowJob := &actionsv1alpha1.WorkflowJob{
		TypeMeta:   metav1.TypeMeta{APIVersion: actionsv1alpha1.GroupVersion.String(), Kind: "WorkflowJob"},
		ObjectMeta: metav1.ObjectMeta{Name: "build", Namespace: "default", UID: types.UID("job-uid")},
		Spec:       actionsv1alpha1.WorkflowJobSpec{WorkflowRunRef: corev1.LocalObjectReference{Name: run.Name}},
		Status:     actionsv1alpha1.WorkflowJobStatus{RunnerRef: &corev1.LocalObjectReference{Name: "runner"}},
	}
	if err := controllerutil.SetControllerReference(run, workflowJob, scheme); err != nil {
		t.Fatal(err)
	}
	nativeJob := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: workflowJob.Name, Namespace: workflowJob.Namespace}}
	if err := controllerutil.SetControllerReference(workflowJob, nativeJob, scheme); err != nil {
		t.Fatal(err)
	}
	clusterClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&actionsv1alpha1.WorkflowJob{}, &batchv1.Job{}).
		WithObjects(run, workflowJob, nativeJob).
		Build()
	reconciler := &RunnerReconciler{Client: clusterClient, APIReader: clusterClient}
	gateway := &actionsv1alpha1.ActionsGateway{ObjectMeta: metav1.ObjectMeta{Name: "gateway", Namespace: "default"}}
	runnerObject := &actionsv1alpha1.Runner{ObjectMeta: metav1.ObjectMeta{Name: "runner", Namespace: "default"}}
	terminal, err := reconciler.executeWorkflowJob(context.Background(), runnerObject, workflowJob, gateway)
	if err != nil {
		t.Fatal(err)
	}
	if terminal {
		t.Fatal("an active native Job was failed after its plan was deleted")
	}
	stored := &actionsv1alpha1.WorkflowJob{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(workflowJob), stored); err != nil {
		t.Fatal(err)
	}
	condition := meta.FindStatusCondition(stored.Status.Conditions, actionsv1alpha1.WorkflowJobConditionSucceeded)
	if condition == nil || condition.Status != metav1.ConditionUnknown || condition.Reason != "JobRunning" {
		t.Fatalf("succeeded condition = %#v", condition)
	}
}

func TestDeletingWorkflowRunStopsAssignedJobBeforeExecution(t *testing.T) {
	scheme := runnerTestScheme(t)
	deletionTime := metav1.Now()
	run := &actionsv1alpha1.WorkflowRun{
		TypeMeta: metav1.TypeMeta{APIVersion: actionsv1alpha1.GroupVersion.String(), Kind: "WorkflowRun"},
		ObjectMeta: metav1.ObjectMeta{
			Name: "ci", Namespace: "default", UID: types.UID("run-uid"),
			DeletionTimestamp: &deletionTime, Finalizers: []string{"test"},
		},
		Spec: actionsv1alpha1.WorkflowRunSpec{GatewayRef: corev1.LocalObjectReference{Name: "gateway"}},
	}
	workflowJob := &actionsv1alpha1.WorkflowJob{
		TypeMeta:   metav1.TypeMeta{APIVersion: actionsv1alpha1.GroupVersion.String(), Kind: "WorkflowJob"},
		ObjectMeta: metav1.ObjectMeta{Name: "build", Namespace: "default", UID: types.UID("job-uid")},
		Spec:       actionsv1alpha1.WorkflowJobSpec{WorkflowRunRef: corev1.LocalObjectReference{Name: run.Name}},
		Status:     actionsv1alpha1.WorkflowJobStatus{RunnerRef: &corev1.LocalObjectReference{Name: "runner"}},
	}
	if err := controllerutil.SetControllerReference(run, workflowJob, scheme); err != nil {
		t.Fatal(err)
	}
	clusterClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&actionsv1alpha1.WorkflowJob{}).
		WithObjects(run, workflowJob).
		Build()
	reconciler := &RunnerReconciler{Client: clusterClient, APIReader: clusterClient}
	gateway := &actionsv1alpha1.ActionsGateway{ObjectMeta: metav1.ObjectMeta{Name: "gateway", Namespace: "default"}}
	runnerObject := &actionsv1alpha1.Runner{ObjectMeta: metav1.ObjectMeta{Name: "runner", Namespace: "default"}}

	terminal, err := reconciler.executeWorkflowJob(context.Background(), runnerObject, workflowJob, gateway)
	if err != nil {
		t.Fatal(err)
	}
	if !terminal {
		t.Fatal("assigned WorkflowJob remained active after WorkflowRun deletion was requested")
	}
	stored := &actionsv1alpha1.WorkflowJob{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(workflowJob), stored); err != nil {
		t.Fatal(err)
	}
	condition := meta.FindStatusCondition(stored.Status.Conditions, actionsv1alpha1.WorkflowJobConditionSucceeded)
	if condition == nil || condition.Status != metav1.ConditionFalse || condition.Reason != "CancellationRequested" {
		t.Fatalf("succeeded condition = %#v", condition)
	}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(workflowJob), &batchv1.Job{}); !apierrors.IsNotFound(err) {
		t.Fatalf("native Job exists: %v", err)
	}
	secretKey := client.ObjectKey{Namespace: workflowJob.Namespace, Name: childName(workflowJob.Name, "auth")}
	if err := clusterClient.Get(context.Background(), secretKey, &corev1.Secret{}); !apierrors.IsNotFound(err) {
		t.Fatalf("authentication Secret exists: %v", err)
	}
}

func TestMissingNativeJobDoesNotRestartExecution(t *testing.T) {
	scheme := runnerTestScheme(t)
	run := &actionsv1alpha1.WorkflowRun{
		TypeMeta:   metav1.TypeMeta{APIVersion: actionsv1alpha1.GroupVersion.String(), Kind: "WorkflowRun"},
		ObjectMeta: metav1.ObjectMeta{Name: "ci", Namespace: "default", UID: types.UID("run-uid")},
		Spec:       actionsv1alpha1.WorkflowRunSpec{GatewayRef: corev1.LocalObjectReference{Name: "gateway"}},
	}
	workflowJob := &actionsv1alpha1.WorkflowJob{
		TypeMeta:   metav1.TypeMeta{APIVersion: actionsv1alpha1.GroupVersion.String(), Kind: "WorkflowJob"},
		ObjectMeta: metav1.ObjectMeta{Name: "build", Namespace: "default", UID: types.UID("job-uid")},
		Spec:       actionsv1alpha1.WorkflowJobSpec{WorkflowRunRef: corev1.LocalObjectReference{Name: run.Name}},
		Status: actionsv1alpha1.WorkflowJobStatus{
			RunnerRef: &corev1.LocalObjectReference{Name: "runner"},
			StartTime: pointerTo(metav1.Now()),
			Conditions: []metav1.Condition{{
				Type: actionsv1alpha1.WorkflowJobConditionSucceeded, Status: metav1.ConditionUnknown, Reason: "JobRunning", LastTransitionTime: metav1.Now(),
			}},
		},
	}
	if err := controllerutil.SetControllerReference(run, workflowJob, scheme); err != nil {
		t.Fatal(err)
	}
	authSecret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: childName(workflowJob.Name, "auth"), Namespace: workflowJob.Namespace}}
	if err := controllerutil.SetControllerReference(workflowJob, authSecret, scheme); err != nil {
		t.Fatal(err)
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "build-pod", Namespace: "default", Labels: map[string]string{actionsv1alpha1.LabelWorkflowJobUID: string(workflowJob.UID)}},
		Status:     corev1.PodStatus{Phase: corev1.PodRunning},
	}
	clusterClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&actionsv1alpha1.WorkflowJob{}).
		WithObjects(run, workflowJob, authSecret, pod).
		Build()
	reconciler := &RunnerReconciler{Client: clusterClient, APIReader: clusterClient}
	gateway := &actionsv1alpha1.ActionsGateway{ObjectMeta: metav1.ObjectMeta{Name: "gateway", Namespace: "default"}}
	runnerObject := &actionsv1alpha1.Runner{ObjectMeta: metav1.ObjectMeta{Name: "runner", Namespace: "default"}}

	terminal, err := reconciler.executeWorkflowJob(context.Background(), runnerObject, workflowJob, gateway)
	if err != nil {
		t.Fatal(err)
	}
	if terminal {
		t.Fatal("WorkflowJob terminated while its Pod was still active")
	}
	if err := clusterClient.Delete(context.Background(), pod); err != nil {
		t.Fatal(err)
	}
	terminal, err = reconciler.executeWorkflowJob(context.Background(), runnerObject, workflowJob, gateway)
	if err != nil {
		t.Fatal(err)
	}
	if !terminal {
		t.Fatal("WorkflowJob did not fail after its native Job and Pod disappeared")
	}
	if err := clusterClient.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: workflowJob.Name}, &batchv1.Job{}); !apierrors.IsNotFound(err) {
		t.Fatalf("native Job was recreated: %v", err)
	}
	stored := &actionsv1alpha1.WorkflowJob{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(workflowJob), stored); err != nil {
		t.Fatal(err)
	}
	condition := meta.FindStatusCondition(stored.Status.Conditions, actionsv1alpha1.WorkflowJobConditionSucceeded)
	if condition == nil || condition.Status != metav1.ConditionFalse || condition.Reason != "ExecutionStateLost" {
		t.Fatalf("succeeded condition = %#v", condition)
	}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(authSecret), &corev1.Secret{}); !apierrors.IsNotFound(err) {
		t.Fatalf("authentication Secret still exists: %v", err)
	}
}

func TestAssignedWorkflowJobRejectsRecreatedGateway(t *testing.T) {
	scheme := runnerTestScheme(t)
	run := &actionsv1alpha1.WorkflowRun{
		TypeMeta:   metav1.TypeMeta{APIVersion: actionsv1alpha1.GroupVersion.String(), Kind: "WorkflowRun"},
		ObjectMeta: metav1.ObjectMeta{Name: "ci", Namespace: "default", UID: types.UID("run-uid")},
		Spec:       actionsv1alpha1.WorkflowRunSpec{GatewayRef: corev1.LocalObjectReference{Name: "gateway"}},
	}
	workflowJob := &actionsv1alpha1.WorkflowJob{
		TypeMeta: metav1.TypeMeta{APIVersion: actionsv1alpha1.GroupVersion.String(), Kind: "WorkflowJob"},
		ObjectMeta: metav1.ObjectMeta{
			Name: "build", Namespace: "default", UID: types.UID("job-uid"),
			Labels: map[string]string{actionsv1alpha1.LabelGatewayUID: "original-gateway-uid"},
		},
		Spec:   actionsv1alpha1.WorkflowJobSpec{WorkflowRunRef: corev1.LocalObjectReference{Name: run.Name}},
		Status: actionsv1alpha1.WorkflowJobStatus{RunnerRef: &corev1.LocalObjectReference{Name: "runner"}},
	}
	if err := controllerutil.SetControllerReference(run, workflowJob, scheme); err != nil {
		t.Fatal(err)
	}
	authSecret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: childName(workflowJob.Name, "auth"), Namespace: workflowJob.Namespace}}
	if err := controllerutil.SetControllerReference(workflowJob, authSecret, scheme); err != nil {
		t.Fatal(err)
	}
	clusterClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&actionsv1alpha1.WorkflowJob{}).WithObjects(run, workflowJob, authSecret).Build()
	reconciler := &RunnerReconciler{Client: clusterClient, APIReader: clusterClient}
	gateway := &actionsv1alpha1.ActionsGateway{ObjectMeta: metav1.ObjectMeta{Name: "gateway", Namespace: "default", UID: types.UID("replacement-gateway-uid")}}
	runnerObject := &actionsv1alpha1.Runner{ObjectMeta: metav1.ObjectMeta{Name: "runner", Namespace: "default"}}

	terminal, err := reconciler.executeWorkflowJob(context.Background(), runnerObject, workflowJob, gateway)
	if err != nil {
		t.Fatal(err)
	}
	if !terminal {
		t.Fatal("WorkflowJob remained active after its gateway was recreated")
	}
	stored := &actionsv1alpha1.WorkflowJob{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(workflowJob), stored); err != nil {
		t.Fatal(err)
	}
	condition := meta.FindStatusCondition(stored.Status.Conditions, actionsv1alpha1.WorkflowJobConditionSucceeded)
	if condition == nil || condition.Status != metav1.ConditionFalse || condition.Reason != "GatewayRecreated" {
		t.Fatalf("succeeded condition = %#v", condition)
	}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(authSecret), &corev1.Secret{}); !apierrors.IsNotFound(err) {
		t.Fatalf("authentication Secret still exists: %v", err)
	}
}

func TestEnsureAuthSecretRejectsUnownedCollision(t *testing.T) {
	scheme := runnerTestScheme(t)
	workflowJob := &actionsv1alpha1.WorkflowJob{
		TypeMeta:   metav1.TypeMeta{APIVersion: actionsv1alpha1.GroupVersion.String(), Kind: "WorkflowJob"},
		ObjectMeta: metav1.ObjectMeta{Name: "build", Namespace: "default", UID: types.UID("job-uid")},
	}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: childName(workflowJob.Name, "auth"), Namespace: workflowJob.Namespace},
		Data:       map[string][]byte{"token": []byte("existing")},
	}
	clusterClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(workflowJob, secret).Build()
	reconciler := &RunnerReconciler{Client: clusterClient, APIReader: clusterClient}
	if err := reconciler.ensureAuthSecret(context.Background(), workflowJob, "replacement"); err == nil {
		t.Fatal("unowned authentication Secret was accepted")
	}
	stored := &corev1.Secret{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(secret), stored); err != nil {
		t.Fatal(err)
	}
	if string(stored.Data["token"]) != "existing" {
		t.Fatalf("unowned Secret token = %q", stored.Data["token"])
	}
}

func TestDeletingBusyRunnerFinalizesItsWorkflowJob(t *testing.T) {
	scheme := runnerTestScheme(t)
	deletionTime := metav1.Now()
	runnerObject := &actionsv1alpha1.Runner{
		ObjectMeta: metav1.ObjectMeta{
			Name: "runner", Namespace: "default", DeletionTimestamp: &deletionTime, Finalizers: []string{runnerFinalizer},
		},
		Status: actionsv1alpha1.RunnerStatus{WorkflowJobRef: &corev1.LocalObjectReference{Name: "build"}},
	}
	workflowJob := &actionsv1alpha1.WorkflowJob{
		TypeMeta:   metav1.TypeMeta{APIVersion: actionsv1alpha1.GroupVersion.String(), Kind: "WorkflowJob"},
		ObjectMeta: metav1.ObjectMeta{Name: "build", Namespace: "default", UID: types.UID("job-uid")},
		Status: actionsv1alpha1.WorkflowJobStatus{
			RunnerRef:  &corev1.LocalObjectReference{Name: runnerObject.Name},
			Conditions: []metav1.Condition{{Type: actionsv1alpha1.WorkflowJobConditionSucceeded, Status: metav1.ConditionTrue, Reason: "JobSucceeded", LastTransitionTime: metav1.Now()}},
		},
	}
	nativeJob := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: workflowJob.Name, Namespace: workflowJob.Namespace}}
	if err := controllerutil.SetControllerReference(workflowJob, nativeJob, scheme); err != nil {
		t.Fatal(err)
	}
	clusterClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&actionsv1alpha1.Runner{}, &actionsv1alpha1.WorkflowJob{}, &batchv1.Job{}).
		WithObjects(runnerObject, workflowJob, nativeJob).
		Build()
	reconciler := &RunnerReconciler{Client: clusterClient, APIReader: clusterClient}
	if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(runnerObject)}); err != nil {
		t.Fatal(err)
	}
	storedJob := &batchv1.Job{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(nativeJob), storedJob); err != nil {
		t.Fatal(err)
	}
	if storedJob.Spec.TTLSecondsAfterFinished == nil {
		t.Fatal("native Job was not finalized before Runner deletion")
	}
	storedRunner := &actionsv1alpha1.Runner{}
	err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(runnerObject), storedRunner)
	if err == nil && controllerutil.ContainsFinalizer(storedRunner, runnerFinalizer) {
		t.Fatal("Runner finalizer remained after job finalization")
	}
	if err != nil && !apierrors.IsNotFound(err) {
		t.Fatal(err)
	}
}

func TestWorkflowJobEventEnqueuesOneIdleMatchingRunner(t *testing.T) {
	scheme := runnerTestScheme(t)
	run := &actionsv1alpha1.WorkflowRun{
		ObjectMeta: metav1.ObjectMeta{Name: "ci", Namespace: "default"},
		Spec:       actionsv1alpha1.WorkflowRunSpec{GatewayRef: corev1.LocalObjectReference{Name: "gateway"}},
	}
	workflowJob := &actionsv1alpha1.WorkflowJob{
		ObjectMeta: metav1.ObjectMeta{Name: "build", Namespace: "default"},
		Spec: actionsv1alpha1.WorkflowJobSpec{
			WorkflowRunRef: corev1.LocalObjectReference{Name: run.Name},
			RunsOn:         []string{"linux"},
		},
	}
	runner := func(name, gateway string, labels []string) *actionsv1alpha1.Runner {
		return &actionsv1alpha1.Runner{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
			Spec: actionsv1alpha1.RunnerSpec{
				GatewayRef: corev1.LocalObjectReference{Name: gateway},
				Labels:     labels,
			},
		}
	}
	idleA := runner("idle-a", "gateway", []string{"linux"})
	idleB := runner("idle-b", "gateway", []string{"linux"})
	busy := runner("busy", "gateway", []string{"linux"})
	busy.Status.WorkflowJobRef = &corev1.LocalObjectReference{Name: "other-job"}
	wrongGateway := runner("wrong-gateway", "other", []string{"linux"})
	wrongLabels := runner("wrong-labels", "gateway", []string{"windows"})
	clusterClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(run, idleA, idleB, busy, wrongGateway, wrongLabels).Build()
	reconciler := &RunnerReconciler{Client: clusterClient, APIReader: clusterClient}
	requests := reconciler.runnersForWorkflowJob(context.Background(), workflowJob)
	if len(requests) != 1 {
		t.Fatalf("enqueued %d Runners, want 1", len(requests))
	}
	selected := requests[0].Name
	if selected != idleA.Name && selected != idleB.Name {
		t.Fatalf("enqueued Runner %q", selected)
	}
}

func TestPlannedWorkflowRunWakesIdleGatewayRunners(t *testing.T) {
	scheme := runnerTestScheme(t)
	run := &actionsv1alpha1.WorkflowRun{
		ObjectMeta: metav1.ObjectMeta{Name: "ci", Namespace: "default"},
		Spec:       actionsv1alpha1.WorkflowRunSpec{GatewayRef: corev1.LocalObjectReference{Name: "gateway"}},
		Status: actionsv1alpha1.WorkflowRunStatus{Conditions: []metav1.Condition{{
			Type: actionsv1alpha1.WorkflowRunConditionPlanned, Status: metav1.ConditionTrue, Reason: "JobsPlanned", LastTransitionTime: metav1.Now(),
		}}},
	}
	idle := &actionsv1alpha1.Runner{ObjectMeta: metav1.ObjectMeta{Name: "idle", Namespace: "default"}, Spec: actionsv1alpha1.RunnerSpec{GatewayRef: corev1.LocalObjectReference{Name: "gateway"}}}
	busy := &actionsv1alpha1.Runner{ObjectMeta: metav1.ObjectMeta{Name: "busy", Namespace: "default"}, Spec: actionsv1alpha1.RunnerSpec{GatewayRef: corev1.LocalObjectReference{Name: "gateway"}}, Status: actionsv1alpha1.RunnerStatus{WorkflowJobRef: &corev1.LocalObjectReference{Name: "other"}}}
	other := &actionsv1alpha1.Runner{ObjectMeta: metav1.ObjectMeta{Name: "other", Namespace: "default"}, Spec: actionsv1alpha1.RunnerSpec{GatewayRef: corev1.LocalObjectReference{Name: "other"}}}
	clusterClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(run, idle, busy, other).Build()
	reconciler := &RunnerReconciler{Client: clusterClient, APIReader: clusterClient}
	requests := reconciler.runnersForWorkflowRun(context.Background(), run)
	if len(requests) != 1 || requests[0].Name != idle.Name {
		t.Fatalf("requests = %#v", requests)
	}
}

func TestWorkflowJobAssignmentUsesOptimisticLock(t *testing.T) {
	scheme := runnerTestScheme(t)
	workflowJob := &actionsv1alpha1.WorkflowJob{ObjectMeta: metav1.ObjectMeta{Name: "build", Namespace: "default"}}
	clusterClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&actionsv1alpha1.WorkflowJob{}).
		WithObjects(workflowJob).
		Build()
	reconciler := &RunnerReconciler{Client: clusterClient, APIReader: clusterClient}

	first := &actionsv1alpha1.WorkflowJob{}
	second := &actionsv1alpha1.WorkflowJob{}
	key := client.ObjectKeyFromObject(workflowJob)
	if err := clusterClient.Get(context.Background(), key, first); err != nil {
		t.Fatal(err)
	}
	if err := clusterClient.Get(context.Background(), key, second); err != nil {
		t.Fatal(err)
	}
	if err := reconciler.assignWorkflowJob(context.Background(), first, "runner-1"); err != nil {
		t.Fatal(err)
	}
	if err := reconciler.assignWorkflowJob(context.Background(), second, "runner-2"); !apierrors.IsConflict(err) {
		t.Fatalf("second assignment error = %v, want conflict", err)
	}
}

func TestRunnerLabelsMatchRunsOn(t *testing.T) {
	if !runnerLabelsMatch([]string{"self-hosted", "linux", "arm64"}, []string{"linux", "arm64"}) {
		t.Error("expected all requested labels to match")
	}
	if runnerLabelsMatch([]string{"self-hosted", "linux"}, []string{"linux", "x64"}) {
		t.Error("matched a job with a missing label")
	}
	if runnerLabelsMatch([]string{"Linux"}, []string{"linux"}) {
		t.Error("matched labels using non-canonical case folding")
	}
}

func runnerTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := actionsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := batchv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	return scheme
}
