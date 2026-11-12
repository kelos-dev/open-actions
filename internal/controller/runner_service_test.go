package controller

import (
	"context"
	"maps"
	"strings"
	"testing"

	actionsv1alpha1 "github.com/kelos-dev/open-actions/api/v1alpha1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

func serviceTestRunner() *actionsv1alpha1.Runner {
	return &actionsv1alpha1.Runner{Spec: actionsv1alpha1.RunnerSpec{Execution: actionsv1alpha1.RunnerExecutionSpec{
		Service: &actionsv1alpha1.RunnerServiceSpec{Ports: []actionsv1alpha1.RunnerServicePort{
			{Name: "http", Port: 80, TargetPort: pointerTo(int32(8080))},
		}},
	}}}
}

func serviceTestJob() *actionsv1alpha1.WorkflowJob {
	return &actionsv1alpha1.WorkflowJob{
		ObjectMeta: metav1.ObjectMeta{Name: "ci-preview", Namespace: "team", UID: types.UID("12345678-1234-1234-1234-123456789abc")},
		Spec:       actionsv1alpha1.WorkflowJobSpec{JobID: "preview"},
		Status:     actionsv1alpha1.WorkflowJobStatus{RunnerRef: &corev1.LocalObjectReference{Name: "runner"}},
	}
}

func serviceTestReconciler(t *testing.T, job *actionsv1alpha1.WorkflowJob) *RunnerReconciler {
	t.Helper()
	clusterClient := fake.NewClientBuilder().WithScheme(runnerTestScheme(t)).
		WithStatusSubresource(&actionsv1alpha1.WorkflowJob{}, &batchv1.Job{}).WithObjects(job).Build()
	return &RunnerReconciler{Client: clusterClient, APIReader: clusterClient}
}

func getJobService(t *testing.T, r *RunnerReconciler, job *actionsv1alpha1.WorkflowJob) *corev1.Service {
	t.Helper()
	service := &corev1.Service{}
	if err := r.Get(context.Background(), client.ObjectKey{Namespace: job.Namespace, Name: workflowJobServiceName(job)}, service); err != nil {
		t.Fatal(err)
	}
	return service
}

func TestRunnerCreatesJobService(t *testing.T) {
	for _, serviceType := range []corev1.ServiceType{"", corev1.ServiceTypeNodePort, corev1.ServiceTypeLoadBalancer} {
		t.Run(string(serviceType), func(t *testing.T) {
			ctx := context.Background()
			job := serviceTestJob()
			r := serviceTestReconciler(t, job)
			runnerObject := serviceTestRunner()
			runnerObject.Spec.Execution.Service.Type = serviceType
			runnerObject.Spec.Execution.Service.Ports = append(runnerObject.Spec.Execution.Service.Ports,
				actionsv1alpha1.RunnerServicePort{Name: "dns", Port: 53, Protocol: corev1.ProtocolUDP})
			if err := r.prepareWorkflowJobService(ctx, job, runnerObject); err != nil {
				t.Fatal(err)
			}
			service := getJobService(t, r, job)
			wantType := serviceType
			if wantType == "" {
				wantType = corev1.ServiceTypeClusterIP
			}
			if service.Spec.Type != wantType || !metav1.IsControlledBy(service, job) || service.Namespace != job.Namespace {
				t.Fatalf("Service = %#v", service)
			}
			if service.OwnerReferences[0].BlockOwnerDeletion == nil || *service.OwnerReferences[0].BlockOwnerDeletion {
				t.Fatal("Service must not require permission to block WorkflowJob deletion")
			}
			if !maps.Equal(service.Spec.Selector, map[string]string{actionsv1alpha1.LabelWorkflowJobUID: string(job.UID)}) {
				t.Fatalf("selector = %v", service.Spec.Selector)
			}
			if len(service.Spec.Ports) != 2 || service.Spec.Ports[0].Port != 80 || service.Spec.Ports[0].TargetPort.IntVal != 8080 || service.Spec.Ports[0].Protocol != corev1.ProtocolTCP || service.Spec.Ports[1].TargetPort.IntVal != 53 || service.Spec.Ports[1].Protocol != corev1.ProtocolUDP {
				t.Fatalf("Service ports = %#v", service.Spec.Ports)
			}
			stored := &actionsv1alpha1.WorkflowJob{}
			if err := r.Get(ctx, client.ObjectKeyFromObject(job), stored); err != nil {
				t.Fatal(err)
			}
			if stored.Status.ServiceRef == nil || stored.Status.ServiceRef.Name != service.Name || stored.Annotations[jobServiceAnnotation] == "" {
				t.Fatalf("WorkflowJob = %#v", stored)
			}
			if err := r.reconcileWorkflowJobService(ctx, job); err != nil {
				t.Fatal(err)
			}
			if updated := getJobService(t, r, job); updated.ResourceVersion != service.ResourceVersion {
				t.Fatal("unchanged Service was updated")
			}
		})
	}
}

func TestRunnerServiceIsOptIn(t *testing.T) {
	job := serviceTestJob()
	r := serviceTestReconciler(t, job)
	if err := r.prepareWorkflowJobService(context.Background(), job, &actionsv1alpha1.Runner{}); err != nil {
		t.Fatal(err)
	}
	services := &corev1.ServiceList{}
	if err := r.List(context.Background(), services); err != nil {
		t.Fatal(err)
	}
	if len(services.Items) != 0 || job.Status.ServiceRef != nil || job.Annotations[jobServiceAnnotation] != "" {
		t.Fatalf("disabled Service: services %v, job %#v", services.Items, job)
	}
}

func TestRunnerServiceRecreationUsesCapturedConfiguration(t *testing.T) {
	ctx := context.Background()
	job := serviceTestJob()
	r := serviceTestReconciler(t, job)
	runnerObject := serviceTestRunner()
	if err := r.prepareWorkflowJobService(ctx, job, runnerObject); err != nil {
		t.Fatal(err)
	}
	runnerObject.Spec.Execution.Service = nil
	if err := r.prepareWorkflowJobService(ctx, job, runnerObject); err != nil {
		t.Fatal(err)
	}
	if err := r.Delete(ctx, getJobService(t, r, job)); err != nil {
		t.Fatal(err)
	}
	stored := &actionsv1alpha1.WorkflowJob{}
	if err := r.Get(ctx, client.ObjectKeyFromObject(job), stored); err != nil {
		t.Fatal(err)
	}
	nativeJob := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: job.Name, Namespace: job.Namespace, UID: types.UID("native-job-uid")}}
	if err := controllerutil.SetControllerReference(job, nativeJob, r.Scheme()); err != nil {
		t.Fatal(err)
	}
	if err := r.Create(ctx, nativeJob); err != nil {
		t.Fatal(err)
	}
	if found, terminal, err := r.observeNativeJob(ctx, stored); err != nil || !found || terminal {
		t.Fatalf("observe active job: found %t, terminal %t, error %v", found, terminal, err)
	}
	if service := getJobService(t, r, job); service.Spec.Ports[0].TargetPort.IntVal != 8080 {
		t.Fatalf("recreated ports = %#v", service.Spec.Ports)
	}

	other := serviceTestJob()
	other.Name = "ci-preview-attempt-2"
	other.UID = "87654321-1234-1234-1234-123456789abc"
	if err := r.Create(ctx, other); err != nil {
		t.Fatal(err)
	}
	if err := r.prepareWorkflowJobService(ctx, other, serviceTestRunner()); err != nil {
		t.Fatal(err)
	}
	first, second := getJobService(t, r, job), getJobService(t, r, other)
	if first.Name == second.Name || maps.Equal(first.Spec.Selector, second.Spec.Selector) {
		t.Fatalf("job executions share a Service: %v, %v", first, second)
	}
}

func TestRunnerServicePreservesAllocatedAddresses(t *testing.T) {
	ctx := context.Background()
	job := serviceTestJob()
	r := serviceTestReconciler(t, job)
	runnerObject := serviceTestRunner()
	runnerObject.Spec.Execution.Service.Type = corev1.ServiceTypeLoadBalancer
	if err := r.prepareWorkflowJobService(ctx, job, runnerObject); err != nil {
		t.Fatal(err)
	}
	service := getJobService(t, r, job)
	service.Spec.ClusterIP = "10.0.0.2"
	service.Spec.ClusterIPs = []string{"10.0.0.2"}
	service.Spec.Ports[0].NodePort = 30080
	service.Spec.Selector = map[string]string{"wrong": "job"}
	service.Spec.Ports[0].TargetPort.IntVal = 9090
	service.Annotations = map[string]string{"example.com/load-balancer": "allocated"}
	if err := r.Update(ctx, service); err != nil {
		t.Fatal(err)
	}
	if err := r.reconcileWorkflowJobService(ctx, job); err != nil {
		t.Fatal(err)
	}
	service = getJobService(t, r, job)
	if service.Spec.ClusterIP != "10.0.0.2" || len(service.Spec.ClusterIPs) != 1 || service.Spec.ClusterIPs[0] != "10.0.0.2" || service.Spec.Ports[0].NodePort != 30080 || service.Annotations["example.com/load-balancer"] != "allocated" {
		t.Fatalf("allocated Service fields = %#v", service)
	}
	if service.Spec.Ports[0].TargetPort.IntVal != 8080 || !maps.Equal(service.Spec.Selector, map[string]string{actionsv1alpha1.LabelWorkflowJobUID: string(job.UID)}) {
		t.Fatalf("Service routing was not restored: %#v", service.Spec)
	}
}

func TestRunnerServiceRejectsForeignOwnership(t *testing.T) {
	ctx := context.Background()
	job := serviceTestJob()
	r := serviceTestReconciler(t, job)
	foreign := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: workflowJobServiceName(job), Namespace: job.Namespace}}
	if err := r.Create(ctx, foreign); err != nil {
		t.Fatal(err)
	}
	for _, operation := range []func() error{
		func() error { return r.prepareWorkflowJobService(ctx, job, serviceTestRunner()) },
		func() error { return r.cleanupWorkflowJobService(ctx, job) },
	} {
		if err := operation(); err == nil || !strings.Contains(err.Error(), job.Name) || !strings.Contains(err.Error(), foreign.Name) {
			t.Fatalf("foreign Service error = %v", err)
		}
	}
	if service := getJobService(t, r, job); len(service.OwnerReferences) != 0 || len(service.Spec.Selector) != 0 {
		t.Fatalf("foreign Service was modified: %#v", service)
	}
}

func TestRunnerServiceCleanupOnCompletion(t *testing.T) {
	for _, outcome := range []string{"success", "failure", "timeout", "cancellation", "start failure"} {
		t.Run(outcome, func(t *testing.T) {
			ctx := context.Background()
			job := serviceTestJob()
			r := serviceTestReconciler(t, job)
			if err := r.prepareWorkflowJobService(ctx, job, serviceTestRunner()); err != nil {
				t.Fatal(err)
			}
			nativeJob := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: job.Name, Namespace: job.Namespace, UID: types.UID("native-job-uid")}}
			if err := controllerutil.SetControllerReference(job, nativeJob, r.Scheme()); err != nil {
				t.Fatal(err)
			}
			condition := batchv1.JobCondition{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}
			if outcome == "failure" || outcome == "timeout" {
				condition.Type = batchv1.JobFailed
			}
			if outcome == "timeout" {
				condition.Reason = batchv1.JobReasonDeadlineExceeded
			}
			nativeJob.Status.Conditions = []batchv1.JobCondition{condition}
			if err := r.Create(ctx, nativeJob); err != nil {
				t.Fatal(err)
			}
			var err error
			switch outcome {
			case "cancellation":
				err = r.cancelWorkflowJob(ctx, job, &workflowJobCancellation{reason: "CancellationRequested", message: "Workflow cancelled"})
			case "start failure":
				err = r.failAssignedWorkflowJob(ctx, job, "JobStartFailed", "Job did not start")
			default:
				var found, terminal bool
				found, terminal, err = r.observeNativeJob(ctx, job)
				if !found || !terminal {
					t.Fatalf("observed job: found %t, terminal %t", found, terminal)
				}
			}
			if err != nil {
				t.Fatal(err)
			}
			if err := r.Get(ctx, client.ObjectKey{Namespace: job.Namespace, Name: workflowJobServiceName(job)}, &corev1.Service{}); !apierrors.IsNotFound(err) {
				t.Fatalf("completed job Service exists: %v", err)
			}
			stored := &actionsv1alpha1.WorkflowJob{}
			if err := r.Get(ctx, client.ObjectKeyFromObject(job), stored); err != nil {
				t.Fatal(err)
			}
			if stored.Status.ServiceRef != nil || stored.Status.Result == "" {
				t.Fatalf("completed WorkflowJob status = %#v", stored.Status)
			}
			if err := r.cleanupWorkflowJobService(ctx, stored); err != nil {
				t.Fatalf("repeated cleanup: %v", err)
			}
		})
	}
}
