package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	actionsv1alpha1 "github.com/kelos-dev/open-actions/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apiEquality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const jobServiceAnnotation = "actions.kelos.dev/job-service"

func workflowJobServiceName(job *actionsv1alpha1.WorkflowJob) string {
	return "job-" + string(job.UID)
}

func (r *RunnerReconciler) prepareWorkflowJobService(ctx context.Context, job *actionsv1alpha1.WorkflowJob, runnerObject *actionsv1alpha1.Runner) error {
	// Persist the configuration so retries and Service recreation do not depend
	// on subsequent edits to the Runner or RunnerSet.
	if job.Annotations[jobServiceAnnotation] == "" && runnerObject.Spec.Execution.Service != nil {
		data, err := json.Marshal(runnerObject.Spec.Execution.Service)
		if err != nil {
			return fmt.Errorf("encode Service configuration for WorkflowJob %q: %w", job.Name, err)
		}
		before := job.DeepCopy()
		if job.Annotations == nil {
			job.Annotations = map[string]string{}
		}
		job.Annotations[jobServiceAnnotation] = string(data)
		if err := r.Patch(ctx, job, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
			return fmt.Errorf("record Service configuration for WorkflowJob %q: %w", job.Name, err)
		}
	}
	return r.reconcileWorkflowJobService(ctx, job)
}

func (r *RunnerReconciler) reconcileWorkflowJobService(ctx context.Context, job *actionsv1alpha1.WorkflowJob) error {
	data := job.Annotations[jobServiceAnnotation]
	if data == "" {
		return nil
	}
	configuration := &actionsv1alpha1.RunnerServiceSpec{}
	if err := json.Unmarshal([]byte(data), configuration); err != nil {
		return fmt.Errorf("decode Service configuration for WorkflowJob %q: %w", job.Name, err)
	}
	service := &corev1.Service{ObjectMeta: metav1.ObjectMeta{
		Name: workflowJobServiceName(job), Namespace: job.Namespace,
		Labels: map[string]string{actionsv1alpha1.LabelWorkflowJobUID: string(job.UID)},
	}, Spec: corev1.ServiceSpec{
		Type:     configuration.Type,
		Selector: map[string]string{actionsv1alpha1.LabelWorkflowJobUID: string(job.UID)},
	}}
	if service.Spec.Type == "" {
		service.Spec.Type = corev1.ServiceTypeClusterIP
	}
	for _, port := range configuration.Ports {
		targetPort := port.Port
		if port.TargetPort != nil {
			targetPort = *port.TargetPort
		}
		protocol := port.Protocol
		if protocol == "" {
			protocol = corev1.ProtocolTCP
		}
		service.Spec.Ports = append(service.Spec.Ports, corev1.ServicePort{
			Name: port.Name, Port: port.Port, TargetPort: intstr.FromInt32(targetPort), Protocol: protocol,
		})
	}
	if err := controllerutil.SetControllerReference(job, service, r.Scheme(), controllerutil.WithBlockOwnerDeletion(false)); err != nil {
		return fmt.Errorf("set Service owner for WorkflowJob %q: %w", job.Name, err)
	}
	existing := &corev1.Service{}
	if err := r.APIReader.Get(ctx, client.ObjectKeyFromObject(service), existing); apierrors.IsNotFound(err) {
		if err := r.Create(ctx, service); err != nil {
			return fmt.Errorf("create Service %q for WorkflowJob %q: %w", service.Name, job.Name, err)
		}
	} else if err != nil {
		return fmt.Errorf("get Service %q for WorkflowJob %q: %w", service.Name, job.Name, err)
	} else {
		if !metav1.IsControlledBy(existing, job) {
			return fmt.Errorf("Service %q is not controlled by WorkflowJob %q", service.Name, job.Name)
		}
		if !existing.DeletionTimestamp.IsZero() {
			return fmt.Errorf("Service %q for WorkflowJob %q is being deleted", service.Name, job.Name)
		}
		before := existing.DeepCopy()
		existing.Spec.Type = service.Spec.Type
		existing.Spec.Selector = service.Spec.Selector
		for index := range service.Spec.Ports {
			port := &service.Spec.Ports[index]
			if service.Spec.Type == corev1.ServiceTypeClusterIP {
				continue
			}
			for _, allocated := range existing.Spec.Ports {
				if allocated.Name == port.Name && allocated.Protocol == port.Protocol {
					port.NodePort = allocated.NodePort
					break
				}
			}
		}
		existing.Spec.Ports = service.Spec.Ports
		if !apiEquality.Semantic.DeepEqual(before.Spec, existing.Spec) {
			if err := r.Patch(ctx, existing, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
				return fmt.Errorf("update Service %q for WorkflowJob %q: %w", service.Name, job.Name, err)
			}
		}
	}
	return r.updateWorkflowJobServiceRef(ctx, job, &corev1.LocalObjectReference{Name: service.Name})
}

func (r *RunnerReconciler) updateWorkflowJobServiceRef(ctx context.Context, job *actionsv1alpha1.WorkflowJob, reference *corev1.LocalObjectReference) error {
	if apiEquality.Semantic.DeepEqual(job.Status.ServiceRef, reference) {
		return nil
	}
	before := job.DeepCopy()
	job.Status.ServiceRef = reference
	if err := r.Status().Patch(ctx, job, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
		return fmt.Errorf("update Service reference for WorkflowJob %q: %w", job.Name, err)
	}
	return nil
}

func (r *RunnerReconciler) cleanupWorkflowJobService(ctx context.Context, job *actionsv1alpha1.WorkflowJob) error {
	if job.Annotations[jobServiceAnnotation] == "" && job.Status.ServiceRef == nil {
		return nil
	}
	service := &corev1.Service{}
	key := client.ObjectKey{Namespace: job.Namespace, Name: workflowJobServiceName(job)}
	if err := r.APIReader.Get(ctx, key, service); err != nil {
		if !apierrors.IsNotFound(err) {
			return fmt.Errorf("get Service %q for WorkflowJob %q: %w", key.Name, job.Name, err)
		}
	} else {
		if !metav1.IsControlledBy(service, job) {
			return fmt.Errorf("Service %q is not controlled by WorkflowJob %q", service.Name, job.Name)
		}
		if err := r.Delete(ctx, service, client.Preconditions{UID: &service.UID}); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("delete Service %q for WorkflowJob %q: %w", service.Name, job.Name, err)
		}
	}
	return client.IgnoreNotFound(r.updateWorkflowJobServiceRef(ctx, job, nil))
}

func (r *RunnerReconciler) cleanupWorkflowJobResources(ctx context.Context, job *actionsv1alpha1.WorkflowJob) error {
	return errors.Join(r.cleanupWorkflowJobService(ctx, job), r.cleanupAuthSecret(ctx, job))
}
