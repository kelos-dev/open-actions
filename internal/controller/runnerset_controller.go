package controller

import (
	"context"
	"fmt"
	"sort"

	actionsv1alpha1 "github.com/kelos-dev/open-actions/api/v1alpha1"
	apiEquality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const (
	runnerSetOwnerUIDIndex = "actions.kelos.dev/runner-set-owner-uid"
	runnerSetScaleBatch    = int32(100)
)

type RunnerSetReconciler struct {
	client.Client
	APIReader client.Reader
}

func (r *RunnerSetReconciler) Reconcile(ctx context.Context, request ctrl.Request) (ctrl.Result, error) {
	runnerSet := &actionsv1alpha1.RunnerSet{}
	if err := r.Get(ctx, request.NamespacedName, runnerSet); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !runnerSet.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}

	runners, err := r.managedRunners(ctx, runnerSet)
	if err != nil {
		return ctrl.Result{}, err
	}
	for index := range runners {
		if err := r.reconcileRunner(ctx, runnerSet, runners[index]); err != nil {
			return ctrl.Result{}, err
		}
	}
	if err := r.reconcileReplicas(ctx, runnerSet, runners); err != nil {
		return ctrl.Result{}, err
	}

	runners, err = r.managedRunners(ctx, runnerSet)
	if err != nil {
		return ctrl.Result{}, err
	}
	if err := r.updateStatus(ctx, runnerSet, runners); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

func (r *RunnerSetReconciler) managedRunners(ctx context.Context, runnerSet *actionsv1alpha1.RunnerSet) ([]*actionsv1alpha1.Runner, error) {
	list := &actionsv1alpha1.RunnerList{}
	if err := r.List(ctx, list,
		client.InNamespace(runnerSet.Namespace),
		client.MatchingFields{runnerSetOwnerUIDIndex: string(runnerSet.UID)},
	); err != nil {
		return nil, fmt.Errorf("list Runners for RunnerSet %q: %w", runnerSet.Name, err)
	}
	runners := make([]*actionsv1alpha1.Runner, 0, len(list.Items))
	for index := range list.Items {
		runners = append(runners, &list.Items[index])
	}
	return runners, nil
}

func (r *RunnerSetReconciler) reconcileRunner(ctx context.Context, runnerSet *actionsv1alpha1.RunnerSet, runner *actionsv1alpha1.Runner) error {
	if !runner.DeletionTimestamp.IsZero() {
		return nil
	}
	before := runner.DeepCopy()
	runner.Spec = *runnerSet.Spec.Template.Spec.DeepCopy()
	if runner.Labels == nil {
		runner.Labels = map[string]string{}
	}
	runner.Labels[actionsv1alpha1.LabelRunnerSetUID] = string(runnerSet.UID)
	if apiEquality.Semantic.DeepEqual(before, runner) {
		return nil
	}
	if err := r.Patch(ctx, runner, client.MergeFrom(before)); err != nil {
		return fmt.Errorf("update Runner %q for RunnerSet %q: %w", runner.Name, runnerSet.Name, err)
	}
	return nil
}

func (r *RunnerSetReconciler) reconcileReplicas(ctx context.Context, runnerSet *actionsv1alpha1.RunnerSet, runners []*actionsv1alpha1.Runner) error {
	desired := desiredRunnerSetReplicas(runnerSet)
	active := make([]*actionsv1alpha1.Runner, 0, len(runners))
	for _, runner := range runners {
		if runner.DeletionTimestamp.IsZero() {
			active = append(active, runner)
		}
	}
	current := int32(len(active))
	if current < desired {
		liveRunners, err := r.liveManagedRunners(ctx, runnerSet)
		if err != nil {
			return err
		}
		liveReplicas := int32(0)
		for _, runner := range liveRunners {
			if runner.DeletionTimestamp.IsZero() {
				liveReplicas++
			}
		}
		if liveReplicas >= desired {
			return nil
		}
		count := min(desired-liveReplicas, runnerSetScaleBatch)
		for range count {
			if err := r.createRunner(ctx, runnerSet); err != nil {
				return err
			}
		}
		return nil
	}
	if current == desired {
		return nil
	}

	sort.Slice(active, func(left, right int) bool {
		leftBusy := active[left].Status.WorkflowJobRef != nil
		rightBusy := active[right].Status.WorkflowJobRef != nil
		if leftBusy != rightBusy {
			return !leftBusy
		}
		leftReady := runnerReady(active[left])
		rightReady := runnerReady(active[right])
		if leftReady != rightReady {
			return !leftReady
		}
		if !active[left].CreationTimestamp.Equal(&active[right].CreationTimestamp) {
			return active[left].CreationTimestamp.After(active[right].CreationTimestamp.Time)
		}
		return active[left].Name > active[right].Name
	})
	count := min(current-desired, runnerSetScaleBatch)
	for _, runner := range active[:count] {
		if err := r.Delete(ctx, runner); err != nil {
			return fmt.Errorf("delete Runner %q for RunnerSet %q: %w", runner.Name, runnerSet.Name, err)
		}
	}
	return nil
}

func (r *RunnerSetReconciler) liveManagedRunners(ctx context.Context, runnerSet *actionsv1alpha1.RunnerSet) ([]*actionsv1alpha1.Runner, error) {
	list := &actionsv1alpha1.RunnerList{}
	if err := r.APIReader.List(ctx, list,
		client.InNamespace(runnerSet.Namespace),
		client.MatchingLabels{actionsv1alpha1.LabelRunnerSetUID: string(runnerSet.UID)},
	); err != nil {
		return nil, fmt.Errorf("list live Runners for RunnerSet %q: %w", runnerSet.Name, err)
	}
	runners := make([]*actionsv1alpha1.Runner, 0, len(list.Items))
	for index := range list.Items {
		runner := &list.Items[index]
		if metav1.IsControlledBy(runner, runnerSet) {
			runners = append(runners, runner)
		}
	}
	return runners, nil
}

func (r *RunnerSetReconciler) createRunner(ctx context.Context, runnerSet *actionsv1alpha1.RunnerSet) error {
	runner := &actionsv1alpha1.Runner{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: runnerSet.Name + "-",
			Namespace:    runnerSet.Namespace,
			Labels: map[string]string{
				actionsv1alpha1.LabelRunnerSetUID: string(runnerSet.UID),
			},
		},
		Spec: *runnerSet.Spec.Template.Spec.DeepCopy(),
	}
	if err := controllerutil.SetControllerReference(runnerSet, runner, r.Scheme()); err != nil {
		return fmt.Errorf("set Runner owner for RunnerSet %q: %w", runnerSet.Name, err)
	}
	if err := r.Create(ctx, runner); err != nil {
		return fmt.Errorf("create Runner for RunnerSet %q: %w", runnerSet.Name, err)
	}
	return nil
}

func (r *RunnerSetReconciler) updateStatus(ctx context.Context, runnerSet *actionsv1alpha1.RunnerSet, runners []*actionsv1alpha1.Runner) error {
	before := runnerSet.DeepCopy()
	status := runnerSet.Status.DeepCopy()
	status.ObservedGeneration = runnerSet.Generation
	status.Replicas = 0
	status.ReadyReplicas = 0
	status.BusyReplicas = 0
	status.IdleReplicas = 0
	status.TerminatingReplicas = 0
	for _, runner := range runners {
		if !runner.DeletionTimestamp.IsZero() {
			status.TerminatingReplicas++
			continue
		}
		status.Replicas++
		busy := runner.Status.WorkflowJobRef != nil
		ready := runnerReady(runner)
		if busy {
			status.BusyReplicas++
		}
		if ready {
			status.ReadyReplicas++
			if !busy {
				status.IdleReplicas++
			}
		}
	}

	desired := desiredRunnerSetReplicas(runnerSet)
	conditionStatus := metav1.ConditionFalse
	reason := "ReplicaCountMismatch"
	message := fmt.Sprintf("RunnerSet has %d of %d desired non-terminating Runners", status.Replicas, desired)
	switch {
	case status.Replicas != desired:
	case status.TerminatingReplicas > 0:
		reason = "RunnersTerminating"
		message = fmt.Sprintf("RunnerSet has %d terminating Runners", status.TerminatingReplicas)
	case status.ReadyReplicas != desired:
		reason = "RunnersNotReady"
		message = fmt.Sprintf("RunnerSet has %d of %d desired ready Runners", status.ReadyReplicas, desired)
	default:
		conditionStatus = metav1.ConditionTrue
		reason = "RunnersReady"
		message = "All desired Runners are ready"
	}
	meta.SetStatusCondition(&status.Conditions, metav1.Condition{
		Type:               actionsv1alpha1.RunnerSetConditionReady,
		Status:             conditionStatus,
		ObservedGeneration: runnerSet.Generation,
		Reason:             reason,
		Message:            message,
	})
	runnerSet.Status = *status
	if apiEquality.Semantic.DeepEqual(before.Status, runnerSet.Status) {
		return nil
	}
	if err := r.Status().Patch(ctx, runnerSet, client.MergeFrom(before)); err != nil {
		return fmt.Errorf("update RunnerSet %q status: %w", runnerSet.Name, err)
	}
	return nil
}

func desiredRunnerSetReplicas(runnerSet *actionsv1alpha1.RunnerSet) int32 {
	if runnerSet.Spec.Replicas == nil {
		return 1
	}
	return *runnerSet.Spec.Replicas
}

func runnerReady(runner *actionsv1alpha1.Runner) bool {
	condition := meta.FindStatusCondition(runner.Status.Conditions, actionsv1alpha1.RunnerConditionReady)
	return condition != nil && condition.Status == metav1.ConditionTrue && condition.ObservedGeneration == runner.Generation
}

func indexRunnerSetOwnerUID(object client.Object) []string {
	owner := metav1.GetControllerOf(object)
	if owner == nil || owner.APIVersion != actionsv1alpha1.GroupVersion.String() || owner.Kind != "RunnerSet" {
		return nil
	}
	return []string{string(owner.UID)}
}

func (r *RunnerSetReconciler) SetupWithManager(manager ctrl.Manager) error {
	if err := manager.GetFieldIndexer().IndexField(context.Background(), &actionsv1alpha1.Runner{}, runnerSetOwnerUIDIndex, indexRunnerSetOwnerUID); err != nil {
		return err
	}
	return ctrl.NewControllerManagedBy(manager).
		For(&actionsv1alpha1.RunnerSet{}).
		Owns(&actionsv1alpha1.Runner{}).
		Complete(r)
}
