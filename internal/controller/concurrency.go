package controller

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"unicode"

	actionsv1alpha1 "github.com/kelos-dev/open-actions/api/v1alpha1"
	batchv1 "k8s.io/api/batch/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	concurrencyGroupAnnotation      = "actions.kelos.dev/concurrency-group"
	concurrencyProjectAnnotation    = "actions.kelos.dev/concurrency-project"
	concurrencyRepositoryAnnotation = "actions.kelos.dev/concurrency-repository-id"
	concurrencyPendingAnnotation    = "actions.kelos.dev/concurrency-pending"

	concurrencyWaitingReason    = "WaitingForConcurrency"
	concurrencyAcquiredReason   = "ConcurrencyAcquired"
	concurrencySupersededReason = "ConcurrencySuperseded"
	concurrencyCancelledReason  = "ConcurrencyCancelled"
)

type concurrencyScope struct {
	project      string
	repositoryID int64
}

type concurrencyMember struct {
	Kind             string    `json:"kind"`
	Name             string    `json:"name"`
	UID              types.UID `json:"uid"`
	CancelInProgress bool      `json:"cancelInProgress,omitempty"`
}

type concurrencyGateState uint8

const (
	concurrencyGateAcquired concurrencyGateState = iota
	concurrencyGateWaiting
	concurrencyGateSuperseded
)

type concurrencyGateResult struct {
	state       concurrencyGateState
	displaced   *concurrencyMember
	cancelOwner *concurrencyMember
}

type workflowRunConcurrencyCandidate struct {
	member concurrencyMember
	run    *actionsv1alpha1.WorkflowRun
}

func workflowRunConcurrencyScope(run *actionsv1alpha1.WorkflowRun) (concurrencyScope, error) {
	if run.Spec.Source.Type != actionsv1alpha1.SourceTypeGitHub || run.Spec.Source.GitHub == nil {
		return concurrencyScope{}, fmt.Errorf("WorkflowRun %q has no GitHub repository for concurrency", run.Name)
	}
	return concurrencyScope{project: run.Spec.ProjectRef.Name, repositoryID: run.Spec.Source.GitHub.Repository.ID}, nil
}

func workflowRunConcurrencyMember(run *actionsv1alpha1.WorkflowRun) concurrencyMember {
	cancelInProgress := run.Status.Concurrency != nil && run.Status.Concurrency.CancelInProgress
	return concurrencyMember{Kind: "WorkflowRun", Name: run.Name, UID: run.UID, CancelInProgress: cancelInProgress}
}

func (r *WorkflowRunReconciler) olderWorkflowRunPlanning(ctx context.Context, current *actionsv1alpha1.WorkflowRun) (bool, error) {
	scope, err := workflowRunConcurrencyScope(current)
	if err != nil {
		return false, err
	}
	runs := &actionsv1alpha1.WorkflowRunList{}
	if err := r.APIReader.List(ctx, runs, client.InNamespace(current.Namespace)); err != nil {
		return false, err
	}
	for index := range runs.Items {
		other := &runs.Items[index]
		if other.UID == current.UID || terminalRun(other) || !workflowRunMatchesConcurrencyScope(other, scope) || !workflowRunOlderThan(other, current) {
			continue
		}
		planned := meta.FindStatusCondition(other.Status.Conditions, actionsv1alpha1.WorkflowRunConditionPlanned)
		if planned == nil || (planned.Status == metav1.ConditionUnknown && !waitingForConcurrencyCondition(planned)) {
			return true, nil
		}
	}
	return false, nil
}

func workflowRunOlderThan(left, right *actionsv1alpha1.WorkflowRun) bool {
	if left.CreationTimestamp.Time.Equal(right.CreationTimestamp.Time) {
		return left.Name < right.Name
	}
	return left.CreationTimestamp.Time.Before(right.CreationTimestamp.Time)
}

func workflowJobConcurrencyMember(job *actionsv1alpha1.WorkflowJob) concurrencyMember {
	cancelInProgress := job.Status.Concurrency != nil && job.Status.Concurrency.CancelInProgress
	return concurrencyMember{Kind: "WorkflowJob", Name: job.Name, UID: job.UID, CancelInProgress: cancelInProgress}
}

func concurrencyLeaseName(scope concurrencyScope, group string) string {
	digest := sha256.Sum256([]byte(scope.project + "\x00" + strconv.FormatInt(scope.repositoryID, 10) + "\x00" + foldConcurrencyGroup(group)))
	return "oa-cg-" + strings.ToLower(digestEncoding.EncodeToString(digest[:]))
}

func foldConcurrencyGroup(value string) string {
	var result strings.Builder
	for _, character := range value {
		canonical := character
		for folded := unicode.SimpleFold(character); folded != character; folded = unicode.SimpleFold(folded) {
			if folded < canonical {
				canonical = folded
			}
		}
		result.WriteRune(canonical)
	}
	return result.String()
}

func (member concurrencyMember) identity() string {
	return member.Kind + ":" + member.Name + ":" + string(member.UID)
}

func parseConcurrencyIdentity(value string) (*concurrencyMember, error) {
	parts := strings.SplitN(value, ":", 3)
	if len(parts) != 3 || parts[0] == "" || parts[1] == "" || parts[2] == "" {
		return nil, fmt.Errorf("invalid concurrency member identity %q", value)
	}
	if parts[0] != "WorkflowRun" && parts[0] != "WorkflowJob" {
		return nil, fmt.Errorf("invalid concurrency member kind %q", parts[0])
	}
	return &concurrencyMember{Kind: parts[0], Name: parts[1], UID: types.UID(parts[2])}, nil
}

func concurrencyPending(lease *coordinationv1.Lease) (*concurrencyMember, error) {
	value := lease.Annotations[concurrencyPendingAnnotation]
	if value == "" {
		return nil, nil
	}
	member := &concurrencyMember{}
	if err := json.Unmarshal([]byte(value), member); err != nil {
		return nil, fmt.Errorf("decode pending member from concurrency Lease %q: %w", lease.Name, err)
	}
	if member.Kind != "WorkflowRun" && member.Kind != "WorkflowJob" || member.Name == "" || member.UID == "" {
		return nil, fmt.Errorf("concurrency Lease %q has an invalid pending member", lease.Name)
	}
	return member, nil
}

func setConcurrencyPending(lease *coordinationv1.Lease, member *concurrencyMember) error {
	if lease.Annotations == nil {
		lease.Annotations = map[string]string{}
	}
	if member == nil {
		delete(lease.Annotations, concurrencyPendingAnnotation)
		return nil
	}
	encoded, err := json.Marshal(member)
	if err != nil {
		return err
	}
	lease.Annotations[concurrencyPendingAnnotation] = string(encoded)
	return nil
}

func concurrencyHolder(lease *coordinationv1.Lease) (*concurrencyMember, error) {
	if lease.Spec.HolderIdentity == nil || *lease.Spec.HolderIdentity == "" {
		return nil, nil
	}
	return parseConcurrencyIdentity(*lease.Spec.HolderIdentity)
}

func (r *WorkflowRunReconciler) acquireConcurrency(ctx context.Context, namespace string, scope concurrencyScope, group string, member concurrencyMember, registered bool) (concurrencyGateResult, error) {
	key := client.ObjectKey{Namespace: namespace, Name: concurrencyLeaseName(scope, group)}
	for attempt := 0; attempt < 5; attempt++ {
		lease := &coordinationv1.Lease{}
		err := r.APIReader.Get(ctx, key, lease)
		if apierrors.IsNotFound(err) {
			holderMember, pendingMember, err := r.bootstrapWorkflowRunConcurrency(ctx, namespace, scope, group)
			if err != nil {
				return concurrencyGateResult{}, err
			}
			if holderMember == nil {
				holderMember = &member
			}
			holder := holderMember.identity()
			now := metav1.NewMicroTime(r.now())
			lease = &coordinationv1.Lease{
				ObjectMeta: metav1.ObjectMeta{
					Name: key.Name, Namespace: key.Namespace,
					Annotations: map[string]string{
						concurrencyGroupAnnotation: group, concurrencyProjectAnnotation: scope.project,
						concurrencyRepositoryAnnotation: strconv.FormatInt(scope.repositoryID, 10),
					},
				},
				Spec: coordinationv1.LeaseSpec{HolderIdentity: &holder, AcquireTime: &now},
			}
			if err := setConcurrencyPending(lease, pendingMember); err != nil {
				return concurrencyGateResult{}, err
			}
			if err := r.Create(ctx, lease); err != nil {
				if apierrors.IsAlreadyExists(err) {
					continue
				}
				return concurrencyGateResult{}, err
			}
			continue
		}
		if err != nil {
			return concurrencyGateResult{}, err
		}
		if err := validateConcurrencyLease(lease, scope, group); err != nil {
			return concurrencyGateResult{}, err
		}

		holder, err := concurrencyHolder(lease)
		if err != nil {
			return concurrencyGateResult{}, err
		}
		pending, err := concurrencyPending(lease)
		if err != nil {
			return concurrencyGateResult{}, err
		}
		changed := false
		if holder != nil {
			active, err := r.concurrencyMemberActive(ctx, namespace, *holder)
			if err != nil {
				return concurrencyGateResult{}, err
			}
			if !active {
				holder = nil
				lease.Spec.HolderIdentity = nil
				changed = true
			}
		}
		if pending != nil {
			active, err := r.concurrencyMemberActive(ctx, namespace, *pending)
			if err != nil {
				return concurrencyGateResult{}, err
			}
			if !active {
				pending = nil
				if err := setConcurrencyPending(lease, nil); err != nil {
					return concurrencyGateResult{}, err
				}
				changed = true
			}
		}
		if holder == nil && pending != nil {
			holder = pending
			pending = nil
			identity := holder.identity()
			lease.Spec.HolderIdentity = &identity
			now := metav1.NewMicroTime(r.now())
			lease.Spec.AcquireTime = &now
			lease.Spec.LeaseTransitions = incrementLeaseTransitions(lease.Spec.LeaseTransitions)
			if err := setConcurrencyPending(lease, nil); err != nil {
				return concurrencyGateResult{}, err
			}
			changed = true
		}

		result := concurrencyGateResult{}
		switch {
		case holder != nil && holder.identity() == member.identity():
			result.state = concurrencyGateAcquired
		case pending != nil && pending.identity() == member.identity():
			result.state = concurrencyGateWaiting
			if pending.CancelInProgress && holder != nil {
				copy := *holder
				result.cancelOwner = &copy
			}
		case registered:
			result.state = concurrencyGateSuperseded
		default:
			if holder == nil {
				holder = &member
				identity := member.identity()
				lease.Spec.HolderIdentity = &identity
				now := metav1.NewMicroTime(r.now())
				lease.Spec.AcquireTime = &now
				lease.Spec.LeaseTransitions = incrementLeaseTransitions(lease.Spec.LeaseTransitions)
				result.state = concurrencyGateAcquired
			} else {
				if pending != nil {
					copy := *pending
					result.displaced = &copy
				}
				pending = &member
				if err := setConcurrencyPending(lease, pending); err != nil {
					return concurrencyGateResult{}, err
				}
				result.state = concurrencyGateWaiting
				if member.CancelInProgress {
					copy := *holder
					result.cancelOwner = &copy
				}
			}
			changed = true
		}
		if !changed {
			return result, nil
		}
		if err := r.Update(ctx, lease); err != nil {
			if apierrors.IsConflict(err) || apierrors.IsNotFound(err) {
				continue
			}
			return concurrencyGateResult{}, err
		}
		return result, nil
	}
	return concurrencyGateResult{}, fmt.Errorf("concurrency Lease %q changed too frequently", key.Name)
}

func (r *WorkflowRunReconciler) bootstrapWorkflowRunConcurrency(ctx context.Context, namespace string, scope concurrencyScope, group string) (*concurrencyMember, *concurrencyMember, error) {
	runs := &actionsv1alpha1.WorkflowRunList{}
	if err := r.APIReader.List(ctx, runs, client.InNamespace(namespace)); err != nil {
		return nil, nil, err
	}
	holders := make([]workflowRunConcurrencyCandidate, 0)
	pending := make([]workflowRunConcurrencyCandidate, 0)
	for index := range runs.Items {
		run := &runs.Items[index]
		if terminalRun(run) || run.Status.Concurrency == nil || !workflowRunMatchesConcurrencyScope(run, scope) || !strings.EqualFold(run.Status.Concurrency.Group, group) {
			continue
		}
		planned := meta.FindStatusCondition(run.Status.Conditions, actionsv1alpha1.WorkflowRunConditionPlanned)
		item := workflowRunConcurrencyCandidate{member: workflowRunConcurrencyMember(run), run: run}
		switch {
		case planned != nil && planned.Status == metav1.ConditionTrue:
			holders = append(holders, item)
		case waitingForConcurrencyCondition(planned):
			pending = append(pending, item)
		}
	}
	sortConcurrencyCandidates(holders)
	sortConcurrencyCandidates(pending)
	var holder *concurrencyMember
	if len(holders) > 0 {
		copy := holders[0].member
		holder = &copy
	}
	var waiting *concurrencyMember
	if len(pending) > 0 {
		copy := pending[len(pending)-1].member
		waiting = &copy
	}
	if holder == nil && waiting != nil {
		holder = waiting
		waiting = nil
	}
	return holder, waiting, nil
}

func workflowRunMatchesConcurrencyScope(run *actionsv1alpha1.WorkflowRun, scope concurrencyScope) bool {
	return run.Spec.ProjectRef.Name == scope.project && run.Spec.Source.Type == actionsv1alpha1.SourceTypeGitHub &&
		run.Spec.Source.GitHub != nil && run.Spec.Source.GitHub.Repository.ID == scope.repositoryID
}

func sortConcurrencyCandidates(candidates []workflowRunConcurrencyCandidate) {
	sort.Slice(candidates, func(left, right int) bool {
		leftRun := candidates[left].run
		rightRun := candidates[right].run
		if leftRun.CreationTimestamp.Time.Equal(rightRun.CreationTimestamp.Time) {
			return leftRun.Name < rightRun.Name
		}
		return leftRun.CreationTimestamp.Time.Before(rightRun.CreationTimestamp.Time)
	})
}

func validateConcurrencyLease(lease *coordinationv1.Lease, scope concurrencyScope, group string) error {
	wantRepository := strconv.FormatInt(scope.repositoryID, 10)
	if lease.Annotations[concurrencyProjectAnnotation] != scope.project ||
		lease.Annotations[concurrencyRepositoryAnnotation] != wantRepository ||
		!strings.EqualFold(lease.Annotations[concurrencyGroupAnnotation], group) {
		return fmt.Errorf("concurrency Lease %q does not match Project %q, repository %d, and group %q", lease.Name, scope.project, scope.repositoryID, group)
	}
	return nil
}

func incrementLeaseTransitions(value *int32) *int32 {
	result := int32(1)
	if value != nil {
		result = *value + 1
	}
	return &result
}

func (r *WorkflowRunReconciler) concurrencyMemberActive(ctx context.Context, namespace string, member concurrencyMember) (bool, error) {
	switch member.Kind {
	case "WorkflowRun":
		run := &actionsv1alpha1.WorkflowRun{}
		if err := r.APIReader.Get(ctx, client.ObjectKey{Namespace: namespace, Name: member.Name}, run); err != nil {
			if !apierrors.IsNotFound(err) {
				return false, err
			}
			return activeRuntimeWorkloads(ctx, r.APIReader, &actionsv1alpha1.WorkflowRun{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, UID: member.UID}})
		}
		if run.UID != member.UID {
			return activeRuntimeWorkloads(ctx, r.APIReader, &actionsv1alpha1.WorkflowRun{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, UID: member.UID}})
		}
		return !terminalRun(run), nil
	case "WorkflowJob":
		job := &actionsv1alpha1.WorkflowJob{}
		if err := r.APIReader.Get(ctx, client.ObjectKey{Namespace: namespace, Name: member.Name}, job); err != nil {
			if !apierrors.IsNotFound(err) {
				return false, err
			}
			return r.workflowJobUIDExecutionActive(ctx, namespace, member.UID)
		}
		if job.UID != member.UID {
			return r.workflowJobUIDExecutionActive(ctx, namespace, member.UID)
		}
		if !workflowJobTerminal(job) {
			return true, nil
		}
		return r.workflowJobExecutionActive(ctx, job)
	default:
		return false, fmt.Errorf("unsupported concurrency member kind %q", member.Kind)
	}
}

func (r *WorkflowRunReconciler) workflowJobUIDExecutionActive(ctx context.Context, namespace string, uid types.UID) (bool, error) {
	selector := client.MatchingLabels{actionsv1alpha1.LabelWorkflowJobUID: string(uid)}
	nativeJobs := &batchv1.JobList{}
	if err := r.APIReader.List(ctx, nativeJobs, client.InNamespace(namespace), selector); err != nil {
		return false, err
	}
	for index := range nativeJobs.Items {
		if !jobTerminal(&nativeJobs.Items[index]) {
			return true, nil
		}
	}
	pods := &corev1.PodList{}
	if err := r.APIReader.List(ctx, pods, client.InNamespace(namespace), selector); err != nil {
		return false, err
	}
	for index := range pods.Items {
		if podActive(&pods.Items[index]) {
			return true, nil
		}
	}
	return false, nil
}

func (r *WorkflowRunReconciler) workflowJobExecutionActive(ctx context.Context, job *actionsv1alpha1.WorkflowJob) (bool, error) {
	nativeJob := &batchv1.Job{}
	key := client.ObjectKey{Namespace: job.Namespace, Name: job.Name}
	if err := r.APIReader.Get(ctx, key, nativeJob); err != nil {
		if !apierrors.IsNotFound(err) {
			return false, err
		}
	} else {
		if !metav1.IsControlledBy(nativeJob, job) {
			return false, fmt.Errorf("native Job %q is not controlled by WorkflowJob %q", nativeJob.Name, job.Name)
		}
		if !jobTerminal(nativeJob) {
			return true, nil
		}
	}
	pods := &corev1.PodList{}
	selector := client.MatchingLabels{actionsv1alpha1.LabelWorkflowJobUID: string(job.UID)}
	if err := r.APIReader.List(ctx, pods, client.InNamespace(job.Namespace), selector); err != nil {
		return false, err
	}
	for index := range pods.Items {
		if podActive(&pods.Items[index]) {
			return true, nil
		}
	}
	return false, nil
}

func (r *WorkflowRunReconciler) releaseConcurrency(ctx context.Context, namespace string, scope concurrencyScope, group string, member concurrencyMember) error {
	key := client.ObjectKey{Namespace: namespace, Name: concurrencyLeaseName(scope, group)}
	for attempt := 0; attempt < 5; attempt++ {
		lease := &coordinationv1.Lease{}
		if err := r.APIReader.Get(ctx, key, lease); err != nil {
			return client.IgnoreNotFound(err)
		}
		if err := validateConcurrencyLease(lease, scope, group); err != nil {
			return err
		}
		holder, err := concurrencyHolder(lease)
		if err != nil {
			return err
		}
		pending, err := concurrencyPending(lease)
		if err != nil {
			return err
		}
		identity := member.identity()
		changed := false
		if pending != nil && pending.identity() == identity {
			pending = nil
			if err := setConcurrencyPending(lease, nil); err != nil {
				return err
			}
			changed = true
		}
		if holder == nil || holder.identity() != identity {
			if !changed {
				return nil
			}
			if err := r.Update(ctx, lease); err != nil {
				if apierrors.IsConflict(err) || apierrors.IsNotFound(err) {
					continue
				}
				return err
			}
			return nil
		}
		if pending != nil {
			active, err := r.concurrencyMemberActive(ctx, namespace, *pending)
			if err != nil {
				return err
			}
			if active {
				promoted := pending.identity()
				lease.Spec.HolderIdentity = &promoted
				now := metav1.NewMicroTime(r.now())
				lease.Spec.AcquireTime = &now
				lease.Spec.LeaseTransitions = incrementLeaseTransitions(lease.Spec.LeaseTransitions)
				if err := setConcurrencyPending(lease, nil); err != nil {
					return err
				}
				if err := r.Update(ctx, lease); err != nil {
					if apierrors.IsConflict(err) || apierrors.IsNotFound(err) {
						continue
					}
					return err
				}
				return nil
			}
		}
		resourceVersion := lease.ResourceVersion
		uid := lease.UID
		err = r.Delete(ctx, lease, &client.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid, ResourceVersion: &resourceVersion}})
		if apierrors.IsConflict(err) {
			continue
		}
		return client.IgnoreNotFound(err)
	}
	return fmt.Errorf("concurrency Lease %q changed too frequently", key.Name)
}

func (r *WorkflowRunReconciler) requestConcurrencyCancellation(ctx context.Context, namespace string, member concurrencyMember, superseded bool) error {
	reason := concurrencyCancelledReason
	message := "A newer concurrency member requested cancellation"
	if superseded {
		reason = concurrencySupersededReason
		message = "A newer pending member replaced this concurrency member"
	}
	switch member.Kind {
	case "WorkflowRun":
		run := &actionsv1alpha1.WorkflowRun{}
		if err := r.APIReader.Get(ctx, client.ObjectKey{Namespace: namespace, Name: member.Name}, run); err != nil {
			return client.IgnoreNotFound(err)
		}
		if run.UID != member.UID || terminalRun(run) {
			return nil
		}
		return r.cancelWorkflowRun(ctx, run)
	case "WorkflowJob":
		job := &actionsv1alpha1.WorkflowJob{}
		if err := r.APIReader.Get(ctx, client.ObjectKey{Namespace: namespace, Name: member.Name}, job); err != nil {
			return client.IgnoreNotFound(err)
		}
		if job.UID != member.UID || workflowJobTerminal(job) {
			return nil
		}
		if job.Status.RunnerRef == nil {
			return r.completeUnscheduledWorkflowJob(ctx, job, actionsv1alpha1.WorkflowJobResultCancelled, reason, message)
		}
		return r.setWorkflowJobCancellationRequested(ctx, job, true, reason, message)
	default:
		return fmt.Errorf("unsupported concurrency member kind %q", member.Kind)
	}
}

func workflowJobConcurrencyRegistered(job *actionsv1alpha1.WorkflowJob) bool {
	return meta.FindStatusCondition(job.Status.Conditions, actionsv1alpha1.WorkflowJobConditionConcurrencyAcquired) != nil
}
