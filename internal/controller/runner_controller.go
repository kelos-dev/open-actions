package controller

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"time"

	actionsv1alpha1 "github.com/kelos-dev/open-actions/api/v1alpha1"
	githubclient "github.com/kelos-dev/open-actions/internal/github"
	"github.com/kelos-dev/open-actions/internal/runner"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apiEquality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

var errRunnerAlreadyAssigned = errors.New("runner is already assigned a WorkflowJob")

const (
	jobPlanVolume            = "open-actions-job"
	workspaceVolume          = "open-actions-workspace"
	dockerSocketVolume       = "open-actions-docker-socket"
	dockerStorageVolume      = "open-actions-docker-storage"
	jobPlanMountPath         = "/var/run/open-actions"
	workspaceVolumeMountPath = "/workspace"
	dockerSocketDirectory    = "/var/run/open-actions-docker"
	dockerSocketPath         = dockerSocketDirectory + "/docker.sock"
	dockerHost               = "unix://" + dockerSocketPath
	dockerStoragePath        = "/var/lib/docker"
	// The repository lives below the volume root so the runner owns its Git worktree.
	workspacePath               = workspaceVolumeMountPath + "/repository"
	jobTTLSeconds               = int32(3600)
	jobTimeoutSeconds           = int64(50 * 60)
	jobStartTimeout             = 5 * time.Minute
	runnerFinalizer             = "actions.kelos.dev/runner-finalizer"
	workflowJobQueuedIndex      = "actions.kelos.dev/workflow-job-queued"
	workflowJobRunnerNameIndex  = "actions.kelos.dev/workflow-job-runner-name"
	workflowJobProjectNameIndex = "actions.kelos.dev/workflow-job-project-name"
)

type RunnerReconciler struct {
	client.Client
	APIReader client.Reader
	GitHub    *githubclient.Client
}

func (r *RunnerReconciler) Reconcile(ctx context.Context, request ctrl.Request) (ctrl.Result, error) {
	runnerObject := &actionsv1alpha1.Runner{}
	if err := r.Get(ctx, request.NamespacedName, runnerObject); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	deleting := !runnerObject.DeletionTimestamp.IsZero()
	if !deleting && !controllerutil.ContainsFinalizer(runnerObject, runnerFinalizer) {
		controllerutil.AddFinalizer(runnerObject, runnerFinalizer)
		if err := r.Update(ctx, runnerObject); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}
	if deleting && !controllerutil.ContainsFinalizer(runnerObject, runnerFinalizer) {
		return ctrl.Result{}, nil
	}

	workflowJob, err := r.assignedWorkflowJob(ctx, runnerObject)
	if err != nil {
		return ctrl.Result{}, err
	}
	if deleting && workflowJob == nil {
		controllerutil.RemoveFinalizer(runnerObject, runnerFinalizer)
		return ctrl.Result{}, r.Update(ctx, runnerObject)
	}
	if workflowJob != nil {
		if err := r.updateWorkflowJobScheduled(ctx, workflowJob); err != nil {
			return ctrl.Result{}, err
		}
		found, terminal, err := r.observeNativeJob(ctx, workflowJob)
		if err != nil {
			if !found {
				expired, failureErr := r.failExpiredPreStart(ctx, workflowJob, err.Error())
				if failureErr != nil {
					return ctrl.Result{}, failureErr
				}
				if expired {
					if statusErr := r.updateRunnerStatus(ctx, runnerObject, metav1.ConditionTrue, "Ready", "Runner is operational", nil); statusErr != nil {
						return ctrl.Result{}, statusErr
					}
					return ctrl.Result{Requeue: true}, nil
				}
			}
			return ctrl.Result{}, err
		}
		if found {
			if terminal {
				if err := r.updateRunnerStatus(ctx, runnerObject, metav1.ConditionTrue, "Ready", "Runner is operational", nil); err != nil {
					return ctrl.Result{}, err
				}
				return ctrl.Result{Requeue: true}, nil
			}
			return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
		}
	}

	project := &actionsv1alpha1.Project{}
	projectKey := client.ObjectKey{Namespace: runnerObject.Namespace, Name: runnerObject.Spec.ProjectRef.Name}
	if err := r.APIReader.Get(ctx, projectKey, project); err != nil {
		message := fmt.Sprintf("Get Project %q: %v", projectKey.Name, err)
		if workflowJob != nil {
			expired, failureErr := r.failExpiredPreStart(ctx, workflowJob, message)
			if failureErr != nil {
				return ctrl.Result{}, failureErr
			}
			if expired {
				if statusErr := r.updateRunnerStatus(ctx, runnerObject, metav1.ConditionFalse, "ProjectUnavailable", message, nil); statusErr != nil {
					return ctrl.Result{}, statusErr
				}
				return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
			}
		}
		if statusErr := r.updateRunnerStatus(ctx, runnerObject, metav1.ConditionFalse, "ProjectUnavailable", message, runnerObject.Status.WorkflowJobRef); statusErr != nil {
			return ctrl.Result{}, statusErr
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	projectConfigured := meta.FindStatusCondition(project.Status.Conditions, actionsv1alpha1.ProjectConditionConfigured)
	if projectConfigured == nil || projectConfigured.Status != metav1.ConditionTrue {
		message := fmt.Sprintf("Project %q is not configured", project.Name)
		if projectConfigured != nil && projectConfigured.Message != "" {
			message = projectConfigured.Message
		}
		if workflowJob != nil {
			expired, failureErr := r.failExpiredPreStart(ctx, workflowJob, message)
			if failureErr != nil {
				return ctrl.Result{}, failureErr
			}
			if expired {
				if statusErr := r.updateRunnerStatus(ctx, runnerObject, metav1.ConditionFalse, "ProjectNotConfigured", message, nil); statusErr != nil {
					return ctrl.Result{}, statusErr
				}
				return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
			}
		}
		if err := r.updateRunnerStatus(ctx, runnerObject, metav1.ConditionFalse, "ProjectNotConfigured", message, runnerObject.Status.WorkflowJobRef); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	if workflowJob == nil {
		workflowJob, err = r.claimWorkflowJob(ctx, runnerObject, project)
		if err != nil {
			if errors.Is(err, errRunnerAlreadyAssigned) {
				return ctrl.Result{Requeue: true}, nil
			}
			return ctrl.Result{}, err
		}
		if workflowJob == nil {
			if err := r.updateRunnerStatus(ctx, runnerObject, metav1.ConditionTrue, "Ready", "Runner is operational", nil); err != nil {
				return ctrl.Result{}, err
			}
			return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
		}
		return ctrl.Result{Requeue: true}, nil
	}

	terminal, err := r.executeWorkflowJob(ctx, runnerObject, workflowJob, project)
	if err != nil {
		expired, failureErr := r.failExpiredPreStart(ctx, workflowJob, err.Error())
		if failureErr != nil {
			return ctrl.Result{}, failureErr
		}
		if expired {
			if statusErr := r.updateRunnerStatus(ctx, runnerObject, metav1.ConditionTrue, "Ready", "Runner is operational", nil); statusErr != nil {
				return ctrl.Result{}, statusErr
			}
			return ctrl.Result{Requeue: true}, nil
		}
		return ctrl.Result{}, err
	}
	if terminal {
		if err := r.updateRunnerStatus(ctx, runnerObject, metav1.ConditionTrue, "Ready", "Runner is operational", nil); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}
	return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
}

func (r *RunnerReconciler) assignedWorkflowJob(ctx context.Context, runnerObject *actionsv1alpha1.Runner) (*actionsv1alpha1.WorkflowJob, error) {
	if runnerObject.Status.WorkflowJobRef != nil {
		workflowJob := &actionsv1alpha1.WorkflowJob{}
		key := client.ObjectKey{Namespace: runnerObject.Namespace, Name: runnerObject.Status.WorkflowJobRef.Name}
		if err := r.APIReader.Get(ctx, key, workflowJob); err != nil {
			if apierrors.IsNotFound(err) {
				if err := r.updateRunnerStatus(ctx, runnerObject, metav1.ConditionTrue, "Ready", "Runner is operational", nil); err != nil {
					return nil, err
				}
				return nil, nil
			}
			return nil, err
		}
		if terminalWorkflowJob(workflowJob) {
			if err := r.finalizeWorkflowJob(ctx, workflowJob); err != nil {
				return nil, err
			}
			if err := r.updateRunnerStatus(ctx, runnerObject, metav1.ConditionTrue, "Ready", "Runner is operational", nil); err != nil {
				return nil, err
			}
			return nil, nil
		}
		if workflowJob.Status.RunnerRef == nil || workflowJob.Status.RunnerRef.Name != runnerObject.Name {
			if err := r.updateRunnerStatus(ctx, runnerObject, metav1.ConditionTrue, "Ready", "Runner is operational", nil); err != nil {
				return nil, err
			}
			return nil, nil
		}
		return workflowJob, nil
	}

	jobs := &actionsv1alpha1.WorkflowJobList{}
	if err := r.List(ctx, jobs, client.InNamespace(runnerObject.Namespace), client.MatchingFields{workflowJobRunnerNameIndex: runnerObject.Name}); err != nil {
		return nil, err
	}
	for index := range jobs.Items {
		workflowJob := &jobs.Items[index]
		if workflowJob.Status.RunnerRef != nil && workflowJob.Status.RunnerRef.Name == runnerObject.Name && !terminalWorkflowJob(workflowJob) {
			if err := r.updateRunnerStatus(ctx, runnerObject, metav1.ConditionTrue, "Ready", "Runner is operational", &corev1.LocalObjectReference{Name: workflowJob.Name}); err != nil {
				return nil, err
			}
			return workflowJob, nil
		}
	}
	return nil, nil
}

func (r *RunnerReconciler) claimWorkflowJob(ctx context.Context, runnerObject *actionsv1alpha1.Runner, project *actionsv1alpha1.Project) (*actionsv1alpha1.WorkflowJob, error) {
	jobs := &actionsv1alpha1.WorkflowJobList{}
	if err := r.List(ctx, jobs,
		client.InNamespace(runnerObject.Namespace),
		client.MatchingFields{workflowJobQueuedIndex: "true", workflowJobProjectNameIndex: project.Name},
	); err != nil {
		return nil, err
	}
	matrixActivity := map[string]int32{}
	for index := range jobs.Items {
		matrix := jobs.Items[index].Spec.Matrix
		if matrix != nil && matrix.MaxParallel > 0 {
			var err error
			matrixActivity, err = r.activeMatrixJobs(ctx, runnerObject.Namespace, project)
			if err != nil {
				return nil, err
			}
			break
		}
	}
	candidates := make([]*actionsv1alpha1.WorkflowJob, 0)
	plannedRuns := map[string]bool{}
	loadedRuns := map[string]bool{}
	for index := range jobs.Items {
		workflowJob := &jobs.Items[index]
		if !workflowJob.DeletionTimestamp.IsZero() || terminalWorkflowJob(workflowJob) {
			continue
		}
		if workflowJob.Labels[actionsv1alpha1.LabelProjectUID] != string(project.UID) {
			if err := r.failQueuedWorkflowJob(ctx, workflowJob, "ProjectRecreated", fmt.Sprintf("Project %q was recreated before the job was assigned", project.Name)); err != nil {
				return nil, err
			}
			continue
		}
		if !runnerLabelsMatch(runnerObject.Spec.Labels, workflowJob.Spec.RunsOn) {
			continue
		}
		if matrix := workflowJob.Spec.Matrix; matrix != nil && matrix.MaxParallel > 0 && matrixActivity[matrixJobGroup(workflowJob)] >= matrix.MaxParallel {
			continue
		}
		runName := workflowJob.Spec.WorkflowRunRef.Name
		if !loadedRuns[runName] {
			run := &actionsv1alpha1.WorkflowRun{}
			err := r.APIReader.Get(ctx, client.ObjectKey{Namespace: workflowJob.Namespace, Name: runName}, run)
			if err != nil && !apierrors.IsNotFound(err) {
				return nil, err
			}
			if err == nil {
				condition := meta.FindStatusCondition(run.Status.Conditions, actionsv1alpha1.WorkflowRunConditionPlanned)
				plannedRuns[runName] = run.DeletionTimestamp.IsZero() && condition != nil && condition.Status == metav1.ConditionTrue
			}
			loadedRuns[runName] = true
		}
		if plannedRuns[runName] {
			candidates = append(candidates, workflowJob)
		}
	}
	if len(candidates) == 0 {
		return nil, nil
	}
	sort.Slice(candidates, func(left, right int) bool {
		if candidates[left].CreationTimestamp.Time.Equal(candidates[right].CreationTimestamp.Time) {
			return candidates[left].Name < candidates[right].Name
		}
		return candidates[left].CreationTimestamp.Time.Before(candidates[right].CreationTimestamp.Time)
	})
	workflowJob := candidates[0]
	jobRef := &corev1.LocalObjectReference{Name: workflowJob.Name}
	currentRunner := &actionsv1alpha1.Runner{}
	if err := r.APIReader.Get(ctx, client.ObjectKeyFromObject(runnerObject), currentRunner); err != nil {
		return nil, err
	}
	if currentRunner.UID != runnerObject.UID {
		return nil, fmt.Errorf("Runner %q was recreated before claiming a WorkflowJob", runnerObject.Name)
	}
	if !currentRunner.DeletionTimestamp.IsZero() || currentRunner.Status.WorkflowJobRef != nil {
		return nil, errRunnerAlreadyAssigned
	}
	if err := r.updateRunnerStatus(ctx, currentRunner, metav1.ConditionTrue, "Ready", "Runner is operational", jobRef); err != nil {
		return nil, err
	}
	if err := r.assignWorkflowJob(ctx, workflowJob, runnerObject.Name); err != nil {
		return nil, errors.Join(err, r.releaseRunnerClaim(ctx, currentRunner, workflowJob.Name))
	}
	return workflowJob, nil
}

func (r *RunnerReconciler) activeMatrixJobs(ctx context.Context, namespace string, project *actionsv1alpha1.Project) (map[string]int32, error) {
	jobs := &actionsv1alpha1.WorkflowJobList{}
	if err := r.APIReader.List(ctx, jobs, client.InNamespace(namespace), client.MatchingLabels{actionsv1alpha1.LabelProjectUID: string(project.UID)}); err != nil {
		return nil, err
	}
	active := make(map[string]int32)
	for index := range jobs.Items {
		job := &jobs.Items[index]
		if job.Spec.Matrix != nil && job.Status.RunnerRef != nil && !terminalWorkflowJob(job) {
			active[matrixJobGroup(job)]++
		}
	}
	return active, nil
}

func matrixJobGroup(job *actionsv1alpha1.WorkflowJob) string {
	runID := job.Labels[actionsv1alpha1.LabelWorkflowRunUID]
	if runID == "" {
		runID = job.Spec.WorkflowRunRef.Name
	}
	return runID + "\x00" + job.Spec.Matrix.LogicalJobID
}

func (r *RunnerReconciler) releaseRunnerClaim(ctx context.Context, runnerObject *actionsv1alpha1.Runner, workflowJobName string) error {
	current := &actionsv1alpha1.Runner{}
	if err := r.APIReader.Get(ctx, client.ObjectKeyFromObject(runnerObject), current); err != nil {
		return client.IgnoreNotFound(err)
	}
	if current.UID != runnerObject.UID || current.Status.WorkflowJobRef == nil || current.Status.WorkflowJobRef.Name != workflowJobName {
		return nil
	}
	return r.updateRunnerStatus(ctx, current, metav1.ConditionTrue, "Ready", "Runner is operational", nil)
}

func (r *RunnerReconciler) executeWorkflowJob(ctx context.Context, runnerObject *actionsv1alpha1.Runner, workflowJob *actionsv1alpha1.WorkflowJob, project *actionsv1alpha1.Project) (bool, error) {
	run := &actionsv1alpha1.WorkflowRun{}
	if err := r.Get(ctx, client.ObjectKey{Namespace: workflowJob.Namespace, Name: workflowJob.Spec.WorkflowRunRef.Name}, run); err != nil {
		return false, err
	}
	if run.Spec.ProjectRef.Name != project.Name {
		return false, fmt.Errorf("WorkflowJob %q belongs to Project %q, not Runner project %q", workflowJob.Name, run.Spec.ProjectRef.Name, project.Name)
	}
	if !metav1.IsControlledBy(workflowJob, run) {
		return false, fmt.Errorf("WorkflowJob %q is not controlled by WorkflowRun %q", workflowJob.Name, run.Name)
	}
	if found, terminal, err := r.observeNativeJob(ctx, workflowJob); err != nil || found {
		return terminal, err
	}
	if canceled, err := r.workflowJobCancellationRequested(ctx, workflowJob, run); err != nil {
		return false, err
	} else if canceled {
		return true, r.cancelWorkflowJob(ctx, workflowJob)
	}
	if workflowJob.Labels[actionsv1alpha1.LabelProjectUID] != string(project.UID) {
		return true, r.failAssignedWorkflowJob(ctx, workflowJob, "ProjectRecreated", fmt.Sprintf("Project %q was recreated before execution started", project.Name))
	}
	if workflowJobStarted(workflowJob) {
		active, err := r.activeWorkflowJobPods(ctx, workflowJob)
		if err != nil {
			return false, err
		}
		if active {
			return false, nil
		}
		return true, r.failAssignedWorkflowJob(ctx, workflowJob, "ExecutionStateLost", "The native Job disappeared after execution started")
	}
	plan := &corev1.ConfigMap{}
	planKey := client.ObjectKey{Namespace: workflowJob.Namespace, Name: childName(workflowJob.Name, "plan")}
	if err := r.APIReader.Get(ctx, planKey, plan); err != nil {
		if apierrors.IsNotFound(err) {
			return true, r.failAssignedWorkflowJob(ctx, workflowJob, "PlanUnavailable", fmt.Sprintf("Job plan ConfigMap %q is missing", planKey.Name))
		}
		return false, fmt.Errorf("get job plan ConfigMap %q: %w", planKey.Name, err)
	}
	if !metav1.IsControlledBy(plan, workflowJob) {
		return false, fmt.Errorf("job plan ConfigMap %q is not controlled by WorkflowJob %q", plan.Name, workflowJob.Name)
	}
	githubConfig := project.Spec.Source.GitHub
	githubSource := run.Spec.Source.GitHub
	privateKey, err := secretValue(ctx, r.APIReader, project.Namespace, githubConfig.PrivateKeySecretRef)
	if err != nil {
		return false, err
	}
	installation, err := r.GitHub.Installation(ctx, githubConfig.AppID, githubConfig.InstallationID, privateKey, githubSource.Repository.Name, githubclient.InstallationPermissions{ContentsRead: true})
	if err != nil {
		return false, err
	}
	if canceled, err := r.workflowJobCancellationRequested(ctx, workflowJob, run); err != nil {
		return false, err
	} else if canceled {
		return true, r.cancelWorkflowJob(ctx, workflowJob)
	}
	if err := r.ensureAuthSecret(ctx, workflowJob, installation.Token()); err != nil {
		return false, err
	}
	nativeJob, err := r.buildJob(workflowJob, run, project, runnerObject)
	if err != nil {
		return false, err
	}
	if canceled, err := r.workflowJobCancellationRequested(ctx, workflowJob, run); err != nil {
		return false, err
	} else if canceled {
		return true, r.cancelWorkflowJob(ctx, workflowJob)
	}
	if err := r.Create(ctx, nativeJob); err != nil {
		if apierrors.IsAlreadyExists(err) {
			_, terminal, observeErr := r.observeNativeJob(ctx, workflowJob)
			return terminal, observeErr
		}
		return false, err
	}
	return false, r.updateWorkflowJobStatus(ctx, workflowJob, nativeJob)
}

func (r *RunnerReconciler) workflowJobCancellationRequested(ctx context.Context, workflowJob *actionsv1alpha1.WorkflowJob, run *actionsv1alpha1.WorkflowRun) (bool, error) {
	currentWorkflowJob := &actionsv1alpha1.WorkflowJob{}
	if err := r.APIReader.Get(ctx, client.ObjectKeyFromObject(workflowJob), currentWorkflowJob); err != nil {
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		return false, err
	}
	if currentWorkflowJob.UID != workflowJob.UID {
		return false, fmt.Errorf("WorkflowJob %q was recreated before execution started", workflowJob.Name)
	}
	if !currentWorkflowJob.DeletionTimestamp.IsZero() {
		return true, nil
	}
	currentRun := &actionsv1alpha1.WorkflowRun{}
	if err := r.APIReader.Get(ctx, client.ObjectKeyFromObject(run), currentRun); err != nil {
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		return false, err
	}
	if currentRun.UID != run.UID {
		return false, fmt.Errorf("WorkflowRun %q was recreated before execution started", run.Name)
	}
	return !currentRun.DeletionTimestamp.IsZero(), nil
}

func (r *RunnerReconciler) cancelWorkflowJob(ctx context.Context, workflowJob *actionsv1alpha1.WorkflowJob) error {
	err := r.failAssignedWorkflowJob(ctx, workflowJob, "CancellationRequested", "WorkflowJob or WorkflowRun deletion was requested before execution started")
	if apierrors.IsNotFound(err) {
		return r.cleanupAuthSecret(ctx, workflowJob)
	}
	return err
}

func (r *RunnerReconciler) observeNativeJob(ctx context.Context, workflowJob *actionsv1alpha1.WorkflowJob) (bool, bool, error) {
	nativeJob := &batchv1.Job{}
	key := client.ObjectKey{Namespace: workflowJob.Namespace, Name: workflowJob.Name}
	if err := r.Get(ctx, key, nativeJob); err != nil {
		if apierrors.IsNotFound(err) {
			return false, false, nil
		}
		return false, false, err
	}
	if !metav1.IsControlledBy(nativeJob, workflowJob) {
		return false, false, fmt.Errorf("native Job %q is not controlled by WorkflowJob %q", nativeJob.Name, workflowJob.Name)
	}
	terminal := jobTerminal(nativeJob)
	if err := r.updateWorkflowJobStatus(ctx, workflowJob, nativeJob); err != nil {
		return true, terminal, err
	}
	if terminal {
		if err := r.finalizeWorkflowJob(ctx, workflowJob); err != nil {
			return true, true, err
		}
	}
	return true, terminal, nil
}

func workflowJobStarted(workflowJob *actionsv1alpha1.WorkflowJob) bool {
	if workflowJob.Status.StartTime != nil {
		return true
	}
	condition := meta.FindStatusCondition(workflowJob.Status.Conditions, actionsv1alpha1.WorkflowJobConditionSucceeded)
	return condition != nil && condition.Status == metav1.ConditionUnknown && condition.Reason == "JobRunning"
}

func (r *RunnerReconciler) activeWorkflowJobPods(ctx context.Context, workflowJob *actionsv1alpha1.WorkflowJob) (bool, error) {
	pods := &corev1.PodList{}
	if err := r.APIReader.List(ctx, pods, client.InNamespace(workflowJob.Namespace), client.MatchingLabels{
		actionsv1alpha1.LabelWorkflowJobUID: string(workflowJob.UID),
	}); err != nil {
		return false, err
	}
	for index := range pods.Items {
		phase := pods.Items[index].Status.Phase
		if phase != corev1.PodSucceeded && phase != corev1.PodFailed {
			return true, nil
		}
	}
	return false, nil
}

func (r *RunnerReconciler) finalizeWorkflowJob(ctx context.Context, workflowJob *actionsv1alpha1.WorkflowJob) error {
	if err := r.cleanupAuthSecret(ctx, workflowJob); err != nil {
		return err
	}
	nativeJob := &batchv1.Job{}
	nativeJobKey := client.ObjectKey{Namespace: workflowJob.Namespace, Name: workflowJob.Name}
	nativeJobFound := true
	if err := r.Get(ctx, nativeJobKey, nativeJob); err != nil {
		if !apierrors.IsNotFound(err) {
			return err
		}
		nativeJobFound = false
	} else if !metav1.IsControlledBy(nativeJob, workflowJob) {
		return fmt.Errorf("native Job %q is not controlled by WorkflowJob %q", nativeJob.Name, workflowJob.Name)
	}

	if nativeJobFound && nativeJob.Spec.TTLSecondsAfterFinished == nil {
		before := nativeJob.DeepCopy()
		nativeJob.Spec.TTLSecondsAfterFinished = pointerTo(jobTTLSeconds)
		if err := r.Patch(ctx, nativeJob, client.MergeFrom(before)); err != nil {
			return err
		}
	}
	return nil
}

func (r *RunnerReconciler) cleanupAuthSecret(ctx context.Context, workflowJob *actionsv1alpha1.WorkflowJob) error {
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: childName(workflowJob.Name, "auth"), Namespace: workflowJob.Namespace}}
	if err := r.Get(ctx, client.ObjectKeyFromObject(secret), secret); err != nil {
		if !apierrors.IsNotFound(err) {
			return err
		}
	} else {
		if !metav1.IsControlledBy(secret, workflowJob) {
			return fmt.Errorf("authentication Secret %q is not controlled by WorkflowJob %q", secret.Name, workflowJob.Name)
		}
		if err := r.Delete(ctx, secret); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	}
	return nil
}

func (r *RunnerReconciler) ensureAuthSecret(ctx context.Context, workflowJob *actionsv1alpha1.WorkflowJob, token string) error {
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{
		Name:      childName(workflowJob.Name, "auth"),
		Namespace: workflowJob.Namespace,
		Labels: map[string]string{
			actionsv1alpha1.LabelWorkflowRunUID: workflowJob.Labels[actionsv1alpha1.LabelWorkflowRunUID],
			actionsv1alpha1.LabelWorkflowJobUID: string(workflowJob.UID),
		},
	}, Data: map[string][]byte{"token": []byte(token)}}
	if err := controllerutil.SetControllerReference(workflowJob, secret, r.Scheme()); err != nil {
		return err
	}
	if err := r.Create(ctx, secret); err == nil {
		return nil
	} else if !apierrors.IsAlreadyExists(err) {
		return err
	}

	existing := &corev1.Secret{}
	if err := r.APIReader.Get(ctx, client.ObjectKeyFromObject(secret), existing); err != nil {
		return err
	}
	if !metav1.IsControlledBy(existing, workflowJob) {
		return fmt.Errorf("authentication Secret %q is not controlled by WorkflowJob %q", existing.Name, workflowJob.Name)
	}
	before := existing.DeepCopy()
	if existing.Labels == nil {
		existing.Labels = map[string]string{}
	}
	for key, value := range secret.Labels {
		existing.Labels[key] = value
	}
	if existing.Data == nil {
		existing.Data = map[string][]byte{}
	}
	existing.Data["token"] = []byte(token)
	if apiEquality.Semantic.DeepEqual(before, existing) {
		return nil
	}
	return r.Patch(ctx, existing, client.MergeFrom(before))
}

func (r *RunnerReconciler) buildJob(workflowJob *actionsv1alpha1.WorkflowJob, run *actionsv1alpha1.WorkflowRun, project *actionsv1alpha1.Project, runnerObject *actionsv1alpha1.Runner) (*batchv1.Job, error) {
	labels := nativeJobLabels(workflowJob, run, project, runnerObject)
	annotations := map[string]string{
		actionsv1alpha1.AnnotationWorkflowJobID: workflowJob.Spec.JobID,
		actionsv1alpha1.AnnotationRunnerName:    runnerObject.Name,
	}
	if workflowJob.Spec.DisplayName != "" {
		annotations[actionsv1alpha1.AnnotationWorkflowJobDisplayName] = workflowJob.Spec.DisplayName
	}
	podTemplate := corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{Labels: labels, Annotations: annotations},
		Spec: corev1.PodSpec{
			AutomountServiceAccountToken: pointerTo(false),
			RestartPolicy:                corev1.RestartPolicyNever,
			SecurityContext: &corev1.PodSecurityContext{
				FSGroup:      pointerTo(int64(65532)),
				RunAsNonRoot: pointerTo(true),
				SeccompProfile: &corev1.SeccompProfile{
					Type: corev1.SeccompProfileTypeRuntimeDefault,
				},
			},
			Containers: []corev1.Container{{
				Name:      runner.ContainerName,
				Image:     runnerObject.Spec.Execution.Image,
				Resources: runnerResources(runnerObject.Spec.Execution.Resources),
				SecurityContext: &corev1.SecurityContext{
					AllowPrivilegeEscalation: pointerTo(false),
					Capabilities: &corev1.Capabilities{
						Drop: []corev1.Capability{"ALL"},
					},
				},
				Args: []string{"--job-file=" + jobPlanMountPath + "/" + jobPlanKey, "--workspace=" + workspacePath},
				Env: []corev1.EnvVar{{
					Name: "OPEN_ACTIONS_GITHUB_TOKEN",
					ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{Name: childName(workflowJob.Name, "auth")},
						Key:                  "token",
					}},
				}},
				VolumeMounts: []corev1.VolumeMount{
					{Name: jobPlanVolume, MountPath: jobPlanMountPath, ReadOnly: true},
					{Name: workspaceVolume, MountPath: workspaceVolumeMountPath},
				},
			}},
			Volumes: []corev1.Volume{
				{Name: jobPlanVolume, VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: childName(workflowJob.Name, "plan")}}}},
				{Name: workspaceVolume, VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
			},
		},
	}
	if runnerObject.Spec.Execution.Docker != nil {
		configureDockerExecution(&podTemplate.Spec, &podTemplate.Spec.Containers[0], runnerObject.Spec.Execution.Docker)
	}
	backoffLimit := int32(0)
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:        workflowJob.Name,
			Namespace:   workflowJob.Namespace,
			Labels:      labels,
			Annotations: annotations,
		},
		Spec: batchv1.JobSpec{
			ActiveDeadlineSeconds: pointerTo(jobTimeoutSeconds),
			BackoffLimit:          &backoffLimit,
			Template:              podTemplate,
		},
	}
	if err := controllerutil.SetControllerReference(workflowJob, job, r.Scheme()); err != nil {
		return nil, err
	}
	return job, nil
}

func configureDockerExecution(pod *corev1.PodSpec, runnerContainer *corev1.Container, dockerSpec *actionsv1alpha1.RunnerDockerSpec) {
	runnerContainer.Env = append(runnerContainer.Env, corev1.EnvVar{Name: "DOCKER_HOST", Value: dockerHost})
	runnerContainer.VolumeMounts = append(runnerContainer.VolumeMounts, corev1.VolumeMount{Name: dockerSocketVolume, MountPath: dockerSocketDirectory})

	dockerStorage := &corev1.EmptyDirVolumeSource{}
	if dockerSpec.Resources != nil {
		if limit, found := dockerSpec.Resources.Limits[corev1.ResourceEphemeralStorage]; found {
			sizeLimit := limit.DeepCopy()
			dockerStorage.SizeLimit = &sizeLimit
		}
	}
	pod.Volumes = append(pod.Volumes,
		corev1.Volume{Name: dockerSocketVolume, VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
		corev1.Volume{Name: dockerStorageVolume, VolumeSource: corev1.VolumeSource{EmptyDir: dockerStorage}},
	)
	pod.InitContainers = append(pod.InitContainers, corev1.Container{
		Name:            "docker",
		Image:           dockerSpec.Image,
		RestartPolicy:   pointerTo(corev1.ContainerRestartPolicyAlways),
		Args:            []string{"dockerd", "--host=" + dockerHost, "--group=65532"},
		Env:             []corev1.EnvVar{{Name: "DOCKER_HOST", Value: dockerHost}, {Name: "DOCKER_TLS_CERTDIR", Value: ""}},
		Resources:       runnerResources(dockerSpec.Resources),
		SecurityContext: &corev1.SecurityContext{Privileged: pointerTo(true), RunAsNonRoot: pointerTo(false), RunAsUser: pointerTo(int64(0))},
		StartupProbe: &corev1.Probe{
			ProbeHandler:     corev1.ProbeHandler{Exec: &corev1.ExecAction{Command: []string{"docker", "info"}}},
			FailureThreshold: 60,
			PeriodSeconds:    1,
			TimeoutSeconds:   5,
		},
		VolumeMounts: []corev1.VolumeMount{
			{Name: dockerSocketVolume, MountPath: dockerSocketDirectory},
			{Name: dockerStorageVolume, MountPath: dockerStoragePath},
			{Name: workspaceVolume, MountPath: workspaceVolumeMountPath},
		},
	})
}

func runnerResources(resources *actionsv1alpha1.RunnerResources) corev1.ResourceRequirements {
	if resources == nil {
		return corev1.ResourceRequirements{}
	}
	return corev1.ResourceRequirements{
		Limits:   corev1.ResourceList(resources.Limits).DeepCopy(),
		Requests: corev1.ResourceList(resources.Requests).DeepCopy(),
	}
}

func (r *RunnerReconciler) updateWorkflowJobStatus(ctx context.Context, workflowJob *actionsv1alpha1.WorkflowJob, nativeJob *batchv1.Job) error {
	before := workflowJob.Status.DeepCopy()
	if err := setWorkflowJobScheduled(workflowJob); err != nil {
		return err
	}
	if nativeJob.Status.StartTime != nil {
		workflowJob.Status.StartTime = nativeJob.Status.StartTime.DeepCopy()
	}
	switch jobResult(nativeJob) {
	case metav1.ConditionTrue:
		workflowJob.Status.CompletionTime = completionTime(nativeJob)
		meta.SetStatusCondition(&workflowJob.Status.Conditions, metav1.Condition{Type: actionsv1alpha1.WorkflowJobConditionSucceeded, Status: metav1.ConditionTrue, ObservedGeneration: workflowJob.Generation, Reason: "JobSucceeded", Message: "The workflow job succeeded"})
	case metav1.ConditionFalse:
		workflowJob.Status.CompletionTime = completionTime(nativeJob)
		meta.SetStatusCondition(&workflowJob.Status.Conditions, metav1.Condition{Type: actionsv1alpha1.WorkflowJobConditionSucceeded, Status: metav1.ConditionFalse, ObservedGeneration: workflowJob.Generation, Reason: "JobFailed", Message: "The workflow job failed"})
	default:
		meta.SetStatusCondition(&workflowJob.Status.Conditions, metav1.Condition{Type: actionsv1alpha1.WorkflowJobConditionSucceeded, Status: metav1.ConditionUnknown, ObservedGeneration: workflowJob.Generation, Reason: "JobRunning", Message: "The workflow job is running"})
	}
	if apiEquality.Semantic.DeepEqual(before, &workflowJob.Status) {
		return nil
	}
	return r.Status().Update(ctx, workflowJob)
}

func (r *RunnerReconciler) failAssignedWorkflowJob(ctx context.Context, workflowJob *actionsv1alpha1.WorkflowJob, reason, message string) error {
	before := workflowJob.Status.DeepCopy()
	if err := setWorkflowJobScheduled(workflowJob); err != nil {
		return err
	}
	now := metav1.Now()
	workflowJob.Status.CompletionTime = &now
	meta.SetStatusCondition(&workflowJob.Status.Conditions, metav1.Condition{
		Type:               actionsv1alpha1.WorkflowJobConditionSucceeded,
		Status:             metav1.ConditionFalse,
		ObservedGeneration: workflowJob.Generation,
		Reason:             reason,
		Message:            message,
	})
	if !apiEquality.Semantic.DeepEqual(before, &workflowJob.Status) {
		if err := r.Status().Update(ctx, workflowJob); err != nil {
			return err
		}
	}
	return r.cleanupAuthSecret(ctx, workflowJob)
}

func (r *RunnerReconciler) failQueuedWorkflowJob(ctx context.Context, workflowJob *actionsv1alpha1.WorkflowJob, reason, message string) error {
	before := workflowJob.Status.DeepCopy()
	workflowJob.Status.ObservedGeneration = workflowJob.Generation
	now := metav1.Now()
	workflowJob.Status.CompletionTime = &now
	meta.SetStatusCondition(&workflowJob.Status.Conditions, metav1.Condition{
		Type:               actionsv1alpha1.WorkflowJobConditionScheduled,
		Status:             metav1.ConditionFalse,
		ObservedGeneration: workflowJob.Generation,
		Reason:             reason,
		Message:            message,
	})
	meta.SetStatusCondition(&workflowJob.Status.Conditions, metav1.Condition{
		Type:               actionsv1alpha1.WorkflowJobConditionSucceeded,
		Status:             metav1.ConditionFalse,
		ObservedGeneration: workflowJob.Generation,
		Reason:             reason,
		Message:            message,
	})
	if apiEquality.Semantic.DeepEqual(before, &workflowJob.Status) {
		return nil
	}
	return r.Status().Update(ctx, workflowJob)
}

func (r *RunnerReconciler) failExpiredPreStart(ctx context.Context, workflowJob *actionsv1alpha1.WorkflowJob, message string) (bool, error) {
	scheduled := meta.FindStatusCondition(workflowJob.Status.Conditions, actionsv1alpha1.WorkflowJobConditionScheduled)
	if scheduled == nil || time.Since(scheduled.LastTransitionTime.Time) < jobStartTimeout {
		return false, nil
	}
	if err := r.failAssignedWorkflowJob(ctx, workflowJob, "JobStartFailed", fmt.Sprintf("WorkflowJob did not start within %s: %s", jobStartTimeout, message)); err != nil {
		return false, err
	}
	return true, nil
}

func (r *RunnerReconciler) updateWorkflowJobScheduled(ctx context.Context, workflowJob *actionsv1alpha1.WorkflowJob) error {
	before := workflowJob.DeepCopy()
	if err := setWorkflowJobScheduled(workflowJob); err != nil {
		return err
	}
	if apiEquality.Semantic.DeepEqual(&before.Status, &workflowJob.Status) {
		return nil
	}
	return r.Status().Patch(ctx, workflowJob, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{}))
}

func (r *RunnerReconciler) assignWorkflowJob(ctx context.Context, workflowJob *actionsv1alpha1.WorkflowJob, runnerName string) error {
	before := workflowJob.DeepCopy()
	workflowJob.Status.RunnerRef = &corev1.LocalObjectReference{Name: runnerName}
	if err := setWorkflowJobScheduled(workflowJob); err != nil {
		return err
	}
	return r.Status().Patch(ctx, workflowJob, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{}))
}

func setWorkflowJobScheduled(workflowJob *actionsv1alpha1.WorkflowJob) error {
	if workflowJob.Status.RunnerRef == nil {
		return fmt.Errorf("WorkflowJob %q has no assigned Runner", workflowJob.Name)
	}
	workflowJob.Status.ObservedGeneration = workflowJob.Generation
	meta.SetStatusCondition(&workflowJob.Status.Conditions, metav1.Condition{
		Type:               actionsv1alpha1.WorkflowJobConditionScheduled,
		Status:             metav1.ConditionTrue,
		ObservedGeneration: workflowJob.Generation,
		Reason:             "RunnerAssigned",
		Message:            fmt.Sprintf("WorkflowJob is assigned to Runner %q", workflowJob.Status.RunnerRef.Name),
	})
	return nil
}

func (r *RunnerReconciler) updateRunnerStatus(ctx context.Context, runnerObject *actionsv1alpha1.Runner, ready metav1.ConditionStatus, readyReason, readyMessage string, workflowJobRef *corev1.LocalObjectReference) error {
	before := runnerObject.Status.DeepCopy()
	runnerObject.Status.ObservedGeneration = runnerObject.Generation
	runnerObject.Status.WorkflowJobRef = workflowJobRef
	meta.SetStatusCondition(&runnerObject.Status.Conditions, metav1.Condition{
		Type:               actionsv1alpha1.RunnerConditionReady,
		Status:             ready,
		ObservedGeneration: runnerObject.Generation,
		Reason:             readyReason,
		Message:            readyMessage,
	})
	busy := metav1.ConditionFalse
	busyReason := "Idle"
	busyMessage := "Runner is not assigned a WorkflowJob"
	if workflowJobRef != nil {
		busy = metav1.ConditionTrue
		busyReason = "JobAssigned"
		busyMessage = "Runner is assigned a WorkflowJob"
	}
	meta.SetStatusCondition(&runnerObject.Status.Conditions, metav1.Condition{
		Type:               actionsv1alpha1.RunnerConditionBusy,
		Status:             busy,
		ObservedGeneration: runnerObject.Generation,
		Reason:             busyReason,
		Message:            busyMessage,
	})
	if apiEquality.Semantic.DeepEqual(before, &runnerObject.Status) {
		return nil
	}
	return r.Status().Update(ctx, runnerObject)
}

func (r *RunnerReconciler) SetupWithManager(manager ctrl.Manager) error {
	if err := manager.GetFieldIndexer().IndexField(context.Background(), &actionsv1alpha1.WorkflowJob{}, workflowJobQueuedIndex, indexQueuedWorkflowJob); err != nil {
		return err
	}
	if err := manager.GetFieldIndexer().IndexField(context.Background(), &actionsv1alpha1.WorkflowJob{}, workflowJobRunnerNameIndex, indexWorkflowJobRunnerName); err != nil {
		return err
	}
	if err := manager.GetFieldIndexer().IndexField(context.Background(), &actionsv1alpha1.WorkflowJob{}, workflowJobProjectNameIndex, indexWorkflowJobProjectName); err != nil {
		return err
	}
	return ctrl.NewControllerManagedBy(manager).
		For(&actionsv1alpha1.Runner{}).
		Watches(&actionsv1alpha1.WorkflowJob{}, handler.EnqueueRequestsFromMapFunc(r.runnersForWorkflowJob)).
		Watches(&actionsv1alpha1.WorkflowRun{}, handler.EnqueueRequestsFromMapFunc(r.runnersForWorkflowRun)).
		Watches(&actionsv1alpha1.Project{}, handler.EnqueueRequestsFromMapFunc(r.runnersForProject)).
		Watches(&batchv1.Job{}, handler.EnqueueRequestsFromMapFunc(r.runnerForNativeJob)).
		Complete(r)
}

func (r *RunnerReconciler) runnersForWorkflowRun(ctx context.Context, object client.Object) []reconcile.Request {
	run, ok := object.(*actionsv1alpha1.WorkflowRun)
	if !ok {
		return nil
	}
	if !run.DeletionTimestamp.IsZero() {
		return nil
	}
	planned := meta.FindStatusCondition(run.Status.Conditions, actionsv1alpha1.WorkflowRunConditionPlanned)
	if planned == nil || planned.Status != metav1.ConditionTrue {
		return nil
	}
	runners := &actionsv1alpha1.RunnerList{}
	if err := r.List(ctx, runners, client.InNamespace(run.Namespace)); err != nil {
		return nil
	}
	requests := make([]reconcile.Request, 0, len(runners.Items))
	for index := range runners.Items {
		runnerObject := &runners.Items[index]
		if runnerObject.Status.WorkflowJobRef == nil && runnerObject.Spec.ProjectRef.Name == run.Spec.ProjectRef.Name {
			requests = append(requests, requestFor(runnerObject))
		}
	}
	return requests
}

func indexQueuedWorkflowJob(object client.Object) []string {
	workflowJob := object.(*actionsv1alpha1.WorkflowJob)
	if workflowJob.DeletionTimestamp.IsZero() && workflowJob.Status.RunnerRef == nil && !terminalWorkflowJob(workflowJob) {
		return []string{"true"}
	}
	return nil
}

func indexWorkflowJobRunnerName(object client.Object) []string {
	workflowJob := object.(*actionsv1alpha1.WorkflowJob)
	if workflowJob.Status.RunnerRef == nil {
		return nil
	}
	return []string{workflowJob.Status.RunnerRef.Name}
}

func indexWorkflowJobProjectName(object client.Object) []string {
	name := object.GetAnnotations()[actionsv1alpha1.AnnotationProjectName]
	if name == "" {
		return nil
	}
	return []string{name}
}

func (r *RunnerReconciler) runnersForProject(ctx context.Context, object client.Object) []reconcile.Request {
	runners := &actionsv1alpha1.RunnerList{}
	if err := r.List(ctx, runners, client.InNamespace(object.GetNamespace())); err != nil {
		return nil
	}
	requests := []reconcile.Request{}
	for index := range runners.Items {
		runnerObject := &runners.Items[index]
		if runnerObject.Spec.ProjectRef.Name == object.GetName() {
			requests = append(requests, requestFor(runnerObject))
		}
	}
	return requests
}

func (r *RunnerReconciler) runnersForWorkflowJob(ctx context.Context, object client.Object) []reconcile.Request {
	workflowJob, ok := object.(*actionsv1alpha1.WorkflowJob)
	if !ok {
		return nil
	}
	if workflowJob.Status.RunnerRef != nil {
		return []reconcile.Request{{NamespacedName: client.ObjectKey{Namespace: workflowJob.Namespace, Name: workflowJob.Status.RunnerRef.Name}}}
	}
	run := &actionsv1alpha1.WorkflowRun{}
	if err := r.Get(ctx, client.ObjectKey{Namespace: workflowJob.Namespace, Name: workflowJob.Spec.WorkflowRunRef.Name}, run); err != nil {
		return nil
	}
	if !run.DeletionTimestamp.IsZero() {
		return nil
	}
	runners := &actionsv1alpha1.RunnerList{}
	if err := r.List(ctx, runners, client.InNamespace(workflowJob.Namespace)); err != nil {
		return nil
	}
	candidates := make([]*actionsv1alpha1.Runner, 0, len(runners.Items))
	for index := range runners.Items {
		candidate := &runners.Items[index]
		if candidate.Status.WorkflowJobRef == nil &&
			candidate.Spec.ProjectRef.Name == run.Spec.ProjectRef.Name &&
			runnerLabelsMatch(candidate.Spec.Labels, workflowJob.Spec.RunsOn) {
			candidates = append(candidates, candidate)
		}
	}
	if len(candidates) == 0 {
		return nil
	}
	sort.Slice(candidates, func(left, right int) bool {
		return candidates[left].Name < candidates[right].Name
	})
	digest := sha256.Sum256([]byte(workflowJob.Namespace + "\x00" + workflowJob.Name))
	selected := binary.BigEndian.Uint64(digest[:8]) % uint64(len(candidates))
	return []reconcile.Request{requestFor(candidates[selected])}
}

func (r *RunnerReconciler) runnerForNativeJob(_ context.Context, object client.Object) []reconcile.Request {
	name := object.GetAnnotations()[actionsv1alpha1.AnnotationRunnerName]
	if name == "" {
		return nil
	}
	return []reconcile.Request{{NamespacedName: client.ObjectKey{Namespace: object.GetNamespace(), Name: name}}}
}

func runnerLabelsMatch(available, requested []string) bool {
	labels := make(map[string]struct{}, len(available))
	for _, label := range available {
		labels[label] = struct{}{}
	}
	for _, label := range requested {
		if _, found := labels[label]; !found {
			return false
		}
	}
	return true
}

func terminalWorkflowJob(workflowJob *actionsv1alpha1.WorkflowJob) bool {
	condition := meta.FindStatusCondition(workflowJob.Status.Conditions, actionsv1alpha1.WorkflowJobConditionSucceeded)
	return condition != nil && (condition.Status == metav1.ConditionTrue || condition.Status == metav1.ConditionFalse)
}

func nativeJobLabels(workflowJob *actionsv1alpha1.WorkflowJob, run *actionsv1alpha1.WorkflowRun, project *actionsv1alpha1.Project, runnerObject *actionsv1alpha1.Runner) map[string]string {
	return map[string]string{
		actionsv1alpha1.LabelProjectUID:     string(project.UID),
		actionsv1alpha1.LabelRunnerUID:      string(runnerObject.UID),
		actionsv1alpha1.LabelWorkflowRunUID: string(run.UID),
		actionsv1alpha1.LabelWorkflowJobUID: string(workflowJob.UID),
		actionsv1alpha1.LabelWorkflowJob:    workflowJob.Labels[actionsv1alpha1.LabelWorkflowJob],
	}
}

func jobResult(job *batchv1.Job) metav1.ConditionStatus {
	for _, condition := range job.Status.Conditions {
		if condition.Status != corev1.ConditionTrue {
			continue
		}
		if condition.Type == batchv1.JobComplete {
			return metav1.ConditionTrue
		}
		if condition.Type == batchv1.JobFailed {
			return metav1.ConditionFalse
		}
	}
	return metav1.ConditionUnknown
}

func jobTerminal(job *batchv1.Job) bool {
	return jobResult(job) != metav1.ConditionUnknown
}

func completionTime(job *batchv1.Job) *metav1.Time {
	if job.Status.CompletionTime != nil {
		return job.Status.CompletionTime.DeepCopy()
	}
	now := metav1.Now()
	return &now
}

func pointerTo[T any](value T) *T {
	return &value
}
