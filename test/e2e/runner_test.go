//go:build e2e

package e2e_test

import (
	"context"
	"net/http"
	"time"

	actionsv1alpha1 "github.com/kelos-dev/open-actions/api/v1alpha1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var _ = Describe("Runner", func() {
	BeforeEach(func() {
		setupTestProject(true)
	})

	It("executes a typed WorkflowRun and deletes its owned resources after its TTL expires", func() {
		ctx := context.Background()
		resetPreparationActionDownloads()
		DeferCleanup(resetPreparationActionDownloads)
		run := &actionsv1alpha1.WorkflowRun{
			ObjectMeta: metav1.ObjectMeta{Name: "runner-execution", Namespace: e2eNamespace},
			Spec: actionsv1alpha1.WorkflowRunSpec{
				ProjectRef: corev1.LocalObjectReference{Name: "default"},
				Source: actionsv1alpha1.WorkflowRunSource{
					Type: actionsv1alpha1.SourceTypeGitHub,
					GitHub: &actionsv1alpha1.GitHubWorkflowRunSource{
						Repository: actionsv1alpha1.GitHubRepository{ID: 123456789, Owner: "acme", Name: "example"},
						Event: actionsv1alpha1.GitHubEvent{
							Name:       "push",
							DeliveryID: "41111111-2222-3333-4444-555555555555",
						},
						Revision: actionsv1alpha1.GitRevision{
							SHA: fixtureRevision,
							Ref: "refs/heads/main",
						},
					},
				},
				WorkflowPath: preparationWorkflowPath,
			},
		}
		Expect(clusterClient.Create(ctx, run)).To(Succeed())

		var workflowJobName string
		var reportWorkflowJobName string
		Eventually(func(g Gomega) {
			jobs := &actionsv1alpha1.WorkflowJobList{}
			g.Expect(clusterClient.List(ctx, jobs, client.InNamespace(e2eNamespace))).To(Succeed())
			g.Expect(jobs.Items).To(HaveLen(2))
			if len(jobs.Items) != 2 {
				return
			}
			for index := range jobs.Items {
				workflowJob := &jobs.Items[index]
				switch workflowJob.Spec.JobID {
				case "test":
					workflowJobName = workflowJob.Name
					g.Expect(workflowJob.Spec.RunsOn).To(ConsistOf("ubuntu-latest", "docker"))
					g.Expect(workflowJob.Status.RunnerRef).NotTo(BeNil())
					if workflowJob.Status.RunnerRef != nil {
						g.Expect(workflowJob.Status.RunnerRef.Name).To(Equal("runner-1"))
					}
					g.Expect(meta.IsStatusConditionTrue(workflowJob.Status.Conditions, actionsv1alpha1.WorkflowJobConditionScheduled)).To(BeTrue())
				case "report":
					reportWorkflowJobName = workflowJob.Name
					g.Expect(workflowJob.Spec.Needs).To(Equal([]string{"test"}))
					g.Expect(workflowJob.Status.RunnerRef).To(BeNil())
					ready := meta.FindStatusCondition(workflowJob.Status.Conditions, actionsv1alpha1.WorkflowJobConditionReady)
					g.Expect(ready).NotTo(BeNil())
					if ready != nil {
						g.Expect(ready.Status).To(Equal(metav1.ConditionUnknown))
						g.Expect(ready.Reason).To(Equal("DependenciesPending"))
					}
				default:
					g.Expect(workflowJob.Spec.JobID).To(BeElementOf("test", "report"))
				}
			}
			g.Expect(workflowJobName).NotTo(BeEmpty())
			g.Expect(reportWorkflowJobName).NotTo(BeEmpty())
		}, 60*time.Second, time.Second).Should(Succeed())

		var jobName string
		Eventually(func(g Gomega) {
			jobs := &batchv1.JobList{}
			g.Expect(clusterClient.List(ctx, jobs, client.InNamespace(e2eNamespace))).To(Succeed())
			g.Expect(jobs.Items).To(HaveLen(1))
			if len(jobs.Items) != 1 {
				return
			}
			job := jobs.Items[0]
			jobName = job.Name
			g.Expect(job.Annotations[actionsv1alpha1.AnnotationRunnerName]).To(Equal("runner-1"))
			g.Expect(job.OwnerReferences).To(HaveLen(1))
			g.Expect(job.Spec.Template.Spec.RestartPolicy).To(Equal(corev1.RestartPolicyNever))
			g.Expect(job.Spec.Template.Spec.AutomountServiceAccountToken).NotTo(BeNil())
			if job.Spec.Template.Spec.AutomountServiceAccountToken != nil {
				g.Expect(*job.Spec.Template.Spec.AutomountServiceAccountToken).To(BeFalse())
			}
			g.Expect(job.Spec.Template.Spec.SecurityContext).NotTo(BeNil())
			if job.Spec.Template.Spec.SecurityContext != nil {
				g.Expect(job.Spec.Template.Spec.SecurityContext.RunAsNonRoot).NotTo(BeNil())
				if job.Spec.Template.Spec.SecurityContext.RunAsNonRoot != nil {
					g.Expect(*job.Spec.Template.Spec.SecurityContext.RunAsNonRoot).To(BeTrue())
				}
				g.Expect(job.Spec.Template.Spec.SecurityContext.SeccompProfile).NotTo(BeNil())
				if job.Spec.Template.Spec.SecurityContext.SeccompProfile != nil {
					g.Expect(job.Spec.Template.Spec.SecurityContext.SeccompProfile.Type).To(Equal(corev1.SeccompProfileTypeRuntimeDefault))
				}
			}
			container := job.Spec.Template.Spec.Containers[0]
			g.Expect(container.SecurityContext).NotTo(BeNil())
			if container.SecurityContext != nil {
				g.Expect(container.SecurityContext.AllowPrivilegeEscalation).NotTo(BeNil())
				if container.SecurityContext.AllowPrivilegeEscalation != nil {
					g.Expect(*container.SecurityContext.AllowPrivilegeEscalation).To(BeFalse())
				}
				g.Expect(container.SecurityContext.Capabilities).NotTo(BeNil())
				if container.SecurityContext.Capabilities != nil {
					g.Expect(container.SecurityContext.Capabilities.Drop).To(ConsistOf(corev1.Capability("ALL")))
				}
			}
		}, 60*time.Second, time.Second).Should(Succeed())

		Eventually(func(g Gomega) {
			stored := &actionsv1alpha1.WorkflowRun{}
			g.Expect(clusterClient.Get(ctx, client.ObjectKeyFromObject(run), stored)).To(Succeed())
			condition := meta.FindStatusCondition(stored.Status.Conditions, actionsv1alpha1.WorkflowRunConditionSucceeded)
			g.Expect(condition).NotTo(BeNil())
			if condition != nil {
				g.Expect(condition.Status).To(Equal(metav1.ConditionTrue), condition.Message)
			}
		}, 180*time.Second, time.Second).Should(Succeed())

		job := &batchv1.Job{}
		Expect(clusterClient.Get(ctx, client.ObjectKey{Namespace: e2eNamespace, Name: jobName}, job)).To(Succeed())
		condition := findJobCondition(job.Status.Conditions, batchv1.JobComplete)
		Expect(condition).NotTo(BeNil())
		if condition != nil {
			Expect(condition.Status).To(Equal(corev1.ConditionTrue), condition.Message)
		}

		workflowJob := &actionsv1alpha1.WorkflowJob{}
		Expect(clusterClient.Get(ctx, client.ObjectKey{Namespace: e2eNamespace, Name: workflowJobName}, workflowJob)).To(Succeed())
		Expect(meta.IsStatusConditionTrue(workflowJob.Status.Conditions, actionsv1alpha1.WorkflowJobConditionSucceeded)).To(BeTrue())
		reportWorkflowJob := &actionsv1alpha1.WorkflowJob{}
		Expect(clusterClient.Get(ctx, client.ObjectKey{Namespace: e2eNamespace, Name: reportWorkflowJobName}, reportWorkflowJob)).To(Succeed())
		Expect(reportWorkflowJob.Status.Result).To(Equal(actionsv1alpha1.WorkflowJobResultSuccess))
		Expect(meta.IsStatusConditionTrue(reportWorkflowJob.Status.Conditions, actionsv1alpha1.WorkflowJobConditionScheduled)).To(BeTrue())
		Expect(meta.IsStatusConditionTrue(reportWorkflowJob.Status.Conditions, actionsv1alpha1.WorkflowJobConditionSucceeded)).To(BeTrue())
		Eventually(func(g Gomega) {
			runnerObject := &actionsv1alpha1.Runner{}
			g.Expect(clusterClient.Get(ctx, client.ObjectKey{Namespace: e2eNamespace, Name: "runner-1"}, runnerObject)).To(Succeed())
			g.Expect(runnerObject.Status.WorkflowJobRef).To(BeNil())
			g.Expect(meta.IsStatusConditionTrue(runnerObject.Status.Conditions, actionsv1alpha1.RunnerConditionReady)).To(BeTrue())
			g.Expect(meta.IsStatusConditionFalse(runnerObject.Status.Conditions, actionsv1alpha1.RunnerConditionBusy)).To(BeTrue())
			secrets := &corev1.SecretList{}
			g.Expect(clusterClient.List(ctx, secrets, client.InNamespace(e2eNamespace))).To(Succeed())
			g.Expect(secrets.Items).To(HaveLen(1))
			if len(secrets.Items) == 1 {
				g.Expect(secrets.Items[0].Name).To(Equal("github-app"))
			}
		}, 30*time.Second, time.Second).Should(Succeed())

		pods, err := clientset.CoreV1().Pods(e2eNamespace).List(ctx, metav1.ListOptions{
			LabelSelector: labels.Set{batchv1.JobNameLabel: jobName}.String(),
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(pods.Items).To(HaveLen(1))
		podName := pods.Items[0].Name
		logs, err := clientset.CoreV1().Pods(e2eNamespace).GetLogs(pods.Items[0].Name, &corev1.PodLogOptions{}).DoRaw(ctx)
		Expect(err).NotTo(HaveOccurred())
		output := string(logs)
		Expect(output).To(ContainSubstring("action downloads blocked after preparation"))
		Expect(output).To(ContainSubstring("external preparation composite run"))
		Expect(output).To(ContainSubstring("prepared action graph e2e works"))
		Expect(output).To(ContainSubstring("external checkout main ran"))
		Expect(output).To(ContainSubstring("external setup-go main ran"))
		Expect(output).To(ContainSubstring("external kind-action main ran"))
		Expect(output).To(ContainSubstring("external kind-action Docker ready"))
		Expect(output).To(ContainSubstring("external composite run"))
		Expect(output).To(ContainSubstring("Docker execution works"))
		Expect(output).To(ContainSubstring("runner workspace git works"))
		Expect(output).To(ContainSubstring("open actions e2e works"))
		Expect(output).To(ContainSubstring("external marker post ran"))
		Expect(output).To(ContainSubstring("external kind-action post ran"))
		Expect(output).To(ContainSubstring("external setup-go post ran"))
		Expect(output).To(ContainSubstring("external checkout post ran"))

		By("setting the completed WorkflowRun TTL to zero")
		Eventually(func() error {
			stored := &actionsv1alpha1.WorkflowRun{}
			if err := clusterClient.Get(ctx, client.ObjectKeyFromObject(run), stored); err != nil {
				return err
			}
			ttl := int32(0)
			stored.Spec.TTLSecondsAfterFinished = &ttl
			return clusterClient.Update(ctx, stored)
		}, 30*time.Second, time.Second).Should(Succeed())

		Eventually(func() bool {
			err := clusterClient.Get(ctx, client.ObjectKeyFromObject(run), &actionsv1alpha1.WorkflowRun{})
			return apierrors.IsNotFound(err)
		}, 60*time.Second, time.Second).Should(BeTrue())
		Eventually(func(g Gomega) {
			err := clusterClient.Get(ctx, client.ObjectKey{Namespace: e2eNamespace, Name: workflowJobName}, &actionsv1alpha1.WorkflowJob{})
			g.Expect(apierrors.IsNotFound(err)).To(BeTrue())
			err = clusterClient.Get(ctx, client.ObjectKey{Namespace: e2eNamespace, Name: jobName}, &batchv1.Job{})
			g.Expect(apierrors.IsNotFound(err)).To(BeTrue())
			err = clusterClient.Get(ctx, client.ObjectKey{Namespace: e2eNamespace, Name: podName}, &corev1.Pod{})
			g.Expect(apierrors.IsNotFound(err)).To(BeTrue())
		}, 60*time.Second, time.Second).Should(Succeed())
	})

	It("expands a matrix from a dependency output", func() {
		ctx := context.Background()
		run := &actionsv1alpha1.WorkflowRun{
			ObjectMeta: metav1.ObjectMeta{Name: "dynamic-matrix", Namespace: e2eNamespace},
			Spec: actionsv1alpha1.WorkflowRunSpec{
				ProjectRef: corev1.LocalObjectReference{Name: "default"},
				Source: actionsv1alpha1.WorkflowRunSource{
					Type: actionsv1alpha1.SourceTypeGitHub,
					GitHub: &actionsv1alpha1.GitHubWorkflowRunSource{
						Repository: actionsv1alpha1.GitHubRepository{ID: 123456789, Owner: "acme", Name: "example"},
						Event: actionsv1alpha1.GitHubEvent{
							Name:       "push",
							DeliveryID: "51111111-2222-3333-4444-555555555555",
						},
						Revision: actionsv1alpha1.GitRevision{SHA: fixtureRevision, Ref: "refs/heads/main"},
					},
				},
				WorkflowPath: dynamicMatrixWorkflowPath,
			},
		}
		Expect(clusterClient.Create(ctx, run)).To(Succeed())

		Eventually(func(g Gomega) {
			stored := &actionsv1alpha1.WorkflowRun{}
			g.Expect(clusterClient.Get(ctx, client.ObjectKeyFromObject(run), stored)).To(Succeed())
			condition := meta.FindStatusCondition(stored.Status.Conditions, actionsv1alpha1.WorkflowRunConditionSucceeded)
			g.Expect(condition).NotTo(BeNil())
			if condition != nil {
				g.Expect(condition.Status).To(Equal(metav1.ConditionTrue), condition.Message)
			}
			g.Expect(stored.Status.Jobs).NotTo(BeNil())
			if stored.Status.Jobs != nil {
				g.Expect(stored.Status.Jobs.Total).To(Equal(int32(3)))
				g.Expect(stored.Status.Jobs.Succeeded).To(Equal(int32(3)))
			}
		}, 180*time.Second, time.Second).Should(Succeed())

		jobs := &actionsv1alpha1.WorkflowJobList{}
		Expect(clusterClient.List(ctx, jobs, client.InNamespace(e2eNamespace), client.MatchingLabels{actionsv1alpha1.LabelWorkflowRunUID: string(run.UID)})).To(Succeed())
		Expect(jobs.Items).To(HaveLen(3))
		matrixValues := map[string]bool{}
		for index := range jobs.Items {
			job := &jobs.Items[index]
			if job.Spec.Matrix == nil {
				continue
			}
			Expect(job.Spec.Matrix.LogicalJobID).To(Equal("execute"))
			Expect(job.Spec.Matrix.MaxParallel).To(Equal(int32(1)))
			Expect(job.Status.Result).To(Equal(actionsv1alpha1.WorkflowJobResultSuccess))
			matrixValues[job.Spec.Matrix.Values["target"]] = true
		}
		Expect(matrixValues).To(Equal(map[string]bool{"first": true, "second": true}))
	})

	It("uploads artifacts from parallel matrix jobs and aggregates them in a dependent job", func() {
		ctx := context.Background()
		runnerOne := &actionsv1alpha1.Runner{}
		Expect(clusterClient.Get(ctx, client.ObjectKey{Namespace: e2eNamespace, Name: "runner-1"}, runnerOne)).To(Succeed())
		runnerTwo := &actionsv1alpha1.Runner{
			ObjectMeta: metav1.ObjectMeta{Name: "runner-2", Namespace: e2eNamespace},
			Spec:       *runnerOne.Spec.DeepCopy(),
		}
		Expect(clusterClient.Create(ctx, runnerTwo)).To(Succeed())
		Eventually(func(g Gomega) {
			stored := &actionsv1alpha1.Runner{}
			g.Expect(clusterClient.Get(ctx, client.ObjectKeyFromObject(runnerTwo), stored)).To(Succeed())
			g.Expect(meta.IsStatusConditionTrue(stored.Status.Conditions, actionsv1alpha1.RunnerConditionReady)).To(BeTrue())
		}, 60*time.Second, time.Second).Should(Succeed())

		run := &actionsv1alpha1.WorkflowRun{
			ObjectMeta: metav1.ObjectMeta{Name: "runner-artifacts", Namespace: e2eNamespace},
			Spec: actionsv1alpha1.WorkflowRunSpec{
				ProjectRef: corev1.LocalObjectReference{Name: "default"},
				Source: actionsv1alpha1.WorkflowRunSource{
					Type: actionsv1alpha1.SourceTypeGitHub,
					GitHub: &actionsv1alpha1.GitHubWorkflowRunSource{
						Repository: actionsv1alpha1.GitHubRepository{ID: 123456789, Owner: "acme", Name: "example"},
						Event:      actionsv1alpha1.GitHubEvent{Name: "push", DeliveryID: "71111111-2222-3333-4444-555555555555"},
						Revision:   actionsv1alpha1.GitRevision{SHA: fixtureRevision, Ref: "refs/heads/main"},
					},
				},
				WorkflowPath: artifactWorkflowPath,
			},
		}
		Expect(clusterClient.Create(ctx, run)).To(Succeed())
		Eventually(func(g Gomega) {
			stored := &actionsv1alpha1.WorkflowRun{}
			g.Expect(clusterClient.Get(ctx, client.ObjectKeyFromObject(run), stored)).To(Succeed())
			condition := meta.FindStatusCondition(stored.Status.Conditions, actionsv1alpha1.WorkflowRunConditionSucceeded)
			g.Expect(condition).NotTo(BeNil())
			if condition != nil {
				g.Expect(condition.Status).To(Equal(metav1.ConditionTrue), condition.Message)
			}
		}, 180*time.Second, time.Second).Should(Succeed())

		workflowJobs := &actionsv1alpha1.WorkflowJobList{}
		Expect(clusterClient.List(ctx, workflowJobs, client.InNamespace(e2eNamespace), client.MatchingLabels{
			actionsv1alpha1.LabelWorkflowRunUID: string(run.UID),
		})).To(Succeed())
		Expect(workflowJobs.Items).To(HaveLen(3))
		matrixJobs := make([]actionsv1alpha1.WorkflowJob, 0, 2)
		var aggregate *actionsv1alpha1.WorkflowJob
		for index := range workflowJobs.Items {
			job := &workflowJobs.Items[index]
			if job.Spec.Matrix != nil {
				matrixJobs = append(matrixJobs, *job)
			} else if job.Spec.JobID == "aggregate" {
				aggregate = job
			}
		}
		Expect(matrixJobs).To(HaveLen(2))
		Expect(aggregate).NotTo(BeNil())
		if len(matrixJobs) == 2 {
			Expect(matrixJobs[0].Status.StartTime).NotTo(BeNil())
			Expect(matrixJobs[1].Status.StartTime).NotTo(BeNil())
			Expect(matrixJobs[0].Status.CompletionTime).NotTo(BeNil())
			Expect(matrixJobs[1].Status.CompletionTime).NotTo(BeNil())
			if matrixJobs[0].Status.StartTime != nil && matrixJobs[1].Status.StartTime != nil &&
				matrixJobs[0].Status.CompletionTime != nil && matrixJobs[1].Status.CompletionTime != nil {
				latestStart := matrixJobs[0].Status.StartTime.Time
				if matrixJobs[1].Status.StartTime.Time.After(latestStart) {
					latestStart = matrixJobs[1].Status.StartTime.Time
				}
				earliestCompletion := matrixJobs[0].Status.CompletionTime.Time
				if matrixJobs[1].Status.CompletionTime.Time.Before(earliestCompletion) {
					earliestCompletion = matrixJobs[1].Status.CompletionTime.Time
				}
				Expect(latestStart.Before(earliestCompletion)).To(BeTrue(), "matrix artifact jobs did not overlap")
			}
		}
		if aggregate != nil {
			pods, err := clientset.CoreV1().Pods(e2eNamespace).List(ctx, metav1.ListOptions{
				LabelSelector: labels.Set{batchv1.JobNameLabel: aggregate.Name}.String(),
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(pods.Items).To(HaveLen(1))
			if len(pods.Items) == 1 {
				logs, err := clientset.CoreV1().Pods(e2eNamespace).GetLogs(pods.Items[0].Name, &corev1.PodLogOptions{}).DoRaw(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(string(logs)).To(ContainSubstring("artifact aggregation e2e works"))
			}
		}
	})

	It("executes a pull request checkout with persisted credentials", func() {
		ctx := context.Background()
		run := &actionsv1alpha1.WorkflowRun{
			ObjectMeta: metav1.ObjectMeta{Name: "runner-pull-request", Namespace: e2eNamespace},
			Spec: actionsv1alpha1.WorkflowRunSpec{
				ProjectRef: corev1.LocalObjectReference{Name: "default"},
				Source: actionsv1alpha1.WorkflowRunSource{
					Type: actionsv1alpha1.SourceTypeGitHub,
					GitHub: &actionsv1alpha1.GitHubWorkflowRunSource{
						Repository: actionsv1alpha1.GitHubRepository{ID: 123456789, Owner: "acme", Name: "example"},
						Event: actionsv1alpha1.GitHubEvent{
							Name:       actionsv1alpha1.GitHubEventNamePullRequest,
							Action:     "synchronize",
							DeliveryID: "61111111-2222-3333-4444-555555555555",
							PullRequest: &actionsv1alpha1.GitHubPullRequest{
								Number:         42,
								HTMLURL:        "https://github.example/acme/example/pull/42",
								HeadRepository: actionsv1alpha1.GitHubRepository{ID: 123456789, Owner: "acme", Name: "example"},
								HeadRef:        "feature",
								HeadSHA:        fixtureRepositoryRevision.HeadSHA,
								BaseRef:        "main",
							},
						},
						Revision: actionsv1alpha1.GitRevision{
							SHA:          fixtureRepositoryRevision.IntegrationSHA,
							HeadSHA:      fixtureRepositoryRevision.HeadSHA,
							BaseSHA:      fixtureRepositoryRevision.BaseSHA,
							MergeBaseSHA: fixtureRepositoryRevision.MergeBaseSHA,
							Ref:          "refs/pull/42/merge",
							HeadRef:      "feature",
							BaseRef:      "main",
						},
					},
				},
				WorkflowPath: pullRequestWorkflowPath,
			},
		}
		Expect(clusterClient.Create(ctx, run)).To(Succeed())

		Eventually(func(g Gomega) {
			stored := &actionsv1alpha1.WorkflowRun{}
			g.Expect(clusterClient.Get(ctx, client.ObjectKeyFromObject(run), stored)).To(Succeed())
			condition := meta.FindStatusCondition(stored.Status.Conditions, actionsv1alpha1.WorkflowRunConditionSucceeded)
			g.Expect(condition).NotTo(BeNil())
			if condition != nil {
				g.Expect(condition.Status).To(Equal(metav1.ConditionTrue), condition.Message)
			}
		}, 180*time.Second, time.Second).Should(Succeed())

		jobs := &batchv1.JobList{}
		Expect(clusterClient.List(ctx, jobs, client.InNamespace(e2eNamespace), client.MatchingLabels{
			actionsv1alpha1.LabelWorkflowRunUID: string(run.UID),
		})).To(Succeed())
		Expect(jobs.Items).To(HaveLen(1))
		pods, err := clientset.CoreV1().Pods(e2eNamespace).List(ctx, metav1.ListOptions{
			LabelSelector: labels.Set{batchv1.JobNameLabel: jobs.Items[0].Name}.String(),
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(pods.Items).To(HaveLen(1))
		logs, err := clientset.CoreV1().Pods(e2eNamespace).GetLogs(pods.Items[0].Name, &corev1.PodLogOptions{}).DoRaw(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(logs)).To(ContainSubstring("pull request checkout integration works"))
		Expect(string(logs)).To(ContainSubstring("external checkout post ran"))
	})
})

func resetPreparationActionDownloads() {
	request, err := http.NewRequest(http.MethodDelete, fixtureURL+"/fixture/block-preparation-actions", nil)
	Expect(err).NotTo(HaveOccurred())
	response, err := http.DefaultClient.Do(request)
	Expect(err).NotTo(HaveOccurred())
	Expect(response.Body.Close()).To(Succeed())
	Expect(response.StatusCode).To(Equal(http.StatusNoContent))
}

func findJobCondition(conditions []batchv1.JobCondition, conditionType batchv1.JobConditionType) *batchv1.JobCondition {
	for index := range conditions {
		if conditions[index].Type == conditionType {
			return &conditions[index]
		}
	}
	return nil
}
