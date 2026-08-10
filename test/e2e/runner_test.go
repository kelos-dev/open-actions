//go:build e2e

package e2e_test

import (
	"context"
	"time"

	actionsv1alpha1 "github.com/kelos-dev/open-actions/api/v1alpha1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var _ = Describe("Runner", func() {
	BeforeEach(func() {
		setupTestProject(true)
	})

	It("claims a typed WorkflowRun job and executes it in an owned native Job", func() {
		ctx := context.Background()
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
				WorkflowPath: workflowPath,
			},
		}
		Expect(clusterClient.Create(ctx, run)).To(Succeed())

		var workflowJobName string
		Eventually(func(g Gomega) {
			jobs := &actionsv1alpha1.WorkflowJobList{}
			g.Expect(clusterClient.List(ctx, jobs, client.InNamespace(e2eNamespace))).To(Succeed())
			g.Expect(jobs.Items).To(HaveLen(1))
			if len(jobs.Items) != 1 {
				return
			}
			workflowJob := jobs.Items[0]
			workflowJobName = workflowJob.Name
			g.Expect(workflowJob.Spec.RunsOn).To(ConsistOf("ubuntu-latest"))
			g.Expect(workflowJob.Status.RunnerRef).NotTo(BeNil())
			if workflowJob.Status.RunnerRef != nil {
				g.Expect(workflowJob.Status.RunnerRef.Name).To(Equal("runner-1"))
			}
			g.Expect(meta.IsStatusConditionTrue(workflowJob.Status.Conditions, actionsv1alpha1.WorkflowJobConditionScheduled)).To(BeTrue())
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
		logs, err := clientset.CoreV1().Pods(e2eNamespace).GetLogs(pods.Items[0].Name, &corev1.PodLogOptions{}).DoRaw(ctx)
		Expect(err).NotTo(HaveOccurred())
		output := string(logs)
		Expect(output).To(ContainSubstring("external checkout main ran"))
		Expect(output).To(ContainSubstring("external setup-go main ran"))
		Expect(output).To(ContainSubstring("external composite run"))
		Expect(output).To(ContainSubstring("runner workspace git works"))
		Expect(output).To(ContainSubstring("open actions e2e works"))
		Expect(output).To(ContainSubstring("external marker post ran"))
		Expect(output).To(ContainSubstring("external setup-go post ran"))
		Expect(output).To(ContainSubstring("external checkout post ran"))
	})
})

func findJobCondition(conditions []batchv1.JobCondition, conditionType batchv1.JobConditionType) *batchv1.JobCondition {
	for index := range conditions {
		if conditions[index].Type == conditionType {
			return &conditions[index]
		}
	}
	return nil
}
