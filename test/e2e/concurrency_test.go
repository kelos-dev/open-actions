//go:build e2e

package e2e_test

import (
	"context"
	"time"

	actionsv1alpha1 "github.com/kelos-dev/open-actions/api/v1alpha1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var _ = Describe("Concurrency", func() {
	BeforeEach(func() {
		setupTestProject(true)
	})

	It("serializes jobs that share a concurrency group", func() {
		ctx := context.Background()
		run := concurrencyWorkflowRun("job-concurrency", jobConcurrencyWorkflowPath, "71111111-2222-3333-4444-555555555555")
		Expect(clusterClient.Create(ctx, run)).To(Succeed())

		Eventually(func(g Gomega) {
			jobs := &actionsv1alpha1.WorkflowJobList{}
			g.Expect(clusterClient.List(ctx, jobs, client.InNamespace(e2eNamespace), client.MatchingLabels{
				actionsv1alpha1.LabelWorkflowRunUID: string(run.UID),
			})).To(Succeed())
			g.Expect(jobs.Items).To(HaveLen(2))
			if len(jobs.Items) != 2 {
				return
			}
			acquired := 0
			waiting := 0
			for index := range jobs.Items {
				job := &jobs.Items[index]
				g.Expect(job.Status.Concurrency).NotTo(BeNil())
				if job.Status.Concurrency == nil {
					continue
				}
				g.Expect(job.Status.Concurrency.Group).To(Equal("deploy-acme/example"))
				g.Expect(job.Status.Concurrency.CancelInProgress).To(BeFalse())
				condition := meta.FindStatusCondition(job.Status.Conditions, actionsv1alpha1.WorkflowJobConditionConcurrencyAcquired)
				g.Expect(condition).NotTo(BeNil())
				if condition == nil {
					continue
				}
				switch condition.Status {
				case metav1.ConditionTrue:
					acquired++
				case metav1.ConditionUnknown:
					g.Expect(condition.Reason).To(Equal("WaitingForConcurrency"))
					waiting++
				}
			}
			g.Expect(acquired).To(Equal(1))
			g.Expect(waiting).To(Equal(1))
		}, 90*time.Second, time.Second).Should(Succeed())

		Eventually(func(g Gomega) {
			stored := &actionsv1alpha1.WorkflowRun{}
			g.Expect(clusterClient.Get(ctx, client.ObjectKeyFromObject(run), stored)).To(Succeed())
			condition := meta.FindStatusCondition(stored.Status.Conditions, actionsv1alpha1.WorkflowRunConditionSucceeded)
			g.Expect(condition).NotTo(BeNil())
			if condition != nil {
				g.Expect(condition.Status).To(Equal(metav1.ConditionTrue), condition.Message)
			}
		}, 180*time.Second, time.Second).Should(Succeed())

		jobs := &actionsv1alpha1.WorkflowJobList{}
		Expect(clusterClient.List(ctx, jobs, client.InNamespace(e2eNamespace), client.MatchingLabels{
			actionsv1alpha1.LabelWorkflowRunUID: string(run.UID),
		})).To(Succeed())
		Expect(jobs.Items).To(HaveLen(2))
		for index := range jobs.Items {
			job := &jobs.Items[index]
			Expect(job.Status.Result).To(Equal(actionsv1alpha1.WorkflowJobResultSuccess))
			Expect(meta.IsStatusConditionTrue(job.Status.Conditions, actionsv1alpha1.WorkflowJobConditionConcurrencyAcquired)).To(BeTrue())
		}
	})

	It("fails a job whose group matches its parent WorkflowRun", func() {
		ctx := context.Background()
		run := concurrencyWorkflowRun("concurrency-conflict", concurrencyConflictWorkflowPath, "81111111-2222-3333-4444-555555555555")
		Expect(clusterClient.Create(ctx, run)).To(Succeed())

		Eventually(func(g Gomega) {
			storedRun := &actionsv1alpha1.WorkflowRun{}
			g.Expect(clusterClient.Get(ctx, client.ObjectKeyFromObject(run), storedRun)).To(Succeed())
			g.Expect(storedRun.Spec.CancelRequested).To(BeFalse())
			runSucceeded := meta.FindStatusCondition(storedRun.Status.Conditions, actionsv1alpha1.WorkflowRunConditionSucceeded)
			g.Expect(runSucceeded).NotTo(BeNil())
			if runSucceeded != nil {
				g.Expect(runSucceeded.Status).To(Equal(metav1.ConditionFalse), runSucceeded.Message)
			}

			jobs := &actionsv1alpha1.WorkflowJobList{}
			g.Expect(clusterClient.List(ctx, jobs, client.InNamespace(e2eNamespace), client.MatchingLabels{
				actionsv1alpha1.LabelWorkflowRunUID: string(run.UID),
			})).To(Succeed())
			g.Expect(jobs.Items).To(HaveLen(1))
			if len(jobs.Items) != 1 {
				return
			}
			job := &jobs.Items[0]
			g.Expect(job.Status.Result).To(Equal(actionsv1alpha1.WorkflowJobResultFailure))
			g.Expect(job.Status.Concurrency).NotTo(BeNil())
			if job.Status.Concurrency != nil {
				g.Expect(job.Status.Concurrency.Group).To(Equal("PARENT"))
				g.Expect(job.Status.Concurrency.CancelInProgress).To(BeTrue())
			}
			succeeded := meta.FindStatusCondition(job.Status.Conditions, actionsv1alpha1.WorkflowJobConditionSucceeded)
			g.Expect(succeeded).NotTo(BeNil())
			if succeeded != nil {
				g.Expect(succeeded.Status).To(Equal(metav1.ConditionFalse))
				g.Expect(succeeded.Reason).To(Equal("ConcurrencyEvaluationFailed"))
				g.Expect(succeeded.Message).To(ContainSubstring(`WorkflowRun "concurrency-conflict"`))
			}
		}, 90*time.Second, time.Second).Should(Succeed())
	})
})

func concurrencyWorkflowRun(name, workflowPath, deliveryID string) *actionsv1alpha1.WorkflowRun {
	return &actionsv1alpha1.WorkflowRun{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: e2eNamespace},
		Spec: actionsv1alpha1.WorkflowRunSpec{
			ProjectRef: corev1.LocalObjectReference{Name: "default"},
			Source: actionsv1alpha1.WorkflowRunSource{
				Type: actionsv1alpha1.SourceTypeGitHub,
				GitHub: &actionsv1alpha1.GitHubWorkflowRunSource{
					Repository: actionsv1alpha1.GitHubRepository{ID: 123456789, Owner: "acme", Name: "example"},
					Event: actionsv1alpha1.GitHubEvent{
						Name:       "push",
						DeliveryID: deliveryID,
					},
					Revision: actionsv1alpha1.GitRevision{SHA: fixtureRevision, Ref: "refs/heads/main"},
				},
			},
			WorkflowPath: workflowPath,
		},
	}
}
