//go:build e2e

package e2e_test

import (
	"context"
	"time"

	actionsv1alpha1 "github.com/kelos-dev/open-actions/api/v1alpha1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var _ = Describe("Workflow job Service", func() {
	BeforeEach(func() {
		setupTestProject(true)
	})

	It("routes requests to the workflow server and removes its Service after cancellation", func() {
		ctx := context.Background()
		runnerObject := &actionsv1alpha1.Runner{}
		Expect(clusterClient.Get(ctx, client.ObjectKey{Namespace: e2eNamespace, Name: "runner-1"}, runnerObject)).To(Succeed())
		targetPort := int32(8080)
		runnerObject.Spec.Execution.Service = &actionsv1alpha1.RunnerServiceSpec{
			Ports: []actionsv1alpha1.RunnerServicePort{{Name: "http", Port: 80, TargetPort: &targetPort}},
		}
		runnerObject.Spec.Execution.Docker = nil
		Expect(clusterClient.Update(ctx, runnerObject)).To(Succeed())
		Eventually(func(g Gomega) {
			stored := &actionsv1alpha1.Runner{}
			g.Expect(clusterClient.Get(ctx, client.ObjectKeyFromObject(runnerObject), stored)).To(Succeed())
			condition := meta.FindStatusCondition(stored.Status.Conditions, actionsv1alpha1.RunnerConditionReady)
			g.Expect(condition).NotTo(BeNil())
			if condition != nil {
				g.Expect(condition.ObservedGeneration).To(Equal(stored.Generation))
				g.Expect(condition.Status).To(Equal(metav1.ConditionTrue))
			}
		}, 60*time.Second, time.Second).Should(Succeed())

		run := concurrencyWorkflowRun("job-service", ".open-actions/workflows/job-service.yaml", "91111111-2222-3333-4444-555555555555")
		Expect(clusterClient.Create(ctx, run)).To(Succeed())
		var jobName, serviceName, podName string
		Eventually(func(g Gomega) {
			jobs := &actionsv1alpha1.WorkflowJobList{}
			g.Expect(clusterClient.List(ctx, jobs, client.InNamespace(e2eNamespace), client.MatchingLabels{
				actionsv1alpha1.LabelWorkflowRunUID: string(run.UID),
			})).To(Succeed())
			g.Expect(jobs.Items).To(HaveLen(1))
			if len(jobs.Items) != 1 {
				return
			}
			g.Expect(jobs.Items[0].Status.ServiceRef).NotTo(BeNil())
			if jobs.Items[0].Status.ServiceRef == nil {
				return
			}
			job := &jobs.Items[0]
			jobName, serviceName = job.Name, job.Status.ServiceRef.Name
			service := &corev1.Service{}
			g.Expect(clusterClient.Get(ctx, client.ObjectKey{Namespace: e2eNamespace, Name: serviceName}, service)).To(Succeed())
			g.Expect(service.Spec.Type).To(Equal(corev1.ServiceTypeClusterIP))
			g.Expect(service.Spec.ClusterIP).NotTo(BeEmpty())
			g.Expect(metav1.IsControlledBy(service, job)).To(BeTrue())
			pods := &corev1.PodList{}
			g.Expect(clusterClient.List(ctx, pods, client.InNamespace(e2eNamespace), client.MatchingLabels(service.Spec.Selector))).To(Succeed())
			g.Expect(pods.Items).To(HaveLen(1))
			if len(pods.Items) == 1 {
				podName = pods.Items[0].Name
				g.Expect(pods.Items[0].Status.Phase).To(Equal(corev1.PodRunning))
			}
		}, 120*time.Second, time.Second).Should(Succeed())

		Eventually(func(g Gomega) {
			slices := &discoveryv1.EndpointSliceList{}
			g.Expect(clusterClient.List(ctx, slices, client.InNamespace(e2eNamespace), client.MatchingLabels{discoveryv1.LabelServiceName: serviceName})).To(Succeed())
			ready := false
			for _, slice := range slices.Items {
				for _, endpoint := range slice.Endpoints {
					if endpoint.TargetRef != nil && endpoint.TargetRef.Name == podName && endpoint.Conditions.Ready != nil && *endpoint.Conditions.Ready {
						ready = true
					}
				}
			}
			g.Expect(ready).To(BeTrue())
		}, 30*time.Second, time.Second).Should(Succeed())

		Eventually(func() (string, error) {
			requestContext, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			data, err := clientset.CoreV1().Services(e2eNamespace).
				ProxyGet("http", serviceName, "http", "/", nil).DoRaw(requestContext)
			return string(data), err
		}, 60*time.Second, time.Second).Should(Equal("Workflow preview\n"))

		Eventually(func() error {
			stored := &actionsv1alpha1.WorkflowRun{}
			if err := clusterClient.Get(ctx, client.ObjectKeyFromObject(run), stored); err != nil {
				return err
			}
			stored.Spec.CancelRequested = true
			return clusterClient.Update(ctx, stored)
		}, 30*time.Second, time.Second).Should(Succeed())
		Eventually(func(g Gomega) {
			job := &actionsv1alpha1.WorkflowJob{}
			g.Expect(clusterClient.Get(ctx, client.ObjectKey{Namespace: e2eNamespace, Name: jobName}, job)).To(Succeed())
			g.Expect(job.Status.Result).To(Equal(actionsv1alpha1.WorkflowJobResultCancelled))
			g.Expect(job.Status.ServiceRef).To(BeNil())
			err := clusterClient.Get(ctx, client.ObjectKey{Namespace: e2eNamespace, Name: serviceName}, &corev1.Service{})
			g.Expect(apierrors.IsNotFound(err)).To(BeTrue())
		}, 90*time.Second, time.Second).Should(Succeed())
	})
})
