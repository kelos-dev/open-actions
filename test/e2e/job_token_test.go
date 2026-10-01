//go:build e2e

package e2e_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	actionsv1alpha1 "github.com/kelos-dev/open-actions/api/v1alpha1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var _ = Describe("Job token triggers", func() {
	BeforeEach(func() {
		setupTestProject(false)
	})

	DescribeTable("applies the recursive-trigger policy to signed webhooks", func(event, actor, nativeEvent string, expectedRuns int) {
		ctx := context.Background()
		payload := fmt.Sprintf(`{"action":"created","sender":{"login":%q},"installation":{"id":%d},"repository":{"id":123456789,"name":"example","default_branch":"main","owner":{"login":"acme"}},"issue":{"number":42},"comment":{"body":"Run the workflow"}}`, actor, installationID)
		workflowPath := ".open-actions/workflows/issue-comment.yaml"
		if event == "workflow_run" {
			payload = fmt.Sprintf(`{"action":"completed","sender":{"login":%q},"installation":{"id":%d},"repository":{"id":123456789,"name":"example","default_branch":"main","owner":{"login":"acme"}},"workflow_run":{"name":"CI","event":%q,"head_branch":"main","head_sha":%q,"conclusion":"success"}}`, actor, installationID, nativeEvent, fixtureRevision)
			workflowPath = ".open-actions/workflows/workflow-run.yaml"
		}
		expectAcceptedWebhook(event, "41111111-2222-3333-4444-555555555555", payload)

		Eventually(func(g Gomega) {
			expectCompletedDelivery(g, expectedRuns)
			runs := &actionsv1alpha1.WorkflowRunList{}
			g.Expect(clusterClient.List(ctx, runs, client.InNamespace(e2eNamespace))).To(Succeed())
			g.Expect(runs.Items).To(HaveLen(expectedRuns))
			for _, run := range runs.Items {
				g.Expect(run.Spec.WorkflowPath).To(Equal(workflowPath))
				g.Expect(run.Spec.Source.GitHub.Actor).To(Equal(actor))
				g.Expect(run.Spec.Source.GitHub.Event.Name).To(Equal(actionsv1alpha1.GitHubEventName(event)))
				g.Expect(run.Spec.Source.GitHub.Revision.SHA).To(Equal(fixtureRevision))
				g.Expect(run.Spec.Approval).To(BeNil())
				g.Expect(meta.IsStatusConditionTrue(run.Status.Conditions, actionsv1alpha1.WorkflowRunConditionPlanned)).To(BeTrue())
			}
			jobs := &actionsv1alpha1.WorkflowJobList{}
			g.Expect(clusterClient.List(ctx, jobs, client.InNamespace(e2eNamespace))).To(Succeed())
			g.Expect(jobs.Items).To(HaveLen(expectedRuns))
		}, 60*time.Second, time.Second).Should(Succeed())
	},
		Entry("suppresses the Project App comment", "issue_comment", "open-actions[bot]", "", 0),
		Entry("allows a human comment", "issue_comment", "octocat", "", 1),
		Entry("allows another App comment", "issue_comment", "other-app[bot]", "", 1),
		Entry("suppresses the Project App native push run", "workflow_run", "open-actions[bot]", "push", 0),
		Entry("allows the Project App native dispatch", "workflow_run", "open-actions[bot]", "workflow_dispatch", 1),
		Entry("allows a human native push run", "workflow_run", "octocat", "push", 1),
	)

	It("holds an App-triggered pull request until an administrator approves it in the Console", func() {
		ctx := context.Background()
		payload := fmt.Sprintf(`{"action":"opened","sender":{"login":"open-actions[bot]"},"installation":{"id":%d},"repository":{"id":123456789,"name":"example","default_branch":"main","owner":{"login":"acme"}},"pull_request":{"number":42,"state":"open","mergeable":true,"html_url":"https://github.com/acme/example/pull/42","user":{"login":"open-actions[bot]"},"head":{"ref":"feature","sha":%q,"repo":{"id":123456789,"name":"example","owner":{"login":"acme"}}},"base":{"ref":"main","sha":%q}}}`, installationID, fixtureRepositoryRevision.HeadSHA, fixtureRepositoryRevision.BaseSHA)
		expectAcceptedWebhook("pull_request", "51111111-2222-3333-4444-555555555555", payload)

		var run actionsv1alpha1.WorkflowRun
		Eventually(func(g Gomega) {
			expectCompletedDelivery(g, 1)
			runs := &actionsv1alpha1.WorkflowRunList{}
			g.Expect(clusterClient.List(ctx, runs, client.InNamespace(e2eNamespace))).To(Succeed())
			g.Expect(runs.Items).To(HaveLen(1))
			if len(runs.Items) != 1 {
				return
			}
			run = runs.Items[0]
			g.Expect(run.Spec.WorkflowPath).To(Equal(pullRequestWorkflowPath))
			g.Expect(run.Spec.Source.GitHub.Event.Name).To(Equal(actionsv1alpha1.GitHubEventNamePullRequest))
			g.Expect(run.Spec.Source.GitHub.Revision.SHA).To(Equal(fixtureRepositoryRevision.IntegrationSHA))
			g.Expect(run.Spec.Approval).To(Equal(&actionsv1alpha1.WorkflowRunApproval{Approved: false}))
			g.Expect(run.Spec.ForkPullRequest).To(BeNil())
			g.Expect(meta.IsStatusConditionFalse(run.Status.Conditions, actionsv1alpha1.WorkflowRunConditionApproved)).To(BeTrue())
			jobs := &actionsv1alpha1.WorkflowJobList{}
			g.Expect(clusterClient.List(ctx, jobs, client.InNamespace(e2eNamespace))).To(Succeed())
			g.Expect(jobs.Items).To(BeEmpty())
		}, 60*time.Second, time.Second).Should(Succeed())

		runPath := "/runs/" + url.PathEscape(run.Namespace) + "/" + url.PathEscape(run.Name)
		pageRequest, err := http.NewRequestWithContext(ctx, http.MethodGet, consoleURL+runPath, nil)
		Expect(err).NotTo(HaveOccurred())
		pageRequest.Header.Set("Authorization", "Bearer "+consoleToken)
		pageResponse, err := http.DefaultClient.Do(pageRequest)
		Expect(err).NotTo(HaveOccurred())
		page, err := io.ReadAll(pageResponse.Body)
		Expect(pageResponse.Body.Close()).To(Succeed())
		Expect(err).NotTo(HaveOccurred())
		Expect(pageResponse.StatusCode).To(Equal(http.StatusOK), string(page))
		Expect(string(page)).To(ContainSubstring(`action="` + runPath + `/approve"`))
		csrfMatch := regexp.MustCompile(`name="csrf" value="([^"]+)"`).FindSubmatch(page)
		Expect(csrfMatch).To(HaveLen(2))

		form := url.Values{"csrf": {string(csrfMatch[1])}}
		request, err := http.NewRequestWithContext(ctx, http.MethodPost, consoleURL+runPath+"/approve", strings.NewReader(form.Encode()))
		Expect(err).NotTo(HaveOccurred())
		request.Header.Set("Authorization", "Bearer "+consoleToken)
		request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		webClient := &http.Client{Timeout: 30 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		}}
		response, err := webClient.Do(request)
		Expect(err).NotTo(HaveOccurred())
		Expect(response.Body.Close()).To(Succeed())
		Expect(response.StatusCode).To(Equal(http.StatusSeeOther))

		Eventually(func(g Gomega) {
			stored := &actionsv1alpha1.WorkflowRun{}
			g.Expect(clusterClient.Get(ctx, client.ObjectKeyFromObject(&run), stored)).To(Succeed())
			g.Expect(stored.Spec.Approval).To(Equal(&actionsv1alpha1.WorkflowRunApproval{Approved: true}))
			g.Expect(meta.IsStatusConditionTrue(stored.Status.Conditions, actionsv1alpha1.WorkflowRunConditionApproved)).To(BeTrue())
			g.Expect(meta.IsStatusConditionTrue(stored.Status.Conditions, actionsv1alpha1.WorkflowRunConditionPlanned)).To(BeTrue())
			jobs := &actionsv1alpha1.WorkflowJobList{}
			g.Expect(clusterClient.List(ctx, jobs, client.InNamespace(e2eNamespace))).To(Succeed())
			g.Expect(jobs.Items).To(HaveLen(1))
			if len(jobs.Items) == 1 {
				g.Expect(jobs.Items[0].Labels).To(HaveKeyWithValue(actionsv1alpha1.LabelWorkflowRunUID, string(run.UID)))
				g.Expect(jobs.Items[0].Spec.JobID).To(Equal("test"))
			}
		}, 60*time.Second, time.Second).Should(Succeed())
	})
})

func expectAcceptedWebhook(event, deliveryID, payload string) {
	GinkgoHelper()
	response, err := sendGitHubWebhook(event, deliveryID, payload)
	Expect(err).NotTo(HaveOccurred())
	defer response.Body.Close()
	var result struct {
		Accepted bool `json:"accepted"`
		Queued   bool `json:"queued"`
	}
	Expect(json.NewDecoder(response.Body).Decode(&result)).To(Succeed())
	Expect(response.StatusCode).To(Equal(http.StatusAccepted))
	Expect(result.Accepted).To(BeTrue())
	Expect(result.Queued).To(BeTrue())
}

func expectCompletedDelivery(g Gomega, expectedRuns int) {
	GinkgoHelper()
	deliveries := &corev1.ConfigMapList{}
	g.Expect(clusterClient.List(context.Background(), deliveries, client.InNamespace(e2eNamespace), client.MatchingLabels{"actions.kelos.dev/webhook-delivery": "true"})).To(Succeed())
	g.Expect(deliveries.Items).To(HaveLen(1))
	if len(deliveries.Items) == 1 {
		g.Expect(deliveries.Items[0].Data["state"]).To(Equal("Completed"))
		g.Expect(deliveries.Items[0].Data["workflowRuns"]).To(Equal(strconv.Itoa(expectedRuns)))
	}
}
