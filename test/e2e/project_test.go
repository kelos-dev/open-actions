//go:build e2e

package e2e_test

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	actionsv1alpha1 "github.com/kelos-dev/open-actions/api/v1alpha1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var _ = Describe("Project", func() {
	BeforeEach(func() {
		setupTestProject(false)
	})

	It("turns a signed GitHub webhook into a typed WorkflowRun", func() {
		response, err := sendPushWebhook("example", "11111111-2222-3333-4444-555555555555")
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

		Eventually(func(g Gomega) {
			runs := &actionsv1alpha1.WorkflowRunList{}
			g.Expect(clusterClient.List(context.Background(), runs, client.InNamespace(e2eNamespace))).To(Succeed())
			g.Expect(runs.Items).To(HaveLen(1))
			if len(runs.Items) != 1 {
				return
			}
			run := runs.Items[0]
			githubSource := run.Spec.Source.GitHub
			g.Expect(run.Spec.ProjectRef.Name).To(Equal("default"))
			g.Expect(run.Spec.Source.Type).To(Equal(actionsv1alpha1.SourceTypeGitHub))
			g.Expect(githubSource.Repository.Owner).To(Equal("acme"))
			g.Expect(githubSource.Repository.Name).To(Equal("example"))
			g.Expect(run.Spec.WorkflowPath).To(Equal(workflowPath))
			g.Expect(githubSource.Event.Name).To(Equal(actionsv1alpha1.GitHubEventNamePush))
			g.Expect(githubSource.Revision.SHA).To(Equal(fixtureRevision))
			g.Expect(githubSource.Revision.Ref).To(Equal("refs/heads/main"))
		}, 60*time.Second, time.Second).Should(Succeed())
	})

	DescribeTable("reports invalid workflows while planning valid siblings",
		func(repository, deliveryID, message string) {
			response, err := sendPushWebhook(repository, deliveryID)
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
			var invalidRun actionsv1alpha1.WorkflowRun
			Eventually(func(g Gomega) {
				deliveries := &corev1.ConfigMapList{}
				g.Expect(clusterClient.List(context.Background(), deliveries, client.InNamespace(e2eNamespace), client.MatchingLabels{"actions.kelos.dev/webhook-delivery": "true"})).To(Succeed())
				g.Expect(deliveries.Items).To(HaveLen(1))
				if len(deliveries.Items) == 1 {
					g.Expect(deliveries.Items[0].Data["state"]).To(Equal("Completed"))
					g.Expect(deliveries.Items[0].Data["workflowRuns"]).To(Equal("2"))
				}
				runs := &actionsv1alpha1.WorkflowRunList{}
				g.Expect(clusterClient.List(context.Background(), runs, client.InNamespace(e2eNamespace))).To(Succeed())
				g.Expect(runs.Items).To(HaveLen(2))
				for _, run := range runs.Items {
					planned := meta.FindStatusCondition(run.Status.Conditions, actionsv1alpha1.WorkflowRunConditionPlanned)
					g.Expect(planned).NotTo(BeNil())
					if planned == nil {
						continue
					}
					jobs := &actionsv1alpha1.WorkflowJobList{}
					g.Expect(clusterClient.List(context.Background(), jobs, client.InNamespace(e2eNamespace), client.MatchingLabels{
						actionsv1alpha1.LabelWorkflowRunUID: string(run.UID),
					})).To(Succeed())
					if run.Spec.WorkflowPath != workflowPath {
						g.Expect(run.Spec.WorkflowPath).To(Equal(".open-actions/workflows/valid.yaml"))
						g.Expect(planned.Status).To(Equal(metav1.ConditionTrue))
						g.Expect(jobs.Items).To(HaveLen(2))
						continue
					}
					invalidRun = run
					g.Expect(run.Status.WorkflowName).To(Equal(workflowPath))
					g.Expect(run.Status.CompletionTime).NotTo(BeNil())
					g.Expect(planned.Status).To(Equal(metav1.ConditionFalse))
					g.Expect(planned.Reason).To(Equal("WorkflowInvalid"))
					g.Expect(planned.Message).To(ContainSubstring(message))
					g.Expect(planned.Message).To(ContainSubstring(workflowPath))
					g.Expect(meta.IsStatusConditionFalse(run.Status.Conditions, actionsv1alpha1.WorkflowRunConditionSucceeded)).To(BeTrue())
					g.Expect(jobs.Items).To(BeEmpty())
					g.Expect(run.Status.Source).NotTo(BeNil())
					if run.Status.Source != nil {
						g.Expect(run.Status.Source.GitHub).NotTo(BeNil())
						if run.Status.Source.GitHub != nil {
							status := run.Status.Source.GitHub.ValidationStatus
							g.Expect(status).NotTo(BeNil())
							if status != nil {
								g.Expect(status.State).To(Equal(actionsv1alpha1.GitHubCommitStatusStateFailure))
								g.Expect(status.ReportDigest).NotTo(BeEmpty())
							}
						}
					}
				}
				g.Expect(invalidRun.Name).NotTo(BeEmpty())
			}, 60*time.Second, time.Second).Should(Succeed())
			runURL := consoleURL + "/runs/" + url.PathEscape(invalidRun.Namespace) + "/" + url.PathEscape(invalidRun.Name)
			statusResponse, err := http.Get(fixtureURL + "/fixture/commit-status?target_url=" + url.QueryEscape(runURL))
			Expect(err).NotTo(HaveOccurred())
			defer statusResponse.Body.Close()
			Expect(statusResponse.StatusCode).To(Equal(http.StatusOK))
			var report struct {
				State   string `json:"state"`
				Context string `json:"context"`
			}
			Expect(json.NewDecoder(statusResponse.Body).Decode(&report)).To(Succeed())
			Expect(report.State).To(Equal("failure"))
			Expect(report.Context).To(Equal("Open Actions validation / " + workflowPath))
			page := getConsolePage(&http.Client{Timeout: 30 * time.Second}, runURL, http.StatusOK)
			Expect(page).To(ContainSubstring("Workflow validation failed"))
			Expect(page).To(ContainSubstring(message))
		},
		Entry("for an unsupported trigger", "invalid-trigger", "21111111-2222-3333-4444-555555555555", "unsupported trigger"),
		Entry("for an unsupported workflow field", "invalid-field", "31111111-2222-3333-4444-555555555555", "field unsupported-field not found"),
	)
})

func sendPushWebhook(repository, deliveryID string) (*http.Response, error) {
	payload := fmt.Sprintf(`{"after":%q,"ref":"refs/heads/main","installation":{"id":%d},"repository":{"id":123456789,"name":%q,"owner":{"login":"acme"}}}`, fixtureRevision, installationID, repository)
	mac := hmac.New(sha256.New, []byte(webhookSecret))
	if _, err := mac.Write([]byte(payload)); err != nil {
		return nil, err
	}
	request, err := http.NewRequest(http.MethodPost, webhookURL+"/", strings.NewReader(payload))
	if err != nil {
		return nil, err
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-GitHub-Event", "push")
	request.Header.Set("X-GitHub-Delivery", deliveryID)
	request.Header.Set("X-Hub-Signature-256", "sha256="+hex.EncodeToString(mac.Sum(nil)))
	return http.DefaultClient.Do(request)
}
