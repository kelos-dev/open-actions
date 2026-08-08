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
	"strings"
	"time"

	actionsv1alpha1 "github.com/kelos-dev/open-actions/api/v1alpha1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var _ = Describe("ActionsGateway", func() {
	BeforeEach(func() {
		setupControlPlane(false)
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
			g.Expect(run.Spec.GatewayRef.Name).To(Equal("default"))
			g.Expect(run.Spec.Source.Type).To(Equal(actionsv1alpha1.SourceTypeGitHub))
			g.Expect(githubSource.Repository.Owner).To(Equal("acme"))
			g.Expect(githubSource.Repository.Name).To(Equal("example"))
			g.Expect(run.Spec.WorkflowPath).To(Equal(workflowPath))
			g.Expect(githubSource.Event.Name).To(Equal(actionsv1alpha1.GitHubEventNamePush))
			g.Expect(githubSource.Revision.SHA).To(Equal(fixtureRevision))
			g.Expect(githubSource.Revision.Ref).To(Equal("refs/heads/main"))
		}, 60*time.Second, time.Second).Should(Succeed())
	})

	DescribeTable("records invalid workflows as failed deliveries",
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
			Eventually(func(g Gomega) {
				deliveries := &corev1.ConfigMapList{}
				g.Expect(clusterClient.List(context.Background(), deliveries, client.InNamespace(e2eNamespace), client.MatchingLabels{"actions.kelos.dev/webhook-delivery": "true"})).To(Succeed())
				g.Expect(deliveries.Items).To(HaveLen(1))
				if len(deliveries.Items) == 1 {
					g.Expect(deliveries.Items[0].Data["state"]).To(Equal("Failed"))
					g.Expect(deliveries.Items[0].Data["message"]).To(ContainSubstring(message))
				}
			}, 60*time.Second, time.Second).Should(Succeed())
		},
		Entry("for an unsupported trigger", "invalid-trigger", "21111111-2222-3333-4444-555555555555", "unsupported trigger"),
		Entry("for an unsupported workflow field", "invalid-field", "31111111-2222-3333-4444-555555555555", "field timeout-minutes not found"),
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
