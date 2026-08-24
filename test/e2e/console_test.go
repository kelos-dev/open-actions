//go:build e2e

package e2e_test

import (
	"context"
	"encoding/json"
	"io"
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

var _ = Describe("Console", func() {
	BeforeEach(func() {
		setupTestProject(true)
	})

	It("accepts administrator login and serves run logs without authentication", func() {
		ctx := context.Background()
		run := &actionsv1alpha1.WorkflowRun{
			ObjectMeta: metav1.ObjectMeta{Name: "console", Namespace: e2eNamespace},
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

		Eventually(func(g Gomega) {
			stored := &actionsv1alpha1.WorkflowRun{}
			g.Expect(clusterClient.Get(ctx, client.ObjectKeyFromObject(run), stored)).To(Succeed())
			condition := meta.FindStatusCondition(stored.Status.Conditions, actionsv1alpha1.WorkflowRunConditionSucceeded)
			g.Expect(condition).NotTo(BeNil())
			if condition != nil {
				g.Expect(condition.Status).To(Equal(metav1.ConditionTrue), condition.Message)
			}
			g.Expect(stored.Status.Source).NotTo(BeNil())
			if stored.Status.Source == nil {
				return
			}
			g.Expect(stored.Status.Source.GitHub).NotTo(BeNil())
			if stored.Status.Source.GitHub == nil {
				return
			}
			checkRun := stored.Status.Source.GitHub.CheckRun
			g.Expect(checkRun).NotTo(BeNil())
			if checkRun != nil {
				g.Expect(checkRun.Status).To(Equal("completed"))
				g.Expect(checkRun.Conclusion).To(Equal("success"))
			}
		}, 180*time.Second, time.Second).Should(Succeed())
		Eventually(func(g Gomega) {
			response, err := http.Get(fixtureURL + "/fixture/check-runs/" + url.PathEscape(string(run.UID)))
			g.Expect(err).NotTo(HaveOccurred())
			if err != nil {
				return
			}
			defer response.Body.Close()
			g.Expect(response.StatusCode).To(Equal(http.StatusOK))
			if response.StatusCode != http.StatusOK {
				return
			}
			report := struct {
				DetailsURL string `json:"details_url"`
				Status     string `json:"status"`
				Conclusion string `json:"conclusion"`
			}{}
			g.Expect(json.NewDecoder(response.Body).Decode(&report)).To(Succeed())
			runPath := "/runs/" + url.PathEscape(run.Namespace) + "/" + url.PathEscape(run.Name)
			g.Expect(report.DetailsURL).To(Equal(consoleURL + runPath))
			g.Expect(report.Status).To(Equal("completed"))
			g.Expect(report.Conclusion).To(Equal("success"))
		}, 30*time.Second, time.Second).Should(Succeed())

		var workflowJob actionsv1alpha1.WorkflowJob
		Eventually(func(g Gomega) {
			jobs := &actionsv1alpha1.WorkflowJobList{}
			g.Expect(clusterClient.List(ctx, jobs, client.InNamespace(e2eNamespace), client.MatchingLabels{
				actionsv1alpha1.LabelWorkflowRunUID: string(run.UID),
			})).To(Succeed())
			g.Expect(jobs.Items).To(HaveLen(2))
			for index := range jobs.Items {
				if jobs.Items[index].Spec.JobID == "test" {
					workflowJob = jobs.Items[index]
				}
			}
			g.Expect(workflowJob.Name).NotTo(BeEmpty())
		}, 30*time.Second, time.Second).Should(Succeed())

		webClient := &http.Client{Timeout: 30 * time.Second}
		loginBody, err := json.Marshal(map[string]string{"token": consoleToken})
		Expect(err).NotTo(HaveOccurred())
		loginResponse, err := webClient.Post(consoleURL+"/api/login", "application/json", strings.NewReader(string(loginBody)))
		Expect(err).NotTo(HaveOccurred())
		Expect(loginResponse.StatusCode).To(Equal(http.StatusOK))
		Expect(loginResponse.Cookies()).To(ContainElement(And(
			WithTransform(func(cookie *http.Cookie) string { return cookie.Name }, Equal("open_actions_console_session")),
			WithTransform(func(cookie *http.Cookie) bool { return cookie.HttpOnly }, BeTrue()),
		)))
		Expect(loginResponse.Body.Close()).To(Succeed())

		runPath := "/runs/" + url.PathEscape(run.Namespace) + "/" + url.PathEscape(run.Name)
		mainPage := getConsolePage(webClient, consoleURL+"/", http.StatusOK)
		Expect(mainPage).To(ContainSubstring("Workflow runs"))
		Expect(mainPage).To(ContainSubstring(`href="` + runPath + `"`))
		Expect(mainPage).To(ContainSubstring("Fixture CI"))
		Expect(mainPage).To(ContainSubstring("acme/example"))
		runPage := getConsolePage(webClient, consoleURL+runPath, http.StatusOK)
		Expect(runPage).To(ContainSubstring("Fixture CI"))
		Expect(runPage).To(ContainSubstring("acme/example"))
		Expect(runPage).To(ContainSubstring(`<pre class="workflow-source"><code>name: Fixture CI`))
		Expect(runPage).To(ContainSubstring("open actions e2e works"))
		Expect(runPage).To(ContainSubstring(">test</span>"))
		Expect(runPage).To(ContainSubstring(">Succeeded</span>"))
		Expect(runPage).To(ContainSubstring(">runner-1</span>"))

		jobPath := runPath + "/jobs/" + url.PathEscape(workflowJob.Name)
		jobPage := getConsolePage(webClient, consoleURL+jobPath, http.StatusOK)
		Expect(jobPage).To(ContainSubstring("<h1>test</h1>"))
		Expect(jobPage).To(ContainSubstring("Show debug"))
		Expect(jobPage).To(ContainSubstring("Show timestamps"))
		Expect(jobPage).To(ContainSubstring(`data-stream-url="` + jobPath + `/stream"`))

		stream := getConsolePage(webClient, consoleURL+jobPath+"/stream", http.StatusOK)
		Expect(stream).To(ContainSubstring("event: log"))
		Expect(stream).To(ContainSubstring(`"kind":"group"`))
		Expect(stream).To(ContainSubstring(`"kind":"input"`))
		Expect(stream).To(ContainSubstring(`"kind":"step-output"`))
		Expect(stream).To(ContainSubstring("open actions e2e works"))
		Expect(stream).To(ContainSubstring("event: end"))
	})
})

func getConsolePage(webClient *http.Client, target string, status int) string {
	response, err := webClient.Get(target)
	Expect(err).NotTo(HaveOccurred())
	defer response.Body.Close()
	data, err := io.ReadAll(response.Body)
	Expect(err).NotTo(HaveOccurred())
	Expect(response.StatusCode).To(Equal(status), strings.TrimSpace(string(data)))
	Expect(response.Header.Get("Cache-Control")).To(Equal("no-store"))
	return string(data)
}
