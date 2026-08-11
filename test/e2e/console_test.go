//go:build e2e

package e2e_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/cookiejar"
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

	It("publishes a discoverable run, authenticates the user, and streams logs", func() {
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
			g.Expect(jobs.Items).To(HaveLen(1))
			if len(jobs.Items) == 1 {
				workflowJob = jobs.Items[0]
			}
		}, 30*time.Second, time.Second).Should(Succeed())

		webClient := authenticateConsole(run)
		runPath := "/runs/" + url.PathEscape(run.Namespace) + "/" + url.PathEscape(run.Name)
		mainPage := getConsolePage(webClient, consoleURL+"/", http.StatusOK)
		Expect(mainPage).To(ContainSubstring("Workflow runs"))
		Expect(mainPage).To(ContainSubstring(`href="` + runPath + `"`))
		Expect(mainPage).To(ContainSubstring("Fixture CI"))
		Expect(mainPage).To(ContainSubstring("acme/example"))
		runPage := getConsolePage(webClient, consoleURL+runPath, http.StatusOK)
		Expect(runPage).To(ContainSubstring("Fixture CI"))
		Expect(runPage).To(ContainSubstring("acme/example"))
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

func authenticateConsole(run *actionsv1alpha1.WorkflowRun) *http.Client {
	jar, err := cookiejar.New(nil)
	Expect(err).NotTo(HaveOccurred())
	webClient := &http.Client{
		Jar:     jar,
		Timeout: 30 * time.Second,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	runPath := "/runs/" + url.PathEscape(run.Namespace) + "/" + url.PathEscape(run.Name)
	response, err := webClient.Get(consoleURL + runPath)
	Expect(err).NotTo(HaveOccurred())
	Expect(response.StatusCode).To(Equal(http.StatusFound))
	Expect(response.Body.Close()).To(Succeed())

	loginURL, err := response.Location()
	Expect(err).NotTo(HaveOccurred())
	Expect(loginURL.Path).To(Equal("/login"))
	Expect(loginURL.Query().Get("next")).To(Equal(runPath))
	response, err = webClient.Get(loginURL.String())
	Expect(err).NotTo(HaveOccurred())
	Expect(response.StatusCode).To(Equal(http.StatusOK))
	Expect(response.Body.Close()).To(Succeed())

	body, err := json.Marshal(map[string]string{"token": consoleToken})
	Expect(err).NotTo(HaveOccurred())
	response, err = webClient.Post(consoleURL+"/api/login", "application/json", strings.NewReader(string(body)))
	Expect(err).NotTo(HaveOccurred())
	Expect(response.StatusCode).To(Equal(http.StatusOK))
	Expect(response.Body.Close()).To(Succeed())
	return webClient
}

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
