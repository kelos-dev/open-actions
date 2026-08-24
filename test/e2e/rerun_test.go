//go:build e2e

package e2e_test

import (
	"context"
	"io"
	"net/http"
	"net/url"
	"regexp"
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

var _ = Describe("Reruns", func() {
	BeforeEach(func() {
		setupTestProject(true)
	})

	It("reuses successful prerequisite results when rerunning failed jobs", func() {
		ctx := context.Background()
		root := &actionsv1alpha1.WorkflowRun{
			ObjectMeta: metav1.ObjectMeta{Name: "selective-rerun", Namespace: e2eNamespace},
			Spec: actionsv1alpha1.WorkflowRunSpec{
				ProjectRef: corev1.LocalObjectReference{Name: "default"},
				Source: actionsv1alpha1.WorkflowRunSource{
					Type: actionsv1alpha1.SourceTypeGitHub,
					GitHub: &actionsv1alpha1.GitHubWorkflowRunSource{
						Repository: actionsv1alpha1.GitHubRepository{ID: 123456789, Owner: "acme", Name: "example"},
						Event: actionsv1alpha1.GitHubEvent{
							Name:       "push",
							DeliveryID: "91111111-2222-3333-4444-555555555555",
						},
						Revision: actionsv1alpha1.GitRevision{SHA: fixtureRevision, Ref: "refs/heads/main"},
					},
				},
				WorkflowPath: selectiveRerunWorkflowPath,
			},
		}
		Expect(clusterClient.Create(ctx, root)).To(Succeed())

		Eventually(func(g Gomega) {
			stored := &actionsv1alpha1.WorkflowRun{}
			g.Expect(clusterClient.Get(ctx, client.ObjectKeyFromObject(root), stored)).To(Succeed())
			condition := meta.FindStatusCondition(stored.Status.Conditions, actionsv1alpha1.WorkflowRunConditionSucceeded)
			g.Expect(condition).NotTo(BeNil())
			if condition != nil {
				g.Expect(condition.Status).To(Equal(metav1.ConditionFalse), condition.Message)
				g.Expect(condition.Reason).To(Equal("JobFailed"))
			}
		}, 180*time.Second, time.Second).Should(Succeed())

		rootJobs := &actionsv1alpha1.WorkflowJobList{}
		Expect(clusterClient.List(ctx, rootJobs, client.InNamespace(e2eNamespace), client.MatchingLabels{
			actionsv1alpha1.LabelWorkflowRunUID: string(root.UID),
		})).To(Succeed())
		Expect(rootJobs.Items).To(HaveLen(2))
		for index := range rootJobs.Items {
			job := &rootJobs.Items[index]
			switch job.Spec.JobID {
			case "prepare":
				Expect(job.Status.Result).To(Equal(actionsv1alpha1.WorkflowJobResultSuccess))
				Expect(job.Status.Outputs).To(HaveKeyWithValue("marker", "attempt-1"))
			case "verify":
				Expect(job.Status.Result).To(Equal(actionsv1alpha1.WorkflowJobResultFailure))
			default:
				Fail("unexpected WorkflowJob " + job.Spec.JobID)
			}
		}

		runPath := "/runs/" + url.PathEscape(root.Namespace) + "/" + url.PathEscape(root.Name)
		pageRequest, err := http.NewRequestWithContext(ctx, http.MethodGet, consoleURL+runPath, nil)
		Expect(err).NotTo(HaveOccurred())
		pageRequest.Header.Set("Authorization", "Bearer "+consoleToken)
		pageResponse, err := http.DefaultClient.Do(pageRequest)
		Expect(err).NotTo(HaveOccurred())
		page, err := io.ReadAll(pageResponse.Body)
		Expect(pageResponse.Body.Close()).To(Succeed())
		Expect(err).NotTo(HaveOccurred())
		Expect(pageResponse.StatusCode).To(Equal(http.StatusOK), strings.TrimSpace(string(page)))
		csrfMatch := regexp.MustCompile(`name="csrf" value="([^"]+)"`).FindSubmatch(page)
		Expect(csrfMatch).To(HaveLen(2))

		form := url.Values{"csrf": {string(csrfMatch[1])}, "jobs": {"failed"}}
		rerunRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, consoleURL+runPath+"/rerun", strings.NewReader(form.Encode()))
		Expect(err).NotTo(HaveOccurred())
		rerunRequest.Header.Set("Authorization", "Bearer "+consoleToken)
		rerunRequest.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		webClient := &http.Client{
			Timeout: 30 * time.Second,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		}
		rerunResponse, err := webClient.Do(rerunRequest)
		Expect(err).NotTo(HaveOccurred())
		Expect(rerunResponse.Body.Close()).To(Succeed())
		Expect(rerunResponse.StatusCode).To(Equal(http.StatusSeeOther))
		location := rerunResponse.Header.Get("Location")
		Expect(location).To(HavePrefix("/runs/" + url.PathEscape(root.Namespace) + "/"))
		rerunName, err := url.PathUnescape(strings.TrimPrefix(location, "/runs/"+url.PathEscape(root.Namespace)+"/"))
		Expect(err).NotTo(HaveOccurred())

		Eventually(func(g Gomega) {
			rerun := &actionsv1alpha1.WorkflowRun{}
			g.Expect(clusterClient.Get(ctx, client.ObjectKey{Namespace: e2eNamespace, Name: rerunName}, rerun)).To(Succeed())
			g.Expect(rerun.Spec.Rerun).NotTo(BeNil())
			if rerun.Spec.Rerun != nil {
				g.Expect(rerun.Spec.Rerun.Attempt).To(Equal(int32(2)))
				g.Expect(rerun.Spec.Rerun.JobIDs).To(ConsistOf("verify"))
			}
			condition := meta.FindStatusCondition(rerun.Status.Conditions, actionsv1alpha1.WorkflowRunConditionSucceeded)
			g.Expect(condition).NotTo(BeNil())
			if condition != nil {
				g.Expect(condition.Status).To(Equal(metav1.ConditionTrue), condition.Message)
			}
			g.Expect(rerun.Status.Jobs).NotTo(BeNil())
			if rerun.Status.Jobs != nil {
				g.Expect(rerun.Status.Jobs.Total).To(Equal(int32(1)))
				g.Expect(rerun.Status.Jobs.Succeeded).To(Equal(int32(1)))
			}
		}, 180*time.Second, time.Second).Should(Succeed())

		rerun := &actionsv1alpha1.WorkflowRun{}
		Expect(clusterClient.Get(ctx, client.ObjectKey{Namespace: e2eNamespace, Name: rerunName}, rerun)).To(Succeed())
		rerunJobs := &actionsv1alpha1.WorkflowJobList{}
		Expect(clusterClient.List(ctx, rerunJobs, client.InNamespace(e2eNamespace), client.MatchingLabels{
			actionsv1alpha1.LabelWorkflowRunUID: string(rerun.UID),
		})).To(Succeed())
		Expect(rerunJobs.Items).To(HaveLen(1))
		Expect(rerunJobs.Items[0].Spec.JobID).To(Equal("verify"))
		Expect(rerunJobs.Items[0].Status.Result).To(Equal(actionsv1alpha1.WorkflowJobResultSuccess))
	})
})
