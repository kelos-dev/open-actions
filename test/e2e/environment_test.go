//go:build e2e

package e2e_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	actionsv1alpha1 "github.com/kelos-dev/open-actions/api/v1alpha1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	clientconfig "sigs.k8s.io/controller-runtime/pkg/client/config"
)

const (
	webhookSecret = "open-actions-e2e-secret"
	fixtureURL    = "http://127.0.0.1:18081"
	webhookURL    = "http://127.0.0.1:18080"
	workflowPath  = ".open-actions/workflows/ci.yaml"
)

var (
	repositoryRoot  string
	fixtureRevision string
	portForwards    []*exec.Cmd
	clusterClient   client.Client
	clientset       kubernetes.Interface
	e2eNamespace    string
	installationID  int64
)

var _ = SynchronizedBeforeSuite(func() []byte {
	var err error
	repositoryRoot, err = filepath.Abs(filepath.Join("..", ".."))
	Expect(err).NotTo(HaveOccurred())

	Expect(kubectl("apply", "-k", "config/e2e")).To(Succeed())
	Expect(kubectl(
		"wait", "--for=condition=Available",
		"deployment/open-actions-controller", "deployment/github-fixture",
		"--namespace", "open-actions-system", "--timeout=180s",
	)).To(Succeed())

	startPortForward("open-actions-system", "service/github-fixture", "18081:8080")
	Eventually(func() error {
		response, err := http.Get(fixtureURL + "/fixture/sha")
		if err != nil {
			return err
		}
		defer response.Body.Close()
		data, err := io.ReadAll(response.Body)
		if err != nil {
			return err
		}
		fixtureRevision = strings.TrimSpace(string(data))
		if response.StatusCode != http.StatusOK || len(fixtureRevision) != 40 {
			return fmt.Errorf("fixture returned status %d and revision %q", response.StatusCode, fixtureRevision)
		}
		return nil
	}, 30*time.Second, 500*time.Millisecond).Should(Succeed())

	startPortForward("open-actions-system", "service/open-actions-webhook", "18080:80")
	Eventually(func() error {
		response, err := http.Get(webhookURL + "/")
		if err != nil {
			return err
		}
		return response.Body.Close()
	}, 30*time.Second, 500*time.Millisecond).Should(Succeed())
	return []byte(fixtureRevision)
}, func(data []byte) {
	var err error
	repositoryRoot, err = filepath.Abs(filepath.Join("..", ".."))
	Expect(err).NotTo(HaveOccurred())
	fixtureRevision = string(data)

	scheme := runtime.NewScheme()
	Expect(clientgoscheme.AddToScheme(scheme)).To(Succeed())
	Expect(actionsv1alpha1.AddToScheme(scheme)).To(Succeed())
	restConfig, err := clientconfig.GetConfig()
	Expect(err).NotTo(HaveOccurred())
	clusterClient, err = client.New(restConfig, client.Options{Scheme: scheme})
	Expect(err).NotTo(HaveOccurred())
	clientset, err = kubernetes.NewForConfig(restConfig)
	Expect(err).NotTo(HaveOccurred())
})

var _ = SynchronizedAfterSuite(func() {}, func() {
	for _, command := range portForwards {
		stop(command)
	}
})

var _ = ReportAfterEach(func(report SpecReport) {
	if !report.Failed() {
		return
	}
	for _, arguments := range [][]string{
		{"get", "actionsgateways,runners,workflowruns,workflowjobs,jobs,pods", "-A", "-o", "wide"},
		{"get", "runners,workflowruns,workflowjobs,jobs,pods", "--namespace", e2eNamespace, "-o", "yaml"},
		{"logs", "--namespace", e2eNamespace, "--all-containers=true", "--prefix=true", "--selector", "actions.kelos.dev/workflow-run-uid"},
		{"logs", "--namespace", "open-actions-system", "deployment/open-actions-controller", "--tail=200"},
		{"get", "events", "-A", "--sort-by=.lastTimestamp"},
	} {
		output, _ := kubectlOutput(arguments...)
		fmt.Fprintf(GinkgoWriter, "\n$ kubectl %s\n%s\n", strings.Join(arguments, " "), output)
	}
})

func setupControlPlane(withRunner bool) {
	ctx := context.Background()
	e2eNamespace = fmt.Sprintf("open-actions-e2e-%d", GinkgoParallelProcess())
	installationID = 67890 + int64(GinkgoParallelProcess())
	namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: e2eNamespace}}
	Expect(client.IgnoreNotFound(clusterClient.Delete(ctx, namespace))).To(Succeed())
	Eventually(func() bool {
		err := clusterClient.Get(ctx, client.ObjectKey{Name: e2eNamespace}, &corev1.Namespace{})
		return apierrors.IsNotFound(err)
	}, 60*time.Second, time.Second).Should(BeTrue())
	Expect(clusterClient.Create(ctx, namespace)).To(Succeed())
	DeferCleanup(func() {
		Expect(client.IgnoreNotFound(clusterClient.Delete(ctx, namespace))).To(Succeed())
		Eventually(func() bool {
			err := clusterClient.Get(ctx, client.ObjectKey{Name: e2eNamespace}, &corev1.Namespace{})
			return apierrors.IsNotFound(err)
		}, 60*time.Second, time.Second).Should(BeTrue())
	})

	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	Expect(err).NotTo(HaveOccurred())
	privateKeyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(privateKey)})
	objects := []client.Object{
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "github-app", Namespace: e2eNamespace},
			Data: map[string][]byte{
				"private-key.pem": privateKeyPEM,
				"webhook-secret":  []byte(webhookSecret),
			},
		},
		&actionsv1alpha1.ActionsGateway{
			ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: e2eNamespace},
			Spec: actionsv1alpha1.ActionsGatewaySpec{
				Source: actionsv1alpha1.ActionsGatewaySource{
					Type: actionsv1alpha1.SourceTypeGitHub,
					GitHub: &actionsv1alpha1.GitHubAppConfiguration{
						AppID:          12345,
						InstallationID: installationID,
						PrivateKeySecretRef: corev1.SecretKeySelector{
							LocalObjectReference: corev1.LocalObjectReference{Name: "github-app"},
							Key:                  "private-key.pem",
						},
						WebhookSecretRef: corev1.SecretKeySelector{
							LocalObjectReference: corev1.LocalObjectReference{Name: "github-app"},
							Key:                  "webhook-secret",
						},
					},
				},
				WorkflowDirectory: ".open-actions/workflows",
			},
		},
	}
	if withRunner {
		runnerImage := os.Getenv("RUNNER_IMAGE")
		if runnerImage == "" {
			runnerImage = "ghcr.io/kelos-dev/open-actions-runner:e2e"
		}
		objects = append(objects, &actionsv1alpha1.Runner{
			ObjectMeta: metav1.ObjectMeta{Name: "runner-1", Namespace: e2eNamespace},
			Spec: actionsv1alpha1.RunnerSpec{
				GatewayRef: corev1.LocalObjectReference{Name: "default"},
				Execution: actionsv1alpha1.RunnerExecutionSpec{
					Image: runnerImage,
					Resources: &actionsv1alpha1.RunnerResources{
						Requests: actionsv1alpha1.RunnerResourceList{
							corev1.ResourceCPU:    resource.MustParse("50m"),
							corev1.ResourceMemory: resource.MustParse("64Mi"),
						},
						Limits: actionsv1alpha1.RunnerResourceList{
							corev1.ResourceCPU:    resource.MustParse("1"),
							corev1.ResourceMemory: resource.MustParse("512Mi"),
						},
					},
				},
				Labels: []string{"self-hosted", "linux", "x64", "ubuntu-latest"},
			},
		})
	}
	for _, object := range objects {
		Expect(clusterClient.Create(ctx, object)).To(Succeed())
	}

	Eventually(func(g Gomega) {
		gateway := &actionsv1alpha1.ActionsGateway{}
		g.Expect(clusterClient.Get(ctx, client.ObjectKey{Namespace: e2eNamespace, Name: "default"}, gateway)).To(Succeed())
		condition := meta.FindStatusCondition(gateway.Status.Conditions, actionsv1alpha1.ActionsGatewayConditionConfigured)
		g.Expect(condition).NotTo(BeNil())
		if condition != nil {
			g.Expect(condition.Status).To(Equal(metav1.ConditionTrue), condition.Message)
		}
	}, 120*time.Second, time.Second).Should(Succeed())
	if withRunner {
		Eventually(func(g Gomega) {
			runnerObject := &actionsv1alpha1.Runner{}
			g.Expect(clusterClient.Get(ctx, client.ObjectKey{Namespace: e2eNamespace, Name: "runner-1"}, runnerObject)).To(Succeed())
			condition := meta.FindStatusCondition(runnerObject.Status.Conditions, actionsv1alpha1.RunnerConditionReady)
			g.Expect(condition).NotTo(BeNil())
			if condition != nil {
				g.Expect(condition.Status).To(Equal(metav1.ConditionTrue), condition.Message)
			}
		}, 120*time.Second, time.Second).Should(Succeed())
	}
}

func startPortForward(namespace, resource, ports string) {
	command := exec.Command("kubectl", "port-forward", "--namespace", namespace, resource, ports)
	command.Dir = repositoryRoot
	command.Stdout = GinkgoWriter
	command.Stderr = GinkgoWriter
	Expect(command.Start()).To(Succeed())
	portForwards = append(portForwards, command)
}

func stop(command *exec.Cmd) {
	if command == nil || command.Process == nil {
		return
	}
	_ = command.Process.Signal(os.Interrupt)
	done := make(chan struct{})
	go func() {
		_ = command.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		_ = command.Process.Kill()
		<-done
	}
}

func kubectl(arguments ...string) error {
	_, err := kubectlOutput(arguments...)
	return err
}

func kubectlOutput(arguments ...string) (string, error) {
	command := exec.Command("kubectl", arguments...)
	command.Dir = repositoryRoot
	output, err := command.CombinedOutput()
	if err != nil {
		return strings.TrimSpace(string(output)), fmt.Errorf("kubectl %s: %w: %s", strings.Join(arguments, " "), err, output)
	}
	return strings.TrimSpace(string(output)), nil
}
