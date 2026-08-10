package console

import (
	"context"
	"io"

	"github.com/kelos-dev/open-actions/internal/runner"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// LogSource reads runner Pods and their logs.
type LogSource interface {
	ListPods(context.Context, string, string) (*corev1.PodList, error)
	Stream(context.Context, string, string) (io.ReadCloser, error)
}

type kubernetesLogSource struct {
	client kubernetes.Interface
}

// NewKubernetesLogSource creates a Pod-log source backed by Kubernetes.
func NewKubernetesLogSource(client kubernetes.Interface) LogSource {
	return &kubernetesLogSource{client: client}
}

func (s *kubernetesLogSource) ListPods(ctx context.Context, namespace, selector string) (*corev1.PodList, error) {
	return s.client.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{LabelSelector: selector})
}

func (s *kubernetesLogSource) Stream(ctx context.Context, namespace, name string) (io.ReadCloser, error) {
	return s.client.CoreV1().Pods(namespace).GetLogs(name, &corev1.PodLogOptions{Container: runner.ContainerName, Follow: true}).Stream(ctx)
}
