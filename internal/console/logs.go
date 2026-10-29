package console

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/kelos-dev/open-actions/internal/joblog"
	"github.com/kelos-dev/open-actions/internal/runner"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

var errArchiveIncomplete = errors.New("archived logs are incomplete or expired")

const cancelledArchiveWait = runner.CleanupTimeout + joblog.FlushTimeout

func runnerPodActive(pod *corev1.Pod) bool {
	if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
		return false
	}
	for _, container := range pod.Status.ContainerStatuses {
		if container.Name == runner.ContainerName {
			return container.State.Terminated == nil
		}
	}
	return true
}

// LogSource reads runner Pods and their logs.
type LogSource interface {
	ListPods(context.Context, string, string) (*corev1.PodList, error)
	Stream(context.Context, string, string, bool) (io.ReadCloser, error)
}

type kubernetesLogSource struct {
	client  kubernetes.Interface
	archive *joblog.Client
	tokens  *joblog.ReadTokenCodec
}

// NewKubernetesLogSource creates a Pod-log source backed by Kubernetes.
func NewKubernetesLogSource(client kubernetes.Interface) LogSource {
	return &kubernetesLogSource{client: client}
}

func (s *kubernetesLogSource) ListPods(ctx context.Context, namespace, selector string) (*corev1.PodList, error) {
	return s.client.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{LabelSelector: selector})
}

// NewArchivedLogSource reads archived output for jobs configured to upload it.
func NewArchivedLogSource(client kubernetes.Interface, archive *joblog.Client, tokens *joblog.ReadTokenCodec) (LogSource, error) {
	if client == nil || archive == nil || tokens == nil {
		return nil, errors.New("archived log clients and signing key are required")
	}
	return &kubernetesLogSource{client: client, archive: archive, tokens: tokens}, nil
}

func (s *kubernetesLogSource) Stream(ctx context.Context, namespace, name string, follow bool) (io.ReadCloser, error) {
	return s.client.CoreV1().Pods(namespace).GetLogs(name, &corev1.PodLogOptions{Container: runner.ContainerName, Follow: follow, Timestamps: true}).Stream(ctx)
}

func (s *kubernetesLogSource) StreamArchive(ctx context.Context, job string, follow bool, uploadsStopped func(context.Context) (bool, error)) (io.ReadCloser, error) {
	if s.archive == nil {
		return nil, fmt.Errorf("archived logs for WorkflowJob UID %q are not configured", job)
	}
	streamCtx, cancel := context.WithCancel(ctx)
	reader, writer := io.Pipe()
	go func() {
		defer cancel()
		err := s.readArchive(streamCtx, writer, job, follow, uploadsStopped)
		_ = writer.CloseWithError(err)
	}()
	return &archiveStream{PipeReader: reader, cancel: cancel}, nil
}

type archiveStream struct {
	*io.PipeReader
	cancel context.CancelFunc
}

func (s *archiveStream) Close() error { s.cancel(); return s.PipeReader.Close() }

func (s *kubernetesLogSource) readArchive(ctx context.Context, writer io.Writer, job string, follow bool, uploadsStopped func(context.Context) (bool, error)) error {
	var lastStatusCheck time.Time
	finished := false
	for sequence := uint64(0); ; {
		token := s.tokens.Sign(job, time.Now().Add(time.Minute))
		chunk, err := s.archive.Read(ctx, job, sequence, token)
		if errors.Is(err, joblog.ErrNotFound) {
			if time.Since(lastStatusCheck) >= 5*time.Second {
				var getErr error
				finished, getErr = uploadsStopped(ctx)
				if getErr != nil {
					return getErr
				}
				lastStatusCheck = time.Now()
			}
			if finished {
				// The final upload can race with the check for a stopped runner.
				chunk, err = s.archive.Read(ctx, job, sequence, token)
				if errors.Is(err, joblog.ErrNotFound) {
					return fmt.Errorf("%w for WorkflowJob UID %q", errArchiveIncomplete, job)
				}
			} else {
				if !follow {
					return nil
				}
				timer := time.NewTimer(time.Second)
				select {
				case <-ctx.Done():
					timer.Stop()
					return ctx.Err()
				case <-timer.C:
				}
				continue
			}
		}
		if err != nil {
			return err
		}
		if _, err := writer.Write(chunk.Data); err != nil {
			return err
		}
		if chunk.Complete {
			return nil
		}
		sequence++
	}
}
