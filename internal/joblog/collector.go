package joblog

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"time"

	actionsv1alpha1 "github.com/kelos-dev/open-actions/api/v1alpha1"
	"github.com/kelos-dev/open-actions/internal/runner"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
)

type PodSource interface {
	List(context.Context) (*corev1.PodList, error)
	Get(context.Context, string, string) (*corev1.Pod, error)
	Logs(context.Context, *corev1.Pod, *metav1.Time) (io.ReadCloser, error)
}

type KubernetesPods struct{ Client kubernetes.Interface }

func (s KubernetesPods) List(ctx context.Context) (*corev1.PodList, error) {
	return s.Client.CoreV1().Pods("").List(ctx, metav1.ListOptions{LabelSelector: actionsv1alpha1.LabelWorkflowJobUID})
}
func (s KubernetesPods) Get(ctx context.Context, namespace, name string) (*corev1.Pod, error) {
	return s.Client.CoreV1().Pods(namespace).Get(ctx, name, metav1.GetOptions{})
}
func (s KubernetesPods) Logs(ctx context.Context, pod *corev1.Pod, since *metav1.Time) (io.ReadCloser, error) {
	return s.Client.CoreV1().Pods(pod.Namespace).GetLogs(pod.Name, &corev1.PodLogOptions{Container: runner.ContainerName, Follow: true, Timestamps: true, SinceTime: since}).Stream(ctx)
}

type Collector struct {
	source PodSource
	store  *Store
	logger *slog.Logger
}

func NewCollector(source PodSource, store *Store, logger *slog.Logger) (*Collector, error) {
	if source == nil || store == nil || logger == nil {
		return nil, errors.New("job log collector configuration is incomplete")
	}
	return &Collector{source: source, store: store, logger: logger}, nil
}

func (c *Collector) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	var workers sync.WaitGroup
	defer func() { cancel(); workers.Wait() }()
	seen := map[types.UID]bool{}
	retry := make(chan types.UID, 1)
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	cleanup := time.NewTicker(time.Hour)
	defer cleanup.Stop()
	for {
		pods, err := c.source.List(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			c.logger.Error("unable to list runner Pods for log collection", "error", err)
			select {
			case <-ctx.Done():
				return nil
			case <-ticker.C:
				continue
			}
		}
		present := map[types.UID]bool{}
		for _, pod := range pods.Items {
			if !hasRunner(pod) {
				continue
			}
			present[pod.UID] = true
			if seen[pod.UID] {
				continue
			}
			seen[pod.UID] = true
			workers.Add(1)
			go func(pod corev1.Pod) {
				defer workers.Done()
				if err := c.collect(ctx, &pod); err != nil && ctx.Err() == nil {
					c.logger.Error("runner log collection failed", "namespace", pod.Namespace, "pod", pod.Name, "error", err)
					select {
					case retry <- pod.UID:
					case <-ctx.Done():
					}
				}
			}(pod)
		}
		for uid := range seen {
			if !present[uid] {
				delete(seen, uid)
			}
		}
		select {
		case <-ctx.Done():
			cancel()
			return nil
		case <-ticker.C:
		case uid := <-retry:
			delete(seen, uid)
		case <-cleanup.C:
			if err := c.store.Cleanup(); err != nil {
				c.logger.Error("job log cleanup failed", "error", err)
			}
		}
	}
}

func (c *Collector) collect(ctx context.Context, pod *corev1.Pod) error {
	record, err := c.store.Begin(Record{Namespace: pod.Namespace, JobUID: pod.Labels[actionsv1alpha1.LabelWorkflowJobUID], PodUID: string(pod.UID), CreatedAt: pod.CreationTimestamp.Time})
	if err != nil || record.Finished {
		return err
	}
	cursor, err := c.store.Cursor(record)
	if err != nil {
		return err
	}
	for ctx.Err() == nil {
		current, err := c.source.Get(ctx, pod.Namespace, pod.Name)
		if apierrors.IsNotFound(err) || err == nil && current.UID != pod.UID {
			return c.store.Finish(record, false)
		}
		if err == nil && runnerStarted(current) {
			wasTerminated := runnerTerminated(current)
			var since *metav1.Time
			if !cursor.Time.IsZero() {
				value := metav1.NewTime(cursor.Time)
				since = &value
			}
			stream, streamErr := c.source.Logs(ctx, pod, since)
			if streamErr == nil {
				streamErr = c.copy(stream, record, &cursor)
				stream.Close()
			}
			if errors.Is(streamErr, ErrLimit) {
				return c.store.Finish(record, false)
			}
			if streamErr == nil {
				current, err = c.source.Get(ctx, pod.Namespace, pod.Name)
				if apierrors.IsNotFound(err) || err == nil && current.UID != pod.UID {
					return c.store.Finish(record, false)
				}
				if err == nil && wasTerminated {
					return c.store.Finish(record, true)
				}
			}
			if streamErr != nil && ctx.Err() == nil {
				c.logger.Error("runner log stream interrupted", "namespace", pod.Namespace, "pod", pod.Name, "error", streamErr)
			}
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(time.Second):
		}
	}
	return nil
}

func runnerTerminated(pod *corev1.Pod) bool {
	for _, status := range pod.Status.ContainerStatuses {
		if status.Name == runner.ContainerName {
			return status.State.Terminated != nil
		}
	}
	return false
}

func (c *Collector) copy(stream io.Reader, record Record, cursor *Cursor) error {
	reader := bufio.NewReaderSize(stream, 32*1024)
	next := *cursor
	skip := cursor.Count
	var batch bytes.Buffer
	flush := func() error {
		if batch.Len() == 0 {
			return nil
		}
		if err := c.store.Append(record, batch.Bytes()); err != nil {
			return err
		}
		*cursor = next
		batch.Reset()
		return nil
	}
	for {
		line, err := readLogLine(reader)
		if err != nil {
			if flushErr := flush(); flushErr != nil {
				return flushErr
			}
			if err == io.EOF {
				return nil
			}
			return err
		}
		timestamp, _, _ := strings.Cut(string(line), " ")
		parsed, err := time.Parse(time.RFC3339Nano, timestamp)
		if err != nil {
			return fmt.Errorf("invalid Pod log timestamp: %w", err)
		}
		if parsed.Before(next.Time) {
			continue
		}
		if parsed.Equal(next.Time) && skip > 0 {
			skip--
			continue
		}
		if parsed.Equal(next.Time) {
			next.Count++
		} else {
			next = Cursor{Time: parsed, Count: 1}
			skip = 0
		}
		batch.Write(line)
		if batch.Len() >= 32*1024 || reader.Buffered() == 0 {
			if err := flush(); err != nil {
				return err
			}
		}
	}
}

const maxLineBytes = 1024 * 1024

func readLogLine(reader *bufio.Reader) ([]byte, error) {
	var line []byte
	for {
		fragment, err := reader.ReadSlice('\n')
		if len(line)+len(fragment) >= maxLineBytes {
			return nil, ErrLimit
		}
		line = append(line, fragment...)
		if err == bufio.ErrBufferFull {
			continue
		}
		if err == io.EOF && len(line) > 0 {
			return append(line, '\n'), nil
		}
		return line, err
	}
}

func hasRunner(pod corev1.Pod) bool {
	for _, container := range pod.Spec.Containers {
		if container.Name == runner.ContainerName {
			return true
		}
	}
	return false
}

func runnerStarted(pod *corev1.Pod) bool {
	for _, status := range pod.Status.ContainerStatuses {
		if status.Name == runner.ContainerName {
			return status.State.Running != nil || status.State.Terminated != nil
		}
	}
	return false
}
