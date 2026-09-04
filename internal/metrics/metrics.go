package metrics

import (
	"fmt"
	"strings"
	"time"

	actionsv1alpha1 "github.com/kelos-dev/open-actions/api/v1alpha1"
	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var (
	queueBuckets = []float64{0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600, 1800, 3600}
	runBuckets   = []float64{1, 5, 15, 30, 60, 120, 300, 600, 900, 1800, 3600, 7200, 21600, 86400}
)

// DurationRecorder records completed Open Actions lifecycle intervals.
type DurationRecorder interface {
	WorkflowRunCompleted(previous *actionsv1alpha1.WorkflowRunStatus, run *actionsv1alpha1.WorkflowRun)
	WorkflowJobScheduled(job *actionsv1alpha1.WorkflowJob, queuedAt time.Time)
	WorkflowJobUpdated(previous *actionsv1alpha1.WorkflowJobStatus, job *actionsv1alpha1.WorkflowJob)
	WebhookRequest(event, result string, duration time.Duration)
	WebhookDelivery(namespace, project, event, result string, duration time.Duration)
}

// Recorder publishes Open Actions duration histograms to Prometheus.
type Recorder struct {
	workflowRunDuration      *prometheus.HistogramVec
	workflowJobQueueDuration *prometheus.HistogramVec
	workflowJobStartDuration *prometheus.HistogramVec
	workflowJobRunDuration   *prometheus.HistogramVec
	webhookRequestDuration   *prometheus.HistogramVec
	webhookDeliveryDuration  *prometheus.HistogramVec
}

// New registers and returns the Open Actions duration metrics.
func New(registerer prometheus.Registerer) (*Recorder, error) {
	if registerer == nil {
		return nil, fmt.Errorf("metrics registerer must be specified")
	}
	recorder := &Recorder{
		workflowRunDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "open_actions",
			Subsystem: "workflow_run",
			Name:      "duration_seconds",
			Help:      "Time from the first workflow job starting until the workflow run completes",
			Buckets:   runBuckets,
		}, []string{"namespace", "project", "conclusion"}),
		workflowJobQueueDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "open_actions",
			Subsystem: "workflow_job",
			Name:      "queue_duration_seconds",
			Help:      "Time from workflow job readiness until Runner assignment",
			Buckets:   queueBuckets,
		}, []string{"namespace", "project"}),
		workflowJobStartDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "open_actions",
			Subsystem: "workflow_job",
			Name:      "startup_duration_seconds",
			Help:      "Time from Runner assignment until the native Job starts",
			Buckets:   queueBuckets,
		}, []string{"namespace", "project"}),
		workflowJobRunDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "open_actions",
			Subsystem: "workflow_job",
			Name:      "execution_duration_seconds",
			Help:      "Time from the native Job starting until the workflow job completes",
			Buckets:   runBuckets,
		}, []string{"namespace", "project", "conclusion"}),
		webhookRequestDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "open_actions",
			Subsystem: "webhook",
			Name:      "request_duration_seconds",
			Help:      "Time spent handling a GitHub webhook HTTP request",
			Buckets:   queueBuckets,
		}, []string{"event", "result"}),
		webhookDeliveryDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "open_actions",
			Subsystem: "webhook",
			Name:      "delivery_duration_seconds",
			Help:      "Time from queuing a GitHub webhook delivery until asynchronous discovery completes",
			Buckets:   runBuckets,
		}, []string{"namespace", "project", "event", "result"}),
	}
	collectors := []prometheus.Collector{
		recorder.workflowRunDuration,
		recorder.workflowJobQueueDuration,
		recorder.workflowJobStartDuration,
		recorder.workflowJobRunDuration,
		recorder.webhookRequestDuration,
		recorder.webhookDeliveryDuration,
	}
	for _, collector := range collectors {
		if err := registerer.Register(collector); err != nil {
			return nil, fmt.Errorf("register duration metric: %w", err)
		}
	}
	return recorder, nil
}

// WorkflowRunCompleted records the active duration when a run first completes.
func (r *Recorder) WorkflowRunCompleted(previous *actionsv1alpha1.WorkflowRunStatus, run *actionsv1alpha1.WorkflowRun) {
	if previous.CompletionTime != nil || run.Status.StartTime == nil || run.Status.CompletionTime == nil {
		return
	}
	duration, valid := elapsed(run.Status.StartTime.Time, run.Status.CompletionTime.Time)
	if !valid {
		return
	}
	r.workflowRunDuration.WithLabelValues(
		label(run.Namespace), label(run.Spec.ProjectRef.Name), workflowRunConclusion(run),
	).Observe(duration.Seconds())
}

// WorkflowJobScheduled records how long a job waited for Runner assignment.
func (r *Recorder) WorkflowJobScheduled(job *actionsv1alpha1.WorkflowJob, queuedAt time.Time) {
	scheduled := meta.FindStatusCondition(job.Status.Conditions, actionsv1alpha1.WorkflowJobConditionScheduled)
	if scheduled == nil || scheduled.Status != metav1.ConditionTrue {
		return
	}
	duration, valid := elapsed(queuedAt, scheduled.LastTransitionTime.Time)
	if !valid {
		return
	}
	r.workflowJobQueueDuration.WithLabelValues(jobLabels(job)...).Observe(duration.Seconds())
}

// WorkflowJobUpdated records startup and execution intervals on their first completed transition.
func (r *Recorder) WorkflowJobUpdated(previous *actionsv1alpha1.WorkflowJobStatus, job *actionsv1alpha1.WorkflowJob) {
	if previous.StartTime == nil && job.Status.StartTime != nil {
		scheduled := meta.FindStatusCondition(job.Status.Conditions, actionsv1alpha1.WorkflowJobConditionScheduled)
		if scheduled != nil && scheduled.Status == metav1.ConditionTrue {
			if duration, valid := elapsed(scheduled.LastTransitionTime.Time, job.Status.StartTime.Time); valid {
				r.workflowJobStartDuration.WithLabelValues(jobLabels(job)...).Observe(duration.Seconds())
			}
		}
	}
	if previous.CompletionTime == nil && job.Status.StartTime != nil && job.Status.CompletionTime != nil {
		if duration, valid := elapsed(job.Status.StartTime.Time, job.Status.CompletionTime.Time); valid {
			labels := append(jobLabels(job), workflowJobConclusion(job))
			r.workflowJobRunDuration.WithLabelValues(labels...).Observe(duration.Seconds())
		}
	}
}

// WebhookRequest records one completed webhook HTTP request.
func (r *Recorder) WebhookRequest(event, result string, duration time.Duration) {
	r.webhookRequestDuration.WithLabelValues(webhookEvent(event), label(result)).Observe(nonNegative(duration).Seconds())
}

// WebhookDelivery records one completed asynchronous webhook delivery.
func (r *Recorder) WebhookDelivery(namespace, project, event, result string, duration time.Duration) {
	r.webhookDeliveryDuration.WithLabelValues(
		label(namespace), label(project), webhookEvent(event), label(result),
	).Observe(nonNegative(duration).Seconds())
}

func workflowRunConclusion(run *actionsv1alpha1.WorkflowRun) string {
	condition := meta.FindStatusCondition(run.Status.Conditions, actionsv1alpha1.WorkflowRunConditionSucceeded)
	if condition == nil {
		return "unknown"
	}
	if condition.Status == metav1.ConditionTrue {
		return "success"
	}
	switch condition.Reason {
	case "JobCancelled", "RevisionSuperseded":
		return "cancelled"
	case "JobTimedOut":
		return "timed_out"
	default:
		return "failure"
	}
}

func workflowJobConclusion(job *actionsv1alpha1.WorkflowJob) string {
	condition := meta.FindStatusCondition(job.Status.Conditions, actionsv1alpha1.WorkflowJobConditionSucceeded)
	if condition != nil && condition.Reason == "JobTimedOut" {
		return "timed_out"
	}
	if job.Status.Result == actionsv1alpha1.WorkflowJobResultCancelled {
		return "cancelled"
	}
	if condition != nil {
		switch condition.Status {
		case metav1.ConditionTrue:
			return "success"
		case metav1.ConditionFalse:
			return "failure"
		}
	}
	switch job.Status.Result {
	case actionsv1alpha1.WorkflowJobResultSuccess:
		return "success"
	case actionsv1alpha1.WorkflowJobResultFailure:
		return "failure"
	case actionsv1alpha1.WorkflowJobResultSkipped:
		return "skipped"
	default:
		return "unknown"
	}
}

func jobLabels(job *actionsv1alpha1.WorkflowJob) []string {
	return []string{label(job.Namespace), label(job.Annotations[actionsv1alpha1.AnnotationProjectName])}
}

func webhookEvent(event string) string {
	switch actionsv1alpha1.GitHubEventName(event) {
	case actionsv1alpha1.GitHubEventNamePush,
		actionsv1alpha1.GitHubEventNamePullRequest,
		actionsv1alpha1.GitHubEventNameMergeGroup,
		actionsv1alpha1.GitHubEventNameWorkflowRun,
		actionsv1alpha1.GitHubEventNameWorkflowDispatch,
		actionsv1alpha1.GitHubEventNameIssues,
		actionsv1alpha1.GitHubEventNamePullRequestTarget,
		actionsv1alpha1.GitHubEventNameIssueComment,
		actionsv1alpha1.GitHubEventNamePullRequestReviewComment,
		actionsv1alpha1.GitHubEventNamePullRequestReview,
		actionsv1alpha1.GitHubEventNameSchedule,
		actionsv1alpha1.GitHubEventNameRelease:
		return event
	}
	return "unknown"
}

func elapsed(start, completion time.Time) (time.Duration, bool) {
	if start.IsZero() || completion.IsZero() || completion.Before(start) {
		return 0, false
	}
	return completion.Sub(start), true
}

func nonNegative(duration time.Duration) time.Duration {
	if duration < 0 {
		return 0
	}
	return duration
}

func label(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "unknown"
	}
	return value
}
