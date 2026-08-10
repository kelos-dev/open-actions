package console

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	actionsv1alpha1 "github.com/kelos-dev/open-actions/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	sessionCookieName = "open_actions_console_session"
	sessionLifetime   = 7 * 24 * time.Hour
	streamHeartbeat   = 15 * time.Second
)

var errLogsUnavailable = errors.New("logs are no longer available")

// Config contains required Console dependencies.
type Config struct {
	Client       client.Reader
	Logs         LogSource
	Token        string
	SecureCookie bool
	Logger       *slog.Logger
}

// Handler serves authenticated workflow run details and logs.
type Handler struct {
	client       client.Reader
	logs         LogSource
	tokenDigest  [sha256.Size]byte
	sessionValue string
	secureCookie bool
	logger       *slog.Logger
	loginPage    *template.Template
	runPage      *template.Template
	logPage      *template.Template
}

type loginRequest struct {
	Token string `json:"token"`
}

type runPageData struct {
	Repository   string
	WorkflowName string
	WorkflowPath string
	Revision     string
	Status       string
	Jobs         []jobPageData
}

type jobPageData struct {
	ID        string
	Runner    string
	Status    string
	URL       string
	Started   string
	Completed string
}

type logPageData struct {
	JobID     string
	Status    string
	RunURL    string
	StreamURL string
}

type logRead struct {
	data string
	err  error
}

// New creates a Console handler.
func New(config Config) (*Handler, error) {
	if config.Client == nil || config.Logs == nil {
		return nil, errors.New("Console clients are required")
	}
	if strings.TrimSpace(config.Token) == "" {
		return nil, errors.New("Console token is required")
	}
	if config.Logger == nil {
		return nil, errors.New("Console logger is required")
	}
	loginPage, err := template.New("login").Parse(loginPageTemplate)
	if err != nil {
		return nil, err
	}
	runPage, err := template.New("run").Parse(runPageTemplate)
	if err != nil {
		return nil, err
	}
	logPage, err := template.New("logs").Parse(logPageTemplate)
	if err != nil {
		return nil, err
	}
	tokenDigest := sha256.Sum256([]byte(config.Token))
	return &Handler{
		client: config.Client, logs: config.Logs, tokenDigest: tokenDigest,
		sessionValue: sessionValue(config.Token), secureCookie: config.SecureCookie, logger: config.Logger,
		loginPage: loginPage, runPage: runPage, logPage: logPage,
	}, nil
}

// ServeHTTP routes authenticated Console requests and token login requests.
func (h *Handler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	writer.Header().Set("Cache-Control", "no-store")
	writer.Header().Set("Referrer-Policy", "no-referrer")
	writer.Header().Set("X-Content-Type-Options", "nosniff")
	if request.URL.Path == "/login" {
		h.serveLoginPage(writer, request)
		return
	}
	if request.URL.Path == "/api/login" {
		h.login(writer, request)
		return
	}
	if request.Method != http.MethodGet {
		writer.Header().Set("Allow", http.MethodGet)
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !h.authenticated(request) {
		http.Redirect(writer, request, "/login?next="+url.QueryEscape(request.URL.RequestURI()), http.StatusFound)
		return
	}
	parts := splitPath(request.URL.Path)
	if len(parts) < 3 || parts[0] != "runs" {
		http.NotFound(writer, request)
		return
	}
	run, err := h.resolveRun(request.Context(), parts[1], parts[2])
	if err != nil {
		h.writeResolutionError(writer, request, err)
		return
	}
	switch {
	case len(parts) == 3:
		h.runDetails(writer, request, run)
	case len(parts) == 5 && parts[3] == "jobs":
		h.jobLogs(writer, request, run, parts[4])
	case len(parts) == 6 && parts[3] == "jobs" && parts[5] == "stream":
		h.streamJobLogs(writer, request, run, parts[4])
	default:
		http.NotFound(writer, request)
	}
}

func (h *Handler) resolveRun(ctx context.Context, namespace, name string) (*actionsv1alpha1.WorkflowRun, error) {
	run := &actionsv1alpha1.WorkflowRun{}
	if err := h.client.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, run); err != nil {
		return nil, err
	}
	if run.Spec.Source.Type != actionsv1alpha1.SourceTypeGitHub || run.Spec.Source.GitHub == nil {
		return nil, errors.New("workflow source is not supported by the Console")
	}
	return run, nil
}

func (h *Handler) serveLoginPage(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		writer.Header().Set("Allow", http.MethodGet)
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if h.authenticated(request) {
		if next := safeNext(request.URL.Query().Get("next")); next != "" {
			http.Redirect(writer, request, next, http.StatusFound)
			return
		}
	}
	h.writeHTML(writer, h.loginPage, nil)
}

func (h *Handler) login(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		writer.Header().Set("Allow", http.MethodPost)
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var submitted loginRequest
	decoder := json.NewDecoder(io.LimitReader(request.Body, 1<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&submitted); err != nil || !h.validToken(submitted.Token) {
		http.Error(writer, "invalid token", http.StatusUnauthorized)
		return
	}
	http.SetCookie(writer, &http.Cookie{
		Name: sessionCookieName, Value: h.sessionValue, Path: "/", MaxAge: int(sessionLifetime.Seconds()),
		HttpOnly: true, Secure: h.secureCookie, SameSite: http.SameSiteLaxMode,
	})
	writer.Header().Set("Content-Type", "application/json")
	_, _ = writer.Write([]byte(`{"authenticated":true}`))
}

func (h *Handler) authenticated(request *http.Request) bool {
	if authorization := request.Header.Get("Authorization"); strings.HasPrefix(authorization, "Bearer ") {
		return h.validToken(strings.TrimPrefix(authorization, "Bearer "))
	}
	cookie, err := request.Cookie(sessionCookieName)
	return err == nil && hmac.Equal([]byte(cookie.Value), []byte(h.sessionValue))
}

func (h *Handler) validToken(token string) bool {
	digest := sha256.Sum256([]byte(token))
	return hmac.Equal(digest[:], h.tokenDigest[:])
}

func (h *Handler) runDetails(writer http.ResponseWriter, request *http.Request, run *actionsv1alpha1.WorkflowRun) {
	jobs := &actionsv1alpha1.WorkflowJobList{}
	if err := h.client.List(request.Context(), jobs, client.InNamespace(run.Namespace), client.MatchingLabels{actionsv1alpha1.LabelWorkflowRunUID: string(run.UID)}); err != nil {
		http.Error(writer, "load workflow jobs", http.StatusInternalServerError)
		return
	}
	sort.Slice(jobs.Items, func(left, right int) bool { return jobs.Items[left].Spec.JobID < jobs.Items[right].Spec.JobID })
	data := runPageData{
		Repository:   run.Spec.Source.GitHub.Repository.Owner + "/" + run.Spec.Source.GitHub.Repository.Name,
		WorkflowName: run.Status.WorkflowName,
		WorkflowPath: run.Spec.WorkflowPath,
		Revision:     run.Spec.Source.GitHub.Revision.SHA,
		Status:       workflowRunStatus(run),
	}
	if data.WorkflowName == "" {
		data.WorkflowName = data.WorkflowPath
	}
	for index := range jobs.Items {
		job := &jobs.Items[index]
		item := jobPageData{ID: job.Spec.JobID, Status: workflowJobStatus(job), URL: runPath(run) + "/jobs/" + url.PathEscape(job.Name)}
		if job.Status.RunnerRef != nil {
			item.Runner = job.Status.RunnerRef.Name
		}
		if job.Status.StartTime != nil {
			item.Started = job.Status.StartTime.UTC().Format(time.RFC3339)
		}
		if job.Status.CompletionTime != nil {
			item.Completed = job.Status.CompletionTime.UTC().Format(time.RFC3339)
		}
		data.Jobs = append(data.Jobs, item)
	}
	h.writeHTML(writer, h.runPage, data)
}

func (h *Handler) jobLogs(writer http.ResponseWriter, request *http.Request, run *actionsv1alpha1.WorkflowRun, jobName string) {
	job, err := h.workflowJob(request.Context(), run, jobName)
	if err != nil {
		h.writeResolutionError(writer, request, err)
		return
	}
	path := runPath(run)
	h.writeHTML(writer, h.logPage, logPageData{
		JobID: job.Spec.JobID, Status: workflowJobStatus(job), RunURL: path,
		StreamURL: path + "/jobs/" + url.PathEscape(job.Name) + "/stream",
	})
}

func (h *Handler) streamJobLogs(writer http.ResponseWriter, request *http.Request, run *actionsv1alpha1.WorkflowRun, jobName string) {
	job, err := h.workflowJob(request.Context(), run, jobName)
	if err != nil {
		h.writeResolutionError(writer, request, err)
		return
	}
	flusher, ok := writer.(http.Flusher)
	if !ok {
		http.Error(writer, "streaming is unsupported", http.StatusInternalServerError)
		return
	}
	writer.Header().Set("Content-Type", "text/event-stream")
	writer.Header().Set("Cache-Control", "no-store")
	writer.Header().Set("X-Accel-Buffering", "no")
	writeEvent(writer, flusher, "status", "Waiting for runner logs...")
	pod, err := h.waitForPod(request.Context(), job, func() { writeSSEComment(writer, flusher, "keepalive") })
	if err != nil {
		writeEvent(writer, flusher, "error", err.Error())
		return
	}
	stream, err := h.logs.Stream(request.Context(), pod.Namespace, pod.Name)
	if err != nil {
		writeEvent(writer, flusher, "error", "Unable to open runner logs.")
		return
	}
	defer stream.Close()
	reads := readLogStream(request.Context(), stream)
	heartbeat := time.NewTicker(streamHeartbeat)
	defer heartbeat.Stop()
	for {
		select {
		case result, ok := <-reads:
			if !ok {
				return
			}
			if result.data != "" {
				writeEvent(writer, flusher, "log", result.data)
			}
			if result.err == nil {
				continue
			}
			if result.err == io.EOF {
				writeEvent(writer, flusher, "end", "Log stream ended.")
			} else if request.Context().Err() == nil {
				writeEvent(writer, flusher, "error", "Runner log stream failed.")
			}
			return
		case <-heartbeat.C:
			writeSSEComment(writer, flusher, "keepalive")
		case <-request.Context().Done():
			return
		}
	}
}

func readLogStream(ctx context.Context, stream io.Reader) <-chan logRead {
	results := make(chan logRead)
	go func() {
		defer close(results)
		buffer := make([]byte, 32*1024)
		for {
			count, err := stream.Read(buffer)
			result := logRead{data: string(buffer[:count]), err: err}
			select {
			case results <- result:
			case <-ctx.Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()
	return results
}

func (h *Handler) workflowJob(ctx context.Context, run *actionsv1alpha1.WorkflowRun, name string) (*actionsv1alpha1.WorkflowJob, error) {
	job := &actionsv1alpha1.WorkflowJob{}
	if err := h.client.Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: name}, job); err != nil {
		return nil, err
	}
	if job.Labels[actionsv1alpha1.LabelWorkflowRunUID] != string(run.UID) || !metav1.IsControlledBy(job, run) {
		return nil, errors.New("workflow job does not belong to this run")
	}
	return job, nil
}

func (h *Handler) waitForPod(ctx context.Context, job *actionsv1alpha1.WorkflowJob, heartbeat func()) (*corev1.Pod, error) {
	poll := time.NewTicker(time.Second)
	defer poll.Stop()
	heartbeatTicker := time.NewTicker(streamHeartbeat)
	defer heartbeatTicker.Stop()
	selector := labels.Set{actionsv1alpha1.LabelWorkflowJobUID: string(job.UID)}.String()
	for {
		pods, err := h.logs.ListPods(ctx, job.Namespace, selector)
		if err != nil {
			return nil, err
		}
		if len(pods.Items) > 0 {
			sort.Slice(pods.Items, func(left, right int) bool {
				return pods.Items[left].CreationTimestamp.Before(&pods.Items[right].CreationTimestamp)
			})
			return &pods.Items[0], nil
		}
		current := &actionsv1alpha1.WorkflowJob{}
		if err := h.client.Get(ctx, client.ObjectKeyFromObject(job), current); err != nil {
			return nil, err
		}
		condition := meta.FindStatusCondition(current.Status.Conditions, actionsv1alpha1.WorkflowJobConditionSucceeded)
		if condition != nil && condition.Status != metav1.ConditionUnknown {
			return nil, errLogsUnavailable
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-poll.C:
		case <-heartbeatTicker.C:
			heartbeat()
		}
	}
}

func workflowRunStatus(run *actionsv1alpha1.WorkflowRun) string {
	condition := meta.FindStatusCondition(run.Status.Conditions, actionsv1alpha1.WorkflowRunConditionSucceeded)
	if condition == nil {
		return "Queued"
	}
	switch condition.Status {
	case metav1.ConditionTrue:
		return "Succeeded"
	case metav1.ConditionFalse:
		return "Failed"
	default:
		if run.Status.StartTime != nil {
			return "Running"
		}
		return "Queued"
	}
}

func workflowJobStatus(job *actionsv1alpha1.WorkflowJob) string {
	condition := meta.FindStatusCondition(job.Status.Conditions, actionsv1alpha1.WorkflowJobConditionSucceeded)
	if condition != nil {
		switch condition.Status {
		case metav1.ConditionTrue:
			return "Succeeded"
		case metav1.ConditionFalse:
			return "Failed"
		}
	}
	if job.Status.RunnerRef != nil {
		return "Running"
	}
	return "Queued"
}

func (h *Handler) writeHTML(writer http.ResponseWriter, page *template.Template, data any) {
	writer.Header().Set("Content-Type", "text/html; charset=utf-8")
	writer.Header().Set("Content-Security-Policy", "default-src 'none'; style-src 'unsafe-inline'; script-src 'unsafe-inline'; connect-src 'self'; frame-ancestors 'none'; form-action 'self'")
	if err := page.Execute(writer, data); err != nil {
		h.logger.Error("failed to render Console page", "error", err)
	}
}

func (h *Handler) writeResolutionError(writer http.ResponseWriter, request *http.Request, err error) {
	h.logger.Warn("Console request failed", "path", request.URL.Path, "error", err)
	if client.IgnoreNotFound(err) == nil {
		http.NotFound(writer, request)
		return
	}
	http.Error(writer, "Console request failed", http.StatusServiceUnavailable)
}

func runPath(run *actionsv1alpha1.WorkflowRun) string {
	return "/runs/" + url.PathEscape(run.Namespace) + "/" + url.PathEscape(run.Name)
}

func splitPath(value string) []string {
	trimmed := strings.Trim(value, "/")
	if trimmed == "" {
		return nil
	}
	return strings.Split(trimmed, "/")
}

func safeNext(value string) string {
	parsed, err := url.Parse(value)
	if err != nil || parsed.IsAbs() || parsed.Host != "" || !strings.HasPrefix(parsed.Path, "/runs/") {
		return ""
	}
	return value
}

func sessionValue(token string) string {
	mac := hmac.New(sha256.New, []byte(token))
	_, _ = mac.Write([]byte("open-actions/console/session"))
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

func writeEvent(writer io.Writer, flusher http.Flusher, event, value string) {
	encoded, _ := json.Marshal(value)
	_, _ = fmt.Fprintf(writer, "event: %s\ndata: %s\n\n", event, encoded)
	flusher.Flush()
}

func writeSSEComment(writer io.Writer, flusher http.Flusher, value string) {
	_, _ = fmt.Fprintf(writer, ": %s\n\n", value)
	flusher.Flush()
}
