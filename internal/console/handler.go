package console

import (
	"bufio"
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
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	actionsv1alpha1 "github.com/kelos-dev/open-actions/api/v1alpha1"
	"github.com/kelos-dev/open-actions/internal/projectvalue"
	"github.com/kelos-dev/open-actions/internal/workflowstatus"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	sessionCookieName  = "open_actions_console_session"
	sessionLifetime    = 7 * 24 * time.Hour
	streamHeartbeat    = 15 * time.Second
	maxLogLineBytes    = 256 << 10
	truncatedLogMarker = " … [log line truncated]"
	mainPageRunLimit   = 100
	secretRequestSize  = 3*projectvalue.MaxValueBytes + (8 << 10)
)

var errLogsUnavailable = errors.New("logs are no longer available")

type secretCountLimitError struct {
	name string
}

func (e *secretCountLimitError) Error() string {
	return fmt.Sprintf("Secret %q already contains %d values", e.name, projectvalue.MaxSecretCount)
}

// Config contains required Console dependencies.
type Config struct {
	Client                    client.Client
	Logs                      LogSource
	Token                     string
	SecretManagementNamespace string
	SecureCookie              bool
	Logger                    *slog.Logger
}

// Handler serves an authenticated workflow run overview, details, and logs.
type Handler struct {
	client                    client.Client
	logs                      LogSource
	tokenDigest               [sha256.Size]byte
	sessionValue              string
	csrfToken                 string
	secretManagementNamespace string
	secureCookie              bool
	logger                    *slog.Logger
	loginPage                 *template.Template
	mainPage                  *template.Template
	projectsPage              *template.Template
	projectPage               *template.Template
	runPage                   *template.Template
	logPage                   *template.Template
}

type loginRequest struct {
	Token string `json:"token"`
}

type mainPageData struct {
	Runs      []mainRunPageData
	Limit     int
	Truncated bool
}

type mainRunPageData struct {
	URL           string
	Repository    string
	WorkflowName  string
	Namespace     string
	Project       string
	Event         string
	RefName       string
	Revision      string
	ShortRevision string
	Status        string
	StatusClass   string
	Created       string
	Duration      string
}

type projectsPageData struct {
	Projects []projectListItem
}

type projectListItem struct {
	URL          string
	Name         string
	Namespace    string
	Installation int64
	Status       string
	StatusClass  string
	SecretName   string
}

type projectPageData struct {
	Name              string
	Namespace         string
	SecretsURL        string
	Installation      int64
	Status            string
	StatusClass       string
	StatusMessage     string
	WorkflowDirectory string
	SecretName        string
	SecretNames       []string
	SecretMissing     bool
	CanManageSecrets  bool
	ManagementNotice  string
	CSRFToken         string
}

type runPageData struct {
	RunURL        string
	Repository    string
	WorkflowName  string
	WorkflowPath  string
	Revision      string
	ShortRevision string
	RefName       string
	Status        string
	StatusClass   string
	Started       string
	Duration      string
	Jobs          []jobPageData
}

type jobPageData struct {
	ID          string
	DisplayName string
	Runner      string
	Status      string
	StatusClass string
	URL         string
	Started     string
	Duration    string
	Selected    bool
}

type logPageData struct {
	Repository    string
	WorkflowName  string
	ShortRevision string
	Status        string
	JobName       string
	Runner        string
	Duration      string
	RunURL        string
	StreamURL     string
	Jobs          []jobPageData
}

type logRead struct {
	entry *logEntry
	err   error
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
	mainPage, err := template.New("main").Parse(mainPageTemplate)
	if err != nil {
		return nil, err
	}
	projectsPage, err := template.New("projects").Parse(projectsPageTemplate)
	if err != nil {
		return nil, err
	}
	projectPage, err := template.New("project").Parse(projectPageTemplate)
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
		sessionValue: sessionValue(config.Token), csrfToken: csrfValue(config.Token), secretManagementNamespace: config.SecretManagementNamespace,
		secureCookie: config.SecureCookie, logger: config.Logger,
		loginPage: loginPage, mainPage: mainPage, projectsPage: projectsPage, projectPage: projectPage, runPage: runPage, logPage: logPage,
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
	if !h.authenticated(request) {
		http.Redirect(writer, request, "/login?next="+url.QueryEscape(request.URL.RequestURI()), http.StatusFound)
		return
	}
	parts := splitPath(request.URL.Path)
	if request.Method == http.MethodPost && len(parts) == 4 && parts[0] == "projects" && parts[3] == "secrets" {
		h.updateProjectSecret(writer, request, parts[1], parts[2])
		return
	}
	if request.Method != http.MethodGet {
		writer.Header().Set("Allow", http.MethodGet)
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if request.URL.Path == "/" {
		h.main(writer, request)
		return
	}
	if request.URL.Path == "/projects" {
		h.projects(writer, request)
		return
	}
	if len(parts) == 3 && parts[0] == "projects" {
		h.projectDetails(writer, request, parts[1], parts[2])
		return
	}
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
		next := safeNext(request.URL.Query().Get("next"))
		if next == "" {
			next = "/"
		}
		http.Redirect(writer, request, next, http.StatusFound)
		return
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

func (h *Handler) validCSRF(token string) bool {
	return hmac.Equal([]byte(token), []byte(h.csrfToken))
}

func (h *Handler) main(writer http.ResponseWriter, request *http.Request) {
	data, err := h.loadMainPageData(request.Context())
	if err != nil {
		h.writeResolutionError(writer, request, err)
		return
	}
	h.writeHTML(writer, h.mainPage, data)
}

func (h *Handler) loadMainPageData(ctx context.Context) (mainPageData, error) {
	runs := &actionsv1alpha1.WorkflowRunList{}
	if err := h.client.List(ctx, runs); err != nil {
		return mainPageData{}, fmt.Errorf("load workflow runs: %w", err)
	}
	sort.SliceStable(runs.Items, func(left, right int) bool {
		leftRun := &runs.Items[left]
		rightRun := &runs.Items[right]
		if leftRun.CreationTimestamp.Equal(&rightRun.CreationTimestamp) {
			if leftRun.Namespace == rightRun.Namespace {
				return leftRun.Name < rightRun.Name
			}
			return leftRun.Namespace < rightRun.Namespace
		}
		return leftRun.CreationTimestamp.After(rightRun.CreationTimestamp.Time)
	})
	data := mainPageData{Limit: mainPageRunLimit}
	for index := range runs.Items {
		run := &runs.Items[index]
		if run.Spec.Source.Type != actionsv1alpha1.SourceTypeGitHub || run.Spec.Source.GitHub == nil {
			continue
		}
		if len(data.Runs) == mainPageRunLimit {
			data.Truncated = true
			break
		}
		workflowName := run.Status.WorkflowName
		if workflowName == "" {
			workflowName = run.Spec.WorkflowPath
		}
		status := workflowstatus.Run(run)
		refName := run.Spec.Source.GitHub.Revision.HeadRef
		if refName == "" {
			refName = shortRef(run.Spec.Source.GitHub.Revision.Ref)
		}
		item := mainRunPageData{
			URL: runPath(run), Repository: run.Spec.Source.GitHub.Repository.Owner + "/" + run.Spec.Source.GitHub.Repository.Name,
			WorkflowName: workflowName, Namespace: run.Namespace, Project: run.Spec.ProjectRef.Name,
			Event: strings.ReplaceAll(string(run.Spec.Source.GitHub.Event.Name), "_", " "), RefName: refName,
			Revision: run.Spec.Source.GitHub.Revision.SHA, ShortRevision: shortRevision(run.Spec.Source.GitHub.Revision.SHA),
			Status: status, StatusClass: statusClass(status), Duration: elapsedTime(run.Status.StartTime, run.Status.CompletionTime),
		}
		if !run.CreationTimestamp.IsZero() {
			item.Created = run.CreationTimestamp.UTC().Format(time.RFC3339)
		}
		data.Runs = append(data.Runs, item)
	}
	return data, nil
}

func (h *Handler) projects(writer http.ResponseWriter, request *http.Request) {
	projects := &actionsv1alpha1.ProjectList{}
	if err := h.client.List(request.Context(), projects); err != nil {
		h.writeResolutionError(writer, request, fmt.Errorf("load Projects: %w", err))
		return
	}
	sort.Slice(projects.Items, func(left, right int) bool {
		if projects.Items[left].Namespace == projects.Items[right].Namespace {
			return projects.Items[left].Name < projects.Items[right].Name
		}
		return projects.Items[left].Namespace < projects.Items[right].Namespace
	})
	data := projectsPageData{Projects: make([]projectListItem, 0, len(projects.Items))}
	for index := range projects.Items {
		project := &projects.Items[index]
		status, statusClass, _ := projectConfigurationStatus(project)
		item := projectListItem{
			URL:  "/projects/" + url.PathEscape(project.Namespace) + "/" + url.PathEscape(project.Name),
			Name: project.Name, Namespace: project.Namespace, Status: status, StatusClass: statusClass,
		}
		if project.Spec.Source.GitHub != nil {
			item.Installation = project.Spec.Source.GitHub.InstallationID
		}
		if project.Spec.Secrets != nil {
			item.SecretName = project.Spec.Secrets.SecretRef.Name
		}
		data.Projects = append(data.Projects, item)
	}
	h.writeHTML(writer, h.projectsPage, data)
}

func (h *Handler) projectDetails(writer http.ResponseWriter, request *http.Request, namespace, name string) {
	project := &actionsv1alpha1.Project{}
	if err := h.client.Get(request.Context(), types.NamespacedName{Namespace: namespace, Name: name}, project); err != nil {
		h.writeResolutionError(writer, request, err)
		return
	}
	status, statusClass, statusMessage := projectConfigurationStatus(project)
	data := projectPageData{
		Name: project.Name, Namespace: project.Namespace, Status: status, StatusClass: statusClass, StatusMessage: statusMessage,
		SecretsURL:        "/projects/" + url.PathEscape(project.Namespace) + "/" + url.PathEscape(project.Name) + "/secrets",
		WorkflowDirectory: project.Spec.WorkflowDirectory, CSRFToken: h.csrfToken,
	}
	if project.Spec.Source.GitHub != nil {
		data.Installation = project.Spec.Source.GitHub.InstallationID
	}
	if data.WorkflowDirectory == "" {
		data.WorkflowDirectory = ".open-actions/workflows"
	}
	if project.Spec.Secrets == nil {
		data.ManagementNotice = "Configure spec.secrets.secretRef before managing workflow secrets."
		h.writeHTML(writer, h.projectPage, data)
		return
	}
	data.SecretName = project.Spec.Secrets.SecretRef.Name
	if project.Namespace != h.secretManagementNamespace {
		data.ManagementNotice = "Secret management is not enabled for this Project namespace."
		h.writeHTML(writer, h.projectPage, data)
		return
	}
	data.CanManageSecrets = true
	secret := &corev1.Secret{}
	if err := h.client.Get(request.Context(), types.NamespacedName{Namespace: project.Namespace, Name: data.SecretName}, secret); err != nil {
		if apierrors.IsNotFound(err) {
			data.SecretMissing = true
			h.writeHTML(writer, h.projectPage, data)
			return
		}
		h.writeResolutionError(writer, request, fmt.Errorf("load Project %q Secret %q: %w", project.Name, data.SecretName, err))
		return
	}
	data.SecretNames = sortedSecretDataNames(secret.Data)
	h.writeHTML(writer, h.projectPage, data)
}

func (h *Handler) updateProjectSecret(writer http.ResponseWriter, request *http.Request, namespace, name string) {
	request.Body = http.MaxBytesReader(writer, request.Body, secretRequestSize)
	if err := request.ParseForm(); err != nil {
		http.Error(writer, "invalid secret update", http.StatusBadRequest)
		return
	}
	if !h.validCSRF(request.PostForm.Get("csrf")) {
		http.Error(writer, "invalid CSRF token", http.StatusForbidden)
		return
	}
	if namespace != h.secretManagementNamespace {
		http.Error(writer, "secret management is not enabled for this namespace", http.StatusForbidden)
		return
	}
	project := &actionsv1alpha1.Project{}
	if err := h.client.Get(request.Context(), types.NamespacedName{Namespace: namespace, Name: name}, project); err != nil {
		h.writeResolutionError(writer, request, err)
		return
	}
	if project.Spec.Secrets == nil {
		http.Error(writer, "Project does not reference a workflow Secret", http.StatusConflict)
		return
	}
	action := request.PostForm.Get("action")
	if action != "set" && action != "delete" {
		http.Error(writer, "invalid secret action", http.StatusBadRequest)
		return
	}
	secretName := strings.TrimSpace(request.PostForm.Get("name"))
	if action == "set" {
		secretName = strings.ToUpper(secretName)
		if err := projectvalue.ValidateName(secretName); err != nil {
			http.Error(writer, "invalid secret name: "+err.Error(), http.StatusBadRequest)
			return
		}
	} else if validationErrors := validation.IsConfigMapKey(secretName); len(validationErrors) > 0 {
		http.Error(writer, "invalid Secret key", http.StatusBadRequest)
		return
	}
	value := request.PostForm.Get("value")
	if action == "set" && (len(value) > projectvalue.MaxValueBytes || !utf8.ValidString(value)) {
		http.Error(writer, fmt.Sprintf("secret value must be valid UTF-8 and no more than %d bytes", projectvalue.MaxValueBytes), http.StatusBadRequest)
		return
	}
	key := types.NamespacedName{Namespace: project.Namespace, Name: project.Spec.Secrets.SecretRef.Name}
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		secret := &corev1.Secret{}
		err := h.client.Get(request.Context(), key, secret)
		if apierrors.IsNotFound(err) {
			if action == "delete" {
				return nil
			}
			return h.client.Create(request.Context(), &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: key.Name, Namespace: key.Namespace},
				Type:       corev1.SecretTypeOpaque, Data: map[string][]byte{secretName: []byte(value)},
			})
		}
		if err != nil {
			return err
		}
		if secret.Data == nil {
			secret.Data = map[string][]byte{}
		}
		if action == "delete" {
			delete(secret.Data, secretName)
		} else {
			if _, found := secret.Data[secretName]; !found && len(secret.Data) >= projectvalue.MaxSecretCount {
				return &secretCountLimitError{name: secret.Name}
			}
			secret.Data[secretName] = []byte(value)
		}
		return h.client.Update(request.Context(), secret)
	})
	if err != nil {
		var limit *secretCountLimitError
		if errors.As(err, &limit) {
			http.Error(writer, limit.Error(), http.StatusConflict)
			return
		}
		h.writeResolutionError(writer, request, fmt.Errorf("update Project %q Secret %q: %w", project.Name, key.Name, err))
		return
	}
	h.logger.Info("Updated Project secret", "namespace", project.Namespace, "project", project.Name, "secret", key.Name, "key", secretName, "action", action)
	http.Redirect(writer, request, "/projects/"+url.PathEscape(project.Namespace)+"/"+url.PathEscape(project.Name), http.StatusSeeOther)
}

func projectConfigurationStatus(project *actionsv1alpha1.Project) (string, string, string) {
	configured := meta.FindStatusCondition(project.Status.Conditions, actionsv1alpha1.ProjectConditionConfigured)
	if configured == nil || configured.ObservedGeneration != project.Generation {
		return "Pending", "queued", "Configuration has not been observed."
	}
	if configured.Status == metav1.ConditionTrue {
		return "Configured", "succeeded", configured.Message
	}
	return "Unconfigured", "failed", configured.Message
}

func sortedSecretDataNames(data map[string][]byte) []string {
	names := make([]string, 0, len(data))
	for name := range data {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func (h *Handler) runDetails(writer http.ResponseWriter, request *http.Request, run *actionsv1alpha1.WorkflowRun) {
	data, err := h.loadRunPageData(request.Context(), run)
	if err != nil {
		h.writeResolutionError(writer, request, err)
		return
	}
	h.writeHTML(writer, h.runPage, data)
}

func (h *Handler) loadRunPageData(ctx context.Context, run *actionsv1alpha1.WorkflowRun) (runPageData, error) {
	jobs := &actionsv1alpha1.WorkflowJobList{}
	if err := h.client.List(ctx, jobs, client.InNamespace(run.Namespace), client.MatchingLabels{actionsv1alpha1.LabelWorkflowRunUID: string(run.UID)}); err != nil {
		return runPageData{}, fmt.Errorf("load workflow jobs: %w", err)
	}
	sort.Slice(jobs.Items, func(left, right int) bool { return jobs.Items[left].Spec.JobID < jobs.Items[right].Spec.JobID })
	data := runPageData{
		RunURL:        runPath(run),
		Repository:    run.Spec.Source.GitHub.Repository.Owner + "/" + run.Spec.Source.GitHub.Repository.Name,
		WorkflowName:  run.Status.WorkflowName,
		WorkflowPath:  run.Spec.WorkflowPath,
		Revision:      run.Spec.Source.GitHub.Revision.SHA,
		ShortRevision: shortRevision(run.Spec.Source.GitHub.Revision.SHA),
		RefName:       shortRef(run.Spec.Source.GitHub.Revision.Ref),
		Status:        workflowstatus.Run(run),
	}
	data.StatusClass = statusClass(data.Status)
	if data.WorkflowName == "" {
		data.WorkflowName = data.WorkflowPath
	}
	if run.Status.StartTime != nil {
		data.Started = run.Status.StartTime.UTC().Format(time.RFC3339)
	}
	data.Duration = elapsedTime(run.Status.StartTime, run.Status.CompletionTime)
	for index := range jobs.Items {
		job := &jobs.Items[index]
		item := jobPageData{ID: job.Spec.JobID, DisplayName: job.Spec.DisplayName, Status: workflowstatus.Job(job), URL: runPath(run) + "/jobs/" + url.PathEscape(job.Name)}
		if item.DisplayName == "" {
			item.DisplayName = item.ID
		}
		item.StatusClass = statusClass(item.Status)
		if job.Status.RunnerRef != nil {
			item.Runner = job.Status.RunnerRef.Name
		}
		if job.Status.StartTime != nil {
			item.Started = job.Status.StartTime.UTC().Format(time.RFC3339)
		}
		item.Duration = elapsedTime(job.Status.StartTime, job.Status.CompletionTime)
		data.Jobs = append(data.Jobs, item)
	}
	return data, nil
}

func (h *Handler) jobLogs(writer http.ResponseWriter, request *http.Request, run *actionsv1alpha1.WorkflowRun, jobName string) {
	job, err := h.workflowJob(request.Context(), run, jobName)
	if err != nil {
		h.writeResolutionError(writer, request, err)
		return
	}
	runData, err := h.loadRunPageData(request.Context(), run)
	if err != nil {
		h.writeResolutionError(writer, request, err)
		return
	}
	path := runPath(run)
	jobStatus := workflowstatus.Job(job)
	displayName := job.Spec.DisplayName
	if displayName == "" {
		displayName = job.Spec.JobID
	}
	for index := range runData.Jobs {
		runData.Jobs[index].Selected = runData.Jobs[index].ID == job.Spec.JobID
	}
	runnerName := "Waiting for a runner"
	if job.Status.RunnerRef != nil {
		runnerName = job.Status.RunnerRef.Name
	}
	h.writeHTML(writer, h.logPage, logPageData{
		Repository: runData.Repository, WorkflowName: runData.WorkflowName,
		ShortRevision: runData.ShortRevision, Status: jobStatus,
		JobName: displayName, Runner: runnerName, Duration: elapsedTime(job.Status.StartTime, job.Status.CompletionTime),
		RunURL: path, StreamURL: path + "/jobs/" + url.PathEscape(job.Name) + "/stream", Jobs: runData.Jobs,
	})
}

func (h *Handler) streamJobLogs(writer http.ResponseWriter, request *http.Request, run *actionsv1alpha1.WorkflowRun, jobName string) {
	job, err := h.workflowJob(request.Context(), run, jobName)
	if err != nil {
		h.writeResolutionError(writer, request, err)
		return
	}
	lastLogID, err := parseLastEventID(request.Header.Get("Last-Event-ID"))
	if err != nil {
		http.Error(writer, "invalid Last-Event-ID", http.StatusBadRequest)
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
	// Log IDs are positions in the currently retained Pod log, not durable offsets across log rotation.
	var logID uint64
	for {
		select {
		case result, ok := <-reads:
			if !ok {
				return
			}
			if result.entry != nil {
				logID++
				if logID > lastLogID {
					writeLogEvent(writer, flusher, logID, result.entry)
				}
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

func parseLastEventID(value string) (uint64, error) {
	if value == "" {
		return 0, nil
	}
	return strconv.ParseUint(value, 10, 64)
}

func readLogStream(ctx context.Context, stream io.Reader) <-chan logRead {
	results := make(chan logRead)
	go func() {
		defer close(results)
		reader := bufio.NewReaderSize(stream, maxLogLineBytes+1)
		parser := &actionLogParser{}
		send := func(result logRead) bool {
			select {
			case results <- result:
				return true
			case <-ctx.Done():
				return false
			}
		}
		for {
			line, err := reader.ReadSlice('\n')
			if errors.Is(err, bufio.ErrBufferFull) {
				line = line[:maxLogLineBytes]
				text := strings.ToValidUTF8(string(line), "�")
				timestamp, content := splitLogTimestamp(strings.TrimSuffix(text, "\r"))
				entry := logEntry{Kind: "output", Text: content, Time: timestamp}
				entry.Text, entry.Parts = parser.ansi.format(entry.Text)
				parser.ansi.reset()
				entry.Text += truncatedLogMarker
				if entry.Parts != nil {
					entry.Parts = append(entry.Parts, logTextPart{Text: truncatedLogMarker})
				}
				if !send(logRead{entry: &entry}) {
					return
				}
				err = discardLogLine(reader)
				if err != nil {
					send(logRead{err: err})
					return
				}
				continue
			}
			text := strings.TrimSuffix(string(line), "\n")
			result := logRead{err: err}
			if text != "" || err == nil {
				if entry, visible := parser.parse(text); visible {
					result.entry = &entry
				}
			}
			if !send(result) {
				return
			}
			if err != nil {
				return
			}
		}
	}()
	return results
}

func discardLogLine(reader *bufio.Reader) error {
	for {
		_, err := reader.ReadSlice('\n')
		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		return err
	}
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
		if workflowstatus.JobTerminal(current) {
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

func statusClass(status string) string {
	return strings.ToLower(status)
}

func shortRevision(revision string) string {
	if len(revision) <= 7 {
		return revision
	}
	return revision[:7]
}

func shortRef(ref string) string {
	for _, prefix := range []string{"refs/heads/", "refs/tags/"} {
		if value, found := strings.CutPrefix(ref, prefix); found {
			return value
		}
	}
	return ref
}

func elapsedTime(start, completion *metav1.Time) string {
	if start == nil || completion == nil || completion.Time.Before(start.Time) {
		return ""
	}
	duration := completion.Time.Sub(start.Time).Round(time.Second)
	if duration < time.Second {
		return "<1s"
	}
	return duration.String()
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
	if err != nil || parsed.IsAbs() || parsed.Host != "" || (parsed.Path != "/" && parsed.Path != "/projects" && !strings.HasPrefix(parsed.Path, "/projects/") && !strings.HasPrefix(parsed.Path, "/runs/")) {
		return ""
	}
	return value
}

func sessionValue(token string) string {
	mac := hmac.New(sha256.New, []byte(token))
	_, _ = mac.Write([]byte("open-actions/console/session"))
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

func csrfValue(token string) string {
	mac := hmac.New(sha256.New, []byte(token))
	_, _ = mac.Write([]byte("open-actions/console/csrf"))
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

func writeEvent(writer io.Writer, flusher http.Flusher, event string, value any) {
	encoded, _ := json.Marshal(value)
	_, _ = fmt.Fprintf(writer, "event: %s\ndata: %s\n\n", event, encoded)
	flusher.Flush()
}

func writeLogEvent(writer io.Writer, flusher http.Flusher, id uint64, value any) {
	encoded, _ := json.Marshal(value)
	_, _ = fmt.Fprintf(writer, "id: %d\nevent: log\ndata: %s\n\n", id, encoded)
	flusher.Flush()
}

func writeSSEComment(writer io.Writer, flusher http.Flusher, value string) {
	_, _ = fmt.Fprintf(writer, ": %s\n\n", value)
	flusher.Flush()
}
