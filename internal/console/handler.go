package console

import (
	"bufio"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	actionsv1alpha1 "github.com/kelos-dev/open-actions/api/v1alpha1"
	"github.com/kelos-dev/open-actions/internal/projectvalue"
	"github.com/kelos-dev/open-actions/internal/workflowrun"
	"github.com/kelos-dev/open-actions/internal/workflowstatus"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
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
	sessionCookieName   = "open_actions_console_session"
	sessionLifetime     = 7 * 24 * time.Hour
	streamHeartbeat     = 15 * time.Second
	logBatchInterval    = 50 * time.Millisecond
	logBatchEntryLimit  = 200
	logBatchByteLimit   = 64 << 10
	maxLogLineBytes     = 256 << 10
	truncatedLogMarker  = " … [log line truncated]"
	mainPageRunLimit    = 100
	secretRequestSize   = 3*projectvalue.MaxValueBytes + (8 << 10)
	adminRequestSize    = 8 << 10
	dispatchRequestSize = 12*65_535 + (16 << 10)
	maxRerunAttempt     = int32(2147483647)
	maxDispatchInputs   = 25
	maxDispatchPayload  = 65_535
)

var errLogsUnavailable = errors.New("logs are no longer available")

var (
	dispatchInputNamePattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_-]{0,99}$`)
	repositoryOwnerPattern   = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]*$`)
	repositoryNamePattern    = regexp.MustCompile(`^[A-Za-z0-9._-]+$`)
	invalidGitRefCharacter   = regexp.MustCompile(`[\x00-\x20\x7f~^:?*\[\\]`)
)

type secretCountLimitError struct {
	name string
}

func (e *secretCountLimitError) Error() string {
	return fmt.Sprintf("Secret %q already contains %d values", e.name, projectvalue.MaxSecretCount)
}

// Config contains required Console dependencies.
type Config struct {
	Client                             client.Client
	Logs                               LogSource
	Token                              string
	SecretManagementNamespace          string
	WorkflowRunTTLSecondsAfterFinished *int32
	SecureCookie                       bool
	Logger                             *slog.Logger
}

// Handler serves workflow information and authenticated Console administration.
type Handler struct {
	client                             client.Client
	logs                               LogSource
	tokenDigest                        [sha256.Size]byte
	sessionValue                       string
	csrfToken                          string
	secretManagementNamespace          string
	workflowRunTTLSecondsAfterFinished *int32
	secureCookie                       bool
	logger                             *slog.Logger
	loginPage                          *template.Template
	mainPage                           *template.Template
	projectsPage                       *template.Template
	projectPage                        *template.Template
	dispatchPage                       *template.Template
	runPage                            *template.Template
	logPage                            *template.Template
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
	CanReadSecrets    bool
	CanManageSecrets  bool
	ManagementNotice  string
	LoginURL          string
	CSRFToken         string
}

type dispatchPageData struct {
	Projects        []dispatchProjectOption
	SelectedProject string
	RepositoryID    string
	RepositoryOwner string
	RepositoryName  string
	RefType         string
	RefName         string
	Revision        string
	WorkflowPath    string
	Inputs          []dispatchInputPageData
	CSRFToken       string
	RequestID       string
}

type dispatchProjectOption struct {
	Value    string
	Label    string
	Selected bool
}

type dispatchInputPageData struct {
	Name  string
	Value string
}

type runPageData struct {
	RunURL         string
	CancelURL      string
	RerunURL       string
	LoginURL       string
	CSRFToken      string
	CanCancel      bool
	CanRerun       bool
	CanRerunFailed bool
	Repository     string
	WorkflowName   string
	WorkflowPath   string
	Revision       string
	ShortRevision  string
	RefName        string
	Status         string
	StatusClass    string
	Started        string
	Duration       string
	DispatchURL    string
	Jobs           []jobPageData
}

type runIdentityResponse struct {
	ID        string `json:"id"`
	Number    string `json:"number"`
	Attempt   string `json:"attempt"`
	Actor     string `json:"actor"`
	URL       string `json:"url,omitempty"`
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
}

type newerRunResponse struct {
	Newer       bool                 `json:"newer"`
	Current     runIdentityResponse  `json:"current"`
	Superseding *runIdentityResponse `json:"superseding,omitempty"`
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
	if config.WorkflowRunTTLSecondsAfterFinished != nil && *config.WorkflowRunTTLSecondsAfterFinished < 0 {
		return nil, errors.New("Console WorkflowRun TTL must not be negative")
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
	dispatchPage, err := template.New("dispatch").Parse(dispatchPageTemplate)
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
	var workflowRunTTLSecondsAfterFinished *int32
	if config.WorkflowRunTTLSecondsAfterFinished != nil {
		value := *config.WorkflowRunTTLSecondsAfterFinished
		workflowRunTTLSecondsAfterFinished = &value
	}
	return &Handler{
		client: config.Client, logs: config.Logs, tokenDigest: tokenDigest,
		sessionValue: sessionValue(config.Token), csrfToken: csrfValue(config.Token), secretManagementNamespace: config.SecretManagementNamespace,
		workflowRunTTLSecondsAfterFinished: workflowRunTTLSecondsAfterFinished,
		secureCookie:                       config.SecureCookie, logger: config.Logger,
		loginPage: loginPage, mainPage: mainPage, projectsPage: projectsPage, projectPage: projectPage, dispatchPage: dispatchPage, runPage: runPage, logPage: logPage,
	}, nil
}

// ServeHTTP routes public read requests and authenticated administration requests.
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
	parts := splitPath(request.URL.Path)
	if request.URL.Path == "/dispatch" {
		if !h.authenticated(request) {
			h.redirectToLogin(writer, request)
			return
		}
		switch request.Method {
		case http.MethodGet:
			h.workflowDispatchPage(writer, request)
		case http.MethodPost:
			h.createWorkflowDispatch(writer, request)
		default:
			writer.Header().Set("Allow", http.MethodGet+", "+http.MethodPost)
			http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		}
		return
	}
	if request.Method == http.MethodPost && len(parts) == 4 && parts[0] == "runs" && parts[3] == "cancel" {
		next := "/runs/" + url.PathEscape(parts[1]) + "/" + url.PathEscape(parts[2])
		if !h.authenticated(request) {
			http.Redirect(writer, request, "/login?next="+url.QueryEscape(next), http.StatusFound)
			return
		}
		h.cancelWorkflow(writer, request, parts[1], parts[2])
		return
	}
	if request.Method == http.MethodPost && len(parts) == 4 && parts[0] == "runs" && parts[3] == "rerun" {
		next := "/runs/" + url.PathEscape(parts[1]) + "/" + url.PathEscape(parts[2])
		if !h.authenticated(request) {
			http.Redirect(writer, request, "/login?next="+url.QueryEscape(next), http.StatusFound)
			return
		}
		h.rerunWorkflow(writer, request, parts[1], parts[2])
		return
	}
	if request.Method == http.MethodPost && len(parts) == 4 && parts[0] == "projects" && parts[3] == "secrets" {
		if !h.authenticated(request) {
			next := "/projects/" + url.PathEscape(parts[1]) + "/" + url.PathEscape(parts[2])
			http.Redirect(writer, request, "/login?next="+url.QueryEscape(next), http.StatusFound)
			return
		}
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
	if len(parts) == 6 && parts[0] == "api" && parts[1] == "v1" && parts[2] == "runs" && parts[5] == "newer" {
		run, err := h.resolveRun(request.Context(), parts[3], parts[4])
		if err != nil {
			h.writeResolutionError(writer, request, err)
			return
		}
		h.newerRun(writer, request, run)
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

func (h *Handler) redirectToLogin(writer http.ResponseWriter, request *http.Request) {
	http.Redirect(writer, request, "/login?next="+url.QueryEscape(request.URL.RequestURI()), http.StatusFound)
}

func (h *Handler) newerRun(writer http.ResponseWriter, request *http.Request, run *actionsv1alpha1.WorkflowRun) {
	if run.Status.Identity == nil {
		http.Error(writer, "WorkflowRun identity is not available", http.StatusServiceUnavailable)
		return
	}
	runs := &actionsv1alpha1.WorkflowRunList{}
	if err := h.client.List(request.Context(), runs, client.InNamespace(run.Namespace)); err != nil {
		h.writeResolutionError(writer, request, fmt.Errorf("list workflow runs: %w", err))
		return
	}
	var superseding *actionsv1alpha1.WorkflowRun
	for index := range runs.Items {
		candidate := &runs.Items[index]
		if !matchingRunRevisionAndWorkflow(run, candidate) || !newerRunIdentity(candidate.Status.Identity, run.Status.Identity) {
			continue
		}
		if superseding == nil || newerRunIdentity(candidate.Status.Identity, superseding.Status.Identity) {
			superseding = candidate
		}
	}
	response := newerRunResponse{Current: workflowRunIdentityResponse(run)}
	if superseding != nil {
		identity := workflowRunIdentityResponse(superseding)
		response.Newer = true
		response.Superseding = &identity
	}
	writer.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(writer).Encode(response); err != nil {
		h.logger.Error("failed to encode newer workflow run response", "error", err)
	}
}

func matchingRunRevisionAndWorkflow(current, candidate *actionsv1alpha1.WorkflowRun) bool {
	if candidate.Status.Identity == nil || current.Spec.ProjectRef != candidate.Spec.ProjectRef || current.Spec.WorkflowPath != candidate.Spec.WorkflowPath {
		return false
	}
	currentSource := current.Spec.Source.GitHub
	candidateSource := candidate.Spec.Source.GitHub
	return currentSource != nil && candidateSource != nil &&
		currentSource.Repository.ID == candidateSource.Repository.ID &&
		currentSource.Revision.SHA == candidateSource.Revision.SHA
}

func newerRunIdentity(candidate, current *actionsv1alpha1.WorkflowRunIdentityStatus) bool {
	return candidate != nil && current != nil &&
		(candidate.ID > current.ID || candidate.ID == current.ID && candidate.Attempt > current.Attempt)
}

func workflowRunIdentityResponse(run *actionsv1alpha1.WorkflowRun) runIdentityResponse {
	identity := run.Status.Identity
	actor := "open-actions"
	if source := run.Spec.Source.GitHub; source != nil && source.Actor != "" {
		actor = source.Actor
	}
	return runIdentityResponse{
		ID: strconv.FormatInt(identity.ID, 10), Number: strconv.FormatInt(identity.Number, 10),
		Attempt: strconv.FormatInt(int64(identity.Attempt), 10), Actor: actor, URL: identity.URL,
		Namespace: run.Namespace, Name: run.Name,
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
			next = "/projects"
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
		WorkflowDirectory: project.Spec.WorkflowDirectory,
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
	data.CanReadSecrets = true
	data.CanManageSecrets = h.authenticated(request)
	if data.CanManageSecrets {
		data.CSRFToken = h.csrfToken
	} else {
		data.LoginURL = "/login?next=" + url.QueryEscape(request.URL.RequestURI())
	}
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

func (h *Handler) workflowDispatchPage(writer http.ResponseWriter, request *http.Request) {
	query := request.URL.Query()
	if source := query.Get("source"); source != "" {
		if _, _, valid := splitNamespacedValue(source); !valid {
			http.Error(writer, "invalid workflow dispatch source", http.StatusBadRequest)
			return
		}
	}
	data, err := h.loadDispatchPageData(request.Context(), query)
	if err != nil {
		h.writeResolutionError(writer, request, err)
		return
	}
	data.CSRFToken = h.csrfToken
	data.RequestID, err = newDispatchRequestID()
	if err != nil {
		h.writeResolutionError(writer, request, fmt.Errorf("create workflow dispatch request ID: %w", err))
		return
	}
	h.writeHTML(writer, h.dispatchPage, data)
}

func (h *Handler) loadDispatchPageData(ctx context.Context, query url.Values) (dispatchPageData, error) {
	projects := &actionsv1alpha1.ProjectList{}
	if err := h.client.List(ctx, projects); err != nil {
		return dispatchPageData{}, fmt.Errorf("load Projects for workflow dispatch: %w", err)
	}
	sort.Slice(projects.Items, func(left, right int) bool {
		if projects.Items[left].Namespace == projects.Items[right].Namespace {
			return projects.Items[left].Name < projects.Items[right].Name
		}
		return projects.Items[left].Namespace < projects.Items[right].Namespace
	})
	data := dispatchPageData{SelectedProject: query.Get("project"), RefType: "branch"}
	if source := query.Get("source"); source != "" {
		namespace, name, valid := splitNamespacedValue(source)
		if !valid {
			return dispatchPageData{}, errors.New("workflow dispatch source run is invalid")
		}
		run, err := h.resolveRun(ctx, namespace, name)
		if err != nil {
			return dispatchPageData{}, fmt.Errorf("load workflow dispatch source WorkflowRun %q: %w", source, err)
		}
		githubSource := run.Spec.Source.GitHub
		data.SelectedProject = namespacedValue(run.Namespace, run.Spec.ProjectRef.Name)
		data.RepositoryID = strconv.FormatInt(githubSource.Repository.ID, 10)
		data.RepositoryOwner = githubSource.Repository.Owner
		data.RepositoryName = githubSource.Repository.Name
		data.Revision = githubSource.Revision.SHA
		data.WorkflowPath = run.Spec.WorkflowPath
		switch {
		case strings.HasPrefix(githubSource.Revision.Ref, "refs/heads/"):
			data.RefName = strings.TrimPrefix(githubSource.Revision.Ref, "refs/heads/")
		case strings.HasPrefix(githubSource.Revision.Ref, "refs/tags/"):
			data.RefType = "tag"
			data.RefName = strings.TrimPrefix(githubSource.Revision.Ref, "refs/tags/")
		}
		inputNames := make([]string, 0, len(githubSource.Event.Inputs))
		for name := range githubSource.Event.Inputs {
			inputNames = append(inputNames, name)
		}
		sort.Strings(inputNames)
		for _, name := range inputNames {
			data.Inputs = append(data.Inputs, dispatchInputPageData{Name: name, Value: githubSource.Event.Inputs[name]})
		}
	}
	for index := range projects.Items {
		project := &projects.Items[index]
		if project.Spec.Source.Type != actionsv1alpha1.SourceTypeGitHub || project.Spec.Source.GitHub == nil {
			continue
		}
		configured := meta.FindStatusCondition(project.Status.Conditions, actionsv1alpha1.ProjectConditionConfigured)
		if configured == nil || configured.Status != metav1.ConditionTrue || configured.ObservedGeneration != project.Generation {
			continue
		}
		value := namespacedValue(project.Namespace, project.Name)
		data.Projects = append(data.Projects, dispatchProjectOption{Value: value, Label: value, Selected: value == data.SelectedProject})
	}
	selected := false
	for _, project := range data.Projects {
		if project.Selected {
			selected = true
			break
		}
	}
	if !selected && len(data.Projects) > 0 {
		data.SelectedProject = data.Projects[0].Value
		data.Projects[0].Selected = true
	}
	return data, nil
}

func (h *Handler) createWorkflowDispatch(writer http.ResponseWriter, request *http.Request) {
	request.Body = http.MaxBytesReader(writer, request.Body, dispatchRequestSize)
	if err := request.ParseForm(); err != nil {
		http.Error(writer, "invalid workflow dispatch", http.StatusBadRequest)
		return
	}
	if !h.validCSRF(request.PostForm.Get("csrf")) {
		http.Error(writer, "invalid CSRF token", http.StatusForbidden)
		return
	}
	requestID := request.PostForm.Get("request-id")
	if !validDispatchRequestID(requestID) {
		http.Error(writer, "invalid workflow dispatch request ID", http.StatusBadRequest)
		return
	}
	namespace, projectName, valid := splitNamespacedValue(request.PostForm.Get("project"))
	if !valid {
		http.Error(writer, "invalid Project", http.StatusBadRequest)
		return
	}
	project := &actionsv1alpha1.Project{}
	if err := h.client.Get(request.Context(), types.NamespacedName{Namespace: namespace, Name: projectName}, project); err != nil {
		h.writeResolutionError(writer, request, fmt.Errorf("load Project %q: %w", namespacedValue(namespace, projectName), err))
		return
	}
	configured := meta.FindStatusCondition(project.Status.Conditions, actionsv1alpha1.ProjectConditionConfigured)
	if configured == nil || configured.Status != metav1.ConditionTrue || configured.ObservedGeneration != project.Generation {
		http.Error(writer, fmt.Sprintf("Project %q is not configured", project.Name), http.StatusConflict)
		return
	}
	if project.Spec.Source.Type != actionsv1alpha1.SourceTypeGitHub || project.Spec.Source.GitHub == nil {
		http.Error(writer, fmt.Sprintf("Project %q source is not supported", project.Name), http.StatusConflict)
		return
	}
	repositoryID, err := strconv.ParseInt(strings.TrimSpace(request.PostForm.Get("repository-id")), 10, 64)
	if err != nil || repositoryID < 1 || repositoryID > 9_007_199_254_740_991 {
		http.Error(writer, "invalid GitHub repository ID", http.StatusBadRequest)
		return
	}
	repositoryOwner := strings.TrimSpace(request.PostForm.Get("repository-owner"))
	if len(repositoryOwner) > 100 || !repositoryOwnerPattern.MatchString(repositoryOwner) {
		http.Error(writer, "invalid GitHub repository owner", http.StatusBadRequest)
		return
	}
	repositoryName := strings.TrimSpace(request.PostForm.Get("repository-name"))
	if len(repositoryName) > 100 || repositoryName == "." || repositoryName == ".." || !repositoryNamePattern.MatchString(repositoryName) {
		http.Error(writer, "invalid GitHub repository name", http.StatusBadRequest)
		return
	}
	refType := request.PostForm.Get("ref-type")
	if refType != "branch" && refType != "tag" {
		http.Error(writer, "invalid Git ref type", http.StatusBadRequest)
		return
	}
	refPrefix := "refs/heads/"
	if refType == "tag" {
		refPrefix = "refs/tags/"
	}
	ref := refPrefix + strings.TrimSpace(request.PostForm.Get("ref-name"))
	if !validGitRef(ref) {
		http.Error(writer, "invalid Git ref", http.StatusBadRequest)
		return
	}
	revision := strings.TrimSpace(request.PostForm.Get("revision"))
	if !validGitSHA(revision) {
		http.Error(writer, "revision must be a full lowercase Git SHA", http.StatusBadRequest)
		return
	}
	workflowPath := strings.TrimSpace(request.PostForm.Get("workflow-path"))
	if !validWorkflowPath(workflowPath) {
		http.Error(writer, "invalid workflow path", http.StatusBadRequest)
		return
	}
	inputs, err := dispatchInputs(request.PostForm["input-name"], request.PostForm["input-value"])
	if err != nil {
		http.Error(writer, err.Error(), http.StatusBadRequest)
		return
	}
	desired := &actionsv1alpha1.WorkflowRun{
		ObjectMeta: metav1.ObjectMeta{Name: "dispatch-" + requestID, Namespace: project.Namespace},
		Spec: actionsv1alpha1.WorkflowRunSpec{
			ProjectRef:              corev1.LocalObjectReference{Name: project.Name},
			TTLSecondsAfterFinished: h.workflowRunTTLSecondsAfterFinished,
			Source: actionsv1alpha1.WorkflowRunSource{Type: actionsv1alpha1.SourceTypeGitHub, GitHub: &actionsv1alpha1.GitHubWorkflowRunSource{
				Repository: actionsv1alpha1.GitHubRepository{ID: repositoryID, Owner: repositoryOwner, Name: repositoryName},
				Event:      actionsv1alpha1.GitHubEvent{Name: actionsv1alpha1.GitHubEventNameWorkflowDispatch, Inputs: inputs},
				Revision:   actionsv1alpha1.GitRevision{SHA: revision, Ref: ref},
			}},
			WorkflowPath: workflowPath,
		},
	}
	if err := h.client.Create(request.Context(), desired); err != nil {
		if apierrors.IsAlreadyExists(err) {
			existing := &actionsv1alpha1.WorkflowRun{}
			if getErr := h.client.Get(request.Context(), client.ObjectKeyFromObject(desired), existing); getErr != nil {
				h.writeResolutionError(writer, request, fmt.Errorf("load existing WorkflowRun %q: %w", desired.Name, getErr))
				return
			}
			if matchingWorkflowDispatch(existing, desired) {
				http.Redirect(writer, request, runPath(existing), http.StatusSeeOther)
				return
			}
			http.Error(writer, fmt.Sprintf("WorkflowRun %q already exists with different parameters", desired.Name), http.StatusConflict)
			return
		}
		if apierrors.IsInvalid(err) {
			http.Error(writer, "invalid workflow dispatch: "+err.Error(), http.StatusBadRequest)
			return
		}
		h.writeResolutionError(writer, request, fmt.Errorf("create WorkflowRun %q: %w", desired.Name, err))
		return
	}
	h.logger.Info("Created workflow dispatch", "namespace", desired.Namespace, "workflow_run", desired.Name, "project", project.Name, "repository", repositoryOwner+"/"+repositoryName, "workflow", workflowPath)
	http.Redirect(writer, request, runPath(desired), http.StatusSeeOther)
}

func newDispatchRequestID() (string, error) {
	data := make([]byte, 10)
	if _, err := rand.Read(data); err != nil {
		return "", err
	}
	return hex.EncodeToString(data), nil
}

func validDispatchRequestID(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == 10 && value == strings.ToLower(value)
}

func dispatchInputs(names, values []string) (map[string]string, error) {
	if len(names) != len(values) {
		return nil, errors.New("workflow input names and values do not match")
	}
	if len(names) > maxDispatchInputs {
		return nil, fmt.Errorf("workflow dispatch accepts at most %d inputs", maxDispatchInputs)
	}
	inputs := make(map[string]string, len(names))
	canonicalNames := make(map[string]struct{}, len(names))
	totalLength := 0
	for index, name := range names {
		name = strings.TrimSpace(name)
		if !dispatchInputNamePattern.MatchString(name) {
			return nil, fmt.Errorf("invalid workflow input name %q", name)
		}
		canonicalName := strings.ToLower(name)
		if _, found := canonicalNames[canonicalName]; found {
			return nil, fmt.Errorf("workflow input name %q is duplicated", name)
		}
		canonicalNames[canonicalName] = struct{}{}
		value := values[index]
		if !utf8.ValidString(value) {
			return nil, fmt.Errorf("workflow input %q must be valid UTF-8", name)
		}
		if utf8.RuneCountInString(value) > maxDispatchPayload {
			return nil, fmt.Errorf("workflow input %q exceeds %d characters", name, maxDispatchPayload)
		}
		totalLength += utf8.RuneCountInString(name) + utf8.RuneCountInString(value)
		inputs[name] = value
	}
	if totalLength > maxDispatchPayload {
		return nil, fmt.Errorf("workflow input names and values exceed %d characters", maxDispatchPayload)
	}
	if len(inputs) == 0 {
		return nil, nil
	}
	return inputs, nil
}

func matchingWorkflowDispatch(existing, desired *actionsv1alpha1.WorkflowRun) bool {
	existingSpec := existing.Spec.DeepCopy()
	desiredSpec := desired.Spec.DeepCopy()
	existingSpec.CancelRequested = false
	desiredSpec.CancelRequested = false
	existingSpec.TTLSecondsAfterFinished = nil
	desiredSpec.TTLSecondsAfterFinished = nil
	return apiequality.Semantic.DeepEqual(existingSpec, desiredSpec)
}

func validGitSHA(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == 20 && value == strings.ToLower(value)
}

func validGitRef(value string) bool {
	return utf8.RuneCountInString(value) <= 1024 && (strings.HasPrefix(value, "refs/heads/") || strings.HasPrefix(value, "refs/tags/")) &&
		len(strings.TrimPrefix(strings.TrimPrefix(value, "refs/heads/"), "refs/tags/")) > 0 &&
		!invalidGitRefCharacter.MatchString(value) && !strings.Contains(value, "//") && !strings.Contains(value, "..") &&
		!strings.Contains(value, "/.") && !strings.Contains(value, ".lock/") && !strings.HasSuffix(value, "/") &&
		!strings.HasSuffix(value, ".") && !strings.HasSuffix(value, ".lock") && !strings.Contains(value, "@{")
}

func validWorkflowPath(value string) bool {
	return utf8.RuneCountInString(value) <= 512 && (strings.HasSuffix(value, ".yaml") || strings.HasSuffix(value, ".yml")) &&
		!strings.HasPrefix(value, "/") && !strings.HasPrefix(value, "./") && !strings.HasPrefix(value, "../") &&
		!strings.Contains(value, "//") && !strings.Contains(value, "/./") && !strings.Contains(value, "/../")
}

func namespacedValue(namespace, name string) string {
	return namespace + "/" + name
}

func splitNamespacedValue(value string) (string, string, bool) {
	parts := strings.Split(value, "/")
	if len(parts) != 2 || len(validation.IsDNS1123Label(parts[0])) > 0 || len(validation.IsDNS1123Subdomain(parts[1])) > 0 {
		return "", "", false
	}
	return parts[0], parts[1], true
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
	authenticated := h.authenticated(request)
	if !workflowrun.Terminal(run) && !run.Spec.CancelRequested {
		data.CancelURL = runPath(run) + "/cancel"
		if authenticated {
			data.CanCancel = true
			data.CSRFToken = h.csrfToken
		} else {
			data.LoginURL = "/login?next=" + url.QueryEscape(runPath(run))
		}
	}
	_, latest, err := h.workflowRunLineage(request.Context(), run)
	if err != nil {
		h.logger.Warn("Unable to load WorkflowRun lineage", "namespace", run.Namespace, "workflow_run", run.Name, "error", err)
	} else if workflowrun.Terminal(latest) && (latest.Spec.Rerun == nil || latest.Spec.Rerun.Attempt < maxRerunAttempt) {
		data.RerunURL = runPath(run) + "/rerun"
		if authenticated {
			data.CanRerun = true
			data.CanRerunFailed = workflowrun.Failed(latest)
			data.CSRFToken = h.csrfToken
		} else {
			data.LoginURL = "/login?next=" + url.QueryEscape(runPath(run))
		}
	}
	h.writeHTML(writer, h.runPage, data)
}

func (h *Handler) cancelWorkflow(writer http.ResponseWriter, request *http.Request, namespace, name string) {
	request.Body = http.MaxBytesReader(writer, request.Body, adminRequestSize)
	if err := request.ParseForm(); err != nil {
		http.Error(writer, "invalid cancellation request", http.StatusBadRequest)
		return
	}
	if !h.validCSRF(request.PostForm.Get("csrf")) {
		http.Error(writer, "invalid CSRF token", http.StatusForbidden)
		return
	}
	alreadyRequested := false
	completed := false
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		alreadyRequested = false
		completed = false
		run, err := h.resolveRun(request.Context(), namespace, name)
		if err != nil {
			return err
		}
		if run.Spec.CancelRequested {
			alreadyRequested = true
			return nil
		}
		if workflowrun.Terminal(run) {
			completed = true
			return nil
		}
		run.Spec.CancelRequested = true
		return h.client.Update(request.Context(), run)
	})
	if err != nil {
		h.writeResolutionError(writer, request, fmt.Errorf("request cancellation for WorkflowRun %q: %w", name, err))
		return
	}
	if completed {
		http.Error(writer, fmt.Sprintf("WorkflowRun %q is already complete", name), http.StatusConflict)
		return
	}
	if !alreadyRequested {
		h.logger.Info("Requested WorkflowRun cancellation", "namespace", namespace, "workflow_run", name)
	}
	http.Redirect(writer, request, "/runs/"+url.PathEscape(namespace)+"/"+url.PathEscape(name), http.StatusSeeOther)
}

func (h *Handler) rerunWorkflow(writer http.ResponseWriter, request *http.Request, namespace, name string) {
	request.Body = http.MaxBytesReader(writer, request.Body, adminRequestSize)
	if err := request.ParseForm(); err != nil {
		http.Error(writer, "invalid rerun request", http.StatusBadRequest)
		return
	}
	if !h.validCSRF(request.PostForm.Get("csrf")) {
		http.Error(writer, "invalid CSRF token", http.StatusForbidden)
		return
	}
	selection := request.PostForm.Get("jobs")
	if selection != "all" && selection != "failed" {
		http.Error(writer, "invalid rerun job selection", http.StatusBadRequest)
		return
	}
	run, err := h.resolveRun(request.Context(), namespace, name)
	if err != nil {
		h.writeResolutionError(writer, request, err)
		return
	}
	root, latest, err := h.workflowRunLineage(request.Context(), run)
	if err != nil {
		h.writeResolutionError(writer, request, err)
		return
	}
	if !workflowrun.Terminal(latest) {
		http.Error(writer, "the latest workflow attempt is not complete", http.StatusConflict)
		return
	}
	attempt := int32(2)
	if latest.Spec.Rerun != nil {
		if latest.Spec.Rerun.Attempt == maxRerunAttempt {
			http.Error(writer, "workflow rerun attempt limit reached", http.StatusConflict)
			return
		}
		attempt = latest.Spec.Rerun.Attempt + 1
	}
	var jobIDs []string
	if selection == "failed" {
		if !workflowrun.Failed(latest) {
			http.Error(writer, "the latest workflow attempt did not fail because of a job", http.StatusConflict)
			return
		}
		jobs := &actionsv1alpha1.WorkflowJobList{}
		if err := h.client.List(request.Context(), jobs, client.InNamespace(latest.Namespace), client.MatchingLabels{actionsv1alpha1.LabelWorkflowRunUID: string(latest.UID)}); err != nil {
			h.writeResolutionError(writer, request, fmt.Errorf("load WorkflowJobs for WorkflowRun %q: %w", latest.Name, err))
			return
		}
		jobIDs, err = workflowrun.FailedJobIDs(latest, jobs.Items)
		if err != nil {
			http.Error(writer, err.Error(), http.StatusConflict)
			return
		}
	}
	desired := workflowrun.NewRerun(root, latest, attempt, "", jobIDs)
	if err := h.client.Create(request.Context(), desired); err != nil {
		if apierrors.IsAlreadyExists(err) {
			http.Error(writer, "another workflow rerun was requested", http.StatusConflict)
			return
		}
		h.writeResolutionError(writer, request, fmt.Errorf("create rerun WorkflowRun %q for WorkflowRun %q: %w", desired.Name, latest.Name, err))
		return
	}
	h.logger.Info("Created WorkflowRun rerun", "namespace", desired.Namespace, "workflow_run", desired.Name, "previous_run", latest.Name, "attempt", attempt, "jobs", selection, "selected_jobs", len(jobIDs))
	http.Redirect(writer, request, runPath(desired), http.StatusSeeOther)
}

func (h *Handler) workflowRunLineage(ctx context.Context, run *actionsv1alpha1.WorkflowRun) (*actionsv1alpha1.WorkflowRun, *actionsv1alpha1.WorkflowRun, error) {
	root := run
	if run.Spec.Rerun != nil {
		root = &actionsv1alpha1.WorkflowRun{}
		key := types.NamespacedName{Namespace: run.Namespace, Name: run.Spec.Rerun.OriginalRunRef.Name}
		if err := h.client.Get(ctx, key, root); err != nil {
			return nil, nil, fmt.Errorf("load original WorkflowRun %q for WorkflowRun %q: %w", key.Name, run.Name, err)
		}
		if root.UID != run.Spec.Rerun.OriginalRunRef.UID || root.Spec.Rerun != nil {
			return nil, nil, fmt.Errorf("WorkflowRun %q does not have a valid original WorkflowRun", run.Name)
		}
	}
	runs := &actionsv1alpha1.WorkflowRunList{}
	if err := h.client.List(ctx, runs, client.InNamespace(root.Namespace), client.MatchingLabels{actionsv1alpha1.LabelWorkflowRunRootUID: string(root.UID)}); err != nil {
		return nil, nil, fmt.Errorf("load rerun attempts for WorkflowRun %q: %w", root.Name, err)
	}
	if run.Spec.Rerun != nil {
		found := false
		for index := range runs.Items {
			found = runs.Items[index].UID == run.UID
			if found {
				break
			}
		}
		if !found {
			runs.Items = append(runs.Items, *run.DeepCopy())
		}
	}
	latest, err := workflowrun.LatestAttempt(root, runs.Items)
	if err != nil {
		return nil, nil, fmt.Errorf("resolve rerun attempts for WorkflowRun %q: %w", root.Name, err)
	}
	return root, latest, nil
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
	if ref := run.Spec.Source.GitHub.Revision.Ref; strings.HasPrefix(ref, "refs/heads/") || strings.HasPrefix(ref, "refs/tags/") {
		data.DispatchURL = "/dispatch?source=" + url.QueryEscape(namespacedValue(run.Namespace, run.Name))
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
	batchTicker := time.NewTicker(logBatchInterval)
	defer batchTicker.Stop()
	// Log IDs are positions in the currently retained Pod log, not durable offsets across log rotation.
	var logID uint64
	batch := make([]json.RawMessage, 0, logBatchEntryLimit)
	batchBytes := 0
	var batchLastLogID uint64
	flushBatch := func() {
		if len(batch) == 0 {
			return
		}
		writeLogEvent(writer, flusher, batchLastLogID, batch)
		clear(batch)
		batch = batch[:0]
		batchBytes = 0
	}
	for {
		select {
		case result, ok := <-reads:
			if !ok {
				flushBatch()
				return
			}
			if result.entry != nil {
				logID++
				if logID > lastLogID {
					encoded, _ := json.Marshal(result.entry)
					entryBytes := len(encoded) + 1
					if len(batch) > 0 && (len(batch) == logBatchEntryLimit || batchBytes+entryBytes > logBatchByteLimit) {
						flushBatch()
					}
					batch = append(batch, encoded)
					batchBytes += entryBytes
					batchLastLogID = logID
					if len(batch) == logBatchEntryLimit || batchBytes >= logBatchByteLimit {
						flushBatch()
					}
				}
			}
			if result.err == nil {
				continue
			}
			flushBatch()
			if result.err == io.EOF {
				writeEvent(writer, flusher, "end", "Log stream ended.")
			} else if request.Context().Err() == nil {
				writeEvent(writer, flusher, "error", "Runner log stream failed.")
			}
			return
		case <-batchTicker.C:
			flushBatch()
		case <-heartbeat.C:
			flushBatch()
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
	if status == "Cancelling" {
		return "running"
	}
	if status == "Timed out" {
		return "failed"
	}
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
	if err != nil || parsed.IsAbs() || parsed.Host != "" {
		return ""
	}
	dispatchPath := parsed.Path == "/dispatch"
	projectPath := parsed.Path == "/projects" || strings.HasPrefix(parsed.Path, "/projects/")
	runPath := strings.HasPrefix(parsed.Path, "/runs/")
	if !dispatchPath && !projectPath && !runPath {
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
