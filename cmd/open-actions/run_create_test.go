package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"reflect"
	"regexp"
	"strings"
	"testing"

	actionsv1alpha1 "github.com/kelos-dev/open-actions/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

const createWorkflowPath = ".open-actions/workflows/deploy.yaml"

const createWorkflow = `on:
  workflow_dispatch:
    inputs:
      environment:
        type: choice
        options: [staging, production]
        default: staging
        required: true
      dry-run:
        type: boolean
        default: true
      retries:
        type: number
        default: 3
      notes:
        type: string
      target:
        type: environment
jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - run: echo deploy
`

type runCreateGitHubFixture struct {
	t                *testing.T
	permission       string
	branch           string
	tag              string
	commit           string
	defaultWorkflow  string
	selectedWorkflow string
	workflowPath     string
	failEndpoint     string
	graphqlError     bool
	requests         [][]string
}

func (f *runCreateGitHubFixture) run(_ context.Context, executable string, args []string, stdout, stderr io.Writer) error {
	f.t.Helper()
	if executable != "gh" || len(args) < 4 || !reflect.DeepEqual(args[:3], []string{"api", "--hostname", "github.com"}) {
		f.t.Fatalf("unexpected GitHub command: %s %q", executable, args)
	}
	f.requests = append(f.requests, append([]string(nil), args...))
	endpoint := args[3]
	if endpoint == "--method" {
		if len(args) < 6 || args[4] != "GET" {
			f.t.Fatalf("unexpected REST arguments: %q", args)
		}
		endpoint = args[5]
	}
	if endpoint == f.failEndpoint {
		fmt.Fprint(stderr, "request denied")
		return errors.New("exit status 1")
	}
	var response any
	switch endpoint {
	case "graphql":
		fields := map[string]string{}
		for index := 4; index < len(args); index += 2 {
			if args[index] != "-f" {
				f.t.Fatalf("unexpected GraphQL field: %q", args[index])
			}
			key, value, _ := strings.Cut(args[index+1], "=")
			fields[key] = value
		}
		if fields["owner"] != "acme" || fields["name"] != "example" || fields["query"] != runRepositoryQuery {
			f.t.Fatalf("unexpected GraphQL fields: %#v", fields)
		}
		var branch, tag any
		if f.branch != "" && fields["branch"] == "refs/heads/"+f.branch {
			branch = map[string]any{"name": f.branch}
		}
		if f.tag != "" && fields["tag"] == "refs/tags/"+f.tag {
			tag = map[string]any{"name": f.tag}
		}
		response = map[string]any{"data": map[string]any{
			"viewer": map[string]any{"login": "octocat"},
			"repository": map[string]any{
				"databaseId": 123, "name": "Example", "owner": map[string]any{"login": "Acme"},
				"viewerPermission": f.permission,
				"defaultBranchRef": map[string]any{"name": "main", "target": map[string]any{"oid": strings.Repeat("a", 40)}},
				"branch":           branch, "tag": tag,
			},
		}}
		if f.graphqlError {
			response.(map[string]any)["errors"] = []any{map[string]any{"message": "permission check failed"}}
		}
	default:
		if strings.HasPrefix(endpoint, "repos/Acme/Example/commits/") {
			response = map[string]any{"sha": f.commit}
		} else if endpoint == "repos/Acme/Example/contents/"+url.PathEscape(f.workflowPath) {
			if len(args) != 8 || args[6] != "-f" {
				f.t.Fatalf("unexpected contents arguments: %q", args)
			}
			content := f.selectedWorkflow
			if args[7] == "ref="+strings.Repeat("a", 40) {
				content = f.defaultWorkflow
			} else if args[7] != "ref="+f.commit {
				f.t.Fatalf("workflow fetch was not pinned to a commit: %q", args)
			}
			response = map[string]any{"type": "file", "encoding": "base64", "content": base64.StdEncoding.EncodeToString([]byte(content))}
		} else {
			f.t.Fatalf("unexpected GitHub endpoint: %q", endpoint)
		}
	}
	return json.NewEncoder(stdout).Encode(response)
}

func runCreateFixture(t *testing.T) (commandDependencies, client.Client, *runCreateGitHubFixture) {
	t.Helper()
	project := &actionsv1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: "project", Namespace: "default", Generation: 1},
		Spec: actionsv1alpha1.ProjectSpec{
			Source:            actionsv1alpha1.ProjectSource{Type: actionsv1alpha1.SourceTypeGitHub, GitHub: &actionsv1alpha1.GitHubAppConfiguration{}},
			WorkflowDirectory: ".open-actions/workflows",
		},
		Status: actionsv1alpha1.ProjectStatus{Conditions: []metav1.Condition{{
			Type: actionsv1alpha1.ProjectConditionConfigured, Status: metav1.ConditionTrue, ObservedGeneration: 1,
		}}},
	}
	dependencies := testRunDependencies(t, &testRunLogSource{}, project)
	clients, err := dependencies.newRunClients(runKubeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	github := &runCreateGitHubFixture{
		t: t, permission: "WRITE", branch: "main", tag: "v1", commit: strings.Repeat("b", 40),
		defaultWorkflow: createWorkflow, selectedWorkflow: createWorkflow, workflowPath: createWorkflowPath,
	}
	dependencies.runCommand = github.run
	return dependencies, clients.reader.(client.Client), github
}

func createArguments(extra ...string) []string {
	return append([]string{"run", "create", createWorkflowPath, "--project", "project", "--repo", "acme/example"}, extra...)
}

func TestRunCreatePinsRevisionAndPrintsRunName(t *testing.T) {
	for _, test := range []struct {
		name, ref, branch, tag, wantRef string
	}{
		{name: "default branch", wantRef: "refs/heads/main"},
		{name: "branch", ref: "feature/deploy", branch: "feature/deploy", wantRef: "refs/heads/feature/deploy"},
		{name: "tag", ref: "v1", tag: "v1", wantRef: "refs/tags/v1"},
		{name: "explicit branch", ref: "refs/heads/release", branch: "release", tag: "release", wantRef: "refs/heads/release"},
		{name: "explicit tag", ref: "refs/tags/release", branch: "release", tag: "release", wantRef: "refs/tags/release"},
	} {
		t.Run(test.name, func(t *testing.T) {
			dependencies, kube, github := runCreateFixture(t)
			github.branch, github.tag = test.branch, test.tag
			var stdout bytes.Buffer
			args := createArguments("-f", "environment=production", "-f", "dry-run=false", "-f", "retries=0", "-f", "notes=a=b,c\nline", "-f", "target=production")
			if test.ref != "" {
				args = append(args, "--ref", test.ref)
			}
			if err := runWithDependencies(context.Background(), args, &stdout, &bytes.Buffer{}, dependencies); err != nil {
				t.Fatal(err)
			}
			if !regexp.MustCompile(`^dispatch-[a-f0-9]{20}\n$`).MatchString(stdout.String()) {
				t.Fatalf("stdout = %q", stdout.String())
			}
			run := &actionsv1alpha1.WorkflowRun{}
			if err := kube.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: strings.TrimSpace(stdout.String())}, run); err != nil {
				t.Fatal(err)
			}
			want := actionsv1alpha1.WorkflowRunSpec{
				ProjectRef: corev1.LocalObjectReference{Name: "project"}, WorkflowPath: createWorkflowPath,
				Source: actionsv1alpha1.WorkflowRunSource{Type: actionsv1alpha1.SourceTypeGitHub, GitHub: &actionsv1alpha1.GitHubWorkflowRunSource{
					Actor: "octocat", Repository: actionsv1alpha1.GitHubRepository{ID: 123, Owner: "Acme", Name: "Example"},
					Revision: actionsv1alpha1.GitRevision{SHA: github.commit, Ref: test.wantRef},
					Event: actionsv1alpha1.GitHubEvent{Name: actionsv1alpha1.GitHubEventNameWorkflowDispatch, Inputs: map[string]string{
						"environment": "production", "dry-run": "false", "retries": "0", "notes": "a=b,c\nline", "target": "production",
					}},
				}},
			}
			if !reflect.DeepEqual(run.Spec, want) {
				t.Fatalf("run spec = %#v, source = %#v", run.Spec, run.Spec.Source.GitHub)
			}
			if len(github.requests) != 4 || github.requests[1][5] != "repos/Acme/Example/commits/"+url.PathEscape(test.wantRef) {
				t.Fatalf("GitHub requests = %q", github.requests)
			}
		})
	}
}

func TestRunCreateInputConformance(t *testing.T) {
	// https://docs.github.com/en/actions/reference/workflows-and-actions/workflow-syntax#onworkflow_dispatchinputs
	for _, test := range []struct {
		name, workflow string
		fields         []string
		want           map[string]string
		failure        string
	}{
		{name: "omitted defaults", workflow: createWorkflow},
		{name: "explicit empty", workflow: createWorkflow, fields: []string{"notes="}, want: map[string]string{"notes": ""}},
		{name: "unknown input", workflow: createWorkflow, fields: []string{"unknown=x"}, failure: "unknown input"},
		{name: "invalid choice", workflow: createWorkflow, fields: []string{"environment=unknown"}, failure: "invalid for type choice"},
		{name: "invalid boolean", workflow: createWorkflow, fields: []string{"dry-run=yes"}, failure: "invalid for type boolean"},
		{name: "invalid number", workflow: createWorkflow, fields: []string{"retries=abc"}, failure: "invalid for type number"},
		{name: "required input", workflow: strings.Replace(createWorkflow, "        default: staging\n", "", 1), failure: "missing required input"},
		{name: "selected revision inputs", workflow: strings.Replace(createWorkflow, "[staging, production]", "[staging, preview]", 1), fields: []string{"environment=preview"}, want: map[string]string{"environment": "preview"}},
		{name: "no inputs", workflow: "on: workflow_dispatch\njobs:\n  deploy:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo deploy\n"},
	} {
		t.Run(test.name, func(t *testing.T) {
			dependencies, kube, github := runCreateFixture(t)
			github.selectedWorkflow = test.workflow
			args := createArguments("--name", "manual-deploy")
			for _, field := range test.fields {
				args = append(args, "-f", field)
			}
			var stdout bytes.Buffer
			err := runWithDependencies(context.Background(), args, &stdout, &bytes.Buffer{}, dependencies)
			if test.failure != "" {
				assertRunCreateFailure(t, kube, stdout.String(), err, test.failure)
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			run := &actionsv1alpha1.WorkflowRun{}
			if err := kube.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "manual-deploy"}, run); err != nil {
				t.Fatal(err)
			}
			if stdout.String() != "manual-deploy\n" || !reflect.DeepEqual(run.Spec.Source.GitHub.Event.Inputs, test.want) {
				t.Fatalf("stdout = %q, inputs = %#v", stdout.String(), run.Spec.Source.GitHub.Event.Inputs)
			}
		})
	}
}

func assertRunCreateFailure(t *testing.T, kube client.Client, stdout string, err error, want string) {
	t.Helper()
	if err == nil || !strings.Contains(err.Error(), want) || stdout != "" {
		t.Fatalf("stdout = %q, error = %v; want %q", stdout, err, want)
	}
	runs := &actionsv1alpha1.WorkflowRunList{}
	if err := kube.List(context.Background(), runs); err != nil {
		t.Fatal(err)
	}
	if len(runs.Items) != 0 {
		t.Fatalf("created %d WorkflowRuns after failure", len(runs.Items))
	}
}

func TestRunCreateRejectsUnavailableWorkflowsAndRepositories(t *testing.T) {
	// https://docs.github.com/en/actions/how-tos/manage-workflow-runs/manually-run-a-workflow
	for _, test := range []struct {
		name, want string
		configure  func(*runCreateGitHubFixture)
		extra      []string
	}{
		{name: "read permission", want: "requires write access", configure: func(f *runCreateGitHubFixture) { f.permission = "READ" }},
		{name: "triage permission", want: "requires write access", configure: func(f *runCreateGitHubFixture) { f.permission = "TRIAGE" }},
		{name: "missing permission", want: "requires write access", configure: func(f *runCreateGitHubFixture) { f.permission = "" }},
		{name: "authentication failure", want: "request denied", configure: func(f *runCreateGitHubFixture) { f.failEndpoint = "graphql" }},
		{name: "partial GraphQL failure", want: "permission check failed", configure: func(f *runCreateGitHubFixture) { f.graphqlError = true }},
		{name: "missing ref", want: "does not exist", extra: []string{"--ref", "missing"}},
		{name: "commit ref", want: "does not exist", extra: []string{"--ref", strings.Repeat("a", 40)}},
		{name: "missing explicit branch", want: "does not exist", extra: []string{"--ref", "refs/heads/missing"}},
		{name: "missing explicit tag", want: "does not exist", extra: []string{"--ref", "refs/tags/missing"}},
		{name: "ambiguous ref", want: "both a branch and tag", extra: []string{"--ref", "main"}, configure: func(f *runCreateGitHubFixture) { f.tag = "main" }},
		{name: "invalid commit response", want: "invalid commit", configure: func(f *runCreateGitHubFixture) { f.commit = "" }},
		{name: "commit failure", want: "resolve ref", configure: func(f *runCreateGitHubFixture) {
			f.failEndpoint = "repos/Acme/Example/commits/" + url.PathEscape("refs/heads/main")
		}},
		{name: "missing workflow", want: "workflow must exist on the default branch", configure: func(f *runCreateGitHubFixture) {
			f.failEndpoint = "repos/Acme/Example/contents/" + url.PathEscape(createWorkflowPath)
		}},
		{name: "default branch trigger", want: "default branch: workflow", configure: func(f *runCreateGitHubFixture) {
			f.defaultWorkflow = "on: push\njobs:\n  deploy:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo deploy\n"
		}},
		{name: "selected trigger", want: "does not declare workflow_dispatch", configure: func(f *runCreateGitHubFixture) {
			f.selectedWorkflow = "on: push\njobs:\n  deploy:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo deploy\n"
		}},
		{name: "invalid workflow", want: "parse workflow", configure: func(f *runCreateGitHubFixture) { f.selectedWorkflow = "[invalid" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			dependencies, kube, github := runCreateFixture(t)
			if test.configure != nil {
				test.configure(github)
			}
			var stdout bytes.Buffer
			err := runWithDependencies(context.Background(), createArguments(test.extra...), &stdout, &bytes.Buffer{}, dependencies)
			assertRunCreateFailure(t, kube, stdout.String(), err, test.want)
		})
	}
}

func TestRunCreateKubernetesOptionsAndErrors(t *testing.T) {
	for _, test := range []struct {
		name, namespace, want string
		mutate                func(*actionsv1alpha1.Project)
		createError           bool
	}{
		{name: "context namespace", namespace: "team-ci"},
		{name: "explicit namespace", namespace: "other-ci"},
		{name: "missing Project", want: `get Project "project"`, mutate: func(p *actionsv1alpha1.Project) { p.Name = "other" }},
		{name: "unconfigured Project", want: `Project "project" is not configured`, mutate: func(p *actionsv1alpha1.Project) { p.Status.Conditions = nil }},
		{name: "stale Project", want: `Project "project" is not configured`, mutate: func(p *actionsv1alpha1.Project) { p.Generation++ }},
		{name: "unsupported source", want: `Project "project" does not have a GitHub source`, mutate: func(p *actionsv1alpha1.Project) { p.Spec.Source.GitHub = nil }},
		{name: "create forbidden", want: `create WorkflowRun "manual-deploy" in namespace "default": forbidden`, createError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			dependencies, kube, github := runCreateFixture(t)
			project := &actionsv1alpha1.Project{}
			key := client.ObjectKey{Namespace: "default", Name: "project"}
			if err := kube.Get(context.Background(), key, project); err != nil {
				t.Fatal(err)
			}
			if err := kube.Delete(context.Background(), project); err != nil {
				t.Fatal(err)
			}
			project.ResourceVersion = ""
			if test.namespace != "" {
				project.Namespace = test.namespace
			}
			if test.mutate != nil {
				test.mutate(project)
			}
			if err := kube.Create(context.Background(), project); err != nil {
				t.Fatal(err)
			}
			dependencies.defaultKubeconfig = "default-config"
			dependencies.newRunClients = func(options runKubeOptions) (*runClients, error) {
				if options.kubeconfig != "selected-config" || options.context != "development" || options.defaultKubeconfig != "default-config" {
					t.Fatalf("Kubernetes options = %#v", options)
				}
				writer := client.Writer(kube)
				if test.createError {
					writer = interceptor.NewClient(kube.(client.WithWatch), interceptor.Funcs{Create: func(context.Context, client.WithWatch, client.Object, ...client.CreateOption) error {
						return errors.New("forbidden")
					}})
				}
				return &runClients{reader: kube, writer: writer, defaultNamespace: "team-ci"}, nil
			}
			args := createArguments("--name", "manual-deploy", "--kubeconfig", "selected-config", "--context", "development")
			if test.name != "context namespace" {
				args = append(args, "-n", project.Namespace)
			}
			var stdout bytes.Buffer
			err := runWithDependencies(context.Background(), args, &stdout, &bytes.Buffer{}, dependencies)
			if test.want != "" {
				assertRunCreateFailure(t, kube, stdout.String(), err, test.want)
				if !test.createError && len(github.requests) != 0 {
					t.Fatalf("GitHub requests after invalid Project: %q", github.requests)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if err := kube.Get(context.Background(), client.ObjectKey{Namespace: test.namespace, Name: "manual-deploy"}, &actionsv1alpha1.WorkflowRun{}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestRunCreateValidatesArgumentsBeforeLoadingClients(t *testing.T) {
	for _, test := range []struct {
		name string
		args []string
		want string
	}{
		{name: "missing workflow", args: []string{"run", "create"}, want: "accepts 1 arg"},
		{name: "extra workflow", args: createArguments("extra"), want: "accepts 1 arg"},
		{name: "missing project", args: []string{"run", "create", createWorkflowPath, "--repo", "acme/example"}, want: "--project"},
		{name: "missing repo", args: []string{"run", "create", createWorkflowPath, "--project", "project"}, want: "--repo"},
		{name: "invalid project", args: createArguments("--project", "../bad"), want: "--project"},
		{name: "invalid repo", args: createArguments("--repo", "https://github.com/acme/example"), want: "--repo"},
		{name: "invalid path", args: []string{"run", "create", "../deploy.yaml", "--project", "project", "--repo", "acme/example"}, want: "repository-relative"},
		{name: "invalid ref", args: createArguments("--ref", "main~1"), want: "--ref"},
		{name: "pull ref", args: createArguments("--ref", "refs/pull/1/head"), want: "--ref"},
		{name: "invalid name", args: createArguments("--name", "UPPER"), want: "--name"},
		{name: "invalid hostname", args: createArguments("--hostname", "https://github.com"), want: "--hostname"},
		{name: "missing input value", args: createArguments("-f", "notes"), want: "key=value"},
		{name: "invalid input name", args: createArguments("-f", "bad.name=x"), want: "key=value"},
		{name: "duplicate input", args: createArguments("-f", "notes=a", "-f", "notes=b"), want: "duplicated"},
		{name: "case duplicate input", args: createArguments("-f", "notes=a", "-f", "NOTES=b"), want: "duplicated"},
		{name: "invalid UTF-8", args: createArguments("-f", "notes=\xff"), want: "UTF-8"},
		{name: "large input", args: createArguments("-f", "notes="+strings.Repeat("x", 65_535)), want: "exceed 65535"},
	} {
		t.Run(test.name, func(t *testing.T) {
			dependencies := commandDependencies{newRunClients: func(runKubeOptions) (*runClients, error) {
				t.Fatal("invalid command loaded Kubernetes clients")
				return nil, nil
			}}
			err := runWithDependencies(context.Background(), test.args, &bytes.Buffer{}, &bytes.Buffer{}, dependencies)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestRunCreateHelpDoesNotLoadClients(t *testing.T) {
	var stdout bytes.Buffer
	if err := runWithDependencies(context.Background(), []string{"run", "create", "--help"}, &stdout, &bytes.Buffer{}, commandDependencies{}); err != nil {
		t.Fatal(err)
	}
	for _, flag := range []string{"--project", "--repo", "--ref", "--raw-field", "--name", "--hostname", "--namespace", "--context", "--kubeconfig"} {
		if !regexp.MustCompile(`(?m)^\s+(?:-\w, )?` + regexp.QuoteMeta(flag) + `\s+\w+\s+[^\n]+$`).MatchString(stdout.String()) {
			t.Fatalf("help does not contain %q", flag)
		}
	}
}

func TestRunCreateNamedRequestsReuseMatchingRuns(t *testing.T) {
	for _, permission := range []string{"WRITE", "MAINTAIN", "ADMIN"} {
		t.Run(permission, func(t *testing.T) {
			dependencies, kube, github := runCreateFixture(t)
			github.permission = permission
			github.commit = strings.Repeat("a", 40)
			args := createArguments("--name", "manual-deploy")
			if err := runWithDependencies(context.Background(), args, &bytes.Buffer{}, &bytes.Buffer{}, dependencies); err != nil {
				t.Fatal(err)
			}
			if len(github.requests) != 3 {
				t.Fatalf("default branch workflow fetched more than once: %q", github.requests)
			}
			var stdout bytes.Buffer
			if err := runWithDependencies(context.Background(), args, &stdout, &bytes.Buffer{}, dependencies); err != nil || stdout.String() != "manual-deploy\n" {
				t.Fatalf("matching retry: stdout = %q, error = %v", stdout.String(), err)
			}
			stdout.Reset()
			err := runWithDependencies(context.Background(), append(args, "-f", "notes=changed"), &stdout, &bytes.Buffer{}, dependencies)
			if err == nil || err.Error() != `WorkflowRun "manual-deploy" already exists with different parameters` || stdout.Len() != 0 {
				t.Fatalf("repeat creation: stdout = %q, error = %v", stdout.String(), err)
			}
			runs := &actionsv1alpha1.WorkflowRunList{}
			if err := kube.List(context.Background(), runs); err != nil {
				t.Fatal(err)
			}
			if len(runs.Items) != 1 || len(runs.Items[0].Spec.Source.GitHub.Event.Inputs) != 0 {
				t.Fatalf("repeat creation changed WorkflowRuns: %#v", runs.Items)
			}
		})
	}
}

func TestRunCreateNamedRetriesPreserveMutableFields(t *testing.T) {
	dependencies, kube, github := runCreateFixture(t)
	args := createArguments("--name", "manual-deploy")
	if err := runWithDependencies(context.Background(), args, &bytes.Buffer{}, &bytes.Buffer{}, dependencies); err != nil {
		t.Fatal(err)
	}
	run := &actionsv1alpha1.WorkflowRun{}
	key := client.ObjectKey{Namespace: "default", Name: "manual-deploy"}
	if err := kube.Get(context.Background(), key, run); err != nil {
		t.Fatal(err)
	}
	ttl := int32(3600)
	run.Spec.CancelRequested = true
	run.Spec.TTLSecondsAfterFinished = &ttl
	if err := kube.Update(context.Background(), run); err != nil {
		t.Fatal(err)
	}
	var stdout bytes.Buffer
	if err := runWithDependencies(context.Background(), args, &stdout, &bytes.Buffer{}, dependencies); err != nil || stdout.String() != "manual-deploy\n" {
		t.Fatalf("retry after cancellation and retention update: stdout = %q, error = %v", stdout.String(), err)
	}
	github.commit = strings.Repeat("c", 40)
	stdout.Reset()
	err := runWithDependencies(context.Background(), args, &stdout, &bytes.Buffer{}, dependencies)
	if err == nil || err.Error() != `WorkflowRun "manual-deploy" already exists with different parameters` || stdout.Len() != 0 {
		t.Fatalf("retry after ref moved: stdout = %q, error = %v", stdout.String(), err)
	}
	existing := &actionsv1alpha1.WorkflowRun{}
	if err := kube.Get(context.Background(), key, existing); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(existing.Spec, run.Spec) {
		t.Fatalf("retry changed existing WorkflowRun spec: %#v", existing.Spec)
	}
}

func TestRunCreateWorkflowDirectoryConformance(t *testing.T) {
	// https://docs.github.com/en/actions/reference/workflows-and-actions/workflow-syntax#about-yaml-syntax-for-workflows
	for _, test := range []struct {
		name, directory, path string
		valid                 bool
	}{
		{name: "default directory", directory: ".open-actions/workflows", path: createWorkflowPath, valid: true},
		{name: "custom directory", directory: "ci/workflows", path: "ci/workflows/deploy.yml", valid: true},
		{name: "repository root", directory: ".open-actions/workflows", path: "deploy.yml"},
		{name: "elsewhere in repository", directory: ".open-actions/workflows", path: "docs/examples/deploy.yaml"},
		{name: "nested workflow", directory: ".open-actions/workflows", path: ".open-actions/workflows/nested/deploy.yaml"},
		{name: "similar directory prefix", directory: ".open-actions/workflows", path: ".open-actions/workflows-other/deploy.yaml"},
		{name: "custom directory rejects default", directory: "ci/workflows", path: createWorkflowPath},
	} {
		t.Run(test.name, func(t *testing.T) {
			dependencies, kube, github := runCreateFixture(t)
			project := &actionsv1alpha1.Project{}
			if err := kube.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "project"}, project); err != nil {
				t.Fatal(err)
			}
			project.Spec.WorkflowDirectory = test.directory
			if err := kube.Update(context.Background(), project); err != nil {
				t.Fatal(err)
			}
			github.workflowPath = test.path
			args := createArguments("--name", "manual-deploy")
			args[2] = test.path
			var stdout bytes.Buffer
			err := runWithDependencies(context.Background(), args, &stdout, &bytes.Buffer{}, dependencies)
			if !test.valid {
				want := fmt.Sprintf("workflow %q must be a direct child of Project %q workflow directory %q", test.path, project.Name, test.directory)
				assertRunCreateFailure(t, kube, stdout.String(), err, want)
				if len(github.requests) != 0 {
					t.Fatalf("invalid workflow path made GitHub requests: %q", github.requests)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			run := &actionsv1alpha1.WorkflowRun{}
			if err := kube.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "manual-deploy"}, run); err != nil {
				t.Fatal(err)
			}
			if run.Spec.WorkflowPath != test.path || len(github.requests) != 4 || stdout.String() != "manual-deploy\n" {
				t.Fatalf("created workflow path = %q, requests = %q, stdout = %q", run.Spec.WorkflowPath, github.requests, stdout.String())
			}
		})
	}
}

func TestRunCreateInputLimits(t *testing.T) {
	fields := make([]string, 26)
	for index := range fields {
		fields[index] = fmt.Sprintf("input%d=value", index)
	}
	if _, err := runCreateInputs(fields); err == nil || !strings.Contains(err.Error(), "at most 25") {
		t.Fatalf("input count error = %v", err)
	}
	value := strings.Repeat("界", 65_535-len("notes"))
	inputs, err := runCreateInputs([]string{"notes=" + value})
	if err != nil || inputs["notes"] != value {
		t.Fatalf("input at Unicode character limit: error = %v", err)
	}
	if _, err := runCreateInputs([]string{"notes=" + value, "target=x"}); err == nil || !strings.Contains(err.Error(), "exceed 65535") {
		t.Fatalf("combined payload error = %v", err)
	}
}

func TestRunGitHubUsesConfiguredHostname(t *testing.T) {
	github, err := newRunGitHub("github.example.com", func(_ context.Context, name string, args []string, stdout, _ io.Writer) error {
		if name != "gh" || !reflect.DeepEqual(args, []string{"api", "--hostname", "github.example.com", "--method", "GET", "user"}) {
			t.Fatalf("command = %q %q", name, args)
		}
		_, err := io.WriteString(stdout, `{"login":"octocat"}`)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	var user struct{ Login string }
	if err := github.api(context.Background(), &user, "--method", "GET", "user"); err != nil || user.Login != "octocat" {
		t.Fatalf("GitHub response = %#v, %v", user, err)
	}
}
