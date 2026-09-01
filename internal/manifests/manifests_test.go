package manifests

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"strconv"
	"strings"
	"testing"
	"text/template"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"sigs.k8s.io/yaml"
)

func TestWorkloadTemplatesUseConfiguredResources(t *testing.T) {
	chart := Chart()
	valuesData, err := fs.ReadFile(chart, "values.yaml")
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name      string
		component string
		path      string
	}{
		{name: "controller", component: "controller", path: "templates/deployment.yaml"},
		{name: "artifacts", component: "artifacts", path: "templates/artifact-statefulset.yaml"},
		{name: "console", component: "console", path: "templates/console-deployment.yaml"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var values map[string]any
			if err := yaml.Unmarshal(valuesData, &values); err != nil {
				t.Fatalf("parse values: %v", err)
			}
			component := values[test.component].(map[string]any)
			component["resources"] = map[string]any{
				"requests": map[string]any{"cpu": "125m", "memory": "96Mi"},
				"limits":   map[string]any{"cpu": "2", "example.com/accelerator": "1"},
			}

			podSpec := renderWorkloadPodSpec(t, chart, test.path, values)
			if len(podSpec.Containers) != 1 {
				t.Fatalf("containers = %d, want 1", len(podSpec.Containers))
			}
			resources := podSpec.Containers[0].Resources
			assertQuantityEqual(t, resources.Requests[corev1.ResourceCPU], "125m")
			assertQuantityEqual(t, resources.Requests[corev1.ResourceMemory], "96Mi")
			assertQuantityEqual(t, resources.Limits[corev1.ResourceCPU], "2")
			assertQuantityEqual(t, resources.Limits[corev1.ResourceName("example.com/accelerator")], "1")
		})
	}
}

func TestConsoleUsesConfiguredGitHubAPIURL(t *testing.T) {
	chart := Chart()
	valuesData, err := fs.ReadFile(chart, "values.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var values map[string]any
	if err := yaml.Unmarshal(valuesData, &values); err != nil {
		t.Fatalf("parse values: %v", err)
	}
	values["controller"].(map[string]any)["githubAPIURL"] = "https://github.example/api/v3"
	podSpec := renderWorkloadPodSpec(t, chart, "templates/console-deployment.yaml", values)
	for _, argument := range podSpec.Containers[0].Args {
		if argument == "--github-api-url=https://github.example/api/v3" {
			return
		}
	}
	t.Fatalf("Console arguments = %#v", podSpec.Containers[0].Args)
}

func TestConsoleCanReadProjectPrivateKeys(t *testing.T) {
	chart := Chart()
	data, err := fs.ReadFile(chart, "templates/console-rbac.yaml")
	if err != nil {
		t.Fatal(err)
	}
	tmpl, err := template.New("rbac").Option("missingkey=error").Parse(string(data))
	if err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if err := tmpl.Execute(&output, map[string]any{
		"Release": map[string]any{"Namespace": "open-actions-system"},
		"Values":  map[string]any{"console": map[string]any{"enabled": true}},
	}); err != nil {
		t.Fatal(err)
	}
	clusterRole := rbacv1.ClusterRole{}
	if err := yaml.Unmarshal(bytes.Split(output.Bytes(), []byte("---"))[0], &clusterRole); err != nil {
		t.Fatal(err)
	}
	for _, rule := range clusterRole.Rules {
		if len(rule.APIGroups) == 1 && rule.APIGroups[0] == "" && len(rule.Resources) == 1 && rule.Resources[0] == "secrets" && len(rule.Verbs) == 1 && rule.Verbs[0] == "get" {
			return
		}
	}
	t.Fatalf("Console ClusterRole does not grant get access to Secrets: %#v", clusterRole.Rules)
}

func TestConsoleCanWatchWorkflowRuns(t *testing.T) {
	chart := Chart()
	data, err := fs.ReadFile(chart, "templates/console-rbac.yaml")
	if err != nil {
		t.Fatal(err)
	}
	tmpl, err := template.New("rbac").Option("missingkey=error").Parse(string(data))
	if err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if err := tmpl.Execute(&output, map[string]any{
		"Release": map[string]any{"Namespace": "open-actions-system"},
		"Values":  map[string]any{"console": map[string]any{"enabled": true}},
	}); err != nil {
		t.Fatal(err)
	}
	clusterRole := rbacv1.ClusterRole{}
	if err := yaml.Unmarshal(bytes.Split(output.Bytes(), []byte("---"))[0], &clusterRole); err != nil {
		t.Fatal(err)
	}
	for _, rule := range clusterRole.Rules {
		if len(rule.APIGroups) == 1 && rule.APIGroups[0] == "actions.kelos.dev" && len(rule.Resources) == 1 && rule.Resources[0] == "workflowruns" {
			for _, verb := range rule.Verbs {
				if verb == "watch" {
					return
				}
			}
		}
	}
	t.Fatalf("Console ClusterRole does not grant watch access to WorkflowRuns: %#v", clusterRole.Rules)
}

func TestServiceTemplate(t *testing.T) {
	chart := Chart()
	valuesData, err := fs.ReadFile(chart, "values.yaml")
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name      string
		configure func(map[string]any)
		wantType  corev1.ServiceType
		wantPort  int32
	}{
		{name: "defaults", configure: func(map[string]any) {}, wantType: corev1.ServiceTypeClusterIP},
		{
			name: "node port",
			configure: func(values map[string]any) {
				service := values["service"].(map[string]any)
				service["type"] = "NodePort"
				service["nodePort"] = 30082
			},
			wantType: corev1.ServiceTypeNodePort,
			wantPort: 30082,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var testValues map[string]any
			if err := yaml.Unmarshal(valuesData, &testValues); err != nil {
				t.Fatalf("parse values: %v", err)
			}
			test.configure(testValues)

			service := renderService(t, chart, "templates/service.yaml", testValues)
			if service.Spec.Type != test.wantType {
				t.Errorf("service type = %q, want %q", service.Spec.Type, test.wantType)
			}
			if service.Spec.Ports[0].NodePort != test.wantPort {
				t.Errorf("node port = %d, want %d", service.Spec.Ports[0].NodePort, test.wantPort)
			}
		})
	}
}

func TestConsoleServiceTemplate(t *testing.T) {
	chart := Chart()
	valuesData, err := fs.ReadFile(chart, "values.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var values map[string]any
	if err := yaml.Unmarshal(valuesData, &values); err != nil {
		t.Fatalf("parse values: %v", err)
	}
	serviceValues := values["console"].(map[string]any)["service"].(map[string]any)
	serviceValues["type"] = "NodePort"
	serviceValues["nodePort"] = 30083

	service := renderService(t, chart, "templates/console-service.yaml", values)
	if service.Name != "open-actions-console" || service.Spec.Type != corev1.ServiceTypeNodePort || service.Spec.Ports[0].NodePort != 30083 {
		t.Fatalf("Console Service = %#v", service)
	}
	if service.Spec.Selector["app.kubernetes.io/component"] != "console" {
		t.Fatalf("Console Service selector = %#v", service.Spec.Selector)
	}
}

func TestArtifactServicesTargetArtifactPods(t *testing.T) {
	chart := Chart()
	valuesData, err := fs.ReadFile(chart, "values.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var values map[string]any
	if err := yaml.Unmarshal(valuesData, &values); err != nil {
		t.Fatalf("parse values: %v", err)
	}
	service := renderService(t, chart, "templates/artifact-service.yaml", values)
	if service.Name != "open-actions-artifacts" || service.Spec.Selector["app.kubernetes.io/component"] != "artifacts" {
		t.Fatalf("artifact Service = %#v", service)
	}
	if service.Spec.Ports[0].TargetPort.StrVal != "http" {
		t.Fatalf("artifact Service target port = %#v", service.Spec.Ports[0].TargetPort)
	}
	headless := renderService(t, chart, "templates/artifact-headless-service.yaml", values)
	if headless.Name != "open-actions-artifacts-headless" || headless.Spec.ClusterIP != corev1.ClusterIPNone {
		t.Fatalf("artifact headless Service = %#v", headless)
	}
	if headless.Spec.Selector["app.kubernetes.io/component"] != "artifacts" {
		t.Fatalf("artifact headless Service selector = %#v", headless.Spec.Selector)
	}
}

func renderService(t *testing.T, chart fs.FS, path string, values map[string]any) corev1.Service {
	t.Helper()
	data, err := fs.ReadFile(chart, path)
	if err != nil {
		t.Fatal(err)
	}
	tmpl, err := template.New("service").Option("missingkey=error").Parse(string(data))
	if err != nil {
		t.Fatalf("parse Service template: %v", err)
	}
	var output bytes.Buffer
	if err := tmpl.Execute(&output, map[string]any{
		"Release": map[string]any{"Namespace": "open-actions-system"},
		"Values":  values,
	}); err != nil {
		t.Fatalf("render Service template: %v", err)
	}
	var service corev1.Service
	if err := yaml.Unmarshal(output.Bytes(), &service); err != nil {
		t.Fatalf("parse rendered Service: %v", err)
	}
	if len(service.Spec.Ports) != 1 {
		t.Fatalf("service ports = %d, want 1", len(service.Spec.Ports))
	}
	return service
}

func renderWorkloadPodSpec(t *testing.T, chart fs.FS, path string, values map[string]any) corev1.PodSpec {
	t.Helper()
	data, err := fs.ReadFile(chart, path)
	if err != nil {
		t.Fatal(err)
	}
	functions := template.FuncMap{
		"default": func(fallback, value any) any {
			if value == nil || value == "" {
				return fallback
			}
			return value
		},
		"fail":      func(message string) (string, error) { return "", errors.New(message) },
		"hasPrefix": func(prefix, value string) bool { return strings.HasPrefix(value, prefix) },
		"int64": func(value any) int64 {
			switch typed := value.(type) {
			case int:
				return int64(typed)
			case int64:
				return typed
			case float64:
				return int64(typed)
			default:
				panic(fmt.Sprintf("unsupported integer type %T", value))
			}
		},
		"nindent": func(spaces int, value string) string {
			indent := strings.Repeat(" ", spaces)
			return "\n" + indent + strings.ReplaceAll(value, "\n", "\n"+indent)
		},
		"quote": func(value any) string { return strconv.Quote(fmt.Sprint(value)) },
		"toYaml": func(value any) (string, error) {
			data, err := yaml.Marshal(value)
			return strings.TrimSuffix(string(data), "\n"), err
		},
	}
	tmpl, err := template.New("workload").Option("missingkey=error").Funcs(functions).Parse(string(data))
	if err != nil {
		t.Fatalf("parse workload template: %v", err)
	}
	var output bytes.Buffer
	if err := tmpl.Execute(&output, map[string]any{
		"Release": map[string]any{"Namespace": "open-actions-system"},
		"Values":  values,
	}); err != nil {
		t.Fatalf("render workload template: %v", err)
	}
	var workload struct {
		Spec struct {
			Template struct {
				Spec corev1.PodSpec `json:"spec"`
			} `json:"template"`
		} `json:"spec"`
	}
	if err := yaml.Unmarshal(output.Bytes(), &workload); err != nil {
		t.Fatalf("parse rendered workload: %v\n%s", err, output.String())
	}
	return workload.Spec.Template.Spec
}

func assertQuantityEqual(t *testing.T, got resource.Quantity, want string) {
	t.Helper()
	wantQuantity := resource.MustParse(want)
	if !got.Equal(wantQuantity) {
		t.Errorf("resource quantity = %s, want %s", got.String(), want)
	}
}
