package manifests

import (
	"bytes"
	"io/fs"
	"testing"
	"text/template"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/yaml"
)

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
