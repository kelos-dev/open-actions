package apischema

import (
	"fmt"
	"strings"
	"testing"
)

func TestRunnerServiceContract(t *testing.T) {
	port := func(name string, number int64, protocol string) map[string]any {
		result := map[string]any{"name": name, "port": number}
		if protocol != "" {
			result["protocol"] = protocol
		}
		return result
	}
	for _, resource := range []string{"runner", "runnerset"} {
		t.Run(resource, func(t *testing.T) {
			crd, _ := loadCRD(t, fmt.Sprintf("actions.kelos.dev_%ss.yaml", resource))
			schema := storageVersion(t, crd).Schema.OpenAPIV3Schema.Properties["spec"]
			if resource == "runnerset" {
				schema = schema.Properties["template"].Properties["spec"]
			}
			serviceSchema := schema.Properties["execution"].Properties["service"]
			typeDefault := serviceSchema.Properties["type"].Default
			protocolDefault := serviceSchema.Properties["ports"].Items.Schema.Properties["protocol"].Default
			if typeDefault == nil || string(typeDefault.Raw) != `"ClusterIP"` || protocolDefault == nil || string(protocolDefault.Raw) != `"TCP"` {
				t.Fatalf("Service defaults: type %v, protocol %v", typeDefault, protocolDefault)
			}
			if resource == "runnerset" {
				sample := loadSample(t, "actions_v1alpha1_runnerset-service.yaml")
				service := sample["spec"].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)["execution"].(map[string]any)["service"].(map[string]any)
				for _, entry := range service["ports"].([]any) {
					port := entry.(map[string]any)
					for _, key := range []string{"port", "targetPort"} {
						if number, ok := port[key].(float64); ok {
							port[key] = int64(number)
						}
					}
				}
				if errs := validateObject(t, crd, sample, nil); len(errs) > 0 {
					t.Fatalf("Service RunnerSet sample: %v", errs.ToAggregate())
				}
			}
			for _, test := range []struct {
				name    string
				service map[string]any
				valid   bool
			}{
				{"disabled", nil, true},
				{"defaults", map[string]any{"ports": []any{port("http", 80, "")}}, true},
				{"ClusterIP", map[string]any{"type": "ClusterIP", "ports": []any{port("http", 80, "TCP")}}, true},
				{"NodePort", map[string]any{"type": "NodePort", "ports": []any{port("http", 80, "TCP")}}, true},
				{"LoadBalancer", map[string]any{"type": "LoadBalancer", "ports": []any{port("http", 80, "TCP")}}, true},
				{"multiple protocols", map[string]any{"ports": []any{port("dns-tcp", 53, "TCP"), port("dns-udp", 53, "UDP"), port("sctp", 65535, "SCTP")}}, true},
				{"numeric target", map[string]any{"ports": []any{map[string]any{"name": "http", "port": int64(80), "targetPort": int64(8080)}}}, true},
				{"missing ports", map[string]any{}, false},
				{"empty ports", map[string]any{"ports": []any{}}, false},
				{"ExternalName", map[string]any{"type": "ExternalName", "ports": []any{port("http", 80, "")}}, false},
				{"empty type", map[string]any{"type": "", "ports": []any{port("http", 80, "")}}, false},
				{"duplicate names", map[string]any{"ports": []any{port("http", 80, ""), port("http", 81, "")}}, false},
				{"duplicate port and protocol", map[string]any{"ports": []any{port("http", 80, ""), port("other", 80, "TCP")}}, false},
				{"duplicate implicit protocols", map[string]any{"ports": []any{port("http", 80, ""), port("other", 80, "")}}, false},
				{"invalid protocol", map[string]any{"ports": []any{port("http", 80, "HTTP")}}, false},
				{"empty name", map[string]any{"ports": []any{port("", 80, "")}}, false},
				{"invalid name", map[string]any{"ports": []any{port("HTTP", 80, "")}}, false},
				{"long name", map[string]any{"ports": []any{port(strings.Repeat("a", 64), 80, "")}}, false},
				{"zero port", map[string]any{"ports": []any{port("http", 0, "")}}, false},
				{"port too large", map[string]any{"ports": []any{port("http", 65536, "")}}, false},
				{"zero target", map[string]any{"ports": []any{map[string]any{"name": "http", "port": int64(80), "targetPort": int64(0)}}}, false},
				{"target too large", map[string]any{"ports": []any{map[string]any{"name": "http", "port": int64(80), "targetPort": int64(65536)}}}, false},
				{"named target", map[string]any{"ports": []any{map[string]any{"name": "http", "port": int64(80), "targetPort": "http"}}}, false},
			} {
				t.Run(test.name, func(t *testing.T) {
					object := loadSample(t, fmt.Sprintf("actions_v1alpha1_%s.yaml", resource))
					spec := object["spec"].(map[string]any)
					if resource == "runnerset" {
						spec = spec["template"].(map[string]any)["spec"].(map[string]any)
					}
					if test.service != nil {
						spec["execution"].(map[string]any)["service"] = test.service
					}
					if errs := validateObject(t, crd, object, nil); (len(errs) == 0) != test.valid {
						t.Fatalf("Service validation = %v, want valid %t", errs.ToAggregate(), test.valid)
					}
				})
			}
			object := loadSample(t, fmt.Sprintf("actions_v1alpha1_%s.yaml", resource))
			spec := object["spec"].(map[string]any)
			if resource == "runnerset" {
				spec = spec["template"].(map[string]any)["spec"].(map[string]any)
			}
			ports := make([]any, 17)
			for index := range ports {
				ports[index] = port(fmt.Sprintf("port-%d", index), int64(8000+index), "TCP")
			}
			spec["execution"].(map[string]any)["service"] = map[string]any{"ports": ports}
			if errs := validateObject(t, crd, object, nil); len(errs) == 0 {
				t.Fatal("more than 16 ports passed validation")
			}
		})
	}
}

func TestWorkflowJobServiceReference(t *testing.T) {
	crd, _ := loadCRD(t, "actions.kelos.dev_workflowjobs.yaml")
	for _, name := range []string{"job-12345678-1234-1234-1234-123456789abc", "", "invalid/name", strings.Repeat("a", 64)} {
		object := loadSample(t, "actions_v1alpha1_workflowjob.yaml")
		object["status"] = map[string]any{"serviceRef": map[string]any{"name": name}}
		valid := strings.HasPrefix(name, "job-")
		if errs := validateObject(t, crd, object, nil); (len(errs) == 0) != valid {
			t.Fatalf("Service reference %q: validation = %v, want valid %t", name, errs.ToAggregate(), valid)
		}
	}
}
