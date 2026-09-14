package console

import (
	"context"
	"fmt"
	"html"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"testing"

	actionsv1alpha1 "github.com/kelos-dev/open-actions/api/v1alpha1"
	"github.com/kelos-dev/open-actions/internal/projectvalue"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestConsoleManagesProjectSecretsAcrossNamespaces(t *testing.T) {
	handler := newTestHandler(t, false)
	for _, namespace := range []string{"team-a", "team-b"} {
		project := &actionsv1alpha1.Project{
			ObjectMeta: metav1.ObjectMeta{Name: "project", Namespace: namespace},
			Spec: actionsv1alpha1.ProjectSpec{Secrets: &actionsv1alpha1.ProjectSecretSource{
				SecretRef: actionsv1alpha1.ProjectValueReference{Name: "project-secrets"},
			}},
		}
		if err := handler.client.Create(context.Background(), project); err != nil {
			t.Fatal(err)
		}
		path := "/projects/" + namespace + "/project"
		for _, value := range []string{"first-value", "replacement-value"} {
			response := postProjectValue(handler, path+"/secrets", url.Values{
				"csrf": {handler.csrfToken}, "action": {"set"}, "name": {"deploy_token"}, "value": {value},
			}, true)
			if response.Code != http.StatusSeeOther || response.Header().Get("Location") != path {
				t.Fatalf("set secret in %s = %d, %q", namespace, response.Code, response.Body.String())
			}
			secret := &corev1.Secret{}
			if err := handler.client.Get(context.Background(), client.ObjectKey{Namespace: namespace, Name: "project-secrets"}, secret); err != nil {
				t.Fatal(err)
			}
			if string(secret.Data["DEPLOY_TOKEN"]) != value {
				t.Fatalf("Secret in %s was not updated", namespace)
			}
		}
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
		if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), "DEPLOY_TOKEN") || strings.Contains(response.Body.String(), "replacement-value") {
			t.Fatalf("secret page in %s = %d, %q", namespace, response.Code, response.Body.String())
		}
		response = postProjectValue(handler, path+"/secrets", url.Values{
			"csrf": {handler.csrfToken}, "action": {"delete"}, "name": {"DEPLOY_TOKEN"},
		}, true)
		if response.Code != http.StatusSeeOther {
			t.Fatalf("delete secret in %s = %d, %q", namespace, response.Code, response.Body.String())
		}
		secret := &corev1.Secret{}
		if err := handler.client.Get(context.Background(), client.ObjectKey{Namespace: namespace, Name: "project-secrets"}, secret); err != nil {
			t.Fatal(err)
		}
		if len(secret.Data) != 0 {
			t.Fatalf("Secret in %s still has keys", namespace)
		}
	}
	secret := &corev1.Secret{}
	if err := handler.client.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "project-secrets"}, secret); err != nil {
		t.Fatal(err)
	}
	if string(secret.Data["DEPLOY_TOKEN"]) != "existing-secret-value" {
		t.Fatal("updated Secret in an unrelated namespace")
	}
}

func TestConsoleManagesReferencedProjectVariables(t *testing.T) {
	handler, configMap := variableManagementFixture(t)
	path := "/projects/team-a/project"
	value := "first line\n</textarea><script>alert('value')</script>"
	configMap.Data["REGION"] = value
	if err := handler.client.Update(context.Background(), configMap); err != nil {
		t.Fatal(err)
	}
	for _, authenticated := range []bool{false, true} {
		request := httptest.NewRequest(http.MethodGet, path, nil)
		if authenticated {
			request.Header.Set("Authorization", "Bearer "+testConsoleToken)
		}
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		body := response.Body.String()
		if response.Code != http.StatusOK || !strings.Contains(body, html.EscapeString(value)) || strings.Contains(body, "<script>") {
			t.Fatalf("variables page = %d, %q", response.Code, body)
		}
		if authenticated {
			if !strings.Contains(body, `action="`+path+`/variables"`) || !strings.Contains(body, `name="action" value="set">Save</button>`) {
				t.Fatalf("missing variable edit controls: %s", body)
			}
		} else if strings.Contains(body, handler.csrfToken) || strings.Contains(body, `method="post"`) || !strings.Contains(body, "Sign in as an administrator") {
			t.Fatalf("incorrect anonymous variable controls: %s", body)
		}
	}
	for _, test := range []struct {
		action string
		name   string
		value  string
		want   map[string]string
	}{
		{action: "set", name: "region", value: "eu-west-1\r\n", want: map[string]string{"REGION": "eu-west-1\n", "KEEP": "unchanged"}},
		{action: "set", name: "empty", want: map[string]string{"REGION": "eu-west-1\n", "KEEP": "unchanged", "EMPTY": ""}},
		{action: "delete", name: "REGION", want: map[string]string{"KEEP": "unchanged", "EMPTY": ""}},
	} {
		response := postProjectValue(handler, path+"/variables", url.Values{
			"csrf": {handler.csrfToken}, "action": {test.action}, "name": {test.name}, "value": {test.value},
			"configmap": {"unrelated"}, "namespace": {"default"},
		}, true)
		if response.Code != http.StatusSeeOther || response.Header().Get("Location") != path {
			t.Fatalf("%s variable = %d, %q", test.action, response.Code, response.Body.String())
		}
		if err := handler.client.Get(context.Background(), client.ObjectKeyFromObject(configMap), configMap); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(configMap.Data, test.want) || configMap.Labels["owner"] != "team-a" {
			t.Fatalf("updated ConfigMap = %#v", configMap)
		}
	}
	if err := handler.client.Delete(context.Background(), configMap); err != nil {
		t.Fatal(err)
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), "The referenced ConfigMap does not exist") {
		t.Fatalf("missing ConfigMap page = %d, %q", response.Code, response.Body.String())
	}
	response = postProjectValue(handler, path+"/variables", url.Values{
		"csrf": {handler.csrfToken}, "action": {"set"}, "name": {"region"}, "value": {"us-east-1"},
	}, true)
	if response.Code != http.StatusSeeOther {
		t.Fatalf("create ConfigMap = %d, %q", response.Code, response.Body.String())
	}
	if err := handler.client.Get(context.Background(), client.ObjectKeyFromObject(configMap), configMap); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(configMap.Data, map[string]string{"REGION": "us-east-1"}) {
		t.Fatalf("created ConfigMap data = %#v", configMap.Data)
	}
	other := &corev1.ConfigMap{}
	if err := handler.client.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: configMap.Name}, other); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(other.Data, map[string]string{"REGION": "untouched"}) {
		t.Fatalf("updated ConfigMap in another namespace: %#v", other.Data)
	}
}

func TestConsoleValidatesProjectVariableUpdates(t *testing.T) {
	// https://docs.github.com/en/actions/reference/workflows-and-actions/variables
	for _, test := range []struct {
		name          string
		action        string
		variable      string
		value         string
		csrf          string
		authenticated bool
		want          int
	}{
		{name: "anonymous", action: "set", variable: "REGION", csrf: "valid", want: http.StatusFound},
		{name: "invalid CSRF", action: "set", variable: "REGION", authenticated: true, want: http.StatusForbidden},
		{name: "invalid action", action: "rename", variable: "REGION", csrf: "valid", authenticated: true, want: http.StatusBadRequest},
		{name: "reserved prefix", action: "set", variable: "github_region", csrf: "valid", authenticated: true, want: http.StatusBadRequest},
		{name: "leading digit", action: "set", variable: "1REGION", csrf: "valid", authenticated: true, want: http.StatusBadRequest},
		{name: "invalid character", action: "set", variable: "MY-REGION", csrf: "valid", authenticated: true, want: http.StatusBadRequest},
		{name: "empty name", action: "set", csrf: "valid", authenticated: true, want: http.StatusBadRequest},
		{name: "long name", action: "set", variable: strings.Repeat("A", 256), csrf: "valid", authenticated: true, want: http.StatusBadRequest},
		{name: "oversize value", action: "set", variable: "REGION", value: strings.Repeat("é", projectvalue.MaxValueBytes/2+1), csrf: "valid", authenticated: true, want: http.StatusBadRequest},
		{name: "invalid UTF-8", action: "set", variable: "REGION", value: "\xff", csrf: "valid", authenticated: true, want: http.StatusBadRequest},
		{name: "invalid delete key", action: "delete", variable: "../REGION", csrf: "valid", authenticated: true, want: http.StatusBadRequest},
	} {
		t.Run(test.name, func(t *testing.T) {
			handler, configMap := variableManagementFixture(t)
			before := configMap.DeepCopy()
			csrf := test.csrf
			if csrf == "valid" {
				csrf = handler.csrfToken
			}
			response := postProjectValue(handler, "/projects/team-a/project/variables", url.Values{
				"csrf": {csrf}, "action": {test.action}, "name": {test.variable}, "value": {test.value},
			}, test.authenticated)
			if response.Code != test.want {
				t.Fatalf("response = %d, %q, want %d", response.Code, response.Body.String(), test.want)
			}
			if err := handler.client.Get(context.Background(), client.ObjectKeyFromObject(configMap), configMap); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(configMap, before) {
				t.Fatal("rejected request changed ConfigMap")
			}
		})
	}
}

func TestConsoleEnforcesProjectVariableLimits(t *testing.T) {
	handler, configMap := variableManagementFixture(t)
	for index := len(configMap.Data); index < projectvalue.MaxVariableCount; index++ {
		configMap.Data[fmt.Sprintf("VAR_%03d", index)] = "value"
	}
	if err := handler.client.Update(context.Background(), configMap); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name string
		want int
	}{
		{name: "EXTRA", want: http.StatusConflict},
		{name: "region", want: http.StatusSeeOther},
	} {
		value := strings.Repeat("%", projectvalue.MaxValueBytes)
		response := postProjectValue(handler, "/projects/team-a/project/variables", url.Values{
			"csrf": {handler.csrfToken}, "action": {"set"}, "name": {test.name}, "value": {value},
		}, true)
		if response.Code != test.want {
			t.Fatalf("set %s at limit = %d, %q", test.name, response.Code, response.Body.String())
		}
		if err := handler.client.Get(context.Background(), client.ObjectKeyFromObject(configMap), configMap); err != nil {
			t.Fatal(err)
		}
		if len(configMap.Data) != projectvalue.MaxVariableCount {
			t.Fatalf("variable count = %d", len(configMap.Data))
		}
		if test.want == http.StatusSeeOther && configMap.Data["REGION"] != value {
			t.Fatal("replacement at size and count limits was not stored")
		}
		if _, found := configMap.Data["EXTRA"]; found {
			t.Fatal("stored variable beyond count limit")
		}
	}
}

func TestConsoleAppliesVariableSizeLimitAfterNormalizingLineEndings(t *testing.T) {
	// HTML form submission encodes each textarea newline as CRLF.
	// https://html.spec.whatwg.org/multipage/form-control-infrastructure.html#converting-an-entry-list-to-a-list-of-name-value-pairs
	for _, test := range []struct {
		name string
		size int
		want int
	}{
		{name: "at limit", size: projectvalue.MaxValueBytes, want: http.StatusSeeOther},
		{name: "over limit", size: projectvalue.MaxValueBytes + 1, want: http.StatusBadRequest},
	} {
		t.Run(test.name, func(t *testing.T) {
			handler, configMap := variableManagementFixture(t)
			before := configMap.DeepCopy()
			response := postProjectValue(handler, "/projects/team-a/project/variables", url.Values{
				"csrf": {handler.csrfToken}, "action": {"set"}, "name": {"REGION"}, "value": {strings.Repeat("\r\n", test.size)},
			}, true)
			if response.Code != test.want {
				t.Fatalf("set multiline variable = %d, %q, want %d", response.Code, response.Body.String(), test.want)
			}
			if err := handler.client.Get(context.Background(), client.ObjectKeyFromObject(configMap), configMap); err != nil {
				t.Fatal(err)
			}
			if test.want == http.StatusSeeOther {
				if configMap.Data["REGION"] != strings.Repeat("\n", test.size) {
					t.Fatal("stored variable has incorrect line endings or size")
				}
			} else if !reflect.DeepEqual(configMap, before) {
				t.Fatal("oversize variable changed ConfigMap")
			}
		})
	}
}

func TestConsoleRequiresProjectValueReferences(t *testing.T) {
	for _, test := range []struct {
		path      string
		reference string
	}{
		{path: "secrets", reference: "spec.secrets.secretRef"},
		{path: "variables", reference: "spec.variables.configMapRef"},
	} {
		t.Run(test.path, func(t *testing.T) {
			handler := newTestHandler(t, false)
			project := &actionsv1alpha1.Project{}
			if err := handler.client.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "project"}, project); err != nil {
				t.Fatal(err)
			}
			project.Spec.Secrets = nil
			if err := handler.client.Update(context.Background(), project); err != nil {
				t.Fatal(err)
			}
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/projects/default/project", nil))
			if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), test.reference) {
				t.Fatalf("unconfigured Project page = %d, %q", response.Code, response.Body.String())
			}
			response = postProjectValue(handler, "/projects/default/project/"+test.path, url.Values{
				"csrf": {handler.csrfToken}, "action": {"set"}, "name": {"REGION"}, "value": {"us-east-1"},
			}, true)
			if response.Code != http.StatusConflict || !strings.Contains(response.Body.String(), `Project "project"`) {
				t.Fatalf("unconfigured value update = %d, %q", response.Code, response.Body.String())
			}
		})
	}
}

func variableManagementFixture(t *testing.T) (*Handler, *corev1.ConfigMap) {
	t.Helper()
	handler := newTestHandler(t, false)
	project := &actionsv1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: "project", Namespace: "team-a"},
		Spec: actionsv1alpha1.ProjectSpec{
			Variables: &actionsv1alpha1.ProjectVariableSource{ConfigMapRef: actionsv1alpha1.ProjectValueReference{Name: "project-variables"}},
		},
	}
	configMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "project-variables", Namespace: "team-a", Labels: map[string]string{"owner": "team-a"}},
		Data:       map[string]string{"REGION": "us-east-1", "KEEP": "unchanged"},
	}
	other := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: configMap.Name, Namespace: "default"}, Data: map[string]string{"REGION": "untouched"}}
	for _, object := range []client.Object{project, configMap, other} {
		if err := handler.client.Create(context.Background(), object); err != nil {
			t.Fatal(err)
		}
	}
	return handler, configMap
}

func postProjectValue(handler *Handler, path string, form url.Values, authenticated bool) *httptest.ResponseRecorder {
	request := httptest.NewRequest(http.MethodPost, path, strings.NewReader(form.Encode()))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	if authenticated {
		request.Header.Set("Authorization", "Bearer "+testConsoleToken)
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}
