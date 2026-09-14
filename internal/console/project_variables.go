package console

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"unicode/utf8"

	actionsv1alpha1 "github.com/kelos-dev/open-actions/api/v1alpha1"
	"github.com/kelos-dev/open-actions/internal/projectvalue"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/util/retry"
)

// A newline can occupy six bytes as a form-encoded CRLF pair.
const variableRequestSize = 6*projectvalue.MaxValueBytes + (8 << 10)

type projectVariable struct {
	Name  string
	Value string
}

type variableCountLimitError struct {
	name string
}

func (e *variableCountLimitError) Error() string {
	return fmt.Sprintf("ConfigMap %q already contains %d variables", e.name, projectvalue.MaxVariableCount)
}

func (h *Handler) updateProjectVariable(writer http.ResponseWriter, request *http.Request, namespace, name string) {
	request.Body = http.MaxBytesReader(writer, request.Body, variableRequestSize)
	if err := request.ParseForm(); err != nil {
		http.Error(writer, "invalid variable update", http.StatusBadRequest)
		return
	}
	if !h.validCSRF(request.PostForm.Get("csrf")) {
		http.Error(writer, "invalid CSRF token", http.StatusForbidden)
		return
	}
	project := &actionsv1alpha1.Project{}
	if err := h.client.Get(request.Context(), types.NamespacedName{Namespace: namespace, Name: name}, project); err != nil {
		h.writeResolutionError(writer, request, fmt.Errorf("load Project %q: %w", name, err))
		return
	}
	if project.Spec.Variables == nil {
		http.Error(writer, fmt.Sprintf("Project %q does not reference a workflow ConfigMap", project.Name), http.StatusConflict)
		return
	}
	action := request.PostForm.Get("action")
	if action != "set" && action != "delete" {
		http.Error(writer, "invalid variable action", http.StatusBadRequest)
		return
	}
	variableName := strings.TrimSpace(request.PostForm.Get("name"))
	if action == "set" {
		variableName = strings.ToUpper(variableName)
		if err := projectvalue.ValidateName(variableName); err != nil {
			http.Error(writer, "invalid variable name: "+err.Error(), http.StatusBadRequest)
			return
		}
	} else if validationErrors := validation.IsConfigMapKey(variableName); len(validationErrors) > 0 {
		http.Error(writer, "invalid ConfigMap key", http.StatusBadRequest)
		return
	}
	value := strings.ReplaceAll(request.PostForm.Get("value"), "\r\n", "\n")
	if action == "set" && (len(value) > projectvalue.MaxValueBytes || !utf8.ValidString(value)) {
		http.Error(writer, fmt.Sprintf("variable value must be valid UTF-8 and no more than %d bytes", projectvalue.MaxValueBytes), http.StatusBadRequest)
		return
	}
	key := types.NamespacedName{Namespace: project.Namespace, Name: project.Spec.Variables.ConfigMapRef.Name}
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		configMap := &corev1.ConfigMap{}
		err := h.client.Get(request.Context(), key, configMap)
		if apierrors.IsNotFound(err) {
			if action == "delete" {
				return nil
			}
			return h.client.Create(request.Context(), &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{Name: key.Name, Namespace: key.Namespace},
				Data:       map[string]string{variableName: value},
			})
		}
		if err != nil {
			return err
		}
		if configMap.Data == nil {
			configMap.Data = map[string]string{}
		}
		if action == "delete" {
			delete(configMap.Data, variableName)
		} else {
			if _, found := configMap.Data[variableName]; !found && len(configMap.Data) >= projectvalue.MaxVariableCount {
				return &variableCountLimitError{name: configMap.Name}
			}
			configMap.Data[variableName] = value
		}
		return h.client.Update(request.Context(), configMap)
	})
	if err != nil {
		var limit *variableCountLimitError
		if errors.As(err, &limit) {
			http.Error(writer, limit.Error(), http.StatusConflict)
			return
		}
		h.writeResolutionError(writer, request, fmt.Errorf("update Project %q ConfigMap %q: %w", project.Name, key.Name, err))
		return
	}
	h.logger.Info("Updated Project variable", "namespace", project.Namespace, "project", project.Name, "config_map", key.Name, "key", variableName, "action", action)
	http.Redirect(writer, request, "/projects/"+url.PathEscape(project.Namespace)+"/"+url.PathEscape(project.Name), http.StatusSeeOther)
}
