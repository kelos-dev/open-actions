package controller

import (
	"context"
	"fmt"
	"time"

	actionsv1alpha1 "github.com/kelos-dev/open-actions/api/v1alpha1"
	githubclient "github.com/kelos-dev/open-actions/internal/github"
	corev1 "k8s.io/api/core/v1"
	apiEquality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

type ProjectReconciler struct {
	client.Client
	APIReader client.Reader
	Recorder  events.EventRecorder
}

func (r *ProjectReconciler) Reconcile(ctx context.Context, request ctrl.Request) (ctrl.Result, error) {
	project := &actionsv1alpha1.Project{}
	if err := r.Get(ctx, request.NamespacedName, project); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	status := metav1.ConditionTrue
	reason := "ConfigurationValid"
	message := "Referenced credentials and workflow values are present and locally valid"
	if owner, err := r.installationOwner(ctx, project); err != nil {
		return ctrl.Result{}, err
	} else if owner.UID != project.UID {
		status = metav1.ConditionFalse
		reason = "DuplicateInstallation"
		message = fmt.Sprintf("Project %q in namespace %q owns this GitHub App installation", owner.Name, owner.Namespace)
	} else if invalidReason, err := r.validate(ctx, project); err != nil {
		status = metav1.ConditionFalse
		reason = invalidReason
		message = err.Error()
	}

	before := project.Status.DeepCopy()
	project.Status.ObservedGeneration = project.Generation
	meta.SetStatusCondition(&project.Status.Conditions, metav1.Condition{
		Type:               actionsv1alpha1.ProjectConditionConfigured,
		Status:             status,
		ObservedGeneration: project.Generation,
		Reason:             reason,
		Message:            message,
	})
	if !apiEquality.Semantic.DeepEqual(before, &project.Status) {
		if err := r.Status().Update(ctx, project); err != nil {
			return ctrl.Result{}, err
		}
		if status == metav1.ConditionFalse {
			recordConditionWarning(r.Recorder, project, before.Conditions, project.Status.Conditions, actionsv1alpha1.ProjectConditionConfigured)
		}
	}
	return ctrl.Result{RequeueAfter: 5 * time.Minute}, nil
}

func (r *ProjectReconciler) validate(ctx context.Context, project *actionsv1alpha1.Project) (string, error) {
	github := project.Spec.Source.GitHub
	privateKey, err := secretValue(ctx, r.APIReader, project.Namespace, github.PrivateKeySecretRef)
	if err != nil {
		return "CredentialsUnavailable", err
	}
	if err := githubclient.ValidatePrivateKey(privateKey); err != nil {
		return "InvalidCredentials", fmt.Errorf("validate GitHub App private key: %w", err)
	}
	if _, err := secretValue(ctx, r.APIReader, project.Namespace, github.WebhookSecretRef); err != nil {
		return "CredentialsUnavailable", err
	}
	if err := validateProjectSecretValues(ctx, r.APIReader, project); err != nil {
		return "ProjectValuesUnavailable", err
	}
	if err := validateProjectVariableValues(ctx, r.APIReader, project); err != nil {
		return "ProjectValuesUnavailable", err
	}
	return "", nil
}

func (r *ProjectReconciler) installationOwner(ctx context.Context, project *actionsv1alpha1.Project) (*actionsv1alpha1.Project, error) {
	projects := &actionsv1alpha1.ProjectList{}
	if err := r.List(ctx, projects); err != nil {
		return nil, err
	}
	var owner *actionsv1alpha1.Project
	for index := range projects.Items {
		candidate := &projects.Items[index]
		if candidate.Spec.Source.GitHub.InstallationID == project.Spec.Source.GitHub.InstallationID && (owner == nil || projectPrecedes(candidate, owner)) {
			owner = candidate
		}
	}
	if owner == nil {
		return nil, fmt.Errorf("GitHub App installation %d has no Project owner", project.Spec.Source.GitHub.InstallationID)
	}
	return owner, nil
}

func projectPrecedes(left, right *actionsv1alpha1.Project) bool {
	if !left.CreationTimestamp.Equal(&right.CreationTimestamp) {
		return left.CreationTimestamp.Before(&right.CreationTimestamp)
	}
	if left.Namespace != right.Namespace {
		return left.Namespace < right.Namespace
	}
	if left.Name != right.Name {
		return left.Name < right.Name
	}
	return left.UID < right.UID
}

func (r *ProjectReconciler) SetupWithManager(manager ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(manager).
		For(&actionsv1alpha1.Project{}).
		Watches(&actionsv1alpha1.Project{}, handler.EnqueueRequestsFromMapFunc(r.projectsForInstallation)).
		WatchesMetadata(&corev1.Secret{}, handler.EnqueueRequestsFromMapFunc(r.projectsForValueSource)).
		WatchesMetadata(&corev1.ConfigMap{}, handler.EnqueueRequestsFromMapFunc(r.projectsForValueSource)).
		Complete(r)
}

func (r *ProjectReconciler) projectsForValueSource(ctx context.Context, object client.Object) []reconcile.Request {
	projects := &actionsv1alpha1.ProjectList{}
	if err := r.List(ctx, projects, client.InNamespace(object.GetNamespace())); err != nil {
		return nil
	}
	requests := []reconcile.Request{}
	for index := range projects.Items {
		project := &projects.Items[index]
		matched := false
		switch object.GetObjectKind().GroupVersionKind() {
		case corev1.SchemeGroupVersion.WithKind("Secret"):
			github := project.Spec.Source.GitHub
			matched = github != nil && (github.PrivateKeySecretRef.Name == object.GetName() || github.WebhookSecretRef.Name == object.GetName())
			matched = matched || project.Spec.Secrets != nil && project.Spec.Secrets.SecretRef.Name == object.GetName()
		case corev1.SchemeGroupVersion.WithKind("ConfigMap"):
			matched = project.Spec.Variables != nil && project.Spec.Variables.ConfigMapRef.Name == object.GetName()
		}
		if matched {
			requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(project)})
		}
	}
	return requests
}

func (r *ProjectReconciler) projectsForInstallation(ctx context.Context, object client.Object) []reconcile.Request {
	project, ok := object.(*actionsv1alpha1.Project)
	if !ok || project.Spec.Source.GitHub == nil {
		return nil
	}
	projects := &actionsv1alpha1.ProjectList{}
	if err := r.List(ctx, projects); err != nil {
		return nil
	}
	requests := []reconcile.Request{}
	for index := range projects.Items {
		candidate := &projects.Items[index]
		if candidate.Spec.Source.GitHub != nil && candidate.Spec.Source.GitHub.InstallationID == project.Spec.Source.GitHub.InstallationID {
			requests = append(requests, requestFor(candidate))
		}
	}
	return requests
}

func requestFor(object client.Object) reconcile.Request {
	return reconcile.Request{NamespacedName: types.NamespacedName{Namespace: object.GetNamespace(), Name: object.GetName()}}
}
