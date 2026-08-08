package controller

import (
	"context"
	"fmt"
	"time"

	actionsv1alpha1 "github.com/kelos-dev/open-actions/api/v1alpha1"
	githubclient "github.com/kelos-dev/open-actions/internal/github"
	apiEquality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

type ActionsGatewayReconciler struct {
	client.Client
	APIReader client.Reader
}

func (r *ActionsGatewayReconciler) Reconcile(ctx context.Context, request ctrl.Request) (ctrl.Result, error) {
	gateway := &actionsv1alpha1.ActionsGateway{}
	if err := r.Get(ctx, request.NamespacedName, gateway); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	status := metav1.ConditionTrue
	reason := "ConfigurationValid"
	message := "Referenced credentials are present and locally valid"
	if owner, err := r.installationOwner(ctx, gateway); err != nil {
		return ctrl.Result{}, err
	} else if owner.UID != gateway.UID {
		status = metav1.ConditionFalse
		reason = "DuplicateInstallation"
		message = fmt.Sprintf("ActionsGateway %q in namespace %q owns this GitHub App installation", owner.Name, owner.Namespace)
	} else if invalidReason, err := r.validate(ctx, gateway); err != nil {
		status = metav1.ConditionFalse
		reason = invalidReason
		message = err.Error()
	}

	before := gateway.Status.DeepCopy()
	gateway.Status.ObservedGeneration = gateway.Generation
	meta.SetStatusCondition(&gateway.Status.Conditions, metav1.Condition{
		Type:               actionsv1alpha1.ActionsGatewayConditionConfigured,
		Status:             status,
		ObservedGeneration: gateway.Generation,
		Reason:             reason,
		Message:            message,
	})
	if !apiEquality.Semantic.DeepEqual(before, &gateway.Status) {
		if err := r.Status().Update(ctx, gateway); err != nil {
			return ctrl.Result{}, err
		}
	}
	return ctrl.Result{RequeueAfter: 5 * time.Minute}, nil
}

func (r *ActionsGatewayReconciler) validate(ctx context.Context, gateway *actionsv1alpha1.ActionsGateway) (string, error) {
	github := gateway.Spec.Source.GitHub
	privateKey, err := secretValue(ctx, r.APIReader, gateway.Namespace, github.PrivateKeySecretRef)
	if err != nil {
		return "CredentialsUnavailable", err
	}
	if err := githubclient.ValidatePrivateKey(privateKey); err != nil {
		return "InvalidCredentials", fmt.Errorf("validate GitHub App private key: %w", err)
	}
	if _, err := secretValue(ctx, r.APIReader, gateway.Namespace, github.WebhookSecretRef); err != nil {
		return "CredentialsUnavailable", err
	}
	return "", nil
}

func (r *ActionsGatewayReconciler) installationOwner(ctx context.Context, gateway *actionsv1alpha1.ActionsGateway) (*actionsv1alpha1.ActionsGateway, error) {
	gateways := &actionsv1alpha1.ActionsGatewayList{}
	if err := r.List(ctx, gateways); err != nil {
		return nil, err
	}
	var owner *actionsv1alpha1.ActionsGateway
	for index := range gateways.Items {
		candidate := &gateways.Items[index]
		if candidate.Spec.Source.GitHub.InstallationID == gateway.Spec.Source.GitHub.InstallationID && (owner == nil || gatewayPrecedes(candidate, owner)) {
			owner = candidate
		}
	}
	if owner == nil {
		return nil, fmt.Errorf("GitHub App installation %d has no ActionsGateway owner", gateway.Spec.Source.GitHub.InstallationID)
	}
	return owner, nil
}

func gatewayPrecedes(left, right *actionsv1alpha1.ActionsGateway) bool {
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

func (r *ActionsGatewayReconciler) SetupWithManager(manager ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(manager).
		For(&actionsv1alpha1.ActionsGateway{}).
		Watches(&actionsv1alpha1.ActionsGateway{}, handler.EnqueueRequestsFromMapFunc(r.gatewaysForInstallation)).
		Complete(r)
}

func (r *ActionsGatewayReconciler) gatewaysForInstallation(ctx context.Context, object client.Object) []reconcile.Request {
	gateway, ok := object.(*actionsv1alpha1.ActionsGateway)
	if !ok || gateway.Spec.Source.GitHub == nil {
		return nil
	}
	gateways := &actionsv1alpha1.ActionsGatewayList{}
	if err := r.List(ctx, gateways); err != nil {
		return nil
	}
	requests := []reconcile.Request{}
	for index := range gateways.Items {
		candidate := &gateways.Items[index]
		if candidate.Spec.Source.GitHub != nil && candidate.Spec.Source.GitHub.InstallationID == gateway.Spec.Source.GitHub.InstallationID {
			requests = append(requests, requestFor(candidate))
		}
	}
	return requests
}

func requestFor(object client.Object) reconcile.Request {
	return reconcile.Request{NamespacedName: types.NamespacedName{Namespace: object.GetNamespace(), Name: object.GetName()}}
}
