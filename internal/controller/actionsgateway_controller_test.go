package controller

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"testing"
	"time"

	actionsv1alpha1 "github.com/kelos-dev/open-actions/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestGatewayConfiguredConditionDescribesLocalValidation(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := actionsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	gateway := &actionsv1alpha1.ActionsGateway{
		ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: "default"},
		Spec: actionsv1alpha1.ActionsGatewaySpec{Source: actionsv1alpha1.ActionsGatewaySource{
			Type: actionsv1alpha1.SourceTypeGitHub,
			GitHub: &actionsv1alpha1.GitHubAppConfiguration{
				AppID: 1, InstallationID: 2,
				PrivateKeySecretRef: corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "github"}, Key: "private-key"},
				WebhookSecretRef:    corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "github"}, Key: "webhook-secret"},
			},
		}},
	}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "github", Namespace: "default"},
		Data: map[string][]byte{
			"private-key":    pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(privateKey)}),
			"webhook-secret": []byte("secret"),
		},
	}
	clusterClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&actionsv1alpha1.ActionsGateway{}).WithObjects(gateway, secret).Build()
	reconciler := &ActionsGatewayReconciler{Client: clusterClient, APIReader: clusterClient}
	if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "default"}}); err != nil {
		t.Fatal(err)
	}
	stored := &actionsv1alpha1.ActionsGateway{}
	if err := clusterClient.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "default"}, stored); err != nil {
		t.Fatal(err)
	}
	configured := meta.FindStatusCondition(stored.Status.Conditions, actionsv1alpha1.ActionsGatewayConditionConfigured)
	if configured == nil || configured.Status != metav1.ConditionTrue || configured.Reason != "ConfigurationValid" {
		t.Fatalf("configured condition = %#v", configured)
	}
}

func TestEarlierGatewayOwnsInstallationAcrossNamespaces(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := actionsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	gateway := &actionsv1alpha1.ActionsGateway{
		ObjectMeta: metav1.ObjectMeta{Name: "first", Namespace: "first", UID: types.UID("first"), CreationTimestamp: metav1.NewTime(time.Unix(100, 0))},
		Spec: actionsv1alpha1.ActionsGatewaySpec{Source: actionsv1alpha1.ActionsGatewaySource{
			Type:   actionsv1alpha1.SourceTypeGitHub,
			GitHub: &actionsv1alpha1.GitHubAppConfiguration{AppID: 1, InstallationID: 3},
		}},
	}
	other := &actionsv1alpha1.ActionsGateway{
		ObjectMeta: metav1.ObjectMeta{Name: "second", Namespace: "second", UID: types.UID("second"), CreationTimestamp: metav1.NewTime(time.Unix(200, 0))},
		Spec: actionsv1alpha1.ActionsGatewaySpec{Source: actionsv1alpha1.ActionsGatewaySource{
			Type:   actionsv1alpha1.SourceTypeGitHub,
			GitHub: &actionsv1alpha1.GitHubAppConfiguration{AppID: 2, InstallationID: 3},
		}},
	}
	reconciler := &ActionsGatewayReconciler{Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(gateway, other).Build()}

	owner, err := reconciler.installationOwner(context.Background(), other)
	if err != nil {
		t.Fatal(err)
	}
	if owner.UID != gateway.UID {
		t.Fatalf("installation owner = %s/%s, want %s/%s", owner.Namespace, owner.Name, gateway.Namespace, gateway.Name)
	}
}
