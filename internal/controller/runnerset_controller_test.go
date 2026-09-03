package controller

import (
	"context"
	"testing"
	"time"

	actionsv1alpha1 "github.com/kelos-dev/open-actions/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

func TestRunnerSetCreatesDesiredRunners(t *testing.T) {
	scheme := runnerSetTestScheme(t)
	replicas := int32(2)
	runnerSet := &actionsv1alpha1.RunnerSet{
		ObjectMeta: metav1.ObjectMeta{Name: "linux", Namespace: "default", UID: types.UID("runner-set-uid")},
		Spec: actionsv1alpha1.RunnerSetSpec{
			Replicas: &replicas,
			Template: actionsv1alpha1.RunnerTemplateSpec{Spec: runnerSetTestRunnerSpec()},
		},
	}
	clusterClient := runnerSetTestClient(scheme, runnerSet)
	reconciler := runnerSetTestReconciler(clusterClient)
	request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(runnerSet)}
	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatal(err)
	}

	runners := &actionsv1alpha1.RunnerList{}
	if err := clusterClient.List(context.Background(), runners, client.InNamespace(runnerSet.Namespace)); err != nil {
		t.Fatal(err)
	}
	if len(runners.Items) != 2 {
		t.Fatalf("managed Runners = %d, want 2", len(runners.Items))
	}
	for index := range runners.Items {
		runner := &runners.Items[index]
		if !metav1.IsControlledBy(runner, runnerSet) {
			t.Errorf("Runner %q is not controlled by RunnerSet %q", runner.Name, runnerSet.Name)
		}
		if runner.Labels[actionsv1alpha1.LabelRunnerSetUID] != string(runnerSet.UID) {
			t.Errorf("Runner %q RunnerSet UID label = %q", runner.Name, runner.Labels[actionsv1alpha1.LabelRunnerSetUID])
		}
		if runner.Spec.Execution.Runner.Image != "runner:test" || len(runner.Spec.Execution.Runner.Env) != 1 || runner.Spec.Execution.Runner.Env[0].Name != "CACHE_URL" || runner.Spec.Execution.Runner.Env[0].Value != "https://cache.example" || len(runner.Spec.Labels) != 2 {
			t.Errorf("Runner %q spec = %#v", runner.Name, runner.Spec)
		}
	}
	stored := &actionsv1alpha1.RunnerSet{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(runnerSet), stored); err != nil {
		t.Fatal(err)
	}
	if stored.Status.Replicas != 2 || stored.Status.ReadyReplicas != 0 {
		t.Fatalf("RunnerSet status = %#v", stored.Status)
	}
	ready := meta.FindStatusCondition(stored.Status.Conditions, actionsv1alpha1.RunnerSetConditionReady)
	if ready == nil || ready.Status != metav1.ConditionFalse || ready.Reason != "RunnersNotReady" {
		t.Fatalf("Ready condition = %#v", ready)
	}
}

func TestRunnerSetDoesNotCreateRunnerFromStaleCache(t *testing.T) {
	scheme := runnerSetTestScheme(t)
	replicas := int32(1)
	runnerSet := &actionsv1alpha1.RunnerSet{
		ObjectMeta: metav1.ObjectMeta{Name: "linux", Namespace: "default", UID: types.UID("runner-set-uid")},
		Spec: actionsv1alpha1.RunnerSetSpec{
			Replicas: &replicas,
			Template: actionsv1alpha1.RunnerTemplateSpec{Spec: runnerSetTestRunnerSpec()},
		},
	}
	liveRunner := runnerSetTestRunner(t, scheme, runnerSet, "runner")
	liveRunner.Labels = map[string]string{actionsv1alpha1.LabelRunnerSetUID: string(runnerSet.UID)}
	cachedClient := runnerSetTestClient(scheme, runnerSet)
	liveReader := runnerSetTestClient(scheme, runnerSet.DeepCopy(), liveRunner)
	reconciler := &RunnerSetReconciler{Client: cachedClient, APIReader: liveReader}
	if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(runnerSet)}); err != nil {
		t.Fatal(err)
	}

	cachedRunners := &actionsv1alpha1.RunnerList{}
	if err := cachedClient.List(context.Background(), cachedRunners); err != nil {
		t.Fatal(err)
	}
	if len(cachedRunners.Items) != 0 {
		t.Fatalf("created %d Runners from a stale cache, want 0", len(cachedRunners.Items))
	}
}

func TestRunnerSetDefaultsToOneRunner(t *testing.T) {
	scheme := runnerSetTestScheme(t)
	runnerSet := &actionsv1alpha1.RunnerSet{
		ObjectMeta: metav1.ObjectMeta{Name: "linux", Namespace: "default", UID: types.UID("runner-set-uid")},
		Spec: actionsv1alpha1.RunnerSetSpec{
			Template: actionsv1alpha1.RunnerTemplateSpec{Spec: runnerSetTestRunnerSpec()},
		},
	}
	clusterClient := runnerSetTestClient(scheme, runnerSet)
	if _, err := runnerSetTestReconciler(clusterClient).Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(runnerSet)}); err != nil {
		t.Fatal(err)
	}
	runners := &actionsv1alpha1.RunnerList{}
	if err := clusterClient.List(context.Background(), runners); err != nil {
		t.Fatal(err)
	}
	if len(runners.Items) != 1 {
		t.Fatalf("managed Runners = %d, want 1", len(runners.Items))
	}
}

func TestDeletingRunnerSetDoesNotCreateRunners(t *testing.T) {
	scheme := runnerSetTestScheme(t)
	deletionTime := metav1.Now()
	runnerSet := &actionsv1alpha1.RunnerSet{
		ObjectMeta: metav1.ObjectMeta{
			Name: "linux", Namespace: "default", UID: types.UID("runner-set-uid"),
			DeletionTimestamp: &deletionTime, Finalizers: []string{"test"},
		},
		Spec: actionsv1alpha1.RunnerSetSpec{
			Template: actionsv1alpha1.RunnerTemplateSpec{Spec: runnerSetTestRunnerSpec()},
		},
	}
	clusterClient := runnerSetTestClient(scheme, runnerSet)
	if _, err := runnerSetTestReconciler(clusterClient).Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(runnerSet)}); err != nil {
		t.Fatal(err)
	}
	runners := &actionsv1alpha1.RunnerList{}
	if err := clusterClient.List(context.Background(), runners); err != nil {
		t.Fatal(err)
	}
	if len(runners.Items) != 0 {
		t.Fatalf("managed Runners = %d, want 0", len(runners.Items))
	}
}

func TestRunnerSetReconcilesTemplate(t *testing.T) {
	scheme := runnerSetTestScheme(t)
	replicas := int32(1)
	runnerSet := &actionsv1alpha1.RunnerSet{
		ObjectMeta: metav1.ObjectMeta{Name: "linux", Namespace: "default", UID: types.UID("runner-set-uid")},
		Spec: actionsv1alpha1.RunnerSetSpec{
			Replicas: &replicas,
			Template: actionsv1alpha1.RunnerTemplateSpec{Spec: runnerSetTestRunnerSpec()},
		},
	}
	runner := runnerSetTestRunner(t, scheme, runnerSet, "runner")
	runner.Spec.Execution.Runner.Image = "stale:test"
	clusterClient := runnerSetTestClient(scheme, runnerSet, runner)
	if _, err := runnerSetTestReconciler(clusterClient).Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(runnerSet)}); err != nil {
		t.Fatal(err)
	}
	updated := &actionsv1alpha1.Runner{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(runner), updated); err != nil {
		t.Fatal(err)
	}
	if updated.Spec.Execution.Runner.Image != "runner:test" {
		t.Fatalf("Runner image = %q", updated.Spec.Execution.Runner.Image)
	}
}

func TestRunnerSetReportsStatus(t *testing.T) {
	scheme := runnerSetTestScheme(t)
	replicas := int32(2)
	runnerSet := &actionsv1alpha1.RunnerSet{
		ObjectMeta: metav1.ObjectMeta{Name: "linux", Namespace: "default", UID: types.UID("runner-set-uid"), Generation: 3},
		Spec: actionsv1alpha1.RunnerSetSpec{
			Replicas: &replicas,
			Template: actionsv1alpha1.RunnerTemplateSpec{Spec: runnerSetTestRunnerSpec()},
		},
	}
	idle := runnerSetTestRunner(t, scheme, runnerSet, "idle")
	idle.Generation = 2
	idle.Status.Conditions = []metav1.Condition{{
		Type: actionsv1alpha1.RunnerConditionReady, Status: metav1.ConditionTrue,
		ObservedGeneration: idle.Generation, Reason: "Ready", LastTransitionTime: metav1.Now(),
	}}
	busy := runnerSetTestRunner(t, scheme, runnerSet, "busy")
	busy.Generation = 1
	busy.Status.WorkflowJobRef = &corev1.LocalObjectReference{Name: "build"}
	busy.Status.Conditions = []metav1.Condition{{
		Type: actionsv1alpha1.RunnerConditionReady, Status: metav1.ConditionTrue,
		ObservedGeneration: busy.Generation, Reason: "Ready", LastTransitionTime: metav1.Now(),
	}}
	clusterClient := runnerSetTestClient(scheme, runnerSet, idle, busy)
	if _, err := runnerSetTestReconciler(clusterClient).Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(runnerSet)}); err != nil {
		t.Fatal(err)
	}

	stored := &actionsv1alpha1.RunnerSet{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(runnerSet), stored); err != nil {
		t.Fatal(err)
	}
	if stored.Status.ObservedGeneration != runnerSet.Generation || stored.Status.Replicas != 2 || stored.Status.ReadyReplicas != 2 || stored.Status.BusyReplicas != 1 || stored.Status.IdleReplicas != 1 {
		t.Fatalf("RunnerSet status = %#v", stored.Status)
	}
	ready := meta.FindStatusCondition(stored.Status.Conditions, actionsv1alpha1.RunnerSetConditionReady)
	if ready == nil || ready.Status != metav1.ConditionTrue || ready.Reason != "RunnersReady" {
		t.Fatalf("Ready condition = %#v", ready)
	}
}

func TestRunnerSetScalesDownIdleRunnerBeforeBusyRunner(t *testing.T) {
	scheme := runnerSetTestScheme(t)
	replicas := int32(1)
	runnerSet := &actionsv1alpha1.RunnerSet{
		ObjectMeta: metav1.ObjectMeta{Name: "linux", Namespace: "default", UID: types.UID("runner-set-uid")},
		Spec: actionsv1alpha1.RunnerSetSpec{
			Replicas: &replicas,
			Template: actionsv1alpha1.RunnerTemplateSpec{Spec: runnerSetTestRunnerSpec()},
		},
	}
	idle := runnerSetTestRunner(t, scheme, runnerSet, "idle")
	idle.Finalizers = []string{"test"}
	busy := runnerSetTestRunner(t, scheme, runnerSet, "busy")
	busy.Finalizers = []string{"test"}
	busy.Status.WorkflowJobRef = &corev1.LocalObjectReference{Name: "build"}
	clusterClient := runnerSetTestClient(scheme, runnerSet, idle, busy)
	if _, err := runnerSetTestReconciler(clusterClient).Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(runnerSet)}); err != nil {
		t.Fatal(err)
	}

	storedIdle := &actionsv1alpha1.Runner{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(idle), storedIdle); err != nil {
		t.Fatal(err)
	}
	if storedIdle.DeletionTimestamp.IsZero() {
		t.Fatal("idle Runner was not selected for scale-down")
	}
	storedBusy := &actionsv1alpha1.Runner{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(busy), storedBusy); err != nil {
		t.Fatal(err)
	}
	if !storedBusy.DeletionTimestamp.IsZero() {
		t.Fatal("busy Runner was selected while an idle Runner was available")
	}
	storedSet := &actionsv1alpha1.RunnerSet{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(runnerSet), storedSet); err != nil {
		t.Fatal(err)
	}
	if storedSet.Status.Replicas != 1 || storedSet.Status.TerminatingReplicas != 1 {
		t.Fatalf("RunnerSet status = %#v", storedSet.Status)
	}
	ready := meta.FindStatusCondition(storedSet.Status.Conditions, actionsv1alpha1.RunnerSetConditionReady)
	if ready == nil || ready.Reason != "RunnersTerminating" {
		t.Fatalf("Ready condition = %#v", ready)
	}
}

func TestRunnerSetScaleDownPreference(t *testing.T) {
	tests := []struct {
		name      string
		configure func(first, second *actionsv1alpha1.Runner)
		expected  string
	}{
		{
			name: "not ready before ready",
			configure: func(first, _ *actionsv1alpha1.Runner) {
				first.Status.Conditions = []metav1.Condition{{
					Type: actionsv1alpha1.RunnerConditionReady, Status: metav1.ConditionTrue,
					Reason: "Ready", LastTransitionTime: metav1.Now(),
				}}
			},
			expected: "second",
		},
		{
			name: "newest before oldest",
			configure: func(first, second *actionsv1alpha1.Runner) {
				first.CreationTimestamp = metav1.NewTime(time.Unix(100, 0))
				second.CreationTimestamp = metav1.NewTime(time.Unix(200, 0))
			},
			expected: "second",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			scheme := runnerSetTestScheme(t)
			replicas := int32(1)
			runnerSet := &actionsv1alpha1.RunnerSet{
				ObjectMeta: metav1.ObjectMeta{Name: "linux", Namespace: "default", UID: types.UID("runner-set-uid")},
				Spec: actionsv1alpha1.RunnerSetSpec{
					Replicas: &replicas,
					Template: actionsv1alpha1.RunnerTemplateSpec{Spec: runnerSetTestRunnerSpec()},
				},
			}
			first := runnerSetTestRunner(t, scheme, runnerSet, "first")
			first.Finalizers = []string{"test"}
			second := runnerSetTestRunner(t, scheme, runnerSet, "second")
			second.Finalizers = []string{"test"}
			test.configure(first, second)
			clusterClient := runnerSetTestClient(scheme, runnerSet, first, second)
			if _, err := runnerSetTestReconciler(clusterClient).Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(runnerSet)}); err != nil {
				t.Fatal(err)
			}

			for _, runner := range []*actionsv1alpha1.Runner{first, second} {
				stored := &actionsv1alpha1.Runner{}
				if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(runner), stored); err != nil {
					t.Fatal(err)
				}
				expectDeleting := runner.Name == test.expected
				if expectDeleting != !stored.DeletionTimestamp.IsZero() {
					t.Errorf("Runner %q deletion timestamp = %v, expected selected Runner %q", runner.Name, stored.DeletionTimestamp, test.expected)
				}
			}
		})
	}
}

func runnerSetTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := actionsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	return scheme
}

func runnerSetTestClient(scheme *runtime.Scheme, objects ...client.Object) client.Client {
	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&actionsv1alpha1.Runner{}, &actionsv1alpha1.RunnerSet{}).
		WithIndex(&actionsv1alpha1.Runner{}, runnerSetOwnerUIDIndex, indexRunnerSetOwnerUID).
		WithObjects(objects...).
		Build()
}

func runnerSetTestReconciler(clusterClient client.Client) *RunnerSetReconciler {
	return &RunnerSetReconciler{Client: clusterClient, APIReader: clusterClient}
}

func runnerSetTestRunner(t *testing.T, scheme *runtime.Scheme, runnerSet *actionsv1alpha1.RunnerSet, name string) *actionsv1alpha1.Runner {
	t.Helper()
	runner := &actionsv1alpha1.Runner{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: runnerSet.Namespace, UID: types.UID(name + "-uid")},
		Spec:       runnerSetTestRunnerSpec(),
	}
	if err := controllerutil.SetControllerReference(runnerSet, runner, scheme); err != nil {
		t.Fatal(err)
	}
	return runner
}

func runnerSetTestRunnerSpec() actionsv1alpha1.RunnerSpec {
	return actionsv1alpha1.RunnerSpec{
		ProjectRef: corev1.LocalObjectReference{Name: "default"},
		Execution: actionsv1alpha1.RunnerExecutionSpec{
			Runner: actionsv1alpha1.RunnerContainerSpec{
				Image: "runner:test",
				Env:   []corev1.EnvVar{{Name: "CACHE_URL", Value: "https://cache.example"}},
			},
		},
		Labels: []string{"self-hosted", "linux"},
	}
}
