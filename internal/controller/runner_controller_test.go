package controller

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	actionsv1alpha1 "github.com/kelos-dev/open-actions/api/v1alpha1"
	"github.com/kelos-dev/open-actions/internal/artifact"
	"github.com/kelos-dev/open-actions/internal/eventsnapshot"
	githubclient "github.com/kelos-dev/open-actions/internal/github"
	"github.com/kelos-dev/open-actions/internal/runner"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

func TestRunnerBuildsOwnedJob(t *testing.T) {
	scheme := runnerTestScheme(t)
	reconciler := &RunnerReconciler{Client: fake.NewClientBuilder().WithScheme(scheme).Build()}
	run := &actionsv1alpha1.WorkflowRun{ObjectMeta: metav1.ObjectMeta{Name: "ci", Namespace: "default", UID: types.UID("run-uid")}}
	project := &actionsv1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: "default", UID: types.UID("project-uid")},
	}
	runnerObject := &actionsv1alpha1.Runner{
		ObjectMeta: metav1.ObjectMeta{Name: "runner-1", Namespace: "default", UID: types.UID("runner-uid")},
		Spec: actionsv1alpha1.RunnerSpec{Execution: actionsv1alpha1.RunnerExecutionSpec{
			Image:           "runner:test",
			ImagePullPolicy: corev1.PullAlways,
			Resources: &actionsv1alpha1.RunnerResources{Requests: actionsv1alpha1.RunnerResourceList{
				corev1.ResourceCPU: resource.MustParse("1"),
			}},
		}},
	}
	workflowJob := &actionsv1alpha1.WorkflowJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "ci-build",
			Namespace:   "default",
			UID:         types.UID("workflow-job-uid"),
			Labels:      map[string]string{actionsv1alpha1.LabelWorkflowJob: "build"},
			Annotations: map[string]string{actionsv1alpha1.AnnotationRunnerResultVersion: jobResultVersion},
		},
		Spec: actionsv1alpha1.WorkflowJobSpec{JobID: "build", DisplayName: "Build and test", TimeoutSeconds: 90 * 60},
	}
	job, err := reconciler.buildJob(workflowJob, run, project, runnerObject)
	if err != nil {
		t.Fatalf("build job: %v", err)
	}
	container := job.Spec.Template.Spec.Containers[0]
	if container.Image != "runner:test" {
		t.Errorf("image = %q", container.Image)
	}
	if container.ImagePullPolicy != corev1.PullAlways {
		t.Errorf("image pull policy = %q", container.ImagePullPolicy)
	}
	if container.Resources.Requests.Cpu().String() != "1" {
		t.Errorf("cpu request = %s", container.Resources.Requests.Cpu().String())
	}
	if strings.Join(container.Args, " ") != "--job-file=/var/run/open-actions/job.json --result-file=/dev/termination-log --workspace=/workspace/repository" {
		t.Errorf("args = %v", container.Args)
	}
	if container.TerminationMessagePath != jobResultPath || container.TerminationMessagePolicy != corev1.TerminationMessageReadFile {
		t.Errorf("termination message = %q, policy = %q", container.TerminationMessagePath, container.TerminationMessagePolicy)
	}
	if len(container.Env) != 2 || container.Env[0].Name != runner.GitHubTokenEnvVar || container.Env[0].ValueFrom.SecretKeyRef.Key != jobTokenSecretKey ||
		container.Env[1].Name != runner.ActionTokenEnvVar || container.Env[1].ValueFrom.SecretKeyRef.Key != actionTokenSecretKey {
		t.Fatalf("runner credential environment = %#v", container.Env)
	}
	workspaceMount := ""
	for _, mount := range container.VolumeMounts {
		if mount.Name == workspaceVolume {
			workspaceMount = mount.MountPath
		}
	}
	if workspaceMount == "" || !strings.HasPrefix(workspacePath, workspaceMount+"/") {
		t.Fatalf("workspace path %q must be below volume mount %q", workspacePath, workspaceMount)
	}
	if job.Labels[actionsv1alpha1.LabelWorkflowJob] != "build" {
		t.Errorf("workflow job label = %q", job.Labels[actionsv1alpha1.LabelWorkflowJob])
	}
	if job.Annotations[actionsv1alpha1.AnnotationWorkflowJobDisplayName] != "Build and test" ||
		job.Spec.Template.Annotations[actionsv1alpha1.AnnotationWorkflowJobDisplayName] != "Build and test" {
		t.Errorf("workflow job display name annotations = job %q, pod %q", job.Annotations[actionsv1alpha1.AnnotationWorkflowJobDisplayName], job.Spec.Template.Annotations[actionsv1alpha1.AnnotationWorkflowJobDisplayName])
	}
	if job.Annotations[actionsv1alpha1.AnnotationRunnerResultVersion] != jobResultVersion || job.Spec.Template.Annotations[actionsv1alpha1.AnnotationRunnerResultVersion] != jobResultVersion {
		t.Errorf("runner result annotations = job %q, pod %q", job.Annotations[actionsv1alpha1.AnnotationRunnerResultVersion], job.Spec.Template.Annotations[actionsv1alpha1.AnnotationRunnerResultVersion])
	}
	if len(job.OwnerReferences) != 1 || job.OwnerReferences[0].UID != workflowJob.UID {
		t.Errorf("owner references = %#v", job.OwnerReferences)
	}
	if job.Spec.TTLSecondsAfterFinished != nil {
		t.Error("job TTL must not start until the terminal result is recorded")
	}
	if job.Spec.ActiveDeadlineSeconds == nil || *job.Spec.ActiveDeadlineSeconds != 90*60 {
		t.Errorf("active deadline = %v", job.Spec.ActiveDeadlineSeconds)
	}
	if job.Spec.Template.Spec.TerminationGracePeriodSeconds != nil {
		t.Errorf("termination grace period = %v, want nil", job.Spec.Template.Spec.TerminationGracePeriodSeconds)
	}
	if job.Labels[actionsv1alpha1.LabelRunnerUID] != string(runnerObject.UID) {
		t.Errorf("runner label = %q", job.Labels[actionsv1alpha1.LabelRunnerUID])
	}
	if job.Spec.Template.Spec.RestartPolicy != corev1.RestartPolicyNever {
		t.Errorf("restart policy = %q", job.Spec.Template.Spec.RestartPolicy)
	}
	if job.Spec.Template.Spec.AutomountServiceAccountToken == nil || *job.Spec.Template.Spec.AutomountServiceAccountToken {
		t.Error("service account token must not be mounted")
	}
	if job.Spec.Template.Spec.SecurityContext == nil ||
		job.Spec.Template.Spec.SecurityContext.RunAsNonRoot == nil || !*job.Spec.Template.Spec.SecurityContext.RunAsNonRoot ||
		job.Spec.Template.Spec.SecurityContext.SeccompProfile == nil || job.Spec.Template.Spec.SecurityContext.SeccompProfile.Type != corev1.SeccompProfileTypeRuntimeDefault {
		t.Errorf("pod security context = %#v", job.Spec.Template.Spec.SecurityContext)
	}
	if container.SecurityContext == nil ||
		container.SecurityContext.AllowPrivilegeEscalation == nil || *container.SecurityContext.AllowPrivilegeEscalation ||
		container.SecurityContext.Capabilities == nil || !slices.Equal(container.SecurityContext.Capabilities.Drop, []corev1.Capability{"ALL"}) {
		t.Errorf("container security context = %#v", container.SecurityContext)
	}
	if len(job.Spec.Template.Spec.Volumes) != 2 {
		t.Errorf("volumes = %#v", job.Spec.Template.Spec.Volumes)
	}
}

func TestRunnerUsesConfiguredTerminationGracePeriod(t *testing.T) {
	scheme := runnerTestScheme(t)
	reconciler := &RunnerReconciler{Client: fake.NewClientBuilder().WithScheme(scheme).Build()}
	terminationGracePeriodSeconds := int64(0)
	runnerObject := &actionsv1alpha1.Runner{Spec: actionsv1alpha1.RunnerSpec{Execution: actionsv1alpha1.RunnerExecutionSpec{
		Image:                         "runner:test",
		TerminationGracePeriodSeconds: &terminationGracePeriodSeconds,
	}}}

	job, err := reconciler.buildJob(
		&actionsv1alpha1.WorkflowJob{ObjectMeta: metav1.ObjectMeta{Name: "ci-build", Namespace: "default", UID: types.UID("workflow-job-uid")}},
		&actionsv1alpha1.WorkflowRun{},
		&actionsv1alpha1.Project{},
		runnerObject,
	)
	if err != nil {
		t.Fatal(err)
	}
	if job.Spec.Template.Spec.TerminationGracePeriodSeconds == nil || *job.Spec.Template.Spec.TerminationGracePeriodSeconds != 0 {
		t.Fatalf("termination grace period = %v, want 0", job.Spec.Template.Spec.TerminationGracePeriodSeconds)
	}
}

func TestRunnerMountsGitHubEventSnapshot(t *testing.T) {
	scheme := runnerTestScheme(t)
	reconciler := &RunnerReconciler{Client: fake.NewClientBuilder().WithScheme(scheme).Build()}
	run := &actionsv1alpha1.WorkflowRun{ObjectMeta: metav1.ObjectMeta{
		Name: "ci", Namespace: "default", UID: types.UID("run-uid"),
		Annotations: map[string]string{eventsnapshot.Annotation: "event-snapshot"},
	}}
	workflowJob := &actionsv1alpha1.WorkflowJob{ObjectMeta: metav1.ObjectMeta{Name: "build", Namespace: "default", UID: types.UID("job-uid")}}
	runnerObject := &actionsv1alpha1.Runner{Spec: actionsv1alpha1.RunnerSpec{Execution: actionsv1alpha1.RunnerExecutionSpec{Image: "runner:test"}}}
	job, err := reconciler.buildJob(workflowJob, run, &actionsv1alpha1.Project{}, runnerObject)
	if err != nil {
		t.Fatal(err)
	}
	container := job.Spec.Template.Spec.Containers[0]
	if !slices.Contains(container.Args, "--event-file="+jobEventMountPath+"/"+eventsnapshot.DataKey) ||
		!slices.Contains(container.VolumeMounts, corev1.VolumeMount{Name: jobEventVolume, MountPath: jobEventMountPath, ReadOnly: true}) {
		t.Fatalf("runner event snapshot configuration = args %#v, mounts %#v", container.Args, container.VolumeMounts)
	}
	for _, volume := range job.Spec.Template.Spec.Volumes {
		if volume.Name == jobEventVolume && volume.Secret != nil && volume.Secret.SecretName == "event-snapshot" {
			return
		}
	}
	t.Fatalf("runner volumes = %#v", job.Spec.Template.Spec.Volumes)
}

func TestRunnerCapsNativeJobDeadline(t *testing.T) {
	scheme := runnerTestScheme(t)
	reconciler := &RunnerReconciler{Client: fake.NewClientBuilder().WithScheme(scheme).Build(), MaxJobTimeout: 45 * time.Minute}
	workflowJob := &actionsv1alpha1.WorkflowJob{
		ObjectMeta: metav1.ObjectMeta{Name: "ci-build", Namespace: "default", UID: types.UID("workflow-job-uid")},
		Spec:       actionsv1alpha1.WorkflowJobSpec{TimeoutSeconds: 90 * 60},
	}
	nativeJob, err := reconciler.buildJob(
		workflowJob,
		&actionsv1alpha1.WorkflowRun{},
		&actionsv1alpha1.Project{},
		&actionsv1alpha1.Runner{Spec: actionsv1alpha1.RunnerSpec{Execution: actionsv1alpha1.RunnerExecutionSpec{Image: "runner:test"}}},
	)
	if err != nil {
		t.Fatal(err)
	}
	if nativeJob.Spec.ActiveDeadlineSeconds == nil || *nativeJob.Spec.ActiveDeadlineSeconds != 45*60 {
		t.Fatalf("active deadline = %v, want %d", nativeJob.Spec.ActiveDeadlineSeconds, 45*60)
	}
}

func TestRunnerUsesDefaultDeadlineWhenWorkflowJobOmitsTimeout(t *testing.T) {
	for _, test := range []struct {
		name    string
		maximum time.Duration
		want    time.Duration
	}{
		{name: "maximum above default", maximum: 12 * time.Hour, want: defaultJobTimeout},
		{name: "maximum below default", maximum: 90 * time.Minute, want: 90 * time.Minute},
	} {
		t.Run(test.name, func(t *testing.T) {
			scheme := runnerTestScheme(t)
			reconciler := &RunnerReconciler{Client: fake.NewClientBuilder().WithScheme(scheme).Build(), MaxJobTimeout: test.maximum}
			nativeJob, err := reconciler.buildJob(
				&actionsv1alpha1.WorkflowJob{ObjectMeta: metav1.ObjectMeta{Name: "ci-build", Namespace: "default", UID: types.UID("workflow-job-uid")}},
				&actionsv1alpha1.WorkflowRun{},
				&actionsv1alpha1.Project{},
				&actionsv1alpha1.Runner{Spec: actionsv1alpha1.RunnerSpec{Execution: actionsv1alpha1.RunnerExecutionSpec{Image: "runner:test"}}},
			)
			if err != nil {
				t.Fatal(err)
			}
			wantSeconds := int64(test.want / time.Second)
			if nativeJob.Spec.ActiveDeadlineSeconds == nil || *nativeJob.Spec.ActiveDeadlineSeconds != wantSeconds {
				t.Fatalf("active deadline = %v, want %d", nativeJob.Spec.ActiveDeadlineSeconds, wantSeconds)
			}
		})
	}
}

func TestArtifactTokenLifetimeCoversEffectiveJobLifetime(t *testing.T) {
	reconciler := &RunnerReconciler{MaxJobTimeout: 2 * time.Hour}
	workflowJob := &actionsv1alpha1.WorkflowJob{Spec: actionsv1alpha1.WorkflowJobSpec{TimeoutSeconds: 3 * 60 * 60}}
	want := 2*time.Hour + jobStartTimeout + runner.CleanupTimeout
	if got := reconciler.artifactTokenLifetime(workflowJob); got != want {
		t.Fatalf("artifact token lifetime = %s, want %s", got, want)
	}
}

func TestRunnerMountsNeedsContextForDependentJob(t *testing.T) {
	scheme := runnerTestScheme(t)
	reconciler := &RunnerReconciler{Client: fake.NewClientBuilder().WithScheme(scheme).Build()}
	workflowJob := &actionsv1alpha1.WorkflowJob{
		ObjectMeta: metav1.ObjectMeta{Name: "report", Namespace: "default", UID: types.UID("job-uid")},
		Spec:       actionsv1alpha1.WorkflowJobSpec{Needs: []string{"build"}},
	}
	runnerObject := &actionsv1alpha1.Runner{Spec: actionsv1alpha1.RunnerSpec{Execution: actionsv1alpha1.RunnerExecutionSpec{Image: "runner:test"}}}
	job, err := reconciler.buildJob(workflowJob, &actionsv1alpha1.WorkflowRun{}, &actionsv1alpha1.Project{}, runnerObject)
	if err != nil {
		t.Fatal(err)
	}
	container := job.Spec.Template.Spec.Containers[0]
	if !slices.Contains(container.Args, "--needs-file="+jobNeedsMountPath+"/"+jobNeedsKey) ||
		!slices.Contains(container.VolumeMounts, corev1.VolumeMount{Name: jobNeedsVolume, MountPath: jobNeedsMountPath, ReadOnly: true}) {
		t.Fatalf("runner needs context configuration = args %#v, mounts %#v", container.Args, container.VolumeMounts)
	}
	for _, volume := range job.Spec.Template.Spec.Volumes {
		if volume.Name == jobNeedsVolume && volume.ConfigMap != nil && volume.ConfigMap.Name == childName(workflowJob.Name, "needs") {
			return
		}
	}
	t.Fatalf("runner volumes = %#v", job.Spec.Template.Spec.Volumes)
}

func TestRunnerBuildsJobWithProjectValues(t *testing.T) {
	scheme := runnerTestScheme(t)
	reconciler := &RunnerReconciler{Client: fake.NewClientBuilder().WithScheme(scheme).Build()}
	project := &actionsv1alpha1.Project{
		Spec: actionsv1alpha1.ProjectSpec{
			Secrets:   &actionsv1alpha1.ProjectSecretSource{SecretRef: corev1.LocalObjectReference{Name: "project-secrets"}},
			Variables: &actionsv1alpha1.ProjectVariableSource{ConfigMapRef: corev1.LocalObjectReference{Name: "project-variables"}},
		},
	}
	runnerObject := &actionsv1alpha1.Runner{Spec: actionsv1alpha1.RunnerSpec{Execution: actionsv1alpha1.RunnerExecutionSpec{Image: "runner:test"}}}
	workflowJob := &actionsv1alpha1.WorkflowJob{ObjectMeta: metav1.ObjectMeta{Name: "ci-build", Namespace: "default", UID: types.UID("workflow-job-uid")}}
	job, err := reconciler.buildJob(workflowJob, &actionsv1alpha1.WorkflowRun{}, project, runnerObject)
	if err != nil {
		t.Fatal(err)
	}
	container := job.Spec.Template.Spec.Containers[0]
	if !slices.Contains(container.Args, "--secrets-directory="+jobContextMountPath+"/secrets") ||
		!slices.Contains(container.Args, "--variables-directory="+jobContextMountPath+"/variables") {
		t.Fatalf("runner args = %v", container.Args)
	}
	if !slices.Contains(container.VolumeMounts, corev1.VolumeMount{Name: jobSecretsVolume, MountPath: jobContextMountPath + "/secrets", ReadOnly: true}) ||
		!slices.Contains(container.VolumeMounts, corev1.VolumeMount{Name: jobVariablesVolume, MountPath: jobContextMountPath + "/variables", ReadOnly: true}) {
		t.Fatalf("runner volume mounts = %#v", container.VolumeMounts)
	}
	var secret *corev1.SecretVolumeSource
	var variables *corev1.ConfigMapVolumeSource
	for _, volume := range job.Spec.Template.Spec.Volumes {
		switch volume.Name {
		case jobSecretsVolume:
			secret = volume.Secret
		case jobVariablesVolume:
			variables = volume.ConfigMap
		}
	}
	if secret == nil || secret.SecretName != "project-secrets" {
		t.Fatalf("secret volume = %#v", secret)
	}
	if variables == nil || variables.Name != "project-variables" {
		t.Fatalf("variable volume = %#v", variables)
	}
}

func TestRunnerBuildsJobWithArtifactCredential(t *testing.T) {
	scheme := runnerTestScheme(t)
	reconciler := &RunnerReconciler{
		Client:                   fake.NewClientBuilder().WithScheme(scheme).Build(),
		ArtifactResultsURL:       "http://open-actions-artifacts.open-actions-system.svc",
		ArtifactMaxRetentionDays: 30,
	}
	run := &actionsv1alpha1.WorkflowRun{}
	workflowJob := &actionsv1alpha1.WorkflowJob{ObjectMeta: metav1.ObjectMeta{Name: "build", Namespace: "default", UID: types.UID("job-uid")}}
	runnerObject := &actionsv1alpha1.Runner{Spec: actionsv1alpha1.RunnerSpec{Execution: actionsv1alpha1.RunnerExecutionSpec{Image: "runner:test"}}}
	job, err := reconciler.buildJob(workflowJob, run, &actionsv1alpha1.Project{}, runnerObject)
	if err != nil {
		t.Fatal(err)
	}
	environment := map[string]corev1.EnvVar{}
	for _, variable := range job.Spec.Template.Spec.Containers[0].Env {
		environment[variable.Name] = variable
	}
	if environment[runner.ArtifactResultsURLEnvVar].Value != reconciler.ArtifactResultsURL || environment["GITHUB_RETENTION_DAYS"].Value != "30" {
		t.Fatalf("artifact environment = %#v", environment)
	}
	for _, name := range []string{"GITHUB_RUN_ID", "GITHUB_RUN_ATTEMPT"} {
		if _, found := environment[name]; found {
			t.Fatalf("artifact environment contains runner-owned variable %q", name)
		}
	}
	token := environment[runner.ArtifactTokenEnvVar].ValueFrom
	if token == nil || token.SecretKeyRef == nil || token.SecretKeyRef.Name != childName(workflowJob.Name, "auth") || token.SecretKeyRef.Key != artifact.TokenSecretKey {
		t.Fatalf("artifact token source = %#v", token)
	}
}

func TestRunnerBuildsDockerEnabledJob(t *testing.T) {
	scheme := runnerTestScheme(t)
	reconciler := &RunnerReconciler{Client: fake.NewClientBuilder().WithScheme(scheme).Build()}
	runnerObject := &actionsv1alpha1.Runner{
		ObjectMeta: metav1.ObjectMeta{Name: "runner-1", Namespace: "default", UID: types.UID("runner-uid")},
		Spec: actionsv1alpha1.RunnerSpec{Execution: actionsv1alpha1.RunnerExecutionSpec{
			Image: "runner:test",
			Docker: &actionsv1alpha1.RunnerDockerSpec{
				Image: "docker:dind",
				Resources: &actionsv1alpha1.RunnerResources{
					Requests: actionsv1alpha1.RunnerResourceList{corev1.ResourceCPU: resource.MustParse("500m")},
					Limits:   actionsv1alpha1.RunnerResourceList{corev1.ResourceEphemeralStorage: resource.MustParse("6Gi")},
				},
			},
		}},
	}
	workflowJob := &actionsv1alpha1.WorkflowJob{ObjectMeta: metav1.ObjectMeta{Name: "ci-kind", Namespace: "default", UID: types.UID("workflow-job-uid")}}
	job, err := reconciler.buildJob(workflowJob, &actionsv1alpha1.WorkflowRun{}, &actionsv1alpha1.Project{}, runnerObject)
	if err != nil {
		t.Fatalf("build job: %v", err)
	}

	pod := job.Spec.Template.Spec
	if len(pod.InitContainers) != 1 {
		t.Fatalf("init containers = %#v", pod.InitContainers)
	}
	docker := pod.InitContainers[0]
	if docker.Name != "docker" || docker.Image != "docker:dind" {
		t.Errorf("Docker sidecar = %q %q", docker.Name, docker.Image)
	}
	if docker.RestartPolicy == nil || *docker.RestartPolicy != corev1.ContainerRestartPolicyAlways {
		t.Errorf("Docker restart policy = %v", docker.RestartPolicy)
	}
	if !slices.Equal(docker.Args, []string{"dockerd", "--host=" + dockerHost, "--group=65532"}) {
		t.Errorf("Docker args = %v", docker.Args)
	}
	if docker.SecurityContext == nil || docker.SecurityContext.Privileged == nil || !*docker.SecurityContext.Privileged ||
		docker.SecurityContext.RunAsNonRoot == nil || *docker.SecurityContext.RunAsNonRoot ||
		docker.SecurityContext.RunAsUser == nil || *docker.SecurityContext.RunAsUser != 0 {
		t.Errorf("Docker security context = %#v", docker.SecurityContext)
	}
	if docker.StartupProbe == nil || docker.StartupProbe.Exec == nil || !slices.Equal(docker.StartupProbe.Exec.Command, []string{"docker", "info"}) {
		t.Errorf("Docker startup probe = %#v", docker.StartupProbe)
	}
	if docker.Resources.Requests.Cpu().String() != "500m" {
		t.Errorf("Docker CPU request = %s", docker.Resources.Requests.Cpu().String())
	}

	runnerContainer := pod.Containers[0]
	environment := map[string]string{}
	for _, variable := range runnerContainer.Env {
		environment[variable.Name] = variable.Value
	}
	if environment["DOCKER_HOST"] != dockerHost {
		t.Errorf("runner DOCKER_HOST = %q", environment["DOCKER_HOST"])
	}
	expectedRunnerMounts := []corev1.VolumeMount{
		{Name: jobPlanVolume, MountPath: jobPlanMountPath, ReadOnly: true},
		{Name: workspaceVolume, MountPath: workspaceVolumeMountPath},
		{Name: dockerSocketVolume, MountPath: dockerSocketDirectory},
	}
	if !slices.Equal(runnerContainer.VolumeMounts, expectedRunnerMounts) {
		t.Errorf("runner mounts = %#v", runnerContainer.VolumeMounts)
	}
	expectedDockerMounts := []corev1.VolumeMount{
		{Name: dockerSocketVolume, MountPath: dockerSocketDirectory},
		{Name: dockerStorageVolume, MountPath: dockerStoragePath},
		{Name: workspaceVolume, MountPath: workspaceVolumeMountPath},
	}
	if !slices.Equal(docker.VolumeMounts, expectedDockerMounts) {
		t.Errorf("Docker mounts = %#v", docker.VolumeMounts)
	}
	volumes := map[string]corev1.Volume{}
	for _, volume := range pod.Volumes {
		volumes[volume.Name] = volume
	}
	storage := volumes[dockerStorageVolume].EmptyDir
	if storage == nil || storage.SizeLimit == nil || storage.SizeLimit.String() != "6Gi" {
		t.Errorf("Docker storage volume = %#v", storage)
	}
	if socket := volumes[dockerSocketVolume].EmptyDir; socket == nil {
		t.Errorf("Docker socket volume = %#v", volumes[dockerSocketVolume])
	}
}

func TestRunnerClaimsOldestMatchingWorkflowJobInProject(t *testing.T) {
	scheme := runnerTestScheme(t)
	created := metav1.NewTime(time.Unix(100, 0))
	runnerObject := &actionsv1alpha1.Runner{
		ObjectMeta: metav1.ObjectMeta{Name: "runner-1", Namespace: "default", UID: types.UID("runner-uid")},
		Spec: actionsv1alpha1.RunnerSpec{
			Execution: actionsv1alpha1.RunnerExecutionSpec{Image: "runner:test"},
			Labels:    []string{"self-hosted", "linux", "arm64"},
		},
	}
	project := &actionsv1alpha1.Project{ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: "default", UID: types.UID("project-uid")}}
	run := &actionsv1alpha1.WorkflowRun{
		ObjectMeta: metav1.ObjectMeta{Name: "ci", Namespace: "default", UID: types.UID("run-uid")},
		Status: actionsv1alpha1.WorkflowRunStatus{Conditions: []metav1.Condition{
			plannedCondition(metav1.ConditionTrue, "JobsPlanned"),
		}},
	}
	matching := &actionsv1alpha1.WorkflowJob{
		ObjectMeta: metav1.ObjectMeta{Name: "matching", Namespace: "default", CreationTimestamp: created, Labels: map[string]string{actionsv1alpha1.LabelProjectUID: string(project.UID)}, Annotations: map[string]string{actionsv1alpha1.AnnotationProjectName: project.Name}},
		Spec: actionsv1alpha1.WorkflowJobSpec{
			WorkflowRunRef: corev1.LocalObjectReference{Name: run.Name},
			RunsOn:         []string{"linux", "arm64"},
		},
	}
	newer := matching.DeepCopy()
	newer.Name = "newer"
	newer.CreationTimestamp = metav1.NewTime(created.Add(time.Minute))
	wrongProject := matching.DeepCopy()
	wrongProject.Name = "wrong-project"
	wrongProject.Labels = map[string]string{actionsv1alpha1.LabelProjectUID: "other-project"}
	wrongLabels := matching.DeepCopy()
	wrongLabels.Name = "wrong-labels"
	wrongLabels.Spec.RunsOn = []string{"windows"}

	clusterClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithIndex(&actionsv1alpha1.WorkflowJob{}, workflowJobQueuedIndex, indexQueuedWorkflowJob).
		WithIndex(&actionsv1alpha1.WorkflowJob{}, workflowJobProjectNameIndex, indexWorkflowJobProjectName).
		WithStatusSubresource(&actionsv1alpha1.Runner{}, &actionsv1alpha1.WorkflowJob{}).
		WithObjects(runnerObject, project, run, matching, newer, wrongProject, wrongLabels).
		Build()
	reconciler := &RunnerReconciler{Client: clusterClient, APIReader: clusterClient}
	claimed, err := reconciler.claimWorkflowJob(context.Background(), runnerObject, project)
	if err != nil {
		t.Fatal(err)
	}
	if claimed == nil || claimed.Name != matching.Name {
		t.Fatalf("claimed WorkflowJob = %#v", claimed)
	}

	storedJob := &actionsv1alpha1.WorkflowJob{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(matching), storedJob); err != nil {
		t.Fatal(err)
	}
	if storedJob.Status.RunnerRef == nil || storedJob.Status.RunnerRef.Name != runnerObject.Name {
		t.Errorf("runnerRef = %#v", storedJob.Status.RunnerRef)
	}
	scheduled := meta.FindStatusCondition(storedJob.Status.Conditions, actionsv1alpha1.WorkflowJobConditionScheduled)
	if scheduled == nil || scheduled.Status != metav1.ConditionTrue || scheduled.Reason != "RunnerAssigned" {
		t.Errorf("scheduled condition = %#v", scheduled)
	}
	storedRunner := &actionsv1alpha1.Runner{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(runnerObject), storedRunner); err != nil {
		t.Fatal(err)
	}
	if storedRunner.Status.WorkflowJobRef == nil || storedRunner.Status.WorkflowJobRef.Name != matching.Name {
		t.Errorf("workflowJobRef = %#v", storedRunner.Status.WorkflowJobRef)
	}
	ready := meta.FindStatusCondition(storedRunner.Status.Conditions, actionsv1alpha1.RunnerConditionReady)
	if ready == nil || ready.Status != metav1.ConditionTrue || ready.Reason != "Ready" {
		t.Errorf("ready condition = %#v", ready)
	}
	busy := meta.FindStatusCondition(storedRunner.Status.Conditions, actionsv1alpha1.RunnerConditionBusy)
	if busy == nil || busy.Status != metav1.ConditionTrue || busy.Reason != "JobAssigned" {
		t.Errorf("busy condition = %#v", busy)
	}
	staleJob := &actionsv1alpha1.WorkflowJob{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(wrongProject), staleJob); err != nil {
		t.Fatal(err)
	}
	staleCondition := meta.FindStatusCondition(staleJob.Status.Conditions, actionsv1alpha1.WorkflowJobConditionSucceeded)
	if staleCondition == nil || staleCondition.Status != metav1.ConditionFalse || staleCondition.Reason != "ProjectRecreated" {
		t.Fatalf("recreated project condition = %#v", staleCondition)
	}
}

func TestMatrixMaxParallelLimitsRunnerClaims(t *testing.T) {
	scheme := runnerTestScheme(t)
	project := &actionsv1alpha1.Project{ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: "default", UID: types.UID("project-uid")}}
	run := &actionsv1alpha1.WorkflowRun{
		ObjectMeta: metav1.ObjectMeta{Name: "release", Namespace: "default", UID: types.UID("run-uid")},
		Status: actionsv1alpha1.WorkflowRunStatus{Conditions: []metav1.Condition{
			plannedCondition(metav1.ConditionTrue, "JobsPlanned"),
		}},
	}
	runner1 := &actionsv1alpha1.Runner{ObjectMeta: metav1.ObjectMeta{Name: "runner-1", Namespace: "default", UID: "runner-1"}, Spec: actionsv1alpha1.RunnerSpec{Labels: []string{"ubuntu-latest"}}}
	runner2 := &actionsv1alpha1.Runner{ObjectMeta: metav1.ObjectMeta{Name: "runner-2", Namespace: "default", UID: "runner-2"}, Spec: actionsv1alpha1.RunnerSpec{Labels: []string{"ubuntu-24.04-arm"}}}
	created := metav1.NewTime(time.Unix(100, 0))
	jobs := []*actionsv1alpha1.WorkflowJob{}
	for index, arch := range []string{"amd64", "arm64"} {
		runsOn := "ubuntu-latest"
		if arch == "arm64" {
			runsOn = "ubuntu-24.04-arm"
		}
		jobs = append(jobs, &actionsv1alpha1.WorkflowJob{
			ObjectMeta: metav1.ObjectMeta{
				Name: fmt.Sprintf("build-%s", arch), Namespace: "default", CreationTimestamp: metav1.NewTime(created.Add(time.Duration(index) * time.Second)),
				Labels: map[string]string{actionsv1alpha1.LabelProjectUID: string(project.UID), actionsv1alpha1.LabelWorkflowRunUID: string(run.UID)}, Annotations: map[string]string{actionsv1alpha1.AnnotationProjectName: project.Name},
			},
			Spec: actionsv1alpha1.WorkflowJobSpec{
				WorkflowRunRef: corev1.LocalObjectReference{Name: run.Name}, JobID: fmt.Sprintf("build-matrix-%d", index+1), RunsOn: []string{runsOn},
				Matrix: &actionsv1alpha1.WorkflowJobMatrix{LogicalJobID: "build", Values: map[string]string{"arch": arch}, MaxParallel: 1},
			},
		})
	}
	clusterClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithIndex(&actionsv1alpha1.WorkflowJob{}, workflowJobQueuedIndex, indexQueuedWorkflowJob).
		WithIndex(&actionsv1alpha1.WorkflowJob{}, workflowJobProjectNameIndex, indexWorkflowJobProjectName).
		WithStatusSubresource(&actionsv1alpha1.Runner{}, &actionsv1alpha1.WorkflowJob{}).
		WithObjects(project, run, runner1, runner2, jobs[0], jobs[1]).
		Build()
	reconciler := &RunnerReconciler{Client: clusterClient, APIReader: clusterClient}

	first, err := reconciler.claimWorkflowJob(context.Background(), runner1, project)
	if err != nil {
		t.Fatal(err)
	}
	if first == nil || first.Name != jobs[0].Name {
		t.Fatalf("first claim = %#v", first)
	}
	second, err := reconciler.claimWorkflowJob(context.Background(), runner2, project)
	if err != nil {
		t.Fatal(err)
	}
	if second != nil {
		t.Fatalf("claimed second matrix job while first was active: %#v", second)
	}

	storedFirst := &actionsv1alpha1.WorkflowJob{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(jobs[0]), storedFirst); err != nil {
		t.Fatal(err)
	}
	meta.SetStatusCondition(&storedFirst.Status.Conditions, metav1.Condition{Type: actionsv1alpha1.WorkflowJobConditionSucceeded, Status: metav1.ConditionTrue, Reason: "JobSucceeded", Message: "Job completed"})
	if err := clusterClient.Status().Update(context.Background(), storedFirst); err != nil {
		t.Fatal(err)
	}
	second, err = reconciler.claimWorkflowJob(context.Background(), runner2, project)
	if err != nil {
		t.Fatal(err)
	}
	if second == nil || second.Name != jobs[1].Name {
		t.Fatalf("second claim after slot opened = %#v", second)
	}
}

func TestMatrixFailFastFailurePreventsNewClaims(t *testing.T) {
	scheme := runnerTestScheme(t)
	project := &actionsv1alpha1.Project{ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: "default", UID: types.UID("project-uid")}}
	run := &actionsv1alpha1.WorkflowRun{
		ObjectMeta: metav1.ObjectMeta{Name: "release", Namespace: "default", UID: types.UID("run-uid")},
		Status: actionsv1alpha1.WorkflowRunStatus{Conditions: []metav1.Condition{
			plannedCondition(metav1.ConditionTrue, "JobsPlanned"),
		}},
	}
	runnerObject := &actionsv1alpha1.Runner{
		ObjectMeta: metav1.ObjectMeta{Name: "runner-1", Namespace: "default", UID: "runner-1"},
		Spec:       actionsv1alpha1.RunnerSpec{Labels: []string{"ubuntu-latest"}},
	}
	job := func(name, logicalID string, failFast *bool) *actionsv1alpha1.WorkflowJob {
		return &actionsv1alpha1.WorkflowJob{
			ObjectMeta: metav1.ObjectMeta{
				Name: name, Namespace: "default",
				Labels:      map[string]string{actionsv1alpha1.LabelProjectUID: string(project.UID), actionsv1alpha1.LabelWorkflowRunUID: string(run.UID)},
				Annotations: map[string]string{actionsv1alpha1.AnnotationProjectName: project.Name},
			},
			Spec: actionsv1alpha1.WorkflowJobSpec{
				WorkflowRunRef: corev1.LocalObjectReference{Name: run.Name}, JobID: name, RunsOn: []string{"ubuntu-latest"},
				Matrix: &actionsv1alpha1.WorkflowJobMatrix{LogicalJobID: logicalID, Values: map[string]string{"case": name}, MaxParallel: 1, FailFast: failFast},
			},
		}
	}
	failed := job("build-failed", "build", nil)
	meta.SetStatusCondition(&failed.Status.Conditions, metav1.Condition{
		Type: actionsv1alpha1.WorkflowJobConditionSucceeded, Status: metav1.ConditionFalse, Reason: "JobFailed", Message: "Failed",
	})
	blocked := job("build-queued", "build", nil)
	unrelated := job("test-queued", "test", nil)
	disabledFailed := job("lint-failed", "lint", pointerTo(false))
	meta.SetStatusCondition(&disabledFailed.Status.Conditions, metav1.Condition{
		Type: actionsv1alpha1.WorkflowJobConditionSucceeded, Status: metav1.ConditionFalse, Reason: "JobFailed", Message: "Failed",
	})
	disabledQueued := job("lint-queued", "lint", pointerTo(false))
	clusterClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithIndex(&actionsv1alpha1.WorkflowJob{}, workflowJobQueuedIndex, indexQueuedWorkflowJob).
		WithIndex(&actionsv1alpha1.WorkflowJob{}, workflowJobProjectNameIndex, indexWorkflowJobProjectName).
		WithStatusSubresource(&actionsv1alpha1.Runner{}, &actionsv1alpha1.WorkflowJob{}).
		WithObjects(project, run, runnerObject, failed, blocked, unrelated, disabledFailed, disabledQueued).
		Build()
	reconciler := &RunnerReconciler{Client: clusterClient, APIReader: clusterClient}

	claimed, err := reconciler.claimWorkflowJob(context.Background(), runnerObject, project)
	if err != nil {
		t.Fatal(err)
	}
	if claimed == nil || claimed.Name != disabledQueued.Name {
		t.Fatalf("claim after matrix failure = %#v", claimed)
	}
}

func TestMatrixFailFastCancelsActiveNativeJobAndReleasesRunner(t *testing.T) {
	scheme := runnerTestScheme(t)
	run := &actionsv1alpha1.WorkflowRun{
		TypeMeta:   metav1.TypeMeta{APIVersion: actionsv1alpha1.GroupVersion.String(), Kind: "WorkflowRun"},
		ObjectMeta: metav1.ObjectMeta{Name: "release", Namespace: "default", UID: types.UID("run-uid")},
	}
	runnerObject := &actionsv1alpha1.Runner{
		ObjectMeta: metav1.ObjectMeta{Name: "runner-1", Namespace: "default", UID: types.UID("runner-uid"), Finalizers: []string{runnerFinalizer}},
		Status:     actionsv1alpha1.RunnerStatus{WorkflowJobRef: &corev1.LocalObjectReference{Name: "build-arm64"}},
	}
	workflowJob := &actionsv1alpha1.WorkflowJob{
		TypeMeta: metav1.TypeMeta{APIVersion: actionsv1alpha1.GroupVersion.String(), Kind: "WorkflowJob"},
		ObjectMeta: metav1.ObjectMeta{
			Name: "build-arm64", Namespace: "default", UID: types.UID("job-uid"),
			Labels: map[string]string{actionsv1alpha1.LabelWorkflowRunUID: string(run.UID)},
		},
		Spec: actionsv1alpha1.WorkflowJobSpec{
			WorkflowRunRef: corev1.LocalObjectReference{Name: run.Name}, JobID: "build-matrix-2", RunsOn: []string{"ubuntu-latest"},
			Matrix: &actionsv1alpha1.WorkflowJobMatrix{LogicalJobID: "build", Values: map[string]string{"arch": "arm64"}},
		},
		Status: actionsv1alpha1.WorkflowJobStatus{
			RunnerRef: &corev1.LocalObjectReference{Name: runnerObject.Name},
			Conditions: []metav1.Condition{
				{Type: actionsv1alpha1.WorkflowJobConditionScheduled, Status: metav1.ConditionTrue, Reason: "RunnerAssigned", Message: "Assigned"},
				{Type: actionsv1alpha1.WorkflowJobConditionSucceeded, Status: metav1.ConditionUnknown, Reason: "JobRunning", Message: "Running"},
				{Type: actionsv1alpha1.WorkflowJobConditionCancellationRequested, Status: metav1.ConditionTrue, Reason: matrixFailFastReason, Message: matrixFailFastMessage},
			},
		},
	}
	if err := controllerutil.SetControllerReference(run, workflowJob, scheme); err != nil {
		t.Fatal(err)
	}
	nativeJob := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: workflowJob.Name, Namespace: workflowJob.Namespace, UID: types.UID("native-job-uid")}}
	if err := controllerutil.SetControllerReference(workflowJob, nativeJob, scheme); err != nil {
		t.Fatal(err)
	}
	clusterClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithIndex(&actionsv1alpha1.WorkflowJob{}, workflowJobRunnerNameIndex, indexWorkflowJobRunnerName).
		WithStatusSubresource(&actionsv1alpha1.Runner{}, &actionsv1alpha1.WorkflowJob{}, &batchv1.Job{}).
		WithObjects(run, runnerObject, workflowJob, nativeJob).
		Build()
	reconciler := &RunnerReconciler{Client: clusterClient, APIReader: clusterClient}
	request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(runnerObject)}

	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(nativeJob), &batchv1.Job{}); !apierrors.IsNotFound(err) {
		t.Fatalf("native Job remains after cancellation request: %v", err)
	}
	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	storedJob := &actionsv1alpha1.WorkflowJob{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(workflowJob), storedJob); err != nil {
		t.Fatal(err)
	}
	result := meta.FindStatusCondition(storedJob.Status.Conditions, actionsv1alpha1.WorkflowJobConditionSucceeded)
	if result == nil || result.Status != metav1.ConditionFalse || result.Reason != matrixFailFastReason {
		t.Fatalf("cancelled WorkflowJob result = %#v", result)
	}
	if storedJob.Status.Result != actionsv1alpha1.WorkflowJobResultCancelled {
		t.Fatalf("cancelled WorkflowJob status result = %q", storedJob.Status.Result)
	}
	storedRunner := &actionsv1alpha1.Runner{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(runnerObject), storedRunner); err != nil {
		t.Fatal(err)
	}
	if storedRunner.Status.WorkflowJobRef != nil {
		t.Fatalf("runner assignment = %#v", storedRunner.Status.WorkflowJobRef)
	}
}

func TestRunnerDoesNotClaimWorkflowJobBeforePlanningCompletes(t *testing.T) {
	scheme := runnerTestScheme(t)
	runnerObject := &actionsv1alpha1.Runner{
		ObjectMeta: metav1.ObjectMeta{Name: "runner-1", Namespace: "default", UID: types.UID("runner-uid")},
		Spec: actionsv1alpha1.RunnerSpec{
			Execution: actionsv1alpha1.RunnerExecutionSpec{Image: "runner:test"},
			Labels:    []string{"linux"},
		},
	}
	project := &actionsv1alpha1.Project{ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: "default", UID: types.UID("project-uid")}}
	run := &actionsv1alpha1.WorkflowRun{ObjectMeta: metav1.ObjectMeta{Name: "ci", Namespace: "default", UID: types.UID("run-uid")}}
	workflowJob := &actionsv1alpha1.WorkflowJob{
		ObjectMeta: metav1.ObjectMeta{Name: "build", Namespace: "default", Labels: map[string]string{actionsv1alpha1.LabelProjectUID: string(project.UID)}, Annotations: map[string]string{actionsv1alpha1.AnnotationProjectName: project.Name}},
		Spec: actionsv1alpha1.WorkflowJobSpec{
			WorkflowRunRef: corev1.LocalObjectReference{Name: run.Name},
			RunsOn:         []string{"linux"},
		},
	}
	clusterClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithIndex(&actionsv1alpha1.WorkflowJob{}, workflowJobQueuedIndex, indexQueuedWorkflowJob).
		WithIndex(&actionsv1alpha1.WorkflowJob{}, workflowJobProjectNameIndex, indexWorkflowJobProjectName).
		WithStatusSubresource(&actionsv1alpha1.Runner{}, &actionsv1alpha1.WorkflowJob{}).
		WithObjects(runnerObject, project, run, workflowJob).
		Build()
	reconciler := &RunnerReconciler{Client: clusterClient, APIReader: clusterClient}
	claimed, err := reconciler.claimWorkflowJob(context.Background(), runnerObject, project)
	if err != nil {
		t.Fatal(err)
	}
	if claimed != nil {
		t.Fatalf("claimed WorkflowJob before planning completed: %s", claimed.Name)
	}
	stored := &actionsv1alpha1.WorkflowJob{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(workflowJob), stored); err != nil {
		t.Fatal(err)
	}
	if stored.Status.RunnerRef != nil {
		t.Fatalf("runnerRef = %#v", stored.Status.RunnerRef)
	}
}

func TestRunnerDoesNotClaimAnotherJobWhenLiveStatusIsBusy(t *testing.T) {
	scheme := runnerTestScheme(t)
	runnerObject := &actionsv1alpha1.Runner{
		ObjectMeta: metav1.ObjectMeta{Name: "runner-1", Namespace: "default", UID: types.UID("runner-uid")},
		Spec:       actionsv1alpha1.RunnerSpec{Labels: []string{"linux"}},
	}
	liveRunner := runnerObject.DeepCopy()
	liveRunner.Status.WorkflowJobRef = &corev1.LocalObjectReference{Name: "assigned"}
	project := &actionsv1alpha1.Project{ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: "default", UID: types.UID("project-uid")}}
	run := &actionsv1alpha1.WorkflowRun{
		ObjectMeta: metav1.ObjectMeta{Name: "ci", Namespace: "default"},
		Status:     actionsv1alpha1.WorkflowRunStatus{Conditions: []metav1.Condition{plannedCondition(metav1.ConditionTrue, "JobsPlanned")}},
	}
	workflowJob := &actionsv1alpha1.WorkflowJob{
		ObjectMeta: metav1.ObjectMeta{
			Name: "queued", Namespace: "default",
			Labels:      map[string]string{actionsv1alpha1.LabelProjectUID: string(project.UID)},
			Annotations: map[string]string{actionsv1alpha1.AnnotationProjectName: project.Name},
		},
		Spec: actionsv1alpha1.WorkflowJobSpec{WorkflowRunRef: corev1.LocalObjectReference{Name: run.Name}, RunsOn: []string{"linux"}},
	}
	cachedClient := fake.NewClientBuilder().WithScheme(scheme).
		WithIndex(&actionsv1alpha1.WorkflowJob{}, workflowJobQueuedIndex, indexQueuedWorkflowJob).
		WithIndex(&actionsv1alpha1.WorkflowJob{}, workflowJobProjectNameIndex, indexWorkflowJobProjectName).
		WithStatusSubresource(&actionsv1alpha1.Runner{}, &actionsv1alpha1.WorkflowJob{}).
		WithObjects(runnerObject, project, run, workflowJob).Build()
	liveReader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(liveRunner, run, workflowJob).Build()
	reconciler := &RunnerReconciler{Client: cachedClient, APIReader: liveReader}

	claimed, err := reconciler.claimWorkflowJob(context.Background(), runnerObject, project)
	if !errors.Is(err, errRunnerAlreadyAssigned) || claimed != nil {
		t.Fatalf("claimWorkflowJob() = %#v, %v", claimed, err)
	}
	stored := &actionsv1alpha1.WorkflowJob{}
	if err := cachedClient.Get(context.Background(), client.ObjectKeyFromObject(workflowJob), stored); err != nil {
		t.Fatal(err)
	}
	if stored.Status.RunnerRef != nil {
		t.Fatalf("queued WorkflowJob runnerRef = %#v", stored.Status.RunnerRef)
	}
}

func TestAssignedWorkflowJobUsesLiveReader(t *testing.T) {
	scheme := runnerTestScheme(t)
	runnerObject := &actionsv1alpha1.Runner{
		ObjectMeta: metav1.ObjectMeta{Name: "runner", Namespace: "default", UID: types.UID("runner-uid")},
		Status:     actionsv1alpha1.RunnerStatus{WorkflowJobRef: &corev1.LocalObjectReference{Name: "build"}},
	}
	staleJob := &actionsv1alpha1.WorkflowJob{ObjectMeta: metav1.ObjectMeta{Name: "build", Namespace: "default", UID: types.UID("job-uid")}}
	liveJob := staleJob.DeepCopy()
	liveJob.Status.RunnerRef = &corev1.LocalObjectReference{Name: runnerObject.Name}
	cachedClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&actionsv1alpha1.Runner{}).WithObjects(runnerObject, staleJob).Build()
	liveReader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(liveJob).Build()
	reconciler := &RunnerReconciler{Client: cachedClient, APIReader: liveReader}

	assigned, err := reconciler.assignedWorkflowJob(context.Background(), runnerObject)
	if err != nil {
		t.Fatal(err)
	}
	if assigned == nil || assigned.Name != liveJob.Name || assigned.Status.RunnerRef == nil {
		t.Fatalf("assigned WorkflowJob = %#v", assigned)
	}
}

func TestRunnerDoesNotClaimWorkflowJobFromDeletingRun(t *testing.T) {
	scheme := runnerTestScheme(t)
	deletionTime := metav1.Now()
	runnerObject := &actionsv1alpha1.Runner{
		ObjectMeta: metav1.ObjectMeta{Name: "runner-1", Namespace: "default"},
		Spec:       actionsv1alpha1.RunnerSpec{Labels: []string{"linux"}},
	}
	project := &actionsv1alpha1.Project{ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: "default", UID: types.UID("project-uid")}}
	run := &actionsv1alpha1.WorkflowRun{
		ObjectMeta: metav1.ObjectMeta{Name: "ci", Namespace: "default", UID: types.UID("run-uid"), DeletionTimestamp: &deletionTime, Finalizers: []string{"test"}},
		Status: actionsv1alpha1.WorkflowRunStatus{Conditions: []metav1.Condition{
			plannedCondition(metav1.ConditionTrue, "JobsPlanned"),
		}},
	}
	workflowJob := &actionsv1alpha1.WorkflowJob{
		ObjectMeta: metav1.ObjectMeta{
			Name: "build", Namespace: "default",
			Labels:      map[string]string{actionsv1alpha1.LabelProjectUID: string(project.UID)},
			Annotations: map[string]string{actionsv1alpha1.AnnotationProjectName: project.Name},
		},
		Spec: actionsv1alpha1.WorkflowJobSpec{
			WorkflowRunRef: corev1.LocalObjectReference{Name: run.Name},
			RunsOn:         []string{"linux"},
		},
	}
	clusterClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithIndex(&actionsv1alpha1.WorkflowJob{}, workflowJobQueuedIndex, indexQueuedWorkflowJob).
		WithIndex(&actionsv1alpha1.WorkflowJob{}, workflowJobProjectNameIndex, indexWorkflowJobProjectName).
		WithStatusSubresource(&actionsv1alpha1.Runner{}, &actionsv1alpha1.WorkflowJob{}).
		WithObjects(runnerObject, project, run, workflowJob).
		Build()
	reconciler := &RunnerReconciler{Client: clusterClient, APIReader: clusterClient}

	claimed, err := reconciler.claimWorkflowJob(context.Background(), runnerObject, project)
	if err != nil {
		t.Fatal(err)
	}
	if claimed != nil {
		t.Fatalf("claimed WorkflowJob from deleting WorkflowRun: %s", claimed.Name)
	}
}

func TestDeletingWorkflowJobIsNotQueued(t *testing.T) {
	deletionTime := metav1.Now()
	workflowJob := &actionsv1alpha1.WorkflowJob{ObjectMeta: metav1.ObjectMeta{DeletionTimestamp: &deletionTime}}
	if values := indexQueuedWorkflowJob(workflowJob); len(values) != 0 {
		t.Fatalf("queue index = %#v", values)
	}
}

func TestDependencyBlockedWorkflowJobIsNotQueued(t *testing.T) {
	workflowJob := &actionsv1alpha1.WorkflowJob{
		Spec: actionsv1alpha1.WorkflowJobSpec{Needs: []string{"build"}},
		Status: actionsv1alpha1.WorkflowJobStatus{Conditions: []metav1.Condition{{
			Type: actionsv1alpha1.WorkflowJobConditionReady, Status: metav1.ConditionUnknown,
		}}},
	}
	if values := indexQueuedWorkflowJob(workflowJob); len(values) != 0 {
		t.Fatalf("queued index = %v", values)
	}
}

func TestTerminalWorkflowJobStatusIsDurableBeforeCredentialCleanup(t *testing.T) {
	scheme := runnerTestScheme(t)
	workflowJob := &actionsv1alpha1.WorkflowJob{
		TypeMeta:   metav1.TypeMeta{APIVersion: actionsv1alpha1.GroupVersion.String(), Kind: "WorkflowJob"},
		ObjectMeta: metav1.ObjectMeta{Name: "build", Namespace: "default", UID: types.UID("workflow-job-uid")},
		Status:     actionsv1alpha1.WorkflowJobStatus{RunnerRef: &corev1.LocalObjectReference{Name: "runner"}},
	}
	nativeJob := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: workflowJob.Name, Namespace: workflowJob.Namespace, UID: types.UID("native-job-uid"), Annotations: map[string]string{actionsv1alpha1.AnnotationRunnerResultVersion: jobResultVersion}},
		Status: batchv1.JobStatus{Conditions: []batchv1.JobCondition{{
			Type: batchv1.JobComplete, Status: corev1.ConditionTrue,
		}}},
	}
	if err := controllerutil.SetControllerReference(workflowJob, nativeJob, scheme); err != nil {
		t.Fatal(err)
	}
	resultData, err := runner.EncodeResult(runner.Result{Version: runner.ResultVersion, Conclusion: runner.ResultConclusionSuccess, Outputs: map[string]string{"artifact": "ready"}})
	if err != nil {
		t.Fatal(err)
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "build-pod",
			Namespace: workflowJob.Namespace,
			Labels:    map[string]string{actionsv1alpha1.LabelWorkflowJobUID: string(workflowJob.UID)},
		},
		Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{
			Name: runner.ContainerName,
			State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
				Message: string(resultData),
			}},
		}}},
	}
	if err := controllerutil.SetControllerReference(nativeJob, pod, scheme); err != nil {
		t.Fatal(err)
	}
	authSecret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: childName(workflowJob.Name, "auth"), Namespace: workflowJob.Namespace}}
	clusterClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&actionsv1alpha1.WorkflowJob{}, &batchv1.Job{}).
		WithObjects(workflowJob, nativeJob, pod, authSecret).
		Build()
	reconciler := &RunnerReconciler{Client: clusterClient, APIReader: clusterClient}
	found, terminal, err := reconciler.observeNativeJob(context.Background(), workflowJob)
	if !found || !terminal || err == nil {
		t.Fatalf("observeNativeJob() = found %t, terminal %t, error %v", found, terminal, err)
	}
	stored := &actionsv1alpha1.WorkflowJob{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(workflowJob), stored); err != nil {
		t.Fatal(err)
	}
	if !terminalWorkflowJob(stored) {
		t.Fatal("WorkflowJob terminal result was not persisted before credential cleanup")
	}
	if stored.Status.Outputs["artifact"] != "ready" {
		t.Fatalf("WorkflowJob outputs = %#v", stored.Status.Outputs)
	}
	storedNativeJob := &batchv1.Job{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(nativeJob), storedNativeJob); err != nil {
		t.Fatal(err)
	}
	if storedNativeJob.Spec.TTLSecondsAfterFinished != nil {
		t.Fatal("native Job TTL started before credential cleanup completed")
	}
}

func TestSuccessfulNativeJobRequiresRunnerResult(t *testing.T) {
	scheme := runnerTestScheme(t)
	workflowJob := &actionsv1alpha1.WorkflowJob{
		TypeMeta:   metav1.TypeMeta{APIVersion: actionsv1alpha1.GroupVersion.String(), Kind: "WorkflowJob"},
		ObjectMeta: metav1.ObjectMeta{Name: "build", Namespace: "default", UID: types.UID("workflow-job-uid")},
		Status:     actionsv1alpha1.WorkflowJobStatus{RunnerRef: &corev1.LocalObjectReference{Name: "runner"}},
	}
	nativeJob := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: workflowJob.Name, Namespace: workflowJob.Namespace, UID: types.UID("native-job-uid"), Annotations: map[string]string{actionsv1alpha1.AnnotationRunnerResultVersion: jobResultVersion}},
		Status: batchv1.JobStatus{Conditions: []batchv1.JobCondition{{
			Type: batchv1.JobComplete, Status: corev1.ConditionTrue,
		}}},
	}
	if err := controllerutil.SetControllerReference(workflowJob, nativeJob, scheme); err != nil {
		t.Fatal(err)
	}
	clusterClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&actionsv1alpha1.WorkflowJob{}, &batchv1.Job{}).
		WithObjects(workflowJob, nativeJob).
		Build()
	reconciler := &RunnerReconciler{Client: clusterClient, APIReader: clusterClient}
	found, terminal, err := reconciler.observeNativeJob(context.Background(), workflowJob)
	if err != nil || !found || !terminal {
		t.Fatalf("observeNativeJob() = found %t, terminal %t, error %v", found, terminal, err)
	}
	stored := &actionsv1alpha1.WorkflowJob{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(workflowJob), stored); err != nil {
		t.Fatal(err)
	}
	condition := meta.FindStatusCondition(stored.Status.Conditions, actionsv1alpha1.WorkflowJobConditionSucceeded)
	if condition == nil || condition.Status != metav1.ConditionFalse || condition.Reason != "JobResultInvalid" {
		t.Fatalf("succeeded condition = %#v", condition)
	}
}

func TestWorkflowJobResultRequiresAssignedVersion(t *testing.T) {
	scheme := runnerTestScheme(t)
	workflowJob := &actionsv1alpha1.WorkflowJob{
		ObjectMeta: metav1.ObjectMeta{Name: "build", Namespace: "default", UID: types.UID("workflow-job-uid")},
	}
	nativeJob := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:        workflowJob.Name,
			Namespace:   workflowJob.Namespace,
			UID:         types.UID("native-job-uid"),
			Annotations: map[string]string{actionsv1alpha1.AnnotationRunnerResultVersion: "1"},
		},
	}
	resultData, err := runner.EncodeResult(runner.Result{Version: runner.ResultVersion, Conclusion: runner.ResultConclusionSuccess})
	if err != nil {
		t.Fatal(err)
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "build-pod", Namespace: workflowJob.Namespace, Labels: map[string]string{actionsv1alpha1.LabelWorkflowJobUID: string(workflowJob.UID)}},
		Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{
			Name: runner.ContainerName,
			State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
				Message: string(resultData),
			}},
		}}},
	}
	if err := controllerutil.SetControllerReference(nativeJob, pod, scheme); err != nil {
		t.Fatal(err)
	}
	clusterClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pod).Build()
	result, invalid, err := (&RunnerReconciler{APIReader: clusterClient}).workflowJobResult(context.Background(), workflowJob, nativeJob)
	if err != nil || result != nil || !invalid {
		t.Fatalf("workflowJobResult() = result %#v, invalid %t, error %v", result, invalid, err)
	}
}

func TestRunnerResultReadErrorIsRetried(t *testing.T) {
	scheme := runnerTestScheme(t)
	workflowJob := &actionsv1alpha1.WorkflowJob{
		TypeMeta:   metav1.TypeMeta{APIVersion: actionsv1alpha1.GroupVersion.String(), Kind: "WorkflowJob"},
		ObjectMeta: metav1.ObjectMeta{Name: "build", Namespace: "default", UID: types.UID("workflow-job-uid")},
		Status:     actionsv1alpha1.WorkflowJobStatus{RunnerRef: &corev1.LocalObjectReference{Name: "runner"}},
	}
	nativeJob := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: workflowJob.Name, Namespace: workflowJob.Namespace, UID: types.UID("native-job-uid"), Annotations: map[string]string{actionsv1alpha1.AnnotationRunnerResultVersion: jobResultVersion}},
		Status: batchv1.JobStatus{Conditions: []batchv1.JobCondition{{
			Type: batchv1.JobComplete, Status: corev1.ConditionTrue,
		}}},
	}
	if err := controllerutil.SetControllerReference(workflowJob, nativeJob, scheme); err != nil {
		t.Fatal(err)
	}
	clusterClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&actionsv1alpha1.WorkflowJob{}, &batchv1.Job{}).
		WithObjects(workflowJob, nativeJob).
		Build()
	readError := errors.New("temporary Pod list failure")
	reconciler := &RunnerReconciler{Client: clusterClient, APIReader: &podListErrorReader{Reader: clusterClient, err: readError}}
	found, terminal, err := reconciler.observeNativeJob(context.Background(), workflowJob)
	if !found || !terminal || !errors.Is(err, readError) {
		t.Fatalf("observeNativeJob() = found %t, terminal %t, error %v", found, terminal, err)
	}
	stored := &actionsv1alpha1.WorkflowJob{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(workflowJob), stored); err != nil {
		t.Fatal(err)
	}
	if terminalWorkflowJob(stored) {
		t.Fatalf("WorkflowJob became terminal after a transient read error: %#v", stored.Status.Conditions)
	}
}

func TestFailedNativeJobPersistsRunnerOutputs(t *testing.T) {
	scheme := runnerTestScheme(t)
	workflowJob := &actionsv1alpha1.WorkflowJob{
		TypeMeta:   metav1.TypeMeta{APIVersion: actionsv1alpha1.GroupVersion.String(), Kind: "WorkflowJob"},
		ObjectMeta: metav1.ObjectMeta{Name: "build", Namespace: "default", UID: types.UID("workflow-job-uid")},
		Status:     actionsv1alpha1.WorkflowJobStatus{RunnerRef: &corev1.LocalObjectReference{Name: "runner"}},
	}
	nativeJob := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: workflowJob.Name, Namespace: workflowJob.Namespace, UID: types.UID("native-job-uid"), Annotations: map[string]string{actionsv1alpha1.AnnotationRunnerResultVersion: jobResultVersion}},
		Status: batchv1.JobStatus{Conditions: []batchv1.JobCondition{{
			Type: batchv1.JobFailed, Status: corev1.ConditionTrue,
		}}},
	}
	if err := controllerutil.SetControllerReference(workflowJob, nativeJob, scheme); err != nil {
		t.Fatal(err)
	}
	resultData, err := runner.EncodeResult(runner.Result{Version: runner.ResultVersion, Conclusion: runner.ResultConclusionFailure, Outputs: map[string]string{"artifact": "available"}})
	if err != nil {
		t.Fatal(err)
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "build-pod", Namespace: workflowJob.Namespace, Labels: map[string]string{actionsv1alpha1.LabelWorkflowJobUID: string(workflowJob.UID)}},
		Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{
			Name: runner.ContainerName,
			State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
				Message: string(resultData),
			}},
		}}},
	}
	if err := controllerutil.SetControllerReference(nativeJob, pod, scheme); err != nil {
		t.Fatal(err)
	}
	clusterClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&actionsv1alpha1.WorkflowJob{}, &batchv1.Job{}).
		WithObjects(workflowJob, nativeJob, pod).
		Build()
	reconciler := &RunnerReconciler{Client: clusterClient, APIReader: clusterClient}
	found, terminal, err := reconciler.observeNativeJob(context.Background(), workflowJob)
	if err != nil || !found || !terminal {
		t.Fatalf("observeNativeJob() = found %t, terminal %t, error %v", found, terminal, err)
	}
	stored := &actionsv1alpha1.WorkflowJob{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(workflowJob), stored); err != nil {
		t.Fatal(err)
	}
	condition := meta.FindStatusCondition(stored.Status.Conditions, actionsv1alpha1.WorkflowJobConditionSucceeded)
	if condition == nil || condition.Status != metav1.ConditionFalse || condition.Reason != "JobFailed" {
		t.Fatalf("succeeded condition = %#v", condition)
	}
	if stored.Status.Outputs["artifact"] != "available" {
		t.Fatalf("WorkflowJob outputs = %#v", stored.Status.Outputs)
	}
}

func TestExpiredNativeJobIsReportedAsTimedOut(t *testing.T) {
	scheme := runnerTestScheme(t)
	workflowJob := &actionsv1alpha1.WorkflowJob{
		TypeMeta:   metav1.TypeMeta{APIVersion: actionsv1alpha1.GroupVersion.String(), Kind: "WorkflowJob"},
		ObjectMeta: metav1.ObjectMeta{Name: "build", Namespace: "default", UID: types.UID("workflow-job-uid")},
		Status:     actionsv1alpha1.WorkflowJobStatus{RunnerRef: &corev1.LocalObjectReference{Name: "runner"}},
	}
	nativeJob := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: workflowJob.Name, Namespace: workflowJob.Namespace, UID: types.UID("native-job-uid")},
		Status: batchv1.JobStatus{Conditions: []batchv1.JobCondition{{
			Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Reason: batchv1.JobReasonDeadlineExceeded,
		}}},
	}
	if err := controllerutil.SetControllerReference(workflowJob, nativeJob, scheme); err != nil {
		t.Fatal(err)
	}
	clusterClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&actionsv1alpha1.WorkflowJob{}, &batchv1.Job{}).
		WithObjects(workflowJob, nativeJob).
		Build()
	reconciler := &RunnerReconciler{Client: clusterClient, APIReader: clusterClient}
	found, terminal, err := reconciler.observeNativeJob(context.Background(), workflowJob)
	if err != nil || !found || !terminal {
		t.Fatalf("observeNativeJob() = found %t, terminal %t, error %v", found, terminal, err)
	}
	stored := &actionsv1alpha1.WorkflowJob{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(workflowJob), stored); err != nil {
		t.Fatal(err)
	}
	condition := meta.FindStatusCondition(stored.Status.Conditions, actionsv1alpha1.WorkflowJobConditionSucceeded)
	if stored.Status.Result != actionsv1alpha1.WorkflowJobResultFailure || condition == nil || condition.Status != metav1.ConditionFalse || condition.Reason != "JobTimedOut" {
		t.Fatalf("timed-out WorkflowJob status = %#v", stored.Status)
	}
}

func TestRunnerResultConclusionDeterminesWorkflowJobStatus(t *testing.T) {
	for _, test := range []struct {
		name       string
		conclusion runner.ResultConclusion
		wantResult actionsv1alpha1.WorkflowJobResult
		wantReason string
	}{
		{name: "timed out", conclusion: runner.ResultConclusionTimedOut, wantResult: actionsv1alpha1.WorkflowJobResultFailure, wantReason: "JobTimedOut"},
		{name: "cancelled", conclusion: runner.ResultConclusionCancelled, wantResult: actionsv1alpha1.WorkflowJobResultCancelled, wantReason: "JobCancelled"},
	} {
		t.Run(test.name, func(t *testing.T) {
			scheme := runnerTestScheme(t)
			workflowJob := &actionsv1alpha1.WorkflowJob{
				TypeMeta:   metav1.TypeMeta{APIVersion: actionsv1alpha1.GroupVersion.String(), Kind: "WorkflowJob"},
				ObjectMeta: metav1.ObjectMeta{Name: "build", Namespace: "default"},
				Status:     actionsv1alpha1.WorkflowJobStatus{RunnerRef: &corev1.LocalObjectReference{Name: "runner"}},
			}
			clusterClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&actionsv1alpha1.WorkflowJob{}).WithObjects(workflowJob).Build()
			reconciler := &RunnerReconciler{Client: clusterClient}
			nativeJob := &batchv1.Job{Status: batchv1.JobStatus{Conditions: []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue}}}}
			executionResult := &runner.Result{Version: runner.ResultVersion, Conclusion: test.conclusion, Outputs: map[string]string{"artifact": "available"}}
			if err := reconciler.updateWorkflowJobStatus(context.Background(), workflowJob, nativeJob, executionResult, false); err != nil {
				t.Fatal(err)
			}
			stored := &actionsv1alpha1.WorkflowJob{}
			if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(workflowJob), stored); err != nil {
				t.Fatal(err)
			}
			condition := meta.FindStatusCondition(stored.Status.Conditions, actionsv1alpha1.WorkflowJobConditionSucceeded)
			if stored.Status.Result != test.wantResult || condition == nil || condition.Status != metav1.ConditionFalse || condition.Reason != test.wantReason {
				t.Fatalf("WorkflowJob status = %#v", stored.Status)
			}
			if stored.Status.Outputs["artifact"] != "available" {
				t.Fatalf("WorkflowJob outputs = %#v", stored.Status.Outputs)
			}
		})
	}
}

func TestNativeJobWithoutResultVersionSucceedsWithoutOutputs(t *testing.T) {
	scheme := runnerTestScheme(t)
	workflowJob := &actionsv1alpha1.WorkflowJob{
		TypeMeta:   metav1.TypeMeta{APIVersion: actionsv1alpha1.GroupVersion.String(), Kind: "WorkflowJob"},
		ObjectMeta: metav1.ObjectMeta{Name: "build", Namespace: "default", UID: types.UID("workflow-job-uid")},
		Status:     actionsv1alpha1.WorkflowJobStatus{RunnerRef: &corev1.LocalObjectReference{Name: "runner"}},
	}
	nativeJob := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: workflowJob.Name, Namespace: workflowJob.Namespace, UID: types.UID("native-job-uid")},
		Status: batchv1.JobStatus{Conditions: []batchv1.JobCondition{{
			Type: batchv1.JobComplete, Status: corev1.ConditionTrue,
		}}},
	}
	if err := controllerutil.SetControllerReference(workflowJob, nativeJob, scheme); err != nil {
		t.Fatal(err)
	}
	clusterClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&actionsv1alpha1.WorkflowJob{}, &batchv1.Job{}).
		WithObjects(workflowJob, nativeJob).
		Build()
	reconciler := &RunnerReconciler{Client: clusterClient, APIReader: clusterClient}
	found, terminal, err := reconciler.observeNativeJob(context.Background(), workflowJob)
	if err != nil || !found || !terminal {
		t.Fatalf("observeNativeJob() = found %t, terminal %t, error %v", found, terminal, err)
	}
	stored := &actionsv1alpha1.WorkflowJob{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(workflowJob), stored); err != nil {
		t.Fatal(err)
	}
	condition := meta.FindStatusCondition(stored.Status.Conditions, actionsv1alpha1.WorkflowJobConditionSucceeded)
	if condition == nil || condition.Status != metav1.ConditionTrue || condition.Reason != "JobSucceeded" {
		t.Fatalf("succeeded condition = %#v", condition)
	}
	if stored.Status.Outputs != nil {
		t.Fatalf("WorkflowJob outputs = %#v", stored.Status.Outputs)
	}
}

func TestExpiredPreStartFailureTerminatesJobAndCleansCredentials(t *testing.T) {
	scheme := runnerTestScheme(t)
	workflowJob := &actionsv1alpha1.WorkflowJob{
		TypeMeta:   metav1.TypeMeta{APIVersion: actionsv1alpha1.GroupVersion.String(), Kind: "WorkflowJob"},
		ObjectMeta: metav1.ObjectMeta{Name: "build", Namespace: "default", UID: types.UID("workflow-job-uid")},
		Status: actionsv1alpha1.WorkflowJobStatus{
			RunnerRef: &corev1.LocalObjectReference{Name: "runner"},
			Conditions: []metav1.Condition{{
				Type: actionsv1alpha1.WorkflowJobConditionScheduled, Status: metav1.ConditionTrue,
				Reason: "RunnerAssigned", LastTransitionTime: metav1.NewTime(time.Now().Add(-jobStartTimeout - time.Second)),
			}},
		},
	}
	authSecret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: childName(workflowJob.Name, "auth"), Namespace: workflowJob.Namespace}}
	if err := controllerutil.SetControllerReference(workflowJob, authSecret, scheme); err != nil {
		t.Fatal(err)
	}
	clusterClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&actionsv1alpha1.WorkflowJob{}).
		WithObjects(workflowJob, authSecret).
		Build()
	reconciler := &RunnerReconciler{Client: clusterClient, APIReader: clusterClient}
	expired, err := reconciler.failExpiredPreStart(context.Background(), workflowJob, "admission unavailable")
	if err != nil {
		t.Fatal(err)
	}
	if !expired {
		t.Fatal("expired pre-start failure remained retryable")
	}
	stored := &actionsv1alpha1.WorkflowJob{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(workflowJob), stored); err != nil {
		t.Fatal(err)
	}
	condition := meta.FindStatusCondition(stored.Status.Conditions, actionsv1alpha1.WorkflowJobConditionSucceeded)
	if condition == nil || condition.Status != metav1.ConditionFalse || condition.Reason != "JobStartFailed" {
		t.Fatalf("succeeded condition = %#v", condition)
	}
	err = clusterClient.Get(context.Background(), client.ObjectKeyFromObject(authSecret), &corev1.Secret{})
	if !apierrors.IsNotFound(err) {
		t.Fatalf("authentication Secret still exists: %v", err)
	}
}

func TestHandleJobTokenCreationError(t *testing.T) {
	tests := []struct {
		name       string
		statusCode int
		terminal   bool
	}{
		{name: "permission rejection", statusCode: http.StatusUnprocessableEntity, terminal: true},
		{name: "server failure", statusCode: http.StatusInternalServerError},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			scheme := runnerTestScheme(t)
			workflowJob := &actionsv1alpha1.WorkflowJob{
				ObjectMeta: metav1.ObjectMeta{Name: "build", Namespace: "default"},
				Status:     actionsv1alpha1.WorkflowJobStatus{RunnerRef: &corev1.LocalObjectReference{Name: "runner"}},
			}
			clusterClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&actionsv1alpha1.WorkflowJob{}).WithObjects(workflowJob).Build()
			reconciler := &RunnerReconciler{Client: clusterClient, APIReader: clusterClient}
			permissions := githubclient.InstallationPermissions{"issues": "write"}
			terminal, err := reconciler.handleJobTokenCreationError(context.Background(), workflowJob, permissions, &githubclient.APIError{
				StatusCode: test.statusCode,
				Status:     fmt.Sprintf("%d %s", test.statusCode, http.StatusText(test.statusCode)),
				Message:    "token request failed",
			})
			if terminal != test.terminal {
				t.Fatalf("terminal = %t, want %t", terminal, test.terminal)
			}
			if !test.terminal {
				if err == nil || !strings.Contains(err.Error(), `WorkflowJob "build"`) || !strings.Contains(err.Error(), "issues:write") {
					t.Fatalf("error = %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			stored := &actionsv1alpha1.WorkflowJob{}
			if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(workflowJob), stored); err != nil {
				t.Fatal(err)
			}
			condition := meta.FindStatusCondition(stored.Status.Conditions, actionsv1alpha1.WorkflowJobConditionSucceeded)
			if condition == nil || condition.Status != metav1.ConditionFalse || condition.Reason != "GitHubTokenPermissionsRejected" ||
				!strings.HasPrefix(condition.Message, "Creating the GitHub token") ||
				!strings.Contains(condition.Message, `WorkflowJob "build"`) || !strings.Contains(condition.Message, "issues:write") {
				t.Fatalf("succeeded condition = %#v", condition)
			}
		})
	}
}

func TestMissingPlanFailsAssignedWorkflowJob(t *testing.T) {
	scheme := runnerTestScheme(t)
	run := &actionsv1alpha1.WorkflowRun{
		TypeMeta:   metav1.TypeMeta{APIVersion: actionsv1alpha1.GroupVersion.String(), Kind: "WorkflowRun"},
		ObjectMeta: metav1.ObjectMeta{Name: "ci", Namespace: "default", UID: types.UID("run-uid")},
		Spec:       actionsv1alpha1.WorkflowRunSpec{ProjectRef: corev1.LocalObjectReference{Name: "project"}},
	}
	workflowJob := &actionsv1alpha1.WorkflowJob{
		TypeMeta: metav1.TypeMeta{APIVersion: actionsv1alpha1.GroupVersion.String(), Kind: "WorkflowJob"},
		ObjectMeta: metav1.ObjectMeta{
			Name: "build", Namespace: "default", UID: types.UID("job-uid"),
		},
		Spec:   actionsv1alpha1.WorkflowJobSpec{WorkflowRunRef: corev1.LocalObjectReference{Name: run.Name}},
		Status: actionsv1alpha1.WorkflowJobStatus{RunnerRef: &corev1.LocalObjectReference{Name: "runner"}},
	}
	if err := controllerutil.SetControllerReference(run, workflowJob, scheme); err != nil {
		t.Fatal(err)
	}
	authSecret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: childName(workflowJob.Name, "auth"), Namespace: workflowJob.Namespace}}
	if err := controllerutil.SetControllerReference(workflowJob, authSecret, scheme); err != nil {
		t.Fatal(err)
	}
	project := &actionsv1alpha1.Project{ObjectMeta: metav1.ObjectMeta{Name: "project", Namespace: "default"}}
	runnerObject := &actionsv1alpha1.Runner{ObjectMeta: metav1.ObjectMeta{Name: "runner", Namespace: "default"}}
	clusterClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&actionsv1alpha1.WorkflowJob{}).
		WithObjects(run, workflowJob, authSecret).
		Build()
	reconciler := &RunnerReconciler{Client: clusterClient, APIReader: clusterClient}
	terminal, err := reconciler.executeWorkflowJob(context.Background(), runnerObject, workflowJob, project)
	if err != nil {
		t.Fatal(err)
	}
	if !terminal {
		t.Fatal("missing plan did not terminate the WorkflowJob")
	}
	stored := &actionsv1alpha1.WorkflowJob{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(workflowJob), stored); err != nil {
		t.Fatal(err)
	}
	condition := meta.FindStatusCondition(stored.Status.Conditions, actionsv1alpha1.WorkflowJobConditionSucceeded)
	if condition == nil || condition.Status != metav1.ConditionFalse || condition.Reason != "PlanUnavailable" {
		t.Fatalf("succeeded condition = %#v", condition)
	}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(authSecret), &corev1.Secret{}); !apierrors.IsNotFound(err) {
		t.Fatalf("authentication Secret still exists: %v", err)
	}
}

func TestExecuteWorkflowJobMintsPlannedTokenPermissions(t *testing.T) {
	t.Run("external action", func(t *testing.T) {
		testExecuteWorkflowJobMintsPlannedTokenPermissions(t, []runner.Step{{Uses: "actions/checkout@v4"}}, true)
	})
	t.Run("script only", func(t *testing.T) {
		testExecuteWorkflowJobMintsPlannedTokenPermissions(t, []runner.Step{{Run: "true"}}, false)
	})
}

func testExecuteWorkflowJobMintsPlannedTokenPermissions(t *testing.T, steps []runner.Step, wantActionToken bool) {
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	privateKeyData := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(privateKey)})
	requests := []struct {
		Repositories []string
		Permissions  map[string]string
	}{}
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost || request.URL.Path != "/app/installations/2/access_tokens" {
			http.NotFound(writer, request)
			return
		}
		body := struct {
			Repositories []string          `json:"repositories"`
			Permissions  map[string]string `json:"permissions"`
		}{}
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			http.Error(writer, "invalid request", http.StatusBadRequest)
			return
		}
		requests = append(requests, struct {
			Repositories []string
			Permissions  map[string]string
		}{Repositories: body.Repositories, Permissions: body.Permissions})
		if len(body.Repositories) == 0 {
			fmt.Fprint(writer, `{"token":"action-token"}`)
			return
		}
		fmt.Fprint(writer, `{"token":"job-token"}`)
	}))
	defer server.Close()
	github, err := githubclient.NewClient(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}

	scheme := runnerTestScheme(t)
	project := &actionsv1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: "project", Namespace: "default", UID: types.UID("project-uid")},
		Spec: actionsv1alpha1.ProjectSpec{Source: actionsv1alpha1.ProjectSource{Type: actionsv1alpha1.SourceTypeGitHub, GitHub: &actionsv1alpha1.GitHubAppConfiguration{
			AppID: 1, InstallationID: 2,
			PrivateKeySecretRef: corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "github"}, Key: "private-key"},
		}}},
	}
	run := &actionsv1alpha1.WorkflowRun{
		TypeMeta:   metav1.TypeMeta{APIVersion: actionsv1alpha1.GroupVersion.String(), Kind: "WorkflowRun"},
		ObjectMeta: metav1.ObjectMeta{Name: "ci", Namespace: "default", UID: types.UID("run-uid")},
		Spec: actionsv1alpha1.WorkflowRunSpec{
			ProjectRef: corev1.LocalObjectReference{Name: project.Name},
			Source: actionsv1alpha1.WorkflowRunSource{Type: actionsv1alpha1.SourceTypeGitHub, GitHub: &actionsv1alpha1.GitHubWorkflowRunSource{
				Repository: actionsv1alpha1.GitHubRepository{ID: 1, Owner: "acme", Name: "example"},
				Event:      actionsv1alpha1.GitHubEvent{Name: actionsv1alpha1.GitHubEventNamePush, DeliveryID: "delivery"},
				Revision:   actionsv1alpha1.GitRevision{SHA: strings.Repeat("a", 40), Ref: "refs/heads/main"},
			}},
		},
	}
	workflowJob := &actionsv1alpha1.WorkflowJob{
		TypeMeta: metav1.TypeMeta{APIVersion: actionsv1alpha1.GroupVersion.String(), Kind: "WorkflowJob"},
		ObjectMeta: metav1.ObjectMeta{
			Name: "build", Namespace: "default", UID: types.UID("job-uid"),
			Labels: map[string]string{actionsv1alpha1.LabelProjectUID: string(project.UID), actionsv1alpha1.LabelWorkflowRunUID: string(run.UID)},
		},
		Spec:   actionsv1alpha1.WorkflowJobSpec{WorkflowRunRef: corev1.LocalObjectReference{Name: run.Name}, JobID: "build"},
		Status: actionsv1alpha1.WorkflowJobStatus{RunnerRef: &corev1.LocalObjectReference{Name: "runner"}},
	}
	if err := controllerutil.SetControllerReference(run, workflowJob, scheme); err != nil {
		t.Fatal(err)
	}
	plan := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: childName(workflowJob.Name, "plan"), Namespace: workflowJob.Namespace},
		Data:       map[string]string{jobPlanKey: runnerControllerPlanData(t, map[string]string{"issues": "write", "statuses": "read"}, steps...)},
	}
	if err := controllerutil.SetControllerReference(workflowJob, plan, scheme); err != nil {
		t.Fatal(err)
	}
	credentials := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "github", Namespace: "default"}, Data: map[string][]byte{"private-key": privateKeyData}}
	runnerObject := &actionsv1alpha1.Runner{
		ObjectMeta: metav1.ObjectMeta{Name: "runner", Namespace: "default", UID: types.UID("runner-uid")},
		Spec:       actionsv1alpha1.RunnerSpec{Execution: actionsv1alpha1.RunnerExecutionSpec{Image: "runner:test"}},
	}
	clusterClient := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&actionsv1alpha1.WorkflowJob{}, &batchv1.Job{}).
		WithObjects(project, run, workflowJob, plan, credentials, runnerObject).Build()
	reconciler := &RunnerReconciler{Client: clusterClient, APIReader: clusterClient, GitHub: github}
	terminal, err := reconciler.executeWorkflowJob(context.Background(), runnerObject, workflowJob, project)
	if err != nil || terminal {
		t.Fatalf("executeWorkflowJob() = terminal %v, error %v", terminal, err)
	}
	wantRequests := 1
	if wantActionToken {
		wantRequests = 2
	}
	if len(requests) != wantRequests || !slices.Equal(requests[0].Repositories, []string{"example"}) ||
		!maps.Equal(requests[0].Permissions, map[string]string{"issues": "write", "statuses": "read"}) {
		t.Fatalf("installation token requests = %#v", requests)
	}
	if wantActionToken && (len(requests[1].Repositories) != 0 || !maps.Equal(requests[1].Permissions, map[string]string{"contents": "read"})) {
		t.Fatalf("action installation token request = %#v", requests[1])
	}
	auth := &corev1.Secret{}
	if err := clusterClient.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: childName(workflowJob.Name, "auth")}, auth); err != nil {
		t.Fatal(err)
	}
	wantActionTokenValue := ""
	if wantActionToken {
		wantActionTokenValue = "action-token"
	}
	if string(auth.Data[jobTokenSecretKey]) != "job-token" || string(auth.Data[actionTokenSecretKey]) != wantActionTokenValue {
		t.Fatalf("authentication Secret data = %#v", auth.Data)
	}
}

func TestMissingNeedsContextFailsAssignedWorkflowJob(t *testing.T) {
	scheme := runnerTestScheme(t)
	run := &actionsv1alpha1.WorkflowRun{
		TypeMeta:   metav1.TypeMeta{APIVersion: actionsv1alpha1.GroupVersion.String(), Kind: "WorkflowRun"},
		ObjectMeta: metav1.ObjectMeta{Name: "ci", Namespace: "default", UID: types.UID("run-uid")},
		Spec:       actionsv1alpha1.WorkflowRunSpec{ProjectRef: corev1.LocalObjectReference{Name: "project"}},
	}
	workflowJob := &actionsv1alpha1.WorkflowJob{
		TypeMeta: metav1.TypeMeta{APIVersion: actionsv1alpha1.GroupVersion.String(), Kind: "WorkflowJob"},
		ObjectMeta: metav1.ObjectMeta{
			Name: "report", Namespace: "default", UID: types.UID("job-uid"),
		},
		Spec: actionsv1alpha1.WorkflowJobSpec{
			WorkflowRunRef: corev1.LocalObjectReference{Name: run.Name},
			Needs:          []string{"build"},
		},
		Status: actionsv1alpha1.WorkflowJobStatus{RunnerRef: &corev1.LocalObjectReference{Name: "runner"}},
	}
	if err := controllerutil.SetControllerReference(run, workflowJob, scheme); err != nil {
		t.Fatal(err)
	}
	plan := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: childName(workflowJob.Name, "plan"), Namespace: workflowJob.Namespace},
		Data:       map[string]string{jobPlanKey: runnerControllerPlanData(t, map[string]string{"issues": "write"})},
	}
	if err := controllerutil.SetControllerReference(workflowJob, plan, scheme); err != nil {
		t.Fatal(err)
	}
	authSecret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: childName(workflowJob.Name, "auth"), Namespace: workflowJob.Namespace}}
	if err := controllerutil.SetControllerReference(workflowJob, authSecret, scheme); err != nil {
		t.Fatal(err)
	}
	project := &actionsv1alpha1.Project{ObjectMeta: metav1.ObjectMeta{Name: "project", Namespace: "default"}}
	runnerObject := &actionsv1alpha1.Runner{ObjectMeta: metav1.ObjectMeta{Name: "runner", Namespace: "default"}}
	clusterClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&actionsv1alpha1.WorkflowJob{}).
		WithObjects(run, workflowJob, plan, authSecret).
		Build()
	reconciler := &RunnerReconciler{Client: clusterClient, APIReader: clusterClient}
	terminal, err := reconciler.executeWorkflowJob(context.Background(), runnerObject, workflowJob, project)
	if err != nil {
		t.Fatal(err)
	}
	if !terminal {
		t.Fatal("missing needs context did not terminate the WorkflowJob")
	}
	stored := &actionsv1alpha1.WorkflowJob{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(workflowJob), stored); err != nil {
		t.Fatal(err)
	}
	condition := meta.FindStatusCondition(stored.Status.Conditions, actionsv1alpha1.WorkflowJobConditionSucceeded)
	if condition == nil || condition.Status != metav1.ConditionFalse || condition.Reason != "PlanUnavailable" || !strings.Contains(condition.Message, "Needs context ConfigMap") {
		t.Fatalf("succeeded condition = %#v", condition)
	}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(authSecret), &corev1.Secret{}); !apierrors.IsNotFound(err) {
		t.Fatalf("authentication Secret still exists: %v", err)
	}
}

func TestPlanReadUsesLiveReader(t *testing.T) {
	scheme := runnerTestScheme(t)
	run := &actionsv1alpha1.WorkflowRun{
		TypeMeta:   metav1.TypeMeta{APIVersion: actionsv1alpha1.GroupVersion.String(), Kind: "WorkflowRun"},
		ObjectMeta: metav1.ObjectMeta{Name: "ci", Namespace: "default", UID: types.UID("run-uid")},
		Spec:       actionsv1alpha1.WorkflowRunSpec{ProjectRef: corev1.LocalObjectReference{Name: "project"}},
	}
	workflowJob := &actionsv1alpha1.WorkflowJob{
		TypeMeta:   metav1.TypeMeta{APIVersion: actionsv1alpha1.GroupVersion.String(), Kind: "WorkflowJob"},
		ObjectMeta: metav1.ObjectMeta{Name: "build", Namespace: "default", UID: types.UID("job-uid")},
		Spec:       actionsv1alpha1.WorkflowJobSpec{WorkflowRunRef: corev1.LocalObjectReference{Name: run.Name}},
		Status:     actionsv1alpha1.WorkflowJobStatus{RunnerRef: &corev1.LocalObjectReference{Name: "runner"}},
	}
	if err := controllerutil.SetControllerReference(run, workflowJob, scheme); err != nil {
		t.Fatal(err)
	}
	plan := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: childName(workflowJob.Name, "plan"), Namespace: workflowJob.Namespace},
		Data:       map[string]string{jobPlanKey: runnerControllerPlanData(t, map[string]string{"issues": "write"})},
	}
	if err := controllerutil.SetControllerReference(workflowJob, plan, scheme); err != nil {
		t.Fatal(err)
	}
	cachedClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&actionsv1alpha1.WorkflowJob{}).WithObjects(run, workflowJob).Build()
	liveReader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(run, workflowJob, plan).Build()
	reconciler := &RunnerReconciler{Client: cachedClient, APIReader: liveReader}
	project := &actionsv1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: "project", Namespace: "default"},
		Spec: actionsv1alpha1.ProjectSpec{Source: actionsv1alpha1.ProjectSource{GitHub: &actionsv1alpha1.GitHubAppConfiguration{
			PrivateKeySecretRef: corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "credentials"}, Key: "private-key"},
		}}},
	}
	runnerObject := &actionsv1alpha1.Runner{ObjectMeta: metav1.ObjectMeta{Name: "runner", Namespace: "default"}}

	terminal, err := reconciler.executeWorkflowJob(context.Background(), runnerObject, workflowJob, project)
	if err == nil || terminal {
		t.Fatalf("executeWorkflowJob() = terminal %v, error %v", terminal, err)
	}
	stored := &actionsv1alpha1.WorkflowJob{}
	if err := cachedClient.Get(context.Background(), client.ObjectKeyFromObject(workflowJob), stored); err != nil {
		t.Fatal(err)
	}
	condition := meta.FindStatusCondition(stored.Status.Conditions, actionsv1alpha1.WorkflowJobConditionSucceeded)
	if condition != nil && condition.Reason == "PlanUnavailable" {
		t.Fatalf("live plan was treated as unavailable: %#v", condition)
	}
}

func TestMissingPlanDoesNotInterruptAnExistingNativeJob(t *testing.T) {
	scheme := runnerTestScheme(t)
	run := &actionsv1alpha1.WorkflowRun{
		TypeMeta:   metav1.TypeMeta{APIVersion: actionsv1alpha1.GroupVersion.String(), Kind: "WorkflowRun"},
		ObjectMeta: metav1.ObjectMeta{Name: "ci", Namespace: "default", UID: types.UID("run-uid")},
		Spec:       actionsv1alpha1.WorkflowRunSpec{ProjectRef: corev1.LocalObjectReference{Name: "project"}},
	}
	workflowJob := &actionsv1alpha1.WorkflowJob{
		TypeMeta:   metav1.TypeMeta{APIVersion: actionsv1alpha1.GroupVersion.String(), Kind: "WorkflowJob"},
		ObjectMeta: metav1.ObjectMeta{Name: "build", Namespace: "default", UID: types.UID("job-uid")},
		Spec:       actionsv1alpha1.WorkflowJobSpec{WorkflowRunRef: corev1.LocalObjectReference{Name: run.Name}},
		Status:     actionsv1alpha1.WorkflowJobStatus{RunnerRef: &corev1.LocalObjectReference{Name: "runner"}},
	}
	if err := controllerutil.SetControllerReference(run, workflowJob, scheme); err != nil {
		t.Fatal(err)
	}
	nativeJob := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: workflowJob.Name, Namespace: workflowJob.Namespace}}
	if err := controllerutil.SetControllerReference(workflowJob, nativeJob, scheme); err != nil {
		t.Fatal(err)
	}
	clusterClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&actionsv1alpha1.WorkflowJob{}, &batchv1.Job{}).
		WithObjects(run, workflowJob, nativeJob).
		Build()
	reconciler := &RunnerReconciler{Client: clusterClient, APIReader: clusterClient}
	project := &actionsv1alpha1.Project{ObjectMeta: metav1.ObjectMeta{Name: "project", Namespace: "default"}}
	runnerObject := &actionsv1alpha1.Runner{ObjectMeta: metav1.ObjectMeta{Name: "runner", Namespace: "default"}}
	terminal, err := reconciler.executeWorkflowJob(context.Background(), runnerObject, workflowJob, project)
	if err != nil {
		t.Fatal(err)
	}
	if terminal {
		t.Fatal("an active native Job was failed after its plan was deleted")
	}
	stored := &actionsv1alpha1.WorkflowJob{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(workflowJob), stored); err != nil {
		t.Fatal(err)
	}
	condition := meta.FindStatusCondition(stored.Status.Conditions, actionsv1alpha1.WorkflowJobConditionSucceeded)
	if condition == nil || condition.Status != metav1.ConditionUnknown || condition.Reason != "JobRunning" {
		t.Fatalf("succeeded condition = %#v", condition)
	}
}

func TestDeletingWorkflowRunStopsAssignedJobBeforeExecution(t *testing.T) {
	scheme := runnerTestScheme(t)
	deletionTime := metav1.Now()
	run := &actionsv1alpha1.WorkflowRun{
		TypeMeta: metav1.TypeMeta{APIVersion: actionsv1alpha1.GroupVersion.String(), Kind: "WorkflowRun"},
		ObjectMeta: metav1.ObjectMeta{
			Name: "ci", Namespace: "default", UID: types.UID("run-uid"),
			DeletionTimestamp: &deletionTime, Finalizers: []string{"test"},
		},
		Spec: actionsv1alpha1.WorkflowRunSpec{ProjectRef: corev1.LocalObjectReference{Name: "project"}},
	}
	workflowJob := &actionsv1alpha1.WorkflowJob{
		TypeMeta:   metav1.TypeMeta{APIVersion: actionsv1alpha1.GroupVersion.String(), Kind: "WorkflowJob"},
		ObjectMeta: metav1.ObjectMeta{Name: "build", Namespace: "default", UID: types.UID("job-uid")},
		Spec:       actionsv1alpha1.WorkflowJobSpec{WorkflowRunRef: corev1.LocalObjectReference{Name: run.Name}},
		Status:     actionsv1alpha1.WorkflowJobStatus{RunnerRef: &corev1.LocalObjectReference{Name: "runner"}},
	}
	if err := controllerutil.SetControllerReference(run, workflowJob, scheme); err != nil {
		t.Fatal(err)
	}
	clusterClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&actionsv1alpha1.WorkflowJob{}).
		WithObjects(run, workflowJob).
		Build()
	reconciler := &RunnerReconciler{Client: clusterClient, APIReader: clusterClient}
	project := &actionsv1alpha1.Project{ObjectMeta: metav1.ObjectMeta{Name: "project", Namespace: "default"}}
	runnerObject := &actionsv1alpha1.Runner{ObjectMeta: metav1.ObjectMeta{Name: "runner", Namespace: "default"}}

	terminal, err := reconciler.executeWorkflowJob(context.Background(), runnerObject, workflowJob, project)
	if err != nil {
		t.Fatal(err)
	}
	if !terminal {
		t.Fatal("assigned WorkflowJob remained active after WorkflowRun deletion was requested")
	}
	stored := &actionsv1alpha1.WorkflowJob{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(workflowJob), stored); err != nil {
		t.Fatal(err)
	}
	condition := meta.FindStatusCondition(stored.Status.Conditions, actionsv1alpha1.WorkflowJobConditionSucceeded)
	if condition == nil || condition.Status != metav1.ConditionFalse || condition.Reason != "CancellationRequested" {
		t.Fatalf("succeeded condition = %#v", condition)
	}
	if stored.Status.Result != actionsv1alpha1.WorkflowJobResultCancelled {
		t.Fatalf("result = %q, want cancelled", stored.Status.Result)
	}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(workflowJob), &batchv1.Job{}); !apierrors.IsNotFound(err) {
		t.Fatalf("native Job exists: %v", err)
	}
	secretKey := client.ObjectKey{Namespace: workflowJob.Namespace, Name: childName(workflowJob.Name, "auth")}
	if err := clusterClient.Get(context.Background(), secretKey, &corev1.Secret{}); !apierrors.IsNotFound(err) {
		t.Fatalf("authentication Secret exists: %v", err)
	}
}

func TestCancellationRequestStopsActiveNativeJob(t *testing.T) {
	scheme := runnerTestScheme(t)
	run := &actionsv1alpha1.WorkflowRun{
		ObjectMeta: metav1.ObjectMeta{Name: "ci", Namespace: "default", UID: types.UID("run-uid")},
		Spec:       actionsv1alpha1.WorkflowRunSpec{ProjectRef: corev1.LocalObjectReference{Name: "project"}},
	}
	workflowJob := &actionsv1alpha1.WorkflowJob{
		ObjectMeta: metav1.ObjectMeta{Name: "build", Namespace: "default", UID: types.UID("job-uid")},
		Spec:       actionsv1alpha1.WorkflowJobSpec{WorkflowRunRef: corev1.LocalObjectReference{Name: run.Name}},
		Status: actionsv1alpha1.WorkflowJobStatus{
			RunnerRef: &corev1.LocalObjectReference{Name: "runner"},
			Conditions: []metav1.Condition{{
				Type: actionsv1alpha1.WorkflowJobConditionCancellationRequested, Status: metav1.ConditionTrue,
			}},
		},
	}
	if err := controllerutil.SetControllerReference(run, workflowJob, scheme); err != nil {
		t.Fatal(err)
	}
	nativeJob := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: workflowJob.Name, Namespace: workflowJob.Namespace},
		Status:     batchv1.JobStatus{Active: 1, StartTime: pointerTo(metav1.Now())},
	}
	if err := controllerutil.SetControllerReference(workflowJob, nativeJob, scheme); err != nil {
		t.Fatal(err)
	}
	clusterClient := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&actionsv1alpha1.WorkflowJob{}, &batchv1.Job{}).
		WithObjects(run, workflowJob, nativeJob).Build()
	writeClient := &recordingDeleteClient{Client: clusterClient}
	reconciler := &RunnerReconciler{Client: writeClient, APIReader: clusterClient}
	project := &actionsv1alpha1.Project{ObjectMeta: metav1.ObjectMeta{Name: "project", Namespace: "default"}}
	runnerObject := &actionsv1alpha1.Runner{ObjectMeta: metav1.ObjectMeta{Name: "runner", Namespace: "default"}}

	terminal, err := reconciler.executeWorkflowJob(context.Background(), runnerObject, workflowJob, project)
	if err != nil {
		t.Fatal(err)
	}
	if !terminal {
		t.Fatal("active native Job remained active after cancellation was requested")
	}
	stored := &actionsv1alpha1.WorkflowJob{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(workflowJob), stored); err != nil {
		t.Fatal(err)
	}
	if stored.Status.Result != actionsv1alpha1.WorkflowJobResultCancelled {
		t.Fatalf("result = %q, want cancelled", stored.Status.Result)
	}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(nativeJob), &batchv1.Job{}); !apierrors.IsNotFound(err) {
		t.Fatalf("native Job exists: %v", err)
	}
	if writeClient.deleteOptions == nil || writeClient.deleteOptions.PropagationPolicy == nil || *writeClient.deleteOptions.PropagationPolicy != metav1.DeletePropagationBackground {
		t.Fatalf("native Job delete options = %#v, want background propagation", writeClient.deleteOptions)
	}
}

func TestMissingNativeJobDoesNotRestartExecution(t *testing.T) {
	scheme := runnerTestScheme(t)
	run := &actionsv1alpha1.WorkflowRun{
		TypeMeta:   metav1.TypeMeta{APIVersion: actionsv1alpha1.GroupVersion.String(), Kind: "WorkflowRun"},
		ObjectMeta: metav1.ObjectMeta{Name: "ci", Namespace: "default", UID: types.UID("run-uid")},
		Spec:       actionsv1alpha1.WorkflowRunSpec{ProjectRef: corev1.LocalObjectReference{Name: "project"}},
	}
	workflowJob := &actionsv1alpha1.WorkflowJob{
		TypeMeta:   metav1.TypeMeta{APIVersion: actionsv1alpha1.GroupVersion.String(), Kind: "WorkflowJob"},
		ObjectMeta: metav1.ObjectMeta{Name: "build", Namespace: "default", UID: types.UID("job-uid")},
		Spec:       actionsv1alpha1.WorkflowJobSpec{WorkflowRunRef: corev1.LocalObjectReference{Name: run.Name}},
		Status: actionsv1alpha1.WorkflowJobStatus{
			RunnerRef: &corev1.LocalObjectReference{Name: "runner"},
			StartTime: pointerTo(metav1.Now()),
			Conditions: []metav1.Condition{{
				Type: actionsv1alpha1.WorkflowJobConditionSucceeded, Status: metav1.ConditionUnknown, Reason: "JobRunning", LastTransitionTime: metav1.Now(),
			}},
		},
	}
	if err := controllerutil.SetControllerReference(run, workflowJob, scheme); err != nil {
		t.Fatal(err)
	}
	authSecret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: childName(workflowJob.Name, "auth"), Namespace: workflowJob.Namespace}}
	if err := controllerutil.SetControllerReference(workflowJob, authSecret, scheme); err != nil {
		t.Fatal(err)
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "build-pod", Namespace: "default", Labels: map[string]string{actionsv1alpha1.LabelWorkflowJobUID: string(workflowJob.UID)}},
		Status:     corev1.PodStatus{Phase: corev1.PodRunning},
	}
	clusterClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&actionsv1alpha1.WorkflowJob{}).
		WithObjects(run, workflowJob, authSecret, pod).
		Build()
	reconciler := &RunnerReconciler{Client: clusterClient, APIReader: clusterClient}
	project := &actionsv1alpha1.Project{ObjectMeta: metav1.ObjectMeta{Name: "project", Namespace: "default"}}
	runnerObject := &actionsv1alpha1.Runner{ObjectMeta: metav1.ObjectMeta{Name: "runner", Namespace: "default"}}

	terminal, err := reconciler.executeWorkflowJob(context.Background(), runnerObject, workflowJob, project)
	if err != nil {
		t.Fatal(err)
	}
	if terminal {
		t.Fatal("WorkflowJob terminated while its Pod was still active")
	}
	if err := clusterClient.Delete(context.Background(), pod); err != nil {
		t.Fatal(err)
	}
	terminal, err = reconciler.executeWorkflowJob(context.Background(), runnerObject, workflowJob, project)
	if err != nil {
		t.Fatal(err)
	}
	if !terminal {
		t.Fatal("WorkflowJob did not fail after its native Job and Pod disappeared")
	}
	if err := clusterClient.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: workflowJob.Name}, &batchv1.Job{}); !apierrors.IsNotFound(err) {
		t.Fatalf("native Job was recreated: %v", err)
	}
	stored := &actionsv1alpha1.WorkflowJob{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(workflowJob), stored); err != nil {
		t.Fatal(err)
	}
	condition := meta.FindStatusCondition(stored.Status.Conditions, actionsv1alpha1.WorkflowJobConditionSucceeded)
	if condition == nil || condition.Status != metav1.ConditionFalse || condition.Reason != "ExecutionStateLost" {
		t.Fatalf("succeeded condition = %#v", condition)
	}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(authSecret), &corev1.Secret{}); !apierrors.IsNotFound(err) {
		t.Fatalf("authentication Secret still exists: %v", err)
	}
}

func TestAssignedWorkflowJobRejectsRecreatedProject(t *testing.T) {
	scheme := runnerTestScheme(t)
	run := &actionsv1alpha1.WorkflowRun{
		TypeMeta:   metav1.TypeMeta{APIVersion: actionsv1alpha1.GroupVersion.String(), Kind: "WorkflowRun"},
		ObjectMeta: metav1.ObjectMeta{Name: "ci", Namespace: "default", UID: types.UID("run-uid")},
		Spec:       actionsv1alpha1.WorkflowRunSpec{ProjectRef: corev1.LocalObjectReference{Name: "project"}},
	}
	workflowJob := &actionsv1alpha1.WorkflowJob{
		TypeMeta: metav1.TypeMeta{APIVersion: actionsv1alpha1.GroupVersion.String(), Kind: "WorkflowJob"},
		ObjectMeta: metav1.ObjectMeta{
			Name: "build", Namespace: "default", UID: types.UID("job-uid"),
			Labels: map[string]string{actionsv1alpha1.LabelProjectUID: "original-project-uid"},
		},
		Spec:   actionsv1alpha1.WorkflowJobSpec{WorkflowRunRef: corev1.LocalObjectReference{Name: run.Name}},
		Status: actionsv1alpha1.WorkflowJobStatus{RunnerRef: &corev1.LocalObjectReference{Name: "runner"}},
	}
	if err := controllerutil.SetControllerReference(run, workflowJob, scheme); err != nil {
		t.Fatal(err)
	}
	authSecret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: childName(workflowJob.Name, "auth"), Namespace: workflowJob.Namespace}}
	if err := controllerutil.SetControllerReference(workflowJob, authSecret, scheme); err != nil {
		t.Fatal(err)
	}
	clusterClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&actionsv1alpha1.WorkflowJob{}).WithObjects(run, workflowJob, authSecret).Build()
	reconciler := &RunnerReconciler{Client: clusterClient, APIReader: clusterClient}
	project := &actionsv1alpha1.Project{ObjectMeta: metav1.ObjectMeta{Name: "project", Namespace: "default", UID: types.UID("replacement-project-uid")}}
	runnerObject := &actionsv1alpha1.Runner{ObjectMeta: metav1.ObjectMeta{Name: "runner", Namespace: "default"}}

	terminal, err := reconciler.executeWorkflowJob(context.Background(), runnerObject, workflowJob, project)
	if err != nil {
		t.Fatal(err)
	}
	if !terminal {
		t.Fatal("WorkflowJob remained active after its project was recreated")
	}
	stored := &actionsv1alpha1.WorkflowJob{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(workflowJob), stored); err != nil {
		t.Fatal(err)
	}
	condition := meta.FindStatusCondition(stored.Status.Conditions, actionsv1alpha1.WorkflowJobConditionSucceeded)
	if condition == nil || condition.Status != metav1.ConditionFalse || condition.Reason != "ProjectRecreated" {
		t.Fatalf("succeeded condition = %#v", condition)
	}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(authSecret), &corev1.Secret{}); !apierrors.IsNotFound(err) {
		t.Fatalf("authentication Secret still exists: %v", err)
	}
}

func TestEnsureAuthSecretStoresRunnerTokens(t *testing.T) {
	scheme := runnerTestScheme(t)
	workflowJob := &actionsv1alpha1.WorkflowJob{
		TypeMeta:   metav1.TypeMeta{APIVersion: actionsv1alpha1.GroupVersion.String(), Kind: "WorkflowJob"},
		ObjectMeta: metav1.ObjectMeta{Name: "build", Namespace: "default", UID: types.UID("job-uid")},
	}
	clusterClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(workflowJob).Build()
	reconciler := &RunnerReconciler{Client: clusterClient, APIReader: clusterClient}
	if err := reconciler.ensureAuthSecret(context.Background(), workflowJob, "job-token", "action-token", "artifact-token"); err != nil {
		t.Fatal(err)
	}
	secret := &corev1.Secret{}
	if err := clusterClient.Get(context.Background(), client.ObjectKey{Namespace: workflowJob.Namespace, Name: childName(workflowJob.Name, "auth")}, secret); err != nil {
		t.Fatal(err)
	}
	if string(secret.Data[jobTokenSecretKey]) != "job-token" || string(secret.Data[actionTokenSecretKey]) != "action-token" {
		t.Fatalf("authentication Secret data = %#v", secret.Data)
	}
	if string(secret.Data[artifact.TokenSecretKey]) != "artifact-token" {
		t.Fatalf("artifact token = %q", secret.Data[artifact.TokenSecretKey])
	}
}

func TestCleanupAuthSecretRevokesRunnerTokens(t *testing.T) {
	scheme := runnerTestScheme(t)
	workflowJob := &actionsv1alpha1.WorkflowJob{
		TypeMeta:   metav1.TypeMeta{APIVersion: actionsv1alpha1.GroupVersion.String(), Kind: "WorkflowJob"},
		ObjectMeta: metav1.ObjectMeta{Name: "build", Namespace: "default", UID: types.UID("job-uid")},
	}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: childName(workflowJob.Name, "auth"), Namespace: workflowJob.Namespace},
		Data: map[string][]byte{
			jobTokenSecretKey:    []byte("job-token"),
			actionTokenSecretKey: []byte("action-token"),
		},
	}
	if err := controllerutil.SetControllerReference(workflowJob, secret, scheme); err != nil {
		t.Fatal(err)
	}
	revoked := []string{}
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodDelete || request.URL.Path != "/installation/token" {
			http.NotFound(writer, request)
			return
		}
		revoked = append(revoked, strings.TrimPrefix(request.Header.Get("Authorization"), "Bearer "))
		writer.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	github, err := githubclient.NewClient(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	clusterClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(workflowJob, secret).Build()
	reconciler := &RunnerReconciler{Client: clusterClient, APIReader: clusterClient, GitHub: github}
	if err := reconciler.cleanupAuthSecret(context.Background(), workflowJob); err != nil {
		t.Fatal(err)
	}
	slices.Sort(revoked)
	if !slices.Equal(revoked, []string{"action-token", "job-token"}) {
		t.Fatalf("revoked tokens = %#v", revoked)
	}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(secret), &corev1.Secret{}); !apierrors.IsNotFound(err) {
		t.Fatalf("authentication Secret still exists: %v", err)
	}
}

func TestEnsureAuthSecretRejectsUnownedCollision(t *testing.T) {
	scheme := runnerTestScheme(t)
	workflowJob := &actionsv1alpha1.WorkflowJob{
		TypeMeta:   metav1.TypeMeta{APIVersion: actionsv1alpha1.GroupVersion.String(), Kind: "WorkflowJob"},
		ObjectMeta: metav1.ObjectMeta{Name: "build", Namespace: "default", UID: types.UID("job-uid")},
	}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: childName(workflowJob.Name, "auth"), Namespace: workflowJob.Namespace},
		Data:       map[string][]byte{jobTokenSecretKey: []byte("existing")},
	}
	clusterClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(workflowJob, secret).Build()
	reconciler := &RunnerReconciler{Client: clusterClient, APIReader: clusterClient}
	if err := reconciler.ensureAuthSecret(context.Background(), workflowJob, "replacement", "action-replacement", "artifact-replacement"); err == nil {
		t.Fatal("unowned authentication Secret was accepted")
	}
	stored := &corev1.Secret{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(secret), stored); err != nil {
		t.Fatal(err)
	}
	if string(stored.Data[jobTokenSecretKey]) != "existing" {
		t.Fatalf("unowned Secret token = %q", stored.Data[jobTokenSecretKey])
	}
}

func runnerControllerPlanData(t *testing.T, permissions map[string]string, steps ...runner.Step) string {
	t.Helper()
	if len(steps) == 0 {
		steps = []runner.Step{{Run: "true"}}
	}
	plan := runner.Plan{
		Version:                runner.PlanVersion,
		Run:                    runner.Run{ID: 1, Number: 1, Attempt: 1, Actor: "octocat"},
		Repository:             runner.Repository{ID: 1, Owner: "acme", Name: "example", ServerURL: "https://github.com", APIURL: "https://api.github.com", ActionCloneBaseURL: "https://github.com"},
		Event:                  runner.Event{Name: "push", DeliveryID: "delivery"},
		Revision:               runner.Revision{SHA: strings.Repeat("a", 40), Ref: "refs/heads/main", RefName: "main"},
		WorkflowName:           "CI",
		JobID:                  "build",
		GitHubTokenPermissions: permissions,
		TimeoutSeconds:         int64((6 * time.Hour) / time.Second),
		CleanupTimeoutSeconds:  int64(runner.CleanupTimeout / time.Second),
		Steps:                  steps,
	}
	data, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func TestDeletingBusyRunnerFinalizesItsWorkflowJob(t *testing.T) {
	scheme := runnerTestScheme(t)
	deletionTime := metav1.Now()
	runnerObject := &actionsv1alpha1.Runner{
		ObjectMeta: metav1.ObjectMeta{
			Name: "runner", Namespace: "default", DeletionTimestamp: &deletionTime, Finalizers: []string{runnerFinalizer},
		},
		Status: actionsv1alpha1.RunnerStatus{WorkflowJobRef: &corev1.LocalObjectReference{Name: "build"}},
	}
	workflowJob := &actionsv1alpha1.WorkflowJob{
		TypeMeta:   metav1.TypeMeta{APIVersion: actionsv1alpha1.GroupVersion.String(), Kind: "WorkflowJob"},
		ObjectMeta: metav1.ObjectMeta{Name: "build", Namespace: "default", UID: types.UID("job-uid")},
		Status: actionsv1alpha1.WorkflowJobStatus{
			RunnerRef:  &corev1.LocalObjectReference{Name: runnerObject.Name},
			Conditions: []metav1.Condition{{Type: actionsv1alpha1.WorkflowJobConditionSucceeded, Status: metav1.ConditionTrue, Reason: "JobSucceeded", LastTransitionTime: metav1.Now()}},
		},
	}
	nativeJob := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: workflowJob.Name, Namespace: workflowJob.Namespace}}
	if err := controllerutil.SetControllerReference(workflowJob, nativeJob, scheme); err != nil {
		t.Fatal(err)
	}
	clusterClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&actionsv1alpha1.Runner{}, &actionsv1alpha1.WorkflowJob{}, &batchv1.Job{}).
		WithObjects(runnerObject, workflowJob, nativeJob).
		Build()
	reconciler := &RunnerReconciler{Client: clusterClient, APIReader: clusterClient}
	if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(runnerObject)}); err != nil {
		t.Fatal(err)
	}
	storedJob := &batchv1.Job{}
	if err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(nativeJob), storedJob); err != nil {
		t.Fatal(err)
	}
	if storedJob.Spec.TTLSecondsAfterFinished == nil {
		t.Fatal("native Job was not finalized before Runner deletion")
	}
	storedRunner := &actionsv1alpha1.Runner{}
	err := clusterClient.Get(context.Background(), client.ObjectKeyFromObject(runnerObject), storedRunner)
	if err == nil && controllerutil.ContainsFinalizer(storedRunner, runnerFinalizer) {
		t.Fatal("Runner finalizer remained after job finalization")
	}
	if err != nil && !apierrors.IsNotFound(err) {
		t.Fatal(err)
	}
}

func TestWorkflowJobEventEnqueuesOneIdleMatchingRunner(t *testing.T) {
	scheme := runnerTestScheme(t)
	run := &actionsv1alpha1.WorkflowRun{
		ObjectMeta: metav1.ObjectMeta{Name: "ci", Namespace: "default"},
		Spec:       actionsv1alpha1.WorkflowRunSpec{ProjectRef: corev1.LocalObjectReference{Name: "project"}},
	}
	workflowJob := &actionsv1alpha1.WorkflowJob{
		ObjectMeta: metav1.ObjectMeta{Name: "build", Namespace: "default"},
		Spec: actionsv1alpha1.WorkflowJobSpec{
			WorkflowRunRef: corev1.LocalObjectReference{Name: run.Name},
			RunsOn:         []string{"linux"},
		},
	}
	runner := func(name, project string, labels []string) *actionsv1alpha1.Runner {
		return &actionsv1alpha1.Runner{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
			Spec: actionsv1alpha1.RunnerSpec{
				ProjectRef: corev1.LocalObjectReference{Name: project},
				Labels:     labels,
			},
		}
	}
	idleA := runner("idle-a", "project", []string{"linux"})
	idleB := runner("idle-b", "project", []string{"linux"})
	busy := runner("busy", "project", []string{"linux"})
	busy.Status.WorkflowJobRef = &corev1.LocalObjectReference{Name: "other-job"}
	wrongProject := runner("wrong-project", "other", []string{"linux"})
	wrongLabels := runner("wrong-labels", "project", []string{"windows"})
	clusterClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(run, idleA, idleB, busy, wrongProject, wrongLabels).Build()
	reconciler := &RunnerReconciler{Client: clusterClient, APIReader: clusterClient}
	requests := reconciler.runnersForWorkflowJob(context.Background(), workflowJob)
	if len(requests) != 1 {
		t.Fatalf("enqueued %d Runners, want 1", len(requests))
	}
	selected := requests[0].Name
	if selected != idleA.Name && selected != idleB.Name {
		t.Fatalf("enqueued Runner %q", selected)
	}
}

func TestPlannedWorkflowRunWakesIdleProjectRunners(t *testing.T) {
	scheme := runnerTestScheme(t)
	run := &actionsv1alpha1.WorkflowRun{
		ObjectMeta: metav1.ObjectMeta{Name: "ci", Namespace: "default"},
		Spec:       actionsv1alpha1.WorkflowRunSpec{ProjectRef: corev1.LocalObjectReference{Name: "project"}},
		Status: actionsv1alpha1.WorkflowRunStatus{Conditions: []metav1.Condition{{
			Type: actionsv1alpha1.WorkflowRunConditionPlanned, Status: metav1.ConditionTrue, Reason: "JobsPlanned", LastTransitionTime: metav1.Now(),
		}}},
	}
	idle := &actionsv1alpha1.Runner{ObjectMeta: metav1.ObjectMeta{Name: "idle", Namespace: "default"}, Spec: actionsv1alpha1.RunnerSpec{ProjectRef: corev1.LocalObjectReference{Name: "project"}}}
	busy := &actionsv1alpha1.Runner{ObjectMeta: metav1.ObjectMeta{Name: "busy", Namespace: "default"}, Spec: actionsv1alpha1.RunnerSpec{ProjectRef: corev1.LocalObjectReference{Name: "project"}}, Status: actionsv1alpha1.RunnerStatus{WorkflowJobRef: &corev1.LocalObjectReference{Name: "other"}}}
	other := &actionsv1alpha1.Runner{ObjectMeta: metav1.ObjectMeta{Name: "other", Namespace: "default"}, Spec: actionsv1alpha1.RunnerSpec{ProjectRef: corev1.LocalObjectReference{Name: "other"}}}
	clusterClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(run, idle, busy, other).Build()
	reconciler := &RunnerReconciler{Client: clusterClient, APIReader: clusterClient}
	requests := reconciler.runnersForWorkflowRun(context.Background(), run)
	if len(requests) != 1 || requests[0].Name != idle.Name {
		t.Fatalf("requests = %#v", requests)
	}
}

func TestWorkflowJobAssignmentUsesOptimisticLock(t *testing.T) {
	scheme := runnerTestScheme(t)
	workflowJob := &actionsv1alpha1.WorkflowJob{ObjectMeta: metav1.ObjectMeta{Name: "build", Namespace: "default"}}
	clusterClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&actionsv1alpha1.WorkflowJob{}).
		WithObjects(workflowJob).
		Build()
	reconciler := &RunnerReconciler{Client: clusterClient, APIReader: clusterClient}

	first := &actionsv1alpha1.WorkflowJob{}
	second := &actionsv1alpha1.WorkflowJob{}
	key := client.ObjectKeyFromObject(workflowJob)
	if err := clusterClient.Get(context.Background(), key, first); err != nil {
		t.Fatal(err)
	}
	if err := clusterClient.Get(context.Background(), key, second); err != nil {
		t.Fatal(err)
	}
	if err := reconciler.assignWorkflowJob(context.Background(), first, "runner-1"); err != nil {
		t.Fatal(err)
	}
	if err := reconciler.assignWorkflowJob(context.Background(), second, "runner-2"); !apierrors.IsConflict(err) {
		t.Fatalf("second assignment error = %v, want conflict", err)
	}
}

func TestRunnerLabelsMatchRunsOn(t *testing.T) {
	if !runnerLabelsMatch([]string{"self-hosted", "linux", "arm64"}, []string{"linux", "arm64"}) {
		t.Error("expected all requested labels to match")
	}
	if runnerLabelsMatch([]string{"self-hosted", "linux"}, []string{"linux", "x64"}) {
		t.Error("matched a job with a missing label")
	}
	if runnerLabelsMatch([]string{"Linux"}, []string{"linux"}) {
		t.Error("matched labels using non-canonical case folding")
	}
}

func runnerTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := actionsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := batchv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	return scheme
}

type podListErrorReader struct {
	client.Reader
	err error
}

func (r *podListErrorReader) List(ctx context.Context, list client.ObjectList, options ...client.ListOption) error {
	if _, ok := list.(*corev1.PodList); ok {
		return r.err
	}
	return r.Reader.List(ctx, list, options...)
}
