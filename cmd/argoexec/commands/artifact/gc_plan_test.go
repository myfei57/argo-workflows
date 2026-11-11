package artifact

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1alpha1 "github.com/argoproj/argo-workflows/v4/pkg/apis/workflow/v1alpha1"
	fakewfclientset "github.com/argoproj/argo-workflows/v4/pkg/client/clientset/versioned/fake"
	"github.com/argoproj/argo-workflows/v4/workflow/artifactgc"
	"github.com/argoproj/argo-workflows/v4/workflow/common"
)

func sampleTask() *v1alpha1.WorkflowArtifactGCTask {
	return &v1alpha1.WorkflowArtifactGCTask{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "wf-artgc-0",
			Namespace: "argo",
			OwnerReferences: []metav1.OwnerReference{
				{Kind: "Workflow", Name: "wf-1", UID: "uid-1234"},
			},
		},
		Spec: v1alpha1.ArtifactGCSpec{
			ArtifactsByNode: map[string]v1alpha1.ArtifactNodeSpec{
				"node-1": {
					ArchiveLocation: &v1alpha1.ArtifactLocation{
						S3: &v1alpha1.S3Artifact{Endpoint: "minio:9000", Bucket: "archive-bucket"},
					},
					Artifacts: map[string]v1alpha1.Artifact{
						"main-art": {
							Name: "main-art",
							S3:   &v1alpha1.S3Artifact{Key: "wf-1/main.tgz"},
						},
						"dir-art": {
							Name: "dir-art",
							S3:   &v1alpha1.S3Artifact{Key: "wf-1/dir/"},
						},
					},
				},
			},
		},
	}
}

func TestCollectPlanInputsRelocatesAndDetectsPrefixes(t *testing.T) {
	inputs, err := collectPlanInputs(sampleTask())
	require.NoError(t, err)
	require.Len(t, inputs, 2)

	byName := map[string]planInput{}
	for _, in := range inputs {
		byName[in.artifactName] = in
	}
	main := byName["main-art"]
	assert.False(t, main.prefix)
	assert.Equal(t, "archive-bucket", main.artifact.S3.Bucket, "archive location must be relocated onto the artifact")
	assert.Equal(t, "minio:9000", main.artifact.S3.Endpoint)

	dir := byName["dir-art"]
	assert.True(t, dir.prefix, "trailing-slash keys are prefix (directory) artifacts")
}

func TestWorkflowIdentityFromTaskAndEnv(t *testing.T) {
	t.Setenv(common.EnvVarArtifactGCWorkflowUID, "uid-1234")
	t.Setenv(common.EnvVarArtifactGCWorkflowRV, "7738582")

	identity, err := workflowIdentityFromTask(sampleTask())
	require.NoError(t, err)
	assert.Equal(t, "argo", identity.Namespace)
	assert.Equal(t, "wf-1", identity.Name)
	assert.Equal(t, "uid-1234", identity.UID)
	assert.Equal(t, "7738582", identity.ResourceVersion)

	taskWithoutOwner := &v1alpha1.WorkflowArtifactGCTask{ObjectMeta: metav1.ObjectMeta{Name: "x", Namespace: "argo"}}
	_, err = workflowIdentityFromTask(taskWithoutOwner)
	require.Error(t, err)
}

func TestTaskPlanStoreRoundTripPersistsNoCredentials(t *testing.T) {
	ctx := t.Context()
	task := sampleTask()
	client := fakewfclientset.NewSimpleClientset(task)
	iface := client.ArgoprojV1alpha1().WorkflowArtifactGCTasks("argo")
	store := &taskPlanStore{iface: iface}

	plan := &artifactgc.ArtifactGCPlan{
		APIVersion: artifactgc.PlanAPIVersion,
		Kind:       artifactgc.PlanKind,
		Workflow: artifactgc.WorkflowIdentity{
			Namespace: "argo", Name: "wf-1", UID: "uid-1234", ResourceVersion: "7738582",
		},
		Strategy:     string(v1alpha1.ArtifactGCOnWorkflowCompletion),
		Phase:        artifactgc.PhasePreview,
		Completeness: artifactgc.CompletenessComplete,
		Limits:       artifactgc.PlanLimits{MaxObjects: 10, MaxBytes: 1024, TTLSeconds: 3600},
		Targets: []artifactgc.PlanTarget{{
			NodeID: "node-1", ArtifactName: "main-art", Supported: true,
			Completeness: artifactgc.CompletenessComplete,
			S3:           &artifactgc.S3Location{Endpoint: "minio:9000", Bucket: "archive-bucket", Key: "wf-1/main.tgz"},
			Objects: []artifactgc.PlannedObject{{
				ObjectVersion: artifactgc.ObjectVersion{Bucket: "archive-bucket", Key: "wf-1/main.tgz", VersionID: "v1", Size: 7},
				NodeID:        "node-1",
				ArtifactName:  "main-art",
				Result:        artifactgc.ResultPending,
			}},
		}},
	}
	require.NoError(t, store.Save(ctx, task.Name, plan))

	loaded, err := store.Load(ctx, task.Name)
	require.NoError(t, err)
	require.NotNil(t, loaded)
	assert.Equal(t, plan.Workflow.UID, loaded.Workflow.UID)
	assert.Equal(t, "v1", loaded.Targets[0].Objects[0].VersionID)
	require.NoError(t, loaded.VerifyIdentity(), "persisted Plan must keep a verifiable identity")

	stored, err := iface.Get(ctx, task.Name, metav1.GetOptions{})
	require.NoError(t, err)
	raw := stored.Annotations[common.AnnotationKeyArtifactGCPlan]
	assert.NotEmpty(t, raw)
	for _, secretWord := range []string{"secret", "accessKey", "secretKey", "password", "token"} {
		assert.False(t, strings.Contains(strings.ToLower(raw), secretWord),
			"Plan annotation must not carry credential material: found %q", secretWord)
	}

	// A task without a Plan loads as nil without error.
	other := &v1alpha1.WorkflowArtifactGCTask{ObjectMeta: metav1.ObjectMeta{Name: "other", Namespace: "argo"}}
	_, err = iface.Create(ctx, other, metav1.CreateOptions{})
	require.NoError(t, err)
	missing, err := store.Load(ctx, "other")
	require.NoError(t, err)
	assert.Nil(t, missing)
}
