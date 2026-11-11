package controller

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	wfv1 "github.com/argoproj/argo-workflows/v4/pkg/apis/workflow/v1alpha1"
	"github.com/argoproj/argo-workflows/v4/workflow/artifactgc"
	"github.com/argoproj/argo-workflows/v4/workflow/common"
)

func s3SearchResult(name string) wfv1.ArtifactSearchResult {
	return wfv1.ArtifactSearchResult{
		NodeID: "node-1",
		Artifact: wfv1.Artifact{
			Name: name,
			S3:   &wfv1.S3Artifact{Bucket: "b", Key: name},
		},
	}
}

func TestPlanModeEligibleOnlyForAllS3(t *testing.T) {
	assert.False(t, planModeEligible(nil), "empty results are not eligible")

	s3Results := wfv1.ArtifactSearchResults{s3SearchResult("a"), s3SearchResult("b")}
	assert.True(t, planModeEligible(s3Results))

	mixed := append(s3Results, wfv1.ArtifactSearchResult{
		NodeID:   "node-2",
		Artifact: wfv1.Artifact{Name: "raw", Raw: &wfv1.RawArtifact{Data: "x"}},
	})
	assert.False(t, planModeEligible(mixed), "a mixed group keeps the legacy GC path")
}

func TestPlanEnabledDefaultsOff(t *testing.T) {
	t.Setenv(artifactgc.EnvVarPlanEnabled, "")
	assert.False(t, artifactgc.PlanEnabled(), "the legacy GC path must remain the default")
	t.Setenv(artifactgc.EnvVarPlanEnabled, "true")
	assert.True(t, artifactgc.PlanEnabled())
}

func TestPlanGCEnvCarriesWorkflowIdentity(t *testing.T) {
	wf := &wfv1.Workflow{
		ObjectMeta: metav1.ObjectMeta{Name: "wf", ResourceVersion: "42", UID: "uid-1"},
	}
	env := planGCEnv(wf, common.ArtifactGCModeApply)
	values := map[string]string{}
	for _, e := range env {
		values[e.Name] = e.Value
	}
	assert.Equal(t, common.ArtifactGCModeApply, values[common.EnvVarArtifactGCMode])
	assert.Equal(t, "uid-1", values[common.EnvVarArtifactGCWorkflowUID])
	assert.Equal(t, "42", values[common.EnvVarArtifactGCWorkflowRV])
}

func podWithPhase(name string, phase corev1.PodPhase) *corev1.Pod {
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name}, Status: corev1.PodStatus{Phase: phase}}
}

func TestDecidePlanPhaseStateMachine(t *testing.T) {
	plans := []*artifactgc.ArtifactGCPlan{{PlanID: "p1"}}

	tests := map[string]struct {
		planPod  *corev1.Pod
		applyPod *corev1.Pod
		plans    []*artifactgc.ArtifactGCPlan
		want     planPhase
	}{
		"no plan pod yet": {
			want: planPhaseNeedPlanPod,
		},
		"preview pod running": {
			planPod: podWithPhase("p", corev1.PodRunning),
			want:    planPhasePlanPodRunning,
		},
		"preview pod failed is retried": {
			planPod: podWithPhase("p", corev1.PodFailed),
			want:    planPhasePlanPodFailed,
		},
		"preview succeeded without plan is a failure": {
			planPod: podWithPhase("p", corev1.PodSucceeded),
			want:    planPhasePlanPodFailed,
		},
		"plan frozen, apply pod not yet created": {
			planPod: podWithPhase("p", corev1.PodSucceeded),
			plans:   plans,
			want:    planPhaseNeedApplyPod,
		},
		"apply pod running never advances success early": {
			planPod:  podWithPhase("p", corev1.PodSucceeded),
			applyPod: podWithPhase("p-apply", corev1.PodRunning),
			plans:    plans,
			want:     planPhaseApplyPodRunning,
		},
		"apply pod failed is retried against the same frozen plan": {
			planPod:  podWithPhase("p", corev1.PodSucceeded),
			applyPod: podWithPhase("p-apply", corev1.PodFailed),
			plans:    plans,
			want:     planPhaseApplyPodFailed,
		},
		"apply succeeded only after both pods succeed": {
			planPod:  podWithPhase("p", corev1.PodSucceeded),
			applyPod: podWithPhase("p-apply", corev1.PodSucceeded),
			plans:    plans,
			want:     planPhaseSucceeded,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.want, decidePlanPhase(tc.planPod, tc.applyPod, tc.plans))
		})
	}
}

func encodedPlan(t *testing.T, uid string, completeness artifactgc.Completeness,
	result artifactgc.ObjectResult) *artifactgc.ArtifactGCPlan {
	t.Helper()
	planner := artifactgc.NewPlanner(artifactgc.PlanLimits{MaxObjects: 10, MaxBytes: 1024, TTLSeconds: 3600},
		func() time.Time { return time.Date(2026, 11, 11, 10, 0, 0, 0, time.UTC) })
	target := artifactgc.PlanTarget{
		NodeID:       "node-1",
		ArtifactName: "main-art",
		Supported:    true,
		Completeness: completeness,
		Objects: []artifactgc.PlannedObject{
			{
				ObjectVersion: artifactgc.ObjectVersion{Bucket: "b", Key: "k", VersionID: "v1", Size: 1},
				NodeID:        "node-1",
				ArtifactName:  "main-art",
				Result:        result,
			},
		},
	}
	plan, err := planner.BuildPlan(artifactgc.WorkflowIdentity{Namespace: "argo", Name: "wf", UID: uid, ResourceVersion: "1"},
		string(wfv1.ArtifactGCOnWorkflowCompletion), []artifactgc.PlanTarget{target})
	require.NoError(t, err)
	return plan
}

func taskWithPlan(t *testing.T, name, uid string, plan *artifactgc.ArtifactGCPlan) *wfv1.WorkflowArtifactGCTask {
	t.Helper()
	raw, err := artifactgc.EncodePlan(plan)
	require.NoError(t, err)
	return &wfv1.WorkflowArtifactGCTask{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   "argo",
			Annotations: map[string]string{common.AnnotationKeyArtifactGCPlan: string(raw)},
		},
	}
}

func TestLoadPlanFromTasksVerifiesIdentityAndHandlesMissingAnnotation(t *testing.T) {
	ctx := t.Context()

	// No annotation yet: empty slice, no error.
	empty, err := loadPlanFromTasks(ctx, []*wfv1.WorkflowArtifactGCTask{{ObjectMeta: metav1.ObjectMeta{Name: "t"}}})
	require.NoError(t, err)
	assert.Empty(t, empty)

	planA := encodedPlan(t, "uid-1", artifactgc.CompletenessComplete, artifactgc.ResultSucceeded)
	planB := encodedPlan(t, "uid-1", artifactgc.CompletenessComplete, artifactgc.ResultAlreadyGone)
	tasks := []*wfv1.WorkflowArtifactGCTask{
		taskWithPlan(t, "t1", "uid-1", planA),
		taskWithPlan(t, "t2", "uid-1", planB),
	}
	plans, err := loadPlanFromTasks(ctx, tasks)
	require.NoError(t, err)
	require.Len(t, plans, 2)

	// Conflicting Workflow identities are rejected rather than applied.
	planOther := encodedPlan(t, "uid-other", artifactgc.CompletenessComplete, artifactgc.ResultSucceeded)
	bad := append(tasks, taskWithPlan(t, "t3", "uid-other", planOther))
	_, err = loadPlanFromTasks(ctx, bad)
	require.Error(t, err)
}

func TestPlansContainRefusalAndFullCleanupGates(t *testing.T) {
	complete := encodedPlan(t, "uid-1", artifactgc.CompletenessComplete, artifactgc.ResultSucceeded)
	assert.True(t, complete.PromisesFullCleanup())
	assert.False(t, plansContainRefusal([]*artifactgc.ArtifactGCPlan{complete}))

	// An incomplete listing with all deletes done still cannot promise full cleanup,
	// so the strategy/finalizer must not advance to success.
	incomplete := encodedPlan(t, "uid-1", artifactgc.CompletenessIncomplete, artifactgc.ResultSucceeded)
	assert.False(t, incomplete.PromisesFullCleanup())

	unsupported := encodedPlan(t, "uid-1", artifactgc.CompletenessIncomplete, artifactgc.ResultUnsupported)
	unsupported.Targets[0].Supported = false
	assert.True(t, plansContainRefusal([]*artifactgc.ArtifactGCPlan{unsupported}))
}
