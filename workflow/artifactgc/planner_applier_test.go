package artifactgc

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	wfv1 "github.com/argoproj/argo-workflows/v4/pkg/apis/workflow/v1alpha1"
)

var fixedNow = time.Date(2026, 11, 11, 10, 0, 0, 0, time.UTC)

func testClock() func() time.Time {
	t := fixedNow
	return func() time.Time { return t }
}

func testIdentity() WorkflowIdentity {
	return WorkflowIdentity{
		Namespace:       "argo",
		Name:            "wf-1",
		UID:             "uid-1234",
		ResourceVersion: "7738582",
	}
}

func testArtifact() *wfv1.Artifact {
	return &wfv1.Artifact{
		Name: "main-art",
		S3: &wfv1.S3Artifact{
			Endpoint: "minio:9000",
			Bucket:   "bucket-1",
			Key:      "wf-1/main-art.tgz",
			Region:   "us-east-1",
		},
	}
}

// fakeVersionedDriver is an in-memory versioned store. Objects is mutated by deletes so
// tests can assert that versions not named in a Plan survive (overwrite safety).
type fakeVersionedDriver struct {
	objects      []ObjectVersion
	completeness Completeness
	snapshotErr  error
	// deleteResults maps VersionID to the outcome of the next delete attempt.
	deleteResults map[string]deleteOutcome
	deleteCalls   []ObjectVersion
	bareDeletes   int
}

type deleteOutcome struct {
	result ObjectResult
	err    error
}

func (f *fakeVersionedDriver) SnapshotVersions(_ context.Context, _ *wfv1.Artifact) (SnapshotOutcome, error) {
	if f.snapshotErr != nil {
		return SnapshotOutcome{}, f.snapshotErr
	}
	copied := make([]ObjectVersion, len(f.objects))
	copy(copied, f.objects)
	return SnapshotOutcome{Objects: copied, Completeness: f.completeness}, nil
}

func (f *fakeVersionedDriver) DeleteVersion(_ context.Context, _ *wfv1.Artifact, v ObjectVersion) (ObjectResult, error) {
	f.deleteCalls = append(f.deleteCalls, v)
	if v.VersionID == "" {
		// The fake mirrors the S3 contract: no bare/unversioned delete is ever issued.
		f.bareDeletes++
		return ResultUnsupported, ErrVersionedGCUnsupported
	}
	if out, ok := f.deleteResults[v.VersionID]; ok {
		if out.err == nil || errors.Is(out.err, ErrObjectAlreadyGone) {
			f.removeVersion(v.VersionID)
		}
		return out.result, out.err
	}
	f.removeVersion(v.VersionID)
	return ResultSucceeded, nil
}

func (f *fakeVersionedDriver) removeVersion(versionID string) {
	kept := f.objects[:0]
	for _, o := range f.objects {
		if o.VersionID != versionID {
			kept = append(kept, o)
		}
	}
	f.objects = kept
}

type fakeLocator struct {
	artifacts map[string]*wfv1.Artifact
}

func (l fakeLocator) FindArtifact(nodeID, artifactName string) (*wfv1.Artifact, bool) {
	a, ok := l.artifacts[nodeID+"/"+artifactName]
	return a, ok
}

func newPlanner() *Planner {
	return NewPlanner(PlanLimits{MaxObjects: 100, MaxBytes: 1024, TTLSeconds: 3600}, testClock())
}

func newSingleTarget(t *testing.T, drv VersionedGCDriver, prefix bool, completeness Completeness, objects ...ObjectVersion) *PlanTarget {
	t.Helper()
	planner := newPlanner()
	fake := drv.(*fakeVersionedDriver)
	fake.completeness = completeness
	fake.objects = objects
	target, err := planner.BuildTarget(t.Context(), drv, PlanInput{
		NodeID:       "node-1",
		ArtifactName: "main-art",
		Artifact:     testArtifact(),
		Prefix:       prefix,
	})
	require.NoError(t, err)
	return target
}

func TestPreviewPerformsNoDeletionAndFreezesVersions(t *testing.T) {
	drv := &fakeVersionedDriver{}
	target := newSingleTarget(t, drv, false, CompletenessComplete,
		ObjectVersion{Bucket: "bucket-1", Key: "wf-1/main-art.tgz", VersionID: "v1", ETag: "etag-1", Size: 10})

	planner := newPlanner()
	plan, err := planner.BuildPlan(testIdentity(), string(wfv1.ArtifactGCOnWorkflowCompletion), []PlanTarget{*target})
	require.NoError(t, err)

	assert.Equal(t, PhasePreview, plan.Phase)
	assert.Equal(t, CompletenessComplete, plan.Completeness)
	assert.Equal(t, 1, plan.Summary.ObjectCount)
	assert.Equal(t, int64(10), plan.Summary.TotalBytes)
	assert.Equal(t, "v1", plan.Targets[0].Objects[0].VersionID)
	assert.Equal(t, ResultPending, plan.Targets[0].Objects[0].Result)
	assert.NotEmpty(t, plan.PlanID)
	assert.Equal(t, 0, len(drv.deleteCalls), "preview must never delete")
	assert.False(t, plan.IsExpired(fixedNow))
	assert.True(t, plan.IsExpired(fixedNow.Add(2*time.Hour)))

	// A serialized round trip preserves identity and stays valid.
	raw, err := EncodePlan(plan)
	require.NoError(t, err)
	decoded, err := DecodePlan(raw)
	require.NoError(t, err)
	require.NoError(t, decoded.VerifyIdentity())
	assert.Equal(t, plan.PlanID, decoded.PlanID)
}

func TestOverwriteAfterPreviewDeletesOnlyFrozenVersion(t *testing.T) {
	ctx := t.Context()
	drv := &fakeVersionedDriver{}
	// Planning freezes v1 as the current object version.
	target := newSingleTarget(t, drv, false, CompletenessComplete,
		ObjectVersion{Bucket: "bucket-1", Key: "k", VersionID: "v1", ETag: "e1", Size: 10})
	planner := newPlanner()
	plan, err := planner.BuildPlan(testIdentity(), string(wfv1.ArtifactGCOnWorkflowCompletion), []PlanTarget{*target})
	require.NoError(t, err)

	// After preview, the key is overwritten: v2 becomes current while v1 remains.
	drv.objects = []ObjectVersion{
		{Bucket: "bucket-1", Key: "k", VersionID: "v1", ETag: "e1", Size: 10},
		{Bucket: "bucket-1", Key: "k", VersionID: "v2", ETag: "e2", Size: 12},
	}

	applier := NewApplier(
		func(_ context.Context, _ *wfv1.Artifact) (VersionedGCDriver, error) { return drv, nil },
		func(_ context.Context, id WorkflowIdentity) error {
			assert.Equal(t, "uid-1234", id.UID)
			return nil
		},
		testClock(),
	)
	loc := fakeLocator{artifacts: map[string]*wfv1.Artifact{"node-1/main-art": testArtifact()}}
	require.NoError(t, applier.Apply(ctx, plan, loc))

	assert.Equal(t, PhaseComplete, plan.Phase)
	require.Len(t, drv.deleteCalls, 1)
	assert.Equal(t, "v1", drv.deleteCalls[0].VersionID, "only the frozen version may be addressed")
	assert.Equal(t, 0, drv.bareDeletes)
	// The overwrite-created v2 must survive.
	var survivingVersions []string
	for _, o := range drv.objects {
		survivingVersions = append(survivingVersions, o.VersionID)
	}
	assert.Equal(t, []string{"v2"}, survivingVersions)
	assert.Equal(t, ResultSucceeded, plan.Targets[0].Objects[0].Result)
}

func TestAlreadyGoneVersionIsTerminalAndNotRetried(t *testing.T) {
	ctx := t.Context()
	drv := &fakeVersionedDriver{
		deleteResults: map[string]deleteOutcome{
			"v1": {result: ResultAlreadyGone, err: ErrObjectAlreadyGone},
		},
	}
	target := newSingleTarget(t, drv, false, CompletenessComplete,
		ObjectVersion{Bucket: "b", Key: "k", VersionID: "v1", Size: 1})
	plan, err := newPlanner().BuildPlan(testIdentity(), string(wfv1.ArtifactGCOnWorkflowDeletion), []PlanTarget{*target})
	require.NoError(t, err)

	applier := NewApplier(
		func(_ context.Context, _ *wfv1.Artifact) (VersionedGCDriver, error) { return drv, nil },
		nil, testClock())
	loc := fakeLocator{artifacts: map[string]*wfv1.Artifact{"node-1/main-art": testArtifact()}}
	require.NoError(t, applier.Apply(ctx, plan, loc))
	assert.Equal(t, ResultAlreadyGone, plan.Targets[0].Objects[0].Result)
	assert.Equal(t, PhaseComplete, plan.Phase)

	// Re-running Apply (e.g. after a restart) must not address the terminal object.
	require.NoError(t, applier.Apply(ctx, plan, loc))
	assert.Len(t, drv.deleteCalls, 1)
}

func TestDirectoryAdditionAfterPlanIsNotDeletedAndPlanIncomplete(t *testing.T) {
	ctx := t.Context()
	drv := &fakeVersionedDriver{}
	target := newSingleTarget(t, drv, true, CompletenessIncomplete,
		ObjectVersion{Bucket: "b", Key: "dir/a", VersionID: "va", Size: 1},
		ObjectVersion{Bucket: "b", Key: "dir/b", VersionID: "vb", Size: 1})
	plan, err := newPlanner().BuildPlan(testIdentity(), string(wfv1.ArtifactGCOnWorkflowCompletion), []PlanTarget{*target})
	require.NoError(t, err)
	assert.Equal(t, CompletenessIncomplete, plan.Completeness)
	assert.False(t, plan.PromisesFullCleanup(), "Incomplete listing must not promise full cleanup")

	// A new object appears under the prefix after the directory was frozen.
	drv.objects = append(drv.objects, ObjectVersion{Bucket: "b", Key: "dir/c", VersionID: "vc", Size: 1})

	applier := NewApplier(
		func(_ context.Context, _ *wfv1.Artifact) (VersionedGCDriver, error) { return drv, nil },
		nil, testClock())
	loc := fakeLocator{artifacts: map[string]*wfv1.Artifact{"node-1/main-art": testArtifact()}}
	require.NoError(t, applier.Apply(ctx, plan, loc))

	assert.Len(t, drv.deleteCalls, 2, "only frozen objects may be deleted")
	for _, call := range drv.deleteCalls {
		assert.NotEqual(t, "vc", call.VersionID, "newly added object must survive")
	}
	var surviving []string
	for _, o := range drv.objects {
		surviving = append(surviving, o.VersionID)
	}
	assert.Contains(t, surviving, "vc")
	// All frozen objects are gone, but the Incomplete plan still cannot promise
	// full cleanup or advance the workflow to full success.
	assert.True(t, plan.AllObjectsDeleted())
	assert.False(t, plan.PromisesFullCleanup())
	assert.NotEqual(t, PhaseComplete, plan.Phase)
}

func TestPartialFailureIsRecordedIndependentlyAndRecoversOnRetry(t *testing.T) {
	ctx := t.Context()
	drv := &fakeVersionedDriver{
		deleteResults: map[string]deleteOutcome{
			"v2": {result: ResultUnknown, err: errors.New("503 Service Unavailable")},
		},
	}
	target := newSingleTarget(t, drv, false, CompletenessComplete,
		ObjectVersion{Bucket: "b", Key: "k1", VersionID: "v1", Size: 1},
		ObjectVersion{Bucket: "b", Key: "k2", VersionID: "v2", Size: 1},
		ObjectVersion{Bucket: "b", Key: "k3", VersionID: "v3", Size: 1})
	plan, err := newPlanner().BuildPlan(testIdentity(), string(wfv1.ArtifactGCOnWorkflowCompletion), []PlanTarget{*target})
	require.NoError(t, err)

	applier := NewApplier(
		func(_ context.Context, _ *wfv1.Artifact) (VersionedGCDriver, error) { return drv, nil },
		nil, testClock())
	loc := fakeLocator{artifacts: map[string]*wfv1.Artifact{"node-1/main-art": testArtifact()}}
	require.NoError(t, applier.Apply(ctx, plan, loc))

	results := map[string]ObjectResult{}
	for _, o := range plan.Targets[0].Objects {
		results[o.VersionID] = o.Result
	}
	assert.Equal(t, ResultSucceeded, results["v1"])
	assert.Equal(t, ResultUnknown, results["v2"])
	assert.Equal(t, ResultSucceeded, results["v3"])
	assert.True(t, plan.HasRetryableObjects())
	assert.NotEqual(t, PhaseComplete, plan.Phase)

	// Storage recovers: a second pass retries only the unconfirmed same-version op.
	delete(drv.deleteResults, "v2")
	require.NoError(t, applier.Apply(ctx, plan, loc))
	assert.Len(t, drv.deleteCalls, 4, "three first-pass attempts plus one retried version")
	assert.Equal(t, "v2", drv.deleteCalls[3].VersionID)
	assert.Equal(t, PhaseComplete, plan.Phase)
	assert.False(t, plan.HasRetryableObjects())
}

func TestRestartRecoveryFromStoreSkipsTerminalObjects(t *testing.T) {
	ctx := t.Context()
	store := NewMemoryPlanStore()
	drv := &fakeVersionedDriver{
		deleteResults: map[string]deleteOutcome{
			"v2": {result: ResultUnknown, err: errors.New("connection reset")},
		},
	}
	target := newSingleTarget(t, drv, false, CompletenessComplete,
		ObjectVersion{Bucket: "b", Key: "k1", VersionID: "v1", Size: 1},
		ObjectVersion{Bucket: "b", Key: "k2", VersionID: "v2", Size: 1})
	plan, err := newPlanner().BuildPlan(testIdentity(), string(wfv1.ArtifactGCOnWorkflowCompletion), []PlanTarget{*target})
	require.NoError(t, err)

	applier := NewApplier(
		func(_ context.Context, _ *wfv1.Artifact) (VersionedGCDriver, error) { return drv, nil },
		nil, testClock())
	loc := fakeLocator{artifacts: map[string]*wfv1.Artifact{"node-1/main-art": testArtifact()}}
	require.NoError(t, applier.Apply(ctx, plan, loc))
	require.NoError(t, store.Save(ctx, "task-1", plan))

	// Simulate a controller/Pod restart: a brand new applier loads the persisted Plan.
	reloaded, err := store.Load(ctx, "task-1")
	require.NoError(t, err)
	require.NoError(t, reloaded.VerifyIdentity())
	assert.Equal(t, plan.PlanID, reloaded.PlanID)

	reloadedDriver := &fakeVersionedDriver{
		objects: []ObjectVersion{{Bucket: "b", Key: "k2", VersionID: "v2", Size: 1}},
	}
	reloadedApplier := NewApplier(
		func(_ context.Context, _ *wfv1.Artifact) (VersionedGCDriver, error) { return reloadedDriver, nil },
		nil, testClock())
	require.NoError(t, reloadedApplier.Apply(ctx, reloaded, loc))

	assert.Len(t, reloadedDriver.deleteCalls, 1, "only the Unknown object is retried after restart")
	assert.Equal(t, "v2", reloadedDriver.deleteCalls[0].VersionID)
	assert.Equal(t, PhaseComplete, reloaded.Phase)
}

func TestUnsupportedStorageIsExplicitAndNeverBareDeleted(t *testing.T) {
	ctx := t.Context()
	planner := newPlanner()

	// Driver without the versioned capability.
	target := UnsupportedTarget(PlanInput{NodeID: "node-1", ArtifactName: "main-art", Artifact: testArtifact()},
		ErrVersionedGCUnsupported.Error())
	plan, err := planner.BuildPlan(testIdentity(), string(wfv1.ArtifactGCOnWorkflowCompletion), []PlanTarget{*target})
	require.NoError(t, err)
	assert.True(t, plan.HasUnsupportedTarget())
	assert.Equal(t, CompletenessIncomplete, plan.Completeness)

	drv := &fakeVersionedDriver{}
	applier := NewApplier(
		func(_ context.Context, _ *wfv1.Artifact) (VersionedGCDriver, error) { return drv, nil },
		nil, testClock())
	require.NoError(t, applier.Apply(ctx, plan, fakeLocator{}))
	assert.Empty(t, drv.deleteCalls, "unsupported target must not be deleted")
	assert.Equal(t, 0, drv.bareDeletes)
	assert.Equal(t, PhaseFailed, plan.Phase)

	// A snapshot that returns an object without a version ID is refused as well.
	drv2 := &fakeVersionedDriver{}
	badTarget := newSingleTarget(t, drv2, false, CompletenessComplete,
		ObjectVersion{Bucket: "b", Key: "k", VersionID: "", Size: 1})
	assert.False(t, badTarget.Supported)
	assert.Empty(t, badTarget.Objects)
}

func TestVersionConflictLeavesCurrentObjectUntouched(t *testing.T) {
	ctx := t.Context()
	drv := &fakeVersionedDriver{
		deleteResults: map[string]deleteOutcome{
			"v-old": {result: ResultVersionConflict, err: ErrVersionConflict},
		},
	}
	target := newSingleTarget(t, drv, false, CompletenessComplete,
		ObjectVersion{Bucket: "b", Key: "k", VersionID: "v-old", Size: 1})
	plan, err := newPlanner().BuildPlan(testIdentity(), string(wfv1.ArtifactGCOnWorkflowCompletion), []PlanTarget{*target})
	require.NoError(t, err)

	// After planning the key was overwritten: storage now only holds the new version.
	drv.objects = []ObjectVersion{{Bucket: "b", Key: "k", VersionID: "v-new", Size: 2}}

	applier := NewApplier(
		func(_ context.Context, _ *wfv1.Artifact) (VersionedGCDriver, error) { return drv, nil },
		nil, testClock())
	loc := fakeLocator{artifacts: map[string]*wfv1.Artifact{"node-1/main-art": testArtifact()}}
	require.NoError(t, applier.Apply(ctx, plan, loc))

	assert.Equal(t, ResultVersionConflict, plan.Targets[0].Objects[0].Result)
	assert.Equal(t, PhaseFailed, plan.Phase)
	// The conflict is terminal: re-Apply never retries it.
	require.NoError(t, applier.Apply(ctx, plan, loc))
	assert.Len(t, drv.deleteCalls, 1)
	// The current object survives.
	assert.Len(t, drv.objects, 1)
	assert.Equal(t, "v-new", drv.objects[0].VersionID)
}

func TestPlanLimitsAreEnforced(t *testing.T) {
	planner := NewPlanner(PlanLimits{MaxObjects: 1, MaxBytes: 5, TTLSeconds: 60}, testClock())
	drv := &fakeVersionedDriver{}
	target := newSingleTarget(t, drv, false, CompletenessComplete,
		ObjectVersion{Bucket: "b", Key: "k1", VersionID: "v1", Size: 4},
		ObjectVersion{Bucket: "b", Key: "k2", VersionID: "v2", Size: 4})
	_, err := planner.BuildPlan(testIdentity(), "s", []PlanTarget{*target})
	require.ErrorIs(t, err, ErrPlanLimitExceeded)

	planner = NewPlanner(PlanLimits{MaxObjects: 10, MaxBytes: 5, TTLSeconds: 60}, testClock())
	_, err = planner.BuildPlan(testIdentity(), "s", []PlanTarget{*target})
	require.ErrorIs(t, err, ErrPlanLimitExceeded)
}

func TestPlanIdentityTamperingIsRejected(t *testing.T) {
	ctx := t.Context()
	drv := &fakeVersionedDriver{}
	target := newSingleTarget(t, drv, false, CompletenessComplete,
		ObjectVersion{Bucket: "b", Key: "k", VersionID: "v1", Size: 1})
	plan, err := newPlanner().BuildPlan(testIdentity(), "s", []PlanTarget{*target})
	require.NoError(t, err)

	plan.Targets[0].Objects[0].VersionID = "v-attacker"
	applier := NewApplier(
		func(_ context.Context, _ *wfv1.Artifact) (VersionedGCDriver, error) { return drv, nil },
		nil, testClock())
	err = applier.Apply(ctx, plan, fakeLocator{})
	require.ErrorIs(t, err, ErrPlanIdentityMismatch)
	assert.Empty(t, drv.deleteCalls)
}

func TestExpiredPlanIsNotApplied(t *testing.T) {
	ctx := t.Context()
	drv := &fakeVersionedDriver{}
	target := newSingleTarget(t, drv, false, CompletenessComplete,
		ObjectVersion{Bucket: "b", Key: "k", VersionID: "v1", Size: 1})
	plan, err := newPlanner().BuildPlan(testIdentity(), "s", []PlanTarget{*target})
	require.NoError(t, err)

	later := func() time.Time { return fixedNow.Add(2 * time.Hour) }
	applier := NewApplier(
		func(_ context.Context, _ *wfv1.Artifact) (VersionedGCDriver, error) { return drv, nil },
		nil, later)
	err = applier.Apply(ctx, plan, fakeLocator{})
	require.ErrorIs(t, err, ErrPlanNotApplicable)
	assert.Empty(t, drv.deleteCalls)
	assert.Equal(t, PhaseExpired, plan.Phase)
}

func TestWorkflowIdentityVerifierCanAbortApply(t *testing.T) {
	ctx := t.Context()
	drv := &fakeVersionedDriver{}
	target := newSingleTarget(t, drv, false, CompletenessComplete,
		ObjectVersion{Bucket: "b", Key: "k", VersionID: "v1", Size: 1})
	plan, err := newPlanner().BuildPlan(testIdentity(), "s", []PlanTarget{*target})
	require.NoError(t, err)

	applier := NewApplier(
		func(_ context.Context, _ *wfv1.Artifact) (VersionedGCDriver, error) { return drv, nil },
		func(_ context.Context, id WorkflowIdentity) error {
			return errors.New("workflow UID " + id.UID + " no longer matches")
		}, testClock())
	err = applier.Apply(ctx, plan, fakeLocator{})
	require.ErrorIs(t, err, ErrPlanNotApplicable)
	assert.Empty(t, drv.deleteCalls)
}

func TestApplyWithLostDriverCapabilityRecordsUnsupportedWithoutBreakingIdentity(t *testing.T) {
	ctx := t.Context()
	drv := &fakeVersionedDriver{}
	target := newSingleTarget(t, drv, false, CompletenessComplete,
		ObjectVersion{Bucket: "b", Key: "k", VersionID: "v1", Size: 1})
	plan, err := newPlanner().BuildPlan(testIdentity(), "s", []PlanTarget{*target})
	require.NoError(t, err)
	originalID := plan.PlanID

	// Between planning and Apply the storage driver loses the versioned capability.
	applier := NewApplier(
		func(_ context.Context, _ *wfv1.Artifact) (VersionedGCDriver, error) { return nil, nil },
		nil, testClock())
	loc := fakeLocator{artifacts: map[string]*wfv1.Artifact{"node-1/main-art": testArtifact()}}
	require.NoError(t, applier.Apply(ctx, plan, loc))

	assert.Equal(t, ResultUnsupported, plan.Targets[0].Objects[0].Result)
	assert.Equal(t, PhaseFailed, plan.Phase)
	assert.Empty(t, drv.deleteCalls, "no bare delete must be attempted when capability is lost")
	// Mutable results must not invalidate the frozen Plan identity.
	require.NoError(t, plan.VerifyIdentity())
	assert.Equal(t, originalID, plan.PlanID)
}

func TestSnapshotErrorFailsPlanningWithoutDeleting(t *testing.T) {
	drv := &fakeVersionedDriver{snapshotErr: errors.New("listing denied")}
	_, err := newPlanner().BuildTarget(t.Context(), drv,
		PlanInput{NodeID: "n", ArtifactName: "a", Artifact: testArtifact()})
	require.Error(t, err)
	assert.Empty(t, drv.deleteCalls)
}

func TestLimitsFromEnvDefaultsAndOverrides(t *testing.T) {
	t.Setenv(EnvVarPlanEnabled, "true")
	assert.True(t, PlanEnabled())
	t.Setenv(EnvVarPlanMaxObjects, "7")
	t.Setenv(EnvVarPlanMaxBytes, "1024")
	t.Setenv(EnvVarPlanTTL, "2m")
	limits := LimitsFromEnv()
	assert.Equal(t, 7, limits.MaxObjects)
	assert.Equal(t, int64(1024), limits.MaxBytes)
	assert.Equal(t, int64(120), limits.TTLSeconds)

	t.Setenv(EnvVarPlanMaxObjects, "garbage")
	t.Setenv(EnvVarPlanMaxBytes, "-1")
	t.Setenv(EnvVarPlanTTL, "nonsense")
	fallback := LimitsFromEnv()
	assert.Equal(t, DefaultMaxObjects, fallback.MaxObjects)
	assert.Equal(t, DefaultMaxBytes, fallback.MaxBytes)
	assert.Equal(t, int64(DefaultTTL/time.Second), fallback.TTLSeconds)
}
