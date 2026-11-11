package s3

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	wfv1 "github.com/argoproj/argo-workflows/v4/pkg/apis/workflow/v1alpha1"
	"github.com/argoproj/argo-workflows/v4/util/logging"
	"github.com/argoproj/argo-workflows/v4/workflow/artifactgc"
)

var fixedPlanTime = time.Date(2026, 11, 11, 10, 0, 0, 0, time.UTC)

func newVersionedMockClient(bucket string, enabled bool, versions []artifactgc.ObjectVersion) *mockClient {
	return &mockClient{
		files:             map[string][]string{},
		mockedErrs:        map[string]error{},
		versioningEnabled: map[string]bool{bucket: enabled},
		versions:          map[string][]artifactgc.ObjectVersion{bucket: versions},
	}
}

func s3Artifact(key string) *wfv1.Artifact {
	return &wfv1.Artifact{
		Name: "main-art",
		S3: &wfv1.S3Artifact{
			Endpoint: "minio:9000",
			Bucket:   "bucket-1",
			Key:      key,
		},
	}
}

func TestSnapshotSingleObjectIsCompleteAndReadonly(t *testing.T) {
	ctx := logging.TestContext(t.Context())
	cli := newVersionedMockClient("bucket-1", true, []artifactgc.ObjectVersion{
		{Bucket: "bucket-1", Key: "k", VersionID: "v1", ETag: "e1", Size: 42},
	})

	outcome, err := snapshotVersions(ctx, cli, s3Artifact("k"))
	require.NoError(t, err)
	assert.Equal(t, artifactgc.CompletenessComplete, outcome.Completeness)
	require.Len(t, outcome.Objects, 1)
	assert.Equal(t, "v1", outcome.Objects[0].VersionID)
	assert.Empty(t, cli.removedVersions, "snapshot must not delete")
}

func TestSnapshotUnversionedBucketIsExplicitlyUnsupported(t *testing.T) {
	ctx := logging.TestContext(t.Context())
	cli := newVersionedMockClient("bucket-1", false, nil)

	_, err := snapshotVersions(ctx, cli, s3Artifact("k"))
	require.ErrorIs(t, err, artifactgc.ErrVersionedGCUnsupported)
	assert.Empty(t, cli.removedVersions, "unsupported storage must never be bare-deleted")
}

func TestSnapshotMissingObjectIsEmptyComplete(t *testing.T) {
	ctx := logging.TestContext(t.Context())
	cli := newVersionedMockClient("bucket-1", true, nil)

	outcome, err := snapshotVersions(ctx, cli, s3Artifact("gone"))
	require.NoError(t, err)
	assert.Equal(t, artifactgc.CompletenessComplete, outcome.Completeness)
	assert.Empty(t, outcome.Objects)
}

func TestSnapshotPrefixIsFiniteButIncomplete(t *testing.T) {
	ctx := logging.TestContext(t.Context())
	cli := newVersionedMockClient("bucket-1", true, []artifactgc.ObjectVersion{
		{Bucket: "bucket-1", Key: "dir/a", VersionID: "va", Size: 1},
		{Bucket: "bucket-1", Key: "dir/b", VersionID: "vb", Size: 1},
		{Bucket: "bucket-1", Key: "other/c", VersionID: "vc", Size: 1},
	})

	outcome, err := snapshotVersions(ctx, cli, s3Artifact("dir/"))
	require.NoError(t, err)
	assert.Equal(t, artifactgc.CompletenessIncomplete, outcome.Completeness)
	require.Len(t, outcome.Objects, 2, "only the frozen prefix is enumerated")
}

func TestDeleteVersionAddressesOnlyTheFrozenVersion(t *testing.T) {
	ctx := logging.TestContext(t.Context())
	cli := newVersionedMockClient("bucket-1", true, []artifactgc.ObjectVersion{
		{Bucket: "bucket-1", Key: "k", VersionID: "v1", Size: 1},
		{Bucket: "bucket-1", Key: "k", VersionID: "v2", Size: 2},
	})

	result, err := deleteVersion(ctx, cli, artifactgc.ObjectVersion{Bucket: "bucket-1", Key: "k", VersionID: "v1"})
	require.NoError(t, err)
	assert.Equal(t, artifactgc.ResultSucceeded, result)
	require.Len(t, cli.removedVersions, 1)
	assert.Equal(t, "v1", cli.removedVersions[0].VersionID)

	// The overwrite-created new version survives.
	var surviving []string
	for _, v := range cli.versions["bucket-1"] {
		surviving = append(surviving, v.VersionID)
	}
	assert.Equal(t, []string{"v2"}, surviving)
}

func TestDeleteVersionEmptyIDIsUnsupportedWithoutBareDelete(t *testing.T) {
	ctx := logging.TestContext(t.Context())
	cli := newVersionedMockClient("bucket-1", true, nil)

	result, err := deleteVersion(ctx, cli, artifactgc.ObjectVersion{Bucket: "bucket-1", Key: "k", VersionID: ""})
	require.ErrorIs(t, err, artifactgc.ErrVersionedGCUnsupported)
	assert.Equal(t, artifactgc.ResultUnsupported, result)
	assert.Empty(t, cli.removedVersions)
}

func TestDeleteVersionErrorClassification(t *testing.T) {
	ctx := logging.TestContext(t.Context())

	tests := map[string]struct {
		err        error
		wantResult artifactgc.ObjectResult
		wantErr    error
	}{
		"already gone": {
			err:        minio.ErrorResponse{Code: "NoSuchVersion"},
			wantResult: artifactgc.ResultAlreadyGone,
			wantErr:    artifactgc.ErrObjectAlreadyGone,
		},
		"version conflict": {
			err:        minio.ErrorResponse{Code: "PreconditionFailed", StatusCode: 412},
			wantResult: artifactgc.ResultVersionConflict,
			wantErr:    artifactgc.ErrVersionConflict,
		},
		"transient unknown": {
			err:        minio.ErrorResponse{Code: "ServiceUnavailable", StatusCode: 503},
			wantResult: artifactgc.ResultUnknown,
			wantErr:    nil,
		},
		"unidentified stays unknown": {
			err:        errors.New("broken pipe"),
			wantResult: artifactgc.ResultUnknown,
			wantErr:    nil,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			cli := newVersionedMockClient("bucket-1", true, nil)
			cli.mockedErrs["RemoveObjectVersion"] = tc.err
			result, err := deleteVersion(ctx, cli, artifactgc.ObjectVersion{Bucket: "bucket-1", Key: "k", VersionID: "v1"})
			assert.Equal(t, tc.wantResult, result)
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
			} else {
				require.Error(t, err)
			}
			assert.Empty(t, cli.removedVersions, "failed deletes must not remove anything")
		})
	}
}

// mockBackedVersionedDriver adapts a mock S3 Client to the Plan engine's
// VersionedGCDriver capability using the package's real snapshot/delete logic.
type mockBackedVersionedDriver struct {
	cli Client
}

func (m *mockBackedVersionedDriver) SnapshotVersions(ctx context.Context, art *wfv1.Artifact) (artifactgc.SnapshotOutcome, error) {
	return snapshotVersions(ctx, m.cli, art)
}

func (m *mockBackedVersionedDriver) DeleteVersion(ctx context.Context, art *wfv1.Artifact, v artifactgc.ObjectVersion) (artifactgc.ObjectResult, error) {
	return deleteVersion(ctx, m.cli, v)
}

type testLocator struct {
	art *wfv1.Artifact
}

func (l testLocator) FindArtifact(nodeID, artifactName string) (*wfv1.Artifact, bool) {
	return l.art, true
}

// TestS3DriverEndToEndWithPlan drives the real S3 snapshot/delete logic through the
// Plan/Apply engine: a frozen v1 is conditionally deleted while a later v2 survives,
// and Apply never re-lists the prefix.
func TestS3DriverEndToEndWithPlan(t *testing.T) {
	ctx := logging.TestContext(t.Context())
	cli := newVersionedMockClient("bucket-1", true, []artifactgc.ObjectVersion{
		{Bucket: "bucket-1", Key: "k", VersionID: "v1", ETag: "e1", Size: 1},
	})
	drv := &mockBackedVersionedDriver{cli: cli}
	art := s3Artifact("k")

	// Planning freezes v1 and performs no delete.
	planner := artifactgc.NewPlanner(artifactgc.PlanLimits{MaxObjects: 10, MaxBytes: 1024, TTLSeconds: 3600},
		func() time.Time { return fixedPlanTime })
	target, err := planner.BuildTarget(ctx, drv, artifactgc.PlanInput{
		NodeID: "node-1", ArtifactName: "main-art", Artifact: art,
	})
	require.NoError(t, err)
	plan, err := planner.BuildPlan(artifactgc.WorkflowIdentity{Namespace: "argo", Name: "wf", UID: "uid", ResourceVersion: "1"},
		string(wfv1.ArtifactGCOnWorkflowCompletion), []artifactgc.PlanTarget{*target})
	require.NoError(t, err)
	require.Empty(t, cli.removedVersions, "preview must not delete")

	// The key is overwritten after planning: v2 becomes current, v1 remains.
	cli.versions["bucket-1"] = append(cli.versions["bucket-1"],
		artifactgc.ObjectVersion{Bucket: "bucket-1", Key: "k", VersionID: "v2", ETag: "e2", Size: 2})

	applier := artifactgc.NewApplier(
		func(_ context.Context, _ *wfv1.Artifact) (artifactgc.VersionedGCDriver, error) { return drv, nil },
		nil, func() time.Time { return fixedPlanTime })
	require.NoError(t, applier.Apply(ctx, plan, testLocator{art: art}))

	assert.Equal(t, artifactgc.PhaseComplete, plan.Phase)
	require.Len(t, cli.removedVersions, 1)
	assert.Equal(t, "v1", cli.removedVersions[0].VersionID)
	var surviving []string
	for _, v := range cli.versions["bucket-1"] {
		surviving = append(surviving, v.VersionID)
	}
	assert.Equal(t, []string{"v2"}, surviving, "overwrite-created version must never be misdeleted")
}
