package s3

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/minio/minio-go/v7"

	wfv1 "github.com/argoproj/argo-workflows/v4/pkg/apis/workflow/v1alpha1"
	"github.com/argoproj/argo-workflows/v4/util/logging"
	"github.com/argoproj/argo-workflows/v4/workflow/artifactgc"
)

// bucketVersioningEnabled is the S3 status string returned for a version-enabled
// bucket. Compare against the literal so no dependency on minio constant names is
// required.
const bucketVersioningEnabled = "Enabled"

// conflictErrorCodes are definite S3 refusals that mean the storage state no longer
// matches the frozen Plan version. They are terminal VersionConflict outcomes rather
// than retries.
var conflictErrorCodes = map[string]bool{
	"PreconditionFailed": true,
	"InvalidArgument":    true,
	"MethodNotAllowed":   true,
	"Conflict":           true,
}

var _ artifactgc.VersionedGCDriver = &ArtifactDriver{}

// GetBucketVersioningEnabled reports whether the bucket has object versioning enabled.
func (s *s3client) GetBucketVersioningEnabled(bucket string) (bool, error) {
	logging.RequireLoggerFromContext(s.ctx).WithField("bucket", bucket).Info(s.ctx, "Checking S3 bucket versioning")
	cfg, err := s.minioClient.GetBucketVersioning(s.ctx, bucket)
	if err != nil {
		return false, err
	}
	return cfg.Status == bucketVersioningEnabled, nil
}

// StatObjectVersion reads one concrete object version.
func (s *s3client) StatObjectVersion(bucket, key, versionID string) (artifactgc.ObjectVersion, error) {
	encOpts, err := s.EncryptOpts.buildServerSideEnc(bucket, key)
	if err != nil {
		return artifactgc.ObjectVersion{}, err
	}
	info, err := s.minioClient.StatObject(s.ctx, bucket, key, minio.StatObjectOptions{
		VersionID:            versionID,
		ServerSideEncryption: encOpts,
	})
	if err != nil {
		return artifactgc.ObjectVersion{}, err
	}
	return toObjectVersion(bucket, info), nil
}

// ListObjectVersions enumerates every object version (including non-current versions
// and delete markers) under a key prefix through the S3 ListObjectVersions capability.
// It drains the whole paginated listing so the returned set is finite, but it cannot
// prove no object was added concurrently, especially on S3-compatible stores with
// weaker listing guarantees: callers mark a prefix snapshot Incomplete.
func (s *s3client) ListObjectVersions(bucket, keyPrefix string) ([]artifactgc.ObjectVersion, error) {
	keyPrefix = keyPrefixForListing(keyPrefix)
	log := logging.RequireLoggerFromContext(s.ctx).WithFields(logging.Fields{"bucket": bucket, "prefix": keyPrefix})
	log.Info(s.ctx, "Listing S3 object versions for Artifact GC Plan")

	out := make([]artifactgc.ObjectVersion, 0)
	listOpts := minio.ListObjectsOptions{
		Prefix:       keyPrefix,
		Recursive:    true,
		WithVersions: true,
	}
	for obj := range s.minioClient.ListObjects(s.ctx, bucket, listOpts) {
		if obj.Err != nil {
			return nil, obj.Err
		}
		if strings.HasSuffix(obj.Key, "/") {
			// Skip console-created directory marker objects.
			continue
		}
		out = append(out, toObjectVersion(bucket, obj))
	}
	return out, nil
}

// RemoveObjectVersion conditionally deletes one immutable version. A bare
// (unversioned) delete is refused at the caller boundary, so this method never
// downgrades a conditional request.
func (s *s3client) RemoveObjectVersion(bucket, key, versionID string) error {
	if versionID == "" {
		// Defense in depth: the driver-level guard should prevent this call. Never
		// issue an unversioned delete, which could remove an overwritten new object.
		return fmt.Errorf("%w: refusing unversioned delete of %s/%s", artifactgc.ErrVersionedGCUnsupported, bucket, key)
	}
	logging.RequireLoggerFromContext(s.ctx).
		WithFields(logging.Fields{"bucket": bucket, "key": key, "versionID": versionID}).
		Info(s.ctx, "Conditionally deleting one S3 object version")
	return s.minioClient.RemoveObject(s.ctx, bucket, key, minio.RemoveObjectOptions{VersionID: versionID})
}

func toObjectVersion(bucket string, info minio.ObjectInfo) artifactgc.ObjectVersion {
	return artifactgc.ObjectVersion{
		Bucket:         bucket,
		Key:            info.Key,
		VersionID:      info.VersionID,
		ETag:           info.ETag,
		Size:           info.Size,
		IsDeleteMarker: info.IsDeleteMarker,
	}
}

// SnapshotVersions implements artifactgc.VersionedGCDriver. It is read-only and
// freezes the finite object/version set for one artifact.
func (s3Driver *ArtifactDriver) SnapshotVersions(ctx context.Context, artifact *wfv1.Artifact) (artifactgc.SnapshotOutcome, error) {
	s3cli, err := s3Driver.newClient(ctx)
	if err != nil {
		return artifactgc.SnapshotOutcome{}, fmt.Errorf("failed to create S3 client for version snapshot: %w", err)
	}
	return snapshotVersions(ctx, s3cli, artifact)
}

// snapshotVersions contains the testable snapshot logic against an injected Client.
func snapshotVersions(ctx context.Context, s3cli Client, artifact *wfv1.Artifact) (artifactgc.SnapshotOutcome, error) {
	enabled, err := s3cli.GetBucketVersioningEnabled(artifact.S3.Bucket)
	if err != nil {
		return artifactgc.SnapshotOutcome{}, fmt.Errorf("failed to read bucket versioning for %s: %w", artifact.S3.Bucket, err)
	}
	if !enabled {
		return artifactgc.SnapshotOutcome{}, fmt.Errorf("%w: bucket %s is not version-enabled", artifactgc.ErrVersionedGCUnsupported, artifact.S3.Bucket)
	}

	if !strings.HasSuffix(artifact.S3.Key, "/") {
		// A single object is a provably complete, one-element snapshot.
		version, statErr := s3cli.StatObjectVersion(artifact.S3.Bucket, artifact.S3.Key, "")
		if statErr != nil {
			if IsS3ErrCode(statErr, "NoSuchKey") {
				return artifactgc.SnapshotOutcome{Objects: nil, Completeness: artifactgc.CompletenessComplete}, nil
			}
			return artifactgc.SnapshotOutcome{}, fmt.Errorf("failed to stat object %s: %w", artifact.S3.Key, statErr)
		}
		return artifactgc.SnapshotOutcome{Objects: []artifactgc.ObjectVersion{version}, Completeness: artifactgc.CompletenessComplete}, nil
	}

	// A prefix listing is finite but cannot prove no object is added concurrently,
	// especially on S3-compatible stores with weaker listing guarantees.
	objects, listErr := s3cli.ListObjectVersions(artifact.S3.Bucket, artifact.S3.Key)
	if listErr != nil {
		return artifactgc.SnapshotOutcome{}, fmt.Errorf("failed to list versions under prefix %s: %w", artifact.S3.Key, listErr)
	}
	return artifactgc.SnapshotOutcome{Objects: objects, Completeness: artifactgc.CompletenessIncomplete}, nil
}

// DeleteVersion implements artifactgc.VersionedGCDriver. It conditionally deletes
// exactly the frozen version and never falls back to a bare delete.
func (s3Driver *ArtifactDriver) DeleteVersion(ctx context.Context, artifact *wfv1.Artifact, version artifactgc.ObjectVersion) (artifactgc.ObjectResult, error) {
	if version.VersionID == "" {
		return artifactgc.ResultUnsupported, fmt.Errorf("%w: object %s/%s has no version ID", artifactgc.ErrVersionedGCUnsupported, version.Bucket, version.Key)
	}
	s3cli, err := s3Driver.newClient(ctx)
	if err != nil {
		// A client construction failure is unconfirmed: nothing was sent to storage.
		return artifactgc.ResultUnknown, fmt.Errorf("failed to create S3 client for conditional delete: %w", err)
	}
	return deleteVersion(ctx, s3cli, version)
}

// deleteVersion contains the testable conditional-delete logic against an injected
// Client.
func deleteVersion(ctx context.Context, s3cli Client, version artifactgc.ObjectVersion) (artifactgc.ObjectResult, error) {
	if version.VersionID == "" {
		// Never issue an unversioned (bare) delete, which could remove an overwritten
		// new object. Record an explicit Unsupported outcome.
		return artifactgc.ResultUnsupported, fmt.Errorf("%w: object %s/%s has no version ID", artifactgc.ErrVersionedGCUnsupported, version.Bucket, version.Key)
	}
	if delErr := s3cli.RemoveObjectVersion(version.Bucket, version.Key, version.VersionID); delErr != nil {
		return classifyDeleteVersionError(ctx, delErr)
	}
	return artifactgc.ResultSucceeded, nil
}

// classifyDeleteVersionError maps a storage error onto the closed per-object result
// set. Anything not positively identified stays Unknown so an unconfirmed operation is
// never reported as success.
func classifyDeleteVersionError(ctx context.Context, err error) (artifactgc.ObjectResult, error) {
	switch {
	case err == nil:
		return artifactgc.ResultSucceeded, nil
	case IsS3ErrCode(err, "NoSuchKey"), IsS3ErrCode(err, "NoSuchVersion"):
		return artifactgc.ResultAlreadyGone, fmt.Errorf("%w: %v", artifactgc.ErrObjectAlreadyGone, err)
	case errors.Is(err, artifactgc.ErrVersionedGCUnsupported):
		return artifactgc.ResultUnsupported, err
	case isConflictS3Err(err):
		return artifactgc.ResultVersionConflict, fmt.Errorf("%w: %v", artifactgc.ErrVersionConflict, err)
	default:
		// Includes transient 5xx/network errors and any unidentified response: the
		// outcome is unconfirmed and a same-version retry remains safe.
		_ = isTransientS3Err(ctx, err) // retain existing transient-error telemetry
		return artifactgc.ResultUnknown, err
	}
}

func isConflictS3Err(err error) bool {
	for code := range conflictErrorCodes {
		if IsS3ErrCode(err, code) {
			return true
		}
	}
	return false
}
