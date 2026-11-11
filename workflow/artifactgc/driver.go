package artifactgc

import (
	"context"
	"errors"

	wfv1 "github.com/argoproj/argo-workflows/v4/pkg/apis/workflow/v1alpha1"
)

// Sentinel errors returned by VersionedGCDriver implementations. Callers classify
// outcomes with errors.Is rather than string matching.
var (
	// ErrVersionedGCUnsupported means the storage cannot enumerate immutable object
	// versions or cannot delete one specific version conditionally. Callers must not
	// fall back to an unversioned (bare) delete, which would mask overwrite races.
	ErrVersionedGCUnsupported = errors.New("versioned conditional artifact deletion is not supported by this storage")
	// ErrObjectAlreadyGone means the exact frozen object version does not exist any
	// more, so the desired end state is already satisfied.
	ErrObjectAlreadyGone = errors.New("object version no longer exists")
	// ErrVersionConflict means storage refused the conditional delete because the
	// current state does not match the frozen version. The current object must be
	// left untouched.
	ErrVersionConflict = errors.New("refusing to delete: object state conflicts with the frozen Plan version")
)

// SnapshotOutcome is the finite, frozen object/version set read from real storage
// during the preview phase.
type SnapshotOutcome struct {
	// Objects is the exhaustive set of object versions visible at snapshot time. It
	// must be finite.
	Objects []ObjectVersion
	// Completeness says whether Objects can be proven exhaustive. A weakly consistent
	// prefix listing that cannot prove it must return CompletenessIncomplete.
	Completeness Completeness
}

// VersionedGCDriver is the optional capability of an artifact driver whose storage
// supports version-aware, conditional deletion.
//
// Implementations contract:
//
//   - SnapshotVersions is read-only and performs no deletion.
//   - DeleteVersion addresses exactly one immutable version and must never issue an
//     unversioned delete, including as a fallback on errors.
//   - DeleteVersion returns one of the package sentinel errors alongside a matching
//     ObjectResult, or a ResultUnknown result when the outcome cannot be confirmed.
type VersionedGCDriver interface {
	// SnapshotVersions reads the concrete object versions under one artifact
	// location (a single object or a key prefix) using real storage capabilities.
	SnapshotVersions(ctx context.Context, artifact *wfv1.Artifact) (SnapshotOutcome, error)
	// DeleteVersion conditionally deletes exactly the given version. It is idempotent
	// for the same (Bucket, Key, VersionID) tuple.
	DeleteVersion(ctx context.Context, artifact *wfv1.Artifact, version ObjectVersion) (ObjectResult, error)
}

// AsVersionedGCDriver returns drv as a VersionedGCDriver when it implements the
// capability, and false otherwise. The unsupported case must be recorded explicitly
// instead of falling back to bare deletion.
func AsVersionedGCDriver(drv any) (VersionedGCDriver, bool) {
	versioned, ok := drv.(VersionedGCDriver)
	return versioned, ok
}
