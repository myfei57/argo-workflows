package artifactgc

import (
	"context"
	"errors"
	"fmt"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	wfv1 "github.com/argoproj/argo-workflows/v4/pkg/apis/workflow/v1alpha1"
)

// ArtifactLocator resolves the runtime artifact specification for a frozen target.
// Implementations source it from the WorkflowArtifactGCTask Spec, which carries secret
// references but never secret values.
type ArtifactLocator interface {
	FindArtifact(nodeID, artifactName string) (*wfv1.Artifact, bool)
}

// DriverFactory constructs a VersionedGCDriver for an artifact at Apply time, so
// credentials remain runtime-only and never enter the Plan.
type DriverFactory func(ctx context.Context, artifact *wfv1.Artifact) (VersionedGCDriver, error)

// IdentityVerifier re-checks the Workflow GC preconditions before Apply: the Workflow
// UID must still match the frozen Plan identity and the Strategy must still be due.
type IdentityVerifier func(ctx context.Context, identity WorkflowIdentity) error

// Applier executes a frozen Plan. It never enumerates storage. Each deletion addresses
// exactly one frozen (Bucket, Key, VersionID) tuple through the driver.
type Applier struct {
	// NewDriver builds the versioned driver for one artifact at runtime.
	NewDriver DriverFactory
	// Verify is invoked once before any deletion and may abort the whole Apply run.
	Verify IdentityVerifier
	// Now is the clock, overridable in tests.
	Now func() time.Time
}

// NewApplier constructs an Applier with the given runtime collaborators.
func NewApplier(factory DriverFactory, verifier IdentityVerifier, now func() time.Time) *Applier {
	if now == nil {
		now = time.Now
	}
	return &Applier{NewDriver: factory, Verify: verifier, Now: now}
}

// ErrPlanNotApplicable covers Plan-level refusals (identity, expiry, state) that mean
// no object may be touched.
var ErrPlanNotApplicable = errors.New("artifact GC Plan is not applicable")

// Apply runs one bounded Apply pass over the frozen Plan and records every per-object
// outcome independently on the Plan itself. It is safe to re-run after a crash:
// terminal results are skipped and retryable results (Pending, Unknown) are retried
// with the exact same immutable version, which is provably idempotent.
//
// The returned error is reserved for Plan-level refusal (identity mismatch, expiry,
// missing runtime artifact). Per-object storage failures are recorded as results.
func (a *Applier) Apply(ctx context.Context, plan *ArtifactGCPlan, locator ArtifactLocator) error {
	if plan == nil {
		return fmt.Errorf("%w: nil Plan", ErrPlanNotApplicable)
	}
	now := a.Now()

	// 1. Re-verify Plan identity: a substituted or tampered Plan must not run.
	if err := plan.VerifyIdentity(); err != nil {
		return fmt.Errorf("%w: %w", ErrPlanNotApplicable, err)
	}
	// 2. Re-verify Workflow identity and GC preconditions.
	if a.Verify != nil {
		if err := a.Verify(ctx, plan.Workflow); err != nil {
			return fmt.Errorf("%w: %w", ErrPlanNotApplicable, err)
		}
	}
	// 3. An expired Plan must be rebuilt, never applied.
	if plan.IsExpired(now) {
		plan.Phase = PhaseExpired
		return fmt.Errorf("%w: Plan expired at %s", ErrPlanNotApplicable, plan.ExpiresAt.Time)
	}
	// A failed Plan carries terminal refusals; only retryable objects may still move,
	// and a Preview Plan starts applying on first touch.
	if plan.Phase == PhaseComplete {
		return nil
	}
	if plan.Phase != PhasePreview && plan.Phase != PhaseApplying && plan.Phase != PhaseFailed {
		return fmt.Errorf("%w: Plan phase %s forbids Apply", ErrPlanNotApplicable, plan.Phase)
	}

	for i := range plan.Targets {
		target := &plan.Targets[i]
		if !target.Supported {
			// Explicit unsupported outcome: never substitute a bare delete.
			continue
		}
		if err := a.applyTarget(ctx, plan, target, locator, now); err != nil {
			// A missing runtime artifact or driver capability is a Plan-level problem.
			return err
		}
	}

	return plan.recomputeDerived(now)
}

// applyTarget handles the frozen objects of one target. It performs no listing.
func (a *Applier) applyTarget(ctx context.Context, plan *ArtifactGCPlan, target *PlanTarget, locator ArtifactLocator, now time.Time) error {
	artifact, found := locator.FindArtifact(target.NodeID, target.ArtifactName)
	if !found {
		return fmt.Errorf("%w: runtime artifact %q for node %q is no longer present", ErrPlanNotApplicable, target.ArtifactName, target.NodeID)
	}

	drv, err := a.NewDriver(ctx, artifact)
	if err != nil {
		return fmt.Errorf("failed to construct versioned GC driver for node %q artifact %q: %w", target.NodeID, target.ArtifactName, err)
	}
	if drv == nil {
		// Driver lost the capability between planning and Apply. Record the explicit
		// Unsupported outcome per object without mutating immutable Plan identity
		// fields, and never fall back to a bare delete.
		for j := range target.Objects {
			a.record(&target.Objects[j], ResultUnsupported, ErrVersionedGCUnsupported, now)
		}
		return nil
	}

	for j := range target.Objects {
		obj := &target.Objects[j]
		if obj.Result.IsTerminal() {
			// Crash recovery and idempotency: never touch a terminal object again.
			continue
		}
		if !obj.Result.IsRetryable() {
			continue
		}
		result, delErr := drv.DeleteVersion(ctx, artifact, obj.ObjectVersion)
		a.record(obj, normalizeResult(result, delErr), delErr, now)
	}
	return nil
}

// normalizeResult maps a driver outcome to the closed result set. A driver that
// returns no explicit result is classified from its sentinel error; anything else is
// Unknown so an unconfirmed operation is never reported as success.
func normalizeResult(result ObjectResult, err error) ObjectResult {
	switch result {
	case ResultSucceeded, ResultAlreadyGone, ResultVersionConflict, ResultUnsupported, ResultUnknown:
		return result
	case ResultPending:
		// Drivers must not report Pending; treat as unconfirmed.
		return ResultUnknown
	}
	switch {
	case err == nil:
		return ResultSucceeded
	case errors.Is(err, ErrObjectAlreadyGone):
		return ResultAlreadyGone
	case errors.Is(err, ErrVersionConflict):
		return ResultVersionConflict
	case errors.Is(err, ErrVersionedGCUnsupported):
		return ResultUnsupported
	default:
		return ResultUnknown
	}
}

// record stores an independently auditable per-object outcome.
func (a *Applier) record(obj *PlannedObject, result ObjectResult, err error, now time.Time) {
	obj.Result = result
	obj.Attempts++
	at := metav1.NewTime(now.UTC())
	obj.LastAttemptAt = &at
	if err != nil {
		obj.Error = err.Error()
	} else {
		obj.Error = ""
	}
}
