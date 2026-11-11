package artifactgc

import (
	"context"
	"errors"
	"fmt"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	wfv1 "github.com/argoproj/argo-workflows/v4/pkg/apis/workflow/v1alpha1"
)

// Default plan bounds. They keep the serialized Plan well inside a Kubernetes
// annotation size limit and bound the Apply blast radius. Controllers and executors
// override them through the documented environment variables.
const (
	DefaultMaxObjects = 10000
	// DefaultMaxBytes is 10 GiB.
	DefaultMaxBytes int64 = 10 * 1024 * 1024 * 1024
	// DefaultTTL is how long a frozen Plan remains applicable.
	DefaultTTL = 24 * time.Hour
)

// ErrPlanLimitExceeded means building the Plan would exceed a configured bound. The
// Plan is rejected rather than truncated, because a truncated directory listing could
// not be safely distinguished from an incomplete one.
var ErrPlanLimitExceeded = errors.New("artifact GC Plan limit exceeded")

// PlanInput identifies one output artifact to snapshot during the preview phase.
type PlanInput struct {
	// NodeID is the Workflow node that produced the artifact.
	NodeID string
	// ArtifactName is the output artifact name on the node.
	ArtifactName string
	// Artifact is the fully relocated artifact specification, including secret
	// references. It is used only to construct runtime drivers; no credential value is
	// copied into the Plan.
	Artifact *wfv1.Artifact
	// Prefix is true when the artifact is a directory addressed by a key prefix.
	Prefix bool
}

// Planner builds bounded Plans without deleting anything.
type Planner struct {
	Limits PlanLimits
	Now    func() time.Time
}

// NewPlanner constructs a Planner with the given bounds and clock.
func NewPlanner(limits PlanLimits, now func() time.Time) *Planner {
	if now == nil {
		now = time.Now
	}
	return &Planner{Limits: limits, Now: now}
}

// BuildTarget snapshots one artifact through a real VersionedGCDriver and freezes its
// finite object/version list. It never deletes.
//
// When the driver reports ErrVersionedGCUnsupported the returned target is marked
// unsupported with no objects, so the caller can record that outcome explicitly; no
// bare delete fallback is attempted.
func (p *Planner) BuildTarget(ctx context.Context, drv VersionedGCDriver, in PlanInput) (*PlanTarget, error) {
	target := &PlanTarget{
		NodeID:       in.NodeID,
		ArtifactName: in.ArtifactName,
		Prefix:       in.Prefix,
		Supported:    true,
		Completeness: CompletenessComplete,
	}
	if in.Artifact != nil && in.Artifact.S3 != nil {
		target.S3 = &S3Location{
			Endpoint: in.Artifact.S3.Endpoint,
			Bucket:   in.Artifact.S3.Bucket,
			Key:      in.Artifact.S3.Key,
			Region:   in.Artifact.S3.Region,
		}
	}

	outcome, err := drv.SnapshotVersions(ctx, in.Artifact)
	if err != nil {
		if errors.Is(err, ErrVersionedGCUnsupported) {
			target.Supported = false
			target.UnsupportedReason = err.Error()
			target.Completeness = CompletenessIncomplete
			return target, nil
		}
		return nil, fmt.Errorf("failed to snapshot versions for node %q artifact %q: %w", in.NodeID, in.ArtifactName, err)
	}

	target.Completeness = outcome.Completeness
	if target.Completeness == "" {
		target.Completeness = CompletenessIncomplete
	}
	target.Objects = make([]PlannedObject, 0, len(outcome.Objects))
	for _, obj := range outcome.Objects {
		if obj.VersionID == "" {
			// An object without an immutable version ID cannot be conditionally
			// deleted. Refuse the whole target rather than silently downgrading to a
			// bare delete.
			target.Supported = false
			target.UnsupportedReason = ErrVersionedGCUnsupported.Error() + ": object " + obj.Bucket + "/" + obj.Key + " has no version ID"
			target.Completeness = CompletenessIncomplete
			target.Objects = nil
			return target, nil
		}
		target.Objects = append(target.Objects, PlannedObject{
			ObjectVersion: obj,
			NodeID:        in.NodeID,
			ArtifactName:  in.ArtifactName,
			Result:        ResultPending,
		})
	}
	return target, nil
}

// UnsupportedTarget records the explicit outcome for a driver that has no versioned
// GC capability at all. It never carries deletable objects.
func UnsupportedTarget(in PlanInput, reason string) *PlanTarget {
	return &PlanTarget{
		NodeID:            in.NodeID,
		ArtifactName:      in.ArtifactName,
		Prefix:            in.Prefix,
		Supported:         false,
		UnsupportedReason: reason,
		Completeness:      CompletenessIncomplete,
	}
}

// BuildPlan assembles frozen targets into a bounded Plan for one Workflow identity and
// Strategy. The preview Plan performs no deletion.
func (p *Planner) BuildPlan(identity WorkflowIdentity, strategy string, targets []PlanTarget) (*ArtifactGCPlan, error) {
	now := p.Now()
	plan := &ArtifactGCPlan{
		APIVersion: PlanAPIVersion,
		Kind:       PlanKind,
		Workflow:   identity,
		Strategy:   strategy,
		Phase:      PhasePreview,
		Limits:     p.Limits,
		CreatedAt:  metav1.NewTime(now.UTC()),
		ExpiresAt:  metav1.NewTime(now.UTC().Add(p.TTL())),
		Targets:    targets,
	}

	objectCount := 0
	var totalBytes int64
	for i := range targets {
		objectCount += len(targets[i].Objects)
		for j := range targets[i].Objects {
			totalBytes += targets[i].Objects[j].Size
		}
	}
	if p.Limits.MaxObjects > 0 && objectCount > p.Limits.MaxObjects {
		return nil, fmt.Errorf("%w: %d objects exceed MaxObjects %d", ErrPlanLimitExceeded, objectCount, p.Limits.MaxObjects)
	}
	if p.Limits.MaxBytes > 0 && totalBytes > p.Limits.MaxBytes {
		return nil, fmt.Errorf("%w: %d bytes exceed MaxBytes %d", ErrPlanLimitExceeded, totalBytes, p.Limits.MaxBytes)
	}

	if err := plan.recomputeDerived(now); err != nil {
		return nil, err
	}
	return plan, nil
}

// TTL returns the configured Plan TTL or the default.
func (p *Planner) TTL() time.Duration {
	if p.Limits.TTLSeconds <= 0 {
		return DefaultTTL
	}
	return time.Duration(p.Limits.TTLSeconds) * time.Second
}
