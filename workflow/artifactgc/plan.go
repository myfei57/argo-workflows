// Package artifactgc implements the bounded, version-aware Artifact GC Plan and its
// explicit Apply flow.
//
// A Plan is built during a preview phase that performs no deletion. It freezes the
// finite set of concrete object versions addressed by one Workflow's Artifact GC
// Strategy, together with the Workflow identity (UID and resourceVersion) at planning
// time. Apply never re-enumerates storage by prefix: it conditionally deletes exactly
// the versions frozen in the Plan, so an object that was overwritten after planning
// cannot have its new version removed.
//
// The Plan type carries no credentials. Drivers are constructed at runtime with the
// same Service Account and secret mounts as the existing Artifact GC Pods.
package artifactgc

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// PlanAPIVersion is the schema version of an ArtifactGCPlan document.
	PlanAPIVersion = "argoproj.io/v1alpha1"
	// PlanKind identifies a serialized ArtifactGCPlan.
	PlanKind = "ArtifactGCPlan"
)

// PlanPhase is the lifecycle state of an ArtifactGCPlan.
type PlanPhase string

const (
	// PhasePreview means the Plan has only been built: no deletion has happened.
	PhasePreview PlanPhase = "Preview"
	// PhaseApplying means at least one Apply run has started and work may remain.
	PhaseApplying PlanPhase = "Applying"
	// PhaseComplete means every frozen object is terminal and all were deleted
	// (or had already gone). Only a Complete Plan may be used to promise full cleanup.
	PhaseComplete PlanPhase = "Complete"
	// PhaseFailed means a terminal, non-retryable refusal (version conflict or
	// unsupported storage) prevents the Plan from finishing.
	PhaseFailed PlanPhase = "Failed"
	// PhaseExpired means the Plan outlived its TTL without finishing and must be
	// rebuilt rather than applied.
	PhaseExpired PlanPhase = "Expired"
)

// Completeness states whether a frozen directory listing can be proven to contain
// every object version under a prefix.
type Completeness string

const (
	// CompletenessComplete means the frozen set is provably finite and exhaustive.
	CompletenessComplete Completeness = "Complete"
	// CompletenessIncomplete means the underlying listing could not prove
	// exhaustiveness (for example a weakly consistent prefix listing). The Plan may
	// still be applied to the frozen versions, but it must never be used to promise
	// full cleanup or to remove the Workflow's Artifact GC finalizer.
	CompletenessIncomplete Completeness = "Incomplete"
)

// ObjectResult is the independently recorded outcome for one frozen object version.
type ObjectResult string

const (
	// ResultPending is the initial state before any Apply attempt.
	ResultPending ObjectResult = "Pending"
	// ResultSucceeded means the exact frozen version was deleted conditionally.
	ResultSucceeded ObjectResult = "Succeeded"
	// ResultAlreadyGone means the exact frozen version no longer existed when Apply
	// ran. The desired end state is already satisfied.
	ResultAlreadyGone ObjectResult = "AlreadyGone"
	// ResultVersionConflict means storage refused the conditional delete because the
	// current state no longer matches the frozen version. The new object is untouched.
	ResultVersionConflict ObjectResult = "VersionConflict"
	// ResultUnsupported means the storage cannot provide versioned conditional
	// deletion. A bare delete must never be substituted.
	ResultUnsupported ObjectResult = "Unsupported"
	// ResultUnknown means the outcome of an idempotent same-version attempt could not
	// be confirmed (timeout, transient storage error, unparseable response).
	ResultUnknown ObjectResult = "Unknown"
)

// terminalResults never receive another Apply attempt.
var terminalResults = map[ObjectResult]bool{
	ResultSucceeded:       true,
	ResultAlreadyGone:     true,
	ResultVersionConflict: true,
	ResultUnsupported:     true,
}

// retryableResults are safe to retry because a same-version conditional delete is
// provably idempotent: a repeated attempt deletes the same immutable version, and a
// second success is reported as AlreadyGone.
var retryableResults = map[ObjectResult]bool{
	ResultPending: true,
	ResultUnknown: true,
}

// IsTerminal reports whether no further attempt must be made for this result.
func (r ObjectResult) IsTerminal() bool { return terminalResults[r] }

// IsRetryable reports whether a same-version retry is permitted for this result.
func (r ObjectResult) IsRetryable() bool { return retryableResults[r] }

// WorkflowIdentity pins the Workflow instance a Plan was built for.
type WorkflowIdentity struct {
	// Namespace of the Workflow.
	Namespace string `json:"namespace"`
	// Name of the Workflow.
	Name string `json:"name"`
	// UID is the immutable Workflow identity. Apply requires the same UID.
	UID string `json:"uid"`
	// ResourceVersion is the Workflow resourceVersion captured at planning time and
	// is part of the audited Plan identity even though Apply does not require an
	// unchanged value (controllers legitimately update the Workflow between phases).
	ResourceVersion string `json:"resourceVersion"`
}

// ObjectVersion is one concrete, addressable object version. It never contains
// credentials.
type ObjectVersion struct {
	// Bucket holding the object.
	Bucket string `json:"bucket"`
	// Key of the object.
	Key string `json:"key"`
	// VersionID is the storage-assigned, immutable version identifier. An empty
	// VersionID can never be conditionally deleted.
	VersionID string `json:"versionID"`
	// ETag captured at planning time, retained for auditing.
	ETag string `json:"etag,omitempty"`
	// Size is the object size in bytes captured at planning time.
	Size int64 `json:"size"`
	// IsDeleteMarker marks a version that is a delete marker rather than data.
	IsDeleteMarker bool `json:"isDeleteMarker,omitempty"`
}

// PlannedObject pairs a frozen version with its independently recorded Apply result.
type PlannedObject struct {
	ObjectVersion `json:",inline"`
	// NodeID is the Workflow node that produced the artifact.
	NodeID string `json:"nodeID"`
	// ArtifactName is the output artifact name on the node.
	ArtifactName string `json:"artifactName"`
	// Result is the latest independently recorded outcome.
	Result ObjectResult `json:"result"`
	// Error carries the storage error message for the latest attempt, if any.
	Error string `json:"error,omitempty"`
	// Attempts counts Apply attempts against this exact version.
	Attempts int `json:"attempts,omitempty"`
	// LastAttemptAt records when the latest attempt happened, if any.
	LastAttemptAt *metav1.Time `json:"lastAttemptAt,omitempty"`
}

// S3Location records the non-secret coordinates needed to reconstruct an S3 driver at
// runtime. Secret references stay on the WorkflowArtifactGCTask Spec and are never
// copied into a Plan.
type S3Location struct {
	Endpoint string `json:"endpoint"`
	Bucket   string `json:"bucket"`
	Key      string `json:"key"`
	Region   string `json:"region,omitempty"`
}

// PlanTarget is the frozen, finite object/version list for one output artifact.
type PlanTarget struct {
	// NodeID is the Workflow node that produced the artifact.
	NodeID string `json:"nodeID"`
	// ArtifactName is the output artifact name on the node.
	ArtifactName string `json:"artifactName"`
	// S3 is the non-secret storage coordinate of the artifact.
	S3 *S3Location `json:"s3,omitempty"`
	// Prefix is true when the artifact is a directory addressed by a key prefix.
	Prefix bool `json:"prefix,omitempty"`
	// Supported is false when the storage cannot provide versioned conditional
	// deletion for this target. Such a target never carries deletable objects and a
	// bare delete must not be substituted.
	Supported bool `json:"supported"`
	// UnsupportedReason explains why Supported is false.
	UnsupportedReason string `json:"unsupportedReason,omitempty"`
	// Completeness is the strongest exhaustiveness guarantee for Objects.
	Completeness Completeness `json:"completeness"`
	// Objects is the frozen finite object/version list. Apply only ever addresses
	// entries from this list and must not re-enumerate the prefix.
	Objects []PlannedObject `json:"objects"`
}

// PlanLimits bounds a Plan so its serialized form and its blast radius stay finite.
type PlanLimits struct {
	// MaxObjects is the maximum number of frozen object versions across all targets.
	MaxObjects int `json:"maxObjects"`
	// MaxBytes is the maximum summed size of frozen object versions.
	MaxBytes int64 `json:"maxBytes"`
	// TTLSeconds bounds how long a frozen Plan remains valid for Apply.
	TTLSeconds int64 `json:"ttlSeconds"`
}

// PlanSummary counts what the frozen Plan covers.
type PlanSummary struct {
	// ObjectCount is the total number of frozen object versions.
	ObjectCount int `json:"objectCount"`
	// TotalBytes is the summed size of the frozen object versions.
	TotalBytes int64 `json:"totalBytes"`
}

// ArtifactGCPlan is the bounded, serializable garbage-collection Plan for one Workflow
// and one Artifact GC Strategy.
type ArtifactGCPlan struct {
	// APIVersion identifies the Plan schema.
	APIVersion string `json:"apiVersion"`
	// Kind is always ArtifactGCPlan.
	Kind string `json:"kind"`
	// PlanID is the hash of the frozen Plan content and is used to detect tampering or
	// substitution before Apply.
	PlanID string `json:"planID"`
	// Workflow pins the Workflow identity captured at planning time.
	Workflow WorkflowIdentity `json:"workflow"`
	// Strategy is the Artifact GC Strategy this Plan serves.
	Strategy string `json:"strategy"`
	// Phase is the current Plan lifecycle state.
	Phase PlanPhase `json:"phase"`
	// Completeness is the weakest completeness guarantee across all targets.
	Completeness Completeness `json:"completeness"`
	// CreatedAt is when the Plan was frozen.
	CreatedAt metav1.Time `json:"createdAt"`
	// ExpiresAt is CreatedAt plus the configured TTL.
	ExpiresAt metav1.Time `json:"expiresAt"`
	// Limits records the bounds enforced while building the Plan.
	Limits PlanLimits `json:"limits"`
	// Summary counts the frozen contents.
	Summary PlanSummary `json:"summary"`
	// Targets is the bounded list of per-artifact frozen object sets.
	Targets []PlanTarget `json:"targets"`
}

// objectIdentity is the immutable part of a PlannedObject: the concrete version and
// its owner node/artifact. Mutable Apply results are deliberately excluded.
type objectIdentity struct {
	ObjectVersion `json:",inline"`
	NodeID        string `json:"nodeID"`
	ArtifactName  string `json:"artifactName"`
}

// targetIdentity is the immutable part of a PlanTarget.
type targetIdentity struct {
	NodeID            string           `json:"nodeID"`
	ArtifactName      string           `json:"artifactName"`
	S3                *S3Location      `json:"s3,omitempty"`
	Prefix            bool             `json:"prefix,omitempty"`
	Supported         bool             `json:"supported"`
	UnsupportedReason string           `json:"unsupportedReason,omitempty"`
	Completeness      Completeness     `json:"completeness"`
	Objects           []objectIdentity `json:"objects"`
}

// planIdentity is the canonical, immutable subset of Plan content hashed into PlanID.
// Maps are deliberately absent so marshaling is deterministic without custom ordering.
type planIdentity struct {
	Workflow  WorkflowIdentity `json:"workflow"`
	Strategy  string           `json:"strategy"`
	Targets   []targetIdentity `json:"targets"`
	Limits    PlanLimits       `json:"limits"`
	CreatedAt metav1.Time      `json:"createdAt"`
	ExpiresAt metav1.Time      `json:"expiresAt"`
}

// ComputePlanID hashes the frozen Plan content. Per-object results, attempt counters
// and Phase are deliberately excluded so Apply result updates do not change the Plan
// identity.
func ComputePlanID(p *ArtifactGCPlan) (string, error) {
	identity := planIdentity{
		Workflow:  p.Workflow,
		Strategy:  p.Strategy,
		Targets:   make([]targetIdentity, 0, len(p.Targets)),
		Limits:    p.Limits,
		CreatedAt: p.CreatedAt,
		ExpiresAt: p.ExpiresAt,
	}
	for i := range p.Targets {
		t := &p.Targets[i]
		ti := targetIdentity{
			NodeID:            t.NodeID,
			ArtifactName:      t.ArtifactName,
			S3:                t.S3,
			Prefix:            t.Prefix,
			Supported:         t.Supported,
			UnsupportedReason: t.UnsupportedReason,
			Completeness:      t.Completeness,
			Objects:           make([]objectIdentity, 0, len(t.Objects)),
		}
		for j := range t.Objects {
			o := &t.Objects[j]
			ti.Objects = append(ti.Objects, objectIdentity{
				ObjectVersion: o.ObjectVersion,
				NodeID:        o.NodeID,
				ArtifactName:  o.ArtifactName,
			})
		}
		identity.Targets = append(identity.Targets, ti)
	}
	raw, err := json.Marshal(identity)
	if err != nil {
		return "", fmt.Errorf("failed to canonicalize ArtifactGCPlan identity: %w", err)
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

// ErrPlanIdentityMismatch means a Plan document changed after it was frozen.
var ErrPlanIdentityMismatch = errors.New("artifact GC Plan identity does not match its frozen content")

// VerifyIdentity recomputes PlanID and rejects a tampered or substituted Plan.
func (p *ArtifactGCPlan) VerifyIdentity() error {
	want, err := ComputePlanID(p)
	if err != nil {
		return err
	}
	if want != p.PlanID {
		return fmt.Errorf("%w: expected %s, got %s", ErrPlanIdentityMismatch, want, p.PlanID)
	}
	return nil
}

// IsExpired reports whether the Plan outlived its TTL at the given time.
func (p *ArtifactGCPlan) IsExpired(now time.Time) bool {
	return !p.ExpiresAt.IsZero() && now.After(p.ExpiresAt.Time)
}

// AllObjectsTerminal reports whether every frozen object has a terminal result.
func (p *ArtifactGCPlan) AllObjectsTerminal() bool {
	for i := range p.Targets {
		for j := range p.Targets[i].Objects {
			if !p.Targets[i].Objects[j].Result.IsTerminal() {
				return false
			}
		}
	}
	return true
}

// AllObjectsDeleted reports whether every terminal object reached a deleted end state
// (deleted by Apply or already absent).
func (p *ArtifactGCPlan) AllObjectsDeleted() bool {
	for i := range p.Targets {
		for j := range p.Targets[i].Objects {
			r := p.Targets[i].Objects[j].Result
			if r != ResultSucceeded && r != ResultAlreadyGone {
				return false
			}
		}
	}
	return true
}

// HasRetryableObjects reports whether another Apply run has work to do.
func (p *ArtifactGCPlan) HasRetryableObjects() bool {
	for i := range p.Targets {
		for j := range p.Targets[i].Objects {
			if p.Targets[i].Objects[j].Result.IsRetryable() {
				return true
			}
		}
	}
	return false
}

// HasUnsupportedTarget reports whether any target cannot use versioned conditional
// deletion.
func (p *ArtifactGCPlan) HasUnsupportedTarget() bool {
	for i := range p.Targets {
		if !p.Targets[i].Supported {
			return true
		}
	}
	return false
}

// HasVersionConflict reports whether a frozen version was refused due to conflicting
// storage state.
func (p *ArtifactGCPlan) HasVersionConflict() bool {
	for i := range p.Targets {
		for j := range p.Targets[i].Objects {
			if p.Targets[i].Objects[j].Result == ResultVersionConflict {
				return true
			}
		}
	}
	return false
}

// HasRefusalResult reports whether any frozen object hit a terminal Unsupported or
// VersionConflict result during Apply. Unlike target-level Supported flags these
// results are mutable and excluded from the Plan identity hash, so recording them does
// not invalidate the frozen Plan.
func (p *ArtifactGCPlan) HasRefusalResult() bool {
	for i := range p.Targets {
		for j := range p.Targets[i].Objects {
			switch p.Targets[i].Objects[j].Result {
			case ResultUnsupported, ResultVersionConflict:
				return true
			}
		}
	}
	return false
}

// PromisesFullCleanup is the only gate for marking a Strategy fully processed or
// removing the Artifact GC finalizer: the listing must be provably complete and every
// frozen object must have reached a deleted end state.
func (p *ArtifactGCPlan) PromisesFullCleanup() bool {
	return p.Completeness == CompletenessComplete && p.AllObjectsTerminal() && p.AllObjectsDeleted()
}

// recomputeDerived refreshes summary, aggregate completeness, PlanID and Phase from
// the targets. It is shared by Plan building and Apply result updates.
func (p *ArtifactGCPlan) recomputeDerived(now time.Time) error {
	p.Summary = PlanSummary{}
	completeness := CompletenessComplete
	for i := range p.Targets {
		t := &p.Targets[i]
		if t.Completeness == CompletenessIncomplete {
			completeness = CompletenessIncomplete
		}
		p.Summary.ObjectCount += len(t.Objects)
		for j := range t.Objects {
			p.Summary.TotalBytes += t.Objects[j].Size
		}
	}
	p.Completeness = completeness

	switch {
	case p.IsExpired(now):
		p.Phase = PhaseExpired
	case p.HasUnsupportedTarget() || p.HasRefusalResult():
		p.Phase = PhaseFailed
	case p.HasRetryableObjects():
		if p.Phase != PhasePreview {
			p.Phase = PhaseApplying
		}
	case p.PromisesFullCleanup():
		p.Phase = PhaseComplete
	default:
		// All terminal but an Incomplete listing forbids a full-cleanup promise.
		p.Phase = PhaseApplying
	}

	id, err := ComputePlanID(p)
	if err != nil {
		return err
	}
	p.PlanID = id
	return nil
}

// Marshal serializes a Plan to JSON after refreshing its derived fields.
func (p *ArtifactGCPlan) Marshal(now time.Time) ([]byte, error) {
	if err := p.recomputeDerived(now); err != nil {
		return nil, err
	}
	return json.Marshal(p)
}

// Unmarshal parses a serialized Plan without trusting its PlanID.
func UnmarshalPlan(raw []byte) (*ArtifactGCPlan, error) {
	var p ArtifactGCPlan
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, fmt.Errorf("failed to unmarshal ArtifactGCPlan: %w", err)
	}
	if p.Kind != PlanKind || p.APIVersion != PlanAPIVersion {
		return nil, fmt.Errorf("not an ArtifactGCPlan document: apiVersion=%q kind=%q", p.APIVersion, p.Kind)
	}
	return &p, nil
}
