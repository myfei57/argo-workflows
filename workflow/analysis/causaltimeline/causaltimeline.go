// Package causaltimeline builds a finite, evidence-bounded execution-activity
// DAG from a fixed (for example archived) Workflow status and explains the
// longest causal path through it.
//
// The analyzer is strictly read-only: the input Workflow and its status are
// never mutated, and every duration that enters a path is backed by an
// explicit piece of status or spec evidence. Parent DAG, Steps, Retry and
// group nodes are enclosing intervals only: their total duration is never
// added to a path on top of the leaf Pod and gap segments they contain.
// When timestamps are missing, regress, or overlap impossibly, the affected
// segment or edge is marked Unknown or Inconsistent and represented with
// bounds instead of a fabricated precise duration. The longest causal path is
// therefore not, and must not be read as, the resource-constrained scheduling
// makespan of the workflow.
package causaltimeline

import (
	"fmt"
	"time"

	wfv1 "github.com/argoproj/argo-workflows/v4/pkg/apis/workflow/v1alpha1"
)

// Limits caps the size of the generated graph and explanation so the analyzer
// stays finite on large workflows. Zero values fall back to DefaultLimits.
type Limits struct {
	// MaxActivities bounds the number of activities (including synthesized
	// gaps) returned in the timeline.
	MaxActivities int
	// MaxEdges bounds the number of causal edges returned in the timeline.
	MaxEdges int
	// MaxExamplePaths bounds the number of concrete longest-path examples
	// retained when several paths share the maximum weight.
	MaxExamplePaths int
	// MaxPathCount is the saturation threshold for counting equal-length
	// longest paths. Once the count reaches it, counting stops and the
	// truncated flag is reported instead of claiming an exact count.
	MaxPathCount int
}

// DefaultLimits are the caps applied when a Limits field is left zero.
var DefaultLimits = Limits{
	MaxActivities:   1000,
	MaxEdges:        2000,
	MaxExamplePaths: 10,
	MaxPathCount:    1000,
}

func (l Limits) withDefaults() Limits {
	d := DefaultLimits
	if l.MaxActivities > 0 {
		d.MaxActivities = l.MaxActivities
	}
	if l.MaxEdges > 0 {
		d.MaxEdges = l.MaxEdges
	}
	if l.MaxExamplePaths > 0 {
		d.MaxExamplePaths = l.MaxExamplePaths
	}
	if l.MaxPathCount > 0 {
		d.MaxPathCount = l.MaxPathCount
	}
	return d
}

// Certainty describes how strongly the evidence supports a precise claim
// about an activity interval or a causal edge.
type Certainty string

const (
	// CertaintyKnown means timestamps or phase evidence fully support the
	// claimed interval or ordering.
	CertaintyKnown Certainty = "Known"
	// CertaintyUnknown means evidence is missing, so only bounds are given.
	CertaintyUnknown Certainty = "Unknown"
	// CertaintyInconsistent means the available evidence contradicts itself
	// (for example a clock regression or an impossible overlap), so no
	// precise duration or ordering is claimed.
	CertaintyInconsistent Certainty = "Inconsistent"
)

// ActivityKind classifies a timeline activity.
type ActivityKind string

const (
	// KindPodExecution is real leaf work: a Pod, container, HTTP, plugin or
	// suspend activity. Only leaf executions and gaps carry path weight.
	KindPodExecution ActivityKind = "PodExecution"
	// KindOtherExecution is a leaf execution node of a type the analyzer does
	// not specifically model. It is accounted like a Pod execution.
	KindOtherExecution ActivityKind = "OtherExecution"
	// KindSkipped is a skipped or omitted node: it has zero duration and is
	// retained for identity and branch evidence.
	KindSkipped ActivityKind = "Skipped"
	// KindDAGScope is the enclosing interval of a DAG template invocation.
	KindDAGScope ActivityKind = "DAGScope"
	// KindStepsScope is the enclosing interval of a Steps template invocation.
	KindStepsScope ActivityKind = "StepsScope"
	// KindRetryScope is the enclosing interval of a Retry node.
	KindRetryScope ActivityKind = "RetryScope"
	// KindTaskGroup is the enclosing interval of an expanded with-items DAG
	// task group.
	KindTaskGroup ActivityKind = "TaskGroup"
	// KindStepGroup is the enclosing interval of one parallel Steps group.
	KindStepGroup ActivityKind = "StepGroup"
	// KindDependencyWait is a synthesized gap whose evidence shows the
	// successor was waiting on a declared dependency or a Steps group order.
	KindDependencyWait ActivityKind = "DependencyWait"
	// KindRetryBackoff is a synthesized gap between two consecutive retry
	// attempts, present only when both attempt timestamps evidence it.
	KindRetryBackoff ActivityKind = "RetryBackoff"
	// KindUnknownGap is a synthesized interval whose cause the status cannot
	// determine (for example unattributed scheduling time).
	KindUnknownGap ActivityKind = "UnknownGap"
)

// EvidenceSource identifies where a piece of evidence came from.
type EvidenceSource string

const (
	// EvidenceStatusTimestamp is a startedAt or finishedAt field in the
	// node status.
	EvidenceStatusTimestamp EvidenceSource = "StatusTimestamp"
	// EvidenceStatusPhase is a node phase field.
	EvidenceStatusPhase EvidenceSource = "StatusPhase"
	// EvidenceStatusTree is the parent/children or outbound-nodes structure
	// recorded in the node status.
	EvidenceStatusTree EvidenceSource = "StatusTree"
	// EvidenceSpecDepends is a depends/dependencies declaration in a DAG
	// template, combined with the observed task result.
	EvidenceSpecDepends EvidenceSource = "SpecDepends"
	// EvidenceSpecStepsOrder is the sequential ordering of Steps groups.
	EvidenceSpecStepsOrder EvidenceSource = "SpecStepsOrder"
	// EvidenceRetrySequence is the ordered attempt children of a Retry node.
	EvidenceRetrySequence EvidenceSource = "RetrySequence"
	// EvidenceInferredTemporal is an ordering inferred purely from observed
	// timestamps when no spec evidence is available.
	EvidenceInferredTemporal EvidenceSource = "InferredTemporal"
	// EvidenceBounds is a bound derived from an enclosing scope interval.
	EvidenceBounds EvidenceSource = "Bounds"
)

// Evidence is one auditable source supporting an activity or edge.
type Evidence struct {
	// Source classifies the evidence.
	Source EvidenceSource
	// Field is the status or spec field path carrying the evidence.
	Field string
	// NodeName is the status node the evidence was read from, when applicable.
	NodeName string
	// Detail is a short, deterministic human-readable explanation.
	Detail string
}

// Activity is one node of the execution timeline. Enclosing scopes and groups
// carry zero path weight; leaf executions and gaps carry the only durations
// that may be summed along a causal path.
type Activity struct {
	// ID is the stable identity of the activity: the node ID for
	// node-backed activities, or a deterministic gap identifier derived from
	// the owning scope and gap index.
	ID string
	// NodeID is the backing node ID, or empty for synthesized gaps.
	NodeID string
	// NodeName is the backing node tree name, or empty for gaps.
	NodeName string
	// DisplayName mirrors the node display name when present.
	DisplayName string
	// Kind classifies the activity.
	Kind ActivityKind
	// Phase is the observed node phase, empty for synthesized gaps.
	Phase wfv1.NodePhase
	// Enclosing is true for DAG/Steps/Retry/group scopes: they bound their
	// children's intervals but never contribute path weight themselves.
	Enclosing bool
	// ScopeID is the ID of the directly enclosing activity, empty at root.
	ScopeID string
	// StartedAt and FinishedAt are the observed interval endpoints. They are
	// nil when the status carries no timestamp; skipped activities have no
	// interval at all.
	StartedAt  *time.Time
	FinishedAt *time.Time
	// LowerBound is the duration proven by evidence. It is zero for unknown
	// segments and must never be read as a known zero-length duration: check
	// Certainty first.
	LowerBound time.Duration
	// UpperBound is the largest duration consistent with the evidence. A nil
	// upper bound means no finite upper bound can be derived.
	UpperBound *time.Duration
	// Certainty states how strongly the interval is evidenced.
	Certainty Certainty
	// Evidence is the ordered list of sources supporting the activity.
	Evidence []Evidence
	// TieBreaker is a deterministic ordering key: activities are numbered in
	// a stable, content-derived order so equal timestamps never make the
	// output depend on map iteration.
	TieBreaker int
}

// EdgeKind classifies a causal edge.
type EdgeKind string

const (
	// EdgeDepends is a causal edge supported by a DAG depends/dependencies
	// declaration and the observed task result.
	EdgeDepends EdgeKind = "Depends"
	// EdgeStepsOrder is the sequential ordering between Steps groups.
	EdgeStepsOrder EdgeKind = "StepsOrder"
	// EdgeRetrySequence is the ordering between consecutive retry attempts.
	EdgeRetrySequence EdgeKind = "RetrySequence"
	// EdgeGap links a synthesized wait/backoff/unknown gap to the activities
	// that temporally bound it.
	EdgeGap EdgeKind = "GapInterval"
	// EdgeInferredTemporal is an edge inferred from timestamp ordering when
	// no spec evidence exists.
	EdgeInferredTemporal EdgeKind = "InferredTemporal"
)

// CausalEdge is one directed edge of the execution-activity DAG. Containment
// of activities inside scopes is represented via Activity.ScopeID, not as an
// edge, so enclosing intervals can never be double counted.
type CausalEdge struct {
	// From and To are activity IDs.
	From string
	To   string
	// Kind classifies the edge.
	Kind EdgeKind
	// Certainty states how strongly the ordering is evidenced.
	Certainty Certainty
	// Evidence describes the source of the edge.
	Evidence Evidence
}

// Truncation records that one of the configured limits was reached.
type Truncation struct {
	// What identifies the truncated collection (for example "causal edges").
	What string
	// Limit is the cap that was reached.
	Limit int
	// Dropped is the number of elements not retained, when known.
	Dropped int
}

// PathExplanation summarizes the bounded longest causal path.
type PathExplanation struct {
	// LowerBoundDuration is the longest path duration using only fully
	// evidenced durations; unknown and inconsistent segments contribute zero
	// to the lower bound.
	LowerBoundDuration time.Duration
	// UpperBoundDuration is the longest path duration consistent with the
	// derived upper bounds. It is nil when any reachable segment has no
	// finite upper bound.
	UpperBoundDuration *time.Duration
	// UpperUnbounded reports that no finite upper bound exists.
	UpperUnbounded bool
	// LowerPath is the deterministic, tie-broken activity chain achieving
	// LowerBoundDuration, from a source activity to a sink.
	LowerPath []string
	// EqualLengthPathCount is the number of distinct paths achieving the
	// lower-bound maximum, saturated at Limits.MaxPathCount.
	EqualLengthPathCount int
	// PathCountTruncated reports that the true equal-length path count exceeds
	// the saturation threshold.
	PathCountTruncated bool
	// ExamplePaths retains up to Limits.MaxExamplePaths concrete maximum-weight
	// paths so ties are represented by finite examples rather than expanded
	// blindly.
	ExamplePaths [][]string
	// ContainsInconsistentEdge reports that at least one maximum-weight path
	// traverses an edge whose timestamp ordering is inconsistent.
	ContainsInconsistentEdge bool
	// IsResourceMakespan is always false: a causal path never sums parallel
	// branches and cannot claim to be the resource-constrained makespan.
	IsResourceMakespan bool
	// Notes carries deterministic explanatory text, including evidence gaps
	// and interpretation warnings.
	Notes []string
}

// WorkflowCausalTimeline is the finite execution-activity DAG plus its longest-path
// explanation for one fixed Workflow status.
type WorkflowCausalTimeline struct {
	// WorkflowName is the analyzed workflow's name.
	WorkflowName string
	// Limits is the effective caps after defaults were applied.
	Limits Limits
	// Activities is the deterministic, tie-broken activity list.
	Activities []Activity
	// Edges is the deduplicated causal edge list, sorted deterministically.
	Edges []CausalEdge
	// LongestPath explains the bounded longest causal path.
	LongestPath PathExplanation
	// Truncated lists every cap reached while building the timeline.
	Truncated []Truncation
	// UnknownActivities counts activities whose interval lacks evidence.
	UnknownActivities int
	// InconsistentActivities counts activities with contradictory evidence.
	InconsistentActivities int
	// InconsistentEdges counts edges with impossible timestamp ordering.
	InconsistentEdges int
	// Notes carries workflow-level evidence and interpretation notes.
	Notes []string
}

// AnalyzeWorkflow builds the causal timeline from an archived workflow. Both
// spec (when present) and status evidence are used; the workflow is never
// mutated.
func AnalyzeWorkflow(wf *wfv1.Workflow, limits Limits) (*WorkflowCausalTimeline, error) {
	if wf == nil {
		return nil, fmt.Errorf("causaltimeline: nil workflow")
	}
	b := newBuilder(wf.Name, wf.Status.Nodes, limits.withDefaults())
	b.templateLookup = wf.GetTemplateByName
	return b.build(wf.Status.StartedAt.Time, wf.Status.FinishedAt.Time), nil
}

// AnalyzeStatus builds the causal timeline from a workflow status alone, for
// example when the spec is unavailable. DAG dependency edges cannot be proven
// without a spec, so cross-activity order is inferred from timestamp evidence
// and explicitly marked InferredTemporal. The status is never mutated.
func AnalyzeStatus(workflowName string, status *wfv1.WorkflowStatus, limits Limits) (*WorkflowCausalTimeline, error) {
	if status == nil {
		return nil, fmt.Errorf("causaltimeline: nil workflow status")
	}
	b := newBuilder(workflowName, status.Nodes, limits.withDefaults())
	return b.build(status.StartedAt.Time, status.FinishedAt.Time), nil
}
