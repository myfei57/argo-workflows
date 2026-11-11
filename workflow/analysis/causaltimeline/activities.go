package causaltimeline

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	wfv1 "github.com/argoproj/argo-workflows/v4/pkg/apis/workflow/v1alpha1"
)

type builder struct {
	wfName string
	nodes  wfv1.Nodes

	byName map[string]*wfv1.NodeStatus

	limits Limits

	// templateLookup resolves a DAG template by name from the workflow spec
	// or stored templates. A nil lookup means status-only analysis.
	templateLookup func(name string) *wfv1.Template

	acts  map[string]*Activity
	order []string

	kidsByScope map[string][]*Activity

	edges      map[edgeKey]*CausalEdge
	edgeOrder  []edgeKey
	incomingTo map[string][]*CausalEdge

	truncations       []Truncation
	notes             []string
	inconsistentEdges int
	unknownActivities int
	inconsistentActs  int
}

type edgeKey struct {
	from string
	to   string
	kind EdgeKind
}

func newBuilder(wfName string, nodes wfv1.Nodes, limits Limits) *builder {
	return &builder{
		wfName:      wfName,
		nodes:       nodes,
		byName:      make(map[string]*wfv1.NodeStatus),
		limits:      limits,
		acts:        make(map[string]*Activity),
		kidsByScope: make(map[string][]*Activity),
		edges:       make(map[edgeKey]*CausalEdge),
	}
}

func (b *builder) build(wfStarted, wfFinished time.Time) *WorkflowCausalTimeline {
	b.buildActivities()
	b.buildCausalEdges()
	b.synthesizeGaps()
	tl := b.finalize(wfStarted, wfFinished)
	return tl
}

// buildActivities maps every status node to exactly one activity. Status map
// keys are stable node IDs and therefore safe activity identities.
func (b *builder) buildActivities() {
	nodeIDs := make([]string, 0, len(b.nodes))
	for id := range b.nodes {
		nodeIDs = append(nodeIDs, id)
	}
	slices.Sort(nodeIDs)

	for _, id := range nodeIDs {
		node := b.nodes[id]
		b.byName[node.Name] = &node
		if len(b.acts) >= b.limits.MaxActivities {
			b.truncations = append(b.truncations, Truncation{What: "activities", Limit: b.limits.MaxActivities, Dropped: len(nodeIDs) - len(b.acts)})
			break
		}
		b.acts[id] = b.activityForNode(id, &node)
		b.order = append(b.order, id)
	}

	// Link the containment hierarchy from the recorded children lists.
	for _, id := range b.order {
		parent := b.acts[id]
		node := b.nodes[id]
		for _, childID := range node.Children {
			child, ok := b.acts[childID]
			if !ok {
				b.notes = append(b.notes, fmt.Sprintf("node %q references unknown child node %q; tree edge ignored", node.Name, childID))
				continue
			}
			child.ScopeID = parent.ID
			b.kidsByScope[parent.ID] = append(b.kidsByScope[parent.ID], child)
		}
	}
	for id := range b.kidsByScope {
		kids := b.kidsByScope[id]
		slices.SortStableFunc(kids, compareActivities)
		b.kidsByScope[id] = kids
	}
}

func (b *builder) activityForNode(id string, node *wfv1.NodeStatus) *Activity {
	kind, enclosing := classifyNode(node.Type)
	a := &Activity{
		ID:          id,
		NodeID:      id,
		NodeName:    node.Name,
		DisplayName: node.DisplayName,
		Kind:        kind,
		Phase:       node.Phase,
		Enclosing:   enclosing,
		Evidence: []Evidence{{
			Source:   EvidenceStatusTree,
			Field:    fmt.Sprintf("status.nodes[%q].type", id),
			NodeName: node.Name,
			Detail:   fmt.Sprintf("node type %s, phase %s", node.Type, node.Phase),
		}},
	}

	switch {
	case node.Phase == wfv1.NodeSkipped || node.Phase == wfv1.NodeOmitted:
		// A skipped or omitted node never executed: zero duration is a fact
		// evidenced by the phase, not a timestamp-derived guess.
		d := time.Duration(0)
		a.LowerBound = 0
		a.UpperBound = &d
		a.Certainty = CertaintyKnown
		a.Evidence = append(a.Evidence, Evidence{
			Source:   EvidenceStatusPhase,
			Field:    fmt.Sprintf("status.nodes[%q].phase", id),
			NodeName: node.Name,
			Detail:   fmt.Sprintf("phase %s means no execution occurred", node.Phase),
		})
	case !node.StartedAt.IsZero() && !node.FinishedAt.IsZero():
		start := node.StartedAt.Time
		finish := node.FinishedAt.Time
		a.StartedAt = &start
		a.FinishedAt = &finish
		d := finish.Sub(start)
		if d < 0 {
			// Clock regression: never turn an impossible interval into a
			// precise (or negative) duration.
			a.Certainty = CertaintyInconsistent
			a.LowerBound = 0
			a.inconsistent()
			b.inconsistentActs++
			a.Evidence = append(a.Evidence, Evidence{
				Source:   EvidenceStatusTimestamp,
				Field:    fmt.Sprintf("status.nodes[%q].startedAt/finishedAt", id),
				NodeName: node.Name,
				Detail:   fmt.Sprintf("finishedAt %s is before startedAt %s", finish.Format(time.RFC3339Nano), start.Format(time.RFC3339Nano)),
			})
		} else {
			a.Certainty = CertaintyKnown
			a.LowerBound = d
			a.UpperBound = &d
			a.Evidence = append(a.Evidence, Evidence{
				Source:   EvidenceStatusTimestamp,
				Field:    fmt.Sprintf("status.nodes[%q].startedAt/finishedAt", id),
				NodeName: node.Name,
				Detail:   fmt.Sprintf("observed interval %s to %s, duration %s", start.Format(time.RFC3339Nano), finish.Format(time.RFC3339Nano), d),
			})
		}
	default:
		// Missing timestamp (or an unfinished node in a non-terminal status):
		// unknown interval, no fabricated zero duration.
		if !node.StartedAt.IsZero() {
			start := node.StartedAt.Time
			a.StartedAt = &start
		}
		if !node.FinishedAt.IsZero() {
			finish := node.FinishedAt.Time
			a.FinishedAt = &finish
		}
		a.Certainty = CertaintyUnknown
		a.LowerBound = 0
		b.unknownActivities++
		a.Evidence = append(a.Evidence, Evidence{
			Source:   EvidenceStatusTimestamp,
			Field:    fmt.Sprintf("status.nodes[%q].startedAt/finishedAt", id),
			NodeName: node.Name,
			Detail:   "interval endpoint missing; duration cannot be evidenced precisely",
		})
	}
	if enclosing {
		// Enclosing scopes and groups bound their children's intervals but
		// never enter path weight themselves: their observed interval stays
		// visible via StartedAt/FinishedAt and the bounds below are zero.
		zero := time.Duration(0)
		a.LowerBound = 0
		a.UpperBound = &zero
		a.Evidence = append(a.Evidence, Evidence{
			Source:   EvidenceStatusTree,
			Field:    fmt.Sprintf("status.nodes[%q].children", id),
			NodeName: node.Name,
			Detail:   "enclosing interval excluded from path weight; only contained leaves and gaps are summed",
		})
	}
	return a
}

func (a *Activity) inconsistent() {
	if a.Certainty != CertaintyInconsistent {
		a.Certainty = CertaintyInconsistent
	}
}

func classifyNode(t wfv1.NodeType) (ActivityKind, bool) {
	switch t {
	case wfv1.NodeTypeDAG:
		return KindDAGScope, true
	case wfv1.NodeTypeSteps:
		return KindStepsScope, true
	case wfv1.NodeTypeRetry:
		return KindRetryScope, true
	case wfv1.NodeTypeTaskGroup:
		return KindTaskGroup, true
	case wfv1.NodeTypeStepGroup:
		return KindStepGroup, true
	case wfv1.NodeTypeSkipped:
		return KindSkipped, false
	case wfv1.NodeTypePod, wfv1.NodeTypeContainer, wfv1.NodeTypeHTTP, wfv1.NodeTypePlugin, wfv1.NodeTypeSuspend:
		return KindPodExecution, false
	default:
		return KindOtherExecution, false
	}
}

// compareActivities defines the stable content-derived order: observed start,
// then observed finish, then activity ID. Nil timestamps sort last.
func compareActivities(a, b *Activity) int {
	if c := compareTimePtr(a.StartedAt, b.StartedAt); c != 0 {
		return c
	}
	if c := compareTimePtr(a.FinishedAt, b.FinishedAt); c != 0 {
		return c
	}
	return strings.Compare(a.ID, b.ID)
}

func compareTimePtr(a, b *time.Time) int {
	switch {
	case a == nil && b == nil:
		return 0
	case a == nil:
		return 1
	case b == nil:
		return -1
	case a.Before(*b):
		return -1
	case a.After(*b):
		return 1
	default:
		return 0
	}
}

// terminals resolves an activity to the leaf activities that represent its
// observable completion. DAG/Steps scopes resolve through their recorded
// outbound nodes (target ancestor semantics), falling back to all leaf
// descendants.
func (b *builder) terminals(a *Activity) []*Activity {
	if !a.Enclosing {
		return []*Activity{a}
	}
	node := b.nodes[a.NodeID]
	var out []*Activity
	seen := map[string]bool{}
	add := func(x *Activity) {
		for _, t := range b.terminals(x) {
			if !seen[t.ID] {
				seen[t.ID] = true
				out = append(out, t)
			}
		}
	}
	if len(node.OutboundNodes) > 0 {
		for _, id := range node.OutboundNodes {
			if child, ok := b.acts[id]; ok {
				add(child)
			}
		}
	}
	if len(out) == 0 {
		for _, child := range b.kidsByScope[a.ID] {
			add(child)
		}
	}
	if len(out) == 0 {
		return []*Activity{a}
	}
	slices.SortStableFunc(out, compareActivities)
	return out
}

// entries resolves an activity to its single deterministic starting leaf,
// used to fan causal edges into enclosing scopes without weighting the scope.
func (b *builder) entries(a *Activity) []*Activity {
	if !a.Enclosing {
		return []*Activity{a}
	}
	kids := b.kidsByScope[a.ID]
	if len(kids) == 0 {
		return []*Activity{a}
	}
	entry := slices.MinFunc(kids, compareActivities)
	return b.entries(entry)
}

// gapCandidate is an evidenced interval that sits before a child activity and
// still needs to be materialized as a gap.
type gapCandidate struct {
	start, finish time.Time
	prev, next    *Activity
}

// synthesizeGaps creates gap activities for unaccounted intervals inside
// every enclosing scope. Each gap is owned by exactly one scope, so gap
// durations can never be double counted. Intervals are derived per child from
// its latest finishing predecessor rather than only from adjacent siblings,
// which matters when parallel branches interleave on the timeline.
func (b *builder) synthesizeGaps() {
	scopeIDs := make([]string, 0, len(b.kidsByScope))
	for id, kids := range b.kidsByScope {
		if len(kids) > 0 {
			scopeIDs = append(scopeIDs, id)
		}
	}
	slices.Sort(scopeIDs)

	for _, scopeID := range scopeIDs {
		scope := b.acts[scopeID]
		kids := b.kidsByScope[scopeID]
		b.noteSiblingOverlaps(scope, kids)

		var candidates []gapCandidate

		for _, child := range kids {
			if child.StartedAt == nil {
				// The child's own Unknown activity already represents any
				// unknowable interval before it; synthesizing another gap
				// would allocate the same residual twice.
				continue
			}
			pred, predFinish := b.latestPredecessor(kids, child)
			var start *time.Time
			switch {
			case pred != nil:
				start = predFinish
			case scope.StartedAt != nil:
				t := *scope.StartedAt
				start = &t
			default:
				continue
			}
			if start.After(*child.StartedAt) {
				b.notes = append(b.notes, fmt.Sprintf("child %q starts before its evidenced predecessor in scope %q; interval evidence inconsistent", child.NodeName, scope.NodeName))
				continue
			}
			if start.Equal(*child.StartedAt) {
				continue
			}
			candidates = append(candidates, gapCandidate{start: *start, finish: *child.StartedAt, prev: pred, next: child})
		}

		last := kids[len(kids)-1]
		if scope.FinishedAt != nil && last.FinishedAt != nil && last.FinishedAt.Before(*scope.FinishedAt) {
			candidates = append(candidates, gapCandidate{start: *last.FinishedAt, finish: *scope.FinishedAt, prev: last})
		}

		slices.SortFunc(candidates, func(x, y gapCandidate) int {
			if !x.start.Equal(y.start) {
				return x.start.Compare(y.start)
			}
			if !x.finish.Equal(y.finish) {
				return x.finish.Compare(y.finish)
			}
			xid, yid := "", ""
			if x.next != nil {
				xid = x.next.ID
			}
			if y.next != nil {
				yid = y.next.ID
			}
			return strings.Compare(xid, yid)
		})

		gapIndex := 0
		seen := map[string]bool{}
		for _, cand := range candidates {
			key := cand.start.Format(time.RFC3339Nano) + "/" + cand.finish.Format(time.RFC3339Nano)
			if seen[key] {
				continue
			}
			seen[key] = true
			if !b.roomForActivity() {
				break
			}
			b.materializeGap(scope, gapIndex, cand)
			gapIndex++
		}
	}
	b.applyResidualBounds()
}

// applyResidualBounds gives an unbounded child a conservative upper bound
// derived from its enclosing scope's residual time, but only when it is the
// sole unbounded segment of that scope. When several segments lack evidence,
// the residual cannot be partitioned between them without inventing
// durations, so they stay unbounded. The lower bound always stays zero.
func (b *builder) applyResidualBounds() {
	scopeIDs := make([]string, 0, len(b.kidsByScope))
	for id := range b.kidsByScope {
		scopeIDs = append(scopeIDs, id)
	}
	slices.Sort(scopeIDs)
	for _, scopeID := range scopeIDs {
		scope := b.acts[scopeID]
		if scope.StartedAt == nil || scope.FinishedAt == nil {
			continue
		}
		var unbounded []*Activity
		for _, kid := range b.kidsByScope[scopeID] {
			if kid.UpperBound == nil {
				unbounded = append(unbounded, kid)
			}
		}
		if len(unbounded) != 1 {
			continue
		}
		residual := b.scopeResidual(scope)
		kid := unbounded[0]
		kid.UpperBound = &residual
		kid.Evidence = append(kid.Evidence, Evidence{
			Source:   EvidenceBounds,
			Field:    fmt.Sprintf("status.nodes[%q].startedAt/finishedAt", scope.NodeID),
			NodeName: scope.NodeName,
			Detail:   fmt.Sprintf("upper bound %s derived from the enclosing scope residual; the segment is not measured", residual),
		})
	}
}

// latestPredecessor returns the sibling (and its finish time) with the latest
// finish that is wholly before the child starts.
func (b *builder) latestPredecessor(kids []*Activity, child *Activity) (*Activity, *time.Time) {
	var pred *Activity
	var best time.Time
	for _, sibling := range kids {
		if sibling.ID == child.ID || sibling.FinishedAt == nil || sibling.FinishedAt.After(*child.StartedAt) {
			continue
		}
		if pred == nil || sibling.FinishedAt.After(best) || (sibling.FinishedAt.Equal(best) && sibling.ID < pred.ID) {
			pred = sibling
			best = *sibling.FinishedAt
		}
	}
	if pred == nil {
		return nil, nil
	}
	return pred, &best
}

func (b *builder) noteSiblingOverlaps(scope *Activity, kids []*Activity) {
	for i := 0; i+1 < len(kids); i++ {
		prev, next := kids[i], kids[i+1]
		if prev.FinishedAt != nil && next.StartedAt != nil && next.StartedAt.Before(*prev.FinishedAt) {
			b.notes = append(b.notes, fmt.Sprintf("sibling activities %q and %q overlap inside scope %q; no precise gap can be derived", prev.NodeName, next.NodeName, scope.NodeName))
		}
	}
}

// materializeGap creates a gap with two evidenced endpoints. Zero-length
// intervals are filtered before this point.
func (b *builder) materializeGap(scope *Activity, index int, cand gapCandidate) {
	kind, evidence := b.classifyGap(scope, cand.prev, cand.next, &cand.start, &cand.finish)
	gap := b.newGap(scope, index, kind, CertaintyKnown, evidence)
	gap.StartedAt = new(cand.start)
	gap.FinishedAt = new(cand.finish)
	d := cand.finish.Sub(cand.start)
	gap.LowerBound = d
	gap.UpperBound = &d
	b.wireGap(gap, cand.prev, cand.next)
}

func (b *builder) newGap(scope *Activity, index int, kind ActivityKind, certainty Certainty, evidence Evidence) *Activity {
	id := fmt.Sprintf("%s/gap/%03d", scope.ID, index)
	gap := &Activity{
		ID:        id,
		Kind:      kind,
		Enclosing: false,
		ScopeID:   scope.ID,
		Certainty: certainty,
		Evidence:  []Evidence{evidence},
	}
	if certainty == CertaintyInconsistent {
		b.inconsistentActs++
	}
	if certainty == CertaintyUnknown && gap.UpperBound == nil {
		b.unknownActivities++
	}
	b.acts[id] = gap
	b.order = append(b.order, id)
	b.kidsByScope[scope.ID] = append(b.kidsByScope[scope.ID], gap)
	return gap
}

func (b *builder) roomForActivity() bool {
	if len(b.acts) < b.limits.MaxActivities {
		return true
	}
	last := len(b.truncations) - 1
	if last >= 0 && b.truncations[last].What == "gap activities" {
		b.truncations[last].Dropped++
	} else {
		b.truncations = append(b.truncations, Truncation{What: "gap activities", Limit: b.limits.MaxActivities, Dropped: 1})
	}
	return false
}

// classifyGap decides whether an evidenced gap is dependency wait, retry
// backoff or unattributed unknown scheduling time. Evidenced gaps always
// carry Known certainty; unknown endpoints never reach this function.
func (b *builder) classifyGap(scope, prev, next *Activity, start, finish *time.Time) (ActivityKind, Evidence) {
	if scope.Kind == KindRetryScope && prev != nil {
		return KindRetryBackoff, Evidence{
			Source:   EvidenceRetrySequence,
			Field:    fmt.Sprintf("status.nodes[%q].children", scope.NodeID),
			NodeName: scope.NodeName,
			Detail:   fmt.Sprintf("backoff between consecutive retry attempts, evidenced by gap %s to %s", start.Format(time.RFC3339Nano), finish.Format(time.RFC3339Nano)),
		}
	}
	if scope.Kind == KindStepsScope && prev != nil {
		return KindDependencyWait, Evidence{
			Source:   EvidenceSpecStepsOrder,
			Field:    fmt.Sprintf("status.nodes[%q].children", scope.NodeID),
			NodeName: scope.NodeName,
			Detail:   "wait between sequential Steps groups",
		}
	}
	if next != nil {
		for _, target := range b.entries(next) {
			for _, e := range b.incomingTo[target.ID] {
				if e.Kind != EdgeDepends && e.Kind != EdgeStepsOrder {
					continue
				}
				source, ok := b.acts[e.From]
				if !ok || source.FinishedAt == nil {
					continue
				}
				if !source.FinishedAt.Before(*start) && !source.FinishedAt.After(*finish) {
					return KindDependencyWait, Evidence{
						Source:   e.Evidence.Source,
						Field:    e.Evidence.Field,
						NodeName: e.Evidence.NodeName,
						Detail:   fmt.Sprintf("successor %q waited on %q; dependency completed inside this gap", target.NodeName, source.NodeName),
					}
				}
			}
		}
	}
	return KindUnknownGap, Evidence{
		Source:   EvidenceStatusTimestamp,
		Field:    "status.nodes[*].startedAt/finishedAt",
		NodeName: scope.NodeName,
		Detail:   "no declared dependency completion explains this interval; unattributed scheduling time",
	}
}

// scopeResidual returns the scope interval not yet covered by evidenced
// direct segments. It is a conservative upper bound: enclosing children carry
// zero own weight, so the residual may over-approximate.
func (b *builder) scopeResidual(scope *Activity) time.Duration {
	if scope.StartedAt == nil || scope.FinishedAt == nil {
		return 0
	}
	used := time.Duration(0)
	for _, kid := range b.kidsByScope[scope.ID] {
		if kid.Certainty == CertaintyKnown {
			used += kid.LowerBound
		}
	}
	residual := scope.FinishedAt.Sub(*scope.StartedAt) - used
	if residual < 0 {
		return 0
	}
	return residual
}

// wireGap links a gap to its temporal neighbors through their terminal and
// entry leaves.
func (b *builder) wireGap(gap, prev, next *Activity) {
	if prev != nil {
		for _, from := range b.terminals(prev) {
			b.addEdge(from.ID, gap.ID, EdgeGap, gap.Certainty, gap.Evidence[0])
		}
	}
	if next != nil {
		for _, to := range b.entries(next) {
			b.addEdge(gap.ID, to.ID, EdgeGap, gap.Certainty, gap.Evidence[0])
		}
	}
}

// attemptIndex parses the trailing "(n)" retry attempt index from a node name.
func attemptIndex(name string) (int, bool) {
	open := strings.LastIndex(name, "(")
	if open <= 0 || !strings.HasSuffix(name, ")") {
		return 0, false
	}
	n, err := strconv.Atoi(name[open+1 : len(name)-1])
	if err != nil {
		return 0, false
	}
	return n, true
}
