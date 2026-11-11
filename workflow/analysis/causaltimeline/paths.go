package causaltimeline

import (
	"fmt"
	"slices"
	"time"
)

// finalize orders activities and edges deterministically, resolves any
// (evidence-impossible) cycle, runs the bounded longest-path calculation and
// assembles the timeline.
func (b *builder) finalize(wfStarted, wfFinished time.Time) *WorkflowCausalTimeline {
	ids := make([]string, 0, len(b.acts))
	for id := range b.acts {
		ids = append(ids, id)
	}
	slices.SortFunc(ids, func(x, y string) int {
		return compareActivities(b.acts[x], b.acts[y])
	})

	activities := make([]Activity, 0, len(ids))
	for i, id := range ids {
		a := b.acts[id]
		a.TieBreaker = i
		b.acts[id] = a
		activities = append(activities, *a)
	}

	topo, adj, pred := b.topologicalOrder(ids)
	explanation := b.longestPath(topo, adj, pred)

	edgeKeys := slices.Clone(b.edgeOrder)
	slices.SortFunc(edgeKeys, func(x, y edgeKey) int {
		if x.from != y.from {
			return slices.Compare([]string{x.from}, []string{y.from})
		}
		if x.to != y.to {
			return slices.Compare([]string{x.to}, []string{y.to})
		}
		return slices.Compare([]string{string(x.kind)}, []string{string(y.kind)})
	})
	edges := make([]CausalEdge, 0, len(edgeKeys))
	for _, key := range edgeKeys {
		edges = append(edges, *b.edges[key])
	}

	tl := &WorkflowCausalTimeline{
		WorkflowName:           b.wfName,
		Limits:                 b.limits,
		Activities:             activities,
		Edges:                  edges,
		LongestPath:            explanation,
		Truncated:              slices.Clone(b.truncations),
		UnknownActivities:      b.unknownActivities,
		InconsistentActivities: b.inconsistentActs,
		InconsistentEdges:      b.inconsistentEdges,
	}
	tl.Notes = b.workflowNotes(wfStarted, wfFinished)
	return tl
}

// topologicalOrder runs Kahn's algorithm over the causal graph. Edges only
// connect non-enclosing leaves and gaps. If contradictory evidence would form
// a cycle, the deterministically latest edge is flagged Inconsistent and
// excluded from path calculation instead of producing an impossible ordering.
func (b *builder) topologicalOrder(orderedIDs []string) ([]string, map[string][]string, map[string][]string) {
	causal := make(map[string]bool, len(orderedIDs))
	for _, id := range orderedIDs {
		if !b.acts[id].Enclosing {
			causal[id] = true
		}
	}

	adj := map[string][]string{}
	pred := map[string][]string{}
	for id := range causal {
		adj[id] = nil
		pred[id] = nil
	}
	var activeEdges []edgeKey
	for _, key := range b.edgeOrder {
		if causal[key.from] && causal[key.to] {
			activeEdges = append(activeEdges, key)
			adj[key.from] = append(adj[key.from], key.to)
			pred[key.to] = append(pred[key.to], key.from)
		}
	}
	for id := range adj {
		slices.Sort(adj[id])
		slices.Sort(pred[id])
	}

	for {
		topo, leftover := b.kahn(orderedIDs, adj, pred)
		if len(leftover) == 0 {
			return topo, adj, pred
		}
		drop := b.cycleEdge(activeEdges, leftover)
		if drop == nil {
			return topo, adj, pred
		}
		edge := b.edges[*drop]
		if edge.Certainty != CertaintyInconsistent {
			edge.Certainty = CertaintyInconsistent
			b.inconsistentEdges++
			b.notes = append(b.notes, fmt.Sprintf("causal edge %s -> %s (%s) forms an ordering cycle and is excluded from path calculation", drop.from, drop.to, drop.kind))
		}
		adj[drop.from] = slices.DeleteFunc(adj[drop.from], func(x string) bool { return x == drop.to })
		pred[drop.to] = slices.DeleteFunc(pred[drop.to], func(x string) bool { return x == drop.from })
		activeEdges = slices.DeleteFunc(activeEdges, func(k edgeKey) bool { return k == *drop })
	}
}

func (b *builder) kahn(orderedIDs []string, adj, pred map[string][]string) ([]string, map[string]bool) {
	indegree := make(map[string]int, len(orderedIDs))
	queue := make([]string, 0)
	for _, id := range orderedIDs {
		if _, ok := adj[id]; !ok {
			continue
		}
		indegree[id] = len(pred[id])
		if indegree[id] == 0 {
			queue = append(queue, id)
		}
	}
	var topo []string
	for len(queue) > 0 {
		id := queue[0]
		queue = queue[1:]
		topo = append(topo, id)
		nexts := slices.Clone(adj[id])
		slices.Sort(nexts)
		for _, next := range nexts {
			indegree[next]--
			if indegree[next] == 0 {
				queue = append(queue, next)
			}
		}
	}
	leftover := map[string]bool{}
	for id := range adj {
		if indegree[id] > 0 {
			leftover[id] = true
		}
	}
	return topo, leftover
}

// cycleEdge picks the deterministically latest edge internal to a cycle set.
func (b *builder) cycleEdge(edges []edgeKey, leftover map[string]bool) *edgeKey {
	var chosen *edgeKey
	for i := range edges {
		key := edges[i]
		if !leftover[key.from] || !leftover[key.to] {
			continue
		}
		if chosen == nil || edgeKeyLess(key, *chosen) {
			keyCopy := key
			chosen = &keyCopy
		}
	}
	return chosen
}

func edgeKeyLess(a, c edgeKey) bool {
	if a.from != c.from {
		return a.from > c.from
	}
	if a.to != c.to {
		return a.to > c.to
	}
	return a.kind > c.kind
}

// pathState holds the dynamic programming result for one node.
type pathState struct {
	bestLower    int64
	upper        *time.Duration
	bestPred     string
	chain        []string
	count        int
	countLimited bool
}

func (b *builder) longestPath(topo []string, adj, pred map[string][]string) PathExplanation {
	if len(topo) == 0 {
		return PathExplanation{
			IsResourceMakespan: false,
			Notes:              []string{"no execution activities were present in the status"},
		}
	}

	weightLower := func(id string) int64 { return int64(b.acts[id].LowerBound) }
	weightUpper := func(id string) *time.Duration { return b.acts[id].UpperBound }

	states := make(map[string]*pathState, len(topo))
	for _, id := range topo {
		state := &pathState{chain: []string{id}, count: 1}
		wL := weightLower(id)
		state.bestLower = wL
		wU := weightUpper(id)
		state.upper = wU

		var bestPred string
		for _, p := range pred[id] {
			ps := states[p]
			candidateLower := ps.bestLower + wL
			switch {
			case candidateLower > state.bestLower:
				state.bestLower = candidateLower
				bestPred = p
			case candidateLower == state.bestLower && bestPred != "" && lexLess(ps.chain, states[bestPred].chain):
				bestPred = p
			case candidateLower == state.bestLower && bestPred == "":
				bestPred = p
			}
			if wU != nil && ps.upper != nil {
				candidateUpper := *ps.upper + *wU
				if state.upper == nil || candidateUpper > *state.upper {
					d := candidateUpper
					state.upper = &d
				}
			} else {
				state.upper = nil
			}
		}
		if bestPred != "" {
			state.bestPred = bestPred
			state.chain = append(slices.Clone(states[bestPred].chain), id)
		}
		state.count, state.countLimited = b.sumPredCounts(pred[id], wL, state.bestLower, states)
		states[id] = state
	}

	var sinks []string
	for _, id := range topo {
		if len(adj[id]) == 0 {
			sinks = append(sinks, id)
		}
	}
	slices.Sort(sinks)

	maxLower := int64(0)
	for _, id := range sinks {
		if states[id].bestLower > maxLower {
			maxLower = states[id].bestLower
		}
	}
	maxSinks := make([]string, 0)
	totalCount := 0
	countLimited := false
	for _, id := range sinks {
		if states[id].bestLower != maxLower {
			continue
		}
		maxSinks = append(maxSinks, id)
		totalCount += states[id].count
		if states[id].countLimited {
			countLimited = true
		}
		if totalCount >= b.limits.MaxPathCount {
			totalCount = b.limits.MaxPathCount
			countLimited = true
			break
		}
	}

	examples := b.enumerateMaxPaths(maxSinks, pred, weightLower, states)

	var lowerPath []string
	if len(maxSinks) > 0 {
		lowerPath = slices.Clone(states[maxSinks[0]].chain)
	}
	inconsistent := b.pathHasInconsistentEdge(lowerPath)

	var upperMax *time.Duration
	unbounded := false
	for _, id := range sinks {
		u := states[id].upper
		if u == nil {
			unbounded = true
			continue
		}
		if upperMax == nil || *u > *upperMax {
			d := *u
			upperMax = &d
		}
	}

	return PathExplanation{
		LowerBoundDuration:       time.Duration(maxLower),
		UpperBoundDuration:       upperMax,
		UpperUnbounded:           unbounded,
		LowerPath:                lowerPath,
		EqualLengthPathCount:     totalCount,
		PathCountTruncated:       countLimited,
		ExamplePaths:             examples,
		ContainsInconsistentEdge: inconsistent,
		IsResourceMakespan:       false,
		Notes: []string{
			"path weight sums leaf execution and synthesized gap segments only; enclosing DAG/Steps/Retry/group durations are never added",
			"parallel branches are never summed and unknown segments contribute zero to the lower bound",
			"this is a causal-path bound, not the resource-constrained scheduling makespan",
		},
	}
}

func (b *builder) sumPredCounts(preds []string, wL, target int64, states map[string]*pathState) (int, bool) {
	if len(preds) == 0 {
		return 1, false
	}
	total := 0
	limited := false
	for _, p := range preds {
		ps := states[p]
		if ps.bestLower+wL != target {
			continue
		}
		total += ps.count
		if ps.countLimited {
			limited = true
		}
		if total >= b.limits.MaxPathCount {
			return b.limits.MaxPathCount, true
		}
	}
	return total, limited
}

// enumerateMaxPaths backtracks through best-weight predecessors and returns up
// to MaxExamplePaths concrete chains, deterministically ordered by ID.
func (b *builder) enumerateMaxPaths(sinks []string, pred map[string][]string, weightLower func(string) int64, states map[string]*pathState) [][]string {
	var examples [][]string
	var walk func(id string, chain []string)
	walk = func(id string, chain []string) {
		if len(examples) >= b.limits.MaxExamplePaths {
			return
		}
		next := append([]string{id}, chain...)
		ps := pred[id]
		if len(ps) == 0 {
			examples = append(examples, slices.Clone(reversed(next)))
			return
		}
		extended := false
		for _, p := range ps {
			if states[p].bestLower+weightLower(id) != states[id].bestLower {
				continue
			}
			extended = true
			walk(p, next)
			if len(examples) >= b.limits.MaxExamplePaths {
				return
			}
		}
		if !extended {
			examples = append(examples, slices.Clone(reversed(next)))
		}
	}
	for _, sink := range sinks {
		if len(examples) >= b.limits.MaxExamplePaths {
			break
		}
		walk(sink, nil)
	}
	return examples
}

func reversed(in []string) []string {
	out := make([]string, len(in))
	for i := range in {
		out[len(in)-1-i] = in[i]
	}
	return out
}

func lexLess(a, c []string) bool {
	n := min(len(a), len(c))
	for i := range n {
		if a[i] != c[i] {
			return a[i] < c[i]
		}
	}
	return len(a) < len(c)
}

func (b *builder) pathHasInconsistentEdge(path []string) bool {
	for i := 0; i+1 < len(path); i++ {
		for _, kind := range []EdgeKind{EdgeDepends, EdgeStepsOrder, EdgeRetrySequence, EdgeGap, EdgeInferredTemporal} {
			if e, ok := b.edges[edgeKey{from: path[i], to: path[i+1], kind: kind}]; ok && e.Certainty == CertaintyInconsistent {
				return true
			}
		}
	}
	return false
}

func (b *builder) workflowNotes(wfStarted, wfFinished time.Time) []string {
	seen := map[string]bool{}
	var notes []string
	add := func(note string) {
		if !seen[note] {
			seen[note] = true
			notes = append(notes, note)
		}
	}
	if !wfStarted.IsZero() && !wfFinished.IsZero() && !wfFinished.Before(wfStarted) {
		add(fmt.Sprintf("workflow observed interval %s to %s (%s); scope intervals bound but do not enter path weights", wfStarted.Format(time.RFC3339Nano), wfFinished.Format(time.RFC3339Nano), wfFinished.Sub(wfStarted)))
	}
	if b.unknownActivities > 0 {
		add(fmt.Sprintf("%d activity interval(s) lack timestamp evidence and are bounded rather than measured", b.unknownActivities))
	}
	if b.inconsistentActs > 0 {
		add(fmt.Sprintf("%d activity interval(s) contain contradictory timestamps", b.inconsistentActs))
	}
	if b.inconsistentEdges > 0 {
		add(fmt.Sprintf("%d causal edge(s) have inconsistent timestamp ordering and carry no precise claim", b.inconsistentEdges))
	}
	for _, note := range b.notes {
		add(note)
	}
	return notes
}
