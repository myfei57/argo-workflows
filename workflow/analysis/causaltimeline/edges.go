package causaltimeline

import (
	"fmt"
	"slices"
	"strings"
	"time"

	wfv1 "github.com/argoproj/argo-workflows/v4/pkg/apis/workflow/v1alpha1"
	"github.com/argoproj/argo-workflows/v4/workflow/common"
)

func (b *builder) addEdge(from, to string, kind EdgeKind, certainty Certainty, evidence Evidence) {
	if from == "" || to == "" || from == to {
		return
	}
	key := edgeKey{from: from, to: to, kind: kind}
	if _, exists := b.edges[key]; exists {
		return
	}
	if len(b.edges) >= b.limits.MaxEdges {
		if n := len(b.truncations); n > 0 && b.truncations[n-1].What == "causal edges" {
			b.truncations[n-1].Dropped++
		} else {
			b.truncations = append(b.truncations, Truncation{What: "causal edges", Limit: b.limits.MaxEdges, Dropped: 1})
		}
		return
	}
	e := &CausalEdge{From: from, To: to, Kind: kind, Certainty: certainty, Evidence: evidence}
	b.edges[key] = e
	b.edgeOrder = append(b.edgeOrder, key)
	if certainty == CertaintyInconsistent {
		b.inconsistentEdges++
	}
}

// orderingCertainty checks an edge against the observed timestamps of its
// endpoints. Missing endpoints stay Unknown; an observed overlap or clock
// regression makes the ordering Inconsistent rather than silently accepted.
func orderingCertainty(from, to *Activity) Certainty {
	if from.FinishedAt == nil || to.StartedAt == nil {
		return CertaintyUnknown
	}
	if from.FinishedAt.After(*to.StartedAt) {
		return CertaintyInconsistent
	}
	return CertaintyKnown
}

// buildCausalEdges derives every non-containment edge. Containment lives in
// Activity.ScopeID; edges here represent causal ordering only.
func (b *builder) buildCausalEdges() {
	b.buildRetryEdges()
	b.buildStepsEdges()
	b.buildDependsEdges()
	b.buildInferredTemporalEdges()

	incomingTo := make(map[string][]*CausalEdge, len(b.edges))
	for _, key := range b.edgeOrder {
		e := b.edges[key]
		incomingTo[e.To] = append(incomingTo[e.To], e)
	}
	b.incomingTo = incomingTo
}

// buildRetryEdges chains consecutive attempts of every Retry scope.
func (b *builder) buildRetryEdges() {
	for _, id := range b.order {
		scope := b.acts[id]
		if scope.Kind != KindRetryScope {
			continue
		}
		attempts := slices.Clone(b.kidsByScope[scope.ID])
		slices.SortStableFunc(attempts, func(a, c *Activity) int {
			ai, aok := attemptIndex(a.NodeName)
			ci, cok := attemptIndex(c.NodeName)
			if aok && cok && ai != ci {
				return ai - ci
			}
			return compareActivities(a, c)
		})
		for i := 0; i+1 < len(attempts); i++ {
			prev, next := attempts[i], attempts[i+1]
			evidence := Evidence{
				Source:   EvidenceRetrySequence,
				Field:    fmt.Sprintf("status.nodes[%q].children", scope.NodeID),
				NodeName: scope.NodeName,
				Detail:   fmt.Sprintf("retry attempt %q precedes attempt %q", prev.NodeName, next.NodeName),
			}
			for _, from := range b.terminals(prev) {
				for _, to := range b.entries(next) {
					b.addEdge(from.ID, to.ID, EdgeRetrySequence, orderingCertainty(from, to), evidence)
				}
			}
		}
	}
}

// buildStepsEdges chains consecutive StepGroups of every Steps scope; steps
// inside one group run in parallel and get no ordering edges.
func (b *builder) buildStepsEdges() {
	for _, id := range b.order {
		scope := b.acts[id]
		if scope.Kind != KindStepsScope {
			continue
		}
		groups := slices.Clone(b.kidsByScope[scope.ID])
		slices.SortStableFunc(groups, func(a, c *Activity) int {
			ai, aok := stepGroupIndex(a.NodeName)
			ci, cok := stepGroupIndex(c.NodeName)
			if aok && cok && ai != ci {
				return ai - ci
			}
			return compareActivities(a, c)
		})
		for i := 0; i+1 < len(groups); i++ {
			prev, next := groups[i], groups[i+1]
			evidence := Evidence{
				Source:   EvidenceSpecStepsOrder,
				Field:    fmt.Sprintf("status.nodes[%q].children", scope.NodeID),
				NodeName: scope.NodeName,
				Detail:   fmt.Sprintf("Steps group %q precedes group %q", prev.NodeName, next.NodeName),
			}
			for _, from := range b.terminals(prev) {
				for _, to := range b.entries(next) {
					b.addEdge(from.ID, to.ID, EdgeStepsOrder, orderingCertainty(from, to), evidence)
				}
			}
		}
	}
}

// stepGroupIndex parses the trailing "[i]" group index of a StepGroup name.
func stepGroupIndex(name string) (int, bool) {
	open := strings.LastIndex(name, "[")
	if open < 0 || !strings.HasSuffix(name, "]") {
		return 0, false
	}
	var n int
	if _, err := fmt.Sscanf(name[open+1:len(name)-1], "%d", &n); err != nil {
		return 0, false
	}
	return n, true
}

// buildDependsEdges resolves DAG task dependencies from the recorded spec and
// keeps only the references whose observed task result actually supports the
// executed branch.
func (b *builder) buildDependsEdges() {
	if b.templateLookup == nil {
		return
	}
	for _, id := range b.order {
		scope := b.acts[id]
		if scope.Kind != KindDAGScope {
			continue
		}
		scopeNode := b.nodes[id]
		tmpl := b.templateLookup(scopeNode.TemplateName)
		if tmpl == nil || tmpl.DAG == nil {
			b.notes = append(b.notes, fmt.Sprintf("DAG scope %q has no resolvable spec template %q; temporal inference only", scope.NodeName, scopeNode.TemplateName))
			continue
		}
		for _, task := range tmpl.DAG.Tasks {
			b.buildTaskDependsEdges(scope, tmpl, task)
		}
	}
}

func (b *builder) buildTaskDependsEdges(scope *Activity, tmpl *wfv1.Template, task wfv1.DAGTask) {
	targetName := scope.NodeName + "." + task.Name
	targetNode, ok := b.byName[targetName]
	if !ok {
		return
	}
	target, ok := b.acts[targetNode.ID]
	if !ok {
		// The node was dropped by an activity cap: no edge can be retained.
		return
	}
	expr := dependsExpression(tmpl, task)
	if expr == "" {
		return
	}

	observed := make(map[string]observedResult)
	for _, ref := range mustParseRefs(expr) {
		if _, seen := observed[ref.Task]; seen {
			continue
		}
		observed[ref.Task] = b.observeTask(scope.NodeName, ref.Task)
	}

	supporters, softUnknown := supportersOf(expr, observed)
	if target.Phase == wfv1.NodeOmitted || target.Phase == wfv1.NodeSkipped {
		target.Evidence = append(target.Evidence, Evidence{
			Source:   EvidenceSpecDepends,
			Field:    "spec.templates[*].dag.tasks[*].depends",
			NodeName: target.NodeName,
			Detail:   fmt.Sprintf("node did not execute; depends expression %q evaluated against observed results (soft-unknown=%t)", expr, softUnknown),
		})
		return
	}
	if len(supporters) == 0 {
		b.notes = append(b.notes, fmt.Sprintf("task %q ran but no observed result satisfies depends %q; causal support recorded as unknown", target.NodeName, expr))
		return
	}

	for _, ref := range supporters {
		sourceNode, ok := b.byName[scope.NodeName+"."+ref.Task]
		if !ok {
			continue
		}
		source, ok := b.acts[sourceNode.ID]
		if !ok {
			continue
		}
		fromLeaves := b.supportingTerminals(source, ref)
		toLeaves := b.entries(target)
		evidence := Evidence{
			Source:   EvidenceSpecDepends,
			Field:    "spec.templates[*].dag.tasks[*].depends",
			NodeName: source.NodeName,
			Detail: fmt.Sprintf(
				"depends %q: observed %s phase %s satisfies %s; OR disjuncts without observed support are not linked",
				expr, ref.Task, sourceNode.Phase, refString(ref),
			),
		}
		for _, from := range fromLeaves {
			for _, to := range toLeaves {
				b.addEdge(from.ID, to.ID, EdgeDepends, orderingCertainty(from, to), evidence)
			}
		}
	}
}

// supportingTerminals resolves a dependency reference to the leaves that
// actually provide the cited result. Aggregate qualifiers on a task group
// resolve to the matching item leaves instead of the whole group.
func (b *builder) supportingTerminals(source *Activity, ref depRef) []*Activity {
	leaves := b.terminals(source)
	if ref.Result != common.TaskResultAnySucceeded && ref.Result != common.TaskResultAllFailed {
		return leaves
	}
	want := wfv1.NodeSucceeded
	if ref.Result == common.TaskResultAllFailed {
		want = wfv1.NodeFailed
	}
	var filtered []*Activity
	for _, leaf := range leaves {
		if leaf.Phase == want {
			filtered = append(filtered, leaf)
		}
	}
	if len(filtered) == 0 {
		return leaves
	}
	return filtered
}

// observedResult captures the real task result vocabulary derivable from the
// status of one dependency node.
type observedResult struct {
	exists       bool
	phase        wfv1.NodePhase
	daemoned     bool
	isGroup      bool
	anySucceeded bool
	allFailed    bool
}

func (b *builder) observeTask(scopeName, taskName string) observedResult {
	node, ok := b.byName[scopeName+"."+taskName]
	if !ok {
		return observedResult{}
	}
	o := observedResult{
		exists:   true,
		phase:    node.Phase,
		daemoned: node.IsDaemoned() && node.Phase != wfv1.NodePending,
		isGroup:  node.Type == wfv1.NodeTypeTaskGroup,
	}
	if o.isGroup {
		failed := 0
		for _, childID := range node.Children {
			child, ok := b.nodes[childID]
			if !ok || child.Phase == wfv1.NodeSkipped || child.Phase == wfv1.NodeOmitted {
				continue
			}
			if child.Phase == wfv1.NodeSucceeded {
				o.anySucceeded = true
			}
			if child.Phase == wfv1.NodeFailed {
				failed++
			}
		}
		o.allFailed = failed > 0 && failed == len(nonSkippedChildren(b.nodes, node))
	}
	return o
}

func nonSkippedChildren(nodes wfv1.Nodes, node *wfv1.NodeStatus) []wfv1.NodeStatus {
	var out []wfv1.NodeStatus
	for _, childID := range node.Children {
		child, ok := nodes[childID]
		if ok && child.Phase != wfv1.NodeSkipped && child.Phase != wfv1.NodeOmitted {
			out = append(out, child)
		}
	}
	return out
}

func (o observedResult) matches(result common.TaskResult) bool {
	if !o.exists {
		return false
	}
	switch result {
	case "":
		return o.phase == wfv1.NodeSucceeded || o.phase == wfv1.NodeSkipped || o.daemoned
	case common.TaskResultSucceeded:
		return o.phase == wfv1.NodeSucceeded
	case common.TaskResultFailed:
		return o.phase == wfv1.NodeFailed
	case common.TaskResultErrored:
		return o.phase == wfv1.NodeError
	case common.TaskResultSkipped:
		return o.phase == wfv1.NodeSkipped
	case common.TaskResultOmitted:
		return o.phase == wfv1.NodeOmitted
	case common.TaskResultDaemoned:
		return o.daemoned
	case common.TaskResultAnySucceeded:
		return o.isGroup && o.anySucceeded
	case common.TaskResultAllFailed:
		return o.isGroup && o.allFailed
	default:
		return false
	}
}

// buildInferredTemporalEdges adds skyline predecessor edges inside DAG scopes
// whose spec cannot be resolved. It only connects activities whose timestamps
// fully evidence the ordering; it never guesses an edge without timestamps.
func (b *builder) buildInferredTemporalEdges() {
	for _, id := range b.order {
		scope := b.acts[id]
		if scope.Kind != KindDAGScope {
			continue
		}
		scopeNode := b.nodes[id]
		if b.templateLookup != nil {
			if tmpl := b.templateLookup(scopeNode.TemplateName); tmpl != nil && tmpl.DAG != nil {
				continue
			}
		}
		type unit struct {
			entry    *Activity
			terminal *Activity
		}
		var units []unit
		for _, kid := range b.kidsByScope[scope.ID] {
			entries := b.entries(kid)
			terminals := b.terminals(kid)
			if len(entries) == 0 || len(terminals) == 0 {
				continue
			}
			units = append(units, unit{entry: entries[0], terminal: terminals[0]})
		}
		for _, target := range units {
			if target.entry.StartedAt == nil {
				continue
			}
			var skyline []*Activity
			var bestFinish *time.Time
			for _, source := range units {
				if source.terminal.ID == target.entry.ID || source.terminal.FinishedAt == nil {
					continue
				}
				if source.terminal.FinishedAt.After(*target.entry.StartedAt) {
					continue
				}
				if bestFinish == nil || source.terminal.FinishedAt.After(*bestFinish) {
					t := *source.terminal.FinishedAt
					bestFinish = &t
					skyline = []*Activity{source.terminal}
				} else if source.terminal.FinishedAt.Equal(*bestFinish) {
					skyline = append(skyline, source.terminal)
				}
			}
			for _, from := range skyline {
				evidence := Evidence{
					Source:   EvidenceInferredTemporal,
					Field:    "status.nodes[*].startedAt/finishedAt",
					NodeName: from.NodeName,
					Detail:   fmt.Sprintf("inferred predecessor of %q from timestamp ordering; no spec evidence available", target.entry.NodeName),
				}
				b.addEdge(from.ID, target.entry.ID, EdgeInferredTemporal, CertaintyKnown, evidence)
			}
		}
	}
}

// dependsExpression returns the effective depends expression for a task,
// expanding legacy dependencies through the same grammar as validation.
func dependsExpression(tmpl *wfv1.Template, task wfv1.DAGTask) string {
	if task.Depends != "" {
		return task.Depends
	}
	if len(task.Dependencies) == 0 {
		return ""
	}
	var groups []string
	for _, depName := range task.Dependencies {
		var depTask *wfv1.DAGTask
		for i := range tmpl.DAG.Tasks {
			if tmpl.DAG.Tasks[i].Name == depName {
				depTask = &tmpl.DAG.Tasks[i]
				break
			}
		}
		var continueOn *wfv1.ContinueOn
		if depTask != nil {
			continueOn = depTask.ContinueOn
		}
		groups = append(groups, common.ExpandDependency(depName, continueOn, func(name string) string { return name }))
	}
	return strings.Join(groups, " && ")
}

func refString(ref depRef) string {
	if ref.Result == "" {
		return ref.Task
	}
	return ref.Task + "." + string(ref.Result)
}
