package causaltimeline

import (
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	wfv1 "github.com/argoproj/argo-workflows/v4/pkg/apis/workflow/v1alpha1"
)

type fixture struct {
	t    *testing.T
	wf   *wfv1.Workflow
	base time.Time
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	wf := &wfv1.Workflow{
		ObjectMeta: metav1.ObjectMeta{Name: "test-wf"},
		Spec: wfv1.WorkflowSpec{
			Templates: []wfv1.Template{podTemplate("pod")},
		},
		Status: wfv1.WorkflowStatus{Nodes: wfv1.Nodes{}},
	}
	return &fixture{t: t, wf: wf, base: base}
}

func podTemplate(name string) wfv1.Template {
	return wfv1.Template{
		Name:      name,
		Container: &corev1.Container{Name: "main", Image: "registry.example/img:latest"},
	}
}

func (f *fixture) at(offset int) time.Time {
	return f.base.Add(time.Duration(offset) * time.Second)
}

// addNode records a status node. Nil timestamps deliberately stay zero so
// tests can exercise missing-evidence paths.
func (f *fixture) addNode(name string, typ wfv1.NodeType, phase wfv1.NodePhase, start, end *int, children ...string) *wfv1.NodeStatus {
	f.t.Helper()
	node := wfv1.NodeStatus{
		ID:          f.wf.NodeID(name),
		Name:        name,
		DisplayName: name,
		Type:        typ,
		Phase:       phase,
	}
	if start != nil {
		node.StartedAt = metav1.Time{Time: f.at(*start)}
	}
	if end != nil {
		node.FinishedAt = metav1.Time{Time: f.at(*end)}
	}
	for _, child := range children {
		node.Children = append(node.Children, f.wf.NodeID(child))
	}
	f.wf.Status.Nodes[node.ID] = node
	return &node
}

func (f *fixture) setWorkflowInterval(end int) {
	f.wf.Status.StartedAt = metav1.Time{Time: f.at(0)}
	f.wf.Status.FinishedAt = metav1.Time{Time: f.at(end)}
}

// setTemplateName updates the stored node's template name; mutating the
// pointer returned by addNode would not reach the value held in the map.
func (f *fixture) setTemplateName(nodeName, templateName string) {
	f.t.Helper()
	id := f.wf.NodeID(nodeName)
	node := f.wf.Status.Nodes[id]
	node.TemplateName = templateName
	f.wf.Status.Nodes[id] = node
}

func (f *fixture) setOutboundNodes(nodeName string, childNames ...string) {
	f.t.Helper()
	id := f.wf.NodeID(nodeName)
	node := f.wf.Status.Nodes[id]
	node.OutboundNodes = nil
	for _, child := range childNames {
		node.OutboundNodes = append(node.OutboundNodes, f.wf.NodeID(child))
	}
	f.wf.Status.Nodes[id] = node
}

func (f *fixture) addDAGTemplate(name string, tasks ...wfv1.DAGTask) {
	f.wf.Spec.Templates = append(f.wf.Spec.Templates, wfv1.Template{
		Name: name,
		DAG:  &wfv1.DAGTemplate{Tasks: tasks},
	})
}

func (f *fixture) analyze() *WorkflowCausalTimeline {
	f.t.Helper()
	tl, err := AnalyzeWorkflow(f.wf, DefaultLimits)
	require.NoError(f.t, err)
	return tl
}

func findByNodeName(t *testing.T, tl *WorkflowCausalTimeline, name string) *Activity {
	t.Helper()
	for i := range tl.Activities {
		if tl.Activities[i].NodeName == name {
			return &tl.Activities[i]
		}
	}
	t.Fatalf("activity for node %q not found", name)
	return nil
}

func findGap(t *testing.T, tl *WorkflowCausalTimeline, kind ActivityKind) *Activity {
	t.Helper()
	for i := range tl.Activities {
		if tl.Activities[i].Kind == kind {
			return &tl.Activities[i]
		}
	}
	t.Fatalf("gap activity of kind %s not found", kind)
	return nil
}

func edgesBetween(tl *WorkflowCausalTimeline, fromName, toName string) []CausalEdge {
	fromID := idByName(tl, fromName)
	toID := idByName(tl, toName)
	var out []CausalEdge
	for _, e := range tl.Edges {
		if e.From == fromID && e.To == toID {
			out = append(out, e)
		}
	}
	return out
}

func idByName(tl *WorkflowCausalTimeline, name string) string {
	for _, a := range tl.Activities {
		if a.NodeName == name {
			return a.ID
		}
	}
	return ""
}

func pathNodeNames(t *testing.T, tl *WorkflowCausalTimeline, path []string) []string {
	t.Helper()
	byID := map[string]Activity{}
	for _, a := range tl.Activities {
		byID[a.ID] = a
	}
	names := make([]string, 0, len(path))
	for _, id := range path {
		a, ok := byID[id]
		require.True(t, ok, "path references unknown activity %q", id)
		if a.NodeName != "" {
			names = append(names, a.NodeName)
		} else {
			names = append(names, string(a.Kind))
		}
	}
	return names
}

// refGraph is an independent longest-path reference: a hand-built weighted
// DAG analyzed by exhaustive DFS, deliberately separate from the production
// dynamic program.
type refGraph struct {
	weights map[string]time.Duration
	adj     map[string][]string
}

func newRefGraph() *refGraph {
	return &refGraph{weights: map[string]time.Duration{}, adj: map[string][]string{}}
}

func (g *refGraph) node(id string, weight time.Duration) {
	g.weights[id] = weight
	if _, ok := g.adj[id]; !ok {
		g.adj[id] = nil
	}
}

func (g *refGraph) edge(from, to string) {
	g.adj[from] = append(g.adj[from], to)
	if _, ok := g.adj[to]; !ok {
		g.adj[to] = nil
	}
}

// longest is the independent reference implementation: exhaustive DFS over a
// hand-built weighted DAG, deliberately different from the production DP.
func (g *refGraph) longest() time.Duration {
	var maxWeight time.Duration

	var walk func(id string, weight time.Duration)
	walk = func(id string, weight time.Duration) {
		weight += g.weights[id]
		if len(g.adj[id]) == 0 {
			maxWeight = max(maxWeight, weight)
			return
		}
		for _, next := range g.adj[id] {
			walk(next, weight)
		}
	}
	roots := make([]string, 0)
	hasIncoming := map[string]bool{}
	for _, nexts := range g.adj {
		for _, n := range nexts {
			hasIncoming[n] = true
		}
	}
	for id := range g.adj {
		if !hasIncoming[id] {
			roots = append(roots, id)
		}
	}
	sort.Strings(roots)
	for _, root := range roots {
		walk(root, 0)
	}
	return maxWeight
}

// assertNoDoubleCounting verifies the structural accounting invariants:
// enclosing scopes carry zero path weight, gap IDs are unique, and the union
// of evidenced intervals directly inside each scope never exceeds the scope's
// own observed interval.
func assertNoDoubleCounting(t *testing.T, tl *WorkflowCausalTimeline) {
	t.Helper()
	byID := map[string]*Activity{}
	kids := map[string][]*Activity{}
	for i := range tl.Activities {
		a := &tl.Activities[i]
		byID[a.ID] = a
		if a.ScopeID != "" {
			kids[a.ScopeID] = append(kids[a.ScopeID], a)
		}
		if a.Enclosing {
			require.Zero(t, a.LowerBound, "enclosing scope %q must not carry path weight", a.NodeName)
		}
	}
	gapSeen := map[string]bool{}
	for _, a := range tl.Activities {
		if a.Kind == KindDependencyWait || a.Kind == KindRetryBackoff || a.Kind == KindUnknownGap {
			require.False(t, gapSeen[a.ID], "gap identity %q duplicated", a.ID)
			gapSeen[a.ID] = true
		}
	}
	type interval struct{ start, end time.Time }
	for scopeID, children := range kids {
		scope := byID[scopeID]
		if scope.StartedAt == nil || scope.FinishedAt == nil {
			continue
		}
		var intervals []interval
		for _, child := range children {
			if child.Enclosing || child.Certainty != CertaintyKnown || child.StartedAt == nil || child.FinishedAt == nil {
				continue
			}
			intervals = append(intervals, interval{start: *child.StartedAt, end: *child.FinishedAt})
		}
		sort.Slice(intervals, func(i, j int) bool { return intervals[i].start.Before(intervals[j].start) })
		var union time.Duration
		var cur *interval
		for _, iv := range intervals {
			if cur == nil {
				cur = &interval{start: iv.start, end: iv.end}
				continue
			}
			if iv.start.After(cur.end) {
				union += cur.end.Sub(cur.start)
				cur = &interval{start: iv.start, end: iv.end}
			} else if iv.end.After(cur.end) {
				cur.end = iv.end
			}
		}
		if cur != nil {
			union += cur.end.Sub(cur.start)
		}
		scopeDur := scope.FinishedAt.Sub(*scope.StartedAt)
		require.LessOrEqual(t, union, scopeDur, "scope %q double counts accounted time", scope.NodeName)
	}
}

// pathWeight sums the evidenced lower-bound durations on a returned path.
func pathWeight(tl *WorkflowCausalTimeline, path []string) time.Duration {
	byID := map[string]*Activity{}
	for i := range tl.Activities {
		byID[tl.Activities[i].ID] = &tl.Activities[i]
	}
	var total time.Duration
	for _, id := range path {
		total += byID[id].LowerBound
	}
	return total
}
