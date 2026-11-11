package causaltimeline

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	wfv1 "github.com/argoproj/argo-workflows/v4/pkg/apis/workflow/v1alpha1"
)

// TestAnalyze_DiamondParallel covers a hand-verifiable diamond: A fans out to
// B and C, D joins both. B starts two seconds after A finishes, and the
// analyzer must attribute that interval to dependency wait rather than hide
// it or double count it.
func TestAnalyze_DiamondParallel(t *testing.T) {
	f := newFixture(t)
	f.addDAGTemplate("root",
		wfv1.DAGTask{Name: "A", Template: "pod"},
		wfv1.DAGTask{Name: "B", Template: "pod", Dependencies: []string{"A"}},
		wfv1.DAGTask{Name: "C", Template: "pod", Dependencies: []string{"A"}},
		wfv1.DAGTask{Name: "D", Template: "pod", Dependencies: []string{"B", "C"}},
	)
	f.setWorkflowInterval(30)
	f.addNode("test-wf", wfv1.NodeTypeDAG, wfv1.NodeSucceeded, new(0), new(30), "test-wf.A", "test-wf.B", "test-wf.C", "test-wf.D")
	f.setTemplateName("test-wf", "root")
	f.addNode("test-wf.A", wfv1.NodeTypePod, wfv1.NodeSucceeded, new(0), new(10))
	f.addNode("test-wf.B", wfv1.NodeTypePod, wfv1.NodeSucceeded, new(12), new(20))
	f.addNode("test-wf.C", wfv1.NodeTypePod, wfv1.NodeSucceeded, new(10), new(15))
	f.addNode("test-wf.D", wfv1.NodeTypePod, wfv1.NodeSucceeded, new(20), new(30))

	tl := f.analyze()

	// All four declared dependencies are present, evidenced and ordered.
	for _, pair := range [][2]string{{"test-wf.A", "test-wf.B"}, {"test-wf.A", "test-wf.C"}, {"test-wf.B", "test-wf.D"}, {"test-wf.C", "test-wf.D"}} {
		edges := edgesBetween(tl, pair[0], pair[1])
		require.Len(t, edges, 1, "expected one edge %s -> %s", pair[0], pair[1])
		assert.Equal(t, EdgeDepends, edges[0].Kind)
		assert.Equal(t, CertaintyKnown, edges[0].Certainty)
		assert.Equal(t, EvidenceSpecDepends, edges[0].Evidence.Source)
	}

	// The two-second interval is an evidenced dependency wait, not an
	// unattributed gap and not a zero.
	wait := findGap(t, tl, KindDependencyWait)
	require.NotNil(t, wait.StartedAt)
	require.NotNil(t, wait.FinishedAt)
	assert.Equal(t, 2*time.Second, wait.LowerBound)
	assert.Equal(t, f.at(10), *wait.StartedAt)
	assert.Equal(t, f.at(12), *wait.FinishedAt)
	assert.Equal(t, CertaintyKnown, wait.Certainty)

	// Hand-verifiable longest path: A(10) + wait(2) + B(8) + D(10) = 30s.
	assert.Equal(t, 30*time.Second, tl.LongestPath.LowerBoundDuration)
	names := pathNodeNames(t, tl, tl.LongestPath.LowerPath)
	assert.Equal(t, []string{"test-wf.A", "DependencyWait", "test-wf.B", "test-wf.D"}, names)

	// The parallel B/C overlap is reported as evidence, not silently merged.
	assert.Contains(t, tl.Notes, "sibling activities \"test-wf.C\" and \"test-wf.B\" overlap inside scope \"test-wf\"; no precise gap can be derived")

	// Independent reference built from hand-entered weights and edges.
	ref := newRefGraph()
	ref.node("A", 10*time.Second)
	ref.node("wait", 2*time.Second)
	ref.node("B", 8*time.Second)
	ref.node("C", 5*time.Second)
	ref.node("D", 10*time.Second)
	ref.edge("A", "wait")
	ref.edge("wait", "B")
	ref.edge("A", "C")
	ref.edge("B", "D")
	ref.edge("C", "D")
	refMax := ref.longest()
	assert.Equal(t, refMax, tl.LongestPath.LowerBoundDuration)

	// Every returned example path really weighs the claimed maximum.
	for _, example := range tl.LongestPath.ExamplePaths {
		assert.Equal(t, tl.LongestPath.LowerBoundDuration, pathWeight(tl, example))
	}
	assertNoDoubleCounting(t, tl)
	assert.False(t, tl.LongestPath.IsResourceMakespan)
}

// TestAnalyze_EqualLengthPaths checks the compressed tie representation: two
// equally long diamond branches must both be countable and finitely
// exemplified rather than expanded without bound.
func TestAnalyze_EqualLengthPaths(t *testing.T) {
	f := newFixture(t)
	f.addDAGTemplate("root",
		wfv1.DAGTask{Name: "A", Template: "pod"},
		wfv1.DAGTask{Name: "B", Template: "pod", Dependencies: []string{"A"}},
		wfv1.DAGTask{Name: "C", Template: "pod", Dependencies: []string{"A"}},
		wfv1.DAGTask{Name: "D", Template: "pod", Dependencies: []string{"B", "C"}},
	)
	f.setWorkflowInterval(30)
	f.addNode("test-wf", wfv1.NodeTypeDAG, wfv1.NodeSucceeded, new(0), new(30), "test-wf.A", "test-wf.B", "test-wf.C", "test-wf.D")
	f.setTemplateName("test-wf", "root")
	f.addNode("test-wf.A", wfv1.NodeTypePod, wfv1.NodeSucceeded, new(0), new(10))
	f.addNode("test-wf.B", wfv1.NodeTypePod, wfv1.NodeSucceeded, new(10), new(20))
	f.addNode("test-wf.C", wfv1.NodeTypePod, wfv1.NodeSucceeded, new(10), new(20))
	f.addNode("test-wf.D", wfv1.NodeTypePod, wfv1.NodeSucceeded, new(20), new(30))

	tl := f.analyze()
	assert.Equal(t, 30*time.Second, tl.LongestPath.LowerBoundDuration)
	assert.GreaterOrEqual(t, tl.LongestPath.EqualLengthPathCount, 2)
	assert.False(t, tl.LongestPath.PathCountTruncated)
	assert.GreaterOrEqual(t, len(tl.LongestPath.ExamplePaths), 2)

	joined := map[string]bool{}
	for _, example := range tl.LongestPath.ExamplePaths {
		names := pathNodeNames(t, tl, example)
		joined[names[1]] = true
		assert.Equal(t, 30*time.Second, pathWeight(tl, example))
	}
	assert.True(t, joined["test-wf.B"])
	assert.True(t, joined["test-wf.C"])
}

// TestAnalyze_NestedDAG checks target ancestor semantics through a nested DAG
// invocation and proves enclosing scope durations never enter path weight.
func TestAnalyze_NestedDAG(t *testing.T) {
	f := newFixture(t)
	f.addDAGTemplate("root", wfv1.DAGTask{Name: "X", Template: "inner"})
	f.addDAGTemplate("inner",
		wfv1.DAGTask{Name: "P", Template: "pod"},
		wfv1.DAGTask{Name: "Q", Template: "pod", Dependencies: []string{"P"}},
	)
	f.setWorkflowInterval(28)
	f.addNode("test-wf", wfv1.NodeTypeDAG, wfv1.NodeSucceeded, new(0), new(28), "test-wf.X")
	f.setTemplateName("test-wf", "root")
	f.addNode("test-wf.X", wfv1.NodeTypeDAG, wfv1.NodeSucceeded, new(2), new(28), "test-wf.X.P", "test-wf.X.Q")
	f.setTemplateName("test-wf.X", "inner")
	f.setOutboundNodes("test-wf.X", "test-wf.X.Q")
	f.addNode("test-wf.X.P", wfv1.NodeTypePod, wfv1.NodeSucceeded, new(2), new(12))
	f.addNode("test-wf.X.Q", wfv1.NodeTypePod, wfv1.NodeSucceeded, new(12), new(28))

	tl := f.analyze()

	xAct := findByNodeName(t, tl, "test-wf.X")
	assert.True(t, xAct.Enclosing)
	assert.Zero(t, xAct.LowerBound)
	require.NotNil(t, xAct.StartedAt)
	assert.Equal(t, f.at(2), *xAct.StartedAt)

	// Outbound-node resolution points the terminal at Q, not at the scope.
	terms := leafDescendants(t, tl, xAct)
	names := make([]string, 0, len(terms))
	for _, term := range terms {
		names = append(names, term.NodeName)
	}
	assert.ElementsMatch(t, []string{"test-wf.X.P", "test-wf.X.Q"}, names)

	// White-box check of target ancestor semantics through OutboundNodes.
	b := newBuilder(f.wf.Name, f.wf.Status.Nodes, DefaultLimits)
	b.templateLookup = f.wf.GetTemplateByName
	b.buildActivities()
	outbound := b.terminals(b.acts[xAct.ID])
	require.Len(t, outbound, 1)
	assert.Equal(t, "test-wf.X.Q", outbound[0].NodeName)

	// Leading unattributed 2s gap + P(10) + Q(16) = 28s; scope durations
	// themselves contribute nothing.
	unknownGap := findGap(t, tl, KindUnknownGap)
	assert.Equal(t, 2*time.Second, unknownGap.LowerBound)
	assert.Equal(t, 28*time.Second, tl.LongestPath.LowerBoundDuration)

	ref := newRefGraph()
	ref.node("gap", 2*time.Second)
	ref.node("P", 10*time.Second)
	ref.node("Q", 16*time.Second)
	ref.edge("gap", "P")
	ref.edge("P", "Q")
	refMax := ref.longest()
	assert.Equal(t, refMax, tl.LongestPath.LowerBoundDuration)
	assertNoDoubleCounting(t, tl)
}

// TestAnalyze_Retry covers retry attempt expansion, evidenced backoff and the
// rule that a retry scope's total duration is not added on top of attempts.
func TestAnalyze_Retry(t *testing.T) {
	f := newFixture(t)
	f.addDAGTemplate("root", wfv1.DAGTask{Name: "R", Template: "pod"})
	f.setWorkflowInterval(12)
	f.addNode("test-wf", wfv1.NodeTypeDAG, wfv1.NodeSucceeded, new(0), new(12), "test-wf.R")
	f.setTemplateName("test-wf", "root")
	f.addNode("test-wf.R", wfv1.NodeTypeRetry, wfv1.NodeFailed, new(0), new(12), "test-wf.R(0)", "test-wf.R(1)")
	f.addNode("test-wf.R(0)", wfv1.NodeTypePod, wfv1.NodeFailed, new(0), new(5))
	f.addNode("test-wf.R(1)", wfv1.NodeTypePod, wfv1.NodeSucceeded, new(8), new(12))

	tl := f.analyze()

	retry := findByNodeName(t, tl, "test-wf.R")
	assert.True(t, retry.Enclosing)
	assert.Zero(t, retry.LowerBound)

	backoff := findGap(t, tl, KindRetryBackoff)
	assert.Equal(t, 3*time.Second, backoff.LowerBound)
	assert.Equal(t, EvidenceRetrySequence, backoff.Evidence[0].Source)

	chain := edgesBetween(tl, "test-wf.R(0)", "test-wf.R(1)")
	require.Len(t, chain, 1)
	assert.Equal(t, EdgeRetrySequence, chain[0].Kind)

	// 5s failed attempt + 3s evidenced backoff + 4s successful attempt.
	assert.Equal(t, 12*time.Second, tl.LongestPath.LowerBoundDuration)
	names := pathNodeNames(t, tl, tl.LongestPath.LowerPath)
	assert.Equal(t, []string{"test-wf.R(0)", "RetryBackoff", "test-wf.R(1)"}, names)

	ref := newRefGraph()
	ref.node("a0", 5*time.Second)
	ref.node("backoff", 3*time.Second)
	ref.node("a1", 4*time.Second)
	ref.edge("a0", "backoff")
	ref.edge("backoff", "a1")
	refMax := ref.longest()
	assert.Equal(t, refMax, tl.LongestPath.LowerBoundDuration)
	assertNoDoubleCounting(t, tl)
}

// TestAnalyze_RetryMissingTimestamp ensures backoff without endpoint evidence
// becomes a bounded unknown segment instead of a fabricated precise duration.
func TestAnalyze_RetryMissingTimestamp(t *testing.T) {
	f := newFixture(t)
	f.addDAGTemplate("root", wfv1.DAGTask{Name: "R", Template: "pod"})
	f.setWorkflowInterval(12)
	f.addNode("test-wf", wfv1.NodeTypeDAG, wfv1.NodeSucceeded, new(0), new(12), "test-wf.R")
	f.setTemplateName("test-wf", "root")
	f.addNode("test-wf.R", wfv1.NodeTypeRetry, wfv1.NodeSucceeded, new(0), new(12), "test-wf.R(0)", "test-wf.R(1)")
	f.addNode("test-wf.R(0)", wfv1.NodeTypePod, wfv1.NodeFailed, new(0), new(5))
	// Second attempt carries no timestamps at all.
	second := f.addNode("test-wf.R(1)", wfv1.NodeTypePod, wfv1.NodeSucceeded, nil, nil)
	assert.Equal(t, wfv1.NodeSucceeded, second.Phase)

	tl := f.analyze()

	attempt1 := findByNodeName(t, tl, "test-wf.R(1)")
	assert.Equal(t, CertaintyUnknown, attempt1.Certainty)
	require.NotNil(t, attempt1.UpperBound, "sole unbounded segment takes the scope residual as upper bound")
	assert.Equal(t, 7*time.Second, *attempt1.UpperBound)
	assert.Zero(t, attempt1.LowerBound)
	for _, a := range tl.Activities {
		assert.NotContains(t, []ActivityKind{KindRetryBackoff, KindDependencyWait, KindUnknownGap}, a.Kind)
	}

	// Lower bound counts only evidenced time; the upper bound stays finite
	// thanks to the enclosing retry interval, and never claims precision.
	assert.Equal(t, 5*time.Second, tl.LongestPath.LowerBoundDuration)
	require.NotNil(t, tl.LongestPath.UpperBoundDuration)
	assert.False(t, tl.LongestPath.UpperUnbounded)
	assert.Equal(t, 12*time.Second, *tl.LongestPath.UpperBoundDuration)
	assert.LessOrEqual(t, tl.LongestPath.LowerBoundDuration, *tl.LongestPath.UpperBoundDuration)
}

// TestAnalyze_ClockRegression marks an impossible finished-before-start
// interval Inconsistent and never reports a negative or fabricated duration.
func TestAnalyze_ClockRegression(t *testing.T) {
	f := newFixture(t)
	f.addDAGTemplate("root", wfv1.DAGTask{Name: "R", Template: "pod"})
	f.setWorkflowInterval(12)
	f.addNode("test-wf", wfv1.NodeTypeDAG, wfv1.NodeSucceeded, new(0), new(12), "test-wf.R")
	f.setTemplateName("test-wf", "root")
	f.addNode("test-wf.R", wfv1.NodeTypeRetry, wfv1.NodeSucceeded, new(0), new(12), "test-wf.R(0)", "test-wf.R(1)")
	f.addNode("test-wf.R(0)", wfv1.NodeTypePod, wfv1.NodeFailed, new(8), new(5))
	f.addNode("test-wf.R(1)", wfv1.NodeTypePod, wfv1.NodeSucceeded, new(8), new(12))

	tl := f.analyze()
	attempt0 := findByNodeName(t, tl, "test-wf.R(0)")
	assert.Equal(t, CertaintyInconsistent, attempt0.Certainty)
	assert.GreaterOrEqual(t, attempt0.LowerBound, time.Duration(0))
	require.NotNil(t, attempt0.UpperBound)
	assert.GreaterOrEqual(t, *attempt0.UpperBound, time.Duration(0))
	assert.GreaterOrEqual(t, tl.InconsistentActivities, 1)
}

// TestAnalyze_OrBranches verifies that OR depends only link ancestors whose
// observed result actually supports the executed branch, while omitted joins
// gain no execution edges.
func TestAnalyze_OrBranches(t *testing.T) {
	t.Run("omitted join records no support edge", func(t *testing.T) {
		f := newFixture(t)
		f.addDAGTemplate("root",
			wfv1.DAGTask{Name: "A", Template: "pod"},
			wfv1.DAGTask{Name: "B", Template: "pod"},
			wfv1.DAGTask{Name: "J", Template: "pod", Depends: "A.Succeeded || B.Failed"},
		)
		f.setWorkflowInterval(5)
		f.addNode("test-wf", wfv1.NodeTypeDAG, wfv1.NodeSucceeded, new(0), new(5), "test-wf.A", "test-wf.B", "test-wf.J")
		f.setTemplateName("test-wf", "root")
		f.addNode("test-wf.A", wfv1.NodeTypePod, wfv1.NodeFailed, new(0), new(5))
		f.addNode("test-wf.B", wfv1.NodeTypePod, wfv1.NodeSucceeded, new(0), new(5))
		f.addNode("test-wf.J", wfv1.NodeTypeSkipped, wfv1.NodeOmitted, nil, nil)

		tl := f.analyze()
		join := findByNodeName(t, tl, "test-wf.J")
		assert.Equal(t, KindSkipped, join.Kind)
		assert.Zero(t, join.LowerBound)
		assert.Empty(t, edgesBetween(tl, "test-wf.A", "test-wf.J"))
		assert.Empty(t, edgesBetween(tl, "test-wf.B", "test-wf.J"))
		require.NotEmpty(t, join.Evidence)
		assert.Equal(t, EvidenceSpecDepends, join.Evidence[len(join.Evidence)-1].Source)
	})

	t.Run("executed join keeps only the supporting ancestor", func(t *testing.T) {
		f := newFixture(t)
		f.addDAGTemplate("root",
			wfv1.DAGTask{Name: "A", Template: "pod"},
			wfv1.DAGTask{Name: "B", Template: "pod"},
			wfv1.DAGTask{Name: "J", Template: "pod", Depends: "A.Succeeded || B.Failed"},
		)
		f.setWorkflowInterval(10)
		f.addNode("test-wf", wfv1.NodeTypeDAG, wfv1.NodeSucceeded, new(0), new(10), "test-wf.A", "test-wf.B", "test-wf.J")
		f.setTemplateName("test-wf", "root")
		f.addNode("test-wf.A", wfv1.NodeTypePod, wfv1.NodeFailed, new(0), new(5))
		f.addNode("test-wf.B", wfv1.NodeTypePod, wfv1.NodeFailed, new(0), new(5))
		f.addNode("test-wf.J", wfv1.NodeTypePod, wfv1.NodeSucceeded, new(5), new(10))

		tl := f.analyze()
		assert.Empty(t, edgesBetween(tl, "test-wf.A", "test-wf.J"), "A.Succeeded is false; A must not be force-linked")
		supporting := edgesBetween(tl, "test-wf.B", "test-wf.J")
		require.Len(t, supporting, 1)
		assert.Equal(t, EdgeDepends, supporting[0].Kind)
	})
}

// TestAnalyze_MissingTimestamps checks unknown intervals never collapse into
// claimed zero durations and propagate an unbounded upper result.
func TestAnalyze_MissingTimestamps(t *testing.T) {
	f := newFixture(t)
	f.addDAGTemplate("root",
		wfv1.DAGTask{Name: "A", Template: "pod"},
		wfv1.DAGTask{Name: "B", Template: "pod", Dependencies: []string{"A"}},
	)
	f.addNode("test-wf", wfv1.NodeTypeDAG, wfv1.NodeSucceeded, nil, nil, "test-wf.A", "test-wf.B")
	f.setTemplateName("test-wf", "root")
	f.addNode("test-wf.A", wfv1.NodeTypePod, wfv1.NodeSucceeded, nil, nil)
	f.addNode("test-wf.B", wfv1.NodeTypePod, wfv1.NodeSucceeded, new(5), new(10))

	tl := f.analyze()
	a := findByNodeName(t, tl, "test-wf.A")
	assert.Equal(t, CertaintyUnknown, a.Certainty)
	assert.Nil(t, a.UpperBound)
	assert.Zero(t, a.LowerBound)

	edge := edgesBetween(tl, "test-wf.A", "test-wf.B")
	require.Len(t, edge, 1)
	assert.Equal(t, CertaintyUnknown, edge[0].Certainty)

	assert.Equal(t, 5*time.Second, tl.LongestPath.LowerBoundDuration)
	assert.True(t, tl.LongestPath.UpperUnbounded)
	assert.Nil(t, tl.LongestPath.UpperBoundDuration)
}

// TestAnalyze_StatusOnlyInferredEdges ensures status-only analysis falls back
// to timestamp-inferred edges and never invents spec dependencies.
func TestAnalyze_StatusOnlyInferredEdges(t *testing.T) {
	f := newFixture(t)
	// No templates recorded: only status evidence exists.
	f.addNode("test-wf", wfv1.NodeTypeDAG, wfv1.NodeSucceeded, new(0), new(30), "test-wf.A", "test-wf.B", "test-wf.C", "test-wf.D")
	f.addNode("test-wf.A", wfv1.NodeTypePod, wfv1.NodeSucceeded, new(0), new(10))
	f.addNode("test-wf.B", wfv1.NodeTypePod, wfv1.NodeSucceeded, new(10), new(20))
	f.addNode("test-wf.C", wfv1.NodeTypePod, wfv1.NodeSucceeded, new(10), new(20))
	f.addNode("test-wf.D", wfv1.NodeTypePod, wfv1.NodeSucceeded, new(20), new(30))

	tl, err := AnalyzeStatus("test-wf", &f.wf.Status, DefaultLimits)
	require.NoError(t, err)

	for _, e := range tl.Edges {
		assert.NotEqual(t, EdgeDepends, e.Kind)
	}
	inferred := 0
	for _, e := range tl.Edges {
		if e.Kind == EdgeInferredTemporal {
			inferred++
			assert.Equal(t, EvidenceInferredTemporal, e.Evidence.Source)
		}
	}
	assert.Positive(t, inferred)
	assert.Equal(t, 30*time.Second, tl.LongestPath.LowerBoundDuration)
}

// TestAnalyze_Steps checks sequential group ordering and parallel steps inside
// a group: only group boundaries gain causal edges.
func TestAnalyze_Steps(t *testing.T) {
	f := newFixture(t)
	f.setWorkflowInterval(20)
	f.addNode("test-wf", wfv1.NodeTypeSteps, wfv1.NodeSucceeded, new(0), new(20), "test-wf[0]", "test-wf[1]")
	f.addNode("test-wf[0]", wfv1.NodeTypeStepGroup, wfv1.NodeSucceeded, new(0), new(10), "test-wf[0].s1")
	f.addNode("test-wf[1]", wfv1.NodeTypeStepGroup, wfv1.NodeSucceeded, new(10), new(20), "test-wf[1].s2a", "test-wf[1].s2b")
	f.addNode("test-wf[0].s1", wfv1.NodeTypePod, wfv1.NodeSucceeded, new(0), new(10))
	f.addNode("test-wf[1].s2a", wfv1.NodeTypePod, wfv1.NodeSucceeded, new(10), new(20))
	f.addNode("test-wf[1].s2b", wfv1.NodeTypePod, wfv1.NodeSucceeded, new(12), new(18))

	tl := f.analyze()
	orderEdges := 0
	for _, e := range tl.Edges {
		if e.Kind == EdgeStepsOrder {
			orderEdges++
			assert.Equal(t, EvidenceSpecStepsOrder, e.Evidence.Source)
		}
	}
	assert.Positive(t, orderEdges)
	assert.Empty(t, edgesBetween(tl, "test-wf[1].s2a", "test-wf[1].s2b"), "parallel steps must not be ordered")
	// s1(10s) -> s2a(10s) is the deterministic longest causal chain; s2b's
	// 6s runs in parallel and is never summed with it.
	assert.Equal(t, 20*time.Second, tl.LongestPath.LowerBoundDuration)
	assertNoDoubleCounting(t, tl)
}

// TestAnalyze_TaskGroup expands an with-items task group into its item leaves
// while keeping the group itself a zero-weight enclosing interval.
func TestAnalyze_TaskGroup(t *testing.T) {
	f := newFixture(t)
	f.addDAGTemplate("root", wfv1.DAGTask{Name: "G", Template: "pod"})
	f.setWorkflowInterval(10)
	f.addNode("test-wf", wfv1.NodeTypeDAG, wfv1.NodeSucceeded, new(0), new(10), "test-wf.G")
	f.setTemplateName("test-wf", "root")
	f.addNode("test-wf.G", wfv1.NodeTypeTaskGroup, wfv1.NodeSucceeded, new(0), new(10), "test-wf.G(0:x)", "test-wf.G(1:y)")
	f.setOutboundNodes("test-wf.G", "test-wf.G(0:x)", "test-wf.G(1:y)")
	f.addNode("test-wf.G(0:x)", wfv1.NodeTypePod, wfv1.NodeSucceeded, new(0), new(10))
	f.addNode("test-wf.G(1:y)", wfv1.NodeTypePod, wfv1.NodeSucceeded, new(0), new(10))

	tl := f.analyze()
	g := findByNodeName(t, tl, "test-wf.G")
	assert.True(t, g.Enclosing)
	assert.Zero(t, g.LowerBound)
	terms := leafDescendants(t, tl, g)
	assert.Len(t, terms, 2)
	assertNoDoubleCounting(t, tl)
}

// TestAnalyze_DoesNotMutateStatus guarantees the archived input is unchanged.
func TestAnalyze_DoesNotMutateStatus(t *testing.T) {
	f := newFixture(t)
	f.addDAGTemplate("root",
		wfv1.DAGTask{Name: "A", Template: "pod"},
		wfv1.DAGTask{Name: "B", Template: "pod", Dependencies: []string{"A"}},
	)
	f.setWorkflowInterval(10)
	f.addNode("test-wf", wfv1.NodeTypeDAG, wfv1.NodeSucceeded, new(0), new(10), "test-wf.A", "test-wf.B")
	f.setTemplateName("test-wf", "root")
	f.addNode("test-wf.A", wfv1.NodeTypePod, wfv1.NodeSucceeded, new(0), new(5))
	f.addNode("test-wf.B", wfv1.NodeTypePod, wfv1.NodeSucceeded, new(5), new(10))

	before, err := json.Marshal(f.wf)
	require.NoError(t, err)

	_, err = AnalyzeWorkflow(f.wf, DefaultLimits)
	require.NoError(t, err)
	// A second analysis must be identical too: output is deterministic and
	// carries no wall-clock-dependent content.
	tl1, err := AnalyzeWorkflow(f.wf, DefaultLimits)
	require.NoError(t, err)
	tl2, err := AnalyzeWorkflow(f.wf, DefaultLimits)
	require.NoError(t, err)
	assert.Equal(t, tl1, tl2)

	after, err := json.Marshal(f.wf)
	require.NoError(t, err)
	assert.JSONEq(t, string(before), string(after))
}

// TestAnalyze_Limits verifies hard caps are honored and reported.
func TestAnalyze_Limits(t *testing.T) {
	f := newFixture(t)
	f.addDAGTemplate("root",
		wfv1.DAGTask{Name: "A", Template: "pod"},
		wfv1.DAGTask{Name: "B", Template: "pod", Dependencies: []string{"A"}},
		wfv1.DAGTask{Name: "C", Template: "pod", Dependencies: []string{"A"}},
	)
	f.setWorkflowInterval(10)
	f.addNode("test-wf", wfv1.NodeTypeDAG, wfv1.NodeSucceeded, new(0), new(10), "test-wf.A", "test-wf.B", "test-wf.C")
	f.setTemplateName("test-wf", "root")
	f.addNode("test-wf.A", wfv1.NodeTypePod, wfv1.NodeSucceeded, new(0), new(4))
	f.addNode("test-wf.B", wfv1.NodeTypePod, wfv1.NodeSucceeded, new(4), new(10))
	f.addNode("test-wf.C", wfv1.NodeTypePod, wfv1.NodeSucceeded, new(4), new(10))

	tl, err := AnalyzeWorkflow(f.wf, Limits{MaxActivities: 3, MaxEdges: 1, MaxExamplePaths: 1, MaxPathCount: 1})
	require.NoError(t, err)
	assert.LessOrEqual(t, len(tl.Activities), 3)
	assert.LessOrEqual(t, len(tl.Edges), 1)
	assert.NotEmpty(t, tl.Truncated)
	for _, tr := range tl.Truncated {
		assert.Positive(t, tr.Limit)
	}
	// Tie-breakers stay unique and bounded within the retained set.
	seen := map[int]bool{}
	for _, a := range tl.Activities {
		assert.False(t, seen[a.TieBreaker])
		seen[a.TieBreaker] = true
	}
}

// TestAnalyze_NilInputs covers the documented error contracts.
func TestAnalyze_NilInputs(t *testing.T) {
	_, err := AnalyzeWorkflow(nil, DefaultLimits)
	require.Error(t, err)
	_, err = AnalyzeStatus("x", nil, DefaultLimits)
	require.Error(t, err)
}

// TestAnalyze_InconsistentDependsEdge marks a declared edge whose timestamps
// overlap impossibly as Inconsistent instead of trusting the ordering.
func TestAnalyze_InconsistentDependsEdge(t *testing.T) {
	f := newFixture(t)
	f.addDAGTemplate("root",
		wfv1.DAGTask{Name: "A", Template: "pod"},
		wfv1.DAGTask{Name: "B", Template: "pod", Dependencies: []string{"A"}},
	)
	f.setWorkflowInterval(10)
	f.addNode("test-wf", wfv1.NodeTypeDAG, wfv1.NodeSucceeded, new(0), new(10), "test-wf.A", "test-wf.B")
	f.setTemplateName("test-wf", "root")
	// B starts at 5 while A only finishes at 10: the declared order is
	// contradicted by the timestamps.
	f.addNode("test-wf.A", wfv1.NodeTypePod, wfv1.NodeSucceeded, new(0), new(10))
	f.addNode("test-wf.B", wfv1.NodeTypePod, wfv1.NodeSucceeded, new(5), new(10))

	tl := f.analyze()
	edges := edgesBetween(tl, "test-wf.A", "test-wf.B")
	require.Len(t, edges, 1)
	assert.Equal(t, CertaintyInconsistent, edges[0].Certainty)
	assert.Positive(t, tl.InconsistentEdges)
	assert.True(t, tl.LongestPath.ContainsInconsistentEdge)
}

// TestAnalyze_CyclicDependsGuarded keeps the analyzer finite when the recorded
// spec contradicts itself with a dependency cycle: one edge is excluded and
// flagged, and the longest path is still produced.
func TestAnalyze_CyclicDependsGuarded(t *testing.T) {
	f := newFixture(t)
	f.addDAGTemplate("root",
		wfv1.DAGTask{Name: "A", Template: "pod", Depends: "B.Succeeded"},
		wfv1.DAGTask{Name: "B", Template: "pod", Depends: "A.Succeeded"},
	)
	f.setWorkflowInterval(10)
	f.addNode("test-wf", wfv1.NodeTypeDAG, wfv1.NodeSucceeded, new(0), new(10), "test-wf.A", "test-wf.B")
	f.setTemplateName("test-wf", "root")
	f.addNode("test-wf.A", wfv1.NodeTypePod, wfv1.NodeSucceeded, new(0), new(5))
	f.addNode("test-wf.B", wfv1.NodeTypePod, wfv1.NodeSucceeded, new(5), new(10))

	tl := f.analyze()
	assert.Positive(t, tl.InconsistentEdges)
	assert.NotContains(t, tl.LongestPath.Notes, "panic")
	assert.NotNil(t, tl.LongestPath.LowerPath)
}

// TestAnalyze_TaskGroupAnySucceeded resolves an aggregate qualifier to the
// specific item leaves providing the observed result.
func TestAnalyze_TaskGroupAnySucceeded(t *testing.T) {
	f := newFixture(t)
	f.addDAGTemplate("root",
		wfv1.DAGTask{Name: "G", Template: "pod"},
		wfv1.DAGTask{Name: "J", Template: "pod", Depends: "G.AnySucceeded"},
	)
	f.setWorkflowInterval(10)
	f.addNode("test-wf", wfv1.NodeTypeDAG, wfv1.NodeSucceeded, new(0), new(10), "test-wf.G", "test-wf.J")
	f.setTemplateName("test-wf", "root")
	f.addNode("test-wf.G", wfv1.NodeTypeTaskGroup, wfv1.NodeSucceeded, new(0), new(5), "test-wf.G(0:ok)", "test-wf.G(1:bad)")
	f.addNode("test-wf.G(0:ok)", wfv1.NodeTypePod, wfv1.NodeSucceeded, new(0), new(5))
	f.addNode("test-wf.G(1:bad)", wfv1.NodeTypePod, wfv1.NodeFailed, new(0), new(5))
	f.addNode("test-wf.J", wfv1.NodeTypePod, wfv1.NodeSucceeded, new(5), new(10))

	tl := f.analyze()
	// Only the succeeded item supports AnySucceeded; the failed item is not
	// force-linked.
	supporting := edgesBetween(tl, "test-wf.G(0:ok)", "test-wf.J")
	require.Len(t, supporting, 1)
	assert.Equal(t, EdgeDepends, supporting[0].Kind)
	assert.Empty(t, edgesBetween(tl, "test-wf.G(1:bad)", "test-wf.J"))
}

// TestAnalyze_TrailingGapAndStepsWait covers the trailing unattributed gap and
// the Steps group-order wait classification.
func TestAnalyze_TrailingGapAndStepsWait(t *testing.T) {
	t.Run("trailing unattributed gap", func(t *testing.T) {
		f := newFixture(t)
		f.addDAGTemplate("root", wfv1.DAGTask{Name: "A", Template: "pod"})
		f.setWorkflowInterval(10)
		f.addNode("test-wf", wfv1.NodeTypeDAG, wfv1.NodeSucceeded, new(0), new(10), "test-wf.A")
		f.setTemplateName("test-wf", "root")
		f.addNode("test-wf.A", wfv1.NodeTypePod, wfv1.NodeSucceeded, new(0), new(5))

		tl := f.analyze()
		gap := findGap(t, tl, KindUnknownGap)
		assert.Equal(t, 5*time.Second, gap.LowerBound)
		assert.Equal(t, EvidenceStatusTimestamp, gap.Evidence[0].Source)
	})

	t.Run("steps group wait", func(t *testing.T) {
		f := newFixture(t)
		f.setWorkflowInterval(20)
		f.addNode("test-wf", wfv1.NodeTypeSteps, wfv1.NodeSucceeded, new(0), new(20), "test-wf[0]", "test-wf[1]")
		f.addNode("test-wf[0]", wfv1.NodeTypeStepGroup, wfv1.NodeSucceeded, new(0), new(10), "test-wf[0].s1")
		f.addNode("test-wf[1]", wfv1.NodeTypeStepGroup, wfv1.NodeSucceeded, new(12), new(20), "test-wf[1].s2")
		f.addNode("test-wf[0].s1", wfv1.NodeTypePod, wfv1.NodeSucceeded, new(0), new(10))
		f.addNode("test-wf[1].s2", wfv1.NodeTypePod, wfv1.NodeSucceeded, new(12), new(20))

		tl := f.analyze()
		wait := findGap(t, tl, KindDependencyWait)
		assert.Equal(t, 2*time.Second, wait.LowerBound)
		assert.Equal(t, EvidenceSpecStepsOrder, wait.Evidence[0].Source)
		assert.Equal(t, 20*time.Second, tl.LongestPath.LowerBoundDuration)
	})
}

// TestSupportersOf directly exercises the OR/AND evidence evaluator, including
// three-valued unknown handling.
func TestSupportersOf(t *testing.T) {
	observed := map[string]observedResult{
		"A": {exists: true, phase: wfv1.NodeFailed},
		"B": {exists: true, phase: wfv1.NodeSucceeded},
		"C": {exists: true, phase: wfv1.NodeFailed},
	}
	supporters, unknown := supportersOf("A.Succeeded || B.Failed", observed)
	assert.False(t, unknown)
	require.Empty(t, supporters, "neither disjunct is satisfied")

	supporters, unknown = supportersOf("A.Failed || B.Succeeded", observed)
	assert.False(t, unknown)
	names := []string{}
	for _, s := range supporters {
		names = append(names, s.Task)
	}
	assert.ElementsMatch(t, []string{"A", "B"}, names)

	supporters, unknown = supportersOf("A.Succeeded && C.Failed", observed)
	assert.False(t, unknown)
	assert.Empty(t, supporters)

	// Missing dependency evidence is unknown, not proven false.
	supporters, unknown = supportersOf("Z.Succeeded", observed)
	assert.True(t, unknown)
	assert.Empty(t, supporters)

	// Negation is supported by the observed fact that makes it true.
	supporters, unknown = supportersOf("!A.Failed", map[string]observedResult{
		"A": {exists: true, phase: wfv1.NodeSucceeded},
	})
	assert.False(t, unknown)
	require.Len(t, supporters, 1)
	assert.Equal(t, "A", supporters[0].Task)

	// Parenthesized AND/OR mixture only returns the satisfied disjunct.
	supporters, unknown = supportersOf("(A.Succeeded && B.Succeeded) || C.Failed", observed)
	assert.False(t, unknown)
	require.Len(t, supporters, 1)
	assert.Equal(t, "C", supporters[0].Task)
}

// leafDescendants collects all leaf descendants of a scope from the public
// activity list; outbound-specific filtering is asserted white-box in the
// nested DAG test because outbound IDs are not part of the public output.
func leafDescendants(t *testing.T, tl *WorkflowCausalTimeline, scope *Activity) []*Activity {
	t.Helper()
	byID := map[string]*Activity{}
	for i := range tl.Activities {
		byID[tl.Activities[i].ID] = &tl.Activities[i]
	}
	var walk func(a *Activity) []*Activity
	walk = func(a *Activity) []*Activity {
		if !a.Enclosing {
			return []*Activity{a}
		}
		var out []*Activity
		for _, candidate := range tl.Activities {
			if candidate.ScopeID == a.ID {
				out = append(out, walk(&candidate)...)
			}
		}
		if len(out) == 0 {
			return []*Activity{a}
		}
		return out
	}
	return walk(byID[scope.ID])
}
