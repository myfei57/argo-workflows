package dag

import (
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	wfv1 "github.com/argoproj/argo-workflows/v4/pkg/apis/workflow/v1alpha1"
)

// --- Test helpers -----------------------------------------------------------

// explainDAG builds an evaluator over a DAG template whose boundary is "dag".
func explainDAG(t *testing.T, wf *wfv1.Workflow, tasks ...wfv1.DAGTask) *DAGEvaluator {
	t.Helper()
	return NewDAGEvaluator(wf, createDAGTemplate(tasks), "", "dag")
}

// pendingDAGWorkflow returns an empty-node workflow: every task is open.
func pendingDAGWorkflow() *wfv1.Workflow {
	return newTestWorkflow("test-wf")
}

// addGroup builds a Running TaskGroup node with the given item nodes and,
// optionally, a dangling child ID that is absent from the node map (the
// pruned/offloaded unknown-member case).
func addGroup(t *testing.T, wf *wfv1.Workflow, name string, items map[string]wfv1.NodePhase, missing bool) {
	t.Helper()
	group := addTaskGroupParent(t, wf, name)
	for itemName, phase := range items {
		addTaskGroupChild(t, wf, group, name+itemName, phase, nil)
	}
	if missing {
		updated := wf.Status.Nodes[group.ID]
		updated.Children = append(updated.Children, wf.NodeID(name+"(9:missing)"))
		wf.Status.Nodes.Set(testCtx(t), updated.ID, updated)
	}
}

func outcomePhase(o ExplainOutcome) (wfv1.NodePhase, bool) {
	switch o {
	case ExplainSucceeded:
		return wfv1.NodeSucceeded, false
	case ExplainFailed:
		return wfv1.NodeFailed, false
	case ExplainErrored:
		return wfv1.NodeError, false
	case ExplainSkipped:
		return wfv1.NodeSkipped, false
	case ExplainOmitted:
		return wfv1.NodeOmitted, false
	case ExplainDaemoned:
		return wfv1.NodeRunning, true
	default:
		return "", false
	}
}

// applyConstraints materializes a solution on a deep copy of wf, so the real
// evaluator can be run against it. Unknown-member constraints cannot be
// materialized as concrete nodes and are skipped; those solutions are
// validated exhaustively instead.
func applyConstraints(t *testing.T, wf *wfv1.Workflow, constraints []OutcomeConstraint) *wfv1.Workflow {
	t.Helper()
	copyWF := wf.DeepCopy()
	for _, c := range constraints {
		if c.Member == UnknownMember {
			continue
		}
		phase, daemon := outcomePhase(c.Outcome)
		nodeName := "dag." + c.Task
		if c.Member != "" {
			nodeName = "dag." + c.Task + c.Member
		}
		node := wfv1.NodeStatus{
			ID:    copyWF.NodeID(nodeName),
			Name:  nodeName,
			Phase: phase,
			Type:  wfv1.NodeTypePod,
		}
		if daemon {
			daemoned := true
			node.Daemoned = &daemoned
		}
		copyWF.Status.Nodes.Set(testCtx(t), node.ID, node)
		if c.Member != "" {
			// A fulfilled group node for the dependant to read items from.
			groupName := "dag." + c.Task
			groupID := copyWF.NodeID(groupName)
			group, err := copyWF.Status.Nodes.Get(groupID)
			require.NoError(t, err)
			group.Phase = wfv1.NodeSucceeded
			if !slices.Contains(group.Children, node.ID) {
				group.Children = append(group.Children, node.ID)
			}
			copyWF.Status.Nodes.Set(testCtx(t), groupID, *group)
		}
	}
	return copyWF
}

// solutionKey renders a solution as a deterministic string set for comparison.
func solutionKey(constraints []OutcomeConstraint) string {
	keys := make([]string, len(constraints))
	for i, c := range constraints {
		keys[i] = constraintKey(c)
	}
	sort.Strings(keys)
	return fmt.Sprint(keys)
}

func solutionSet(solutions [][]OutcomeConstraint) []string {
	keys := make([]string, len(solutions))
	for i, s := range solutions {
		keys[i] = solutionKey(s)
	}
	sort.Strings(keys)
	return keys
}

// assertMutexAndAntichain checks the finite-domain invariants: at most one
// outcome per (task, member) inside a solution, and no solution contains
// another.
func assertMutexAndAntichain(t *testing.T, solutions [][]OutcomeConstraint) {
	t.Helper()
	sets := make([]map[string]bool, len(solutions))
	for i, solution := range solutions {
		seen := make(map[string]bool)
		set := make(map[string]bool)
		for _, c := range solution {
			key := c.Task + "/" + c.Member
			assert.False(t, seen[key], "two outcomes assigned to %s: %s", key, solutionKey(solution))
			seen[key] = true
			set[constraintKey(c)] = true
		}
		sets[i] = set
	}
	for i, smaller := range sets {
		for j, larger := range sets {
			if i == j || len(smaller) >= len(larger) {
				continue
			}
			contains := true
			for k := range smaller {
				if !larger[k] {
					contains = false
					break
				}
			}
			assert.False(t, contains, "solution %v is contained by another solution: antichain violated", solutions[i])
		}
	}
}

// assertProven independently re-proves every emitted solution against the
// real expression evaluator: every completion of unbound variables must make
// the expression true, and removing any single constraint must leave a
// completion that makes it false.
func assertProven(t *testing.T, e *DAGEvaluator, m *explainModel, solutions [][]OutcomeConstraint) {
	t.Helper()
	require.NotEmpty(t, m.logic)
	for _, solution := range solutions {
		partial := map[int]int{}
		for _, c := range solution {
			varIdx := -1
			for i, v := range m.vars {
				if v.task == c.Task && v.member == c.Member {
					varIdx = i
					break
				}
			}
			require.GreaterOrEqual(t, varIdx, 0, "constraint has no variable: %+v", c)
			valueIdx := -1
			for i, value := range m.vars[varIdx].values {
				if value.outcome == c.Outcome {
					valueIdx = i
					break
				}
			}
			require.GreaterOrEqual(t, valueIdx, 0, "outcome not in domain: %+v", c)
			partial[varIdx] = valueIdx
		}

		// All full assignments extending partial must satisfy the expression.
		forEachFullAssignment(m, func(assignment []int) {
			consistent := true
			for varIdx, valueIdx := range partial {
				if assignment[varIdx] != valueIdx {
					consistent = false
					break
				}
			}
			if !consistent {
				return
			}
			ok, err := e.evalBool(m.logic, m.scope(assignment))
			require.NoError(t, err)
			assert.True(t, ok, "solution %s does not guarantee readiness", solutionKey(solution))
		})

		// Each constraint must be necessary.
		for varIdx := range partial {
			reduced := maps.Clone(partial)
			delete(reduced, varIdx)
			foundFalse := false
			forEachFullAssignment(m, func(assignment []int) {
				if foundFalse {
					return
				}
				for v, valueIdx := range reduced {
					if assignment[v] != valueIdx {
						return
					}
				}
				ok, err := e.evalBool(m.logic, m.scope(assignment))
				require.NoError(t, err)
				if !ok {
					foundFalse = true
				}
			})
			assert.True(t, foundFalse, "constraint %d of %s is redundant", varIdx, solutionKey(solution))
		}
	}
}

// forEachFullAssignment enumerates every Cartesian-product assignment of the
// model's variables. It is the test oracle's own enumeration.
func forEachFullAssignment(m *explainModel, visit func([]int)) {
	assignment := make([]int, len(m.vars))
	var walk func(int)
	walk = func(idx int) {
		if idx == len(m.vars) {
			visit(slices.Clone(assignment))
			return
		}
		for valueIdx := range m.vars[idx].values {
			assignment[idx] = valueIdx
			walk(idx + 1)
		}
	}
	walk(0)
}

// referenceMinimalSolutions is an independent brute-force oracle: it builds
// the full truth table over the small finite domains and then tests every
// partial assignment directly, keeping the minimal guaranteeing ones.
func referenceMinimalSolutions(t *testing.T, e *DAGEvaluator, m *explainModel) [][]OutcomeConstraint {
	t.Helper()
	var falsifying [][]int
	forEachFullAssignment(m, func(assignment []int) {
		ok, err := e.evalBool(m.logic, m.scope(assignment))
		require.NoError(t, err)
		if !ok {
			falsifying = append(falsifying, slices.Clone(assignment))
		}
	})

	var accepted []map[int]int
	// Independent subset test (deliberately not reusing containsPartial):
	// the oracle must cross-check the production antichain logic on its own.
	subsetOfAnAccepted := func(partial map[int]int) bool {
		for _, acceptedPartial := range accepted {
			subset := true
			for varIdx, valueIdx := range acceptedPartial {
				if acceptedValue, ok := partial[varIdx]; !ok || acceptedValue != valueIdx {
					subset = false
					break
				}
			}
			if subset {
				return true
			}
		}
		return false
	}

	for size := 1; size <= len(m.vars); size++ {
		forEachVarCombination(len(m.vars), size, func(combo []int) bool {
			forEachValueCombo(m.vars, combo, func(partial map[int]int) bool {
				if subsetOfAnAccepted(partial) {
					return false
				}
				for _, row := range falsifying {
					extends := true
					for varIdx, valueIdx := range partial {
						if row[varIdx] != valueIdx {
							extends = false
							break
						}
					}
					if extends {
						return false
					}
				}
				accepted = append(accepted, maps.Clone(partial))
				return false
			})
			return false
		})
	}
	return m.toConstraints(accepted)
}

// assertExplanationMatchesOracle runs the full brute-force reference and
// compares the complete minimal antichain.
func assertExplanationMatchesOracle(t *testing.T, e *DAGEvaluator, tasks []wfv1.DAGTask, target string, wf *wfv1.Workflow) {
	t.Helper()
	evaluator := NewDAGEvaluator(wf, createDAGTemplate(tasks), "", "dag")
	explanation, err := evaluator.ExplainBlockedTask(testCtx(t), target, DefaultExplainBudget())
	require.NoError(t, err)
	require.False(t, explanation.Incomplete, "reference case must stay within budget: %s", explanation.Reason)

	deps, err := evaluator.tasks.GetDependencies(testCtx(t), target)
	require.NoError(t, err)
	model, _, _, _, err := evaluator.buildExplainModel(deps)
	require.NoError(t, err)
	model.logic = evaluator.tasks.GetDependsLogic(testCtx(t), target)

	want := solutionSet(referenceMinimalSolutions(t, evaluator, model))
	got := solutionSet(explanation.Solutions)
	assert.Equal(t, want, got)
	assertMutexAndAntichain(t, explanation.Solutions)
	assertProven(t, evaluator, model, explanation.Solutions)
}

// --- Tests ------------------------------------------------------------------

// A single open task has a six-value mutually-exclusive domain: the minimal
// solution for "A.Succeeded" is one assignment, never a set of independent
// booleans.
func TestExplainBlockedTask_MutexOutcomeDomain(t *testing.T) {
	wf := pendingDAGWorkflow()
	tasks := []wfv1.DAGTask{
		{Name: "A", Template: "t"},
		{Name: "C", Template: "t", Depends: "A.Succeeded"},
	}
	e := explainDAG(t, wf, tasks...)

	explanation, err := e.ExplainBlockedTask(testCtx(t), "C", DefaultExplainBudget())
	require.NoError(t, err)
	assert.Equal(t, JudgmentWaiting, explanation.CurrentJudgment)
	assert.Contains(t, explanation.WaitingOn, "A")
	assert.Equal(t, []string{"A"}, explanation.Pending)
	assert.Empty(t, explanation.Terminal)
	require.Len(t, explanation.Solutions, 1)
	assert.Equal(t, []OutcomeConstraint{{Task: "A", Outcome: ExplainSucceeded}}, explanation.Solutions[0])
	assert.False(t, explanation.Unsatisfiable)
	assert.False(t, explanation.Incomplete)
	assertExplanationMatchesOracle(t, e, tasks, "C", wf)
}

// Negation opens every outcome except the negated one; the antichain is five
// single-constraint solutions, not the whole ancestor set.
func TestExplainBlockedTask_Negation(t *testing.T) {
	wf := pendingDAGWorkflow()
	tasks := []wfv1.DAGTask{
		{Name: "A", Template: "t"},
		{Name: "C", Template: "t", Depends: "!A.Failed"},
	}
	e := explainDAG(t, wf, tasks...)

	explanation, err := e.ExplainBlockedTask(testCtx(t), "C", DefaultExplainBudget())
	require.NoError(t, err)
	require.Len(t, explanation.Solutions, 5)
	for _, solution := range explanation.Solutions {
		require.Len(t, solution, 1)
		assert.NotEqual(t, ExplainFailed, solution[0].Outcome)
		assert.Equal(t, "A", solution[0].Task)
	}
	assertExplanationMatchesOracle(t, e, tasks, "C", wf)
}

// OR yields one minimal solution per alternative: listing both ancestors is
// not minimal.
func TestExplainBlockedTask_OrDoesNotListAllAncestors(t *testing.T) {
	wf := pendingDAGWorkflow()
	tasks := []wfv1.DAGTask{
		{Name: "A", Template: "t"},
		{Name: "B", Template: "t"},
		{Name: "C", Template: "t", Depends: "A.Succeeded || B.Succeeded"},
	}
	e := explainDAG(t, wf, tasks...)

	explanation, err := e.ExplainBlockedTask(testCtx(t), "C", DefaultExplainBudget())
	require.NoError(t, err)
	assert.Equal(t, []string{
		solutionKey([]OutcomeConstraint{{Task: "A", Outcome: ExplainSucceeded}}),
		solutionKey([]OutcomeConstraint{{Task: "B", Outcome: ExplainSucceeded}}),
	}, solutionSet(explanation.Solutions))
	for _, solution := range explanation.Solutions {
		assert.Len(t, solution, 1, "OR alternatives are one-constraint solutions")
	}
	assertExplanationMatchesOracle(t, e, tasks, "C", wf)
}

// Negation combined with AND: a fixed terminal dependency is reported as
// immutable and only the open dependency appears in solutions.
func TestExplainBlockedTask_NegationWithTerminalDependency(t *testing.T) {
	wf := pendingDAGWorkflow()
	addNodeToWorkflow(testCtx(t), wf, "dag.A", wfv1.NodeSucceeded)
	tasks := []wfv1.DAGTask{
		{Name: "A", Template: "t"},
		{Name: "B", Template: "t"},
		{Name: "C", Template: "t", Depends: "A.Succeeded && !B.Failed"},
	}
	e := explainDAG(t, wf, tasks...)

	explanation, err := e.ExplainBlockedTask(testCtx(t), "C", DefaultExplainBudget())
	require.NoError(t, err)
	assert.Equal(t, map[string]ExplainOutcome{"A": ExplainSucceeded}, explanation.Terminal)
	assert.Equal(t, []string{"B"}, explanation.Pending)
	require.Len(t, explanation.Solutions, 5)
	for _, solution := range explanation.Solutions {
		require.Len(t, solution, 1)
		assert.Equal(t, "B", solution[0].Task)
		assert.NotEqual(t, ExplainFailed, solution[0].Outcome)
	}
	assertExplanationMatchesOracle(t, e, tasks, "C", wf)
}

// De Morgan-style negation over two ancestors gives single-constraint
// solutions, proving the search handles NOT over an AND rather than emitting
// the ancestor pair.
func TestExplainBlockedTask_NegatedAnd(t *testing.T) {
	wf := pendingDAGWorkflow()
	tasks := []wfv1.DAGTask{
		{Name: "A", Template: "t"},
		{Name: "B", Template: "t"},
		{Name: "C", Template: "t", Depends: "!(A.Failed && B.Failed)"},
	}
	e := explainDAG(t, wf, tasks...)

	explanation, err := e.ExplainBlockedTask(testCtx(t), "C", DefaultExplainBudget())
	require.NoError(t, err)
	// Every non-Failed outcome of either ancestor alone suffices: 10 solutions.
	assert.Len(t, explanation.Solutions, 10)
	for _, solution := range explanation.Solutions {
		assert.Len(t, solution, 1)
	}
	assertExplanationMatchesOracle(t, e, tasks, "C", wf)
}

// Legacy "dependencies" with continueOn.failed expand through the real
// ExpandDependency rule: a Failed dependency is then a valid sufficient
// outcome, while Errored/Omitted are not.
func TestExplainBlockedTask_LegacyDependenciesWithContinueOn(t *testing.T) {
	wf := pendingDAGWorkflow()
	tasks := []wfv1.DAGTask{
		{Name: "A", Template: "t", ContinueOn: &wfv1.ContinueOn{Failed: true}},
		{Name: "C", Template: "t", Dependencies: []string{"A"}},
	}
	e := explainDAG(t, wf, tasks...)

	explanation, err := e.ExplainBlockedTask(testCtx(t), "C", DefaultExplainBudget())
	require.NoError(t, err)
	var outcomes []ExplainOutcome
	for _, solution := range explanation.Solutions {
		require.Len(t, solution, 1)
		outcomes = append(outcomes, solution[0].Outcome)
	}
	assert.ElementsMatch(t, []ExplainOutcome{
		ExplainSucceeded, ExplainSkipped, ExplainDaemoned, ExplainFailed,
	}, outcomes)
	assertExplanationMatchesOracle(t, e, tasks, "C", wf)
}

// A task-group's AnySucceeded follows real member states: the open item must
// Succeed; the constraint is expressed at item level.
func TestExplainBlockedTask_TaskGroupAnySucceeded(t *testing.T) {
	wf := pendingDAGWorkflow()
	addGroup(t, wf, "dag.G", map[string]wfv1.NodePhase{
		"(0:x)": wfv1.NodeRunning,
		"(1:y)": wfv1.NodeFailed,
	}, false)
	tasks := []wfv1.DAGTask{
		{Name: "G", Template: "t", WithItems: []wfv1.Item{{Value: []byte(`"x"`)}, {Value: []byte(`"y"`)}}},
		{Name: "C", Template: "t", Depends: "G.AnySucceeded"},
	}
	e := explainDAG(t, wf, tasks...)

	explanation, err := e.ExplainBlockedTask(testCtx(t), "C", DefaultExplainBudget())
	require.NoError(t, err)
	assert.Equal(t, []string{"G"}, explanation.Pending)
	require.Len(t, explanation.Solutions, 1)
	assert.Equal(t, []OutcomeConstraint{{Task: "G", Member: "(0:x)", Outcome: ExplainSucceeded}}, explanation.Solutions[0])
	assertExplanationMatchesOracle(t, e, tasks, "C", wf)

	// Materializing the solution on a copy makes the real evaluator run C.
	concrete := applyConstraints(t, wf, explanation.Solutions[0])
	concreteEvaluator := NewDAGEvaluator(concrete, createDAGTemplate(tasks), "", "dag")
	result := concreteEvaluator.Evaluate(testCtx(t), "C")
	require.NoError(t, result.Error)
	assert.True(t, result.ShouldRun)
}

// AllFailed needs every open item to Fail: one multi-constraint solution.
func TestExplainBlockedTask_TaskGroupAllFailed(t *testing.T) {
	wf := pendingDAGWorkflow()
	addGroup(t, wf, "dag.G", map[string]wfv1.NodePhase{
		"(0:x)": wfv1.NodeRunning,
		"(1:y)": wfv1.NodePending,
	}, false)
	tasks := []wfv1.DAGTask{
		{Name: "G", Template: "t", WithItems: []wfv1.Item{{Value: []byte(`"x"`)}, {Value: []byte(`"y"`)}}},
		{Name: "C", Template: "t", Depends: "G.AllFailed"},
	}
	e := explainDAG(t, wf, tasks...)

	explanation, err := e.ExplainBlockedTask(testCtx(t), "C", DefaultExplainBudget())
	require.NoError(t, err)
	require.Len(t, explanation.Solutions, 1)
	assert.Equal(t, solutionSet([][]OutcomeConstraint{{
		{Task: "G", Member: "(0:x)", Outcome: ExplainFailed},
		{Task: "G", Member: "(1:y)", Outcome: ExplainFailed},
	}}), solutionSet(explanation.Solutions))
	assertMutexAndAntichain(t, explanation.Solutions)
	assertExplanationMatchesOracle(t, e, tasks, "C", wf)
}

// A Succeeded item makes AllFailed impossible even while other items are open.
func TestExplainBlockedTask_TaskGroupAllFailedUnsat(t *testing.T) {
	wf := pendingDAGWorkflow()
	addGroup(t, wf, "dag.G", map[string]wfv1.NodePhase{
		"(0:x)": wfv1.NodeSucceeded,
		"(1:y)": wfv1.NodeRunning,
	}, false)
	tasks := []wfv1.DAGTask{
		{Name: "G", Template: "t", WithItems: []wfv1.Item{{Value: []byte(`"x"`)}, {Value: []byte(`"y"`)}}},
		{Name: "C", Template: "t", Depends: "G.AllFailed"},
	}
	e := explainDAG(t, wf, tasks...)

	explanation, err := e.ExplainBlockedTask(testCtx(t), "C", DefaultExplainBudget())
	require.NoError(t, err)
	assert.True(t, explanation.Unsatisfiable)
	assert.Empty(t, explanation.Solutions)
	assert.False(t, explanation.Incomplete)
}

// Unknown group members are modeled explicitly, not assumed absent:
// AllFailed stays satisfiable through the unseen-members variable.
func TestExplainBlockedTask_TaskGroupUnknownMembers(t *testing.T) {
	wf := pendingDAGWorkflow()
	addGroup(t, wf, "dag.G", map[string]wfv1.NodePhase{
		"(0:x)": wfv1.NodeFailed,
	}, true)
	tasks := []wfv1.DAGTask{
		{Name: "G", Template: "t", WithItems: []wfv1.Item{{Value: []byte(`"x"`)}}},
		{Name: "C", Template: "t", Depends: "G.AllFailed"},
		{Name: "D", Template: "t", Depends: "G.AnySucceeded"},
	}
	e := explainDAG(t, wf, tasks...)
	assert.Contains(t, e.buildUnknownMembers(t, "C"), "G")

	cExplanation, err := e.ExplainBlockedTask(testCtx(t), "C", DefaultExplainBudget())
	require.NoError(t, err)
	assert.Contains(t, cExplanation.UnknownMembers, "G")
	assert.False(t, cExplanation.Unsatisfiable, "unseen members must not be assumed empty")
	require.NotEmpty(t, cExplanation.Solutions)
	assert.Equal(t, UnknownMember, cExplanation.Solutions[0][0].Member)
	assert.Equal(t, ExplainUnknownFailed, cExplanation.Solutions[0][0].Outcome)

	dExplanation, err := e.ExplainBlockedTask(testCtx(t), "D", DefaultExplainBudget())
	require.NoError(t, err)
	assert.Contains(t, dExplanation.UnknownMembers, "G")
	require.NotEmpty(t, dExplanation.Solutions)
	assert.Equal(t, ExplainUnknownSucceeded, dExplanation.Solutions[0][0].Outcome)
}

// buildUnknownMembers is a small test accessor for the model classification.
func (e *DAGEvaluator) buildUnknownMembers(t *testing.T, target string) []string {
	t.Helper()
	deps, err := e.tasks.GetDependencies(testCtx(t), target)
	require.NoError(t, err)
	_, _, _, unknown, err := e.buildExplainModel(deps)
	require.NoError(t, err)
	return unknown
}

// A group node that exists but has not expanded any item yet must not be
// treated as an empty, hence settled, group.
func TestExplainBlockedTask_UnexpandedTaskGroupIsUnknown(t *testing.T) {
	wf := pendingDAGWorkflow()
	addTaskGroupParent(t, wf, "dag.G")
	tasks := []wfv1.DAGTask{
		{Name: "G", Template: "t", WithItems: []wfv1.Item{{Value: []byte(`"x"`)}, {Value: []byte(`"y"`)}}},
		{Name: "C", Template: "t", Depends: "G.AllFailed"},
	}
	e := explainDAG(t, wf, tasks...)

	explanation, err := e.ExplainBlockedTask(testCtx(t), "C", DefaultExplainBudget())
	require.NoError(t, err)
	assert.Contains(t, explanation.UnknownMembers, "G")
	assert.False(t, explanation.Unsatisfiable)
	require.NotEmpty(t, explanation.Solutions)
	assert.Equal(t, UnknownMember, explanation.Solutions[0][0].Member)
}

// A group with no node at all is entirely unknown: both aggregates stay open.
func TestExplainBlockedTask_UncreatedTaskGroupIsUnknown(t *testing.T) {
	wf := pendingDAGWorkflow()
	tasks := []wfv1.DAGTask{
		{Name: "G", Template: "t", WithItems: []wfv1.Item{{Value: []byte(`"x"`)}}},
		{Name: "C", Template: "t", Depends: "G.AnySucceeded || G.AllFailed"},
	}
	e := explainDAG(t, wf, tasks...)

	explanation, err := e.ExplainBlockedTask(testCtx(t), "C", DefaultExplainBudget())
	require.NoError(t, err)
	assert.Contains(t, explanation.UnknownMembers, "G")
	assert.False(t, explanation.Unsatisfiable)
	assert.NotEmpty(t, explanation.Solutions)
	for _, solution := range explanation.Solutions {
		assert.Equal(t, UnknownMember, solution[0].Member)
	}
}

// A terminal Failed dependency can never satisfy A.Succeeded: an exhaustive,
// non-budgeted no-solution proof.
func TestExplainBlockedTask_Unsatisfiable(t *testing.T) {
	wf := pendingDAGWorkflow()
	addNodeToWorkflow(testCtx(t), wf, "dag.A", wfv1.NodeFailed)
	tasks := []wfv1.DAGTask{
		{Name: "A", Template: "t"},
		{Name: "C", Template: "t", Depends: "A.Succeeded"},
	}
	e := explainDAG(t, wf, tasks...)

	explanation, err := e.ExplainBlockedTask(testCtx(t), "C", DefaultExplainBudget())
	require.NoError(t, err)
	assert.Equal(t, JudgmentOmitted, explanation.CurrentJudgment)
	assert.True(t, explanation.Unsatisfiable)
	assert.Empty(t, explanation.Solutions)
	assert.False(t, explanation.Incomplete)
	assert.Equal(t, map[string]ExplainOutcome{"A": ExplainFailed}, explanation.Terminal)
}

// Already-satisfied conditions report Ready without inventing open work.
func TestExplainBlockedTask_ReadyAndFulfilled(t *testing.T) {
	wf := pendingDAGWorkflow()
	addNodeToWorkflow(testCtx(t), wf, "dag.A", wfv1.NodeSucceeded)
	tasks := []wfv1.DAGTask{
		{Name: "A", Template: "t"},
		{Name: "C", Template: "t", Depends: "A.Succeeded"},
	}
	e := explainDAG(t, wf, tasks...)

	explanation, err := e.ExplainBlockedTask(testCtx(t), "C", DefaultExplainBudget())
	require.NoError(t, err)
	assert.Equal(t, JudgmentReady, explanation.CurrentJudgment)
	assert.Empty(t, explanation.Solutions)
	assert.False(t, explanation.Unsatisfiable)

	// Once the target has its own fulfilled node, depends is no longer the gate.
	addNodeToWorkflow(testCtx(t), wf, "dag.C", wfv1.NodeSucceeded)
	explanation, err = e.ExplainBlockedTask(testCtx(t), "C", DefaultExplainBudget())
	require.NoError(t, err)
	assert.Equal(t, JudgmentFulfilled, explanation.CurrentJudgment)
	assert.NotEmpty(t, explanation.Reason)
}

// Hitting a budget returns Incomplete with a proven subset, never an
// unproven solution.
func TestExplainBlockedTask_BudgetIncomplete(t *testing.T) {
	wf := pendingDAGWorkflow()
	tasks := []wfv1.DAGTask{
		{Name: "A", Template: "t"},
		{Name: "B", Template: "t"},
		{Name: "C", Template: "t", Depends: "A.Succeeded || B.Succeeded"},
	}

	t.Run("solution cap", func(t *testing.T) {
		e := explainDAG(t, wf, tasks...)
		explanation, err := e.ExplainBlockedTask(testCtx(t), "C", ExplainBudget{MaxModels: 1000, MaxCandidates: 1000, MaxSolutions: 1})
		require.NoError(t, err)
		assert.True(t, explanation.Incomplete)
		require.Len(t, explanation.Solutions, 1)
	})

	t.Run("variable cap flags incompleteness but keeps the proof", func(t *testing.T) {
		e := explainDAG(t, wf, tasks...)
		explanation, err := e.ExplainBlockedTask(testCtx(t), "C", ExplainBudget{MaxVariables: 1, MaxModels: 1000, MaxCandidates: 1000, MaxSolutions: 10})
		require.NoError(t, err)
		assert.True(t, explanation.Incomplete)
		assert.Len(t, explanation.Solutions, 2)
	})

	t.Run("model cap truncates enumeration", func(t *testing.T) {
		big := pendingDAGWorkflow()
		bigTasks := []wfv1.DAGTask{
			{Name: "A", Template: "t"}, {Name: "B", Template: "t"},
			{Name: "C", Template: "t"}, {Name: "D", Template: "t"},
			{Name: "E", Template: "t", Depends: "A.Succeeded && B.Succeeded && C.Succeeded && D.Succeeded"},
		}
		e := explainDAG(t, big, bigTasks...)
		explanation, err := e.ExplainBlockedTask(testCtx(t), "E", ExplainBudget{MaxVariables: 32, MaxModels: 50, MaxCandidates: 5000, MaxSolutions: 10})
		require.NoError(t, err)
		assert.True(t, explanation.Incomplete)
		// Whatever subset came back must still be independently provable.
		deps, err := e.tasks.GetDependencies(testCtx(t), "E")
		require.NoError(t, err)
		model, _, _, _, err := e.buildExplainModel(deps)
		require.NoError(t, err)
		model.logic = e.tasks.GetDependsLogic(testCtx(t), "E")
		assertProven(t, e, model, explanation.Solutions)
	})
}

// ExplainBlockedTask is read-only: the workflow nodes are byte-identical
// before and after.
func TestExplainBlockedTask_DoesNotModifyInput(t *testing.T) {
	wf := pendingDAGWorkflow()
	addGroup(t, wf, "dag.G", map[string]wfv1.NodePhase{
		"(0:x)": wfv1.NodeRunning,
		"(1:y)": wfv1.NodeFailed,
	}, true)
	addNodeToWorkflow(testCtx(t), wf, "dag.A", wfv1.NodeSucceeded)
	tasks := []wfv1.DAGTask{
		{Name: "A", Template: "t"},
		{Name: "G", Template: "t", WithItems: []wfv1.Item{{Value: []byte(`"x"`)}, {Value: []byte(`"y"`)}}},
		{Name: "C", Template: "t", Depends: "A && (G.AnySucceeded || G.AllFailed)"},
	}
	before, err := json.Marshal(wf.Status.Nodes)
	require.NoError(t, err)

	e := explainDAG(t, wf, tasks...)
	_, err = e.ExplainBlockedTask(testCtx(t), "C", DefaultExplainBudget())
	require.NoError(t, err)

	after, err := json.Marshal(wf.Status.Nodes)
	require.NoError(t, err)
	assert.JSONEq(t, string(before), string(after))
}

// Error surfaces: unknown task and invalid depends grammar.
func TestExplainBlockedTask_Errors(t *testing.T) {
	wf := pendingDAGWorkflow()
	e := explainDAG(t, wf, wfv1.DAGTask{Name: "A", Template: "t"})

	_, err := e.ExplainBlockedTask(testCtx(t), "missing", DefaultExplainBudget())
	assert.Error(t, err)

	bad := explainDAG(t, pendingDAGWorkflow(),
		wfv1.DAGTask{Name: "A", Template: "t"},
		wfv1.DAGTask{Name: "C", Template: "t", Depends: "A.Bogus"},
	)
	_, err = bad.ExplainBlockedTask(testCtx(t), "C", DefaultExplainBudget())
	assert.Error(t, err)
}

// A larger mixed expression matches the brute-force oracle end to end,
// including the real-evaluator materialization check for every solution.
func TestExplainBlockedTask_MixedExpressionOracleParity(t *testing.T) {
	wf := pendingDAGWorkflow()
	addNodeToWorkflow(testCtx(t), wf, "dag.A", wfv1.NodeFailed)
	tasks := []wfv1.DAGTask{
		{Name: "A", Template: "t"},
		{Name: "B", Template: "t"},
		{Name: "C", Template: "t"},
		{Name: "D", Template: "t", Depends: "A.Errored || (B.Succeeded && !C.Failed)"},
	}
	e := explainDAG(t, wf, tasks...)
	explanation, err := e.ExplainBlockedTask(testCtx(t), "D", DefaultExplainBudget())
	require.NoError(t, err)
	assert.Equal(t, map[string]ExplainOutcome{"A": ExplainFailed}, explanation.Terminal)
	// A is terminally Failed, so A.Errored is impossible: every solution
	// combines B=Succeeded with one non-Failed outcome of C.
	for _, solution := range explanation.Solutions {
		require.Len(t, solution, 2)
	}
	assertExplanationMatchesOracle(t, e, tasks, "D", wf)

	// Every concrete solution unlocks the task under the real evaluator.
	for _, solution := range explanation.Solutions {
		concrete := applyConstraints(t, wf, solution)
		concreteEvaluator := NewDAGEvaluator(concrete, createDAGTemplate(tasks), "", "dag")
		result := concreteEvaluator.Evaluate(testCtx(t), "D")
		require.NoError(t, result.Error, "solution %s", solutionKey(solution))
		assert.True(t, result.ShouldRun, "solution %s did not unlock D", solutionKey(solution))
	}
}
