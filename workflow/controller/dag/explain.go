package dag

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"sort"
	"strings"

	wfv1 "github.com/argoproj/argo-workflows/v4/pkg/apis/workflow/v1alpha1"
)

// ExplainOutcome is one mutually-exclusive outcome a still-open task or task
// group item can take. The six task outcomes are the same one-hot vocabulary
// the evaluator's taskResult scope uses: a single task can never be
// Succeeded and Failed at once, so the explanation treats the outcome as a
// finite-domain value rather than eight independently-settable booleans.
type ExplainOutcome string

// The terminal outcomes a task can present to its dependants.
const (
	ExplainSucceeded ExplainOutcome = "Succeeded"
	ExplainFailed    ExplainOutcome = "Failed"
	ExplainErrored   ExplainOutcome = "Errored"
	ExplainSkipped   ExplainOutcome = "Skipped"
	ExplainOmitted   ExplainOutcome = "Omitted"
	ExplainDaemoned  ExplainOutcome = "Daemoned"
)

// Outcomes that only appear on constraints targeting the synthetic unknown
// members variable (OutcomeConstraint.Member == UnknownMember). They describe
// what TaskGroup members absent from the node map (pruned, offloaded, or not
// yet expanded) contribute to the group's aggregates and completed phase.
const (
	// ExplainUnknownNone: no unseen members exist.
	ExplainUnknownNone ExplainOutcome = "UnknownNone"
	// ExplainUnknownSucceeded: unseen members include a Succeeded one and the
	// group completes Succeeded.
	ExplainUnknownSucceeded ExplainOutcome = "UnknownSucceeded"
	// ExplainUnknownSucceededFailed: unseen members include a Succeeded one
	// and the group's completed phase is Failed.
	ExplainUnknownSucceededFailed ExplainOutcome = "UnknownSucceededFailed"
	// ExplainUnknownSucceededErrored: unseen members include a Succeeded one
	// and the group's completed phase is Errored.
	ExplainUnknownSucceededErrored ExplainOutcome = "UnknownSucceededErrored"
	// ExplainUnknownFailed: every member, seen and unseen, Failed.
	ExplainUnknownFailed ExplainOutcome = "UnknownFailed"
	// ExplainUnknownOther: no member Succeeded, not all Failed, group
	// completes Succeeded.
	ExplainUnknownOther ExplainOutcome = "UnknownOther"
	// ExplainUnknownFailedOther: no member Succeeded, not all Failed, group
	// completes Failed.
	ExplainUnknownFailedOther ExplainOutcome = "UnknownFailedOther"
	// ExplainUnknownErrored: the unseen members include an Errored one.
	ExplainUnknownErrored ExplainOutcome = "UnknownErrored"
)

// UnknownMember is the Member value of the synthetic variable standing in for
// TaskGroup members the node map does not currently contain.
const UnknownMember = "*"

// ExplainJudgment is the evaluator's current depends decision for the task.
type ExplainJudgment string

// Current depends judgments, mirroring EvaluationResult.
const (
	// JudgmentReady: the task can run now (its node may already be dispatching).
	JudgmentReady ExplainJudgment = "Ready"
	// JudgmentWaiting: at least one dependency has not finished, so the
	// expression is not decidable yet.
	JudgmentWaiting ExplainJudgment = "Waiting"
	// JudgmentOmitted: every dependency has finished and the expression is
	// false, so the task can never run.
	JudgmentOmitted ExplainJudgment = "Omitted"
	// JudgmentFulfilled: the task's own node exists and is fulfilled; its
	// depends gate is no longer what blocks it.
	JudgmentFulfilled ExplainJudgment = "Fulfilled"
)

// OutcomeConstraint is one assignment in a minimal solution: Task (and, for a
// task-group item, Member) must take Outcome. A solution is a set of these
// that guarantees the depends expression holds whatever the unconstrained
// open tasks do.
type OutcomeConstraint struct {
	// Task is the dependency task name as written in the depends expression.
	Task string
	// Member is empty for a whole task. For a task-group item it is the item
	// name relative to the group ("(0:item)"); UnknownMember marks the
	// synthetic unseen-members variable.
	Member string
	// Outcome is the required finite-domain value.
	Outcome ExplainOutcome
}

// ExplainBudget bounds the explanation search. The zero value is replaced by
// DefaultExplainBudget; every limit is a hard stop, never a reason to emit an
// unproven solution: hitting one sets BlockedTaskExplanation.Incomplete.
type ExplainBudget struct {
	// MaxVariables is the number of finite-domain variables (open tasks plus
	// task-group items and unknown-member variables) the model may contain
	// before the result is flagged Incomplete.
	MaxVariables int
	// MaxModels is the maximum number of full assignments evaluated with the
	// real expression evaluator, across enumeration and revalidation.
	MaxModels int
	// MaxCandidates is the maximum number of partial assignments tested while
	// extracting the minimal-set antichain.
	MaxCandidates int
	// MaxSolutions is the maximum number of minimal solutions returned.
	MaxSolutions int
}

// DefaultExplainBudget is the budget ExplainBlockedTask uses when none is
// supplied. A DAG task's direct dependencies are normally a handful, so this
// covers realistic graphs while keeping worst-case work bounded.
func DefaultExplainBudget() ExplainBudget {
	return ExplainBudget{
		MaxVariables:  32,
		MaxModels:     200_000,
		MaxCandidates: 200_000,
		MaxSolutions:  100,
	}
}

func (b ExplainBudget) normalized() ExplainBudget {
	def := DefaultExplainBudget()
	if b.MaxVariables <= 0 {
		b.MaxVariables = def.MaxVariables
	}
	if b.MaxModels <= 0 {
		b.MaxModels = def.MaxModels
	}
	if b.MaxCandidates <= 0 {
		b.MaxCandidates = def.MaxCandidates
	}
	if b.MaxSolutions <= 0 {
		b.MaxSolutions = def.MaxSolutions
	}
	return b
}

// BlockedTaskExplanation explains a task's depends condition against the
// workflow's fixed node state. It never changes the workflow, retries
// anything, or promises scheduling: it only states which open outcomes would
// make the condition hold.
type BlockedTaskExplanation struct {
	// TaskName is the explained DAG task.
	TaskName string
	// CurrentJudgment is the evaluator's decision against the current nodes.
	CurrentJudgment ExplainJudgment
	// WaitingOn names dependencies that have not finished, when judgment is
	// Waiting.
	WaitingOn []string
	// Terminal maps dependencies whose outcome can no longer change (their
	// node is fulfilled and hooks are done) to the outcome they present.
	Terminal map[string]ExplainOutcome
	// Pending lists dependencies whose outcome is still open, sorted.
	Pending []string
	// UnknownMembers lists task-group dependencies whose full item set is not
	// present in the node map; their unseen items are modeled explicitly
	// instead of being assumed empty.
	UnknownMembers []string
	// Solutions is the antichain of minimal constraint sets whose every
	// completion makes the depends expression true. No solution contains
	// another one, and every solution has been re-evaluated with the real
	// DAGEvaluator expression path and proven minimal.
	Solutions [][]OutcomeConstraint
	// Unsatisfiable is true when no completion of the open tasks can make
	// the expression true: this is a proof, not a budget cutoff.
	Unsatisfiable bool
	// Incomplete is true when a budget was reached. Solutions is then a
	// proven subset: every returned solution still holds the full guarantee
	// and minimality proof, but the antichain may have more members.
	Incomplete bool
	// Reason explains an Incomplete result or why no hypothetical was built.
	Reason string
}

// domainValue is one value of a finite-domain variable: the taskResult scope
// vector it produces plus the user-facing outcome name.
type domainValue struct {
	outcome ExplainOutcome
	vector  taskResult
}

// domainVar is one finite-domain variable: an open whole task (Member == ""),
// an open task-group item, or the synthetic unseen-members variable.
type domainVar struct {
	task   string
	member string
	values []domainValue
}

// groupSpec records how an open task-group dependency's scope vector is
// derived from its item variables and, when members are missing, its unknown
// variable.
type groupSpec struct {
	task       string
	fixed      []taskResult // scope vectors of fulfilled item nodes
	itemVars   []int        // domainVar indexes of open item nodes
	unknownVar int          // domainVar index of the unseen-members variable, -1 if none
}

// explainModel is the finite-domain problem for one task's depends expression.
type explainModel struct {
	logic    string
	vars     []domainVar
	fixed    map[string]taskResult // normalized dep name -> terminal scope vector
	plainVar map[string]int        // dep name -> domainVar index for whole tasks
	groups   map[string]*groupSpec
}

// ExplainBlockedTask reports the current depends judgment for taskName and,
// while its own node does not yet exist, the minimal sets of still-open
// dependency outcomes that would make it executable.
//
// It is read-only: the workflow's nodes are never modified, nothing is
// retried, and no scheduling decision is implied. Dependency states use the
// same finite domains as a real evaluation (one mutually-exclusive outcome
// per task; task-group aggregates derived from real member states), every
// solution is re-validated by evaluating the actual depends expression with
// DAGEvaluator.evalBool over all completions, and each constraint of a
// solution is proven necessary: removing it lets some completion make the
// expression false.
func (e *DAGEvaluator) ExplainBlockedTask(ctx context.Context, taskName string, budget ExplainBudget) (*BlockedTaskExplanation, error) {
	budget = budget.normalized()

	if e.tasks.GetTask(taskName) == nil {
		return nil, fmt.Errorf("task %q not found", taskName)
	}
	if err := e.tasks.GetDependsError(taskName); err != nil {
		return nil, err
	}

	current := e.Evaluate(ctx, taskName)
	if current.Error != nil {
		return nil, current.Error
	}

	explanation := &BlockedTaskExplanation{TaskName: taskName}
	switch {
	case current.Skipped:
		explanation.CurrentJudgment = JudgmentOmitted
	case current.ShouldRun:
		explanation.CurrentJudgment = JudgmentReady
	case current.Suspended:
		explanation.CurrentJudgment = JudgmentWaiting
	default:
		explanation.CurrentJudgment = JudgmentFulfilled
	}
	explanation.WaitingOn = slices.Clone(current.WaitingOn)

	// Once the task has a node of its own, its depends gate has already been
	// passed: hypothetical outcome assignments cannot change what blocks it.
	if node := e.store.getNode(taskName); node != nil {
		explanation.Reason = "task already has a node; its depends condition is no longer the gate"
		return explanation, nil
	}

	deps, err := e.tasks.GetDependencies(ctx, taskName)
	if err != nil {
		return nil, err
	}
	logic := e.tasks.GetDependsLogic(ctx, taskName)
	if logic == "" {
		// No dependencies: the evaluator reports ready and nothing is open.
		return explanation, nil
	}

	model, terminal, pending, unknownMembers, err := e.buildExplainModel(deps)
	if err != nil {
		return nil, err
	}
	explanation.Terminal = terminal
	explanation.Pending = pending
	explanation.UnknownMembers = unknownMembers
	if len(model.vars) > budget.MaxVariables {
		explanation.Incomplete = true
		explanation.Reason = fmt.Sprintf("finite-domain variable budget exceeded: %d open variables, budget %d", len(model.vars), budget.MaxVariables)
	}
	model.logic = logic

	solutions, unsat, incomplete, reason, err := model.solve(e, budget, explanation.Incomplete)
	if err != nil {
		return nil, err
	}
	explanation.Solutions = solutions
	explanation.Incomplete = explanation.Incomplete || incomplete
	if explanation.Reason == "" {
		explanation.Reason = reason
	}
	explanation.Unsatisfiable = unsat
	return explanation, nil
}

// buildExplainModel classifies every dependency as terminal (fixed scope
// vector) or open (a finite-domain variable), expanding open task groups into
// item variables and an explicit unseen-members variable.
func (e *DAGEvaluator) buildExplainModel(deps []string) (*explainModel, map[string]ExplainOutcome, []string, []string, error) {
	model := &explainModel{
		fixed:    make(map[string]taskResult),
		plainVar: make(map[string]int),
		groups:   make(map[string]*groupSpec),
	}
	terminal := make(map[string]ExplainOutcome)
	var pending []string
	var unknownMembers []string

	for _, dep := range deps {
		node := e.store.getNode(dep)
		if !e.isPendingDependency(dep) {
			// Immutable for readiness: mirror the exact scope the evaluator
			// builds for this node.
			var children []*wfv1.NodeStatus
			var missing bool
			if node.Type == wfv1.NodeTypeTaskGroup {
				children, missing = e.store.taskGroupChildren(dep)
			}
			model.fixed[normalizeTaskName(dep)] = taskResultForNode(node, children, missing)
			terminal[dep] = terminalOutcome(node)
			continue
		}

		pending = append(pending, dep)
		if node != nil && node.Type == wfv1.NodeTypeTaskGroup {
			spec := &groupSpec{task: dep, unknownVar: -1}
			children, missing := e.store.taskGroupChildren(dep)
			for _, child := range children {
				vector := taskResultForNode(child, nil, false)
				if child.Fulfilled() {
					spec.fixed = append(spec.fixed, vector)
					continue
				}
				idx := len(model.vars)
				model.vars = append(model.vars, domainVar{
					task:   dep,
					member: itemMemberName(node.Name, child.Name),
					values: plainOutcomeDomain(),
				})
				spec.itemVars = append(spec.itemVars, idx)
			}
			if missing || (len(children) == 0 && e.tasks.GetTask(dep) != nil && e.tasks.GetTask(dep).ShouldExpand()) {
				// Members are absent from the node map (pruned/offloaded), or
				// the group has not expanded its items yet: do not assume the
				// group is empty. Model every aggregate/phase the unseen
				// members can produce explicitly.
				idx := len(model.vars)
				model.vars = append(model.vars, domainVar{
					task:   dep,
					member: UnknownMember,
					values: unknownMemberDomain(),
				})
				spec.unknownVar = idx
				unknownMembers = append(unknownMembers, dep)
			}
			model.groups[dep] = spec
			continue
		}

		// A whole open task, including a not-yet-created group node whose
		// member set is entirely unknown.
		if node == nil {
			if task := e.tasks.GetTask(dep); task != nil && task.ShouldExpand() {
				spec := &groupSpec{task: dep, unknownVar: len(model.vars)}
				model.vars = append(model.vars, domainVar{
					task:   dep,
					member: UnknownMember,
					values: unknownMemberDomain(),
				})
				model.groups[dep] = spec
				unknownMembers = append(unknownMembers, dep)
				continue
			}
		}
		idx := len(model.vars)
		model.vars = append(model.vars, domainVar{task: dep, values: plainOutcomeDomain()})
		model.plainVar[dep] = idx
	}

	sort.Strings(pending)
	sort.Strings(unknownMembers)
	return model, terminal, pending, unknownMembers, nil
}

// isPendingDependency mirrors evaluateDependsReadiness's waiting conditions
// exactly: no node, unfinished hooks, or an unfulfilled non-daemon node.
func (e *DAGEvaluator) isPendingDependency(depName string) bool {
	node := e.store.getNode(depName)
	if node == nil {
		return true
	}
	if !e.store.areHooksFulfilled(depName) {
		return true
	}
	daemonRunning := node.IsDaemoned() && !node.Phase.Fulfilled(node.TaskResultSynced)
	return !daemonRunning && !node.Fulfilled()
}

// terminalOutcome labels the fixed outcome of a fulfilled node.
func terminalOutcome(node *wfv1.NodeStatus) ExplainOutcome {
	switch node.Phase {
	case wfv1.NodeSucceeded:
		return ExplainSucceeded
	case wfv1.NodeFailed:
		return ExplainFailed
	case wfv1.NodeError:
		return ExplainErrored
	case wfv1.NodeSkipped:
		return ExplainSkipped
	case wfv1.NodeOmitted:
		return ExplainOmitted
	default:
		// A fulfilled non-pending daemon presents Daemoned to its dependants.
		return ExplainDaemoned
	}
}

// plainOutcomeDomain is the six-value mutually-exclusive domain of an open
// task or open task-group item.
func plainOutcomeDomain() []domainValue {
	return []domainValue{
		{outcome: ExplainSucceeded, vector: taskResult{Succeeded: true}},
		{outcome: ExplainFailed, vector: taskResult{Failed: true}},
		{outcome: ExplainErrored, vector: taskResult{Errored: true}},
		{outcome: ExplainSkipped, vector: taskResult{Skipped: true}},
		{outcome: ExplainOmitted, vector: taskResult{Omitted: true}},
		{outcome: ExplainDaemoned, vector: taskResult{Daemoned: true}},
	}
}

// unknownMemberDomain spans every (AnySucceeded, AllFailed, completed phase)
// combination the unseen members of a task group can induce. AllFailed and
// AnySucceeded cannot both be true.
func unknownMemberDomain() []domainValue {
	values := []domainValue{
		// No unseen members: the group is determined by its known items alone.
		{outcome: ExplainUnknownNone, vector: taskResult{}},
	}
	// AnySucceeded == true, across the three possible completed group phases.
	values = append(values,
		domainValue{outcome: ExplainUnknownSucceeded, vector: taskResult{AnySucceeded: true, Succeeded: true}},
		domainValue{outcome: ExplainUnknownSucceededFailed, vector: taskResult{AnySucceeded: true, Failed: true}},
		domainValue{outcome: ExplainUnknownSucceededErrored, vector: taskResult{AnySucceeded: true, Errored: true}},
	)
	// AllFailed == true forces a Failed phase.
	values = append(values, domainValue{outcome: ExplainUnknownFailed, vector: taskResult{AllFailed: true, Failed: true}})
	// Neither aggregate, across the three possible completed group phases.
	values = append(values,
		domainValue{outcome: ExplainUnknownOther, vector: taskResult{Succeeded: true}},
		domainValue{outcome: ExplainUnknownFailedOther, vector: taskResult{Failed: true}},
		domainValue{outcome: ExplainUnknownErrored, vector: taskResult{Errored: true}},
	)
	return values
}

// itemMemberName returns an item node's name relative to its group node.
func itemMemberName(groupName, childName string) string {
	if rest, ok := strings.CutPrefix(childName, groupName); ok {
		return rest
	}
	return childName
}

// scope builds the exact evaluator scope map for one full assignment, using
// the same scope-construction rule as evaluateDependsReadiness.
func (m *explainModel) scope(assignment []int) map[string]taskResult {
	scope := maps.Clone(m.fixed)
	for dep, idx := range m.plainVar {
		scope[normalizeTaskName(dep)] = m.vars[idx].values[assignment[idx]].vector
	}
	for dep, spec := range m.groups {
		known := slices.Clone(spec.fixed)
		var delta taskResult
		hasUnknown := spec.unknownVar >= 0
		unknownNone := false
		if hasUnknown {
			value := m.vars[spec.unknownVar].values[assignment[spec.unknownVar]]
			delta = value.vector
			unknownNone = value.outcome == ExplainUnknownNone
		}
		for _, idx := range spec.itemVars {
			known = append(known, m.vars[idx].values[assignment[idx]].vector)
		}
		scope[normalizeTaskName(dep)] = deriveGroupScope(known, delta, hasUnknown && !unknownNone)
	}
	return scope
}

// deriveGroupScope derives a task group's scope vector from its known item
// vectors and the unseen-members delta, following the evaluator's rules:
// AnySucceeded is the OR of item success, AllFailed needs at least one member
// with every member Failed, and the group's phase is the worst item phase
// (Error outranks Failed outranks everything else).
func deriveGroupScope(known []taskResult, delta taskResult, unknownPresent bool) taskResult {
	knownAnySucceeded := false
	// Vacuously true while no counterexample has been seen: the empty set is
	// "all failed", but it does not satisfy the evaluator's non-empty rule.
	knownAllFailed := true
	phase := byte(0) // 0 = Succeeded, 1 = Failed, 2 = Errored
	for _, item := range known {
		if item.Succeeded {
			knownAnySucceeded = true
		}
		if !item.Failed {
			knownAllFailed = false
		}
		if item.Errored {
			phase = 2
		} else if item.Failed && phase < 1 {
			phase = 1
		}
	}

	if !unknownPresent {
		// Mirror the evaluator: AllFailed on a known, complete member set also
		// requires at least one member to exist.
		return groupScopeVector(knownAnySucceeded, len(known) > 0 && knownAllFailed, phase)
	}

	anySucceeded := knownAnySucceeded || delta.AnySucceeded
	// Unseen members can only make AllFailed true when every known item also
	// Failed and the delta itself asserts at least one unseen member Failed;
	// an empty known set is vacuously all-Failed.
	allFailed := knownAllFailed && delta.AllFailed
	switch {
	case delta.Errored:
		phase = 2
	case delta.Failed && phase < 1:
		phase = 1
	}
	return groupScopeVector(anySucceeded, allFailed, phase)
}

// groupScopeVector assembles a group node's one-hot phase vector with its
// aggregate booleans. A completed group's phase is only Succeeded, Failed or
// Errored (see TaskGroupPhase).
func groupScopeVector(anySucceeded, allFailed bool, phase byte) taskResult {
	vector := taskResult{AnySucceeded: anySucceeded, AllFailed: allFailed}
	switch phase {
	case 2:
		vector.Errored = true
	case 1:
		vector.Failed = true
	default:
		vector.Succeeded = true
	}
	return vector
}

// solve enumerates full assignments within the budget, then extracts the
// minimal-solution antichain. unsat is an exhaustive proof that no
// completion satisfies the expression. Every returned solution is
// revalidated and proven minimal with the real expression evaluator.
func (m *explainModel) solve(e *DAGEvaluator, budget ExplainBudget, alreadyIncomplete bool) (solutions [][]OutcomeConstraint, unsat, incomplete bool, reason string, err error) {
	eval := func(assignment []int) (bool, error) {
		return e.evalBool(m.logic, m.scope(assignment))
	}

	satRows, failRows, truncated, evalCount, err := m.enumerate(eval, budget.MaxModels)
	if err != nil {
		return nil, false, false, "", err
	}
	if !truncated && len(satRows) == 0 {
		// Exhaustive proof: no completion makes the expression true.
		return nil, !alreadyIncomplete, alreadyIncomplete, "", nil
	}

	incomplete = alreadyIncomplete
	if truncated {
		incomplete = true
		reason = fmt.Sprintf("model budget reached after %d full assignments; returning proven subset", evalCount)
	}

	var accepted []map[int]int
	candidates := 0
	searchDone := false

	for size := 1; size <= len(m.vars) && !searchDone; size++ {
		stop := forEachVarCombination(len(m.vars), size, func(combo []int) bool {
			stop := forEachValueCombo(m.vars, combo, func(partial map[int]int) bool {
				candidates++
				if candidates > budget.MaxCandidates {
					searchDone = true
					return true
				}
				if containsPartial(accepted, partial) {
					return false
				}
				guaranteed, uncertain, guaranteeErr := m.guarantees(partial, failRows, truncated, eval, budget.MaxModels-evalCount)
				if guaranteeErr != nil {
					err = guaranteeErr
					searchDone = true
					return true
				}
				if uncertain {
					incomplete = true
					if reason == "" {
						reason = "verification budget reached while proving a candidate; returning proven subset"
					}
					return false
				}
				if !guaranteed {
					return false
				}
				// Revalidate with the real evaluator over every completion,
				// then prove every constraint necessary. Candidates that
				// cannot be fully proven within budget are never emitted.
				proven, proofEvals, ok, proofErr := m.proveSolution(partial, eval, budget.MaxModels-evalCount)
				if proofErr != nil {
					err = proofErr
					searchDone = true
					return true
				}
				evalCount += proofEvals
				if !ok {
					incomplete = true
					if reason == "" {
						reason = "verification budget reached while proving a minimal solution; returning proven subset"
					}
					return false
				}
				if proven {
					accepted = append(accepted, partial)
					if len(accepted) >= budget.MaxSolutions {
						incomplete = true
						if reason == "" {
							reason = fmt.Sprintf("solution budget reached: %d minimal solutions", budget.MaxSolutions)
						}
						searchDone = true
						return true
					}
				}
				return false
			})
			return stop
		})
		if stop {
			break
		}
	}
	if candidates > budget.MaxCandidates && reason == "" {
		incomplete = true
		reason = fmt.Sprintf("candidate budget reached after %d partial assignments; returning proven subset", candidates)
	}

	return m.toConstraints(accepted), false, incomplete, reason, err
}

// enumerate evaluates every full assignment (the Cartesian product of all
// variable domains), splitting rows into satisfying and falsifying ones.
// truncated is true when the model cap stopped the walk early.
func (m *explainModel) enumerate(eval func([]int) (bool, error), maxModels int) (satRows, failRows [][]int, truncated bool, evals int, err error) {
	assignment := make([]int, len(m.vars))
	var walk func(varIdx int) bool
	walk = func(varIdx int) bool {
		if varIdx == len(m.vars) {
			if evals >= maxModels {
				truncated = true
				return true
			}
			evals++
			ok, evalErr := eval(assignment)
			if evalErr != nil {
				err = evalErr
				return true
			}
			row := slices.Clone(assignment)
			if ok {
				satRows = append(satRows, row)
			} else {
				failRows = append(failRows, row)
			}
			return false
		}
		for valueIdx := range m.vars[varIdx].values {
			assignment[varIdx] = valueIdx
			if walk(varIdx + 1) {
				return true
			}
		}
		return false
	}
	walk(0)
	return satRows, failRows, truncated, evals, err
}

// guarantees reports whether partial makes the expression true for every
// completion of the unbound variables. When the full truth table was
// enumerated it answers set-theoretically; otherwise it evaluates every
// completion directly, and uncertain means the verification cap was hit.
func (m *explainModel) guarantees(partial map[int]int, failRows [][]int, truncated bool, eval func([]int) (bool, error), maxEvals int) (guaranteed, uncertain bool, err error) {
	if !truncated {
		// Exact: a falsifying full assignment must not extend the partial.
		for _, row := range failRows {
			extends := true
			for varIdx, valueIdx := range partial {
				if row[varIdx] != valueIdx {
					extends = false
					break
				}
			}
			if extends {
				return false, false, nil
			}
		}
		return true, false, nil
	}

	allTrue, _, _, capHit, err := m.walkCompletions(partial, eval, maxEvals, false)
	if err != nil {
		return false, false, err
	}
	return allTrue && !capHit, capHit, nil
}

// proveSolution re-runs the real expression for every completion of the
// unbound variables (all must be true) and then removes each constraint in
// turn: each removal must leave a completion where the expression is false.
// ok is false when the evaluation cap prevents completing the proof; such a
// candidate must not be emitted.
func (m *explainModel) proveSolution(partial map[int]int, eval func([]int) (bool, error), maxEvals int) (proven bool, evals int, ok bool, err error) {
	allTrue, _, used, capHit, err := m.walkCompletions(partial, eval, maxEvals, false)
	evals += used
	if err != nil {
		return false, evals, false, err
	}
	if capHit || !allTrue {
		return false, evals, !capHit, nil
	}
	for varIdx := range partial {
		reduced := maps.Clone(partial)
		delete(reduced, varIdx)
		_, foundFalse, used, capHit, err := m.walkCompletions(reduced, eval, maxEvals-evals, true)
		if err != nil {
			return false, evals, false, err
		}
		evals += used
		if capHit {
			return false, evals, false, nil
		}
		if !foundFalse {
			// The constraint was redundant, so this is not a minimal solution.
			return false, evals, true, nil
		}
	}
	return true, evals, true, nil
}

// walkCompletions walks every full assignment extending partial. When
// stopOnFalse is true it stops at the first falsifying completion (used to
// prove a removed constraint is insufficient); otherwise it requires every
// completion to be true. capHit reports the evaluation cap was reached.
func (m *explainModel) walkCompletions(partial map[int]int, eval func([]int) (bool, error), maxEvals int, stopOnFalse bool) (allTrue bool, foundFalse bool, evals int, capHit bool, err error) {
	assignment := make([]int, len(m.vars))
	for varIdx, valueIdx := range partial {
		assignment[varIdx] = valueIdx
	}
	allTrue = true
	var walk func(varIdx int) bool
	walk = func(varIdx int) bool {
		if varIdx == len(m.vars) {
			if evals >= maxEvals {
				capHit = true
				return true
			}
			evals++
			ok, evalErr := eval(assignment)
			if evalErr != nil {
				err = evalErr
				return true
			}
			if !ok {
				allTrue = false
				foundFalse = true
				if stopOnFalse {
					return true
				}
			}
			return false
		}
		if fixed, ok := partial[varIdx]; ok {
			assignment[varIdx] = fixed
			return walk(varIdx + 1)
		}
		for valueIdx := range m.vars[varIdx].values {
			assignment[varIdx] = valueIdx
			if walk(varIdx + 1) {
				return true
			}
		}
		return false
	}
	walk(0)
	return allTrue, foundFalse, evals, capHit, err
}

// forEachVarCombination invokes fn with every length-k combination of indices
// of [0,n). It returns true when fn asked to stop.
func forEachVarCombination(n, k int, fn func([]int) bool) bool {
	combo := make([]int, k)
	var walk func(start, depth int) bool
	walk = func(start, depth int) bool {
		if depth == k {
			return fn(slices.Clone(combo))
		}
		for i := start; i <= n-(k-depth); i++ {
			combo[depth] = i
			if walk(i+1, depth+1) {
				return true
			}
		}
		return false
	}
	return walk(0, 0)
}

// forEachValueCombination invokes fn with every partial assignment choosing
// one domain value for each variable in combo. It returns true when fn asked
// to stop.
func forEachValueCombo(vars []domainVar, combo []int, fn func(map[int]int) bool) bool {
	partial := make(map[int]int, len(combo))
	var walk func(depth int) bool
	walk = func(depth int) bool {
		if depth == len(combo) {
			return fn(maps.Clone(partial))
		}
		varIdx := combo[depth]
		for valueIdx := range vars[varIdx].values {
			partial[varIdx] = valueIdx
			if walk(depth + 1) {
				return true
			}
		}
		return false
	}
	return walk(0)
}

// containsPartial reports whether an already accepted minimal solution is a
// subset of candidate (then candidate cannot itself be minimal).
func containsPartial(accepted []map[int]int, candidate map[int]int) bool {
	for _, solution := range accepted {
		subset := true
		for varIdx, valueIdx := range solution {
			// Comma-ok is required: an absent key must not read as value 0.
			if candidateValue, ok := candidate[varIdx]; !ok || candidateValue != valueIdx {
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

// toConstraints renders accepted minimal partials as sorted, deterministic
// OutcomeConstraint sets and orders the antichain by size then lexicographic
// key, so output is stable across runs.
func (m *explainModel) toConstraints(accepted []map[int]int) [][]OutcomeConstraint {
	solutions := make([][]OutcomeConstraint, 0, len(accepted))
	for _, partial := range accepted {
		constraints := make([]OutcomeConstraint, 0, len(partial))
		for varIdx, valueIdx := range partial {
			v := m.vars[varIdx]
			constraints = append(constraints, OutcomeConstraint{
				Task:    v.task,
				Member:  v.member,
				Outcome: v.values[valueIdx].outcome,
			})
		}
		sort.Slice(constraints, func(i, j int) bool {
			return constraintKey(constraints[i]) < constraintKey(constraints[j])
		})
		solutions = append(solutions, constraints)
	}
	sort.Slice(solutions, func(i, j int) bool {
		left, right := solutions[i], solutions[j]
		if len(left) != len(right) {
			return len(left) < len(right)
		}
		for k := range left {
			if li, lj := constraintKey(left[k]), constraintKey(right[k]); li != lj {
				return li < lj
			}
		}
		return false
	})
	return solutions
}

func constraintKey(c OutcomeConstraint) string {
	return c.Task + "/" + c.Member + "=" + string(c.Outcome)
}
