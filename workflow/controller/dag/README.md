# DAG Evaluation Package

This package decides which tasks of a DAG or Steps template are ready to run, which are waiting on dependencies, which should be omitted because their `depends` condition can never be satisfied, and what a retry or task-group node's current state amounts to.
It performs no side effects: the `Engine` in `workflow/controller/engine.go` reads its results and creates, dispatches and marks nodes.
Both template types use it — Steps tasks are adapted to the same `Task` interface with synthetic dependencies on the previous step group.

## Files

| File | Purpose |
|------|---------|
| `doc.go` | Package comment |
| `argo.go` | `DAGEvaluator` — readiness evaluation, retry and task-group assessment, public API |
| `topology.go` | `WorkflowTasks` — task collection, dependency resolution, leaf tasks; `PullOrder` |
| `store.go` | `workflowStore` — maps task names to workflow nodes; `TaskNodeName` naming convention |
| `task.go` | `Task` interface and the `DAGTask` adapter for `wfv1.DAGTask` (`StepAdapter` lives in `workflow/controller/steps.go`) |
| `types.go` | `EvaluationResult`, `Action`, and the `taskResult` scope struct |
| `expansion.go` | `withItems` / `withParam` / `withSequence` expansion and expanded task naming |
| `explain.go` | `ExplainBlockedTask` — read-only explanation of a blocked task's depends condition and the minimal open-outcome sets that would unlock it |
| `helpers_test.go` | Test-only convenience (`NewDAGEvaluator`); production code does not use it |

## How it works

### 1. Construction

```go
evaluator := dag.NewDAGEvaluatorFromTasks(wf, tasks, tmpl, boundaryID, boundaryName)
```

`tasks` is the boundary's task list as `dag.Task` values (`DAGTask` for DAG templates, `StepAdapter` for Steps).
Construction builds a `workflowStore` over `wf.Status.Nodes` and a `WorkflowTasks` that resolves every task's dependencies once.
A new evaluator is created every reconcile cycle, so nothing here is long-lived.

### 2. Dependency resolution

A user-written `depends` expression is tokenized with `common.ParseDepends` — the same grammar workflow validation uses, so an expression cannot pass validation and be read differently here.
Legacy `dependencies` lists (and the synthetic dependencies of Steps tasks, whose names are `[<group index>].<step name>`, for example `[0].build`) are structured data and are expanded directly with `common.ExpandDependency`; they are never re-parsed as an expression.

Task names are rewritten to hex-encoded identifiers (`my-task` → `t6d792d7461736b`) so they are valid, collision-free identifiers in the evaluated expression.

### 3. Readiness evaluation

A task **waits** while any dependency is still pending: not started, running, a retry node whose outcome the operator's retry handling has not recorded yet, or finished with lifecycle or exit hooks still running (for an expanded task, its items' hooks too).
A running daemon counts as finished.
Once every dependency has finished, `evaluateDependsReadiness` builds a scope of dependency states — a `taskResult` per dependency with the fields `Succeeded`, `Failed`, `Errored`, `Skipped`, `Omitted`, `Daemoned`, `AnySucceeded`, `AllFailed` (the same vocabulary as `common.TaskResult*`) — and evaluates the normalized expression with a cached, compiled `expr` program.
The task is **ready** if the expression is true and is **omitted** if it is false.

`Evaluate` evaluates one task this way against the nodes as they are now.
The engine calls it for each task in dependency order, immediately before acting on the task, so the task sees what the engine has just done to its dependencies.

### 4. Cascading omission

The evaluator records no state of its own: the engine creates the Omitted node of each task that can never run before it evaluates that task's dependants, so `Evaluate` sees an omission through the node.
Because the engine's walk evaluates a task after all of its dependencies, an omission propagates in the same walk: A fails → B (`depends: A.Succeeded`) is omitted → C (`depends: B`) is omitted.

### 5. Retry and task-group nodes

The evaluator makes no retry decision. `evaluateRetryNode` reports a retry node by its own phase, which the controller's `processNodeRetries` records from the processed `retryStrategy`: until the node is fulfilled it asks for it to be dispatched (`ActionExecute`), so that `processNodeRetries` alone decides whether to start another attempt, wait out a backoff (it requeues the workflow), or finish the node; once it is fulfilled, a running daemon included, nothing is dispatched.
An expanded item's retry node is not assessed here: the engine re-enters the item on every dispatch of its TaskGroup, and `processNodeRetries` drives its retries.

An expanded task has one result, for its TaskGroup node: `evaluateTaskGroupNode` asks for it to be dispatched until the node is fulfilled, and the engine's dispatch creates or re-enters each item, drives each item's hooks, and completes the group once every item and its hooks have finished; while a hook error is ending the template, an item with no node is not created, and the group completes from the items that exist.
`TaskGroupPhase` is the one rule for a group's phase, used by the engine to complete it: the worst item phase (Error over Failed over Succeeded), and not done while an item it is given is missing or unfinished; a running daemon counts as finished.

### 6. Public API

What the `Engine` uses:

```go
evaluator.Evaluate(ctx, taskName)        // one task's EvaluationResult, for the walk
evaluator.GetTargetTasks(ctx)            // explicit dag.target tasks, or the leaves
evaluator.FindLeafTaskNames(ctx)         // tasks nothing depends on
evaluator.GetAncestors(ctx, taskName)    // transitive dependencies (unordered)
evaluator.GetDependencies(ctx, taskName) // direct dependencies
evaluator.GetTask(name)                  // the Task by name
evaluator.ExplainBlockedTask(ctx, name, budget) // read-only depends explanation + minimal unlocking outcome sets
task.Expand / task.ShouldExpand          // withItems/withParam/withSequence expansion
dag.TaskNodeName                         // task → node name convention
dag.TaskNodeID / dag.TaskNode            // a task's node ID and node
dag.TaskGroupPhase                       // a TaskGroup's phase from its items
dag.WorsePhase                           // the worse of two phases for an outcome (Error over Failed)
dag.PullOrder                            // a DAG's tasks in walk order: the targets' ancestry, dependencies first
```

Fields of `EvaluationResult` the engine acts on:

- `ShouldRun` — the engine dispatches the task: its dependencies allow it to run, or its node exists and is unfinished (an unfulfilled retry or TaskGroup node comes with `ActionExecute`).
- `Skipped` / `SkipReason` — the task will never run; the engine creates the Omitted node with this reason.
- `Error` — the task could not be assessed; the engine records a terminal Error node.

`Action`, `ActionReason`, `Suspended` and `WaitingOn` are diagnostic: the engine logs them at debug level and does not act on them.

### 7. Blocked-task explanation (`ExplainBlockedTask`, `explain.go`)

`ExplainBlockedTask` is a read-only diagnostic over the same nodes and depends logic: it never mutates the workflow, retries a task, or promises scheduling. For a task without a node it reports:

- the current judgment (`Ready` / `Waiting` / `Omitted` / `Fulfilled`, from `Evaluate`);
- dependencies split into **terminal** (a fulfilled node whose outcome cannot change) and **pending** (still open);
- the antichain of **minimal constraint sets** over pending outcomes whose every completion makes the depends expression true.

Each open task is one finite-domain variable over the mutually-exclusive outcomes `Succeeded`, `Failed`, `Errored`, `Skipped`, `Omitted`, `Daemoned` — the one-hot vocabulary of `taskResult` — so an answer can never assign two independent booleans to one task. An open TaskGroup expands into one variable per unfinished item, and its `AnySucceeded`/`AllFailed` and completed phase are derived from member states exactly like `taskResultForNode`; members missing from the node map (pruned, offloaded, or not yet expanded) become an explicit unseen-members variable listed in `UnknownMembers`, never an assumed empty set. Legacy `dependencies`, `continueOn` and user `depends` all flow through the same `ParseDepends`-based topology, so negation and OR produce real minimal sets instead of the full ancestor list.

A bounded Cartesian enumeration (the BDD/SAT equivalent for these small finite domains) builds the truth table with the real `evalBool`; candidates are then extracted by increasing size, pruned against accepted minimal sets, and every emitted solution is re-evaluated over all completions and proven minimal — removing any one constraint leaves a completion where the expression is false. Hitting `ExplainBudget` sets `Incomplete` with the proven subset found so far; an exhaustive empty satisfying set sets `Unsatisfiable`.

## Architecture

```text
Engine (workflow/controller/engine.go)
  │
  ├── DAGEvaluator (argo.go)
  │     ├── WorkflowTasks (topology.go)
  │     │     ├── common.ParseDepends / ExpandDependency (shared with validation)
  │     │     ├── hex-encoded identifiers
  │     │     └── PullOrder (walk order)
  │     ├── workflowStore (store.go)
  │     │     ├── node lookup by task name (TaskNodeName)
  │     │     └── hook-fulfilment checks
  │     └── ExplainBlockedTask (explain.go)
  │           ├── finite-domain outcome model + task-group member derivation
  │           └── bounded enumeration, minimal antichain, real-evaluator proofs
  │
  └── Task interface (task.go)
        ├── DAGTask   (DAG templates)
        └── StepAdapter (Steps templates, workflow/controller/steps.go)
```
