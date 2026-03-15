# Tech Debt Tracker

These are the current architecture and process questions that remain intentionally open.

## Open items

### Explain purity vs rich provider descriptions

- Current state: `explain()` may compile provider-owned fragments to produce provider plan descriptions.
- Question: should `explain()` stay on that model, or should providers get a describe-only path that avoids compile-time side effects?

### Non-`ROWS` window frames

- Current state: `RANGE` and `GROUPS` frame modes are explicitly rejected.
- Question: implement correct semantics later or continue treating them as out of scope?

### Recursive CTE iteration guardrail

- Current state: recursive CTE execution stops after 256 iterations with a hardcoded runtime limit.
- Question: keep that fixed safety valve, or surface it as an explicit query/runtime guardrail alongside the existing row and timeout controls?

### Provider-local barrier traversal consistency

- Current state: some planner/runtime/provider-normalization switches intentionally treat local-only barrier nodes like `correlate` and `repeat_union` as unreachable in provider-owned paths and return early rather than traversing children.
- Question: keep that as an explicit invariant-only simplification, or normalize these switches to traverse children for consistency and future-proofing even when the current planner should never route them there?

### Lookup-join planning vs runtime lookup capability

- Current state: planner lookup-join candidacy no longer checks whether the right-side provider implements `lookupMany`; runtime still guards the optimization before execution.
- Question: is the current split acceptable as a planner simplification, or should planner candidacy regain lookup-capability awareness to keep explain/physical-plan diagnostics closer to actual execution choices?

### Aggregate-mode navigation window validation

- Current state: grouped aggregate window validation checks `partitionBy`, `orderBy`, and aggregate `column` refs against aggregate output columns, but it does not yet validate navigation-function `value` / `defaultExpr` expression refs in the same way.
- Question: should aggregate-mode window preparation reject non-aggregate navigation expressions early with a specific lowering error, rather than relying on later execution-time failure?

### Planner subquery callback Result bridge

- Current state: structured/simple SELECT lowering now returns typed `Result` values for direct validation failures, but nested subquery lowering still crosses an older callback seam that expects `RelNode | null`, so the structured-select bridge temporarily rethrows `RelLoweringError` across that seam and immediately re-captures it.
- Question: should expression/subquery lowering gain a fully Result-typed callback path so planner lowering can remove that last throw-based bridge entirely?

### Mechanical plan-coverage enforcement

- Current state: substantial work is expected to have a checked-in execution plan, but enforcement is social/documented rather than diff-based.
- Question: add a low-noise repo check later, or keep it as documented workflow only?
