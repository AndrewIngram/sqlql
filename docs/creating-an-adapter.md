# Creating a New Adapter (Progressive Path)

This guide shows how to ship an adapter incrementally:

- get a useful baseline quickly
- rely on local fallback for unsupported shapes
- add pushdown coverage over time
- reject unsupported/high-risk shapes explicitly

## 1) Adapter Interface and Fragment Kinds

Provider adapter methods:

- `canExecute(fragment, context)`
- `compile(fragment, context)`
- `execute(compiled, context)`
- optional `lookupMany(request, context)`
- optional `estimate(fragment, context)`

Fragment kinds:

- `scan`
- `rel`
- `aggregate`

## 2) Stage 1: Minimal Working Adapter

Start with `scan` support:

- projection
- where/filter
- sort
- limit/offset

For `rel` fragments you cannot handle yet, return unsupported in `canExecute`.

That is enough to deliver value while planner/executor handles local operations when possible.

## 3) Stage 2: Add `lookupMany`

Implement `lookupMany` early if possible.

Why it matters:

- enables efficient lookup joins across providers
- avoids broad scans on the lookup side
- improves cross-provider performance significantly

## 4) Stage 3: Add Core `rel` Pushdown

Add single-query pushdown for core relational shapes:

- scan/filter/project
- join
- aggregate/group-by
- order
- limit/offset

Goal: collapse common same-provider queries into one downstream operation.

## 5) Stage 4: Reduce Fallback

Expand supported `rel` shapes iteratively:

- set operations
- CTEs
- window operations

Treat this as progressive hardening, not all-or-nothing.

## 6) Query-Shape Rejection Patterns

Use deterministic rejection when a shape is unsupported or unsafe.

Recommended:

- return structured unsupported reason from `canExecute`
- keep messages specific and stable
- reject early for impossible/unsafe fragments

Examples:

- unsupported window frame variant
- unsupported correlated shape
- unsupported join topology for your backend

## 7) Testing Strategy

Minimum test layers:

1. Unit tests
- scan behavior (filter/sort/limit/projection)
- compile/execute correctness

2. Capability tests
- unsupported shapes return expected reasons

3. Conformance tests
- result parity vs other first-party adapters for shared shapes

4. Fallback tests
- when pushdown is unsupported, local/fallback execution remains correct

## 8) Operational Guidance

- add operation telemetry (provider operation logs)
- respect query guardrails (`maxExecutionRows`, timeouts)
- ensure deterministic scope behavior across scan and lookup paths

## 9) Recommended Build Order

1. `scan`
2. `lookupMany`
3. core `rel` pushdown
4. advanced `rel` pushdown
5. tighten rejection rules

This sequence gets users to production value quickly while keeping future improvements straightforward.
