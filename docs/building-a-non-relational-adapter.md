# Building a Non-Relational Adapter (Redis-Style)

This guide covers a practical pattern for non-relational systems (Redis/KV/document/index).

## 1) Model Entities as Queryable Provider Tables

In `sqlql`, provider entities are source-neutral.

A non-relational entity can represent:

- keyspace subset
- index projection
- collection/document subset

Expose stable logical columns that your adapter can materialize.

## 2) Stage 1: Map Key/Value to Rows + Scan Support

Baseline implementation:

- map source records (`key`, `value`) to row objects
- implement scan with:
  - projection
  - filter
  - sort
  - limit/offset

Keep mapping logic configurable, not hardcoded to one data shape.

## 3) Stage 2: Add `lookupMany` for Join Participation

Implement `lookupMany` using key/indexed access.

Benefits:

- supports cross-provider lookup joins
- avoids full scans for join lookups
- usually highest ROI feature after basic scan

## 4) Stage 3: Optional `rel` Pushdown Subset

Only add non-trivial `rel` pushdown when your backend can do it reliably.

Common strategy:

- keep `scan` + `lookupMany` strong
- rely on local logical execution for complex operations
- selectively push down simple aggregates/sorts where native support is clear

## 5) Explicit Rejection Strategy

Reject unsupported shapes explicitly in `canExecute`.

Examples:

- unsupported multi-join graph
- unsupported aggregate semantics
- unsupported window operations

Provide clear reasons so users can understand why fallback occurs.

## 6) Performance and Key Design Guidance

Design for your access paths:

- choose key patterns aligned with common filters
- precompute/index where needed for lookupMany
- avoid adapter-level full scans for high-cardinality joins
- log operation volume and hotspots

## 7) Testing and Fallback Expectations

Test categories:

1. mapping correctness (`key/value -> row`)
2. scan/filter/sort/limit correctness
3. lookupMany behavior and edge cases
4. fallback correctness for unsupported `rel` shapes
5. explicit rejection reason stability

Fallback is expected in early versions; the adapter should still produce correct results.
