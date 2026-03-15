# Provider Capability Matrix

This document maps route families and practical pushdown maturity. Provider authoring should work
from rel-shape helpers and field policies, not a separate atom vocabulary.

## Route Families

Use route families for rough maturity only:

- `scan`
- `lookup`
- `aggregate`
- `rel-core`
- `rel-advanced`

Examples:

- a provider can be in `rel-core` while still rejecting some join or set-op shapes
- a provider can expose `rel-advanced` support only for selected `WITH` or window subtrees

## Typical Shapes

### Scan-only relational

Expected helper coverage:

- simple single-source scan pipeline
- projected/filter/sort/limit field validation

Typical behavior:

- one-table scans push down
- joins, aggregates, and advanced expressions stay local

### Lookup-first / KV

Expected helper coverage:

- keyed single-source scan pipeline
- lookup key inference
- fetch-column collection

Typical behavior:

- keyed fetches and constrained scans push down
- broad scans and joins stay local unless the runtime can optimize safely

### Aggregate-capable

Expected helper coverage:

- single-source aggregate pipeline extraction
- grouped aggregate validation

### Rel-core

Typical capabilities:

- same-provider joins
- set operations
- grouped aggregates

These providers usually rely on strategy-based relational helpers instead of narrow scan helpers.

## Diagnostics and Policy

Unsupported or expensive shapes should be reported with:

- stable `SQLQL_*` diagnostic code
- SQLSTATE-like class
- concrete reason tied to the rel shape or field policy

Fallback policy decides whether those diagnostics are:

- warnings with local fallback
- hard errors

Important policy knobs:

- `allowFallback`
- `warnOnFallback`
- `rejectOnEstimatedCost`
- `maxLocalRows`
- `maxLookupFanout`
- `maxJoinExpansionRisk`

Default behavior is `allow + annotate`.

## Remaining Roadmap Gaps

Still incomplete or intentionally partial:

- computed-expression pushdown across first-party adapters
- `FROM` subqueries / derived tables
- correlated subqueries
- recursive CTEs
- richer window semantics beyond the current subset
- provider-specific function families such as regex, JSON operators, and advanced date/time functions

## Recommended Principle

A provider is "working" when it can execute a rational subset of rel shapes and the runtime can glue the rest together safely.

Do not wait for complete `rel-core` or `rel-advanced` support before shipping.
