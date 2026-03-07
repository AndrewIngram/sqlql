# Provider Capability Matrix

This document maps route families, capability atoms, and practical SQL coverage.

Route families are coarse. Capability atoms are the real unit of support.

## Route Families

Use route families for rough maturity only:

- `scan`
- `lookup`
- `aggregate`
- `rel-core`
- `rel-advanced`

Examples:

- a provider can be in `rel-core` while still missing `join.right_full`
- a provider can support `cte.non_recursive` without any window atoms

## Starter Atom Bundles

### Scan-Only Relational

Minimum atoms:

- `scan.project`
- `scan.filter.basic`
- `scan.sort`
- `scan.limit_offset`

Unlocks:

- single-table scans with local fallback for joins, aggregates, and advanced expressions

### Lookup-First / KV

Minimum atoms:

- `lookup.bulk`

Optional additions:

- `scan.project`
- `scan.filter.basic`
- `scan.limit_offset`

Unlocks:

- cross-provider lookup joins
- useful point-lookup workflows without pretending to support general scans

### Aggregate-Capable

Add:

- `aggregate.group_by`
- `aggregate.having`

Unlocks:

- grouped aggregate pushdown where the backend supports it

### Rel-Core Starter

Add:

- `join.inner`
- `join.left`
- `set_op.union_all`
- `set_op.union_distinct`

Typical next additions:

- `join.right_full`
- `set_op.intersect`
- `set_op.except`

Unlocks:

- common same-provider joins and set operations as one remote fragment

### Rel-Advanced Incremental

Add selectively:

- `cte.non_recursive`
- `window.rank_basic`
- `window.aggregate_default_frame`
- `window.frame_explicit`
- `window.navigation`
- `subquery.from`
- `subquery.correlated`

Do not implement these as a bundle. Add them atom-by-atom.

## Expression Atoms

Current first expression layer:

- `expr.compare_basic`
- `expr.like`
- `expr.in_not_in`
- `expr.null_distinct`
- `expr.arithmetic`
- `expr.case_simple`
- `expr.case_searched`
- `expr.string_basic`
- `expr.numeric_basic`
- `expr.cast_basic`

These map to local runtime support for:

- `LIKE`, `NOT LIKE`
- `NOT IN`
- `IS DISTINCT FROM`, `IS NOT DISTINCT FROM`
- arithmetic `+ - * / %`
- `LOWER`, `UPPER`, `TRIM`, `LENGTH`, `SUBSTR`
- `COALESCE`, `NULLIF`
- `ABS`, `ROUND`
- `CAST`
- searched `CASE`

Provider pushdown of computed expressions is still incremental. Scan-filter pushdown is the expected first step.

## Diagnostics and Policy

Unsupported or expensive shapes should be reported with:

- route family
- required atoms
- missing atoms
- stable `SQLQL_*` diagnostic code
- SQLSTATE-like class

Fallback policy decides whether those diagnostics are:

- warnings with local fallback
- hard errors

Important policy knobs:

- `allowFallback`
- `warnOnFallback`
- `rejectOnMissingAtom`
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

A provider is "working" when it can execute a rational subset of atoms and the runtime can glue the rest together safely.

Do not wait for complete `rel-core` or `rel-advanced` support before shipping.
