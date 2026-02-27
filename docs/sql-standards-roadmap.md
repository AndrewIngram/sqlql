# Incremental SQL Standards Roadmap

This roadmap defines how `sqlql` expands SQL support while keeping the user-facing API stable:

1. Define schema.
2. Define table methods (`scan`, optional `lookup`, optional `aggregate`).
3. Execute SQL with `query(...)`.

Planning remains internal; users write SQL, not plans.

## Current Baseline (v0.1.x)

Implemented today:

- `SELECT ... FROM ...`
- `INNER JOIN ... ON a = b` (equality joins)
- `LEFT JOIN ... ON a = b`
- `RIGHT JOIN ... ON a = b`
- `FULL JOIN ... ON a = b`
- `WHERE` with boolean predicate trees (`AND`, `OR`, `NOT`)
- Operators: `=`, `!=`, `<>`, `>`, `>=`, `<`, `<=`, `IN`, `IS NULL`, `IS NOT NULL`
- `ORDER BY` column refs
- `LIMIT`, `OFFSET`
- `GROUP BY` + aggregate functions: `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`
- `COUNT(DISTINCT col)`
- `HAVING` with aggregate expressions
- `SELECT DISTINCT`
- Set operations: `UNION ALL`, `UNION`, `INTERSECT`, `EXCEPT`
- Subqueries in predicates: `IN (SELECT ...)`, `EXISTS (SELECT ...)`
- Scalar subqueries in `WHERE` and `SELECT`
- Non-recursive `WITH` CTEs
- Aggregate route (`aggregate(...)`) with local fallback when route is unavailable
- Join-aware dependency pushdown via `scan(...)` and optional `lookup(...)`

Currently rejected (not yet implemented):

- Correlated subqueries
- Subqueries in `FROM`
- Recursive CTEs

Target direction:

- SQL standards compliance for read queries over time.
- Keep parser/planner/executor behavior converging with SQLite parity for supported subsets.
- Use a single parser mode (`node-sql-parser` default dialect), with no parser fallbacks/workarounds.
- Continue expanding feature support milestone by milestone.
- Keep performance pragmatic: semi-optimal pushdown and batching where possible, without pursuing full database-style optimization.

## Milestones

### M1: Predicate Richness

Goal: support richer filters with controlled planning complexity.

Status: complete for runtime semantics; planner normalization remains future work.

Execution contract impact:

- No new public table methods.
- `scan(...)` receives richer normalized predicate structures.

### M2: Post-Aggregation Filtering

Goal: unlock common analytical SQL patterns.

Status: complete for `HAVING`; aggregate-route optimizations can be expanded.

Execution contract impact:

- No new public methods.
- Planner inserts post-aggregate filter steps when needed.

### M3: Set Operations

Goal: enable report composition and unionable result pipelines.

Status: complete for `UNION ALL`, `UNION`, `INTERSECT`, and `EXCEPT`.

Execution contract impact:

- Introduce internal set-op steps over row sets.
- No resolver API changes required.

### M4: Distinct and Join Expansion

Goal: close major SQL gaps for read-only workflows.

Status: complete for current target (`SELECT DISTINCT`, `LEFT/RIGHT/FULL JOIN`).

Execution contract impact:

- No new public methods.
- Planner/executor adds dedicated distinct/join strategy steps.

### M5: Subqueries

Goal: improve expressiveness while keeping execution safe.

Status: partial. Uncorrelated `IN`, `EXISTS`, and scalar subqueries are implemented.
Correlated subqueries and `FROM` subqueries remain pending.

Execution contract impact:

- No new public methods.
- Planner lowers supported subqueries to existing join/filter/aggregate steps.

## Writes (Explicit Non-Goal for Now)

Writes remain explicitly unsupported in v0.x.

Design reservation only:

- Keep IR and capability surfaces open so keyed writes can be introduced later.
- Do not imply write semantics/transactions in current API behavior.
- Continue rejecting write SQL statements with clear errors.

## Performance Positioning

Performance is important but not the primary goal.

- `sqlql` should avoid obvious inefficiencies and over-fetching.
- It should exploit available capabilities in underlying methods (`scan`, `lookup`, `aggregate`) for practical efficiency.
- It is not intended to compete with database engines on optimizer sophistication.
- If a workload needs deep cost-based optimization, push computation to the backing store or specialized engine.

## Security Boundary

`sqlql` is a query/planning/runtime layer, not an authorization system.

- The underlying domain/storage methods are responsible for enforcing security guarantees.
- Tenant scoping, row/column access control, and sensitive-data restrictions must be implemented in domain logic.
- `sqlql` should not be treated as the source of truth for authorization correctness.

## Compatibility Matrix

| Feature                             | Parser              | Planner             | Executor            | Resolver method           |
| ----------------------------------- | ------------------- | ------------------- | ------------------- | ------------------------- |
| Basic select/join/filter            | done                | done                | done                | `scan`, optional `lookup` |
| Offset/null checks                  | done                | done                | done                | `scan`                    |
| Aggregates/group by                 | done                | done                | done                | `aggregate` (preferred)   |
| Non-recursive CTE                   | done                | done                | done                | none new                  |
| OR/NOT                              | done                | done                | done                | `scan`                    |
| HAVING                              | done                | done                | done                | `aggregate`/local         |
| Set ops (`UNION ALL`/`UNION`)       | done                | done                | done                | none new                  |
| Set ops (`INTERSECT`/`EXCEPT`)      | done                | done                | done                | none new                  |
| DISTINCT                            | done                | done                | done                | none new                  |
| Outer joins (`LEFT`/`RIGHT`/`FULL`) | done                | done                | done                | none new                  |
| Subqueries (uncorrelated)           | done                | done                | done                | none new                  |
| Subqueries (correlated/from)        | planned             | planned             | planned             | none new                  |
| Writes (`INSERT/UPDATE/DELETE`)     | explicit no-support | explicit no-support | explicit no-support | none                      |

## Release Gate for Each Milestone

Each milestone is complete only when all are true:

- Parser acceptance tests for supported syntax and clear unsupported errors.
- Planner tests showing step graph and pushdown decisions.
- Dual-engine integration parity tests (`sqlql` vs SQLite) for supported shapes.
- `explain(...)` output updated to reflect new plan decisions.
