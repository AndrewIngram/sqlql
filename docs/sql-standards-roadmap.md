# Incremental SQL Standards Roadmap

This roadmap defines how `tupl` expands SQL support while keeping the user-facing API stable:

1. Define providers.
2. Build an executable schema from provider-owned entities.
3. Execute SQL with `executableSchema.query(...)`.

Planning remains internal; users write SQL, not plans.

## Baseline

Implemented:

- `SELECT ... FROM ...`
- `INNER JOIN ... ON a = b` (equality joins)
- `LEFT JOIN ... ON a = b`
- `RIGHT JOIN ... ON a = b`
- `FULL JOIN ... ON a = b`
- `WHERE` with boolean predicate trees (`AND`, `OR`, `NOT`)
- Operators: `=`, `!=`, `<>`, `>`, `>=`, `<`, `<=`, `IN`, `NOT IN`, `BETWEEN`, `LIKE`, `NOT LIKE`, `IS NULL`, `IS NOT NULL`, `IS DISTINCT FROM`, `IS NOT DISTINCT FROM`
- `ORDER BY` column refs and ordinals
- `LIMIT`, `OFFSET`
- `GROUP BY` + aggregate functions: `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`
- `GROUP BY` ordinals (including computed select-list expressions via local fallback)
- `COUNT(DISTINCT col)`
- `HAVING` with aggregate expressions
- `SELECT DISTINCT`
- Set operations: `UNION ALL`, `UNION`, `INTERSECT`, `EXCEPT`
- Subqueries in predicates: `IN (SELECT ...)`, `EXISTS (SELECT ...)`
- Scalar subqueries in `WHERE` and `SELECT`
- Non-recursive `WITH` CTEs
- First local scalar-expression layer for read queries:
  - arithmetic `+`, `-`, `*`, `/`, `%`
  - string concat
  - `LOWER`, `UPPER`, `TRIM`, `LENGTH`, `SUBSTR`
  - `COALESCE`, `NULLIF`
  - `ABS`, `ROUND`
  - `CAST`
  - searched `CASE`
- Core window functions:
  - ranking: `ROW_NUMBER`, `RANK`, `DENSE_RANK`
  - aggregate windows: `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`
  - `PARTITION BY` + `ORDER BY` with default frame behavior
- Provider capability helper vocabulary and route-family diagnostics for pushdown/rejection decisions
- Structured fallback diagnostics with SQLSTATE-like classes
- Query/runtime fallback policy controls for unsupported or expensive provider pushdown
- Rel-first provider compilation with local fallback when a provider rejects a subtree
- Optional keyed lookup helpers for targeted execution optimizations
- Dependency-aware parallel execution for independent branches:
  - set-op branches
  - independent CTE branches
  - eligible source scan stages
- Opt-in step execution sessions via `executableSchema.createSession(...)`
- Schema constraint metadata: `PRIMARY KEY`, `UNIQUE`, `FOREIGN KEY`
- Structured `CHECK` metadata (`kind: "in"`) and enum-derived checks
- Optional query-time constraint validation modes: `off`, `warn`, `error`
  - runtime checks: `NOT NULL`, primary-key uniqueness, unique-key uniqueness, enum/CHECK validation
  - foreign-key runtime checks are not implemented

Unsupported:

- Computed-expression pushdown is still partial and adapter-specific
- Cost-based physical planning
- Some provider-specific advanced rel pushdown shapes
- Writes (`INSERT`, `UPDATE`, `DELETE`)

Target direction:

- SQL standards compliance for read queries over time.
- Keep parser/planner/executor behavior converging with SQLite parity for supported subsets.
- Use a single in-house parser targeting SQLite SQL, with no parser fallbacks/workarounds.
- Treat schema constraints as communication-first metadata and optional runtime checks (not at-rest guarantees).
- Defer index metadata and index-driven planning until constraint semantics are fully settled.
- Continue expanding feature support milestone by milestone.
- Keep performance pragmatic: semi-optimal pushdown and batching where possible, without pursuing full database-style optimization.
- Reintroduce optional capability/pushdown policy hints only as explicit performance controls (future).
  Legacy `filterable`/`sortable` and query reject/fallback policy knobs were removed from the core API and may return later only as opt-in performance hints.

## Milestones

### M1: Predicate Richness

Goal: support richer filters with controlled planning complexity.

Status: complete for runtime semantics; planner normalization is out of scope for this milestone.

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

Status: complete for target scope (`SELECT DISTINCT`, `LEFT/RIGHT/FULL JOIN`).

Execution contract impact:

- No new public methods.
- Planner/executor adds dedicated distinct/join strategy steps.

### M5: Subqueries

Goal: improve expressiveness while keeping execution safe.

Status: complete for the current read-query target, including correlated forms in the supported decorrelatable subset and derived tables in `FROM`.

Execution contract impact:

- No new public methods.
- Planner lowers supported subqueries into canonical rel plus decorrelation rewrites.

## Writes (Explicit Non-Goal)

Writes remain explicitly unsupported.

Design reservation only:

- Keep IR and capability surfaces open so keyed writes can be introduced later.
- Do not imply write semantics/transactions in API behavior.
- Continue rejecting write SQL statements with clear errors.

## Performance Positioning

Performance is important but not the primary goal.

- `tupl` should avoid obvious inefficiencies and over-fetching.
- It should exploit available provider subtree pushdown and optional helper-level optimizations for practical efficiency.
- It is not intended to compete with database engines on optimizer sophistication.
- If a workload needs deep cost-based optimization, push computation to the backing store or specialized engine.

## Security Boundary

`tupl` is a query/planning/runtime layer, not an authorization system.

- The underlying domain/storage methods are responsible for enforcing security guarantees.
- Tenant scoping, row/column access control, and sensitive-data restrictions must be implemented in domain logic.
- `tupl` should not be treated as the source of truth for authorization correctness.

## Compatibility Matrix

| Feature                                  | Parser              | Planner | Executor | Provider contract impact    |
| ---------------------------------------- | ------------------- | ------- | -------- | --------------------------- |
| Basic select/join/filter                 | done                | done    | done     | rel subtree compile/execute |
| Aggregates/group by/having               | done                | done    | done     | rel subtree compile/execute |
| Non-recursive and recursive CTE          | done                | done    | done     | none new                    |
| Set ops (`UNION`/`INTERSECT`/`EXCEPT`)   | done                | done    | done     | none new                    |
| Derived tables and correlated subqueries | done                | done    | done     | none new                    |
| Window functions (current supported set) | done                | done    | done     | none new                    |
| Branch-level parallel execution          | n/a                 | done    | done     | none new                    |
| Step-by-step query session API           | n/a                 | done    | done     | none new                    |
| Constraint runtime validation            | n/a                 | n/a     | done     | none new                    |
| Writes (`INSERT/UPDATE/DELETE`)          | explicit no-support | n/a     | n/a      | none                        |

## Release Gate for Each Milestone

Each milestone is complete only when all are true:

- Parser acceptance tests for supported syntax and clear unsupported errors.
- Planner tests showing step graph and pushdown decisions.
- Dual-engine integration parity tests (`tupl` vs SQLite) for supported shapes.
- `explain(...)` output updated to reflect new plan decisions.

Compliance test locations:

- `test/compliance/*-parity.test.ts`: curated sqllogictest-style parity scenarios split by capability.
- `test/compliance/standards-gaps.todo.test.ts`: explicit standards-gap TODOs for not-yet-supported SQL features.
- `docs/parser-known-issues.md`: in-house parser behavior notes and known gaps.
