# SQL Standards Roadmap

This document describes the incremental plan for supporting SQL features in sqlql,
driven by missing capabilities. Each feature is classified as either a **query affordance**
(handled entirely inside the executor, no new resolver API) or a **new API capability**
(requires the resolver/method layer to expose a new hook or operator).

---

## Currently Supported (Baseline)

- `SELECT col, ...` with table aliases
- `FROM table AS alias`
- `INNER JOIN` / `JOIN` with equality condition
- `WHERE` with `AND`-connected scalar comparisons (`=`, `!=`, `<>`, `>`, `>=`, `<`, `<=`)
- `WHERE col IN (...)` — literal list membership
- `ORDER BY col ASC|DESC`
- `LIMIT n`
- `SELECT *` (single-table only)
- Schema definition with `filterable`, `sortable`, `maxRows` constraints
- `scan`, optional `lookup`, optional `aggregate` method hooks

---

## Tier 1 — SQL-92 Query Affordances (no new API)

These features are handled entirely in the executor layer. The `scan` method signature
(`TableScanRequest`) already covers them, and user code does not need to change.

### OFFSET ✅ Implemented
**Classification:** Query affordance — executor applies `LIMIT n OFFSET m`

`LIMIT n OFFSET m` syntax. For single-table queries, the offset is pushed down to the
`TableScanRequest.offset` field. For multi-table queries (joins), offset is applied
in memory after joining and sorting.

```sql
SELECT o.id FROM orders o ORDER BY o.created_at DESC LIMIT 10 OFFSET 20
```

### DISTINCT ✅ Implemented
**Classification:** Query affordance — executor deduplicates projected rows

`SELECT DISTINCT` removes duplicate rows from the result after projection.
Applied in memory post-scan; not pushed down to the scan method.

```sql
SELECT DISTINCT o.status FROM orders o
```

### LEFT (OUTER) JOIN ✅ Implemented
**Classification:** Query affordance — executor uses null-padding for non-matches

Preserves all rows from the left table, filling right-side columns with `null`
when no matching right-side row exists. Left join conditions are intentionally
**not** used as dependency scan edges (since the right side may have no rows).

```sql
SELECT o.id, u.email FROM orders o LEFT JOIN users u ON o.user_id = u.id
```

### BETWEEN / NOT BETWEEN ✅ Implemented
**Classification:** Query affordance — desugared to `>=`/`<=` pairs at parse time

`x BETWEEN a AND b` is desugared to `x >= a AND x <= b`, which are pushed down
to the scan request. `x NOT BETWEEN a AND b` desugars to `x < a OR x > b`,
which is applied as a post-scan predicate (OR is not pushdownable).

```sql
WHERE o.total_cents BETWEEN 1000 AND 5000
WHERE o.created_at NOT BETWEEN '2024-01-01' AND '2024-12-31'
```

### OR Predicates ✅ Implemented
**Classification:** Query affordance — post-scan predicate evaluation

`WHERE` clauses containing `OR` cannot be pushed down to individual scan calls.
The executor parses the full predicate tree, pushes down the AND-connected leaf
conditions (optimization), and applies the full predicate post-join in memory.

Cross-table OR (e.g., `WHERE o.status = 'paid' OR u.tier = 'enterprise'`) is
evaluated against joined row bundles. Single-table OR (most common) evaluates
on a single table's rows.

```sql
WHERE o.status = 'paid' OR o.total_cents > 5000
WHERE (o.org_id = 'org1' AND o.status = 'pending') OR o.total_cents > 10000
```

### NOT Predicate (Future)
**Classification:** Query affordance — post-scan predicate evaluation

`WHERE NOT (predicate)` is not yet parsed, but the `WhereNode` type already
includes a `not` variant. Once added, it will follow the same post-scan evaluation
path as OR. Simple cases like `NOT (col = val)` can be pushed down as `neq`.

---

## Tier 1 — SQL-92 New API Operators

These features extend the `ScanFilterClause` type, adding new operators that the
resolver's `scan` method receives and must handle (or they can be evaluated
in-memory by the executor if the scan ignores unknown operators).

### IS NULL / IS NOT NULL ✅ Implemented
**Classification:** New API operators — `is_null` / `is_not_null` in `ScanFilterClause`

Pushed down to the scan as `NullFilterClause` with `op: "is_null"` or `op: "is_not_null"`.
User-implemented `scan` methods that wish to push this to their data source must
handle these new operator values.

```sql
WHERE o.deleted_at IS NULL
WHERE o.archived_at IS NOT NULL
```

New types:
```ts
export interface NullFilterClause extends FilterClauseBase {
  op: "is_null" | "is_not_null";
}
```

### NOT IN ✅ Implemented
**Classification:** New API operator — `not_in` in `ScanFilterClause`

Pushed down as a `SetFilterClause` with `op: "not_in"`. Existing scan implementations
that exhaustively switch on `op` must add a `not_in` case.

```sql
WHERE o.status NOT IN ('cancelled', 'refunded')
```

### LIKE / NOT LIKE ✅ Implemented
**Classification:** New API operators — `like` / `not_like` in `ScanFilterClause`

Pushed down as `ScalarFilterClause` with `op: "like"` or `op: "not_like"`.
The `value` field contains the SQL pattern string (`%` = any chars, `_` = one char).

For scan implementations that cannot push LIKE to the data source, the executor
also evaluates LIKE patterns in-memory via `postWhere` when the WHERE contains OR.
For simple AND-only LIKE queries, the scan must either apply the filter or return
extra rows (which is safe since the executor does not double-filter in the simple
case).

```sql
WHERE o.description LIKE '%refund%'
WHERE u.email NOT LIKE '%@internal.example.com'
```

---

## Tier 2 — SQL-92 Aggregation (new API capability)

### GROUP BY + Aggregate Functions ✅ Implemented
**Classification:** New API capability — uses the `aggregate` method hook

Queries with `GROUP BY` or aggregate functions (`COUNT`, `SUM`, `AVG`, `MIN`, `MAX`)
in `SELECT` are routed to a separate execution path:

1. **Single-table with `aggregate` method**: delegates directly to `method.aggregate()`
   with a `TableAggregateRequest` containing `groupBy`, `metrics`, and `where`.
2. **Fallback (no `aggregate` method, or multi-table)**: scans + joins rows in memory,
   then aggregates in memory using JavaScript.

```sql
SELECT o.org_id, COUNT(*) as cnt, SUM(o.total_cents) as total
FROM orders o
WHERE o.status = 'paid'
GROUP BY o.org_id
```

`TableAggregateRequest` (already in `@sqlql/core`):
```ts
interface TableAggregateRequest {
  table: string;
  alias?: string;
  where?: ScanFilterClause[];
  groupBy?: string[];
  metrics: TableAggregateMetric[];
  limit?: number;
}
```

#### COUNT(DISTINCT col)
`COUNT(DISTINCT col)` is parsed and passed to the aggregate method with
`distinct: true` on the metric. The in-memory fallback also handles distinct
aggregation.

---

## Tier 3 — SQL-92/99 (Planned)

The following features are identified as next priorities. They are listed roughly
in order of implementation complexity.

### HAVING Clause
**Classification:** New API capability — post-aggregate filter

`HAVING COUNT(*) > 5` filters the result of GROUP BY. For the delegate path,
the filter must be sent to the aggregate method. For the in-memory path, it is
applied after aggregation.

Needs: extend `TableAggregateRequest` with a `having?: ScanFilterClause[]` field.

```sql
SELECT o.org_id, COUNT(*) as cnt FROM orders o GROUP BY o.org_id HAVING COUNT(*) > 2
```

### UNION / UNION ALL
**Classification:** Query affordance — executor merges result sets

`UNION` deduplicates, `UNION ALL` keeps duplicates. Both can be implemented as
executor-level set operations on two separately-executed query result arrays.

```sql
SELECT id FROM orders WHERE status = 'paid'
UNION ALL
SELECT id FROM orders WHERE status = 'pending'
```

### Subqueries
**Classification:** New API capability (correlated) / query affordance (uncorrelated)

Uncorrelated subqueries (`WHERE x IN (SELECT ...)`) can be pre-executed and
converted to `IN (...)` lists. Correlated subqueries require per-row execution
and are significantly more complex.

```sql
WHERE o.user_id IN (SELECT id FROM users WHERE tier = 'enterprise')
```

### CTEs (WITH clause)
**Classification:** Query affordance — the planner executes the CTE once and
materializes the result as an in-memory table

The planning IR already has `CteBinding` and `CteStep` types. The executor
needs to evaluate CTEs before the main query.

```sql
WITH paid_orders AS (SELECT * FROM orders WHERE status = 'paid')
SELECT * FROM paid_orders WHERE total_cents > 1000
```

---

## Tier 4 — SQL:1999 and Beyond (Future)

| Feature | Classification | Notes |
|---------|---------------|-------|
| Window functions (`OVER`) | New API capability | Requires per-row context; complex to delegate |
| `CASE` expressions | Query affordance | In-memory computed columns |
| `COALESCE` / `NULLIF` | Query affordance | In-memory computed columns |
| `FULL OUTER JOIN` | Query affordance | Union of left+right results |
| `CROSS JOIN` | Query affordance | Cartesian product |
| Scalar subqueries in SELECT | New API capability | Returns single value per row |
| `RECURSIVE` CTEs | New API capability | Requires iterative execution |
| `ROLLUP` / `CUBE` / `GROUPING SETS` | New API capability | Multi-level GROUP BY |
| `FETCH FIRST n ROWS ONLY` | Query affordance | Alias for LIMIT |
| `LATERAL` joins | New API capability | Per-row subquery execution |
| JSON operators | New API capability | Extend schema type system |

---

## API Extension Summary

The following changes have been made to `@sqlql/core` to support Tier 1–2 features:

**New `ScanFilterOperator` values:**
- `not_in` — negated set membership (`NOT IN (...)`)
- `is_null` — null check (`IS NULL`)
- `is_not_null` — non-null check (`IS NOT NULL`)
- `like` — SQL pattern match (`LIKE 'foo%'`)
- `not_like` — negated SQL pattern match (`NOT LIKE 'foo%'`)

**New `ScanFilterClause` variant:**
- `NullFilterClause` — for `is_null` / `is_not_null` (no `value` field)

Existing `SetFilterClause` now accepts `"not_in"` in its `op` field.
Existing `ScalarFilterClause` now accepts `"like"` and `"not_like"` in its `op` field.

Scan implementations should handle all new operators or return unfiltered rows
(the executor applies post-join predicate evaluation for complex cases).
