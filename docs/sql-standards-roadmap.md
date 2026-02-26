# SQL Standards Roadmap

This document describes the incremental plan for supporting SQL features in sqlql,
driven by missing capabilities. Each feature is classified as either a **query affordance**
(handled entirely inside the executor, no new resolver API) or a **new API capability**
(requires the resolver/method layer to expose a new hook or operator).

> **Scope note:** This roadmap is not exhaustive of every SQL clause or function that exists
> in the various SQL standards. It focuses on the features most commonly needed for data query
> workloads. Vendor-specific extensions (e.g., PostgreSQL JSON operators, MySQL-specific syntax)
> are out of scope unless they are widely adopted.

---

## SQL Standards Conformance

### How servers declare compliance

The SQL standard is published by ISO/IEC as ISO/IEC 9075. Full conformance is rarely claimed
by any vendor. Instead, two practical models are used:

**SQL-92 conformance levels** — SQL-92 defined four tiers:
- **Entry** — the minimum subset most databases implement (basic SELECT/INSERT/UPDATE/DELETE,
  JOINs, WHERE operators, aggregate functions, basic type system)
- **Transitional / Intermediate / Full** — progressively stricter; almost no vendor reaches Full

Most databases (PostgreSQL, MySQL, SQLite, SQL Server) describe themselves as "Entry SQL-92
compatible" and then document extensions and deviations in their own compatibility matrix.

**SQL:1999+ Feature IDs** — from SQL:1999 onward the tiered model was replaced with individual
Feature IDs (e.g., `F031` = CAST function, `T611` = Elementary OLAP operations, `T612–T615` =
window function subsets). Vendors can reference these IDs when documenting what they support.
PostgreSQL publishes a page listing each Feature ID and its support status.

### What developers typically expect

In practice, developers writing portable SQL expect coverage roughly equivalent to:

- **Entry SQL-92** — the near-universal baseline
- **Common table expressions (WITH)** — introduced in SQL:1999, adopted everywhere
- **Window functions (OVER / PARTITION BY)** — introduced in SQL:2003, now ubiquitous in analytics
- **`FETCH FIRST n ROWS ONLY`** — SQL:2008, widely adopted as an alternative to `LIMIT`

sqlql targets this "practical superset" as its long-term goal. The tiers below are ordered by
implementation priority and practical value.

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

## Tier 3 — SQL-92 Remaining (Planned)

These are all SQL-92 features. They are not yet implemented.

### CAST
**Classification:** Query affordance — in-memory type coercion on projected values

`CAST(expr AS type)` converts a value to a different SQL scalar type. For sqlql,
this is applied in-memory during projection; the scan method always sees the raw
value. CAST does not need to be pushed to the resolver because the schema already
declares each column's type.

```sql
SELECT CAST(o.total_cents AS text) FROM orders o
SELECT CAST('2024-01-01' AS timestamp) FROM orders o
```

The type set (`text`, `integer`, `boolean`, `timestamp`) maps directly to
`SqlScalarType` in `@sqlql/core`. Numeric narrowing (`CAST(x AS numeric(10,2))`)
and other precision-qualified types are deferred.

### CASE Expressions
**Classification:** Query affordance — in-memory computed column

`CASE WHEN ... THEN ... ELSE ... END` evaluated in memory during projection.
No resolver API change needed; the computed value is returned as a projected column.

```sql
SELECT o.id, CASE WHEN o.total_cents > 5000 THEN 'high' ELSE 'low' END AS tier
FROM orders o
```

Searched CASE (`CASE WHEN cond THEN val`) and simple CASE (`CASE col WHEN val THEN ...`)
both need to be supported.

### COALESCE / NULLIF
**Classification:** Query affordance — in-memory computed value

SQL-92 scalar functions applied in-memory during projection or in WHERE.

```sql
SELECT COALESCE(o.note, 'n/a') AS note FROM orders o
SELECT NULLIF(o.status, 'pending') AS status FROM orders o
```

### RIGHT (OUTER) JOIN
**Classification:** Query affordance — mirrors LEFT JOIN with sides swapped

`RIGHT JOIN` is equivalent to swapping the left and right tables and performing
a LEFT JOIN. Can be implemented by transposing the join operands before execution.

```sql
SELECT o.id, u.email FROM users u RIGHT JOIN orders o ON o.user_id = u.id
```

### FULL (OUTER) JOIN
**Classification:** Query affordance — union of LEFT JOIN and unmatched right rows

All rows from both sides, with `null`-padding where no match exists.
Implementable as a LEFT JOIN merged with the unmatched rows from a RIGHT JOIN.

```sql
SELECT o.id, u.email FROM orders o FULL OUTER JOIN users u ON o.user_id = u.id
```

### CROSS JOIN
**Classification:** Query affordance — cartesian product

No join condition; every left row is paired with every right row. Useful for
small reference tables or generating row sets.

```sql
SELECT o.id, c.currency FROM orders o CROSS JOIN currencies c
```

### NATURAL JOIN
**Classification:** Query affordance — implicit equi-join on same-name columns

Automatically joins on all columns with matching names between the two tables.
No resolver API change; the executor synthesises the join condition from the schema.

```sql
SELECT o.id, u.email FROM orders o NATURAL JOIN users u
```

### JOIN ... USING (col, ...)
**Classification:** Query affordance — shorthand equi-join on named columns

Equivalent to `ON left.col = right.col` for each listed column.

```sql
SELECT o.id, u.email FROM orders o JOIN users u USING (id)
```

### INTERSECT / EXCEPT
**Classification:** Query affordance — executor performs set operations on two result arrays

`INTERSECT` returns rows present in both result sets; `EXCEPT` returns rows in the
first set not present in the second. Both require the two result sets to have
compatible column shapes.

```sql
SELECT user_id FROM orders WHERE status = 'paid'
INTERSECT
SELECT user_id FROM orders WHERE total_cents > 1000

SELECT id FROM users
EXCEPT
SELECT user_id FROM orders
```

### EXISTS Subquery
**Classification:** Query affordance (uncorrelated) — new API capability (correlated)

`WHERE EXISTS (SELECT ...)` evaluates to `true` if the subquery returns any rows.
Uncorrelated EXISTS can be pre-executed once; correlated EXISTS requires per-row
evaluation and is significantly more complex.

```sql
WHERE EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id AND o.status = 'paid')
```

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

### Scalar String Functions
**Classification:** Query affordance — in-memory computed values

SQL-92 defines a core set of string functions applicable in SELECT and WHERE:

| Function | Description |
|----------|-------------|
| `UPPER(str)` / `LOWER(str)` | Case conversion |
| `TRIM([LEADING\|TRAILING\|BOTH] FROM str)` | Whitespace/char trimming |
| `SUBSTRING(str FROM n FOR m)` | Substring extraction |
| `CHAR_LENGTH(str)` | Character count |
| `POSITION(needle IN haystack)` | Substring position |
| `CONCAT(str, ...)` / `\|\|` operator | String concatenation |

### Scalar Datetime Functions
**Classification:** Query affordance — in-memory computed values / current-time literals

| Function | Description |
|----------|-------------|
| `CURRENT_DATE` | Current date (literal) |
| `CURRENT_TIME` | Current time (literal) |
| `CURRENT_TIMESTAMP` | Current timestamp (literal) |
| `EXTRACT(field FROM datetime)` | Extracts year/month/day/etc. |

### Subqueries (non-EXISTS)
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

## Tier 4 — SQL:1999–2003 (High Value)

These features are from SQL:1999 or SQL:2003 and are practically important enough to be
worth implementing despite their complexity.

### Window Functions (`OVER` / `PARTITION BY`) — SQL:2003, Feature T611–T615
**Classification:** New API capability — requires per-row window context

Window functions (`ROW_NUMBER`, `RANK`, `DENSE_RANK`, `LAG`, `LEAD`, `FIRST_VALUE`,
`LAST_VALUE`, `SUM OVER`, `AVG OVER`, etc.) are essential for data analytics and are
universally supported by modern SQL engines. They were standardised in SQL:2003 (Feature
IDs T611–T615) but have been in widespread use for decades.

For sqlql, window functions require a new API path — the resolver must either:
1. Delegate to `method.windowAggregate()` if the data source can compute them natively, or
2. Fall back to in-memory computation after scanning and sorting all rows.

The in-memory fallback is feasible for analytics workloads where data volumes are moderate;
for large datasets native delegation is preferable.

```sql
SELECT
  o.user_id,
  o.total_cents,
  SUM(o.total_cents) OVER (PARTITION BY o.user_id) AS user_total,
  ROW_NUMBER() OVER (PARTITION BY o.user_id ORDER BY o.created_at DESC) AS rn
FROM orders o
```

### CTEs (WITH clause) — SQL:1999
**Classification:** Query affordance — the planner executes the CTE once and
materializes the result as an in-memory table

The planning IR already has `CteBinding` and `CteStep` types. The executor
needs to evaluate CTEs before the main query.

```sql
WITH paid_orders AS (SELECT * FROM orders WHERE status = 'paid')
SELECT * FROM paid_orders WHERE total_cents > 1000
```

### ROLLUP / CUBE / GROUPING SETS — SQL:1999
**Classification:** New API capability — multi-level GROUP BY

`ROLLUP` and `CUBE` generate multiple levels of subtotals in a single query.
`GROUPING SETS` gives explicit control over which grouping combinations are produced.
Widely used in reporting and dashboards.

```sql
SELECT region, product, SUM(revenue) FROM sales GROUP BY ROLLUP(region, product)
```

---

## Tier 5 — SQL:1999+ (Low Value or High Complexity)

The following features are either rarely needed in practice, add complexity disproportionate
to their value, or are better served by application-level logic.

| Feature | Spec | Classification | Notes |
|---------|------|---------------|-------|
| `RECURSIVE` CTEs | SQL:1999 | New API capability | Useful for hierarchical data but complex to implement; requires iterative execution |
| `FETCH FIRST n ROWS ONLY` | SQL:2008 | Query affordance | Trivial alias for `LIMIT`; easy to add |
| Scalar subqueries in `SELECT` | SQL:1999 | New API capability | Returns a single value per row; complex to plan and delegate |
| `LATERAL` joins | SQL:1999 | New API capability | Per-row subquery execution; niche use cases |
| `MULTISET` / array types | SQL:2003 | New API capability | Collection column types; requires schema type system extension |

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

