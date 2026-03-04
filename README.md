# sqlql

_Warning:_ Currently very rough and LLM-generated, not ready for production use.

`sqlql` is a TypeScript library for exposing a SQL interface over arbitrary data sources.
You define a logical schema and provider adapters, then users write SQL and `sqlql` executes by planning relational fragments across providers.

The SQL surface stays relational (`table`, `view`), while provider internals are source-neutral (`DataSourceAdapter`, `DataEntityHandle`) so non-relational systems (Redis/Elasticsearch/MongoDB) fit naturally.

## Why

One core motivation is AI tooling.

If you are building tools that accept SQL input, directly exposing production databases is usually a bad fit: security boundaries, tenancy boundaries, query cost, and coupling risks show up immediately.

`sqlql` gives you a controlled middle layer:

- expose only an allowlisted logical schema
- map table access to provider adapters (`canExecute`, `compile`, `execute`, optional `lookupMany`)
- keep control over what data is queryable and how it is fetched

This keeps SQL ergonomics for agents and developers without requiring direct DB connectivity from the tool runtime.

For LLM-driven tools specifically, a SQL interface gives the model flexible retrieval patterns while minimizing how much raw data needs to be injected into the context window, all through a single tool surface.

## Guides

- Schema guide: [`docs/defining-your-schema.md`](./docs/defining-your-schema.md)
- Integration guide: [`docs/integrating-with-your-system.md`](./docs/integrating-with-your-system.md)
- Planner and provider API overview: [`docs/resolver-plan-api.md`](./docs/resolver-plan-api.md)
- Upgrade guide (v0 -> v1): [`docs/upgrade-v1.md`](./docs/upgrade-v1.md)

## Conceptual limits and non-goals

`sqlql` intentionally does not try to be a full database.

Explicit non-goals:

- write statements (`INSERT`, `UPDATE`, `DELETE`)

Currently unsupported query shapes:

- recursive CTEs
- correlated subqueries
- subqueries in `FROM`

Accepted limitation (relational data sources):

- provider pushdown is capability-driven; unsupported fragments fall back to local execution paths.
- cross-provider joins can route via lookup joins when the target provider exposes `lookupMany`.
- recursive CTE pushdown and correlated-subquery pushdown are still limited.

## Quick usage

Install:

```bash
pnpm add sqlql
```

Minimal end-to-end flow:

```ts
import { defineProviders, defineSchema, query } from "sqlql";

const schema = defineSchema({
  tables: {
    orders: {
      provider: "warehouse",
      columns: {
        id: "text",
        org_id: "text",
        user_id: "text",
        total_cents: "integer",
      },
    },
    users: {
      provider: "warehouse",
      columns: {
        id: "text",
        email: "text",
      },
    },
  },
});

const providers = defineProviders({
  warehouse: {
    canExecute() {
      return true;
    },
    async compile(fragment) {
      return { provider: "warehouse", kind: fragment.kind, payload: fragment };
    },
    async execute(_plan) {
      return [];
    },
    async lookupMany(_request) {
      return [];
    },
  },
});

const rows = await query({
  schema,
  providers,
  context: {},
  sql: `
    SELECT o.id, u.email
    FROM orders o
    JOIN users u ON o.user_id = u.id
    WHERE o.org_id = 'org_1'
    LIMIT 50
  `,
});
```

## Provider contract

Providers expose fragment planning/execution and optional lookup joins:

- `canExecute(fragment, context)`
- `compile(fragment, context)`
- `execute(compiled, context)`
- `lookupMany(request, context)` (optional, used for cross-provider lookup joins)
- `estimate(fragment, context)` (optional)

Cross-provider joins:

- `lookupMany` is used for lookup-join plans across providers.
- There is currently no explicit boundary/domain field in provider registration.

First-party provider packages:

- `@sqlql/drizzle`
- `@sqlql/objection`
- `@sqlql/kysely`

Schema DSL typed refs:

- `table(...)` definitions can be passed directly to `rel.scan(tableRef)`.
- `col(tableRef, "column")` is type-checked against that table's logical columns.
- String refs remain supported for compatibility (`rel.scan("table")`, `col("table.column")`).

Prisma is intentionally not shipped in v1. The recommended path is a custom provider using raw SQL execution hooks until a dedicated adapter lands.

## Column capabilities

Capabilities are defined per column:

- `filterable?: boolean` (default `true`)
- `sortable?: boolean` (default `true`)
- `enum?: readonly string[]` (text columns only)
- `description?: string`

`sqlql` v1 execution safety is query-level (`queryGuardrails` input).

## Query guardrails

`query(...)` accepts optional global guardrails:

- `maxPlannerNodes`
- `maxExecutionRows`
- `maxLookupKeysPerBatch`
- `maxLookupBatches`
- `timeoutMs`

## Enums and CHECK constraints

- Column `enum` metadata emits deterministic `CHECK (... IN (...))` in DDL.
- Linked enum domains are supported with `enumFrom` (+ optional `enumMap`), with strict unmapped-value validation.
- Structured table checks are supported via `constraints.checks` (`kind: "in"`).
- Column-level constraints are supported directly on columns: `primaryKey`, `unique`, `foreignKey`.
- Table-level constraints remain available for composite keys/uniques/FKs via `constraints.*`.
- `toSqlDDL(...)` emits compact column metadata comments (`filterable:*`, `sortable:*`, `format:iso8601` for timestamps) and table-level policy metadata as JSON (`sqlql: query:{...}`).
- Optional runtime `constraintValidation` modes (`warn`/`error`) include enum/CHECK violations on returned rows.

## In-memory prototyping

For demos/tests, register a lightweight provider that reads from in-memory row arrays and implements `scan` + optional `lookupMany`.

## Security model

- `sqlql` does not provide authorization or tenancy guarantees by itself.
- Provider adapters must enforce access control and data security.
- `sqlql` can add query-shape guardrails, but security guarantees come from your application/storage layer.

## Performance philosophy

- `sqlql` should be reasonably efficient and avoid obvious over-fetching.
- It should use pragmatic optimizations when available (projection pushdown, filter pushdown, lookup routing, aggregate routing).
- It can execute independent branches in parallel (for example set-op branches and independent CTEs).
- It is not a full cost-based optimizer.
- Correctness, safety, and predictable behavior are prioritized over aggressive optimization.

## SQLite alignment and feature status

Parser alignment:

- single in-house parser targeting SQLite SQL (baseline aligned to SQLite 3.51 semantics)
- no parser fallback/workaround paths

Supported:

- `SELECT` queries
- `INNER JOIN`, `LEFT JOIN`, `RIGHT JOIN`, `FULL JOIN`
- boolean `WHERE` predicates (`AND`, `OR`, `NOT`)
- `IN`, `BETWEEN`, `IS NULL`, `IS NOT NULL`
- aggregates (`COUNT`, `SUM`, `AVG`, `MIN`, `MAX`) and `HAVING`
- `SELECT DISTINCT`
- `UNION ALL`, `UNION`, `INTERSECT`, `EXCEPT`
- uncorrelated subqueries (`IN (SELECT ...)`, `EXISTS`, scalar subqueries)
- non-recursive CTEs (`WITH ...`)
- window functions (core set): `ROW_NUMBER`, `RANK`, `DENSE_RANK`, `LEAD`, `LAG`, and aggregate windows
- `ORDER BY` on selected output aliases (including window output aliases)
- `toSqlDDL(...)` with SQLite-oriented output (`TEXT`/`INTEGER`) and timestamp metadata comments

Not yet supported:

- write statements (`INSERT`, `UPDATE`, `DELETE`)
- recursive CTEs
- correlated subqueries
- subqueries in `FROM`
- advanced window frame clauses (beyond `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`)
- some navigation/value window functions (`FIRST_VALUE`, `LAST_VALUE`, etc.)

## Playground

The playground is a Vite + React app for interactive exploration with three top-level tabs:

- `PostgreSQL`:
  - editable downstream data/structure workspace for pglite + Drizzle (plus playground KV rows)
  - table browser + row editor + generated downstream DDL
- `SQLQL Schema`:
  - TypeScript module editor for the **facade schema** (`export const schema = defineSchema(...)`)
  - Monaco TypeScript diagnostics + API IntelliSense (typed refs and DSL helpers)
  - relation diagram (React Flow) from declared foreign keys
  - generated DDL viewer (syntax-highlighted SQL)
- `Query`:
  - query preset selector
  - runtime lens context controls (`orgId`, `userId`)
  - compatibility-aware query picker (incompatible queries are disabled with reasons)
  - compact one-line SQL preview that expands into Monaco on focus
  - auto-run on valid schema/data/query (no manual run button)
  - `Result` and `Explain` tabs with plan graph + step overlay details
  - executed provider operations panel (SQL queries + KV lookups)

Run:

```bash
pnpm example:playground:dev
```

Build / preview:

```bash
pnpm example:playground:build
pnpm example:playground:start
```

## Facade example (Drizzle)

Optional example showing a restricted SQL facade over a Drizzle-backed store.

Run:

```bash
pnpm example:drizzle:build
pnpm example:drizzle:start
```

## Contributing (quick start)

```bash
pnpm install
pnpm build
pnpm test
pnpm typecheck
pnpm example:playground:dev
```
