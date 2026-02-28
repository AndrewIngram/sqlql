# sqlql

_Warning:_ Currently very rough and LLM-generated, not ready for production use.

`sqlql` is a TypeScript library for exposing a SQL interface over arbitrary data sources.

You define:

- a schema (tables + columns)
- table methods (`scan`, optional `lookup`, optional `aggregate`)

Then users write SQL queries, and `sqlql` parses and executes them by calling your methods.

## SQL capabilities (v0.2.x)

Parser policy:

- single parser mode via `node-sql-parser` default dialect
- no parser fallback/workaround paths

Supported:

- `SELECT` queries
- `INNER JOIN`, `LEFT JOIN`, `RIGHT JOIN`, and `FULL JOIN`
- boolean `WHERE` predicates (`AND`, `OR`, `NOT`)
- `IN`, `BETWEEN`, `IS NULL`, `IS NOT NULL`
- aggregates (`COUNT`, `SUM`, `AVG`, `MIN`, `MAX`) and `HAVING`
- `SELECT DISTINCT`
- `UNION ALL`, `UNION`, `INTERSECT`, and `EXCEPT`
- uncorrelated subqueries (`IN (SELECT ...)`, `EXISTS`, scalar subqueries)
- non-recursive CTEs (`WITH ...`)
- window functions (core set): `ROW_NUMBER`, `RANK`, `DENSE_RANK`, `COUNT/SUM/AVG/MIN/MAX ... OVER (...)`

Not yet supported:

- write statements (`INSERT`, `UPDATE`, `DELETE`)
- recursive CTEs
- correlated subqueries
- subqueries in `FROM`
- explicit window frame clauses
- named `WINDOW` clauses/references
- navigation window functions (`LEAD`, `LAG`, etc.)

## Schema constraints

`sqlql` supports schema-level metadata for:

- `PRIMARY KEY`
- `UNIQUE`
- `FOREIGN KEY`

This metadata is used to:

- improve schema communication/introspection for consumers (including LLMs)
- generate richer SQL DDL via `toSqlDDL(...)`

Optional query-time validation is available through `query(...)`:

- `constraintValidation.mode = "off" | "warn" | "error"`
- checks implemented now: `NOT NULL`, `PRIMARY KEY` uniqueness, `UNIQUE` uniqueness on retrieved table rows
- foreign-key runtime checks are intentionally deferred in this release

Important limitation:

- these constraints are metadata plus optional query-time checks; they do not guarantee at-rest integrity unless the underlying data store enforces them

## Why

One core motivation is AI tooling.

If you are building tools (for example with the AI SDK) that accept SQL as input, directly exposing your production database is usually a bad fit: security, tenancy boundaries, query cost, and brittle coupling all become immediate risks.

`sqlql` gives you a middle layer:

- expose only an allowlisted logical schema
- map table access to domain-aware methods (`scan`, `lookup`, `aggregate`)
- keep full control over what data can be queried and how it is fetched

That means you keep the ergonomic upside of SQL for agents and developers, without requiring direct DB connectivity from the tool runtime.

Security model:

- `sqlql` does not provide authorization or tenancy guarantees by itself.
- The underlying domain methods (`scan`, `lookup`, `aggregate`) are responsible for enforcing access control and data-security constraints.
- `sqlql` can help with query-shape guardrails, but security guarantees must come from your domain/storage layer.

Current explicit non-goals:

- write statements (`INSERT`, `UPDATE`, `DELETE`)
- recursive CTEs
- correlated / `FROM` subqueries

Performance philosophy:

- `sqlql` should be reasonably efficient and avoid obvious over-fetching.
- It should use pragmatic optimizations (projection pushdown, filter pushdown, lookup batching, aggregate routing) when available.
- It can execute independent branches in parallel (set-op branches, independent CTEs, and eligible scan stages).
- It is not trying to be a full database or a cost-based optimizer.
- Correctness, safety, and predictable behavior are prioritized over aggressive optimization.

## Install

```bash
pnpm add sqlql
```

## Quick start

```ts
import {
  createArrayTableMethods,
  defineSchema,
  defineTableMethods,
  query,
  type QueryRow,
} from "sqlql";

const schema = defineSchema({
  tables: {
    orders: {
      columns: {
        id: "text",
        org_id: "text",
        user_id: "text",
        total_cents: "integer",
      },
    },
    users: {
      columns: {
        id: "text",
        email: "text",
      },
    },
  },
});

const tableData = {
  orders: [] as QueryRow<typeof schema, "orders">[],
  users: [] as QueryRow<typeof schema, "users">[],
};

const methods = defineTableMethods(schema, {
  orders: createArrayTableMethods(tableData.orders),
  users: createArrayTableMethods(tableData.users),
});

const rows = await query({
  schema,
  methods,
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

`createArrayTableMethods(...)` is useful for demos/tests where each table is a JSON-like object array.

## Step-by-step execution session (experimental)

For debugging and tooling UIs, you can create a query session and step execution manually:

```ts
import { createQuerySession } from "sqlql";

const session = createQuerySession({
  schema,
  methods,
  context: {},
  sql: "SELECT id FROM orders",
});

const event = await session.next(); // one execution step
const rows = await session.runToCompletion(); // finish all remaining steps
const plan = session.getPlan(); // discovered execution steps
```

## DDL output

```ts
import { toSqlDDL } from "sqlql";

console.log(toSqlDDL(schema, { ifNotExists: true }));
```

## Facade example (Drizzle)

The Drizzle example demonstrates a restricted, consumer-oriented facade:

- internal DB has org tables and internal-only tables
- exposed schema omits internal plumbing (`organizations`, `org_id`, `admin_notes`)
- all table methods scope implicitly from `ctx.userId`

Run:

```bash
pnpm example:drizzle:build
pnpm example:drizzle:start
```

## Project structure

- `src/schema.ts`: schema and table method contracts
- `src/constraints.ts`: optional query-time constraint validation
- `src/parser.ts`: SQL parser adapter (`node-sql-parser`, single mode, no fallback)
- `src/query.ts`: SQL parsing + query execution
- `src/planning.ts`: planning model types
- `src/index.ts`: package entrypoint
- `packages/drizzle`: optional Drizzle adapter helpers (`@sqlql/drizzle`)
- `docs/sql-standards-roadmap.md`: incremental SQL support plan
- `docs/parser-known-issues.md`: parser-specific limitations and desired future behavior

## Local development

```bash
pnpm install
pnpm build
pnpm test
pnpm typecheck
```

Run the example:

```bash
pnpm example:build
pnpm example:start
pnpm example:drizzle:build
pnpm example:drizzle:start
```

Run compliance-focused parity tests:

```bash
pnpm test -- test/compliance
```

## Publish

```bash
pnpm publish --access public
```
