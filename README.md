# sqlql

*Warning:* Currently very rough and LLM-generated, not ready for production use.

`sqlql` is a TypeScript library for exposing a SQL interface over arbitrary data sources.

You define:

- a schema (tables + columns)
- table methods (`scan`, optional `lookup`, optional `aggregate`)

Then users write SQL queries, and `sqlql` parses and executes them by calling your methods.

## SQL capabilities (v0.1.x)

Parser policy:

- single parser mode via `node-sql-parser` default dialect
- no parser fallback/workaround paths

Supported:

- `SELECT` queries
- `INNER JOIN`, `LEFT JOIN`, `RIGHT JOIN`, and `FULL JOIN`
- boolean `WHERE` predicates (`AND`, `OR`, `NOT`)
- `IN`, `IS NULL`, `IS NOT NULL`
- aggregates (`COUNT`, `SUM`, `AVG`, `MIN`, `MAX`) and `HAVING`
- `SELECT DISTINCT`
- `UNION ALL`, `UNION`, `INTERSECT`, and `EXCEPT`
- uncorrelated subqueries (`IN (SELECT ...)`, `EXISTS`, scalar subqueries)
- non-recursive CTEs (`WITH ...`)

Not yet supported:

- write statements (`INSERT`, `UPDATE`, `DELETE`)
- recursive CTEs
- correlated subqueries
- subqueries in `FROM`

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

## DDL output

```ts
import { toSqlDDL } from "sqlql";

console.log(toSqlDDL(schema, { ifNotExists: true }));
```

## Project structure

- `src/schema.ts`: schema and table method contracts
- `src/parser.ts`: SQL parser adapter (`node-sql-parser`, single mode, no fallback)
- `src/query.ts`: SQL parsing + query execution
- `src/planning.ts`: planning model types
- `src/index.ts`: package entrypoint
- `docs/sql-standards-roadmap.md`: incremental SQL support plan

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
```

## Publish

```bash
pnpm publish --access public
```
