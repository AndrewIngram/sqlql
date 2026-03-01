# sqlql

_Warning:_ Currently very rough and LLM-generated, not ready for production use.

`sqlql` is a TypeScript library for exposing a SQL interface over arbitrary data sources.
You define a logical schema and table methods, then users write SQL and `sqlql` executes by calling your methods.

## Why

One core motivation is AI tooling.

If you are building tools that accept SQL input, directly exposing production databases is usually a bad fit: security boundaries, tenancy boundaries, query cost, and coupling risks show up immediately.

`sqlql` gives you a controlled middle layer:

- expose only an allowlisted logical schema
- map table access to domain-aware methods (`scan`, optional `lookup`, optional `aggregate`)
- keep control over what data is queryable and how it is fetched

This keeps SQL ergonomics for agents and developers without requiring direct DB connectivity from the tool runtime.

For LLM-driven tools specifically, a SQL interface gives the model flexible retrieval patterns while minimizing how much raw data needs to be injected into the context window, all through a single tool surface.

## Conceptual limits and non-goals

`sqlql` intentionally does not try to be a full database.

Explicit non-goals:

- write statements (`INSERT`, `UPDATE`, `DELETE`)

Currently unsupported query shapes:

- recursive CTEs
- correlated subqueries
- subqueries in `FROM`

Accepted limitation (relational data sources):

- `sqlql` executes relational workflows in staged table-method calls.
- `sqlql` does not currently collapse multi-stage relational work into a single provider-native joined query.
- Even when a relational backing store could answer in one SQL statement, execution may still involve multiple stage calls.

## Quick usage

Install:

```bash
pnpm add sqlql
```

Minimal end-to-end flow:

```ts
import { defineSchema, defineTableMethods, query } from "sqlql";

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

const methods = defineTableMethods(schema, {
  orders: {
    async scan(_request, _context) {
      return [];
    },
  },
  users: {
    async scan(_request, _context) {
      return [];
    },
  },
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

## Method contract examples (scan + aggregate)

Inside `defineTableMethods(schema, ...)`, request parameter types are inferred from your schema, so typical implementations can stay concise:

```ts
import { defineTableMethods } from "sqlql";

const methods = defineTableMethods(schema, {
  orders: {
    async scan(request, context) {
      return context.ordersRepo.scan({
        table: request.table,
        alias: request.alias,
        select: request.select,
        where: request.where,
        orderBy: request.orderBy,
        limit: request.limit,
        offset: request.offset,
      });
    },
    async aggregate(request, context) {
      return context.ordersRepo.aggregate({
        table: request.table,
        alias: request.alias,
        where: request.where,
        groupBy: request.groupBy,
        metrics: request.metrics,
        limit: request.limit,
      });
    },
  },
  users: {
    async scan(request, context) {
      return context.usersRepo.scan({
        table: request.table,
        alias: request.alias,
        select: request.select,
        where: request.where,
        orderBy: request.orderBy,
        limit: request.limit,
        offset: request.offset,
      });
    },
  },
});
```

`scan(request, context)` argument shape:

- `request.table`: logical table name for the method call.
- `request.alias`: optional SQL alias for the table binding.
- `request.select`: projected column list to return.
- `request.where`: optional normalized filter clauses (`eq`, `in`, `is_null`, etc.).
- `request.orderBy`: optional sort terms (`column`, `direction`).
- `request.limit` / `request.offset`: optional pagination bounds.
- `context`: your app/domain context from `query({ context })`.
- return type: `Promise<QueryRow[]>`.

`aggregate(request, context)` argument shape:

- `request.table`: logical table name for the method call.
- `request.alias`: optional SQL alias for the table binding.
- `request.where`: optional normalized filter clauses before grouping.
- `request.groupBy`: optional grouping columns.
- `request.metrics`: aggregate spec list (`fn`, optional `column`, `as`, optional `distinct`).
- `request.limit`: optional output row cap.
- `context`: your app/domain context from `query({ context })`.
- return type: `Promise<QueryRow[]>` with grouped rows and metric outputs.

## JSON helper methods (optional)

For demos, tests, and rapid prototypes, `createArrayTableMethods(...)` generates table methods from in-memory rows:

```ts
import { createArrayTableMethods, defineTableMethods } from "sqlql";

const methods = defineTableMethods(schema, {
  orders: createArrayTableMethods(ordersRows),
  users: createArrayTableMethods(usersRows),
});
```

## Security model

- `sqlql` does not provide authorization or tenancy guarantees by itself.
- Domain methods (`scan`, `lookup`, `aggregate`) must enforce access control and data security.
- `sqlql` can add query-shape guardrails, but security guarantees come from your domain/storage layer.

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
- `toSqlDDL(...)` with SQLite-oriented output (`TEXT`/`INTEGER`) and timestamp metadata comments

Not yet supported:

- write statements (`INSERT`, `UPDATE`, `DELETE`)
- recursive CTEs
- correlated subqueries
- subqueries in `FROM`
- advanced window frame clauses (beyond `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`)
- some navigation/value window functions (`FIRST_VALUE`, `LAST_VALUE`, etc.)

## Playground

The playground is a Vite + React app for interactive exploration:

- edit schema/data/query in Monaco
- run queries and inspect results
- view plan dependencies as a graph

Run:

```bash
pnpm example:playground:dev
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
