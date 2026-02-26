# sqlql

*Warning:* Currently very rough and LLM-generated, not ready for production use.

`sqlql` is a TypeScript library for exposing a SQL interface over arbitrary data sources.

You define:

- a schema (tables + columns)
- table methods (`scan`, optional `lookup`, optional `aggregate`)

Then users write SQL queries, and `sqlql` parses and executes them by calling your methods.

## Why this exists

- Keep SQL as the user-facing query language.
- Keep backend details inside typed table methods.
- Support dependency-aware execution across joins (downstream scans get `IN (...)` filters from upstream results).

## Install

For consumers, install a single package:

```bash
pnpm add sqlql
```

This monorepo contains internal implementation packages, but consumers should install only `sqlql`.

## Quick start

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

const methods = defineTableMethods({
  orders: {
    async scan(req, ctx) {
      return []; // implement with ORM/service/files/etc.
    },
  },
  users: {
    async scan(req, ctx) {
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

## DDL generation

You can print SQL DDL from the schema:

```ts
import { toSqlDDL } from "sqlql";

console.log(toSqlDDL(schema, { ifNotExists: true }));
```

## Local development

```bash
pnpm install
pnpm build
pnpm test
pnpm typecheck
```

Run the example:

```bash
pnpm --filter @sqlql/example-basic build
pnpm --filter @sqlql/example-basic start
```
