# Resolver and Planning API (Draft)

The user-facing surface should stay minimal:

1. Define schema once.
2. Define per-table methods (`scan`, optional `lookup`, optional `aggregate`).
3. Run `query(sql)`.

SQL parsing and plan-step wiring stay internal.

## Current direction

- Schema is the single source of table metadata and query defaults.
- Table methods only describe execution hooks.
- No duplicated capabilities between schema and resolver.
- Defaults are permissive:
  - all columns filterable
  - all columns sortable
  - no max row limit

## Shape

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
        team_id: "text",
        email: "text",
      },
    },
    teams: {
      columns: {
        id: "text",
        tier: "text",
      },
    },
  },
});

const methods = defineTableMethods(schema, {
  orders: {
    async scan(request, ctx) {
      return [];
    },
  },
  users: {
    async scan(request, ctx) {
      return [];
    },
  },
  teams: {
    async scan(request, ctx) {
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
  `,
});
```

## Internal planning behavior

- SQL is parsed to an internal representation.
- Join dependencies are converted into chained scan calls.
- Downstream scans receive `IN (...)` filters derived from upstream rows.
- Final join/order/limit/projection are applied after scan steps.

This gives Grafast-like dependency flow while keeping plans invisible to consumers.

## Roadmap

For incremental SQL standards support, see:

- `docs/sql-standards-roadmap.md`
