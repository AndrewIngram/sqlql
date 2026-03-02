# Defining Your Schema

This guide covers how to model a `sqlql` schema with:

- table/column structure
- constraints
- query capabilities and policy
- DDL generation

## Basic shape

```ts
import { defineSchema } from "sqlql";

const schema = defineSchema({
  tables: {
    orders: {
      columns: {
        id: { type: "text", nullable: false, primaryKey: true },
        customer_id: { type: "text", nullable: false },
        status: { type: "text", nullable: false },
        total_cents: { type: "integer", nullable: false },
        ordered_at: { type: "timestamp", nullable: false },
      },
    },
  },
});
```

`type` supports:

- `"text"`
- `"integer"`
- `"boolean"`
- `"timestamp"` (serialized as `TEXT` in SQLite-style DDL, with metadata comment)

You can also use shorthand:

```ts
id: "text";
```

That expands to defaults (`nullable: true`, `filterable: true`, `sortable: true`).

## Column capabilities

Capabilities are column-centric and default to `true`:

```ts
status: {
  type: "text",
  nullable: false,
  filterable: true,
  sortable: false,
  description: "Public order lifecycle status",
}
```

When a query filters on `filterable: false` or sorts on `sortable: false`, `sqlql` rejects the query at planning time.

## Enums

Model enums directly on text columns:

```ts
status: {
  type: "text",
  nullable: false,
  enum: ["pending", "paid", "void"] as const,
}
```

Effects:

- invalid enum literals in SQL are rejected at planning time
- DDL includes a generated `CHECK (... IN (...))`
- optional runtime constraint validation can report returned-row enum violations

## Field-level constraints

Single-column keys are best declared on fields:

```ts
id: { type: "text", nullable: false, primaryKey: true },
sku: { type: "text", nullable: false, unique: true },
customer_id: {
  type: "text",
  nullable: false,
  foreignKey: { table: "customers", column: "id" },
},
```

Notes:

- `primaryKey` and `unique` are mutually exclusive on one column
- primary key columns must be `nullable: false`

## Table-level constraints

Use table-level constraints for multi-column rules or explicit naming:

```ts
const schema = defineSchema({
  tables: {
    line_items: {
      columns: {
        order_id: { type: "text", nullable: false },
        product_id: { type: "text", nullable: false },
        quantity: { type: "integer", nullable: false },
      },
      constraints: {
        primaryKey: { columns: ["order_id", "product_id"], name: "line_items_pk" },
        checks: [
          { kind: "in", column: "quantity", values: [1, 2, 3, 4, 5] },
        ],
      },
    },
  },
});
```

## Query policy (table-level)

Non-column governance lives under `table.query`:

```ts
orders: {
  columns: { ... },
  query: {
    maxRows: 500,
    reject: {
      requiresLimit: true,
      forbidFullScan: true,
      requireAnyFilterOn: ["customer_id"],
    },
    fallback: {
      filters: "allow_local",
      sorting: "require_pushdown",
      aggregates: "allow_local",
      limitOffset: "require_pushdown",
    },
  },
}
```

Global defaults can be set at `schema.defaults.query` and overridden per table.

## Generating DDL

```ts
import { toSqlDDL } from "sqlql";

const ddl = toSqlDDL(schema, { ifNotExists: true });
```

Generated DDL includes:

- column types and nullability
- PK/UNIQUE/FK/CHECK constraints
- `sqlql` metadata comments:
  - column comments (for behavior not expressed directly in SQL syntax)
  - table query policy as JSON

## Validation behavior

`defineSchema(...)` validates:

- referenced FK tables/columns exist
- constraint column lists are valid
- composite/single-column key declaration conflicts
- enum declarations (non-empty, deduplicated, text-only)

Invalid schemas throw deterministic errors at definition time.
