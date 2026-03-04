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
      provider: "warehouse",
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

In object-form schemas, tables declare `provider`, which maps table access to a registered provider adapter.
In lens DSL form (`table({ from: dataEntityHandle, ... })`), provider is inferred from the handle.

## Lens DSL (provider-owned entities)

For source-neutral modeling (SQL tables, Elasticsearch indices, Redis keyspaces, Mongo collections),
you can define a logical schema as a lens over provider-owned entities:

```ts
import { createDataEntityHandle, defineSchema } from "sqlql";

const ordersEntity = createDataEntityHandle({
  entity: "orders_raw",
  provider: "regional",
});

const schema = defineSchema(({ table }) => ({
  tables: {
    my_orders: table({
      from: ordersEntity,
      columns: {
        id: { source: "id", type: "text", nullable: false },
        total_cents: { source: "total_cents", type: "integer", nullable: false },
      },
    }),
  },
}));
```

This keeps SQL-facing names relational while allowing provider-facing entities to map to non-relational sources.

Typed references are also supported when defining synthetic views:

```ts
const schema = defineSchema(({ table, view, rel, col, expr }) => {
  const myOrders = table({
    from: ordersEntity,
    columns: {
      id: col("id"),
      vendorId: col("vendor_id"),
    },
  });

  const vendors = table({
    from: vendorsEntity,
    columns: {
      id: col("id"),
      name: col("name"),
    },
  });

  return {
    tables: {
      myOrders,
      vendors,
      orderVendors: view({
        rel: () =>
          rel.join({
            left: rel.scan(myOrders),
            right: rel.scan(vendors),
            on: expr.eq(col(myOrders, "vendorId"), col(vendors, "id")),
          }),
        columns: {
          orderId: col("myOrders.id"),
          vendorName: col("vendors.name"),
        },
      }),
    },
  };
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

Linked enums are also supported when a facade/view column maps to an upstream enum domain:

```ts
status: {
  source: col(myOrdersEntity, "status"),
  type: "text",
  nullable: false,
  enumFrom: col(myOrdersEntity, "status"),
}
```

Mapped enums let facade values differ from upstream values:

```ts
status: {
  source: col(myOrdersEntity, "status"),
  type: "text",
  nullable: false,
  enumFrom: col(myOrdersEntity, "status"),
  enum: ["open", "closed"] as const,
  enumMap: {
    pending: "open",
    paid: "closed",
    shipped: "closed",
  },
}
```

By default, linked enum resolution is strict: unmapped upstream values are rejected.

## Physical metadata

Logical scalar types remain `text|integer|boolean|timestamp`, and you can attach physical hints:

```ts
total_cents: {
  type: "integer",
  physicalType: "numeric(12,0)",
  physicalDialect: "postgres",
}
```

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

## Query policy metadata (optional)

`table.query` is still accepted as schema metadata and appears in generated DDL comments:

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

In v1, execution safety limits should be enforced through `queryGuardrails` on `query(...)`. Treat `table.query` as metadata unless your provider layer explicitly consumes it.

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
