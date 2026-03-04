# sqlql

`sqlql` lets you expose a controlled SQL facade over one or more underlying data systems.

## What

`sqlql` is a provider-first query runtime:

- You define a logical SQL-facing schema.
- You register one or more providers (Drizzle/Kysely/Objection/custom).
- `sqlql` plans query fragments across providers and local logical operators.

The facade stays relational (`SELECT` over tables/views), while providers can be relational or non-relational.

## Why

Typical reasons to use `sqlql`:

- enforce a safer query boundary than direct DB access
- expose only an allowlisted, user-facing data model
- centralize tenancy/scope logic in provider integration
- keep SQL ergonomics for developers and agents while supporting mixed backends

## Examples

### Example A (Primary): DSL Schema + Scoped Drizzle Provider

```ts
import { and, eq } from "drizzle-orm";
import { createDrizzleProvider } from "@sqlql/drizzle";
import { createDataEntityHandle, defineProviders, defineSchema, query } from "sqlql";

type QueryContext = { orgId: string; userId: string };

const orders = createDataEntityHandle<"id" | "vendor_id" | "total_cents">({
  provider: "dbProvider",
  entity: "orders",
});

const vendors = createDataEntityHandle<"id" | "name">({
  provider: "dbProvider",
  entity: "vendors",
});

const dbProvider = createDrizzleProvider<QueryContext>({
  name: "dbProvider",
  db,
  tables: {
    orders: {
      table: tables.orders,
      scope: (ctx) =>
        and(eq(tables.orders.org_id, ctx.orgId), eq(tables.orders.user_id, ctx.userId)),
    },
    vendors: {
      table: tables.vendors,
      scope: (ctx) => eq(tables.vendors.org_id, ctx.orgId),
    },
  },
});

const providers = defineProviders({ dbProvider });

const schema = defineSchema<QueryContext>(({ table, col }) => ({
  tables: {
    my_orders: table({
      from: orders,
      columns: {
        id: { source: col(orders, "id"), type: "text", nullable: false, primaryKey: true },
        vendor_id: { source: col(orders, "vendor_id"), type: "text", nullable: false },
        total_cents: { source: col(orders, "total_cents"), type: "integer", nullable: false },
      },
    }),
    vendors_for_org: table({
      from: vendors,
      columns: {
        id: { source: col(vendors, "id"), type: "text", nullable: false, primaryKey: true },
        name: { source: col(vendors, "name"), type: "text", nullable: false },
      },
    }),
  },
}));

const rows = await query({
  schema,
  providers,
  context: { orgId: "org_1", userId: "u1" },
  sql: `
    SELECT o.id, v.name, o.total_cents
    FROM my_orders o
    JOIN vendors_for_org v ON v.id = o.vendor_id
    ORDER BY o.total_cents DESC
    LIMIT 50
  `,
});
```

### Example B: Non-Relational Mapping Pattern

```ts
import { createKvProvider } from "@playground/kv-provider-core";
import { createDataEntityHandle, defineSchema } from "sqlql";

const productViewCounts = createDataEntityHandle<"product_id" | "view_count">({
  provider: "kvProvider",
  entity: "product_view_counts",
});

const kvProvider = createKvProvider({
  name: "kvProvider",
  rows, // raw [{ key, value }]
  entities: {
    product_view_counts: {
      entity: "product_view_counts",
      columns: ["product_id", "view_count"] as const,
      mapRow({ key, value, context }) {
        // example key: "${userId}:${productId}"
        // map to relational row shape, or return null to skip
        return mappedOrNull;
      },
    },
  },
});

const schema = defineSchema(({ table, col }) => ({
  tables: {
    product_view_counts: table({
      from: productViewCounts,
      columns: {
        product_id: { source: col(productViewCounts, "product_id"), type: "text", nullable: false },
        view_count: { source: col(productViewCounts, "view_count"), type: "integer", nullable: false },
      },
    }),
  },
}));
```

## Limitations

Current limitations/non-goals:

- write statements are not supported (`INSERT`, `UPDATE`, `DELETE`)
- recursive CTEs are not supported
- correlated subqueries are not supported
- subqueries in `FROM` are not supported
- some advanced window SQL shapes are still partial

Execution behavior notes:

- unsupported provider pushdown shapes can fall back to local logical execution
- providers can explicitly reject shapes (`canExecute`) for deterministic behavior
- cross-provider joins generally depend on `lookupMany` availability

## Adapter support matrix

| Adapter | scan/filter/sort/limit | lookupMany | single-query rel pushdown (core join/aggregate) | advanced rel pushdown (with/set-op/window) | local fallback when unsupported | explicit shape rejection |
| --- | --- | --- | --- | --- | --- | --- |
| `@sqlql/drizzle` | Yes | Yes | Yes | Partial | Yes | Yes |
| `@sqlql/kysely` | Yes | Yes | Yes | Partial | Yes | Yes |
| `@sqlql/objection` | Yes | Yes | Yes | Partial | Yes | Yes |
| Custom non-relational | Custom | Custom | Custom | Custom | Yes | Yes |

## Guides

- [Building a schema (DSL form, Drizzle example)](./docs/building-a-schema.md)
- [Creating a new adapter (progressive path)](./docs/creating-an-adapter.md)
- [Building a non-relational adapter (Redis-style)](./docs/building-a-non-relational-adapter.md)
