# sqlql

`sqlql` lets you expose a controlled SQL facade over one or more underlying data systems.

## What

`sqlql` is a provider-first query runtime:

- You define one or more providers (Drizzle/Kysely/Objection/custom).
- Providers expose normalized entities that represent physical sources.
- You build an executable logical SQL-facing schema from those entities.
- `sqlql` plans query fragments across providers and local logical operators.

The facade stays relational (`SELECT` over tables/views), while providers can be relational or non-relational.

## Why

Typical reasons to use `sqlql`:

- enforce a safer query boundary than direct DB access
- expose only an allowlisted, user-facing data model
- centralize tenancy/scope logic in provider integration
- keep SQL ergonomics for developers and agents while supporting mixed backends

## Examples

### Example A (Primary): Executable Schema + Scoped Drizzle Provider

```ts
import { and, eq } from "drizzle-orm";
import { createDrizzleProvider } from "@sqlql/drizzle";
import { createExecutableSchema } from "sqlql";

type QueryContext = { orgId: string; userId: string };

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

const executableSchema = createExecutableSchema<QueryContext>(({ table, view }) => {
  const myOrders = table(dbProvider.entities.orders, {
    columns: ({ col }) => ({
      id: col.id("id"),
      vendorId: col.string("vendor_id"),
      totalCents: col.integer("total_cents"),
    }),
  });

  const vendorsForOrg = table(dbProvider.entities.vendors, {
    columns: ({ col }) => ({
      id: col.id("id"),
      name: col.string("name"),
    }),
  });

  return {
    tables: {
      myOrders,
      vendorsForOrg,
      myVendorSpend: view({
        rel: ({ scan, join, aggregate, col, expr, agg }) =>
          aggregate({
            from: join({
              left: scan(myOrders),
              right: scan(vendorsForOrg),
              on: expr.eq(col(myOrders, "vendorId"), col(vendorsForOrg, "id")),
              type: "inner",
            }),
            groupBy: {
              vendorId: col(vendorsForOrg, "id"),
              vendorName: col(vendorsForOrg, "name"),
            },
            measures: {
              totalSpendCents: agg.sum(col(myOrders, "totalCents")),
              orderCount: agg.count(),
            },
          }),
        columns: ({ col }) => ({
          vendorId: col.id("vendorId"),
          vendorName: col.string("vendorName"),
          totalSpendCents: col.integer("totalSpendCents"),
          orderCount: col.integer("orderCount"),
        }),
      }),
    },
  };
});

const rows = await executableSchema.query({
  context: { orgId: "org_1", userId: "u1" },
  sql: `
    SELECT vendorName, totalSpendCents, orderCount
    FROM myVendorSpend
    ORDER BY totalSpendCents DESC
  `,
});
```

### Example B: Non-Relational Mapping Pattern

```ts
import { createKvProvider } from "@playground/kv-provider-core";
import { createExecutableSchema } from "sqlql";

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

const executableSchema = createExecutableSchema(({ table }) => ({
  tables: {
    productViewCounts: table(kvProvider.entities.product_view_counts, {
      columns: ({ col }) => ({
        productId: col.string("product_id"),
        viewCount: col.integer("view_count"),
      }),
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

- [Building a schema (executable schema, Drizzle example)](./docs/building-a-schema.md)
- [Creating a new adapter (progressive path)](./docs/creating-an-adapter.md)
- [Building a non-relational adapter (Redis-style)](./docs/building-a-non-relational-adapter.md)
