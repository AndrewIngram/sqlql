# tupl

`tupl` lets you expose a controlled SQL facade over one or more underlying data systems.

## What

`tupl` is a provider-first query runtime:

- You define one or more providers (Drizzle/Kysely/Objection/custom).
- Providers expose normalized entities that represent physical sources.
- You build an executable logical SQL-facing schema from those entities.
- `tupl` plans query fragments across providers and local logical operators.

The facade stays relational (`SELECT` over tables/views), while providers can be relational or non-relational.

## Why

Typical reasons to use `tupl`:

- enforce a safer query boundary than direct DB access
- expose only an allowlisted, user-facing data model
- centralize tenancy/scope logic in provider integration
- keep SQL ergonomics for developers and agents while supporting mixed backends

## Examples

### Example A (Primary): Executable Schema + Scoped Drizzle Provider

```ts
import { and, eq } from "drizzle-orm";
import { createDrizzleProvider } from "@tupl/provider-drizzle";
import { createExecutableSchema, createSchemaBuilder } from "@tupl/core";

type QueryContext = { orgId: string; userId: string; db: typeof db };

const dbProvider = createDrizzleProvider<QueryContext>({
  name: "dbProvider",
  db: (ctx) => ctx.db,
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

const builder = createSchemaBuilder<QueryContext>();

const myOrders = builder.table("myOrders", dbProvider.entities.orders, {
  columns: ({ col, expr }) => ({
    id: col.id("id"),
    vendorId: col.string("vendor_id"),
    totalCents: col.integer("total_cents"),
    createdAt: col.timestamp("created_at"),
    totalDollars: col.real(expr.divide(col("totalCents"), expr.literal(100)), {
      nullable: false,
    }),
    isLargeOrder: col.boolean(expr.gte(col("totalCents"), expr.literal(3000)), {
      nullable: false,
    }),
  }),
});

const myOrderFacts = builder.view(
  "myOrderFacts",
  ({ scan, join, col, expr }) =>
    join({
      left: scan(myOrders),
      right: scan(dbProvider.entities.vendors),
      on: expr.eq(col(myOrders, "vendorId"), col(dbProvider.entities.vendors, "id")),
      type: "inner",
    }),
  {
    columns: ({ col }) => ({
      orderId: col.id(myOrders, "id"),
      vendorId: col.string(myOrders, "vendorId", { nullable: false }),
      vendorName: col.string(dbProvider.entities.vendors, "name", { nullable: false }),
      totalCents: col.integer(myOrders, "totalCents", { nullable: false }),
      totalDollars: col.real(myOrders, "totalDollars", { nullable: false }),
      isLargeOrder: col.boolean(myOrders, "isLargeOrder", { nullable: false }),
    }),
  },
);

builder.view(
  "myVendorSpend",
  ({ scan, aggregate, col, agg }) =>
    aggregate({
      from: scan(myOrderFacts),
      groupBy: {
        vendorId: col(myOrderFacts, "vendorId"),
        vendorName: col(myOrderFacts, "vendorName"),
      },
      measures: {
        totalSpendCents: agg.sum(col(myOrderFacts, "totalCents")),
        orderCount: agg.count(),
      },
    }),
  {
    columns: ({ col }) => ({
      vendorId: col.id("vendorId"),
      vendorName: col.string("vendorName"),
      totalSpendCents: col.integer("totalSpendCents"),
      orderCount: col.integer("orderCount"),
    }),
  },
);

const executableSchema = createExecutableSchema(builder);

const rows = await executableSchema.query({
  context: { orgId: "org_1", userId: "u1", db },
  sql: `
    SELECT vendorName, totalSpendCents, orderCount
    FROM myVendorSpend
    ORDER BY totalSpendCents DESC
  `,
});

const highValueOrders = await executableSchema.query({
  context: { orgId: "org_1", userId: "u1", db },
  sql: `
    SELECT orderId, vendorName, totalDollars, isLargeOrder
    FROM myOrderFacts
    WHERE totalDollars >= 20
    ORDER BY totalDollars DESC
  `,
});
```

If your runtime handle is static, `db` can still be passed directly instead of using a context callback.

### Example B: Non-Relational Mapping Pattern

```ts
import { createIoredisProvider, type RedisLike } from "@tupl/provider-ioredis";
import { createExecutableSchema, createSchemaBuilder } from "@tupl/core";

type QueryContext = {
  userId: string;
  redis: RedisLike;
};

const redisProvider = createIoredisProvider<QueryContext>({
  name: "redisProvider",
  redis: (ctx) => ctx.redis,
  entities: {
    product_view_counts: {
      entity: "product_view_counts",
      lookupKey: "product_id",
      columns: ["product_id", "view_count"] as const,
      buildRedisKey({ key, context }) {
        return `product_view_counts:${context.userId}:${String(key)}`;
      },
      decodeRow({ hash }) {
        if (typeof hash.product_id !== "string" || typeof hash.view_count !== "string") {
          return null;
        }
        return {
          product_id: hash.product_id,
          view_count: Number(hash.view_count),
        };
      },
    },
  },
});

const builder = createSchemaBuilder<QueryContext>();

builder.table("productViewCounts", redisProvider.entities.product_view_counts, {
  columns: ({ col }) => ({
    productId: col.string("product_id"),
    viewCount: col.integer("view_count"),
  }),
});

const executableSchema = createExecutableSchema(builder);
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

| Adapter               | scan/filter/sort/limit | lookupMany | single-query rel pushdown (core join/aggregate) | advanced rel pushdown (with/set-op/window) | local fallback when unsupported | explicit shape rejection |
| --------------------- | ---------------------- | ---------- | ----------------------------------------------- | ------------------------------------------ | ------------------------------- | ------------------------ |
| `@tupl/provider-drizzle`      | Yes                    | Yes        | Yes                                             | Partial                                    | Yes                             | Yes                      |
| `@tupl/provider-kysely`       | Yes                    | Yes        | Yes                                             | Partial                                    | Yes                             | Yes                      |
| `@tupl/provider-objection`    | Yes                    | Yes        | Yes                                             | Partial                                    | Yes                             | Yes                      |
| Custom non-relational | Custom                 | Custom     | Custom                                          | Custom                                     | Yes                             | Yes                      |

## Guides

- [Building a schema (executable schema, Drizzle example)](./docs/building-a-schema.md)
- [Creating a new adapter (progressive path)](./docs/creating-an-adapter.md)
- [Building a non-relational adapter (Redis-style)](./docs/building-a-non-relational-adapter.md)
