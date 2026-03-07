# Building a Schema (Executable Schema, Drizzle Example)

This guide is DSL-first.

You will build:

1. provider-backed physical entities exposed by an adapter
2. scoped provider configuration (tenancy/user scope)
3. an executable SQL facade built with `table(...)` and `view(...)`

## Mental model

- Provider config describes physical access, normalization, and scope.
- The executable schema describes the user-facing facade (renames/transforms/views).
- SQL queries run against the facade, not your physical tables.

## End-to-end example

### 1) Define downstream tables and Drizzle DB

```ts
import Database from "better-sqlite3";
import { drizzle } from "drizzle-orm/better-sqlite3";
import { integer, sqliteTable, text } from "drizzle-orm/sqlite-core";

const sqlite = new Database(":memory:");

const ordersRaw = sqliteTable("orders_raw", {
  id: text("id").primaryKey().notNull(),
  org_id: text("org_id").notNull(),
  user_id: text("user_id").notNull(),
  vendor_id: text("vendor_id").notNull(),
  status: text("status").notNull(),
  total_cents: integer("total_cents").notNull(),
  created_at: text("created_at").notNull(),
});

const vendorsRaw = sqliteTable("vendors_raw", {
  id: text("id").primaryKey().notNull(),
  org_id: text("org_id").notNull(),
  name: text("name").notNull(),
  tier: text("tier").notNull(),
});

const db = drizzle(sqlite);
```

### 2) Create scoped Drizzle provider

```ts
import { and, eq } from "drizzle-orm";
import { createDrizzleProvider } from "@sqlql/drizzle";

type QueryContext = { orgId: string; userId: string };

const dbProvider = createDrizzleProvider<QueryContext>({
  name: "dbProvider",
  db,
  tables: {
    orders: {
      table: ordersRaw,
      scope: (ctx) =>
        and(eq(ordersRaw.org_id, ctx.orgId), eq(ordersRaw.user_id, ctx.userId)),
    },
    vendors: {
      table: vendorsRaw,
      scope: (ctx) => eq(vendorsRaw.org_id, ctx.orgId),
    },
  },
});
```

`dbProvider.entities.orders` and `dbProvider.entities.vendors` are now typed, provider-owned entities that the schema can bind to directly.

### 3) Build facade schema with `createExecutableSchema(...)`

```ts
import { createExecutableSchema } from "sqlql";

const executableSchema = createExecutableSchema<QueryContext>(({ table, view }) => {
  const myOrders = table(dbProvider.entities.orders, {
    columns: ({ col, expr }) => ({
      id: col.id("id"),
      vendorId: col.string("vendor_id"),
      status: col.string("status", {
        enum: ["pending", "paid", "shipped"] as const,
      }),
      totalCents: col.integer("total_cents"),
      createdAt: col.timestamp("created_at"),
      totalDollars: col.real(
        expr.divide(col("totalCents"), expr.literal(100)),
        { nullable: false },
      ),
      isLargeOrder: col.boolean(
        expr.gte(col("totalCents"), expr.literal(3000)),
        { nullable: false },
      ),
    }),
  });

  const myOrderFacts = view(
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

  const myVendorSpend = view(
    ({ scan, aggregate, col, agg }) =>
      aggregate({
        from: scan(myOrderFacts),
        groupBy: {
          vendorId: col(myOrderFacts, "vendorId"),
          vendorName: col(myOrderFacts, "vendorName"),
        },
        measures: {
          spendCents: agg.sum(col(myOrderFacts, "totalCents")),
          orderCount: agg.count(),
        },
      }),
    {
      columns: ({ col }) => ({
        vendorId: col.id("vendorId"),
        vendorName: col.string("vendorName"),
        spendCents: col.integer("spendCents"),
        orderCount: col.integer("orderCount"),
      }),
    },
  );

  return {
    tables: {
      myOrders,
      myOrderFacts,
      myVendorSpend,
    },
  };
});
```

When a view only needs a provider entity as a private source, `scan(...)` can read the `DataEntityHandle` directly. You only need `table(...)` when you want that source to be part of the public facade.

### 4) Use calculated columns on a base table

`virtuals` attaches calculated columns directly to a physical facade table. They behave like any other logical column in `SELECT`, `WHERE`, and `ORDER BY`.

```ts
const highValueOrders = await executableSchema.query({
  context: { orgId: "org_1", userId: "u1" },
  sql: `
    SELECT id, totalDollars, isLargeOrder
    FROM myOrders
    WHERE totalDollars >= 20
    ORDER BY totalDollars DESC
  `,
});
```

### 5) Query composed and aggregate views

```ts
const rows = await executableSchema.query({
  context: { orgId: "org_1", userId: "u1" },
  sql: `
    SELECT vendorName, spendCents, orderCount
    FROM myVendorSpend
    ORDER BY spendCents DESC
  `,
});

const facts = await executableSchema.query({
  context: { orgId: "org_1", userId: "u1" },
  sql: `
    SELECT orderId, vendorName, totalDollars
    FROM myOrderFacts
    ORDER BY totalDollars DESC
  `,
});
```

## Why this pattern stays clean

- Physical concerns stay in provider config (`table`, `scope`, backend APIs).
- Facade concerns stay in the executable schema (`table`, `virtuals`, `view`, logical names).
- Scoped typed builders (`columns: ({ col }) => ...`) reduce ref/column drift.

## Troubleshooting checklist

- provider `tables` keys match the entities you bind in `table(provider.entities.someTable, ...)`
- scoped columns exist on physical tables
- facade FK references target facade table/column names
- any unsupported query shape is either pushed down partially or handled by fallback/local execution
