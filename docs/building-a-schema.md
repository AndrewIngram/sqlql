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
    columns: ({ col }) => ({
      id: col.id("id"),
      vendorId: col.string("vendor_id", {
        foreignKey: { table: "vendorsForOrg", column: "id" },
      }),
      status: col.string("status", {
        enum: ["pending", "paid", "shipped"] as const,
      }),
      totalCents: col.integer("total_cents"),
      createdAt: col.timestamp("created_at"),
    }),
  });

  const vendorsForOrg = table(dbProvider.entities.vendors, {
    columns: ({ col }) => ({
      id: col.id("id"),
      name: col.string("name"),
      tier: col.string("tier", {
        enum: ["standard", "preferred"] as const,
      }),
    }),
  });

  const myVendorSpend = view({
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
          spendCents: agg.sum(col(myOrders, "totalCents")),
          orderCount: agg.count(),
        },
      }),
    columns: ({ col }) => ({
      vendorId: col.id("vendorId"),
      vendorName: col.string("vendorName"),
      spendCents: col.integer("spendCents"),
      orderCount: col.integer("orderCount"),
    }),
  });

  return {
    tables: {
      myOrders,
      vendorsForOrg,
      myVendorSpend,
    },
  };
});
```

### 4) Query the facade

```ts
const rows = await executableSchema.query({
  context: { orgId: "org_1", userId: "u1" },
  sql: `
    SELECT vendorName, spendCents, orderCount
    FROM myVendorSpend
    ORDER BY spendCents DESC
  `,
});
```

## Why this pattern stays clean

- Physical concerns stay in provider config (`table`, `scope`, backend APIs).
- Facade concerns stay in the executable schema (`table`, `view`, logical names).
- Scoped typed builders (`columns: ({ col }) => ...`) reduce ref/column drift.

## Troubleshooting checklist

- provider `tables` keys match the entities you bind in `table(provider.entities.someTable, ...)`
- scoped columns exist on physical tables
- facade FK references target facade table/column names
- any unsupported query shape is either pushed down partially or handled by fallback/local execution
