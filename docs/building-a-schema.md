# Building a Schema (DSL Form, Drizzle Example)

This guide is DSL-first.

You will build:

1. physical entities exposed by a provider
2. scoped provider configuration (tenancy/user scope)
3. a user-facing SQL schema built with `table(...)` and `view(...)`

## Mental model

- Provider config describes physical access and scope.
- Schema DSL describes the user-facing facade (renames/transforms/views).
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

### 2) Create provider entities + scoped Drizzle provider

```ts
import { and, eq } from "drizzle-orm";
import { createDrizzleProvider } from "@sqlql/drizzle";
import { createDataEntityHandle, defineProviders } from "sqlql";

type QueryContext = { orgId: string; userId: string };

// Physical entities exposed by this provider.
const orders = createDataEntityHandle<
  "id" | "org_id" | "user_id" | "vendor_id" | "status" | "total_cents" | "created_at"
>({
  provider: "dbProvider",
  entity: "orders",
});

const vendors = createDataEntityHandle<"id" | "org_id" | "name" | "tier">({
  provider: "dbProvider",
  entity: "vendors",
});

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

const providers = defineProviders({ dbProvider });
```

### 3) Build facade schema with DSL (`table`, `view`, typed `col`)

```ts
import { defineSchema } from "sqlql";

const schema = defineSchema<QueryContext>(({ table, view, rel, expr, col, agg }) => {
  const myOrders = table({
    from: orders,
    columns: {
      id: { source: col(orders, "id"), type: "text", nullable: false, primaryKey: true },
      vendor_id: {
        source: col(orders, "vendor_id"),
        type: "text",
        nullable: false,
        foreignKey: { table: "vendors_for_org", column: "id" },
      },
      status: {
        source: col(orders, "status"),
        type: "text",
        nullable: false,
        enum: ["pending", "paid", "shipped"] as const,
      },
      total_cents: { source: col(orders, "total_cents"), type: "integer", nullable: false },
      created_at: { source: col(orders, "created_at"), type: "timestamp", nullable: false },
    },
  });

  const vendorsForOrg = table({
    from: vendors,
    columns: {
      id: { source: col(vendors, "id"), type: "text", nullable: false, primaryKey: true },
      name: { source: col(vendors, "name"), type: "text", nullable: false },
      tier: {
        source: col(vendors, "tier"),
        type: "text",
        nullable: false,
        enum: ["standard", "preferred"] as const,
      },
    },
  });

  const myVendorSpend = view({
    rel: () =>
      rel.aggregate({
        from: rel.join({
          left: rel.scan(myOrders),
          right: rel.scan(vendorsForOrg),
          on: expr.eq(col(myOrders, "vendor_id"), col(vendorsForOrg, "id")),
          type: "inner",
        }),
        groupBy: [col(vendorsForOrg, "id"), col(vendorsForOrg, "name")],
        measures: {
          spend_cents: agg.sum(col(myOrders, "total_cents")),
          order_count: agg.count(),
        },
      }),
    columns: {
      vendor_id: { source: col(vendorsForOrg, "id"), type: "text", nullable: false },
      vendor_name: { source: col(vendorsForOrg, "name"), type: "text", nullable: false },
      spend_cents: { source: col("spend_cents"), type: "integer", nullable: false },
      order_count: { source: col("order_count"), type: "integer", nullable: false },
    },
  });

  return {
    tables: {
      my_orders: myOrders,
      vendors_for_org: vendorsForOrg,
      my_vendor_spend: myVendorSpend,
    },
  };
});
```

### 4) Query the facade

```ts
import { query } from "sqlql";

const rows = await query({
  schema,
  providers,
  context: { orgId: "org_1", userId: "u1" },
  sql: `
    SELECT vendor_name, spend_cents, order_count
    FROM my_vendor_spend
    ORDER BY spend_cents DESC
  `,
});
```

## Why this pattern stays clean

- Physical concerns stay in provider config (`table`, `scope`, backend APIs).
- Facade concerns stay in schema (`table`, `view`, logical names).
- Typed refs (`col(handleOrToken, ...)`) reduce ref/column drift.

## Troubleshooting checklist

- provider names on entity handles match `defineProviders` keys
- provider `tables` keys match handle `entity` values
- scoped columns exist on physical tables
- facade FK references target facade table/column names
- any unsupported query shape is either pushed down partially or handled by fallback/local execution
