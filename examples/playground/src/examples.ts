import { createDataEntityHandle } from "@tupl/core/provider";
import { createSchemaBuilder, type SchemaDefinition } from "@tupl/schema";

import type {
  CatalogQueryEntry,
  DownstreamRows,
  PlaygroundQueryPreset,
  PlaygroundScenarioPreset,
} from "./types";

const ordersEntity = createDataEntityHandle<
  "id" | "org_id" | "user_id" | "vendor_id" | "status" | "total_cents" | "created_at"
>({
  provider: "dbProvider",
  entity: "orders",
});
const orderItemsEntity = createDataEntityHandle<
  "id" | "org_id" | "user_id" | "order_id" | "product_id" | "quantity" | "line_total_cents"
>({
  provider: "dbProvider",
  entity: "order_items",
});
const vendorsEntity = createDataEntityHandle<"id" | "org_id" | "name" | "tier">({
  provider: "dbProvider",
  entity: "vendors",
});
const productsEntity = createDataEntityHandle<
  "id" | "org_id" | "sku" | "name" | "category" | "active"
>({
  provider: "dbProvider",
  entity: "products",
});
const userProductAccessEntity = createDataEntityHandle<"id" | "user_id" | "product_id">({
  provider: "dbProvider",
  entity: "user_product_access",
});
const productViewCountsEntity = createDataEntityHandle<"product_id" | "view_count">({
  provider: "redisProvider",
  entity: "product_view_counts",
});

const facadeSchemaBuilder = createSchemaBuilder<Record<string, never>>();

facadeSchemaBuilder.table("my_orders", ordersEntity, {
  columns: ({ col, expr }) => ({
    id: col.id("id"),
    vendor_id: col.string("vendor_id", {
      nullable: false,
      foreignKey: {
        table: "vendors_for_org",
        column: "id",
      },
    }),
    status: col.string("status", {
      nullable: false,
      enum: ["pending", "paid", "shipped"] as const,
    }),
    total_cents: col.integer("total_cents", { nullable: false }),
    created_at: col.timestamp("created_at", { nullable: false }),
    total_dollars: col.real(expr.divide(col("total_cents"), expr.literal(100)), {
      nullable: false,
    }),
    is_large_order: col.boolean(expr.gte(col("total_cents"), expr.literal(30000)), {
      nullable: false,
    }),
  }),
});

const myOrderItemsRef = facadeSchemaBuilder.table("my_order_items", orderItemsEntity, {
  columns: ({ col, expr }) => ({
    id: col.id("id"),
    order_id: col.string("order_id", {
      nullable: false,
      foreignKey: {
        table: "my_orders",
        column: "id",
      },
    }),
    product_id: col.string("product_id", {
      nullable: false,
      foreignKey: {
        table: "active_products",
        column: "id",
      },
    }),
    quantity: col.integer("quantity", { nullable: false }),
    line_total_cents: col.integer("line_total_cents", { nullable: false }),
    unit_price_cents: col.real(expr.divide(col("line_total_cents"), col("quantity")), {
      nullable: false,
    }),
  }),
});

facadeSchemaBuilder.table("vendors_for_org", vendorsEntity, {
  columns: ({ col }) => ({
    id: col.id("id"),
    name: col.string("name", { nullable: false }),
    tier: col.string("tier", {
      nullable: false,
      enum: ["standard", "preferred"] as const,
    }),
  }),
});

const productsForOrgRef = facadeSchemaBuilder.table("products_for_org", productsEntity, {
  columns: ({ col }) => ({
    id: col.id("id"),
    sku: col.string("sku", { nullable: false }),
    name: col.string("name", { nullable: false }),
    category: col.string("category", {
      nullable: false,
      enum: ["hardware", "software", "services"] as const,
    }),
  }),
});

const productAccessForUserRef = facadeSchemaBuilder.table(
  "product_access_for_user",
  userProductAccessEntity,
  {
    columns: ({ col }) => ({
      product_id: col.string("product_id", {
        nullable: false,
        foreignKey: {
          table: "products_for_org",
          column: "id",
        },
      }),
    }),
  },
);

const activeProductsRef = facadeSchemaBuilder.view(
  "active_products",
  ({ scan, join, col, expr }) =>
    join({
      left: scan(productsForOrgRef),
      right: scan(productAccessForUserRef),
      on: expr.eq(col(productsForOrgRef, "id"), col(productAccessForUserRef, "product_id")),
      type: "inner",
    }),
  {
    columns: ({ col }) => ({
      id: col.id(productsForOrgRef, "id"),
      sku: col.string(productsForOrgRef, "sku", { nullable: false }),
      name: col.string(productsForOrgRef, "name", { nullable: false }),
      category: col.string(productsForOrgRef, "category", {
        nullable: false,
        enum: ["hardware", "software", "services"] as const,
      }),
    }),
  },
);

const myOrderLinesRef = facadeSchemaBuilder.view(
  "my_order_lines",
  ({ scan, join, col, expr }) =>
    join({
      left: scan(myOrderItemsRef),
      right: scan(activeProductsRef),
      on: expr.eq(col(myOrderItemsRef, "product_id"), col(activeProductsRef, "id")),
      type: "inner",
    }),
  {
    columns: ({ col }) => ({
      order_id: col.string(myOrderItemsRef, "order_id", {
        nullable: false,
        foreignKey: { table: "my_orders", column: "id" },
      }),
      product_id: col.string(activeProductsRef, "id", {
        nullable: false,
        foreignKey: { table: "active_products", column: "id" },
      }),
      product_sku: col.string(activeProductsRef, "sku", { nullable: false }),
      product_name: col.string(activeProductsRef, "name", { nullable: false }),
      product_category: col.string(activeProductsRef, "category", {
        nullable: false,
        enum: ["hardware", "software", "services"] as const,
      }),
      quantity: col.integer(myOrderItemsRef, "quantity", { nullable: false }),
      line_total_cents: col.integer(myOrderItemsRef, "line_total_cents", { nullable: false }),
      unit_price_cents: col.real(myOrderItemsRef, "unit_price_cents", { nullable: false }),
    }),
  },
);

const productViewCountsRef = facadeSchemaBuilder.table(
  "product_view_counts",
  productViewCountsEntity,
  {
    columns: ({ col }) => ({
      product_id: col.string("product_id", {
        nullable: false,
        foreignKey: {
          table: "active_products",
          column: "id",
        },
      }),
      view_count: col.integer("view_count", { nullable: false }),
    }),
  },
);

facadeSchemaBuilder.view(
  "product_engagement",
  ({ scan, join, col, expr }) =>
    join({
      left: scan(activeProductsRef),
      right: scan(productViewCountsRef),
      on: expr.eq(col(activeProductsRef, "id"), col(productViewCountsRef, "product_id")),
      type: "left",
    }),
  {
    columns: ({ col, expr }) => ({
      product_id: col.id(activeProductsRef, "id"),
      product_sku: col.string(activeProductsRef, "sku", { nullable: false }),
      product_name: col.string(activeProductsRef, "name", { nullable: false }),
      product_category: col.string(activeProductsRef, "category", {
        nullable: false,
        enum: ["hardware", "software", "services"] as const,
      }),
      view_count: col.integer(productViewCountsRef, "view_count", { nullable: true }),
      engagement_score: col.real(
        expr.divide(expr.add(col("view_count"), expr.literal(1)), expr.literal(10)),
        { nullable: false },
      ),
    }),
  },
);

facadeSchemaBuilder.view(
  "product_performance",
  ({ scan, aggregate, col, agg }) =>
    aggregate({
      from: scan(myOrderLinesRef),
      groupBy: {
        product_id: col(myOrderLinesRef, "product_id"),
        product_name: col(myOrderLinesRef, "product_name"),
        product_category: col(myOrderLinesRef, "product_category"),
      },
      measures: {
        line_count: agg.count(),
        units_sold: agg.sum(col(myOrderLinesRef, "quantity")),
        revenue_cents: agg.sum(col(myOrderLinesRef, "line_total_cents")),
      },
    }),
  {
    columns: ({ col }) => ({
      product_id: col.id("product_id", {
        foreignKey: {
          table: "active_products",
          column: "id",
        },
      }),
      product_name: col.string("product_name", { nullable: false }),
      product_category: col.string("product_category", {
        nullable: false,
        enum: ["hardware", "software", "services"] as const,
      }),
      line_count: col.integer("line_count", { nullable: false }),
      units_sold: col.integer("units_sold", { nullable: false }),
      revenue_cents: col.integer("revenue_cents", { nullable: false }),
    }),
  },
);

export const FACADE_SCHEMA: SchemaDefinition = facadeSchemaBuilder.build();

export const GENERATED_DB_MODULE_ID = "./generated-db";
export const DB_PROVIDER_MODULE_ID = "./db-provider";
export const REDIS_PROVIDER_MODULE_ID = "./redis-provider";
export const CONTEXT_MODULE_ID = "./context";

function dedent(text: string): string {
  const trimmed = text.replace(/^\n/u, "").replace(/\n\s*$/u, "");
  const lines = trimmed.split("\n");
  const margin = lines.reduce<number>((smallest, line) => {
    if (line.trim().length === 0) {
      return smallest;
    }
    const indent = line.match(/^\s*/u)?.[0].length ?? 0;
    return Math.min(smallest, indent);
  }, Number.POSITIVE_INFINITY);

  if (!Number.isFinite(margin) || margin === 0) {
    return trimmed;
  }

  return lines.map((line) => line.slice(margin)).join("\n");
}

export const DEFAULT_CONTEXT_CODE = dedent(`
  import { drizzle } from "drizzle-orm/pglite";
  import type { RedisLike } from "@tupl/provider-ioredis";

  export type QueryContext = {
    orgId: string;
    userId: string;
    db: ReturnType<typeof drizzle>;
    redis: RedisLike;
  };
`);

export const DEFAULT_GENERATED_DB_FILE_CODE = dedent(`
  // Generated from the downstream Postgres model used by the playground.
  // This file is read-only in the editor.
  import { boolean, integer, pgTable, text, timestamp } from "drizzle-orm/pg-core";

  const orgsTable = pgTable("orgs", {
    id: text("id").primaryKey().notNull(),
    name: text("name").notNull(),
  });

  const usersTable = pgTable("users", {
    id: text("id").primaryKey().notNull(),
    org_id: text("org_id").notNull(),
    email: text("email").notNull(),
    display_name: text("display_name").notNull(),
    role: text("role").notNull(),
  });

  const vendorsTable = pgTable("vendors", {
    id: text("id").primaryKey().notNull(),
    org_id: text("org_id").notNull(),
    name: text("name").notNull(),
    tier: text("tier").notNull(),
  });

  const productsTable = pgTable("products", {
    id: text("id").primaryKey().notNull(),
    org_id: text("org_id").notNull(),
    sku: text("sku").notNull(),
    name: text("name").notNull(),
    category: text("category").notNull(),
    active: boolean("active").notNull(),
  });

  const ordersTable = pgTable("orders", {
    id: text("id").primaryKey().notNull(),
    org_id: text("org_id").notNull(),
    user_id: text("user_id").notNull(),
    vendor_id: text("vendor_id").notNull(),
    status: text("status").notNull(),
    total_cents: integer("total_cents").notNull(),
    created_at: timestamp("created_at", { mode: "string" }).notNull(),
  });

  const orderItemsTable = pgTable("order_items", {
    id: text("id").primaryKey().notNull(),
    org_id: text("org_id").notNull(),
    user_id: text("user_id").notNull(),
    order_id: text("order_id").notNull(),
    product_id: text("product_id").notNull(),
    quantity: integer("quantity").notNull(),
    line_total_cents: integer("line_total_cents").notNull(),
  });

  const userProductAccessTable = pgTable("user_product_access", {
    id: text("id").primaryKey().notNull(),
    user_id: text("user_id").notNull(),
    product_id: text("product_id").notNull(),
  });

  export const tables = {
    orgs: orgsTable,
    users: usersTable,
    vendors: vendorsTable,
    products: productsTable,
    orders: ordersTable,
    order_items: orderItemsTable,
    user_product_access: userProductAccessTable,
  } as const;
`);

export const DEFAULT_DB_PROVIDER_CODE = dedent(`
  import { and, eq } from "drizzle-orm";
  import { createDrizzleProvider } from "@tupl/provider-drizzle";
  import type { QueryContext } from "${CONTEXT_MODULE_ID}";
  import { tables } from "${GENERATED_DB_MODULE_ID}";

  const providerTables = {
    orders: {
      table: tables.orders,
      scope: (ctx: QueryContext) =>
        and(
          eq(tables.orders.org_id, ctx.orgId),
          eq(tables.orders.user_id, ctx.userId),
        ),
    },
    order_items: {
      table: tables.order_items,
      scope: (ctx: QueryContext) =>
        and(
          eq(tables.order_items.org_id, ctx.orgId),
          eq(tables.order_items.user_id, ctx.userId),
        ),
    },
    vendors: {
      table: tables.vendors,
      scope: (ctx: QueryContext) => eq(tables.vendors.org_id, ctx.orgId),
    },
    products: {
      table: tables.products,
      scope: (ctx: QueryContext) =>
        and(
          eq(tables.products.org_id, ctx.orgId),
          eq(tables.products.active, true),
        ),
    },
    user_product_access: {
      table: tables.user_product_access,
      scope: (ctx: QueryContext) => eq(tables.user_product_access.user_id, ctx.userId),
    },
  };

  export const dbProvider = createDrizzleProvider({
    name: "dbProvider",
    dialect: "postgres",
    db: (ctx: QueryContext) => ctx.db,
    tables: providerTables,
  });
`);

export const DEFAULT_REDIS_PROVIDER_CODE = dedent(`
  import {
    createIoredisProvider,
    playgroundIoredisRuntime,
  } from "@playground/provider-ioredis-provider-core";
  import type { QueryContext } from "${CONTEXT_MODULE_ID}";

  export const redisProvider = createIoredisProvider<QueryContext>({
    name: "redisProvider",
    redis: (ctx: QueryContext) => ctx.redis,
    recordOperation: playgroundIoredisRuntime.recordOperation,
    entities: {
      product_view_counts: {
        entity: "product_view_counts",
        lookupKey: "product_id",
        columns: ["product_id", "view_count"] as const,
        buildRedisKey({ key, context }) {
          return \`product_view_counts:\${context.userId}:\${String(key)}\`;
        },
        decodeRow({ hash }) {
          if (typeof hash.product_id !== "string" || typeof hash.view_count !== "string") {
            return null;
          }
          const viewCount = Number(hash.view_count);
          if (!Number.isFinite(viewCount)) {
            return null;
          }
          return {
            product_id: hash.product_id,
            view_count: Math.trunc(viewCount),
          };
        },
      },
    },
  });
`);

export const DEFAULT_FACADE_SCHEMA_CODE = dedent(`
  import { createExecutableSchema, createSchemaBuilder } from "@tupl/schema";
  import type { QueryContext } from "${CONTEXT_MODULE_ID}";
  import { dbProvider } from "${DB_PROVIDER_MODULE_ID}";
  import { redisProvider } from "${REDIS_PROVIDER_MODULE_ID}";

  const builder = createSchemaBuilder<QueryContext>();

  builder.table("my_orders", dbProvider.entities.orders, {
    columns: ({ col, expr }) => ({
      id: col.id("id"),
      vendor_id: col.string("vendor_id", {
        nullable: false,
        foreignKey: {
          table: "vendors_for_org",
          column: "id",
        },
      }),
      status: col.string("status", {
        nullable: false,
        enum: ["pending", "paid", "shipped"] as const,
      }),
      total_cents: col.integer("total_cents", { nullable: false }),
      created_at: col.timestamp("created_at", { nullable: false }),
      total_dollars: col.real(
        expr.divide(col("total_cents"), expr.literal(100)),
        { nullable: false },
      ),
      is_large_order: col.boolean(
        expr.gte(col("total_cents"), expr.literal(30000)),
        { nullable: false },
      ),
    }),
  });

  const myOrderItemsRef = builder.table("my_order_items", dbProvider.entities.order_items, {
    columns: ({ col, expr }) => ({
      id: col.id("id"),
      order_id: col.string("order_id", {
        nullable: false,
        foreignKey: {
          table: "my_orders",
          column: "id",
        },
      }),
      product_id: col.string("product_id", {
        nullable: false,
        foreignKey: {
          table: "active_products",
          column: "id",
        },
      }),
      quantity: col.integer("quantity", { nullable: false }),
      line_total_cents: col.integer("line_total_cents", { nullable: false }),
      unit_price_cents: col.real(
        expr.divide(col("line_total_cents"), col("quantity")),
        { nullable: false },
      ),
    }),
  });

  builder.table("vendors_for_org", dbProvider.entities.vendors, {
    columns: ({ col }) => ({
      id: col.id("id"),
      name: col.string("name", { nullable: false }),
      tier: col.string("tier", {
        nullable: false,
        enum: ["standard", "preferred"] as const,
      }),
    }),
  });

  const productsForOrgRef = builder.table("products_for_org", dbProvider.entities.products, {
    columns: ({ col }) => ({
      id: col.id("id"),
      sku: col.string("sku", { nullable: false }),
      name: col.string("name", { nullable: false }),
      category: col.string("category", {
        nullable: false,
        enum: ["hardware", "software", "services"] as const,
      }),
    }),
  });

  const productAccessForUserRef = builder.table(
    "product_access_for_user",
    dbProvider.entities.user_product_access,
    {
      columns: ({ col }) => ({
        product_id: col.string("product_id", {
          nullable: false,
          foreignKey: {
            table: "products_for_org",
            column: "id",
          },
        }),
      }),
    },
  );

  const activeProductsRef = builder.view(
    "active_products",
    ({ scan, join, col, expr }) =>
      join({
        left: scan(productsForOrgRef),
        right: scan(productAccessForUserRef),
        on: expr.eq(
          col(productsForOrgRef, "id"),
          col(productAccessForUserRef, "product_id"),
        ),
        type: "inner",
      }),
    {
      columns: ({ col }) => ({
        id: col.id(productsForOrgRef, "id"),
        sku: col.string(productsForOrgRef, "sku", { nullable: false }),
        name: col.string(productsForOrgRef, "name", { nullable: false }),
        category: col.string(productsForOrgRef, "category", {
          nullable: false,
          enum: ["hardware", "software", "services"] as const,
        }),
      }),
    },
  );

  const myOrderLinesRef = builder.view(
    "my_order_lines",
    ({ scan, join, col, expr }) =>
      join({
        left: scan(myOrderItemsRef),
        right: scan(activeProductsRef),
        on: expr.eq(
          col(myOrderItemsRef, "product_id"),
          col(activeProductsRef, "id"),
        ),
        type: "inner",
      }),
    {
      columns: ({ col }) => ({
        order_id: col.string(myOrderItemsRef, "order_id", {
          nullable: false,
          foreignKey: {
            table: "my_orders",
            column: "id",
          },
        }),
        product_id: col.string(activeProductsRef, "id", {
          nullable: false,
          foreignKey: {
            table: "active_products",
            column: "id",
          },
        }),
        product_sku: col.string(activeProductsRef, "sku", { nullable: false }),
        product_name: col.string(activeProductsRef, "name", { nullable: false }),
        product_category: col.string(activeProductsRef, "category", {
          nullable: false,
          enum: ["hardware", "software", "services"] as const,
        }),
        quantity: col.integer(myOrderItemsRef, "quantity", { nullable: false }),
        line_total_cents: col.integer(myOrderItemsRef, "line_total_cents", { nullable: false }),
        unit_price_cents: col.real(myOrderItemsRef, "unit_price_cents", { nullable: false }),
      }),
    },
  );

  const productViewCountsRef = builder.table(
    "product_view_counts",
    redisProvider.entities.product_view_counts,
    {
      columns: ({ col }) => ({
        product_id: col.string("product_id", {
          nullable: false,
          foreignKey: {
            table: "active_products",
            column: "id",
          },
        }),
        view_count: col.integer("view_count", { nullable: false }),
      }),
    },
  );

  builder.view(
    "product_engagement",
    ({ scan, join, col, expr }) =>
      join({
        left: scan(activeProductsRef),
        right: scan(productViewCountsRef),
        on: expr.eq(
          col(activeProductsRef, "id"),
          col(productViewCountsRef, "product_id"),
        ),
        type: "left",
      }),
    {
      columns: ({ col, expr }) => ({
        product_id: col.id(activeProductsRef, "id"),
        product_sku: col.string(activeProductsRef, "sku", { nullable: false }),
        product_name: col.string(activeProductsRef, "name", { nullable: false }),
        product_category: col.string(activeProductsRef, "category", {
          nullable: false,
          enum: ["hardware", "software", "services"] as const,
        }),
        view_count: col.integer(productViewCountsRef, "view_count", { nullable: true }),
        engagement_score: col.real(
          expr.divide(expr.add(col("view_count"), expr.literal(1)), expr.literal(10)),
          { nullable: false },
        ),
      }),
    },
  );

  builder.view(
    "product_performance",
    ({ scan, aggregate, col, agg }) =>
      aggregate({
        from: scan(myOrderLinesRef),
        groupBy: {
          product_id: col(myOrderLinesRef, "product_id"),
          product_name: col(myOrderLinesRef, "product_name"),
          product_category: col(myOrderLinesRef, "product_category"),
        },
        measures: {
          line_count: agg.count(),
          units_sold: agg.sum(col(myOrderLinesRef, "quantity")),
          revenue_cents: agg.sum(col(myOrderLinesRef, "line_total_cents")),
        },
      }),
    {
      columns: ({ col }) => ({
        product_id: col.id("product_id", {
          foreignKey: {
            table: "active_products",
            column: "id",
          },
        }),
        product_name: col.string("product_name", { nullable: false }),
        product_category: col.string("product_category", {
          nullable: false,
          enum: ["hardware", "software", "services"] as const,
        }),
        line_count: col.integer("line_count", { nullable: false }),
        units_sold: col.integer("units_sold", { nullable: false }),
        revenue_cents: col.integer("revenue_cents", { nullable: false }),
      }),
    },
  );

  export const executableSchema = createExecutableSchema(builder);
`);

export const QUERY_PRESETS: PlaygroundQueryPreset[] = [
  {
    id: "orders_calculated_columns",
    label: "Orders using calculated columns",
    description: "Filter and sort directly on calculated columns defined on a base table.",
    sql: `
SELECT id, total_cents, total_dollars, is_large_order
FROM my_orders
WHERE total_dollars >= 200
ORDER BY total_dollars DESC, id;
    `.trim(),
  },
  {
    id: "orders_with_vendors",
    label: "My orders with vendors",
    description: "Simple join over facade tables; downstream adds org/user scope.",
    sql: `
SELECT o.id, o.status, o.total_cents, o.created_at, v.name AS vendor_name
FROM my_orders o
JOIN vendors_for_org v ON o.vendor_id = v.id
ORDER BY o.created_at DESC;
    `.trim(),
  },
  {
    id: "vendor_spend",
    label: "My spend by vendor",
    description: "Join + grouped aggregate with ORDER BY metric alias.",
    sql: `
SELECT v.name AS vendor_name, COUNT(*) AS order_count, SUM(o.total_cents) AS spend_cents
FROM my_orders o
JOIN vendors_for_org v ON o.vendor_id = v.id
GROUP BY v.name
ORDER BY spend_cents DESC;
    `.trim(),
  },
  {
    id: "items_with_products",
    label: "My order lines",
    description: "Query a composed facade view that joins order items to the active-products view.",
    sql: `
SELECT order_id, product_sku, product_name, quantity, unit_price_cents, line_total_cents
FROM my_order_lines
ORDER BY order_id, product_sku;
    `.trim(),
  },
  {
    id: "product_engagement",
    label: "Product engagement",
    description: "Cross-provider facade view over SQL-backed products and Redis-backed view counters.",
    sql: `
SELECT product_name, product_category, view_count
FROM product_engagement
ORDER BY view_count DESC, product_name;
    `.trim(),
  },
  {
    id: "product_performance",
    label: "Product performance",
    description: "Aggregate-over-view example built from the derived order-lines facade view.",
    sql: `
SELECT product_name, units_sold, revenue_cents, line_count
FROM product_performance
ORDER BY revenue_cents DESC, product_name;
    `.trim(),
  },
  {
    id: "preferred_vendor_orders",
    label: "Orders from preferred vendors",
    description: "IN subquery over another facade table.",
    sql: `
SELECT id, vendor_id, total_cents
FROM my_orders
WHERE vendor_id IN (
  SELECT id
  FROM vendors_for_org
  WHERE tier = 'preferred'
)
ORDER BY total_cents DESC;
    `.trim(),
  },
  {
    id: "status_distinct",
    label: "Distinct statuses",
    description: "DISTINCT projection shape.",
    sql: `
SELECT DISTINCT status
FROM my_orders
ORDER BY status;
    `.trim(),
  },
  {
    id: "product_coverage",
    label: "Product coverage (left join)",
    description: "LEFT JOIN with aggregate to include products with zero units.",
    sql: `
SELECT p.name, p.category, COUNT(i.id) AS line_count, SUM(i.quantity) AS units
FROM active_products p
LEFT JOIN my_order_items i ON i.product_id = p.id
GROUP BY p.name, p.category
ORDER BY units DESC, p.name;
    `.trim(),
  },
  {
    id: "activity_union",
    label: "Activity union",
    description: "UNION ALL over two facade tables.",
    sql: `
SELECT id, vendor_id AS activity_ref
FROM my_orders
UNION ALL
SELECT id, product_id AS activity_ref
FROM my_order_items
ORDER BY activity_ref, id;
    `.trim(),
  },
  {
    id: "paid_orders",
    label: "Paid orders over threshold",
    description: "Simple filter + ordering.",
    sql: `
SELECT id, total_cents, created_at
FROM my_orders
WHERE status = 'paid' AND total_cents >= 20000
ORDER BY total_cents DESC;
    `.trim(),
  },
  {
    id: "vendor_rank",
    label: "Vendor spend rank",
    description: "CTE + window function shape.",
    sql: `
WITH vendor_totals AS (
  SELECT
    v.name AS vendor_name,
    SUM(o.total_cents) AS spend_cents
  FROM my_orders o
  JOIN vendors_for_org v ON o.vendor_id = v.id
  GROUP BY v.name
)
SELECT
  vendor_name,
  spend_cents,
  DENSE_RANK() OVER (ORDER BY spend_cents DESC) AS spend_rank
FROM vendor_totals
ORDER BY spend_rank, vendor_name;
    `.trim(),
  },
];

const BASE_DOWNSTREAM_ROWS: DownstreamRows = {
  orgs: [
    { id: "org_acme", name: "Acme Corp" },
    { id: "org_globex", name: "Globex LLC" },
    { id: "org_umbrella", name: "Umbrella Industries" },
  ],
  users: [
    {
      id: "u_alex",
      org_id: "org_acme",
      email: "alex@acme.example",
      display_name: "Alex Rivera",
      role: "buyer",
    },
    {
      id: "u_jordan",
      org_id: "org_acme",
      email: "jordan@acme.example",
      display_name: "Jordan Lee",
      role: "manager",
    },
    {
      id: "u_taylor",
      org_id: "org_acme",
      email: "taylor@acme.example",
      display_name: "Taylor Chen",
      role: "buyer",
    },
    {
      id: "u_mina",
      org_id: "org_acme",
      email: "mina@acme.example",
      display_name: "Mina Alvarez",
      role: "buyer",
    },
    {
      id: "u_sam",
      org_id: "org_globex",
      email: "sam@globex.example",
      display_name: "Sam Patel",
      role: "buyer",
    },
    {
      id: "u_riley",
      org_id: "org_globex",
      email: "riley@globex.example",
      display_name: "Riley Brooks",
      role: "manager",
    },
    {
      id: "u_ava",
      org_id: "org_globex",
      email: "ava@globex.example",
      display_name: "Ava Khan",
      role: "buyer",
    },
    {
      id: "u_noah",
      org_id: "org_umbrella",
      email: "noah@umbrella.example",
      display_name: "Noah Stone",
      role: "buyer",
    },
  ],
  vendors: [
    { id: "v_northwind", org_id: "org_acme", name: "Northwind Supply", tier: "preferred" },
    { id: "v_metro", org_id: "org_acme", name: "Metro Parts", tier: "standard" },
    { id: "v_pinnacle", org_id: "org_acme", name: "Pinnacle Systems", tier: "preferred" },
    { id: "v_harbor", org_id: "org_acme", name: "Harbor Industrial", tier: "standard" },
    { id: "v_sunrise", org_id: "org_globex", name: "Sunrise Tech", tier: "preferred" },
    { id: "v_orbit", org_id: "org_globex", name: "Orbit Components", tier: "standard" },
    { id: "v_delta", org_id: "org_globex", name: "Delta Machine Works", tier: "preferred" },
    { id: "v_redline", org_id: "org_umbrella", name: "Redline Logistics", tier: "standard" },
  ],
  products: [
    {
      id: "p_router",
      org_id: "org_acme",
      sku: "RTR-100",
      name: "Edge Router",
      category: "hardware",
      active: true,
    },
    {
      id: "p_backup",
      org_id: "org_acme",
      sku: "BKP-200",
      name: "Backup Service",
      category: "services",
      active: true,
    },
    {
      id: "p_archive",
      org_id: "org_acme",
      sku: "ARC-300",
      name: "Archive Toolkit",
      category: "software",
      active: false,
    },
    {
      id: "p_switch",
      org_id: "org_acme",
      sku: "SWT-410",
      name: "Core Switch",
      category: "hardware",
      active: true,
    },
    {
      id: "p_support",
      org_id: "org_acme",
      sku: "SUP-520",
      name: "Premium Support",
      category: "services",
      active: true,
    },
    {
      id: "p_audit",
      org_id: "org_acme",
      sku: "AUD-610",
      name: "Compliance Audit",
      category: "software",
      active: false,
    },
    {
      id: "p_sensor",
      org_id: "org_globex",
      sku: "SNS-500",
      name: "Plant Sensor",
      category: "hardware",
      active: true,
    },
    {
      id: "p_gateway",
      org_id: "org_globex",
      sku: "GTW-710",
      name: "Edge Gateway",
      category: "hardware",
      active: true,
    },
    {
      id: "p_suite",
      org_id: "org_globex",
      sku: "SWT-800",
      name: "Ops Suite",
      category: "software",
      active: false,
    },
    {
      id: "p_vision",
      org_id: "org_globex",
      sku: "VIS-830",
      name: "Vision Analytics",
      category: "services",
      active: true,
    },
    {
      id: "p_shield",
      org_id: "org_umbrella",
      sku: "SHD-910",
      name: "Shield Device",
      category: "hardware",
      active: true,
    },
  ],
  orders: [
    {
      id: "o_1001",
      org_id: "org_acme",
      user_id: "u_alex",
      vendor_id: "v_northwind",
      status: "paid",
      total_cents: 48000,
      created_at: "2026-02-02T10:00:00Z",
    },
    {
      id: "o_1002",
      org_id: "org_acme",
      user_id: "u_alex",
      vendor_id: "v_metro",
      status: "shipped",
      total_cents: 25500,
      created_at: "2026-02-06T09:30:00Z",
    },
    {
      id: "o_1003",
      org_id: "org_acme",
      user_id: "u_jordan",
      vendor_id: "v_metro",
      status: "pending",
      total_cents: 11500,
      created_at: "2026-02-10T14:15:00Z",
    },
    {
      id: "o_1005",
      org_id: "org_acme",
      user_id: "u_alex",
      vendor_id: "v_pinnacle",
      status: "paid",
      total_cents: 73500,
      created_at: "2026-02-14T11:40:00Z",
    },
    {
      id: "o_1006",
      org_id: "org_acme",
      user_id: "u_alex",
      vendor_id: "v_northwind",
      status: "pending",
      total_cents: 18200,
      created_at: "2026-02-18T16:05:00Z",
    },
    {
      id: "o_1007",
      org_id: "org_acme",
      user_id: "u_jordan",
      vendor_id: "v_harbor",
      status: "shipped",
      total_cents: 32900,
      created_at: "2026-02-20T09:50:00Z",
    },
    {
      id: "o_1008",
      org_id: "org_acme",
      user_id: "u_taylor",
      vendor_id: "v_metro",
      status: "paid",
      total_cents: 40200,
      created_at: "2026-02-22T12:25:00Z",
    },
    {
      id: "o_1009",
      org_id: "org_acme",
      user_id: "u_jordan",
      vendor_id: "v_pinnacle",
      status: "paid",
      total_cents: 56800,
      created_at: "2026-02-24T15:35:00Z",
    },
    {
      id: "o_1010",
      org_id: "org_acme",
      user_id: "u_alex",
      vendor_id: "v_harbor",
      status: "shipped",
      total_cents: 22100,
      created_at: "2026-02-26T08:45:00Z",
    },
    {
      id: "o_2001",
      org_id: "org_globex",
      user_id: "u_sam",
      vendor_id: "v_sunrise",
      status: "paid",
      total_cents: 91000,
      created_at: "2026-02-04T08:20:00Z",
    },
    {
      id: "o_2002",
      org_id: "org_globex",
      user_id: "u_sam",
      vendor_id: "v_orbit",
      status: "shipped",
      total_cents: 34400,
      created_at: "2026-02-11T10:10:00Z",
    },
    {
      id: "o_2003",
      org_id: "org_globex",
      user_id: "u_sam",
      vendor_id: "v_sunrise",
      status: "pending",
      total_cents: 18800,
      created_at: "2026-02-19T13:05:00Z",
    },
    {
      id: "o_2004",
      org_id: "org_globex",
      user_id: "u_riley",
      vendor_id: "v_delta",
      status: "paid",
      total_cents: 62700,
      created_at: "2026-02-25T09:00:00Z",
    },
    {
      id: "o_2005",
      org_id: "org_globex",
      user_id: "u_ava",
      vendor_id: "v_orbit",
      status: "shipped",
      total_cents: 27300,
      created_at: "2026-02-27T14:10:00Z",
    },
    {
      id: "o_3001",
      org_id: "org_umbrella",
      user_id: "u_noah",
      vendor_id: "v_redline",
      status: "paid",
      total_cents: 14900,
      created_at: "2026-02-16T11:55:00Z",
    },
  ],
  order_items: [
    {
      id: "oi_1",
      org_id: "org_acme",
      user_id: "u_alex",
      order_id: "o_1001",
      product_id: "p_router",
      quantity: 2,
      line_total_cents: 36000,
    },
    {
      id: "oi_2",
      org_id: "org_acme",
      user_id: "u_alex",
      order_id: "o_1001",
      product_id: "p_backup",
      quantity: 1,
      line_total_cents: 12000,
    },
    {
      id: "oi_3",
      org_id: "org_acme",
      user_id: "u_alex",
      order_id: "o_1002",
      product_id: "p_backup",
      quantity: 2,
      line_total_cents: 25500,
    },
    {
      id: "oi_4",
      org_id: "org_acme",
      user_id: "u_jordan",
      order_id: "o_1003",
      product_id: "p_router",
      quantity: 1,
      line_total_cents: 11500,
    },
    {
      id: "oi_5",
      org_id: "org_acme",
      user_id: "u_alex",
      order_id: "o_1005",
      product_id: "p_switch",
      quantity: 3,
      line_total_cents: 52500,
    },
    {
      id: "oi_6",
      org_id: "org_acme",
      user_id: "u_alex",
      order_id: "o_1005",
      product_id: "p_support",
      quantity: 1,
      line_total_cents: 21000,
    },
    {
      id: "oi_7",
      org_id: "org_acme",
      user_id: "u_alex",
      order_id: "o_1006",
      product_id: "p_archive",
      quantity: 1,
      line_total_cents: 8200,
    },
    {
      id: "oi_8",
      org_id: "org_acme",
      user_id: "u_alex",
      order_id: "o_1006",
      product_id: "p_backup",
      quantity: 1,
      line_total_cents: 10000,
    },
    {
      id: "oi_9",
      org_id: "org_acme",
      user_id: "u_jordan",
      order_id: "o_1007",
      product_id: "p_switch",
      quantity: 1,
      line_total_cents: 17900,
    },
    {
      id: "oi_10",
      org_id: "org_acme",
      user_id: "u_jordan",
      order_id: "o_1007",
      product_id: "p_router",
      quantity: 1,
      line_total_cents: 15000,
    },
    {
      id: "oi_11",
      org_id: "org_acme",
      user_id: "u_taylor",
      order_id: "o_1008",
      product_id: "p_support",
      quantity: 2,
      line_total_cents: 23200,
    },
    {
      id: "oi_12",
      org_id: "org_acme",
      user_id: "u_taylor",
      order_id: "o_1008",
      product_id: "p_switch",
      quantity: 1,
      line_total_cents: 17000,
    },
    {
      id: "oi_13",
      org_id: "org_acme",
      user_id: "u_jordan",
      order_id: "o_1009",
      product_id: "p_router",
      quantity: 2,
      line_total_cents: 34000,
    },
    {
      id: "oi_14",
      org_id: "org_acme",
      user_id: "u_jordan",
      order_id: "o_1009",
      product_id: "p_switch",
      quantity: 1,
      line_total_cents: 22800,
    },
    {
      id: "oi_15",
      org_id: "org_acme",
      user_id: "u_alex",
      order_id: "o_1010",
      product_id: "p_backup",
      quantity: 1,
      line_total_cents: 12100,
    },
    {
      id: "oi_16",
      org_id: "org_acme",
      user_id: "u_alex",
      order_id: "o_1010",
      product_id: "p_switch",
      quantity: 1,
      line_total_cents: 10000,
    },
    {
      id: "oi_17",
      org_id: "org_globex",
      user_id: "u_sam",
      order_id: "o_2001",
      product_id: "p_sensor",
      quantity: 7,
      line_total_cents: 91000,
    },
    {
      id: "oi_18",
      org_id: "org_globex",
      user_id: "u_sam",
      order_id: "o_2002",
      product_id: "p_gateway",
      quantity: 2,
      line_total_cents: 22400,
    },
    {
      id: "oi_19",
      org_id: "org_globex",
      user_id: "u_sam",
      order_id: "o_2002",
      product_id: "p_sensor",
      quantity: 1,
      line_total_cents: 12000,
    },
    {
      id: "oi_20",
      org_id: "org_globex",
      user_id: "u_sam",
      order_id: "o_2003",
      product_id: "p_vision",
      quantity: 1,
      line_total_cents: 18800,
    },
    {
      id: "oi_21",
      org_id: "org_globex",
      user_id: "u_riley",
      order_id: "o_2004",
      product_id: "p_gateway",
      quantity: 4,
      line_total_cents: 62700,
    },
    {
      id: "oi_22",
      org_id: "org_globex",
      user_id: "u_ava",
      order_id: "o_2005",
      product_id: "p_sensor",
      quantity: 1,
      line_total_cents: 9400,
    },
    {
      id: "oi_23",
      org_id: "org_globex",
      user_id: "u_ava",
      order_id: "o_2005",
      product_id: "p_vision",
      quantity: 1,
      line_total_cents: 17900,
    },
    {
      id: "oi_24",
      org_id: "org_umbrella",
      user_id: "u_noah",
      order_id: "o_3001",
      product_id: "p_shield",
      quantity: 1,
      line_total_cents: 14900,
    },
  ],
  user_product_access: [
    { id: "upa_1", user_id: "u_alex", product_id: "p_router" },
    { id: "upa_2", user_id: "u_alex", product_id: "p_backup" },
    { id: "upa_3", user_id: "u_alex", product_id: "p_switch" },
    { id: "upa_4", user_id: "u_alex", product_id: "p_support" },
    { id: "upa_5", user_id: "u_alex", product_id: "p_archive" },
    { id: "upa_6", user_id: "u_jordan", product_id: "p_router" },
    { id: "upa_7", user_id: "u_jordan", product_id: "p_switch" },
    { id: "upa_8", user_id: "u_jordan", product_id: "p_support" },
    { id: "upa_9", user_id: "u_taylor", product_id: "p_switch" },
    { id: "upa_10", user_id: "u_taylor", product_id: "p_support" },
    { id: "upa_11", user_id: "u_mina", product_id: "p_backup" },
    { id: "upa_12", user_id: "u_sam", product_id: "p_sensor" },
    { id: "upa_13", user_id: "u_sam", product_id: "p_gateway" },
    { id: "upa_14", user_id: "u_sam", product_id: "p_vision" },
    { id: "upa_15", user_id: "u_riley", product_id: "p_gateway" },
    { id: "upa_16", user_id: "u_ava", product_id: "p_sensor" },
    { id: "upa_17", user_id: "u_ava", product_id: "p_vision" },
    { id: "upa_18", user_id: "u_noah", product_id: "p_shield" },
  ],
  redis_product_view_counts: [
    { user_id: "u_alex", product_id: "p_router", view_count: 94 },
    { user_id: "u_alex", product_id: "p_backup", view_count: 72 },
    { user_id: "u_alex", product_id: "p_switch", view_count: 45 },
    { user_id: "u_alex", product_id: "p_support", view_count: 28 },
    { user_id: "u_alex", product_id: "p_archive", view_count: 17 },
    { user_id: "u_jordan", product_id: "p_router", view_count: 61 },
    { user_id: "u_jordan", product_id: "p_switch", view_count: 39 },
    { user_id: "u_jordan", product_id: "p_support", view_count: 26 },
    { user_id: "u_taylor", product_id: "p_switch", view_count: 33 },
    { user_id: "u_taylor", product_id: "p_support", view_count: 18 },
    { user_id: "u_mina", product_id: "p_backup", view_count: 21 },
    { user_id: "u_sam", product_id: "p_sensor", view_count: 102 },
    { user_id: "u_sam", product_id: "p_gateway", view_count: 67 },
    { user_id: "u_sam", product_id: "p_vision", view_count: 31 },
    { user_id: "u_riley", product_id: "p_gateway", view_count: 49 },
    { user_id: "u_ava", product_id: "p_sensor", view_count: 37 },
    { user_id: "u_ava", product_id: "p_vision", view_count: 22 },
    { user_id: "u_noah", product_id: "p_shield", view_count: 13 },
  ],
};

function cloneRows(rows: DownstreamRows): DownstreamRows {
  return JSON.parse(JSON.stringify(rows)) as DownstreamRows;
}

function withJordanExpansion(): DownstreamRows {
  const rows = cloneRows(BASE_DOWNSTREAM_ROWS);
  rows.orders = [
    ...(rows.orders ?? []),
    {
      id: "o_1004",
      org_id: "org_acme",
      user_id: "u_jordan",
      vendor_id: "v_northwind",
      status: "paid",
      total_cents: 64000,
      created_at: "2026-02-12T12:00:00Z",
    },
  ];
  rows.order_items = [
    ...(rows.order_items ?? []),
    {
      id: "oi_25",
      org_id: "org_acme",
      user_id: "u_jordan",
      order_id: "o_1004",
      product_id: "p_router",
      quantity: 3,
      line_total_cents: 64000,
    },
  ];
  return rows;
}

export const SCENARIO_PRESETS: PlaygroundScenarioPreset[] = [
  {
    id: "acme_alex",
    label: "Acme: Alex",
    description:
      "Alex is a buyer in Acme. Data includes multiple vendors and active/inactive products.",
    context: {
      orgId: "org_acme",
      userId: "u_alex",
    },
    rows: cloneRows(BASE_DOWNSTREAM_ROWS),
    defaultQueryId: "orders_calculated_columns",
  },
  {
    id: "acme_jordan",
    label: "Acme: Jordan",
    description: "Jordan is a manager in Acme with a larger paid order profile.",
    context: {
      orgId: "org_acme",
      userId: "u_jordan",
    },
    rows: withJordanExpansion(),
    defaultQueryId: "product_performance",
  },
  {
    id: "globex_sam",
    label: "Globex: Sam",
    description: "Sam is a buyer in a different org to demonstrate strict org/user lens scoping.",
    context: {
      orgId: "org_globex",
      userId: "u_sam",
    },
    rows: cloneRows(BASE_DOWNSTREAM_ROWS),
    defaultQueryId: "product_engagement",
  },
];

export const DEFAULT_SCENARIO_ID = SCENARIO_PRESETS[0]?.id ?? "acme_alex";
export const DEFAULT_QUERY_ID =
  SCENARIO_PRESETS[0]?.defaultQueryId ?? QUERY_PRESETS[0]?.id ?? "orders_with_vendors";

export function buildQueryCatalog(queries: PlaygroundQueryPreset[]): CatalogQueryEntry[] {
  return queries.map((query) => ({
    id: query.id,
    label: query.label,
    sql: query.sql,
    ...(query.description ? { description: query.description } : {}),
  }));
}

export function serializeJson(value: unknown): string {
  return `${JSON.stringify(value, null, 2)}\n`;
}
