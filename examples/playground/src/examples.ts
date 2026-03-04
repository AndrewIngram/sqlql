import {
  createDataEntityHandle,
  defineSchema,
  type SchemaDefinition,
} from "sqlql";

import type {
  CatalogQueryEntry,
  DownstreamRows,
  PlaygroundQueryPreset,
  PlaygroundScenarioPreset,
} from "./types";

const myOrdersEntity = createDataEntityHandle<
  "id" | "vendor_id" | "status" | "total_cents" | "created_at"
>({
  provider: "dbProvider",
  entity: "my_orders",
});
const myOrderItemsEntity = createDataEntityHandle<
  "id" | "order_id" | "product_id" | "quantity" | "line_total_cents"
>({
  provider: "dbProvider",
  entity: "my_order_items",
});
const vendorsForOrgEntity = createDataEntityHandle<"id" | "name" | "tier">({
  provider: "dbProvider",
  entity: "vendors_for_org",
});
const activeProductsEntity = createDataEntityHandle<"id" | "sku" | "name" | "category">({
  provider: "dbProvider",
  entity: "active_products",
});
const productViewCountsEntity = createDataEntityHandle<"product_id" | "view_count">({
  provider: "kvProvider",
  entity: "product_view_counts",
});

export const FACADE_SCHEMA: SchemaDefinition = defineSchema(({ table, view, rel, expr, col }) => {
  const myOrders = table({
    from: myOrdersEntity,
    columns: {
      id: { source: col(myOrdersEntity, "id"), type: "text", nullable: false, primaryKey: true },
      vendor_id: {
        source: col(myOrdersEntity, "vendor_id"),
        type: "text",
        nullable: false,
        foreignKey: {
          table: "vendors_for_org",
          column: "id",
        },
      },
      status: {
        source: col(myOrdersEntity, "status"),
        type: "text",
        nullable: false,
        enum: ["pending", "paid", "shipped"] as const,
      },
      total_cents: { source: col(myOrdersEntity, "total_cents"), type: "integer", nullable: false },
      created_at: { source: col(myOrdersEntity, "created_at"), type: "timestamp", nullable: false },
    },
  });

  const myOrderItems = table({
    from: myOrderItemsEntity,
    columns: {
      id: { source: col(myOrderItemsEntity, "id"), type: "text", nullable: false, primaryKey: true },
      order_id: {
        source: col(myOrderItemsEntity, "order_id"),
        type: "text",
        nullable: false,
        foreignKey: {
          table: "my_orders",
          column: "id",
        },
      },
      product_id: {
        source: col(myOrderItemsEntity, "product_id"),
        type: "text",
        nullable: false,
        foreignKey: {
          table: "active_products",
          column: "id",
        },
      },
      quantity: { source: col(myOrderItemsEntity, "quantity"), type: "integer", nullable: false },
      line_total_cents: {
        source: col(myOrderItemsEntity, "line_total_cents"),
        type: "integer",
        nullable: false,
      },
    },
  });

  const vendorsForOrg = table({
    from: vendorsForOrgEntity,
    columns: {
      id: { source: col(vendorsForOrgEntity, "id"), type: "text", nullable: false, primaryKey: true },
      name: { source: col(vendorsForOrgEntity, "name"), type: "text", nullable: false },
      tier: {
        source: col(vendorsForOrgEntity, "tier"),
        type: "text",
        nullable: false,
        enum: ["standard", "preferred"] as const,
      },
    },
  });

  const activeProducts = table({
    from: activeProductsEntity,
    columns: {
      id: { source: col(activeProductsEntity, "id"), type: "text", nullable: false, primaryKey: true },
      sku: { source: col(activeProductsEntity, "sku"), type: "text", nullable: false },
      name: { source: col(activeProductsEntity, "name"), type: "text", nullable: false },
      category: {
        source: col(activeProductsEntity, "category"),
        type: "text",
        nullable: false,
        enum: ["hardware", "software", "services"] as const,
      },
    },
  });

  const myOrderLines = view({
    rel: () =>
      rel.join({
        left: rel.scan(myOrderItems),
        right: rel.scan(activeProducts),
        on: expr.eq(
          col(myOrderItems, "product_id"),
          col(activeProducts, "id"),
        ),
        type: "inner",
      }),
    columns: {
      order_id: {
        source: col(myOrderItems, "order_id"),
        type: "text",
        nullable: false,
        foreignKey: {
          table: "my_orders",
          column: "id",
        },
      },
      product_id: {
        source: col(activeProducts, "id"),
        type: "text",
        nullable: false,
        foreignKey: {
          table: "active_products",
          column: "id",
        },
      },
      product_sku: { source: col(activeProducts, "sku"), type: "text", nullable: false },
      product_name: { source: col(activeProducts, "name"), type: "text", nullable: false },
      product_category: {
        source: col(activeProducts, "category"),
        type: "text",
        nullable: false,
        enum: ["hardware", "software", "services"] as const,
      },
      quantity: { source: col(myOrderItems, "quantity"), type: "integer", nullable: false },
      line_total_cents: {
        source: col(myOrderItems, "line_total_cents"),
        type: "integer",
        nullable: false,
      },
    },
  });

  const productViewCounts = table({
    from: productViewCountsEntity,
    columns: {
      product_id: {
        source: col(productViewCountsEntity, "product_id"),
        type: "text",
        nullable: false,
        foreignKey: {
          table: "active_products",
          column: "id",
        },
      },
      view_count: { source: col(productViewCountsEntity, "view_count"), type: "integer", nullable: false },
    },
  });

  return {
    tables: {
      my_orders: myOrders,
      my_order_items: myOrderItems,
      vendors_for_org: vendorsForOrg,
      active_products: activeProducts,
      my_order_lines: myOrderLines,
      product_view_counts: productViewCounts,
    },
  };
});

export const GENERATED_DB_MODULE_ID = "./generated-db";
export const DB_PROVIDER_MODULE_ID = "./db-provider";
export const KV_PROVIDER_MODULE_ID = "./kv-provider";

export const DEFAULT_GENERATED_DB_FILE_CODE = `
// Generated from the downstream Postgres model used by the playground.
// This file is read-only in the editor.
import { PGlite } from "https://cdn.jsdelivr.net/npm/@electric-sql/pglite/dist/index.js";
import { drizzle } from "drizzle-orm/pglite";
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

const client = new PGlite();
export const db = drizzle({ client });
`.trim();

export const DEFAULT_DB_PROVIDER_CODE = `
import { createDrizzleProvider } from "@sqlql/drizzle";
import { db, tables } from "${GENERATED_DB_MODULE_ID}";

const tableConfigs = {
  my_orders: {
    table: tables.orders,
  },
  my_order_items: {
    table: tables.order_items,
  },
  vendors_for_org: {
    table: tables.vendors,
  },
  active_products: {
    table: tables.products,
  },
};

export const dbProvider = createDrizzleProvider({
  name: "dbProvider",
  db,
  tables: tableConfigs,
});
`.trim();

export const DEFAULT_KV_PROVIDER_CODE = `
import { createDataEntityHandle } from "sqlql";

const productViewCounts = createDataEntityHandle<"product_id" | "view_count">({
  provider: "kvProvider",
  entity: "product_view_counts",
});

export const kvProvider = {
  tables: {
    product_view_counts: productViewCounts,
  },
  entities: {
    product_view_counts: productViewCounts,
  },
} as const;
`.trim();

export const DEFAULT_FACADE_SCHEMA_CODE = `
import { defineSchema } from "sqlql";
import { dbProvider } from "${DB_PROVIDER_MODULE_ID}";
import { kvProvider } from "${KV_PROVIDER_MODULE_ID}";

export const schema = defineSchema(({ table, view, rel, expr, col }) => {
  const myOrders = table({
    from: dbProvider.tables.my_orders,
    columns: {
      id: { source: col(dbProvider.tables.my_orders, "id"), type: "text", nullable: false, primaryKey: true },
      vendor_id: {
        source: col(dbProvider.tables.my_orders, "vendor_id"),
        type: "text",
        nullable: false,
        foreignKey: {
          table: "vendors_for_org",
          column: "id",
        },
      },
      status: { source: col(dbProvider.tables.my_orders, "status"), type: "text", nullable: false, enum: ["pending", "paid", "shipped"] as const },
      total_cents: { source: col(dbProvider.tables.my_orders, "total_cents"), type: "integer", nullable: false },
      created_at: { source: col(dbProvider.tables.my_orders, "created_at"), type: "timestamp", nullable: false },
    },
  });

  const myOrderItems = table({
    from: dbProvider.tables.my_order_items,
    columns: {
      id: { source: col(dbProvider.tables.my_order_items, "id"), type: "text", nullable: false, primaryKey: true },
      order_id: {
        source: col(dbProvider.tables.my_order_items, "order_id"),
        type: "text",
        nullable: false,
        foreignKey: {
          table: "my_orders",
          column: "id",
        },
      },
      product_id: {
        source: col(dbProvider.tables.my_order_items, "product_id"),
        type: "text",
        nullable: false,
        foreignKey: {
          table: "active_products",
          column: "id",
        },
      },
      quantity: { source: col(dbProvider.tables.my_order_items, "quantity"), type: "integer", nullable: false },
      line_total_cents: { source: col(dbProvider.tables.my_order_items, "line_total_cents"), type: "integer", nullable: false },
    },
  });

  const vendorsForOrg = table({
    from: dbProvider.tables.vendors_for_org,
    columns: {
      id: { source: col(dbProvider.tables.vendors_for_org, "id"), type: "text", nullable: false, primaryKey: true },
      name: { source: col(dbProvider.tables.vendors_for_org, "name"), type: "text", nullable: false },
      tier: { source: col(dbProvider.tables.vendors_for_org, "tier"), type: "text", nullable: false, enum: ["standard", "preferred"] as const },
    },
  });

  const activeProducts = table({
    from: dbProvider.tables.active_products,
    columns: {
      id: { source: col(dbProvider.tables.active_products, "id"), type: "text", nullable: false, primaryKey: true },
      sku: { source: col(dbProvider.tables.active_products, "sku"), type: "text", nullable: false },
      name: { source: col(dbProvider.tables.active_products, "name"), type: "text", nullable: false },
      category: {
        source: col(dbProvider.tables.active_products, "category"),
        type: "text",
        nullable: false,
        enum: ["hardware", "software", "services"] as const,
      },
    },
  });

  const myOrderLines = view({
    rel: () =>
      rel.join({
        left: rel.scan(myOrderItems),
        right: rel.scan(activeProducts),
        on: expr.eq(
          col(myOrderItems, "product_id"),
          col(activeProducts, "id"),
        ),
        type: "inner",
      }),
    columns: {
      order_id: {
        source: col(myOrderItems, "order_id"),
        type: "text",
        nullable: false,
        foreignKey: {
          table: "my_orders",
          column: "id",
        },
      },
      product_id: {
        source: col(activeProducts, "id"),
        type: "text",
        nullable: false,
        foreignKey: {
          table: "active_products",
          column: "id",
        },
      },
      product_sku: { source: col(activeProducts, "sku"), type: "text", nullable: false },
      product_name: { source: col(activeProducts, "name"), type: "text", nullable: false },
      product_category: {
        source: col(activeProducts, "category"),
        type: "text",
        nullable: false,
        enum: ["hardware", "software", "services"] as const,
      },
      quantity: { source: col(myOrderItems, "quantity"), type: "integer", nullable: false },
      line_total_cents: { source: col(myOrderItems, "line_total_cents"), type: "integer", nullable: false },
    },
  });

  const productViewCounts = table({
    from: kvProvider.tables.product_view_counts,
    columns: {
      product_id: {
        source: col(kvProvider.tables.product_view_counts, "product_id"),
        type: "text",
        nullable: false,
        foreignKey: {
          table: "active_products",
          column: "id",
        },
      },
      view_count: { source: col(kvProvider.tables.product_view_counts, "view_count"), type: "integer", nullable: false },
    },
  });

  return {
    tables: {
      my_orders: myOrders,
      my_order_items: myOrderItems,
      vendors_for_org: vendorsForOrg,
      active_products: activeProducts,
      my_order_lines: myOrderLines,
      product_view_counts: productViewCounts,
    },
  };
});
`.trim();

export const QUERY_PRESETS: PlaygroundQueryPreset[] = [
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
    description: "Query a derived facade view that joins order items with product attributes.",
    sql: `
SELECT order_id, product_sku, product_name, quantity, line_total_cents
FROM my_order_lines
ORDER BY order_id, product_sku;
    `.trim(),
  },
  {
    id: "top_products",
    label: "Top products by spend",
    description: "Aggregate directly over the derived order-lines view.",
    sql: `
SELECT product_name, SUM(line_total_cents) AS spend_cents, SUM(quantity) AS units
FROM my_order_lines
GROUP BY product_name
ORDER BY spend_cents DESC;
    `.trim(),
  },
  {
    id: "product_views",
    label: "Product views from KV",
    description: "Cross-provider LEFT JOIN over SQL product catalog and KV view counters.",
    sql: `
SELECT p.name, v.view_count
FROM active_products p
LEFT JOIN product_view_counts v ON v.product_id = p.id
ORDER BY v.view_count DESC, p.name;
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
  kv_product_views: [
    { key: "u_alex:p_router", value: 94 },
    { key: "u_alex:p_backup", value: 72 },
    { key: "u_alex:p_switch", value: 45 },
    { key: "u_alex:p_support", value: 28 },
    { key: "u_alex:p_archive", value: 17 },
    { key: "u_jordan:p_router", value: 61 },
    { key: "u_jordan:p_switch", value: 39 },
    { key: "u_jordan:p_support", value: 26 },
    { key: "u_taylor:p_switch", value: 33 },
    { key: "u_taylor:p_support", value: 18 },
    { key: "u_mina:p_backup", value: 21 },
    { key: "u_sam:p_sensor", value: 102 },
    { key: "u_sam:p_gateway", value: 67 },
    { key: "u_sam:p_vision", value: 31 },
    { key: "u_riley:p_gateway", value: 49 },
    { key: "u_ava:p_sensor", value: 37 },
    { key: "u_ava:p_vision", value: 22 },
    { key: "u_noah:p_shield", value: 13 },
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
    description: "Alex is a buyer in Acme. Data includes multiple vendors and active/inactive products.",
    context: {
      orgId: "org_acme",
      userId: "u_alex",
    },
    rows: cloneRows(BASE_DOWNSTREAM_ROWS),
    defaultQueryId: "orders_with_vendors",
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
    defaultQueryId: "vendor_spend",
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
    defaultQueryId: "items_with_products",
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
