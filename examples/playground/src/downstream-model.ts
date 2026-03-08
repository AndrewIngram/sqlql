import { boolean, integer, pgTable, text, timestamp } from "drizzle-orm/pg-core";
import { createDataEntityHandle } from "@tupl/core";
import { createSchemaBuilder, type SchemaDefinition } from "@tupl/schema";

export const orgsTable = pgTable("orgs", {
  id: text("id").primaryKey().notNull(),
  name: text("name").notNull(),
});

export const usersTable = pgTable("users", {
  id: text("id").primaryKey().notNull(),
  org_id: text("org_id")
    .notNull()
    .references(() => orgsTable.id),
  email: text("email").notNull(),
  display_name: text("display_name").notNull(),
  role: text("role").notNull(),
});

export const vendorsTable = pgTable("vendors", {
  id: text("id").primaryKey().notNull(),
  org_id: text("org_id")
    .notNull()
    .references(() => orgsTable.id),
  name: text("name").notNull(),
  tier: text("tier").notNull(),
});

export const productsTable = pgTable("products", {
  id: text("id").primaryKey().notNull(),
  org_id: text("org_id")
    .notNull()
    .references(() => orgsTable.id),
  sku: text("sku").notNull(),
  name: text("name").notNull(),
  category: text("category").notNull(),
  active: boolean("active").notNull(),
});

export const ordersTable = pgTable("orders", {
  id: text("id").primaryKey().notNull(),
  org_id: text("org_id")
    .notNull()
    .references(() => orgsTable.id),
  user_id: text("user_id")
    .notNull()
    .references(() => usersTable.id),
  vendor_id: text("vendor_id")
    .notNull()
    .references(() => vendorsTable.id),
  status: text("status").notNull(),
  total_cents: integer("total_cents").notNull(),
  created_at: timestamp("created_at", { mode: "string" }).notNull(),
});

export const orderItemsTable = pgTable("order_items", {
  id: text("id").primaryKey().notNull(),
  org_id: text("org_id")
    .notNull()
    .references(() => orgsTable.id),
  user_id: text("user_id")
    .notNull()
    .references(() => usersTable.id),
  order_id: text("order_id")
    .notNull()
    .references(() => ordersTable.id),
  product_id: text("product_id")
    .notNull()
    .references(() => productsTable.id),
  quantity: integer("quantity").notNull(),
  line_total_cents: integer("line_total_cents").notNull(),
});

export const userProductAccessTable = pgTable("user_product_access", {
  id: text("id").primaryKey().notNull(),
  user_id: text("user_id")
    .notNull()
    .references(() => usersTable.id),
  product_id: text("product_id")
    .notNull()
    .references(() => productsTable.id),
});

export const DOWNSTREAM_TABLES = {
  orgs: orgsTable,
  users: usersTable,
  vendors: vendorsTable,
  products: productsTable,
  orders: ordersTable,
  order_items: orderItemsTable,
  user_product_access: userProductAccessTable,
};

export const DOWNSTREAM_TABLE_NAMES = Object.keys(DOWNSTREAM_TABLES);

const downstreamRowsSchemaBuilder = createSchemaBuilder<Record<string, never>>();
const downstreamEntity = (name: string) =>
  createDataEntityHandle({ entity: name, provider: "dbProvider" });

downstreamRowsSchemaBuilder.table("orgs", downstreamEntity("orgs"), {
  columns: {
    id: { type: "text", nullable: false },
    name: { type: "text", nullable: false },
  },
});

downstreamRowsSchemaBuilder.table("users", downstreamEntity("users"), {
  columns: {
    id: { type: "text", nullable: false },
    org_id: {
      type: "text",
      nullable: false,
      foreignKey: {
        table: "orgs",
        column: "id",
      },
    },
    email: { type: "text", nullable: false },
    display_name: { type: "text", nullable: false },
    role: {
      type: "text",
      nullable: false,
      physicalType: "user_role",
      enum: ["buyer", "manager"] as const,
    },
  },
});

downstreamRowsSchemaBuilder.table("vendors", downstreamEntity("vendors"), {
  columns: {
    id: { type: "text", nullable: false },
    org_id: {
      type: "text",
      nullable: false,
      foreignKey: {
        table: "orgs",
        column: "id",
      },
    },
    name: { type: "text", nullable: false },
    tier: {
      type: "text",
      nullable: false,
      physicalType: "vendor_tier",
      enum: ["standard", "preferred"] as const,
    },
  },
});

downstreamRowsSchemaBuilder.table("products", downstreamEntity("products"), {
  columns: {
    id: { type: "text", nullable: false },
    org_id: {
      type: "text",
      nullable: false,
      foreignKey: {
        table: "orgs",
        column: "id",
      },
    },
    sku: { type: "text", nullable: false },
    name: { type: "text", nullable: false },
    category: {
      type: "text",
      nullable: false,
      physicalType: "product_category",
      enum: ["hardware", "software", "services"] as const,
    },
    active: { type: "boolean", nullable: false },
  },
});

downstreamRowsSchemaBuilder.table("orders", downstreamEntity("orders"), {
  columns: {
    id: { type: "text", nullable: false },
    org_id: {
      type: "text",
      nullable: false,
      foreignKey: {
        table: "orgs",
        column: "id",
      },
    },
    user_id: {
      type: "text",
      nullable: false,
      foreignKey: {
        table: "users",
        column: "id",
      },
    },
    vendor_id: {
      type: "text",
      nullable: false,
      foreignKey: {
        table: "vendors",
        column: "id",
      },
    },
    status: {
      type: "text",
      nullable: false,
      physicalType: "order_status",
      enum: ["pending", "paid", "shipped"] as const,
    },
    total_cents: { type: "integer", nullable: false, physicalType: "numeric(12,0)" },
    created_at: { type: "timestamp", nullable: false, physicalType: "timestamptz" },
  },
});

downstreamRowsSchemaBuilder.table("order_items", downstreamEntity("order_items"), {
  columns: {
    id: { type: "text", nullable: false },
    org_id: {
      type: "text",
      nullable: false,
      foreignKey: {
        table: "orgs",
        column: "id",
      },
    },
    user_id: {
      type: "text",
      nullable: false,
      foreignKey: {
        table: "users",
        column: "id",
      },
    },
    order_id: {
      type: "text",
      nullable: false,
      foreignKey: {
        table: "orders",
        column: "id",
      },
    },
    product_id: {
      type: "text",
      nullable: false,
      foreignKey: {
        table: "products",
        column: "id",
      },
    },
    quantity: { type: "integer", nullable: false },
    line_total_cents: { type: "integer", nullable: false },
  },
});

downstreamRowsSchemaBuilder.table("user_product_access", downstreamEntity("user_product_access"), {
  columns: {
    id: { type: "text", nullable: false },
    user_id: {
      type: "text",
      nullable: false,
      foreignKey: {
        table: "users",
        column: "id",
      },
    },
    product_id: {
      type: "text",
      nullable: false,
      foreignKey: {
        table: "products",
        column: "id",
      },
    },
  },
});

export const DOWNSTREAM_ROWS_SCHEMA: SchemaDefinition = downstreamRowsSchemaBuilder.build();
