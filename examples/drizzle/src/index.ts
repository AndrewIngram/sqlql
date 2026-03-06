import { drizzle } from "drizzle-orm/better-sqlite3";
import { and, eq } from "drizzle-orm";
import { integer, sqliteTable, text } from "drizzle-orm/sqlite-core";
import { createDrizzleProvider } from "@sqlql/drizzle";
import { createSeededSqliteDatabase, type DemoContext } from "@sqlql/example-shared";
import {
  createExecutableSchema,
} from "sqlql";

const vendorsRawTable = sqliteTable("vendors_raw", {
  id: text("id").primaryKey().notNull(),
  orgId: text("org_id").notNull(),
  name: text("name").notNull(),
});

const ordersRawTable = sqliteTable("orders_raw", {
  id: text("id").primaryKey().notNull(),
  orgId: text("org_id").notNull(),
  userId: text("user_id").notNull(),
  vendorId: text("vendor_id").notNull(),
  totalCents: integer("total_cents").notNull(),
  createdAt: text("created_at").notNull(),
});

async function main(): Promise<void> {
  const sqlite = createSeededSqliteDatabase();

  const db = drizzle(sqlite);

  const tableConfigs = {
    orders_raw: {
      table: ordersRawTable,
      scope: (context: DemoContext) =>
        and(
          eq(ordersRawTable.orgId, context.orgId),
          eq(ordersRawTable.userId, context.userId),
        ),
    },
    vendors_raw: {
      table: vendorsRawTable,
      scope: (context: DemoContext) => eq(vendorsRawTable.orgId, context.orgId),
    },
  };

  const dbProvider = createDrizzleProvider<DemoContext, typeof tableConfigs>({
    name: "dbProvider",
    db,
    tables: tableConfigs,
  });

  const executableSchema = createExecutableSchema<DemoContext>(({ table, view }) => {
    const myOrders = table(dbProvider.entities.orders_raw, {
      columns: ({ col }) => ({
        id: col.id("id"),
        vendorId: col.string("vendorId"),
        totalCents: col.integer("totalCents"),
        createdAt: col.string("createdAt"),
      }),
    });

    const vendorsForOrg = table(dbProvider.entities.vendors_raw, {
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
    context: {
      orgId: "org_1",
      userId: "u1",
    },
    sql: `
      SELECT vendorName, totalSpendCents, orderCount
      FROM myVendorSpend
      ORDER BY totalSpendCents DESC
    `,
  });

  const orderRows = await executableSchema.query({
    context: {
      orgId: "org_1",
      userId: "u1",
    },
    sql: `
      SELECT o.id, v.name, o.totalCents
      FROM myOrders o
      JOIN vendorsForOrg v ON o.vendorId = v.id
      WHERE o.totalCents >= 2000
      ORDER BY o.totalCents DESC
    `,
  });

  console.log("myVendorSpend:");
  console.log(rows);
  console.log("joined rows:");
  console.log(orderRows);
  sqlite.close();
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
