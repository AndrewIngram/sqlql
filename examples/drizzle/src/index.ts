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
  type RuntimeDemoContext = DemoContext & { db: typeof db };

  const tableConfigs = {
    orders_raw: {
      table: ordersRawTable,
      scope: (context: RuntimeDemoContext) =>
        and(
          eq(ordersRawTable.orgId, context.orgId),
          eq(ordersRawTable.userId, context.userId),
        ),
    },
    vendors_raw: {
      table: vendorsRawTable,
      scope: (context: RuntimeDemoContext) => eq(vendorsRawTable.orgId, context.orgId),
    },
  };

  const dbProvider = createDrizzleProvider({
    name: "dbProvider",
    db: (context: RuntimeDemoContext) => context.db,
    tables: tableConfigs,
  });

  const executableSchema = createExecutableSchema<RuntimeDemoContext>(({ table, view }) => {
    const myOrders = table(dbProvider.entities.orders_raw, {
      columns: ({ col, expr }) => ({
        id: col.id("id"),
        vendorId: col.string("vendorId"),
        totalCents: col.integer("totalCents"),
        createdAt: col.string("createdAt"),
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
          right: scan(dbProvider.entities.vendors_raw),
          on: expr.eq(col(myOrders, "vendorId"), col(dbProvider.entities.vendors_raw, "id")),
          type: "inner",
        }),
      {
        columns: ({ col }) => ({
          orderId: col.id(myOrders, "id"),
          vendorId: col.string(myOrders, "vendorId", { nullable: false }),
          vendorName: col.string(dbProvider.entities.vendors_raw, "name", { nullable: false }),
          totalCents: col.integer(myOrders, "totalCents", { nullable: false }),
          totalDollars: col.real(myOrders, "totalDollars", { nullable: false }),
          isLargeOrder: col.boolean(myOrders, "isLargeOrder", { nullable: false }),
        }),
      },
    );

    return {
      tables: {
        myOrders,
        myOrderFacts,
        myVendorSpend: view(
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
        ),
      },
    };
  });

  const virtualRows = await executableSchema.query({
    context: {
      orgId: "org_1",
      userId: "u1",
      db,
    },
    sql: `
      SELECT id, totalDollars, isLargeOrder
      FROM myOrders
      WHERE totalDollars >= 20
      ORDER BY totalDollars DESC
    `,
  });

  const orderFactRows = await executableSchema.query({
    context: {
      orgId: "org_1",
      userId: "u1",
      db,
    },
    sql: `
      SELECT orderId, vendorName, totalDollars, isLargeOrder
      FROM myOrderFacts
      ORDER BY totalDollars DESC
    `,
  });

  const spendRows = await executableSchema.query({
    context: {
      orgId: "org_1",
      userId: "u1",
      db,
    },
    sql: `
      SELECT vendorName, totalSpendCents, orderCount
      FROM myVendorSpend
      ORDER BY totalSpendCents DESC
    `,
  });

  console.log("myOrders with virtual columns:");
  console.log(virtualRows);
  console.log("myOrderFacts view:");
  console.log(orderFactRows);
  console.log("myVendorSpend aggregate view:");
  console.log(spendRows);
  sqlite.close();
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
