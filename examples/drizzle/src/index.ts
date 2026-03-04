import { drizzle } from "drizzle-orm/better-sqlite3";
import { and, eq } from "drizzle-orm";
import { integer, sqliteTable, text } from "drizzle-orm/sqlite-core";
import { createDrizzleProvider, type DrizzleProviderTableConfig } from "@sqlql/drizzle";
import { createSeededSqliteDatabase, type DemoContext } from "@sqlql/example-shared";
import {
  defineProviders,
  defineSchema,
  query,
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
    orders: {
      table: ordersRawTable,
      scope: (context: DemoContext) =>
        and(
          eq(ordersRawTable.orgId, context.orgId),
          eq(ordersRawTable.userId, context.userId),
        ),
    },
    vendors: {
      table: vendorsRawTable,
      scope: (context: DemoContext) => eq(vendorsRawTable.orgId, context.orgId),
    },
  } satisfies Record<"orders" | "vendors", DrizzleProviderTableConfig<DemoContext, string>>;

  const dbProvider = createDrizzleProvider<DemoContext, typeof tableConfigs>({
    name: "dbProvider",
    db,
    tables: tableConfigs,
  });

  const schema = defineSchema<DemoContext>(({ table, view, col, expr, agg, rel }) => {
    const myOrders = table({
      from: dbProvider.entities.orders,
      columns: {
        id: col(dbProvider.entities.orders, "id"),
        vendorId: col(dbProvider.entities.orders, "vendor_id"),
        totalCents: col(dbProvider.entities.orders, "total_cents"),
        createdAt: col(dbProvider.entities.orders, "created_at"),
      },
    });

    const vendorsForOrg = table({
      from: dbProvider.entities.vendors,
      columns: {
        id: col(dbProvider.entities.vendors, "id"),
        name: col(dbProvider.entities.vendors, "name"),
      },
    });

    return {
      tables: {
        myOrders,
        vendorsForOrg,
        myVendorSpend: view({
          rel: () =>
            rel.aggregate({
              from: rel.join({
                left: rel.scan(myOrders),
                right: rel.scan(vendorsForOrg),
                on: expr.eq(col(myOrders, "vendorId"), col(vendorsForOrg, "id")),
                type: "inner",
              }),
              groupBy: [col(vendorsForOrg, "id"), col(vendorsForOrg, "name")],
              measures: {
                totalSpendCents: agg.sum(col(myOrders, "totalCents")),
                orderCount: agg.count(),
              },
            }),
          columns: {
            vendorId: col("id"),
            vendorName: col("name"),
            totalSpendCents: col("totalSpendCents"),
            orderCount: col("orderCount"),
          },
        }),
      },
    };
  });

  const providers = defineProviders({
    dbProvider,
  });

  const rows = await query({
    schema,
    providers,
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

  const orderRows = await query({
    schema,
    providers,
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
