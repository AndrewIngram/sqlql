import { Kysely, SqliteDialect } from "kysely";
import { createKyselyProvider } from "@sqlql/kysely";
import { createSeededSqliteDatabase, type DemoContext } from "@sqlql/example-shared";
import {
  createExecutableSchema,
} from "sqlql";

type Db = {
  orders_raw: {
    id: string;
    org_id: string;
    user_id: string;
    vendor_id: string;
    total_cents: number;
    created_at: string;
  };
  vendors_raw: {
    id: string;
    org_id: string;
    name: string;
  };
};

async function main(): Promise<void> {
  const sqlite = createSeededSqliteDatabase();
  const db = new Kysely<Db>({
    dialect: new SqliteDialect({
      database: sqlite,
    }),
  });

  const dbProvider = createKyselyProvider<DemoContext, Db>({
    name: "dbProvider",
    db,
    entities: {
      orders_raw: {
        table: "orders_raw",
        base: ({ query, context, alias }) =>
          query
            .where(`${alias}.org_id`, "=", context.orgId)
            .where(`${alias}.user_id`, "=", context.userId),
      },
      vendors_raw: {
        table: "vendors_raw",
        base: ({ query, context, alias }) => query.where(`${alias}.org_id`, "=", context.orgId),
      },
    },
  });

  const ordersEntity = dbProvider.entities.orders_raw;
  const vendorsEntity = dbProvider.entities.vendors_raw;
  if (!ordersEntity || !vendorsEntity) {
    throw new Error("Kysely provider did not expose expected entity handles.");
  }

  const executableSchema = createExecutableSchema<DemoContext>(({ table, view }) => {
    const myOrders = table(ordersEntity, {
      columns: ({ col }) => ({
        id: col.id("id"),
        vendorId: col.string("vendor_id"),
        totalCents: col.integer("total_cents"),
        createdAt: col.string("created_at"),
      }),
    });

    const vendorsForOrg = table(vendorsEntity, {
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

  await db.destroy();
  sqlite.close();
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
