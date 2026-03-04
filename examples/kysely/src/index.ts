import { Kysely, SqliteDialect } from "kysely";
import { createKyselyProvider } from "@sqlql/kysely";
import { createSeededSqliteDatabase, type DemoContext } from "@sqlql/example-shared";
import {
  defineProviders,
  defineSchema,
  query,
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

  const dbProvider = createKyselyProvider<DemoContext>({
    name: "dbProvider",
    db,
    entities: {
      orders: {
        table: "orders_raw",
        base: ({ query, context, alias }) =>
          query
            .where(`${alias}.org_id`, "=", context.orgId)
            .where(`${alias}.user_id`, "=", context.userId),
      },
      vendors: {
        table: "vendors_raw",
        base: ({ query, context, alias }) => query.where(`${alias}.org_id`, "=", context.orgId),
      },
    },
  });

  const ordersEntity = dbProvider.entities.orders;
  const vendorsEntity = dbProvider.entities.vendors;
  if (!ordersEntity || !vendorsEntity) {
    throw new Error("Kysely provider did not expose expected entity handles.");
  }

  const schema = defineSchema<DemoContext>(({ table, view, col, expr, agg, rel }) => {
    const myOrders = table({
      from: ordersEntity,
      columns: {
        id: col(ordersEntity, "id"),
        vendorId: col(ordersEntity, "vendor_id"),
        totalCents: col(ordersEntity, "total_cents"),
        createdAt: col(ordersEntity, "created_at"),
      },
    });

    const vendorsForOrg = table({
      from: vendorsEntity,
      columns: {
        id: col(vendorsEntity, "id"),
        name: col(vendorsEntity, "name"),
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

  await db.destroy();
  sqlite.close();
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
