import { Kysely, SqliteDialect } from "kysely";
import { createKyselyProvider } from "@tupl/provider-kysely";
import { createSeededSqliteDatabase, type DemoContext } from "@tupl/example-shared";
import { createExecutableSchema, createSchemaBuilder } from "@tupl/schema";

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

  const schemaBuilder = createSchemaBuilder<DemoContext>();
  const myOrders = schemaBuilder.table("myOrders", ordersEntity, {
    columns: ({ col, expr }) => ({
      id: col.id("id"),
      vendorId: col.string("vendor_id"),
      totalCents: col.integer("total_cents"),
      createdAt: col.string("created_at"),
      totalDollars: col.real(expr.divide(col("totalCents"), expr.literal(100)), {
        nullable: false,
      }),
      isLargeOrder: col.boolean(expr.gte(col("totalCents"), expr.literal(3000)), {
        nullable: false,
      }),
    }),
  });

  const myOrderFacts = schemaBuilder.view(
    "myOrderFacts",
    ({ scan, join, col, expr }) =>
      join({
        left: scan(myOrders),
        right: scan(vendorsEntity),
        on: expr.eq(col(myOrders, "vendorId"), col(vendorsEntity, "id")),
        type: "inner",
      }),
    {
      columns: ({ col }) => ({
        orderId: col.id(myOrders, "id"),
        vendorId: col.string(myOrders, "vendorId", { nullable: false }),
        vendorName: col.string(vendorsEntity, "name", { nullable: false }),
        totalCents: col.integer(myOrders, "totalCents", { nullable: false }),
        totalDollars: col.real(myOrders, "totalDollars", { nullable: false }),
        isLargeOrder: col.boolean(myOrders, "isLargeOrder", { nullable: false }),
      }),
    },
  );
  schemaBuilder.view(
    "myVendorSpend",
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
  );

  const executableSchema = createExecutableSchema(schemaBuilder);

  const virtualRows = await executableSchema.query({
    context: {
      orgId: "org_1",
      userId: "u1",
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

  await db.destroy();
  sqlite.close();
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
