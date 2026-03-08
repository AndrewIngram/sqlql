import { createObjectionProvider, type ObjectionProviderShape } from "@tupl/provider-objection";
import { SQLITE_DDL, SQLITE_SEED, type DemoContext } from "@tupl/example-shared";
import { createExecutableSchema, createSchemaBuilder } from "@tupl/schema";
import knexModule from "knex";

const { knex: createKnex } = knexModule;

type OrdersRawRow = {
  id: string;
  org_id: string;
  user_id: string;
  vendor_id: string;
  total_cents: number;
  created_at: string;
};

type VendorsRawRow = {
  id: string;
  org_id: string;
  name: string;
};

type DemoObjectionEntities = ObjectionProviderShape<
  {
    orders_raw: OrdersRawRow;
    vendors_raw: VendorsRawRow;
  },
  DemoContext
>;

function getOrdersQueryBuilder(knex: ReturnType<typeof createKnex>, context: DemoContext) {
  return knex("orders_raw").where({
    org_id: context.orgId,
    user_id: context.userId,
  });
}

function getVendorsQueryBuilder(knex: ReturnType<typeof createKnex>, context: DemoContext) {
  return knex("vendors_raw").where({
    org_id: context.orgId,
  });
}

async function executeSqlBatch(
  knexInstance: ReturnType<typeof createKnex>,
  sqlText: string,
): Promise<void> {
  const statements = sqlText
    .split(";")
    .map((statement) => statement.trim())
    .filter((statement) => statement.length > 0);

  for (const statement of statements) {
    await knexInstance.raw(statement);
  }
}

async function main(): Promise<void> {
  const knex = createKnex({
    client: "better-sqlite3",
    connection: {
      filename: ":memory:",
    },
    useNullAsDefault: true,
  });

  await executeSqlBatch(knex, SQLITE_DDL);
  await executeSqlBatch(knex, SQLITE_SEED);

  const dbProvider = createObjectionProvider<DemoContext, DemoObjectionEntities>({
    name: "dbProvider",
    knex,
    entities: {
      orders_raw: {
        table: "orders_raw",
        // `shape` supplies runtime metadata; the generic row type above supplies
        // the compile-time read shape for provider-owned entity inference.
        shape: {
          id: { type: "text", primaryKey: true },
          org_id: "text",
          user_id: "text",
          vendor_id: "text",
          total_cents: "integer",
          created_at: "text",
        },
        base: (context) => getOrdersQueryBuilder(knex, context),
      },
      vendors_raw: {
        table: "vendors_raw",
        shape: {
          id: { type: "text", primaryKey: true },
          org_id: "text",
          name: "text",
        },
        base: (context) => getVendorsQueryBuilder(knex, context),
      },
    },
  });

  const ordersEntity = dbProvider.entities.orders_raw;
  const vendorsEntity = dbProvider.entities.vendors_raw;
  if (!ordersEntity || !vendorsEntity) {
    throw new Error("Objection provider did not expose expected entity handles.");
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

  await knex.destroy();
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
