import { createObjectionProvider, type ObjectionProviderShape } from "@sqlql/objection";
import { SQLITE_DDL, SQLITE_SEED, type DemoContext } from "@sqlql/example-shared";
import {
  createExecutableSchema,
} from "sqlql";
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

function getOrdersQueryBuilder(
  knex: ReturnType<typeof createKnex>,
  context: DemoContext,
) {
  return knex("orders_raw").where({
    org_id: context.orgId,
    user_id: context.userId,
  });
}

function getVendorsQueryBuilder(
  knex: ReturnType<typeof createKnex>,
  context: DemoContext,
) {
  return knex("vendors_raw").where({
    org_id: context.orgId,
  });
}

async function executeSqlBatch(knexInstance: ReturnType<typeof createKnex>, sqlText: string): Promise<void> {
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

  await knex.destroy();
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
