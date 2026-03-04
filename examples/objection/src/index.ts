import { createObjectionProvider } from "@sqlql/objection";
import { SQLITE_DDL, SQLITE_SEED, type DemoContext } from "@sqlql/example-shared";
import { Model } from "objection";
import {
  defineProviders,
  defineSchema,
  query,
} from "sqlql";
import knexModule from "knex";

const { knex: createKnex } = knexModule;

class OrdersRawModel extends Model {
  static get tableName(): string {
    return "orders_raw";
  }

  static getQueryBuilder(context: DemoContext) {
    return this.query().where({
      org_id: context.orgId,
      user_id: context.userId,
    });
  }
}

class VendorsRawModel extends Model {
  static get tableName(): string {
    return "vendors_raw";
  }

  static getQueryBuilder(context: DemoContext) {
    return this.query().where({
      org_id: context.orgId,
    });
  }
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

  Model.knex(knex);

  await executeSqlBatch(knex, SQLITE_DDL);
  await executeSqlBatch(knex, SQLITE_SEED);

  const dbProvider = createObjectionProvider<DemoContext>({
    name: "dbProvider",
    knex,
    entities: {
      orders_raw: {
        table: "orders_raw",
        base: (context) => OrdersRawModel.getQueryBuilder(context).toKnexQuery(),
      },
      vendors_raw: {
        table: "vendors_raw",
        base: (context) => VendorsRawModel.getQueryBuilder(context).toKnexQuery(),
      },
    },
  });

  const ordersEntity = dbProvider.entities.orders_raw;
  const vendorsEntity = dbProvider.entities.vendors_raw;
  if (!ordersEntity || !vendorsEntity) {
    throw new Error("Objection provider did not expose expected entity handles.");
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

  await knex.destroy();
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
