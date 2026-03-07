import { Result } from "better-result";
import {
  bindAdapterEntities,
  createDataEntityHandle,
  createExecutableSchema,
  defineSchema,
  toSqlDDL,
  type ProviderAdapter,
  type QueryRow,
  type SchemaDefinition,
  type ScanFilterClause,
  type TableScanRequest,
} from "sqlql";

function createMemoryProvider<TContext>(
  schema: SchemaDefinition,
  tables: Record<string, QueryRow[]>,
): ProviderAdapter<TContext> {
  const adapter: ProviderAdapter<TContext> = {
    name: "memory",
    entities: {},
    canExecute(fragment) {
      return fragment.kind === "scan";
    },
    async compile(fragment) {
      return Result.ok({
        provider: "memory",
        kind: fragment.kind,
        payload: fragment,
      });
    },
    async execute(plan) {
      const fragment = plan.payload as { kind: "scan"; table: string; request: TableScanRequest };
      const rows = tables[fragment.table] ?? [];
      return Result.ok(scanRows(rows, fragment.request));
    },
    async lookupMany(request) {
      const rows = tables[request.table] ?? [];
      const keys = new Set(request.keys);
      return Result.ok(
        rows
          .filter((row) => keys.has(row[request.key]))
          .map((row) => projectRow(row, request.select)),
      );
    },
  };

  for (const [tableName, table] of Object.entries(schema.tables)) {
    adapter.entities![tableName] = createDataEntityHandle({
      entity: tableName,
      provider: adapter.name,
      adapter,
      columns: Object.fromEntries(
        Object.entries(table.columns).map(([columnName, definition]) => [
          columnName,
          typeof definition === "string"
            ? { source: columnName, type: definition }
            : {
                source: columnName,
                type: definition.type,
                ...(definition.nullable != null ? { nullable: definition.nullable } : {}),
              },
        ]),
      ),
    });
  }

  return bindAdapterEntities(adapter);
}

function scanRows(rows: QueryRow[], request: TableScanRequest): QueryRow[] {
  let out = rows.filter((row) => matchesFilters(row, request.where ?? []));

  if (request.orderBy && request.orderBy.length > 0) {
    out = [...out].sort((left, right) => {
      for (const term of request.orderBy ?? []) {
        const leftValue = left[term.column] ?? null;
        const rightValue = right[term.column] ?? null;
        if (leftValue === rightValue) {
          continue;
        }

        if (leftValue == null) {
          return term.direction === "asc" ? -1 : 1;
        }
        if (rightValue == null) {
          return term.direction === "asc" ? 1 : -1;
        }

        const comparison = String(leftValue).localeCompare(String(rightValue));
        if (comparison !== 0) {
          return term.direction === "asc" ? comparison : -comparison;
        }
      }

      return 0;
    });
  }

  if (request.offset != null) {
    out = out.slice(request.offset);
  }

  if (request.limit != null) {
    out = out.slice(0, request.limit);
  }

  return out.map((row) => projectRow(row, request.select));
}

function matchesFilters(row: QueryRow, filters: ScanFilterClause[]): boolean {
  for (const clause of filters) {
    const value = row[clause.column];

    switch (clause.op) {
      case "eq":
        if (value !== clause.value) {
          return false;
        }
        break;
      case "neq":
        if (value === clause.value) {
          return false;
        }
        break;
      case "gt":
        if (value == null || clause.value == null || Number(value) <= Number(clause.value)) {
          return false;
        }
        break;
      case "gte":
        if (value == null || clause.value == null || Number(value) < Number(clause.value)) {
          return false;
        }
        break;
      case "lt":
        if (value == null || clause.value == null || Number(value) >= Number(clause.value)) {
          return false;
        }
        break;
      case "lte":
        if (value == null || clause.value == null || Number(value) > Number(clause.value)) {
          return false;
        }
        break;
      case "in":
        if (!clause.values.includes(value)) {
          return false;
        }
        break;
      case "is_null":
        if (value != null) {
          return false;
        }
        break;
      case "is_not_null":
        if (value == null) {
          return false;
        }
        break;
    }
  }

  return true;
}

function projectRow(row: QueryRow, select: string[]): QueryRow {
  const out: QueryRow = {};
  for (const column of select) {
    out[column] = row[column] ?? null;
  }
  return out;
}

async function main(): Promise<void> {
  const rawSchema = defineSchema({
    tables: {
      orders_raw: {
        provider: "memory",
        columns: {
          id: "text",
          org_id: "text",
          user_id: "text",
          vendor_id: "text",
          total_cents: "integer",
          created_at: "timestamp",
        },
      },
      vendors_raw: {
        provider: "memory",
        columns: {
          id: "text",
          name: "text",
          org_id: "text",
        },
      },
    },
  });

  const tableData = {
    orders_raw: [
      {
        id: "o1",
        org_id: "org_1",
        user_id: "u1",
        vendor_id: "v1",
        total_cents: 1500,
        created_at: "2026-02-01T00:00:00.000Z",
      },
      {
        id: "o2",
        org_id: "org_1",
        user_id: "u1",
        vendor_id: "v2",
        total_cents: 3200,
        created_at: "2026-02-03T00:00:00.000Z",
      },
      {
        id: "o3",
        org_id: "org_1",
        user_id: "u2",
        vendor_id: "v1",
        total_cents: 7000,
        created_at: "2026-02-04T00:00:00.000Z",
      },
      {
        id: "o4",
        org_id: "org_2",
        user_id: "u9",
        vendor_id: "v3",
        total_cents: 1200,
        created_at: "2026-02-05T00:00:00.000Z",
      },
    ],
    vendors_raw: [
      { id: "v1", org_id: "org_1", name: "Northwind" },
      { id: "v2", org_id: "org_1", name: "Acme Parts" },
      { id: "v3", org_id: "org_2", name: "Other Org Vendor" },
    ],
  };

  const memoryProvider = createMemoryProvider(rawSchema, tableData);
  const executableSchema = createExecutableSchema<Record<string, never>>(({ table, view }) => {
    const myOrders = table(memoryProvider.entities!.orders_raw!, {
      columns: ({ col, expr }) => ({
        id: col.id("id"),
        vendorId: col.string("vendor_id"),
        totalCents: col.integer("total_cents"),
        createdAt: col.timestamp("created_at"),
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
          right: scan(memoryProvider.entities!.vendors_raw!),
          on: expr.eq(col(myOrders, "vendorId"), col(memoryProvider.entities!.vendors_raw!, "id")),
          type: "inner",
        }),
      {
        columns: ({ col }) => ({
          orderId: col.id(myOrders, "id"),
          vendorId: col.string(myOrders, "vendorId", { nullable: false }),
          vendorName: col.string(memoryProvider.entities!.vendors_raw!, "name", { nullable: false }),
          totalCents: col.integer(myOrders, "totalCents", { nullable: false }),
          totalDollars: col.real(myOrders, "totalDollars", { nullable: false }),
          isLargeOrder: col.boolean(myOrders, "isLargeOrder", { nullable: false }),
        }),
      },
    );

    const myVendorSpend = view(
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

    return {
      tables: {
        myOrders,
        myOrderFacts,
        myVendorSpend,
      },
    };
  });

  const ddl = toSqlDDL(rawSchema, { ifNotExists: true });

  const virtualRows = await executableSchema.query({
    context: {},
    sql: `
      SELECT id, totalDollars, isLargeOrder
      FROM myOrders
      WHERE totalDollars >= 20
      ORDER BY totalDollars DESC
    `,
  });

  const orderFactRows = await executableSchema.query({
    context: {},
    sql: `
      SELECT orderId, vendorName, totalDollars, isLargeOrder
      FROM myOrderFacts
      ORDER BY totalDollars DESC
    `,
  });

  const spendRows = await executableSchema.query({
    context: {},
    sql: `
      SELECT vendorName, totalSpendCents, orderCount
      FROM myVendorSpend
      ORDER BY totalSpendCents DESC
    `,
  });

  console.log("Generated DDL:");
  console.log(ddl);
  console.log("");
  console.log("myOrders with virtual columns:");
  console.log(virtualRows);
  console.log("myOrderFacts view:");
  console.log(orderFactRows);
  console.log("myVendorSpend aggregate view:");
  console.log(spendRows);
}

main().catch((error: unknown) => {
  console.error(error);
  process.exitCode = 1;
});
