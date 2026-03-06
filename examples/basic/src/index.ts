import {
  bindAdapterEntities,
  createDataEntityHandle,
  createExecutableSchema,
  defineSchema,
  toSqlDDL,
  type ProviderAdapter,
  type QueryRow,
  type SchemaColumnLensDefinition,
  type SchemaDefinition,
  type ScanFilterClause,
  type TableColumnDefinition,
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
      return {
        provider: "memory",
        kind: fragment.kind,
        payload: fragment,
      };
    },
    async execute(plan) {
      const fragment = plan.payload as { kind: "scan"; table: string; request: TableScanRequest };
      const rows = tables[fragment.table] ?? [];
      return scanRows(rows, fragment.request);
    },
    async lookupMany(request) {
      const rows = tables[request.table] ?? [];
      const keys = new Set(request.keys);
      return rows
        .filter((row) => keys.has(row[request.key]))
        .map((row) => projectRow(row, request.select));
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

function toColumnLens(
  columnName: string,
  definition: TableColumnDefinition,
): SchemaColumnLensDefinition {
  if (typeof definition === "string") {
    return {
      source: columnName,
      type: definition,
    };
  }

  const lens: SchemaColumnLensDefinition = {
    source: columnName,
    type: definition.type,
  };
  if (definition.nullable != null) {
    lens.nullable = definition.nullable;
  }
  if (definition.primaryKey != null) {
    lens.primaryKey = definition.primaryKey;
  }
  if (definition.unique != null) {
    lens.unique = definition.unique;
  }
  if (definition.enum) {
    lens.enum = definition.enum;
  }
  return lens;
}

async function main(): Promise<void> {
  const schema = defineSchema({
    tables: {
      orders: {
        provider: "memory",
        columns: {
          id: "text",
          org_id: "text",
          user_id: "text",
          total_cents: "integer",
          created_at: "timestamp",
        },
      },
      users: {
        provider: "memory",
        columns: {
          id: "text",
          team_id: "text",
          email: "text",
        },
      },
      teams: {
        provider: "memory",
        columns: {
          id: "text",
          name: "text",
          tier: "text",
        },
      },
    },
  });

  const tableData = {
    orders: [
      {
        id: "ord_1",
        org_id: "org_1",
        user_id: "usr_1",
        total_cents: 1200,
        created_at: "2026-02-01",
      },
      {
        id: "ord_2",
        org_id: "org_1",
        user_id: "usr_1",
        total_cents: 1800,
        created_at: "2026-02-03",
      },
      {
        id: "ord_3",
        org_id: "org_1",
        user_id: "usr_2",
        total_cents: 2400,
        created_at: "2026-02-04",
      },
      {
        id: "ord_4",
        org_id: "org_2",
        user_id: "usr_3",
        total_cents: 9900,
        created_at: "2026-02-05",
      },
    ],
    users: [
      { id: "usr_1", team_id: "team_enterprise", email: "alice@example.com" },
      { id: "usr_2", team_id: "team_smb", email: "bob@example.com" },
      { id: "usr_3", team_id: "team_enterprise", email: "charlie@example.com" },
    ],
    teams: [
      { id: "team_enterprise", name: "Enterprise", tier: "enterprise" },
      { id: "team_smb", name: "SMB", tier: "smb" },
    ],
  };

  const memoryProvider = createMemoryProvider(schema, tableData);
  const executableSchema = createExecutableSchema<Record<string, never>>(({ table }) => ({
    tables: Object.fromEntries(
      Object.entries(schema.tables).map(([tableName, tableDefinition]) => [
        tableName,
        table(memoryProvider.entities![tableName]!, {
          columns: () => Object.fromEntries(
            Object.entries(tableDefinition.columns).map(([columnName, definition]) => [
              columnName,
              toColumnLens(columnName, definition),
            ]),
          ),
          ...(("constraints" in tableDefinition && tableDefinition.constraints)
            ? { constraints: tableDefinition.constraints }
            : {}),
        }),
      ]),
    ),
  }));

  const ddl = toSqlDDL(schema, { ifNotExists: true });

  const joinRows = await executableSchema.query({
    context: {},
    sql: `
      SELECT o.id, o.total_cents, u.email
      FROM orders o
      JOIN users u ON o.user_id = u.id
      WHERE o.org_id = 'org_1'
      ORDER BY o.created_at DESC
      LIMIT 3
    `,
  });

  console.log("Generated DDL:");
  console.log(ddl);
  console.log("");
  console.log("Join result:");
  console.log(joinRows);
}

main().catch((error: unknown) => {
  console.error(error);
  process.exitCode = 1;
});
