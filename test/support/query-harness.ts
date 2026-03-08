import Database from "better-sqlite3";

import { Result } from "better-result";
import {
  toSqlDDL,
  type ProviderAdapter,
  type ProviderFragment,
  type ProvidersMap,
  type QueryRow,
  type ScanFilterClause,
  type SchemaDefinition,
  type TableName,
  type TableScanRequest,
} from "../../src";
import { createExecutableSchemaFromProviders } from "./executable-schema";

export type RowsByTable<TSchema extends SchemaDefinition> = {
  [TTable in TableName<TSchema>]: QueryRow<TSchema, TTable>[];
};

export interface QueryHarness<TSchema extends SchemaDefinition, TContext> {
  schema: TSchema;
  executableSchema: ReturnType<typeof createExecutableSchemaFromProviders<TContext, TSchema>>;
  runSqlql: (sql: string, context: TContext) => Promise<QueryRow[]>;
  runSqlite: (sql: string) => QueryRow[];
  runAgainstBoth: (
    sql: string,
    context: TContext,
  ) => Promise<{ actual: QueryRow[]; expected: QueryRow[] }>;
  close: () => void;
}

export function createQueryHarness<
  TSchema extends SchemaDefinition,
  TContext = Record<string, never>,
>(options: {
  schema: TSchema;
  rowsByTable: RowsByTable<TSchema>;
  providers?: ProvidersMap<TContext>;
}): QueryHarness<TSchema, TContext> {
  const schema = ensureSchemaProviders(options.schema, "memory");
  const rowsByTable = options.rowsByTable as Record<string, QueryRow[]>;
  const controlDb = createControlDatabase(schema, options.rowsByTable);

  const providers = options.providers ?? {
    memory: createMemoryProvider<TContext>(rowsByTable),
  };
  const executableSchema = createExecutableSchemaFromProviders(schema, providers);

  return {
    schema,
    executableSchema,
    runSqlql: (sql, context) => executableSchema.query({ context, sql }),
    runSqlite: (sql) => controlDb.prepare(sql).all() as QueryRow[],
    runAgainstBoth: async (sql, context) => {
      const actual = await executableSchema.query({ context, sql });

      const expected = controlDb.prepare(sql).all() as QueryRow[];
      return { actual, expected };
    },
    close: () => {
      controlDb.close();
    },
  };
}

export async function withQueryHarness<
  TSchema extends SchemaDefinition,
  TContext = Record<string, never>,
  TResult = void,
>(
  options: {
    schema: TSchema;
    rowsByTable: RowsByTable<TSchema>;
    providers?: ProvidersMap<TContext>;
  },
  fn: (harness: QueryHarness<TSchema, TContext>) => Promise<TResult>,
): Promise<TResult> {
  const harness = createQueryHarness<TSchema, TContext>(options);

  try {
    return await fn(harness);
  } finally {
    harness.close();
  }
}

function createControlDatabase<TSchema extends SchemaDefinition>(
  schema: TSchema,
  rowsByTable: RowsByTable<TSchema>,
): InstanceType<typeof Database> {
  const db = new Database(":memory:");
  db.exec(toSqlDDL(schema, { ifNotExists: true }));

  for (const [tableName, table] of Object.entries(schema.tables)) {
    const columns = Object.keys(table.columns);
    const quotedColumns = columns.map(quoteIdentifier).join(", ");
    const placeholders = columns.map(() => "?").join(", ");

    const insert = db.prepare(
      `INSERT INTO ${quoteIdentifier(tableName)} (${quotedColumns}) VALUES (${placeholders})`,
    );

    const rows = rowsByTable[tableName as keyof RowsByTable<TSchema>] as QueryRow[];
    const tx = db.transaction((batch: QueryRow[]) => {
      for (const row of batch) {
        insert.run(...columns.map((column) => normalizeSqliteValue(row[column])));
      }
    });

    tx(rows);
  }

  return db;
}

function quoteIdentifier(name: string): string {
  return `"${name.replaceAll('"', '""')}"`;
}

function normalizeSqliteValue(value: unknown): unknown {
  if (typeof value === "boolean") {
    return value ? 1 : 0;
  }

  if (value instanceof Date) {
    return value.toISOString();
  }

  return value ?? null;
}

function ensureSchemaProviders<TSchema extends SchemaDefinition>(
  schema: TSchema,
  provider: string,
): TSchema {
  const tables = Object.fromEntries(
    Object.entries(schema.tables).map(([tableName, table]) => {
      if (table.provider && table.provider.length > 0) {
        return [tableName, table];
      }
      return [
        tableName,
        {
          ...table,
          provider,
        },
      ];
    }),
  ) as TSchema["tables"];

  return {
    ...schema,
    tables,
  };
}

function createMemoryProvider<TContext>(
  rowsByTable: Record<string, QueryRow[]>,
): ProviderAdapter<TContext> {
  return {
    name: "memory",
    canExecute(fragment) {
      return fragment.kind === "scan";
    },
    async compile(fragment) {
      if (fragment.kind !== "scan") {
        return Result.err(new Error(`Unsupported memory provider fragment: ${fragment.kind}`));
      }
      return Result.ok({
        provider: "memory",
        kind: "scan",
        payload: fragment,
      });
    },
    async execute(plan) {
      if (plan.kind !== "scan") {
        return Result.err(new Error(`Unsupported memory provider compiled plan: ${plan.kind}`));
      }

      const fragment = plan.payload as Extract<ProviderFragment, { kind: "scan" }>;
      return Result.ok(scanRows(rowsByTable[fragment.table] ?? [], fragment.request));
    },
    async lookupMany(request) {
      const scanRequest: TableScanRequest = {
        table: request.table,
        select: request.select,
        where: [
          ...(request.where ?? []),
          {
            op: "in",
            column: request.key,
            values: request.keys,
          } as ScanFilterClause,
        ],
      };

      return Result.ok(scanRows(rowsByTable[request.table] ?? [], scanRequest));
    },
  };
}

function scanRows(rows: QueryRow[], request: TableScanRequest): QueryRow[] {
  const normalizedRows = normalizeDateRows(rows);
  let out = normalizedRows.filter((row) => matchesFilters(row, request.where ?? []));

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

  return out.map((row) => {
    const projected: QueryRow = {};
    for (const column of request.select) {
      projected[column] = row[column] ?? null;
    }
    return projected;
  });
}

function normalizeDateRows(rows: QueryRow[]): QueryRow[] {
  return rows.map((row) => {
    const next: QueryRow = {};
    for (const [key, value] of Object.entries(row)) {
      next[key] = value instanceof Date ? value.toISOString() : value;
    }
    return next;
  });
}

function matchesFilters(row: QueryRow, filters: ScanFilterClause[]): boolean {
  for (const clause of filters) {
    const value = row[clause.column];

    switch (clause.op) {
      case "eq":
        if (value == null || clause.value == null || value !== clause.value) {
          return false;
        }
        break;
      case "neq":
        if (value == null || clause.value == null || value === clause.value) {
          return false;
        }
        break;
      case "gt":
        if (value == null || clause.value == null || compareNonNull(value, clause.value) <= 0) {
          return false;
        }
        break;
      case "gte":
        if (value == null || clause.value == null || compareNonNull(value, clause.value) < 0) {
          return false;
        }
        break;
      case "lt":
        if (value == null || clause.value == null || compareNonNull(value, clause.value) >= 0) {
          return false;
        }
        break;
      case "lte":
        if (value == null || clause.value == null || compareNonNull(value, clause.value) > 0) {
          return false;
        }
        break;
      case "in":
        if (value == null || !clause.values.filter((entry) => entry != null).includes(value)) {
          return false;
        }
        break;
      case "not_in":
        if (value == null || clause.values.filter((entry) => entry != null).includes(value)) {
          return false;
        }
        break;
      case "like":
        if (
          typeof value !== "string" ||
          typeof clause.value !== "string" ||
          !matchesLike(value, clause.value)
        ) {
          return false;
        }
        break;
      case "not_like":
        if (
          typeof value !== "string" ||
          typeof clause.value !== "string" ||
          matchesLike(value, clause.value)
        ) {
          return false;
        }
        break;
      case "is_distinct_from":
        if (value === clause.value) {
          return false;
        }
        break;
      case "is_not_distinct_from":
        if (value !== clause.value) {
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

function matchesLike(value: string, pattern: string): boolean {
  const escaped = pattern
    .replace(/[.*+?^${}()|[\]\\]/g, "\\$&")
    .replace(/%/g, ".*")
    .replace(/_/g, ".");
  return new RegExp(`^${escaped}$`, "su").test(value);
}

function compareNonNull(left: unknown, right: unknown): number {
  if (typeof left === "number" && typeof right === "number") {
    return left === right ? 0 : left < right ? -1 : 1;
  }

  if (typeof left === "boolean" && typeof right === "boolean") {
    const leftNumber = Number(left);
    const rightNumber = Number(right);
    return leftNumber === rightNumber ? 0 : leftNumber < rightNumber ? -1 : 1;
  }

  const leftString = String(left);
  const rightString = String(right);
  if (leftString === rightString) {
    return 0;
  }
  return leftString < rightString ? -1 : 1;
}
