import Database from "better-sqlite3";

import {
  createArrayTableMethods,
  defineTableMethods,
  query,
  toSqlDDL,
  type QueryRow,
  type SchemaDefinition,
  type TableMethodsForSchema,
  type TableName,
} from "../../src";

export type RowsByTable<TSchema extends SchemaDefinition> = {
  [TTable in TableName<TSchema>]: QueryRow<TSchema, TTable>[];
};

export interface QueryHarness<TSchema extends SchemaDefinition, TContext> {
  schema: TSchema;
  methods: TableMethodsForSchema<TSchema, TContext>;
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
  methods?: TableMethodsForSchema<TSchema, TContext>;
}): QueryHarness<TSchema, TContext> {
  const { schema, rowsByTable } = options;
  const controlDb = createControlDatabase(schema, rowsByTable);

  const methods =
    options.methods ??
    defineTableMethods(
      schema,
      Object.fromEntries(
        Object.entries(rowsByTable).map(([table, rows]) => [table, createArrayTableMethods(rows)]),
      ) as TableMethodsForSchema<TSchema, TContext>,
    );

  return {
    schema,
    methods,
    runSqlql: (sql, context) =>
      query({
        schema,
        methods,
        context,
        sql,
      }),
    runSqlite: (sql) => controlDb.prepare(sql).all() as QueryRow[],
    runAgainstBoth: async (sql, context) => {
      const actual = await query({
        schema,
        methods,
        context,
        sql,
      });

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
    methods?: TableMethodsForSchema<TSchema, TContext>;
  },
  fn: (harness: QueryHarness<TSchema, TContext>) => Promise<TResult>,
): Promise<TResult> {
  const harness = createQueryHarness(options);

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

  return value ?? null;
}
