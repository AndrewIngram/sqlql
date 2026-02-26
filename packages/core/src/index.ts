export type SqlScalarType = "text" | "integer" | "boolean" | "timestamp";

export type TableColumns = Record<string, SqlScalarType>;

export interface SchemaQueryDefaults {
  filterable: "all" | string[];
  sortable: "all" | string[];
  maxRows: number | null;
}

export interface TableQueryOverrides {
  filterable?: "all" | string[];
  sortable?: "all" | string[];
  maxRows?: number | null;
}

export interface TableDefinition {
  columns: TableColumns;
  query?: TableQueryOverrides;
}

export interface SchemaDefinition {
  defaults?: {
    query?: Partial<SchemaQueryDefaults>;
  };
  tables: Record<string, TableDefinition>;
}

export const DEFAULT_QUERY_BEHAVIOR: SchemaQueryDefaults = {
  filterable: "all",
  sortable: "all",
  maxRows: null,
};

export function defineSchema<TSchema extends SchemaDefinition>(schema: TSchema): TSchema {
  return schema;
}

export function getTable(schema: SchemaDefinition, tableName: string): TableDefinition {
  const table = schema.tables[tableName];
  if (!table) {
    throw new Error(`Unknown table: ${tableName}`);
  }

  return table;
}

export function resolveTableQueryBehavior(
  schema: SchemaDefinition,
  tableName: string,
): SchemaQueryDefaults {
  const table = getTable(schema, tableName);
  const defaults = schema.defaults?.query;

  return {
    filterable:
      table.query?.filterable ?? defaults?.filterable ?? DEFAULT_QUERY_BEHAVIOR.filterable,
    sortable: table.query?.sortable ?? defaults?.sortable ?? DEFAULT_QUERY_BEHAVIOR.sortable,
    maxRows: table.query?.maxRows ?? defaults?.maxRows ?? DEFAULT_QUERY_BEHAVIOR.maxRows,
  };
}

export type ScanFilterOperator =
  | "eq"
  | "neq"
  | "gt"
  | "gte"
  | "lt"
  | "lte"
  | "in"
  | "not_in"
  | "is_null"
  | "is_not_null"
  | "like"
  | "not_like";

export interface FilterClauseBase {
  column: string;
  op: ScanFilterOperator;
}

export interface ScalarFilterClause extends FilterClauseBase {
  op: "eq" | "neq" | "gt" | "gte" | "lt" | "lte" | "like" | "not_like";
  value: unknown;
}

export interface SetFilterClause extends FilterClauseBase {
  op: "in" | "not_in";
  values: unknown[];
}

export interface NullFilterClause extends FilterClauseBase {
  op: "is_null" | "is_not_null";
}

export type ScanFilterClause = ScalarFilterClause | SetFilterClause | NullFilterClause;

export interface ScanOrderBy {
  column: string;
  direction: "asc" | "desc";
}

export interface TableScanRequest {
  table: string;
  alias?: string;
  select: string[];
  where?: ScanFilterClause[];
  orderBy?: ScanOrderBy[];
  limit?: number;
  offset?: number;
}

export interface TableLookupRequest {
  table: string;
  alias?: string;
  key: string;
  values: unknown[];
  select: string[];
  where?: ScanFilterClause[];
}

export type AggregateFunction = "count" | "sum" | "avg" | "min" | "max";

export interface TableAggregateMetric {
  fn: AggregateFunction;
  column?: string;
  as: string;
  distinct?: boolean;
}

export interface TableAggregateRequest {
  table: string;
  alias?: string;
  where?: ScanFilterClause[];
  groupBy?: string[];
  metrics: TableAggregateMetric[];
  limit?: number;
}

export type QueryRow = Record<string, unknown>;

export interface TableMethods<TContext = unknown> {
  scan: (request: TableScanRequest, context: TContext) => Promise<QueryRow[]>;
  lookup?: (request: TableLookupRequest, context: TContext) => Promise<QueryRow[]>;
  aggregate?: (request: TableAggregateRequest, context: TContext) => Promise<QueryRow[]>;
}

export type TableMethodsMap<TContext = unknown> = Record<string, TableMethods<TContext>>;

export function defineTableMethods<TContext, TMethods extends TableMethodsMap<TContext>>(
  methods: TMethods,
): TMethods {
  return methods;
}

export interface SqlDdlOptions {
  ifNotExists?: boolean;
}

export function toSqlDDL(schema: SchemaDefinition, options: SqlDdlOptions = {}): string {
  const createPrefix = options.ifNotExists ? "CREATE TABLE IF NOT EXISTS" : "CREATE TABLE";
  const statements: string[] = [];

  for (const [tableName, table] of Object.entries(schema.tables)) {
    const columnEntries = Object.entries(table.columns);
    if (columnEntries.length === 0) {
      throw new Error(`Cannot generate DDL for table ${tableName} with no columns.`);
    }

    const columnsSql = columnEntries
      .map(
        ([columnName, columnType]) => `  ${escapeIdentifier(columnName)} ${toSqlType(columnType)}`,
      )
      .join(",\n");

    statements.push(`${createPrefix} ${escapeIdentifier(tableName)} (\n${columnsSql}\n);`);
  }

  return statements.join("\n\n");
}

function toSqlType(type: SqlScalarType): string {
  switch (type) {
    case "text":
      return "TEXT";
    case "integer":
      return "INTEGER";
    case "boolean":
      return "BOOLEAN";
    case "timestamp":
      return "TIMESTAMP";
  }
}

function escapeIdentifier(name: string): string {
  return `"${name.replaceAll('"', '""')}"`;
}

export * as planning from "./planning";
