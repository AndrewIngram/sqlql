export type SqlScalarType = "text" | "integer" | "boolean" | "timestamp";

export interface ColumnDefinition {
  type: SqlScalarType;
  nullable?: boolean;
}

export type TableColumnDefinition = SqlScalarType | ColumnDefinition;

export type TableColumns = Record<string, TableColumnDefinition>;

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

export type TableName<TSchema extends SchemaDefinition> = Extract<keyof TSchema["tables"], string>;

export type TableColumnName<
  TSchema extends SchemaDefinition,
  TTableName extends TableName<TSchema>,
> = Extract<keyof TSchema["tables"][TTableName]["columns"], string>;

export type SqlTypeValue<TType extends SqlScalarType> = TType extends "integer"
  ? number
  : TType extends "boolean"
    ? boolean
    : string;

export type ColumnValue<TColumn extends TableColumnDefinition> = TColumn extends SqlScalarType
  ? SqlTypeValue<TColumn> | null
  : TColumn extends ColumnDefinition
    ? TColumn["nullable"] extends false
      ? SqlTypeValue<TColumn["type"]>
      : SqlTypeValue<TColumn["type"]> | null
    : never;

export type TableRow<TSchema extends SchemaDefinition, TTableName extends TableName<TSchema>> = {
  [TColumnName in TableColumnName<TSchema, TTableName>]: ColumnValue<
    TSchema["tables"][TTableName]["columns"][TColumnName]
  >;
};

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
  | "is_null"
  | "is_not_null";

export interface FilterClauseBase<TColumn extends string = string> {
  column: TColumn;
  op: ScanFilterOperator;
}

export interface ScalarFilterClause<
  TColumn extends string = string,
> extends FilterClauseBase<TColumn> {
  op: "eq" | "neq" | "gt" | "gte" | "lt" | "lte";
  value: unknown;
}

export interface SetFilterClause<
  TColumn extends string = string,
> extends FilterClauseBase<TColumn> {
  op: "in";
  values: unknown[];
}

export interface NullFilterClause<
  TColumn extends string = string,
> extends FilterClauseBase<TColumn> {
  op: "is_null" | "is_not_null";
}

export type ScanFilterClause<TColumn extends string = string> =
  | ScalarFilterClause<TColumn>
  | SetFilterClause<TColumn>
  | NullFilterClause<TColumn>;

export interface ScanOrderBy<TColumn extends string = string> {
  column: TColumn;
  direction: "asc" | "desc";
}

export interface TableScanRequest<TTable extends string = string, TColumn extends string = string> {
  table: TTable;
  alias?: string;
  select: TColumn[];
  where?: ScanFilterClause<TColumn>[];
  orderBy?: ScanOrderBy<TColumn>[];
  limit?: number;
  offset?: number;
}

export interface TableLookupRequest<
  TTable extends string = string,
  TColumn extends string = string,
> {
  table: TTable;
  alias?: string;
  key: TColumn;
  values: unknown[];
  select: TColumn[];
  where?: ScanFilterClause<TColumn>[];
}

export type AggregateFunction = "count" | "sum" | "avg" | "min" | "max";

export interface TableAggregateMetric<TColumn extends string = string> {
  fn: AggregateFunction;
  column?: TColumn;
  as: string;
  distinct?: boolean;
}

export interface TableAggregateRequest<
  TTable extends string = string,
  TColumn extends string = string,
> {
  table: TTable;
  alias?: string;
  where?: ScanFilterClause<TColumn>[];
  groupBy?: TColumn[];
  metrics: TableAggregateMetric<TColumn>[];
  limit?: number;
}

export type QueryRow<
  TSchema extends SchemaDefinition | never = never,
  TTableName extends string = string,
> = [TSchema] extends [never]
  ? Record<string, unknown>
  : TSchema extends SchemaDefinition
    ? TTableName extends TableName<TSchema>
      ? TableRow<TSchema, TTableName>
      : never
    : Record<string, unknown>;

export interface TableMethods<
  TContext = unknown,
  TTable extends string = string,
  TColumn extends string = string,
> {
  scan(request: TableScanRequest<TTable, TColumn>, context: TContext): Promise<QueryRow[]>;
  lookup?(request: TableLookupRequest<TTable, TColumn>, context: TContext): Promise<QueryRow[]>;
  aggregate?(
    request: TableAggregateRequest<TTable, TColumn>,
    context: TContext,
  ): Promise<QueryRow[]>;
}

export type TableMethodsMap<TContext = unknown> = Record<
  string,
  TableMethods<TContext, string, string>
>;

export type TableMethodsForSchema<TSchema extends SchemaDefinition, TContext = unknown> = {
  [TTableName in TableName<TSchema>]: TableMethods<
    TContext,
    TTableName,
    TableColumnName<TSchema, TTableName>
  >;
};

export function defineTableMethods<TContext, TMethods extends TableMethodsMap<TContext>>(
  methods: TMethods,
): TMethods;

export function defineTableMethods<
  TSchema extends SchemaDefinition,
  TContext,
  TMethods extends TableMethodsForSchema<TSchema, TContext>,
>(schema: TSchema, methods: TMethods): TMethods;

export function defineTableMethods(...args: unknown[]): unknown {
  if (args.length === 1) {
    return args[0];
  }

  if (args.length === 2) {
    return args[1];
  }

  throw new Error("defineTableMethods expects either (methods) or (schema, methods).");
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
      .map(([columnName, columnDefinition]) => {
        const resolved = resolveColumnDefinition(columnDefinition);
        const nullability = resolved.nullable ? "" : " NOT NULL";
        return `  ${escapeIdentifier(columnName)} ${toSqlType(resolved.type)}${nullability}`;
      })
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

export interface ResolvedColumnDefinition {
  type: SqlScalarType;
  nullable: boolean;
}

export function resolveColumnDefinition(
  definition: TableColumnDefinition,
): ResolvedColumnDefinition {
  if (typeof definition === "string") {
    return {
      type: definition,
      nullable: true,
    };
  }

  return {
    type: definition.type,
    nullable: definition.nullable ?? true,
  };
}

export function resolveColumnType(definition: TableColumnDefinition): SqlScalarType {
  return resolveColumnDefinition(definition).type;
}

function escapeIdentifier(name: string): string {
  return `"${name.replaceAll('"', '""')}"`;
}
