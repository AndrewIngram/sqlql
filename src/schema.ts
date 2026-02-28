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

export interface PrimaryKeyConstraint {
  columns: string[];
  name?: string;
}

export interface UniqueConstraint {
  columns: string[];
  name?: string;
}

export type ReferentialAction = "NO ACTION" | "RESTRICT" | "CASCADE" | "SET NULL" | "SET DEFAULT";

export interface ForeignKeyConstraint {
  columns: string[];
  references: {
    table: string;
    columns: string[];
  };
  name?: string;
  onDelete?: ReferentialAction;
  onUpdate?: ReferentialAction;
}

export interface TableConstraints {
  primaryKey?: PrimaryKeyConstraint;
  unique?: UniqueConstraint[];
  foreignKeys?: ForeignKeyConstraint[];
}

export interface TableDefinition {
  columns: TableColumns;
  query?: TableQueryOverrides;
  constraints?: TableConstraints;
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
  validateSchemaConstraints(schema);
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
  validateSchemaConstraints(schema);

  const createPrefix = options.ifNotExists ? "CREATE TABLE IF NOT EXISTS" : "CREATE TABLE";
  const statements: string[] = [];

  for (const [tableName, table] of Object.entries(schema.tables)) {
    const columnEntries = Object.entries(table.columns);
    if (columnEntries.length === 0) {
      throw new Error(`Cannot generate DDL for table ${tableName} with no columns.`);
    }

    const definitionLines = columnEntries.map(([columnName, columnDefinition]) => {
      const resolved = resolveColumnDefinition(columnDefinition);
      const nullability = resolved.nullable ? "" : " NOT NULL";
      return `  ${escapeIdentifier(columnName)} ${toSqlType(resolved.type)}${nullability}`;
    });

    const constraints = table.constraints;
    if (constraints?.primaryKey) {
      definitionLines.push(
        `  ${renderConstraintPrefix(constraints.primaryKey.name)}PRIMARY KEY (${renderColumnList(constraints.primaryKey.columns)})`,
      );
    }

    for (const uniqueConstraint of constraints?.unique ?? []) {
      definitionLines.push(
        `  ${renderConstraintPrefix(uniqueConstraint.name)}UNIQUE (${renderColumnList(uniqueConstraint.columns)})`,
      );
    }

    for (const foreignKey of constraints?.foreignKeys ?? []) {
      const onDelete = foreignKey.onDelete ? ` ON DELETE ${foreignKey.onDelete}` : "";
      const onUpdate = foreignKey.onUpdate ? ` ON UPDATE ${foreignKey.onUpdate}` : "";
      definitionLines.push(
        `  ${renderConstraintPrefix(foreignKey.name)}FOREIGN KEY (${renderColumnList(foreignKey.columns)}) REFERENCES ${escapeIdentifier(foreignKey.references.table)} (${renderColumnList(foreignKey.references.columns)})${onDelete}${onUpdate}`,
      );
    }

    statements.push(
      `${createPrefix} ${escapeIdentifier(tableName)} (\n${definitionLines.join(",\n")}\n);`,
    );
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

function renderColumnList(columns: string[]): string {
  return columns.map(escapeIdentifier).join(", ");
}

function renderConstraintPrefix(name: string | undefined): string {
  return name ? `CONSTRAINT ${escapeIdentifier(name)} ` : "";
}

function validateSchemaConstraints(schema: SchemaDefinition): void {
  for (const [tableName, table] of Object.entries(schema.tables)) {
    const constraints = table.constraints;
    if (!constraints) {
      continue;
    }

    if (constraints.primaryKey) {
      validateConstraintColumns(schema, tableName, "primary key", constraints.primaryKey.columns);
      validateNoDuplicateColumns(tableName, "primary key", constraints.primaryKey.columns);
    }

    constraints.unique?.forEach((uniqueConstraint, index) => {
      const label = uniqueConstraint.name ?? `unique constraint #${index + 1}`;
      validateConstraintColumns(schema, tableName, label, uniqueConstraint.columns);
      validateNoDuplicateColumns(tableName, label, uniqueConstraint.columns);
    });

    constraints.foreignKeys?.forEach((foreignKey, index) => {
      const label = foreignKey.name ?? `foreign key #${index + 1}`;
      validateConstraintColumns(schema, tableName, label, foreignKey.columns);
      validateNoDuplicateColumns(tableName, label, foreignKey.columns);

      const referencedTable = schema.tables[foreignKey.references.table];
      if (!referencedTable) {
        throw new Error(
          `Invalid ${label} on ${tableName}: referenced table "${foreignKey.references.table}" does not exist.`,
        );
      }

      if (foreignKey.columns.length !== foreignKey.references.columns.length) {
        throw new Error(
          `Invalid ${label} on ${tableName}: local columns (${foreignKey.columns.length}) and referenced columns (${foreignKey.references.columns.length}) must have the same length.`,
        );
      }

      if (foreignKey.references.columns.length === 0) {
        throw new Error(`Invalid ${label} on ${tableName}: referenced columns cannot be empty.`);
      }

      for (const referencedColumn of foreignKey.references.columns) {
        if (!(referencedColumn in referencedTable.columns)) {
          throw new Error(
            `Invalid ${label} on ${tableName}: referenced column "${referencedColumn}" does not exist on table "${foreignKey.references.table}".`,
          );
        }
      }

      validateNoDuplicateColumns(
        `${tableName} -> ${foreignKey.references.table}`,
        `${label} referenced columns`,
        foreignKey.references.columns,
      );
    });
  }
}

function validateConstraintColumns(
  schema: SchemaDefinition,
  tableName: string,
  label: string,
  columns: string[],
): void {
  if (columns.length === 0) {
    throw new Error(`Invalid ${label} on ${tableName}: columns cannot be empty.`);
  }

  const table = schema.tables[tableName];
  if (!table) {
    throw new Error(`Unknown table in schema constraints: ${tableName}`);
  }

  for (const column of columns) {
    if (!(column in table.columns)) {
      throw new Error(
        `Invalid ${label} on ${tableName}: column "${column}" does not exist on table "${tableName}".`,
      );
    }
  }
}

function validateNoDuplicateColumns(tableName: string, label: string, columns: string[]): void {
  const seen = new Set<string>();
  for (const column of columns) {
    if (seen.has(column)) {
      throw new Error(
        `Invalid ${label} on ${tableName}: duplicate column "${column}" in constraint definition.`,
      );
    }
    seen.add(column);
  }
}
