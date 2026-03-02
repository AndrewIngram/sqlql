export type SqlScalarType = "text" | "integer" | "boolean" | "timestamp";

declare const ISO_8601_TIMESTAMP_BRAND: unique symbol;

export type Iso8601TimestampString = string & {
  readonly [ISO_8601_TIMESTAMP_BRAND]: "Iso8601TimestampString";
};

export type TimestampValue = Iso8601TimestampString | string | Date;

export function asIso8601Timestamp(value: string | Date): Iso8601TimestampString {
  return (value instanceof Date ? value.toISOString() : value) as Iso8601TimestampString;
}

type ColumnConstraintFlags =
  | {
      primaryKey?: false | undefined;
      unique?: false | undefined;
    }
  | {
      primaryKey: true;
      unique?: false | undefined;
    }
  | {
      primaryKey?: false | undefined;
      unique: true;
    };

interface ColumnDefinitionBase {
  type: SqlScalarType;
  nullable?: boolean;
  filterable?: boolean;
  sortable?: boolean;
  enum?: readonly string[];
  foreignKey?: ColumnForeignKeyReference;
  description?: string;
}

export type ColumnDefinition = ColumnDefinitionBase & ColumnConstraintFlags;

export type TableColumnDefinition = SqlScalarType | ColumnDefinition;

export type TableColumns = Record<string, TableColumnDefinition>;

export interface TableQueryRejectPolicy {
  requiresLimit?: boolean;
  forbidFullScan?: boolean;
  requireAnyFilterOn?: string[];
}

export type TableQueryFallbackMode = "allow_local" | "require_pushdown";

export interface TableQueryFallbackPolicy {
  filters?: TableQueryFallbackMode;
  sorting?: TableQueryFallbackMode;
  aggregates?: TableQueryFallbackMode;
  limitOffset?: TableQueryFallbackMode;
}

export interface SchemaQueryDefaults {
  maxRows: number | null;
  reject: TableQueryRejectPolicy;
  fallback: Required<TableQueryFallbackPolicy>;
}

export interface SchemaQueryDefaultsInput {
  maxRows?: number | null;
  reject?: TableQueryRejectPolicy;
  fallback?: TableQueryFallbackPolicy;
}

export interface TableQueryOverrides {
  maxRows?: number | null;
  reject?: TableQueryRejectPolicy;
  fallback?: TableQueryFallbackPolicy;

  /** @deprecated Use per-column `filterable` instead. */
  filterable?: "all" | string[];
  /** @deprecated Use per-column `sortable` instead. */
  sortable?: "all" | string[];
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

export interface ColumnForeignKeyReference {
  table: string;
  column: string;
  name?: string;
  onDelete?: ReferentialAction;
  onUpdate?: ReferentialAction;
}

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

export interface CheckConstraintIn {
  kind: "in";
  column: string;
  values: readonly (string | number | boolean | null)[];
  name?: string;
}

export type CheckConstraint = CheckConstraintIn;

export interface TableConstraints {
  primaryKey?: PrimaryKeyConstraint;
  unique?: UniqueConstraint[];
  foreignKeys?: ForeignKeyConstraint[];
  checks?: CheckConstraint[];
}

export interface TableDefinition {
  columns: TableColumns;
  query?: TableQueryOverrides;
  constraints?: TableConstraints;
}

export interface SchemaDefinition {
  defaults?: {
    query?: SchemaQueryDefaultsInput;
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
    : TType extends "timestamp"
      ? TimestampValue
      : string;

type ColumnEnumValue<TColumn extends ColumnDefinition> = TColumn extends {
  type: "text";
  enum: readonly string[];
}
  ? TColumn["enum"][number]
  : never;

type ColumnScalarValue<TColumn extends ColumnDefinition> = [ColumnEnumValue<TColumn>] extends [never]
  ? SqlTypeValue<TColumn["type"]>
  : ColumnEnumValue<TColumn>;

export type ColumnValue<TColumn extends TableColumnDefinition> = TColumn extends SqlScalarType
  ? SqlTypeValue<TColumn> | null
  : TColumn extends ColumnDefinition
    ? TColumn["nullable"] extends false
      ? ColumnScalarValue<TColumn>
      : ColumnScalarValue<TColumn> | null
    : never;

export type TableRow<TSchema extends SchemaDefinition, TTableName extends TableName<TSchema>> = {
  [TColumnName in TableColumnName<TSchema, TTableName>]: ColumnValue<
    TSchema["tables"][TTableName]["columns"][TColumnName]
  >;
};

export interface ResolvedTableQueryBehavior {
  maxRows: number | null;
  reject: {
    requiresLimit: boolean;
    forbidFullScan: boolean;
    requireAnyFilterOn: string[];
  };
  fallback: {
    filters: TableQueryFallbackMode;
    sorting: TableQueryFallbackMode;
    aggregates: TableQueryFallbackMode;
    limitOffset: TableQueryFallbackMode;
  };
}

export const DEFAULT_QUERY_BEHAVIOR: ResolvedTableQueryBehavior = {
  maxRows: null,
  reject: {
    requiresLimit: false,
    forbidFullScan: false,
    requireAnyFilterOn: [],
  },
  fallback: {
    filters: "allow_local",
    sorting: "allow_local",
    aggregates: "allow_local",
    limitOffset: "allow_local",
  },
};

const warnedDeprecatedQueryCapabilities = new Set<string>();

export function defineSchema<TSchema extends SchemaDefinition>(schema: TSchema): TSchema {
  validateSchemaConstraints(schema);
  warnDeprecatedQueryCapabilities(schema);
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
): ResolvedTableQueryBehavior {
  const table = getTable(schema, tableName);
  const defaults = schema.defaults?.query;
  const tableReject = table.query?.reject;
  const defaultReject = defaults?.reject;
  const tableFallback = table.query?.fallback;
  const defaultFallback = defaults?.fallback;

  return {
    maxRows: table.query?.maxRows ?? defaults?.maxRows ?? DEFAULT_QUERY_BEHAVIOR.maxRows,
    reject: {
      requiresLimit:
        tableReject?.requiresLimit ??
        defaultReject?.requiresLimit ??
        DEFAULT_QUERY_BEHAVIOR.reject.requiresLimit,
      forbidFullScan:
        tableReject?.forbidFullScan ??
        defaultReject?.forbidFullScan ??
        DEFAULT_QUERY_BEHAVIOR.reject.forbidFullScan,
      requireAnyFilterOn:
        tableReject?.requireAnyFilterOn ??
        defaultReject?.requireAnyFilterOn ??
        DEFAULT_QUERY_BEHAVIOR.reject.requireAnyFilterOn,
    },
    fallback: {
      filters:
        tableFallback?.filters ??
        defaultFallback?.filters ??
        DEFAULT_QUERY_BEHAVIOR.fallback.filters,
      sorting:
        tableFallback?.sorting ??
        defaultFallback?.sorting ??
        DEFAULT_QUERY_BEHAVIOR.fallback.sorting,
      aggregates:
        tableFallback?.aggregates ??
        defaultFallback?.aggregates ??
        DEFAULT_QUERY_BEHAVIOR.fallback.aggregates,
      limitOffset:
        tableFallback?.limitOffset ??
        defaultFallback?.limitOffset ??
        DEFAULT_QUERY_BEHAVIOR.fallback.limitOffset,
    },
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
  id?: string;
  column: TColumn;
  op: ScanFilterOperator;
}

export interface ScalarFilterClause<
  TColumn extends string = string,
  TValue = unknown,
> extends FilterClauseBase<TColumn> {
  op: "eq" | "neq" | "gt" | "gte" | "lt" | "lte";
  value: TValue;
}

export interface SetFilterClause<
  TColumn extends string = string,
  TValue = unknown,
> extends FilterClauseBase<TColumn> {
  op: "in";
  values: TValue[];
}

export interface NullFilterClause<
  TColumn extends string = string,
> extends FilterClauseBase<TColumn> {
  op: "is_null" | "is_not_null";
}

type ColumnName<TColumns extends TableColumns> = Extract<keyof TColumns, string>;
type ColumnFilterValueForDefinition<TDefinition extends TableColumnDefinition> =
  [TDefinition] extends [TableColumnDefinition]
    ? TableColumnDefinition extends TDefinition
      ? unknown
      : NonNullable<ColumnValue<TDefinition>>
    : unknown;
type ColumnFilterValue<TColumns extends TableColumns, TColumn extends ColumnName<TColumns>> =
  ColumnFilterValueForDefinition<TColumns[TColumn]>;

export type ScanFilterClause<
  _TColumn extends string = string,
  TColumns extends TableColumns = any,
> = {
  [TKey in ColumnName<TColumns>]:
    | ScalarFilterClause<TKey, ColumnFilterValue<TColumns, TKey>>
    | SetFilterClause<TKey, ColumnFilterValue<TColumns, TKey>>
    | NullFilterClause<TKey>;
}[ColumnName<TColumns>];

export interface ScanOrderBy<TColumn extends string = string> {
  id?: string;
  column: TColumn;
  direction: "asc" | "desc";
}

export interface TableScanRequest<
  TTable extends string = string,
  TColumn extends string = string,
  TColumns extends TableColumns = any,
> {
  table: TTable;
  alias?: string;
  select: TColumn[];
  where?: ScanFilterClause<TColumn, TColumns>[];
  orderBy?: ScanOrderBy<TColumn>[];
  limit?: number;
  offset?: number;
}

export interface TableLookupRequest<
  TTable extends string = string,
  TColumn extends string = string,
  TColumns extends TableColumns = any,
> {
  table: TTable;
  alias?: string;
  key: TColumn;
  values: unknown[];
  select: TColumn[];
  where?: ScanFilterClause<TColumn, TColumns>[];
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
  TColumns extends TableColumns = any,
> {
  table: TTable;
  alias?: string;
  where?: ScanFilterClause<TColumn, TColumns>[];
  groupBy?: TColumn[];
  metrics: TableAggregateMetric<TColumn>[];
  limit?: number;
}

export interface PlannedFilterTerm<
  TColumn extends string = string,
  TColumns extends TableColumns = any,
> {
  id: string;
  clause: ScanFilterClause<TColumn, TColumns>;
}

export interface PlannedOrderTerm<TColumn extends string = string> {
  id: string;
  term: ScanOrderBy<TColumn>;
}

export interface PlannedAggregateMetricTerm<TColumn extends string = string> {
  id: string;
  metric: TableAggregateMetric<TColumn>;
}

export interface PlannedScanRequest<
  TTable extends string = string,
  TColumn extends string = string,
  TColumns extends TableColumns = any,
> {
  table: TTable;
  alias?: string;
  select: TColumn[];
  where?: PlannedFilterTerm<TColumn, TColumns>[];
  orderBy?: PlannedOrderTerm<TColumn>[];
  limit?: number;
  offset?: number;
}

export interface PlannedLookupRequest<
  TTable extends string = string,
  TColumn extends string = string,
  TColumns extends TableColumns = any,
> {
  table: TTable;
  alias?: string;
  key: TColumn;
  values: unknown[];
  select: TColumn[];
  where?: PlannedFilterTerm<TColumn, TColumns>[];
}

export interface PlannedAggregateRequest<
  TTable extends string = string,
  TColumn extends string = string,
  TColumns extends TableColumns = any,
> {
  table: TTable;
  alias?: string;
  where?: PlannedFilterTerm<TColumn, TColumns>[];
  groupBy?: TColumn[];
  metrics: PlannedAggregateMetricTerm<TColumn>[];
  limit?: number;
}

export interface PlanRejectDecision {
  code: string;
  message: string;
}

export interface ScanPlanDecisionById {
  mode?: "by_id";
  whereIds?: string[];
  orderByIds?: string[];
  limitOffset?: "push" | "residual";
  reject?: PlanRejectDecision;
  notes?: string[];
}

export interface ScanPlanDecisionRemoteResidual<
  _TTable extends string = string,
  TColumn extends string = string,
  TColumns extends TableColumns = any,
> {
  mode: "remote_residual";
  remote?: {
    where?: ScanFilterClause<TColumn, TColumns>[];
    orderBy?: ScanOrderBy<TColumn>[];
    limit?: number;
    offset?: number;
  };
  residual?: {
    where?: ScanFilterClause<TColumn, TColumns>[];
    orderBy?: ScanOrderBy<TColumn>[];
    limit?: number;
    offset?: number;
  };
  reject?: PlanRejectDecision;
  notes?: string[];
}

export type ScanPlanDecision<
  TTable extends string = string,
  TColumn extends string = string,
  TColumns extends TableColumns = any,
> = ScanPlanDecisionById | ScanPlanDecisionRemoteResidual<TTable, TColumn, TColumns>;

export interface LookupPlanDecisionById {
  mode?: "by_id";
  whereIds?: string[];
  reject?: PlanRejectDecision;
  notes?: string[];
}

export interface LookupPlanDecisionRemoteResidual<
  _TTable extends string = string,
  TColumn extends string = string,
  TColumns extends TableColumns = any,
> {
  mode: "remote_residual";
  remote?: {
    where?: ScanFilterClause<TColumn, TColumns>[];
  };
  residual?: {
    where?: ScanFilterClause<TColumn, TColumns>[];
  };
  reject?: PlanRejectDecision;
  notes?: string[];
}

export type LookupPlanDecision<
  TTable extends string = string,
  TColumn extends string = string,
  TColumns extends TableColumns = any,
> = LookupPlanDecisionById | LookupPlanDecisionRemoteResidual<TTable, TColumn, TColumns>;

export interface AggregatePlanDecisionById {
  mode?: "by_id";
  whereIds?: string[];
  metricIds?: string[];
  groupBy?: "push" | "residual";
  limit?: "push" | "residual";
  reject?: PlanRejectDecision;
  notes?: string[];
}

export interface AggregatePlanDecisionRemoteResidual<
  _TTable extends string = string,
  TColumn extends string = string,
  TColumns extends TableColumns = any,
> {
  mode: "remote_residual";
  remote?: {
    where?: ScanFilterClause<TColumn, TColumns>[];
    groupBy?: TColumn[];
    metrics?: TableAggregateMetric<TColumn>[];
    limit?: number;
  };
  residual?: {
    where?: ScanFilterClause<TColumn, TColumns>[];
    groupBy?: TColumn[];
    metrics?: TableAggregateMetric<TColumn>[];
    limit?: number;
  };
  reject?: PlanRejectDecision;
  notes?: string[];
}

export type AggregatePlanDecision<
  TTable extends string = string,
  TColumn extends string = string,
  TColumns extends TableColumns = any,
> = AggregatePlanDecisionById | AggregatePlanDecisionRemoteResidual<TTable, TColumn, TColumns>;

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
  TColumns extends TableColumns = any,
> {
  scan(request: TableScanRequest<TTable, TColumn, TColumns>, context: TContext): Promise<QueryRow[]>;
  lookup?(
    request: TableLookupRequest<TTable, TColumn, TColumns>,
    context: TContext,
  ): Promise<QueryRow[]>;
  aggregate?(
    request: TableAggregateRequest<TTable, TColumn, TColumns>,
    context: TContext,
  ): Promise<QueryRow[]>;
  planScan?(
    request: PlannedScanRequest<TTable, TColumn, TColumns>,
    context: TContext,
  ): ScanPlanDecision<TTable, TColumn, TColumns>;
  planLookup?(
    request: PlannedLookupRequest<TTable, TColumn, TColumns>,
    context: TContext,
  ): LookupPlanDecision<TTable, TColumn, TColumns>;
  planAggregate?(
    request: PlannedAggregateRequest<TTable, TColumn, TColumns>,
    context: TContext,
  ): AggregatePlanDecision<TTable, TColumn, TColumns>;
}

export type TableMethodsMap<TContext = unknown> = Record<
  string,
  TableMethods<TContext, any, any, any>
>;

export type TableMethodsForSchema<TSchema extends SchemaDefinition, TContext = unknown> = {
  [TTableName in TableName<TSchema>]: TableMethods<
    TContext,
    TTableName,
    TableColumnName<TSchema, TTableName>,
    TSchema["tables"][TTableName]["columns"]
  >;
};

export function defineTableMethods<TContext, TMethods extends TableMethodsMap<TContext>>(
  methods: TMethods,
): TMethods;

export function defineTableMethods<
  TSchema extends SchemaDefinition,
  TContext,
>(
  schema: TSchema,
  methods: TableMethodsForSchema<TSchema, TContext>,
): TableMethodsForSchema<TSchema, TContext>;

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
      const resolved = resolveColumnDefinition(columnDefinition, {
        tableQuery: table.query,
        columnName,
      });
      const nullability = resolved.nullable ? "" : " NOT NULL";
      const metadataComment = renderColumnMetadataComment(resolved);
      return `  ${escapeIdentifier(columnName)} ${toSqlType(resolved.type)}${nullability}${metadataComment}`;
    });

    const primaryKey = resolveTablePrimaryKeyConstraint(table);
    if (primaryKey) {
      definitionLines.push(
        `  ${renderConstraintPrefix(primaryKey.name)}PRIMARY KEY (${renderColumnList(primaryKey.columns)})`,
      );
    }

    for (const uniqueConstraint of resolveTableUniqueConstraints(table)) {
      definitionLines.push(
        `  ${renderConstraintPrefix(uniqueConstraint.name)}UNIQUE (${renderColumnList(uniqueConstraint.columns)})`,
      );
    }

    for (const foreignKey of resolveTableForeignKeys(table)) {
      const onDelete = foreignKey.onDelete ? ` ON DELETE ${foreignKey.onDelete}` : "";
      const onUpdate = foreignKey.onUpdate ? ` ON UPDATE ${foreignKey.onUpdate}` : "";
      definitionLines.push(
        `  ${renderConstraintPrefix(foreignKey.name)}FOREIGN KEY (${renderColumnList(foreignKey.columns)}) REFERENCES ${escapeIdentifier(foreignKey.references.table)} (${renderColumnList(foreignKey.references.columns)})${onDelete}${onUpdate}`,
      );
    }

    for (const checkConstraint of buildCheckConstraints(tableName, table)) {
      definitionLines.push(
        `  ${renderConstraintPrefix(checkConstraint.name)}CHECK (${renderCheckExpression(checkConstraint)})`,
      );
    }

    const tableMetadata = renderTableMetadataComment(resolveTableQueryBehavior(schema, tableName));
    statements.push(
      `${createPrefix} ${escapeIdentifier(tableName)} (\n${definitionLines.join(",\n")}\n)${tableMetadata};`,
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
      return "INTEGER";
    case "timestamp":
      return "TEXT";
  }
}

function renderColumnMetadataComment(column: ResolvedColumnDefinition): string {
  const attributes = [
    `filterable:${String(column.filterable)}`,
    `sortable:${String(column.sortable)}`,
  ];

  if (column.type === "timestamp") {
    attributes.push("format:iso8601");
  }

  if (column.description) {
    attributes.push(`description:${JSON.stringify(column.description)}`);
  }

  return ` /* sqlql: ${attributes.join(" ")} */`;
}

function renderTableMetadataComment(tableBehavior: ResolvedTableQueryBehavior): string {
  return ` /* sqlql: query:${JSON.stringify(tableBehavior)} */`;
}

interface CheckConstraintForDDL {
  name?: string;
  column: string;
  values: readonly (string | number | boolean | null)[];
}

function readColumnPrimaryKeyColumns(table: TableDefinition): string[] {
  const primaryKeyColumns: string[] = [];

  for (const [columnName, columnDefinition] of Object.entries(table.columns)) {
    if (typeof columnDefinition === "string" || columnDefinition.primaryKey !== true) {
      continue;
    }
    primaryKeyColumns.push(columnName);
  }

  return primaryKeyColumns;
}

function readColumnUniqueConstraints(table: TableDefinition): UniqueConstraint[] {
  const uniqueConstraints: UniqueConstraint[] = [];

  for (const [columnName, columnDefinition] of Object.entries(table.columns)) {
    if (typeof columnDefinition === "string" || columnDefinition.unique !== true) {
      continue;
    }
    uniqueConstraints.push({
      columns: [columnName],
    });
  }

  return uniqueConstraints;
}

export function resolveTablePrimaryKeyConstraint(
  table: TableDefinition,
): PrimaryKeyConstraint | undefined {
  if (table.constraints?.primaryKey) {
    return table.constraints.primaryKey;
  }

  const primaryKeyColumns = readColumnPrimaryKeyColumns(table);
  if (primaryKeyColumns.length !== 1) {
    return undefined;
  }
  const primaryKeyColumn = primaryKeyColumns[0];
  if (!primaryKeyColumn) {
    return undefined;
  }

  return {
    columns: [primaryKeyColumn],
  };
}

export function resolveTableUniqueConstraints(table: TableDefinition): UniqueConstraint[] {
  return dedupeUniqueConstraints([
    ...readColumnUniqueConstraints(table),
    ...(table.constraints?.unique ?? []),
  ]);
}

export function resolveTableForeignKeys(table: TableDefinition): ForeignKeyConstraint[] {
  const fromColumns: ForeignKeyConstraint[] = [];
  for (const [columnName, columnDefinition] of Object.entries(table.columns)) {
    if (typeof columnDefinition === "string" || !columnDefinition.foreignKey) {
      continue;
    }

    const foreignKey = columnDefinition.foreignKey;
    fromColumns.push({
      columns: [columnName],
      references: {
        table: foreignKey.table,
        columns: [foreignKey.column],
      },
      ...(foreignKey.name ? { name: foreignKey.name } : {}),
      ...(foreignKey.onDelete ? { onDelete: foreignKey.onDelete } : {}),
      ...(foreignKey.onUpdate ? { onUpdate: foreignKey.onUpdate } : {}),
    });
  }

  return dedupeForeignKeys([...fromColumns, ...(table.constraints?.foreignKeys ?? [])]);
}

function dedupeUniqueConstraints(uniqueConstraints: UniqueConstraint[]): UniqueConstraint[] {
  const out: UniqueConstraint[] = [];
  const seen = new Set<string>();

  for (const uniqueConstraint of uniqueConstraints) {
    const signature = JSON.stringify({
      columns: uniqueConstraint.columns,
    });
    if (seen.has(signature)) {
      continue;
    }
    seen.add(signature);
    out.push(uniqueConstraint);
  }

  return out;
}

function dedupeForeignKeys(foreignKeys: ForeignKeyConstraint[]): ForeignKeyConstraint[] {
  const out: ForeignKeyConstraint[] = [];
  const seen = new Set<string>();

  for (const foreignKey of foreignKeys) {
    const signature = JSON.stringify({
      columns: foreignKey.columns,
      references: foreignKey.references,
      name: foreignKey.name ?? null,
      onDelete: foreignKey.onDelete ?? null,
      onUpdate: foreignKey.onUpdate ?? null,
    });
    if (seen.has(signature)) {
      continue;
    }
    seen.add(signature);
    out.push(foreignKey);
  }

  return out;
}

function buildCheckConstraints(tableName: string, table: TableDefinition): CheckConstraintForDDL[] {
  const checks: CheckConstraintForDDL[] = [];

  for (const [columnName, columnDefinition] of Object.entries(table.columns)) {
    const resolved = resolveColumnDefinition(columnDefinition, {
      tableQuery: table.query,
      columnName,
    });
    if (resolved.enum && resolved.enum.length > 0) {
      checks.push({
        name: `${tableName}_${columnName}_enum_check`,
        column: columnName,
        values: [...resolved.enum],
      });
    }
  }

  for (const check of table.constraints?.checks ?? []) {
    if (check.kind === "in") {
      checks.push({
        ...(check.name ? { name: check.name } : {}),
        column: check.column,
        values: [...check.values],
      });
    }
  }

  return checks;
}

function renderCheckExpression(check: CheckConstraintForDDL): string {
  const values = [...check.values];
  const hasNull = values.some((value) => value == null);
  const nonNullValues = values.filter((value) => value != null);

  if (nonNullValues.length === 0) {
    return `${escapeIdentifier(check.column)} IS NULL`;
  }

  const inList = nonNullValues.map((value) => renderSqlLiteral(value)).join(", ");
  const inExpr = `${escapeIdentifier(check.column)} IN (${inList})`;
  if (!hasNull) {
    return inExpr;
  }

  return `(${inExpr} OR ${escapeIdentifier(check.column)} IS NULL)`;
}

function renderSqlLiteral(value: string | number | boolean): string {
  if (typeof value === "string") {
    return `'${value.replaceAll("'", "''")}'`;
  }

  if (typeof value === "boolean") {
    return value ? "1" : "0";
  }

  return String(value);
}

export interface ResolvedColumnDefinition {
  type: SqlScalarType;
  nullable: boolean;
  filterable: boolean;
  sortable: boolean;
  primaryKey: boolean;
  unique: boolean;
  enum?: readonly string[];
  foreignKey?: ColumnForeignKeyReference;
  description?: string;
}

interface ResolveColumnContext {
  tableQuery?: TableQueryOverrides | undefined;
  columnName?: string | undefined;
}

export function resolveColumnDefinition(
  definition: TableColumnDefinition,
  context: ResolveColumnContext = {},
): ResolvedColumnDefinition {
  const legacyFilterable = resolveLegacyColumnCapability(
    context.tableQuery?.filterable,
    context.columnName,
  );
  const legacySortable = resolveLegacyColumnCapability(
    context.tableQuery?.sortable,
    context.columnName,
  );

  if (typeof definition === "string") {
    return {
      type: definition,
      nullable: true,
      filterable: legacyFilterable ?? true,
      sortable: legacySortable ?? true,
      primaryKey: false,
      unique: false,
    };
  }

  return {
    type: definition.type,
    nullable: definition.nullable ?? true,
    filterable: definition.filterable ?? legacyFilterable ?? true,
    sortable: definition.sortable ?? legacySortable ?? true,
    primaryKey: definition.primaryKey === true,
    unique: definition.unique === true,
    ...(definition.enum ? { enum: definition.enum } : {}),
    ...(definition.foreignKey ? { foreignKey: definition.foreignKey } : {}),
    ...(definition.description ? { description: definition.description } : {}),
  };
}

export function resolveTableColumnDefinition(
  schema: SchemaDefinition,
  tableName: string,
  columnName: string,
): ResolvedColumnDefinition {
  const table = getTable(schema, tableName);
  const column = table.columns[columnName];
  if (!column) {
    throw new Error(`Unknown column ${tableName}.${columnName}`);
  }

  return resolveColumnDefinition(column, {
    tableQuery: table.query,
    columnName,
  });
}

function resolveLegacyColumnCapability(
  configured: "all" | string[] | undefined,
  columnName: string | undefined,
): boolean | undefined {
  if (!configured || !columnName) {
    return undefined;
  }

  if (configured === "all") {
    return true;
  }

  return configured.includes(columnName);
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
    for (const [columnName, columnDefinition] of Object.entries(table.columns)) {
      const resolved = resolveColumnDefinition(columnDefinition, {
        tableQuery: table.query,
        columnName,
      });
      validateColumnDefinition(tableName, columnName, resolved);
    }

    validateTableQueryPolicy(schema, tableName, table);

    const columnPrimaryKeyColumns = readColumnPrimaryKeyColumns(table);
    if (columnPrimaryKeyColumns.length > 1) {
      throw new Error(
        `Invalid primary key on ${tableName}: multiple column-level primaryKey declarations found (${columnPrimaryKeyColumns.join(", ")}). Use table.constraints.primaryKey for composite keys.`,
      );
    }

    const tablePrimaryKey = table.constraints?.primaryKey;
    if (tablePrimaryKey && columnPrimaryKeyColumns.length === 1) {
      const columnPrimaryKeyColumn = columnPrimaryKeyColumns[0];
      const tablePrimaryKeyIsSameSingleColumn =
        tablePrimaryKey.columns.length === 1 &&
        tablePrimaryKey.columns[0] === columnPrimaryKeyColumn;
      if (!tablePrimaryKeyIsSameSingleColumn) {
        throw new Error(
          `Invalid primary key on ${tableName}: column-level primaryKey on "${columnPrimaryKeyColumn}" conflicts with table.constraints.primaryKey. Use one declaration style.`,
        );
      }
    }

    const resolvedPrimaryKey = resolveTablePrimaryKeyConstraint(table);
    if (resolvedPrimaryKey) {
      validateConstraintColumns(schema, tableName, "primary key", resolvedPrimaryKey.columns);
      validateNoDuplicateColumns(tableName, "primary key", resolvedPrimaryKey.columns);
    }

    const constraints = table.constraints;
    resolveTableUniqueConstraints(table).forEach((uniqueConstraint, index) => {
      const label = uniqueConstraint.name ?? `unique constraint #${index + 1}`;
      validateConstraintColumns(schema, tableName, label, uniqueConstraint.columns);
      validateNoDuplicateColumns(tableName, label, uniqueConstraint.columns);
    });

    const foreignKeys = resolveTableForeignKeys(table);
    foreignKeys.forEach((foreignKey, index) => {
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

    constraints?.checks?.forEach((checkConstraint, index) => {
      const label = checkConstraint.name ?? `check constraint #${index + 1}`;
      if (checkConstraint.kind === "in") {
        validateConstraintColumns(schema, tableName, label, [checkConstraint.column]);
        if (checkConstraint.values.length === 0) {
          throw new Error(`Invalid ${label} on ${tableName}: values cannot be empty.`);
        }

        const columnType = resolveTableColumnDefinition(schema, tableName, checkConstraint.column).type;
        const valueTypes = new Set(
          checkConstraint.values
            .filter((value): value is string | number | boolean => value != null)
            .map((value) => typeof value),
        );
        for (const valueType of valueTypes) {
          if (
            (columnType === "text" && valueType !== "string") ||
            (columnType === "integer" && valueType !== "number") ||
            (columnType === "boolean" && valueType !== "boolean") ||
            (columnType === "timestamp" && valueType !== "string")
          ) {
            throw new Error(
              `Invalid ${label} on ${tableName}: value type ${valueType} does not match column type ${columnType}.`,
            );
          }
        }
      }
    });
  }
}

function validateTableQueryPolicy(
  schema: SchemaDefinition,
  tableName: string,
  table: TableDefinition,
): void {
  const requireAnyFilterOn = table.query?.reject?.requireAnyFilterOn ?? [];
  for (const columnName of requireAnyFilterOn) {
    if (!(columnName in table.columns)) {
      throw new Error(
        `Invalid reject policy on ${tableName}: required filter column "${columnName}" does not exist on table "${tableName}".`,
      );
    }
  }

  for (const deprecatedKey of ["filterable", "sortable"] as const) {
    const deprecatedValue = table.query?.[deprecatedKey];
    if (!Array.isArray(deprecatedValue)) {
      continue;
    }
    for (const columnName of deprecatedValue) {
      if (!(columnName in table.columns)) {
        throw new Error(
          `Invalid query.${deprecatedKey} on ${tableName}: column "${columnName}" does not exist on table "${tableName}".`,
        );
      }
    }
  }

  const defaultRequireAnyFilterOn = schema.defaults?.query?.reject?.requireAnyFilterOn;
  if (Array.isArray(defaultRequireAnyFilterOn)) {
    for (const columnName of defaultRequireAnyFilterOn) {
      if (!(columnName in table.columns)) {
        throw new Error(
          `Invalid defaults.query.reject.requireAnyFilterOn entry "${columnName}" for table "${tableName}": column does not exist.`,
        );
      }
    }
  }
}

function validateColumnDefinition(
  tableName: string,
  columnName: string,
  definition: ResolvedColumnDefinition,
): void {
  if (definition.primaryKey && definition.unique) {
    throw new Error(
      `Invalid column ${tableName}.${columnName}: primaryKey and unique cannot both be true.`,
    );
  }

  if (definition.primaryKey && definition.nullable) {
    throw new Error(
      `Invalid column ${tableName}.${columnName}: primaryKey columns must be nullable: false.`,
    );
  }

  if (definition.enum && definition.type !== "text") {
    throw new Error(
      `Invalid column ${tableName}.${columnName}: enum is only supported on text columns.`,
    );
  }

  if (definition.enum) {
    if (definition.enum.length === 0) {
      throw new Error(`Invalid column ${tableName}.${columnName}: enum cannot be empty.`);
    }

    const unique = new Set(definition.enum);
    if (unique.size !== definition.enum.length) {
      throw new Error(
        `Invalid column ${tableName}.${columnName}: enum contains duplicate values.`,
      );
    }
  }

  if (definition.foreignKey) {
    if (definition.foreignKey.table.trim().length === 0) {
      throw new Error(
        `Invalid column ${tableName}.${columnName}: foreignKey.table cannot be empty.`,
      );
    }
    if (definition.foreignKey.column.trim().length === 0) {
      throw new Error(
        `Invalid column ${tableName}.${columnName}: foreignKey.column cannot be empty.`,
      );
    }
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

function warnDeprecatedQueryCapabilities(schema: SchemaDefinition): void {
  for (const [tableName, table] of Object.entries(schema.tables)) {
    if (table.query?.filterable == null && table.query?.sortable == null) {
      continue;
    }

    if (warnedDeprecatedQueryCapabilities.has(tableName)) {
      continue;
    }

    warnedDeprecatedQueryCapabilities.add(tableName);
    console.warn(
      `Deprecated schema query capability fields used on table ${tableName}: query.filterable/query.sortable are deprecated; use per-column filterable/sortable instead.`,
    );
  }
}
