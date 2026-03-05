export type SqlScalarType = "text" | "integer" | "boolean" | "timestamp";
export type PhysicalDialect = "postgres" | "sqlite";

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
  enum?: readonly string[];
  enumFrom?: SchemaColRefToken | string;
  enumMap?: Record<string, string>;
  physicalType?: string;
  physicalDialect?: PhysicalDialect;
  foreignKey?: ColumnForeignKeyReference;
  description?: string;
}

export type ColumnDefinition = ColumnDefinitionBase & ColumnConstraintFlags;

export type TableColumnDefinition = SqlScalarType | ColumnDefinition;

export type TableColumns = Record<string, TableColumnDefinition>;

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
  /**
   * Provider binding used by the provider-first planner/executor.
   */
  provider?: string;
  columns: TableColumns;
  constraints?: TableConstraints;
}

export interface SchemaDefinition {
  tables: Record<string, TableDefinition>;
}

export interface SchemaDataEntityHandle<TColumns extends string = string> {
  kind: "data_entity";
  entity: string;
  provider: string;
  readonly __columns__?: TColumns;
}

declare const SCHEMA_DSL_TABLE_TOKEN_BRAND: unique symbol;

export interface SchemaDslTableToken<TColumns extends string = string> {
  kind: "dsl_table_token";
  readonly __id: symbol;
  readonly [SCHEMA_DSL_TABLE_TOKEN_BRAND]: TColumns;
}

export interface SchemaColRefToken {
  kind: "dsl_col_ref";
  ref?: string;
  table?: SchemaDslTableToken<string>;
  entity?: SchemaDataEntityHandle<string>;
  column?: string;
}

export interface SchemaViewEqExpr {
  kind: "eq";
  left: SchemaColRefToken;
  right: SchemaColRefToken;
}

export interface SchemaViewScanNode {
  kind: "scan";
  table: string;
}

export interface SchemaViewJoinNode {
  kind: "join";
  left: SchemaViewRelNode;
  right: SchemaViewRelNode;
  on: SchemaViewEqExpr;
  type: "inner" | "left" | "right" | "full";
}

export interface SchemaViewAggregateMetric {
  kind: "metric";
  fn: "sum" | "count";
  column?: SchemaColRefToken;
}

export interface SchemaViewAggregateNode {
  kind: "aggregate";
  from: SchemaViewRelNode;
  groupBy: SchemaColRefToken[];
  measures: Record<string, SchemaViewAggregateMetric>;
}

export type SchemaViewRelNode = SchemaViewScanNode | SchemaViewJoinNode | SchemaViewAggregateNode;

interface SchemaViewScanNodeInput {
  kind: "scan";
  table: string | SchemaDslTableToken<string>;
}

interface SchemaViewJoinNodeInput {
  kind: "join";
  left: SchemaViewRelNodeInput;
  right: SchemaViewRelNodeInput;
  on: SchemaViewEqExpr;
  type: "inner" | "left" | "right" | "full";
}

interface SchemaViewAggregateNodeInput {
  kind: "aggregate";
  from: SchemaViewRelNodeInput;
  groupBy: SchemaColRefToken[];
  measures: Record<string, SchemaViewAggregateMetric>;
}

type SchemaViewRelNodeInput =
  | SchemaViewScanNodeInput
  | SchemaViewJoinNodeInput
  | SchemaViewAggregateNodeInput;

export interface SchemaColumnLensDefinition {
  source: string | SchemaColRefToken;
  type?: SqlScalarType;
  nullable?: boolean;
  primaryKey?: boolean;
  unique?: boolean;
  enum?: readonly string[];
  enumFrom?: SchemaColRefToken | string;
  enumMap?: Record<string, string>;
  physicalType?: string;
  physicalDialect?: PhysicalDialect;
  foreignKey?: ColumnForeignKeyReference;
  description?: string;
}

interface DslTableDefinition<TColumns extends string = string> {
  kind: "dsl_table";
  tableToken: SchemaDslTableToken<TColumns>;
  from: SchemaDataEntityHandle;
  columns: Record<TColumns, TableColumnDefinition | SchemaColumnLensDefinition | SchemaColRefToken>;
  constraints?: TableConstraints;
}

interface DslViewDefinition<TContext> {
  kind: "dsl_view";
  rel: (context: TContext) => SchemaViewRelNodeInput | unknown;
  columns: Record<string, SchemaColumnLensDefinition | SchemaColRefToken>;
  constraints?: TableConstraints;
}

interface SchemaDslExprHelpers {
  eq: (left: SchemaColRefToken, right: SchemaColRefToken) => SchemaViewEqExpr;
}

interface SchemaDslAggHelpers {
  sum: (column: SchemaColRefToken) => SchemaViewAggregateMetric;
  count: () => SchemaViewAggregateMetric;
}

interface SchemaDslRelHelpers {
  scan: {
    (table: string): SchemaViewScanNodeInput;
    (table: SchemaDslTableToken<string>): SchemaViewScanNodeInput;
    <TColumns extends string>(table: DslTableDefinition<TColumns>): SchemaViewScanNodeInput;
  };
  join: (input: {
    left: SchemaViewRelNodeInput;
    right: SchemaViewRelNodeInput;
    on: SchemaViewEqExpr;
    type?: "inner" | "left" | "right" | "full";
  }) => SchemaViewJoinNodeInput;
  aggregate: (input: {
    from: SchemaViewRelNodeInput;
    groupBy: SchemaColRefToken[];
    measures: Record<string, SchemaViewAggregateMetric>;
  }) => SchemaViewAggregateNodeInput;
}

interface SchemaDslHelpers<TContext> {
  table: {
    <TColumns extends string>(input: {
      from: SchemaDataEntityHandle;
      columns: Record<TColumns, TableColumnDefinition | SchemaColumnLensDefinition | SchemaColRefToken>;
      constraints?: TableConstraints;
    }): DslTableDefinition<TColumns>;
  };
  view: (input: {
    rel: (context: TContext) => SchemaViewRelNodeInput | unknown;
    columns: Record<string, SchemaColumnLensDefinition | SchemaColRefToken>;
    constraints?: TableConstraints;
  }) => DslViewDefinition<TContext>;
  col: {
    (ref: string): SchemaColRefToken;
    <TColumns extends string, TColumn extends TColumns>(
      entity: SchemaDataEntityHandle<TColumns>,
      column: TColumn,
    ): SchemaColRefToken;
    <TColumns extends string, TColumn extends TColumns>(
      table: SchemaDslTableToken<TColumns> | DslTableDefinition<TColumns>,
      column: TColumn,
    ): SchemaColRefToken;
  };
  expr: SchemaDslExprHelpers;
  agg: SchemaDslAggHelpers;
  rel: SchemaDslRelHelpers;
}

interface SchemaDslDefinition<TContext> {
  tables: Record<string, DslTableDefinition | DslViewDefinition<TContext> | TableDefinition>;
}

export interface NormalizedPhysicalTableBinding {
  kind: "physical";
  provider?: string;
  entity: string;
  columnBindings: Record<string, NormalizedColumnBinding>;
  columnToSource: Record<string, string>;
}

export interface NormalizedViewTableBinding<TContext = unknown> {
  kind: "view";
  rel: (context: TContext) => SchemaViewRelNode | unknown;
  columnBindings: Record<string, NormalizedColumnBinding>;
  columnToSource: Record<string, string>;
}

export interface NormalizedColumnBinding {
  source: string;
}

export type NormalizedTableBinding<TContext = unknown> =
  | NormalizedPhysicalTableBinding
  | NormalizedViewTableBinding<TContext>;

interface SchemaNormalizationState {
  tables: Record<string, NormalizedTableBinding>;
}

const normalizedSchemaState = new WeakMap<SchemaDefinition, SchemaNormalizationState>();

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

export function defineSchema<TContext>(
  schemaBuilder: (helpers: SchemaDslHelpers<TContext>) => SchemaDslDefinition<TContext>,
): SchemaDefinition;
export function defineSchema<TSchema extends SchemaDefinition>(schema: TSchema): TSchema;
export function defineSchema<TSchema extends SchemaDefinition, TContext>(
  input: TSchema | ((helpers: SchemaDslHelpers<TContext>) => SchemaDslDefinition<TContext>),
): TSchema | SchemaDefinition {
  const schema = typeof input === "function"
    ? normalizeDslSchema(input as (helpers: SchemaDslHelpers<TContext>) => SchemaDslDefinition<TContext>)
    : (input as SchemaDefinition);

  attachIdentityBindingsIfMissing(schema);
  validateTableProviders(schema);
  validateSchemaConstraints(schema);
  return schema;
}

export function getNormalizedTableBinding(
  schema: SchemaDefinition,
  tableName: string,
): NormalizedTableBinding | undefined {
  return normalizedSchemaState.get(schema)?.tables[tableName];
}

export function getNormalizedColumnBindings(
  binding: Pick<NormalizedPhysicalTableBinding | NormalizedViewTableBinding, "columnBindings" | "columnToSource">,
): Record<string, NormalizedColumnBinding> {
  if (binding.columnBindings && Object.keys(binding.columnBindings).length > 0) {
    return binding.columnBindings;
  }

  return Object.fromEntries(
    Object.entries(binding.columnToSource).map(([column, source]) => [column, { source }]),
  );
}

export function getNormalizedColumnSourceMap(
  binding: Pick<NormalizedPhysicalTableBinding | NormalizedViewTableBinding, "columnBindings" | "columnToSource">,
): Record<string, string> {
  const entries = Object.entries(getNormalizedColumnBindings(binding));
  return Object.fromEntries(entries.map(([column, columnBinding]) => [column, columnBinding.source]));
}

export function resolveNormalizedColumnSource(
  binding: Pick<NormalizedPhysicalTableBinding | NormalizedViewTableBinding, "columnBindings" | "columnToSource">,
  logicalColumn: string,
): string {
  return getNormalizedColumnBindings(binding)[logicalColumn]?.source ?? logicalColumn;
}

function buildColumnSourceMapFromBindings(
  columnBindings: Record<string, NormalizedColumnBinding>,
): Record<string, string> {
  return Object.fromEntries(
    Object.entries(columnBindings).map(([column, binding]) => [column, binding.source]),
  );
}

function normalizeDslSchema<TContext>(
  schemaBuilder: (helpers: SchemaDslHelpers<TContext>) => SchemaDslDefinition<TContext>,
): SchemaDefinition {
  const helpers = buildSchemaDslHelpers<TContext>();
  const built = schemaBuilder(helpers);

  const tables: Record<string, TableDefinition> = {};
  const bindings: Record<string, NormalizedTableBinding> = {};
  const tableTokenToName = new Map<symbol, string>();

  for (const [tableName, rawTable] of Object.entries(built.tables)) {
    if (!isDslTableDefinition(rawTable)) {
      continue;
    }
    tableTokenToName.set(rawTable.tableToken.__id, tableName);
  }

  const resolveTableToken = (token: SchemaDslTableToken<string>): string => {
    const tableName = tableTokenToName.get(token.__id);
    if (!tableName) {
      throw new Error("Schema DSL table token could not be resolved to a table name.");
    }
    return tableName;
  };
  const resolveEntityToken = (entity: SchemaDataEntityHandle<string>): string => {
    if (!entity.entity || entity.entity.length === 0) {
      throw new Error("Schema DSL data entity handle is missing entity name.");
    }
    return entity.entity;
  };

  for (const [tableName, rawTable] of Object.entries(built.tables)) {
    if (isDslTableDefinition(rawTable)) {
      const normalizedColumns: TableColumns = {};
      const columnBindings: Record<string, NormalizedColumnBinding> = {};
      for (const [columnName, rawColumn] of Object.entries(rawTable.columns)) {
        const normalized = normalizeColumnLens(columnName, rawColumn, {
          preserveQualifiedRef: false,
          resolveTableToken,
          resolveEntityToken,
        });
        normalizedColumns[columnName] = normalized.definition;
        columnBindings[columnName] = { source: normalized.source };
      }

      tables[tableName] = {
        provider: rawTable.from.provider,
        columns: normalizedColumns,
        ...(rawTable.constraints ? { constraints: rawTable.constraints } : {}),
      };

      bindings[tableName] = {
        kind: "physical",
        provider: rawTable.from.provider,
        entity: rawTable.from.entity,
        columnBindings,
        columnToSource: buildColumnSourceMapFromBindings(columnBindings),
      };
      continue;
    }

    if (isDslViewDefinition(rawTable)) {
      const normalizedColumns: TableColumns = {};
      const columnBindings: Record<string, NormalizedColumnBinding> = {};
      for (const [columnName, rawColumn] of Object.entries(rawTable.columns)) {
        const normalized = normalizeColumnLens(columnName, rawColumn, {
          preserveQualifiedRef: true,
          resolveTableToken,
          resolveEntityToken,
        });
        normalizedColumns[columnName] = normalized.definition;
        columnBindings[columnName] = { source: normalized.source };
      }

      tables[tableName] = {
        provider: "__view__",
        columns: normalizedColumns,
        ...(rawTable.constraints ? { constraints: rawTable.constraints } : {}),
      };

      bindings[tableName] = {
        kind: "view",
        rel: (context: unknown) => {
          const definition = rawTable.rel(context as TContext);
          return resolveViewRelDefinition(definition, resolveTableToken, resolveEntityToken);
        },
        columnBindings,
        columnToSource: buildColumnSourceMapFromBindings(columnBindings),
      };
      continue;
    }

    tables[tableName] = rawTable as TableDefinition;
  }

  const schema: SchemaDefinition = { tables };

  normalizedSchemaState.set(schema, { tables: bindings });
  return schema;
}

function attachIdentityBindingsIfMissing(schema: SchemaDefinition): void {
  const existing = normalizedSchemaState.get(schema);
  if (existing) {
    const tables = { ...existing.tables };
    for (const [tableName, table] of Object.entries(schema.tables)) {
      if (tables[tableName]) {
        continue;
      }
      const columns = Object.keys(table.columns);
      tables[tableName] = {
        kind: "physical",
        ...(table.provider ? { provider: table.provider } : {}),
        entity: tableName,
        columnBindings: Object.fromEntries(columns.map((column) => [column, { source: column }])),
        columnToSource: Object.fromEntries(columns.map((column) => [column, column])),
      };
    }
    normalizedSchemaState.set(schema, { tables });
    return;
  }

  const tables: Record<string, NormalizedTableBinding> = {};
  for (const [tableName, table] of Object.entries(schema.tables)) {
    const columns = Object.keys(table.columns);
    tables[tableName] = {
      kind: "physical",
      ...(table.provider ? { provider: table.provider } : {}),
      entity: tableName,
      columnBindings: Object.fromEntries(columns.map((column) => [column, { source: column }])),
      columnToSource: Object.fromEntries(columns.map((column) => [column, column])),
    };
  }

  normalizedSchemaState.set(schema, { tables });
}

function normalizeColumnLens(
  columnName: string,
  rawColumn: TableColumnDefinition | SchemaColumnLensDefinition | SchemaColRefToken,
  options: {
    preserveQualifiedRef: boolean;
    resolveTableToken: (token: SchemaDslTableToken<string>) => string;
    resolveEntityToken: (entity: SchemaDataEntityHandle<string>) => string;
  },
): {
  source: string;
  definition: TableColumnDefinition;
} {
  if (isSchemaColRefToken(rawColumn)) {
    const ref = resolveColRefToken(
      rawColumn,
      options.resolveTableToken,
      options.resolveEntityToken,
    );
    return {
      source: options.preserveQualifiedRef ? ref : parseColumnSource(ref),
      definition: "text",
    };
  }

  if (isColumnLensDefinition(rawColumn)) {
    const sourceRef = isSchemaColRefToken(rawColumn.source)
      ? resolveColRefToken(
          rawColumn.source,
          options.resolveTableToken,
          options.resolveEntityToken,
        )
      : rawColumn.source;
    const enumFromRef = rawColumn.enumFrom
      ? resolveEnumRef(
          rawColumn.enumFrom,
          options.resolveTableToken,
          options.resolveEntityToken,
        )
      : undefined;

    const definition = {
        type: rawColumn.type ?? "text",
        ...(rawColumn.nullable != null ? { nullable: rawColumn.nullable } : {}),
        ...(rawColumn.primaryKey === true
          ? { primaryKey: true as const }
          : rawColumn.primaryKey === false
            ? { primaryKey: false as const }
            : {}),
        ...(rawColumn.unique === true
          ? { unique: true as const }
          : rawColumn.unique === false
            ? { unique: false as const }
            : {}),
        ...(rawColumn.enum ? { enum: rawColumn.enum } : {}),
        ...(enumFromRef ? { enumFrom: enumFromRef } : {}),
        ...(rawColumn.enumMap ? { enumMap: rawColumn.enumMap } : {}),
        ...(rawColumn.physicalType ? { physicalType: rawColumn.physicalType } : {}),
        ...(rawColumn.physicalDialect ? { physicalDialect: rawColumn.physicalDialect } : {}),
        ...(rawColumn.foreignKey ? { foreignKey: rawColumn.foreignKey } : {}),
        ...(rawColumn.description ? { description: rawColumn.description } : {}),
      } as TableColumnDefinition;

    return {
      source: options.preserveQualifiedRef
        ? sourceRef
        : parseColumnSource(sourceRef),
      definition,
    };
  }

  if (typeof rawColumn !== "string") {
    const enumFromRef = rawColumn.enumFrom
      ? resolveEnumRef(
          rawColumn.enumFrom,
          options.resolveTableToken,
          options.resolveEntityToken,
        )
      : undefined;
    return {
      source: columnName,
      definition: {
        ...rawColumn,
        ...(enumFromRef ? { enumFrom: enumFromRef } : {}),
      },
    };
  }

  return {
    source: columnName,
    definition: rawColumn,
  };
}

function resolveColRefToken(
  token: SchemaColRefToken,
  resolveTableToken: (token: SchemaDslTableToken<string>) => string,
  resolveEntityToken: (entity: SchemaDataEntityHandle<string>) => string,
): string {
  if (token.ref) {
    return token.ref;
  }

  if (token.table && token.column) {
    return `${resolveTableToken(token.table)}.${token.column}`;
  }

  if (token.entity && token.column) {
    return `${resolveEntityToken(token.entity)}.${token.column}`;
  }

  throw new Error("Invalid schema column reference token.");
}

function resolveEnumRef(
  enumFrom: SchemaColRefToken | string,
  resolveTableToken: (token: SchemaDslTableToken<string>) => string,
  resolveEntityToken: (entity: SchemaDataEntityHandle<string>) => string,
): string {
  if (typeof enumFrom === "string") {
    return enumFrom;
  }

  return resolveColRefToken(enumFrom, resolveTableToken, resolveEntityToken);
}

function resolveViewRelDefinition(
  definition: unknown,
  resolveTableToken: (token: SchemaDslTableToken<string>) => string,
  resolveEntityToken: (entity: SchemaDataEntityHandle<string>) => string,
): SchemaViewRelNode | unknown {
  if (
    definition &&
    typeof definition === "object" &&
    typeof (definition as { convention?: unknown }).convention === "string"
  ) {
    return definition;
  }

  if (
    !definition ||
    typeof definition !== "object" ||
    typeof (definition as { kind?: unknown }).kind !== "string"
  ) {
    return definition;
  }

  const asRef = (token: SchemaColRefToken): SchemaColRefToken => ({
    kind: "dsl_col_ref",
    ref: resolveColRefToken(token, resolveTableToken, resolveEntityToken),
  });

  const resolveNode = (node: SchemaViewRelNodeInput): SchemaViewRelNode => {
    switch (node.kind) {
      case "scan":
        return {
          kind: "scan",
          table: typeof node.table === "string" ? node.table : resolveTableToken(node.table),
        };
      case "join":
        return {
          kind: "join",
          left: resolveNode(node.left),
          right: resolveNode(node.right),
          on: {
            kind: "eq",
            left: asRef(node.on.left),
            right: asRef(node.on.right),
          },
          type: node.type,
        };
      case "aggregate":
        return {
          kind: "aggregate",
          from: resolveNode(node.from),
          groupBy: node.groupBy.map(asRef),
          measures: Object.fromEntries(
            Object.entries(node.measures).map(([name, metric]) => [
              name,
              metric.column
                ? {
                    ...metric,
                    column: asRef(metric.column),
                  }
                : metric,
            ]),
          ),
        };
    }
  };

  return resolveNode(definition as SchemaViewRelNodeInput);
}

function parseColumnSource(ref: string): string {
  const idx = ref.lastIndexOf(".");
  return idx >= 0 ? ref.slice(idx + 1) : ref;
}

function buildSchemaDslHelpers<TContext>(): SchemaDslHelpers<TContext> {
  const createTableToken = <TColumns extends string>(): SchemaDslTableToken<TColumns> =>
    ({
      kind: "dsl_table_token",
      __id: Symbol("schema_dsl_table"),
    }) as SchemaDslTableToken<TColumns>;

  const toTableToken = <TColumns extends string>(
    table: SchemaDslTableToken<TColumns> | DslTableDefinition<TColumns>,
  ): SchemaDslTableToken<TColumns> =>
    isDslTableDefinition(table) ? table.tableToken : table;

  return {
    table<TColumns extends string>(input: {
      from: SchemaDataEntityHandle;
      columns: Record<TColumns, TableColumnDefinition | SchemaColumnLensDefinition | SchemaColRefToken>;
      constraints?: TableConstraints;
    }): DslTableDefinition<TColumns> {
      return {
        kind: "dsl_table",
        tableToken: createTableToken<TColumns>(),
        from: input.from,
        columns: input.columns,
        ...(input.constraints ? { constraints: input.constraints } : {}),
      };
    },
    view(input) {
      return {
        kind: "dsl_view",
        rel: input.rel,
        columns: input.columns,
        ...(input.constraints ? { constraints: input.constraints } : {}),
      };
    },
    col<TColumns extends string, TColumn extends TColumns>(
      tableOrRef:
        | string
        | SchemaDataEntityHandle<TColumns>
        | SchemaDslTableToken<TColumns>
        | DslTableDefinition<TColumns>,
      column?: TColumn,
    ): SchemaColRefToken {
      if (typeof tableOrRef === "string") {
        if (column != null) {
          throw new Error("Schema DSL col(ref) does not accept a second argument for string refs.");
        }
        return {
          kind: "dsl_col_ref",
          ref: tableOrRef,
        } as const;
      }

      if (column == null) {
        throw new Error("Schema DSL col(table, column) requires a column name.");
      }

      if (isSchemaDataEntityHandle(tableOrRef)) {
        return {
          kind: "dsl_col_ref",
          entity: tableOrRef,
          column,
        } as const;
      }

      return {
        kind: "dsl_col_ref",
        table: toTableToken(tableOrRef),
        column,
      } as const;
    },
    expr: {
      eq(left, right) {
        return {
          kind: "eq",
          left,
          right,
        };
      },
    },
    agg: {
      sum(column) {
        return {
          kind: "metric",
          fn: "sum",
          column,
        };
      },
      count() {
        return {
          kind: "metric",
          fn: "count",
        };
      },
    },
    rel: {
      scan<TColumns extends string>(
        table: string | SchemaDslTableToken<TColumns> | DslTableDefinition<TColumns>,
      ): SchemaViewScanNodeInput {
        return {
          kind: "scan",
          table: typeof table === "string" ? table : toTableToken(table),
        } as const;
      },
      join(input) {
        return {
          kind: "join",
          left: input.left,
          right: input.right,
          on: input.on,
          type: input.type ?? "inner",
        };
      },
      aggregate(input) {
        return {
          kind: "aggregate",
          from: input.from,
          groupBy: input.groupBy,
          measures: input.measures,
        };
      },
    },
  };
}

function isDslTableDefinition(value: unknown): value is DslTableDefinition {
  return (
    !!value &&
    typeof value === "object" &&
    (value as { kind?: unknown }).kind === "dsl_table" &&
    isSchemaDslTableToken((value as { tableToken?: unknown }).tableToken)
  );
}

function isDslViewDefinition<TContext>(value: unknown): value is DslViewDefinition<TContext> {
  return !!value && typeof value === "object" && (value as { kind?: unknown }).kind === "dsl_view";
}

function isSchemaColRefToken(value: unknown): value is SchemaColRefToken {
  if (!value || typeof value !== "object" || (value as { kind?: unknown }).kind !== "dsl_col_ref") {
    return false;
  }

  const token = value as { ref?: unknown; table?: unknown; entity?: unknown; column?: unknown };
  const hasStringRef = typeof token.ref === "string";
  const hasTableColumnRef = isSchemaDslTableToken(token.table) && typeof token.column === "string";
  const hasEntityColumnRef =
    isSchemaDataEntityHandle(token.entity) && typeof token.column === "string";
  return hasStringRef || hasTableColumnRef || hasEntityColumnRef;
}

function isSchemaDslTableToken(value: unknown): value is SchemaDslTableToken<string> {
  return (
    !!value &&
    typeof value === "object" &&
    (value as { kind?: unknown }).kind === "dsl_table_token" &&
    typeof (value as { __id?: unknown }).__id === "symbol"
  );
}

function isSchemaDataEntityHandle(value: unknown): value is SchemaDataEntityHandle<string> {
  return (
    !!value &&
    typeof value === "object" &&
    (value as { kind?: unknown }).kind === "data_entity" &&
    typeof (value as { entity?: unknown }).entity === "string" &&
    typeof (value as { provider?: unknown }).provider === "string"
  );
}

function isColumnLensDefinition(value: unknown): value is SchemaColumnLensDefinition {
  if (!value || typeof value !== "object") {
    return false;
  }

  const source = (value as { source?: unknown }).source;
  return typeof source === "string" || isSchemaColRefToken(source);
}

function validateTableProviders(schema: SchemaDefinition): void {
  for (const [tableName, table] of Object.entries(schema.tables)) {
    const normalized = getNormalizedTableBinding(schema, tableName);
    if (normalized?.kind === "view") {
      continue;
    }

    if (table.provider == null) {
      continue;
    }

    if (typeof table.provider !== "string" || table.provider.trim().length === 0) {
      throw new Error(
        `Table ${tableName} must define a non-empty provider binding (table.provider).`,
      );
    }
  }
}

export function getTable(schema: SchemaDefinition, tableName: string): TableDefinition {
  const table = schema.tables[tableName];
  if (!table) {
    throw new Error(`Unknown table: ${tableName}`);
  }

  return table;
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
    const binding = getNormalizedTableBinding(schema, tableName);
    if (binding?.kind === "view") {
      continue;
    }

    const columnEntries = Object.entries(table.columns);
    if (columnEntries.length === 0) {
      throw new Error(`Cannot generate DDL for table ${tableName} with no columns.`);
    }

    const definitionLines = columnEntries.map(([columnName, columnDefinition]) => {
      const resolved = resolveColumnDefinition(columnDefinition);
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
      return "INTEGER";
    case "timestamp":
      return "TEXT";
  }
}

function renderColumnMetadataComment(column: ResolvedColumnDefinition): string {
  const attributes: string[] = [];

  if (column.type === "timestamp") {
    attributes.push("format:iso8601");
  }

  if (column.description) {
    attributes.push(`description:${JSON.stringify(column.description)}`);
  }

  if (attributes.length === 0) {
    return "";
  }

  return ` /* sqlql: ${attributes.join(" ")} */`;
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
    const resolved = resolveColumnDefinition(columnDefinition);
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
  primaryKey: boolean;
  unique: boolean;
  enum?: readonly string[];
  enumFrom?: string;
  enumMap?: Record<string, string>;
  physicalType?: string;
  physicalDialect?: PhysicalDialect;
  foreignKey?: ColumnForeignKeyReference;
  description?: string;
}

export function resolveColumnDefinition(
  definition: TableColumnDefinition,
): ResolvedColumnDefinition {
  if (typeof definition === "string") {
    return {
      type: definition,
      nullable: true,
      primaryKey: false,
      unique: false,
    };
  }

  const normalizedEnumFrom = normalizeEnumFromDefinition(definition.enumFrom);

  return {
    type: definition.type,
    nullable: definition.nullable ?? true,
    primaryKey: definition.primaryKey === true,
    unique: definition.unique === true,
    ...(definition.enum ? { enum: definition.enum } : {}),
    ...(normalizedEnumFrom ? { enumFrom: normalizedEnumFrom } : {}),
    ...(definition.enumMap ? { enumMap: definition.enumMap } : {}),
    ...(definition.physicalType ? { physicalType: definition.physicalType } : {}),
    ...(definition.physicalDialect ? { physicalDialect: definition.physicalDialect } : {}),
    ...(definition.foreignKey ? { foreignKey: definition.foreignKey } : {}),
    ...(definition.description ? { description: definition.description } : {}),
  };
}

function normalizeEnumFromDefinition(
  enumFrom: ColumnDefinition["enumFrom"] | undefined,
): string | undefined {
  if (!enumFrom) {
    return undefined;
  }

  if (typeof enumFrom === "string") {
    return enumFrom;
  }

  return enumFrom.ref;
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

  return resolveColumnDefinition(column);
}

export interface EnumLinkReference {
  table: string;
  column: string;
}

export interface ResolveSchemaLinkedEnumsOptions {
  resolveEnumValues?: (
    ref: EnumLinkReference,
    schema: SchemaDefinition,
  ) => readonly string[] | undefined;
  onUnresolved?: "throw" | "ignore";
  strictUnmapped?: boolean;
}

export function resolveSchemaLinkedEnums(
  schema: SchemaDefinition,
  options: ResolveSchemaLinkedEnumsOptions = {},
): SchemaDefinition {
  const resolveEnumValues = options.resolveEnumValues ?? defaultResolveLinkedEnumValues;
  const onUnresolved = options.onUnresolved ?? "throw";
  const strictUnmapped = options.strictUnmapped ?? true;

  let changed = false;
  const tables: Record<string, TableDefinition> = {};

  for (const [tableName, table] of Object.entries(schema.tables)) {
    const columns: TableColumns = {};

    for (const [columnName, columnDefinition] of Object.entries(table.columns)) {
      if (typeof columnDefinition === "string") {
        columns[columnName] = columnDefinition;
        continue;
      }

      const resolved = resolveColumnDefinition(columnDefinition);
      if (!resolved.enumFrom) {
        columns[columnName] = columnDefinition;
        continue;
      }

      const ref = parseEnumLinkReference(resolved.enumFrom, tableName, columnName);
      const upstreamEnum = resolveEnumValues(ref, schema);
      if (!upstreamEnum || upstreamEnum.length === 0) {
        if (onUnresolved === "throw") {
          throw new Error(
            `Unable to resolve enumFrom for ${tableName}.${columnName} from ${ref.table}.${ref.column}.`,
          );
        }
        columns[columnName] = columnDefinition;
        continue;
      }

      const mappedValues: string[] = [];
      for (const upstreamValue of upstreamEnum) {
        if (resolved.enumMap) {
          const mapped = resolved.enumMap[upstreamValue];
          if (!mapped) {
            if (strictUnmapped) {
              throw new Error(
                `Unmapped enumFrom value "${upstreamValue}" for ${tableName}.${columnName}.`,
              );
            }
            continue;
          }
          mappedValues.push(mapped);
          continue;
        }
        mappedValues.push(upstreamValue);
      }

      const inferredEnum = [...new Set(mappedValues)];
      if (inferredEnum.length === 0 && strictUnmapped) {
        throw new Error(
          `enumFrom resolution for ${tableName}.${columnName} produced no facade values.`,
        );
      }

      if (resolved.enum) {
        for (const enumValue of inferredEnum) {
          if (!resolved.enum.includes(enumValue)) {
            throw new Error(
              `enumFrom mapping produced value "${enumValue}" not listed in enum for ${tableName}.${columnName}.`,
            );
          }
        }
      }

      const materializedEnum = resolved.enum ?? inferredEnum;
      const nextDefinition: ColumnDefinition = {
        ...columnDefinition,
        enum: materializedEnum,
      };
      columns[columnName] = nextDefinition;
      changed = true;
    }

    tables[tableName] = {
      ...table,
      columns,
    };
  }

  if (!changed) {
    return schema;
  }

  const resolvedSchema: SchemaDefinition = { tables };

  const existingBindings = normalizedSchemaState.get(schema);
  if (existingBindings) {
    normalizedSchemaState.set(resolvedSchema, {
      tables: { ...existingBindings.tables },
    });
  }
  attachIdentityBindingsIfMissing(resolvedSchema);
  validateTableProviders(resolvedSchema);
  validateSchemaConstraints(resolvedSchema);
  return resolvedSchema;
}

function parseEnumLinkReference(enumFrom: string, tableName: string, columnName: string): EnumLinkReference {
  const idx = enumFrom.lastIndexOf(".");
  if (idx < 0) {
    return {
      table: tableName,
      column: enumFrom,
    };
  }

  const table = enumFrom.slice(0, idx).trim();
  const column = enumFrom.slice(idx + 1).trim();
  if (!table || !column) {
    throw new Error(`Invalid enumFrom reference on ${tableName}.${columnName}: "${enumFrom}".`);
  }
  return { table, column };
}

function defaultResolveLinkedEnumValues(
  ref: EnumLinkReference,
  schema: SchemaDefinition,
): readonly string[] | undefined {
  const table = schema.tables[ref.table];
  if (!table) {
    return undefined;
  }

  const columnDefinition = table.columns[ref.column];
  if (!columnDefinition || typeof columnDefinition === "string") {
    return undefined;
  }

  const resolved = resolveColumnDefinition(columnDefinition);
  return resolved.enum;
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
      const resolved = resolveColumnDefinition(columnDefinition);
      validateColumnDefinition(tableName, columnName, resolved);
    }

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

  if (definition.enumFrom && definition.type !== "text") {
    throw new Error(
      `Invalid column ${tableName}.${columnName}: enumFrom is only supported on text columns.`,
    );
  }

  if (definition.enumFrom && definition.enumFrom.trim().length === 0) {
    throw new Error(
      `Invalid column ${tableName}.${columnName}: enumFrom cannot be empty.`,
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

  if (definition.enumMap) {
    if (!definition.enumFrom) {
      throw new Error(
        `Invalid column ${tableName}.${columnName}: enumMap requires enumFrom.`,
      );
    }

    for (const [sourceValue, mappedValue] of Object.entries(definition.enumMap)) {
      if (sourceValue.length === 0) {
        throw new Error(
          `Invalid column ${tableName}.${columnName}: enumMap contains an empty source key.`,
        );
      }
      if (mappedValue.length === 0) {
        throw new Error(
          `Invalid column ${tableName}.${columnName}: enumMap contains an empty mapped value.`,
        );
      }
      if (definition.enum && !definition.enum.includes(mappedValue)) {
        throw new Error(
          `Invalid column ${tableName}.${columnName}: enumMap value "${mappedValue}" is not listed in enum.`,
        );
      }
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
