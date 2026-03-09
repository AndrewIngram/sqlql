import type { DataEntityColumnMetadata, DataEntityReadMetadataMap } from "../model/data-entity";
import type { RelExpr } from "../model/rel";
import { getDataEntityAdapter } from "../provider";
import {
  registerNormalizedSchema,
  type NormalizedColumnBinding,
  type NormalizedTableBinding,
} from "./normalize";
import { finalizeSchemaDefinition } from "./validate";
import type {
  ColumnDefinition,
  ColumnForeignKeyReference,
  AggregateFunction,
  PhysicalDialect,
  SchemaDataEntityHandle,
  SchemaDefinition,
  SchemaValueCoercion,
  SqlScalarType,
  TableColumnDefinition,
  TableColumns,
  TableConstraints,
  TableDefinition,
} from "./definition";
import { resolveColumnDefinition } from "./definition";

type CompatibleSourceScalarType<TTarget extends SqlScalarType> = TTarget extends "real"
  ? "real" | "integer"
  : TTarget;

type IsAny<T> = 0 extends 1 & T ? true : false;
type IsExactly<T, U> = [T] extends [U] ? ([U] extends [T] ? true : false) : false;

type StripNullish<T> = Exclude<T, null | undefined>;

type InferSourceScalarTypeFromRead<TRead> =
  IsAny<TRead> extends true
    ? never
    : [StripNullish<TRead>] extends [never]
      ? never
      : StripNullish<TRead> extends number
        ? "integer" | "real"
        : StripNullish<TRead> extends boolean
          ? "boolean"
          : StripNullish<TRead> extends Uint8Array
            ? "blob"
            : StripNullish<TRead> extends Date
              ? "timestamp"
              : StripNullish<TRead> extends string
                ? "text"
                : StripNullish<TRead> extends object
                  ? "json"
                  : never;

type ExplicitSourceScalarType<TMetadata> = Extract<
  TMetadata extends { type?: infer TType } ? TType : never,
  SqlScalarType
>;

type KnownSourceScalarType<TMetadata> = [ExplicitSourceScalarType<TMetadata>] extends [never]
  ? InferSourceScalarTypeFromRead<
      TMetadata extends { readonly __read__?: infer TRead } ? TRead : never
    >
  : IsExactly<ExplicitSourceScalarType<TMetadata>, SqlScalarType> extends true
    ? InferSourceScalarTypeFromRead<
        TMetadata extends { readonly __read__?: infer TRead } ? TRead : never
      >
    : ExplicitSourceScalarType<TMetadata>;

type IsCompileTimeCompatibleSourceType<
  TSource extends SqlScalarType,
  TTarget extends SqlScalarType,
> = [Extract<TSource, CompatibleSourceScalarType<TTarget>>] extends [never] ? false : true;

type CompatibleColumnName<
  TColumns extends string,
  TColumnMetadata extends Partial<Record<TColumns, DataEntityColumnMetadata<any>>>,
  TTarget extends SqlScalarType,
> = Extract<
  {
    [K in TColumns]: [KnownSourceScalarType<TColumnMetadata[K]>] extends [never]
      ? K
      : IsCompileTimeCompatibleSourceType<
            KnownSourceScalarType<TColumnMetadata[K]>,
            TTarget
          > extends true
        ? K
        : never;
  }[TColumns],
  string
>;

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
  entity?: SchemaDataEntityHandle<string>;
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
  fn: AggregateFunction;
  column?: SchemaColRefToken;
  distinct?: boolean;
}

export interface SchemaViewAggregateNode {
  kind: "aggregate";
  from: SchemaViewRelNode;
  groupBy: Record<string, SchemaColRefToken>;
  measures: Record<string, SchemaViewAggregateMetric>;
}

export type SchemaViewRelNode = SchemaViewScanNode | SchemaViewJoinNode | SchemaViewAggregateNode;

interface SchemaViewRelNodeInputBase<TColumns extends string> {
  readonly __columns__?: TColumns;
}

interface SchemaViewScanNodeInput<
  TColumns extends string = string,
> extends SchemaViewRelNodeInputBase<TColumns> {
  kind: "scan";
  table: string | SchemaDslTableToken<string> | SchemaDataEntityHandle<TColumns>;
}

interface SchemaViewJoinNodeInput<
  TColumns extends string = string,
> extends SchemaViewRelNodeInputBase<TColumns> {
  kind: "join";
  left: SchemaViewRelNodeInput;
  right: SchemaViewRelNodeInput;
  on: SchemaViewEqExpr;
  type: "inner" | "left" | "right" | "full";
}

interface SchemaViewAggregateNodeInput<
  TColumns extends string = string,
> extends SchemaViewRelNodeInputBase<TColumns> {
  kind: "aggregate";
  from: SchemaViewRelNodeInput;
  groupBy: Record<string, SchemaColRefToken>;
  measures: Record<string, SchemaViewAggregateMetric>;
}

export type SchemaViewRelNodeInput<TColumns extends string = string> =
  | SchemaViewScanNodeInput<TColumns>
  | SchemaViewJoinNodeInput<TColumns>
  | SchemaViewAggregateNodeInput<TColumns>;

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
  coerce?: SchemaValueCoercion;
}

export interface SchemaTypedColumnDefinition<TSourceColumn extends string = string> {
  kind: "dsl_typed_column";
  sourceColumn: TSourceColumn;
  definition: TableColumnDefinition;
  coerce?: SchemaValueCoercion;
}

export interface SchemaCalculatedColumnDefinition {
  kind: "dsl_calculated_column";
  expr: RelExpr;
  definition: TableColumnDefinition;
  coerce?: SchemaValueCoercion;
}

type DslTableColumnInput<TSourceColumns extends string = string> =
  | TableColumnDefinition
  | SchemaColumnLensDefinition
  | SchemaColRefToken
  | SchemaTypedColumnDefinition<TSourceColumns>
  | SchemaCalculatedColumnDefinition;

type DslViewColumnInput<TSourceColumns extends string = string> =
  | SchemaColumnLensDefinition
  | SchemaColRefToken
  | SchemaTypedColumnDefinition<TSourceColumns>
  | SchemaCalculatedColumnDefinition;

type SchemaDslRelationRef<TColumns extends string> =
  | SchemaDslTableToken<TColumns>
  | DslTableDefinition<TColumns, string>
  | DslViewDefinition<any, TColumns, string>;

interface DslTableDefinition<
  TMappedColumns extends string = string,
  TSourceColumns extends string = string,
> {
  kind: "dsl_table";
  tableToken: SchemaDslTableToken<TMappedColumns>;
  from: SchemaDataEntityHandle<TSourceColumns>;
  columns: Record<TMappedColumns, DslTableColumnInput<TSourceColumns>>;
  constraints?: TableConstraints;
}

interface DslViewDefinition<
  TContext,
  TColumns extends string = string,
  TRelColumns extends string = string,
> {
  kind: "dsl_view";
  tableToken: SchemaDslTableToken<TColumns>;
  rel: (
    context: TContext,
    helpers: SchemaDslViewRelHelpers,
  ) => SchemaViewRelNodeInput<TRelColumns> | unknown;
  columns: Record<TColumns, DslViewColumnInput<TRelColumns>>;
  constraints?: TableConstraints;
}

interface SchemaTypedColumnBuilderOptions {
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
  coerce?: SchemaValueCoercion;
}

interface SchemaDslRelExprHelpers {
  eq: (left: SchemaColRefToken, right: SchemaColRefToken) => SchemaViewEqExpr;
}

interface SchemaDslAggHelpers {
  count: () => SchemaViewAggregateMetric;
  countDistinct: (column: SchemaColRefToken) => SchemaViewAggregateMetric;
  sum: (column: SchemaColRefToken) => SchemaViewAggregateMetric;
  sumDistinct: (column: SchemaColRefToken) => SchemaViewAggregateMetric;
  avg: (column: SchemaColRefToken) => SchemaViewAggregateMetric;
  avgDistinct: (column: SchemaColRefToken) => SchemaViewAggregateMetric;
  min: (column: SchemaColRefToken) => SchemaViewAggregateMetric;
  max: (column: SchemaColRefToken) => SchemaViewAggregateMetric;
}

interface SchemaDslRelHelpers {
  scan: {
    (table: string): SchemaViewScanNodeInput<string>;
    (table: SchemaDslTableToken<string>): SchemaViewScanNodeInput<string>;
    <TColumns extends string>(
      table: SchemaDslTableToken<TColumns>,
    ): SchemaViewScanNodeInput<TColumns>;
    <TColumns extends string>(
      entity: SchemaDataEntityHandle<TColumns>,
    ): SchemaViewScanNodeInput<TColumns>;
    <TColumns extends string>(
      table: DslTableDefinition<TColumns, string>,
    ): SchemaViewScanNodeInput<TColumns>;
    <TColumns extends string>(
      table: DslViewDefinition<any, TColumns, string>,
    ): SchemaViewScanNodeInput<TColumns>;
  };
  join: <TLeftColumns extends string, TRightColumns extends string>(input: {
    left: SchemaViewRelNodeInput<TLeftColumns>;
    right: SchemaViewRelNodeInput<TRightColumns>;
    on: SchemaViewEqExpr;
    type?: "inner" | "left" | "right" | "full";
  }) => SchemaViewJoinNodeInput<TLeftColumns | TRightColumns>;
  aggregate: <
    TGroupBy extends Record<string, SchemaColRefToken>,
    TMeasures extends Record<string, SchemaViewAggregateMetric>,
  >(input: {
    from: SchemaViewRelNodeInput<string>;
    groupBy: TGroupBy;
    measures: TMeasures;
  }) => SchemaViewAggregateNodeInput<Extract<keyof TGroupBy | keyof TMeasures, string>>;
}

interface SchemaDslRelColHelpers {
  (ref: string): SchemaColRefToken;
  <TColumns extends string, TColumn extends TColumns>(
    entity: SchemaDataEntityHandle<TColumns>,
    column: TColumn,
  ): SchemaColRefToken;
  <TColumns extends string, TColumn extends TColumns>(
    table: SchemaDslRelationRef<TColumns>,
    column: TColumn,
  ): SchemaColRefToken;
}

interface SchemaDslViewRelHelpers extends SchemaDslRelHelpers {
  col: SchemaDslRelColHelpers;
  expr: SchemaDslRelExprHelpers;
  agg: SchemaDslAggHelpers;
}

type SchemaTypedColumnBuilderMethod<
  TSourceColumns extends string,
  TColumnMetadata extends Partial<Record<TSourceColumns, DataEntityColumnMetadata<any>>>,
  TType extends SqlScalarType,
  TOptions extends SchemaTypedColumnBuilderOptions,
> = {
  <TSourceColumn extends CompatibleColumnName<TSourceColumns, TColumnMetadata, TType>>(
    sourceColumn: TSourceColumn,
    options?: TOptions,
  ): SchemaTypedColumnDefinition<TSourceColumn>;
  <TSourceColumn extends TSourceColumns>(
    sourceColumn: TSourceColumn,
    options: TOptions & { coerce: SchemaValueCoercion },
  ): SchemaTypedColumnDefinition<TSourceColumn>;
  <TRelColumns extends string, TColumn extends TRelColumns>(
    table: SchemaDataEntityHandle<TRelColumns> | SchemaDslRelationRef<TRelColumns>,
    column: TColumn,
    options?: TOptions,
  ): SchemaColumnLensDefinition;
  (
    expr: RelExpr,
    options?: Omit<TOptions, "primaryKey" | "unique" | "enum" | "enumFrom" | "enumMap">,
  ): SchemaCalculatedColumnDefinition;
};

interface SchemaTypedColumnBuilder<
  TSourceColumns extends string,
  TColumnMetadata extends Partial<Record<TSourceColumns, DataEntityColumnMetadata<any>>> =
    DataEntityReadMetadataMap<TSourceColumns, Record<TSourceColumns, unknown>>,
> {
  id: SchemaTypedColumnBuilderMethod<
    TSourceColumns,
    TColumnMetadata,
    "text",
    Omit<
      SchemaTypedColumnBuilderOptions,
      "primaryKey" | "nullable" | "enum" | "enumFrom" | "enumMap"
    >
  >;
  string: SchemaTypedColumnBuilderMethod<
    TSourceColumns,
    TColumnMetadata,
    "text",
    SchemaTypedColumnBuilderOptions
  >;
  integer: SchemaTypedColumnBuilderMethod<
    TSourceColumns,
    TColumnMetadata,
    "integer",
    SchemaTypedColumnBuilderOptions
  >;
  real: SchemaTypedColumnBuilderMethod<
    TSourceColumns,
    TColumnMetadata,
    "real",
    SchemaTypedColumnBuilderOptions
  >;
  blob: SchemaTypedColumnBuilderMethod<
    TSourceColumns,
    TColumnMetadata,
    "blob",
    SchemaTypedColumnBuilderOptions
  >;
  boolean: SchemaTypedColumnBuilderMethod<
    TSourceColumns,
    TColumnMetadata,
    "boolean",
    SchemaTypedColumnBuilderOptions
  >;
  timestamp: SchemaTypedColumnBuilderMethod<
    TSourceColumns,
    TColumnMetadata,
    "timestamp",
    SchemaTypedColumnBuilderOptions
  >;
  date: SchemaTypedColumnBuilderMethod<
    TSourceColumns,
    TColumnMetadata,
    "date",
    SchemaTypedColumnBuilderOptions
  >;
  datetime: SchemaTypedColumnBuilderMethod<
    TSourceColumns,
    TColumnMetadata,
    "datetime",
    SchemaTypedColumnBuilderOptions
  >;
  json: SchemaTypedColumnBuilderMethod<
    TSourceColumns,
    TColumnMetadata,
    "json",
    SchemaTypedColumnBuilderOptions
  >;
}

type SchemaColumnsColHelper<
  TSourceColumns extends string,
  TColumnMetadata extends Partial<Record<TSourceColumns, DataEntityColumnMetadata<any>>> =
    DataEntityReadMetadataMap<TSourceColumns, Record<TSourceColumns, unknown>>,
> = SchemaTypedColumnBuilder<TSourceColumns, TColumnMetadata> & {
  (ref: string): RelExpr;
  <TColumns extends string, TColumn extends TColumns>(
    table: SchemaDslRelationRef<TColumns>,
    column: TColumn,
  ): RelExpr;
};

interface SchemaColumnExprHelpers {
  literal: (value: string | number | boolean | null) => RelExpr;
  eq: (left: RelExpr, right: RelExpr) => RelExpr;
  neq: (left: RelExpr, right: RelExpr) => RelExpr;
  gt: (left: RelExpr, right: RelExpr) => RelExpr;
  gte: (left: RelExpr, right: RelExpr) => RelExpr;
  lt: (left: RelExpr, right: RelExpr) => RelExpr;
  lte: (left: RelExpr, right: RelExpr) => RelExpr;
  add: (left: RelExpr, right: RelExpr) => RelExpr;
  subtract: (left: RelExpr, right: RelExpr) => RelExpr;
  multiply: (left: RelExpr, right: RelExpr) => RelExpr;
  divide: (left: RelExpr, right: RelExpr) => RelExpr;
  and: (...args: RelExpr[]) => RelExpr;
  or: (...args: RelExpr[]) => RelExpr;
  not: (input: RelExpr) => RelExpr;
}

type SchemaBuilderTableMethods = {
  <
    TSourceColumns extends string,
    TMappedColumns extends string,
    TRow extends Partial<Record<TSourceColumns, unknown>> = Record<TSourceColumns, unknown>,
    TColumnMetadata extends Partial<Record<TSourceColumns, DataEntityColumnMetadata<any>>> =
      DataEntityReadMetadataMap<TSourceColumns, TRow>,
  >(
    name: string,
    from: SchemaDataEntityHandle<TSourceColumns, TRow, TColumnMetadata>,
    input: {
      columns:
        | Record<TMappedColumns, DslTableColumnInput<TSourceColumns>>
        | ((helpers: {
            col: SchemaColumnsColHelper<TSourceColumns, TColumnMetadata>;
            expr: SchemaColumnExprHelpers;
          }) => Record<TMappedColumns, DslTableColumnInput<TSourceColumns>>);
      constraints?: TableConstraints;
    },
  ): DslTableDefinition<TMappedColumns, TSourceColumns>;
};

type SchemaBuilderViewMethods<TContext> = {
  <TRelColumns extends string, TColumns extends string>(
    name: string,
    rel: (
      helpers: SchemaDslViewRelHelpers,
      context: TContext,
    ) => SchemaViewRelNodeInput<TRelColumns> | unknown,
    input: {
      columns:
        | ((helpers: {
            col: SchemaColumnsColHelper<
              TRelColumns,
              DataEntityReadMetadataMap<TRelColumns, Record<TRelColumns, unknown>>
            >;
            expr: SchemaColumnExprHelpers;
          }) => Record<TColumns, DslViewColumnInput<TRelColumns>>)
        | Record<TColumns, DslViewColumnInput<TRelColumns>>;
      constraints?: TableConstraints;
    },
  ): DslViewDefinition<TContext, TColumns, TRelColumns>;
  <TColumns extends string>(
    name: string,
    rel: (context: TContext) => SchemaViewRelNodeInput<string> | unknown,
    input: {
      columns: Record<TColumns, DslViewColumnInput<string>>;
      constraints?: TableConstraints;
    },
  ): DslViewDefinition<TContext, TColumns, string>;
};

export interface SchemaBuilder<TContext> {
  table: SchemaBuilderTableMethods;
  view: SchemaBuilderViewMethods<TContext>;
  build(): SchemaDefinition;
}

type RegisteredSchemaDefinition<TContext> =
  | DslTableDefinition<string, string>
  | DslViewDefinition<TContext, string, string>;

interface SchemaBuilderState<TContext> {
  definitions: Map<string, RegisteredSchemaDefinition<TContext>>;
}

const schemaBuilderState = new WeakMap<object, SchemaBuilderState<any>>();

export function createSchemaBuilder<TContext>(): SchemaBuilder<TContext> {
  const state: SchemaBuilderState<TContext> = {
    definitions: new Map(),
  };

  const registerDefinition = <TDefinition extends RegisteredSchemaDefinition<TContext>>(
    name: string,
    definition: TDefinition,
  ): TDefinition => {
    if (typeof name !== "string" || name.trim().length === 0) {
      throw new Error("Schema builder table/view name must be a non-empty string.");
    }
    if (state.definitions.has(name)) {
      throw new Error(`Schema builder already contains a table or view named ${name}.`);
    }
    state.definitions.set(name, definition);
    return definition;
  };

  const table = ((name: any, from: any, input: any) => {
    if (
      typeof name !== "string" ||
      name.trim().length === 0 ||
      !isSchemaDataEntityHandle(from) ||
      !input
    ) {
      throw new Error(
        "Schema builder table(name, source, config) requires a non-empty name, a data entity handle, and a config object.",
      );
    }

    const columns =
      typeof input.columns === "function"
        ? input.columns({
            col: buildSchemaColumnsColHelper(),
            expr: buildColumnExprHelpers(),
          })
        : input.columns;

    return registerDefinition(name, {
      kind: "dsl_table" as const,
      tableToken: createSchemaDslTableToken(),
      from,
      columns,
      ...(input.constraints ? { constraints: input.constraints } : {}),
    });
  }) as SchemaBuilder<TContext>["table"];

  const view = ((name: any, relFactory: any, input: any) => {
    if (
      typeof name !== "string" ||
      name.trim().length === 0 ||
      typeof relFactory !== "function" ||
      !input
    ) {
      throw new Error(
        "Schema builder view(name, source, config) requires a non-empty name, a rel function, and a config object.",
      );
    }
    const rel = (context: TContext, helpers: SchemaDslViewRelHelpers) =>
      relFactory.length === 0
        ? (relFactory as () => SchemaViewRelNodeInput<string> | unknown)()
        : relFactory(helpers, context);
    const columns =
      typeof input.columns === "function"
        ? input.columns({
            col: buildSchemaColumnsColHelper(),
            expr: buildColumnExprHelpers(),
          })
        : input.columns;

    return registerDefinition(name, {
      kind: "dsl_view" as const,
      tableToken: createSchemaDslTableToken(),
      rel,
      columns,
      ...(input.constraints ? { constraints: input.constraints } : {}),
    });
  }) as SchemaBuilder<TContext>["view"];

  const builder: SchemaBuilder<TContext> = {
    table,
    view,
    build() {
      return buildRegisteredSchemaDefinition(state);
    },
  };

  schemaBuilderState.set(builder as object, state);
  return builder;
}

export function isSchemaBuilder<TContext = unknown>(
  value: unknown,
): value is SchemaBuilder<TContext> {
  return !!value && typeof value === "object" && schemaBuilderState.has(value as object);
}

function buildRegisteredSchemaDefinition<TContext>(
  state: SchemaBuilderState<TContext>,
): SchemaDefinition {
  const tables: Record<string, TableDefinition> = {};
  const bindings: Record<string, NormalizedTableBinding> = {};
  const tableTokenToName = new Map<symbol, string>();
  const entries = [...state.definitions.entries()];

  for (const [tableName, rawTable] of entries) {
    if (isDslTableDefinition(rawTable) || isDslViewDefinition(rawTable)) {
      tableTokenToName.set(rawTable.tableToken.__id, tableName);
    }
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
  const viewRelHelpers = buildSchemaDslViewRelHelpers();

  for (const [tableName, rawTable] of entries) {
    if (isDslTableDefinition(rawTable)) {
      const normalizedColumns: TableColumns = {};
      const columnBindings: Record<string, NormalizedColumnBinding> = {};
      for (const [columnName, rawColumn] of Object.entries(rawTable.columns)) {
        const normalized = normalizeColumnBinding(columnName, rawColumn, {
          preserveQualifiedRef: false,
          resolveTableToken,
          resolveEntityToken,
          entity: rawTable.from,
        });
        normalizedColumns[columnName] = normalized.definition;
        columnBindings[columnName] = normalized.binding;
      }
      validateCalculatedColumnDependencies(tableName, columnBindings);

      tables[tableName] = {
        provider: rawTable.from.provider,
        columns: normalizedColumns,
        ...(rawTable.constraints ? { constraints: rawTable.constraints } : {}),
      };
      const adapter = getDataEntityAdapter(rawTable.from);

      bindings[tableName] = {
        kind: "physical",
        provider: rawTable.from.provider,
        entity: rawTable.from.entity,
        columnBindings,
        columnToSource: buildColumnSourceMapFromBindings(columnBindings),
        ...(adapter ? { adapter } : {}),
      };
      continue;
    }

    if (isDslViewDefinition(rawTable)) {
      const normalizedColumns: TableColumns = {};
      const columnBindings: Record<string, NormalizedColumnBinding> = {};
      for (const [columnName, rawColumn] of Object.entries(rawTable.columns)) {
        const normalized = normalizeColumnBinding(columnName, rawColumn, {
          preserveQualifiedRef: true,
          resolveTableToken,
          resolveEntityToken,
        });
        normalizedColumns[columnName] = normalized.definition;
        columnBindings[columnName] = normalized.binding;
      }
      validateCalculatedColumnDependencies(tableName, columnBindings);

      tables[tableName] = {
        provider: "__view__",
        columns: normalizedColumns,
        ...(rawTable.constraints ? { constraints: rawTable.constraints } : {}),
      };

      bindings[tableName] = {
        kind: "view",
        rel: (context: unknown) => {
          const definition = rawTable.rel(context as TContext, viewRelHelpers);
          return resolveViewRelDefinition(definition, resolveTableToken, resolveEntityToken);
        },
        columnBindings,
        columnToSource: buildColumnSourceMapFromBindings(columnBindings),
      };
      continue;
    }

    tables[tableName] = rawTable as never;
  }

  const schema: SchemaDefinition = { tables };
  registerNormalizedSchema(schema, bindings);
  return finalizeSchemaDefinition(schema);
}

function buildColumnSourceMapFromBindings(
  columnBindings: Record<string, NormalizedColumnBinding>,
): Record<string, string> {
  return Object.fromEntries(
    Object.entries(columnBindings).flatMap(([column, binding]) =>
      binding.kind === "source" ? [[column, binding.source] as const] : [],
    ),
  );
}

function normalizeColumnBinding(
  columnName: string,
  rawColumn: DslTableColumnInput | DslViewColumnInput,
  options: {
    preserveQualifiedRef: boolean;
    resolveTableToken: (token: SchemaDslTableToken<string>) => string;
    resolveEntityToken: (entity: SchemaDataEntityHandle<string>) => string;
    entity?: SchemaDataEntityHandle<string>;
  },
): {
  definition: TableColumnDefinition;
  binding: NormalizedColumnBinding;
} {
  if (isSchemaCalculatedColumnDefinition(rawColumn)) {
    return {
      definition: rawColumn.definition,
      binding: {
        kind: "expr",
        expr: resolveColumnExpr(
          rawColumn.expr,
          options.resolveTableToken,
          options.resolveEntityToken,
        ),
        definition: rawColumn.definition,
        ...(rawColumn.coerce ? { coerce: rawColumn.coerce } : {}),
      },
    };
  }

  if (isSchemaTypedColumnDefinition(rawColumn)) {
    const source = options.entity
      ? resolveEntityColumnSource(rawColumn.sourceColumn, options.entity)
      : rawColumn.sourceColumn;
    assertColumnCompatibility(
      rawColumn.sourceColumn,
      rawColumn.definition,
      rawColumn.coerce,
      options.entity,
    );
    return {
      definition: rawColumn.definition,
      binding: {
        kind: "source",
        source,
        definition: rawColumn.definition,
        ...(rawColumn.coerce ? { coerce: rawColumn.coerce } : {}),
      },
    };
  }

  if (isSchemaColRefToken(rawColumn)) {
    const ref = resolveColRefToken(
      rawColumn,
      options.resolveTableToken,
      options.resolveEntityToken,
    );
    return {
      definition: "text",
      binding: {
        kind: "source",
        source: options.preserveQualifiedRef ? ref : parseColumnSource(ref),
        definition: "text",
      },
    };
  }

  if (isColumnLensDefinition(rawColumn)) {
    const sourceRef = isSchemaColRefToken(rawColumn.source)
      ? resolveColRefToken(rawColumn.source, options.resolveTableToken, options.resolveEntityToken)
      : rawColumn.source;
    const enumFromRef = rawColumn.enumFrom
      ? resolveEnumRef(rawColumn.enumFrom, options.resolveTableToken, options.resolveEntityToken)
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
      definition,
      binding: {
        kind: "source",
        source: options.preserveQualifiedRef ? sourceRef : parseColumnSource(sourceRef),
        definition,
        ...(rawColumn.coerce ? { coerce: rawColumn.coerce } : {}),
      },
    };
  }

  if (typeof rawColumn !== "string") {
    const enumFromRef = rawColumn.enumFrom
      ? resolveEnumRef(rawColumn.enumFrom, options.resolveTableToken, options.resolveEntityToken)
      : undefined;
    return {
      definition: {
        ...rawColumn,
        ...(enumFromRef ? { enumFrom: enumFromRef } : {}),
      },
      binding: {
        kind: "source",
        source: columnName,
        definition: {
          ...rawColumn,
          ...(enumFromRef ? { enumFrom: enumFromRef } : {}),
        },
      },
    };
  }

  return {
    definition: rawColumn,
    binding: {
      kind: "source",
      source: columnName,
      definition: rawColumn,
    },
  };
}

function resolveColumnExpr(
  expr: RelExpr,
  resolveTableToken: (token: SchemaDslTableToken<string>) => string,
  resolveEntityToken: (entity: SchemaDataEntityHandle<string>) => string,
): RelExpr {
  switch (expr.kind) {
    case "literal":
      return expr;
    case "function":
      return {
        kind: "function",
        name: expr.name,
        args: expr.args.map((arg) => resolveColumnExpr(arg, resolveTableToken, resolveEntityToken)),
      };
    case "column": {
      const tableOrAlias = (expr.ref as { table?: unknown; alias?: unknown }).table;
      if (isSchemaDslTableToken(tableOrAlias)) {
        return {
          kind: "column",
          ref: {
            table: resolveTableToken(tableOrAlias),
            column: expr.ref.column,
          },
        };
      }
      if (isSchemaDataEntityHandle(tableOrAlias)) {
        return {
          kind: "column",
          ref: {
            table: resolveEntityToken(tableOrAlias),
            column: expr.ref.column,
          },
        };
      }
      return expr;
    }
    case "subquery":
      return expr;
  }
}

function validateCalculatedColumnDependencies(
  tableName: string,
  columnBindings: Record<string, NormalizedColumnBinding>,
): void {
  const exprColumns = new Set(
    Object.entries(columnBindings)
      .filter(([, binding]) => binding.kind === "expr")
      .map(([column]) => column),
  );

  for (const [columnName, binding] of Object.entries(columnBindings)) {
    if (binding.kind !== "expr") {
      continue;
    }

    for (const dependency of collectUnqualifiedExprColumns(binding.expr)) {
      if (!exprColumns.has(dependency)) {
        continue;
      }
      throw new Error(
        `Calculated column ${tableName}.${columnName} cannot reference calculated sibling ${tableName}.${dependency} in the same columns block.`,
      );
    }
  }
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
        if (isSchemaDataEntityHandle((node as { entity?: unknown }).entity)) {
          const entity = (node as unknown as { entity: SchemaDataEntityHandle<string> }).entity;
          return {
            kind: "scan",
            table: typeof node.table === "string" ? node.table : resolveEntityToken(entity),
            entity,
          };
        }
        if (isSchemaDataEntityHandle(node.table)) {
          return {
            kind: "scan",
            table: resolveEntityToken(node.table),
            entity: node.table,
          };
        }
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
          groupBy: Object.fromEntries(
            Object.entries(node.groupBy).map(([name, token]) => [name, asRef(token)]),
          ),
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

function collectUnqualifiedExprColumns(expr: RelExpr): Set<string> {
  const out = new Set<string>();

  const visit = (current: RelExpr): void => {
    switch (current.kind) {
      case "literal":
      case "subquery":
        return;
      case "function":
        current.args.forEach(visit);
        return;
      case "column":
        if (!current.ref.table && !current.ref.alias) {
          out.add(current.ref.column);
        }
        return;
    }
  };

  visit(expr);
  return out;
}

function resolveEntityColumnSource(column: string, entity: SchemaDataEntityHandle<string>): string {
  return entity.columns?.[column]?.source ?? column;
}

function sourceTypeMatchesTargetType(
  sourceType: SqlScalarType | undefined,
  targetType: SqlScalarType,
): boolean {
  if (!sourceType) {
    return true;
  }
  switch (targetType) {
    case "real":
      return sourceType === "real" || sourceType === "integer";
    default:
      return sourceType === targetType;
  }
}

function assertColumnCompatibility(
  logicalColumn: string,
  definition: TableColumnDefinition,
  coerce: SchemaValueCoercion | undefined,
  entity: SchemaDataEntityHandle<string> | undefined,
): void {
  if (!entity || coerce) {
    return;
  }

  const sourceMetadata = entity.columns?.[logicalColumn];
  if (!sourceMetadata?.type) {
    return;
  }

  const targetType = resolveColumnDefinition(definition).type;
  if (!sourceTypeMatchesTargetType(sourceMetadata.type, targetType)) {
    throw new Error(
      `Column ${entity.entity}.${sourceMetadata.source} is exposed as ${sourceMetadata.type}, but the schema declared ${targetType}. Add a coerce function or align the declared type.`,
    );
  }
}

function createSchemaDslTableToken<TColumns extends string>(): SchemaDslTableToken<TColumns> {
  return {
    kind: "dsl_table_token",
    __id: Symbol("schema_dsl_table"),
  } as SchemaDslTableToken<TColumns>;
}

function toSchemaDslTableToken<TColumns extends string>(
  table: SchemaDslRelationRef<TColumns>,
): SchemaDslTableToken<TColumns> {
  if (isSchemaDslTableToken(table)) {
    return table;
  }
  return table.tableToken as SchemaDslTableToken<TColumns>;
}

function buildTypedColumnDefinition<TSourceColumn extends string>(
  sourceColumn: TSourceColumn,
  type: SqlScalarType,
  options: SchemaTypedColumnBuilderOptions = {},
): SchemaTypedColumnDefinition<TSourceColumn> {
  const definition = { type } as ColumnDefinition;
  if (options.nullable != null) {
    definition.nullable = options.nullable;
  }
  if (options.primaryKey === true) {
    definition.primaryKey = true;
  } else if (options.unique === true) {
    definition.unique = true;
  }
  if (options.enum) {
    definition.enum = options.enum;
  }
  if (options.enumFrom) {
    definition.enumFrom = options.enumFrom;
  }
  if (options.enumMap) {
    definition.enumMap = options.enumMap;
  }
  if (options.physicalType) {
    definition.physicalType = options.physicalType;
  }
  if (options.physicalDialect) {
    definition.physicalDialect = options.physicalDialect;
  }
  if (options.foreignKey) {
    definition.foreignKey = options.foreignKey;
  }
  if (options.description) {
    definition.description = options.description;
  }
  return {
    kind: "dsl_typed_column",
    sourceColumn,
    definition,
    ...(options.coerce ? { coerce: options.coerce } : {}),
  };
}

function buildTypedColumnBuilder<
  TSourceColumns extends string,
  TColumnMetadata extends Partial<Record<TSourceColumns, DataEntityColumnMetadata<any>>> =
    DataEntityReadMetadataMap<TSourceColumns, Record<TSourceColumns, unknown>>,
>(): SchemaTypedColumnBuilder<TSourceColumns, TColumnMetadata> {
  const buildSourceLensDefinition = (
    source: string | SchemaColRefToken,
    type: SqlScalarType,
    options: SchemaTypedColumnBuilderOptions = {},
  ): SchemaColumnLensDefinition => ({
    source,
    type,
    ...(options.nullable != null ? { nullable: options.nullable } : {}),
    ...(options.primaryKey != null ? { primaryKey: options.primaryKey } : {}),
    ...(options.unique != null ? { unique: options.unique } : {}),
    ...(options.enum ? { enum: options.enum } : {}),
    ...(options.enumFrom ? { enumFrom: options.enumFrom } : {}),
    ...(options.enumMap ? { enumMap: options.enumMap } : {}),
    ...(options.physicalType ? { physicalType: options.physicalType } : {}),
    ...(options.physicalDialect ? { physicalDialect: options.physicalDialect } : {}),
    ...(options.foreignKey ? { foreignKey: options.foreignKey } : {}),
    ...(options.description ? { description: options.description } : {}),
    ...(options.coerce ? { coerce: options.coerce } : {}),
  });

  const build = (type: SqlScalarType) =>
    ((arg1: unknown, arg2?: unknown, arg3?: unknown) => {
      if (isRelExpr(arg1)) {
        return buildCalculatedColumnDefinition(
          arg1,
          type,
          (arg2 as
            | Omit<
                SchemaTypedColumnBuilderOptions,
                "primaryKey" | "unique" | "enum" | "enumFrom" | "enumMap"
              >
            | undefined) ?? {},
        );
      }

      if (
        (isSchemaDslTableToken(arg1) || isDslTableDefinition(arg1) || isDslViewDefinition(arg1)) &&
        typeof arg2 === "string"
      ) {
        return buildSourceLensDefinition(
          {
            kind: "dsl_col_ref",
            table: toSchemaDslTableToken(arg1),
            column: arg2,
          },
          type,
          (arg3 as SchemaTypedColumnBuilderOptions | undefined) ?? {},
        );
      }

      if (isSchemaDataEntityHandle(arg1) && typeof arg2 === "string") {
        return buildSourceLensDefinition(
          {
            kind: "dsl_col_ref",
            entity: arg1,
            column: arg2,
          },
          type,
          (arg3 as SchemaTypedColumnBuilderOptions | undefined) ?? {},
        );
      }

      return buildTypedColumnDefinition(
        arg1 as TSourceColumns,
        type,
        (arg2 as SchemaTypedColumnBuilderOptions | undefined) ?? {},
      );
    }) as never;

  return {
    id: ((arg1: unknown, arg2?: unknown, arg3?: unknown) => {
      const options = (
        isRelExpr(arg1)
          ? arg2
          : isSchemaDslTableToken(arg1) ||
              isDslTableDefinition(arg1) ||
              isDslViewDefinition(arg1) ||
              isSchemaDataEntityHandle(arg1)
            ? arg3
            : arg2
      ) as SchemaTypedColumnBuilderOptions | undefined;
      return (build("text") as (...args: unknown[]) => unknown)(arg1, arg2, {
        ...options,
        nullable: false,
        primaryKey: true,
      });
    }) as never,
    string: build("text"),
    integer: build("integer"),
    real: build("real"),
    blob: build("blob"),
    boolean: build("boolean"),
    timestamp: build("timestamp"),
    date: build("date"),
    datetime: build("datetime"),
    json: build("json"),
  } as SchemaTypedColumnBuilder<TSourceColumns, TColumnMetadata>;
}

function buildCalculatedColumnDefinition(
  expr: RelExpr,
  type: SqlScalarType,
  options: Omit<
    SchemaTypedColumnBuilderOptions,
    "primaryKey" | "unique" | "enum" | "enumFrom" | "enumMap"
  > = {},
): SchemaCalculatedColumnDefinition {
  const definition = {
    type,
    ...(options.nullable != null ? { nullable: options.nullable } : {}),
    ...(options.physicalType ? { physicalType: options.physicalType } : {}),
    ...(options.physicalDialect ? { physicalDialect: options.physicalDialect } : {}),
    ...(options.foreignKey ? { foreignKey: options.foreignKey } : {}),
    ...(options.description ? { description: options.description } : {}),
  } satisfies ColumnDefinition;

  return {
    kind: "dsl_calculated_column",
    expr,
    definition,
    ...(options.coerce ? { coerce: options.coerce } : {}),
  };
}

function buildColumnExprHelpers(): SchemaColumnExprHelpers {
  const fn = (name: string, ...args: RelExpr[]): RelExpr => ({
    kind: "function",
    name,
    args,
  });

  return {
    literal(value) {
      return { kind: "literal", value };
    },
    eq(left, right) {
      return fn("eq", left, right);
    },
    neq(left, right) {
      return fn("neq", left, right);
    },
    gt(left, right) {
      return fn("gt", left, right);
    },
    gte(left, right) {
      return fn("gte", left, right);
    },
    lt(left, right) {
      return fn("lt", left, right);
    },
    lte(left, right) {
      return fn("lte", left, right);
    },
    add(left, right) {
      return fn("add", left, right);
    },
    subtract(left, right) {
      return fn("subtract", left, right);
    },
    multiply(left, right) {
      return fn("multiply", left, right);
    },
    divide(left, right) {
      return fn("divide", left, right);
    },
    and(...args) {
      return fn("and", ...args);
    },
    or(...args) {
      return fn("or", ...args);
    },
    not(input) {
      return fn("not", input);
    },
  };
}

function buildSchemaColumnsColHelper<
  TSourceColumns extends string,
  TColumnMetadata extends Partial<Record<TSourceColumns, DataEntityColumnMetadata<any>>> =
    DataEntityReadMetadataMap<TSourceColumns, Record<TSourceColumns, unknown>>,
>(): SchemaColumnsColHelper<TSourceColumns, TColumnMetadata> {
  return Object.assign(function col<TColumns extends string, TColumn extends TColumns>(
    tableOrRef: string | SchemaDslRelationRef<TColumns>,
    column?: TColumn,
  ): RelExpr {
    if (typeof tableOrRef === "string") {
      if (column != null) {
        throw new Error(
          "Schema DSL column expr col(ref) does not accept a second argument for string refs.",
        );
      }
      return {
        kind: "column",
        ref: { column: tableOrRef },
      };
    }

    if (column == null) {
      throw new Error("Schema DSL column expr col(table, column) requires a column name.");
    }

    return {
      kind: "column",
      ref: {
        table: toSchemaDslTableToken(tableOrRef) as unknown as string,
        column,
      },
    };
  }, buildTypedColumnBuilder<TSourceColumns, TColumnMetadata>()) as SchemaColumnsColHelper<
    TSourceColumns,
    TColumnMetadata
  >;
}

function buildSchemaDslViewRelHelpers(): SchemaDslViewRelHelpers {
  return {
    col<TColumns extends string, TColumn extends TColumns>(
      tableOrEntity: string | SchemaDataEntityHandle<TColumns> | SchemaDslRelationRef<TColumns>,
      column?: TColumn,
    ): SchemaColRefToken {
      if (typeof tableOrEntity === "string") {
        if (column != null) {
          throw new Error(
            "Schema DSL rel col(ref) does not accept a second argument for string refs.",
          );
        }
        return {
          kind: "dsl_col_ref",
          ref: tableOrEntity,
        } as const;
      }

      if (column == null) {
        throw new Error("Schema DSL rel col(table, column) requires a column name.");
      }

      if (isSchemaDataEntityHandle(tableOrEntity)) {
        return {
          kind: "dsl_col_ref",
          entity: tableOrEntity,
          column,
        } as const;
      }

      return {
        kind: "dsl_col_ref",
        table: toSchemaDslTableToken(tableOrEntity),
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
      count() {
        return {
          kind: "metric",
          fn: "count",
        };
      },
      countDistinct(column) {
        return {
          kind: "metric",
          fn: "count",
          column,
          distinct: true,
        };
      },
      sum(column) {
        return {
          kind: "metric",
          fn: "sum",
          column,
        };
      },
      sumDistinct(column) {
        return {
          kind: "metric",
          fn: "sum",
          column,
          distinct: true,
        };
      },
      avg(column) {
        return {
          kind: "metric",
          fn: "avg",
          column,
        };
      },
      avgDistinct(column) {
        return {
          kind: "metric",
          fn: "avg",
          column,
          distinct: true,
        };
      },
      min(column) {
        return {
          kind: "metric",
          fn: "min",
          column,
        };
      },
      max(column) {
        return {
          kind: "metric",
          fn: "max",
          column,
        };
      },
    },
    scan<TColumns extends string>(
      table: string | SchemaDataEntityHandle<TColumns> | SchemaDslRelationRef<TColumns>,
    ): SchemaViewScanNodeInput<TColumns> {
      return {
        kind: "scan",
        table:
          typeof table === "string" || isSchemaDataEntityHandle(table)
            ? table
            : toSchemaDslTableToken(table),
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

function isDslViewDefinition<TContext>(
  value: unknown,
): value is DslViewDefinition<TContext, string, string> {
  return (
    !!value &&
    typeof value === "object" &&
    (value as { kind?: unknown }).kind === "dsl_view" &&
    isSchemaDslTableToken((value as { tableToken?: unknown }).tableToken)
  );
}

function isSchemaTypedColumnDefinition(
  value: unknown,
): value is SchemaTypedColumnDefinition<string> {
  return (
    !!value &&
    typeof value === "object" &&
    (value as { kind?: unknown }).kind === "dsl_typed_column" &&
    typeof (value as { sourceColumn?: unknown }).sourceColumn === "string"
  );
}

function isSchemaCalculatedColumnDefinition(
  value: unknown,
): value is SchemaCalculatedColumnDefinition {
  return (
    !!value &&
    typeof value === "object" &&
    (value as { kind?: unknown }).kind === "dsl_calculated_column" &&
    isRelExpr((value as { expr?: unknown }).expr)
  );
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

function isRelExpr(value: unknown): value is RelExpr {
  if (!value || typeof value !== "object") {
    return false;
  }

  const kind = (value as { kind?: unknown }).kind;
  if (kind === "literal") {
    return true;
  }
  if (kind === "function") {
    return Array.isArray((value as { args?: unknown }).args);
  }
  if (kind === "column") {
    return (
      !!(value as { ref?: unknown }).ref && typeof (value as { ref?: unknown }).ref === "object"
    );
  }
  return false;
}

function isColumnLensDefinition(value: unknown): value is SchemaColumnLensDefinition {
  if (!value || typeof value !== "object") {
    return false;
  }

  const source = (value as { source?: unknown }).source;
  return typeof source === "string" || isSchemaColRefToken(source);
}
