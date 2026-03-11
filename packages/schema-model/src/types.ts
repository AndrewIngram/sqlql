import type {
  DataEntityColumnMetadata,
  DataEntityHandle,
  DataEntityReadMetadataMap,
  RelExpr,
  RelNode,
} from "@tupl/foundation";
import type { ProviderAdapter } from "@tupl/provider-kit";

import type { TimestampValue } from "./timestamps";

/**
 * Schema types own the logical schema vocabulary, normalized bindings, and table-method contracts.
 */
export type PhysicalDialect = "postgres" | "sqlite";

export type SqlScalarType =
  | "text"
  | "integer"
  | "real"
  | "blob"
  | "boolean"
  | "timestamp"
  | "date"
  | "datetime"
  | "json";

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

export type SchemaDataEntityHandle<
  TColumns extends string = string,
  TRow extends Partial<Record<TColumns, unknown>> = Record<TColumns, unknown>,
  TColumnMetadata extends Partial<Record<TColumns, DataEntityColumnMetadata<any>>> =
    DataEntityReadMetadataMap<TColumns, TRow>,
> = DataEntityHandle<TColumns, TRow, TColumnMetadata>;

export type SchemaValueCoercionName = "isoTimestamp";
export type SchemaValueCoercion = SchemaValueCoercionName | ((value: unknown) => unknown);

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

export interface DslTableDefinition<
  TMappedColumns extends string = string,
  TSourceColumns extends string = string,
> {
  kind: "dsl_table";
  tableToken: SchemaDslTableToken<TMappedColumns>;
  from: SchemaDataEntityHandle<TSourceColumns>;
  columns: Record<TMappedColumns, DslTableColumnInput<TSourceColumns>>;
  constraints?: TableConstraints;
}

export interface DslViewDefinition<
  TContext,
  TColumns extends string = string,
  TRelColumns extends string = string,
> {
  kind: "dsl_view";
  tableToken: SchemaDslTableToken<TColumns>;
  rel: (
    context: TContext,
    helpers: SchemaDslViewRelHelpers,
  ) => SchemaViewRelNodeInput<TRelColumns> | RelNode;
  columns: Record<TColumns, DslViewColumnInput<TRelColumns>>;
  constraints?: TableConstraints;
}

type SchemaDslRelationRef<TColumns extends string> =
  | SchemaDslTableToken<TColumns>
  | DslTableDefinition<TColumns, string>
  | DslViewDefinition<any, TColumns, string>;

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
    ) => SchemaViewRelNodeInput<TRelColumns> | RelNode,
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
    rel: (context: TContext) => SchemaViewRelNodeInput<string> | RelNode,
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

export interface NormalizedPhysicalTableBinding {
  kind: "physical";
  provider?: string;
  entity: string;
  columnBindings: Record<string, NormalizedColumnBinding>;
  columnToSource: Record<string, string>;
  adapter?: ProviderAdapter<unknown>;
}

export interface NormalizedViewTableBinding<TContext = unknown> {
  kind: "view";
  rel: (context: TContext) => unknown;
  columnBindings: Record<string, NormalizedColumnBinding>;
  columnToSource: Record<string, string>;
}

export interface NormalizedSourceColumnBinding {
  kind: "source";
  source: string;
  definition?: TableColumnDefinition;
  coerce?: SchemaValueCoercion;
}

export interface NormalizedCalculatedColumnBinding {
  kind: "expr";
  expr: RelExpr;
  definition?: TableColumnDefinition;
  coerce?: SchemaValueCoercion;
}

export type NormalizedColumnBinding =
  | NormalizedSourceColumnBinding
  | NormalizedCalculatedColumnBinding;

export type NormalizedTableBinding<TContext = unknown> =
  | NormalizedPhysicalTableBinding
  | NormalizedViewTableBinding<TContext>;

export type TableName<TSchema extends SchemaDefinition> = Extract<keyof TSchema["tables"], string>;

export type TableColumnName<
  TSchema extends SchemaDefinition,
  TTableName extends TableName<TSchema>,
> = Extract<keyof TSchema["tables"][TTableName]["columns"], string>;

export type SqlTypeValue<TType extends SqlScalarType> = TType extends "integer"
  ? number
  : TType extends "real"
    ? number
    : TType extends "blob"
      ? Uint8Array
      : TType extends "boolean"
        ? boolean
        : TType extends "timestamp" | "date" | "datetime"
          ? TimestampValue
          : TType extends "json"
            ? unknown
            : string;

type ColumnEnumValue<TColumn extends ColumnDefinition> = TColumn extends {
  type: "text";
  enum: readonly string[];
}
  ? TColumn["enum"][number]
  : never;

type ColumnScalarValue<TColumn extends ColumnDefinition> = [ColumnEnumValue<TColumn>] extends [
  never,
]
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

export type ScanFilterOperator =
  | "eq"
  | "neq"
  | "gt"
  | "gte"
  | "lt"
  | "lte"
  | "in"
  | "not_in"
  | "like"
  | "not_like"
  | "is_distinct_from"
  | "is_not_distinct_from"
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
  op:
    | "eq"
    | "neq"
    | "gt"
    | "gte"
    | "lt"
    | "lte"
    | "like"
    | "not_like"
    | "is_distinct_from"
    | "is_not_distinct_from";
  value: TValue;
}

export interface SetFilterClause<
  TColumn extends string = string,
  TValue = unknown,
> extends FilterClauseBase<TColumn> {
  op: "in" | "not_in";
  values: TValue[];
}

export interface NullFilterClause<
  TColumn extends string = string,
> extends FilterClauseBase<TColumn> {
  op: "is_null" | "is_not_null";
}

type ColumnName<TColumns extends TableColumns> = Extract<keyof TColumns, string>;
type ColumnFilterValueForDefinition<TDefinition extends TableColumnDefinition> = [
  TDefinition,
] extends [TableColumnDefinition]
  ? TableColumnDefinition extends TDefinition
    ? unknown
    : NonNullable<ColumnValue<TDefinition>>
  : unknown;
type ColumnFilterValue<
  TColumns extends TableColumns,
  TColumn extends ColumnName<TColumns>,
> = ColumnFilterValueForDefinition<TColumns[TColumn]>;

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
  TSchema extends SchemaDefinition = never,
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
  scan(
    request: TableScanRequest<TTable, TColumn, TColumns>,
    context: TContext,
  ): Promise<QueryRow[]>;
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
