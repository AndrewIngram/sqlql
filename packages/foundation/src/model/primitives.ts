/**
 * Primitive contracts define the schema-neutral table and query vocabulary shared by providers,
 * planner, and runtime. They carry logical semantics only and must not imply execution behavior.
 */
export type PhysicalDialect = "postgres" | "sqlite";

/** SQL scalar types are the logical scalar kinds recognized across tupl packages. */
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

declare const ISO_8601_TIMESTAMP_BRAND: unique symbol;

/** Branded timestamp strings mark values already normalized to ISO-8601 text. */
export type Iso8601TimestampString = string & {
  readonly [ISO_8601_TIMESTAMP_BRAND]: "Iso8601TimestampString";
};

/** Timestamp inputs accept a branded ISO string, a plain string, or a `Date`. */
export type TimestampValue = Iso8601TimestampString | string | Date;

/** `asIso8601Timestamp` normalizes `Date` values while preserving already-string inputs. */
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
  enumFrom?: unknown;
  enumMap?: Record<string, string>;
  physicalType?: string;
  physicalDialect?: PhysicalDialect;
  foreignKey?: ColumnForeignKeyReference;
  description?: string;
}

/**
 * Column definitions are the normalized logical schema form for a column.
 * Bare scalar types are shorthand for a column whose only required property is `type`.
 */
export type ColumnDefinition = ColumnDefinitionBase & ColumnConstraintFlags;
/** Table column definitions allow either shorthand scalar types or full logical column definitions. */
export type TableColumnDefinition = SqlScalarType | ColumnDefinition;
/** Table columns are keyed by logical output column name, not physical provider metadata shape. */
export type TableColumns = Record<string, TableColumnDefinition>;

/** Primary-key constraints declare the logical key columns for one table. */
export interface PrimaryKeyConstraint {
  columns: string[];
  name?: string;
}

/** Unique constraints declare alternative candidate keys on one table. */
export interface UniqueConstraint {
  columns: string[];
  name?: string;
}

/** Referential actions mirror downstream SQL referential-action vocabulary. */
export type ReferentialAction = "NO ACTION" | "RESTRICT" | "CASCADE" | "SET NULL" | "SET DEFAULT";

/** Column foreign-key references are the inline form used by individual column definitions. */
export interface ColumnForeignKeyReference {
  table: string;
  column: string;
  name?: string;
  onDelete?: ReferentialAction;
  onUpdate?: ReferentialAction;
}

/** Foreign-key constraints are the table-level form for multi-column or named references. */
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

/** `CheckConstraintIn` constrains one column to a fixed allow-list of scalar values. */
export interface CheckConstraintIn {
  kind: "in";
  column: string;
  values: readonly (string | number | boolean | null)[];
  name?: string;
}

/** Check constraints are intentionally narrow and currently model only the supported logical forms. */
export type CheckConstraint = CheckConstraintIn;

/** Table constraints gather the logical keys and integrity rules attached to one table definition. */
export interface TableConstraints {
  primaryKey?: PrimaryKeyConstraint;
  unique?: UniqueConstraint[];
  foreignKeys?: ForeignKeyConstraint[];
  checks?: CheckConstraint[];
}

/**
 * Table definitions are the logical schema boundary consumed by planner and runtime.
 * `provider` is optional here so schema construction can defer binding until normalization/finalization.
 */
export interface TableDefinition {
  provider?: string;
  columns: TableColumns;
  constraints?: TableConstraints;
}

/** Schema definitions are the top-level logical table map passed between schema, planner, and runtime. */
export interface SchemaDefinition {
  tables: Record<string, TableDefinition>;
}

/** Scan filter operators describe the pushdown-friendly predicate vocabulary for scans and lookups. */
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

/** Filter-clause base carries the shared column/operator identity for all scan predicates. */
export interface FilterClauseBase<TColumn extends string = string> {
  id?: string;
  column: TColumn;
  op: ScanFilterOperator;
}

/** Scalar filter clauses compare one column against a single scalar value. */
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

/** Set filter clauses compare one column against a collection of scalar values. */
export interface SetFilterClause<
  TColumn extends string = string,
  TValue = unknown,
> extends FilterClauseBase<TColumn> {
  op: "in" | "not_in";
  values: TValue[];
}

/** Null filter clauses make null-ness explicit rather than encoding it as a scalar comparison. */
export interface NullFilterClause<
  TColumn extends string = string,
> extends FilterClauseBase<TColumn> {
  op: "is_null" | "is_not_null";
}

/** Scan filter clauses are the complete predicate union accepted by logical scan requests. */
export type ScanFilterClause<TColumn extends string = string> =
  | ScalarFilterClause<TColumn>
  | SetFilterClause<TColumn>
  | NullFilterClause<TColumn>;

/** Scan order terms define the logical sort order for one selected column. */
export interface ScanOrderBy<TColumn extends string = string> {
  id?: string;
  column: TColumn;
  direction: "asc" | "desc";
}

/**
 * Table scan requests represent one provider-readable table read.
 * `select` lists the projected logical columns after filtering, ordering, and pagination.
 */
export interface TableScanRequest<TTable extends string = string, TColumn extends string = string> {
  table: TTable;
  alias?: string;
  select: TColumn[];
  where?: ScanFilterClause<TColumn>[];
  orderBy?: ScanOrderBy<TColumn>[];
  limit?: number;
  offset?: number;
}

/**
 * Table lookup requests represent batched equality lookups over one key column.
 * They may still carry residual filters, but their primary purpose is keyed row retrieval.
 */
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

/** Aggregate functions are the provider/runtime aggregate operations supported by tupl today. */
export type AggregateFunction = "count" | "sum" | "avg" | "min" | "max";

/** Aggregate metrics define one named aggregate output column in a grouped request. */
export interface TableAggregateMetric<TColumn extends string = string> {
  fn: AggregateFunction;
  column?: TColumn;
  as: string;
  distinct?: boolean;
}

/**
 * Table aggregate requests represent one grouped aggregate read against a single table.
 * They stay table-scoped; joins and view expansion are expressed in higher-level relational IR.
 */
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

/** Query rows are runtime/provider row maps keyed by logical output column name. */
export type QueryRow<
  _TSchema extends SchemaDefinition = never,
  _TTableName extends string = string,
> = Record<string, unknown>;
