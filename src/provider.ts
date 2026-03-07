import { Result, type Result as BetterResult } from "better-result";

import { SqlqlProviderBindingError, type SqlqlResult } from "./errors";
import {
  getNormalizedTableBinding,
} from "./schema";
import type {
  PhysicalDialect,
  QueryRow,
  ScanFilterClause,
  SchemaDefinition,
  SqlScalarType,
  TableAggregateRequest,
  TableScanRequest,
} from "./schema";
import type { RelNode } from "./rel";
import type { RelExpr } from "./rel";

export type MaybePromise<T> = T | Promise<T>;
export type ProviderOperationResult<T, E = Error> = BetterResult<T, E>;
export type ProviderRuntimeBinding<TContext, TValue> =
  | TValue
  | ((context: TContext) => MaybePromise<TValue>);

export type ProviderRouteFamily = "scan" | "lookup" | "aggregate" | "rel-core" | "rel-advanced";

export type ProviderCapabilityAtom =
  | "scan.project"
  | "scan.filter.basic"
  | "scan.filter.set_membership"
  | "scan.sort"
  | "scan.limit_offset"
  | "lookup.bulk"
  | "aggregate.group_by"
  | "aggregate.having"
  | "join.inner"
  | "join.left"
  | "join.right_full"
  | "set_op.union_all"
  | "set_op.union_distinct"
  | "set_op.intersect"
  | "set_op.except"
  | "cte.non_recursive"
  | "subquery.scalar_uncorrelated"
  | "subquery.exists_uncorrelated"
  | "subquery.in_uncorrelated"
  | "subquery.from"
  | "subquery.correlated"
  | "window.rank_basic"
  | "window.aggregate_default_frame"
  | "window.frame_explicit"
  | "window.navigation"
  | "expr.compare_basic"
  | "expr.like"
  | "expr.in_not_in"
  | "expr.null_distinct"
  | "expr.arithmetic"
  | "expr.case_simple"
  | "expr.case_searched"
  | "expr.string_basic"
  | "expr.numeric_basic"
  | "expr.cast_basic";

export type SqlqlDiagnosticSeverity = "error" | "warning" | "note";
export type SqlqlDiagnosticClass =
  | "0A000"
  | "22000"
  | "42000"
  | "54000"
  | "57000"
  | "58000";

export interface SqlqlDiagnostic {
  code: string;
  class: SqlqlDiagnosticClass;
  severity: SqlqlDiagnosticSeverity;
  message: string;
  details?: Record<string, unknown>;
}

export interface QueryFallbackPolicy {
  allowFallback?: boolean;
  warnOnFallback?: boolean;
  rejectOnMissingAtom?: boolean;
  rejectOnEstimatedCost?: boolean;
  maxLocalRows?: number;
  maxLookupFanout?: number;
  maxJoinExpansionRisk?: number;
}

export interface ProviderCapabilityReport {
  supported: boolean;
  reason?: string;
  notes?: string[];
  routeFamily?: ProviderRouteFamily;
  requiredAtoms?: ProviderCapabilityAtom[];
  missingAtoms?: ProviderCapabilityAtom[];
  diagnostics?: SqlqlDiagnostic[];
  estimatedRows?: number;
  estimatedCost?: number;
  fallbackAllowed?: boolean;
}

export interface ProviderEstimate {
  rows: number;
  cost: number;
}

export interface ProviderCompiledPlan {
  provider: string;
  kind: string;
  payload: unknown;
}

declare const DATA_ENTITY_COLUMNS_BRAND: unique symbol;
declare const DATA_ENTITY_ROW_BRAND: unique symbol;
const DATA_ENTITY_ADAPTER_BRAND = Symbol("sqlql.data_entity.adapter");

export interface DataEntityColumnMetadata<TRead = unknown> {
  source: string;
  type?: SqlScalarType;
  nullable?: boolean;
  primaryKey?: boolean;
  unique?: boolean;
  enum?: readonly string[];
  physicalType?: string;
  physicalDialect?: PhysicalDialect;
  readonly __read__?: TRead;
}

export type DataEntityShapeColumn = SqlScalarType | Omit<DataEntityColumnMetadata, "source">;
export type DataEntityShape<TColumns extends string = string> = Record<TColumns, DataEntityShapeColumn>;

type DataEntityColumnMetadataRecord<TColumns extends string = string> = Partial<
  Record<TColumns, DataEntityColumnMetadata<any>>
>;

export type DataEntityReadMetadataMap<
  TColumns extends string = string,
  TRow extends Partial<Record<TColumns, unknown>> = Record<TColumns, unknown>,
> = {
  [K in TColumns]: DataEntityColumnMetadata<K extends keyof TRow ? TRow[K] : unknown>;
};

export type InferDataEntityShapeMetadata<
  TColumns extends string,
  TShape extends DataEntityShape<TColumns>,
> = {
  [K in TColumns]: TShape[K] extends SqlScalarType
    ? DataEntityColumnMetadata<unknown> & {
        source: K;
        type: TShape[K];
      }
    : DataEntityColumnMetadata<unknown> & {
        source: K;
      } & Extract<TShape[K], Omit<DataEntityColumnMetadata, "source">>;
};

export type DataEntityColumnMap<
  TColumns extends string = string,
  TRow extends Partial<Record<TColumns, unknown>> = Record<TColumns, unknown>,
  TColumnMetadata extends DataEntityColumnMetadataRecord<TColumns> = DataEntityReadMetadataMap<
    TColumns,
    TRow
  >,
> = {
  [K in TColumns]: K extends keyof TColumnMetadata
    ? TColumnMetadata[K] & {
        readonly __read__?: K extends keyof TRow ? TRow[K] : unknown;
      }
    : DataEntityColumnMetadata<K extends keyof TRow ? TRow[K] : unknown>;
};

export interface DataEntityHandle<
  TColumns extends string = string,
  TRow extends Partial<Record<TColumns, unknown>> = Record<TColumns, unknown>,
  TColumnMetadata extends DataEntityColumnMetadataRecord<TColumns> = DataEntityReadMetadataMap<
    TColumns,
    TRow
  >,
> {
  kind: "data_entity";
  /**
   * Source-neutral entity identifier. This can represent a SQL table, an ES index,
   * a Redis keyspace abstraction, a Mongo collection, etc.
   */
  entity: string;
  /**
   * Logical provider name used for runtime routing.
   */
  provider: string;
  columns?: DataEntityColumnMap<TColumns, TRow, TColumnMetadata>;
  readonly __columns__?: TColumns;
  readonly [DATA_ENTITY_ROW_BRAND]?: TRow;
  readonly [DATA_ENTITY_COLUMNS_BRAND]?: TColumns;
  readonly [DATA_ENTITY_ADAPTER_BRAND]?: ProviderAdapter<any>;
}

export function createDataEntityHandle<
  TColumns extends string = string,
  TRow extends Partial<Record<TColumns, unknown>> = Record<TColumns, unknown>,
  TColumnMetadata extends DataEntityColumnMetadataRecord<TColumns> = DataEntityReadMetadataMap<
    TColumns,
    TRow
  >,
>(input: {
  entity: string;
  provider: string;
  columns?: DataEntityColumnMap<TColumns, TRow, TColumnMetadata>;
  adapter?: ProviderAdapter<any>;
}): DataEntityHandle<TColumns, TRow, TColumnMetadata> {
  const handle = {
    kind: "data_entity",
    entity: input.entity,
    provider: input.provider,
    ...(input.columns ? { columns: input.columns } : {}),
  } as DataEntityHandle<TColumns, TRow, TColumnMetadata>;

  if (input.adapter) {
    bindDataEntityHandleToAdapter(handle, input.adapter);
  }

  return handle;
}

export function bindDataEntityHandleToAdapter(
  handle: DataEntityHandle<string>,
  adapter: ProviderAdapter<any>,
): DataEntityHandle<string> {
  Object.defineProperty(handle, DATA_ENTITY_ADAPTER_BRAND, {
    value: adapter,
    enumerable: false,
    configurable: true,
    writable: false,
  });
  return handle;
}

export function getDataEntityAdapter(
  handle: DataEntityHandle<string>,
): ProviderAdapter<any> | undefined {
  return handle[DATA_ENTITY_ADAPTER_BRAND];
}

export function bindAdapterEntities<TContext, TAdapter extends ProviderAdapter<TContext>>(
  adapter: TAdapter,
): TAdapter {
  const entities = adapter.entities;
  if (!entities) {
    return adapter;
  }

  for (const [entityName, handle] of Object.entries(entities)) {
    if (!handle.provider || handle.provider.length === 0) {
      handle.provider = adapter.name;
    }
    if (!handle.entity || handle.entity.length === 0) {
      handle.entity = entityName;
    }
    bindDataEntityHandleToAdapter(handle, adapter);
  }

  return adapter;
}

export function isDataEntityHandle(value: unknown): value is DataEntityHandle<string> {
  return (
    !!value &&
    typeof value === "object" &&
    (value as { kind?: unknown }).kind === "data_entity" &&
    typeof (value as { entity?: unknown }).entity === "string" &&
    typeof (value as { provider?: unknown }).provider === "string"
  );
}

export function getDataEntityColumnMetadata(
  entity: DataEntityHandle<string>,
  column: string,
): DataEntityColumnMetadata | undefined {
  return entity.columns?.[column];
}

export function normalizeDataEntityShape<
  TColumns extends string,
  TShape extends DataEntityShape<TColumns>,
>(
  shape: TShape,
): DataEntityColumnMap<TColumns, Record<TColumns, unknown>, InferDataEntityShapeMetadata<TColumns, TShape>> {
  return Object.fromEntries(
    Object.entries(shape).map(([column, definition]) => [
      column,
      typeof definition === "string"
        ? {
            source: column,
            type: definition,
          }
        : {
            source: column,
            ...(definition as Omit<DataEntityColumnMetadata, "source">),
          },
    ]),
  ) as DataEntityColumnMap<
    TColumns,
    Record<TColumns, unknown>,
    InferDataEntityShapeMetadata<TColumns, TShape>
  >;
}

export interface ProviderLookupManyRequest {
  table: string;
  key: string;
  keys: unknown[];
  select: string[];
  where?: ScanFilterClause[];
}

export interface RelProviderFragment {
  kind: "rel";
  provider: string;
  rel: RelNode;
}

export interface ScanProviderFragment {
  kind: "scan";
  provider: string;
  table: string;
  request: TableScanRequest;
}

export interface AggregateProviderFragment {
  kind: "aggregate";
  provider: string;
  table: string;
  request: TableAggregateRequest;
}

export type ProviderFragment =
  | RelProviderFragment
  | ScanProviderFragment
  | AggregateProviderFragment;

export interface ProviderAdapter<TContext = unknown> {
  name: string;
  routeFamilies?: ProviderRouteFamily[];
  capabilityAtoms?: ProviderCapabilityAtom[];
  fallbackPolicy?: QueryFallbackPolicy;
  canExecute(fragment: ProviderFragment, context: TContext): MaybePromise<boolean | ProviderCapabilityReport>;
  compile(
    fragment: ProviderFragment,
    context: TContext,
  ): MaybePromise<ProviderOperationResult<ProviderCompiledPlan>>;
  execute(
    plan: ProviderCompiledPlan,
    context: TContext,
  ): MaybePromise<ProviderOperationResult<QueryRow[]>>;
  lookupMany?(
    request: ProviderLookupManyRequest,
    context: TContext,
  ): MaybePromise<ProviderOperationResult<QueryRow[]>>;
  estimate?(fragment: ProviderFragment, context: TContext): MaybePromise<ProviderEstimate>;
  /**
   * Optional source-neutral physical entity handles owned by this adapter.
   */
  entities?: Record<string, DataEntityHandle<string>>;
}

export type ProvidersMap<TContext = unknown> = Record<string, ProviderAdapter<TContext>>;
export type DataSourceAdapter<TContext = unknown> = ProviderAdapter<TContext>;

export function normalizeCapability(
  capability: boolean | ProviderCapabilityReport,
): ProviderCapabilityReport {
  if (typeof capability === "boolean") {
    return capability ? { supported: true } : { supported: false };
  }

  return capability;
}

export function unwrapProviderOperationResult<T, E>(outcome: ProviderOperationResult<T, E>): T {
  if (Result.isError(outcome)) {
    throw outcome.error;
  }

  return outcome.value;
}

export function inferRouteFamilyForFragment(fragment: ProviderFragment): ProviderRouteFamily {
  switch (fragment.kind) {
    case "scan":
      return "scan";
    case "aggregate":
      return "aggregate";
    case "rel":
      return hasAdvancedRelFeatures(fragment.rel) ? "rel-advanced" : "rel-core";
  }
}

export function collectCapabilityAtomsForFragment(fragment: ProviderFragment): ProviderCapabilityAtom[] {
  const atoms = new Set<ProviderCapabilityAtom>();

  switch (fragment.kind) {
    case "scan":
      atoms.add("scan.project");
      if ((fragment.request.where ?? []).length > 0) {
        for (const clause of fragment.request.where ?? []) {
          addFilterAtom(atoms, clause.op);
        }
      }
      if ((fragment.request.orderBy ?? []).length > 0) {
        atoms.add("scan.sort");
      }
      if (fragment.request.limit != null || fragment.request.offset != null) {
        atoms.add("scan.limit_offset");
      }
      return [...atoms];
    case "aggregate":
      atoms.add("aggregate.group_by");
      for (const clause of fragment.request.where ?? []) {
        addFilterAtom(atoms, clause.op);
      }
      return [...atoms];
    case "rel":
      collectCapabilityAtomsForRel(fragment.rel, atoms);
      return [...atoms];
  }
}

function collectCapabilityAtomsForRel(node: RelNode, atoms: Set<ProviderCapabilityAtom>): void {
  switch (node.kind) {
    case "scan":
      atoms.add("scan.project");
      for (const clause of node.where ?? []) {
        addFilterAtom(atoms, clause.op);
      }
      if ((node.orderBy ?? []).length > 0) {
        atoms.add("scan.sort");
      }
      if (node.limit != null || node.offset != null) {
        atoms.add("scan.limit_offset");
      }
      return;
    case "filter":
      for (const clause of node.where ?? []) {
        addFilterAtom(atoms, clause.op);
      }
      if (node.expr) {
        collectCapabilityAtomsForExpr(node.expr, atoms);
      }
      collectCapabilityAtomsForRel(node.input, atoms);
      return;
    case "project":
      for (const column of node.columns) {
        if ("expr" in column) {
          collectCapabilityAtomsForExpr(column.expr, atoms);
        }
      }
      collectCapabilityAtomsForRel(node.input, atoms);
      return;
    case "join":
      atoms.add(node.joinType === "inner" ? "join.inner" : "join.left");
      if (node.joinType === "right" || node.joinType === "full") {
        atoms.add("join.right_full");
      }
      collectCapabilityAtomsForRel(node.left, atoms);
      collectCapabilityAtomsForRel(node.right, atoms);
      return;
    case "aggregate":
      atoms.add("aggregate.group_by");
      collectCapabilityAtomsForRel(node.input, atoms);
      return;
    case "window":
      atoms.add("window.rank_basic");
      collectCapabilityAtomsForRel(node.input, atoms);
      return;
    case "sort":
      atoms.add("scan.sort");
      collectCapabilityAtomsForRel(node.input, atoms);
      return;
    case "limit_offset":
      atoms.add("scan.limit_offset");
      collectCapabilityAtomsForRel(node.input, atoms);
      return;
    case "set_op":
      atoms.add(
        node.op === "union_all"
          ? "set_op.union_all"
          : node.op === "union"
            ? "set_op.union_distinct"
            : node.op === "intersect"
              ? "set_op.intersect"
              : "set_op.except",
      );
      collectCapabilityAtomsForRel(node.left, atoms);
      collectCapabilityAtomsForRel(node.right, atoms);
      return;
    case "with":
      atoms.add("cte.non_recursive");
      for (const cte of node.ctes) {
        collectCapabilityAtomsForRel(cte.query, atoms);
      }
      collectCapabilityAtomsForRel(node.body, atoms);
      return;
    case "sql":
      return;
  }
}

function collectCapabilityAtomsForExpr(expr: RelExpr, atoms: Set<ProviderCapabilityAtom>): void {
  switch (expr.kind) {
    case "literal":
    case "column":
      return;
    case "function":
      switch (expr.name) {
        case "eq":
        case "neq":
        case "gt":
        case "gte":
        case "lt":
        case "lte":
        case "between":
          atoms.add("expr.compare_basic");
          break;
        case "like":
        case "not_like":
          atoms.add("expr.like");
          break;
        case "in":
        case "not_in":
          atoms.add("expr.in_not_in");
          break;
        case "is_distinct_from":
        case "is_not_distinct_from":
        case "is_null":
        case "is_not_null":
          atoms.add("expr.null_distinct");
          break;
        case "add":
        case "subtract":
        case "multiply":
        case "divide":
        case "mod":
          atoms.add("expr.arithmetic");
          break;
        case "concat":
        case "lower":
        case "upper":
        case "trim":
        case "length":
        case "substr":
        case "coalesce":
        case "nullif":
          atoms.add("expr.string_basic");
          break;
        case "abs":
        case "round":
          atoms.add("expr.numeric_basic");
          break;
        case "cast":
          atoms.add("expr.cast_basic");
          break;
        case "case":
          atoms.add("expr.case_searched");
          break;
      }
      for (const arg of expr.args) {
        collectCapabilityAtomsForExpr(arg, atoms);
      }
      return;
  }
}

function addFilterAtom(atoms: Set<ProviderCapabilityAtom>, op: ScanFilterClause["op"]): void {
  switch (op) {
    case "eq":
    case "neq":
    case "gt":
    case "gte":
    case "lt":
    case "lte":
      atoms.add("scan.filter.basic");
      atoms.add("expr.compare_basic");
      return;
    case "in":
    case "not_in":
      atoms.add("scan.filter.set_membership");
      atoms.add("expr.in_not_in");
      return;
    case "like":
    case "not_like":
      atoms.add("scan.filter.basic");
      atoms.add("expr.like");
      return;
    case "is_distinct_from":
    case "is_not_distinct_from":
      atoms.add("scan.filter.basic");
      atoms.add("expr.null_distinct");
      return;
    case "is_null":
    case "is_not_null":
      atoms.add("scan.filter.basic");
      atoms.add("expr.null_distinct");
      return;
  }
}

function hasAdvancedRelFeatures(node: RelNode): boolean {
  switch (node.kind) {
    case "window":
    case "with":
    case "set_op":
      return true;
    case "scan":
    case "sql":
      return false;
    case "filter":
    case "project":
    case "aggregate":
    case "sort":
    case "limit_offset":
      return hasAdvancedRelFeatures(node.input);
    case "join":
      return hasAdvancedRelFeatures(node.left) || hasAdvancedRelFeatures(node.right);
  }
}

export function resolveTableProvider(schema: SchemaDefinition, table: string): string {
  const normalized = getNormalizedTableBinding(schema, table);
  if (normalized?.kind === "physical" && normalized.provider) {
    return normalized.provider;
  }

  if (normalized?.kind === "view") {
    throw new Error(`View table ${table} does not have a direct provider binding.`);
  }

  const tableDefinition = schema.tables[table];
  if (!tableDefinition) {
    throw new Error(`Unknown table: ${table}`);
  }

  if (!tableDefinition.provider || tableDefinition.provider.length === 0) {
    throw new Error(`Table ${table} is missing required provider mapping.`);
  }

  return tableDefinition.provider;
}

export function validateProviderBindings<TContext>(
  schema: SchemaDefinition,
  providers: ProvidersMap<TContext>,
): void {
  const result = validateProviderBindingsResult(schema, providers);
  if (Result.isError(result)) {
    throw result.error;
  }
}

export function validateProviderBindingsResult<TContext>(
  schema: SchemaDefinition,
  providers: ProvidersMap<TContext>,
): SqlqlResult<void> {
  for (const tableName of Object.keys(schema.tables)) {
    const normalized = getNormalizedTableBinding(schema, tableName);
    if (normalized?.kind === "view") {
      continue;
    }

    const providerNameResult = Result.try({
      try: () =>
        normalized?.kind === "physical"
          ? normalized.provider ?? resolveTableProvider(schema, tableName)
          : resolveTableProvider(schema, tableName),
      catch: (error) =>
        new SqlqlProviderBindingError({
          table: tableName,
          message: error instanceof Error ? error.message : String(error),
          cause: error,
        }),
    });
    if (Result.isError(providerNameResult)) {
      return providerNameResult;
    }

    const providerName = providerNameResult.value;
    if (!providers[providerName]) {
      return Result.err(
        new SqlqlProviderBindingError({
          table: tableName,
          provider: providerName,
          message: `Table ${tableName} is bound to provider ${providerName}, but no such provider is registered.`,
        }),
      );
    }
  }

  return Result.ok(undefined);
}
