import { Result, type Result as BetterResult } from "better-result";

import type {
  DataEntityColumnMap,
  DataEntityColumnMetadata,
  DataEntityColumnMetadataRecord,
  DataEntityHandle,
  DataEntityReadMetadataMap,
  DataEntityShape,
  InferDataEntityShapeMetadata,
} from "@tupl-internal/foundation";
import {
  DATA_ENTITY_ADAPTER_BRAND,
  type QueryRow,
  type RelExpr,
  type RelNode,
  type ScanFilterClause,
  type TableAggregateRequest,
  type TableScanRequest,
  type TuplDiagnostic,
} from "@tupl-internal/foundation";
import type {
  DataEntityHandle as FoundationDataEntityHandle,
} from "@tupl-internal/foundation";

export type {
  DataEntityColumnMap,
  DataEntityColumnMetadata,
  DataEntityColumnMetadataRecord,
  DataEntityHandle,
  DataEntityReadMetadataMap,
  DataEntityShape,
  InferDataEntityShapeMetadata,
} from "@tupl-internal/foundation";
export type { TuplDiagnostic } from "@tupl-internal/foundation";

export const AdapterResult = Result;
export type AdapterResult<T, E = Error> = BetterResult<T, E>;
export type MaybePromise<T> = T | Promise<T>;
export type ProviderOperationResult<T, E = Error> = AdapterResult<T, E>;
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
  diagnostics?: TuplDiagnostic[];
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
  return handle[DATA_ENTITY_ADAPTER_BRAND] as ProviderAdapter<any> | undefined;
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
): DataEntityColumnMap<
  TColumns,
  Record<TColumns, unknown>,
  InferDataEntityShapeMetadata<TColumns, TShape>
> {
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
  alias?: string;
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

export interface ProviderAdapterBase<TContext = unknown> {
  name: string;
  routeFamilies?: ProviderRouteFamily[];
  capabilityAtoms?: ProviderCapabilityAtom[];
  fallbackPolicy?: QueryFallbackPolicy;
  canExecute(
    fragment: ProviderFragment,
    context: TContext,
  ): MaybePromise<boolean | ProviderCapabilityReport>;
  estimate?(fragment: ProviderFragment, context: TContext): MaybePromise<ProviderEstimate>;
  /**
   * Optional source-neutral physical entity handles owned by this adapter.
   */
  entities?: Record<string, FoundationDataEntityHandle<string>>;
}

export interface FragmentProviderAdapter<TContext = unknown>
  extends ProviderAdapterBase<TContext> {
  compile(
    fragment: ProviderFragment,
    context: TContext,
  ): MaybePromise<ProviderOperationResult<ProviderCompiledPlan>>;
  execute(
    plan: ProviderCompiledPlan,
    context: TContext,
  ): MaybePromise<ProviderOperationResult<QueryRow[]>>;
}

export interface LookupProviderAdapter<TContext = unknown> extends ProviderAdapterBase<TContext> {
  lookupMany(
    request: ProviderLookupManyRequest,
    context: TContext,
  ): MaybePromise<ProviderOperationResult<QueryRow[]>>;
}

export type ProviderAdapter<TContext = unknown> =
  | FragmentProviderAdapter<TContext>
  | LookupProviderAdapter<TContext>
  | (FragmentProviderAdapter<TContext> & LookupProviderAdapter<TContext>);

export type ProvidersMap<TContext = unknown> = Record<string, ProviderAdapter<TContext>>;
export type DataSourceAdapter<TContext = unknown> = ProviderAdapter<TContext>;

export function supportsFragmentExecution<TContext>(
  provider: ProviderAdapter<TContext>,
): provider is FragmentProviderAdapter<TContext> {
  return (
    "compile" in provider &&
    typeof provider.compile === "function" &&
    "execute" in provider &&
    typeof provider.execute === "function"
  );
}

export function supportsLookupMany<TContext>(
  provider: ProviderAdapter<TContext>,
): provider is LookupProviderAdapter<TContext> {
  return "lookupMany" in provider && typeof provider.lookupMany === "function";
}

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

export function inferRouteFamilyForFragment(fragment: ProviderFragment) {
  switch (fragment.kind) {
    case "scan":
      return "scan";
    case "aggregate":
      return "aggregate";
    case "rel":
      return hasAdvancedRelFeatures(fragment.rel) ? "rel-advanced" : "rel-core";
  }
}

export function collectCapabilityAtomsForFragment(
  fragment: ProviderFragment,
): ProviderCapabilityAtom[] {
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
      if (node.functions.some((fn) => fn.fn === "dense_rank" || fn.fn === "rank" || fn.fn === "row_number")) {
        atoms.add("window.rank_basic");
      }
      if (node.functions.some((fn) => fn.fn === "count" || fn.fn === "sum" || fn.fn === "avg" || fn.fn === "min" || fn.fn === "max")) {
        atoms.add("window.aggregate_default_frame");
      }
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
    case "subquery":
      atoms.add(
        expr.mode === "exists" ? "subquery.exists_uncorrelated" : "subquery.scalar_uncorrelated",
      );
      collectCapabilityAtomsForRel(expr.rel, atoms);
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
