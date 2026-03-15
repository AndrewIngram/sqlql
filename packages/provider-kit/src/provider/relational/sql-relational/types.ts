import type { QueryRow, RelExpr, RelNode } from "@tupl/foundation";

import type { RelationalProviderEntityConfig } from "../relational-adapter-types";
import type { TableScanRequest } from "../../contracts";
import type { ProviderLookupManyRequest } from "../../shapes/lookup-optimization";
import type { MaybePromise } from "../../operations";
import {
  UnsupportedRelationalPlanError,
  type RelationalRegularJoinStep,
  type RelationalScanBindingBase,
  type RelationalSetOpWrapper,
  type RelationalSingleQueryPlan,
  type RelationalWithBodyWrapper,
} from "../../shapes/relational-core";

export type SqlRelationalCompileStrategy = "basic" | "set_op" | "with";

export interface SqlRelationalCompiledPlan {
  strategy: SqlRelationalCompileStrategy;
  rel: RelNode;
}

export interface SqlRelationalResolvedEntity<TConfig = unknown> {
  entity: string;
  table: string;
  config: TConfig;
}

export interface SqlRelationalScanBinding<
  TResolvedEntity extends SqlRelationalResolvedEntity = SqlRelationalResolvedEntity,
> extends RelationalScanBindingBase {
  alias: string;
  entity: string;
  table: string;
  resolved: TResolvedEntity;
}

export interface SqlRelationalColumnSelection {
  kind: "column";
  output: string;
  source: { alias?: string; table?: string; column: string };
}

export interface SqlRelationalMetricSelection {
  kind: "metric";
  output: string;
  metric: Extract<RelNode, { kind: "aggregate" }>["metrics"][number];
}

export interface SqlRelationalExprSelection {
  kind: "expr";
  output: string;
  expr: RelExpr;
}

export type SqlRelationalSelection =
  | SqlRelationalColumnSelection
  | SqlRelationalMetricSelection
  | SqlRelationalExprSelection;

export interface SqlRelationalWindowSelection {
  kind: "window";
  output: string;
  window: Extract<RelNode, { kind: "window" }>["functions"][number];
}

export type SqlRelationalWithSelection =
  | SqlRelationalColumnSelection
  | SqlRelationalWindowSelection;

export interface SqlRelationalQualifiedOrderTerm {
  kind: "qualified";
  direction: "asc" | "desc";
  source: { alias?: string; table?: string; column: string };
}

export interface SqlRelationalOutputOrderTerm {
  kind: "output";
  direction: "asc" | "desc";
  column: string;
}

export type SqlRelationalOrderTerm = SqlRelationalQualifiedOrderTerm | SqlRelationalOutputOrderTerm;

/**
 * Query translation hooks own backend-specific query-builder lowering once provider-kit has chosen
 * a rel strategy and assembled the backend-neutral single-query shape.
 */
export interface SqlRelationalQueryTranslationBackend<
  TContext,
  TResolvedEntity extends SqlRelationalResolvedEntity,
  TBinding extends SqlRelationalScanBinding<TResolvedEntity>,
  TRuntime,
  TQuery,
> {
  createRootQuery(args: {
    runtime: TRuntime;
    root: TBinding;
    context: TContext;
    plan: RelationalSingleQueryPlan<TBinding>;
    selection: SqlRelationalSelection[];
  }): MaybePromise<TQuery>;
  applyRegularJoin(args: {
    query: TQuery;
    join: RelationalRegularJoinStep<TBinding>;
    aliases: Map<string, TBinding>;
    context: TContext;
    runtime: TRuntime;
  }): MaybePromise<TQuery>;
  applySemiJoin(args: {
    query: TQuery;
    leftKey: { alias: string; column: string };
    subquery: TQuery;
    aliases: Map<string, TBinding>;
    context: TContext;
    runtime: TRuntime;
  }): MaybePromise<TQuery>;
  applyWhereClause(args: {
    query: TQuery;
    clause: NonNullable<TableScanRequest["where"]>[number];
    plan: RelationalSingleQueryPlan<TBinding> | RelationalWithBodyWrapper;
    aliases: Map<string, TBinding>;
    context: TContext;
    runtime: TRuntime;
  }): TQuery;
  applySelection(args: {
    query: TQuery;
    plan: RelationalSingleQueryPlan<TBinding>;
    selection: SqlRelationalSelection[];
    aliases: Map<string, TBinding>;
    context: TContext;
    runtime: TRuntime;
  }): MaybePromise<TQuery>;
  applyGroupBy(args: {
    query: TQuery;
    groupBy: Extract<RelNode, { kind: "aggregate" }>["groupBy"];
    aliases: Map<string, TBinding>;
    context: TContext;
    runtime: TRuntime;
  }): MaybePromise<TQuery>;
  applyOrderBy(args: {
    query: TQuery;
    plan: RelationalSingleQueryPlan<TBinding> | RelationalSetOpWrapper | RelationalWithBodyWrapper;
    selection?: SqlRelationalSelection[] | SqlRelationalWithSelection[];
    orderBy: SqlRelationalOrderTerm[];
    aliases: Map<string, TBinding>;
    context: TContext;
    runtime: TRuntime;
  }): MaybePromise<TQuery>;
  applyLimit(args: {
    query: TQuery;
    limit: number;
    context: TContext;
    runtime: TRuntime;
  }): MaybePromise<TQuery>;
  applyOffset(args: {
    query: TQuery;
    offset: number;
    context: TContext;
    runtime: TRuntime;
  }): MaybePromise<TQuery>;
  applySetOp(args: {
    left: TQuery;
    right: TQuery;
    wrapper: RelationalSetOpWrapper;
    context: TContext;
    runtime: TRuntime;
  }): MaybePromise<TQuery>;
  buildWithQuery(args: {
    body: RelationalWithBodyWrapper;
    ctes: Array<{ name: string; query: TQuery }>;
    projection: SqlRelationalWithSelection[];
    orderBy: SqlRelationalOrderTerm[];
    context: TContext;
    runtime: TRuntime;
  }): MaybePromise<TQuery>;
  executeQuery(args: { query: TQuery; context: TContext; runtime: TRuntime }): Promise<QueryRow[]>;
}

export interface SqlRelationalSupportArgs<
  TContext,
  TEntities extends Record<string, RelationalProviderEntityConfig>,
  TResolvedEntity extends SqlRelationalResolvedEntity,
  TRuntime,
> {
  context: TContext;
  entities: TEntities;
  resolvedEntities: Record<string, TResolvedEntity>;
  rel: RelNode;
  strategy: SqlRelationalCompileStrategy | null;
  runtime: TRuntime;
}

export interface SqlRelationalScanArgs<
  TContext,
  TEntities extends Record<string, RelationalProviderEntityConfig>,
  TResolvedEntity extends SqlRelationalResolvedEntity,
  TRuntime,
> {
  context: TContext;
  entities: TEntities;
  resolvedEntities: Record<string, TResolvedEntity>;
  name: string;
  request: TableScanRequest;
  runtime: TRuntime;
}

export interface SqlRelationalLookupArgs<
  TContext,
  TEntities extends Record<string, RelationalProviderEntityConfig>,
  TResolvedEntity extends SqlRelationalResolvedEntity,
  TRuntime,
> {
  context: TContext;
  entities: TEntities;
  resolvedEntities: Record<string, TResolvedEntity>;
  name: string;
  request: ProviderLookupManyRequest;
  runtime: TRuntime;
}

export interface SqlRelationalEntityArgs<
  TEntities extends Record<string, RelationalProviderEntityConfig>,
  TEntityName extends Extract<keyof TEntities, string>,
> {
  config: TEntities[TEntityName];
  entity: TEntityName;
  name: string;
}

export class UnsupportedSqlRelationalPlanError extends UnsupportedRelationalPlanError {}
