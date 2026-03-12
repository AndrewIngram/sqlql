import type { TuplDiagnostic } from "@tupl/provider-kit";
import type { TuplResult } from "@tupl/foundation";

import type { QueryRow } from "@tupl/schema-model";

import type { ExecutableSchemaQueryInput, QueryInput } from "../contracts";

export type { TuplDiagnostic } from "@tupl/provider-kit";

/** Query step kinds are the public-facing execution categories surfaced in plan and session output. */
export type QueryStepKind =
  | "cte"
  | "set_op_branch"
  | "scan"
  | "filter"
  | "join"
  | "aggregate"
  | "window"
  | "distinct"
  | "order"
  | "limit_offset"
  | "projection"
  | "remote_fragment"
  | "lookup_join";

/** Query step phase captures where a step sits in the runtime pipeline. */
export type QueryStepPhase = "logical" | "fetch" | "transform" | "output";

/** Query SQL origin records which SQL clause contributed a step or plan fragment. */
export type QuerySqlOrigin =
  | "SELECT"
  | "FROM"
  | "WHERE"
  | "GROUP BY"
  | "HAVING"
  | "ORDER BY"
  | "WITH"
  | "SET_OP";

/** Query step route identifies the execution family actually used for a step. */
export type QueryStepRoute =
  | "scan"
  | "lookup"
  | "aggregate"
  | "local"
  | "provider_fragment"
  | "lookup_join";

/**
 * Query step operation is the stable operation label surfaced to plan consumers.
 * Details are diagnostic-only and must not be treated as a stable schema.
 */
export interface QueryStepOperation {
  name: string;
  details?: Record<string, unknown>;
}

/** Query plan scope kinds partition steps into the root query, CTEs, subqueries, and set-op branches. */
export type QueryPlanScopeKind = "root" | "cte" | "subquery" | "set_op_branch";

/**
 * Query execution plan scopes provide the hierarchical grouping shown by explain and sessions.
 * They organize related steps but do not impose execution ordering by themselves.
 */
export interface QueryExecutionPlanScope {
  id: string;
  kind: QueryPlanScopeKind;
  label: string;
  parentId?: string;
}

/**
 * Query execution plan steps describe planned work before execution starts.
 * Request, pushdown, and outputs are explanatory fields rather than a stable provider protocol.
 */
export interface QueryExecutionPlanStep {
  id: string;
  kind: QueryStepKind;
  dependsOn: string[];
  summary: string;
  phase: QueryStepPhase;
  operation: QueryStepOperation;
  request?: Record<string, unknown>;
  pushdown?: Record<string, unknown>;
  outputs?: string[];
  sqlOrigin?: QuerySqlOrigin;
  scopeId?: string;
  diagnostics?: TuplDiagnostic[];
}

/**
 * Query execution plans are the static plan/explain view of a query.
 * They are stable enough for human inspection, not a promise of exact internal scheduler behavior.
 */
export interface QueryExecutionPlan {
  steps: QueryExecutionPlanStep[];
  scopes?: QueryExecutionPlanScope[];
  diagnostics?: TuplDiagnostic[];
}

/** Query step status is the mutable lifecycle state tracked by a running session. */
export type QueryStepStatus = "ready" | "running" | "done" | "failed";

/**
 * Query step state is the latest known runtime state for a single plan step.
 * It is query-session stateful data and may include transient rows when capture is enabled.
 */
export interface QueryStepState {
  id: string;
  kind: QueryStepKind;
  status: QueryStepStatus;
  summary: string;
  dependsOn: string[];
  executionIndex?: number;
  startedAt?: number;
  endedAt?: number;
  durationMs?: number;
  rowCount?: number;
  inputRowCount?: number;
  outputRowCount?: number;
  rows?: QueryRow[];
  routeUsed?: QueryStepRoute;
  notes?: string[];
  error?: string;
  diagnostics?: TuplDiagnostic[];
}

/**
 * Query step events are the immutable completion/failure records emitted by a session.
 * Unlike step state they never represent an in-progress step and always include timing data.
 */
export interface QueryStepEvent {
  id: string;
  kind: QueryStepKind;
  status: "done" | "failed";
  summary: string;
  dependsOn: string[];
  executionIndex: number;
  startedAt: number;
  endedAt: number;
  durationMs: number;
  rowCount?: number;
  inputRowCount?: number;
  outputRowCount?: number;
  rows?: QueryRow[];
  routeUsed?: QueryStepRoute;
  notes?: string[];
  error?: string;
  diagnostics?: TuplDiagnostic[];
}

/**
 * Query session options control how much execution detail is surfaced while a session runs.
 * They do not change planning semantics or provider capability decisions.
 */
export interface QuerySessionOptions {
  /** Maximum number of concurrently runnable steps the session may execute. */
  maxConcurrency?: number;
  /** Event order is currently plan order only, even when runtime execution overlaps internally. */
  eventOrder?: "plan";
  /** Captured rows are opt-in because they can materially increase session memory use. */
  captureRows?: "full";
  /** Optional callback invoked for each emitted step event. */
  onEvent?: (event: QueryStepEvent) => void;
}

/** Query session input extends a query request with execution-observer options. */
export interface QuerySessionInput<TContext> extends QueryInput<TContext> {
  options?: QuerySessionOptions;
}

/** Executable-schema session input is the session-producing variant of executable query input. */
export interface ExecutableSchemaSessionInput<
  TContext,
> extends ExecutableSchemaQueryInput<TContext> {
  options?: QuerySessionOptions;
}

/**
 * Query sessions provide pull-based observation over query execution.
 * `next()` yields completed step events first and returns the final row set only once the session is done.
 */
export interface QuerySession {
  getPlan(): QueryExecutionPlan;
  next(): Promise<QueryStepEvent | { done: true; result: QueryRow[] }>;
  runToCompletion(): Promise<QueryRow[]>;
  getResult(): QueryRow[] | null;
  getStepState(stepId: string): QueryStepState | undefined;
}

export type QuerySessionResult = TuplResult<QuerySession>;
