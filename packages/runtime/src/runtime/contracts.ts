import type { ProvidersMap, QueryFallbackPolicy, TuplDiagnostic } from "@tupl/provider-kit";
import type { RelNode, TuplResult } from "@tupl/foundation";

import type { ConstraintValidationOptions } from "./constraints";
import type { QueryRow, SchemaDefinition } from "@tupl/schema-model";

export type { QueryFallbackPolicy, TuplDiagnostic } from "@tupl/provider-kit";

/**
 * Runtime contracts define the public query/session surface exposed by the runtime package.
 */
export interface QueryGuardrails {
  maxPlannerNodes: number;
  maxExecutionRows: number;
  maxLookupKeysPerBatch: number;
  maxLookupBatches: number;
  timeoutMs: number;
}

export const DEFAULT_QUERY_GUARDRAILS: QueryGuardrails = {
  maxPlannerNodes: 50_000,
  maxExecutionRows: 1_000_000,
  maxLookupKeysPerBatch: 1000,
  maxLookupBatches: 100,
  timeoutMs: 30_000,
};

export const DEFAULT_QUERY_FALLBACK_POLICY: Required<QueryFallbackPolicy> = {
  allowFallback: true,
  warnOnFallback: true,
  rejectOnMissingAtom: false,
  rejectOnEstimatedCost: false,
  maxLocalRows: Number.POSITIVE_INFINITY,
  maxLookupFanout: Number.POSITIVE_INFINITY,
  maxJoinExpansionRisk: Number.POSITIVE_INFINITY,
};

export interface QueryInput<TContext> {
  schema: SchemaDefinition;
  providers: ProvidersMap<TContext>;
  context: TContext;
  sql: string;
  queryGuardrails?: Partial<QueryGuardrails>;
  fallbackPolicy?: QueryFallbackPolicy;
  constraintValidation?: ConstraintValidationOptions;
}

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

export type QueryStepPhase = "logical" | "fetch" | "transform" | "output";
export type QuerySqlOrigin =
  | "SELECT"
  | "FROM"
  | "WHERE"
  | "GROUP BY"
  | "HAVING"
  | "ORDER BY"
  | "WITH"
  | "SET_OP";

export type QueryStepRoute =
  | "scan"
  | "lookup"
  | "aggregate"
  | "local"
  | "provider_fragment"
  | "lookup_join";

export interface QueryStepOperation {
  name: string;
  details?: Record<string, unknown>;
}

export type QueryPlanScopeKind = "root" | "cte" | "subquery" | "set_op_branch";

export interface QueryExecutionPlanScope {
  id: string;
  kind: QueryPlanScopeKind;
  label: string;
  parentId?: string;
}

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

export interface QueryExecutionPlan {
  steps: QueryExecutionPlanStep[];
  scopes?: QueryExecutionPlanScope[];
  diagnostics?: TuplDiagnostic[];
}

export type QueryStepStatus = "ready" | "running" | "done" | "failed";

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

export interface QuerySessionOptions {
  maxConcurrency?: number;
  eventOrder?: "plan";
  captureRows?: "full";
  onEvent?: (event: QueryStepEvent) => void;
}

export interface QuerySessionInput<TContext> extends QueryInput<TContext> {
  options?: QuerySessionOptions;
}

export interface ExecutableSchemaQueryInput<TContext> {
  context: TContext;
  sql: string;
  queryGuardrails?: Partial<QueryGuardrails>;
  fallbackPolicy?: QueryFallbackPolicy;
  constraintValidation?: ConstraintValidationOptions;
}

export interface ExecutableSchemaSessionInput<
  TContext,
> extends ExecutableSchemaQueryInput<TContext> {
  options?: QuerySessionOptions;
}

export interface QuerySession {
  getPlan(): QueryExecutionPlan;
  next(): Promise<QueryStepEvent | { done: true; result: QueryRow[] }>;
  runToCompletion(): Promise<QueryRow[]>;
  getResult(): QueryRow[] | null;
  getStepState(stepId: string): QueryStepState | undefined;
}

export interface ExecutableSchema<TContext, TSchema extends SchemaDefinition = SchemaDefinition> {
  schema: TSchema;
  query(input: ExecutableSchemaQueryInput<TContext>): Promise<QueryRow[]>;
  queryResult(input: ExecutableSchemaQueryInput<TContext>): Promise<TuplResult<QueryRow[]>>;
  createSession(input: ExecutableSchemaSessionInput<TContext>): QuerySession;
  createSessionResult(input: ExecutableSchemaSessionInput<TContext>): TuplResult<QuerySession>;
  explain(input: ExecutableSchemaQueryInput<TContext>): ExplainResult;
}

export interface ExplainResult {
  rel: RelNode;
  plannerNodeCount: number;
  guardrails: QueryGuardrails;
  diagnostics?: TuplDiagnostic[];
}

export interface ExecutableSchemaRuntime<TContext> {
  schema: SchemaDefinition;
  providers: ProvidersMap<TContext>;
}
