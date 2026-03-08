import type { ConstraintValidationOptions } from "./constraints";
import { Result, type Result as BetterResult } from "better-result";
import {
  SqlqlDiagnosticError,
  SqlqlExecutionError,
  SqlqlGuardrailError,
  SqlqlRuntimeError,
  SqlqlTimeoutError,
  type SqlqlError,
  type SqlqlResult,
} from "./errors";
import {
  normalizeCapability,
  resolveTableProvider,
  unwrapProviderOperationResult,
  validateProviderBindingsResult,
  type ProviderAdapter,
  type ProviderCapabilityReport,
  type ProviderFragment,
  type ProvidersMap,
  type QueryFallbackPolicy,
  type SqlqlDiagnostic,
} from "./provider";
import { countRelNodes, type RelNode } from "./rel";
import { executeRelWithProvidersResult } from "./executor";
import {
  buildProviderFragmentForRelResult,
  expandRelViewsResult,
  lowerSqlToRelResult,
} from "./planning";
import {
  finalizeSchemaDefinition,
  getNormalizedTableBinding,
  isSchemaBuilder,
  mapProviderRowsToLogical,
  mapProviderRowsToRelOutput,
  resolveSchemaLinkedEnums,
} from "./schema";
import type { QueryRow, SchemaBuilder, SchemaDefinition } from "./schema";

export type { QueryFallbackPolicy, SqlqlDiagnostic } from "./provider";
export { SqlqlDiagnosticError } from "./errors";

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
  diagnostics?: SqlqlDiagnostic[];
}

export interface QueryExecutionPlan {
  steps: QueryExecutionPlanStep[];
  scopes?: QueryExecutionPlanScope[];
  diagnostics?: SqlqlDiagnostic[];
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
  diagnostics?: SqlqlDiagnostic[];
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
  diagnostics?: SqlqlDiagnostic[];
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
  queryResult(input: ExecutableSchemaQueryInput<TContext>): Promise<SqlqlResult<QueryRow[]>>;
  createSession(input: ExecutableSchemaSessionInput<TContext>): QuerySession;
  createSessionResult(input: ExecutableSchemaSessionInput<TContext>): SqlqlResult<QuerySession>;
  explain(input: ExecutableSchemaQueryInput<TContext>): ExplainResult;
}

interface ExecutableSchemaRuntime<TContext> {
  schema: SchemaDefinition;
  providers: ProvidersMap<TContext>;
}

interface QueryCapabilityResolution<TContext> {
  fragment: ProviderFragment | null;
  provider: ProviderAdapter<TContext> | null;
  report: ProviderCapabilityReport | null;
  diagnostics: SqlqlDiagnostic[];
}

function resolveGuardrails(overrides?: Partial<QueryGuardrails>): QueryGuardrails {
  return {
    maxPlannerNodes: overrides?.maxPlannerNodes ?? DEFAULT_QUERY_GUARDRAILS.maxPlannerNodes,
    maxExecutionRows: overrides?.maxExecutionRows ?? DEFAULT_QUERY_GUARDRAILS.maxExecutionRows,
    maxLookupKeysPerBatch:
      overrides?.maxLookupKeysPerBatch ?? DEFAULT_QUERY_GUARDRAILS.maxLookupKeysPerBatch,
    maxLookupBatches: overrides?.maxLookupBatches ?? DEFAULT_QUERY_GUARDRAILS.maxLookupBatches,
    timeoutMs: overrides?.timeoutMs ?? DEFAULT_QUERY_GUARDRAILS.timeoutMs,
  };
}

function resolveFallbackPolicy(
  queryPolicy?: QueryFallbackPolicy,
  providerPolicy?: QueryFallbackPolicy,
): Required<QueryFallbackPolicy> {
  return {
    ...DEFAULT_QUERY_FALLBACK_POLICY,
    ...providerPolicy,
    ...queryPolicy,
  };
}

function makeDiagnostic(
  code: string,
  severity: SqlqlDiagnostic["severity"],
  message: string,
  details?: Record<string, unknown>,
  diagnosticClass: SqlqlDiagnostic["class"] = "0A000",
): SqlqlDiagnostic {
  return {
    code,
    class: diagnosticClass,
    severity,
    message,
    ...(details ? { details } : {}),
  };
}

type QueryResult<T> = SqlqlResult<T>;

function toSqlqlRuntimeError(error: unknown, operation: string): SqlqlError {
  if (
    SqlqlDiagnosticError.is(error) ||
    SqlqlExecutionError.is(error) ||
    SqlqlGuardrailError.is(error) ||
    SqlqlTimeoutError.is(error) ||
    SqlqlRuntimeError.is(error)
  ) {
    return error;
  }

  if (error instanceof Error) {
    return new SqlqlRuntimeError({
      operation,
      message: error.message,
      cause: error,
    });
  }

  return new SqlqlRuntimeError({
    operation,
    message: String(error),
    cause: error,
  });
}

function unwrapQueryResult<T, E>(result: BetterResult<T, E>): T {
  if (Result.isOk(result)) {
    return result.value;
  }

  throw result.error;
}

function tryQueryStep<T>(operation: string, fn: () => T): QueryResult<T> {
  return Result.try({
    try: () => fn() as Awaited<T>,
    catch: (error) => toSqlqlRuntimeError(error, operation),
  }) as QueryResult<T>;
}

async function tryQueryStepAsync<T>(
  operation: string,
  fn: () => Promise<T>,
): Promise<QueryResult<T>> {
  return Result.tryPromise({
    try: fn,
    catch: (error) => toSqlqlRuntimeError(error, operation),
  });
}

function enforceExecutionRowLimitResult(
  rows: QueryRow[],
  guardrails: QueryGuardrails,
): QueryResult<QueryRow[]> {
  if (rows.length > guardrails.maxExecutionRows) {
    return Result.err(
      new SqlqlGuardrailError({
        guardrail: "maxExecutionRows",
        limit: guardrails.maxExecutionRows,
        actual: rows.length,
        message: `Query exceeded maxExecutionRows guardrail (${guardrails.maxExecutionRows}). Received ${rows.length} rows.`,
      }),
    );
  }

  return Result.ok(rows);
}

function isPromiseLike<T>(value: unknown): value is Promise<T> {
  return !!value && typeof value === "object" && "then" in value;
}

async function withTimeoutResult<T>(
  operation: string,
  promiseFactory: () => Promise<T>,
  timeoutMs: number,
): Promise<QueryResult<T>> {
  if (timeoutMs <= 0 || !Number.isFinite(timeoutMs)) {
    return tryQueryStepAsync(operation, promiseFactory);
  }

  let timer: ReturnType<typeof setTimeout> | undefined;
  const timeoutPromise = new Promise<never>((_, reject) => {
    timer = setTimeout(() => {
      reject(
        new SqlqlTimeoutError({
          operation,
          timeoutMs,
          message: `Query timed out after ${timeoutMs}ms.`,
        }),
      );
    }, timeoutMs);
  });

  try {
    return await tryQueryStepAsync(operation, () =>
      Promise.race([promiseFactory(), timeoutPromise]),
    );
  } finally {
    if (timer) {
      clearTimeout(timer);
    }
  }
}

function summarizeCapabilityReason(report: ProviderCapabilityReport | null): string {
  if (!report) {
    return "Provider pushdown is not available for this query shape.";
  }
  if (report.reason && report.reason.length > 0) {
    return report.reason;
  }
  if (report.missingAtoms && report.missingAtoms.length > 0) {
    return `Missing provider capability atoms: ${report.missingAtoms.join(", ")}.`;
  }
  return "Provider pushdown is not available for this query shape.";
}

function buildCapabilityDiagnostics<TContext>(
  provider: ProviderAdapter<TContext> | null,
  fragment: ProviderFragment | null,
  report: ProviderCapabilityReport | null,
  queryPolicy?: QueryFallbackPolicy,
): SqlqlDiagnostic[] {
  const diagnostics = [...(report?.diagnostics ?? [])];
  if (!provider || !fragment || !report || report.supported) {
    return diagnostics;
  }

  const policy = resolveFallbackPolicy(queryPolicy, provider.fallbackPolicy);
  const details: Record<string, unknown> = {
    provider: provider.name,
    fragment: fragment.kind,
  };
  if (report.routeFamily) {
    details.routeFamily = report.routeFamily;
  }
  if (report.requiredAtoms?.length) {
    details.requiredAtoms = report.requiredAtoms;
  }
  if (report.missingAtoms?.length) {
    details.missingAtoms = report.missingAtoms;
  }
  if (report.estimatedRows != null) {
    details.estimatedRows = report.estimatedRows;
  }
  if (report.estimatedCost != null) {
    details.estimatedCost = report.estimatedCost;
  }

  diagnostics.push(
    makeDiagnostic(
      policy.allowFallback ? "SQLQL_WARN_FALLBACK" : "SQLQL_ERR_FALLBACK",
      policy.allowFallback ? "warning" : "error",
      summarizeCapabilityReason(report),
      details,
      policy.allowFallback ? "0A000" : "42000",
    ),
  );

  return diagnostics;
}

async function resolveProviderCapabilityForRel<TContext>(
  input: QueryInput<TContext>,
  rel: RelNode,
): Promise<QueryResult<QueryCapabilityResolution<TContext>>> {
  const fragmentResult = buildProviderFragmentForRelResult(rel, input.schema, input.context);
  if (Result.isError(fragmentResult)) {
    return fragmentResult;
  }

  const fragment = fragmentResult.value;
  if (!fragment) {
    return Result.ok({
      fragment: null,
      provider: null,
      report: null,
      diagnostics: [],
    });
  }

  const provider = input.providers[fragment.provider] ?? null;
  if (!provider) {
    return Result.ok({
      fragment,
      provider: null,
      report: null,
      diagnostics: [],
    });
  }

  const capabilityResult = await tryQueryStepAsync("resolve provider capability", () =>
    Promise.resolve(provider.canExecute(fragment, input.context)),
  );
  if (Result.isError(capabilityResult)) {
    return capabilityResult;
  }

  const report = normalizeCapability(capabilityResult.value);
  return Result.ok({
    fragment,
    provider,
    report,
    diagnostics: buildCapabilityDiagnostics(provider, fragment, report, input.fallbackPolicy),
  });
}

function resolveSyncProviderCapabilityForRel<TContext>(
  input: QueryInput<TContext>,
  rel: RelNode,
): QueryResult<QueryCapabilityResolution<TContext> | null> {
  const fragmentResult = buildProviderFragmentForRelResult(rel, input.schema, input.context);
  if (Result.isError(fragmentResult)) {
    return fragmentResult;
  }

  const fragment = fragmentResult.value;
  if (!fragment) {
    return Result.ok({
      fragment: null,
      provider: null,
      report: null,
      diagnostics: [],
    });
  }

  const provider = input.providers[fragment.provider] ?? null;
  if (!provider) {
    return Result.ok({
      fragment,
      provider: null,
      report: null,
      diagnostics: [],
    });
  }

  const capabilityResult = tryQueryStep("resolve provider capability", () =>
    provider.canExecute(fragment, input.context),
  );
  if (Result.isError(capabilityResult)) {
    return capabilityResult;
  }

  const capability = capabilityResult.value;
  if (isPromiseLike(capability)) {
    return Result.ok(null);
  }

  const report = normalizeCapability(capability);
  return Result.ok({
    fragment,
    provider,
    report,
    diagnostics: buildCapabilityDiagnostics(provider, fragment, report, input.fallbackPolicy),
  });
}

function maybeRejectFallbackResult<TContext>(
  input: QueryInput<TContext>,
  resolution: QueryCapabilityResolution<TContext>,
): QueryResult<QueryCapabilityResolution<TContext>> {
  if (!resolution.provider || !resolution.report || resolution.report.supported) {
    return Result.ok(resolution);
  }

  const policy = resolveFallbackPolicy(input.fallbackPolicy, resolution.provider.fallbackPolicy);
  const exceedsEstimatedCost =
    policy.rejectOnEstimatedCost &&
    resolution.report.estimatedCost != null &&
    Number.isFinite(policy.maxJoinExpansionRisk) &&
    resolution.report.estimatedCost > policy.maxJoinExpansionRisk;

  if (!policy.allowFallback || policy.rejectOnMissingAtom || exceedsEstimatedCost) {
    const diagnostics =
      resolution.diagnostics.length > 0
        ? resolution.diagnostics
        : [
            makeDiagnostic(
              "SQLQL_ERR_FALLBACK",
              "error",
              summarizeCapabilityReason(resolution.report),
              {
                provider: resolution.provider.name,
                fragment: resolution.fragment?.kind,
                missingAtoms: resolution.report.missingAtoms,
              },
              "42000",
            ),
          ];

    return Result.err(
      new SqlqlDiagnosticError({
        message: summarizeCapabilityReason(resolution.report),
        diagnostics,
      }),
    );
  }

  return Result.ok(resolution);
}

function hasSqlNode(node: RelNode): boolean {
  switch (node.kind) {
    case "sql":
      return true;
    case "scan":
      return false;
    case "filter":
    case "project":
    case "aggregate":
    case "window":
    case "sort":
    case "limit_offset":
      return hasSqlNode(node.input);
    case "join":
    case "set_op":
      return hasSqlNode(node.left) || hasSqlNode(node.right);
    case "with":
      return node.ctes.some((cte) => hasSqlNode(cte.query)) || hasSqlNode(node.body);
  }
}

async function maybeExecuteWholeQueryFragmentResult<TContext>(
  input: QueryInput<TContext>,
  rel: RelNode,
): Promise<QueryResult<QueryRow[] | null>> {
  const resolutionResult = await resolveProviderCapabilityForRel(input, rel);
  if (Result.isError(resolutionResult)) {
    return resolutionResult;
  }

  const resolution = resolutionResult.value;
  if (!resolution.fragment || !resolution.provider || !resolution.report) {
    return Result.ok(null);
  }

  if (!resolution.report.supported) {
    const fallbackResult = maybeRejectFallbackResult(input, resolution);
    if (Result.isError(fallbackResult)) {
      return fallbackResult;
    }

    return Result.ok(null);
  }

  const compiled = unwrapProviderOperationResult(
    await resolution.provider.compile(resolution.fragment, input.context),
  );
  const executed = await resolution.provider.execute(compiled, input.context);
  const rows = unwrapProviderOperationResult(executed);

  if (resolution.fragment.kind === "rel") {
    return Result.ok(mapProviderRowsToRelOutput(rows, rel, input.schema));
  }

  if (resolution.fragment.kind === "scan" && rel.kind === "scan") {
    const binding = getNormalizedTableBinding(input.schema, rel.table);
    return Result.ok(
      mapProviderRowsToLogical(
        rows,
        rel.select,
        binding?.kind === "physical" ? binding : null,
        input.schema.tables[rel.table],
      ),
    );
  }

  return Result.ok(rows);
}

function enforcePlannerNodeLimitResult(
  plannerNodeCount: number,
  guardrails: QueryGuardrails,
): QueryResult<number> {
  if (plannerNodeCount > guardrails.maxPlannerNodes) {
    return Result.err(
      new SqlqlGuardrailError({
        guardrail: "maxPlannerNodes",
        limit: guardrails.maxPlannerNodes,
        actual: plannerNodeCount,
        message: `Query exceeded maxPlannerNodes guardrail (${guardrails.maxPlannerNodes}). Planned ${plannerNodeCount} nodes.`,
      }),
    );
  }

  return Result.ok(plannerNodeCount);
}

function setFailedStepState(
  state: QueryStepState,
  error: SqlqlError,
  endedAt: number,
): QueryStepState {
  return {
    ...state,
    status: "failed",
    endedAt,
    durationMs: endedAt - (state.startedAt ?? endedAt),
    error: error.message,
  };
}

function createProviderFragmentSession<TContext>(
  input: QuerySessionInput<TContext>,
  guardrails: QueryGuardrails,
  provider: ProviderAdapter<TContext>,
  providerName: string,
  fragment: ProviderFragment,
  rel: RelNode,
  diagnostics: SqlqlDiagnostic[] = [],
): QuerySession {
  let executed = false;
  let result: QueryRow[] | null = null;
  let eventDispatched = false;

  const stepId = "remote_fragment_1";
  const plan: QueryExecutionPlan = {
    steps: [
      {
        id: stepId,
        kind: "remote_fragment",
        dependsOn: [],
        summary: `Execute provider fragment (${providerName})`,
        phase: "fetch",
        operation: {
          name: "provider_fragment",
          details: {
            provider: providerName,
          },
        },
        request: {
          fragment: fragment.kind,
        },
        ...(diagnostics.length > 0 ? { diagnostics } : {}),
      },
    ],
    scopes: [
      {
        id: "scope_root",
        kind: "root",
        label: "Root query",
      },
    ],
    ...(diagnostics.length > 0 ? { diagnostics } : {}),
  };

  let state: QueryStepState = {
    id: stepId,
    kind: "remote_fragment",
    status: "ready",
    summary: `Execute provider fragment (${providerName})`,
    dependsOn: [],
    ...(diagnostics.length > 0 ? { diagnostics } : {}),
  };

  const runResult = async (): Promise<QueryResult<QueryRow[]>> => {
    if (executed) {
      return Result.ok(result ?? []);
    }

    executed = true;
    const startedAt = Date.now();
    state = {
      ...state,
      status: "running",
      startedAt,
    };

    const compiledResult = await tryQueryStepAsync("compile provider fragment", async () =>
      unwrapProviderOperationResult(
        await Promise.resolve(provider.compile(fragment, input.context)),
      ),
    );
    if (Result.isError(compiledResult)) {
      state = setFailedStepState(state, compiledResult.error, Date.now());
      return compiledResult;
    }

    const executeRowsResult = await withTimeoutResult(
      "execute provider fragment",
      async () =>
        unwrapProviderOperationResult(await provider.execute(compiledResult.value, input.context)),
      guardrails.timeoutMs,
    );
    if (Result.isError(executeRowsResult)) {
      state = setFailedStepState(state, executeRowsResult.error, Date.now());
      return executeRowsResult;
    }

    let rows = executeRowsResult.value;
    if (fragment.kind === "rel") {
      const mappedRowsResult = tryQueryStep("map provider rows to logical rel output rows", () =>
        mapProviderRowsToRelOutput(rows, rel, input.schema),
      );
      if (Result.isError(mappedRowsResult)) {
        state = setFailedStepState(state, mappedRowsResult.error, Date.now());
        return mappedRowsResult;
      }
      rows = mappedRowsResult.value;
    } else if (fragment.kind === "scan" && rel.kind === "scan") {
      const mappedRowsResult = tryQueryStep("map provider rows to logical rows", () => {
        const binding = getNormalizedTableBinding(input.schema, rel.table);
        return mapProviderRowsToLogical(
          rows,
          rel.select,
          binding?.kind === "physical" ? binding : null,
          input.schema.tables[rel.table],
        );
      });
      if (Result.isError(mappedRowsResult)) {
        state = setFailedStepState(state, mappedRowsResult.error, Date.now());
        return mappedRowsResult;
      }
      rows = mappedRowsResult.value;
    }

    const limitedRowsResult = enforceExecutionRowLimitResult(rows, guardrails);
    if (Result.isError(limitedRowsResult)) {
      state = setFailedStepState(state, limitedRowsResult.error, Date.now());
      return limitedRowsResult;
    }

    result = rows;

    const endedAt = Date.now();
    state = {
      ...state,
      status: "done",
      routeUsed: "provider_fragment",
      rowCount: rows.length,
      outputRowCount: rows.length,
      startedAt,
      endedAt,
      durationMs: endedAt - startedAt,
      ...(input.options?.captureRows === "full" ? { rows } : {}),
      ...(diagnostics.length > 0 ? { diagnostics } : {}),
    };

    return Result.ok(rows);
  };

  const run = async (): Promise<QueryRow[]> => {
    return unwrapQueryResult(await runResult());
  };

  return {
    getPlan: () => plan,
    next: async () => {
      await run();
      if (!eventDispatched) {
        eventDispatched = true;
        const event: QueryStepEvent = {
          id: stepId,
          kind: "remote_fragment",
          status: "done",
          summary: state.summary,
          dependsOn: [],
          executionIndex: 1,
          startedAt: state.startedAt ?? Date.now(),
          endedAt: state.endedAt ?? Date.now(),
          durationMs: state.durationMs ?? 0,
          routeUsed: "provider_fragment",
          ...(state.rowCount != null ? { rowCount: state.rowCount } : {}),
          ...(state.outputRowCount != null ? { outputRowCount: state.outputRowCount } : {}),
          ...(input.options?.captureRows === "full" ? { rows: result ?? [] } : {}),
          ...(diagnostics.length > 0 ? { diagnostics } : {}),
        };

        input.options?.onEvent?.(event);
        return event;
      }

      return {
        done: true as const,
        result: result ?? [],
      };
    },
    runToCompletion: async () => {
      const rows = await run();
      return rows;
    },
    getResult: () => result,
    getStepState: (id: string) => (id === stepId ? state : undefined),
  };
}

function createRelExecutionSession<TContext>(
  input: QuerySessionInput<TContext>,
  guardrails: QueryGuardrails,
  rel: RelNode,
  diagnostics: SqlqlDiagnostic[] = [],
): QuerySession {
  const plan = buildRelExecutionPlan(input, rel, diagnostics);
  const states = new Map<string, QueryStepState>(
    plan.steps.map((step) => [
      step.id,
      {
        id: step.id,
        kind: step.kind,
        status: "ready",
        summary: step.summary,
        dependsOn: step.dependsOn,
        ...(step.diagnostics ? { diagnostics: step.diagnostics } : {}),
      },
    ]),
  );
  const stepById = new Map(plan.steps.map((step) => [step.id, step]));
  const executionOrder = plan.steps.map((step) => step.id);
  const rootStepId = executionOrder[executionOrder.length - 1] ?? null;

  let executed = false;
  let result: QueryRow[] | null = null;
  let emittedEvents: QueryStepEvent[] = [];
  let emittedIndex = 0;

  const runResult = async (): Promise<QueryResult<QueryRow[]>> => {
    if (executed) {
      return Result.ok(result ?? []);
    }

    executed = true;
    const startedAt = Date.now();

    if (rootStepId) {
      const rootState = states.get(rootStepId);
      if (rootState) {
        states.set(rootStepId, {
          ...rootState,
          status: "running",
          startedAt,
        });
      }
    }

    const rowsResult = await withTimeoutResult(
      "execute relational query",
      () =>
        executeRelWithProvidersResult(rel, input.schema, input.providers, input.context, {
          maxExecutionRows: guardrails.maxExecutionRows,
          maxLookupKeysPerBatch: guardrails.maxLookupKeysPerBatch,
          maxLookupBatches: guardrails.maxLookupBatches,
        }).then(unwrapQueryResult),
      guardrails.timeoutMs,
    );
    if (Result.isError(rowsResult)) {
      const endedAt = Date.now();
      if (rootStepId) {
        const rootState = states.get(rootStepId);
        if (rootState) {
          states.set(rootStepId, setFailedStepState(rootState, rowsResult.error, endedAt));
        }
      }
      return rowsResult;
    }

    const limitedRowsResult = enforceExecutionRowLimitResult(rowsResult.value, guardrails);
    if (Result.isError(limitedRowsResult)) {
      const endedAt = Date.now();
      if (rootStepId) {
        const rootState = states.get(rootStepId);
        if (rootState) {
          states.set(rootStepId, setFailedStepState(rootState, limitedRowsResult.error, endedAt));
        }
      }
      return limitedRowsResult;
    }

    result = limitedRowsResult.value;
    const completedRows = result;

    const eventBuildResult = tryQueryStep("build session step events", () => {
      const endedAt = Date.now();
      const duration = Math.max(endedAt - startedAt, 1);
      const stepCount = Math.max(executionOrder.length, 1);
      return executionOrder.map((stepId, index) => {
        const step = stepById.get(stepId);
        if (!step) {
          throw new Error(`Unknown query step id: ${stepId}`);
        }
        const stepStartedAt = startedAt + Math.floor((duration * index) / stepCount);
        const stepEndedAt = startedAt + Math.floor((duration * (index + 1)) / stepCount);
        const stepDuration = Math.max(stepEndedAt - stepStartedAt, 0);
        const isRoot = stepId === rootStepId;
        const routeUsed = routeForStepKind(step.kind);

        const nextState: QueryStepState = {
          id: step.id,
          kind: step.kind,
          status: "done",
          summary: step.summary,
          dependsOn: step.dependsOn,
          executionIndex: index + 1,
          startedAt: stepStartedAt,
          endedAt: stepEndedAt,
          durationMs: stepDuration,
          ...(routeUsed ? { routeUsed } : {}),
          ...(isRoot
            ? { rowCount: completedRows.length, outputRowCount: completedRows.length }
            : {}),
          ...(isRoot && input.options?.captureRows === "full" ? { rows: completedRows } : {}),
          ...(step.diagnostics ? { diagnostics: step.diagnostics } : {}),
        };
        states.set(step.id, nextState);

        const event: QueryStepEvent = {
          id: step.id,
          kind: step.kind,
          status: "done",
          summary: step.summary,
          dependsOn: step.dependsOn,
          executionIndex: index + 1,
          startedAt: stepStartedAt,
          endedAt: stepEndedAt,
          durationMs: stepDuration,
          ...(routeUsed ? { routeUsed } : {}),
          ...(isRoot
            ? { rowCount: completedRows.length, outputRowCount: completedRows.length }
            : {}),
          ...(isRoot && input.options?.captureRows === "full" ? { rows: completedRows } : {}),
          ...(step.diagnostics ? { diagnostics: step.diagnostics } : {}),
        };
        return event;
      });
    });
    if (Result.isError(eventBuildResult)) {
      const endedAt = Date.now();
      if (rootStepId) {
        const rootState = states.get(rootStepId);
        if (rootState) {
          states.set(rootStepId, setFailedStepState(rootState, eventBuildResult.error, endedAt));
        }
      }
      return eventBuildResult;
    }

    emittedEvents = eventBuildResult.value;
    return Result.ok(result);
  };

  const run = async (): Promise<QueryRow[]> => {
    return unwrapQueryResult(await runResult());
  };

  return {
    getPlan: () => plan,
    next: async () => {
      await run();
      if (emittedIndex < emittedEvents.length) {
        const event = emittedEvents[emittedIndex];
        emittedIndex += 1;
        if (event) {
          input.options?.onEvent?.(event);
          return event;
        }
      }

      return {
        done: true as const,
        result: result ?? [],
      };
    },
    runToCompletion: async () => run(),
    getResult: () => result,
    getStepState: (id: string) => states.get(id),
  };
}

function buildRelExecutionPlan<TContext>(
  input: QuerySessionInput<TContext>,
  rel: RelNode,
  diagnostics: SqlqlDiagnostic[] = [],
): QueryExecutionPlan {
  let stepCounter = 0;
  const steps: QueryExecutionPlanStep[] = [];
  const scopes: QueryExecutionPlanScope[] = [
    {
      id: "scope_root",
      kind: "root",
      label: "Root query",
    },
  ];

  const nextId = (prefix: string): string => {
    stepCounter += 1;
    return `${prefix}_${stepCounter}`;
  };

  const visit = (node: RelNode, scopeId = "scope_root"): string => {
    const remoteFragmentStepId = tryPlanRemoteFragmentStep(node, scopeId);
    if (remoteFragmentStepId) {
      return remoteFragmentStepId;
    }

    switch (node.kind) {
      case "scan": {
        const id = nextId("scan");
        steps.push({
          id,
          kind: "scan",
          dependsOn: [],
          summary: `Scan ${node.alias ?? node.table} (${node.table})`,
          phase: "fetch",
          operation: {
            name: "scan",
            details: {
              table: node.table,
              alias: node.alias ?? node.table,
            },
          },
          request: {
            select: node.select,
            ...(node.where ? { where: node.where } : {}),
            ...(node.orderBy ? { orderBy: node.orderBy } : {}),
            ...(node.limit != null ? { limit: node.limit } : {}),
            ...(node.offset != null ? { offset: node.offset } : {}),
          },
          outputs: node.output.map((column) => column.name),
          sqlOrigin: "FROM",
          scopeId,
        });
        return id;
      }
      case "filter": {
        const inputId = visit(node.input, scopeId);
        const id = nextId("filter");
        steps.push({
          id,
          kind: "filter",
          dependsOn: [inputId],
          summary: "Apply filter predicates",
          phase: "transform",
          operation: {
            name: "filter",
            details: {
              clauseCount: node.where?.length ?? (node.expr ? 1 : 0),
            },
          },
          ...(node.where || node.expr
            ? {
                request: {
                  ...(node.where ? { where: node.where } : {}),
                  ...(node.expr ? { expr: node.expr } : {}),
                },
              }
            : {}),
          outputs: node.output.map((column) => column.name),
          sqlOrigin: "WHERE",
          scopeId,
        });
        return id;
      }
      case "project": {
        const inputId = visit(node.input, scopeId);
        const id = nextId("projection");
        steps.push({
          id,
          kind: "projection",
          dependsOn: [inputId],
          summary: "Project result rows",
          phase: "output",
          operation: {
            name: "project",
            details: {
              columnCount: node.columns.length,
            },
          },
          outputs: node.output.map((column) => column.name),
          sqlOrigin: "SELECT",
          scopeId,
        });
        return id;
      }
      case "join": {
        const lookupJoin = resolveSyncLookupJoinCandidate(node, input);
        if (lookupJoin) {
          const leftId = visit(node.left, scopeId);
          const id = nextId("lookup_join");
          steps.push({
            id,
            kind: "lookup_join",
            dependsOn: [leftId],
            summary: `Lookup join ${lookupJoin.leftTable}.${lookupJoin.leftKey} -> ${lookupJoin.rightTable}.${lookupJoin.rightKey}`,
            phase: "fetch",
            operation: {
              name: "lookup_join",
              details: {
                leftProvider: lookupJoin.leftProvider,
                rightProvider: lookupJoin.rightProvider,
                joinType: lookupJoin.joinType,
                on: `${lookupJoin.leftTable}.${lookupJoin.leftKey} = ${lookupJoin.rightTable}.${lookupJoin.rightKey}`,
              },
            },
            outputs: node.output.map((column) => column.name),
            sqlOrigin: "FROM",
            scopeId,
          });
          return id;
        }

        const leftId = visit(node.left, scopeId);
        const rightId = visit(node.right, scopeId);
        const id = nextId("join");
        steps.push({
          id,
          kind: "join",
          dependsOn: [leftId, rightId],
          summary: `${node.joinType.toUpperCase()} join`,
          phase: "transform",
          operation: {
            name: "join",
            details: {
              joinType: node.joinType,
              on: `${formatColumnRef(node.leftKey)} = ${formatColumnRef(node.rightKey)}`,
            },
          },
          outputs: node.output.map((column) => column.name),
          sqlOrigin: "FROM",
          scopeId,
        });
        return id;
      }
      case "aggregate": {
        const inputId = visit(node.input, scopeId);
        const id = nextId("aggregate");
        steps.push({
          id,
          kind: "aggregate",
          dependsOn: [inputId],
          summary: "Compute grouped aggregates",
          phase: "transform",
          operation: {
            name: "aggregate",
            details: {
              groupBy: node.groupBy.map(formatColumnRef),
              metrics: node.metrics.map((metric) => ({
                fn: metric.fn,
                as: metric.as,
                ...(metric.column ? { column: formatColumnRef(metric.column) } : {}),
              })),
            },
          },
          outputs: node.output.map((column) => column.name),
          sqlOrigin: "GROUP BY",
          scopeId,
        });
        return id;
      }
      case "window": {
        const inputId = visit(node.input, scopeId);
        const id = nextId("window");
        steps.push({
          id,
          kind: "window",
          dependsOn: [inputId],
          summary: "Compute window functions",
          phase: "transform",
          operation: {
            name: "window",
            details: {
              functions: node.functions.map((fn) => ({
                fn: fn.fn,
                as: fn.as,
                partitionBy: fn.partitionBy.map(formatColumnRef),
                orderBy: fn.orderBy.map((term) => ({
                  source: formatColumnRef(term.source),
                  direction: term.direction,
                })),
              })),
            },
          },
          outputs: node.output.map((column) => column.name),
          sqlOrigin: "SELECT",
          scopeId,
        });
        return id;
      }
      case "sort": {
        const inputId = visit(node.input, scopeId);
        const id = nextId("order");
        steps.push({
          id,
          kind: "order",
          dependsOn: [inputId],
          summary: "Order result rows",
          phase: "transform",
          operation: {
            name: "order",
            details: {
              orderBy: node.orderBy.map((term) => ({
                source: formatColumnRef(term.source),
                direction: term.direction,
              })),
            },
          },
          outputs: node.output.map((column) => column.name),
          sqlOrigin: "ORDER BY",
          scopeId,
        });
        return id;
      }
      case "limit_offset": {
        const inputId = visit(node.input, scopeId);
        const id = nextId("limit_offset");
        steps.push({
          id,
          kind: "limit_offset",
          dependsOn: [inputId],
          summary: "Apply LIMIT/OFFSET",
          phase: "output",
          operation: {
            name: "limit_offset",
            details: {
              ...(node.limit != null ? { limit: node.limit } : {}),
              ...(node.offset != null ? { offset: node.offset } : {}),
            },
          },
          outputs: node.output.map((column) => column.name),
          sqlOrigin: "ORDER BY",
          scopeId,
        });
        return id;
      }
      case "set_op": {
        const leftScopeId = `${nextId("scope_set_left")}`;
        const rightScopeId = `${nextId("scope_set_right")}`;
        scopes.push(
          {
            id: leftScopeId,
            kind: "set_op_branch",
            label: "Set operation left branch",
            parentId: scopeId,
          },
          {
            id: rightScopeId,
            kind: "set_op_branch",
            label: "Set operation right branch",
            parentId: scopeId,
          },
        );
        const leftInput = visit(node.left, leftScopeId);
        const rightInput = visit(node.right, rightScopeId);
        const leftStep = nextId("set_op_branch");
        const rightStep = nextId("set_op_branch");
        steps.push(
          {
            id: leftStep,
            kind: "set_op_branch",
            dependsOn: [leftInput],
            summary: "Set operation left branch",
            phase: "transform",
            operation: {
              name: "set_op_branch",
              details: {
                branch: "left",
              },
            },
            scopeId: leftScopeId,
          },
          {
            id: rightStep,
            kind: "set_op_branch",
            dependsOn: [rightInput],
            summary: "Set operation right branch",
            phase: "transform",
            operation: {
              name: "set_op_branch",
              details: {
                branch: "right",
              },
            },
            scopeId: rightScopeId,
          },
        );
        const id = nextId("projection");
        steps.push({
          id,
          kind: "projection",
          dependsOn: [leftStep, rightStep],
          summary: `Apply set operation (${node.op})`,
          phase: "output",
          operation: {
            name: "set_op",
            details: {
              op: node.op,
            },
          },
          outputs: node.output.map((column) => column.name),
          sqlOrigin: "SET_OP",
          scopeId,
        });
        return id;
      }
      case "with": {
        const cteStepIds: string[] = [];
        for (const cte of node.ctes) {
          const cteScopeId = nextId("scope_cte");
          scopes.push({
            id: cteScopeId,
            kind: "cte",
            label: `CTE ${cte.name}`,
            parentId: scopeId,
          });
          const cteInput = visit(cte.query, cteScopeId);
          const cteStepId = nextId("cte");
          steps.push({
            id: cteStepId,
            kind: "cte",
            dependsOn: [cteInput],
            summary: `CTE ${cte.name}`,
            phase: "transform",
            operation: {
              name: "cte",
              details: {
                name: cte.name,
              },
            },
            sqlOrigin: "WITH",
            scopeId: cteScopeId,
          });
          cteStepIds.push(cteStepId);
        }
        const bodyStepId = visit(node.body, scopeId);
        const id = nextId("projection");
        steps.push({
          id,
          kind: "projection",
          dependsOn: [...cteStepIds, bodyStepId],
          summary: "Finalize WITH query",
          phase: "output",
          operation: {
            name: "with",
            details: {
              cteCount: node.ctes.length,
            },
          },
          outputs: node.output.map((column) => column.name),
          sqlOrigin: "WITH",
          scopeId,
        });
        return id;
      }
      case "sql": {
        const id = nextId("remote_fragment");
        steps.push({
          id,
          kind: "remote_fragment",
          dependsOn: [],
          summary: "Execute SQL-shaped relational fragment",
          phase: "fetch",
          operation: {
            name: "provider_fragment",
            details: {
              fragment: "sql",
            },
          },
          request: {
            tables: node.tables,
          },
          sqlOrigin: "SELECT",
          scopeId,
        });
        return id;
      }
    }
  };

  const tryPlanRemoteFragmentStep = (node: RelNode, scopeId: string): string | null => {
    if (node.kind === "scan" || node.kind === "sql") {
      return null;
    }

    const resolutionResult = resolveSyncProviderCapabilityForRel(input, node);
    if (Result.isError(resolutionResult)) {
      return null;
    }

    const resolution = resolutionResult.value;
    if (
      !resolution ||
      !resolution.fragment ||
      !resolution.provider ||
      !resolution.report?.supported
    ) {
      return null;
    }

    const id = nextId("remote_fragment");
    steps.push({
      id,
      kind: "remote_fragment",
      dependsOn: [],
      summary: `Execute provider fragment (${resolution.fragment.provider})`,
      phase: "fetch",
      operation: {
        name: "provider_fragment",
        details: {
          provider: resolution.fragment.provider,
        },
      },
      request: {
        fragment: resolution.fragment.kind,
      },
      outputs: node.output.map((column) => column.name),
      sqlOrigin: "SELECT",
      scopeId,
      ...(resolution.diagnostics.length > 0 ? { diagnostics: resolution.diagnostics } : {}),
    });
    return id;
  };

  visit(rel, "scope_root");

  return {
    steps,
    scopes,
    ...(diagnostics.length > 0 ? { diagnostics } : {}),
  };
}

function resolveSyncLookupJoinCandidate<TContext>(
  join: Extract<RelNode, { kind: "join" }>,
  input: QuerySessionInput<TContext>,
): {
  leftProvider: string;
  rightProvider: string;
  leftTable: string;
  rightTable: string;
  leftKey: string;
  rightKey: string;
  joinType: "inner" | "left";
} | null {
  if (join.joinType !== "inner" && join.joinType !== "left") {
    return null;
  }

  const leftScan = findFirstScanForPlan(join.left);
  const rightScan = findFirstScanForPlan(join.right);
  if (!leftScan || !rightScan) {
    return null;
  }
  if (
    (!input.schema.tables[leftScan.table] && !leftScan.entity) ||
    (!input.schema.tables[rightScan.table] && !rightScan.entity)
  ) {
    return null;
  }

  const leftBinding = getNormalizedTableBinding(input.schema, leftScan.table);
  const rightBinding = getNormalizedTableBinding(input.schema, rightScan.table);
  if (leftBinding?.kind === "view" || rightBinding?.kind === "view") {
    return null;
  }

  const leftProvider =
    leftScan.entity?.provider ?? resolveTableProvider(input.schema, leftScan.table);
  const rightProvider =
    rightScan.entity?.provider ?? resolveTableProvider(input.schema, rightScan.table);
  if (leftProvider === rightProvider) {
    return null;
  }

  const rightAdapter = input.providers[rightProvider];
  if (!rightAdapter?.lookupMany) {
    return null;
  }

  const capability = rightAdapter.canExecute(
    {
      kind: "scan",
      provider: rightProvider,
      table: rightScan.entity?.entity ?? rightScan.table,
      request: {
        table: rightScan.entity?.entity ?? rightScan.table,
        select: rightScan.select,
      },
    },
    input.context,
  );
  if (isPromiseLike(capability)) {
    return null;
  }

  return {
    leftProvider,
    rightProvider,
    leftTable: leftScan.table,
    rightTable: rightScan.table,
    leftKey: join.leftKey.column,
    rightKey: join.rightKey.column,
    joinType: join.joinType,
  };
}

function findFirstScanForPlan(node: RelNode): Extract<RelNode, { kind: "scan" }> | null {
  switch (node.kind) {
    case "scan":
      return node;
    case "filter":
    case "project":
    case "aggregate":
    case "window":
    case "sort":
    case "limit_offset":
      return findFirstScanForPlan(node.input);
    case "join":
    case "set_op":
      return findFirstScanForPlan(node.left) ?? findFirstScanForPlan(node.right);
    case "with":
      return findFirstScanForPlan(node.body);
    case "sql":
      return null;
  }
}

function formatColumnRef(ref: { alias?: string; table?: string; column: string }): string {
  const prefix = ref.alias ?? ref.table;
  return prefix ? `${prefix}.${ref.column}` : ref.column;
}

function routeForStepKind(kind: QueryStepKind): QueryStepRoute | null {
  switch (kind) {
    case "scan":
      return "scan";
    case "lookup_join":
      return "lookup_join";
    case "remote_fragment":
      return "provider_fragment";
    default:
      return "local";
  }
}

function tryCreateSyncProviderFragmentSession<TContext>(
  input: QuerySessionInput<TContext>,
  guardrails: QueryGuardrails,
  rel: RelNode,
): QueryResult<QuerySession | null> {
  const resolutionResult = resolveSyncProviderCapabilityForRel(input, rel);
  if (Result.isError(resolutionResult)) {
    return resolutionResult;
  }

  const resolution = resolutionResult.value;
  if (!resolution || !resolution.fragment || !resolution.provider || !resolution.report) {
    return Result.ok(null);
  }

  if (!resolution.report.supported) {
    const fallbackResult = maybeRejectFallbackResult(input, resolution);
    if (Result.isError(fallbackResult)) {
      return fallbackResult;
    }
    return Result.ok(null);
  }

  return Result.ok(
    createProviderFragmentSession(
      input,
      guardrails,
      resolution.provider,
      resolution.fragment.provider,
      resolution.fragment,
      rel,
      resolution.diagnostics,
    ),
  );
}

function normalizeRuntimeSchema<TContext>(input: QueryInput<TContext>): QueryInput<TContext> {
  const schema = resolveSchemaLinkedEnums(input.schema);
  return {
    ...input,
    schema,
  };
}

function normalizeRuntimeSchemaResult<TContext>(
  input: QueryInput<TContext>,
): QueryResult<QueryInput<TContext>> {
  return Result.gen(function* () {
    const normalizedInput = yield* tryQueryStep("normalize runtime schema", () =>
      normalizeRuntimeSchema(input),
    );
    yield* validateProviderBindingsResult(normalizedInput.schema, normalizedInput.providers);
    return Result.ok(normalizedInput);
  });
}

function assertNoSqlNodesWithoutProviderFragmentResult(rel: RelNode): QueryResult<RelNode> {
  if (hasSqlNode(rel)) {
    return Result.err(
      new SqlqlRuntimeError({
        operation: "validate provider fragment execution shape",
        message:
          "Query lowered to a SQL-shaped relational node that cannot be executed by the provider runtime without provider rel pushdown.",
      }),
    );
  }

  return Result.ok(rel);
}

function resolveSyncProviderCapabilityForRelResult<TContext>(
  input: QueryInput<TContext>,
  rel: RelNode,
): QueryResult<QueryCapabilityResolution<TContext> | null> {
  return Result.gen(function* () {
    const resolution = yield* resolveSyncProviderCapabilityForRel(input, rel);
    if (resolution) {
      yield* maybeRejectFallbackResult(input, resolution);
    }
    return Result.ok(resolution);
  });
}

function createQuerySessionResult<TContext>(
  input: QuerySessionInput<TContext>,
): QueryResult<QuerySession> {
  const resolvedInputResult = normalizeRuntimeSchemaResult(input);
  if (Result.isError(resolvedInputResult)) {
    return resolvedInputResult;
  }

  const resolvedInput = resolvedInputResult.value;
  const guardrails = resolveGuardrails(input.queryGuardrails);
  const loweredResult = lowerSqlToRelResult(resolvedInput.sql, resolvedInput.schema);
  if (Result.isError(loweredResult)) {
    return loweredResult;
  }

  const plannerNodeCount = countRelNodes(loweredResult.value.rel);
  const plannerNodeCountResult = enforcePlannerNodeLimitResult(plannerNodeCount, guardrails);
  if (Result.isError(plannerNodeCountResult)) {
    return plannerNodeCountResult;
  }

  const expandedRelResult = expandRelViewsResult(
    loweredResult.value.rel,
    resolvedInput.schema,
    resolvedInput.context,
  );
  if (Result.isError(expandedRelResult)) {
    return expandedRelResult;
  }

  const expandedRel = expandedRelResult.value;
  const providerSessionResult = tryCreateSyncProviderFragmentSession(
    resolvedInput,
    guardrails,
    expandedRel,
  );
  if (Result.isError(providerSessionResult)) {
    return providerSessionResult;
  }
  if (providerSessionResult.value) {
    return Result.ok(providerSessionResult.value);
  }

  const capabilityResolutionResult = resolveSyncProviderCapabilityForRelResult(
    resolvedInput,
    expandedRel,
  );
  if (Result.isError(capabilityResolutionResult)) {
    return capabilityResolutionResult;
  }

  const executableRelResult = assertNoSqlNodesWithoutProviderFragmentResult(expandedRel);
  if (Result.isError(executableRelResult)) {
    return executableRelResult;
  }

  return tryQueryStep("create relational execution session", () =>
    createRelExecutionSession(
      resolvedInput,
      guardrails,
      executableRelResult.value,
      capabilityResolutionResult.value?.diagnostics ?? [],
    ),
  );
}

function createQuerySessionInternal<TContext>(input: QuerySessionInput<TContext>): QuerySession {
  return unwrapQueryResult(createQuerySessionResult(input));
}

async function queryInternalResult<TContext>(
  input: QueryInput<TContext>,
): Promise<QueryResult<QueryRow[]>> {
  return Result.gen(async function* () {
    const resolvedInput = yield* normalizeRuntimeSchemaResult(input);
    const guardrails = resolveGuardrails(input.queryGuardrails);
    const lowered = yield* lowerSqlToRelResult(resolvedInput.sql, resolvedInput.schema);
    const plannerNodeCount = countRelNodes(lowered.rel);

    yield* enforcePlannerNodeLimitResult(plannerNodeCount, guardrails);
    const expandedRel = yield* expandRelViewsResult(
      lowered.rel,
      resolvedInput.schema,
      resolvedInput.context,
    );
    const remoteRows = yield* Result.await(
      withTimeoutResult(
        "execute whole provider fragment",
        () => maybeExecuteWholeQueryFragmentResult(resolvedInput, expandedRel).then(unwrapQueryResult),
        guardrails.timeoutMs,
      ),
    );

    if (remoteRows) {
      return enforceExecutionRowLimitResult(remoteRows, guardrails);
    }

    const executableRel = yield* assertNoSqlNodesWithoutProviderFragmentResult(expandedRel);
    const rows = yield* Result.await(
      withTimeoutResult(
        "execute relational query",
        () =>
          executeRelWithProvidersResult(
            executableRel,
            resolvedInput.schema,
            resolvedInput.providers,
            resolvedInput.context,
            {
              maxExecutionRows: guardrails.maxExecutionRows,
              maxLookupKeysPerBatch: guardrails.maxLookupKeysPerBatch,
              maxLookupBatches: guardrails.maxLookupBatches,
            },
          ).then(unwrapQueryResult),
        guardrails.timeoutMs,
      ),
    );

    return enforceExecutionRowLimitResult(rows, guardrails);
  });
}

async function queryInternal<TContext>(input: QueryInput<TContext>): Promise<QueryRow[]> {
  return unwrapQueryResult(await queryInternalResult(input));
}

export interface ExplainResult {
  rel: RelNode;
  plannerNodeCount: number;
  guardrails: QueryGuardrails;
  diagnostics?: SqlqlDiagnostic[];
}

function explainInternal<TContext>(input: QueryInput<TContext>): ExplainResult {
  return unwrapQueryResult(explainInternalResult(input));
}

function explainInternalResult<TContext>(input: QueryInput<TContext>): QueryResult<ExplainResult> {
  return Result.gen(function* () {
    const resolvedInput = yield* normalizeRuntimeSchemaResult(input);
    const guardrails = resolveGuardrails(input.queryGuardrails);
    const lowered = yield* lowerSqlToRelResult(resolvedInput.sql, resolvedInput.schema);
    const capabilityResolution = yield* resolveSyncProviderCapabilityForRelResult(
      resolvedInput,
      lowered.rel,
    );

    return Result.ok({
      rel: lowered.rel,
      plannerNodeCount: countRelNodes(lowered.rel),
      guardrails,
      ...(capabilityResolution?.diagnostics.length
        ? { diagnostics: capabilityResolution.diagnostics }
        : {}),
    });
  });
}

function collectExecutableProvidersResult<TContext>(
  schema: SchemaDefinition,
): QueryResult<ProvidersMap<TContext>> {
  const providers: ProvidersMap<TContext> = {};

  for (const [tableName] of Object.entries(schema.tables)) {
    const binding = getNormalizedTableBinding(schema, tableName);
    if (!binding || binding.kind === "view") {
      continue;
    }

    const provider = binding.adapter as ProviderAdapter<TContext> | undefined;
    if (!provider) {
      return Result.err(
        new SqlqlRuntimeError({
          operation: "collect executable providers",
          message: `Table ${tableName} must be declared from a provider-owned entity via table(name, provider.entities.someTable, config).`,
        }),
      );
    }

    const existing = providers[provider.name];
    if (existing && existing !== provider) {
      return Result.err(
        new SqlqlRuntimeError({
          operation: "collect executable providers",
          message: `Duplicate provider name detected in executable schema: ${provider.name}.`,
        }),
      );
    }
    providers[provider.name] = provider;

    if (!binding.provider || binding.provider !== provider.name) {
      return Result.err(
        new SqlqlRuntimeError({
          operation: "collect executable providers",
          message: `Table ${tableName} is bound to provider ${binding.provider ?? "<missing>"}, but the attached adapter is named ${provider.name}.`,
        }),
      );
    }
  }

  return Result.ok(providers);
}

function createExecutableSchemaResultInternal<TContext, TSchema extends SchemaDefinition>(
  input: TSchema | SchemaBuilder<TContext>,
): SqlqlResult<ExecutableSchema<TContext, TSchema | SchemaDefinition>> {
  const schemaResult = tryQueryStep("create executable schema", () =>
    isSchemaBuilder<TContext>(input) ? input.build() : finalizeSchemaDefinition(input as TSchema),
  );
  if (Result.isError(schemaResult)) {
    return schemaResult as SqlqlResult<ExecutableSchema<TContext, TSchema | SchemaDefinition>>;
  }

  const schema = schemaResult.value;
  const providersResult = collectExecutableProvidersResult<TContext>(schema);
  if (Result.isError(providersResult)) {
    return providersResult as SqlqlResult<ExecutableSchema<TContext, TSchema | SchemaDefinition>>;
  }

  const providers = providersResult.value;
  const runtime: ExecutableSchemaRuntime<TContext> = {
    schema,
    providers,
  };

  return Result.ok({
    schema,
    query(input) {
      return queryInternal({
        schema: runtime.schema,
        providers: runtime.providers,
        ...input,
      });
    },
    queryResult(input) {
      return queryInternalResult({
        schema: runtime.schema,
        providers: runtime.providers,
        ...input,
      });
    },
    createSession(input) {
      return createQuerySessionInternal({
        schema: runtime.schema,
        providers: runtime.providers,
        ...input,
      });
    },
    createSessionResult(input) {
      return createQuerySessionResult({
        schema: runtime.schema,
        providers: runtime.providers,
        ...input,
      });
    },
    explain(input) {
      return explainInternal({
        schema: runtime.schema,
        providers: runtime.providers,
        ...input,
      });
    },
  });
}

export function createExecutableSchemaResult<TContext>(
  builder: SchemaBuilder<TContext>,
): SqlqlResult<ExecutableSchema<TContext, SchemaDefinition>>;
export function createExecutableSchemaResult<TContext, TSchema extends SchemaDefinition>(
  schema: TSchema,
): SqlqlResult<ExecutableSchema<TContext, TSchema>>;
export function createExecutableSchemaResult<TContext, TSchema extends SchemaDefinition>(
  input: TSchema | SchemaBuilder<TContext>,
): SqlqlResult<ExecutableSchema<TContext, TSchema | SchemaDefinition>> {
  return createExecutableSchemaResultInternal(input);
}

export function createExecutableSchema<TContext>(
  builder: SchemaBuilder<TContext>,
): ExecutableSchema<TContext, SchemaDefinition>;
export function createExecutableSchema<TContext, TSchema extends SchemaDefinition>(
  schema: TSchema,
): ExecutableSchema<TContext, TSchema>;
export function createExecutableSchema<TContext, TSchema extends SchemaDefinition>(
  input: TSchema | SchemaBuilder<TContext>,
): ExecutableSchema<TContext, TSchema | SchemaDefinition> {
  return unwrapQueryResult(createExecutableSchemaResultInternal(input));
}
