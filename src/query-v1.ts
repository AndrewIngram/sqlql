import type { ConstraintValidationOptions } from "./constraints";
import {
  normalizeCapability,
  validateProviderBindings,
  type ProviderAdapter,
  type ProviderCompiledPlan,
  type ProviderCapabilityReport,
  type ProviderFragment,
  type ProvidersMap,
  type QueryFallbackPolicy,
  type SqlqlDiagnostic,
} from "./provider";
import { countRelNodes, type RelNode } from "./rel";
import { executeRelWithProviders } from "./executor";
import { buildProviderFragmentForRel, expandRelViews, lowerSqlToRel } from "./planning";
import {
  defineSchema,
  getNormalizedTableBinding,
  mapProviderRowsToLogical,
  resolveSchemaLinkedEnums,
} from "./schema";
import type { QueryRow, SchemaDefinition, SchemaDslDefinition, SchemaDslHelpers } from "./schema";

export type { QueryFallbackPolicy, SqlqlDiagnostic } from "./provider";

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

export interface ExecutableSchemaSessionInput<TContext> extends ExecutableSchemaQueryInput<TContext> {
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
  createSession(input: ExecutableSchemaSessionInput<TContext>): QuerySession;
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

export class SqlqlDiagnosticError extends Error {
  readonly diagnostics: SqlqlDiagnostic[];

  constructor(message: string, diagnostics: SqlqlDiagnostic[]) {
    super(message);
    this.name = "SqlqlDiagnosticError";
    this.diagnostics = diagnostics;
  }
}

function enforceExecutionRowLimit(rows: QueryRow[], guardrails: QueryGuardrails): void {
  if (rows.length > guardrails.maxExecutionRows) {
    throw new Error(
      `Query exceeded maxExecutionRows guardrail (${guardrails.maxExecutionRows}). Received ${rows.length} rows.`,
    );
  }
}

function isPromiseLike<T>(value: unknown): value is Promise<T> {
  return !!value && typeof value === "object" && "then" in value;
}

async function withTimeout<T>(promise: Promise<T>, timeoutMs: number): Promise<T> {
  if (timeoutMs <= 0 || !Number.isFinite(timeoutMs)) {
    return promise;
  }

  let timer: ReturnType<typeof setTimeout> | undefined;
  const timeoutPromise = new Promise<never>((_, reject) => {
    timer = setTimeout(() => {
      reject(new Error(`Query timed out after ${timeoutMs}ms.`));
    }, timeoutMs);
  });

  try {
    return await Promise.race([promise, timeoutPromise]);
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
): Promise<QueryCapabilityResolution<TContext>> {
  const fragment = buildProviderFragmentForRel(rel, input.schema, input.context);
  if (!fragment) {
    return {
      fragment: null,
      provider: null,
      report: null,
      diagnostics: [],
    };
  }

  const provider = input.providers[fragment.provider] ?? null;
  if (!provider) {
    return {
      fragment,
      provider: null,
      report: null,
      diagnostics: [],
    };
  }

  const report = normalizeCapability(await provider.canExecute(fragment, input.context));
  return {
    fragment,
    provider,
    report,
    diagnostics: buildCapabilityDiagnostics(provider, fragment, report, input.fallbackPolicy),
  };
}

function resolveSyncProviderCapabilityForRel<TContext>(
  input: QueryInput<TContext>,
  rel: RelNode,
): QueryCapabilityResolution<TContext> | null {
  const fragment = buildProviderFragmentForRel(rel, input.schema, input.context);
  if (!fragment) {
    return {
      fragment: null,
      provider: null,
      report: null,
      diagnostics: [],
    };
  }

  const provider = input.providers[fragment.provider] ?? null;
  if (!provider) {
    return {
      fragment,
      provider: null,
      report: null,
      diagnostics: [],
    };
  }

  const capability = provider.canExecute(fragment, input.context);
  if (isPromiseLike(capability)) {
    return null;
  }

  const report = normalizeCapability(capability);
  return {
    fragment,
    provider,
    report,
    diagnostics: buildCapabilityDiagnostics(provider, fragment, report, input.fallbackPolicy),
  };
}

function maybeRejectFallback<TContext>(
  input: QueryInput<TContext>,
  resolution: QueryCapabilityResolution<TContext>,
): void {
  if (!resolution.provider || !resolution.report || resolution.report.supported) {
    return;
  }

  const policy = resolveFallbackPolicy(input.fallbackPolicy, resolution.provider.fallbackPolicy);
  const exceedsEstimatedCost =
    policy.rejectOnEstimatedCost &&
    resolution.report.estimatedCost != null &&
    Number.isFinite(policy.maxJoinExpansionRisk) &&
    resolution.report.estimatedCost > policy.maxJoinExpansionRisk;

  if (!policy.allowFallback || policy.rejectOnMissingAtom || exceedsEstimatedCost) {
    const diagnostics = resolution.diagnostics.length > 0
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
    throw new SqlqlDiagnosticError(summarizeCapabilityReason(resolution.report), diagnostics);
  }
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

function assertNoSqlNodesWithoutProviderFragment(rel: RelNode): void {
  if (hasSqlNode(rel)) {
    throw new Error(
      "Query lowered to a SQL-shaped relational node that cannot be executed by the provider runtime without provider rel pushdown.",
    );
  }
}

async function maybeExecuteWholeQueryFragment<TContext>(
  input: QueryInput<TContext>,
  rel: RelNode,
): Promise<QueryRow[] | null> {
  const resolution = await resolveProviderCapabilityForRel(input, rel);
  if (!resolution.fragment || !resolution.provider || !resolution.report) {
    return null;
  }

  if (!resolution.report.supported) {
    maybeRejectFallback(input, resolution);
    return null;
  }

  const compiled = await resolution.provider.compile(resolution.fragment, input.context);
  return resolution.provider.execute(compiled, input.context);
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

  const run = async (): Promise<QueryRow[]> => {
    if (executed) {
      return result ?? [];
    }

    executed = true;
    const startedAt = Date.now();
    state = {
      ...state,
      status: "running",
      startedAt,
    };

    const compiled = await provider.compile(fragment, input.context);
    let rows = await withTimeout(provider.execute(compiled, input.context), guardrails.timeoutMs);
    if (fragment.kind === "scan" && rel.kind === "scan") {
      const binding = getNormalizedTableBinding(input.schema, rel.table);
      rows = mapProviderRowsToLogical(
        rows,
        rel.select,
        binding?.kind === "physical" ? binding : null,
        input.schema.tables[rel.table],
      );
    }
    enforceExecutionRowLimit(rows, guardrails);
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

    return rows;
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
  const plan = buildRelExecutionPlan(rel, diagnostics);
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

  const run = async (): Promise<QueryRow[]> => {
    if (executed) {
      return result ?? [];
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

    try {
      const rows = await withTimeout(
        executeRelWithProviders(
          rel,
          input.schema,
          input.providers,
          input.context,
          {
            maxExecutionRows: guardrails.maxExecutionRows,
            maxLookupKeysPerBatch: guardrails.maxLookupKeysPerBatch,
            maxLookupBatches: guardrails.maxLookupBatches,
          },
        ),
        guardrails.timeoutMs,
      );
      enforceExecutionRowLimit(rows, guardrails);
      result = rows;

      const endedAt = Date.now();
      const duration = Math.max(endedAt - startedAt, 1);
      const stepCount = Math.max(executionOrder.length, 1);
      emittedEvents = executionOrder.map((stepId, index) => {
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
          ...(isRoot ? { rowCount: rows.length, outputRowCount: rows.length } : {}),
          ...(isRoot && input.options?.captureRows === "full" ? { rows } : {}),
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
          ...(isRoot ? { rowCount: rows.length, outputRowCount: rows.length } : {}),
          ...(isRoot && input.options?.captureRows === "full" ? { rows } : {}),
          ...(step.diagnostics ? { diagnostics: step.diagnostics } : {}),
        };
        return event;
      });

      return rows;
    } catch (error) {
      const endedAt = Date.now();
      if (rootStepId) {
        const rootState = states.get(rootStepId);
        if (rootState) {
          states.set(rootStepId, {
            ...rootState,
            status: "failed",
            endedAt,
            durationMs: endedAt - (rootState.startedAt ?? startedAt),
            error: error instanceof Error ? error.message : String(error),
            ...(diagnostics.length > 0 ? { diagnostics } : {}),
          });
        }
      }
      throw error;
    }
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

function buildRelExecutionPlan(rel: RelNode, diagnostics: SqlqlDiagnostic[] = []): QueryExecutionPlan {
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

  visit(rel, "scope_root");

  return {
    steps,
    scopes,
    ...(diagnostics.length > 0 ? { diagnostics } : {}),
  };
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
): QuerySession | null {
  const resolution = resolveSyncProviderCapabilityForRel(input, rel);
  if (!resolution || !resolution.fragment || !resolution.provider || !resolution.report) {
    return null;
  }

  if (!resolution.report.supported) {
    maybeRejectFallback(input, resolution);
    return null;
  }

  return createProviderFragmentSession(
    input,
    guardrails,
    resolution.provider,
    resolution.fragment.provider,
    resolution.fragment,
    rel,
    resolution.diagnostics,
  );
}

function normalizeRuntimeSchema<TContext>(input: QueryInput<TContext>): QueryInput<TContext> {
  const schema = resolveSchemaLinkedEnums(input.schema);
  validateProviderBindings(schema, input.providers);
  return {
    ...input,
    schema,
  };
}

function createQuerySessionInternal<TContext>(input: QuerySessionInput<TContext>): QuerySession {
  const resolvedInput = normalizeRuntimeSchema(input);
  const guardrails = resolveGuardrails(input.queryGuardrails);
  const lowered = lowerSqlToRel(resolvedInput.sql, resolvedInput.schema);

  const plannerNodeCount = countRelNodes(lowered.rel);
  if (plannerNodeCount > guardrails.maxPlannerNodes) {
    throw new Error(
      `Query exceeded maxPlannerNodes guardrail (${guardrails.maxPlannerNodes}). Planned ${plannerNodeCount} nodes.`,
    );
  }

  const providerSession = tryCreateSyncProviderFragmentSession(
    resolvedInput,
    guardrails,
    lowered.rel,
  );
  if (providerSession) {
    return providerSession;
  }

  const capabilityResolution = resolveSyncProviderCapabilityForRel(resolvedInput, lowered.rel);
  if (capabilityResolution) {
    maybeRejectFallback(resolvedInput, capabilityResolution);
  }
  const expandedRel = expandRelViews(lowered.rel, resolvedInput.schema, resolvedInput.context);
  assertNoSqlNodesWithoutProviderFragment(expandedRel);
  return createRelExecutionSession(
    resolvedInput,
    guardrails,
    expandedRel,
    capabilityResolution?.diagnostics ?? [],
  );
}

async function queryInternal<TContext>(input: QueryInput<TContext>): Promise<QueryRow[]> {
  const resolvedInput = normalizeRuntimeSchema(input);
  const guardrails = resolveGuardrails(input.queryGuardrails);
  const lowered = lowerSqlToRel(resolvedInput.sql, resolvedInput.schema);
  const plannerNodeCount = countRelNodes(lowered.rel);

  if (plannerNodeCount > guardrails.maxPlannerNodes) {
    throw new Error(
      `Query exceeded maxPlannerNodes guardrail (${guardrails.maxPlannerNodes}). Planned ${plannerNodeCount} nodes.`,
    );
  }

  const remoteRows = await withTimeout(
    maybeExecuteWholeQueryFragment(resolvedInput, lowered.rel),
    guardrails.timeoutMs,
  );

  if (remoteRows) {
    enforceExecutionRowLimit(remoteRows, guardrails);
    return remoteRows;
  }

  const expandedRel = expandRelViews(lowered.rel, resolvedInput.schema, resolvedInput.context);
  assertNoSqlNodesWithoutProviderFragment(expandedRel);

  const rows = await withTimeout(
    executeRelWithProviders(
      expandedRel,
      resolvedInput.schema,
      resolvedInput.providers,
      resolvedInput.context,
      {
        maxExecutionRows: guardrails.maxExecutionRows,
        maxLookupKeysPerBatch: guardrails.maxLookupKeysPerBatch,
        maxLookupBatches: guardrails.maxLookupBatches,
      },
    ),
    guardrails.timeoutMs,
  );

  enforceExecutionRowLimit(rows, guardrails);
  return rows;
}

export interface ExplainResult {
  rel: RelNode;
  plannerNodeCount: number;
  guardrails: QueryGuardrails;
  diagnostics?: SqlqlDiagnostic[];
}

function explainInternal<TContext>(input: QueryInput<TContext>): ExplainResult {
  const resolvedInput = normalizeRuntimeSchema(input);
  const guardrails = resolveGuardrails(input.queryGuardrails);
  const lowered = lowerSqlToRel(resolvedInput.sql, resolvedInput.schema);
  const capabilityResolution = resolveSyncProviderCapabilityForRel(resolvedInput, lowered.rel);

  return {
    rel: lowered.rel,
    plannerNodeCount: countRelNodes(lowered.rel),
    guardrails,
    ...(capabilityResolution?.diagnostics.length
      ? { diagnostics: capabilityResolution.diagnostics }
      : {}),
  };
}

function collectExecutableProviders<TContext>(schema: SchemaDefinition): ProvidersMap<TContext> {
  const providers: ProvidersMap<TContext> = {};

  for (const [tableName] of Object.entries(schema.tables)) {
    const binding = getNormalizedTableBinding(schema, tableName);
    if (!binding || binding.kind === "view") {
      continue;
    }

    const provider = binding.adapter as ProviderAdapter<TContext> | undefined;
    if (!provider) {
      throw new Error(
        `Table ${tableName} must be declared from a provider-owned entity via table({ from: provider.entities... }).`,
      );
    }

    const existing = providers[provider.name];
    if (existing && existing !== provider) {
      throw new Error(`Duplicate provider name detected in executable schema: ${provider.name}.`);
    }
    providers[provider.name] = provider;

    if (!binding.provider || binding.provider !== provider.name) {
      throw new Error(
        `Table ${tableName} is bound to provider ${binding.provider ?? "<missing>"}, but the attached adapter is named ${provider.name}.`,
      );
    }
  }

  return providers;
}

export function createExecutableSchema<TContext>(
  schemaBuilder: (helpers: SchemaDslHelpers<TContext>) => SchemaDslDefinition<TContext>,
): ExecutableSchema<TContext, SchemaDefinition>;
export function createExecutableSchema<TContext, TSchema extends SchemaDefinition>(
  schema: TSchema,
): ExecutableSchema<TContext, TSchema>;
export function createExecutableSchema<TContext, TSchema extends SchemaDefinition>(
  input: TSchema | ((helpers: SchemaDslHelpers<TContext>) => SchemaDslDefinition<TContext>),
): ExecutableSchema<TContext, TSchema | SchemaDefinition> {
  const schema = defineSchema(input as never) as TSchema | SchemaDefinition;
  const providers = collectExecutableProviders<TContext>(schema);
  const runtime: ExecutableSchemaRuntime<TContext> = {
    schema,
    providers,
  };

  return {
    schema,
    query(input) {
      return queryInternal({
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
    explain(input) {
      return explainInternal({
        schema: runtime.schema,
        providers: runtime.providers,
        ...input,
      });
    },
  };
}

export function asProviderCompiledPlan(
  provider: string,
  kind: string,
  payload: unknown,
): ProviderCompiledPlan {
  return {
    provider,
    kind,
    payload,
  };
}
