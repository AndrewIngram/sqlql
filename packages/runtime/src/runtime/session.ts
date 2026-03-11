import { Result, type Result as BetterResult } from "better-result";

import { countRelNodes, TuplRuntimeError, type RelNode, type TuplError } from "@tupl/foundation";
import {
  supportsFragmentExecution,
  unwrapProviderOperationResult,
  type ProviderAdapter,
  type ProviderFragment,
} from "@tupl/provider-kit";
import { expandRelViewsResult, lowerSqlToRelResult } from "@tupl/planner";
import {
  getNormalizedTableBinding,
  mapProviderRowsToLogical,
  mapProviderRowsToRelOutput,
  type QueryRow,
} from "@tupl/schema-model";

import type {
  QueryExecutionPlan,
  QueryGuardrails,
  QuerySession,
  QuerySessionInput,
  QueryStepEvent,
  QueryStepState,
  TuplDiagnostic,
} from "./contracts";
import { tryQueryStep, tryQueryStepAsync, unwrapQueryResult } from "./diagnostics";
import { executeRelWithProvidersResult } from "./executor";
import { buildRelExecutionPlan, routeForStepKind } from "./plan-graph";
import {
  maybeRejectFallbackResult,
  resolveSyncProviderCapabilityForRel,
  resolveSyncProviderCapabilityForRelResult,
  withTimeoutResult,
} from "./provider-execution";
import {
  enforceExecutionRowLimitResult,
  enforcePlannerNodeLimitResult,
  resolveGuardrails,
} from "./policy";
import {
  assertNoSqlNodesWithoutProviderFragmentResult,
  normalizeRuntimeSchemaResult,
} from "./query-runner";

/**
 * Session owns runtime session construction, provider-fragment sessions, and replayable local sessions.
 */
function setFailedStepState(
  state: QueryStepState,
  error: TuplError,
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

export function createProviderFragmentSession<TContext>(
  input: QuerySessionInput<TContext>,
  guardrails: QueryGuardrails,
  provider: ProviderAdapter<TContext>,
  providerName: string,
  fragment: ProviderFragment,
  rel: RelNode,
  diagnostics: TuplDiagnostic[] = [],
): QuerySession {
  if (!supportsFragmentExecution(provider)) {
    throw new TuplRuntimeError({
      operation: "create provider fragment session",
      message: `Provider ${providerName} does not support compiled fragment execution.`,
    });
  }

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

  const runResult = async () => {
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

  const run = async (): Promise<QueryRow[]> => unwrapQueryResult(await runResult());

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
    runToCompletion: async () => run(),
    getResult: () => result,
    getStepState: (id: string) => (id === stepId ? state : undefined),
  };
}

export function createRelExecutionSession<TContext>(
  input: QuerySessionInput<TContext>,
  guardrails: QueryGuardrails,
  rel: RelNode,
  diagnostics: TuplDiagnostic[] = [],
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

  const runResult = async () => {
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
        executeRelWithProvidersResult(
          rel,
          input.schema,
          input.providers,
          input.context,
          {
            maxExecutionRows: guardrails.maxExecutionRows,
            maxLookupKeysPerBatch: guardrails.maxLookupKeysPerBatch,
            maxLookupBatches: guardrails.maxLookupBatches,
          },
          input.constraintValidation
            ? { constraintValidation: input.constraintValidation }
            : undefined,
        ).then(unwrapQueryResult),
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

    const completedRows = limitedRowsResult.value;
    result = completedRows;

    const eventBuildResult = tryQueryStep("build session step events", (): QueryStepEvent[] => {
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

        return {
          id: step.id,
          kind: step.kind,
          status: "done" as const,
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
      return Result.err(eventBuildResult.error);
    }

    emittedEvents = eventBuildResult.value;
    return Result.ok(completedRows);
  };

  const run = async (): Promise<QueryRow[]> => unwrapQueryResult(await runResult());

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

export function tryCreateSyncProviderFragmentSession<TContext>(
  input: QuerySessionInput<TContext>,
  guardrails: QueryGuardrails,
  rel: RelNode,
): BetterResult<QuerySession | null, TuplError> {
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

export function createQuerySessionResult<TContext>(
  input: QuerySessionInput<TContext>,
): BetterResult<QuerySession, TuplError> {
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

export function createQuerySessionInternal<TContext>(
  input: QuerySessionInput<TContext>,
): QuerySession {
  return unwrapQueryResult(createQuerySessionResult(input));
}
