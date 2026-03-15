import { Result } from "better-result";

import type { RelNode } from "@tupl/foundation";

import type { QueryGuardrails, TuplDiagnostic } from "../contracts";
import type { QuerySession, QuerySessionInput, QueryStepEvent, QueryStepState } from "./contracts";
import { tryQueryStep, unwrapQueryResult } from "../diagnostics";
import { executeRelWithProvidersResult } from "../executor";
import { buildRelExecutionPlan } from "../execution/execution-plan-builder";
import { routeForStepKind } from "../execution/step-routing";
import { withTimeoutResult } from "../provider/provider-execution";
import { enforceExecutionRowLimitResult } from "../policy";
import { setFailedStepState } from "./session-state";
import type { QueryRow } from "@tupl/schema-model";
import type { TuplError } from "@tupl/foundation";

/**
 * Rel-execution sessions own replayable local-runtime sessions over executable rel plans.
 */
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
          input.preparedSchema.schema,
          input.preparedSchema.providers,
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
          states.set(
            rootStepId,
            setFailedStepState(rootState, limitedRowsResult.error as TuplError, endedAt),
          );
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
          states.set(
            rootStepId,
            setFailedStepState(rootState, eventBuildResult.error as TuplError, endedAt),
          );
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
